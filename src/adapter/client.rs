//! Client driven by tokio.

use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures_channel::mpsc as events;
use futures_core::Stream;
use futures_util::FutureExt;
use tokio::sync::Notify;
use tokio::time::Sleep;

use super::transport::{Inbound, WebSocket};
use crate::machine::{Action, FetchId, Input, Machine, Timer, TransportId};
use crate::{ClientEvent, Config, State, TokenError};

/// Centrifugo client running on tokio.
///
/// Dropping the client disconnects it.
#[derive(Debug)]
pub struct Client {
    shared: Arc<Shared>,
}

/// What the client handle and its driver task share.
#[derive(Debug)]
struct Shared {
    core: Mutex<Core>,
    subscribers: Mutex<Vec<events::UnboundedSender<ClientEvent>>>,
    /// Wakes the driver when actions are queued.
    actions_queued: Notify,
    /// Set when the client handle is dropped: the driver stops once the
    /// queued actions are performed.
    closed: AtomicBool,
}

/// The machine and its pending actions, locked together: API calls change
/// the state before they return, and the driver performs actions in the
/// order the machine produced them, whoever fed the input. Events are not
/// queued: they go out while the lock is held, so a stream created after a
/// call never receives that call's events.
#[derive(Debug)]
struct Core {
    machine: Machine,
    actions: VecDeque<Action>,
}

impl Shared {
    fn apply(&self, input: Input) {
        let mut core = lock(&self.core);
        for action in core.machine.handle(input) {
            match action {
                Action::Emit(event) => self.emit(&event),
                action => core.actions.push_back(action),
            }
        }
        drop(core);
        self.actions_queued.notify_one();
    }

    fn emit(&self, event: &ClientEvent) {
        lock(&self.subscribers)
            .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
    }

    fn next_action(&self) -> Option<Action> {
        lock(&self.core).actions.pop_front()
    }
}

/// Async callback returning a fresh connection token.
type GetToken =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send>> + Send + Sync>;

/// Builds a [`Client`] with options beyond [`Config`].
pub struct ClientBuilder {
    endpoint: String,
    config: Config,
    get_token: Option<GetToken>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoint", &self.endpoint)
            .field("config", &self.config)
            .field("get_token", &self.get_token.is_some())
            .finish()
    }
}

impl ClientBuilder {
    /// Sets the options; [`Config::default`] otherwise.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets the callback that provides a new connection token when the
    /// current one expires, or when there is none to connect with.
    /// Return [`TokenError::Unauthorized`] to disconnect the client for good.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, get_token: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + Send + 'static,
    {
        self.get_token = Some(Arc::new(move || Box::pin(get_token())));
        self
    }

    /// Creates the disconnected client.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime: the client runs as a task on it.
    #[must_use]
    pub fn build(self) -> Client {
        Client::start(self.endpoint, self.config, self.get_token)
    }
}

impl Client {
    /// Creates a disconnected client for the websocket `endpoint`, e.g.
    /// `ws://localhost:8000/connection/websocket`.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime: the client runs as a task on it.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, config: Config) -> Self {
        Self::builder(endpoint).config(config).build()
    }

    /// Starts building a client for the websocket `endpoint`.
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            endpoint: endpoint.into(),
            config: Config::default(),
            get_token: None,
        }
    }

    fn start(endpoint: String, config: Config, get_token: Option<GetToken>) -> Self {
        let machine = if get_token.is_some() {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        let shared = Arc::new(Shared {
            core: Mutex::new(Core {
                machine,
                actions: VecDeque::new(),
            }),
            subscribers: Mutex::new(Vec::new()),
            actions_queued: Notify::new(),
            closed: AtomicBool::new(false),
        });
        let driver = Driver {
            endpoint,
            shared: Arc::clone(&shared),
            opening: None,
            transport: None,
            get_token,
            fetching: None,
            reconnect: None,
            connect_timeout: None,
            refresh: None,
        };
        tokio::spawn(driver.run());
        Self { shared }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        lock(&self.shared.core).machine.state()
    }

    /// Events from now on. Each call returns an independent stream.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + Send + Unpin + 'static {
        let (sender, receiver) = events::unbounded();
        lock(&self.shared.subscribers).push(sender);
        receiver
    }

    /// Starts connecting; reconnects until connected or disconnected. The
    /// state is [`State::Connecting`] when this returns.
    pub fn connect(&self) {
        self.shared.apply(Input::Connect);
    }

    /// Disconnects and stops reconnecting. The state is
    /// [`State::Disconnected`] when this returns.
    pub fn disconnect(&self) {
        self.shared.apply(Input::Disconnect);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shared.apply(Input::Disconnect);
        self.shared.closed.store(true, Ordering::Release);
        self.shared.actions_queued.notify_one();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // No code panics while holding these locks; recover rather than cascade.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

type Opening = Pin<Box<dyn Future<Output = Result<WebSocket, String>> + Send>>;
type Fetching = Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send>>;

/// Performs the machine's actions, and turns transport activity and timers
/// into inputs for it.
struct Driver {
    endpoint: String,
    shared: Arc<Shared>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, WebSocket)>,
    get_token: Option<GetToken>,
    fetching: Option<(FetchId, Fetching)>,
    reconnect: Option<Pin<Box<Sleep>>>,
    connect_timeout: Option<Pin<Box<Sleep>>>,
    refresh: Option<Pin<Box<Sleep>>>,
}

impl Driver {
    async fn run(mut self) {
        loop {
            while let Some(action) = self.shared.next_action() {
                self.perform(action).await;
            }
            if self.shared.closed.load(Ordering::Acquire) {
                return;
            }
            let input = tokio::select! {
                // Queued by an API call; performed at the top of the loop.
                () = self.shared.actions_queued.notified() => continue,
                (id, result) = opened(&mut self.opening) => {
                    self.opening = None;
                    match result {
                        Ok(websocket) => {
                            self.transport = Some((id, websocket));
                            Input::TransportOpened(id)
                        }
                        Err(message) => Input::TransportFailed { transport: id, message },
                    }
                }
                (id, inbound) = received(&mut self.transport) => match inbound {
                    Inbound::Frame(data) => Input::Frame { transport: id, data },
                    Inbound::Closed { code, reason } => {
                        self.transport = None;
                        Input::TransportClosed { transport: id, code, reason }
                    }
                },
                () = fired(&mut self.reconnect) => {
                    self.reconnect = None;
                    Input::TimerFired(Timer::Reconnect)
                }
                () = fired(&mut self.connect_timeout) => {
                    self.connect_timeout = None;
                    Input::TimerFired(Timer::ConnectTimeout)
                }
                () = fired(&mut self.refresh) => {
                    self.refresh = None;
                    Input::TimerFired(Timer::Refresh)
                }
                (fetch, result) = fetched(&mut self.fetching) => {
                    self.fetching = None;
                    Input::TokenFetched { fetch, result }
                }
            };
            self.shared.apply(input);
        }
    }

    async fn perform(&mut self, action: Action) {
        match action {
            Action::OpenTransport(id) => {
                let endpoint = self.endpoint.clone();
                self.opening = Some((
                    id,
                    Box::pin(async move { WebSocket::open(&endpoint).await }),
                ));
            }
            Action::CloseTransport(id) => {
                if self
                    .opening
                    .as_ref()
                    .is_some_and(|(opening, _)| *opening == id)
                {
                    self.opening = None;
                }
                if let Some((_, websocket)) = self.transport.take_if(|(open, _)| *open == id) {
                    tokio::spawn(websocket.close());
                }
            }
            Action::Send { transport, frame } => {
                if let Some((id, websocket)) = &mut self.transport
                    && *id == transport
                    && websocket.send(frame).await.is_err()
                {
                    self.transport = None;
                    self.shared.apply(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                }
            }
            Action::SetTimer(timer, delay) => {
                // sleep() clamps delays too large for an Instant instead of panicking.
                *self.timer(timer) = Some(Box::pin(tokio::time::sleep(delay)));
            }
            Action::CancelTimer(timer) => *self.timer(timer) = None,
            Action::Emit(event) => self.shared.emit(&event),
            Action::FetchToken(id) => {
                // The machine asks only when it was built with a token source.
                if let Some(get_token) = &self.get_token {
                    // A panicking callback fails this fetch, not the driver.
                    let fetch = AssertUnwindSafe(get_token()).catch_unwind();
                    self.fetching = Some((
                        id,
                        Box::pin(async move {
                            fetch.await.unwrap_or_else(|_| {
                                Err(TokenError::Failed("token callback panicked".to_owned()))
                            })
                        }),
                    ));
                }
            }
        }
    }

    fn timer(&mut self, timer: Timer) -> &mut Option<Pin<Box<Sleep>>> {
        match timer {
            Timer::Reconnect => &mut self.reconnect,
            Timer::ConnectTimeout => &mut self.connect_timeout,
            Timer::Refresh => &mut self.refresh,
        }
    }
}

/// Resolves when the transport being opened is ready; never without one.
async fn opened(
    opening: &mut Option<(TransportId, Opening)>,
) -> (TransportId, Result<WebSocket, String>) {
    match opening {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

/// Resolves with the token callback's answer; never without a fetch.
async fn fetched(
    fetching: &mut Option<(FetchId, Fetching)>,
) -> (FetchId, Result<String, TokenError>) {
    match fetching {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

/// Resolves with the next inbound message; never without a transport.
async fn received(transport: &mut Option<(TransportId, WebSocket)>) -> (TransportId, Inbound) {
    match transport {
        Some((id, websocket)) => (*id, websocket.recv().await),
        None => std::future::pending().await,
    }
}

/// Resolves when the timer fires; never without one.
async fn fired(timer: &mut Option<Pin<Box<Sleep>>>) {
    match timer {
        Some(sleep) => sleep.await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::*;

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn state_changes_as_soon_as_connect_and_disconnect_return() {
        let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default());

        client.connect();
        let after_connect = client.state();
        client.disconnect();
        let after_disconnect = client.state();

        assert_eq!(after_connect, State::Connecting);
        assert_eq!(after_disconnect, State::Disconnected);
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn events_stream_starts_after_events_already_emitted() {
        let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default());

        client.connect();
        let mut events = client.events();
        client.disconnect();
        let first = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event");

        assert!(
            matches!(first, Some(ClientEvent::Disconnected { .. })),
            "{first:?}"
        );
    }

    #[tokio::test]
    async fn panicking_token_callback_is_reported_as_a_failed_fetch() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(|| async { panic!("callback bug") })
            .build();
        let mut events = client.events();

        client.connect();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
        };
        let connecting = next().await;
        let error = next().await;

        assert!(
            matches!(connecting, Some(ClientEvent::Connecting { .. })),
            "{connecting:?}"
        );
        assert!(
            matches!(&error, Some(ClientEvent::Error(e)) if e.message == "token callback panicked"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn huge_connect_timeout_does_not_panic_the_driver() {
        let client = Client::new(
            "ws://127.0.0.1:1/connection/websocket",
            Config {
                timeout: Duration::MAX,
                ..Config::default()
            },
        );
        let mut events = client.events();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("driver stopped emitting events")
        };

        client.connect();
        let connecting = next().await;
        let error = next().await;
        client.disconnect();
        let disconnected = next().await;

        assert!(
            matches!(connecting, Some(ClientEvent::Connecting { .. })),
            "{connecting:?}"
        );
        assert!(matches!(error, Some(ClientEvent::Error(_))), "{error:?}");
        assert!(
            matches!(disconnected, Some(ClientEvent::Disconnected { .. })),
            "{disconnected:?}"
        );
    }
}
