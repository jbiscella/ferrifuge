//! Client driven by tokio.

use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use futures_channel::mpsc as events;
use futures_core::Stream;
use futures_util::FutureExt;
use tokio::sync::Notify;
use tokio::time::Sleep;

use super::http::HttpTransport;
use super::transport::{Inbound, WebSocket};
use crate::machine::{
    Action, FetchId, FrameType, Input, Machine, Timer, TransportId, TransportKind,
};
use crate::{ClientError, ClientEvent, Config, State, TokenError};

/// Centrifugo client running on tokio.
///
/// Dropping the client disconnects it.
pub struct Client {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Client {
    // Only the state: queued frames may carry the connection token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
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
    /// Transports to try in order, each with its endpoint.
    endpoints: Vec<(TransportKind, String)>,
    config: Config,
    get_token: Option<GetToken>,
    emulation_endpoint: Option<String>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoints", &self.endpoints)
            .field("config", &self.config)
            .field("get_token", &self.get_token.is_some())
            .field("emulation_endpoint", &self.emulation_endpoint)
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

    /// Sets the absolute URL of the emulation endpoint, where commands over
    /// `http_stream` and sse go, e.g. when the server is behind a path
    /// prefix; by default `/emulation` on the server of the stream endpoint
    /// (centrifuge-js `emulationEndpoint`).
    #[must_use]
    pub fn emulation_endpoint(mut self, url: impl Into<String>) -> Self {
        self.emulation_endpoint = Some(url.into());
        self
    }

    /// Creates the disconnected client.
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error when the options cannot
    /// work together, e.g. the Protobuf protocol with an HTTP transport.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime: the client runs as a task on it.
    pub fn build(self) -> Result<Client, ClientError> {
        Client::start(self)
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
        Self::builder(endpoint)
            .config(config)
            .build()
            .expect("a websocket client takes any options")
    }

    /// Starts building a client for the websocket `endpoint`.
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        Self::builder_with_transports([(TransportKind::WebSocket, endpoint.into())])
    }

    /// Starts building a client that tries the `endpoints` in order on its
    /// first connection, e.g. a websocket endpoint, then `http_stream`
    /// and `sse` ones as fallbacks; its reconnects keep the transport that
    /// worked.
    pub fn builder_with_transports(
        endpoints: impl IntoIterator<Item = (TransportKind, String)>,
    ) -> ClientBuilder {
        ClientBuilder {
            endpoints: endpoints.into_iter().collect(),
            config: Config::default(),
            get_token: None,
            emulation_endpoint: None,
        }
    }

    fn start(builder: ClientBuilder) -> Result<Self, ClientError> {
        let ClientBuilder {
            endpoints,
            config,
            get_token,
            emulation_endpoint,
        } = builder;
        let subprotocol = config.protocol.websocket_subprotocol();
        let machine = if get_token.is_some() {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        let machine = machine.with_transports(endpoints.iter().map(|(kind, _)| *kind).collect())?;
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
            endpoints: endpoints
                .into_iter()
                .map(|(_, endpoint)| endpoint)
                .collect(),
            subprotocol,
            emulation_endpoint,
            shared: Arc::clone(&shared),
            opening: None,
            transport: None,
            get_token,
            fetching: None,
            timers: Timers::default(),
        };
        tokio::spawn(driver.run());
        Ok(Self { shared })
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

type Opening = Pin<Box<dyn Future<Output = Result<Transport, String>> + Send>>;
type Fetching = Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send>>;

/// Performs the machine's actions, and turns transport activity and timers
/// into inputs for it.
struct Driver {
    /// Endpoints of the transports, in the machine's order.
    endpoints: Vec<String>,
    /// Websocket subprotocol of the configured encoding, if it needs one.
    subprotocol: Option<&'static str>,
    /// Where commands over emulated transports go, when not the default.
    emulation_endpoint: Option<String>,
    shared: Arc<Shared>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, Transport)>,
    get_token: Option<GetToken>,
    fetching: Option<(FetchId, Fetching)>,
    timers: Timers,
}

/// The machine's timers: one slot per [`Timer`] kind, so a new kind needs
/// no change here.
#[derive(Default)]
struct Timers([Option<Pin<Box<Sleep>>>; Timer::COUNT]);

impl Timers {
    fn set(&mut self, timer: Timer, delay: Duration) {
        // sleep() clamps delays too large for an Instant instead of panicking.
        self.0[timer.index()] = Some(Box::pin(tokio::time::sleep(delay)));
    }

    fn cancel(&mut self, timer: Timer) {
        self.0[timer.index()] = None;
    }

    /// Resolves with the first timer to fire, which is cleared; never while
    /// none is set. Cancel-safe: unfired timers keep running.
    async fn fired(&mut self) -> Timer {
        std::future::poll_fn(|cx| {
            for timer in Timer::ALL {
                let slot = &mut self.0[timer.index()];
                if slot
                    .as_mut()
                    .is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready())
                {
                    *slot = None;
                    return Poll::Ready(timer);
                }
            }
            Poll::Pending
        })
        .await
    }
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
                        Ok(transport) => {
                            self.transport = Some((id, transport));
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
                timer = self.timers.fired() => Input::TimerFired(timer),
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
            Action::OpenTransport {
                transport: id,
                kind,
                endpoint,
                initial_frame,
            } => {
                let url = self.endpoints.get(endpoint).cloned().unwrap_or_default();
                let subprotocol = self.subprotocol;
                let opening: Opening = match kind {
                    TransportKind::WebSocket => Box::pin(async move {
                        WebSocket::open(&url, subprotocol)
                            .await
                            .map(|websocket| Transport::WebSocket(Box::new(websocket)))
                    }),
                    TransportKind::HttpStream | TransportKind::Sse => {
                        let emulation = self.emulation_endpoint.clone();
                        Box::pin(async move {
                            HttpTransport::open(
                                kind,
                                &url,
                                initial_frame.unwrap_or_default(),
                                emulation.as_deref(),
                            )
                            .await
                            .map(|http| Transport::Http(Box::new(http)))
                        })
                    }
                };
                self.opening = Some((id, opening));
            }
            Action::CloseTransport(id) => {
                if self
                    .opening
                    .as_ref()
                    .is_some_and(|(opening, _)| *opening == id)
                {
                    self.opening = None;
                }
                if let Some((_, transport)) = self.transport.take_if(|(open, _)| *open == id) {
                    transport.close();
                }
            }
            Action::Send {
                transport,
                frame,
                frame_type,
            } => {
                if let Some((id, open)) = &mut self.transport
                    && *id == transport
                    && open.send(frame, frame_type).await.is_err()
                {
                    self.transport = None;
                    self.shared.apply(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                }
            }
            Action::SetTimer(timer, delay) => self.timers.set(timer, delay),
            Action::CancelTimer(timer) => self.timers.cancel(timer),
            Action::Emit(event) => self.shared.emit(&event),
            Action::FetchToken(id) => {
                // The machine asks only when it was built with a token source.
                if let Some(get_token) = &self.get_token {
                    // A panicking callback fails this fetch, not the driver:
                    // whether it panics creating its future or polling it.
                    let fetching: Fetching =
                        match std::panic::catch_unwind(AssertUnwindSafe(|| get_token())) {
                            Ok(future) => Box::pin(async move {
                                AssertUnwindSafe(future)
                                    .catch_unwind()
                                    .await
                                    .unwrap_or_else(|_| Err(callback_panicked()))
                            }),
                            Err(_) => Box::pin(std::future::ready(Err(callback_panicked()))),
                        };
                    self.fetching = Some((id, fetching));
                }
            }
        }
    }
}

fn callback_panicked() -> TokenError {
    TokenError::Failed("token callback panicked".to_owned())
}

/// An open transport of any kind.
enum Transport {
    // Boxed: the transports differ much in size, and one is held at a time.
    WebSocket(Box<WebSocket>),
    Http(Box<HttpTransport>),
}

impl Transport {
    async fn send(&mut self, frame: Vec<u8>, frame_type: FrameType) -> Result<(), String> {
        match self {
            Self::WebSocket(websocket) => websocket.send(frame, frame_type).await,
            // Failures end the transport through `recv`.
            Self::Http(http) => {
                http.send(frame);
                Ok(())
            }
        }
    }

    /// Cancel-safe.
    async fn recv(&mut self) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv().await,
            Self::Http(http) => http.recv().await,
        }
    }

    fn close(self) {
        match self {
            Self::WebSocket(websocket) => {
                tokio::spawn(websocket.close());
            }
            // Dropping the response and the requests in flight closes them.
            Self::Http(_) => {}
        }
    }
}

/// Resolves when the transport being opened is ready; never without one.
async fn opened(
    opening: &mut Option<(TransportId, Opening)>,
) -> (TransportId, Result<Transport, String>) {
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
async fn received(transport: &mut Option<(TransportId, Transport)>) -> (TransportId, Inbound) {
    match transport {
        Some((id, transport)) => (*id, transport.recv().await),
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
            .build()
            .unwrap();
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
    async fn token_callback_panicking_before_its_future_is_a_failed_fetch() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(|| -> std::future::Ready<Result<String, TokenError>> {
                panic!("callback bug")
            })
            .build()
            .unwrap();
        let mut events = client.events();

        client.connect();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
        };
        next().await;
        let error = next().await;

        assert!(
            matches!(&error, Some(ClientEvent::Error(e)) if e.message == "token callback panicked"),
            "{error:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn debug_output_of_builder_and_client_never_shows_tokens() {
        let config = Config {
            token: "secret-token".into(),
            ..Config::default()
        };
        let builder = Client::builder("ws://127.0.0.1:1/connection/websocket").config(config);
        let printed_builder = format!("{builder:?}");
        let client = builder.build().unwrap();
        // Queues the connect command, which carries the token, unperformed.
        client.connect();

        let printed = format!("{printed_builder} {client:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    #[tokio::test]
    async fn timers_fire_in_deadline_order_and_cancelled_ones_never() {
        let mut timers = Timers::default();
        timers.set(Timer::ServerPing, Duration::from_millis(60));
        timers.set(Timer::Refresh, Duration::from_millis(20));
        timers.set(Timer::Reconnect, Duration::from_millis(40));
        timers.cancel(Timer::Reconnect);

        let first = timers.fired().await;
        let second = timers.fired().await;
        let none_left = tokio::time::timeout(Duration::from_millis(100), timers.fired()).await;

        assert_eq!((first, second), (Timer::Refresh, Timer::ServerPing));
        assert!(none_left.is_err(), "{none_left:?}");
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
