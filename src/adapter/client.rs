//! Client driven by tokio.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use futures_channel::mpsc as events;
use futures_core::Stream;
use futures_util::future::{self, AbortHandle};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::Notify;
use tokio::time::Sleep;

use super::http::HttpTransport;
use super::transport::{Inbound, WebSocket};
use crate::machine::{
    Action, FetchId, FrameType, Input, Machine, SubscriptionId, SubscriptionTimer, Timer,
    TransportId, TransportKind,
};
use crate::{ClientError, ClientEvent, Config, DataError, State, SubscriptionEvent, TokenError};

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

/// What the client handle, its subscriptions and its driver task share.
#[derive(Debug)]
pub(super) struct Shared {
    pub(super) core: Mutex<Core>,
    subscribers: Mutex<Vec<events::UnboundedSender<ClientEvent>>>,
    /// Listeners of each subscription's events.
    pub(super) subscription_subscribers:
        Mutex<HashMap<SubscriptionId, Vec<events::UnboundedSender<SubscriptionEvent>>>>,
    /// Data and token callbacks of the subscriptions that have them.
    pub(super) callbacks: Mutex<SubscriptionCallbacks>,
    /// Wakes the driver when actions are queued.
    actions_queued: Notify,
    /// Set when the client handle is dropped: the driver stops once the
    /// queued actions are performed.
    pub(super) closed: AtomicBool,
}

/// The machine and its pending actions, locked together: API calls change
/// the state before they return, and the driver performs actions in the
/// order the machine produced them, whoever fed the input. Events are not
/// queued: they go out while the lock is held, so a stream created after a
/// call never receives that call's events.
#[derive(Debug)]
pub(super) struct Core {
    pub(super) machine: Machine,
    pub(super) actions: VecDeque<Action>,
}

/// Async callback returning the data of a subscription's next subscribe
/// command, given its channel.
pub(super) type GetData = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, DataError>> + Send>>
        + Send
        + Sync,
>;

/// Async callback returning a new subscription token, given the channel.
pub(super) type GetSubscriptionToken = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send>>
        + Send
        + Sync,
>;

/// The callbacks of a subscription.
#[derive(Clone, Default)]
pub(super) struct Callbacks {
    pub(super) get_data: Option<GetData>,
    pub(super) get_token: Option<GetSubscriptionToken>,
}

/// Callbacks by subscription; opaque in `Debug` output.
#[derive(Default)]
pub(super) struct SubscriptionCallbacks(pub(super) HashMap<SubscriptionId, Callbacks>);

impl std::fmt::Debug for SubscriptionCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl Shared {
    pub(super) fn apply(&self, input: Input) {
        let mut core = lock(&self.core);
        // The driver has stopped (the client was dropped, after its final
        // disconnect): actions would pile up with nothing to perform them,
        // e.g. from a subscription handle that outlived the client.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.handle(&mut core, input);
        drop(core);
        self.actions_queued.notify_one();
    }

    /// The client's final disconnect, closing it in the same locked
    /// section: an API call either runs before and is shut down with the
    /// rest, or after and sees the client closed.
    fn shut_down(&self) {
        let mut core = lock(&self.core);
        if !self.closed.load(Ordering::Acquire) {
            self.handle(&mut core, Input::Disconnect);
            self.closed.store(true, Ordering::Release);
        }
        drop(core);
        self.actions_queued.notify_one();
    }

    fn handle(&self, core: &mut Core, input: Input) {
        for action in core.machine.handle(input) {
            match action {
                Action::Emit(event) => self.emit(&event),
                Action::EmitSubscription(id, event) => self.emit_subscription(id, &event),
                action => core.actions.push_back(action),
            }
        }
    }

    fn emit(&self, event: &ClientEvent) {
        lock(&self.subscribers)
            .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
    }

    fn emit_subscription(&self, id: SubscriptionId, event: &SubscriptionEvent) {
        if let Some(subscribers) = lock(&self.subscription_subscribers).get_mut(&id) {
            subscribers.retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
        }
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
        let request_timeout = config.timeout;
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
            subscription_subscribers: Mutex::new(HashMap::new()),
            callbacks: Mutex::new(SubscriptionCallbacks::default()),
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
            request_timeout,
            shared: Arc::clone(&shared),
            opening: None,
            transport: None,
            get_token,
            fetching: None,
            timers: Timers::default(),
            subscription_timers: SubscriptionTimers::default(),
            subscription_fetches: FuturesUnordered::new(),
            subscription_fetch_aborts: HashMap::new(),
        };
        tokio::spawn(driver.run());
        Ok(Self { shared })
    }

    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        lock(&self.shared.core).machine.state()
    }

    /// Events from now on. Each call returns an independent stream; it
    /// borrows nothing from the client, so it can be kept beside it, and it
    /// ends once the client is dropped.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + Send + Unpin + use<> {
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
        self.shared.shut_down();
    }
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
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
    /// Time allowed for an emulation request (`Config::timeout`).
    request_timeout: Duration,
    shared: Arc<Shared>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, Transport)>,
    get_token: Option<GetToken>,
    fetching: Option<(FetchId, Fetching)>,
    timers: Timers,
    subscription_timers: SubscriptionTimers,
    /// Subscription data and token callbacks running, each yielding the
    /// input answering it; an aborted one yields `None`.
    subscription_fetches: FuturesUnordered<SubscriptionFetching>,
    /// Handles to abort the callbacks running, by fetch.
    subscription_fetch_aborts: HashMap<FetchId, AbortHandle>,
}

type SubscriptionFetching = Pin<Box<dyn Future<Output = Option<(FetchId, Input)>> + Send>>;

/// The machine's subscription timers, by subscription and kind.
#[derive(Default)]
struct SubscriptionTimers(HashMap<(SubscriptionId, SubscriptionTimer), Pin<Box<Sleep>>>);

impl SubscriptionTimers {
    fn set(&mut self, id: SubscriptionId, timer: SubscriptionTimer, delay: Duration) {
        self.0
            .insert((id, timer), Box::pin(tokio::time::sleep(delay)));
    }

    fn cancel(&mut self, id: SubscriptionId, timer: SubscriptionTimer) {
        self.0.remove(&(id, timer));
    }

    /// Resolves with the first timer to fire, which is removed; never while
    /// none is set. Cancel-safe.
    async fn fired(&mut self) -> (SubscriptionId, SubscriptionTimer) {
        std::future::poll_fn(|cx| {
            let fired = self
                .0
                .iter_mut()
                .find_map(|(key, sleep)| sleep.as_mut().poll(cx).is_ready().then_some(*key));
            match fired {
                Some(key) => {
                    self.0.remove(&key);
                    Poll::Ready(key)
                }
                None => Poll::Pending,
            }
        })
        .await
    }
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
                // Handles of subscriptions may outlive the client: end
                // every event stream rather than leave it pending forever,
                // and release the data callbacks nothing can call anymore.
                lock(&self.shared.subscribers).clear();
                lock(&self.shared.subscription_subscribers).clear();
                lock(&self.shared.callbacks).0.clear();
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
                (id, timer) = self.subscription_timers.fired() => {
                    Input::SubscriptionTimerFired(id, timer)
                }
                Some(fetched) = self.subscription_fetches.next(),
                    if !self.subscription_fetches.is_empty() =>
                {
                    // Aborted: the machine no longer wants it.
                    let Some((fetch, input)) = fetched else {
                        continue;
                    };
                    self.subscription_fetch_aborts.remove(&fetch);
                    input
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
                        let request_timeout = self.request_timeout;
                        Box::pin(async move {
                            HttpTransport::open(
                                kind,
                                &url,
                                initial_frame.unwrap_or_default(),
                                emulation.as_deref(),
                                request_timeout,
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
            Action::EmitSubscription(id, event) => self.shared.emit_subscription(id, &event),
            Action::SetSubscriptionTimer(id, timer, delay) => {
                self.subscription_timers.set(id, timer, delay);
            }
            Action::CancelSubscriptionTimer(id, timer) => {
                self.subscription_timers.cancel(id, timer);
            }
            Action::FetchSubscriptionData {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_data(subscription, channel, fetch),
            Action::FetchSubscriptionToken {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_token(subscription, channel, fetch),
            Action::CancelSubscriptionData(fetch) | Action::CancelSubscriptionToken(fetch) => {
                if let Some(abort) = self.subscription_fetch_aborts.remove(&fetch) {
                    abort.abort();
                }
            }
            Action::FetchToken(id) => {
                // The machine asks only when it was built with a token source.
                if let Some(get_token) = &self.get_token {
                    self.fetching = Some((id, guarded(|| get_token(), callback_panicked)));
                }
            }
        }
    }
}

impl Driver {
    fn fetch_subscription_data(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.get_data.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_data) = callback else {
            return;
        };
        let fetching = guarded(|| get_data(channel), data_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionDataFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    fn fetch_subscription_token(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.get_token.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_token) = callback else {
            return;
        };
        let fetching = guarded(|| get_token(channel), token_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionTokenFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    /// Runs a subscription callback, abortable by its fetch.
    fn start_fetch(
        &mut self,
        fetch: FetchId,
        answer: impl Future<Output = Input> + Send + 'static,
    ) {
        let (fetching, abort) = future::abortable(answer);
        self.subscription_fetch_aborts.insert(fetch, abort);
        self.subscription_fetches.push(Box::pin(
            fetching.map(move |input| input.ok().map(|input| (fetch, input))),
        ));
    }
}

/// The future of a callback, which fails with `panicked()` rather than
/// panicking the driver: whether the callback panics creating its future
/// or polling it.
fn guarded<T: Send + 'static, E: Send + 'static>(
    callback: impl FnOnce() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    panicked: fn() -> E,
) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>> {
    match std::panic::catch_unwind(AssertUnwindSafe(callback)) {
        Ok(future) => Box::pin(async move {
            AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(panicked()))
        }),
        Err(_) => Box::pin(std::future::ready(Err(panicked()))),
    }
}

fn token_callback_panicked() -> TokenError {
    TokenError::Failed("subscription token callback panicked".to_owned())
}

fn data_callback_panicked() -> DataError {
    DataError::Failed("subscription data callback panicked".to_owned())
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

    /// The stream borrows nothing: an application can keep it next to its
    /// client, returned together or in one struct.
    fn client_with_events() -> (Client, impl Stream<Item = ClientEvent> + Unpin) {
        let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default());
        let events = client.events();
        (client, events)
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn an_event_stream_does_not_borrow_its_client() {
        let (client, mut events) = client_with_events();

        client.connect();
        let first = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event");

        assert!(
            matches!(first, Some(ClientEvent::Connecting { .. })),
            "{first:?}"
        );
    }

    #[tokio::test]
    async fn a_build_error_is_a_standard_error() {
        fn build() -> Result<Client, Box<dyn std::error::Error>> {
            let client = Client::builder_with_transports([(
                TransportKind::HttpStream,
                "http://127.0.0.1:1/connection/http_stream".to_owned(),
            )])
            .config(Config {
                protocol: crate::Protocol::Protobuf,
                ..Config::default()
            })
            .build()?;
            Ok(client)
        }

        let error = build().expect_err("Protobuf over http_stream is refused");

        let client_error = error.downcast_ref::<ClientError>().expect("a ClientError");
        assert_eq!(
            error.to_string(),
            format!("{} (code 12) over http_stream", client_error.message)
        );
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
