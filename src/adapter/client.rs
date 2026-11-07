//! Client driven by tokio.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use super::outbox::Outbox;
use futures_core::Stream;
use futures_util::future::{self, AbortHandle};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rustls::pki_types::CertificateDer;
use tokio::sync::{Notify, oneshot};
use tokio::time::Sleep;
use tracing::Instrument;

use super::http::HttpTransport;
use super::log::{self, Log};
use super::net::Network;
use super::proxy::Proxy;
use super::tls::{self, Tls, Trust, unparsable_root};
use super::transport::{self as websocket, Inbound, WebSocket};
use crate::machine::{
    Action, CallId, CallTimer, FetchId, FrameId, FrameType, Input, Machine, SubscriptionId,
    SubscriptionTimer, Timer, TransportId, TransportKind,
};
use crate::{
    CallError, CallReply, CallRequest, ClientError, ClientEvent, Config, DataError, ErrorKind,
    HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, State, StateError,
    StreamPosition, SubscriptionEvent, TokenError, codes,
};
use tracing::{debug, trace, warn};

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

/// Websocket close code "message too big" (RFC 6455).
const MESSAGE_TOO_BIG: u16 = 1009;

/// What the client handle, its subscriptions and its driver task share.
#[derive(Debug)]
pub(super) struct Shared {
    pub(super) core: Mutex<Core>,
    /// Events waiting for the streams of the client and its subscriptions.
    pub(super) outboxes: Mutex<Outboxes>,
    /// Data and token callbacks of the subscriptions that have them.
    pub(super) callbacks: Mutex<SubscriptionCallbacks>,
    /// Where the outcome of each call goes, by call (Block 13).
    calls: Mutex<HashMap<CallId, oneshot::Sender<Result<CallReply, CallError>>>>,
    /// Wakes the driver when actions are queued.
    actions_queued: Notify,
    /// Set when the client handle is dropped: the driver stops once the
    /// queued actions are performed.
    pub(super) closed: AtomicBool,
    /// Spans and records of this client (Block 21).
    pub(super) log: Log,
    /// Its TLS and proxy settings and HTTP clients (Blocks 22, 23).
    pub(super) network: Arc<Network>,
}

/// Events waiting for their streams (Block 15). Locked after
/// [`Shared::core`], never before.
#[derive(Debug, Default)]
pub(super) struct Outboxes {
    pub(super) client: Outbox<ClientEvent>,
    /// Those of the subscriptions whose `events()` was called.
    pub(super) subscriptions: HashMap<SubscriptionId, Outbox<SubscriptionEvent>>,
}

impl Outboxes {
    fn withdraw(&mut self, ids: &[crate::machine::EventId]) {
        self.client.withdraw(ids);
        for outbox in self.subscriptions.values_mut() {
            outbox.withdraw(ids);
        }
    }
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

/// Async callback loading the application's state for a channel, giving
/// the stream position it was loaded at.
pub(super) type GetState = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<StreamPosition, StateError>> + Send>>
        + Send
        + Sync,
>;

/// The callbacks of a subscription.
#[derive(Clone, Default)]
pub(super) struct Callbacks {
    pub(super) data: Option<GetData>,
    pub(super) token: Option<GetSubscriptionToken>,
    pub(super) state: Option<GetState>,
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

    /// Feeds `input` to the machine: events go to their outboxes, taken at
    /// once where no stream takes them, other actions to the driver.
    pub(super) fn handle(&self, core: &mut Core, input: Input) {
        // Application calls and stream reads feed the machine too, on the
        // application's tasks.
        let _entered = self.log.span.enter();
        let mut inputs = VecDeque::from([input]);
        while let Some(input) = inputs.pop_front() {
            self.log.input(&input);
            for action in core.machine.handle(input) {
                self.log.action(&action);
                let mut outboxes = lock(&self.outboxes);
                let taken = match action {
                    Action::Emit(id, event) => outboxes.client.offer(id, event),
                    Action::EmitSubscription(id, subscription, event) => {
                        match outboxes.subscriptions.get_mut(&subscription) {
                            Some(outbox) => outbox.offer(id, event),
                            None => Some(id),
                        }
                    }
                    Action::Withdraw(ids) => {
                        outboxes.withdraw(&ids);
                        None
                    }
                    action => {
                        core.actions.push_back(action);
                        None
                    }
                };
                inputs.extend(taken.map(Input::EventTaken));
            }
        }
    }

    fn next_action(&self) -> Option<Action> {
        lock(&self.core).actions.pop_front()
    }

    /// Makes a call now, of `subscription` when given; the future gives its
    /// outcome. Dropping the future abandons the call (Block 13).
    pub(super) fn call(
        self: &Arc<Self>,
        subscription: Option<SubscriptionId>,
        request: CallRequest,
    ) -> impl Future<Output = Result<CallReply, CallError>> + Send + use<> {
        let (sender, receiver) = oneshot::channel();
        let pending = {
            let mut core = lock(&self.core);
            // The driver has stopped: nothing would ever settle the call.
            if self.closed.load(Ordering::Acquire) {
                None
            } else {
                let kind = request.name();
                let id = match subscription {
                    Some(subscription) => core.machine.add_subscription_call(subscription, request),
                    None => core.machine.add_call(request),
                };
                self.log.call(id, kind);
                lock(&self.calls).insert(id, sender);
                self.handle(&mut core, Input::Call(id));
                Some(PendingCall {
                    shared: Arc::clone(self),
                    id,
                    settled: false,
                })
            }
        };
        self.actions_queued.notify_one();
        async move {
            let Some(mut pending) = pending else {
                return Err(disconnected());
            };
            let result = receiver.await;
            pending.settled = true;
            // The sender was dropped unsettled only when the driver stopped.
            result.unwrap_or_else(|_| Err(disconnected()))
        }
    }
}

fn disconnected() -> CallError {
    CallError::new(codes::error::DISCONNECTED, "disconnected")
}

/// The result `extract` takes from the reply of `call`.
pub(super) async fn reply_of<T>(
    call: impl Future<Output = Result<CallReply, CallError>>,
    extract: fn(CallReply) -> Option<T>,
) -> Result<T, CallError> {
    Ok(extract(call.await?).expect("the machine answers each call with its own method's result"))
}

/// A call awaiting its outcome: dropped before it, the call is abandoned.
struct PendingCall {
    shared: Arc<Shared>,
    id: CallId,
    settled: bool,
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        lock(&self.shared.calls).remove(&self.id);
        self.shared.apply(Input::CallAbandoned(self.id));
    }
}

/// A stream of the client's or a subscription's events, taking them from
/// its outbox: each event it yields is reported taken to the machine
/// under the core lock, so a withdrawal never races a take (Block 15).
pub(super) struct EventStream<E> {
    shared: Arc<Shared>,
    /// The subscription whose events it yields; the client's when `None`.
    of: Option<SubscriptionId>,
    number: u64,
    event: std::marker::PhantomData<fn() -> E>,
}

/// Events with an outbox: the client's and the subscriptions'.
pub(super) trait Routed: Sized {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>>;
}

impl Routed for ClientEvent {
    fn outbox(outboxes: &mut Outboxes, _: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        Some(&mut outboxes.client)
    }
}

impl Routed for SubscriptionEvent {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        outboxes.subscriptions.get_mut(&of?)
    }
}

impl<E> EventStream<E> {
    pub(super) fn new(shared: Arc<Shared>, of: Option<SubscriptionId>, number: u64) -> Self {
        Self {
            shared,
            of,
            number,
            event: std::marker::PhantomData,
        }
    }
}

impl<E: Routed> Stream for EventStream<E> {
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<E>> {
        let mut core = lock(&self.shared.core);
        let mut outboxes = lock(&self.shared.outboxes);
        let Some(outbox) = E::outbox(&mut outboxes, self.of) else {
            return Poll::Ready(None);
        };
        let polled = outbox.poll_take(self.number, cx);
        if outbox.is_finished()
            && let Some(of) = self.of
        {
            outboxes.subscriptions.remove(&of);
        }
        drop(outboxes);
        let Poll::Ready(Some((id, event))) = polled else {
            return polled.map(|_| None);
        };
        self.shared.handle(&mut core, Input::EventTaken(id));
        drop(core);
        self.shared.actions_queued.notify_one();
        Poll::Ready(Some(event))
    }
}

impl<E> Drop for EventStream<E> {
    // Dropping the active stream takes what it held, as with no stream.
    fn drop(&mut self) {
        let mut core = lock(&self.shared.core);
        let mut outboxes = lock(&self.shared.outboxes);
        let taken = match self.of {
            None => outboxes.client.drop_stream(self.number),
            Some(of) => {
                let Some(outbox) = outboxes.subscriptions.get_mut(&of) else {
                    return;
                };
                let taken = outbox.drop_stream(self.number);
                if outbox.is_finished() {
                    outboxes.subscriptions.remove(&of);
                }
                taken
            }
        };
        drop(outboxes);
        // A superseded stream leaves the active one in place.
        let Some(taken) = taken else {
            return;
        };
        for id in taken {
            self.shared.handle(&mut core, Input::EventTaken(id));
        }
        self.shared.handle(&mut core, Input::StreamClosed(self.of));
        drop(core);
        // Held messages may now be handled and the reading resume.
        self.shared.actions_queued.notify_one();
    }
}

/// Async callback returning a fresh connection token.
type GetToken =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send>> + Send + Sync>;

/// Async callback returning the data of the next connect command.
type GetConnectData =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Vec<u8>, DataError>> + Send>> + Send + Sync>;

/// Builds a [`Client`] with options beyond [`Config`].
pub struct ClientBuilder {
    /// Transports to try in order, each with its endpoint.
    endpoints: Vec<(TransportKind, String)>,
    config: Config,
    get_token: Option<GetToken>,
    get_data: Option<GetConnectData>,
    emulation_endpoint: Option<String>,
    /// Real HTTP headers, as given; checked by [`ClientBuilder::build`].
    http_headers: Vec<(String, String)>,
    /// Emulated headers, as given; checked by [`ClientBuilder::build`].
    emulated_headers: Vec<(String, String)>,
    /// Extra root certificates, as given; checked by [`ClientBuilder::build`].
    root_certificates: Vec<CertificateDer<'static>>,
    /// The application's TLS configuration, if any.
    tls_config: Option<Arc<rustls::ClientConfig>>,
    proxy: Proxy,
}

/// Endpoints as they may be logged (Block 21, D5).
struct Endpoints<'a>(&'a [(TransportKind, String)]);

impl std::fmt::Debug for Endpoints<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .map(|(kind, endpoint)| (kind, log::endpoint(endpoint))),
            )
            .finish()
    }
}

/// Header names, without their values, which may be secrets.
struct HeaderNames<'a>(&'a [(String, String)]);

impl std::fmt::Debug for HeaderNames<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| name))
            .finish()
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoints", &Endpoints(&self.endpoints))
            .field("config", &self.config)
            .field("get_token", &self.get_token.is_some())
            .field("get_data", &self.get_data.is_some())
            .field(
                "emulation_endpoint",
                &self.emulation_endpoint.as_deref().map(log::endpoint),
            )
            .field("http_headers", &HeaderNames(&self.http_headers))
            .field("emulated_headers", &HeaderNames(&self.emulated_headers))
            // Their number, and whether one is set: never their content.
            .field("root_certificates", &self.root_certificates.len())
            .field("tls_config", &self.tls_config.is_some())
            .field("proxy", &self.proxy)
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

    /// Sets the callback that provides the data of the next connect command,
    /// called before every connection attempt once a token is obtained; its
    /// data replaces [`Config::data`]. Return [`DataError::Unauthorized`] to
    /// disconnect the client for good; any other error is reported as an
    /// `error` event and the attempt retried after the reconnect backoff.
    #[must_use]
    pub fn get_data<F, Fut>(mut self, get_data: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, DataError>> + Send + 'static,
    {
        self.get_data = Some(Arc::new(move || Box::pin(get_data())));
        self
    }

    /// Trusts `roots`, certificate authorities, in addition to the
    /// platform's, for `wss://` and every `https://` request (Block 22);
    /// they add to the roots of earlier calls. A certificate that is not a
    /// valid root makes [`ClientBuilder::build`] fail, naming its index.
    ///
    /// PEM files parse with [`rustls::pki_types::pem::PemObject`]:
    ///
    /// ```no_run
    /// use ferrifuge::Client;
    /// use ferrifuge::rustls::pki_types::CertificateDer;
    /// use ferrifuge::rustls::pki_types::pem::PemObject;
    ///
    /// # fn run(pem: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    /// let roots = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
    /// let client = Client::builder("wss://example.com/connection/websocket")
    ///     .root_certificates(roots)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Each client given extra roots holds its own TLS settings: many
    /// clients should rather share one [`ClientBuilder::tls_config`].
    #[must_use]
    pub fn root_certificates(
        mut self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.root_certificates.extend(roots);
        self
    }

    /// Uses `config` for `wss://` and every `https://` request, instead of
    /// the platform's trust (Block 22): its roots, verifier, provider and
    /// client certificates are the application's. Ferrifuge only replaces
    /// its ALPN list with `http/1.1`, the one protocol of its transports.
    /// [`ClientBuilder::build`] fails when extra root certificates are also
    /// given, and, in the `fips` build, when `config.fips()` is false.
    ///
    /// The configuration is shared as given, so many clients can use one.
    #[must_use]
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Sets how the client reaches the server's hosts (Block 23): through
    /// the system's proxy by default ([`Proxy::system`]), the environment's,
    /// none, one proxy, or one chosen per endpoint. Every transport takes
    /// the same decision for each URL it opens.
    #[must_use]
    pub fn proxy(mut self, proxy: Proxy) -> Self {
        self.proxy = proxy;
        self
    }

    /// Adds real HTTP headers to the websocket upgrade request and to every
    /// request of `http_stream` and sse, so that a gateway checking one
    /// still admits a client that fell back (Block 17). Names are
    /// case-insensitive, a later value replacing an earlier one. A name or
    /// value that is not valid HTTP, or a header the transport sets itself
    /// (`Host`, `Connection`, `Upgrade`, `Content-Length`,
    /// `Transfer-Encoding`, `Content-Type`, `Accept`, `Sec-WebSocket-*`),
    /// makes [`ClientBuilder::build`] fail.
    #[must_use]
    pub fn http_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.http_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    /// Adds emulated headers, sent in every connect command
    /// (`ConnectRequest.headers`), which the server forwards to its proxies
    /// as HTTP headers when its `emulated_headers` lists them (centrifuge-js
    /// `headers`). They are client-controlled input to those proxies. Names
    /// and values must be valid HTTP, or [`ClientBuilder::build`] fails.
    #[must_use]
    pub fn emulated_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.emulated_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
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
    /// # Errors
    ///
    /// As [`ClientBuilder::build`], e.g. for an empty endpoint or an
    /// `http://` one.
    ///
    /// # Panics
    ///
    /// When called outside a tokio runtime: the client runs as a task on it.
    pub fn new(endpoint: impl Into<String>, config: Config) -> Result<Self, ClientError> {
        Self::builder(endpoint).config(config).build()
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
            get_data: None,
            emulation_endpoint: None,
            http_headers: Vec::new(),
            emulated_headers: Vec::new(),
            root_certificates: Vec::new(),
            tls_config: None,
            proxy: Proxy::default(),
        }
    }

    fn start(builder: ClientBuilder) -> Result<Self, ClientError> {
        let ClientBuilder {
            endpoints,
            config,
            get_token,
            get_data,
            emulation_endpoint,
            http_headers,
            emulated_headers,
            root_certificates,
            tls_config,
            proxy,
        } = builder;
        for (kind, endpoint) in &endpoints {
            check_endpoint(*kind, endpoint)?;
        }
        let android = cfg!(target_os = "android");
        let trust = trust(root_certificates, tls_config, android)?;
        android_gate(
            android && !tls::android_ready(),
            &trust,
            &endpoints,
            emulation_endpoint.as_deref(),
            &proxy,
        )?;
        let http_headers = Arc::new(http_header_map(&http_headers)?);
        let emulated_headers = emulated_header_map(&emulated_headers)?;
        check_limits(&config)?;
        let subprotocol = config.protocol.websocket_subprotocol();
        let timeout = config.timeout;
        let machine = if get_token.is_some() {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        let machine = if get_data.is_some() {
            machine.with_data_source()
        } else {
            machine
        };
        let machine = machine.with_emulated_headers(emulated_headers);
        let machine = machine.with_transports(endpoints.iter().map(|(kind, _)| *kind).collect())?;
        let shared = Arc::new(Shared {
            core: Mutex::new(Core {
                machine,
                actions: VecDeque::new(),
            }),
            outboxes: Mutex::new(Outboxes::default()),
            callbacks: Mutex::new(SubscriptionCallbacks::default()),
            calls: Mutex::new(HashMap::new()),
            actions_queued: Notify::new(),
            closed: AtomicBool::new(false),
            log: Log::new(endpoints.iter().map(|(_, endpoint)| endpoint.as_str())),
            network: Arc::new(Network::new(Tls::new(trust), proxy)),
        });
        let driver = Driver {
            endpoints: endpoints
                .into_iter()
                .map(|(_, endpoint)| endpoint)
                .collect(),
            subprotocol,
            emulation_endpoint,
            http_headers,
            timeout,
            shared: Arc::clone(&shared),
            opening: None,
            transport: None,
            kind: TransportKind::WebSocket,
            reading: true,
            get_token,
            get_data,
            fetching: None,
            stale_fetching: None,
            timers: Timers::default(),
            subscription_timers: KeyedTimers::default(),
            call_timers: KeyedTimers::default(),
            callback_fetches: FuturesUnordered::new(),
            callback_aborts: HashMap::new(),
        };
        let span = shared.log.span.clone();
        tokio::spawn(driver.run().instrument(span));
        Ok(Self { shared })
    }

    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// The number of this client in its log records' `client` field
    /// (Block 21), for tests.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn log_number(&self) -> u64 {
        self.shared.log.number
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        lock(&self.shared.core).machine.state()
    }

    /// Events from now on. An event counts as delivered once this stream
    /// yields it; until a stream is taken, events count as delivered at
    /// once, so take it before calling [`Client::connect`] to get every
    /// one. A later call takes over: the earlier stream ends and the events
    /// it had not yielded are the new one's (Block 15). The stream borrows
    /// nothing from the client, so it can be kept beside it, and it ends
    /// once the client is dropped.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + Send + Unpin + use<> {
        let mut core = lock(&self.shared.core);
        let number = lock(&self.shared.outboxes).client.open_stream();
        self.shared.handle(&mut core, Input::StreamOpened(None));
        drop(core);
        EventStream::new(Arc::clone(&self.shared), None, number)
    }

    /// Messages read from the server and not yet handled, and their bytes
    /// (Block 15), so that tests can wait for the read-ahead to fill. Only
    /// with the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn read_ahead(&self) -> (usize, usize) {
        lock(&self.shared.core).machine.read_ahead()
    }

    /// Events of the client queued for its streams and not yet taken, so
    /// that tests can wait for them. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_events(&self) -> usize {
        lock(&self.shared.outboxes).client.held()
    }

    /// Starts connecting; reconnects until connected or disconnected. The
    /// state is [`State::Connecting`] when this returns.
    pub fn connect(&self) {
        self.shared.apply(Input::Connect);
    }

    /// Disconnects and stops reconnecting. The state is
    /// [`State::Disconnected`] when this returns. Every pending call fails
    /// with code [`codes::error::DISCONNECTED`].
    pub fn disconnect(&self) {
        self.shared.apply(Input::Disconnect);
    }

    /// Starts a batch: until it stops, the application's calls (but
    /// `send`) and the subscribe and unsubscribe commands its
    /// `subscribe()`, `unsubscribe()` and subscription removal cause are
    /// held, then sent in order in frames of several commands, each within
    /// [`Config::batch_frame_bytes`] (centrifuge-js `startBatching`). The
    /// client's own commands are never held. A held call still fails with
    /// the timeout error when not sent within `Config::timeout`; a held
    /// subscribe's reply deadline runs too. Several batches may be open:
    /// stopping one sends what is held so far.
    #[must_use = "the batch stops when dropped"]
    pub fn start_batching(&self) -> Batch {
        self.shared.apply(Input::StartBatching);
        Batch {
            shared: Arc::clone(&self.shared),
            stopped: false,
        }
    }

    /// Sets the connection token used from the next connection attempt,
    /// sending nothing now; empty for none (centrifuge-js `setToken`).
    /// Without a token callback, a token the server rejected as expired
    /// (error 109) is then replaced by this one, instead of the client
    /// stopping on a configuration error; with one, the next attempt still
    /// calls it. A connected client's refresh needs the callback.
    pub fn set_token(&self, token: impl Into<String>) {
        self.shared.apply(Input::SetToken(token.into()));
    }

    /// Sets the connect data used from the next connection attempt, sending
    /// nothing now; a [`ClientBuilder::get_data`] callback still replaces it
    /// on every attempt (centrifuge-js `setData`).
    pub fn set_data(&self, data: impl Into<Vec<u8>>) {
        self.shared.apply(Input::SetData(data.into()));
    }

    /// Replaces the emulated headers sent from the next connection attempt,
    /// sending nothing now (centrifuge-js `setHeaders`).
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error naming a header whose
    /// name or value is not valid HTTP, as [`ClientBuilder::emulated_headers`];
    /// the headers are then left as they were.
    pub fn set_emulated_headers<I, K, V>(&self, headers: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let headers: Vec<(String, String)> = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        let headers = emulated_header_map(&headers)?;
        self.shared.apply(Input::SetEmulatedHeaders(headers));
        Ok(())
    }

    /// Replaces the proxy setting from the next connection attempt
    /// (Block 23), as the other setters do; the current connection is
    /// kept.
    pub fn set_proxy(&self, proxy: Proxy) {
        self.shared.network.set_proxy(proxy);
    }

    /// Calls the server's RPC `method` with `data`; the future gives the
    /// result data: raw JSON with the JSON protocol, any bytes with
    /// Protobuf.
    ///
    /// The call is made when this returns: a call made while not connected
    /// waits for the connection, then for the reply, each for at most
    /// `Config::timeout`. The future borrows nothing from the client, so it
    /// can be spawned. Results are not ordered with events: a reply settles
    /// its call as soon as it is read. Dropping the future abandons the
    /// call: if it is still waiting for the connection, it is never sent.
    ///
    /// # Errors
    ///
    /// The server's error, with its code, message and `temporary` flag, or
    /// a client-side one: [`codes::error::TIMEOUT`],
    /// [`codes::error::DISCONNECTED`] when the client enters
    /// `disconnected`, including when the `Client` is dropped,
    /// [`codes::error::CONNECTION_CLOSED`] when the transport the call was
    /// sent on closes, and [`codes::error::TRANSPORT_WRITE`] when the
    /// server refuses the request carrying it (HTTP 413).
    pub fn rpc(
        &self,
        method: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<u8>, CallError>> + Send + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Rpc {
                method: method.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| match reply {
            CallReply::Rpc(data) => Some(data),
            _ => None,
        })
    }

    /// Publishes `data` to `channel`: raw JSON with the JSON protocol, any
    /// bytes with Protobuf. Made, waiting and failing as [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn publish(
        &self,
        channel: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + Send + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Publish {
                channel: channel.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// The history of `channel`: the publications `options` select, and the
    /// stream's current position. Made, waiting and failing as
    /// [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn history(
        &self,
        channel: impl Into<String>,
        options: HistoryOptions,
    ) -> impl Future<Output = Result<HistoryResult, CallError>> + Send + 'static {
        let call = self.shared.call(
            None,
            CallRequest::History {
                channel: channel.into(),
                options,
            },
        );
        reply_of(call, history_of)
    }

    /// The clients subscribed to `channel`. Made, waiting and failing as
    /// [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn presence(
        &self,
        channel: impl Into<String>,
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + Send + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Presence {
                channel: channel.into(),
            },
        );
        reply_of(call, presence_of)
    }

    /// Sends `data` to the server as an asynchronous message, which gets no
    /// reply (`client.proto`, `SendRequest`): raw JSON with the JSON
    /// protocol, any bytes with Protobuf.
    ///
    /// The call is made when this returns. While the client is not
    /// connected it waits for the connection, for at most
    /// `Config::timeout`; the future resolves once the message is handed to
    /// the transport, which writes it even if the client disconnects right
    /// after (within `Config::timeout`), unless the connection fails first.
    /// Dropping the future before then abandons the message: a send whose
    /// future is never awaited may be lost.
    ///
    /// # Errors
    ///
    /// [`codes::error::TIMEOUT`] when no connection comes in time, and
    /// [`codes::error::DISCONNECTED`] when the client enters
    /// `disconnected`, including when the `Client` is dropped.
    pub fn send(
        &self,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + Send + 'static {
        let call = self
            .shared
            .call(None, CallRequest::Send { data: data.into() });
        reply_of(call, |reply| matches!(reply, CallReply::Send).then_some(()))
    }

    /// How many clients and users are subscribed to `channel`. Made,
    /// waiting and failing as [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn presence_stats(
        &self,
        channel: impl Into<String>,
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + Send + 'static {
        let call = self.shared.call(
            None,
            CallRequest::PresenceStats {
                channel: channel.into(),
            },
        );
        reply_of(call, presence_stats_of)
    }
}

/// An open batch of [`Client::start_batching`]: [`Batch::stop`], or dropping
/// it, stops it and sends what is held (centrifuge-js `stopBatching`). It
/// borrows nothing from the client.
#[derive(Debug)]
pub struct Batch {
    shared: Arc<Shared>,
    stopped: bool,
}

impl Batch {
    /// Stops the batch: what the open batches hold is sent now.
    pub fn stop(mut self) {
        self.end();
    }

    fn end(&mut self) {
        if !std::mem::replace(&mut self.stopped, true) {
            self.shared.apply(Input::StopBatching);
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        self.end();
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
    /// HTTP headers of every transport request.
    http_headers: Arc<HeaderMap>,
    /// Time allowed for an emulation request, and for writing a websocket
    /// frame or closing a websocket (`Config::timeout`).
    timeout: Duration,
    shared: Arc<Shared>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, Transport)>,
    /// The kind of the transport opening or open, for the records.
    kind: TransportKind,
    /// Whether the transport was read on the last turn, for the records of
    /// the read-ahead filling and draining (Block 21).
    reading: bool,
    get_token: Option<GetToken>,
    get_data: Option<GetConnectData>,
    fetching: Option<(FetchId, Fetching)>,
    /// The callback a newer one replaced, still polled: when it was a
    /// refresh of a connection that is gone, its denial must still reach
    /// the machine, which ignores any other answer of it.
    stale_fetching: Option<(FetchId, Fetching)>,
    timers: Timers,
    subscription_timers: KeyedTimers<(SubscriptionId, SubscriptionTimer)>,
    call_timers: KeyedTimers<(CallId, CallTimer)>,
    /// Subscription callbacks and the connect data callback running, each yielding the
    /// input answering it; an aborted one yields `None`.
    callback_fetches: FuturesUnordered<CallbackFetching>,
    /// Handles to abort the callbacks running, by fetch.
    callback_aborts: HashMap<FetchId, AbortHandle>,
}

type CallbackFetching = Pin<Box<dyn Future<Output = Option<(FetchId, Input)>> + Send>>;

/// The machine's subscription timers, by subscription and kind, or its
/// call timers, by call.
struct KeyedTimers<K>(HashMap<K, Pin<Box<Sleep>>>);

impl<K> Default for KeyedTimers<K> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Copy + Eq + std::hash::Hash> KeyedTimers<K> {
    fn set(&mut self, key: K, delay: Duration) {
        self.0.insert(key, Box::pin(tokio::time::sleep(delay)));
    }

    fn cancel(&mut self, key: K) {
        self.0.remove(&key);
    }

    /// Resolves with the first timer to fire, which is removed; never while
    /// none is set. Cancel-safe.
    async fn fired(&mut self) -> K {
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
                self.perform(action);
            }
            if self.shared.closed.load(Ordering::Acquire) {
                // Handles of subscriptions may outlive the client: end
                // every event stream rather than leave it pending forever,
                // and release the data callbacks nothing can call anymore.
                let mut outboxes = lock(&self.shared.outboxes);
                outboxes.client.close();
                outboxes.subscriptions.values_mut().for_each(Outbox::close);
                drop(outboxes);
                lock(&self.shared.callbacks).0.clear();
                // The final disconnect settled every call; any other future
                // gets "disconnected" rather than waiting forever.
                lock(&self.shared.calls).clear();
                return;
            }
            // A full read-ahead stops the reading: the server's handling of
            // slow clients acts instead (Block 15). Taking an event wakes
            // the loop.
            let reading = lock(&self.shared.core).machine.wants_frames();
            if reading != self.reading {
                self.reading = reading;
                if reading {
                    debug!("read-ahead drained: reading resumes");
                } else {
                    debug!("read-ahead full: reading stops");
                }
            }
            let input = tokio::select! {
                // Queued by an API call; performed at the top of the loop.
                () = self.shared.actions_queued.notified() => continue,
                (id, result) = opened(&mut self.opening) => {
                    self.opening = None;
                    match result {
                        Ok(transport) => {
                            debug!(transport = self.kind.name(), "transport opened");
                            self.transport = Some((id, transport));
                            Input::TransportOpened(id)
                        }
                        Err(message) => {
                            debug!(transport = self.kind.name(), reason = %message, "transport failed");
                            Input::TransportFailed { transport: id, message }
                        }
                    }
                }
                (id, inbound) = received(&mut self.transport, reading) => match inbound {
                    Inbound::Frame(data) => {
                        trace!(
                            transport = self.kind.name(),
                            frame_type = ?self.frame_type(),
                            bytes = data.len(),
                            "frame received"
                        );
                        Input::Frame { transport: id, data }
                    }
                    Inbound::Rejected(frame) => Input::SendRejected { transport: id, frame },
                    Inbound::Written(frame) => Input::FrameWritten { transport: id, frame },
                    Inbound::Closed { code, reason } => {
                        self.transport = None;
                        self.closed(code, &reason);
                        Input::TransportClosed { transport: id, code, reason }
                    }
                },
                timer = self.timers.fired() => Input::TimerFired(timer),
                (id, timer) = self.subscription_timers.fired() => {
                    Input::SubscriptionTimerFired(id, timer)
                }
                (id, timer) = self.call_timers.fired() => Input::CallTimerFired(id, timer),
                Some(fetched) = self.callback_fetches.next(),
                    if !self.callback_fetches.is_empty() =>
                {
                    // Aborted: the machine no longer wants it.
                    let Some((fetch, input)) = fetched else {
                        continue;
                    };
                    self.callback_aborts.remove(&fetch);
                    input
                }
                (fetch, result) = fetched(&mut self.fetching) => {
                    self.fetching = None;
                    Input::TokenFetched { fetch, result }
                }
                (fetch, result) = fetched(&mut self.stale_fetching) => {
                    self.stale_fetching = None;
                    Input::TokenFetched { fetch, result }
                }
            };
            self.shared.apply(input);
        }
    }

    fn perform(&mut self, action: Action) {
        match action {
            Action::OpenTransport {
                transport,
                kind,
                endpoint,
                initial_frame,
            } => self.open_transport(transport, kind, endpoint, initial_frame),
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
                id: frame_id,
                frame,
                frame_type,
            } => {
                trace!(
                    transport = self.kind.name(),
                    frame_type = ?frame_type,
                    bytes = frame.len(),
                    "frame sent"
                );
                if let Some((id, open)) = &mut self.transport
                    && *id == transport
                    && open.send(frame_id, frame, frame_type).is_err()
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
            // Logged by `Shared::handle`, as every action.
            Action::Anomaly(_) => {}
            // Shared::handle routes them to the outboxes.
            Action::Emit(..) | Action::EmitSubscription(..) | Action::Withdraw(_) => {
                unreachable!("events never reach the driver")
            }
            // In order with the other actions: a send settles once its
            // frame went to the transport, which writes it even when closed
            // right after.
            Action::CallSettled { call, result } => {
                if let Some(sender) = lock(&self.shared.calls).remove(&call) {
                    // The future may be gone already.
                    let _ = sender.send(result);
                }
            }
            Action::SetSubscriptionTimer(id, timer, delay) => {
                self.subscription_timers.set((id, timer), delay);
            }
            Action::CancelSubscriptionTimer(id, timer) => {
                self.subscription_timers.cancel((id, timer));
            }
            Action::SetCallTimer(id, timer, delay) => self.call_timers.set((id, timer), delay),
            Action::CancelCallTimer(id, timer) => self.call_timers.cancel((id, timer)),
            action @ (Action::FetchSubscriptionData { .. }
            | Action::FetchSubscriptionToken { .. }
            | Action::FetchSubscriptionState { .. }
            | Action::FetchConnectData(_)
            | Action::CancelSubscriptionData(_)
            | Action::CancelSubscriptionToken(_)
            | Action::CancelSubscriptionState(_)
            | Action::CancelConnectData(_)) => self.callback(action),
            Action::FetchToken(id) => {
                // The machine asks only when it was built with a token source.
                if let Some(get_token) = &self.get_token {
                    // A stale callback stays until a running one replaces it.
                    if let Some(running) = self.fetching.take()
                        && let Some((dropped, _)) = self.stale_fetching.replace(running)
                    {
                        // Dropped without an answer: nothing else tells the log.
                        self.shared.log.fetch_dropped(dropped);
                    }
                    self.fetching = Some((id, guarded(|| get_token(), callback_panicked)));
                }
            }
        }
    }
}

impl Driver {
    /// How frames travel with the configured encoding.
    fn frame_type(&self) -> FrameType {
        if self.subprotocol.is_some() {
            FrameType::Binary
        } else {
            FrameType::Text
        }
    }

    /// Records the close of the open transport: a message too large for
    /// either side is a server anomaly.
    fn closed(&self, code: Option<u16>, reason: &str) {
        let transport = self.kind.name();
        if code == Some(MESSAGE_TOO_BIG) {
            warn!(
                client = self.shared.log.number,
                transport, code, "the server closed the websocket: message too big"
            );
        } else if reason == websocket::TOO_LARGE {
            warn!(
                client = self.shared.log.number,
                transport, "a message from the server is larger than the size limit"
            );
        } else {
            debug!(transport, code, reason, "transport closed");
        }
    }

    /// Starts opening transport `id`, of `kind`, at endpoint `endpoint`;
    /// an emulated one sends `initial_frame`, its connect command.
    fn open_transport(
        &mut self,
        id: TransportId,
        kind: TransportKind,
        endpoint: usize,
        initial_frame: Option<Vec<u8>>,
    ) {
        let url = self.endpoints.get(endpoint).cloned().unwrap_or_default();
        self.kind = kind;
        let subprotocol = self.subprotocol;
        let timeout = self.timeout;
        let headers = Arc::clone(&self.http_headers);
        let network = Arc::clone(&self.shared.network);
        // The attempt's proxy, decided once (Block 23, D1); the transport
        // opening it gets the same decision.
        network.new_attempt();
        let decision = reqwest::Url::parse(&url)
            .map_err(|e| e.to_string())
            .and_then(|parsed| network.decide(&parsed));
        self.shared.log.attempt(kind, endpoint, &decision);
        let opening: Opening = match kind {
            TransportKind::WebSocket => Box::pin(async move {
                WebSocket::open(&url, subprotocol, timeout, &headers, &network)
                    .await
                    .map(|websocket| Transport::WebSocket(Box::new(websocket)))
            }),
            TransportKind::HttpStream | TransportKind::Sse => {
                let emulation = self.emulation_endpoint.clone();
                let request_timeout = self.timeout;
                Box::pin(async move {
                    HttpTransport::open(
                        kind,
                        &url,
                        initial_frame.unwrap_or_default(),
                        emulation.as_deref(),
                        request_timeout,
                        headers,
                        &network,
                    )
                    .await
                    .map(|http| Transport::Http(Box::new(http)))
                })
            }
        };
        self.opening = Some((id, opening));
    }

    fn fetch_connect_data(&mut self, fetch: FetchId) {
        // The machine asks only when it was built with a data source.
        let Some(get_data) = &self.get_data else {
            return;
        };
        let fetching = guarded(|| get_data(), connect_data_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::ConnectDataFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    /// Runs or cancels a subscription callback, or the connect data one.
    fn callback(&mut self, action: Action) {
        match action {
            Action::FetchConnectData(fetch) => self.fetch_connect_data(fetch),
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
            Action::FetchSubscriptionState {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_state(subscription, channel, fetch),
            Action::CancelSubscriptionData(fetch)
            | Action::CancelSubscriptionToken(fetch)
            | Action::CancelSubscriptionState(fetch)
            | Action::CancelConnectData(fetch) => {
                if let Some(abort) = self.callback_aborts.remove(&fetch) {
                    abort.abort();
                }
            }
            _ => unreachable!("not a callback action: {action:?}"),
        }
    }

    fn fetch_subscription_data(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.data.clone());
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
            .and_then(|callbacks| callbacks.token.clone());
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

    fn fetch_subscription_state(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.state.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_state) = callback else {
            return;
        };
        let fetching = guarded(|| get_state(channel), state_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionStateFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    /// Runs a subscription or connect data callback, abortable by its
    /// fetch.
    fn start_fetch(
        &mut self,
        fetch: FetchId,
        answer: impl Future<Output = Input> + Send + 'static,
    ) {
        let (fetching, abort) = future::abortable(answer);
        self.callback_aborts.insert(fetch, abort);
        self.callback_fetches.push(Box::pin(
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
    let Ok(future) = std::panic::catch_unwind(AssertUnwindSafe(callback)) else {
        debug!("callback panicked");
        return Box::pin(std::future::ready(Err(panicked())));
    };
    Box::pin(async move {
        AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                debug!("callback panicked");
                Err(panicked())
            })
    })
}

fn token_callback_panicked() -> TokenError {
    TokenError::Failed("subscription token callback panicked".to_owned())
}

fn connect_data_callback_panicked() -> DataError {
    DataError::Failed("connect data callback panicked".to_owned())
}

fn data_callback_panicked() -> DataError {
    DataError::Failed("subscription data callback panicked".to_owned())
}

fn state_callback_panicked() -> StateError {
    StateError("subscription getState callback panicked".to_owned())
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
    /// Queues `frame`; it is written while [`Transport::recv`] runs.
    fn send(&mut self, id: FrameId, frame: Vec<u8>, frame_type: FrameType) -> Result<(), String> {
        match self {
            Self::WebSocket(websocket) => websocket.send(id, frame, frame_type),
            // Failures end the transport through `recv`.
            Self::Http(http) => {
                http.send(id, frame);
                Ok(())
            }
        }
    }

    /// Cancel-safe. Without `reading`, only writes, as Block 15's full
    /// read-ahead requires.
    async fn recv(&mut self, reading: bool) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv(reading).await,
            Self::Http(http) => http.recv(reading).await,
        }
    }

    fn close(self) {
        match self {
            Self::WebSocket(websocket) => {
                tokio::spawn(websocket.close());
            }
            Self::Http(http) => {
                tokio::spawn(http.close());
            }
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

/// Refuses an endpoint the transport cannot use: an empty one, or an
/// `http(s)://` one for the websocket transport, which needs `ws(s)://`;
/// `http(s)://` is right for `http_stream` and sse (owner decision,
/// 2026-11-04). The message leaves the endpoint out: it may carry a token.
fn check_endpoint(kind: TransportKind, endpoint: &str) -> Result<(), ClientError> {
    let endpoint = endpoint.trim();
    let http = endpoint.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    });
    let message = if endpoint.is_empty() {
        format!("no endpoint given for the {} transport", kind.name())
    } else if kind == TransportKind::WebSocket && http {
        "the websocket transport needs a ws:// or wss:// endpoint, not http:// or https://"
            .to_owned()
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: Some(kind.name().to_owned()),
    })
}

/// A read-ahead limit of 0 could never let a message in (Block 15).
/// Headers a transport sets itself, which an application may not set:
/// proxy credentials go through `ProxyTarget` only, never through a tunnel
/// to the server (Block 23, D7).
fn transport_header(name: &HeaderName) -> bool {
    use reqwest::header::{
        ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, PROXY_AUTHORIZATION,
        TRANSFER_ENCODING, UPGRADE,
    };
    [
        HOST,
        CONNECTION,
        UPGRADE,
        CONTENT_LENGTH,
        TRANSFER_ENCODING,
        CONTENT_TYPE,
        ACCEPT,
        PROXY_AUTHORIZATION,
    ]
    .contains(name)
        || name.as_str().starts_with("sec-websocket-")
}

/// A configuration error about header `name`, never naming its value.
fn header_error(name: &str, problem: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: format!("HTTP header {name:?} {problem}"),
        transport: None,
    }
}

/// The checked HTTP headers (Block 17).
fn http_header_map(headers: &[(String, String)]) -> Result<HeaderMap, ClientError> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let header = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| header_error(name, "is not a valid header name"))?;
        if transport_header(&header) {
            return Err(header_error(name, "is set by the transport"));
        }
        let value = HeaderValue::from_str(value)
            .map_err(|_| header_error(name, "has a value that is not valid HTTP"))?;
        map.insert(header, value);
    }
    Ok(map)
}

/// The checked emulated headers, by name (Block 17).
fn emulated_header_map(
    headers: &[(String, String)],
) -> Result<std::collections::BTreeMap<String, String>, ClientError> {
    let mut map = std::collections::BTreeMap::new();
    for (name, value) in headers {
        HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| header_error(name, "is not a valid header name"))?;
        HeaderValue::from_str(value)
            .map_err(|_| header_error(name, "has a value that is not valid HTTP"))?;
        map.insert(name.clone(), value.clone());
    }
    Ok(map)
}

/// Whom the client trusts, from its extra roots and raw configuration,
/// checked (Block 22, D2 and D3).
fn trust(
    roots: Vec<CertificateDer<'static>>,
    raw: Option<Arc<rustls::ClientConfig>>,
    android: bool,
) -> Result<Trust, ClientError> {
    let message = match (roots.is_empty(), raw) {
        // Android's verifier takes no extra roots (Block 24, D3).
        (false, None) if android => {
            "extra root certificates are not supported on Android: give a tls_config trusting them"
                .to_owned()
        }
        (true, None) => return Ok(Trust::Platform),
        (true, Some(raw)) => {
            if cfg!(feature = "fips") && !raw.fips() {
                "the fips build needs a TLS configuration whose fips() is true".to_owned()
            } else {
                return Ok(Trust::Raw(raw));
            }
        }
        (false, Some(_)) => {
            "extra root certificates and a TLS configuration cannot be given together: \
             put the roots in the TLS configuration"
                .to_owned()
        }
        (false, None) => match unparsable_root(&roots) {
            Some(index) => format!("extra root certificate {index} is not a valid root"),
            None => return Ok(Trust::ExtraRoots(roots.into())),
        },
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: None,
    })
}

/// On Android before the verifier's setup (`unready`), refuses a client
/// whose connections known now need the platform verifier (Block 24, D5):
/// a `wss://` or `https://` endpoint, an `https://` emulation endpoint with
/// an HTTP transport, an `https://` proxy for every endpoint. Any other TLS
/// use fails its handshake instead; a raw configuration is the
/// application's.
fn android_gate(
    unready: bool,
    trust: &Trust,
    endpoints: &[(TransportKind, String)],
    emulation_endpoint: Option<&str>,
    proxy: &Proxy,
) -> Result<(), ClientError> {
    if !unready || !matches!(trust, Trust::Platform) {
        return Ok(());
    }
    let secure = |url: &str| {
        url.split_once("://").is_some_and(|(scheme, _)| {
            scheme.eq_ignore_ascii_case("wss") || scheme.eq_ignore_ascii_case("https")
        })
    };
    let emulated = endpoints.iter().any(|(kind, _)| kind.is_emulated());
    let emulation = emulation_endpoint.filter(|_| emulated);
    let needed = endpoints
        .iter()
        .map(|(_, url)| url.as_str())
        .chain(emulation)
        .any(|url| secure(url) || proxy.is_https_proxy_for(url));
    if !needed {
        return Ok(());
    }
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: tls::ANDROID_SETUP_MISSING.to_owned(),
        transport: None,
    })
}

fn check_limits(config: &Config) -> Result<(), ClientError> {
    let message = if config.read_ahead_messages == 0 || config.read_ahead_bytes == 0 {
        "the read-ahead limits must be at least 1"
    } else if config.write_queue_bytes == 0 {
        "the write queue limit must be at least 1"
    } else if config.batch_frame_bytes == 0 {
        "the batch frame limit must be at least 1"
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    })
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
async fn received(
    transport: &mut Option<(TransportId, Transport)>,
    reading: bool,
) -> (TransportId, Inbound) {
    match transport {
        Some((id, transport)) => (*id, transport.recv(reading).await),
        None => std::future::pending().await,
    }
}

pub(super) fn history_of(reply: CallReply) -> Option<HistoryResult> {
    match reply {
        CallReply::History(history) => Some(history),
        _ => None,
    }
}

pub(super) fn presence_of(reply: CallReply) -> Option<PresenceResult> {
    match reply {
        CallReply::Presence(presence) => Some(presence),
        _ => None,
    }
}

#[allow(clippy::needless_pass_by_value)] // The signature `reply_of` takes.
pub(super) fn presence_stats_of(reply: CallReply) -> Option<PresenceStatsResult> {
    match reply {
        CallReply::PresenceStats(stats) => Some(stats),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::*;
    use crate::adapter::SubscriptionBuilder;

    // A batch can be stopped from another task (Block 18, D1).
    #[test]
    fn a_batch_is_send() {
        fn send<T: Send>() {}
        send::<Batch>();
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn http_header_names_are_case_insensitive_and_a_later_value_wins() {
        let map = http_header_map(&headers(&[("X-Device", "a"), ("x-device", "b")])).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map["x-device"], "b");
    }

    #[test]
    fn headers_the_transport_sets_are_refused_without_their_value() {
        for name in ["Host", "upgrade", "Accept", "Sec-WebSocket-Protocol"] {
            let error = http_header_map(&headers(&[(name, "v-secret")])).unwrap_err();

            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(error.message.contains(name), "{error:?}");
            assert!(!error.message.contains("v-secret"), "{error:?}");
        }
    }

    #[test]
    fn invalid_headers_are_refused_without_their_value() {
        let bad_value = headers(&[("X-Device", "line\nbreak-secret")]);
        let bad_name = headers(&[("X Device", "v")]);

        for checked in [
            http_header_map(&bad_value).err(),
            emulated_header_map(&bad_value).err(),
            http_header_map(&bad_name).err(),
            emulated_header_map(&bad_name).err(),
        ] {
            let error = checked.expect("refused");
            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(!error.message.contains("secret"), "{error:?}");
        }
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn state_changes_as_soon_as_connect_and_disconnect_return() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();

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
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
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

    // Owner decision, 2026-11-04: no construction path accepts these.
    #[tokio::test]
    async fn an_empty_or_http_websocket_endpoint_is_refused() {
        for endpoint in [
            "",
            "http://127.0.0.1:1/connection/websocket",
            "HTTPS://example.com/ws",
        ] {
            let built = Client::builder(endpoint).build();

            let Err(error) = built else {
                panic!("{endpoint:?} was accepted");
            };
            assert_eq!(error.kind, crate::ErrorKind::Configuration, "{endpoint:?}");
            assert_eq!(
                error.transport.as_deref(),
                Some("websocket"),
                "{endpoint:?}"
            );
        }
    }

    #[tokio::test]
    async fn client_new_returns_an_endpoint_error_instead_of_panicking() {
        let created = Client::new("", Config::default());

        assert!(
            matches!(&created, Err(error) if error.kind == crate::ErrorKind::Configuration),
            "{created:?}"
        );
    }

    #[tokio::test]
    async fn http_endpoints_stay_valid_for_http_stream_and_sse() {
        let built = Client::builder_with_transports([
            (
                TransportKind::HttpStream,
                "http://127.0.0.1:1/connection/http_stream".to_owned(),
            ),
            (
                TransportKind::Sse,
                "https://127.0.0.1:1/connection/sse".to_owned(),
            ),
        ])
        .build();

        assert!(built.is_ok(), "{built:?}");
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
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();

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

    fn raw_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(tls::crypto_provider()))
                .with_safe_default_protocol_versions()
                .expect("protocol versions")
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    /// Before Android's setup, a connection known now to need the platform
    /// verifier is refused, naming the call (Block 24, D5).
    #[test]
    fn android_refuses_tls_known_at_build_before_the_setup() {
        // Endpoints, the emulation endpoint, the proxy.
        type Case<'a> = (&'a [(TransportKind, &'a str)], Option<&'a str>, Proxy);
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"));
        let cases: [Case<'_>; 4] = [
            (
                &[(
                    TransportKind::WebSocket,
                    "wss://example.com/connection/websocket",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "https://example.com/connection/http_stream",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "http://example.com/connection/http_stream",
                )],
                Some("https://example.com/emulation"),
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::WebSocket,
                    "ws://example.com/connection/websocket",
                )],
                None,
                proxy,
            ),
        ];
        for (endpoints, emulation, proxy) in cases {
            let endpoints: Vec<_> = endpoints
                .iter()
                .map(|(kind, url)| (*kind, (*url).to_owned()))
                .collect();

            let gated = android_gate(true, &Trust::Platform, &endpoints, emulation, &proxy);

            assert!(
                matches!(&gated, Err(error) if error.message.contains("ferrifuge::android::init_with_env")),
                "{endpoints:?} {emulation:?}: {gated:?}"
            );
        }
    }

    /// A no-proxy list excluding every endpoint leaves them direct, with no
    /// verifier needed (Codex, #118).
    #[test]
    fn android_accepts_endpoints_an_https_proxy_excludes_before_the_setup() {
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"))
                .no_proxy("*");
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &proxy).is_ok());
    }

    #[test]
    fn android_accepts_plain_endpoints_and_raw_configurations_before_the_setup() {
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];
        let secure = [(
            TransportKind::WebSocket,
            "wss://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &Proxy::none()).is_ok());
        assert!(
            android_gate(
                true,
                &Trust::Raw(raw_config()),
                &secure,
                None,
                &Proxy::none()
            )
            .is_ok()
        );
        assert!(android_gate(false, &Trust::Platform, &secure, None, &Proxy::none()).is_ok());
    }

    /// Android's verifier takes no extra roots (Block 24, D3).
    #[test]
    fn android_refuses_extra_roots_naming_tls_config() {
        let root = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a certificate")
            .cert
            .der()
            .clone();

        let refused = trust(vec![root.clone()], None, true);
        let elsewhere = trust(vec![root], None, false);

        assert!(
            matches!(&refused, Err(error) if error.message.contains("tls_config")),
            "{:?}",
            refused.err()
        );
        assert!(elsewhere.is_ok());
    }

    /// The fips build never connects with non-FIPS cryptography: a raw
    /// configuration whose `fips()` is false fails `build()` (Block 22, D3).
    #[cfg(feature = "fips")]
    #[test]
    fn the_fips_build_refuses_a_raw_configuration_that_is_not_fips() {
        let mut raw = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::default_fips_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        assert!(raw.fips());
        raw.require_ems = false;

        let built = Client::builder("wss://localhost/connection/websocket")
            .tls_config(Arc::new(raw))
            .build();

        assert!(
            matches!(&built, Err(error) if error.message.contains("fips")),
            "{built:?}"
        );
    }

    /// Each connect of a client without a token asks the callback, which
    /// never answers here: the driver keeps the running callback and one
    /// stale one, and the log no more (Codex, #109).
    #[tokio::test]
    async fn callbacks_the_driver_drops_leave_the_log() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(std::future::pending::<Result<String, TokenError>>)
            .build()
            .unwrap();
        for _ in 0..10 {
            client.connect();
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.disconnect();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let tracked = client.shared.log.tracked_fetches();
        assert!(tracked <= 2, "{tracked} callbacks tracked");
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

    // Codex on PR #65: dropping the stream that held a full read-ahead
    // lets the held messages go, and the driver must then read again.
    #[tokio::test]
    async fn dropping_the_stream_that_held_the_read_ahead_resumes_reading() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        // Answers the connect and the subscribe, then pushes two
        // publications and a disconnect.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(Message::Text(frame))) = ws.next().await {
                for line in frame.as_str().lines() {
                    let command: serde_json::Value = serde_json::from_str(line).unwrap();
                    let id = &command["id"];
                    if command.get("connect").is_some() {
                        let reply = format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                    } else if command.get("subscribe").is_some() {
                        let reply = format!(r#"{{"id":{id},"subscribe":{{}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                        for n in 1..=2 {
                            let push = format!(
                                r#"{{"push":{{"channel":"news","pub":{{"data":{{"n":{n}}}}}}}}}"#
                            );
                            ws.send(Message::text(push)).await.unwrap();
                        }
                        let bye = r#"{"push":{"disconnect":{"code":3500,"reason":"bye"}}}"#;
                        ws.send(Message::text(bye)).await.unwrap();
                    }
                }
            }
        });
        let config = Config {
            read_ahead_messages: 1,
            ..Config::default()
        };
        let client = Client::new(endpoint, config).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        let mut events = subscription.events();
        client.connect();
        subscription.subscribe();
        while !matches!(events.next().await, Some(SubscriptionEvent::Subscribed(_))) {}
        let filled = tokio::time::timeout(Duration::from_secs(5), async {
            while client.read_ahead().0 < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        drop(events);
        let read_on = tokio::time::timeout(Duration::from_secs(2), async {
            while client.state() == State::Connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(filled.is_ok(), "the read-ahead never filled");
        assert!(read_on.is_ok(), "the disconnect push was never read");
    }

    /// A websocket server answering every connect and subscribe command:
    /// its endpoint, and the number of connect commands it got.
    async fn answering_server() -> (String, Arc<AtomicUsize>) {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let connects = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&connects);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    while let Some(Ok(Message::Text(frame))) = ws.next().await {
                        for line in frame.as_str().lines() {
                            let command: serde_json::Value = serde_json::from_str(line).unwrap();
                            let id = &command["id"];
                            let reply = if command.get("connect").is_some() {
                                counted.fetch_add(1, Ordering::SeqCst);
                                format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#)
                            } else if command.get("subscribe").is_some() {
                                format!(r#"{{"id":{id},"subscribe":{{}}}}"#)
                            } else {
                                continue;
                            };
                            ws.send(Message::text(reply)).await.unwrap();
                        }
                    }
                });
            }
        });
        (endpoint, connects)
    }

    /// Subscribes a subscription to "news", set up by `with_callback` and
    /// retried at once, on a client connecting to an answering server: its
    /// events up to "subscribed", by name, an error with its message; then
    /// the client state and the connect commands the server got.
    async fn subscribed_with(
        with_callback: impl FnOnce(SubscriptionBuilder) -> SubscriptionBuilder,
    ) -> (Vec<String>, State, usize) {
        let (endpoint, connects) = answering_server().await;
        let client = Client::new(endpoint, Config::default()).unwrap();
        let subscription = with_callback(
            client
                .subscription_builder("news")
                .min_resubscribe_delay(Duration::from_millis(10))
                .max_resubscribe_delay(Duration::from_millis(50)),
        )
        .build()
        .unwrap();
        let mut events = subscription.events();

        subscription.subscribe();
        client.connect();
        let mut seen = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
                .expect("an open stream");
            seen.push(match &event {
                SubscriptionEvent::Error(error) => format!("error: {}", error.message),
                event => event.name().to_owned(),
            });
            if matches!(event, SubscriptionEvent::Subscribed(_)) {
                break;
            }
        }
        (seen, client.state(), connects.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_panicking_subscription_token_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_token(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok("t".to_owned())
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription token callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
    }

    #[tokio::test]
    async fn a_panicking_subscription_data_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_data(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok(b"{}".to_vec())
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription data callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
    }

    #[tokio::test]
    async fn a_panicking_get_state_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_state(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok(StreamPosition {
                        offset: 0,
                        epoch: "e".into(),
                    })
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription getState callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
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
        )
        .unwrap();
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
