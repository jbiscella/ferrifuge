//! Client driven by the adapter of the target: tokio natively, the
//! browser's event loop on the browser target.
// Block 44, D2.

// Neither `Send` nor `Sync` on the browser target (Block 44, D3).
#![cfg_attr(
    adapter_browser,
    allow(
        clippy::arc_with_non_send_sync,
        reason = "neither `Send` nor `Sync` on the browser target"
    )
)]

#[cfg(feature = "__test-hooks")]
use crate::sans_io::TimerKey;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;

use futures_core::Stream;

use super::builder::ClientBuilder;
use super::builder::emulated_header_map;
use super::ready::{self, Of};
use super::shared::EventStream;
use super::shared::Shared;
use super::shared::lock;
#[cfg(feature = "__test-hooks")]
use crate::sans_io::TimerSetting;
use crate::sans_io::{Action, CallId, Input, TransportKind};
use crate::{
    CallError, CallReply, CallRequest, ClientError, ClientEvent, Config, HistoryOptions,
    HistoryResult, JsonCallError, MaybeSend, PresenceResult, PresenceStatsResult, State, codes,
};

/// Centrifugo client, running on tokio natively and on the page's event
/// loop in the browser.
///
/// Natively it is `Send` and `Sync`; on wasm32-unknown-unknown it is
/// neither, as the browser's objects it holds are not.
///
/// Clones share one client and its connection. Dropping the last clone
/// closes the client, as [`Client::close`] does without waiting; a
/// [`WeakClient`] from [`Client::downgrade`] does not keep it alive.
// Block 44, D1; Block 44, D3; Block 33; Block 41, D5.
#[derive(Clone)]
pub struct Client {
    pub(super) shared: Arc<Shared>,
    owner: Arc<Owner>,
}

/// What the clones of a client own together: the last one dropped shuts the
/// client down. The driver and the subscriptions hold `Shared`, not this.
struct Owner {
    shared: Arc<Shared>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.shut_down();
    }
}

/// A handle that does not keep its client alive, from
/// [`Client::downgrade`], as tokio's `WeakSender`: for the state of an
/// application, or a handler, that must not hold the client.
// Block 41, D5.
#[derive(Clone)]
pub struct WeakClient {
    owner: std::sync::Weak<Owner>,
}

impl WeakClient {
    /// The client, while one of its clones is alive; `None` once the last
    /// one was dropped. A client closed with [`Client::close`] while clones
    /// live is still given, closed.
    #[must_use]
    pub fn upgrade(&self) -> Option<Client> {
        let owner = self.owner.upgrade()?;
        Some(Client {
            shared: Arc::clone(&owner.shared),
            owner,
        })
    }
}

impl std::fmt::Debug for WeakClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakClient").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Client {
    // Only the state: queued frames may carry the connection token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// The error of a call on a closed client.
// Block 33, D4.
pub(super) fn client_closed() -> CallError {
    CallError::new(codes::error::CLIENT_CLOSED, "client closed")
}

/// The result `extract` takes from the reply of `call`.
pub(super) async fn reply_of<T>(
    call: impl Future<Output = Result<CallReply, CallError>>,
    extract: fn(CallReply) -> Option<T>,
) -> Result<T, CallError> {
    Ok(extract(call.await?).expect("the machine answers each call with its own method's result"))
}

/// A call awaiting its outcome: dropped before it, the call is abandoned.
pub(super) struct PendingCall {
    pub(super) shared: Arc<Shared>,
    pub(super) id: CallId,
    pub(super) settled: bool,
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        lock(&self.shared.calls).remove(&self.id);
        super::handler::unwatch(&self.shared, self.id);
        self.shared.apply(Input::CallAbandoned(self.id));
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
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also when called outside a tokio runtime: the client runs as",
        doc = "a task on the current one ([`ClientBuilder::runtime`] takes another)."
    )]
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
            emulated_headers: Vec::new(),
            websocket_subprotocols: Vec::new(),
            tls_only: false,
            handler: None,
            event_delivery: super::EventDelivery::default(),
            backend: super::backend::Options::default(),
        }
    }

    /// The first clone of the client `shared` drives.
    pub(super) fn owning(shared: Arc<Shared>) -> Self {
        Self {
            owner: Arc::new(Owner {
                shared: Arc::clone(&shared),
            }),
            shared,
        }
    }

    /// A handle that does not keep this client alive.
    // Block 41, D5.
    #[must_use]
    pub fn downgrade(&self) -> WeakClient {
        WeakClient {
            owner: Arc::downgrade(&self.owner),
        }
    }

    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// The number of this client in its log records' `client` field, for
    /// tests.
    // Block 21.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn log_number(&self) -> u64 {
        self.shared.log.number
    }

    /// Starts keeping what [`Client::armed_timers`] and
    /// [`Client::pending_calls`] report, for the soak: off until then, so
    /// that no other test pays for it. Call it before `connect()`.
    // Block 52, D6.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn record_timers(&self) {
        let Some(mut core) = self.shared.core() else {
            return;
        };
        // Timers set before, aged from now (reviewer, 52a).
        let running: Vec<(String, TimerSetting)> = core
            .machine
            .running_settings()
            .into_iter()
            .map(|(key, setting)| (super::hooks::name(key), setting))
            .collect();
        core.timer_record.seed(running);
        self.shared.records.start();
        drop(core);
        self.shared.actions_queued.notify_one();
    }

    /// The timers the driver's own maps run, as it last published them, and
    /// those the core set and did not cancel, each with its age and
    /// deadline.
    // Block 52, D8.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    ///
    /// # Panics
    ///
    /// While the records are off: call [`Client::record_timers`] first.
    pub fn armed_timers(&self) -> super::hooks::ArmedTimers {
        assert!(
            self.shared.records.on(),
            "armed_timers: the records are off; call Client::record_timers first (Block 52)"
        );
        let Some(mut guard) = self.shared.core() else {
            return super::hooks::ArmedTimers::default();
        };
        let core = &mut *guard;
        // The core's running settings, read from the machine (Block 53 D4).
        let names: Vec<String> = core
            .machine
            .running_settings()
            .into_iter()
            .map(|(key, _)| super::hooks::name(key))
            .collect();
        let core_list = core.timer_record.report_armed(names);
        drop(guard);
        let driver = lock(&self.shared.records.driver).report();
        super::hooks::ArmedTimers {
            driver,
            core: core_list,
        }
    }

    /// Every call any registry of the core or the adapter holds, with its
    /// age and the registries holding it.
    // Block 52, D8.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    ///
    /// # Panics
    ///
    /// While the records are off: call [`Client::record_timers`] first.
    pub fn pending_calls(&self) -> Vec<super::hooks::CallRecord> {
        assert!(
            self.shared.records.on(),
            "pending_calls: the records are off; call Client::record_timers first (Block 52)"
        );
        let Some(core) = self.shared.core() else {
            return Vec::new();
        };
        let mut held = core.machine.call_registries();
        for id in lock(&self.shared.calls).keys() {
            held.entry(*id).or_default().push("adapter");
        }
        drop(core);
        for id in self.shared.log.calls() {
            held.entry(id).or_default().push("log");
        }
        for id in lock(&self.shared.handlers).watched_calls() {
            held.entry(id).or_default().push("watched");
        }
        lock(&self.shared.records.calls).report(held)
    }

    /// The protocol of the codec the client's core actually uses, for
    /// tests: the real-server chaos runner checks it against its row, as
    /// the server does not report a connection's encoding.
    // Block 51, D10.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn codec_protocol(&self) -> crate::Protocol {
        self.shared.core_for_reading().machine.codec_protocol()
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        // A poisoned core's client is closed (Block 61, D6).
        self.shared
            .core()
            .map_or(State::Disconnected, |core| core.machine.state())
    }

    /// Closes the client for good: every pending call and `ready()` fails
    /// with code 4 "client closed", every subscription emits `unsubscribed`
    /// with code 2 "client closed" (server-side ones too), then the client
    /// emits `disconnected` with code 0 unless it was disconnected; no
    /// unsubscribe command is sent. Later calls fail at once with code 4,
    /// `connect()` and the setters do nothing, and `new_subscription`
    /// returns [`crate::SubscriptionError::ClientClosed`].
    ///
    /// Closing starts when this returns; the future resolves once the
    /// driver has stopped: the transport closed (within `Config::timeout`)
    /// and every stream ended after the events already queued. Dropping the
    /// future, or never awaiting it, still closes the client. A second call
    /// changes nothing. Dropping the client closes it the same way, without
    /// waiting.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) {
    /// client.close().await;
    /// assert!(client.is_closed());
    /// # }
    /// ```
    // Block 33.
    pub fn close(&self) -> impl Future<Output = ()> + MaybeSend + use<> {
        self.shared.shut_down();
        let shared = Arc::clone(&self.shared);
        async move {
            // From one of its own handlers, closing has started: waiting for
            // the driver could wait on the worker the handler holds (Block
            // 41, D6); under `Thread` the driver stops whatever it does.
            // Checked at every poll, as a call is (Codex, #169).
            let ended = shared.ended();
            let mut ended = std::pin::pin!(ended);
            std::future::poll_fn(|cx| {
                if super::handler::would_deadlock(&shared, super::handler::Wait::Close) {
                    return Poll::Ready(());
                }
                ended.as_mut().poll(cx)
            })
            .await;
        }
    }

    /// Whether the client is closed: by [`Client::close`], its runtime shut
    /// down or its driver panicked. A closed client reports `disconnected`
    /// from [`Client::state`].
    // Block 33.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// A snapshot of the client's connection and traffic counters since it
    /// was built, never a round trip. A closed client keeps its last
    /// counters.
    ///
    /// ```no_run
    /// # fn run(client: &ferrifuge::Client) {
    /// let statistics = client.statistics();
    /// println!("{:?} over {:?}", statistics.state, statistics.transport);
    /// # }
    /// ```
    // Block 31.
    #[must_use]
    pub fn statistics(&self) -> crate::Statistics {
        // It changes nothing: read even from a poisoned core (Block 61, D6).
        let core = self.shared.core_for_reading();
        let mut statistics = core.machine.statistics();
        statistics.connected_since = core.connected_since;
        statistics
    }

    /// Waits until the client is connected, without taking an event from
    /// its stream. The wait starts when this method returns, and the future
    /// borrows nothing from the client, so it can be spawned.
    ///
    /// It follows the state [`Client::state`] reports, not the events
    /// taken: it can resolve before the `connected` event is taken, and a
    /// state that lasts only within one message is not seen. The state
    /// moves only as the client handles messages, each after the events of
    /// earlier ones were taken, so a task holding the client's stream with
    /// events not taken, and awaiting this without reading it, waits until
    /// the stream is read or dropped. No timeout applies; wrap the future
    /// in one:
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) {
    /// use std::time::Duration;
    ///
    /// client.connect();
    /// match tokio::time::timeout(Duration::from_secs(10), client.ready()).await {
    ///     Ok(Ok(())) => println!("connected"),
    ///     Ok(Err(error)) => println!("will not connect: {error}"),
    ///     Err(_) => println!("still connecting"),
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Code 3 "disconnected" (`codes::error::DISCONNECTED`) at once when
    /// the client is disconnected, `connect()` never called included; and
    /// when the client enters `disconnected` while connecting:
    /// `disconnect()`, a terminal code, a denied token. Code 4 "client
    /// closed" (`codes::error::CLIENT_CLOSED`) when the client is closed,
    /// its last clone dropped or its runtime shut down included.
    // Block 30; Block 15; Block 33.
    pub fn ready(&self) -> impl Future<Output = Result<(), CallError>> + MaybeSend + use<> {
        ready::ready(&self.shared, Of::Client)
    }

    /// Sets the client's handler: from now on it takes the client's events,
    /// server-side subscriptions' included, one at a time, in order with
    /// those of its subscriptions' handlers, as a later [`Client::events`]
    /// call would; the earlier stream or handler gets no more. Its events
    /// count as delivered when its method returns.
    // Block 41, D1.
    pub fn set_handler(&self, handler: impl super::ClientHandler) {
        super::handler::set_client_handler(&self.shared, Box::new(handler));
    }

    /// Events from now on. An event counts as delivered once this stream
    /// yields it; until a stream is taken, events count as delivered at
    /// once, so take it before calling [`Client::connect`] to get every
    /// one. A later call takes over: the earlier stream ends and the events
    /// it had not yielded are the new one's. The stream borrows nothing
    /// from the client, so it can be kept beside it, and it ends once the
    /// client is dropped or its runtime shuts down.
    ///
    /// A loop over the stream gives its runtime a turn: once the reading
    /// task has spent its tokio budget, `next()` returns `Pending` and takes
    /// nothing, as tokio's own channels do, so a large recovery does not hold
    /// up the task's other work. In the browser no budget is taken, and
    /// `next()` returns events as long as some are queued.
    // Block 15; Block 28; Block 63, D1, D3.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + MaybeSend + Unpin + use<> {
        let Some(mut core) = self.shared.core() else {
            // Ended at once, as a closed client's (Block 61, D6).
            return EventStream::new(Arc::clone(&self.shared), None, 0);
        };
        let number = lock(&self.shared.outboxes).client.open_stream();
        self.shared.handle(&mut core, Input::StreamOpened(None));
        drop(core);
        EventStream::new(Arc::clone(&self.shared), None, number)
    }

    /// Messages read from the server and not yet handled, and their bytes,
    /// so that tests can wait for the read-ahead to fill. Only with the
    /// `__test-hooks` feature.
    // Block 15.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn read_ahead(&self) -> (usize, usize) {
        self.shared.core_for_reading().machine.read_ahead()
    }

    /// Events of the client queued for its streams and not yet taken, so
    /// that tests can wait for them. Only with the `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_events(&self) -> usize {
        lock(&self.shared.outboxes).client.held()
    }

    /// Makes the driver panic before it handles its next input, outside the
    /// lock of the core, as a bug in its own code would. Only with the
    /// `__test-hooks` feature.
    // Block 49, "A panic in the client's driver ends the client visibly".
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn plant_driver_panic(&self) {
        self.shared
            .driver_panic_planted
            .store(true, Ordering::SeqCst);
    }

    /// Leaves the debounce timers set from now on unarmed, until
    /// [`Client::fire_held_debounce_timers`], so that a test decides when a
    /// window ends. Only with the `__test-hooks` feature.
    // Block 46.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn hold_debounce_timers(&self) {
        *lock(&self.shared.held_debounce_timers) = Some(Vec::new());
    }

    /// How many debounce timers are held unarmed. Only with the
    /// `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_debounce_timers(&self) -> usize {
        lock(&self.shared.held_debounce_timers)
            .as_ref()
            .map_or(0, Vec::len)
    }

    /// Fires the debounce timers held so far, oldest first, as if each had
    /// run its interval; timers set later stay held. Only with the
    /// `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn fire_held_debounce_timers(&self) {
        let held = lock(&self.shared.held_debounce_timers)
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for (timer, setting) in held {
            self.shared
                .apply(Input::TimerFired(TimerKey::Debounce(timer), setting));
        }
    }

    /// Starts connecting; reconnects until connected or disconnected. The
    /// state is [`State::Connecting`] when this returns.
    pub fn connect(&self) {
        let Some(mut core) = self.shared.core() else {
            return;
        };
        if self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        self.shared.handle(&mut core, Input::Connect);
        super::handler::start_again(&self.shared, &mut core);
        drop(core);
        self.shared.actions_queued.notify_one();
    }

    /// Connects again at once, as an app does on its platform's wake or
    /// network-change notification (macOS `NSWorkspace.didWakeNotification`,
    /// Windows `WM_POWERBROADCAST`, systemd-logind `PrepareForSleep`,
    /// Android `ConnectivityManager.NetworkCallback`, iOS `NWPathMonitor`).
    ///
    /// A connected client closes its transport and emits
    /// [`ClientEvent::Connecting`] with code
    /// [`codes::connecting::RECONNECT_CALLED`], then connects after the
    /// reconnect jitter window only, without the backoff, so a fleet told
    /// at once does not reconnect at once; a connecting client starts a new
    /// attempt at once, with no event. Either way the attempt counter
    /// restarts, the transports are tried again from the first, and every
    /// client's next attempt reads the environment's and the operating
    /// system's proxy settings again. A disconnected client is left alone:
    /// call [`Client::connect`] to start it.
    ///
    /// Calls waiting for the connection keep waiting; calls sent on the
    /// closed transport fail with code [`codes::error::CONNECTION_CLOSED`].
    /// Publications not yet taken are withdrawn and, on recoverable
    /// channels, recovered by the next connection.
    pub fn reconnect(&self) {
        #[cfg(adapter_tokio)]
        super::tokio::proxy::read_settings_again();
        self.shared.apply(Input::Reconnect);
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
    ) -> impl Future<Output = Result<crate::RpcResult, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Rpc {
                method: method.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| match reply {
            CallReply::Rpc(data) => Some(crate::RpcResult { data }),
            _ => None,
        })
    }

    /// Calls the server's RPC `method` with `data` encoded as JSON, and
    /// decodes the result data as `Resp`. Made, waiting and failing as
    /// [`Client::rpc`]; `data` is encoded when this returns, so the future
    /// borrows nothing. Under the Protobuf protocol the data are the JSON
    /// bytes. For an RPC that returns no data, use [`Client::rpc`]: empty
    /// data is not JSON.
    ///
    /// Large integers: `data` is encoded as [`Client::publish_json`]
    /// describes, and the result decoded as
    /// [`Publication::json`](crate::Publication::json) describes.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) -> Result<(), ferrifuge::JsonCallError> {
    /// #[derive(serde::Serialize)]
    /// struct Terms {
    ///     a: u32,
    ///     b: u32,
    /// }
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Sum {
    ///     sum: u32,
    /// }
    ///
    /// let sum: Sum = client.rpc_json("sum", &Terms { a: 1, b: 2 }).await?;
    /// println!("{}", sum.sum);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of [`Client::rpc`];
    /// [`JsonCallError::Decode`] when the server answered with data that
    /// does not decode as `Resp`: the RPC ran.
    // Block 38.
    pub fn rpc_json<Req, Resp>(
        &self,
        method: impl Into<String>,
        data: &Req,
    ) -> impl Future<Output = Result<Resp, JsonCallError>> + MaybeSend + 'static
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let call = serde_json::to_vec(data).map(|data| self.rpc(method, data));
        async move {
            let data = call.map_err(JsonCallError::Encode)?.await?.data;
            match serde_json::from_slice(&data) {
                Ok(result) => Ok(result),
                Err(error) => Err(JsonCallError::Decode { error, data }),
            }
        }
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
    ) -> impl Future<Output = Result<crate::PublishResult, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Publish {
                channel: channel.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(crate::PublishResult {})
        })
    }

    /// Publishes `data` encoded as JSON to `channel`, as
    /// [`Client::publish`] does; `data` is encoded when this returns, so
    /// the future borrows nothing. Under the Protobuf protocol the data are
    /// the JSON bytes, which a JSON client receives unchanged.
    ///
    /// A JavaScript reader, such as centrifuge-js without its `bigIntegers`
    /// option, rounds a JSON number past 2^53, so an integer that may grow
    /// past it, such as a 64-bit id, is best sent as a JSON string, for
    /// example with `serde_with`'s `DisplayFromStr`.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) -> Result<(), ferrifuge::JsonCallError> {
    /// #[derive(serde::Serialize)]
    /// struct Note<'a> {
    ///     text: &'a str,
    /// }
    ///
    /// client.publish_json("news", &Note { text: "hi" }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of [`Client::rpc`].
    // Block 38.
    pub fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        channel: impl Into<String>,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.publish(channel, data));
        async move {
            call.map_err(JsonCallError::Encode)?.await?;
            Ok(())
        }
    }

    /// Sets `key` of the map channel `channel` to `data`: raw JSON with the
    /// JSON protocol, any bytes with Protobuf. Made, waiting and failing as
    /// [`Client::rpc`]: it waits for the connection only, not for a map
    /// subscription. Every subscriber of the channel, this client's
    /// included, receives the change. Never debounced, unlike
    /// [`MapSubscription::publish`](crate::MapSubscription::publish) on a
    /// namespace where the server advises an interval.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// The key is not checked here: the server may replace it (its
    /// namespace's `map.client_key`) and refuses an empty one.
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`]; the server's refusal keeps its code, e.g. 103
    /// "permission denied" on a namespace that does not allow a client's
    /// writes or is not a map namespace, 107 "bad request" without data or
    /// with an empty key.
    // Block 45, D8; Block 46.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_publish(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::MapPublish {
                channel: channel.into(),
                key: key.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// Sets `key` of the map channel `channel` to `data` encoded as JSON,
    /// as [`Client::map_publish`] does; `data` is encoded when this
    /// returns, so the future borrows nothing.
    ///
    /// Large integers are encoded as
    /// [`Client::publish_json`](crate::Client::publish_json) describes.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of
    /// [`Client::map_publish`].
    // Block 38.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_publish_json<T: serde::Serialize + ?Sized>(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.map_publish(channel, key, data));
        async move { Ok(call.map_err(JsonCallError::Encode)?.await?) }
    }

    /// Removes `key` from the map channel `channel`. Made, waiting and
    /// failing as [`Client::map_publish`]; removing a key the map does not
    /// hold succeeds. Never debounced.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// As [`Client::map_publish`].
    // Block 45, D8; Block 46.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_remove(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::MapRemove {
                channel: channel.into(),
                key: key.into(),
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
    ) -> impl Future<Output = Result<HistoryResult, CallError>> + MaybeSend + 'static {
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
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + MaybeSend + 'static {
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
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
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
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + MaybeSend + 'static {
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

/// One turn's batches: what one `take` moves into `into` (flushing first
/// when `flush`), performed in order; then another take only while an
/// action just performed fed the core an input (`perform` returns true).
/// What `take` would find otherwise waits for the next turn. `into`, empty
/// then, releases what the turn grew: no later turn need come to release
/// it.
// Block 14; Codex, #271.
pub(super) fn perform_batches<S>(
    state: &mut S,
    into: &mut VecDeque<Action>,
    flush: bool,
    take: fn(&mut S, &mut VecDeque<Action>, bool),
    perform: fn(&mut S, Action) -> bool,
) {
    let mut flush = flush;
    loop {
        take(state, into, std::mem::take(&mut flush));
        let mut fed = false;
        while let Some(action) = into.pop_front() {
            fed |= perform(state, action);
        }
        if !fed {
            into.shrink_to(crate::RESTING_SLOTS);
            return;
        }
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

#[expect(
    clippy::needless_pass_by_value,
    reason = "the signature `reply_of` takes"
)]
pub(super) fn presence_stats_of(reply: CallReply) -> Option<PresenceStatsResult> {
    match reply {
        CallReply::PresenceStats(stats) => Some(stats),
        _ => None,
    }
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use super::*;
    use crate::adapter::test_support::*;
    use futures_util::FutureExt;
    use futures_util::StreamExt;
    use std::time::Duration;
    use std::time::SystemTime;

    // The typed calls borrow nothing: their futures can be spawned whatever
    // the application's types (Block 38, D3).
    #[test]
    fn typed_call_futures_are_send_and_static() {
        use std::marker::PhantomData;

        /// Neither `Send` nor `Sync`.
        struct Local(PhantomData<*const ()>);
        impl serde::Serialize for Local {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_unit()
            }
        }
        /// Neither `Send` nor `'static`.
        struct Borrowing<'a>(PhantomData<&'a *const ()>);
        impl<'de> serde::Deserialize<'de> for Borrowing<'_> {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                serde::de::IgnoredAny::deserialize(deserializer)?;
                Ok(Self(PhantomData))
            }
        }
        fn spawnable<F: Future + Send + 'static>(_: &F) {}
        // `'a` is no longer than the call: `Borrowing<'a>` is not `'static`.
        fn rpc_with<'a>(client: &Client, local: &Local, _: PhantomData<&'a ()>) {
            spawnable(&client.rpc_json::<_, Borrowing<'a>>("m", local));
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _entered = runtime.enter();
        let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default())
            .expect("a valid endpoint");
        let local = Local(PhantomData);
        rpc_with(&client, &local, PhantomData);
        spawnable(&client.publish_json("news", &local));
        let subscription = client.new_subscription("news").expect("a new channel");
        spawnable(&subscription.publish_json(&local));
    }

    // Clones and weak handles cross tasks and threads (Block 41, D5).
    #[test]
    fn clients_and_weak_handles_are_send_and_sync() {
        fn shared<T: Send + Sync + Clone>() {}
        shared::<Client>();
        shared::<WeakClient>();
    }

    // A batch can be stopped from another task (Block 18, D1).
    #[test]
    fn a_batch_is_send() {
        fn send<T: Send>() {}
        send::<Batch>();
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

    // Block 32, D1: a refused header leaves the set as it was, named
    // without its value.
    #[tokio::test]
    async fn a_refused_header_leaves_the_set_and_names_the_header_only() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .http_headers([("X-Kept", "k")])
            .build()
            .unwrap();
        for (name, value) in [
            ("Sec-WebSocket-Key", "v-secret"),
            ("Proxy-Authorization", "v-secret"),
            ("bad name", "v-secret"),
            ("X-Bad-Value", "v-secret\n"),
        ] {
            let error = client
                .set_http_headers([("X-New", "n"), (name, value)])
                .expect_err(name);

            assert_eq!(error.kind, crate::ErrorKind::Configuration, "{name}");
            assert!(error.message.contains(name), "{error:?}");
            assert!(!error.message.contains("v-secret"), "{error:?}");
            assert_eq!(
                header_names(&client),
                [("x-kept".to_owned(), "k".to_owned())]
            );
        }
    }

    // Block 32, D1: the whole set is replaced; a name given twice keeps the
    // later value.
    #[tokio::test]
    async fn set_http_headers_replaces_the_whole_set() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .http_headers([("X-Old", "o")])
            .build()
            .unwrap();

        client
            .set_http_headers([("X-New", "1"), ("x-new", "2")])
            .unwrap();

        assert_eq!(
            header_names(&client),
            [("x-new".to_owned(), "2".to_owned())]
        );
    }

    // Block 45, D8: the map calls carry their channel and key to the wire,
    // and the empty publish result resolves them.
    #[cfg(feature = "unstable")]
    #[tokio::test]
    async fn map_calls_send_their_channel_and_key() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (publishes, mut received) = tokio::sync::mpsc::unbounded_channel();
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
                    } else if let Some(publish) = command.get("publish") {
                        publishes.send(publish.clone()).unwrap();
                        let reply = format!(r#"{{"id":{id},"publish":{{}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                    }
                }
            }
        });
        let client = Client::new(endpoint, Config::default()).unwrap();
        client.connect();

        client
            .map_publish("map:a", "k1", br#"{"x":1}"#.to_vec())
            .await
            .unwrap();
        client.map_remove("map:a", "k2").await.unwrap();
        client
            .map_publish_json("map:b", "k3", &serde_json::json!({"y": 2}))
            .await
            .unwrap();

        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(received.recv().await.unwrap());
        }
        assert_eq!(
            seen,
            [
                serde_json::json!({"channel": "map:a", "data": {"x": 1}, "type": 1, "key": "k1"}),
                serde_json::json!({"channel": "map:a", "type": 1, "key": "k2", "removed": true}),
                serde_json::json!({"channel": "map:b", "data": {"y": 2}, "type": 1, "key": "k3"}),
            ]
        );
    }

    // Block 32, D2: the next attempt's upgrade request carries the new set.
    #[tokio::test]
    async fn the_next_upgrade_request_carries_the_new_set() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://{}/connection/websocket",
            listener.local_addr().unwrap()
        );
        let client = Client::builder(url)
            .http_headers([("X-Device-Real", "r1")])
            .build()
            .unwrap();
        client.set_http_headers([("X-Device-Real", "r2")]).unwrap();

        client.connect();
        let (mut socket, _) = listener.accept().await.unwrap();
        // The whole head: TCP may deliver it in several reads.
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0, "the connection ended before the request's head");
            request.extend_from_slice(&buffer[..read]);
        }
        let request = String::from_utf8_lossy(&request).to_lowercase();

        assert!(request.contains("x-device-real: r2\r\n"), "{request}");
        // The header, not "r1" anywhere: the random sec-websocket-key may
        // hold it (seen on macOS, #159).
        assert!(!request.contains("x-device-real: r1"), "{request}");
    }

    // Block 32, D4: the set's values never reach a Debug output.
    #[tokio::test]
    async fn debug_output_shows_header_names_only() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .build()
            .unwrap();
        client
            .set_http_headers([("Authorization", "h-secret")])
            .unwrap();
        let batch = client.start_batching();

        let debug = format!("{client:?} {batch:?} {:?}", client.shared());

        assert!(!debug.contains("h-secret"), "{debug}");
        assert!(debug.contains("authorization"), "{debug}");
        drop(batch);
    }

    // Block 33, D1: close() resolves once the driver has ended, the
    // transport's close awaited (here an emulation request in flight, up
    // to the timeout): no task of the client is left.
    #[test]
    fn close_resolves_once_no_task_of_the_client_is_left() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let server = tokio::runtime::Runtime::new().unwrap();
        let listener = server
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!(
            "http://{}/connection/http_stream",
            listener.local_addr().unwrap()
        );
        server.spawn(stalling_http_stream(listener));
        let timeout = Duration::from_millis(400);
        let client = Client::builder_with_transports([(TransportKind::HttpStream, url)])
            .config(Config::default().timeout(timeout))
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        client.connect();
        let ready = client.ready();
        let connected =
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), ready).await });
        assert_eq!(connected.ok(), Some(Ok(())));
        // An emulation request the server never answers, its deadline
        // counting from when it is made.
        let made = std::time::Instant::now();
        let call = client.rpc("slow", b"{}".to_vec());
        std::thread::sleep(Duration::from_millis(100));

        runtime.block_on(client.close());
        // From the call, not from the close: a sleep that oversleeps, as on
        // macOS runners, shortens the close but not this.
        let took = made.elapsed();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        while runtime.metrics().num_alive_tasks() > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert!(
            took >= timeout.saturating_sub(Duration::from_millis(50)),
            "the close was not awaited: {took:?}"
        );
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
        assert!(client.is_closed());
        let failed = runtime.block_on(call).unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        runtime.shutdown_background();
        server.shutdown_background();
    }

    // Codex, #141: every transport's close is awaited, not only the last.
    // Event-driven, with no wall-clock floor: the first transport's close
    // waits for an emulation request the server holds until the test
    // releases it, under a timeout longer than the test.
    #[cfg(feature = "__test-hooks")]
    #[test]
    fn close_awaits_the_close_of_an_earlier_transport_too() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let server = tokio::runtime::Runtime::new().unwrap();
        let listener = server
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!(
            "http://{}/connection/http_stream",
            listener.local_addr().unwrap()
        );
        let stall = Stall::default();
        server.spawn(stalling_http_stream_with(listener, stall.clone()));
        let client = Client::builder_with_transports([(TransportKind::HttpStream, url)])
            .config(Config::default().timeout(Duration::from_secs(10)))
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        client.connect();
        assert_eq!(within(&runtime, "connected", client.ready()), Ok(()));
        // An emulation request the server holds keeps the first
        // transport's close running.
        let call = client.rpc("slow", b"{}".to_vec());
        within(
            &runtime,
            "the call's emulation request",
            stall.emulation.notified(),
        );
        client.reconnect();
        assert_eq!(within(&runtime, "connected again", client.ready()), Ok(()));

        let mut close = Box::pin(client.close());
        within(
            &runtime,
            "the driver's end awaits the first transport's close",
            client.shared().awaiting_earlier_close.notified(),
        );
        assert!(
            close.as_mut().now_or_never().is_none(),
            "close() resolved before the first transport's close ended"
        );
        stall.release.notify_one();
        within(&runtime, "close() resolves once it is released", close);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while runtime.metrics().num_alive_tasks() > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
        drop(call);
        runtime.shutdown_background();
        server.shutdown_background();
    }

    // Block 33, D1: dropping the future still closes; a second close()
    // resolves; the stream yields what it held, then ends.
    #[tokio::test]
    async fn a_dropped_close_future_still_closes_and_a_second_one_resolves() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut events = client.events();
        client.connect();
        let call = client.rpc("echo", b"{}".to_vec());

        drop(client.close());

        assert!(client.is_closed());
        assert_eq!(client.state(), State::Disconnected);
        tokio::time::timeout(Duration::from_secs(5), client.close())
            .await
            .expect("a second close resolves");
        let failed = call.await.unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        let mut names = Vec::new();
        while let Some(event) = events.next().await {
            names.push(event.name());
        }
        assert_eq!(names, ["connecting", "disconnected"]);
    }

    // Block 33, D3: waiting ready() futures fail with code 4.
    #[tokio::test]
    async fn close_fails_waiting_ready_futures_with_code_4() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        client.connect();
        let of_client = client.ready();
        let of_subscription = subscription.ready();

        client.close().await;

        assert_eq!(
            of_client.await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
        assert_eq!(
            of_subscription.await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
        assert_eq!(subscription.state(), crate::SubscriptionState::Unsubscribed);
        assert_eq!(
            subscription.ready().await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
    }

    // Block 31, D2: set when connected is seen, cleared otherwise.
    #[test]
    fn connected_since_follows_the_state() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        assert_eq!(client.statistics().connected_since, None);

        let before = SystemTime::now();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c"}}"#);
        drop(core);
        let after = SystemTime::now();
        let first = client.statistics().connected_since.expect("connected");
        assert!(before <= first && first <= after);

        let shared = client.shared();
        let mut core = shared.core().expect("a core");
        shared.handle(
            &mut core,
            Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            },
        );
        drop(core);
        assert_eq!(client.statistics().connected_since, None);

        client.disconnect();
        assert_eq!(client.statistics().connected_since, None);
        assert_eq!(client.statistics().connects, 1);
    }

    // Block 31, D2: a closed client, its core lock poisoned too, keeps its
    // counters.
    #[test]
    fn a_closed_client_keeps_its_counters() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        drop(connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c"}}"#,
        ));
        let shared = Arc::clone(client.shared());

        drop(runtime);
        let poisoned = std::thread::spawn(move || {
            let _core = shared.core_mutex.lock().unwrap();
            panic!("poisons the core lock");
        })
        .join();

        assert!(poisoned.is_err());
        let statistics = client.statistics();
        assert_eq!(statistics.connects, 1);
        assert_eq!(statistics.state, State::Disconnected);
    }

    // Block 25, D7.
    #[tokio::test]
    async fn reconnect_asks_for_the_proxy_settings_to_be_read_again() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let before = super::super::tokio::proxy::settings_generation();

        client.reconnect();

        assert!(super::super::tokio::proxy::settings_generation() > before);
    }

    fn header_names(client: &Client) -> Vec<(String, String)> {
        client
            .shared()
            .backend
            .http_headers
            .current()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect()
    }

    /// `future`'s output, within 5 s on `runtime`; else a panic naming
    /// `what` was awaited.
    fn within<F: Future>(runtime: &tokio::runtime::Runtime, what: &str, future: F) -> F::Output {
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), future).await })
            .unwrap_or_else(|_| panic!("not within 5 s: {what}"))
    }
}
