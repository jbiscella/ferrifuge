//! Sans-IO core: the client connection state machine.
//!
//! A [`Machine`] owns no sockets, timers or tasks. Its owner feeds it
//! [`Input`]s and performs the returned [`Action`]s in order. The tokio
//! adapter is one such owner; any other runtime can drive it the same way.
//!
//! Behaviour follows the Client SDK spec, with centrifuge-js as the
//! implementation reference (`src/centrifuge.ts`).

use std::time::Duration;

mod delivery;
mod inbound;
mod server_subscriptions;
mod subscriptions;
mod tokens;
mod transports;

use tokens::{
    BeforeConnect, FetchPurpose, REFRESH_RETRY_MAX, REFRESH_RETRY_MIN, TOKEN_EXPIRED, Tokens,
    refresh_delay,
};

use crate::backoff::{self, Backoff};
use crate::codec::Codec;
use crate::config::Protocol;
use crate::json;
use crate::token::Secret;
use crate::wire::{
    Command, ConnectRequest, ConnectResult, Disconnect, Error, Method, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult,
};
use crate::{
    ClientError, ClientEvent, Config, DataError, ErrorKind, State, StateError, StreamPosition,
    SubscriptionEvent, TokenError, codes,
};
use server_subscriptions::ServerSubscriptions;
use subscriptions::Subscriptions;
use transports::Transports;

/// A transport the client can connect over. `#[non_exhaustive]` so that
/// adding one breaks no owner's `match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransportKind {
    /// Websocket, `ws://` or `wss://`.
    WebSocket,
    /// HTTP streaming with bidirectional emulation: a streamed POST response
    /// carries the replies, commands go in requests to the emulation
    /// endpoint. `http://` or `https://`, JSON only.
    HttpStream,
    /// Server-sent events with bidirectional emulation, as
    /// [`TransportKind::HttpStream`] but the replies come as an event stream.
    Sse,
}

impl TransportKind {
    /// Name of the transport as reported in events, as in centrifuge-js.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::HttpStream => "http_stream",
            Self::Sse => "sse",
        }
    }

    /// Whether the transport emulates a bidirectional connection over
    /// HTTP requests.
    #[must_use]
    pub fn is_emulated(self) -> bool {
        matches!(self, Self::HttpStream | Self::Sse)
    }
}

/// Identifies one transport the machine asked to open. Inputs about a
/// transport the machine has since abandoned are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportId(u64);

/// Identifies one token fetch the machine asked for. Results of fetches the
/// machine has since abandoned are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchId(u64);

/// Identifies an event the machine emitted, for its owner to report with
/// [`Input::EventTaken`] once the application has taken it (Block 15).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId(u64);

impl EventId {
    /// An id for the adapter's unit tests.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn for_tests(n: u64) -> Self {
        Self(n)
    }
}

/// Identifies a subscription in the machine's registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(u64);

/// Options of a subscription (spec, "Subscription common options").
///
/// Its `Debug` output redacts the token.
#[allow(clippy::struct_excessive_bools)] // Independent options of the spec.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionOptions {
    /// Data sent with every subscribe command.
    pub data: Option<Vec<u8>>,
    /// The owner answers [`Action::FetchSubscriptionData`] before every
    /// subscribe command, instead of sending `data`.
    pub data_source: bool,
    /// Lower bound of the resubscribe delay. Default 500 ms (centrifuge-js
    /// `minResubscribeDelay`).
    pub min_resubscribe_delay: Duration,
    /// Upper bound of the resubscribe delay. Default 20 s (centrifuge-js
    /// `maxResubscribeDelay`).
    pub max_resubscribe_delay: Duration,
    /// Subscription token (JWT) sent with the first subscribe command.
    /// Empty for none.
    pub token: String,
    /// The owner answers [`Action::FetchSubscriptionToken`] when a
    /// subscription token is needed (centrifuge-js `getToken`).
    pub token_source: bool,
    /// Position to recover from on the first subscribe (spec, `since`).
    pub since: Option<StreamPosition>,
    /// The owner answers [`Action::FetchSubscriptionState`] when the
    /// subscription has no stream position: before the first subscribe
    /// without `since`, and after a recovery failed (spec, "Subscription
    /// getState").
    pub state_source: bool,
    /// Asks for a positioned subscription on a channel that allows it but
    /// does not force it (spec, `positioned`).
    pub positioned: bool,
    /// Asks for a recoverable subscription on a channel that allows it but
    /// does not force it (spec, `recoverable`).
    pub recoverable: bool,
}

impl std::fmt::Debug for SubscriptionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionOptions")
            .field(
                "data",
                &self
                    .data
                    .as_ref()
                    .map(|data| format!("<{} bytes>", data.len())),
            )
            .field("data_source", &self.data_source)
            .field("min_resubscribe_delay", &self.min_resubscribe_delay)
            .field("max_resubscribe_delay", &self.max_resubscribe_delay)
            .field("token", &Secret(self.token.clone()))
            .field("token_source", &self.token_source)
            .field("since", &self.since)
            .field("state_source", &self.state_source)
            .field("positioned", &self.positioned)
            .field("recoverable", &self.recoverable)
            .finish()
    }
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            data: None,
            data_source: false,
            min_resubscribe_delay: Duration::from_millis(500),
            max_resubscribe_delay: Duration::from_secs(20),
            token: String::new(),
            token_source: false,
            since: None,
            state_source: false,
            positioned: false,
            recoverable: false,
        }
    }
}

impl SubscriptionOptions {
    /// Sends `data` with every subscribe command, instead of asking the
    /// owner: the last of this and [`Self::data_source`] decides.
    #[must_use]
    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = Some(data);
        self.data_source = false;
        self
    }

    /// Asks the owner for the data before every subscribe command, instead
    /// of sending fixed data: the last of this and [`Self::data`] decides.
    #[must_use]
    pub fn data_source(mut self) -> Self {
        self.data = None;
        self.data_source = true;
        self
    }

    /// Sets the lower bound of the resubscribe delay.
    #[must_use]
    pub fn min_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.min_resubscribe_delay = delay;
        self
    }

    /// Sets the upper bound of the resubscribe delay.
    #[must_use]
    pub fn max_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.max_resubscribe_delay = delay;
        self
    }

    /// Sends `token` with the first subscribe command.
    #[must_use]
    pub fn token(mut self, token: String) -> Self {
        self.token = token;
        self
    }

    /// Asks the owner for a token when one is needed: when there is none,
    /// when it expired, or before it expires.
    #[must_use]
    pub fn token_source(mut self) -> Self {
        self.token_source = true;
        self
    }

    /// Recovers the publications after `position` on the first subscribe.
    #[must_use]
    pub fn since(mut self, position: StreamPosition) -> Self {
        self.since = Some(position);
        self
    }

    /// Asks the owner for the application's state and its stream position
    /// when the subscription has none (centrifuge-js `getState`).
    #[must_use]
    pub fn state_source(mut self) -> Self {
        self.state_source = true;
        self
    }

    /// Asks for a positioned subscription.
    #[must_use]
    pub fn positioned(mut self, positioned: bool) -> Self {
        self.positioned = positioned;
        self
    }

    /// Asks for a recoverable subscription.
    #[must_use]
    pub fn recoverable(mut self, recoverable: bool) -> Self {
        self.recoverable = recoverable;
        self
    }
}

/// Timers the machine sets for a subscription. At most one of each kind
/// runs per subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionTimer {
    /// Delay before the next subscribe attempt.
    Resubscribe,
    /// Deadline for the subscribe command to get its reply.
    SubscribeReply,
    /// Deadline for an unsubscribe command to get its reply.
    UnsubscribeReply,
    /// When the subscription token must be refreshed.
    Refresh,
    /// Deadline for the `sub_refresh` command to get its reply.
    RefreshReply,
}

/// Timers the machine sets. At most one of each kind runs at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timer {
    /// Delay before the next connection attempt.
    Reconnect,
    /// Deadline for a connection attempt to get its connect reply.
    ConnectTimeout,
    /// When the connection token must be refreshed.
    Refresh,
    /// Deadline for the refresh command to get its reply.
    RefreshReply,
    /// Deadline for data from a server that announced pings.
    ServerPing,
}

impl Timer {
    /// Number of timer kinds.
    pub const COUNT: usize = 5;

    /// Every timer kind, in [`Timer::index`] order.
    pub const ALL: [Timer; Self::COUNT] = [
        Self::Reconnect,
        Self::ConnectTimeout,
        Self::Refresh,
        Self::RefreshReply,
        Self::ServerPing,
    ];

    /// Position in [`Timer::ALL`], e.g. for an owner keeping one slot per
    /// kind, as the tokio adapter does.
    #[must_use]
    pub fn index(self) -> usize {
        // Exhaustive: a new kind fails to compile until it gets an index
        // (and a place in `ALL`, checked by a test).
        match self {
            Self::Reconnect => 0,
            Self::ConnectTimeout => 1,
            Self::Refresh => 2,
            Self::RefreshReply => 3,
            Self::ServerPing => 4,
        }
    }
}

/// Something that happened, for the machine to react to.
///
/// Its `Debug` output redacts a fetched token.
#[derive(Clone, PartialEq, Eq)]
pub enum Input {
    /// The application called `connect()`.
    Connect,
    /// The application called `disconnect()`.
    Disconnect,
    /// The transport opened.
    TransportOpened(TransportId),
    /// The transport could not be opened.
    TransportFailed {
        /// The transport that failed.
        transport: TransportId,
        /// Why, for the error event.
        message: String,
    },
    /// The transport closed after opening.
    TransportClosed {
        /// The transport that closed.
        transport: TransportId,
        /// Close code, when the transport has one.
        code: Option<u16>,
        /// Close reason, possibly empty.
        reason: String,
    },
    /// The transport received a frame.
    Frame {
        /// The transport that received it.
        transport: TransportId,
        /// Encoded replies.
        data: Vec<u8>,
    },
    /// The server refused a frame sent with [`Action::Send`] without closing
    /// the transport (an emulation request answered with HTTP 413): its
    /// commands failed.
    SendRejected {
        /// The transport the frame was sent on.
        transport: TransportId,
        /// The frame, as the action carried it.
        frame: Vec<u8>,
    },
    /// A timer set by [`Action::SetTimer`] fired.
    TimerFired(Timer),
    /// The token callback answered a [`Action::FetchToken`].
    TokenFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The new token, or why there is none.
        result: Result<String, TokenError>,
    },
    /// The application called `subscribe()` on a subscription.
    Subscribe(SubscriptionId),
    /// The application called `unsubscribe()` on a subscription.
    Unsubscribe(SubscriptionId),
    /// The application removed a subscription from the registry.
    RemoveSubscription(SubscriptionId),
    /// A timer set by [`Action::SetSubscriptionTimer`] fired.
    SubscriptionTimerFired(SubscriptionId, SubscriptionTimer),
    /// The data callback answered a [`Action::FetchSubscriptionData`].
    SubscriptionDataFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The data, or why there is none.
        result: Result<Vec<u8>, DataError>,
    },
    /// The subscription token callback answered a
    /// [`Action::FetchSubscriptionToken`].
    SubscriptionTokenFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The new token, or why there is none.
        result: Result<String, TokenError>,
    },
    /// The getState callback answered a [`Action::FetchSubscriptionState`].
    SubscriptionStateFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The stream position of the state the application loaded, or why
        /// it could not load it.
        result: Result<StreamPosition, StateError>,
    },
    /// The application took an event of [`Action::Emit`] or
    /// [`Action::EmitSubscription`] from its stream.
    EventTaken(EventId),
    /// A stream takes the events of a subscription, or of the client when
    /// `None` (server-side subscriptions' included), from now on. Until
    /// then, and after [`Input::StreamClosed`], their events count as taken
    /// once emitted (Block 15).
    StreamOpened(Option<SubscriptionId>),
    /// No stream takes the events of a subscription, or of the client when
    /// `None`, anymore: report the events it had not taken with
    /// [`Input::EventTaken`] first.
    StreamClosed(Option<SubscriptionId>),
}

/// Something the owner of the machine must do.
///
/// Its `Debug` output shows only the length of a [`Action::Send`] frame:
/// connect and refresh frames carry the connection token.
#[derive(Clone, PartialEq, Eq)]
pub enum Action {
    /// Open a transport to the endpoint; report the outcome with
    /// [`Input::TransportOpened`] or [`Input::TransportFailed`].
    OpenTransport {
        /// Identifies this transport in later inputs and actions.
        transport: TransportId,
        /// Which kind of transport to open.
        kind: TransportKind,
        /// Index of the endpoint to open in the list given to
        /// [`Machine::with_transports`]; 0 for the single websocket.
        endpoint: usize,
        /// For an emulated transport ([`TransportKind::is_emulated`]), the
        /// encoded connect command, sent with the request opening the
        /// stream: in the `http_stream` request body, in the sse
        /// `cf_connect` query parameter. Later [`Action::Send`] frames are
        /// bodies of requests to the emulation endpoint.
        initial_frame: Option<Vec<u8>>,
    },
    /// Close the transport and report nothing more about it.
    CloseTransport(TransportId),
    /// Send a frame over the transport.
    Send {
        /// The transport to send on.
        transport: TransportId,
        /// Encoded commands.
        frame: Vec<u8>,
        /// How the frame travels on transports with typed frames.
        frame_type: FrameType,
    },
    /// Start a timer, replacing a running one of the same kind.
    SetTimer(Timer, Duration),
    /// Stop a timer if it runs.
    CancelTimer(Timer),
    /// Deliver an event to the application; report it with
    /// [`Input::EventTaken`] once the application has taken it.
    Emit(EventId, ClientEvent),
    /// Ask the application's token callback for a new connection token;
    /// report the answer with [`Input::TokenFetched`]. Only issued by a
    /// machine created with [`Machine::with_token_source`].
    FetchToken(FetchId),
    /// Start a subscription's timer, replacing a running one of the kind.
    SetSubscriptionTimer(SubscriptionId, SubscriptionTimer, Duration),
    /// Stop a subscription's timer if it runs.
    CancelSubscriptionTimer(SubscriptionId, SubscriptionTimer),
    /// Deliver an event to a subscription's listeners; report it with
    /// [`Input::EventTaken`] once the application has taken it.
    EmitSubscription(EventId, SubscriptionId, SubscriptionEvent),
    /// Never deliver these events, emitted and not yet taken: publications
    /// of a subscription that left `subscribed`, or whose position was set
    /// otherwise, which recovery brings back (Block 15).
    Withdraw(Vec<EventId>),
    /// Ask the subscription's data callback for the data of its next
    /// subscribe command; report the answer with
    /// [`Input::SubscriptionDataFetched`]. Only issued for subscriptions
    /// with [`SubscriptionOptions::data_source`].
    FetchSubscriptionData {
        /// The subscription asking.
        subscription: SubscriptionId,
        /// Its channel, given to the callback.
        channel: String,
        /// Identifies the fetch in its answer.
        fetch: FetchId,
    },
    /// Stop a [`Action::FetchSubscriptionData`] still running: its answer
    /// is no longer wanted.
    CancelSubscriptionData(FetchId),
    /// Ask the subscription's token callback for a new subscription token;
    /// report the answer with [`Input::SubscriptionTokenFetched`]. Only
    /// issued for subscriptions with [`SubscriptionOptions::token_source`].
    FetchSubscriptionToken {
        /// The subscription asking.
        subscription: SubscriptionId,
        /// Its channel, given to the callback.
        channel: String,
        /// Identifies the fetch in its answer.
        fetch: FetchId,
    },
    /// Stop a [`Action::FetchSubscriptionToken`] still running: its answer
    /// is no longer wanted.
    CancelSubscriptionToken(FetchId),
    /// Ask the subscription's getState callback to load the application's
    /// state and give its stream position; report the answer with
    /// [`Input::SubscriptionStateFetched`]. Only issued for subscriptions
    /// with [`SubscriptionOptions::state_source`].
    FetchSubscriptionState {
        /// The subscription asking.
        subscription: SubscriptionId,
        /// Its channel, given to the callback.
        channel: String,
        /// Identifies the fetch in its answer.
        fetch: FetchId,
    },
    /// Stop a [`Action::FetchSubscriptionState`] still running: its answer
    /// is no longer wanted.
    CancelSubscriptionState(FetchId),
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect => f.write_str("Connect"),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::TransportOpened(id) => f.debug_tuple("TransportOpened").field(id).finish(),
            Self::TransportFailed { transport, message } => f
                .debug_struct("TransportFailed")
                .field("transport", transport)
                .field("message", message)
                .finish(),
            Self::TransportClosed {
                transport,
                code,
                reason,
            } => f
                .debug_struct("TransportClosed")
                .field("transport", transport)
                .field("code", code)
                .field("reason", reason)
                .finish(),
            Self::Frame { transport, data } => f
                .debug_struct("Frame")
                .field("transport", transport)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            // The frame may carry a token: only its length is shown.
            Self::SendRejected { transport, frame } => f
                .debug_struct("SendRejected")
                .field("transport", transport)
                .field("frame", &format_args!("<{} bytes>", frame.len()))
                .finish(),
            Self::TimerFired(timer) => f.debug_tuple("TimerFired").field(timer).finish(),
            Self::TokenFetched { fetch, result } => f
                .debug_struct("TokenFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result.as_ref().map(|token| Secret(token.clone())),
                )
                .finish(),
            Self::Subscribe(id) => f.debug_tuple("Subscribe").field(id).finish(),
            Self::Unsubscribe(id) => f.debug_tuple("Unsubscribe").field(id).finish(),
            Self::RemoveSubscription(id) => f.debug_tuple("RemoveSubscription").field(id).finish(),
            Self::SubscriptionTimerFired(id, timer) => f
                .debug_tuple("SubscriptionTimerFired")
                .field(id)
                .field(timer)
                .finish(),
            Self::SubscriptionDataFetched { fetch, result } => f
                .debug_struct("SubscriptionDataFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result
                        .as_ref()
                        .map(|data| format!("<{} bytes>", data.len())),
                )
                .finish(),
            Self::SubscriptionTokenFetched { fetch, result } => f
                .debug_struct("SubscriptionTokenFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result.as_ref().map(|token| Secret(token.clone())),
                )
                .finish(),
            Self::SubscriptionStateFetched { fetch, result } => f
                .debug_struct("SubscriptionStateFetched")
                .field("fetch", fetch)
                .field("result", result)
                .finish(),
            Self::EventTaken(id) => f.debug_tuple("EventTaken").field(id).finish(),
            Self::StreamOpened(of) => f.debug_tuple("StreamOpened").field(of).finish(),
            Self::StreamClosed(of) => f.debug_tuple("StreamClosed").field(of).finish(),
        }
    }
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenTransport {
                transport,
                kind,
                endpoint,
                initial_frame,
            } => f
                .debug_struct("OpenTransport")
                .field("transport", transport)
                .field("kind", kind)
                .field("endpoint", endpoint)
                .field(
                    "initial_frame",
                    &initial_frame
                        .as_ref()
                        .map(|frame| format!("<{} bytes>", frame.len())),
                )
                .finish(),
            Self::CloseTransport(id) => f.debug_tuple("CloseTransport").field(id).finish(),
            Self::Send {
                transport,
                frame,
                frame_type,
            } => f
                .debug_struct("Send")
                .field("transport", transport)
                .field("frame", &format_args!("<{} bytes>", frame.len()))
                .field("frame_type", frame_type)
                .finish(),
            Self::SetTimer(timer, delay) => {
                f.debug_tuple("SetTimer").field(timer).field(delay).finish()
            }
            Self::CancelTimer(timer) => f.debug_tuple("CancelTimer").field(timer).finish(),
            Self::Emit(id, event) => f.debug_tuple("Emit").field(id).field(event).finish(),
            Self::FetchToken(id) => f.debug_tuple("FetchToken").field(id).finish(),
            Self::SetSubscriptionTimer(id, timer, delay) => f
                .debug_tuple("SetSubscriptionTimer")
                .field(id)
                .field(timer)
                .field(delay)
                .finish(),
            Self::CancelSubscriptionTimer(id, timer) => f
                .debug_tuple("CancelSubscriptionTimer")
                .field(id)
                .field(timer)
                .finish(),
            Self::EmitSubscription(event_id, id, event) => f
                .debug_tuple("EmitSubscription")
                .field(event_id)
                .field(id)
                .field(event)
                .finish(),
            Self::Withdraw(ids) => f.debug_tuple("Withdraw").field(ids).finish(),
            Self::FetchSubscriptionData {
                subscription,
                channel,
                fetch,
            } => f
                .debug_struct("FetchSubscriptionData")
                .field("subscription", subscription)
                .field("channel", channel)
                .field("fetch", fetch)
                .finish(),
            Self::CancelSubscriptionData(fetch) => f
                .debug_tuple("CancelSubscriptionData")
                .field(fetch)
                .finish(),
            Self::FetchSubscriptionToken {
                subscription,
                channel,
                fetch,
            } => f
                .debug_struct("FetchSubscriptionToken")
                .field("subscription", subscription)
                .field("channel", channel)
                .field("fetch", fetch)
                .finish(),
            Self::CancelSubscriptionToken(fetch) => f
                .debug_tuple("CancelSubscriptionToken")
                .field(fetch)
                .finish(),
            Self::FetchSubscriptionState {
                subscription,
                channel,
                fetch,
            } => f
                .debug_struct("FetchSubscriptionState")
                .field("subscription", subscription)
                .field("channel", channel)
                .field("fetch", fetch)
                .finish(),
            Self::CancelSubscriptionState(fetch) => f
                .debug_tuple("CancelSubscriptionState")
                .field(fetch)
                .finish(),
        }
    }
}

/// How a frame travels on a transport that types its frames, such as a
/// websocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameType {
    /// UTF-8 text (JSON encoding).
    Text,
    /// Bytes (Protobuf encoding).
    Binary,
}

/// Server pings announced in the connect reply.
#[derive(Debug, Clone, Copy)]
struct Pings {
    interval: Duration,
    /// Whether the server expects a pong for each ping.
    pong: bool,
}

/// Disconnect code by which the server invalidates the connection's state
/// (centrifuge-js `disconnectedCodes.stateInvalidated`).
const STATE_INVALIDATED: u32 = 3014;

/// The client connection state machine.
#[derive(Debug)]
pub struct Machine {
    config: Config,
    state: State,
    backoff: Backoff,
    rng: fastrand::Rng,
    /// The transport opening or open, if any.
    transport: Option<TransportId>,
    transport_open: bool,
    next_transport: u64,
    next_command: u32,
    next_event: u64,
    /// Publications emitted and not yet taken (Block 15).
    deliveries: delivery::Deliveries,
    /// Messages read and not yet handled (Block 15).
    inbound: inbound::Inbound,
    /// Id of the connect command awaiting its reply.
    connect_command: Option<u32>,
    /// Connection token state and policy.
    tokens: Tokens,
    /// Encoding of the client protocol.
    codec: Codec,
    /// Transports to try, in order, and the one chosen (Block 8).
    transports: Transports,
    /// Session and node of the emulated connection, from its connect reply.
    emulation: Option<(String, String)>,
    /// Client-side subscriptions (Block 9).
    subscriptions: Subscriptions,
    /// Server-side subscriptions (Block 12).
    server_subscriptions: ServerSubscriptions,
    /// The connection is emulated and only its stream is proven: the backoff
    /// resets once the server pings or answers a command, not on the connect
    /// reply (centrifuge-js `_reconnectAttemptsResetPending`).
    backoff_reset_pending: bool,
    /// Pings announced by the server for the current connection.
    pings: Option<Pings>,
    actions: Vec<Action>,
}

impl Machine {
    /// Creates a disconnected machine whose owner has no token callback: an
    /// expiring token cannot be refreshed and ends the connection.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_rng(config, false, fastrand::Rng::new())
    }

    /// Uses `kinds`, tried in order, instead of a single websocket:
    /// [`Action::OpenTransport`] names the one to open by its index here.
    ///
    /// # Errors
    ///
    /// A [`ErrorKind::Configuration`] error when `kinds` is empty, when it
    /// holds an HTTP transport and the protocol is Protobuf, which they do
    /// not carry, or when the machine is not disconnected.
    pub fn with_transports(mut self, kinds: Vec<TransportKind>) -> Result<Self, ClientError> {
        if self.state != State::Disconnected || self.transport.is_some() {
            return Err(configuration_error(
                "transports can only be set while disconnected",
                None,
            ));
        }
        if kinds.is_empty() {
            return Err(configuration_error("no transport configured", None));
        }
        if self.config.protocol == Protocol::Protobuf
            && let Some(kind) = kinds.iter().find(|kind| kind.is_emulated())
        {
            return Err(configuration_error(
                &format!(
                    "the Protobuf protocol cannot be used with the {} transport",
                    kind.name()
                ),
                Some(*kind),
            ));
        }
        // A new list starts a new fallback sequence.
        self.transports = Transports::new(kinds);
        Ok(self)
    }

    /// Seeds the random source of the reconnect, jitter and refresh delays,
    /// so that a simulation or a test is reproducible. Only with the
    /// `test-util` feature, which the crate enables for its own tests and
    /// benchmarks.
    ///
    /// The random sequence a seed gives is not stable across versions.
    /// Production code must never pass the same seed to more than one
    /// client: their delays would coincide, defeating the jitter.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = fastrand::Rng::with_seed(seed);
        self
    }

    /// Creates a disconnected machine whose owner answers
    /// [`Action::FetchToken`] from the application's token callback.
    #[must_use]
    pub fn with_token_source(config: Config) -> Self {
        Self::with_rng(config, true, fastrand::Rng::new())
    }

    fn with_rng(config: Config, token_source: bool, rng: fastrand::Rng) -> Self {
        let backoff = Backoff::new(config.min_reconnect_delay, config.max_reconnect_delay);
        let tokens = Tokens::new(config.token.clone(), token_source);
        let codec = Codec::from(config.protocol);
        Self {
            config,
            state: State::Disconnected,
            backoff,
            rng,
            transport: None,
            transport_open: false,
            next_transport: 0,
            next_event: 0,
            deliveries: delivery::Deliveries::default(),
            inbound: inbound::Inbound::default(),
            next_command: 0,
            connect_command: None,
            tokens,
            codec,
            transports: Transports::new(vec![TransportKind::WebSocket]),
            emulation: None,
            subscriptions: Subscriptions::default(),
            server_subscriptions: ServerSubscriptions::default(),
            backoff_reset_pending: false,
            pings: None,
            actions: Vec::new(),
        }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// Reacts to `input`; the owner performs the returned actions in order.
    pub fn handle(&mut self, input: Input) -> Vec<Action> {
        match input {
            Input::Connect => self.connect(),
            Input::Disconnect => self.disconnect(
                codes::disconnected::DISCONNECT_CALLED,
                "disconnect called",
                false,
            ),
            Input::TransportOpened(id) if self.is_current(id) => self.transport_opened(),
            Input::TransportFailed { transport, message } if self.is_current(transport) => {
                self.transport_failed(message);
            }
            Input::TransportClosed {
                transport,
                code,
                reason,
            } if self.is_current(transport) => {
                // A server's close comes after the messages read before it.
                if self.holds_messages_of(transport) || self.events_hold_inbound() {
                    self.transport_closed_in_order(transport, code, reason);
                } else {
                    self.transport_closed(code, &reason);
                }
            }
            Input::Frame { transport, data } if self.is_current(transport) => {
                self.frame(transport, &data);
            }
            Input::SendRejected { transport, frame } if self.is_current(transport) => {
                self.send_rejected(&frame);
            }
            Input::TimerFired(Timer::Reconnect) => {
                if self.state == State::Connecting && self.transport.is_none() {
                    self.start_attempt();
                }
            }
            Input::TimerFired(Timer::ConnectTimeout) => {
                if self.state == State::Connecting && self.transport.is_some() {
                    self.transport_failed("connect timeout".to_owned());
                }
            }
            Input::TimerFired(Timer::Refresh) => {
                if self.state == State::Connected && !self.tokens.refresh_in_flight() {
                    self.refresh();
                }
            }
            Input::TimerFired(Timer::ServerPing) => {
                if self.state == State::Connected {
                    self.disconnect(codes::connecting::NO_PING, "no ping", true);
                }
            }
            Input::TimerFired(Timer::RefreshReply) => {
                if self.state == State::Connected && self.tokens.refresh_timed_out() {
                    // centrifuge-js: a command timeout is error code 1.
                    self.refresh_error(&Error {
                        code: codes::error::TIMEOUT,
                        message: "timeout".to_owned(),
                        temporary: false,
                    });
                }
            }
            Input::TokenFetched { fetch, result } => {
                if self.tokens.take_stale_fetch(fetch) {
                    self.stale_token_fetched(&result);
                } else if let Some(purpose) = self.tokens.take_fetch(fetch) {
                    self.token_fetched(purpose, result);
                }
            }

            Input::Subscribe(id) => self.subscribe(id),
            Input::Unsubscribe(id) => self.unsubscribe(id),
            Input::RemoveSubscription(id) => self.remove_subscription(id),
            Input::SubscriptionTimerFired(id, timer) => self.subscription_timer_fired(id, timer),
            Input::SubscriptionDataFetched { fetch, result } => {
                self.subscription_data_fetched(fetch, result);
            }
            Input::SubscriptionTokenFetched { fetch, result } => {
                self.subscription_token_fetched(fetch, result);
            }
            Input::SubscriptionStateFetched { fetch, result } => {
                self.subscription_state_fetched(fetch, result);
            }
            Input::EventTaken(id) => {
                self.event_taken(id);
                self.settle_event(id);
            }
            Input::StreamOpened(of) => self.stream_opened(of),
            Input::StreamClosed(of) => self.stream_closed(of),
            // About a transport already abandoned.
            Input::TransportOpened(_)
            | Input::TransportFailed { .. }
            | Input::TransportClosed { .. }
            | Input::Frame { .. }
            | Input::SendRejected { .. } => {}
        }
        // Taken or withdrawn events may let held messages follow; a
        // transport gone drops its own (Block 15).
        self.handle_inbound();
        std::mem::take(&mut self.actions)
    }

    fn is_current(&self, id: TransportId) -> bool {
        self.transport == Some(id)
    }

    fn connect(&mut self) {
        if self.state != State::Disconnected {
            return;
        }
        self.state = State::Connecting;
        self.emit(ClientEvent::Connecting {
            code: codes::connecting::CONNECT_CALLED,
            reason: "connect called".to_owned(),
        });
        // The first attempt is immediate; delays apply to reconnects.
        self.start_attempt();
    }

    fn start_attempt(&mut self) {
        match self.tokens.before_connect() {
            BeforeConnect::Ready => self.open_transport(),
            BeforeConnect::Fetch => self.fetch_token(FetchPurpose::Connect),
            BeforeConnect::NoSource => self.no_token_source(),
        }
    }

    fn open_transport(&mut self) {
        let id = TransportId(self.next_transport);
        self.next_transport += 1;
        self.transport = Some(id);
        self.transport_open = false;
        let kind = self.transport_kind();
        // An emulated stream is opened by a request carrying the connect
        // command (centrifuge-js `_initializeTransport`).
        let initial_frame = kind.is_emulated().then(|| {
            let command = self.connect_command();
            self.codec.encode(&[command])
        });
        self.actions.push(Action::OpenTransport {
            transport: id,
            kind,
            endpoint: self.transports.index(),
            initial_frame,
        });
        self.actions
            .push(Action::SetTimer(Timer::ConnectTimeout, self.config.timeout));
    }

    fn transport_opened(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        self.transport_open = true;
        if self.transport_kind().is_emulated() {
            // The connect command went with the request; only its reply
            // proves the transport (centrifuge-js `onOpen`).
            return;
        }
        // Open is not enough to keep a websocket: a server answer is (see
        // `server_answered`), so one that never answers falls back.
        let command = self.connect_command();
        self.send(transport, command);
    }

    /// A new connect command, now the one awaiting its reply.
    fn connect_command(&mut self) -> Command {
        let id = self.command_id();
        self.connect_command = Some(id);
        Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: self.tokens.for_connect(),
                name: self.config.name.clone(),
                version: self.config.version.clone(),
                subs: self.server_subscriptions_to_recover(),
            }),
        }
    }

    fn transport_kind(&self) -> TransportKind {
        self.transports.current()
    }

    /// The attempt failed before a connection was established.
    fn transport_failed(&mut self, message: String) {
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Transport,
            code: codes::error::TRANSPORT_CLOSED,
            message,
            transport: Some(self.transport_kind().name().to_owned()),
        }));
        self.reconnect_after_transport_closed();
    }

    fn transport_closed(&mut self, code: Option<u16>, reason: &str) {
        match code.map(u32::from) {
            // Codes from 3000 are disconnect codes set by the server.
            Some(code) if code >= 3000 => {
                self.server_answered();
                self.disconnect(code, reason, reconnect_advised(code));
            }
            _ if self.state == State::Connecting && !self.transport_open => {
                self.transport_failed("transport closed".to_owned());
            }
            _ => self.reconnect_after_transport_closed(),
        }
    }

    fn reconnect_after_transport_closed(&mut self) {
        self.disconnect(
            codes::connecting::TRANSPORT_CLOSED,
            "transport closed",
            true,
        );
    }

    fn reply(&mut self, reply: Reply) {
        // A reply to a command proves the connection usable (centrifuge-js
        // `_handleReply`); a connect reply sets the reset pending after this.
        if reply.id > 0 {
            self.reset_backoff_if_pending();
        }
        if reply.id == 0 {
            match reply.result {
                // Transports without close frames, such as http_stream and
                // sse, carry the server's disconnect as a push
                // (centrifuge-js `_handleDisconnect`).
                ReplyResult::Push(Push::Disconnect(Disconnect { code, reason })) => {
                    self.server_answered();
                    self.disconnect(code, &reason, reconnect_advised(code));
                }
                ReplyResult::Push(Push::Publication {
                    channel,
                    publication,
                }) => self.publication(&channel, publication),
                ReplyResult::Push(Push::Unsubscribe {
                    channel,
                    code,
                    reason,
                }) => self.server_unsubscribe(&channel, code, &reason),
                ReplyResult::Push(Push::Subscribe { channel, result }) => {
                    self.server_subscription_pushed(&channel, result);
                }
                // An empty reply is a server ping (client.proto, Reply).
                ReplyResult::Empty if self.state == State::Connected && reply.error.is_none() => {
                    // A ping proves the connection usable (centrifuge-js
                    // `_handleServerPing`).
                    self.reset_backoff_if_pending();
                    if self.pings.is_some_and(|pings| pings.pong) {
                        self.pong();
                    }
                }
                _ => {}
            }
            return;
        }
        self.command_reply(reply);
    }

    /// The server refused a frame as too large without closing the
    /// transport (HTTP 413 on an emulation request): each of its commands
    /// ends at once, and none is retried with the same payload (owner
    /// decision, 2026-11-04). A subscribe or `sub_refresh` unsubscribes, a
    /// refresh disconnects the client, any other command fails as if the
    /// server had answered it with a temporary error (an unsubscribe then
    /// reconnects). It proves nothing about the connection, so the backoff
    /// is not reset.
    fn send_rejected(&mut self, frame: &[u8]) {
        // Over an emulated transport the commands are inside the request.
        let unwrapped;
        let frame = if self.emulation.is_some() {
            let Some(inner) = json::emulation_frame(frame) else {
                return;
            };
            unwrapped = inner;
            &unwrapped
        } else {
            frame
        };
        for id in self.codec.command_ids(frame) {
            if id == 0 || self.subscription_command_too_large(id) {
                continue;
            }
            if self.state == State::Connected && self.tokens.take_refresh_reply(id) {
                self.actions.push(Action::CancelTimer(Timer::RefreshReply));
                self.disconnect(
                    codes::disconnected::MESSAGE_SIZE_LIMIT,
                    subscriptions::MESSAGE_SIZE_LIMIT_REASON,
                    false,
                );
                return;
            }
            self.command_reply(Reply {
                id,
                error: Some(Error {
                    code: codes::error::TRANSPORT_WRITE,
                    message: "request rejected by the server (HTTP 413)".to_owned(),
                    temporary: true,
                }),
                result: ReplyResult::Empty,
            });
        }
    }

    /// The reply to command `reply.id`, or its failure.
    fn command_reply(&mut self, reply: Reply) {
        if self.subscriptions.owns_command(reply.id) {
            self.subscription_command_reply(reply);
            return;
        }
        if self.state == State::Connected && self.tokens.take_refresh_reply(reply.id) {
            self.actions.push(Action::CancelTimer(Timer::RefreshReply));
            match (reply.error, reply.result) {
                (Some(error), _) => self.refresh_error(&error),
                (None, ReplyResult::Refresh(result)) => self.refreshed(&result),
                // A reply to refresh without its result breaks the protocol.
                (None, _) => self.reconnect_after_transport_closed(),
            }
            return;
        }
        if self.state != State::Connecting || self.connect_command != Some(reply.id) {
            return;
        }
        self.connect_command = None;
        match (reply.error, reply.result) {
            (Some(error), _) => self.connect_error(&error),
            (None, ReplyResult::Connect(result)) => self.connected(result),
            // A reply to connect without its result breaks the protocol.
            (None, _) => self.reconnect_after_transport_closed(),
        }
    }

    fn connected(&mut self, mut result: ConnectResult) {
        self.actions
            .push(Action::CancelTimer(Timer::ConnectTimeout));
        self.transports.choose();
        if self.transport_kind().is_emulated() {
            self.emulation = Some((result.session.clone(), result.node.clone()));
        }
        if self.transport_kind().is_emulated() {
            // A connect reply over an emulated transport proves only the
            // stream; commands go through the emulation endpoint, which may
            // keep failing (centrifuge-js `_connectResponse`).
            self.backoff_reset_pending = true;
        } else {
            self.backoff.reset();
        }
        self.tokens.connected();
        if result.expires {
            self.set_refresh_timer(result.ttl);
        }
        self.state = State::Connected;
        self.pings = (result.ping > 0).then(|| Pings {
            interval: Duration::from_secs(result.ping.into()),
            pong: result.pong,
        });
        self.wait_server_ping();
        self.emit(ClientEvent::Connected {
            client_id: std::mem::take(&mut result.client),
            transport: self.transport_kind().name().to_owned(),
        });
        self.subscriptions_connected();
        self.server_subscriptions_connected(std::mem::take(&mut result.subs));
    }

    /// The server answered on the transport being tried: an error reply to
    /// the connect, a `Disconnect` push or, over websocket, a close code from
    /// 3000 (the connect reply chooses it too, in `connected`). The transport
    /// is kept then, and only then: one that opens but never answers falls
    /// back (Block 8; owner decision Q8; review items C2 and C3;
    /// centrifuge-js 63ed73e `_transportWasOpen`).
    fn server_answered(&mut self) {
        self.transports.choose();
    }

    /// centrifuge-js `_connectError`: temporary errors are retried.
    fn connect_error(&mut self, error: &Error) {
        self.server_answered();
        if error.code == TOKEN_EXPIRED {
            self.tokens.connect_token_expired();
        }
        if error.code < 100 || error.temporary || error.code == TOKEN_EXPIRED {
            self.emit(ClientEvent::Error(ClientError {
                kind: ErrorKind::Connect,
                code: error.code,
                message: error.message.clone(),
                transport: None,
            }));
            self.disconnect(error.code, &error.message, true);
        } else {
            self.disconnect(error.code, &error.message, false);
        }
    }

    /// centrifuge-js `_disconnect`: tears the connection down, then moves to
    /// connecting (`reconnect`) or disconnected. An event is emitted only on
    /// an actual state change.
    fn disconnect(&mut self, code: u32, reason: &str, reconnect: bool) {
        // "State invalidated" (Block 11): the connection token and every
        // subscription token, fixed ones too, are dropped and every stream
        // position is made unmatchable, whether a push or a close frame
        // carries the code (centrifuge-js `_disconnect`, which keeps fixed
        // tokens).
        if code == STATE_INVALIDATED {
            self.tokens.invalidate();
            self.invalidate_subscriptions();
            self.invalidate_server_subscriptions();
        }
        if self.state == State::Disconnected {
            return;
        }
        let was_connected = self.state == State::Connected;
        if let Some(transport) = self.transport.take() {
            self.actions.push(Action::CloseTransport(transport));
            if reconnect {
                self.transports.attempt_failed();
            }
        }
        self.transport_open = false;
        self.emulation = None;
        // A connection that ends before it proved usable keeps the backoff
        // growing.
        self.backoff_reset_pending = false;
        self.connect_command = None;
        self.tokens.teardown(reconnect);
        // Every timer belongs to the connection being torn down; the
        // reconnect timer is set again below when reconnecting.
        for timer in Timer::ALL {
            self.actions.push(Action::CancelTimer(timer));
        }
        self.subscriptions_disconnected();
        self.server_subscriptions_disconnected();
        self.pings = None;

        let state = if reconnect {
            State::Connecting
        } else {
            State::Disconnected
        };
        if self.state != state {
            self.state = state;
            let reason = reason.to_owned();
            self.emit(if reconnect {
                ClientEvent::Connecting { code, reason }
            } else {
                ClientEvent::Disconnected { code, reason }
            });
        }
        if reconnect {
            // The first round over the transports is the handshake: each is
            // tried at once (centrifuge-js `_scheduleReconnect`).
            let mut delay = if self.transports.in_first_round() {
                Duration::ZERO
            } else {
                self.backoff.next_delay(&mut self.rng)
            };
            if was_connected {
                delay += self.reconnect_jitter();
            }
            self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
        }
    }

    /// A random delay added to the first reconnect after a lost connection,
    /// so that a fleet disconnected at once does not reconnect at once
    /// (Block 14, "Reconnect jitter window"); none with a zero window.
    fn reconnect_jitter(&mut self) -> Duration {
        let window =
            u64::try_from(self.config.reconnect_jitter_window.as_millis()).unwrap_or(u64::MAX);
        if window == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis(self.rng.u64(0..=window))
    }

    fn fetch_token(&mut self, purpose: FetchPurpose) {
        let id = self.tokens.start_fetch(purpose);
        self.actions.push(Action::FetchToken(id));
    }

    /// The refresh timer fired while connected.
    fn refresh(&mut self) {
        if self.tokens.has_source() {
            self.fetch_token(FetchPurpose::Refresh);
        } else {
            self.no_token_source();
        }
    }

    /// centrifuge-js `_getToken` without `getToken`: a configuration error,
    /// then the client stops as unauthorized.
    fn no_token_source(&mut self) {
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: "token expired but no token callback is set".to_owned(),
            transport: None,
        }));
        self.unauthorized();
    }

    /// The answer to a refresh fetch of a connection that is gone: a denial
    /// still disconnects (spec: the client "will be disconnected"), a token
    /// is not used (owner decision, 2026-11-04; centrifuge-js `_refresh`),
    /// a failure is dropped. The new connection refreshes on its own.
    fn stale_token_fetched(&mut self, result: &Result<String, TokenError>) {
        if matches!(result, Ok(token) if token.is_empty())
            || matches!(result, Err(TokenError::Unauthorized))
        {
            self.unauthorized();
        }
    }

    fn token_fetched(&mut self, purpose: FetchPurpose, result: Result<String, TokenError>) {
        match result {
            // An empty token denies access too, as in centrifuge-js.
            Ok(token) if token.is_empty() => self.unauthorized(),
            Err(TokenError::Unauthorized) => self.unauthorized(),
            Ok(token) => {
                self.tokens.store(token, purpose);
                match purpose {
                    FetchPurpose::Connect => {
                        if self.state == State::Connecting && self.transport.is_none() {
                            self.open_transport();
                        }
                    }
                    FetchPurpose::Refresh if self.state == State::Connected => self.send_refresh(),
                    FetchPurpose::Refresh => {}
                }
            }
            Err(TokenError::Failed(message)) => match purpose {
                FetchPurpose::Connect => {
                    self.emit(ClientEvent::Error(ClientError {
                        kind: ErrorKind::ConnectToken,
                        code: codes::error::CONNECT_TOKEN,
                        message,
                        transport: None,
                    }));
                    let delay = self.backoff.next_delay(&mut self.rng);
                    self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
                }
                // Reconnecting: the next connection gets its own refresh timer.
                FetchPurpose::Refresh if self.state != State::Connected => {}
                FetchPurpose::Refresh => {
                    self.emit(ClientEvent::Error(ClientError {
                        kind: ErrorKind::RefreshToken,
                        code: codes::error::REFRESH_TOKEN,
                        message,
                        transport: None,
                    }));
                    self.retry_refresh();
                }
            },
        }
    }

    fn send_refresh(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        // The reply re-arms it; a timer armed meanwhile, e.g. by a reconnect
        // while the token was being fetched, must not start another refresh.
        self.actions.push(Action::CancelTimer(Timer::Refresh));
        let id = self.command_id();
        let command = Command {
            id,
            method: Method::Refresh(RefreshRequest {
                token: self.tokens.for_refresh(id),
            }),
        };
        self.send(transport, command);
        self.actions
            .push(Action::SetTimer(Timer::RefreshReply, self.config.timeout));
    }

    fn refreshed(&mut self, result: &RefreshResult) {
        if result.expires {
            self.set_refresh_timer(result.ttl);
        }
    }

    /// centrifuge-js `_refreshError`: temporary errors are retried.
    fn refresh_error(&mut self, error: &Error) {
        if error.code < 100 || error.temporary {
            self.emit(ClientEvent::Error(ClientError {
                kind: ErrorKind::Refresh,
                code: error.code,
                message: error.message.clone(),
                transport: None,
            }));
            self.retry_refresh();
        } else {
            self.disconnect(error.code, &error.message, false);
        }
    }

    fn retry_refresh(&mut self) {
        let delay = backoff::delay(0, REFRESH_RETRY_MIN, REFRESH_RETRY_MAX, &mut self.rng);
        self.actions.push(Action::SetTimer(Timer::Refresh, delay));
    }

    fn set_refresh_timer(&mut self, ttl_seconds: u32) {
        let delay = refresh_delay(ttl_seconds, &mut self.rng);
        self.actions.push(Action::SetTimer(Timer::Refresh, delay));
    }

    /// centrifuge-js `_waitServerPing`: (re)arms the deadline for data from a
    /// server that announced pings; disabled by a zero `max_server_ping_delay`.
    fn wait_server_ping(&mut self) {
        let Some(pings) = self.pings else {
            return;
        };
        // Paused while a full read-ahead stops the reading (Block 15).
        if self.reading_stopped() {
            return;
        }
        if self.state != State::Connected || self.config.max_server_ping_delay.is_zero() {
            return;
        }
        let deadline = pings
            .interval
            .saturating_add(self.config.max_server_ping_delay);
        self.actions
            .push(Action::SetTimer(Timer::ServerPing, deadline));
    }

    fn pong(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        self.send(
            transport,
            Command {
                id: 0,
                method: Method::Pong,
            },
        );
    }

    fn reset_backoff_if_pending(&mut self) {
        if std::mem::take(&mut self.backoff_reset_pending) {
            self.backoff.reset();
        }
    }

    /// Sends `command` alone in its frame: over an emulated transport each
    /// command then has its own request, so a request the server refuses
    /// (HTTP 413) concerns that command only (see `send_rejected`).
    fn send(&mut self, transport: TransportId, command: Command) {
        let mut frame = self.codec.encode(&[command]);
        if let Some((session, node)) = &self.emulation {
            frame = json::emulation_request(session, node, &frame);
        }
        self.actions.push(Action::Send {
            transport,
            frame,
            frame_type: self.codec.frame_type(),
        });
    }

    fn unauthorized(&mut self) {
        self.disconnect(codes::disconnected::UNAUTHORIZED, "unauthorized", false);
    }

    fn command_id(&mut self) -> u32 {
        self.next_command = self.next_command.wrapping_add(1).max(1);
        self.next_command
    }

    fn emit(&mut self, event: ClientEvent) {
        let error = matches!(event, ClientEvent::Error(_));
        if let Some(id) = self.admit_event(None, error) {
            self.actions.push(Action::Emit(id, event));
        }
    }

    /// Id of the next event emitted.
    fn event_id(&mut self) -> EventId {
        let id = EventId(self.next_event);
        self.next_event += 1;
        id
    }
}

/// Spec, "Disconnect codes": 3000-3499 and 4000-4499 advise a reconnect,
/// 3500-3999 and 4500-4999 are terminal. Codes from 5000 are reserved by
/// Centrifuge and reconnect (`client.proto`, `Disconnect`; owner decision).
fn reconnect_advised(code: u32) -> bool {
    !matches!(code, 3500..=3999 | 4500..=4999)
}

/// A [`ErrorKind::Configuration`] error about options that cannot work.
fn configuration_error(message: &str, transport: Option<TransportKind>) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: transport.map(|kind| kind.name().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine() -> Machine {
        Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(7))
    }

    fn opened_transport(actions: &[Action]) -> TransportId {
        actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("an OpenTransport action")
    }

    fn events(actions: &[Action]) -> Vec<&ClientEvent> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Emit(_, event) => Some(event),
                _ => None,
            })
            .collect()
    }

    /// A machine whose connect command was sent over `transport`.
    fn awaiting_connect_reply() -> (Machine, TransportId) {
        let mut machine = machine();
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        (machine, transport)
    }

    fn frame(transport: TransportId, json: &str) -> Input {
        Input::Frame {
            transport,
            data: json.as_bytes().to_vec(),
        }
    }

    #[test]
    fn each_failed_attempt_emits_exactly_one_error_and_schedules_a_retry() {
        let mut machine = machine();
        let mut transport = opened_transport(&machine.handle(Input::Connect));

        for _ in 0..3 {
            let actions = machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            });

            let events = events(&actions);
            assert_eq!(events.len(), 1, "{actions:?}");
            assert!(matches!(events[0], ClientEvent::Error(e) if e.kind == ErrorKind::Transport));
            assert!(matches!(
                actions.last(),
                Some(Action::SetTimer(Timer::Reconnect, _))
            ));
            transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        }
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn new_transports_start_a_new_fallback_sequence() {
        let three = vec![
            TransportKind::WebSocket,
            TransportKind::HttpStream,
            TransportKind::Sse,
        ];
        let mut machine = machine().with_transports(three).unwrap();
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportFailed {
            transport,
            message: "refused".into(),
        });
        machine.handle(Input::Disconnect);

        let mut machine = machine
            .with_transports(vec![TransportKind::HttpStream])
            .unwrap();
        let actions = machine.handle(Input::Connect);

        assert!(
            actions.iter().any(|action| matches!(
                action,
                Action::OpenTransport {
                    kind: TransportKind::HttpStream,
                    endpoint: 0,
                    ..
                }
            )),
            "{actions:?}"
        );
    }

    /// Connects an `http_stream`-only machine, then closes the stream;
    /// the reconnect delay, after `ping` if set.
    fn emulated_reconnect_delay(machine: &mut Machine, ping: bool) -> Duration {
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        let connect_id = machine.connect_command.expect("connect command pending");
        machine.handle(frame(
            transport,
            &format!(r#"{{"id":{connect_id},"connect":{{"client":"c"}}}}"#),
        ));
        if ping {
            machine.handle(frame(transport, "{}"));
        }
        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        machine.handle(Input::Disconnect);
        actions
            .iter()
            .find_map(|action| match action {
                Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
                _ => None,
            })
            .expect("a reconnect")
    }

    // Brief C3: a server answer keeps the websocket being tried (the
    // answers centrifuge-js keeps a transport on, brief C2); without one,
    // the scenarios show it falls back.
    #[test]
    fn a_websocket_the_server_answered_is_kept_for_the_next_attempt() {
        type Answer = fn(TransportId) -> Input;
        let answers: [(&str, Answer); 3] = [
            ("an error reply", |transport| {
                frame(
                    transport,
                    r#"{"id":1,"error":{"code":100,"message":"busy","temporary":true}}"#,
                )
            }),
            ("a close code from 3000", |transport| {
                Input::TransportClosed {
                    transport,
                    code: Some(3001),
                    reason: "shutdown".into(),
                }
            }),
            ("a disconnect push", |transport| {
                frame(
                    transport,
                    r#"{"push":{"disconnect":{"code":3001,"reason":"shutdown"}}}"#,
                )
            }),
        ];
        for (name, answer) in answers {
            let mut machine = Machine::new(Config::default())
                .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
                .unwrap();
            let transport = opened_transport(&machine.handle(Input::Connect));
            machine.handle(Input::TransportOpened(transport));

            let mut actions = machine.handle(answer(transport));
            if !actions
                .iter()
                .any(|action| matches!(action, Action::OpenTransport { .. }))
            {
                actions = machine.handle(Input::TimerFired(Timer::Reconnect));
            }

            let kind = actions.iter().find_map(|action| match action {
                Action::OpenTransport { kind, .. } => Some(*kind),
                _ => None,
            });
            assert_eq!(
                kind,
                Some(TransportKind::WebSocket),
                "after {name}: {actions:?}"
            );
        }
    }

    #[test]
    fn an_emulated_connect_reply_alone_does_not_reset_the_backoff() {
        let mut machine = machine()
            .with_transports(vec![TransportKind::HttpStream])
            .unwrap();

        let delays: Vec<_> = (0..8)
            .map(|_| emulated_reconnect_delay(&mut machine, false))
            .collect();
        let after_ping = emulated_reconnect_delay(&mut machine, true);

        // Attempt 0 is bounded by twice the minimum; later ones grow.
        let first_bound = Config::default().min_reconnect_delay * 2;
        assert!(
            delays.iter().any(|delay| *delay > first_bound),
            "{delays:?}"
        );
        assert!(after_ping <= first_bound, "{after_ping:?}");
    }

    #[test]
    fn transports_cannot_change_while_connecting() {
        let mut machine = machine();
        machine.handle(Input::Connect);

        let result = machine.with_transports(vec![TransportKind::HttpStream]);

        assert!(
            matches!(&result, Err(error) if error.kind == ErrorKind::Configuration),
            "{:?}",
            result.err()
        );
    }

    #[test]
    fn a_disconnect_push_disconnects_with_its_code() {
        for (code, state) in [(4000, State::Connecting), (4500, State::Disconnected)] {
            let (mut machine, transport) = awaiting_connect_reply();
            machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

            let actions = machine.handle(frame(
                transport,
                &format!(r#"{{"push":{{"disconnect":{{"code":{code},"reason":"bye"}}}}}}"#),
            ));

            assert_eq!(machine.state(), state);
            assert!(
                events(&actions).iter().any(|event| matches!(
                    event,
                    ClientEvent::Connecting { code: got, reason }
                        | ClientEvent::Disconnected { code: got, reason }
                        if *got == code && reason == "bye"
                )),
                "{actions:?}"
            );
        }
    }

    #[test]
    fn connect_timeout_fails_the_attempt() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(Input::TimerFired(Timer::ConnectTimeout));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError { message, .. })] if message == "connect timeout"
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn inputs_about_an_abandoned_transport_are_ignored() {
        let (mut machine, stale) = awaiting_connect_reply();
        machine.handle(Input::TimerFired(Timer::ConnectTimeout));
        opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));

        let actions = machine.handle(frame(stale, r#"{"id":1,"connect":{"client":"c"}}"#));

        assert_eq!(actions, []);
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn temporary_connect_error_is_reported_and_retried() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":400,"message":"busy","temporary":true}}"#,
        ));

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::Connect,
                code: 400,
                ..
            })]
        ));
        assert_eq!(machine.state(), State::Connecting);
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
    }

    #[test]
    fn permanent_connect_error_disconnects_with_its_code() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":403,"message":"forbidden"}}"#,
        ));

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: 403,
                reason: "forbidden".into()
            }]
        );
        assert_eq!(machine.state(), State::Disconnected);
        assert!(!actions.iter().any(|a| matches!(a, Action::SetTimer(..))));
    }

    #[test]
    fn undecodable_frame_closes_the_transport_and_reconnects() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(transport, "<html>captive portal</html>"));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn transport_closed_while_connected_reconnects_with_transport_closed_code() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::TRANSPORT_CLOSED,
                reason: "transport closed".into()
            }]
        );
    }

    #[test]
    fn reconnect_advice_follows_the_spec_ranges() {
        let advised: Vec<_> = [3000, 3499, 3500, 3999, 4000, 4499, 4500, 4999, 5000, 6000]
            .into_iter()
            .map(|code| (code, reconnect_advised(code)))
            .collect();

        assert_eq!(
            advised,
            [
                (3000, true),
                (3499, true),
                (3500, false),
                (3999, false),
                (4000, true),
                (4499, true),
                (4500, false),
                (4999, false),
                (5000, true),
                (6000, true),
            ]
        );
    }

    #[test]
    fn terminal_server_code_while_connected_disconnects_without_retry() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: Some(3500),
            reason: "bye".into(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: 3500,
                reason: "bye".into()
            }]
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::SetTimer(..))));
    }

    #[test]
    fn close_codes_below_3000_are_transport_closures() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: Some(1006),
            reason: "abnormal".into(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::TRANSPORT_CLOSED,
                reason: "transport closed".into()
            }]
        );
    }

    fn token_machine(token: &str) -> Machine {
        Machine::with_rng(
            Config {
                token: token.to_owned(),
                ..Config::default()
            },
            true,
            fastrand::Rng::with_seed(7),
        )
    }

    fn fetch_requested(actions: &[Action]) -> FetchId {
        actions
            .iter()
            .find_map(|action| match action {
                Action::FetchToken(id) => Some(*id),
                _ => None,
            })
            .expect("a FetchToken action")
    }

    /// A token machine connected over the returned transport; the connect
    /// reply carried `expires`/`ttl`.
    fn connected_with_token(ttl: u32) -> (Machine, TransportId, Vec<Action>) {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        let actions = machine.handle(frame(
            transport,
            &format!(r#"{{"id":1,"connect":{{"client":"c","expires":true,"ttl":{ttl}}}}}"#),
        ));
        (machine, transport, actions)
    }

    fn refresh_timer(actions: &[Action]) -> Option<Duration> {
        actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::Refresh, delay) => Some(*delay),
            _ => None,
        })
    }

    #[test]
    fn refresh_falls_between_80_and_90_percent_of_the_ttl() {
        let mut rng = fastrand::Rng::with_seed(7);
        for ttl in [0, 1, 3, 30, 300, 3600] {
            let delays: Vec<_> = (0..1000).map(|_| refresh_delay(ttl, &mut rng)).collect();

            let ttl = Duration::from_secs(ttl.into());
            assert!(
                delays
                    .iter()
                    .all(|delay| *delay >= ttl * 8 / 10 && *delay <= ttl * 9 / 10),
                "{ttl:?}: {delays:?}"
            );
            if !ttl.is_zero() {
                assert!(delays.iter().any(|delay| *delay != delays[0]), "{ttl:?}");
            }
        }
    }

    #[test]
    fn expiring_connection_arms_the_refresh_timer_before_its_ttl() {
        let (_, _, actions) = connected_with_token(30);

        let delay = refresh_timer(&actions).expect("a refresh timer");
        assert!(
            delay >= Duration::from_secs(24) && delay <= Duration::from_secs(27),
            "{delay:?}"
        );
    }

    #[test]
    fn refresh_sends_the_fetched_token_and_rearms_on_reply() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

        let sent = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        let rearmed = machine.handle(frame(
            transport,
            r#"{"id":2,"refresh":{"expires":true,"ttl":60}}"#,
        ));

        assert_eq!(
            sent,
            [
                Action::CancelTimer(Timer::Refresh),
                Action::Send {
                    transport,
                    frame: br#"{"id":2,"refresh":{"token":"t1"}}"#.to_vec(),
                    frame_type: FrameType::Text,
                },
                Action::SetTimer(Timer::RefreshReply, Config::default().timeout)
            ]
        );
        let delay = refresh_timer(&rearmed).expect("a refresh timer");
        assert!(
            delay >= Duration::from_secs(48) && delay <= Duration::from_secs(54),
            "{delay:?}"
        );
    }

    #[test]
    fn unanswered_refresh_times_out_and_is_retried() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        let sent = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        assert!(sent.contains(&Action::SetTimer(
            Timer::RefreshReply,
            Config::default().timeout
        )));

        let actions = machine.handle(Input::TimerFired(Timer::RefreshReply));
        let late = machine.handle(frame(
            transport,
            r#"{"id":2,"refresh":{"expires":true,"ttl":60}}"#,
        ));

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::Refresh,
                code: codes::error::TIMEOUT,
                ..
            })]
        ));
        assert!(refresh_timer(&actions).is_some(), "{actions:?}");
        assert_eq!(late, [], "a reply after the timeout is ignored");
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn denial_arriving_after_the_transport_dropped_still_disconnects() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Unauthorized),
        });

        assert_eq!(machine.state(), State::Disconnected, "{actions:?}");
        assert!(events(&actions).contains(&&ClientEvent::Disconnected {
            code: codes::disconnected::UNAUTHORIZED,
            reason: "unauthorized".into()
        }));
    }

    #[test]
    fn debug_output_of_send_actions_shows_only_the_frame_length() {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TransportOpened(transport));

        let Action::Send { frame, .. } = &actions[0] else {
            panic!("{actions:?}");
        };
        assert_eq!(
            format!("{:?}", actions[0]),
            format!(
                "Send {{ transport: {transport:?}, frame: <{} bytes>, frame_type: Text }}",
                frame.len()
            )
        );
    }

    #[test]
    fn debug_output_never_shows_subscription_tokens() {
        let mut machine = Machine::new(Config::default());
        let options = SubscriptionOptions::default()
            .token("sub-secret".into())
            .token_source();
        machine
            .add_subscription("news".into(), options.clone())
            .unwrap();
        let fetched = Input::SubscriptionTokenFetched {
            fetch: FetchId(0),
            result: Ok("sub-fetched".into()),
        };

        let printed = format!("{options:?} {machine:?} {fetched:?}");

        assert!(!printed.contains("sub-secret"), "{printed}");
        assert!(!printed.contains("sub-fetched"), "{printed}");
    }

    #[test]
    fn debug_output_of_inputs_never_shows_fetched_tokens() {
        let input = Input::TokenFetched {
            fetch: FetchId(0),
            result: Ok("secret-token".into()),
        };

        let printed = format!("{input:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    // A refresh token fetched for a connection that is gone is not used
    // (owner decision, 2026-11-04; centrifuge-js `_refresh`); a denial
    // still disconnects (see the test above).
    #[test]
    fn late_refresh_token_after_reconnect_is_discarded() {
        let (mut machine, transport, _) = connected_with_token(30);
        let stale = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TransportOpened(transport));
        machine.handle(frame(
            transport,
            r#"{"id":2,"connect":{"client":"c2","expires":true,"ttl":30}}"#,
        ));

        // The new connection refreshes on its own schedule, without waiting
        // for the fetch of the old one.
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        let refreshed = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t2".into()),
        });
        let late = machine.handle(Input::TokenFetched {
            fetch: stale,
            result: Ok("t1".into()),
        });

        assert!(
            refreshed.iter().any(|action| matches!(
                action,
                Action::Send { frame, .. } if frame == br#"{"id":3,"refresh":{"token":"t2"}}"#
            )),
            "{refreshed:?}"
        );
        assert_eq!(late, [], "the late token is not sent");
    }

    // Owner decision, 2026-11-04: a refresh refused as too large would be
    // refused again, so the client stops.
    #[test]
    fn a_refresh_rejected_as_too_large_disconnects() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        let refreshing = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        let Some(Action::Send { frame, .. }) = refreshing
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{refreshing:?}");
        };

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame.clone(),
        });

        assert_eq!(machine.state(), State::Disconnected, "{rejected:?}");
        assert!(events(&rejected).contains(&&ClientEvent::Disconnected {
            code: codes::disconnected::MESSAGE_SIZE_LIMIT,
            reason: "message size limit exceeded".into()
        }));
    }

    #[test]
    fn late_refresh_token_while_reconnecting_is_not_used_for_the_connect() {
        let (mut machine, transport, _) = connected_with_token(30);
        let stale = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        machine.handle(Input::TokenFetched {
            fetch: stale,
            result: Ok("t1".into()),
        });

        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        let connecting = machine.handle(Input::TransportOpened(transport));

        let Some(Action::Send { frame, .. }) = connecting
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{connecting:?}");
        };
        let frame = String::from_utf8(frame.clone()).unwrap();
        assert!(frame.contains(r#""token":"t0""#), "{frame}");
    }

    #[test]
    fn debug_output_never_shows_tokens() {
        let (machine, _, _) = connected_with_token(30);
        let config = Config {
            token: "secret-token".into(),
            ..Config::default()
        };

        let printed = format!("{config:?} {machine:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
        assert!(!printed.contains("\"t0\""), "{printed}");
    }

    #[test]
    fn denied_or_empty_token_disconnects_as_unauthorized() {
        for result in [Err(TokenError::Unauthorized), Ok(String::new())] {
            let (mut machine, _, _) = connected_with_token(30);
            let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

            let actions = machine.handle(Input::TokenFetched { fetch, result });

            assert_eq!(
                events(&actions),
                [&ClientEvent::Disconnected {
                    code: codes::disconnected::UNAUTHORIZED,
                    reason: "unauthorized".into()
                }]
            );
        }
    }

    #[test]
    fn failed_refresh_fetch_is_reported_and_retried_in_5_to_10_s() {
        let (mut machine, _, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Failed("backend down".into())),
        });

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::RefreshToken,
                code: 6,
                ..
            })]
        ));
        let retry = refresh_timer(&actions).expect("retry timer");
        assert!(
            (REFRESH_RETRY_MIN..=REFRESH_RETRY_MAX).contains(&retry),
            "{retry:?}"
        );
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn refresh_reply_errors_retry_when_temporary_and_disconnect_otherwise() {
        for (error, expect_disconnect) in [
            (r#"{"code":400,"message":"busy","temporary":true}"#, false),
            (r#"{"code":403,"message":"forbidden"}"#, true),
        ] {
            let (mut machine, transport, _) = connected_with_token(30);
            let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
            machine.handle(Input::TokenFetched {
                fetch,
                result: Ok("t1".into()),
            });

            let actions =
                machine.handle(frame(transport, &format!(r#"{{"id":2,"error":{error}}}"#)));

            if expect_disconnect {
                assert_eq!(machine.state(), State::Disconnected, "{actions:?}");
            } else {
                assert!(refresh_timer(&actions).is_some(), "{actions:?}");
                assert_eq!(machine.state(), State::Connected);
            }
        }
    }

    #[test]
    fn expiring_token_without_callback_is_a_configuration_error_then_unauthorized() {
        let mut machine = awaiting_connect_reply().0;
        let transport = TransportId(0);
        machine.handle(frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":30}}"#,
        ));

        let actions = machine.handle(Input::TimerFired(Timer::Refresh));

        assert!(matches!(
            events(&actions)[..],
            [
                ClientEvent::Error(ClientError {
                    kind: ErrorKind::Configuration,
                    code: 12,
                    ..
                }),
                ClientEvent::Disconnected { code: 1, .. }
            ]
        ));
    }

    #[test]
    fn without_a_token_the_first_attempt_fetches_one() {
        let mut machine = token_machine("");

        let fetch = fetch_requested(&machine.handle(Input::Connect));
        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        let transport = opened_transport(&actions);
        let sent = machine.handle(Input::TransportOpened(transport));

        assert!(
            matches!(&sent[..], [Action::Send { frame, .. }] if frame.windows(4).any(|w| w == b"\"t1\"")),
            "{sent:?}"
        );
    }

    #[test]
    fn expired_token_error_makes_the_next_attempt_fetch_a_new_token() {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":109,"message":"token expired"}}"#,
        ));

        let actions = machine.handle(Input::TimerFired(Timer::Reconnect));

        fetch_requested(&actions);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::OpenTransport { .. }))
        );
    }

    #[test]
    fn failed_connect_fetch_is_reported_and_retried_with_backoff() {
        let mut machine = token_machine("");
        let fetch = fetch_requested(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Failed("backend down".into())),
        });

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::ConnectToken,
                code: 5,
                ..
            })]
        ));
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn a_state_invalidated_disconnect_abandons_a_refresh_fetch() {
        let mut machine = token_machine("t");
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        machine.handle(frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":60}}"#,
        ));
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: Some(3014),
            reason: "state invalidated".into(),
        });

        // An answer for the invalidated state, before the reconnect.
        machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("stale".into()),
        });
        let reconnecting = machine.handle(Input::TimerFired(Timer::Reconnect));

        assert!(
            reconnecting
                .iter()
                .any(|action| matches!(action, Action::FetchToken(_))),
            "{reconnecting:?}"
        );
    }

    #[test]
    fn token_fetched_after_disconnect_is_ignored() {
        let mut machine = token_machine("");
        let fetch = fetch_requested(&machine.handle(Input::Connect));
        machine.handle(Input::Disconnect);

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });

        assert_eq!(actions, []);
    }

    /// A machine connected to a server announcing `ping` seconds; returns
    /// the actions of the connect reply.
    fn connected_with_ping(ping: u32, max_delay: Duration) -> (Machine, TransportId, Vec<Action>) {
        let mut machine = Machine::with_rng(
            Config {
                max_server_ping_delay: max_delay,
                ..Config::default()
            },
            false,
            fastrand::Rng::with_seed(7),
        );
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        let actions = machine.handle(frame(
            transport,
            &format!(r#"{{"id":1,"connect":{{"client":"c","ping":{ping},"pong":true}}}}"#),
        ));
        (machine, transport, actions)
    }

    fn server_ping_timer(actions: &[Action]) -> Option<Duration> {
        actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::ServerPing, delay) => Some(*delay),
            _ => None,
        })
    }

    #[test]
    fn ping_deadline_is_interval_plus_max_delay_and_off_without_pings_or_delay() {
        let (_, _, with_pings) = connected_with_ping(25, Duration::from_secs(10));
        let (_, _, no_pings) = connected_with_ping(0, Duration::from_secs(10));
        let (_, _, check_off) = connected_with_ping(25, Duration::ZERO);

        assert_eq!(
            server_ping_timer(&with_pings),
            Some(Duration::from_secs(35))
        );
        assert_eq!(server_ping_timer(&no_pings), None);
        assert_eq!(server_ping_timer(&check_off), None);
    }

    #[test]
    fn any_frame_from_the_server_rearms_the_ping_deadline() {
        let (mut machine, transport, _) = connected_with_ping(25, Duration::from_secs(10));

        let actions = machine.handle(frame(transport, r#"{"push":{"channel":"c","pub":{}}}"#));

        assert_eq!(server_ping_timer(&actions), Some(Duration::from_secs(35)));
    }

    #[test]
    fn blank_frames_do_not_rearm_the_ping_deadline() {
        let (mut machine, transport, _) = connected_with_ping(25, Duration::from_secs(10));

        // E.g. keep-alive newlines written by an intermediary.
        let actions = machine.handle(frame(transport, "\n"));

        assert_eq!(server_ping_timer(&actions), None, "{actions:?}");
    }

    #[test]
    fn missing_ping_reconnects_with_no_ping_code() {
        let (mut machine, transport, _) = connected_with_ping(25, Duration::from_secs(10));

        let actions = machine.handle(Input::TimerFired(Timer::ServerPing));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::NO_PING,
                reason: "no ping".into()
            }]
        );
    }

    #[test]
    fn timer_all_lists_every_kind_at_its_index() {
        let indices: Vec<_> = Timer::ALL.iter().map(|timer| timer.index()).collect();

        assert_eq!(indices, (0..Timer::COUNT).collect::<Vec<_>>());
    }

    #[test]
    fn disconnect_cancels_every_timer() {
        let (mut machine, _) = awaiting_connect_reply();

        let actions = machine.handle(Input::Disconnect);

        for timer in Timer::ALL {
            assert!(
                actions.contains(&Action::CancelTimer(timer)),
                "{timer:?}: {actions:?}"
            );
        }
    }

    #[test]
    fn connect_while_connecting_does_nothing() {
        let mut machine = machine();
        machine.handle(Input::Connect);

        assert_eq!(machine.handle(Input::Connect), []);
    }

    #[test]
    fn disconnect_while_disconnected_does_nothing() {
        assert_eq!(machine().handle(Input::Disconnect), []);
    }

    #[test]
    fn successful_connect_resets_the_backoff() {
        let mut machine = Machine::with_rng(
            Config {
                min_reconnect_delay: Duration::from_millis(100),
                max_reconnect_delay: Duration::from_secs(60),
                ..Config::default()
            },
            false,
            fastrand::Rng::with_seed(7),
        );
        let mut transport = opened_transport(&machine.handle(Input::Connect));
        for _ in 0..10 {
            machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            });
            transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        }
        machine.handle(Input::TransportOpened(transport));
        let id = machine.connect_command.expect("connect sent");
        machine.handle(frame(
            transport,
            &format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#),
        ));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let Some(Action::SetTimer(Timer::Reconnect, delay)) = actions.last() else {
            panic!("no reconnect timer in {actions:?}");
        };
        assert!(*delay <= Duration::from_millis(200), "{delay:?}");
    }
}
