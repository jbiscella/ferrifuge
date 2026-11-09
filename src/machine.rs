//! Sans-IO core: the client connection state machine.
//!
//! A [`Machine`] owns no sockets, timers or tasks. Its owner feeds it
//! [`Input`]s and performs the returned [`Action`]s in order. The tokio
//! adapter is one such owner; any other runtime can drive it the same way.
//!
//! Behaviour follows the Client SDK spec, with centrifuge-js as the
//! implementation reference (`src/centrifuge.ts`).

use std::collections::BTreeMap;
use std::time::Duration;

mod batching;
#[cfg(test)]
mod buffer_tests;
mod calls;
#[cfg(any(adapter_tokio, adapter_browser))]
pub(crate) use calls::CallWait;
#[cfg(test)]
mod close_tests;
#[cfg(test)]
mod compaction_tests;
mod debounce;
mod delivery;
mod inbound;
#[cfg(test)]
mod inbox_overflow_tests;
#[cfg(test)]
mod network_events_tests;
#[cfg(test)]
mod optimistic_tests;
#[cfg(test)]
mod publication_channel_tests;
mod server_subscriptions;
#[cfg(test)]
mod statistics_tests;
mod subscriptions;
mod tokens;
mod transports;
#[cfg(test)]
mod unsupported_tests;

use tokens::{
    BeforeConnect, FetchPurpose, REFRESH_RETRY_MAX, REFRESH_RETRY_MIN, TOKEN_EXPIRED, Tokens,
    refresh_delay,
};

use crate::backoff::{self, Backoff};
use crate::codec::Codec;
use crate::config::Protocol;
use crate::token::Secret;
use crate::wire::{
    Command, ConnectRequest, ConnectResult, Disconnect, Error, Method, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult,
};
use crate::{
    CallError, CallReply, ClientError, ClientEvent, Config, DataError, ErrorKind, State,
    StateError, StreamPosition, SubscriptionEvent, TokenError, codes,
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
    /// No event: for a handler failure that has none, as an events thread
    /// that could not start (Block 41, D2).
    #[cfg(any(adapter_tokio, adapter_browser))]
    pub(crate) const NONE: Self = Self(u64::MAX);

    /// An id for the adapter's unit tests.
    #[cfg(all(test, any(adapter_tokio, adapter_browser)))]
    pub(crate) fn for_tests(n: u64) -> Self {
        Self(n)
    }
}

/// Identifies a frame of [`Action::Send`], for its owner to report with
/// [`Input::FrameWritten`] once the transport has written it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FrameId(u64);

impl FrameId {
    /// An id for the adapter's unit tests.
    #[cfg(all(test, any(adapter_tokio, adapter_browser)))]
    pub(crate) fn for_tests(n: u64) -> Self {
        Self(n)
    }
}

/// Identifies the timer of one debounce window (Block 46, D5): the owner
/// runs it for the [`Action::SetDebounceTimer`] and reports it with
/// [`Input::DebounceTimerFired`]. A machine never reuses a number, so a
/// firing for a window already gone is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DebounceTimer(u64);

/// Identifies a call the machine settles with [`Action::CallSettled`];
/// [`Machine::add_call`] gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CallId(u64);

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
    /// Asks the server for join and leave pushes, on channels that have
    /// them (spec, "ask server to push Join/Leave").
    pub join_leave: bool,
    /// Asks the server for publications as deltas against the previous one,
    /// on channels that allow it; the client rebuilds them (Block 19).
    pub delta: Option<crate::DeltaFormat>,
    /// Asks the server to send only the publications this filter matches
    /// (Block 35a); refused together with `delta`.
    pub tag_filter: Option<crate::TagFilter>,
    /// Asks a server with channel compaction (Centrifugo PRO) to name this
    /// subscription by a numeric id instead of its channel in publication,
    /// join and leave pushes (Block 35b).
    pub channel_compaction: bool,
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
            .field("join_leave", &self.join_leave)
            .field("delta", &self.delta)
            .field("tag_filter", &self.tag_filter)
            .field("channel_compaction", &self.channel_compaction)
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
            join_leave: false,
            delta: None,
            tag_filter: None,
            channel_compaction: false,
        }
    }
}

/// Options of a map subscription in the machine (Block 45, D3): its kind,
/// its load's page size and memory bound, what an unrecoverable position
/// does, and the options it shares with a stream subscription.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[cfg(feature = "unstable")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MapSubscriptionOptions {
    /// The map's kind, sent as the subscribe command's `type`.
    pub kind: crate::MapKind,
    /// Entries asked for per page; 0 leaves it to the server, which clamps
    /// it to the namespace's `map.min_page_size` and `map.max_page_size`
    /// (100 and 1000 by default) and uses `map.default_page_size` (100) for 0.
    pub page_size: u32,
    /// Bytes of page replies one load may read before the subscription ends
    /// with `unsubscribed` 5 "map state too large" (Block 45, D5); 64 MiB by
    /// default.
    pub max_state_bytes: std::num::NonZeroUsize,
    /// What an unrecoverable position does (Block 45, D6).
    pub unrecoverable_strategy: crate::UnrecoverableStrategy,
    /// The options a map subscription shares with a stream subscription:
    /// `data`, the token and its callback, the resubscribe delays, `delta`,
    /// `tag_filter` and `channel_compaction`. The others (`data_source`,
    /// `since`, `state_source`, `positioned`, `recoverable`, `join_leave`)
    /// do not apply to map subscriptions and are ignored (Block 45, out of
    /// scope).
    pub common: SubscriptionOptions,
}

/// Block 45, D5: 64 MiB, Block 19 D6's inbound limit.
#[cfg(feature = "unstable")]
const DEFAULT_MAX_STATE_BYTES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(64 * 1024 * 1024).expect("not zero");

#[cfg(feature = "unstable")]
impl MapSubscriptionOptions {
    /// Options of a map subscription of `kind`, every other option at its
    /// default.
    #[must_use]
    pub fn new(kind: crate::MapKind) -> Self {
        Self {
            kind,
            page_size: 0,
            max_state_bytes: DEFAULT_MAX_STATE_BYTES,
            unrecoverable_strategy: crate::UnrecoverableStrategy::default(),
            common: SubscriptionOptions::default(),
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

    /// Asks the server for join and leave pushes.
    #[must_use]
    pub fn join_leave(mut self, join_leave: bool) -> Self {
        self.join_leave = join_leave;
        self
    }

    /// Asks for publications as deltas in `format` (Block 19).
    #[must_use]
    pub fn delta(mut self, format: crate::DeltaFormat) -> Self {
        self.delta = Some(format);
        self
    }

    /// Sends `filter` with every subscribe command (Block 35a).
    #[must_use]
    pub fn tag_filter(mut self, filter: crate::TagFilter) -> Self {
        self.tag_filter = Some(filter);
        self
    }

    /// Asks for channel compaction on every subscribe command (Block 35b).
    #[must_use]
    pub fn channel_compaction(mut self, channel_compaction: bool) -> Self {
        self.channel_compaction = channel_compaction;
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

/// Timers the machine sets for a call, one per phase: a timer of the phase
/// the call has left is ignored when it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CallTimer {
    /// Deadline for a subscription-level call's subscription to be
    /// subscribed, while the call waits for it.
    Subscription,
    /// Deadline for the connection, while the call waits for it.
    Connection,
    /// Deadline for room in the write queue, while the call waits for it
    /// (`Config::write_queue_bytes`).
    Room,
    /// Deadline for the reply, once the call is sent.
    Reply,
}

/// Timers the machine sets. At most one of each kind runs at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timer {
    /// Delay before the next connection attempt.
    Reconnect,
    /// Deadline for a connection attempt: for its transport to open, after
    /// `Config::handshake_timeout`, then for its connect reply, after
    /// `Config::timeout` (Block 29, D1).
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
/// Its `Debug` output redacts a fetched token and shows fetched data as
/// its length.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Input {
    /// The application called `connect()`.
    Connect,
    /// The application called `reconnect()`, e.g. on its platform's wake or
    /// network-change notification: the client connects again at once,
    /// from the first transport (Block 25, D5, D6).
    Reconnect,
    /// The application called `disconnect()`.
    Disconnect,
    /// The application called `close()`, or dropped the client (Block 33):
    /// every call fails with code 4 "client closed", every subscription
    /// is unsubscribed with code 2 "client closed", then the client
    /// disconnects for good. Nothing is fed after it.
    Close,
    /// The owner's task performing the actions ended before the client was
    /// closed: its runtime shut down, or, when `panicked`, the task panicked
    /// (Block 28, D3, D4). The client disconnects for good.
    DriverDropped {
        /// Whether the task ended by a panic.
        panicked: bool,
    },
    /// The transport opened.
    TransportOpened(TransportId),
    /// The owner cannot open the transport the core asked for, as what it
    /// needs is missing (Block 44, D12: a browser without its constructors,
    /// as centrifuge-js 63ed73e `_initializeTransport` skips a transport
    /// whose `supported()` is false): the core skips it in the round,
    /// without an `error` event or a failed attempt, and asks for the next
    /// one. When no transport of the round can be opened, the attempt
    /// fails with "no supported transport found".
    TransportUnsupported(TransportId),
    /// The transport could not be opened.
    TransportFailed {
        /// The transport that failed.
        transport: TransportId,
        /// Why, for the error event.
        message: String,
    },
    /// The transport received a message larger than
    /// `Config::max_message_bytes` and was ended (Block 29, D3). The owner
    /// refuses the message where its bytes are read, holding at most the
    /// limit and one read, and reports it with this input instead of
    /// [`Input::TransportClosed`]. Before the connect reply the attempt
    /// fails; after it the client disconnects with code 3 "message size
    /// limit exceeded", without reconnecting.
    MessageTooLarge(TransportId),
    /// A message arrived while the transport's inbox was full, and the
    /// owner closed the transport (Block 44, D7): a transport that does
    /// not stop reading, such as every browser transport, holds what the
    /// core does not read yet in an inbox bounded by the read-ahead's
    /// limits. The client reconnects at once with code
    /// [`codes::connecting::READ_AHEAD_OVERFLOW`], a client-side reason:
    /// events not taken are withdrawn, and recovery brings them back. Before
    /// a server ping is read on the connection, the backoff keeps growing,
    /// as after a 3008 close (Block 15).
    InboxOverflow(TransportId),
    /// The device went offline, as the browser's `offline` event tells
    /// (Block 44, D10). Connected: the transport closes, and the client
    /// emits `connecting` with code 1 "transport closed" and reconnects
    /// after the reconnect delay, as after a lost connection. Connecting:
    /// the attempt in flight, if any, closes, with no event and counting as
    /// no failed attempt, and the next waits the reconnect delay.
    /// Disconnected: nothing.
    Offline,
    /// The page entered the back/forward cache, as the browser's `pagehide`
    /// with `persisted` tells (Block 44, D11): as [`Input::Offline`], with
    /// the reason "page entered back/forward cache".
    PageCached,
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
    /// The transport wrote the frame of [`Action::Send`] `frame`: a
    /// websocket flushed it to the socket, an emulated transport's request
    /// carrying it was answered (HTTP 413 included). Owners may report
    /// every frame; the machine counts only the application's calls
    /// against [`Config::write_queue_bytes`].
    FrameWritten {
        /// The transport it was sent on.
        transport: TransportId,
        /// The frame.
        frame: FrameId,
    },
    /// A timer set by [`Action::SetTimer`] fired.
    TimerFired(Timer),
    /// A timer set by [`Action::SetDebounceTimer`] fired (Block 46): the
    /// window sends the latest value it holds, if any, and opens anew, or
    /// closes. One whose window is gone is ignored.
    DebounceTimerFired(DebounceTimer),
    /// The token callback answered a [`Action::FetchToken`].
    TokenFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The new token, or why there is none.
        result: Result<String, TokenError>,
    },
    /// The application started a batch (`start_batching`, Block 18): its
    /// calls, but `send`, and the subscribe and unsubscribe commands its
    /// calls cause are held until a batch stops.
    StartBatching,
    /// The application stopped a batch: what all open batches hold is sent.
    StopBatching,
    /// The owner of a machine made with
    /// [`Machine::with_automatic_batching`] has fed the inputs it has: the
    /// commands buffered since the last flush are sent, in as few frames as
    /// [`Config::batch_frame_bytes`] allows (Block 36). Without automatic
    /// batching it does nothing.
    Flush,
    /// The application set the connection token used from the next
    /// connection attempt (`set_token`, Block 17); empty for none.
    SetToken(String),
    /// The application set the connect data used from the next connection
    /// attempt, unless a connect data callback replaces it (`set_data`).
    SetData(Vec<u8>),
    /// The application set a subscription's data, used in every subscribe
    /// command built from now on, unless its data callback replaces it
    /// (`Subscription::set_data`, Block 32). Ignored for an id not in the
    /// registry.
    SetSubscriptionData(SubscriptionId, Vec<u8>),
    /// The application set a subscription's tag filter, `None` to remove
    /// it, for the subscribe commands built from now on
    /// (`Subscription::set_tag_filter`, Block 35a). The owner checks it with
    /// [`Machine::check_subscription_tag_filter`] first.
    SetSubscriptionTagFilter(SubscriptionId, Option<crate::TagFilter>),
    /// The application set the emulated headers sent from the next
    /// connection attempt (`set_emulated_headers`).
    SetEmulatedHeaders(BTreeMap<String, String>),
    /// The connect data callback answered a [`Action::FetchConnectData`].
    ConnectDataFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The data of the next connect command, or why there is none.
        result: Result<Vec<u8>, DataError>,
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
    /// [`Action::EmitSubscription`] from its stream, or the handler it was
    /// handed to returned (Block 41, D3).
    EventTaken(EventId),
    /// The owner handed an event of [`Action::Emit`] or
    /// [`Action::EmitSubscription`] to a handler, which has not returned
    /// (Block 41, D3): the event moves its stream position now, so that a
    /// command sent meanwhile recovers from after it, is not withdrawn,
    /// and holds the messages after it until [`Input::EventTaken`].
    EventHanded(EventId),
    /// The handler an event was handed to panicked (Block 41, D4): the
    /// event's position moves back, and the client disconnects with code 2
    /// "exception in event handler: `message`" and does not reconnect.
    HandlerPanicked {
        /// The event the handler was given.
        event: EventId,
        /// The panic's message.
        message: String,
    },
    /// A stream takes the events of a subscription, or of the client when
    /// `None` (server-side subscriptions' included), from now on. Until
    /// then, and after [`Input::StreamClosed`], their events count as taken
    /// once emitted (Block 15).
    StreamOpened(Option<SubscriptionId>),
    /// No stream takes the events of a subscription, or of the client when
    /// `None`, anymore: report the events it had not taken with
    /// [`Input::EventTaken`] first.
    StreamClosed(Option<SubscriptionId>),
    /// The application made the call [`Machine::add_call`] added: it is
    /// sent when the client is connected, waiting for the connection
    /// until then.
    Call(CallId),
    /// A timer set by [`Action::SetCallTimer`] fired.
    CallTimerFired(CallId, CallTimer),
    /// The application no longer awaits the call, e.g. its future was
    /// dropped: it is forgotten, and never sent if it was waiting.
    CallAbandoned(CallId),
    /// The system slept for `gap`: the wall clock moved that much further
    /// than the monotonic one, whose timers resume with the time they had
    /// left (Block 25, D1). A gap under 2 s is ignored.
    Slept {
        /// How long the system slept.
        gap: Duration,
    },
}

/// Something the owner of the machine must do.
///
/// Its `Debug` output shows only the length of a [`Action::Send`] frame:
/// connect and refresh frames carry the connection token.
///
/// Non-exhaustive since 0.21.0 (Block 45, D3): a variant added under a
/// feature, as `Action::EmitMapSubscription` under `unstable`, would
/// otherwise break the exhaustive `match` of any crate in a graph where
/// another crate enables that feature.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Send a frame over the transport; report it with
    /// [`Input::FrameWritten`] once written.
    Send {
        /// The transport to send on.
        transport: TransportId,
        /// Identifies the frame in [`Input::FrameWritten`].
        id: FrameId,
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
    /// Ask the application's connect data callback for the data of the next
    /// connect command; report the answer with [`Input::ConnectDataFetched`].
    /// Only issued by a machine with [`Machine::with_data_source`].
    FetchConnectData(FetchId),
    /// Stop a [`Action::FetchConnectData`] still running: its answer is no
    /// longer wanted.
    CancelConnectData(FetchId),
    /// Start a subscription's timer, replacing a running one of the kind.
    SetSubscriptionTimer(SubscriptionId, SubscriptionTimer, Duration),
    /// Stop a subscription's timer if it runs.
    CancelSubscriptionTimer(SubscriptionId, SubscriptionTimer),
    /// Deliver an event to a subscription's listeners; report it with
    /// [`Input::EventTaken`] once the application has taken it.
    EmitSubscription(EventId, SubscriptionId, SubscriptionEvent),
    /// Deliver an event to a map subscription's listeners (Block 45, D3);
    /// report it with [`Input::EventTaken`] once the application has taken
    /// it.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    EmitMapSubscription(EventId, SubscriptionId, crate::MapSubscriptionEvent),
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
    /// Start a debounce window's timer (Block 46, D5), which the machine
    /// never sets twice; report its end with
    /// [`Input::DebounceTimerFired`].
    SetDebounceTimer(DebounceTimer, Duration),
    /// Stop a debounce window's timer if it runs.
    CancelDebounceTimer(DebounceTimer),
    /// Start a call's timer, replacing its running one.
    SetCallTimer(CallId, CallTimer, Duration),
    /// Stop a call's timer if it runs.
    CancelCallTimer(CallId, CallTimer),
    /// Give the application the outcome of a call; the call is forgotten.
    CallSettled {
        /// The call settled.
        call: CallId,
        /// What the server returned, or why the call failed.
        result: Result<CallReply, CallError>,
    },
    /// The server broke the client protocol in a way only the machine sees:
    /// the actions that follow reconnect, with code 1 "transport closed",
    /// as for a lost transport (Block 21). For the owner's logs; nothing
    /// needs performing.
    Anomaly(Anomaly),
    /// Make the timer fire `by` earlier, at once if that time has passed;
    /// nothing when it is not running (Block 25, D3: a refresh due during a
    /// sleep).
    ShortenTimer(Timer, Duration),
    /// As [`Action::ShortenTimer`], for a subscription's timer.
    ShortenSubscriptionTimer(SubscriptionId, SubscriptionTimer, Duration),
}

/// A breach of the client protocol by the server, reported with
/// [`Action::Anomaly`]. It carries no payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Anomaly {
    /// A frame that is not client protocol data, e.g. from a misconfigured
    /// proxy.
    UndecodableFrame {
        /// The transport it came on.
        transport: TransportId,
        /// Its length.
        bytes: usize,
    },
    /// A reply without the result of the command it answers, and without
    /// an error.
    ReplyWithoutResult {
        /// The transport it came on.
        transport: TransportId,
        /// The kind of command it answers, as `client.proto` names its
        /// request: `"connect"`, `"rpc"`, `"sub_refresh"`...
        command: &'static str,
    },
    /// A reply handled before the connect reply of its connection, which
    /// the commands sent with the connect command must follow (Block 36,
    /// D6).
    ReplyBeforeConnect {
        /// The transport it came on.
        transport: TransportId,
        /// The kind of command it answers, as `client.proto` names its
        /// request: `"subscribe"`.
        command: &'static str,
    },
    /// A reply to a map subscription's request, or a publication of a map
    /// subscription, that the protocol does not allow (Block 45, D15).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    UnexpectedMapReply {
        /// The transport it came on.
        transport: TransportId,
        /// What is wrong with it.
        fault: &'static str,
    },
}

impl Input {
    /// The `Debug` output of a setter input: the token redacted, the data
    /// as its length, the headers by name.
    fn fmt_setter(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetToken(token) => f
                .debug_tuple("SetToken")
                .field(&Secret(token.clone()))
                .finish(),
            Self::SetData(data) => f
                .debug_tuple("SetData")
                .field(&format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::SetSubscriptionData(id, data) => f
                .debug_tuple("SetSubscriptionData")
                .field(id)
                .field(&format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::SetSubscriptionTagFilter(id, filter) => f
                .debug_tuple("SetSubscriptionTagFilter")
                .field(id)
                .field(filter)
                .finish(),
            Self::SetEmulatedHeaders(headers) => f
                .debug_tuple("SetEmulatedHeaders")
                .field(&headers.keys().collect::<Vec<_>>())
                .finish(),
            _ => unreachable!("not a setter input"),
        }
    }
}

impl std::fmt::Debug for Input {
    #[allow(clippy::too_many_lines)] // One arm per variant.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect => f.write_str("Connect"),
            Self::Reconnect => f.write_str("Reconnect"),
            Self::StartBatching => f.write_str("StartBatching"),
            Self::StopBatching => f.write_str("StopBatching"),
            Self::Flush => f.write_str("Flush"),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::Close => f.write_str("Close"),
            Self::DriverDropped { panicked } => f
                .debug_struct("DriverDropped")
                .field("panicked", panicked)
                .finish(),
            Self::TransportOpened(id) => f.debug_tuple("TransportOpened").field(id).finish(),
            Self::TransportUnsupported(id) => {
                f.debug_tuple("TransportUnsupported").field(id).finish()
            }
            Self::TransportFailed { transport, message } => f
                .debug_struct("TransportFailed")
                .field("transport", transport)
                .field("message", message)
                .finish(),
            Self::MessageTooLarge(id) => f.debug_tuple("MessageTooLarge").field(id).finish(),
            Self::InboxOverflow(id) => f.debug_tuple("InboxOverflow").field(id).finish(),
            Self::Offline => f.write_str("Offline"),
            Self::PageCached => f.write_str("PageCached"),
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
            Self::FrameWritten { transport, frame } => f
                .debug_struct("FrameWritten")
                .field("transport", transport)
                .field("frame", frame)
                .finish(),
            Self::TimerFired(timer) => f.debug_tuple("TimerFired").field(timer).finish(),
            Self::DebounceTimerFired(timer) => {
                f.debug_tuple("DebounceTimerFired").field(timer).finish()
            }
            Self::TokenFetched { fetch, result } => f
                .debug_struct("TokenFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result.as_ref().map(|token| Secret(token.clone())),
                )
                .finish(),
            Self::SetToken(_)
            | Self::SetData(_)
            | Self::SetEmulatedHeaders(_)
            | Self::SetSubscriptionData(..)
            | Self::SetSubscriptionTagFilter(..) => self.fmt_setter(f),
            Self::ConnectDataFetched { fetch, result } => f
                .debug_struct("ConnectDataFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result
                        .as_ref()
                        .map(|data| format!("<{} bytes>", data.len())),
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
            Self::EventHanded(id) => f.debug_tuple("EventHanded").field(id).finish(),
            // The message only by its length: the application's text.
            Self::HandlerPanicked { event, message } => f
                .debug_struct("HandlerPanicked")
                .field("event", event)
                .field("message", &format_args!("<{} bytes>", message.len()))
                .finish(),
            Self::StreamOpened(of) => f.debug_tuple("StreamOpened").field(of).finish(),
            Self::StreamClosed(of) => f.debug_tuple("StreamClosed").field(of).finish(),
            Self::Call(id) => f.debug_tuple("Call").field(id).finish(),
            Self::CallTimerFired(id, timer) => f
                .debug_tuple("CallTimerFired")
                .field(id)
                .field(timer)
                .finish(),
            Self::CallAbandoned(id) => f.debug_tuple("CallAbandoned").field(id).finish(),
            Self::Slept { gap } => f.debug_struct("Slept").field("gap", gap).finish(),
        }
    }
}

impl std::fmt::Debug for Action {
    #[allow(clippy::too_many_lines)] // One arm per variant.
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
                id,
                frame,
                frame_type,
            } => f
                .debug_struct("Send")
                .field("transport", transport)
                .field("id", id)
                .field("frame", &format_args!("<{} bytes>", frame.len()))
                .field("frame_type", frame_type)
                .finish(),
            Self::SetTimer(timer, delay) => {
                f.debug_tuple("SetTimer").field(timer).field(delay).finish()
            }
            Self::CancelTimer(timer) => f.debug_tuple("CancelTimer").field(timer).finish(),
            Self::Emit(id, event) => f.debug_tuple("Emit").field(id).field(event).finish(),
            Self::FetchToken(id) => f.debug_tuple("FetchToken").field(id).finish(),
            Self::FetchConnectData(id) => f.debug_tuple("FetchConnectData").field(id).finish(),
            Self::CancelConnectData(id) => f.debug_tuple("CancelConnectData").field(id).finish(),
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
            #[cfg(feature = "unstable")]
            Self::EmitMapSubscription(event_id, id, event) => f
                .debug_tuple("EmitMapSubscription")
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
            Self::SetDebounceTimer(timer, delay) => f
                .debug_tuple("SetDebounceTimer")
                .field(timer)
                .field(delay)
                .finish(),
            Self::CancelDebounceTimer(timer) => {
                f.debug_tuple("CancelDebounceTimer").field(timer).finish()
            }
            Self::SetCallTimer(id, timer, delay) => f
                .debug_tuple("SetCallTimer")
                .field(id)
                .field(timer)
                .field(delay)
                .finish(),
            Self::CancelCallTimer(id, timer) => f
                .debug_tuple("CancelCallTimer")
                .field(id)
                .field(timer)
                .finish(),
            Self::CallSettled { call, result } => f
                .debug_struct("CallSettled")
                .field("call", call)
                .field("result", result)
                .finish(),
            Self::Anomaly(anomaly) => f.debug_tuple("Anomaly").field(anomaly).finish(),
            Self::ShortenTimer(timer, by) => f
                .debug_tuple("ShortenTimer")
                .field(timer)
                .field(by)
                .finish(),
            Self::ShortenSubscriptionTimer(id, timer, by) => f
                .debug_tuple("ShortenSubscriptionTimer")
                .field(id)
                .field(timer)
                .field(by)
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

/// The smallest gap between the wall and monotonic clocks taken for a
/// system sleep (Block 25, D1).
const SLEEP_THRESHOLD: Duration = Duration::from_secs(2);

/// The reason of the `unsubscribed` events of a closed client (Block 33).
const CLIENT_CLOSED_REASON: &str = "client closed";

/// Websocket close code "message too big" (RFC 6455).
const MESSAGE_TOO_BIG: u32 = 1009;

/// The error event's message for a message over `Config::max_message_bytes`
/// before the connect reply (Block 29, D3).
const MESSAGE_TOO_LARGE: &str = "message larger than the size limit";

/// The reason of [`codes::connecting::READ_AHEAD_OVERFLOW`] (Block 44, D7).
const READ_AHEAD_OVERFLOW_REASON: &str = "read-ahead overflow";
/// The reason of `Input::PageCached` (Block 44, D11).
const PAGE_CACHED_REASON: &str = "page entered back/forward cache";

/// Disconnect code by which the server invalidates the connection's state
/// (centrifuge-js `disconnectedCodes.stateInvalidated`).
const STATE_INVALIDATED: u32 = 3014;
/// The connect data callback's state (Block 17). Its `Debug` output shows
/// the data as its length.
#[derive(Default)]
struct ConnectData {
    /// Whether the owner answers [`Action::FetchConnectData`].
    source: bool,
    /// The fetch awaited, if any.
    fetch: Option<FetchId>,
    /// The data the callback gave the current attempt.
    attempt: Option<Vec<u8>>,
}

impl std::fmt::Debug for ConnectData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectData")
            .field("source", &self.source)
            .field("fetch", &self.fetch)
            .field(
                "attempt",
                &self
                    .attempt
                    .as_ref()
                    .map(|data| format!("<{} bytes>", data.len())),
            )
            .finish()
    }
}

/// Emulated headers, by name. Their `Debug` output shows the names only:
/// a value may be a secret.
#[derive(Default)]
struct EmulatedHeaders(BTreeMap<String, String>);

impl std::fmt::Debug for EmulatedHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.keys()).finish()
    }
}

/// Disconnect code of a client the server found too slow (Centrifugo
/// `DisconnectSlow`).
const SLOW: u32 = 3008;

/// The client connection state machine.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)] // Independent facts of the connection.
pub struct Machine {
    config: Config,
    state: State,
    /// Connection and traffic counters (Block 31).
    counters: crate::statistics::Counters,
    backoff: Backoff,
    rng: fastrand::Rng,
    /// The transport opening or open, if any.
    transport: Option<TransportId>,
    transport_open: bool,
    next_transport: u64,
    next_command: u32,
    next_event: u64,
    next_frame: u64,
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
    /// Transports of the current attempt its owner could not open (Block
    /// 44, D12).
    unsupported: usize,
    /// Session and node of the emulated connection, from its connect reply.
    emulation: Option<(String, String)>,
    /// Client-side subscriptions (Block 9).
    subscriptions: Subscriptions,
    /// Server-side subscriptions (Block 12).
    server_subscriptions: ServerSubscriptions,
    /// Calls not yet settled (Block 13).
    calls: calls::Calls,
    /// The connection is emulated and only its stream is proven: the backoff
    /// resets once the server pings or answers a command, not on the connect
    /// reply (centrifuge-js `_reconnectAttemptsResetPending`).
    backoff_reset_pending: bool,
    /// The client lost its connection and its first attempt since has not
    /// started: a reconnect then waits the jitter window (Block 44, D10,
    /// amending Block 25 D5).
    reconnecting_after_loss: bool,
    /// A server ping was read on the current connection: a 3008 close then
    /// keeps the backoff reset (Block 15, "Backoff after 3008").
    pinged: bool,
    /// Pings announced by the server for the current connection.
    pings: Option<Pings>,
    /// The connect data callback's state (Block 17).
    connect_data: ConnectData,
    /// Emulated headers sent with every connect command (Block 17).
    emulated_headers: EmulatedHeaders,
    /// Open batches and what they hold (Block 18).
    batching: batching::Batching,
    /// Debounce windows of the subscriptions' publishes (Block 46).
    debouncing: debounce::Debouncing,
    actions: Vec<Action>,
}

impl Machine {
    /// Creates a disconnected machine whose owner has no token callback: an
    /// expiring token cannot be refreshed and ends the connection.
    ///
    /// On `wasm32-unknown-unknown` without the `browser` feature, the random
    /// source of its delays (backoff, refresh jitter) starts from a
    /// constant, as fastrand without its `js` feature has no entropy there:
    /// every machine of a page, and the n-th machine of every page, draws
    /// the same delays (Block 40, D4). `browser` seeds it from the
    /// browser's `crypto.getRandomValues` (Block 44, D5).
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
        // Centrifugo's sse carries JSON only; Protobuf goes over websocket
        // and http_stream (Block 27, D3).
        if self.config.protocol == Protocol::Protobuf
            && let Some(kind) = kinds.iter().find(|kind| **kind == TransportKind::Sse)
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
    /// `__test-hooks` feature, which the crate enables for its own tests and
    /// benchmarks.
    ///
    /// The random sequence a seed gives is not stable across versions.
    /// Production code must never pass the same seed to more than one
    /// client: their delays would coincide, defeating the jitter.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = fastrand::Rng::with_seed(seed);
        self
    }

    /// Creates a disconnected machine whose owner answers
    /// [`Action::FetchToken`] from the application's token callback.
    ///
    /// On `wasm32-unknown-unknown` without the `browser` feature, the random
    /// source of its delays (backoff, refresh jitter) starts from a
    /// constant, as fastrand without its `js` feature has no entropy there:
    /// every machine of a page, and the n-th machine of every page, draws
    /// the same delays (Block 40, D4). `browser` seeds it from the
    /// browser's `crypto.getRandomValues` (Block 44, D5).
    #[must_use]
    pub fn with_token_source(config: Config) -> Self {
        Self::with_rng(config, true, fastrand::Rng::new())
    }

    /// Makes the machine buffer every command it would send, until the
    /// owner feeds [`Input::Flush`], which sends them in as few frames as
    /// [`Config::batch_frame_bytes`] allows: the owner feeds it once it has
    /// fed the inputs it has, before performing the actions (Block 36,
    /// D2). An owner that never feeds it holds every command until its
    /// deadline. The tokio adapter always does this.
    #[must_use]
    pub fn with_automatic_batching(mut self) -> Self {
        self.batching.automatic = true;
        self
    }

    /// Honours the `publish_debounce` the server advises in its subscribe
    /// replies (Block 46, D5): the owner runs [`Action::SetDebounceTimer`]
    /// timers and reports them with [`Input::DebounceTimerFired`]. Without
    /// it, every publish is sent: an owner written before this block would
    /// never end a window, and its callers would hang. The tokio and
    /// browser clients always do this.
    #[must_use]
    pub fn with_publish_debounce(mut self) -> Self {
        self.debouncing.enabled = true;
        self
    }

    /// Makes the owner answer [`Action::FetchConnectData`] from the
    /// application's connect data callback, asked before every connection
    /// attempt once a token is obtained; its data replaces
    /// [`Config::data`] (Block 17).
    #[must_use]
    pub fn with_data_source(mut self) -> Self {
        self.connect_data.source = true;
        self
    }

    /// Sends `headers`, by name, with every connect command as
    /// `ConnectRequest.headers`: the emulated headers the server forwards to
    /// its proxies (Block 17).
    #[must_use]
    pub fn with_emulated_headers(mut self, headers: BTreeMap<String, String>) -> Self {
        self.emulated_headers = EmulatedHeaders(headers);
        self
    }

    fn with_rng(config: Config, token_source: bool, rng: fastrand::Rng) -> Self {
        let backoff = Backoff::new(config.min_reconnect_delay, config.max_reconnect_delay);
        let tokens = Tokens::new(config.token.clone(), token_source);
        let codec = Codec::from(config.protocol);
        Self {
            config,
            state: State::Disconnected,
            counters: crate::statistics::Counters::default(),
            backoff,
            rng,
            transport: None,
            transport_open: false,
            next_transport: 0,
            next_event: 0,
            next_frame: 0,
            deliveries: delivery::Deliveries::default(),
            inbound: inbound::Inbound::default(),
            next_command: 0,
            connect_command: None,
            tokens,
            codec,
            transports: Transports::new(vec![TransportKind::WebSocket]),
            unsupported: 0,
            emulation: None,
            subscriptions: Subscriptions::default(),
            server_subscriptions: ServerSubscriptions::default(),
            calls: calls::Calls::default(),
            backoff_reset_pending: false,
            reconnecting_after_loss: false,
            pinged: false,
            pings: None,
            connect_data: ConnectData::default(),
            emulated_headers: EmulatedHeaders::default(),
            batching: batching::Batching::default(),
            debouncing: debounce::Debouncing::default(),
            actions: Vec::new(),
        }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// The connection and traffic counters since the machine was built
    /// (Block 31); `connected_since` is `None`, the core having no clock.
    #[must_use]
    pub fn statistics(&self) -> crate::Statistics {
        crate::Statistics {
            state: self.state,
            transport: (self.state == State::Connected).then(|| self.transport_kind()),
            connected_since: None,
            connects: self.counters.connects,
            failed_attempts: self.counters.failed_attempts,
            bytes_in: self.counters.bytes_in,
            bytes_out: self.counters.bytes_out,
            messages_in: self.counters.messages_in,
            messages_out: self.counters.messages_out,
            publications_delivered: self.counters.publications_delivered,
            subscriptions: self.subscription_count(),
            server_subscriptions: self.server_subscription_count(),
        }
    }

    /// Reacts to `input`; the owner performs the returned actions in order.
    #[allow(clippy::too_many_lines)] // One arm per variant.
    pub fn handle(&mut self, input: Input) -> Vec<Action> {
        self.batching.application_unsubscribe =
            matches!(input, Input::Unsubscribe(_) | Input::RemoveSubscription(_));
        match input {
            Input::Connect => self.connect(),
            Input::Reconnect => self.reconnect(),
            Input::StartBatching => self.start_batching(),
            Input::StopBatching => self.stop_batching(),
            Input::Flush => {
                if self.buffering() {
                    self.flush();
                }
            }
            Input::Disconnect => self.disconnect_called(),
            Input::Close => self.close(codes::disconnected::DISCONNECT_CALLED, "disconnect called"),
            Input::DriverDropped { panicked } => self.driver_dropped(panicked),
            Input::TransportOpened(id) if self.is_current(id) => self.transport_opened(),
            Input::TransportUnsupported(id) if self.is_current(id) => self.transport_unsupported(),
            Input::TransportFailed { transport, message } if self.is_current(transport) => {
                self.transport_failed(message);
            }
            Input::TransportClosed {
                transport,
                code,
                reason,
            } if self.is_current(transport) => {
                // No reply can come for the calls sent on it: they fail
                // when the close is read (Block 13).
                self.fail_sent_calls();
                // A server's close comes after the messages read before it.
                if self.holds_messages_of(transport) || self.events_hold_inbound() {
                    self.transport_closed_in_order(transport, code, reason);
                } else {
                    self.transport_closed(code, &reason);
                }
            }
            Input::MessageTooLarge(transport) if self.is_current(transport) => {
                self.fail_sent_calls();
                // After the messages read before it, as a close (Block 15).
                if self.holds_messages_of(transport) || self.events_hold_inbound() {
                    self.message_too_large_in_order(transport);
                } else {
                    self.message_too_large();
                }
            }
            Input::InboxOverflow(transport) => self.inbox_overflowed(transport),
            Input::Offline => self.offline("transport closed"),
            Input::PageCached => self.offline(PAGE_CACHED_REASON),
            Input::Frame { transport, data } if self.is_current(transport) => {
                self.counters.bytes_in += data.len() as u64;
                self.frame(transport, &data);
            }
            Input::SendRejected { transport, frame } if self.is_current(transport) => {
                self.send_rejected(&frame);
            }
            Input::FrameWritten { transport, frame } if self.is_current(transport) => {
                self.counters.written(frame);
                self.frame_written(frame);
            }
            Input::TimerFired(timer) => self.timer_fired(timer),
            Input::DebounceTimerFired(timer) => self.debounce_timer_fired(timer),
            Input::TokenFetched { fetch, result } => {
                if self.tokens.take_stale_fetch(fetch) {
                    self.stale_token_fetched(&result);
                } else if let Some(purpose) = self.tokens.take_fetch(fetch) {
                    self.token_fetched(purpose, result);
                }
            }

            Input::ConnectDataFetched { fetch, result } => self.connect_data_fetched(fetch, result),
            input @ (Input::SetToken(_)
            | Input::SetData(_)
            | Input::SetEmulatedHeaders(_)
            | Input::SetSubscriptionData(..)
            | Input::SetSubscriptionTagFilter(..)) => {
                self.set(input);
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
            input @ (Input::EventTaken(_)
            | Input::EventHanded(_)
            | Input::HandlerPanicked { .. }
            | Input::StreamOpened(_)
            | Input::StreamClosed(_)) => self.delivery_input(input),
            Input::Call(id) => self.call(id),
            Input::CallTimerFired(id, timer) => self.call_timer_fired(id, timer),
            Input::CallAbandoned(id) => self.call_abandoned(id),
            Input::Slept { gap } => self.slept(gap),
            // About a transport already abandoned.
            Input::TransportOpened(_)
            | Input::TransportUnsupported(_)
            | Input::TransportFailed { .. }
            | Input::TransportClosed { .. }
            | Input::MessageTooLarge(_)
            | Input::Frame { .. }
            | Input::SendRejected { .. }
            | Input::FrameWritten { .. } => {}
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
        // centrifuge-js `connect`. The first attempt is immediate; delays
        // apply to reconnects.
        self.backoff.restart();
        self.start_attempt();
    }

    fn start_attempt(&mut self) {
        self.reconnecting_after_loss = false;
        self.unsupported = 0;
        match self.tokens.before_connect() {
            BeforeConnect::Ready => self.token_ready(),
            BeforeConnect::Fetch => self.fetch_token(FetchPurpose::Connect),
            BeforeConnect::NoSource => self.no_token_source(),
        }
    }

    /// The attempt has its token: the connect data callback, when there is
    /// one, runs next, then the transport opens (centrifuge-js
    /// `_startReconnecting`).
    fn token_ready(&mut self) {
        if !self.connect_data.source {
            self.open_transport();
            return;
        }
        let fetch = self.subscriptions.new_fetch();
        self.connect_data.fetch = Some(fetch);
        self.actions.push(Action::FetchConnectData(fetch));
    }

    /// A timer set by [`Action::SetTimer`] fired; one of a state the
    /// machine has left does nothing.
    fn timer_fired(&mut self, timer: Timer) {
        match timer {
            Timer::Reconnect => {
                if self.state == State::Connecting && self.transport.is_none() {
                    self.start_attempt();
                }
            }
            Timer::ConnectTimeout => {
                if self.state == State::Connecting && self.transport.is_some() {
                    let message = if self.transport_open {
                        "connect timeout"
                    } else {
                        "handshake timeout"
                    };
                    self.transport_failed(message.to_owned());
                }
            }
            Timer::Refresh => {
                if self.state == State::Connected && !self.tokens.refresh_in_flight() {
                    self.refresh();
                } else if self.state == State::Connecting {
                    // Due while reconnecting (Block 25, D3): the next attempt
                    // fetches a new token first.
                    self.tokens.refresh_due();
                }
            }
            Timer::ServerPing => {
                // Paused while the read-ahead is full (Block 15): a timer
                // that fired before its cancellation was performed is stale.
                if self.state == State::Connected && self.wants_frames() {
                    self.disconnect(codes::connecting::NO_PING, "no ping", true);
                }
            }
            Timer::RefreshReply => {
                if self.state == State::Connected && self.tokens.refresh_timed_out() {
                    // centrifuge-js: a command timeout is error code 1.
                    self.refresh_error(&Error {
                        code: codes::error::TIMEOUT,
                        message: "timeout".to_owned(),
                        temporary: false,
                    });
                }
            }
        }
    }

    /// centrifuge-js `setToken`, `setData`, `setHeaders`: the value is used
    /// from the next connection attempt, and nothing is sent now.
    fn set(&mut self, input: Input) {
        match input {
            Input::SetToken(token) => self.tokens.set(token),
            Input::SetData(data) => self.config.data = Some(data),
            Input::SetEmulatedHeaders(headers) => self.emulated_headers = EmulatedHeaders(headers),
            Input::SetSubscriptionData(id, data) => self.set_subscription_data(id, data),
            Input::SetSubscriptionTagFilter(id, filter) => {
                self.set_subscription_tag_filter(id, filter);
            }
            _ => unreachable!("not a setter input: {input:?}"),
        }
    }

    /// centrifuge-js `_handleGetDataError`: a denial disconnects the client
    /// for good, a failure is reported and the attempt retried after the
    /// reconnect backoff.
    fn connect_data_fetched(&mut self, fetch: FetchId, result: Result<Vec<u8>, DataError>) {
        if self.connect_data.fetch != Some(fetch) {
            return;
        }
        self.connect_data.fetch = None;
        if self.state != State::Connecting || self.transport.is_some() {
            return;
        }
        match result {
            // A refresh that came due while the callback ran, e.g. after a
            // sleep, needs a new token first; the data callback then runs
            // again for it, as on any attempt that fetches one (Block 25,
            // D3).
            Ok(_) if self.tokens.before_connect() == BeforeConnect::Fetch => {
                self.fetch_token(FetchPurpose::Connect);
            }
            Ok(data) => {
                self.connect_data.attempt = Some(data);
                self.open_transport();
            }
            Err(DataError::Unauthorized) => self.unauthorized(),
            Err(DataError::Failed(message)) => {
                self.emit(ClientEvent::Error(ClientError {
                    kind: ErrorKind::ConnectData,
                    code: codes::error::BAD_CONFIGURATION,
                    message,
                    transport: None,
                }));
                let delay = self.backoff.next_delay(&mut self.rng);
                self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
            }
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
        if let Some(frame) = &initial_frame {
            self.counters.opening(frame.len());
        }
        self.actions.push(Action::OpenTransport {
            transport: id,
            kind,
            endpoint: self.transports.index(),
            initial_frame,
        });
        // The open first, then the connect reply (Block 29, D1).
        self.actions.push(Action::SetTimer(
            Timer::ConnectTimeout,
            self.config.handshake_timeout,
        ));
    }

    fn transport_opened(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        self.transport_open = true;
        // An emulated transport's opening request carried its connect
        // command (Block 31, D1).
        self.counters.opened();
        // Opened: the connect reply, or a token fetched first, has `timeout`
        // from now, on every transport kind (Block 29, D1).
        self.actions
            .push(Action::SetTimer(Timer::ConnectTimeout, self.config.timeout));
        if self.transport_kind().is_emulated() {
            // The connect command went with the request; only its reply
            // proves the transport (centrifuge-js `onOpen`).
            return;
        }
        // A refresh came due while it opened, e.g. during a sleep: the
        // connect waits on it for a new token, within the connect timeout
        // (Block 25, D3).
        if self.tokens.before_connect() == BeforeConnect::Fetch {
            self.fetch_token(FetchPurpose::Connect);
            return;
        }
        // Open is not enough to keep a websocket: a server answer is (see
        // `server_answered`), so one that never answers falls back.
        self.send_connect(transport);
    }

    /// Sends a new connect command over websocket, the subscribe commands
    /// that fit going in its frame (Block 36, D5).
    fn send_connect(&mut self, transport: TransportId) {
        let connect = self.connect_command();
        let mut commands = self.optimistic_subscribes(&connect);
        commands.insert(0, connect);
        if self.buffering() {
            for command in &commands {
                self.buffer(command, None, 0, false);
            }
        } else {
            self.send_commands(transport, &commands);
        }
    }

    /// The websocket that opened and waits for a token to send its connect
    /// command (see `transport_opened`).
    fn awaiting_token(&self) -> Option<TransportId> {
        self.transport.filter(|_| {
            self.state == State::Connecting
                && self.transport_open
                && self.connect_command.is_none()
                && !self.transport_kind().is_emulated()
        })
    }

    /// A new connect command, now the one awaiting its reply.
    fn connect_command(&mut self) -> Command {
        let id = self.command_id();
        self.connect_command = Some(id);
        Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: self.tokens.for_connect(),
                data: if self.connect_data.source {
                    self.connect_data.attempt.clone()
                } else {
                    self.config.data.clone()
                },
                headers: self.emulated_headers.0.clone(),
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

    /// The owner cannot open the current transport (Block 44, D12): the
    /// next one of the round is asked for at once, the round wrapping as
    /// after a failure; once every transport was skipped in this attempt,
    /// it fails as centrifuge-js 63ed73e's does when it finds none
    /// (`centrifuge.ts:1154-1170`): a failed attempt, then the `error`
    /// event, naming no transport.
    fn transport_unsupported(&mut self) {
        self.unsupported += 1;
        if self.unsupported < self.transports.len() {
            // Nothing went out: an emulated opening request is not counted,
            // and its connect command awaits no reply.
            self.counters.transport_ended();
            self.connect_command = None;
            self.transport = None;
            self.transport_open = false;
            self.transports.skip();
            self.open_transport();
            return;
        }
        self.unsupported = 0;
        self.reconnect_after_transport_closed();
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Transport,
            code: codes::error::TRANSPORT_CLOSED,
            message: "no supported transport found".to_owned(),
            transport: None,
        }));
    }

    fn transport_closed(&mut self, code: Option<u16>, reason: &str) {
        match code.map(u32::from) {
            // Codes from 3000 are disconnect codes set by the server.
            Some(code) if code >= 3000 => {
                self.server_answered();
                self.disconnect(code, reason, reconnect_advised(code));
            }
            // "Message too big": a reconnect would send the same data again
            // (Block 3, review item C4; centrifuge-js 63ed73e
            // `centrifuge.ts:1310-1313`).
            Some(MESSAGE_TOO_BIG) => self.disconnect(
                codes::disconnected::MESSAGE_SIZE_LIMIT,
                subscriptions::MESSAGE_SIZE_LIMIT_REASON,
                false,
            ),
            _ if self.state == State::Connecting && !self.transport_open => {
                self.transport_failed("transport closed".to_owned());
            }
            _ => self.reconnect_after_transport_closed(),
        }
    }

    /// A reply to a `command` command without its result breaks the
    /// protocol: reported, then handled as a lost transport.
    pub(super) fn reply_without_result(&mut self, command: &'static str) {
        if let Some(transport) = self.transport {
            self.actions
                .push(Action::Anomaly(Anomaly::ReplyWithoutResult {
                    transport,
                    command,
                }));
        }
        self.reconnect_after_transport_closed();
    }

    /// A reply to a `command` command handled before the connect reply
    /// breaks the protocol (Block 36, D6): reported, then handled as a lost
    /// transport.
    fn reply_before_connect(&mut self, command: &'static str) {
        if let Some(transport) = self.transport {
            self.actions
                .push(Action::Anomaly(Anomaly::ReplyBeforeConnect {
                    transport,
                    command,
                }));
        }
        self.reconnect_after_transport_closed();
    }

    /// A message over `Config::max_message_bytes` ended the transport
    /// (Block 29, D3): before the connect reply the bytes may not be the
    /// server's, so the attempt fails; after it a reconnect would get the
    /// same data again, so the client ends, as for close code 1009.
    fn message_too_large(&mut self) {
        if self.state == State::Connecting {
            self.transport_failed(MESSAGE_TOO_LARGE.to_owned());
        } else {
            self.disconnect(
                codes::disconnected::MESSAGE_SIZE_LIMIT,
                subscriptions::MESSAGE_SIZE_LIMIT_REASON,
                false,
            );
        }
    }

    /// The transport's inbox overflowed (Block 44, D7): a client-side
    /// reason, as `reconnect()`, so events not taken are withdrawn. Handled
    /// at once, not after the messages held: the application is not taking
    /// their events, which is why the inbox filled.
    fn inbox_overflowed(&mut self, transport: TransportId) {
        // About a transport already abandoned.
        if !self.is_current(transport) {
            return;
        }
        self.disconnect(
            codes::connecting::READ_AHEAD_OVERFLOW,
            READ_AHEAD_OVERFLOW_REASON,
            true,
        );
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
                    id,
                    publication,
                }) => self.publication(&channel, id, publication),
                ReplyResult::Push(Push::Unsubscribe {
                    channel,
                    code,
                    reason,
                }) => self.server_unsubscribe(&channel, code, &reason),
                ReplyResult::Push(Push::Subscribe { channel, result }) => {
                    self.server_subscription_pushed(&channel, result);
                }
                // centrifuge-js `_handleMessage`.
                ReplyResult::Push(Push::Message(data)) => self.emit(ClientEvent::Message(data)),
                ReplyResult::Push(Push::Join { channel, id, info }) => {
                    self.join_or_leave(&channel, id, info, true);
                }
                ReplyResult::Push(Push::Leave { channel, id, info }) => {
                    self.join_or_leave(&channel, id, info, false);
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
    /// refresh disconnects the client, a call fails with "transport write
    /// error", any other command fails as if the server had answered it with
    /// a temporary error (an unsubscribe then reconnects). It proves nothing about the connection, so the backoff
    /// is not reset.
    fn send_rejected(&mut self, frame: &[u8]) {
        // Over an emulated transport the commands are inside the request.
        let unwrapped;
        let frame = if self.emulation.is_some() {
            let Some(inner) = self.codec.emulation_frame(frame) else {
                return;
            };
            unwrapped = inner;
            &unwrapped
        } else {
            frame
        };
        let ids = self.codec.command_ids(frame);
        if ids.len() > 1 {
            self.resend_alone(frame);
            return;
        }
        for id in ids {
            if id == 0 || self.subscription_command_too_large(id) {
                continue;
            }
            // Retrying the same payload would fail the same way.
            if self.calls.owns_command(id) {
                self.call_reply(Reply {
                    id,
                    error: Some(Error {
                        code: codes::error::TRANSPORT_WRITE,
                        message: "transport write error".to_owned(),
                        temporary: false,
                    }),
                    result: ReplyResult::Empty,
                });
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

    /// A request of several commands refused with HTTP 413: each command
    /// still awaited goes again in its own request, at once and in order,
    /// so that only a command refused alone gets the terminal outcome
    /// (Block 8's rule, owner decision 2026-11-04; Block 18, D6). Its
    /// deadlines keep running, and a call's command counts as unwritten
    /// again. Emulated transports carry JSON only (Block 8).
    fn resend_alone(&mut self, frame: &[u8]) {
        let Some(transport) = self.transport else {
            return;
        };
        for line in self.codec.split_commands(frame) {
            let Some(&id) = self.codec.command_ids(line).first() else {
                continue;
            };
            let call = self.calls.owns_command(id);
            let awaited =
                call || self.awaits_subscription_reply(id) || self.tokens.awaits_refresh_reply(id);
            if !awaited {
                // A subscribe its subscription no longer waits for is
                // retired as a rejected one: no reply will clear it.
                self.subscription_command_too_large(id);
                continue;
            }
            let (session, node) = self.emulation.clone().unwrap_or_default();
            let request = self.codec.emulation_request(&session, &node, line);
            let size = request.len();
            if self.buffering() {
                // Through the buffer, never joined to another command
                // (Block 36, D4).
                let call = self.calls.call_of_command(id);
                let queued = if call.is_some() { size } else { 0 };
                self.queue_bytes(queued);
                self.buffer_bytes(line.to_vec(), call, queued, true, false);
                continue;
            }
            let sent = FrameId(self.next_frame);
            self.next_frame += 1;
            self.counters.sent(sent, size, 1);
            self.actions.push(Action::Send {
                transport,
                id: sent,
                frame: request,
                frame_type: self.codec.frame_type(),
            });
            if call {
                // The whole request, as for any call's frame.
                self.batch_frame_sent(sent, size);
            }
        }
    }

    /// The reply to command `reply.id`, or its failure.
    fn command_reply(&mut self, reply: Reply) {
        if self.calls.owns_command(reply.id) {
            self.call_reply(reply);
            return;
        }
        if self.subscriptions.owns_command(reply.id) {
            // Only a subscribe sent with the connect command can be in
            // flight before its reply (Block 36, D5).
            if self.state != State::Connected && self.awaits_subscribe(reply.id) {
                self.reply_before_connect("subscribe");
            } else {
                self.subscription_command_reply(reply);
            }
            return;
        }
        if self.state == State::Connected && self.tokens.take_refresh_reply(reply.id) {
            self.actions.push(Action::CancelTimer(Timer::RefreshReply));
            match (reply.error, reply.result) {
                (Some(error), _) => self.refresh_error(&error),
                (None, ReplyResult::Refresh(result)) => self.refreshed(&result),
                // A reply to refresh without its result breaks the protocol.
                (None, _) => self.reply_without_result("refresh"),
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
            (None, _) => self.reply_without_result("connect"),
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
        } else {
            // One kept from the last connection (Block 25, D3).
            self.actions.push(Action::CancelTimer(Timer::Refresh));
        }
        self.state = State::Connected;
        self.counters.connects += 1;
        self.pings = (result.ping > 0).then(|| Pings {
            interval: Duration::from_secs(result.ping.into()),
            pong: result.pong,
        });
        self.wait_server_ping();
        self.emit(ClientEvent::Connected {
            client_id: std::mem::take(&mut result.client),
            transport: self.transport_kind().name().to_owned(),
            data: result.data.take(),
        });
        self.subscriptions_connected();
        if self.server_subscriptions_connected(std::mem::take(&mut result.subs)) {
            self.server_delta_failed();
            return;
        }
        self.calls_connected();
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
        let Some(was_connected) = self.tear_down(code, reason, reconnect) else {
            return;
        };
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
                self.reconnecting_after_loss = true;
            }
            self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
        } else {
            self.reconnecting_after_loss = false;
        }
    }

    /// The device went offline, or the page entered the back/forward cache
    /// (Block 44, D10, D11; centrifuge-js 63ed73e `_setNetworkEvents`,
    /// `_setPageEvents`).
    fn offline(&mut self, reason: &str) {
        match self.state {
            State::Disconnected => {}
            State::Connected => {
                self.disconnect(codes::connecting::TRANSPORT_CLOSED, reason, true);
            }
            State::Connecting => {
                if !self.attempt_in_flight() {
                    // Already waiting the reconnect delay.
                    return;
                }
                // Neither a failed attempt (Block 31, D1) nor a failed one
                // of its transport's round (Block 8).
                self.counters.cancelling = true;
                let after_loss = self.reconnecting_after_loss;
                if self
                    .tear_down(codes::connecting::TRANSPORT_CLOSED, reason, true)
                    .is_some()
                {
                    let delay = self.backoff.next_delay(&mut self.rng);
                    self.reconnecting_after_loss = after_loss;
                    self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
                }
            }
        }
    }

    /// Whether an attempt is under way: its token, its connect data, its
    /// transport opening or open.
    fn attempt_in_flight(&self) -> bool {
        self.transport.is_some()
            || self.connect_data.fetch.is_some()
            || matches!(
                self.tokens.pending_fetch(),
                Some((_, FetchPurpose::Connect))
            )
    }

    /// `reconnect()` (Block 25, D5, D6): a connected client reconnects
    /// after the jitter window only, a connecting one at once, unless its
    /// first attempt since it lost its connection has not started (Block
    /// 44, D10); a disconnected one is left alone, as one stopped on
    /// purpose.
    /// `disconnect()`: the attempt in flight is not a failed one (Block
    /// 31, D1).
    fn disconnect_called(&mut self) {
        self.counters.cancelling = true;
        self.disconnect(
            codes::disconnected::DISCONNECT_CALLED,
            "disconnect called",
            false,
        );
    }

    /// The owner's task ended (Block 28, D3, D4): closed as by `close()`,
    /// with its own disconnect code (Block 33, D4).
    fn driver_dropped(&mut self, panicked: bool) {
        if panicked {
            self.close(
                codes::disconnected::BAD_PROTOCOL,
                "exception during message handling",
            );
        } else {
            self.close(codes::disconnected::RUNTIME_SHUT_DOWN, "runtime shut down");
        }
    }

    /// Closes the client for good (Block 33, D2): every call fails with
    /// "client closed", every subscription, client-side and server-side,
    /// is unsubscribed with code 2 and no command, then the client
    /// disconnects with `code`. The attempt it ends is not a failed one.
    fn close(&mut self, code: u32, reason: &str) {
        self.counters.cancelling = true;
        self.fail_calls_closed();
        let ids: Vec<SubscriptionId> = self.subscriptions().into_iter().map(|(id, _)| id).collect();
        for id in ids {
            self.set_unsubscribed(
                id,
                codes::unsubscribed::CLIENT_CLOSED,
                CLIENT_CLOSED_REASON,
                false,
            );
        }
        for channel in self.server_subscriptions() {
            self.server_subscription_unsubscribed(
                &channel,
                codes::unsubscribed::CLIENT_CLOSED,
                CLIENT_CLOSED_REASON,
            );
        }
        self.disconnect(code, reason, false);
    }

    fn reconnect(&mut self) {
        // Not a failed attempt (Block 31, D1).
        self.counters.cancelling = true;
        let Some(was_connected) = self.tear_down(
            codes::connecting::RECONNECT_CALLED,
            "reconnect called",
            true,
        ) else {
            return;
        };
        let after_loss = std::mem::take(&mut self.reconnecting_after_loss);
        self.transports.restart();
        // As after `connect()` (Block 2).
        self.backoff.restart();
        // A connected client, or one whose first attempt since it lost its
        // connection has not started, waits the jitter window (Block 25 D5,
        // amended by Block 44 D10), so that a fleet whose network comes
        // back does not reconnect at once.
        if was_connected || (after_loss && !self.config.reconnect_jitter_window.is_zero()) {
            self.reconnecting_after_loss = true;
            let delay = self.reconnect_jitter();
            self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
        } else {
            self.start_attempt();
        }
    }

    /// Ends the connection or the attempt in flight, entering `connecting`
    /// when `reconnect`, else `disconnected`; whether the client was
    /// connected, or `None` when it was disconnected already.
    fn tear_down(&mut self, code: u32, reason: &str, reconnect: bool) -> Option<bool> {
        let cancelled = std::mem::take(&mut self.counters.cancelling);
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
        // Calls cannot outlive `disconnected` (Block 13): a call made after
        // the client stopped fails at the next `disconnect()` too
        // (centrifuge-js `disconnect`).
        if !reconnect {
            self.fail_calls_disconnected();
        }
        if self.state == State::Disconnected {
            return None;
        }
        // No reply can come for the calls sent on the transport.
        self.fail_sent_calls();
        let was_connected = self.state == State::Connected;
        if let Some(transport) = self.transport.take() {
            self.counters.transport_ended();
            if !was_connected && !cancelled {
                self.counters.failed_attempts += 1;
            }
            self.actions.push(Action::CloseTransport(transport));
            // A cancelled attempt keeps its transport's place in the round
            // (Block 44, D10).
            if reconnect && !cancelled {
                self.transports.attempt_failed();
            }
        }
        self.transport_open = false;
        self.emulation = None;
        // A connection that ends before it proved usable keeps the backoff
        // growing, and so does one the server found too slow, or whose
        // inbox overflowed (Block 44, D7), before any ping (Block 15,
        // "Backoff after 3008"): each reconnect would ask for the same
        // backlog.
        self.backoff_reset_pending = false;
        let pinged = std::mem::take(&mut self.pinged);
        // A server-side subscription's broken delta keeps it growing too
        // (Block 19, D8).
        let slow = code == SLOW || code == codes::connecting::READ_AHEAD_OVERFLOW;
        if (slow && !pinged) || code == codes::connecting::DELTA_ERROR {
            self.backoff.undo_reset();
        } else {
            self.backoff.keep_reset();
        }
        self.connect_command = None;
        // A data callback still running answers an attempt that is over
        // (centrifuge-js `_attemptAborted`).
        if let Some(fetch) = self.connect_data.fetch.take() {
            self.actions.push(Action::CancelConnectData(fetch));
        }
        self.drop_held_commands();
        self.tokens.teardown(reconnect);
        // Every timer belongs to the connection being torn down; the
        // reconnect timer is set again below when reconnecting. The refresh
        // timer outlives a reconnect, until the next connect reply re-arms or
        // cancels it, so a token due while reconnecting, or during a sleep,
        // is fetched before the next connect (Block 25, D3).
        for timer in Timer::ALL {
            if !(reconnect && timer == Timer::Refresh) {
                self.actions.push(Action::CancelTimer(timer));
            }
        }
        self.subscriptions_disconnected(reconnect, code == STATE_INVALIDATED);
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
        Some(was_connected)
    }

    /// The system slept for `gap` (Block 25, D2 and D3).
    fn slept(&mut self, gap: Duration) {
        if gap < SLEEP_THRESHOLD || self.state == State::Disconnected {
            return;
        }
        if self.state == State::Connected
            && let Some(pings) = self.pings
            && !self.config.max_server_ping_delay.is_zero()
        {
            let window = pings
                .interval
                .saturating_add(self.config.max_server_ping_delay);
            if gap >= window {
                // As when the no-ping deadline fires: the server gave up
                // on the connection; the backoff and jitter apply.
                self.disconnect(codes::connecting::NO_PING, "no ping", true);
            } else {
                // Pings queued during the sleep are on their way.
                self.wait_server_ping();
            }
        }
        // Only the owner knows how long each timer has left.
        self.actions.push(Action::ShortenTimer(Timer::Refresh, gap));
        self.shorten_subscription_refreshes(gap);
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
                        if let Some(transport) = self.awaiting_token() {
                            self.send_connect(transport);
                        } else if self.state == State::Connecting && self.transport.is_none() {
                            self.token_ready();
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
                    if self.awaiting_token().is_some() {
                        // The open websocket is closed as the attempt ends.
                        self.reconnect_after_transport_closed();
                    } else {
                        let delay = self.backoff.next_delay(&mut self.rng);
                        self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
                    }
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
    /// Hands `command` to the owner; its frame's id and size.
    /// With automatic batching it goes into the output buffer instead
    /// (Block 36, D1).
    fn send(&mut self, transport: TransportId, command: Command) {
        if self.buffering() {
            self.buffer(&command, None, 0, false);
        } else {
            self.send_commands(transport, &[command]);
        }
    }

    /// The frame `commands` make together, as sent: over an emulated
    /// transport, the body of the emulation request.
    fn frame_of(&self, commands: &[Command]) -> Vec<u8> {
        let frame = self.codec.encode(commands);
        match &self.emulation {
            Some((session, node)) => self.codec.emulation_request(session, node, &frame),
            None => frame,
        }
    }

    /// Sends `commands` in one frame (Block 18; one command otherwise).
    fn send_commands(&mut self, transport: TransportId, commands: &[Command]) -> (FrameId, usize) {
        let frame = self.frame_of(commands);
        let id = FrameId(self.next_frame);
        self.next_frame += 1;
        let size = frame.len();
        self.counters.sent(id, size, commands.len());
        self.actions.push(Action::Send {
            transport,
            id,
            frame,
            frame_type: self.codec.frame_type(),
        });
        (id, size)
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

    // Block 25, D5: after `reconnect()` the backoff starts over, whatever
    // it had grown to.
    #[test]
    fn after_reconnect_a_failed_attempt_waits_at_most_the_first_bound() {
        let config = Config::default();
        let bound = config.min_reconnect_delay * 2;
        for seed in 0..1000 {
            let mut machine =
                Machine::with_rng(config.clone(), false, fastrand::Rng::with_seed(seed));
            let mut transport = opened_transport(&machine.handle(Input::Connect));
            for _ in 0..8 {
                machine.handle(Input::TransportFailed {
                    transport,
                    message: "refused".into(),
                });
                transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
            }

            let restarted = opened_transport(&machine.handle(Input::Reconnect));
            let failed = machine.handle(Input::TransportFailed {
                transport: restarted,
                message: "refused".into(),
            });

            let delay = failed
                .iter()
                .find_map(|action| match action {
                    Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
                    _ => None,
                })
                .expect("a reconnect delay");
            assert!(delay <= bound, "seed {seed}: {delay:?}");
        }
    }

    // Codex, #120: the refresh came due while the websocket opened.
    #[test]
    fn a_refresh_due_while_the_websocket_opens_fetches_a_token_for_its_connect() {
        let (mut machine, transport, _) = connected_with_token(60);
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TimerFired(Timer::Refresh));

        let opened = machine.handle(Input::TransportOpened(transport));
        let fetch = fetch_requested(&opened);
        let connecting = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });

        assert!(
            !opened
                .iter()
                .any(|action| matches!(action, Action::Send { .. })),
            "{opened:?}"
        );
        let sent: Vec<String> = connecting
            .iter()
            .filter_map(|action| match action {
                Action::Send {
                    transport: on,
                    frame,
                    ..
                } if *on == transport => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            sent.iter().any(|frame| frame.contains(r#""token":"t1""#)),
            "{connecting:?}"
        );
    }

    #[test]
    fn a_failed_token_for_an_open_websocket_closes_it_and_retries_later() {
        let (mut machine, transport, _) = connected_with_token(60);
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TimerFired(Timer::Refresh));
        let fetch = fetch_requested(&machine.handle(Input::TransportOpened(transport)));

        let failed = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Failed("unavailable".into())),
        });

        assert!(
            failed.contains(&Action::CloseTransport(transport)),
            "{failed:?}"
        );
        assert!(
            failed
                .iter()
                .any(|action| matches!(action, Action::SetTimer(Timer::Reconnect, _))),
            "{failed:?}"
        );
        assert_eq!(machine.state(), State::Connecting);
    }

    // Codex, #120: the refresh came due while the connect data callback
    // ran.
    #[test]
    fn a_refresh_due_during_the_connect_data_callback_fetches_a_token_first() {
        let mut machine = token_machine("t0").with_data_source();
        let fetch_data = |actions: &[Action]| {
            actions
                .iter()
                .find_map(|action| match action {
                    Action::FetchConnectData(fetch) => Some(*fetch),
                    _ => None,
                })
                .expect("a connect data fetch")
        };
        let first = fetch_data(&machine.handle(Input::Connect));
        let transport = opened_transport(&machine.handle(Input::ConnectDataFetched {
            fetch: first,
            result: Ok(b"{}".to_vec()),
        }));
        machine.handle(Input::TransportOpened(transport));
        machine.handle(frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":60}}"#,
        ));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let again = fetch_data(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TimerFired(Timer::Refresh));

        let answered = machine.handle(Input::ConnectDataFetched {
            fetch: again,
            result: Ok(b"{}".to_vec()),
        });

        assert!(
            answered
                .iter()
                .any(|action| matches!(action, Action::FetchToken(_))),
            "{answered:?}"
        );
        assert!(
            !answered
                .iter()
                .any(|action| matches!(action, Action::OpenTransport { .. })),
            "{answered:?}"
        );
    }

    // Block 25, D3: the refresh timer outlives a reconnect, until the next
    // connect reply re-arms or cancels it.
    #[test]
    fn a_connect_reply_without_expiry_cancels_the_kept_refresh() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":60}}"#,
        ));
        let lost = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        assert!(
            !lost
                .iter()
                .any(|action| matches!(action, Action::CancelTimer(Timer::Refresh))),
            "{lost:?}"
        );
        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TransportOpened(transport));

        let connected = machine.handle(frame(transport, r#"{"id":2,"connect":{"client":"c"}}"#));

        assert!(
            connected
                .iter()
                .any(|action| matches!(action, Action::CancelTimer(Timer::Refresh))),
            "{connected:?}"
        );
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
        let next = if machine.state == State::Disconnected {
            Input::Connect
        } else {
            Input::TimerFired(Timer::Reconnect)
        };
        let transport = opened_transport(&machine.handle(next));
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
    fn close_code_1009_is_a_terminal_message_size_limit() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: Some(1009),
            reason: "message too big".into(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: codes::disconnected::MESSAGE_SIZE_LIMIT,
                reason: "message size limit exceeded".into()
            }]
        );
        assert_eq!(machine.state(), State::Disconnected);
    }

    /// The durations `actions` set `Timer::ConnectTimeout` to.
    fn connect_timeouts(actions: &[Action]) -> Vec<Duration> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::SetTimer(Timer::ConnectTimeout, after) => Some(*after),
                _ => None,
            })
            .collect()
    }

    /// The kind of the transport `actions` open, if any.
    fn opened_kind(actions: &[Action]) -> Option<TransportKind> {
        actions.iter().find_map(|action| match action {
            Action::OpenTransport { kind, .. } => Some(*kind),
            _ => None,
        })
    }

    // Block 29, D1: the open has the handshake timeout, the connect reply
    // `timeout`, on every transport kind.
    #[test]
    fn the_open_has_the_handshake_timeout_and_the_connect_reply_the_timeout() {
        for kind in [TransportKind::WebSocket, TransportKind::HttpStream] {
            let config = Config::default()
                .handshake_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(3));
            let mut machine = Machine::new(config).with_transports(vec![kind]).unwrap();

            let connect = machine.handle(Input::Connect);
            let transport = opened_transport(&connect);
            let opened = machine.handle(Input::TransportOpened(transport));

            assert_eq!(
                connect_timeouts(&connect),
                [Duration::from_secs(1)],
                "{kind:?}"
            );
            assert_eq!(
                connect_timeouts(&opened),
                [Duration::from_secs(3)],
                "{kind:?}"
            );
        }
    }

    // Block 29, D1: the error names which deadline passed.
    #[test]
    fn an_open_that_times_out_is_a_handshake_timeout() {
        let mut machine = machine();
        let transport = opened_transport(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TimerFired(Timer::ConnectTimeout));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError { message, .. })] if message == "handshake timeout"
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    // Block 29, D3: before the connect reply, the attempt fails and the
    // next transport is tried.
    #[test]
    fn a_message_too_large_before_the_connect_reply_fails_the_attempt() {
        let mut machine = Machine::new(Config::default())
            .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
            .unwrap();
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));

        let actions = machine.handle(Input::MessageTooLarge(transport));

        assert!(
            matches!(
                events(&actions)[..],
                [ClientEvent::Error(ClientError { message, .. })]
                    if message == "message larger than the size limit"
            ),
            "{actions:?}"
        );
        // Within the first sequence, at once.
        assert!(actions.contains(&Action::SetTimer(Timer::Reconnect, Duration::ZERO)));
        let next = machine.handle(Input::TimerFired(Timer::Reconnect));
        assert_eq!(opened_kind(&next), Some(TransportKind::HttpStream));
        assert_eq!(machine.state(), State::Connecting);
    }

    // Block 29, D3: once connected, the client ends as for close code 1009.
    #[test]
    fn a_message_too_large_after_the_connect_reply_ends_the_client() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::MessageTooLarge(transport));

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: codes::disconnected::MESSAGE_SIZE_LIMIT,
                reason: "message size limit exceeded".into()
            }]
        );
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::SetTimer(Timer::Reconnect, _))),
            "{actions:?}"
        );
        assert_eq!(machine.state(), State::Disconnected);
    }

    // Block 29, D3: the rule is the adapter's; the core handles a frame of
    // any length.
    #[test]
    fn a_frame_over_the_limit_given_to_the_core_is_handled() {
        let mut machine = Machine::new(Config::default().max_message_bytes(8));
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));

        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        assert_eq!(machine.state(), State::Connected);
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
                    id: FrameId(1),
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

        let Some(send @ Action::Send { id, frame, .. }) = actions
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{actions:?}");
        };
        assert_eq!(
            format!("{send:?}"),
            format!(
                "Send {{ transport: {transport:?}, id: {id:?}, frame: <{} bytes>, frame_type: Text }}",
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
        // The refresh got no token: the attempt fetches one first (Block 25,
        // D3).
        let attempt = fetch_requested(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        let transport = opened_transport(&machine.handle(Input::TokenFetched {
            fetch: attempt,
            result: Ok("t0b".into()),
        }));
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

        // The refresh got no token: the attempt fetches one first (Block 25,
        // D3), and the late one is not used.
        let attempt = fetch_requested(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        let transport = opened_transport(&machine.handle(Input::TokenFetched {
            fetch: attempt,
            result: Ok("t2".into()),
        }));
        let connecting = machine.handle(Input::TransportOpened(transport));

        let Some(Action::Send { frame, .. }) = connecting
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{connecting:?}");
        };
        let frame = String::from_utf8(frame.clone()).unwrap();
        assert!(frame.contains(r#""token":"t2""#), "{frame}");
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
            matches!(&sent[..], [Action::SetTimer(Timer::ConnectTimeout, _), Action::Send { frame, .. }] if frame.windows(4).any(|w| w == b"\"t1\"")),
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

    #[test]
    fn debug_hides_emulated_header_values_and_connect_data() {
        let mut machine = machine()
            .with_data_source()
            .with_emulated_headers([("Authorization".to_owned(), "e-secret".to_owned())].into());
        let fetch = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::FetchConnectData(fetch) => Some(*fetch),
                _ => None,
            })
            .expect("a data fetch");
        machine.handle(Input::ConnectDataFetched {
            fetch,
            result: Ok(b"d-secret".to_vec()),
        });

        let debug = format!("{machine:?}");

        assert!(debug.contains("Authorization"), "{debug}");
        assert!(
            !debug.contains("e-secret") && !debug.contains("d-secret"),
            "{debug}"
        );
    }

    // Block 17: a disconnect while the connect data callback runs cancels
    // it, and its late answer opens nothing.
    #[test]
    fn disconnect_cancels_a_pending_connect_data_fetch() {
        let mut machine = machine().with_data_source();
        let fetch = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::FetchConnectData(fetch) => Some(*fetch),
                _ => None,
            })
            .expect("a data fetch before the transport");

        let disconnected = machine.handle(Input::Disconnect);
        let late = machine.handle(Input::ConnectDataFetched {
            fetch,
            result: Ok(b"{}".to_vec()),
        });

        assert!(
            disconnected.contains(&Action::CancelConnectData(fetch)),
            "{disconnected:?}"
        );
        assert_eq!(late, []);
    }

    // centrifuge-js `connect` resets the attempt counter: delays grown
    // before an explicit disconnect do not carry over to the next connect.
    #[test]
    fn connect_resets_the_backoff() {
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
        machine.handle(Input::Disconnect);
        let transport = opened_transport(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TransportFailed {
            transport,
            message: "refused".into(),
        });

        let Some(Action::SetTimer(Timer::Reconnect, delay)) = actions.last() else {
            panic!("no reconnect timer in {actions:?}");
        };
        assert!(*delay <= Duration::from_millis(200), "{delay:?}");
    }
}
