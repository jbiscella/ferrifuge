//! Client-side subscriptions: their states, events and publications
//! (Block 9; spec, "Subscription states", "Listen to channel publications").

use std::collections::HashMap;

use crate::ClientError;
use crate::token::{Bytes, optional_bytes};

/// State of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionState {
    /// Not subscribed and not trying to: initial state, after
    /// `unsubscribe()`, or after the server refused the subscription.
    Unsubscribed,
    /// Waiting to be subscribed: for the client to connect, or for the
    /// server's reply.
    Subscribing,
    /// Subscribed: publications arrive.
    Subscribed,
}

impl SubscriptionState {
    /// Name of the state as used by the Client SDK spec.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsubscribed => "unsubscribed",
            Self::Subscribing => "subscribing",
            Self::Subscribed => "subscribed",
        }
    }
}

/// Something that happened to a subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionEvent {
    /// The subscription moved to [`SubscriptionState::Subscribing`].
    Subscribing {
        /// Why: a code from [`crate::codes::subscribing`].
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The subscription moved to [`SubscriptionState::Subscribed`].
    Subscribed(Subscribed),
    /// The subscription moved to [`SubscriptionState::Unsubscribed`].
    Unsubscribed {
        /// Why: a code from [`crate::codes::unsubscribed`], or the code of
        /// the server's error.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// A publication of the channel.
    Publication(Publication),
    /// An error that does not change the state, e.g. a failing data
    /// callback while the subscription keeps trying.
    Error(ClientError),
    /// A client subscribed to the channel: a join push, sent to
    /// subscriptions with [`crate::machine::SubscriptionOptions::join_leave`]
    /// on channels that have them.
    Join(ClientInfo),
    /// A client unsubscribed from the channel: a leave push, as for
    /// [`SubscriptionEvent::Join`].
    Leave(ClientInfo),
}

impl SubscriptionEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribing { .. } => "subscribing",
            Self::Subscribed(_) => "subscribed",
            Self::Unsubscribed { .. } => "unsubscribed",
            Self::Publication(_) => "publication",
            Self::Error(_) => "error",
            Self::Join(_) => "join",
            Self::Leave(_) => "leave",
        }
    }
}

/// Details of a [`SubscriptionEvent::Subscribed`] (spec, "Subscription
/// recovery state"; centrifuge-js `SubscribedContext`).
///
/// Its `Debug` output shows the data as its length (Block 21).
#[allow(clippy::struct_excessive_bools)] // The flags of the spec's subscribed context.
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Subscribed {
    /// Data the server attached to the subscription, if any
    /// (`SubscribeResult.data`).
    pub data: Option<Vec<u8>>,
    /// The server keeps a stream position for the subscription: the
    /// channel forces it, or the [`positioned`] option asked for it and the
    /// channel allows it.
    ///
    /// [`positioned`]: crate::machine::SubscriptionOptions::positioned
    pub positioned: bool,
    /// Publications missed while not subscribed can be recovered: the
    /// channel forces it, or the [`recoverable`] option asked for it and the
    /// channel allows it.
    ///
    /// [`recoverable`]: crate::machine::SubscriptionOptions::recoverable
    pub recoverable: bool,
    /// The subscribe asked to recover missed publications.
    pub was_recovering: bool,
    /// All missed publications were recovered; they are delivered after
    /// this event. When a recovery fails, publications may be missing.
    pub recovered: bool,
    /// The stream position, when the subscription is positioned or
    /// recoverable: the top of the stream, or after a recovery the position
    /// the recovered publications follow.
    pub stream_position: Option<StreamPosition>,
    /// The server sends this subscription's publications as deltas until
    /// its next subscribe, as the [`delta`] option asked (Block 19). The
    /// client rebuilds them: the application gets whole payloads.
    ///
    /// [`delta`]: crate::machine::SubscriptionOptions::delta
    pub delta: bool,
    /// Publications follow this event, recovered or initial ones, unless
    /// they are withdrawn (Block 15); for metrics and logging (Block 35c,
    /// centrifuge-js `hasRecoveredPublications`).
    pub has_recovered_publications: bool,
}

/// A delta compression format a subscription can ask for (`client.proto`,
/// `SubscribeRequest.delta`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DeltaFormat {
    /// Fossil deltas, the only format `client.proto` supports.
    Fossil,
}

/// A position in a channel's stream (spec, "Subscription recovery state").
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct StreamPosition {
    /// Offset of a publication in the stream; 0 before the first one.
    pub offset: u64,
    /// Epoch of the stream. A stream that is lost, e.g. when its history
    /// expired, starts again with a new epoch.
    pub epoch: String,
}

/// A publication of a channel (`client.proto`, `Publication`).
///
/// Its `Debug` output shows the data as its length and the tags by name
/// (Block 21).
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Publication {
    /// Its data, exactly as received: JSON text with the JSON protocol, any
    /// bytes with Protobuf.
    pub data: Vec<u8>,
    /// Its position in the channel's stream, when the channel keeps one.
    pub offset: Option<u64>,
    /// Tags the publisher attached.
    pub tags: HashMap<String, String>,
    /// The client that published it, when a client did.
    pub info: Option<ClientInfo>,
    /// The stream's new epoch, when this publication starts a new stream
    /// (`client.proto`, `Publication.epoch`): a position saved to resume
    /// from later is then this offset with this epoch.
    pub epoch: Option<String>,
    /// The channel it was published to, when the server says it differs
    /// from the subscription's, as for a wildcard subscription
    /// (`client.proto`, `Publication.channel`). Such a publication belongs
    /// to another stream: it moves neither the offset nor the epoch of its
    /// subscription (Block 35c).
    pub channel: Option<String>,
    /// The wire's `Publication.delta`: `data` is a Fossil delta against the
    /// previous publication. Cleared once the delta is applied, so a
    /// publication the application takes never has it (Block 19, D4).
    pub(crate) delta: bool,
    /// A map publication's key, and whether it removes the key (Block 45,
    /// D3); boxed, as a stream publication has none.
    pub(crate) map: Option<Box<MapPart>>,
}

/// What a map publication carries besides a stream publication's fields
/// (`client.proto`, `Publication.key` and `Publication.removed`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MapPart {
    pub(crate) key: String,
    pub(crate) removed: bool,
}

impl MapPart {
    /// The map part of a publication with `key` and `removed`, `None` for
    /// a stream publication's empty key and false.
    pub(crate) fn of(key: String, removed: bool) -> Option<Box<Self>> {
        (!key.is_empty() || removed).then(|| Box::new(Self { key, removed }))
    }
}

/// A client connection, as the server describes it (`client.proto`,
/// `ClientInfo`).
///
/// Its `Debug` output shows the connection and channel info as their
/// length (Block 21).
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientInfo {
    /// Connection id.
    pub client: String,
    /// User id; empty for anonymous connections.
    pub user: String,
    /// Connection info, from the connection token or proxy.
    pub conn_info: Option<Vec<u8>>,
    /// Channel info, from the subscription token or proxy.
    pub chan_info: Option<Vec<u8>>,
}

impl std::fmt::Debug for Subscribed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscribed")
            .field("data", &optional_bytes(self.data.as_ref()))
            .field("positioned", &self.positioned)
            .field("recoverable", &self.recoverable)
            .field("was_recovering", &self.was_recovering)
            .field("recovered", &self.recovered)
            .field("stream_position", &self.stream_position)
            .field("delta", &self.delta)
            .field(
                "has_recovered_publications",
                &self.has_recovered_publications,
            )
            .finish()
    }
}

impl Publication {
    /// Decodes [`data`](Self::data) as JSON into `T` (Block 38): the JSON a
    /// publisher's [`Client::publish_json`](crate::Client::publish_json)
    /// encoded, or any JSON published, under either protocol. `T` may
    /// borrow from the data, e.g. a `&str` or a
    /// [`RawValue`](serde_json::value::RawValue).
    ///
    /// ```
    /// # use ferrifuge::Publication;
    /// #[derive(serde::Deserialize)]
    /// struct Note {
    ///     text: String,
    /// }
    ///
    /// fn show(publication: &Publication) {
    ///     match publication.json::<Note>() {
    ///         Ok(note) => println!("{}", note.text),
    ///         Err(error) => eprintln!("not a note: {error}"),
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// The data is not JSON, or not JSON that `T` accepts; empty data is not
    /// JSON. `serde_json`'s message may quote fragments of the data.
    pub fn json<'de, T: serde::Deserialize<'de>>(&'de self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}

impl std::fmt::Debug for Publication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tags: Vec<&String> = self.tags.keys().collect();
        tags.sort();
        f.debug_struct("Publication")
            .field("data", &Bytes(&self.data))
            .field("offset", &self.offset)
            .field("tags", &tags)
            .field("info", &self.info)
            .field("epoch", &self.epoch)
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ClientInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientInfo")
            .field("client", &self.client)
            .field("user", &self.user)
            .field("conn_info", &optional_bytes(self.conn_info.as_ref()))
            .field("chan_info", &optional_bytes(self.chan_info.as_ref()))
            .finish()
    }
}

/// Why a data callback, a subscription's or the client's connect data
/// one, could not provide data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    /// Access is denied: the subscription is unsubscribed, or the client
    /// disconnected, for good.
    Unauthorized,
    /// A failure, e.g. the backend is unreachable: retried after the
    /// resubscribe, or the reconnect, backoff.
    Failed(String),
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Failed(reason) => write!(f, "data callback failed: {reason}"),
        }
    }
}

impl std::error::Error for DataError {}

/// Why a subscription getState callback could not load the application's
/// state: the subscription emits an `error` event and retries after the
/// resubscribe backoff (centrifuge-js `getState`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateError(pub String);

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "subscription getState callback failed: {}", self.0)
    }
}

impl std::error::Error for StateError {}

/// Why a subscription could not be created.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionError {
    /// A subscription to the channel exists already in the client's registry
    /// (spec: `newSubscription` can throw).
    AlreadyExists {
        /// The channel.
        channel: String,
    },
    /// The client is closed, dropped or its runtime shut down (Block 28):
    /// it can no longer subscribe.
    ClientClosed,
    /// A tag filter the server would refuse: a comparison with an empty key,
    /// or a set comparison with no value (Block 35a).
    InvalidTagFilter {
        /// What is wrong, naming the key, never a value.
        reason: String,
    },
    /// A tag filter cannot go with delta compression (Block 35a;
    /// `client.proto`: a filter "disables delta compression").
    TagFilterWithDelta,
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { channel } => {
                write!(f, "a subscription to channel {channel} already exists")
            }
            Self::ClientClosed => f.write_str("the client is closed"),
            Self::InvalidTagFilter { reason } => write!(f, "invalid tag filter: {reason}"),
            Self::TagFilterWithDelta => {
                f.write_str("a tag filter cannot be combined with delta compression")
            }
        }
    }
}

impl std::error::Error for SubscriptionError {}
