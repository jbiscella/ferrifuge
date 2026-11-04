//! Client-side subscriptions: their states, events and publications
//! (Block 9; spec, "Subscription states", "Listen to channel publications").

use std::collections::HashMap;

use crate::ClientError;

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
        }
    }
}

/// Details of a [`SubscriptionEvent::Subscribed`] (spec, "Subscription
/// recovery state"; centrifuge-js `SubscribedContext`).
#[allow(clippy::struct_excessive_bools)] // The flags of the spec's subscribed context.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
}

/// A client connection, as the server describes it (`client.proto`,
/// `ClientInfo`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

/// Why a subscription data callback could not provide data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    /// Access is denied: the subscription is unsubscribed for good.
    Unauthorized,
    /// A failure, e.g. the backend is unreachable: retried after the
    /// resubscribe backoff.
    Failed(String),
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Failed(reason) => write!(f, "subscription data callback failed: {reason}"),
        }
    }
}

impl std::error::Error for DataError {}

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
    /// The client was dropped: it can no longer subscribe.
    ClientClosed,
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { channel } => {
                write!(f, "a subscription to channel {channel} already exists")
            }
            Self::ClientClosed => f.write_str("the client was dropped"),
        }
    }
}

impl std::error::Error for SubscriptionError {}
