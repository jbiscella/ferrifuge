//! Client state and the events reporting its changes.

use crate::{ClientInfo, Publication, Subscribed};

/// Connection state of a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    /// Not connected and not trying to connect: initial state, after
    /// `disconnect()`, or after a terminal condition.
    Disconnected,
    /// Trying to establish or re-establish a connection.
    Connecting,
    /// Connected to the server.
    Connected,
}

impl State {
    /// Name of the state as used by the Client SDK spec.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
        }
    }
}

/// Something that happened to a client. `#[non_exhaustive]`: later blocks
/// add events.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientEvent {
    /// The client moved to [`State::Connecting`].
    Connecting {
        /// Why: a client-side code from [`crate::codes::connecting`] or a
        /// server disconnect code.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The client moved to [`State::Connected`].
    #[non_exhaustive]
    Connected {
        /// Connection id assigned by the server.
        client_id: String,
        /// Transport in use, e.g. `"websocket"`.
        transport: String,
        /// The data the server attached to the connect reply
        /// (`ConnectResult.data`), exactly as received: raw JSON with the
        /// JSON protocol, any bytes with Protobuf.
        data: Option<Vec<u8>>,
    },
    /// The client moved to [`State::Disconnected`].
    Disconnected {
        /// Why: a client-side code from [`crate::codes::disconnected`] or a
        /// server disconnect code.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// An error that does not change the state, e.g. a failed connection
    /// attempt while the client keeps reconnecting.
    Error(ClientError),
    /// Something happened to the server-side subscription to `channel`: one
    /// the server made, which the client tracks but does not control (spec,
    /// "Server-side subscriptions").
    ServerSubscription {
        /// The channel subscribed by the server.
        channel: String,
        /// What happened.
        event: ServerSubscriptionEvent,
    },
    /// An asynchronous message from the server (`client.proto`,
    /// `Message.data`): raw JSON with the JSON protocol, any bytes with
    /// Protobuf, exactly as received.
    Message(Vec<u8>),
}

/// Something that happened to a server-side subscription, as for a
/// client-side one (spec, "Server-side subscriptions").
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerSubscriptionEvent {
    /// The connection was lost or `disconnect()` was called: the
    /// subscription stays in the registry and is restored on the next
    /// connection.
    Subscribing,
    /// The server subscribed the connection, at connect or later.
    Subscribed(Subscribed),
    /// The server ended the subscription, which leaves the registry: a code
    /// of the server's `Unsubscribe` push, or
    /// [`crate::codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`].
    Unsubscribed {
        /// Why: a server unsubscribe code or a client-side one.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// A publication in the channel.
    Publication(Publication),
    /// A client subscribed to the channel (a join push).
    Join(ClientInfo),
    /// A client unsubscribed from the channel (a leave push).
    Leave(ClientInfo),
}

impl ServerSubscriptionEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribing => "subscribing",
            Self::Subscribed(_) => "subscribed",
            Self::Unsubscribed { .. } => "unsubscribed",
            Self::Publication(_) => "publication",
            Self::Join(_) => "join",
            Self::Leave(_) => "leave",
        }
    }
}

impl ClientEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connecting { .. } => "connecting",
            Self::Connected { .. } => "connected",
            Self::Disconnected { .. } => "disconnected",
            Self::Error(_) => "error",
            Self::ServerSubscription { event, .. } => event.name(),
            Self::Message(_) => "message",
        }
    }
}

/// Details of a [`ClientEvent::Error`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    /// Where the error happened.
    pub kind: ErrorKind,
    /// A client-side code from [`crate::codes::error`], or a server error code.
    pub code: u32,
    /// Human-readable description.
    pub message: String,
    /// Transport the error happened on, when it is transport-related.
    pub transport: Option<String>,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)?;
        if let Some(transport) = &self.transport {
            write!(f, " over {transport}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClientError {}

/// Origin of a [`ClientError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The transport failed to open, or closed before it opened. As in
    /// centrifuge-js, a transport that opened and then closed before the
    /// connect reply is not reported as an error: the client, still
    /// connecting, schedules the next attempt without an event.
    Transport,
    /// The server answered the connect command with a temporary error.
    Connect,
    /// The token callback failed while getting a token to connect with.
    ConnectToken,
    /// The connect data callback failed; the client retries after the
    /// reconnect backoff (Block 17).
    ConnectData,
    /// The token callback failed while getting a token to refresh with.
    RefreshToken,
    /// The server answered the refresh command with a temporary error.
    Refresh,
    /// The client options cannot work, e.g. a token expires without a
    /// token callback to refresh it.
    Configuration,
    /// A subscription data callback failed; the subscription retries.
    SubscriptionData,
    /// The server answered a subscribe command with a temporary error; the
    /// subscription retries.
    Subscribe,
    /// A subscription token callback failed while getting a token to
    /// subscribe with; the subscription retries.
    SubscribeToken,
    /// A subscription token callback failed while getting a token to
    /// refresh with; the refresh is retried.
    SubscriptionRefreshToken,
    /// The server answered a `sub_refresh` command with a temporary error,
    /// or not in time; the refresh is retried.
    SubscriptionRefresh,
    /// A delta publication could not be applied: the subscription
    /// subscribes again, or, for a server-side subscription, the client
    /// reconnects (Block 19, D8).
    Delta,
}
