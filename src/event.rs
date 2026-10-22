//! Client state and the events reporting its changes.

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

/// Something that happened to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    Connected {
        /// Connection id assigned by the server.
        client_id: String,
        /// Transport in use, e.g. `"websocket"`.
        transport: String,
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

/// Origin of a [`ClientError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
}
