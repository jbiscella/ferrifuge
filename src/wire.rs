//! Codec-independent model of the client protocol messages the client uses.
//!
//! Mirrors the subset of `proto/client.proto` implemented so far. Codecs
//! translate between these types and bytes on the wire.

/// A command sent by the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Command {
    /// Matches the reply to the command; never 0 for commands awaiting one.
    pub(crate) id: u32,
    pub(crate) method: Method,
}

/// The request a command carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Method {
    Connect(ConnectRequest),
    Refresh(RefreshRequest),
}

/// `ConnectRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnectRequest {
    pub(crate) token: String,
    pub(crate) name: String,
    pub(crate) version: String,
}

/// `RefreshRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshRequest {
    pub(crate) token: String,
}

/// A reply or push received from the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reply {
    /// Id of the command replied to; 0 for pushes and pings.
    pub(crate) id: u32,
    pub(crate) error: Option<Error>,
    pub(crate) result: ReplyResult,
}

/// What a reply carries besides its id and error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplyResult {
    Connect(ConnectResult),
    Refresh(RefreshResult),
    Push(Push),
    /// An empty reply: a server ping, or a reply carrying only an error.
    Empty,
    /// A result of a method the client does not implement yet.
    Unsupported,
}

/// `Error` of `client.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Error {
    pub(crate) code: u32,
    pub(crate) message: String,
    pub(crate) temporary: bool,
}

/// `ConnectResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnectResult {
    pub(crate) client: String,
    pub(crate) version: String,
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
    pub(crate) ping: u32,
    pub(crate) pong: bool,
}

/// `RefreshResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshResult {
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
}

/// `Push` of `client.proto`, for the push types implemented so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Push {
    Disconnect(Disconnect),
    Unsupported,
}

/// `Disconnect` of `client.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Disconnect {
    pub(crate) code: u32,
    pub(crate) reason: String,
}
