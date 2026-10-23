//! Codec-independent model of the client protocol messages the client uses.
//!
//! Mirrors the subset of `proto/client.proto` implemented so far. Codecs
//! translate between these types and bytes on the wire. Publications use
//! the public [`Publication`] type, filled as they are delivered.

use crate::Publication;
use crate::token::Secret;

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
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    // Sent once subscription tokens are refreshed (Block 10b); `expect`
    // fails then, so the attribute goes with it.
    #[cfg_attr(not(test), expect(dead_code))]
    SubRefresh(SubRefreshRequest),
    /// The empty command answering a server ping; sent with id 0.
    Pong,
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

/// `SubscribeRequest` of `client.proto`, for the options implemented so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubscribeRequest {
    pub(crate) channel: String,
    /// Subscription token; empty for none.
    pub(crate) token: Secret,
    /// Raw JSON with the JSON protocol, any bytes with Protobuf.
    pub(crate) data: Option<Vec<u8>>,
}

/// `UnsubscribeRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UnsubscribeRequest {
    pub(crate) channel: String,
}

/// `SubRefreshRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubRefreshRequest {
    pub(crate) channel: String,
    pub(crate) token: Secret,
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
    Subscribe(SubscribeResult),
    Unsubscribe,
    /// `SubRefreshResult`, which has the fields of a `RefreshResult`.
    SubRefresh(RefreshResult),
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
    /// Session and node of an emulated connection, which emulation
    /// requests must name; empty otherwise.
    pub(crate) session: String,
    pub(crate) node: String,
}

/// `RefreshResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshResult {
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
}

/// `SubscribeResult` of `client.proto`, for the fields implemented so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubscribeResult {
    /// The subscription token expires, in `ttl` seconds.
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) publications: Vec<Publication>,
}

/// `Push` of `client.proto`, for the push types implemented so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Push {
    Disconnect(Disconnect),
    Publication {
        channel: String,
        publication: Publication,
    },
    /// The server unsubscribed the client from `channel` (`Unsubscribe`).
    Unsubscribe {
        channel: String,
        code: u32,
        reason: String,
    },
    Unsupported,
}

/// `Disconnect` of `client.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Disconnect {
    pub(crate) code: u32,
    pub(crate) reason: String,
}
