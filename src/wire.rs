//! Codec-independent model of the client protocol messages the client uses.
//!
//! Mirrors the subset of `proto/client.proto` implemented so far. Codecs
//! translate between these types and bytes on the wire. Publications use
//! the public [`Publication`] type, filled as they are delivered.

use std::collections::{BTreeMap, HashMap};

use crate::token::Secret;
use crate::{ClientInfo, Publication};

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
    SubRefresh(SubRefreshRequest),
    Rpc(RpcRequest),
    Publish(PublishRequest),
    History(HistoryRequest),
    Presence(ChannelRequest),
    PresenceStats(ChannelRequest),
    /// `SendRequest`; sent with id 0, as no reply follows.
    Send(Vec<u8>),
    /// The empty command answering a server ping; sent with id 0.
    Pong,
}

/// `ConnectRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnectRequest {
    pub(crate) token: String,
    /// The application's connect data, if any (Block 17).
    pub(crate) data: Option<Vec<u8>>,
    /// Emulated headers, by name (`ConnectRequest.headers`, Block 17).
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) name: String,
    pub(crate) version: String,
    /// Server-side subscriptions to recover, by channel: only `recover`,
    /// `epoch` and `offset` are set (`client.proto`, `ConnectRequest.subs`).
    pub(crate) subs: BTreeMap<String, SubscribeRequest>,
}

/// `RefreshRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshRequest {
    pub(crate) token: String,
}

/// `SubscribeRequest` of `client.proto`, for the options implemented so far.
#[allow(clippy::struct_excessive_bools)] // The flags of client.proto's SubscribeRequest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubscribeRequest {
    pub(crate) channel: String,
    /// Subscription token; empty for none.
    pub(crate) token: Secret,
    /// Raw JSON with the JSON protocol, any bytes with Protobuf.
    pub(crate) data: Option<Vec<u8>>,
    /// Asks to recover the publications after `epoch` and `offset`.
    pub(crate) recover: bool,
    pub(crate) epoch: String,
    pub(crate) offset: u64,
    /// Ask for these capabilities on a channel that does not force them.
    pub(crate) positioned: bool,
    pub(crate) recoverable: bool,
    /// Bits of `SubscribeRequest.flag`, e.g. [`REJECT_UNRECOVERED`].
    pub(crate) flag: i64,
    /// Asks the server for join and leave pushes.
    pub(crate) join_leave: bool,
    /// Asks for publications as Fossil deltas (Block 19, D4), sent as
    /// `SubscribeRequest.delta = "fossil"`.
    pub(crate) delta: bool,
    /// Asks the server to send only the publications it matches
    /// (`SubscribeRequest.tf`, Block 35a).
    pub(crate) tag_filter: Option<crate::TagFilter>,
}

/// The only delta format of `client.proto` (`SubscribeRequest.delta`).
pub(crate) const FOSSIL: &str = "fossil";

/// Fail a subscribe whose recovery is not possible with error 112 instead
/// of succeeding unrecovered (`client.proto`, `SubscribeRequest.flag`).
pub(crate) const REJECT_UNRECOVERED: i64 = 1 << 1;

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

/// `RPCRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RpcRequest {
    /// Routes the call; empty for none.
    pub(crate) method: String,
    /// Raw JSON with the JSON protocol, any bytes with Protobuf.
    pub(crate) data: Vec<u8>,
}

/// `PublishRequest` of `client.proto`, for regular publications.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PublishRequest {
    pub(crate) channel: String,
    /// Raw JSON with the JSON protocol, any bytes with Protobuf.
    pub(crate) data: Vec<u8>,
}

/// `HistoryRequest` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryRequest {
    pub(crate) channel: String,
    /// 0 returns only the stream position, -1 every publication.
    pub(crate) limit: i32,
    /// Publications after this offset and epoch (before it with
    /// `reverse`).
    pub(crate) since: Option<(u64, String)>,
    pub(crate) reverse: bool,
}

/// `PresenceRequest` and `PresenceStatsRequest` of `client.proto`, which
/// carry only the channel.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChannelRequest {
    pub(crate) channel: String,
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
    /// `RPCResult.data`: raw JSON with the JSON protocol, any bytes with
    /// Protobuf; empty when the server sent none.
    Rpc(Vec<u8>),
    /// `PublishResult`, which is empty.
    Publish,
    History(HistoryResult),
    /// `PresenceResult.presence`: the clients by client id.
    Presence(HashMap<String, ClientInfo>),
    PresenceStats(PresenceStatsResult),
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
    /// The server-side subscriptions of the connection, by channel, in
    /// channel order.
    pub(crate) subs: BTreeMap<String, SubscribeResult>,
    /// The data the server attached, exactly as received (Block 17).
    pub(crate) data: Option<Vec<u8>>,
}

/// `HistoryResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryResult {
    pub(crate) publications: Vec<Publication>,
    /// The stream's current epoch and top offset.
    pub(crate) epoch: String,
    pub(crate) offset: u64,
}

/// `PresenceStatsResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PresenceStatsResult {
    pub(crate) num_clients: u32,
    pub(crate) num_users: u32,
}

/// `RefreshResult` of `client.proto`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshResult {
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
}

/// `SubscribeResult` of `client.proto`, for the fields implemented so far.
#[allow(clippy::struct_excessive_bools)] // The flags of client.proto's SubscribeResult.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubscribeResult {
    /// The subscription token expires, in `ttl` seconds.
    pub(crate) expires: bool,
    pub(crate) ttl: u32,
    pub(crate) data: Option<Vec<u8>>,
    /// Recovered publications, or initial ones of a subscribe that does not
    /// recover.
    pub(crate) publications: Vec<Publication>,
    /// The capabilities the server granted.
    pub(crate) positioned: bool,
    pub(crate) recoverable: bool,
    /// The stream's epoch, and its offset: the top of the stream, or after a
    /// recovery the position its publications advance from.
    pub(crate) epoch: String,
    pub(crate) offset: u64,
    /// The server attempted a recovery, and whether it recovered everything.
    pub(crate) was_recovering: bool,
    pub(crate) recovered: bool,
    /// Publications come as Fossil deltas until the next subscribe
    /// (`SubscribeResult.delta`, Block 19, D4).
    pub(crate) delta: bool,
}

/// `Push` of `client.proto`, for the push types implemented so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Push {
    Disconnect(Disconnect),
    Publication {
        channel: String,
        publication: Publication,
    },
    /// The server subscribed the connection to `channel` during the
    /// connection (`Subscribe`): a server-side subscription. Its `result`
    /// has the fields of `client.proto`'s `Subscribe`, the others at their
    /// defaults.
    Subscribe {
        channel: String,
        result: SubscribeResult,
    },
    /// The server unsubscribed the client from `channel` (`Unsubscribe`).
    Unsubscribe {
        channel: String,
        code: u32,
        reason: String,
    },
    /// An asynchronous message (`Message.data`): raw JSON with the JSON
    /// protocol, any bytes with Protobuf.
    Message(Vec<u8>),
    /// A client subscribed to `channel` (`Join.info`).
    Join {
        channel: String,
        info: ClientInfo,
    },
    /// A client unsubscribed from `channel` (`Leave.info`).
    Leave {
        channel: String,
        info: ClientInfo,
    },
    Unsupported,
}

/// `Disconnect` of `client.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Disconnect {
    pub(crate) code: u32,
    pub(crate) reason: String,
}
