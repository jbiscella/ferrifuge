//! JSON codec of the client protocol.
//!
//! Field names follow `proto/client.proto`. Several messages in one frame
//! are separated by a newline; empty fields are omitted on encode and
//! default on decode.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::wire::{
    ChannelRequest, Command, ConnectRequest, ConnectResult, Disconnect, Error, HistoryRequest,
    HistoryResult, Method, PresenceStatsResult, PublishRequest, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult, RpcRequest, SubRefreshRequest, SubscribeRequest,
    SubscribeResult, UnsubscribeRequest,
};
use crate::{ClientInfo, Publication};

/// A frame that is not valid JSON client protocol.
#[derive(Debug)]
pub(crate) struct DecodeError(serde_json::Error);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed JSON frame: {}", self.0)
    }
}

/// Encodes commands into one frame.
pub(crate) fn encode(commands: &[Command]) -> Vec<u8> {
    let mut frame = Vec::new();
    for (i, command) in commands.iter().enumerate() {
        if i > 0 {
            frame.push(b'\n');
        }
        serde_json::to_writer(&mut frame, &JsonCommand::from(command))
            .expect("serializing plain structs to a Vec cannot fail");
    }
    frame
}

/// Ids of the commands of `frame`, one this client encoded; 0 for a
/// command without one.
pub(crate) fn command_ids(frame: &[u8]) -> Vec<u32> {
    #[derive(Deserialize)]
    struct Id {
        #[serde(default)]
        id: u32,
    }
    frame
        .split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<Id>(line).ok())
        .map(|command| command.id)
        .collect()
}

/// Decodes every reply in a frame.
#[cfg(test)]
pub(crate) fn decode(frame: &[u8]) -> Result<Vec<Reply>, DecodeError> {
    Ok(decode_sized(frame)?
        .into_iter()
        .map(|(reply, _)| reply)
        .collect())
}

/// Decodes every reply in a frame, each with its encoded size.
pub(crate) fn decode_sized(frame: &[u8]) -> Result<Vec<(Reply, usize)>, DecodeError> {
    let mut stream =
        serde_json::Deserializer::from_slice(frame).into_iter::<BTreeMap<String, Box<RawValue>>>();
    let (mut replies, mut start) = (Vec::new(), 0);
    while let Some(fields) = stream.next() {
        let reply = reply(fields.map_err(DecodeError)?)?;
        let end = stream.byte_offset();
        replies.push((reply, end - start));
        start = end;
    }
    Ok(replies)
}

/// Body of a request to the emulation endpoint carrying `frame` for the
/// emulated connection `session` on `node` (centrifuge-js
/// `transport_http_stream.ts`, `send`). The commands travel as a JSON
/// string: Centrifugo v6.10.1 rejects them embedded as JSON values.
pub(crate) fn emulation_request(session: &str, node: &str, frame: &[u8]) -> Vec<u8> {
    #[derive(Serialize)]
    struct EmulationRequest<'a> {
        session: &'a str,
        node: &'a str,
        data: std::borrow::Cow<'a, str>,
    }
    serde_json::to_vec(&EmulationRequest {
        session,
        node,
        // JSON frames are UTF-8: the client encodes them.
        data: String::from_utf8_lossy(frame),
    })
    .expect("serializing plain structs to a Vec cannot fail")
}

/// The frame an [`emulation_request`] body carries; `None` for a body that
/// is not one.
pub(crate) fn emulation_frame(body: &[u8]) -> Option<Vec<u8>> {
    #[derive(Deserialize)]
    struct EmulationRequest {
        data: String,
    }
    serde_json::from_slice::<EmulationRequest>(body)
        .ok()
        .map(|request| request.data.into_bytes())
}

/// One field per request type; a command sets its own and leaves the
/// others at their default, so adding a command touches one place.
#[derive(Serialize, Default)]
struct JsonCommand<'a> {
    #[serde(skip_serializing_if = "is_zero")]
    id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect: Option<JsonConnectRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh: Option<JsonRefreshRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscribe: Option<JsonSubscribeRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsubscribe: Option<JsonUnsubscribeRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_refresh: Option<JsonSubRefreshRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc: Option<JsonRpcRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publish: Option<JsonPublishRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<JsonHistoryRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<JsonChannelRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence_stats: Option<JsonChannelRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    send: Option<JsonSendRequest<'a>>,
}

#[derive(Serialize)]
struct JsonRefreshRequest<'a> {
    token: &'a str,
}

#[derive(Serialize)]
struct JsonConnectRequest<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<JsonData<'a>>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    headers: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "str::is_empty")]
    name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    version: &'a str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    subs: BTreeMap<&'a str, JsonConnectSub<'a>>,
}

/// An entry of `ConnectRequest.subs`: where to recover a server-side
/// subscription from.
#[derive(Serialize)]
struct JsonConnectSub<'a> {
    #[serde(skip_serializing_if = "is_false")]
    recover: bool,
    #[serde(skip_serializing_if = "str::is_empty")]
    epoch: &'a str,
    #[serde(skip_serializing_if = "is_zero_u64")]
    offset: u64,
}

#[allow(clippy::struct_excessive_bools)] // The flags of client.proto's SubscribeRequest.
#[derive(Serialize)]
struct JsonSubscribeRequest<'a> {
    channel: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<JsonData<'a>>,
    #[serde(skip_serializing_if = "is_false")]
    recover: bool,
    #[serde(skip_serializing_if = "str::is_empty")]
    epoch: &'a str,
    #[serde(skip_serializing_if = "is_zero_u64")]
    offset: u64,
    #[serde(skip_serializing_if = "is_false")]
    positioned: bool,
    #[serde(skip_serializing_if = "is_false")]
    recoverable: bool,
    #[serde(skip_serializing_if = "is_zero_i64")]
    flag: i64,
    #[serde(skip_serializing_if = "is_false")]
    join_leave: bool,
}

#[derive(Serialize)]
struct JsonUnsubscribeRequest<'a> {
    channel: &'a str,
}

#[derive(Serialize)]
struct JsonSubRefreshRequest<'a> {
    channel: &'a str,
    token: &'a str,
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    method: &'a str,
    data: JsonData<'a>,
}

#[derive(Serialize)]
struct JsonPublishRequest<'a> {
    channel: &'a str,
    data: JsonData<'a>,
}

#[derive(Serialize)]
struct JsonHistoryRequest<'a> {
    channel: &'a str,
    #[serde(skip_serializing_if = "is_zero_i32")]
    limit: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    since: Option<JsonStreamPosition<'a>>,
    #[serde(skip_serializing_if = "is_false")]
    reverse: bool,
}

/// A `StreamPosition`: its offset is always sent, as centrifuge-js does.
#[derive(Serialize)]
struct JsonStreamPosition<'a> {
    offset: u64,
    #[serde(skip_serializing_if = "str::is_empty")]
    epoch: &'a str,
}

#[derive(Serialize)]
struct JsonChannelRequest<'a> {
    channel: &'a str,
}

#[derive(Serialize)]
struct JsonSendRequest<'a> {
    data: JsonData<'a>,
}

/// Application data, embedded as the JSON it is. Bytes that are not JSON
/// travel as a JSON string of their text instead of breaking the frame.
#[derive(Serialize)]
#[serde(untagged)]
enum JsonData<'a> {
    Raw(&'a RawValue),
    Text(std::borrow::Cow<'a, str>),
}

impl<'a> JsonData<'a> {
    fn new(data: &'a [u8]) -> Self {
        serde_json::from_slice(data)
            .map_or_else(|_| Self::Text(String::from_utf8_lossy(data)), Self::Raw)
    }
}

impl<'a> From<&'a Command> for JsonCommand<'a> {
    #[allow(clippy::too_many_lines)] // One arm per method.
    fn from(command: &'a Command) -> Self {
        let mut json = Self {
            id: command.id,
            ..Self::default()
        };
        match &command.method {
            Method::Connect(ConnectRequest {
                token,
                data,
                headers,
                name,
                version,
                subs,
            }) => {
                json.connect = Some(JsonConnectRequest {
                    token,
                    data: data.as_deref().map(JsonData::new),
                    headers,
                    name,
                    version,
                    subs: subs
                        .iter()
                        .map(|(channel, sub)| {
                            (
                                channel.as_str(),
                                JsonConnectSub {
                                    recover: sub.recover,
                                    epoch: &sub.epoch,
                                    offset: sub.offset,
                                },
                            )
                        })
                        .collect(),
                });
            }
            Method::Refresh(RefreshRequest { token }) => {
                json.refresh = Some(JsonRefreshRequest { token });
            }
            Method::Subscribe(SubscribeRequest {
                channel,
                token,
                data,
                recover,
                epoch,
                offset,
                positioned,
                recoverable,
                flag,
                join_leave,
            }) => {
                json.subscribe = Some(JsonSubscribeRequest {
                    channel,
                    token: &token.0,
                    data: data.as_deref().map(JsonData::new),
                    recover: *recover,
                    epoch,
                    offset: *offset,
                    positioned: *positioned,
                    recoverable: *recoverable,
                    flag: *flag,
                    join_leave: *join_leave,
                });
            }
            Method::Unsubscribe(UnsubscribeRequest { channel }) => {
                json.unsubscribe = Some(JsonUnsubscribeRequest { channel });
            }
            Method::SubRefresh(SubRefreshRequest { channel, token }) => {
                json.sub_refresh = Some(JsonSubRefreshRequest {
                    channel,
                    token: &token.0,
                });
            }
            Method::Rpc(RpcRequest { method, data }) => {
                json.rpc = Some(JsonRpcRequest {
                    method,
                    data: JsonData::new(data),
                });
            }
            Method::Publish(PublishRequest { channel, data }) => {
                json.publish = Some(JsonPublishRequest {
                    channel,
                    data: JsonData::new(data),
                });
            }
            Method::History(HistoryRequest {
                channel,
                limit,
                since,
                reverse,
            }) => {
                json.history = Some(JsonHistoryRequest {
                    channel,
                    limit: *limit,
                    since: since.as_ref().map(|(offset, epoch)| JsonStreamPosition {
                        offset: *offset,
                        epoch,
                    }),
                    reverse: *reverse,
                });
            }
            Method::Presence(ChannelRequest { channel }) => {
                json.presence = Some(JsonChannelRequest { channel });
            }
            Method::PresenceStats(ChannelRequest { channel }) => {
                json.presence_stats = Some(JsonChannelRequest { channel });
            }
            Method::Send(data) => {
                json.send = Some(JsonSendRequest {
                    data: JsonData::new(data),
                });
            }
            // A pong is the empty command: no request field.
            Method::Pong => {}
        }
        json
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Signature required by skip_serializing_if.
fn is_zero(n: &u32) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Signature required by skip_serializing_if.
fn is_zero_i32(n: &i32) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Signature required by skip_serializing_if.
fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Signature required by skip_serializing_if.
fn is_zero_i64(n: &i64) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Signature required by skip_serializing_if.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Decodes one reply from its top-level fields, kept as raw JSON so that
/// application data reaches the application byte for byte.
fn reply(fields: BTreeMap<String, Box<RawValue>>) -> Result<Reply, DecodeError> {
    let mut id = 0;
    let mut error = None;
    let mut result = None;
    let mut unknown = false;
    for (name, value) in fields {
        match name.as_str() {
            "id" => id = field(&value)?,
            "error" => error = Some(field::<JsonError>(&value)?.into()),
            "connect" => {
                result = Some(ReplyResult::Connect(
                    field::<JsonConnectResult>(&value)?.into(),
                ));
            }
            "refresh" => {
                let refresh: JsonRefreshResult = field(&value)?;
                result = Some(ReplyResult::Refresh(RefreshResult {
                    expires: refresh.expires,
                    ttl: refresh.ttl,
                }));
            }
            "subscribe" => {
                let subscribe: JsonSubscribeResult = field(&value)?;
                result = Some(ReplyResult::Subscribe(subscribe.into()));
            }
            "unsubscribe" => {
                // An object, even empty: `null` or a scalar is no result.
                field::<JsonUnsubscribeResult>(&value)?;
                result = Some(ReplyResult::Unsubscribe);
            }
            "sub_refresh" => {
                let refresh: JsonRefreshResult = field(&value)?;
                result = Some(ReplyResult::SubRefresh(RefreshResult {
                    expires: refresh.expires,
                    ttl: refresh.ttl,
                }));
            }
            "rpc" => {
                let rpc: JsonRpcResult = field(&value)?;
                result = Some(ReplyResult::Rpc(
                    rpc.data.as_deref().map(raw_bytes).unwrap_or_default(),
                ));
            }
            "publish" => {
                // An object, even empty: `null` or a scalar is no result.
                field::<JsonUnsubscribeResult>(&value)?;
                result = Some(ReplyResult::Publish);
            }
            "history" => {
                let history: JsonHistoryResult = field(&value)?;
                result = Some(ReplyResult::History(HistoryResult {
                    publications: history
                        .publications
                        .into_iter()
                        .map(Publication::from)
                        .collect(),
                    epoch: history.epoch,
                    offset: history.offset,
                }));
            }
            "presence" => {
                let presence: JsonPresenceResult = field(&value)?;
                result = Some(ReplyResult::Presence(
                    presence
                        .presence
                        .into_iter()
                        .map(|(client, info)| (client, info.into()))
                        .collect(),
                ));
            }
            "presence_stats" => {
                let stats: JsonPresenceStatsResult = field(&value)?;
                result = Some(ReplyResult::PresenceStats(PresenceStatsResult {
                    num_clients: stats.num_clients,
                    num_users: stats.num_users,
                }));
            }
            "push" => result = Some(ReplyResult::Push(field::<JsonPush>(&value)?.into())),
            _ => unknown = true,
        }
    }
    let result = result.unwrap_or(if unknown {
        // A result of a method the client does not implement yet.
        ReplyResult::Unsupported
    } else {
        ReplyResult::Empty
    });
    Ok(Reply { id, error, result })
}

fn field<'a, T: Deserialize<'a>>(value: &'a RawValue) -> Result<T, DecodeError> {
    serde_json::from_str(value.get()).map_err(DecodeError)
}

fn raw_bytes(value: &RawValue) -> Vec<u8> {
    value.get().as_bytes().to_vec()
}

#[derive(Deserialize)]
struct JsonError {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    message: String,
    #[serde(default)]
    temporary: bool,
}

impl From<JsonError> for Error {
    fn from(error: JsonError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            temporary: error.temporary,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JsonConnectResult {
    client: String,
    version: String,
    expires: bool,
    ttl: u32,
    ping: u32,
    pong: bool,
    session: String,
    node: String,
    subs: BTreeMap<String, JsonSubscribeResult>,
    #[serde(deserialize_with = "present")]
    data: Option<Box<RawValue>>,
}

impl From<JsonConnectResult> for ConnectResult {
    fn from(connect: JsonConnectResult) -> Self {
        Self {
            client: connect.client,
            version: connect.version,
            expires: connect.expires,
            ttl: connect.ttl,
            ping: connect.ping,
            pong: connect.pong,
            session: connect.session,
            node: connect.node,
            subs: connect
                .subs
                .into_iter()
                .map(|(channel, result)| (channel, result.into()))
                .collect(),
            data: connect.data.as_deref().map(raw_bytes),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JsonRefreshResult {
    expires: bool,
    ttl: u32,
}

#[allow(clippy::struct_excessive_bools)] // The flags of client.proto's SubscribeResult.
#[derive(Deserialize, Default)]
#[serde(default)]
struct JsonSubscribeResult {
    expires: bool,
    ttl: u32,
    #[serde(deserialize_with = "present")]
    data: Option<Box<RawValue>>,
    publications: Vec<JsonPublication>,
    positioned: bool,
    recoverable: bool,
    epoch: String,
    offset: u64,
    was_recovering: bool,
    recovered: bool,
}

impl From<JsonSubscribeResult> for SubscribeResult {
    fn from(subscribe: JsonSubscribeResult) -> Self {
        Self {
            expires: subscribe.expires,
            ttl: subscribe.ttl,
            data: subscribe.data.as_deref().map(raw_bytes),
            publications: subscribe
                .publications
                .into_iter()
                .map(Publication::from)
                .collect(),
            positioned: subscribe.positioned,
            recoverable: subscribe.recoverable,
            epoch: subscribe.epoch,
            offset: subscribe.offset,
            was_recovering: subscribe.was_recovering,
            recovered: subscribe.recovered,
        }
    }
}

#[derive(Deserialize)]
struct JsonUnsubscribeResult {}

#[derive(Deserialize)]
struct JsonHistoryResult {
    #[serde(default)]
    publications: Vec<JsonPublication>,
    #[serde(default)]
    epoch: String,
    #[serde(default)]
    offset: u64,
}

#[derive(Deserialize)]
struct JsonPresenceResult {
    #[serde(default)]
    presence: HashMap<String, JsonClientInfo>,
}

#[derive(Deserialize)]
struct JsonPresenceStatsResult {
    #[serde(default)]
    num_clients: u32,
    #[serde(default)]
    num_users: u32,
}

#[derive(Deserialize)]
struct JsonRpcResult {
    #[serde(default, deserialize_with = "present")]
    data: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
struct JsonPush {
    #[serde(default)]
    channel: String,
    #[serde(rename = "pub")]
    publication: Option<JsonPublication>,
    disconnect: Option<JsonDisconnect>,
    unsubscribe: Option<JsonDisconnect>,
    // `Subscribe` has a subset of the fields of `SubscribeResult`.
    subscribe: Option<JsonSubscribeResult>,
    message: Option<JsonMessage>,
    join: Option<JsonJoinLeave>,
    leave: Option<JsonJoinLeave>,
}

/// `Join` and `Leave`, which carry the same field.
#[derive(Deserialize)]
struct JsonJoinLeave {
    info: Option<JsonClientInfo>,
}

#[derive(Deserialize)]
struct JsonMessage {
    #[serde(default, deserialize_with = "present")]
    data: Option<Box<RawValue>>,
}

impl From<JsonPush> for Push {
    fn from(push: JsonPush) -> Self {
        if let Some(publication) = push.publication {
            Self::Publication {
                channel: push.channel,
                publication: publication.into(),
            }
        } else if let Some(JsonDisconnect { code, reason }) = push.disconnect {
            Self::Disconnect(Disconnect { code, reason })
        } else if let Some(subscribe) = push.subscribe {
            Self::Subscribe {
                channel: push.channel,
                result: subscribe.into(),
            }
        } else if let Some(JsonDisconnect { code, reason }) = push.unsubscribe {
            // `Unsubscribe` has the code and reason fields of `Disconnect`.
            Self::Unsubscribe {
                channel: push.channel,
                code,
                reason,
            }
        } else if let Some(message) = push.message {
            Self::Message(message.data.as_deref().map(raw_bytes).unwrap_or_default())
        } else if let Some(join) = push.join {
            Self::Join {
                channel: push.channel,
                info: join.info.map(ClientInfo::from).unwrap_or_default(),
            }
        } else if let Some(leave) = push.leave {
            Self::Leave {
                channel: push.channel,
                info: leave.info.map(ClientInfo::from).unwrap_or_default(),
            }
        } else {
            Self::Unsupported
        }
    }
}

#[derive(Deserialize)]
struct JsonDisconnect {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
}

#[derive(Deserialize)]
struct JsonPublication {
    #[serde(default, deserialize_with = "present")]
    data: Option<Box<RawValue>>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    tags: HashMap<String, String>,
    info: Option<JsonClientInfo>,
    #[serde(default)]
    epoch: String,
}

impl From<JsonPublication> for Publication {
    fn from(publication: JsonPublication) -> Self {
        Self {
            data: publication
                .data
                .as_deref()
                .map(raw_bytes)
                .unwrap_or_default(),
            offset: (publication.offset > 0).then_some(publication.offset),
            tags: publication.tags,
            info: publication.info.map(ClientInfo::from),
            epoch: (!publication.epoch.is_empty()).then_some(publication.epoch),
        }
    }
}

impl From<JsonClientInfo> for ClientInfo {
    fn from(info: JsonClientInfo) -> Self {
        Self {
            client: info.client,
            user: info.user,
            conn_info: info.conn_info.as_deref().map(raw_bytes),
            chan_info: info.chan_info.as_deref().map(raw_bytes),
        }
    }
}

#[derive(Deserialize)]
struct JsonClientInfo {
    #[serde(default)]
    client: String,
    #[serde(default)]
    user: String,
    #[serde(default, deserialize_with = "present")]
    conn_info: Option<Box<RawValue>>,
    #[serde(default, deserialize_with = "present")]
    chan_info: Option<Box<RawValue>>,
}

/// A field that is present, whatever its value: an explicit `null` is data
/// too (`Option` alone would take it for absent).
fn present<'de, D>(deserializer: D) -> Result<Option<Box<RawValue>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Secret;
    use crate::wire::REJECT_UNRECOVERED;

    /// Fuzz target (Block 14e): any bytes decode or fail, never panic.
    /// Random inputs under `cargo test`; coverage-guided with
    /// `cargo bolero test json::tests::fuzz_decode --engine libfuzzer`.
    #[test]
    fn fuzz_decode() {
        bolero::check!().for_each(|frame: &[u8]| {
            let _ = decode(frame);
        });
    }

    fn connect_command(id: u32, token: &str) -> Command {
        Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: token.to_owned(),
                data: None,
                headers: BTreeMap::new(),
                name: "ferrifuge".to_owned(),
                version: "0.1.0".to_owned(),
                subs: BTreeMap::new(),
            }),
        }
    }

    #[test]
    fn encodes_the_server_side_subscriptions_to_recover_in_a_connect_command() {
        let mut command = connect_command(1, "");
        let Method::Connect(connect) = &mut command.method else {
            unreachable!("a connect command");
        };
        connect.subs.insert(
            "alerts".into(),
            SubscribeRequest {
                recover: true,
                epoch: "e".into(),
                offset: 5,
                ..SubscribeRequest::default()
            },
        );

        let frame = encode(&[command]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":1,"connect":{"name":"ferrifuge","version":"0.1.0","subs":{"alerts":{"recover":true,"epoch":"e","offset":5}}}}"#
        );
    }

    #[test]
    fn encodes_an_rpc_with_its_data_embedded_and_decodes_its_result_exactly() {
        let frame = encode(&[Command {
            id: 4,
            method: Method::Rpc(RpcRequest {
                method: "echo".into(),
                data: br#"{"n":1}"#.to_vec(),
            }),
        }]);
        let replies =
            decode(b"{\"id\":4,\"rpc\":{\"data\":{\"b\": 1.0,  \"a\":2}}}\n{\"id\":5,\"rpc\":{}}")
                .unwrap();

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":4,"rpc":{"method":"echo","data":{"n":1}}}"#
        );
        assert_eq!(
            replies[0].result,
            ReplyResult::Rpc(br#"{"b": 1.0,  "a":2}"#.to_vec())
        );
        assert_eq!(replies[1].result, ReplyResult::Rpc(Vec::new()));
    }

    #[test]
    fn encodes_publish_history_presence_and_presence_stats() {
        let commands = [
            Method::Publish(PublishRequest {
                channel: "news".into(),
                data: br#"{"text":"hi"}"#.to_vec(),
            }),
            Method::History(HistoryRequest {
                channel: "news".into(),
                limit: 2,
                since: Some((1, "e".into())),
                reverse: true,
            }),
            Method::History(HistoryRequest {
                channel: "news".into(),
                ..HistoryRequest::default()
            }),
            Method::Presence(ChannelRequest {
                channel: "news".into(),
            }),
            Method::PresenceStats(ChannelRequest {
                channel: "news".into(),
            }),
        ]
        .into_iter()
        .zip(1..)
        .map(|(method, id)| Command { id, method })
        .collect::<Vec<_>>();

        let frame = String::from_utf8(encode(&commands)).unwrap();

        assert_eq!(
            frame.lines().collect::<Vec<_>>(),
            [
                r#"{"id":1,"publish":{"channel":"news","data":{"text":"hi"}}}"#,
                r#"{"id":2,"history":{"channel":"news","limit":2,"since":{"offset":1,"epoch":"e"},"reverse":true}}"#,
                r#"{"id":3,"history":{"channel":"news"}}"#,
                r#"{"id":4,"presence":{"channel":"news"}}"#,
                r#"{"id":5,"presence_stats":{"channel":"news"}}"#,
            ]
        );
    }

    #[test]
    fn decodes_publish_history_presence_and_presence_stats_results() {
        let replies = decode(
            concat!(
                r#"{"id":1,"publish":{}}"#,
                "\n",
                r#"{"id":2,"history":{"publications":[{"data":{"b": 1.0,  "a":2},"offset":3}],"epoch":"e","offset":3}}"#,
                "\n",
                r#"{"id":3,"presence":{"presence":{"c1":{"user":"u1","client":"c1","conn_info":{"x":1},"chan_info":{"y":2}}}}}"#,
                "\n",
                r#"{"id":4,"presence_stats":{"num_clients":2,"num_users":1}}"#,
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(replies[0].result, ReplyResult::Publish);
        let ReplyResult::History(history) = &replies[1].result else {
            panic!("{:?}", replies[1]);
        };
        assert_eq!((history.epoch.as_str(), history.offset), ("e", 3));
        assert_eq!(history.publications[0].data, br#"{"b": 1.0,  "a":2}"#);
        assert_eq!(history.publications[0].offset, Some(3));
        let ReplyResult::Presence(presence) = &replies[2].result else {
            panic!("{:?}", replies[2]);
        };
        let client = &presence["c1"];
        assert_eq!((client.user.as_str(), client.client.as_str()), ("u1", "c1"));
        assert_eq!(client.conn_info.as_deref(), Some(&br#"{"x":1}"#[..]));
        assert_eq!(client.chan_info.as_deref(), Some(&br#"{"y":2}"#[..]));
        assert_eq!(
            replies[3].result,
            ReplyResult::PresenceStats(PresenceStatsResult {
                num_clients: 2,
                num_users: 1
            })
        );
    }

    #[test]
    fn encodes_send_without_an_id_and_decodes_a_message_push_exactly() {
        let frame = encode(&[Command {
            id: 0,
            method: Method::Send(br#"{"note":"x"}"#.to_vec()),
        }]);
        let replies = decode(br#"{"push":{"message":{"data":{"b": 1.0,  "a":2}}}}"#).unwrap();

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"send":{"data":{"note":"x"}}}"#
        );
        assert_eq!(
            replies[0].result,
            ReplyResult::Push(Push::Message(br#"{"b": 1.0,  "a":2}"#.to_vec()))
        );
    }

    #[test]
    fn encodes_join_leave_and_decodes_join_and_leave_pushes() {
        let frame = encode(&[Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                join_leave: true,
                ..SubscribeRequest::default()
            }),
        }]);
        let replies = decode(
            concat!(
                r#"{"push":{"channel":"news","join":{"info":{"user":"u","client":"c","conn_info":{"a": 1},"chan_info":{"b":2}}}}}"#,
                "\n",
                r#"{"push":{"channel":"news","leave":{"info":{"user":"u","client":"c"}}}}"#,
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":2,"subscribe":{"channel":"news","join_leave":true}}"#
        );
        let ReplyResult::Push(Push::Join { channel, info }) = &replies[0].result else {
            panic!("{:?}", replies[0]);
        };
        assert_eq!(
            (channel.as_str(), info.user.as_str(), info.client.as_str()),
            ("news", "u", "c")
        );
        assert_eq!(info.conn_info.as_deref(), Some(&br#"{"a": 1}"#[..]));
        assert_eq!(info.chan_info.as_deref(), Some(&br#"{"b":2}"#[..]));
        assert!(matches!(
            &replies[1].result,
            ReplyResult::Push(Push::Leave { channel, info }) if channel == "news" && info.client == "c"
        ));
    }

    #[test]
    fn encodes_connect_command_omitting_empty_fields() {
        let frame = encode(&[connect_command(1, "")]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":1,"connect":{"name":"ferrifuge","version":"0.1.0"}}"#
        );
    }

    #[test]
    fn encodes_several_commands_newline_separated() {
        let frame = encode(&[connect_command(1, "t1"), connect_command(2, "t2")]);

        let text = String::from_utf8(frame).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(text.lines().nth(1).unwrap().contains(r#""token":"t2""#));
    }

    #[test]
    fn decodes_a_subscribe_push() {
        let replies = decode(
            br#"{"push":{"channel":"alerts","subscribe":{"recoverable":true,"positioned":true,"epoch":"e","offset":3,"data":{"tier":"gold"}}}}"#,
        )
        .unwrap();

        let ReplyResult::Push(Push::Subscribe { channel, result }) = &replies[0].result else {
            panic!("{replies:?}");
        };
        assert_eq!(channel, "alerts");
        assert!(result.recoverable && result.positioned);
        assert_eq!((result.epoch.as_str(), result.offset), ("e", 3));
        assert_eq!(result.data.as_deref(), Some(&br#"{"tier":"gold"}"#[..]));
    }

    #[test]
    fn decodes_the_server_side_subscriptions_of_a_connect_reply() {
        let replies = decode(
            concat!(
                r#"{"id":1,"connect":{"client":"c","subs":{"#,
                r#""b":{"recoverable":true,"epoch":"e","offset":2,"publications":[{"data":{},"offset":2}]},"#,
                r#""a":{"data":{"x":1}}}}}"#
            )
            .as_bytes(),
        )
        .unwrap();

        let ReplyResult::Connect(result) = &replies[0].result else {
            panic!("{replies:?}");
        };
        let channels: Vec<_> = result.subs.keys().map(String::as_str).collect();
        assert_eq!(channels, ["a", "b"]);
        assert_eq!(result.subs["a"].data.as_deref(), Some(&br#"{"x":1}"#[..]));
        assert!(result.subs["b"].recoverable);
        assert_eq!(result.subs["b"].publications.len(), 1);
    }

    #[test]
    fn encodes_connect_data_as_raw_json() {
        let mut command = connect_command(1, "");
        let Method::Connect(connect) = &mut command.method else {
            unreachable!("a connect command");
        };
        connect.data = Some(br#"{"device":"d1"}"#.to_vec());

        let frame = encode(&[command]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":1,"connect":{"data":{"device":"d1"},"name":"ferrifuge","version":"0.1.0"}}"#
        );
    }

    #[test]
    fn keeps_the_connect_reply_data_exactly() {
        let frame = br#"{"id":1,"connect":{"client":"c1","data":{"b": 1.0,  "a":2}}}"#;

        let replies = decode(frame).unwrap();

        let ReplyResult::Connect(result) = &replies[0].result else {
            panic!("{replies:?}");
        };
        assert_eq!(result.data.as_deref(), Some(&br#"{"b": 1.0,  "a":2}"#[..]));
    }

    #[test]
    fn decodes_connect_reply() {
        let frame = br#"{"id":1,"connect":{"client":"c1","version":"6.10.1","expires":true,"ttl":30,"ping":25,"pong":true,"session":"s","node":"n"}}"#;

        let replies = decode(frame).unwrap();

        assert_eq!(
            replies,
            [Reply {
                id: 1,
                error: None,
                result: ReplyResult::Connect(ConnectResult {
                    client: "c1".into(),
                    version: "6.10.1".into(),
                    expires: true,
                    ttl: 30,
                    ping: 25,
                    pong: true,
                    session: "s".into(),
                    node: "n".into(),
                    subs: BTreeMap::new(),
                    data: None,
                }),
            }]
        );
    }

    #[test]
    fn encodes_refresh_command() {
        let frame = encode(&[Command {
            id: 7,
            method: Method::Refresh(RefreshRequest {
                token: "t".to_owned(),
            }),
        }]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":7,"refresh":{"token":"t"}}"#
        );
    }

    #[test]
    fn encodes_pong_as_an_empty_object() {
        let frame = encode(&[Command {
            id: 0,
            method: Method::Pong,
        }]);

        assert_eq!(frame, b"{}");
    }

    #[test]
    fn decodes_refresh_reply() {
        let replies =
            decode(br#"{"id":7,"refresh":{"client":"c","version":"6","expires":true,"ttl":60}}"#)
                .unwrap();

        assert_eq!(
            replies[0].result,
            ReplyResult::Refresh(RefreshResult {
                expires: true,
                ttl: 60
            })
        );
    }

    #[test]
    fn decodes_error_reply() {
        let replies =
            decode(br#"{"id":1,"error":{"code":109,"message":"token expired","temporary":true}}"#)
                .unwrap();

        assert_eq!(
            replies[0].error,
            Some(Error {
                code: 109,
                message: "token expired".into(),
                temporary: true
            })
        );
        assert_eq!(replies[0].result, ReplyResult::Empty);
    }

    #[test]
    fn decodes_several_replies_in_one_frame() {
        let frame = b"{}\n{\"push\":{\"disconnect\":{\"code\":3001,\"reason\":\"shutdown\"}}}\n";

        let replies = decode(frame).unwrap();

        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].result, ReplyResult::Empty);
        assert_eq!(
            replies[1].result,
            ReplyResult::Push(Push::Disconnect(Disconnect {
                code: 3001,
                reason: "shutdown".into()
            }))
        );
    }

    #[test]
    fn unknown_results_and_pushes_are_unsupported_not_errors() {
        let replies =
            decode(b"{\"id\":5,\"ping\":{}}\n{\"push\":{\"channel\":\"c\",\"refresh\":{}}}")
                .unwrap();

        assert_eq!(replies[0].result, ReplyResult::Unsupported);
        assert_eq!(replies[1].result, ReplyResult::Push(Push::Unsupported));
    }

    #[test]
    fn the_frame_of_an_emulation_request_is_read_back() {
        let body = emulation_request("s1", "n1", b"{\"id\":2}\n{}");

        assert_eq!(emulation_frame(&body), Some(b"{\"id\":2}\n{}".to_vec()));
        assert_eq!(emulation_frame(b"{\"id\":2}"), None);
    }

    #[test]
    fn emulation_request_carries_the_frame_as_a_string() {
        let body = emulation_request("s1", "n1", b"{\"id\":2}\n{}");

        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"session":"s1","node":"n1","data":"{\"id\":2}\n{}"}"#
        );
    }

    #[test]
    fn encodes_subscribe_with_its_data_embedded_and_unsubscribe() {
        let subscribe = |data: &[u8]| Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                data: Some(data.to_vec()),
                ..SubscribeRequest::default()
            }),
        };

        let frame = encode(&[
            subscribe(br#"{"room": 1}"#),
            subscribe(b"not json"),
            Command {
                id: 3,
                method: Method::Unsubscribe(UnsubscribeRequest {
                    channel: "news".into(),
                }),
            },
        ]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            concat!(
                r#"{"id":2,"subscribe":{"channel":"news","data":{"room": 1}}}"#,
                "\n",
                r#"{"id":2,"subscribe":{"channel":"news","data":"not json"}}"#,
                "\n",
                r#"{"id":3,"unsubscribe":{"channel":"news"}}"#,
            )
        );
    }

    #[test]
    fn encodes_a_subscribe_token_and_sub_refresh() {
        let frame = encode(&[
            Command {
                id: 2,
                method: Method::Subscribe(SubscribeRequest {
                    channel: "news".into(),
                    token: Secret("t1".into()),
                    ..SubscribeRequest::default()
                }),
            },
            Command {
                id: 3,
                method: Method::SubRefresh(SubRefreshRequest {
                    channel: "news".into(),
                    token: Secret("t2".into()),
                }),
            },
        ]);

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            concat!(
                r#"{"id":2,"subscribe":{"channel":"news","token":"t1"}}"#,
                "\n",
                r#"{"id":3,"sub_refresh":{"channel":"news","token":"t2"}}"#,
            )
        );
    }

    #[test]
    fn decodes_subscribe_expiry_sub_refresh_and_unsubscribe_push() {
        let frame = concat!(
            r#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#,
            "\n",
            r#"{"id":3,"sub_refresh":{"expires":true,"ttl":30}}"#,
            "\n",
            r#"{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"resubscribe"}}}"#,
        );

        let replies = decode(frame.as_bytes()).unwrap();

        assert!(
            matches!(&replies[0].result, ReplyResult::Subscribe(result) if result.expires && result.ttl == 60),
            "{:?}",
            replies[0]
        );
        assert_eq!(
            replies[1].result,
            ReplyResult::SubRefresh(RefreshResult {
                expires: true,
                ttl: 30
            })
        );
        assert_eq!(
            replies[2].result,
            ReplyResult::Push(Push::Unsubscribe {
                channel: "news".into(),
                code: 2500,
                reason: "resubscribe".into()
            })
        );
    }

    #[test]
    fn decodes_publications_with_their_exact_data() {
        let frame = concat!(
            r#"{"id":2,"subscribe":{"data":{"b": 1.0,  "a":2},"publications":[{"data":{"x":1},"offset":4}],"epoch":"e"}}"#,
            "\n",
            r#"{"push":{"channel":"news","pub":{"data":{"b": 1.0,  "a":2},"offset":5,"tags":{"k":"v"},"info":{"client":"c","user":"u","conn_info":{"i": 1}}}}}"#,
            "\n",
            r#"{"id":3,"unsubscribe":{}}"#,
        );

        let replies = decode(frame.as_bytes()).unwrap();

        let ReplyResult::Subscribe(subscribe) = &replies[0].result else {
            panic!("{:?}", replies[0]);
        };
        assert_eq!(
            subscribe.data.as_deref(),
            Some(&br#"{"b": 1.0,  "a":2}"#[..])
        );
        assert_eq!(subscribe.publications[0].data, br#"{"x":1}"#);
        assert_eq!(subscribe.publications[0].offset, Some(4));
        let ReplyResult::Push(Push::Publication {
            channel,
            publication,
            ..
        }) = &replies[1].result
        else {
            panic!("{:?}", replies[1]);
        };
        assert_eq!(channel, "news");
        assert_eq!(publication.data, br#"{"b": 1.0,  "a":2}"#);
        assert_eq!(publication.offset, Some(5));
        assert_eq!(publication.tags.get("k").map(String::as_str), Some("v"));
        let info = publication.info.as_ref().unwrap();
        assert_eq!((info.client.as_str(), info.user.as_str()), ("c", "u"));
        assert_eq!(info.conn_info.as_deref(), Some(&br#"{"i": 1}"#[..]));
        assert_eq!(info.chan_info, None);
        assert_eq!(replies[2].result, ReplyResult::Unsubscribe);
    }

    #[test]
    fn encodes_a_recovering_subscribe_and_decodes_its_position_and_epoch() {
        let frame = encode(&[Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                recover: true,
                epoch: "e1".into(),
                offset: 7,
                positioned: true,
                recoverable: true,
                flag: REJECT_UNRECOVERED,
                ..SubscribeRequest::default()
            }),
        }]);
        let replies = decode(
            concat!(
                r#"{"id":2,"subscribe":{"recoverable":true,"positioned":true,"epoch":"e1","offset":7,"#,
                r#""was_recovering":true,"recovered":true,"publications":[{"data":{},"offset":8}]}}"#,
                "\n",
                r#"{"push":{"channel":"news","pub":{"data":{},"offset":1,"epoch":"e2"}}}"#,
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(frame).unwrap(),
            r#"{"id":2,"subscribe":{"channel":"news","recover":true,"epoch":"e1","offset":7,"positioned":true,"recoverable":true,"flag":2}}"#
        );
        let ReplyResult::Subscribe(result) = &replies[0].result else {
            panic!("{:?}", replies[0]);
        };
        assert!(result.recoverable && result.positioned);
        assert!(result.was_recovering && result.recovered);
        assert_eq!((result.epoch.as_str(), result.offset), ("e1", 7));
        assert_eq!(result.publications[0].offset, Some(8));
        assert!(matches!(
            &replies[1].result,
            ReplyResult::Push(Push::Publication { publication, .. })
                if publication.epoch.as_deref() == Some("e2") && publication.offset == Some(1)
        ));
    }

    #[test]
    fn an_unsubscribe_result_must_be_an_object() {
        assert!(decode(br#"{"id":3,"unsubscribe":null}"#).is_err());
        assert!(decode(br#"{"id":3,"unsubscribe":1}"#).is_err());
        assert_eq!(
            decode(br#"{"id":3,"unsubscribe":{}}"#).unwrap()[0].result,
            ReplyResult::Unsubscribe
        );
    }

    #[test]
    fn explicit_null_data_is_kept() {
        let replies = decode(br#"{"push":{"channel":"c","pub":{"data":null}}}"#).unwrap();

        let ReplyResult::Push(Push::Publication { publication, .. }) = &replies[0].result else {
            panic!("{:?}", replies[0]);
        };
        assert_eq!(publication.data, b"null");
    }

    #[test]
    fn malformed_frame_is_an_error() {
        assert!(decode(b"{\"id\":1,").is_err());
        assert!(decode(b"[1,2]").is_err());
    }
}
