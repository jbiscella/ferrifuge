//! Protobuf codec of the client protocol.
//!
//! Messages are those of `proto/client.proto` (generated in
//! [`crate::protocol`]); several messages in one frame are each prefixed
//! with their length as a varint.

use prost::Message as _;

use crate::protocol as pb;
use crate::wire::{
    ChannelRequest, Command, ConnectRequest, ConnectResult, Disconnect, Error, HistoryRequest,
    HistoryResult, Method, PresenceStatsResult, PublishRequest, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult, RpcRequest, SubRefreshRequest, SubscribeRequest,
    SubscribeResult, UnsubscribeRequest,
};
use crate::{ClientInfo, Publication};

/// Encodes commands into one frame.
pub(crate) fn encode(commands: &[Command]) -> Vec<u8> {
    let mut frame = Vec::new();
    for command in commands {
        pb::Command::from(command)
            .encode_length_delimited(&mut frame)
            .expect("encoding into a Vec cannot run out of space");
    }
    frame
}

/// Decodes every reply in a frame.
/// Ids of the commands of `frame`, one this client encoded; 0 for a
/// command without one.
pub(crate) fn command_ids(mut frame: &[u8]) -> Vec<u32> {
    let mut ids = Vec::new();
    while let Ok(command) = pb::Command::decode_length_delimited(&mut frame) {
        ids.push(command.id);
        if frame.is_empty() {
            break;
        }
    }
    ids
}

/// Body of a request to the emulation endpoint carrying `frame` for the
/// emulated connection `session` on `node` (`client.proto`,
/// `EmulationRequest`; centrifuge-js 63ed73e `encodeEmulationRequest`):
/// the commands as raw bytes.
pub(crate) fn emulation_request(session: &str, node: &str, frame: &[u8]) -> Vec<u8> {
    pb::EmulationRequest {
        node: node.to_owned(),
        session: session.to_owned(),
        data: frame.to_vec(),
    }
    .encode_to_vec()
}

/// The frame an [`emulation_request`] body carries; `None` for a body that
/// is not one.
pub(crate) fn emulation_frame(body: &[u8]) -> Option<Vec<u8>> {
    pb::EmulationRequest::decode(body)
        .ok()
        .map(|request| request.data)
}

/// The commands of `frame`, one this client encoded, each with its length
/// prefix, so each is itself a one-command frame; a truncated end is left
/// out.
pub(crate) fn split_commands(frame: &[u8]) -> Vec<&[u8]> {
    let mut commands = Vec::new();
    let mut rest = frame;
    while !rest.is_empty() {
        let mut cursor = rest;
        let Ok(length) = prost::decode_length_delimiter(&mut cursor) else {
            break;
        };
        let prefix = rest.len() - cursor.len();
        let Some(end) = prefix.checked_add(length).filter(|end| *end <= rest.len()) else {
            break;
        };
        let (command, after) = rest.split_at(end);
        commands.push(command);
        rest = after;
    }
    commands
}

#[cfg(test)]
pub(crate) fn decode(frame: &[u8]) -> Result<Vec<Reply>, prost::DecodeError> {
    Ok(decode_sized(frame)?
        .into_iter()
        .map(|(reply, _)| reply)
        .collect())
}

/// Decodes every reply in a frame, each with its encoded size.
pub(crate) fn decode_sized(mut frame: &[u8]) -> Result<Vec<(Reply, usize)>, prost::DecodeError> {
    let mut replies = Vec::new();
    while !frame.is_empty() {
        let before = frame.len();
        let reply = pb::Reply::decode_length_delimited(&mut frame)?.into();
        replies.push((reply, before - frame.len()));
    }
    Ok(replies)
}

impl From<&Command> for pb::Command {
    #[allow(clippy::too_many_lines)] // One arm per method.
    fn from(command: &Command) -> Self {
        let mut pb = Self {
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
                pb.connect = Some(pb::ConnectRequest {
                    token: token.clone(),
                    data: data.clone().unwrap_or_default(),
                    headers: headers
                        .iter()
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect(),
                    name: name.clone(),
                    version: version.clone(),
                    subs: subs
                        .iter()
                        .map(|(channel, sub)| {
                            (
                                channel.clone(),
                                pb::SubscribeRequest {
                                    recover: sub.recover,
                                    epoch: sub.epoch.clone(),
                                    offset: sub.offset,
                                    ..pb::SubscribeRequest::default()
                                },
                            )
                        })
                        .collect(),
                    ..pb::ConnectRequest::default()
                });
            }
            Method::Refresh(RefreshRequest { token }) => {
                pb.refresh = Some(pb::RefreshRequest {
                    token: token.clone(),
                });
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
                delta,
                tag_filter,
                kind,
                phase,
                cursor,
                limit,
            }) => {
                pb.subscribe = Some(pb::SubscribeRequest {
                    r#type: *kind,
                    phase: *phase,
                    cursor: cursor.clone(),
                    limit: *limit,
                    tf: tag_filter.as_ref().map(|filter| filter_node(&filter.0)),
                    delta: if *delta {
                        crate::wire::FOSSIL.to_owned()
                    } else {
                        String::new()
                    },
                    channel: channel.clone(),
                    token: token.0.clone(),
                    data: data.clone().unwrap_or_default(),
                    recover: *recover,
                    epoch: epoch.clone(),
                    offset: *offset,
                    positioned: *positioned,
                    recoverable: *recoverable,
                    flag: *flag,
                    join_leave: *join_leave,
                    ..pb::SubscribeRequest::default()
                });
            }
            Method::Unsubscribe(UnsubscribeRequest { channel }) => {
                pb.unsubscribe = Some(pb::UnsubscribeRequest {
                    channel: channel.clone(),
                });
            }
            Method::SubRefresh(SubRefreshRequest { channel, token }) => {
                pb.sub_refresh = Some(pb::SubRefreshRequest {
                    channel: channel.clone(),
                    token: token.0.clone(),
                    ..pb::SubRefreshRequest::default()
                });
            }
            Method::Rpc(RpcRequest { method, data }) => {
                pb.rpc = Some(pb::RpcRequest {
                    data: data.clone(),
                    method: method.clone(),
                });
            }
            Method::Publish(PublishRequest {
                channel,
                data,
                kind,
                key,
                removed,
            }) => {
                pb.publish = Some(pb::PublishRequest {
                    channel: channel.clone(),
                    data: data.clone(),
                    r#type: *kind,
                    key: key.clone(),
                    removed: *removed,
                });
            }
            Method::History(HistoryRequest {
                channel,
                limit,
                since,
                reverse,
            }) => {
                pb.history = Some(pb::HistoryRequest {
                    channel: channel.clone(),
                    limit: *limit,
                    since: since.as_ref().map(|(offset, epoch)| pb::StreamPosition {
                        offset: *offset,
                        epoch: epoch.clone(),
                    }),
                    reverse: *reverse,
                });
            }
            Method::Presence(ChannelRequest { channel }) => {
                pb.presence = Some(pb::PresenceRequest {
                    channel: channel.clone(),
                });
            }
            Method::PresenceStats(ChannelRequest { channel }) => {
                pb.presence_stats = Some(pb::PresenceStatsRequest {
                    channel: channel.clone(),
                });
            }
            Method::Send(data) => {
                pb.send = Some(pb::SendRequest { data: data.clone() });
            }
            // A pong is the empty command: no request field.
            Method::Pong => {}
        }
        pb
    }
}

impl From<pb::SubscribeResult> for SubscribeResult {
    fn from(subscribe: pb::SubscribeResult) -> Self {
        Self {
            expires: subscribe.expires,
            ttl: subscribe.ttl,
            data: (!subscribe.data.is_empty()).then_some(subscribe.data),
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
            delta: subscribe.delta,
            id: subscribe.id,
            kind: subscribe.r#type,
            phase: subscribe.phase,
            cursor: subscribe.cursor,
            state: subscribe.state.into_iter().map(Publication::from).collect(),
        }
    }
}

impl From<pb::Reply> for Reply {
    fn from(reply: pb::Reply) -> Self {
        let result = if let Some(connect) = reply.connect {
            ReplyResult::Connect(ConnectResult {
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
                data: (!connect.data.is_empty()).then_some(connect.data),
            })
        } else if let Some(refresh) = reply.refresh {
            ReplyResult::Refresh(RefreshResult {
                expires: refresh.expires,
                ttl: refresh.ttl,
            })
        } else if let Some(subscribe) = reply.subscribe {
            ReplyResult::Subscribe(subscribe.into())
        } else if reply.unsubscribe.is_some() {
            ReplyResult::Unsubscribe
        } else if let Some(refresh) = reply.sub_refresh {
            ReplyResult::SubRefresh(RefreshResult {
                expires: refresh.expires,
                ttl: refresh.ttl,
            })
        } else if let Some(rpc) = reply.rpc {
            ReplyResult::Rpc(rpc.data)
        } else if reply.publish.is_some() {
            ReplyResult::Publish
        } else if let Some(history) = reply.history {
            ReplyResult::History(HistoryResult {
                publications: history
                    .publications
                    .into_iter()
                    .map(Publication::from)
                    .collect(),
                epoch: history.epoch,
                offset: history.offset,
            })
        } else if let Some(presence) = reply.presence {
            ReplyResult::Presence(
                presence
                    .presence
                    .into_iter()
                    .map(|(client, info)| (client, info.into()))
                    .collect(),
            )
        } else if let Some(stats) = reply.presence_stats {
            ReplyResult::PresenceStats(PresenceStatsResult {
                num_clients: stats.num_clients,
                num_users: stats.num_users,
            })
        } else if let Some(push) = reply.push {
            ReplyResult::Push(push.into())
        } else if has_other_result(&reply) {
            ReplyResult::Unsupported
        } else {
            ReplyResult::Empty
        };
        Self {
            id: reply.id,
            error: reply.error.map(|e| Error {
                code: e.code,
                message: e.message,
                temporary: e.temporary,
            }),
            result,
        }
    }
}

impl From<pb::Push> for Push {
    fn from(push: pb::Push) -> Self {
        if let Some(publication) = push.r#pub {
            Push::Publication {
                channel: push.channel,
                id: push.id,
                publication: publication.into(),
            }
        } else if let Some(pb::Disconnect { code, reason, .. }) = push.disconnect {
            Push::Disconnect(Disconnect { code, reason })
        } else if let Some(subscribe) = push.subscribe {
            Push::Subscribe {
                channel: push.channel,
                result: SubscribeResult {
                    recoverable: subscribe.recoverable,
                    epoch: subscribe.epoch,
                    offset: subscribe.offset,
                    positioned: subscribe.positioned,
                    data: (!subscribe.data.is_empty()).then_some(subscribe.data),
                    ..SubscribeResult::default()
                },
            }
        } else if let Some(pb::Unsubscribe { code, reason, .. }) = push.unsubscribe {
            Push::Unsubscribe {
                channel: push.channel,
                code,
                reason,
            }
        } else if let Some(message) = push.message {
            Push::Message(message.data)
        } else if let Some(join) = push.join {
            Push::Join {
                channel: push.channel,
                id: push.id,
                info: join.info.map(ClientInfo::from).unwrap_or_default(),
            }
        } else if let Some(leave) = push.leave {
            Push::Leave {
                channel: push.channel,
                id: push.id,
                info: leave.info.map(ClientInfo::from).unwrap_or_default(),
            }
        } else {
            Push::Unsupported
        }
    }
}

/// Whether the reply carries a result of a method the client does not
/// implement yet.
fn has_other_result(reply: &pb::Reply) -> bool {
    reply.ping.is_some()
}

impl From<pb::ClientInfo> for ClientInfo {
    fn from(info: pb::ClientInfo) -> Self {
        Self {
            client: info.client,
            user: info.user,
            conn_info: (!info.conn_info.is_empty()).then_some(info.conn_info),
            chan_info: (!info.chan_info.is_empty()).then_some(info.chan_info),
        }
    }
}

impl From<pb::Publication> for Publication {
    fn from(publication: pb::Publication) -> Self {
        Self {
            data: publication.data,
            offset: (publication.offset > 0).then_some(publication.offset),
            tags: publication.tags,
            info: publication.info.map(ClientInfo::from),
            epoch: (!publication.epoch.is_empty()).then_some(publication.epoch),
            channel: (!publication.channel.is_empty()).then_some(publication.channel),
            delta: publication.delta,
            map: crate::subscription::MapPart::of(publication.key, publication.removed),
        }
    }
}

/// A tag filter as `client.proto` `FilterNode` (Block 35a).
fn filter_node(node: &crate::tag_filter::Node) -> pb::FilterNode {
    pb::FilterNode {
        op: node.op.to_owned(),
        key: node.key.clone(),
        cmp: node.cmp.to_owned(),
        val: node.val.clone(),
        vals: node.vals.clone(),
        nodes: node.nodes.iter().map(filter_node).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Secret;
    use crate::wire::{CHANNEL_COMPACTION, REJECT_UNRECOVERED};

    /// Fuzz target (Block 14e): any bytes decode or fail, never panic.
    /// Random inputs under `cargo test`; coverage-guided with
    /// `cargo bolero test protobuf::tests::fuzz_decode --engine libfuzzer`.
    #[test]
    // bolero is a native dev-dependency (Block 44, D18).
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn fuzz_decode() {
        bolero::check!().for_each(|frame: &[u8]| {
            let _ = decode(frame);
        });
    }

    // Block 45, D3: the fuzz target's committed seeds, which `cargo test`
    // replays and the CI fuzz runs start from, are a valid map subscribe
    // result and a valid map publication.
    #[test]
    fn the_fuzz_seeds_decode_as_map_replies() {
        let result = decode(include_bytes!(
            "__fuzz__/protobuf__tests__decode/corpus/seed-map-subscribe-result"
        ))
        .unwrap();
        let publication = decode(include_bytes!(
            "__fuzz__/protobuf__tests__decode/corpus/seed-map-publication"
        ))
        .unwrap();

        let [
            Reply {
                result: ReplyResult::Subscribe(result),
                ..
            },
        ] = &result[..]
        else {
            panic!("{result:?}");
        };
        assert_eq!((result.kind, result.phase, result.state.len()), (1, 2, 1));
        let [
            Reply {
                result: ReplyResult::Push(Push::Publication { publication, .. }),
                ..
            },
        ] = &publication[..]
        else {
            panic!("{publication:?}");
        };
        assert_eq!(
            publication.map.as_ref().map(|map| map.key.as_str()),
            Some("a")
        );
    }

    fn frame_of(replies: &[pb::Reply]) -> Vec<u8> {
        let mut frame = Vec::new();
        for reply in replies {
            reply.encode_length_delimited(&mut frame).unwrap();
        }
        frame
    }

    #[test]
    fn encodes_an_rpc_and_decodes_its_result_with_binary_data() {
        let frame = encode(&[Command {
            id: 4,
            method: Method::Rpc(RpcRequest {
                method: "bin".into(),
                data: vec![0xff, 0x00, 0xfe],
            }),
        }]);
        let replies = decode(&frame_of(&[pb::Reply {
            id: 4,
            rpc: Some(pb::RpcResult {
                data: vec![0xfe, 0x00, 0xff],
            }),
            ..Default::default()
        }]))
        .unwrap();

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();
        let rpc = command.rpc.unwrap();
        assert_eq!((command.id, rpc.method.as_str()), (4, "bin"));
        assert_eq!(rpc.data, [0xff, 0x00, 0xfe]);
        assert_eq!(replies[0].result, ReplyResult::Rpc(vec![0xfe, 0x00, 0xff]));
    }

    #[test]
    #[allow(clippy::too_many_lines)] // Four methods, each encoded and decoded.
    fn encodes_publish_history_presence_and_presence_stats_and_decodes_their_results() {
        let frame = encode(&[
            Command {
                id: 1,
                method: Method::Publish(PublishRequest {
                    channel: "news".into(),
                    data: vec![0xff, 0x00, 0xfe],
                    ..PublishRequest::default()
                }),
            },
            Command {
                id: 2,
                method: Method::History(HistoryRequest {
                    channel: "news".into(),
                    limit: 2,
                    since: Some((1, "e".into())),
                    reverse: true,
                }),
            },
            Command {
                id: 3,
                method: Method::Presence(ChannelRequest {
                    channel: "news".into(),
                }),
            },
            Command {
                id: 4,
                method: Method::PresenceStats(ChannelRequest {
                    channel: "news".into(),
                }),
            },
        ]);
        let replies = decode(&frame_of(&[
            pb::Reply {
                id: 1,
                publish: Some(pb::PublishResult::default()),
                ..Default::default()
            },
            pb::Reply {
                id: 2,
                history: Some(pb::HistoryResult {
                    publications: vec![pb::Publication {
                        data: vec![0xfe, 0x00, 0xff],
                        offset: 3,
                        ..Default::default()
                    }],
                    epoch: "e".into(),
                    offset: 3,
                }),
                ..Default::default()
            },
            pb::Reply {
                id: 3,
                presence: Some(pb::PresenceResult {
                    presence: [(
                        "c1".to_owned(),
                        pb::ClientInfo {
                            user: "u1".into(),
                            client: "c1".into(),
                            conn_info: vec![1],
                            chan_info: Vec::new(),
                        },
                    )]
                    .into(),
                }),
                ..Default::default()
            },
            pb::Reply {
                id: 4,
                presence_stats: Some(pb::PresenceStatsResult {
                    num_clients: 2,
                    num_users: 1,
                }),
                ..Default::default()
            },
        ]))
        .unwrap();

        let mut rest = frame.as_slice();
        let commands: Vec<pb::Command> = (0..4)
            .map(|_| pb::Command::decode_length_delimited(&mut rest).unwrap())
            .collect();
        assert_eq!(
            commands[0].publish.as_ref().unwrap().data,
            [0xff, 0x00, 0xfe]
        );
        let history = commands[1].history.as_ref().unwrap();
        assert_eq!((history.limit, history.reverse), (2, true));
        assert_eq!(
            history.since,
            Some(pb::StreamPosition {
                offset: 1,
                epoch: "e".into()
            })
        );
        assert_eq!(commands[2].presence.as_ref().unwrap().channel, "news");
        assert_eq!(commands[3].presence_stats.as_ref().unwrap().channel, "news");
        assert_eq!(replies[0].result, ReplyResult::Publish);
        let ReplyResult::History(result) = &replies[1].result else {
            panic!("{:?}", replies[1]);
        };
        assert_eq!(result.publications[0].data, [0xfe, 0x00, 0xff]);
        assert_eq!((result.epoch.as_str(), result.offset), ("e", 3));
        let ReplyResult::Presence(presence) = &replies[2].result else {
            panic!("{:?}", replies[2]);
        };
        assert_eq!(presence["c1"].conn_info.as_deref(), Some(&[1][..]));
        assert_eq!(presence["c1"].chan_info, None);
        assert_eq!(
            replies[3].result,
            ReplyResult::PresenceStats(PresenceStatsResult {
                num_clients: 2,
                num_users: 1
            })
        );
    }

    // Block 45, D3: a map publication's fields, absent from a regular one.
    // Block 45, D3: a map subscription's fields round-trip; a stream
    // subscription's request leaves them out.
    #[test]
    fn map_subscribe_fields_round_trip() {
        let frame = encode(&[Command {
            id: 1,
            method: Method::Subscribe(SubscribeRequest {
                channel: "scores".into(),
                epoch: "e1".into(),
                offset: 5,
                kind: 3,
                phase: 2,
                cursor: "c1".into(),
                limit: 2,
                ..SubscribeRequest::default()
            }),
        }]);
        let stream = encode(&[Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                ..SubscribeRequest::default()
            }),
        }]);
        let reply = pb::Reply {
            id: 1,
            subscribe: Some(pb::SubscribeResult {
                r#type: 1,
                phase: 2,
                cursor: "c2".into(),
                state: vec![pb::Publication {
                    key: "a".into(),
                    data: vec![0xff],
                    ..pb::Publication::default()
                }],
                publications: vec![pb::Publication {
                    key: "b".into(),
                    removed: true,
                    ..pb::Publication::default()
                }],
                ..pb::SubscribeResult::default()
            }),
            ..pb::Reply::default()
        };

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();
        let replies = decode(&reply.encode_length_delimited_to_vec()).unwrap();

        let subscribe = command.subscribe.unwrap();
        assert_eq!(
            (
                subscribe.r#type,
                subscribe.phase,
                subscribe.cursor.as_str(),
                subscribe.limit
            ),
            (3, 2, "c1", 2)
        );
        assert_eq!(
            pb::Command::decode_length_delimited(stream.as_slice()).unwrap(),
            pb::Command {
                id: 2,
                subscribe: Some(pb::SubscribeRequest {
                    channel: "news".into(),
                    ..pb::SubscribeRequest::default()
                }),
                ..pb::Command::default()
            }
        );
        let ReplyResult::Subscribe(result) = &replies[0].result else {
            panic!("{replies:?}");
        };
        assert_eq!(
            (result.kind, result.phase, result.cursor.as_str()),
            (1, 2, "c2")
        );
        assert_eq!(
            (
                result.state[0].map.as_ref().unwrap().key.as_str(),
                result.state[0].data.as_slice()
            ),
            ("a", &[0xff][..])
        );
        assert!(result.publications[0].map.as_ref().unwrap().removed);
    }

    #[test]
    fn encodes_map_publications_and_leaves_a_regular_one_unchanged() {
        let frame = encode(&[
            Command {
                id: 1,
                method: Method::Publish(PublishRequest {
                    channel: "map:a".into(),
                    data: vec![0xff],
                    kind: 1,
                    key: "k1".into(),
                    removed: false,
                }),
            },
            Command {
                id: 2,
                method: Method::Publish(PublishRequest {
                    channel: "map:a".into(),
                    kind: 1,
                    key: "k1".into(),
                    removed: true,
                    ..PublishRequest::default()
                }),
            },
        ]);
        let regular = encode(&[Command {
            id: 3,
            method: Method::Publish(PublishRequest {
                channel: "news".into(),
                data: vec![0xff],
                ..PublishRequest::default()
            }),
        }]);

        let mut rest = frame.as_slice();
        let commands: Vec<pb::Command> = (0..2)
            .map(|_| pb::Command::decode_length_delimited(&mut rest).unwrap())
            .collect();
        assert_eq!(
            commands[0].publish,
            Some(pb::PublishRequest {
                channel: "map:a".into(),
                data: vec![0xff],
                r#type: 1,
                key: "k1".into(),
                removed: false,
            })
        );
        assert_eq!(
            commands[1].publish,
            Some(pb::PublishRequest {
                channel: "map:a".into(),
                data: Vec::new(),
                r#type: 1,
                key: "k1".into(),
                removed: true,
            })
        );
        // Zero fields are not encoded: the regular publication's bytes are
        // those of its two fields only.
        let expected = pb::Command {
            id: 3,
            publish: Some(pb::PublishRequest {
                channel: "news".into(),
                data: vec![0xff],
                ..pb::PublishRequest::default()
            }),
            ..pb::Command::default()
        };
        assert_eq!(regular, expected.encode_length_delimited_to_vec());
        assert_eq!(
            expected.publish.unwrap().encoded_len(),
            "news".len() + 2 + 1 + 2
        );
    }

    #[test]
    fn encodes_send_and_decodes_a_message_push_with_binary_data() {
        let frame = encode(&[Command {
            id: 0,
            method: Method::Send(vec![0xff, 0x00, 0xfe]),
        }]);
        let replies = decode(&frame_of(&[pb::Reply {
            push: Some(pb::Push {
                message: Some(pb::Message {
                    data: vec![0xff, 0x00, 0xfe],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }]))
        .unwrap();

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();
        assert_eq!(command.id, 0);
        assert_eq!(command.send.unwrap().data, [0xff, 0x00, 0xfe]);
        assert_eq!(
            replies[0].result,
            ReplyResult::Push(Push::Message(vec![0xff, 0x00, 0xfe]))
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
        let info = pb::ClientInfo {
            user: "u".into(),
            client: "c".into(),
            conn_info: vec![0xff],
            chan_info: vec![0x00],
        };
        let replies = decode(&frame_of(&[
            pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    join: Some(pb::Join {
                        info: Some(info.clone()),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    leave: Some(pb::Leave { info: Some(info) }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]))
        .unwrap();

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();
        assert!(command.subscribe.unwrap().join_leave);
        let expected = ClientInfo {
            client: "c".into(),
            user: "u".into(),
            conn_info: Some(vec![0xff]),
            chan_info: Some(vec![0x00]),
        };
        assert_eq!(
            replies[0].result,
            ReplyResult::Push(Push::Join {
                channel: "news".into(),
                id: 0,
                info: expected.clone()
            })
        );
        assert_eq!(
            replies[1].result,
            ReplyResult::Push(Push::Leave {
                channel: "news".into(),
                id: 0,
                info: expected
            })
        );
    }

    #[test]
    fn decodes_the_own_channel_of_a_publication() {
        let push = |channel: &str| pb::Reply {
            push: Some(pb::Push {
                channel: "news".into(),
                r#pub: Some(pb::Publication {
                    channel: channel.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let replies = decode(&frame_of(&[push("news.sport"), push("")])).unwrap();

        let channels: Vec<_> = replies
            .iter()
            .map(|reply| match &reply.result {
                ReplyResult::Push(Push::Publication { publication, .. }) => {
                    publication.channel.clone()
                }
                result => panic!("{result:?}"),
            })
            .collect();
        assert_eq!(channels, [Some("news.sport".to_owned()), None]);
    }

    #[test]
    fn encodes_the_channel_compaction_flag_and_decodes_numeric_channel_ids() {
        let flags = [
            0,
            CHANNEL_COMPACTION,
            CHANNEL_COMPACTION | REJECT_UNRECOVERED,
        ];
        let frame = encode(
            &flags
                .iter()
                .zip(1..)
                .map(|(&flag, id)| Command {
                    id,
                    method: Method::Subscribe(SubscribeRequest {
                        channel: "news".into(),
                        flag,
                        ..SubscribeRequest::default()
                    }),
                })
                .collect::<Vec<_>>(),
        );
        let push = |push: pb::Push| pb::Reply {
            push: Some(push),
            ..Default::default()
        };
        let replies = decode(&frame_of(&[
            pb::Reply {
                id: 1,
                subscribe: Some(pb::SubscribeResult {
                    id: 7,
                    ..Default::default()
                }),
                ..Default::default()
            },
            push(pb::Push {
                id: 7,
                r#pub: Some(pb::Publication::default()),
                ..Default::default()
            }),
            push(pb::Push {
                id: 7,
                join: Some(pb::Join::default()),
                ..Default::default()
            }),
            push(pb::Push {
                id: 7,
                leave: Some(pb::Leave::default()),
                ..Default::default()
            }),
        ]))
        .unwrap();

        let mut rest = frame.as_slice();
        for flag in flags {
            let command = pb::Command::decode_length_delimited(&mut rest).unwrap();
            assert_eq!(command.subscribe.unwrap().flag, flag);
        }
        assert!(matches!(&replies[0].result, ReplyResult::Subscribe(result) if result.id == 7));
        assert!(matches!(
            &replies[1].result,
            ReplyResult::Push(Push::Publication { channel, id: 7, .. }) if channel.is_empty()
        ));
        assert!(matches!(
            &replies[2].result,
            ReplyResult::Push(Push::Join { id: 7, .. })
        ));
        assert!(matches!(
            &replies[3].result,
            ReplyResult::Push(Push::Leave { id: 7, .. })
        ));
    }

    #[test]
    fn encodes_commands_length_delimited() {
        let commands = [
            Command {
                id: 1,
                method: Method::Connect(ConnectRequest {
                    token: "t".into(),
                    data: None,
                    headers: std::collections::BTreeMap::new(),
                    name: "ferrifuge".into(),
                    version: "0.1.0".into(),
                    subs: [(
                        "alerts".to_owned(),
                        SubscribeRequest {
                            recover: true,
                            epoch: "e".into(),
                            offset: 5,
                            ..SubscribeRequest::default()
                        },
                    )]
                    .into(),
                }),
            },
            Command {
                id: 2,
                method: Method::Refresh(RefreshRequest { token: "t2".into() }),
            },
        ];

        let frame = encode(&commands);

        let mut rest = frame.as_slice();
        let first = pb::Command::decode_length_delimited(&mut rest).unwrap();
        let second = pb::Command::decode_length_delimited(&mut rest).unwrap();
        assert_eq!(rest, &[] as &[u8]);
        assert_eq!(first.id, 1);
        let connect = first.connect.unwrap();
        assert_eq!(
            (
                connect.token.as_str(),
                connect.name.as_str(),
                connect.version.as_str()
            ),
            ("t", "ferrifuge", "0.1.0")
        );
        let alerts = &connect.subs["alerts"];
        assert!(alerts.recover);
        assert_eq!((alerts.epoch.as_str(), alerts.offset), ("e", 5));
        assert_eq!(second.id, 2);
        assert_eq!(second.refresh.unwrap().token, "t2");
    }

    #[test]
    fn encodes_pong_as_an_empty_command() {
        let frame = encode(&[Command {
            id: 0,
            method: Method::Pong,
        }]);

        // A zero length prefix and no message bytes.
        assert_eq!(frame, [0]);
    }

    #[test]
    fn decodes_a_subscribe_push() {
        let frame = frame_of(&[pb::Reply {
            push: Some(pb::Push {
                channel: "alerts".into(),
                subscribe: Some(pb::Subscribe {
                    recoverable: true,
                    epoch: "e".into(),
                    offset: 3,
                    positioned: true,
                    data: b"\xff".to_vec(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }]);

        let replies = decode(&frame).unwrap();

        let ReplyResult::Push(Push::Subscribe { channel, result }) = &replies[0].result else {
            panic!("{replies:?}");
        };
        assert_eq!(channel, "alerts");
        assert!(result.recoverable && result.positioned);
        assert_eq!((result.epoch.as_str(), result.offset), ("e", 3));
        assert_eq!(result.data.as_deref(), Some(&b"\xff"[..]));
    }

    #[test]
    fn connect_data_round_trips_as_bytes() {
        let command = Command {
            id: 1,
            method: Method::Connect(ConnectRequest {
                data: Some(vec![0xff, 0x00]),
                ..ConnectRequest::default()
            }),
        };
        let frame = encode(&[command]);
        let mut rest = frame.as_slice();
        let sent = pb::Command::decode_length_delimited(&mut rest).unwrap();
        assert_eq!(sent.connect.unwrap().data, [0xff, 0x00]);

        let reply = |data: Vec<u8>| {
            frame_of(&[pb::Reply {
                id: 1,
                connect: Some(pb::ConnectResult {
                    client: "c".into(),
                    data,
                    ..Default::default()
                }),
                ..Default::default()
            }])
        };
        let data_of = |frame: Vec<u8>| match decode(&frame).unwrap().remove(0).result {
            ReplyResult::Connect(result) => result.data,
            other => panic!("{other:?}"),
        };

        assert_eq!(data_of(reply(vec![0xfe])), Some(vec![0xfe]));
        assert_eq!(data_of(reply(Vec::new())), None);
    }

    #[test]
    fn decodes_the_server_side_subscriptions_of_a_connect_reply() {
        let frame = frame_of(&[pb::Reply {
            id: 1,
            connect: Some(pb::ConnectResult {
                client: "c".into(),
                subs: [(
                    "alerts".to_owned(),
                    pb::SubscribeResult {
                        recoverable: true,
                        epoch: "e".into(),
                        offset: 2,
                        data: b"\xff".to_vec(),
                        ..Default::default()
                    },
                )]
                .into(),
                ..Default::default()
            }),
            ..Default::default()
        }]);

        let replies = decode(&frame).unwrap();

        let ReplyResult::Connect(result) = &replies[0].result else {
            panic!("{replies:?}");
        };
        let alerts = &result.subs["alerts"];
        assert!(alerts.recoverable);
        assert_eq!((alerts.epoch.as_str(), alerts.offset), ("e", 2));
        assert_eq!(alerts.data.as_deref(), Some(&b"\xff"[..]));
    }

    #[test]
    fn decodes_results_errors_pushes_and_pings() {
        let frame = frame_of(&[
            pb::Reply {
                id: 1,
                connect: Some(pb::ConnectResult {
                    client: "c1".into(),
                    version: "6.10.1".into(),
                    expires: true,
                    ttl: 30,
                    ping: 25,
                    pong: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                id: 2,
                refresh: Some(pb::RefreshResult {
                    expires: true,
                    ttl: 60,
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                id: 3,
                error: Some(pb::Error {
                    code: 109,
                    message: "token expired".into(),
                    temporary: true,
                }),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    disconnect: Some(pb::Disconnect {
                        code: 3001,
                        reason: "shutdown".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply::default(),
        ]);

        let replies = decode(&frame).unwrap();

        assert_eq!(
            replies,
            [
                Reply {
                    id: 1,
                    error: None,
                    result: ReplyResult::Connect(ConnectResult {
                        client: "c1".into(),
                        version: "6.10.1".into(),
                        expires: true,
                        ttl: 30,
                        ping: 25,
                        pong: true,
                        session: String::new(),
                        node: String::new(),
                        subs: std::collections::BTreeMap::new(),
                        data: None,
                    }),
                },
                Reply {
                    id: 2,
                    error: None,
                    result: ReplyResult::Refresh(RefreshResult {
                        expires: true,
                        ttl: 60
                    }),
                },
                Reply {
                    id: 3,
                    error: Some(Error {
                        code: 109,
                        message: "token expired".into(),
                        temporary: true
                    }),
                    result: ReplyResult::Empty,
                },
                Reply {
                    id: 0,
                    error: None,
                    result: ReplyResult::Push(Push::Disconnect(Disconnect {
                        code: 3001,
                        reason: "shutdown".into()
                    })),
                },
                Reply {
                    id: 0,
                    error: None,
                    result: ReplyResult::Empty,
                },
            ]
        );
    }

    #[test]
    fn unknown_results_and_pushes_are_unsupported_not_errors() {
        let frame = frame_of(&[
            pb::Reply {
                id: 5,
                ping: Some(pb::PingResult::default()),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "c".into(),
                    refresh: Some(pb::Refresh::default()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]);

        let replies = decode(&frame).unwrap();

        assert_eq!(replies[0].result, ReplyResult::Unsupported);
        assert_eq!(replies[1].result, ReplyResult::Push(Push::Unsupported));
    }

    #[test]
    fn encodes_subscribe_and_decodes_its_result_and_publications() {
        let frame = encode(&[Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                data: Some(vec![0xff, 0x00]),
                ..SubscribeRequest::default()
            }),
        }]);
        let reply_frame = frame_of(&[
            pb::Reply {
                id: 2,
                subscribe: Some(pb::SubscribeResult {
                    data: vec![1],
                    publications: vec![pb::Publication {
                        data: vec![0xff, 0x00, 0xfe],
                        offset: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    r#pub: Some(pb::Publication {
                        data: vec![0xfe],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]);

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();
        let replies = decode(&reply_frame).unwrap();

        let subscribe = command.subscribe.unwrap();
        assert_eq!(
            (subscribe.channel.as_str(), subscribe.data),
            ("news", vec![0xff, 0x00])
        );
        let ReplyResult::Subscribe(result) = &replies[0].result else {
            panic!("{:?}", replies[0]);
        };
        assert_eq!(result.data, Some(vec![1]));
        assert_eq!(result.publications[0].data, [0xff, 0x00, 0xfe]);
        assert_eq!(result.publications[0].offset, Some(3));
        assert!(matches!(
            &replies[1].result,
            ReplyResult::Push(Push::Publication { channel, publication, .. })
                if channel == "news" && publication.data == [0xfe] && publication.offset.is_none() && publication.epoch.is_none()
        ));
    }

    // Block 35a: a nested filter as `client.proto` `FilterNode`.
    #[test]
    fn encodes_a_tag_filter_as_a_filter_node() {
        let filter =
            crate::TagFilter::is_in("ticker", ["BTC"]).or(!crate::TagFilter::exists("venue"));
        let frame = encode(&[Command {
            id: 2,
            method: Method::Subscribe(SubscribeRequest {
                channel: "news".into(),
                tag_filter: Some(filter),
                ..SubscribeRequest::default()
            }),
        }]);

        let command = pb::Command::decode_length_delimited(frame.as_slice()).unwrap();

        let tf = command.subscribe.unwrap().tf.expect("a filter");
        assert_eq!((tf.op.as_str(), tf.nodes.len()), ("or", 2));
        assert_eq!(
            (
                tf.nodes[0].key.as_str(),
                tf.nodes[0].cmp.as_str(),
                tf.nodes[0].vals.clone()
            ),
            ("ticker", "in", vec!["BTC".to_owned()])
        );
        assert_eq!(tf.nodes[1].op, "not");
        assert_eq!(
            (
                tf.nodes[1].nodes[0].key.as_str(),
                tf.nodes[1].nodes[0].cmp.as_str()
            ),
            ("venue", "ex")
        );
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
        let reply_frame = frame_of(&[
            pb::Reply {
                id: 2,
                subscribe: Some(pb::SubscribeResult {
                    recoverable: true,
                    positioned: true,
                    epoch: "e1".into(),
                    offset: 7,
                    was_recovering: true,
                    recovered: true,
                    publications: vec![pb::Publication {
                        offset: 8,
                        ..pb::Publication::default()
                    }],
                    ..pb::SubscribeResult::default()
                }),
                ..pb::Reply::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    r#pub: Some(pb::Publication {
                        offset: 1,
                        epoch: "e2".into(),
                        ..pb::Publication::default()
                    }),
                    ..pb::Push::default()
                }),
                ..pb::Reply::default()
            },
        ]);

        let subscribe = pb::Command::decode_length_delimited(frame.as_slice())
            .unwrap()
            .subscribe
            .unwrap();
        let replies = decode(&reply_frame).unwrap();

        assert!(subscribe.recover && subscribe.positioned && subscribe.recoverable);
        assert_eq!(subscribe.flag, REJECT_UNRECOVERED);
        assert_eq!((subscribe.epoch.as_str(), subscribe.offset), ("e1", 7));
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
    fn encodes_a_subscribe_token_and_sub_refresh_and_decodes_their_results() {
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
        let reply_frame = frame_of(&[
            pb::Reply {
                id: 2,
                subscribe: Some(pb::SubscribeResult {
                    expires: true,
                    ttl: 60,
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                id: 3,
                sub_refresh: Some(pb::SubRefreshResult {
                    expires: true,
                    ttl: 30,
                    ..Default::default()
                }),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    unsubscribe: Some(pb::Unsubscribe {
                        code: 2500,
                        reason: "resubscribe".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]);

        let mut commands = frame.as_slice();
        let subscribe = pb::Command::decode_length_delimited(&mut commands).unwrap();
        let sub_refresh = pb::Command::decode_length_delimited(&mut commands).unwrap();
        let replies = decode(&reply_frame).unwrap();

        assert_eq!(subscribe.subscribe.unwrap().token, "t1");
        let sub_refresh = sub_refresh.sub_refresh.unwrap();
        assert_eq!(
            (sub_refresh.channel.as_str(), sub_refresh.token.as_str()),
            ("news", "t2")
        );
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
    fn malformed_or_truncated_frames_are_errors() {
        let mut truncated = frame_of(&[pb::Reply {
            id: 1,
            connect: Some(pb::ConnectResult {
                client: "c1".into(),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        truncated.pop();

        assert!(decode(&[0xff, 0xff, 0xff]).is_err());
        assert!(decode(&truncated).is_err());
    }

    #[test]
    fn an_empty_frame_holds_no_reply() {
        assert_eq!(decode(&[]).unwrap(), []);
    }
}
