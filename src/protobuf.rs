//! Protobuf codec of the client protocol.
//!
//! Messages are those of `proto/client.proto` (generated in
//! [`crate::protocol`]); several messages in one frame are each prefixed
//! with their length as a varint.

use prost::Message as _;

use crate::protocol as pb;
use crate::wire::{
    Command, ConnectRequest, ConnectResult, Disconnect, Error, Method, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult, SubRefreshRequest, SubscribeRequest, SubscribeResult,
    UnsubscribeRequest,
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
    fn from(command: &Command) -> Self {
        let mut pb = Self {
            id: command.id,
            ..Self::default()
        };
        match &command.method {
            Method::Connect(ConnectRequest {
                token,
                name,
                version,
                subs,
            }) => {
                pb.connect = Some(pb::ConnectRequest {
                    token: token.clone(),
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
            }) => {
                pb.subscribe = Some(pb::SubscribeRequest {
                    channel: channel.clone(),
                    token: token.0.clone(),
                    data: data.clone().unwrap_or_default(),
                    recover: *recover,
                    epoch: epoch.clone(),
                    offset: *offset,
                    positioned: *positioned,
                    recoverable: *recoverable,
                    flag: *flag,
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
        } else if let Some(push) = reply.push {
            ReplyResult::Push(if let Some(publication) = push.r#pub {
                Push::Publication {
                    channel: push.channel,
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
            } else {
                Push::Unsupported
            })
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

/// Whether the reply carries a result of a method the client does not
/// implement yet.
fn has_other_result(reply: &pb::Reply) -> bool {
    reply.publish.is_some()
        || reply.presence.is_some()
        || reply.presence_stats.is_some()
        || reply.history.is_some()
        || reply.ping.is_some()
        || reply.rpc.is_some()
}

impl From<pb::Publication> for Publication {
    fn from(publication: pb::Publication) -> Self {
        Self {
            data: publication.data,
            offset: (publication.offset > 0).then_some(publication.offset),
            tags: publication.tags,
            info: publication.info.map(|info| ClientInfo {
                client: info.client,
                user: info.user,
                conn_info: (!info.conn_info.is_empty()).then_some(info.conn_info),
                chan_info: (!info.chan_info.is_empty()).then_some(info.chan_info),
            }),
            epoch: (!publication.epoch.is_empty()).then_some(publication.epoch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::Secret;
    use crate::wire::REJECT_UNRECOVERED;

    /// Fuzz target (Block 14e): any bytes decode or fail, never panic.
    /// Random inputs under `cargo test`; coverage-guided with
    /// `cargo bolero test protobuf::tests::fuzz_decode --engine libfuzzer`.
    #[test]
    fn fuzz_decode() {
        bolero::check!().for_each(|frame: &[u8]| {
            let _ = decode(frame);
        });
    }

    fn frame_of(replies: &[pb::Reply]) -> Vec<u8> {
        let mut frame = Vec::new();
        for reply in replies {
            reply.encode_length_delimited(&mut frame).unwrap();
        }
        frame
    }

    #[test]
    fn encodes_commands_length_delimited() {
        let commands = [
            Command {
                id: 1,
                method: Method::Connect(ConnectRequest {
                    token: "t".into(),
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
                rpc: Some(pb::RpcResult::default()),
                ..Default::default()
            },
            pb::Reply {
                push: Some(pb::Push {
                    channel: "c".into(),
                    join: Some(pb::Join::default()),
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
            ReplyResult::Push(Push::Publication { channel, publication })
                if channel == "news" && publication.data == [0xfe] && publication.offset.is_none() && publication.epoch.is_none()
        ));
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
