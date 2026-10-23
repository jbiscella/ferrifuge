//! Protobuf codec of the client protocol.
//!
//! Messages are those of `proto/client.proto` (generated in
//! [`crate::protocol`]); several messages in one frame are each prefixed
//! with their length as a varint.

use prost::Message as _;

use crate::protocol as pb;
use crate::wire::{
    Command, ConnectRequest, ConnectResult, Disconnect, Error, Method, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult, SubscribeRequest, SubscribeResult, UnsubscribeRequest,
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
pub(crate) fn decode(mut frame: &[u8]) -> Result<Vec<Reply>, prost::DecodeError> {
    let mut replies = Vec::new();
    while !frame.is_empty() {
        replies.push(pb::Reply::decode_length_delimited(&mut frame)?.into());
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
            }) => {
                pb.connect = Some(pb::ConnectRequest {
                    token: token.clone(),
                    name: name.clone(),
                    version: version.clone(),
                    ..pb::ConnectRequest::default()
                });
            }
            Method::Refresh(RefreshRequest { token }) => {
                pb.refresh = Some(pb::RefreshRequest {
                    token: token.clone(),
                });
            }
            Method::Subscribe(SubscribeRequest { channel, data }) => {
                pb.subscribe = Some(pb::SubscribeRequest {
                    channel: channel.clone(),
                    data: data.clone().unwrap_or_default(),
                    ..pb::SubscribeRequest::default()
                });
            }
            Method::Unsubscribe(UnsubscribeRequest { channel }) => {
                pb.unsubscribe = Some(pb::UnsubscribeRequest {
                    channel: channel.clone(),
                });
            }
            // A pong is the empty command: no request field.
            Method::Pong => {}
        }
        pb
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
            })
        } else if let Some(refresh) = reply.refresh {
            ReplyResult::Refresh(RefreshResult {
                expires: refresh.expires,
                ttl: refresh.ttl,
            })
        } else if let Some(subscribe) = reply.subscribe {
            ReplyResult::Subscribe(SubscribeResult {
                data: (!subscribe.data.is_empty()).then_some(subscribe.data),
                publications: subscribe
                    .publications
                    .into_iter()
                    .map(Publication::from)
                    .collect(),
            })
        } else if reply.unsubscribe.is_some() {
            ReplyResult::Unsubscribe
        } else if let Some(push) = reply.push {
            ReplyResult::Push(if let Some(publication) = push.r#pub {
                Push::Publication {
                    channel: push.channel,
                    publication: publication.into(),
                }
            } else if let Some(pb::Disconnect { code, reason, .. }) = push.disconnect {
                Push::Disconnect(Disconnect { code, reason })
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
        || reply.sub_refresh.is_some()
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                if channel == "news" && publication.data == [0xfe] && publication.offset.is_none()
        ));
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
