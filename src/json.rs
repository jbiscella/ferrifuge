//! JSON codec of the client protocol.
//!
//! Field names follow `proto/client.proto`. Several messages in one frame
//! are separated by a newline; empty fields are omitted on encode and
//! default on decode.

use serde::{Deserialize, Serialize};

use crate::wire::{
    Command, ConnectRequest, ConnectResult, Disconnect, Error, Method, Push, RefreshRequest,
    RefreshResult, Reply, ReplyResult,
};

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

/// Decodes every reply in a frame.
pub(crate) fn decode(frame: &[u8]) -> Result<Vec<Reply>, DecodeError> {
    serde_json::Deserializer::from_slice(frame)
        .into_iter::<JsonReply>()
        .map(|reply| reply.map(Reply::from).map_err(DecodeError))
        .collect()
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
}

#[derive(Serialize)]
struct JsonRefreshRequest<'a> {
    token: &'a str,
}

#[derive(Serialize)]
struct JsonConnectRequest<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    token: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    version: &'a str,
}

impl<'a> From<&'a Command> for JsonCommand<'a> {
    fn from(command: &'a Command) -> Self {
        let mut json = Self {
            id: command.id,
            ..Self::default()
        };
        match &command.method {
            Method::Connect(ConnectRequest {
                token,
                name,
                version,
            }) => {
                json.connect = Some(JsonConnectRequest {
                    token,
                    name,
                    version,
                });
            }
            Method::Refresh(RefreshRequest { token }) => {
                json.refresh = Some(JsonRefreshRequest { token });
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

#[derive(Deserialize)]
struct JsonReply {
    #[serde(default)]
    id: u32,
    error: Option<JsonError>,
    connect: Option<JsonConnectResult>,
    refresh: Option<JsonRefreshResult>,
    push: Option<JsonPush>,
    #[serde(flatten)]
    other: serde_json::Map<String, serde_json::Value>,
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

#[derive(Deserialize, Default)]
#[serde(default)]
struct JsonConnectResult {
    client: String,
    version: String,
    expires: bool,
    ttl: u32,
    ping: u32,
    pong: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JsonRefreshResult {
    expires: bool,
    ttl: u32,
}

#[derive(Deserialize)]
struct JsonPush {
    disconnect: Option<JsonDisconnect>,
}

#[derive(Deserialize)]
struct JsonDisconnect {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    reason: String,
}

impl From<JsonReply> for Reply {
    fn from(reply: JsonReply) -> Self {
        let result = if let Some(connect) = reply.connect {
            ReplyResult::Connect(ConnectResult {
                client: connect.client,
                version: connect.version,
                expires: connect.expires,
                ttl: connect.ttl,
                ping: connect.ping,
                pong: connect.pong,
            })
        } else if let Some(refresh) = reply.refresh {
            ReplyResult::Refresh(RefreshResult {
                expires: refresh.expires,
                ttl: refresh.ttl,
            })
        } else if let Some(push) = reply.push {
            ReplyResult::Push(match push.disconnect {
                Some(JsonDisconnect { code, reason }) => {
                    Push::Disconnect(Disconnect { code, reason })
                }
                None => Push::Unsupported,
            })
        } else if reply.other.is_empty() {
            ReplyResult::Empty
        } else {
            ReplyResult::Unsupported
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

#[cfg(test)]
mod tests {
    use super::*;

    fn connect_command(id: u32, token: &str) -> Command {
        Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: token.to_owned(),
                name: "ferrifuge".to_owned(),
                version: "0.1.0".to_owned(),
            }),
        }
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
    fn decodes_connect_reply() {
        let frame = br#"{"id":1,"connect":{"client":"c1","version":"6.10.1","expires":true,"ttl":30,"ping":25,"pong":true,"session":"s"}}"#;

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
            decode(b"{\"id\":5,\"rpc\":{\"data\":{}}}\n{\"push\":{\"channel\":\"c\",\"pub\":{}}}")
                .unwrap();

        assert_eq!(replies[0].result, ReplyResult::Unsupported);
        assert_eq!(replies[1].result, ReplyResult::Push(Push::Unsupported));
    }

    #[test]
    fn malformed_frame_is_an_error() {
        assert!(decode(b"{\"id\":1,").is_err());
        assert!(decode(b"[1,2]").is_err());
    }
}
