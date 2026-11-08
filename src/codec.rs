//! Encoding of the client protocol on the wire: the one place the machine
//! goes through to encode commands and decode frames.

use crate::config::Protocol;
use crate::machine::FrameType;
use crate::wire::{Command, Reply};
use crate::{json, protobuf};

/// A client protocol encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Codec {
    /// JSON, newline-separated messages in text frames.
    #[default]
    Json,
    /// Protobuf, length-delimited messages in binary frames.
    Protobuf,
}

impl From<Protocol> for Codec {
    fn from(protocol: Protocol) -> Self {
        match protocol {
            Protocol::Json => Self::Json,
            Protocol::Protobuf => Self::Protobuf,
        }
    }
}

impl Codec {
    /// Encodes `commands` into one frame.
    pub(crate) fn encode(self, commands: &[Command]) -> Vec<u8> {
        match self {
            Self::Json => json::encode(commands),
            Self::Protobuf => protobuf::encode(commands),
        }
    }

    /// Decodes every reply of `frame`; `None` for a frame that is not valid
    /// client protocol in this encoding.
    #[cfg(test)]
    pub(crate) fn decode(self, frame: &[u8]) -> Option<Vec<Reply>> {
        match self {
            Self::Json => json::decode(frame).ok(),
            Self::Protobuf => protobuf::decode(frame).ok(),
        }
    }

    /// Decodes the replies of a frame, each with its encoded size.
    pub(crate) fn decode_sized(self, frame: &[u8]) -> Option<Vec<(Reply, usize)>> {
        match self {
            Self::Json => json::decode_sized(frame).ok(),
            Self::Protobuf => protobuf::decode_sized(frame).ok(),
        }
    }

    /// Ids of the commands of `frame`, one [`Codec::encode`] made.
    pub(crate) fn command_ids(self, frame: &[u8]) -> Vec<u32> {
        match self {
            Self::Json => json::command_ids(frame),
            Self::Protobuf => protobuf::command_ids(frame),
        }
    }

    /// The body of an emulation request carrying `frame` for the emulated
    /// connection `session` on `node` (Block 26). Protobuf can be reached
    /// only from Block 27: until then a client with Protobuf and an HTTP
    /// transport is refused when built (Block 8).
    pub(crate) fn emulation_request(self, session: &str, node: &str, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Json => json::emulation_request(session, node, frame),
            Self::Protobuf => protobuf::emulation_request(session, node, frame),
        }
    }

    /// The size of `frame` inside an [`Codec::emulation_request`] body:
    /// JSON escapes it as a string, Protobuf carries its bytes.
    pub(crate) fn escaped_len(self, frame: &[u8]) -> usize {
        match self {
            Self::Json => json::escaped_len(frame),
            Self::Protobuf => frame.len(),
        }
    }

    /// The frame an [`Codec::emulation_request`] body carries; `None` for a
    /// body that is not one.
    pub(crate) fn emulation_frame(self, body: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Json => json::emulation_frame(body),
            Self::Protobuf => protobuf::emulation_frame(body),
        }
    }

    /// The commands of `frame`, one [`Codec::encode`] made, each itself a
    /// one-command frame.
    pub(crate) fn split_commands(self, frame: &[u8]) -> Vec<&[u8]> {
        match self {
            Self::Json => json::split_commands(frame),
            Self::Protobuf => protobuf::split_commands(frame),
        }
    }

    /// The delta, or whole payload, a publication of a subscription with
    /// delta on carries: with JSON its data is a JSON string, whose value it
    /// is; `None` when it is not one (Block 19, D7).
    pub(crate) fn delta_payload(self, data: Vec<u8>) -> Option<Vec<u8>> {
        match self {
            Self::Json => json::string_value(&data),
            Self::Protobuf => Some(data),
        }
    }

    /// How this encoding's frames travel on a websocket.
    pub(crate) fn frame_type(self) -> FrameType {
        match self {
            Self::Json => FrameType::Text,
            Self::Protobuf => FrameType::Binary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Method, UnsubscribeRequest};

    fn two_commands() -> [Command; 2] {
        [2, 3].map(|id| Command {
            id,
            method: Method::Unsubscribe(UnsubscribeRequest {
                channel: "news".into(),
            }),
        })
    }

    // Block 26, D1.
    #[test]
    fn an_emulation_request_carries_its_frame_back_in_both_encodings() {
        for codec in [Codec::Json, Codec::Protobuf] {
            let frame = codec.encode(&two_commands());

            let body = codec.emulation_request("session", "node", &frame);

            assert_eq!(codec.emulation_frame(&body), Some(frame), "{codec:?}");
        }
        assert_eq!(Codec::Json.emulation_frame(b"not json"), None);
        // A body cut inside its first field.
        assert_eq!(Codec::Protobuf.emulation_frame(&[0x0a, 0x05, b'n']), None);
    }

    // Block 26, D1.
    #[test]
    fn a_frame_splits_into_one_command_frames_in_both_encodings() {
        for codec in [Codec::Json, Codec::Protobuf] {
            let frame = codec.encode(&two_commands());

            let ids: Vec<Vec<u32>> = codec
                .split_commands(&frame)
                .into_iter()
                .map(|command| codec.command_ids(command))
                .collect();

            assert_eq!(ids, [vec![2], vec![3]], "{codec:?}");
        }
    }

    // Block 26, D3: the machine names no codec module (Block 5c).
    #[test]
    fn the_machine_reaches_the_encodings_only_through_the_codec() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = vec![root.join("machine.rs")];
        let mut directories = vec![root.join("machine")];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(&directory).expect("src/machine") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    directories.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }
        let names_a_module = |line: &str| {
            let words: Vec<&str> = line
                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                .collect();
            let path = ["json::", "protobuf::"].iter().any(|module| {
                line.match_indices(module).any(|(at, _)| {
                    !line[..at]
                        .chars()
                        .next_back()
                        .is_some_and(|c| c.is_alphanumeric() || c == '_')
                })
            });
            let import = line.trim_start().starts_with("use ")
                && words
                    .iter()
                    .any(|word| *word == "json" || *word == "protobuf");
            path || import
        };
        let mut found = Vec::new();
        for file in files {
            let text = std::fs::read_to_string(&file).expect("a source file");
            for (number, line) in text.lines().enumerate() {
                if names_a_module(line) {
                    found.push(format!("{}:{}: {line}", file.display(), number + 1));
                }
            }
        }

        assert!(found.is_empty(), "{found:#?}");
    }

    #[test]
    fn the_command_ids_of_a_frame_are_read_back_in_both_encodings() {
        let commands = [2, 3].map(|id| Command {
            id,
            method: Method::Unsubscribe(UnsubscribeRequest {
                channel: "news".into(),
            }),
        });

        for codec in [Codec::Json, Codec::Protobuf] {
            let frame = codec.encode(&commands);

            assert_eq!(codec.command_ids(&frame), [2, 3], "{codec:?}");
        }
    }

    #[test]
    fn json_frames_are_text() {
        let codec = Codec::default();

        let frame = codec.encode(&[Command {
            id: 0,
            method: Method::Pong,
        }]);

        assert_eq!(codec.frame_type(), FrameType::Text);
        assert_eq!(frame, b"{}");
        assert!(codec.decode(b"{}").is_some());
        assert!(codec.decode(b"not json").is_none());
    }

    #[test]
    fn protobuf_frames_are_binary() {
        let codec = Codec::from(Protocol::Protobuf);

        let frame = codec.encode(&[Command {
            id: 0,
            method: Method::Pong,
        }]);

        assert_eq!(codec.frame_type(), FrameType::Binary);
        assert_eq!(frame, [0]);
        assert_eq!(codec.decode(&[0]).map(|replies| replies.len()), Some(1));
        assert!(codec.decode(&[0xff]).is_none());
    }
}
