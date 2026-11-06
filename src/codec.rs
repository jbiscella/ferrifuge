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
