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
    pub(crate) fn decode(self, frame: &[u8]) -> Option<Vec<Reply>> {
        match self {
            Self::Json => json::decode(frame).ok(),
            Self::Protobuf => protobuf::decode(frame).ok(),
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
    use crate::wire::Method;

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
