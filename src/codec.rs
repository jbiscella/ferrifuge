//! Encoding of the client protocol on the wire: the one place the machine
//! goes through to encode commands and decode frames. Only JSON so far;
//! Protobuf (Block 0 decision) becomes a second variant.

use crate::json;
use crate::machine::FrameType;
use crate::wire::{Command, Reply};

/// A client protocol encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Codec {
    /// JSON, newline-separated messages in text frames.
    #[default]
    Json,
}

impl Codec {
    /// Encodes `commands` into one frame.
    pub(crate) fn encode(self, commands: &[Command]) -> Vec<u8> {
        match self {
            Self::Json => json::encode(commands),
        }
    }

    /// Decodes every reply of `frame`; `None` for a frame that is not valid
    /// client protocol in this encoding.
    pub(crate) fn decode(self, frame: &[u8]) -> Option<Vec<Reply>> {
        match self {
            Self::Json => json::decode(frame).ok(),
        }
    }

    /// How this encoding's frames travel on a websocket.
    pub(crate) fn frame_type(self) -> FrameType {
        match self {
            Self::Json => FrameType::Text,
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
}
