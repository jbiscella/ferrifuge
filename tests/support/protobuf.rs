//! Protobuf types of the client protocol, the committed code generated from
//! `proto/client.proto` (Block 56), for fixtures that speak the Protobuf
//! encoding. The client's own copy is crate-private.

// Generated code: its style is prost's, not ours.
#![allow(dead_code, missing_docs, unreachable_pub, clippy::pedantic)]

include!("../../src/protocol/centrifugal.centrifuge.protocol.rs");

/// The websocket subprotocol selecting the Protobuf encoding.
pub(crate) const SUBPROTOCOL: &str = "centrifuge-protobuf";
