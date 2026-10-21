//! Protobuf types of the client protocol, generated from `proto/client.proto`
//! by the build script, for fixtures that speak the Protobuf encoding.
//! The client's own copy is crate-private.

// Generated code: its style is prost's, not ours.
#![allow(dead_code, missing_docs, unreachable_pub, clippy::pedantic)]

include!(concat!(
    env!("OUT_DIR"),
    "/centrifugal.centrifuge.protocol.rs"
));

/// The websocket subprotocol selecting the Protobuf encoding.
pub(crate) const SUBPROTOCOL: &str = "centrifuge-protobuf";
