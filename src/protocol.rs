//! Wire types generated from `proto/client.proto`.
//!
//! These are the Protobuf encoding of the client protocol. The JSON encoding
//! uses the same field names but carries `bytes` fields as raw embedded JSON,
//! so it needs its own codec rather than a serde derive on these types.

#![allow(dead_code)] // Consumed as blocks are implemented.
// Generated code: its style is prost's, not ours.
#![allow(missing_docs, unreachable_pub, clippy::pedantic)]

include!(concat!(
    env!("OUT_DIR"),
    "/centrifugal.centrifuge.protocol.rs"
));
