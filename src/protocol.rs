//! Wire types generated from `proto/client.proto`.
//!
//! These are the Protobuf encoding of the client protocol. The JSON encoding
//! uses the same field names but carries `bytes` fields as raw embedded JSON,
//! so it needs its own codec rather than a serde derive on these types.
//!
//! The code is committed: `src/protocol/centrifugal.centrifuge.protocol.rs`
//! is what the locked prost-build writes from `proto/client.proto`, the
//! copy of centrifugal/protocol at the commit `proto/README.md` names, with
//! the locked `protoc-bin-vendored` as protoc. `tests/protocol_codegen.rs`
//! fails when it drifts, and `FERRIFUGE_REGENERATE_PROTOCOL=1 cargo test
//! --test protocol_codegen` rewrites it.
// Block 56.

#![expect(
    dead_code,
    reason = "generated from client.proto: the client uses only some of its messages"
)]
#![expect(
    unreachable_pub,
    clippy::pedantic,
    reason = "generated code: its style is prost's, not ours"
)]

include!("protocol/centrifugal.centrifuge.protocol.rs");
