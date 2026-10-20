//! Centrifugo client SDK.
//!
//! Behaviour follows the Centrifugo Client SDK spec; see `CLAUDE.md` for
//! scope, references and the scenarios each block implements.

#[cfg(feature = "tokio")]
mod adapter;
mod backoff;
mod json;
pub(crate) mod protocol;
mod wire;
