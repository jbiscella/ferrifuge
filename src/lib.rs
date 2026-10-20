//! Centrifugo client SDK.
//!
//! Behaviour follows the Centrifugo Client SDK spec; see `CLAUDE.md` for
//! scope, references and the scenarios each block implements.

#[cfg(feature = "tokio")]
mod adapter;
mod backoff;
pub mod codes;
mod config;
mod event;
mod json;
pub(crate) mod protocol;
mod wire;

#[cfg(feature = "tokio")]
pub use adapter::Client;
pub use config::Config;
pub use event::{ClientError, ClientEvent, ErrorKind, State};
