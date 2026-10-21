//! Centrifugo client SDK.
//!
//! Behaviour follows the Centrifugo Client SDK spec; see `CLAUDE.md` for
//! scope, references and the scenarios each block implements.

#[cfg(feature = "tokio")]
mod adapter;
mod backoff;
mod codec;
pub mod codes;
mod config;
mod event;
mod json;
pub mod machine;
pub(crate) mod protocol;
mod token;
mod wire;

#[cfg(feature = "tokio")]
pub use adapter::{Client, ClientBuilder};
pub use config::Config;
pub use event::{ClientError, ClientEvent, ErrorKind, State};
pub use token::TokenError;
