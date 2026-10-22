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
mod protobuf;
pub(crate) mod protocol;
mod subscription;
mod token;
mod wire;

#[cfg(feature = "tokio")]
pub use adapter::{Client, ClientBuilder, Subscription, SubscriptionBuilder};
pub use config::{Config, Protocol};
pub use event::{ClientError, ClientEvent, ErrorKind, State};
pub use subscription::{
    ClientInfo, DataError, Publication, Subscribed, SubscriptionError, SubscriptionEvent,
    SubscriptionState,
};
pub use token::TokenError;
