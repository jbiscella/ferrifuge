//! Centrifugo client SDK.
//!
//! Behaviour follows the Centrifugo Client SDK spec; see `CLAUDE.md` for
//! scope, references and the scenarios each block implements.

#[cfg(feature = "tokio")]
mod adapter;
mod backoff;
mod call;
mod codec;
pub mod codes;
mod config;
mod event;
mod fossil;
mod json;
pub mod machine;
mod protobuf;
pub(crate) mod protocol;
mod subscription;
mod token;
mod wire;

#[cfg(all(feature = "tokio", feature = "test-util"))]
#[doc(hidden)]
pub use adapter::stalled_websocket_writes;
#[cfg(feature = "tokio")]
pub use adapter::{Batch, Client, ClientBuilder, Subscription, SubscriptionBuilder};
pub use call::{
    CallError, CallReply, CallRequest, HistoryOptions, HistoryResult, PresenceResult,
    PresenceStatsResult,
};
pub use config::{Config, Protocol};
pub use event::{ClientError, ClientEvent, ErrorKind, ServerSubscriptionEvent, State};
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub use fossil::write as write_fossil_delta;
pub use subscription::{
    ClientInfo, DataError, DeltaFormat, Publication, StateError, StreamPosition, Subscribed,
    SubscriptionError, SubscriptionEvent, SubscriptionState,
};
pub use token::TokenError;
