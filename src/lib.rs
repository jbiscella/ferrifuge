//! A [Centrifugo](https://centrifugal.dev) client: real-time messaging over
//! websocket, with `http_stream` and sse fallback, JSON or Protobuf.
//!
//! The client follows the Centrifugo client SDK specification: connection
//! states and reconnects with backoff, connection and subscription tokens,
//! client-side and server-side subscriptions with recovery of missed
//! publications, calls (RPC, publish, history, presence), batching and
//! Fossil delta compression.
//!
//! [`Client`] runs on tokio (the default `tokio` feature). Its events, and
//! each [`Subscription`]'s, come as a stream: a publication counts as
//! delivered when the stream yields it, and a reader that falls behind
//! holds the server back instead of growing the client's memory. The
//! [`machine`] module is the sans-IO core the tokio adapter drives, for
//! other runtimes.
//!
//! ```no_run
//! use ferrifuge::{Client, Config, SubscriptionEvent};
//! use futures_util::StreamExt;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::new("ws://localhost:8000/connection/websocket", Config::default())?;
//! let subscription = client.new_subscription("news")?;
//! // Taken before connect() and subscribe(), so that no event is missed.
//! let mut events = subscription.events();
//! client.connect();
//! subscription.subscribe();
//! while let Some(event) = events.next().await {
//!     if let SubscriptionEvent::Publication(publication) = event {
//!         println!("{}", String::from_utf8_lossy(&publication.data));
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The TLS crypto provider is chosen at build time: `ring` (default),
//! `aws-lc-rs`, or `fips`. The repository's README covers configuration,
//! tokens, transports, recovery and the examples.

#![cfg_attr(docsrs, feature(doc_cfg))]

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
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
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
