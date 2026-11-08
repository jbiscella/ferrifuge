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
//!
//! # Logging
//!
//! The tokio adapter logs through [`tracing`](https://docs.rs/tracing),
//! under targets starting with `ferrifuge`: WARN for server anomalies,
//! DEBUG for state changes, transport attempts, retries, callbacks and
//! failed calls, TRACE for every input and action of the core and every
//! frame, by length. Each client logs in a `client` span, whose `client`
//! field is a number unique in the process, and each subscription in a
//! `subscription` span with its `channel`. No record holds a token, a
//! payload, connection or channel info, or a header value. Without a
//! `tracing` subscriber the records go to the `log` crate, with no span
//! fields. The core logs nothing: it reports the protocol breaches only it
//! sees as [`machine::Action::Anomaly`].
//!
//! Ferrifuge's dependencies log on their own, through `log`. At TRACE,
//! tungstenite logs the websocket upgrade request with its header values and
//! every frame, the connect frame and its token included: keep them at INFO
//! or above in production, e.g. `RUST_LOG=info,ferrifuge=trace`.

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
mod statistics;
mod subscription;
mod tag_filter;
#[cfg(feature = "test-util")]
mod test_util;
mod token;
mod wire;

/// Slots a queue of the client keeps once it is empty: what a burst grew is
/// released, so that a client at rest holds the same memory whatever bursts
/// it went through (Block 14, "Memory stays flat across reconnects").
pub(crate) const RESTING_SLOTS: usize = 4;

#[cfg(all(feature = "tokio", target_os = "android"))]
pub use adapter::android;
#[cfg(all(feature = "tokio", feature = "__test-hooks"))]
#[doc(hidden)]
pub use adapter::stalled_websocket_writes;
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use adapter::{
    Batch, Client, ClientBuilder, Proxy, ProxyTarget, Subscription, SubscriptionBuilder,
};
pub use call::{
    CallError, CallReply, CallRequest, HistoryOptions, HistoryResult, JsonCallError,
    PresenceResult, PresenceStatsResult,
};
pub use config::{Config, Protocol};
pub use event::{ClientError, ClientEvent, ErrorKind, ServerSubscriptionEvent, State};
#[cfg(feature = "__test-hooks")]
#[doc(hidden)]
pub use fossil::write as write_fossil_delta;
/// rustls, whose types [`ClientBuilder::root_certificates`] and
/// [`ClientBuilder::tls_config`] take (Block 22). Moving to another rustls
/// 0.x line is a breaking release of Ferrifuge.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use rustls;
pub use statistics::{DeltaStatistics, Statistics};
pub use subscription::{
    ClientInfo, DataError, DeltaFormat, Publication, StateError, StreamPosition, Subscribed,
    SubscriptionError, SubscriptionEvent, SubscriptionState,
};
pub use tag_filter::TagFilter;
pub use token::TokenError;
