//! The adapters: perform the actions of the sans-IO core.
//!
//! The shared layer (client, subscriptions, handlers, delivery, the driver
//! loop) names no runtime; it reaches the runtime through `platform` and
//! the transports through `backend`: `tokio` natively, `browser` on
//! wasm32-unknown-unknown.
// Block 44, D2.

mod body;
#[cfg(adapter_browser)]
mod browser;
mod builder;
mod client;
#[cfg(test)]
#[cfg(adapter_tokio)]
mod coop_tests;
mod driver;
mod handler;
#[cfg(feature = "__test-hooks")]
mod hooks;
mod kind;
mod log;
#[cfg(feature = "unstable")]
mod map_subscription;
mod outbox;
mod platform;
mod ready;
mod shared;
#[cfg(feature = "unstable")]
mod shared_poll_subscription;
mod subscription;
#[cfg(test)]
#[cfg(adapter_tokio)]
mod test_support;
#[cfg(adapter_tokio)]
mod tokio;
#[cfg(test)]
#[cfg(adapter_tokio)]
pub(crate) use self::tokio::http::header_set_sample;

/// The transports of this build's target.
// Block 44, D2.
#[cfg(adapter_browser)]
use self::browser as backend;
#[cfg(adapter_tokio)]
use self::tokio as backend;

#[cfg(feature = "__test-hooks")]
pub use self::body::{StreamDecoder, StreamFormat, decode_stream};
#[cfg(all(adapter_tokio, target_os = "android"))]
pub use self::tokio::android;
#[cfg(adapter_tokio)]
pub use self::tokio::connector::{BoxError, ConnectedStream, Connecting, Connector};
#[cfg(all(adapter_tokio, feature = "cookies"))]
pub use self::tokio::cookies::{CookieJar, MemoryJar};
#[cfg(adapter_tokio)]
pub use self::tokio::proxy::{Proxy, ProxyTarget};
#[cfg(all(adapter_tokio, feature = "__test-hooks"))]
pub use self::tokio::transport::stalled_websocket_writes;
pub use builder::ClientBuilder;
pub use client::{Batch, Client, WeakClient};
pub use handler::{ClientHandler, EventDelivery, SubscriptionHandler};
#[cfg(feature = "unstable")]
pub use handler::{MapSubscriptionHandler, SharedPollSubscriptionHandler};
#[cfg(feature = "__test-hooks")]
pub use hooks::{ArmedTimer, ArmedTimers, CallRecord};
#[cfg(feature = "unstable")]
pub use map_subscription::{MapSubscription, MapSubscriptionBuilder};
#[cfg(feature = "unstable")]
pub use shared_poll_subscription::{SharedPollSubscription, SharedPollSubscriptionBuilder};
pub use subscription::{Subscription, SubscriptionBuilder};
