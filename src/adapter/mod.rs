//! The adapters: perform the actions of the sans-IO core.
//!
//! The shared layer (client, subscriptions, handlers, delivery, the driver
//! loop) names no runtime; it reaches the runtime through `platform` and
//! the transports through `backend`: `tokio` natively, `browser` on
//! wasm32-unknown-unknown (Block 44, D2).

mod body;
#[cfg(adapter_browser)]
mod browser;
mod client;
mod handler;
mod log;
#[cfg(feature = "unstable")]
mod map_subscription;
mod outbox;
mod platform;
mod ready;
mod subscription;
#[cfg(adapter_tokio)]
mod tokio;

/// The transports of this build's target (Block 44, D2).
#[cfg(adapter_browser)]
use self::browser as backend;
#[cfg(adapter_tokio)]
use self::tokio as backend;

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
pub use client::{Batch, Client, ClientBuilder, WeakClient};
pub use handler::{ClientHandler, EventDelivery, SubscriptionHandler};
#[cfg(feature = "unstable")]
pub use map_subscription::{MapSubscription, MapSubscriptionBuilder};
pub use subscription::{Subscription, SubscriptionBuilder};
