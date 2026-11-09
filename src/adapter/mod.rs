//! Tokio adapter: performs the actions of the sans-IO core.
//!
//! The shared layer (client, subscriptions, handlers, delivery, the driver
//! loop) names no runtime; it reaches the runtime through `platform` and
//! the transports through `tokio` (Block 44, D2).

mod client;
mod handler;
mod log;
mod outbox;
mod platform;
mod ready;
mod subscription;
mod tokio;

#[cfg(target_os = "android")]
pub use self::tokio::android;
pub use self::tokio::connector::{BoxError, ConnectedStream, Connecting, Connector};
#[cfg(feature = "cookies")]
pub use self::tokio::cookies::{CookieJar, MemoryJar};
pub use self::tokio::proxy::{Proxy, ProxyTarget};
#[cfg(feature = "__test-hooks")]
pub use self::tokio::transport::stalled_websocket_writes;
pub use client::{Batch, Client, ClientBuilder, WeakClient};
pub use handler::{ClientHandler, EventDelivery, SubscriptionHandler};
pub use subscription::{Subscription, SubscriptionBuilder};
