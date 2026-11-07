//! Tokio adapter: performs the actions of the sans-IO core.

#[cfg(target_os = "android")]
pub mod android;
mod client;
mod http;
mod log;
mod net;
mod outbox;
mod proxy;
mod ready;
mod subscription;
mod tls;
mod transport;

pub use client::{Batch, Client, ClientBuilder};
pub use proxy::{Proxy, ProxyTarget};
pub use subscription::{Subscription, SubscriptionBuilder};
#[cfg(feature = "test-util")]
pub use transport::stalled_websocket_writes;
