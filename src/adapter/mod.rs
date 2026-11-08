//! Tokio adapter: performs the actions of the sans-IO core.

#[cfg(target_os = "android")]
pub mod android;
mod client;
mod connector;
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
pub use connector::{BoxError, ConnectedStream, Connecting, Connector};
pub use proxy::{Proxy, ProxyTarget};
pub use subscription::{Subscription, SubscriptionBuilder};
#[cfg(feature = "__test-hooks")]
pub use transport::stalled_websocket_writes;
