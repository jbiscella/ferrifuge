//! Tokio adapter: performs the actions of the sans-IO core.

#[cfg(target_os = "android")]
pub mod android;
mod client;
mod connector;
#[cfg(feature = "cookies")]
mod cookies;
mod handler;
mod http;
mod log;
mod net;
mod outbox;
mod proxy;
mod ready;
mod subscription;
mod tls;
mod transport;

pub use client::{Batch, Client, ClientBuilder, WeakClient};
pub use connector::{BoxError, ConnectedStream, Connecting, Connector};
#[cfg(feature = "cookies")]
pub use cookies::{CookieJar, MemoryJar};
pub use handler::{ClientHandler, EventDelivery, SubscriptionHandler};
pub use proxy::{Proxy, ProxyTarget};
pub use subscription::{Subscription, SubscriptionBuilder};
#[cfg(feature = "__test-hooks")]
pub use transport::stalled_websocket_writes;
