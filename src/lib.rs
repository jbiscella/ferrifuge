//! A [Centrifugo](https://centrifugal.dev) client: real-time messaging over
//! websocket, with `http_stream` and sse fallback, JSON or Protobuf.
//!
//! The client follows the Centrifugo client SDK specification: connection
//! states and reconnects with backoff, connection and subscription tokens,
//! client-side and server-side subscriptions with recovery of missed
//! publications, calls (RPC, publish, history, presence), batching and
//! Fossil delta compression.
//!
//! # Adapters
//!
//! One [`Client`] API runs on two adapters, chosen by target:
//!
//! - natively, on tokio (the `tokio` feature), over websocket,
//!   `http_stream` and sse through rustls;
//! - in the browser, on `wasm32-unknown-unknown` (the `browser` feature),
//!   over the browser's `WebSocket`, `fetch` and `EventSource`, on the
//!   page's event loop.
//!
//! Both drive the same sans-IO core, which turns inputs (calls, frames,
//! timers) into actions. Another runtime can drive it too: it is the
//! `machine` module, public with the `unstable-machine` feature.
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
//! # Streams and handlers
//!
//! A [`Client`] and each [`Subscription`] deliver their events in one of
//! two ways:
//!
//! - as a stream, from `events()`: an event counts as delivered when the
//!   stream yields it, and a reader that falls behind holds the server back
//!   instead of growing the client's memory;
//! - to a handler, a closure or a [`ClientHandler`] or
//!   [`SubscriptionHandler`], set with [`ClientBuilder::handler`],
//!   [`Client::set_handler`], [`SubscriptionBuilder::handler`] or
//!   [`Subscription::set_handler`]: an event counts as delivered when its
//!   handler returns.
//!
//! A handler takes over its object's stream. [`EventDelivery`], set with
//! [`ClientBuilder::event_delivery`], chooses where a client's handlers run:
//! on a task of its runtime (the default), on the client's own driver, or,
//! natively, on a thread of their own for handlers that block.
//!
//! # Cargo features
//!
//! | Feature | Default | Adds |
//! |---|---|---|
//! | `tokio` | yes | the native adapter: [`Client`] over websocket, `http_stream` and sse |
//! | `ring` | yes | ring as the TLS crypto provider |
//! | `aws-lc-rs` | no | aws-lc-rs as the TLS crypto provider |
//! | `fips` | no | aws-lc-rs in FIPS mode; building it needs `CMake` and Go |
//! | `browser` | yes | the browser adapter on `wasm32-unknown-unknown`; nothing on other targets |
//! | `cookies` | no | a cookie jar for the native client |
//! | `test-util` | no | constructors of the events and results the client yields, and natively an in-memory server double, for an application's tests |
//! | `unstable` | no | map and shared poll subscriptions, and the browser's WebTransport |
//! | `unstable-webtransport` | no | the native WebTransport transport; implies `unstable` |
//! | `unstable-machine` | no | the sans-IO core, `ferrifuge::machine` |
//!
//! `tokio` needs one TLS provider. When several are enabled the strongest
//! is used: `fips`, then `aws-lc-rs`, then `ring`. A build without ring
//! turns default features off.
//!
//! The three `unstable` features are outside the semver promise: what they
//! enable may change in any minor release. `unstable` and
//! `unstable-webtransport` follow centrifuge-js 63ed73e and Centrifugo's
//! experimental features; `unstable-machine` exposes the core's own API.
//!
//! # Guides
//!
//! The [README](https://github.com/jbiscella/ferrifuge#readme) covers
//! configuration, tokens, subscriptions and recovery, calls, transports and
//! fallback, codes, TLS, proxies, mobile and browser builds, testing an
//! application, and the examples.
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
//! sees as anomalies, which the adapters log at WARN.
//!
//! Ferrifuge's dependencies log on their own, through `log`. At TRACE,
//! tungstenite logs the websocket upgrade request with its header values and
//! every frame, the connect frame and its token included: keep them at INFO
//! or above in production, e.g. `RUST_LOG=info,ferrifuge=trace`.

#![cfg_attr(docsrs, feature(doc_cfg))]
// Library-only lints (Block 60, D4): crate attributes, so `tests/`,
// `benches/` and `examples/` keep their own. `clippy.toml` allows `unwrap`
// in test code.
#![deny(clippy::unwrap_used)]
#![warn(clippy::allow_attributes_without_reason)]
// Without an adapter and without `unstable-machine` nothing drives the
// core, so most of it is unused in that build (Block 54, D1).
#![cfg_attr(
    not(any(feature = "unstable-machine", adapter_tokio, adapter_browser)),
    allow(
        dead_code,
        reason = "nothing drives the core without an adapter or `unstable-machine`"
    )
)]

#[cfg(any(adapter_tokio, adapter_browser))]
mod adapter;
#[cfg(test)]
mod api_shape_tests;
mod backoff;
mod call;
mod callback;
mod codec;
pub mod codes;
mod config;
mod event;
mod fossil;
mod json;
// The core is declared once (Block 54, D1), so that every tool walking the
// module tree, cargo-mutants among them, sees it once.
#[cfg_attr(
    not(feature = "unstable-machine"),
    allow(
        unreachable_pub,
        dead_code,
        reason = "crate-private without the feature: its pub items are unreachable, and those only an owner of the machine uses are unused"
    )
)]
mod sans_io;
#[cfg(feature = "unstable-machine")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-machine")))]
pub mod machine {
    //! Unstable: requires the `unstable-machine` feature. The API by which
    //! another runtime drives the core may change in any minor release; the
    //! `Client` of the `tokio` and `browser` features is the stable API.
    //!
    //! Sans-IO core: the client connection state machine.
    //!
    //! Timers are numbered by setting: each [`Action::SetTimer`],
    //! [`Action::SetSubscriptionTimer`] and [`Action::SetCallTimer`] carries a
    //! [`TimerSetting`] the machine never gives again. The owner returns it
    //! unchanged in the [`Input::TimerFired`], [`Input::SubscriptionTimerFired`]
    //! or [`Input::CallTimerFired`] of that setting; a timer shortened by
    //! [`Action::ShortenTimer`] or [`Action::ShortenSubscriptionTimer`] keeps
    //! its setting. The machine ignores a firing whose setting no longer runs,
    //! because it cancelled that timer, set it again or already took its
    //! firing: the step returns no action. The owner need not drop a late
    //! firing, whatever it fed in between, [`Input::Slept`] included.
    //!
    //! A [`Machine`] owns no sockets, timers or tasks. Its owner feeds it
    //! [`Input`]s and performs the returned [`Action`]s in order. The tokio
    //! adapter is one such owner; any other runtime can drive it the same way.
    //!
    //! # System sleep
    //!
    //! The owner tells the machine that the system slept with
    //! [`Input::Slept`], and applies four rules around it, as the tokio and
    //! browser adapters do:
    //!
    //! 1. **Detection.** At each input it gives, the owner compares how far the
    //!    wall clock and the monotonic clock moved since the previous one. When
    //!    the wall clock moved further by at least 2 s, it feeds
    //!    `Input::Slept { gap }` with the difference. A wall clock set back
    //!    gives no gap, and the machine ignores gaps under 2 s.
    //! 2. **Ordering.** [`Input::Close`] and [`Input::DriverDropped`] are fed
    //!    alone, without `Slept`. While connected, [`Input::EventTaken`],
    //!    [`Input::EventHanded`], [`Input::HandlerPanicked`], [`Input::Frame`],
    //!    [`Input::TransportClosed`] and [`Input::Reconnect`] are fed in this
    //!    order: the due refresh firings (rule 3), then the input, then
    //!    `Slept`. Every other input, and those six while not connected, are
    //!    fed in this order: `Slept`, then the due refresh firings, then the
    //!    input. The owner performs the actions of `Slept`'s answer even when
    //!    an application call revealed the sleep.
    //! 3. **Refresh deadlines.** The owner records each token refresh timer's
    //!    deadline and setting from the [`Action::SetTimer`] and
    //!    [`Action::SetSubscriptionTimer`] refresh actions, and forgets them on
    //!    [`Action::CancelTimer`] and [`Action::CancelSubscriptionTimer`]. On a
    //!    sleep it moves all of them earlier by the gap. It feeds the ones now
    //!    due as [`Input::TimerFired`] or [`Input::SubscriptionTimerFired`] with
    //!    their setting, the connection's first, then by subscription, and
    //!    cancels its own timers for them. Each refresh is fed once: a refresh
    //!    firing the machine is given removes that deadline.
    //! 4. **Shorten filter.** When the input goes before `Slept`, the owner
    //!    drops from `Slept`'s answer every [`Action::ShortenTimer`] and
    //!    [`Action::ShortenSubscriptionTimer`] for a timer that the input's
    //!    answer just set, including the answers to the event takes the input
    //!    caused: those timers lost no time to the sleep.
    //!
    //! Behaviour follows the Client SDK spec, with centrifuge-js as the
    //! implementation reference (`src/centrifuge.ts`).

    pub use crate::sans_io::*;
}
#[cfg(feature = "unstable")]
mod map;
mod maybe_send;
mod protobuf;
pub(crate) mod protocol;
#[cfg(feature = "unstable")]
mod shared_poll;
mod statistics;
mod subscription;
mod tag_filter;
#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod test_util;
mod token;
mod wire;

/// Slots a queue of the client keeps once it is empty: what a burst grew is
/// released, so that a client at rest holds the same memory whatever bursts
/// it went through.
// Block 14, "Memory stays flat across reconnects".
pub(crate) const RESTING_SLOTS: usize = 4;

#[cfg(all(adapter_tokio, target_os = "android"))]
#[cfg_attr(docsrs, doc(cfg(target_os = "android")))]
pub use adapter::android;
#[cfg(all(adapter_tokio, feature = "__test-hooks"))]
#[doc(hidden)]
pub use adapter::stalled_websocket_writes;
#[cfg(all(any(adapter_tokio, adapter_browser), feature = "__test-hooks"))]
#[doc(hidden)]
pub use adapter::{ArmedTimer, ArmedTimers, CallRecord};
#[cfg(all(any(adapter_tokio, adapter_browser), feature = "__test-hooks"))]
#[doc(hidden)]
pub use adapter::{StreamDecoder, StreamFormat, decode_stream};
// The same client on both targets (Block 44, D1).
#[cfg(any(adapter_tokio, adapter_browser))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "browser"))))]
pub use adapter::{
    Batch, Client, ClientBuilder, ClientHandler, EventDelivery, Subscription, SubscriptionBuilder,
    SubscriptionHandler, WeakClient,
};
#[cfg(all(any(adapter_tokio, adapter_browser), feature = "unstable"))]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub use adapter::{
    MapSubscription, MapSubscriptionBuilder, MapSubscriptionHandler, SharedPollSubscription,
    SharedPollSubscriptionBuilder, SharedPollSubscriptionHandler,
};
// Native only (Block 44, D14).
#[cfg(adapter_tokio)]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use adapter::{BoxError, ConnectedStream, Connecting, Connector, Proxy, ProxyTarget};
#[cfg(all(adapter_tokio, feature = "cookies"))]
#[cfg_attr(docsrs, doc(cfg(feature = "cookies")))]
pub use adapter::{CookieJar, MemoryJar};
pub use call::{
    CallError, HistoryOptions, HistoryResult, JsonCallError, PresenceResult, PresenceStatsResult,
    PublishResult, RpcResult,
};
#[cfg(feature = "unstable")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub use callback::SharedPollSignatureContext;
pub use callback::{
    ConnectionDataContext, ConnectionTokenContext, GetStateContext, SubscriptionDataContext,
    SubscriptionTokenContext,
};
pub use config::{Config, Protocol};
pub use event::{
    ClientError, ClientEvent, ConnectedContext, ConnectingContext, DisconnectedContext, ErrorKind,
    MessageContext, ServerSubscriptionEvent, State,
};
#[cfg(feature = "__test-hooks")]
#[doc(hidden)]
pub use fossil::write as write_fossil_delta;
/// The `http` crate, whose `Uri` a [`Connector`] is given. Moving to
/// another major version of it is a breaking change of ferrifuge's.
// Block 37b.
#[cfg(adapter_tokio)]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use http;
#[cfg(feature = "unstable")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub use map::{MapEntry, MapKind, MapSubscriptionEvent, MapUpdate, UnrecoverableStrategy};
pub use maybe_send::{MaybeSend, MaybeSync};
/// rustls, whose types [`ClientBuilder::root_certificates`] and
/// [`ClientBuilder::tls_config`] take. Moving to another rustls 0.x line is
/// a breaking release of Ferrifuge.
// Block 22.
#[cfg(adapter_tokio)]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use rustls;
pub use sans_io::TransportKind;
pub(crate) use sans_io::{CallReply, CallRequest};
#[cfg(feature = "unstable")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
pub use shared_poll::{
    SharedPollSignature, SharedPollSubscriptionEvent, SharedPollTrackItem, SharedPollUpdate,
};
pub use statistics::{DeltaStatistics, Statistics};
pub use subscription::{
    ClientInfo, DataError, DeltaFormat, Publication, StateError, StreamPosition, SubscribedContext,
    SubscribingContext, SubscriptionError, SubscriptionEvent, SubscriptionState,
    UnsubscribedContext,
};
pub use tag_filter::TagFilter;
pub use token::TokenError;
/// The `url` crate, whose `Url` a [`CookieJar`] is given. Moving to another
/// major version of it is a breaking change of ferrifuge's.
// Block 43, D1.
#[cfg(all(adapter_tokio, feature = "cookies"))]
#[cfg_attr(docsrs, doc(cfg(feature = "cookies")))]
pub use url;
