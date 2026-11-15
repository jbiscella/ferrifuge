//! The options only the browser client has: their setters, their checks
//! and what the client keeps of them.
// Block 61, D2.

use std::sync::{Mutex, Weak};

use super::super::builder::{self, ClientBuilder};
use super::super::platform::Handle;
use super::super::shared::Shared;
use super::events::Events;
use super::shims::Shims;
use super::websocket::Bound;
use crate::sans_io::TransportKind;
use crate::{ClientError, Config};

/// The builder's browser options, as given.
#[derive(Default)]
pub(in crate::adapter) struct Options {
    /// Where `offline` and `online` are listened to.
    // Block 44, D10.
    network_event_target: Option<web_sys::EventTarget>,
    /// Where `pagehide` and `pageshow` are listened to.
    // Block 44, D11.
    page_event_target: Option<web_sys::EventTarget>,
    /// The application's constructors in place of the page's.
    // Block 44, D12.
    shims: Shims,
    /// Whether HTTP requests carry credentials to other origins.
    // Block 44, D13.
    include_credentials: bool,
}

/// What the browser client's [`Shared`] keeps of the options.
#[derive(Debug)]
pub(in crate::adapter) struct SharedPart {
    /// The browser's network and page listeners. Locked after `core`.
    // Block 44, D10, D11.
    pub(in crate::adapter) events: Mutex<Events>,
}

/// What [`SharedPart`] is made from: the targets its listeners listen to.
pub(in crate::adapter) struct SharedSeed {
    network_event_target: Option<web_sys::EventTarget>,
    page_event_target: Option<web_sys::EventTarget>,
}

impl SharedSeed {
    /// The part of `shared`, whose listeners hold it weakly.
    pub(in crate::adapter) fn into_part(self, shared: &Weak<Shared>) -> SharedPart {
        SharedPart {
            events: Mutex::new(Events::new(
                self.network_event_target,
                self.page_event_target,
                Weak::clone(shared),
            )),
        }
    }
}

/// What the browser driver keeps of the options.
pub(in crate::adapter) struct DriverPart {
    /// The bound of a browser transport's inbox.
    // Block 44, D7.
    pub(in crate::adapter) read_ahead: Bound,
    /// The application's constructors, looked up with the page's at each
    /// attempt.
    // Block 44, D12.
    pub(in crate::adapter) shims: Shims,
    /// Whether HTTP requests carry credentials to other origins.
    // Block 44, D13.
    pub(in crate::adapter) include_credentials: bool,
}

/// The options checked, for the client being started.
pub(in crate::adapter) struct Started {
    pub(in crate::adapter) shared: SharedSeed,
    pub(in crate::adapter) driver: DriverPart,
    /// The page's event loop.
    pub(in crate::adapter) runtime: Runtime,
}

/// The page's event loop, which needs no handle.
// Block 44, D4.
pub(in crate::adapter) struct Runtime;

impl Runtime {
    /// The page's event loop, always there.
    #[expect(
        clippy::unnecessary_wraps,
        clippy::unused_self,
        reason = "the native runtime's check can fail"
    )]
    pub(in crate::adapter) const fn resolve(self) -> Result<Handle, ClientError> {
        Ok(Handle)
    }
}

impl Options {
    /// Writes the options into the builder's `Debug`.
    pub(in crate::adapter) fn fields(&self, f: &mut std::fmt::DebugStruct<'_, '_>) {
        f.field("network_event_target", &self.network_event_target.is_some())
            .field("page_event_target", &self.page_event_target.is_some())
            .field("shims", &self.shims)
            .field("include_credentials", &self.include_credentials);
    }

    /// Runs the browser checks, for `endpoints`: each endpoint, and that the
    /// page or the shims can open one of their transports.
    pub(in crate::adapter) fn start(
        self,
        endpoints: &[(TransportKind, String)],
        _emulation_endpoint: Option<&str>,
        config: &Config,
    ) -> Result<Started, ClientError> {
        let Self {
            network_event_target,
            page_event_target,
            shims,
            include_credentials,
        } = self;
        for (kind, endpoint) in endpoints {
            builder::check_endpoint(*kind, endpoint)?;
        }
        builder::check_usable(endpoints, &shims)?;
        Ok(Started {
            shared: SharedSeed {
                network_event_target,
                page_event_target,
            },
            driver: DriverPart {
                read_ahead: Bound {
                    messages: config.read_ahead_messages,
                    bytes: config.read_ahead_bytes,
                },
                shims,
                include_credentials,
            },
            runtime: Runtime,
        })
    }
}

impl ClientBuilder {
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Listens to `offline` and `online` on `target` instead of
    /// `globalThis` (centrifuge-js `networkEventTarget`): while the client
    /// is not disconnected, `offline` closes its connection, which then
    /// waits the reconnect delay, and `online` while connecting reconnects
    /// it at once, or after a draw from [`Config::reconnect_jitter_window`]
    /// when it lost its connection. A target that never fires,
    /// `EventTarget::new()`, turns this off.
    // Block 44, D10.
    #[must_use]
    pub fn network_event_target(mut self, target: web_sys::EventTarget) -> Self {
        self.backend.network_event_target = Some(target);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Listens to `pagehide` and `pageshow` on `target` instead of
    /// `globalThis`: a `pagehide` whose `persisted` is true, the page
    /// entering the back/forward cache, closes the connection with the
    /// reason "page entered back/forward cache", and the `pageshow` that
    /// restores the page reconnects as `online` does. Events whose
    /// `persisted` is false change nothing. A target that never fires turns
    /// this off.
    // Block 44, D11.
    #[must_use]
    pub fn page_event_target(mut self, target: web_sys::EventTarget) -> Self {
        self.backend.page_event_target = Some(target);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Opens websocket connections with `constructor`, called as one with
    /// the URL and the subprotocol, in place of the page's `WebSocket`
    /// (centrifuge-js `websocket`). The object it returns is used through
    /// its `WebSocket` methods and properties, by name.
    // Block 44, D12.
    #[must_use]
    pub fn websocket(mut self, constructor: js_sys::Function) -> Self {
        self.backend.shims.websocket = Some(constructor);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Makes the `http_stream` and sse transports' HTTP requests with
    /// `fetch`, called as the page's `fetch` is, `fetch(url, init)` with a
    /// plain `init`, in its place (centrifuge-js `fetch`).
    // Block 44, D12.
    #[must_use]
    pub fn fetch(mut self, fetch: js_sys::Function) -> Self {
        self.backend.shims.fetch = Some(fetch);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Reads the `http_stream` response through a stream built with
    /// `constructor`, called as `new ReadableStream(source, strategy)` is,
    /// in place of reading the body directly (centrifuge-js
    /// `readableStream`): its source's `pull` enqueues each chunk of the
    /// body unchanged, with `highWaterMark` 0, so the body is still read
    /// only when the client wants messages. With it the page needs no
    /// global `ReadableStream` for `http_stream`.
    // Block 44, D12.
    #[must_use]
    pub fn readable_stream(mut self, constructor: js_sys::Function) -> Self {
        self.backend.shims.readable_stream = Some(constructor);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Opens sse streams with `constructor`, called as one with the URL and
    /// `{ withCredentials }`, in place of the page's `EventSource`
    /// (centrifuge-js `eventsource`). The object it returns is used through
    /// its `EventSource` methods and properties, by name.
    ///
    /// Each attempt uses the shim given, else the page's global of that
    /// name; a transport whose constructors neither provides is skipped,
    /// and [`ClientBuilder::build`] fails when no configured transport can
    /// be used: websocket needs `WebSocket`, `http_stream` `fetch`,
    /// `ReadableStream` and `AbortController`, sse `EventSource` and
    /// `fetch`.
    // Block 44, D12.
    #[must_use]
    pub fn event_source(mut self, constructor: js_sys::Function) -> Self {
        self.backend.shims.event_source = Some(constructor);
        self
    }

    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Whether the `http_stream` and sse transports' requests carry the
    /// page's cookies and HTTP authentication to a server on another
    /// origin, false by default: true sends fetch's `credentials:
    /// "include"` and `EventSource`'s `withCredentials: true`, so the
    /// server's `Set-Cookie` is stored too; false sends `credentials:
    /// "same-origin"` and `withCredentials: false`, as centrifuge-js does.
    /// The server must allow credentialed CORS requests from the page's
    /// origin, as Centrifugo's `allowed_origins` does.
    ///
    /// The websocket upgrade is unaffected: the browser sends its cookies
    /// by their `SameSite` rules whatever this says, and so is a
    /// WebTransport session (unstable), whose cookies the browser decides.
    // Block 44, D13; Block 48, D17.
    #[must_use]
    pub fn include_credentials(mut self, include: bool) -> Self {
        self.backend.include_credentials = include;
        self
    }
}
