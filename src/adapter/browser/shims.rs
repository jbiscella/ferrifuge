//! The constructors and functions the browser transports use (Block 44,
//! D12; centrifuge-js 63ed73e `_resolveTransportDeps`, `centrifuge.ts:1020-
//! 1066`): the application's shim, else the global of that name on
//! `globalThis`, looked up at each attempt, so a global removed under a
//! running client is missed at the next one.

use js_sys::Function;

/// The shims the application gave ([`crate::ClientBuilder::websocket`],
/// [`crate::ClientBuilder::fetch`], [`crate::ClientBuilder::readable_stream`],
/// [`crate::ClientBuilder::event_source`]).
#[derive(Clone, Default)]
pub(in crate::adapter) struct Shims {
    pub(in crate::adapter) websocket: Option<Function>,
    pub(in crate::adapter) fetch: Option<Function>,
    pub(in crate::adapter) readable_stream: Option<Function>,
    pub(in crate::adapter) event_source: Option<Function>,
}

impl std::fmt::Debug for Shims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shims")
            .field("websocket", &self.websocket.is_some())
            .field("fetch", &self.fetch.is_some())
            .field("readable_stream", &self.readable_stream.is_some())
            .field("event_source", &self.event_source.is_some())
            .finish()
    }
}
