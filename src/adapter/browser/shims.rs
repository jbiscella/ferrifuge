//! The constructors and functions the browser transports use (Block 44,
//! D12; centrifuge-js 63ed73e `_resolveTransportDeps`, `centrifuge.ts:1020-
//! 1066`): the application's shim, else the global of that name on
//! `globalThis`, looked up at each attempt, so a global removed under a
//! running client is missed at the next one.

use js_sys::{Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};

use crate::machine::TransportKind;

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

/// What an attempt found.
pub(in crate::adapter) struct Found {
    pub(in crate::adapter) websocket: Option<Function>,
    pub(in crate::adapter) fetch: Option<Function>,
    /// The application's `ReadableStream`: the body is read through a stream
    /// built with it; without one, directly (D12).
    pub(in crate::adapter) readable_stream: Option<Function>,
    /// Whether a `ReadableStream` can be had, the shim's or the page's.
    has_readable_stream: bool,
    /// Whether the page has `AbortController`, which has no shim.
    has_abort_controller: bool,
    pub(in crate::adapter) event_source: Option<Function>,
}

/// The global `name` on `globalThis`, if a function.
fn global(name: &str) -> Option<Function> {
    Reflect::get(&js_sys::global(), &JsValue::from_str(name))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
}

impl Shims {
    /// Looks the constructors up, as each attempt does.
    pub(in crate::adapter) fn find(&self) -> Found {
        let readable_stream = self.readable_stream.clone();
        Found {
            websocket: self.websocket.clone().or_else(|| global("WebSocket")),
            fetch: self.fetch.clone().or_else(|| global("fetch")),
            has_readable_stream: readable_stream.is_some() || global("ReadableStream").is_some(),
            readable_stream,
            has_abort_controller: global("AbortController").is_some(),
            event_source: self.event_source.clone().or_else(|| global("EventSource")),
        }
    }
}

impl Found {
    /// Whether a transport of `kind` can be opened, as centrifuge-js's
    /// transports' `supported()` tell: websocket needs `WebSocket`,
    /// `http_stream` `fetch`, `ReadableStream` and `AbortController`
    /// (`transport_http_stream.ts:93-102`), sse `EventSource` and `fetch`
    /// (`transport_sse.ts:31-33`).
    pub(in crate::adapter) fn usable(&self, kind: TransportKind) -> bool {
        match kind {
            TransportKind::WebSocket => self.websocket.is_some(),
            TransportKind::HttpStream => {
                self.fetch.is_some() && self.has_readable_stream && self.has_abort_controller
            }
            TransportKind::Sse => self.event_source.is_some() && self.fetch.is_some(),
            // The browser's WebTransport comes with Block 48c; until then
            // the transport is skipped in the round.
            #[cfg(feature = "unstable")]
            TransportKind::WebTransport => false,
        }
    }
}

/// What a transport of `kind` needs, and the shims that can stand in.
fn needs(kind: TransportKind) -> &'static str {
    match kind {
        TransportKind::WebSocket => "websocket needs WebSocket (the websocket shim)",
        TransportKind::HttpStream => {
            "http_stream needs fetch, ReadableStream and AbortController (the fetch and \
             readable_stream shims)"
        }
        TransportKind::Sse => "sse needs EventSource and fetch (the event_source and fetch shims)",
        #[cfg(feature = "unstable")]
        TransportKind::WebTransport => "webtransport needs WebTransport (no shim)",
    }
}

/// The configuration error of a client none of whose `kinds` can be used
/// (centrifuge-js "no supported transport found", `centrifuge.ts:1106-
/// 1126`): what each needs, and the shims that can stand in.
pub(in crate::adapter) fn none_usable(kinds: &[TransportKind]) -> String {
    let mut seen = Vec::new();
    for kind in kinds {
        if !seen.contains(kind) {
            seen.push(*kind);
        }
    }
    let needs: Vec<&str> = seen.into_iter().map(needs).collect();
    format!("no supported transport found: {}", needs.join("; "))
}

#[cfg(test)]
mod tests {
    use js_sys::Reflect;
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{Shims, none_usable};
    use crate::machine::TransportKind;

    /// Removes the global `name` while `body` runs, then puts it back.
    fn without<T>(name: &str, body: impl FnOnce() -> T) -> T {
        let global = js_sys::global();
        let key = JsValue::from_str(name);
        let saved = Reflect::get(&global, &key).expect("the global");
        Reflect::delete_property(&global, &key).expect("removed");
        let result = body();
        Reflect::set(&global, &key, &saved).expect("restored");
        result
    }

    fn function() -> js_sys::Function {
        js_sys::Function::new_no_args("")
    }

    #[wasm_bindgen_test]
    fn a_shim_stands_in_for_a_missing_global() {
        without("EventSource", || {
            assert!(!Shims::default().find().usable(TransportKind::Sse));
            let shims = Shims {
                event_source: Some(function()),
                ..Shims::default()
            };
            assert!(shims.find().usable(TransportKind::Sse));
        });
    }

    #[wasm_bindgen_test]
    fn each_transport_needs_its_own_constructors() {
        without("fetch", || {
            let found = Shims::default().find();
            assert!(!found.usable(TransportKind::HttpStream));
            assert!(!found.usable(TransportKind::Sse));
        });
        without("AbortController", || {
            assert!(!Shims::default().find().usable(TransportKind::HttpStream));
        });
        without("ReadableStream", || {
            assert!(!Shims::default().find().usable(TransportKind::HttpStream));
            let shims = Shims {
                readable_stream: Some(function()),
                ..Shims::default()
            };
            assert!(shims.find().usable(TransportKind::HttpStream));
        });
        without("WebSocket", || {
            let found = Shims::default().find();
            assert!(!found.usable(TransportKind::WebSocket));
            assert!(found.usable(TransportKind::HttpStream));
        });
    }

    #[wasm_bindgen_test]
    fn the_error_names_each_transport_once_with_its_shims() {
        let message = none_usable(&[TransportKind::Sse, TransportKind::Sse]);

        assert_eq!(
            message,
            "no supported transport found: sse needs EventSource and fetch (the \
             event_source and fetch shims)"
        );
    }
}
