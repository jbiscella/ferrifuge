//! The browser's network and page events (Block 44, D10, D11; centrifuge-js
//! 63ed73e `_setNetworkEvents`, `_setPageEvents`): `offline`, `online`,
//! `pagehide` and `pageshow`, listened to while the client is not
//! disconnected.

use std::sync::Weak;

use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{Event, EventTarget};

use crate::State;
use crate::adapter::client::Shared;
use crate::machine::Input;

/// What a listener heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::adapter) enum Heard {
    Offline,
    Online,
    /// `pagehide` with `persisted` true: the page entered the back/forward
    /// cache.
    PageCached,
    /// `pageshow` with `persisted` true: the page left it.
    PageRestored,
}

/// A listener on its target, by the event's name.
type Listener = (EventTarget, &'static str, Closure<dyn FnMut(Event)>);

/// The client's network and page listeners: on its targets while the
/// client is not disconnected, removed before their closures are dropped,
/// so that the browser never calls a dropped one (D6).
pub(in crate::adapter) struct Events {
    /// The given target, else `globalThis` when it has `addEventListener`.
    network: Option<EventTarget>,
    page: Option<EventTarget>,
    /// The listeners do not keep the client alive (D10).
    client: Weak<Shared>,
    listeners: Vec<Listener>,
    /// The client cached the page and has not reconnected since (D11).
    cached: bool,
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events")
            .field("listeners", &self.listeners.len())
            .field("cached", &self.cached)
            .finish_non_exhaustive()
    }
}

impl Events {
    pub(in crate::adapter) fn new(
        network: Option<EventTarget>,
        page: Option<EventTarget>,
        client: Weak<Shared>,
    ) -> Self {
        let global = global_target();
        Self {
            network: network.or_else(|| global.clone()),
            page: page.or(global),
            client,
            listeners: Vec::new(),
            cached: false,
        }
    }

    /// Follows the client's `state`: listening unless disconnected, and
    /// forgetting a cached page once connected or disconnected (D11).
    pub(in crate::adapter) fn follow(&mut self, state: State) {
        if state != State::Connecting {
            self.cached = false;
        }
        if state == State::Disconnected {
            self.stop();
        } else if self.listeners.is_empty() {
            self.start();
        }
    }

    /// The input `heard` gives a client in `state`, if any (D10, D11).
    pub(in crate::adapter) fn input(&mut self, heard: Heard, state: State) -> Option<Input> {
        match (heard, state) {
            (_, State::Disconnected) => None,
            (Heard::Offline, _) => Some(Input::Offline),
            (Heard::PageCached, _) => {
                self.cached = true;
                Some(Input::PageCached)
            }
            (Heard::Online, State::Connecting) => Some(Input::Reconnect),
            (Heard::PageRestored, State::Connecting) if self.cached => {
                self.cached = false;
                Some(Input::Reconnect)
            }
            (Heard::Online | Heard::PageRestored, _) => None,
        }
    }

    fn start(&mut self) {
        if let Some(network) = self.network.clone() {
            self.add(&network, "offline");
            self.add(&network, "online");
        }
        if let Some(page) = self.page.clone() {
            self.add(&page, "pagehide");
            self.add(&page, "pageshow");
        }
    }

    fn add(&mut self, target: &EventTarget, name: &'static str) {
        let client = Weak::clone(&self.client);
        let listener = move |event: Event| {
            let heard = match name {
                "offline" => Heard::Offline,
                "online" => Heard::Online,
                "pagehide" if persisted(&event) => Heard::PageCached,
                "pageshow" if persisted(&event) => Heard::PageRestored,
                _ => return,
            };
            if let Some(client) = client.upgrade() {
                client.heard(heard);
            }
        };
        let closure = Closure::wrap(Box::new(listener) as Box<dyn FnMut(Event)>);
        let _ = target.add_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        self.listeners.push((target.clone(), name, closure));
    }

    fn stop(&mut self) {
        let listeners = std::mem::take(&mut self.listeners);
        for (target, name, closure) in &listeners {
            let _ =
                target.remove_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        }
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        self.stop();
    }
}

/// `globalThis`, when it has `addEventListener`: a page's or a worker's
/// global, not Node's.
fn global_target() -> Option<EventTarget> {
    let global = js_sys::global();
    js_sys::Reflect::get(&global, &JsValue::from_str("addEventListener"))
        .is_ok_and(|add| add.is_function())
        .then(|| global.unchecked_into())
}

/// `PageTransitionEvent.persisted`, read by name, as the event may be a
/// plain `Event` a test dispatched.
fn persisted(event: &Event) -> bool {
    js_sys::Reflect::get(event, &JsValue::from_str("persisted"))
        .is_ok_and(|persisted| persisted.as_bool() == Some(true))
}
