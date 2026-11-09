//! sse transport of the browser adapter, over the browser's own
//! `EventSource` or the application's shim of it (Block 44, D9, D12), with the bounded inbox of D7: the stream
//! carries the replies, its URL the connect command in `cf_connect`, and
//! commands go in emulation requests over `fetch`, as from `http_stream`
//! (D8).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use js_sys::{Function, Reflect};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::JsValue;
use web_sys::{Event, EventSourceInit, MessageEvent};

use super::http_stream::{Emulation, JSON, report};
use super::websocket::{Bound, Inbound, Inbox, Listener, js_message};
use crate::machine::FrameId;

/// Why the stream ended: `EventSource` tells no more (D9).
const ENDED: &str = "event stream ended";

/// How far the opening went.
enum Opening {
    Pending,
    Opened,
    Failed(String),
}

/// What the listeners and the transport share.
struct State {
    opening: Opening,
    inbox: Inbox,
    bound: Bound,
    max_message: usize,
    /// An `error`, a message over the size limit or an overflow came: the
    /// source is closed, and its later events are not taken.
    ended: bool,
    /// The inbox overflowed, reported in place of anything else.
    overflowed: bool,
    waker: Option<Waker>,
}

impl State {
    fn wake(state: &RefCell<Self>) {
        let waker = state.borrow_mut().waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A browser `EventSource` and its listeners, which it removes before it
/// drops their closures, so that the browser never calls a dropped one
/// (D6, D9).
struct Events {
    source: web_sys::EventSource,
    listeners: Vec<Listener>,
    state: Rc<RefCell<State>>,
}

impl Events {
    fn listen(source: web_sys::EventSource, state: Rc<RefCell<State>>) -> Self {
        let mut listening = Self {
            source,
            listeners: Vec::new(),
            state,
        };
        let on_open = {
            let state = Rc::clone(&listening.state);
            move |_: Event| {
                let mut opened = state.borrow_mut();
                if matches!(opened.opening, Opening::Pending) {
                    opened.opening = Opening::Opened;
                }
                drop(opened);
                State::wake(&state);
            }
        };
        let on_message = {
            let state = Rc::clone(&listening.state);
            let source = listening.source.clone();
            move |event: Event| {
                received(&state, &source, event.unchecked_ref::<MessageEvent>());
                State::wake(&state);
            }
        };
        let on_error = {
            let state = Rc::clone(&listening.state);
            let source = listening.source.clone();
            move |_: Event| {
                // At once, so the browser's own retry, with a token that may
                // have expired, never runs: the client reconnects by its
                // backoff (D9).
                source.close();
                failed(&state);
                State::wake(&state);
            }
        };
        listening.add("open", Box::new(on_open));
        listening.add("message", Box::new(on_message));
        listening.add("error", Box::new(on_error));
        listening
    }

    fn add(&mut self, name: &'static str, listener: Box<dyn FnMut(Event)>) {
        let closure = Closure::wrap(listener);
        let _ = self
            .source
            .add_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        self.listeners.push((name, closure));
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        let listeners = std::mem::take(&mut self.listeners);
        for (name, closure) in &listeners {
            let _ = self
                .source
                .remove_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        }
        drop(listeners);
        // A no-op on a source closed already.
        self.source.close();
    }
}

/// A `message` event: its data, one frame, into the inbox.
fn received(state: &RefCell<State>, source: &web_sys::EventSource, event: &MessageEvent) {
    let mut state = state.borrow_mut();
    if state.ended {
        return;
    }
    // The browser decodes the stream as UTF-8, so the data is text.
    let frame = event.data().as_string().unwrap_or_default().into_bytes();
    if frame.len() > state.max_message {
        state.inbox.end(Inbound::TooLarge);
        state.ended = true;
        source.close();
        return;
    }
    let bound = state.bound;
    if state.inbox.admit(frame, bound).is_err() {
        // What it held was not read: recovery brings it back (D7).
        state.inbox.clear();
        state.overflowed = true;
        state.ended = true;
        source.close();
    }
}

/// An `error` event, the source closed already: the opening fails, or the
/// stream ends after the frames received before it, without a close code
/// (D9).
fn failed(state: &RefCell<State>) {
    let mut state = state.borrow_mut();
    if matches!(state.opening, Opening::Pending) {
        // No status or headers to tell more, nor a captive portal (D9).
        state.opening = Opening::Failed("event stream failed before it opened".to_owned());
        return;
    }
    if state.ended {
        return;
    }
    state.ended = true;
    state.inbox.end(Inbound::Closed {
        code: None,
        reason: ENDED.to_owned(),
    });
}

/// The stream's URL: `url` with the connect command in `cf_connect`, as
/// natively (centrifuge-js `transport_sse.ts:41-50`).
fn stream_url(url: &str, initial_frame: &[u8]) -> Result<String, String> {
    let url = web_sys::Url::new(url).map_err(|error| js_message(&error))?;
    url.search_params()
        .append("cf_connect", &String::from_utf8_lossy(initial_frame));
    Ok(url.href())
}

/// An open sse connection.
///
/// Its messages wait in the inbox until the client reads them (D7);
/// commands go in emulation requests, written once answered (D8, D9).
pub(in crate::adapter) struct Sse {
    source: Events,
    emulation: Emulation,
    /// Frames whose request was answered and not yet reported by
    /// [`Sse::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a read comes first next.
    read_next: bool,
}

impl Sse {
    /// Opens the stream at `url` with the `EventSource` constructor
    /// `event_source`, the encoded connect command `initial_frame` in its
    /// URL; commands then go to `emulation_endpoint`, by default
    /// `/emulation` on the stream's server, with `fetch`. A message over
    /// `max_message` bytes ends it, reported as [`Inbound::TooLarge`]; one
    /// that would take the inbox past `bound` ends it too, reported as
    /// [`Inbound::Overflow`]. The source and the requests include
    /// credentials when `include_credentials` (D13).
    #[allow(clippy::too_many_arguments)] // Each is the transport's own.
    pub(in crate::adapter) async fn open(
        event_source: &Function,
        fetch: Function,
        url: &str,
        initial_frame: &[u8],
        emulation_endpoint: Option<&str>,
        request_timeout: Duration,
        max_message: usize,
        bound: Bound,
        include_credentials: bool,
    ) -> Result<Self, String> {
        // Protobuf with sse is rejected at build (Block 8).
        let emulation = Emulation::new(
            fetch,
            url,
            emulation_endpoint,
            request_timeout,
            JSON,
            include_credentials,
        )?;
        let init = EventSourceInit::new();
        init.set_with_credentials(include_credentials);
        let arguments =
            js_sys::Array::of2(&JsValue::from_str(&stream_url(url, initial_frame)?), &init);
        // A constructor that throws is a failed attempt, as for websocket
        // (D6); a shim is called as one too, its object used through
        // web-sys's bindings, which call it by name (D12).
        let source: web_sys::EventSource = Reflect::construct(event_source, &arguments)
            .map_err(|error| js_message(&error))?
            .unchecked_into();
        let state = Rc::new(RefCell::new(State {
            opening: Opening::Pending,
            inbox: Inbox::default(),
            bound,
            max_message,
            ended: false,
            overflowed: false,
            waker: None,
        }));
        // Dropped with the attempt, it closes the source.
        let source = Events::listen(source, Rc::clone(&state));
        std::future::poll_fn(|cx| {
            let mut state = state.borrow_mut();
            match std::mem::replace(&mut state.opening, Opening::Opened) {
                Opening::Opened => Poll::Ready(Ok(())),
                Opening::Failed(message) => Poll::Ready(Err(message)),
                Opening::Pending => {
                    state.opening = Opening::Pending;
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await?;
        Ok(Self {
            source,
            emulation,
            written: VecDeque::new(),
            read_next: false,
        })
    }

    /// Sends frame `id` in an emulation request, which [`Sse::recv`]
    /// reports written once answered.
    pub(in crate::adapter) fn send(&mut self, id: FrameId, frame: Vec<u8>) {
        self.emulation.send(id, frame);
    }

    /// The next frame received, the end of the connection, or a frame
    /// written. Without `reading`, only frames written, a failed emulation
    /// request and an overflow are reported: received ones stay in the
    /// inbox. Cancel-safe.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        let Self {
            source,
            emulation,
            written,
            read_next,
        } = self;
        std::future::poll_fn(|cx| {
            let mut state = source.state.borrow_mut();
            if state.overflowed {
                return Poll::Ready(Inbound::Overflow);
            }
            let frame_first = reading && !state.inbox.is_empty() && *read_next;
            if !frame_first && let Some(id) = written.pop_front() {
                *read_next = true;
                return Poll::Ready(Inbound::Written(id));
            }
            if reading && let Some(item) = state.inbox.pop() {
                *read_next = false;
                return Poll::Ready(item);
            }
            state.waker = Some(cx.waker().clone());
            drop(state);
            if let Poll::Ready((id, answered)) = emulation.poll_answered(cx) {
                *read_next = true;
                return Poll::Ready(report(id, answered, written));
            }
            Poll::Pending
        })
        .await
    }

    /// Closes the source, its listeners removed first, and aborts every
    /// emulation request in flight (D8, D9). `EventSource` closes at once:
    /// there is no close event to await.
    pub(in crate::adapter) fn close(self) {
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::future::{Either, select};
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{ENDED, Sse};
    use crate::adapter::browser::websocket::{Bound, Inbound};
    use crate::adapter::platform;
    use crate::machine::FrameId;

    const URL: &str = "http://127.0.0.1:8000/connection/sse";
    const LIMIT: usize = 100;
    const BOUND: Bound = Bound {
        messages: 2,
        bytes: 1000,
    };
    const TIMEOUT: Duration = Duration::from_secs(5);

    /// An `EventSource` standing in for the page's, the last one made in
    /// `state.source`, with what it was given (`url`, `init`), its
    /// `readyState`, its listeners by event name, and the `readyState` it
    /// had when its `error` listeners had run (`readyStateAfterError`).
    /// `state.emit(name, data)` dispatches an event on it.
    fn fake_event_source() -> (Function, Object) {
        let state = Object::new();
        let make = Function::new_with_args(
            "state",
            "class FakeEventSource extends EventTarget {
               constructor(url, init) {
                 super();
                 this.url = url;
                 this.init = init;
                 this.readyState = 0;
                 this.listening = {};
                 state.source = this;
               }
               close() { this.readyState = 2; }
               addEventListener(name, listener) {
                 this.listening[name] = (this.listening[name] || 0) + 1;
                 super.addEventListener(name, listener);
               }
               removeEventListener(name, listener) {
                 this.listening[name] -= 1;
                 super.removeEventListener(name, listener);
               }
             }
             state.emit = (name, data) => {
               const event = new Event(name);
               if (data !== undefined) event.data = data;
               if (name === 'open') state.source.readyState = 1;
               state.source.dispatchEvent(event);
               if (name === 'error') state.readyStateAfterError = state.source.readyState;
             };
             return FakeEventSource;",
        );
        let constructor = make
            .call1(&JsValue::NULL, &state)
            .expect("a fake EventSource")
            .unchecked_into();
        (constructor, state)
    }

    /// A `fetch` whose emulation responses have status `status`.
    fn fake_fetch(status: u16) -> Function {
        Function::new_with_args(
            "status",
            "return () => Promise.resolve({ type: 'basic', status, headers: new Headers() });",
        )
        .call1(&JsValue::NULL, &JsValue::from(status))
        .expect("a fake fetch")
        .unchecked_into()
    }

    fn get(object: &JsValue, path: &[&str]) -> JsValue {
        path.iter().fold(object.clone(), |value, name| {
            Reflect::get(&value, &JsValue::from_str(name)).expect("a property")
        })
    }

    fn emit(state: &Object, name: &str, data: Option<&str>) {
        let emit: Function = get(state, &["emit"]).unchecked_into();
        let data = data.map_or(JsValue::UNDEFINED, JsValue::from_str);
        emit.call2(&JsValue::NULL, &JsValue::from_str(name), &data)
            .expect("dispatched");
    }

    fn ready_state(state: &Object) -> f64 {
        get(state, &["source", "readyState"])
            .as_f64()
            .expect("a state")
    }

    /// An open transport over a fake `EventSource` and `fetch`.
    async fn open(status: u16) -> (Sse, Object) {
        let (constructor, state) = fake_event_source();
        let opening = Sse::open(
            &constructor,
            fake_fetch(status),
            URL,
            br#"{"id":1,"connect":{}}"#,
            None,
            TIMEOUT,
            LIMIT,
            BOUND,
            false,
        );
        let opened = {
            let state = state.clone();
            async move {
                platform::sleep(Duration::ZERO).await;
                emit(&state, "open", None);
            }
        };
        let (sse, ()) = futures_util::join!(opening, opened);
        (
            sse.unwrap_or_else(|reason| panic!("not opened: {reason}")),
            state,
        )
    }

    async fn recv_within(sse: &mut Sse, reading: bool, millis: u64) -> Option<Inbound> {
        let timer = platform::sleep(Duration::from_millis(millis));
        match select(std::pin::pin!(sse.recv(reading)), std::pin::pin!(timer)).await {
            Either::Left((inbound, _)) => Some(inbound),
            Either::Right(_) => None,
        }
    }

    // Block 44, D9: the connect command in `cf_connect`, no credentials.
    #[wasm_bindgen_test]
    async fn the_stream_url_carries_the_connect_command_without_credentials() {
        let (_sse, state) = open(200).await;

        let url = web_sys::Url::new(&get(&state, &["source", "url"]).as_string().unwrap()).unwrap();
        assert_eq!(url.pathname(), "/connection/sse");
        assert_eq!(
            url.search_params().get("cf_connect").as_deref(),
            Some(r#"{"id":1,"connect":{}}"#)
        );
        assert_eq!(
            get(&state, &["source", "init", "withCredentials"]),
            JsValue::FALSE
        );
    }

    // D13: credentials included, the source is opened with them.
    #[wasm_bindgen_test]
    async fn included_credentials_open_the_source_with_credentials() {
        let (constructor, state) = fake_event_source();
        let opening = Sse::open(
            &constructor,
            fake_fetch(200),
            URL,
            b"{}",
            None,
            TIMEOUT,
            LIMIT,
            BOUND,
            true,
        );
        let opened = {
            let state = state.clone();
            async move {
                platform::sleep(Duration::ZERO).await;
                emit(&state, "open", None);
            }
        };

        let (sse, ()) = futures_util::join!(opening, opened);

        assert!(sse.is_ok());
        assert_eq!(
            get(&state, &["source", "init", "withCredentials"]),
            JsValue::TRUE
        );
    }

    // D9: an `error` closes the source before the core hears of it.
    #[wasm_bindgen_test]
    async fn an_error_closes_the_source_at_once_and_ends_the_transport() {
        let (mut sse, state) = open(200).await;
        emit(&state, "message", Some("{}"));

        emit(&state, "error", None);

        assert_eq!(get(&state, &["readyStateAfterError"]), 2);
        assert_eq!(
            recv_within(&mut sse, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
        );
        assert_eq!(
            recv_within(&mut sse, true, 1000).await,
            Some(Inbound::Closed {
                code: None,
                reason: ENDED.to_owned(),
            })
        );
    }

    #[wasm_bindgen_test]
    async fn an_error_before_open_fails_the_attempt() {
        let (constructor, state) = fake_event_source();
        let opening = Sse::open(
            &constructor,
            fake_fetch(200),
            URL,
            b"{}",
            None,
            TIMEOUT,
            LIMIT,
            BOUND,
            false,
        );
        let failing = {
            let state = state.clone();
            async move {
                platform::sleep(Duration::ZERO).await;
                emit(&state, "error", None);
            }
        };

        let (opened, ()) = futures_util::join!(opening, failing);

        assert!(opened.is_err());
        assert_eq!(get(&state, &["readyStateAfterError"]), 2);
    }

    // D6, D9: no listener left to call a dropped closure.
    #[wasm_bindgen_test]
    async fn closing_removes_the_listeners_and_closes_the_source() {
        let (sse, state) = open(200).await;

        sse.close();

        for name in ["open", "message", "error"] {
            assert_eq!(get(&state, &["source", "listening", name]), 0, "{name}");
        }
        assert_eq!(ready_state(&state), 2.0);
        // Nothing listens: dispatching calls no dropped closure.
        emit(&state, "message", Some("{}"));
    }

    // D7: messages wait in the inbox while the client does not read.
    #[wasm_bindgen_test]
    async fn messages_wait_in_the_inbox_until_read() {
        let (mut sse, state) = open(200).await;
        emit(&state, "message", Some("{}"));

        assert_eq!(recv_within(&mut sse, false, 200).await, None);
        assert_eq!(
            recv_within(&mut sse, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
        );
    }

    // D7: a message past the inbox's bound closes the source.
    #[wasm_bindgen_test]
    async fn a_message_past_the_inbox_bound_overflows() {
        let (mut sse, state) = open(200).await;
        for _ in 0..3 {
            emit(&state, "message", Some("{}"));
        }

        assert_eq!(ready_state(&state), 2.0);
        assert_eq!(
            recv_within(&mut sse, false, 1000).await,
            Some(Inbound::Overflow)
        );
    }

    // Block 29, D3: a message over the size limit ends the transport.
    #[wasm_bindgen_test]
    async fn a_message_over_the_size_limit_ends_the_transport() {
        let (mut sse, state) = open(200).await;

        emit(&state, "message", Some(&"x".repeat(LIMIT + 1)));

        assert_eq!(ready_state(&state), 2.0);
        assert_eq!(
            recv_within(&mut sse, true, 1000).await,
            Some(Inbound::TooLarge)
        );
    }

    // D9: commands go through D8's emulation requests.
    #[wasm_bindgen_test]
    async fn commands_go_in_emulation_requests() {
        for (status, expected) in [
            (200, Inbound::Written(FrameId::for_tests(1))),
            (
                503,
                Inbound::Closed {
                    code: None,
                    reason: "emulation request failed: HTTP status 503".to_owned(),
                },
            ),
        ] {
            let (mut sse, _state) = open(status).await;
            sse.send(FrameId::for_tests(1), b"{}".to_vec());

            assert_eq!(recv_within(&mut sse, false, 1000).await, Some(expected));
        }
    }
}
