//! `http_stream` transport of the browser adapter, over the browser's
//! `fetch` or the application's shim of it (Block 44, D8, D12), with
//! bidirectional emulation as natively (Block 8): the response to the
//! request opening the stream carries the replies; commands go in requests
//! to the emulation endpoint of the same server, as they do over sse (D9).
//! A task of the transport's own reads the body with wasm-streams,
//! continuously, into the bounded inbox (D7, D8): Safari stops delivering a
//! body left unread, so this transport never stops reading, also through a
//! stream built with the application's `ReadableStream` (D12).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::future::{AbortHandle, Either, abortable, select};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt};
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, future_to_promise, spawn_local};
use wasm_streams::readable::IntoStream;
use web_sys::{AbortController, ReadableStreamDefaultReader, Response, ResponseType};

use super::websocket::{Bound, Inbound, Inbox, js_message};
use crate::adapter::body::{CAPTIVE_PORTAL, Decoder, TOO_LARGE, captive_portal, with_portal};
use crate::adapter::platform;
use crate::machine::FrameId;

/// Path of the emulation endpoint (centrifuge-js `emulationEndpoint`).
const EMULATION_PATH: &str = "/emulation";
pub(super) const JSON: &str = "application/json";
/// The media type of Protobuf requests and streams, exactly (Block 27, D3).
const PROTOBUF: &str = "application/octet-stream";

/// Aborts its request when dropped, as reqwest's `AbortGuard`: a request
/// ends with the transport that made it (D8).
struct Abort(AbortController);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The `credentials` of a request (D13): `include` sends the cookies of
/// another origin too, and stores its `Set-Cookie`; `same-origin`, as
/// centrifuge-js 63ed73e always does, only those of the page's own.
fn credentials(include: bool) -> &'static str {
    if include { "include" } else { "same-origin" }
}

/// Starts `fetch(url, init)` as centrifuge-js calls it, `init` a plain
/// object, so that no global `Request` or `Headers` is needed (D8): a POST
/// of `body` with `headers`, the credentials of the origin only unless
/// `include_credentials` (D13), and redirects not followed. The guard
/// aborting it, and its response.
fn start(
    fetch: &Function,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    include_credentials: bool,
) -> (Option<Abort>, Result<JsFuture, String>) {
    let controller = match AbortController::new() {
        Ok(controller) => controller,
        Err(error) => return (None, Err(js_message(&error))),
    };
    let signal = controller.signal();
    let abort = Some(Abort(controller));
    let header_object = Object::new();
    let init = Object::new();
    let set = |object: &Object, name: &str, value: &JsValue| {
        Reflect::set(object, &JsValue::from_str(name), value).map(|_| ())
    };
    let filled = headers
        .iter()
        .try_for_each(|(name, value)| set(&header_object, name, &JsValue::from_str(value)))
        .and_then(|()| set(&init, "method", &JsValue::from_str("POST")))
        .and_then(|()| set(&init, "headers", &header_object))
        .and_then(|()| set(&init, "body", &Uint8Array::from(body)))
        .and_then(|()| set(&init, "mode", &JsValue::from_str("cors")))
        .and_then(|()| {
            set(
                &init,
                "credentials",
                &JsValue::from_str(credentials(include_credentials)),
            )
        })
        .and_then(|()| set(&init, "redirect", &JsValue::from_str("manual")))
        .and_then(|()| set(&init, "signal", &signal));
    if let Err(error) = filled {
        return (abort, Err(js_message(&error)));
    }
    // A fetch that throws, e.g. on a malformed URL, fails as one rejected.
    let response = fetch
        .call2(&js_sys::global(), &JsValue::from_str(url), &init)
        .map(|promise| JsFuture::from(Promise::resolve(&promise)))
        .map_err(|error| js_message(&error));
    (abort, response)
}

/// Where emulation requests go: `configured`, absolute, or `/emulation` on
/// the server of `stream` (centrifuge-js `emulationEndpoint`).
fn emulation_url(stream: &str, configured: Option<&str>) -> Result<String, String> {
    if let Some(endpoint) = configured {
        return web_sys::Url::new(endpoint)
            .map(|url| url.href())
            .map_err(|error| js_message(&error));
    }
    let url = web_sys::Url::new(stream).map_err(|error| js_message(&error))?;
    url.set_pathname(EMULATION_PATH);
    url.set_search("");
    url.set_hash("");
    Ok(url.href())
}

/// An emulation request in flight; its failure ends the transport.
type Request = Pin<Box<dyn Future<Output = (FrameId, Result<Sent, String>)>>>;

/// How the server took an emulation request it answered.
pub(super) enum Sent {
    Accepted,
    /// Rejected with HTTP 413, with the frame it carried.
    Rejected(Vec<u8>),
}

/// The emulation requests of an emulated transport (Block 8), each a
/// `fetch` of its own (D8, D9).
pub(super) struct Emulation {
    fetch: Function,
    url: String,
    /// Time allowed for a request, as for a command's reply.
    timeout: Duration,
    /// The media type of the requests: JSON's or Protobuf's.
    content_type: &'static str,
    /// Whether requests carry the cookies of another origin (D13).
    include_credentials: bool,
    /// Each holds its request's guard: dropped with the transport, it
    /// aborts the request (D8).
    requests: FuturesUnordered<Request>,
}

impl Emulation {
    /// Requests to `configured`, or to `/emulation` on the server of
    /// `stream`, with `fetch`, including credentials when
    /// `include_credentials` (D13).
    pub(super) fn new(
        fetch: Function,
        stream: &str,
        configured: Option<&str>,
        timeout: Duration,
        content_type: &'static str,
        include_credentials: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            fetch,
            url: emulation_url(stream, configured)?,
            timeout,
            content_type,
            include_credentials,
            requests: FuturesUnordered::new(),
        })
    }

    /// Sends frame `id`, the body of a request started now, which
    /// [`Emulation::poll_answered`] gives once answered or failed.
    pub(super) fn send(&mut self, id: FrameId, frame: Vec<u8>) {
        let (abort, response) = start(
            &self.fetch,
            &self.url,
            &[("Content-Type", self.content_type)],
            &frame,
            self.include_credentials,
        );
        let timeout = self.timeout;
        self.requests.push(Box::pin(async move {
            // Also once answered: its body, unread, is let go.
            let _abort = abort;
            let answered = async {
                let response =
                    response.map_err(|reason| format!("emulation request failed: {reason}"))?;
                let timer = platform::sleep(timeout);
                let response: Response = match select(response, std::pin::pin!(timer)).await {
                    Either::Left((response, _)) => response
                        .map_err(|error| {
                            format!("emulation request failed: {}", js_message(&error))
                        })?
                        .unchecked_into(),
                    Either::Right(_) => {
                        return Err(format!("emulation request timed out after {timeout:?}"));
                    }
                };
                let status = response.status();
                // A server without the session's node (404) or failing
                // (5xx) cannot answer the command; other statuses reject
                // only it (centrifuge-js `send`). A redirect is not
                // followed, so its command never arrived either.
                if response.type_() == ResponseType::Opaqueredirect {
                    Err("emulation request failed: HTTP redirect".to_owned())
                } else if status == 404 || status >= 500 || (300..400).contains(&status) {
                    Err(format!("emulation request failed: HTTP status {status}"))
                } else if status == 413 {
                    // Its commands fail at once (owner decision, 2026-11-04).
                    Ok(Sent::Rejected(frame))
                } else {
                    Ok(Sent::Accepted)
                }
            };
            (id, answered.await)
        }));
    }

    /// The next request answered or failed; pending while none is in
    /// flight. The transport sends only between its reads, so a request
    /// sent later is polled by the next read.
    pub(super) fn poll_answered(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<(FrameId, Result<Sent, String>)> {
        match Pin::new(&mut self.requests).poll_next(cx) {
            Poll::Ready(Some(answered)) => Poll::Ready(answered),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

/// What the transport reports of request `id`: a failure ends it (Block
/// 8); a 413 fails its commands, then counts as written, in `written`.
pub(super) fn report(
    id: FrameId,
    answered: Result<Sent, String>,
    written: &mut VecDeque<FrameId>,
) -> Inbound {
    match answered {
        Err(reason) => Inbound::Closed { code: None, reason },
        Ok(Sent::Rejected(frame)) => {
            written.push_back(id);
            Inbound::Rejected(frame)
        }
        Ok(Sent::Accepted) => Inbound::Written(id),
    }
}

/// The listeners of a stream built with the application's `ReadableStream`
/// (D12), alive while it is read.
struct Through {
    _pull: Closure<dyn FnMut(JsValue) -> Promise>,
    _cancel: Closure<dyn FnMut(JsValue) -> Promise>,
}

/// `body` read through a stream built with `shim`, the application's
/// `ReadableStream` (D12), as centrifuge-js reads it with the constructor it
/// resolved (`transport_http_stream.ts:34-91`): an underlying source whose
/// `pull` reads one chunk from the body's reader and enqueues that chunk
/// unchanged, with `highWaterMark` 0, so the shim's stream reads one chunk
/// for each read of the transport's task (D8). Its `cancel` cancels the
/// body.
fn through(
    shim: &Function,
    body: &web_sys::ReadableStream,
) -> Result<(web_sys::ReadableStream, Through), String> {
    let reader: ReadableStreamDefaultReader = body.get_reader().unchecked_into();
    let pulling = reader.clone();
    let pull = Closure::<dyn FnMut(JsValue) -> Promise>::new(move |controller: JsValue| {
        let read = pulling.read();
        future_to_promise(async move {
            let result = JsFuture::from(read).await?;
            let done = Reflect::get(&result, &JsValue::from_str("done"))?.is_truthy();
            let method = if done { "close" } else { "enqueue" };
            let function: Function =
                Reflect::get(&controller, &JsValue::from_str(method))?.dyn_into()?;
            if done {
                function.call0(&controller)?;
            } else {
                let chunk = Reflect::get(&result, &JsValue::from_str("value"))?;
                function.call1(&controller, &chunk)?;
            }
            Ok(JsValue::UNDEFINED)
        })
    });
    let cancel = Closure::<dyn FnMut(JsValue) -> Promise>::new(move |reason: JsValue| {
        reader.cancel_with_reason(&reason)
    });
    let source = Object::new();
    let strategy = Object::new();
    let set = |object: &Object, name: &str, value: &JsValue| {
        Reflect::set(object, &JsValue::from_str(name), value).map(|_| ())
    };
    set(&source, "pull", pull.as_ref())
        .and_then(|()| set(&source, "cancel", cancel.as_ref()))
        .and_then(|()| set(&strategy, "highWaterMark", &JsValue::from(0)))
        .map_err(|error| js_message(&error))?;
    // A constructor that throws fails the attempt, as a fetch that does.
    let stream = Reflect::construct(shim, &Array::of2(&source, &strategy))
        .map_err(|error| js_message(&error))?
        .unchecked_into();
    Ok((
        stream,
        Through {
            _pull: pull,
            _cancel: cancel,
        },
    ))
}

/// What the task reading the body and the transport share (D7, D8).
#[derive(Default)]
struct State {
    inbox: Inbox,
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

/// Stops the task reading the body when dropped, also when a fetch shim's
/// body ignores the abort (D8).
struct Reading(AbortHandle);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The end of the stream `failure` says: a message over the size limit, or
/// any other failure of the decoder.
fn ended_by(failure: String) -> Inbound {
    // Over `Config::max_message_bytes` (Block 29, D3).
    if failure == TOO_LARGE {
        return Inbound::TooLarge;
    }
    Inbound::Closed {
        code: None,
        reason: failure,
    }
}

/// Admits `message` to the inbox; false when it overflows. One chunk can
/// hold more messages than the inbox, which is no sign of a slow reader:
/// while the driver takes messages between turns of the event loop, this
/// waits for it, and only a turn in which it took none is an overflow.
async fn admit(state: &Rc<RefCell<State>>, mut message: Vec<u8>, bound: Bound) -> bool {
    loop {
        message = match state.borrow_mut().inbox.admit(message, bound) {
            Ok(()) => return true,
            Err(message) => message,
        };
        let waiting = state.borrow().inbox.frames();
        State::wake(state);
        gloo_timers::future::TimeoutFuture::new(0).await;
        if state.borrow().inbox.frames() >= waiting {
            return false;
        }
    }
}

/// Reads `body` until it ends, one chunk after another whether or not the
/// driver polls the transport (D8), and admits each message to the inbox.
/// The stream's end, a failure or a message over the size limit is queued
/// behind the messages before it; an overflow ends the reading, holding
/// nothing, since recovery brings those messages back (D7).
///
/// `through` comes before `body`, so that `body`, which cancels it when
/// dropped, is dropped first.
async fn read_body(
    _through: Option<Through>,
    mut body: IntoStream<'static>,
    mut decoder: Decoder,
    bound: Bound,
    state: Rc<RefCell<State>>,
) {
    let mut messages = VecDeque::new();
    loop {
        let end = match body.next().await {
            Some(Ok(chunk)) => match chunk.dyn_ref::<Uint8Array>() {
                Some(chunk) => decoder
                    .push(&chunk.to_vec(), &mut messages)
                    .err()
                    .map(ended_by),
                None => Some(Inbound::Closed {
                    code: None,
                    reason: "a response body chunk that is not bytes".to_owned(),
                }),
            },
            Some(Err(error)) => Some(Inbound::Closed {
                code: None,
                reason: js_message(&error),
            }),
            None => Some(Inbound::Closed {
                code: None,
                reason: "stream ended".to_owned(),
            }),
        };
        while let Some(message) = messages.pop_front() {
            if !admit(&state, message, bound).await {
                let mut shared = state.borrow_mut();
                shared.inbox.clear();
                shared.overflowed = true;
                drop(shared);
                State::wake(&state);
                return;
            }
        }
        let ended = end.is_some();
        if let Some(end) = end {
            state.borrow_mut().inbox.end(end);
        }
        State::wake(&state);
        if ended {
            return;
        }
    }
}

/// An open `http_stream` connection.
///
/// Its messages wait in the inbox until the client reads them (D7);
/// commands go in emulation requests, written once answered (D8).
pub(in crate::adapter) struct HttpStream {
    state: Rc<RefCell<State>>,
    emulation: Emulation,
    /// Stops the task reading the body when the transport closes or is
    /// dropped.
    _reading: Reading,
    /// Aborts the stream request when the transport closes or is dropped.
    _abort: Option<Abort>,
    /// Frames whose request was answered and not yet reported by
    /// [`HttpStream::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a message comes first next.
    read_next: bool,
}

impl HttpStream {
    /// Opens the stream at `url` with `fetch`, sending `initial_frame`, the
    /// encoded connect command, with the request, its body read through a
    /// stream built with `readable_stream` when given (D12). Commands then
    /// go to `emulation_endpoint`, by default `/emulation` on the stream's
    /// server. A message over `max_message` bytes ends it, reported as
    /// [`Inbound::TooLarge`]; one that would take the inbox past `bound`
    /// ends it too, reported as [`Inbound::Overflow`]. Requests include
    /// credentials when `include_credentials` (D13).
    #[allow(clippy::too_many_arguments)] // Each is the transport's own.
    pub(in crate::adapter) async fn open(
        fetch: Function,
        readable_stream: Option<&Function>,
        url: &str,
        initial_frame: Vec<u8>,
        emulation_endpoint: Option<&str>,
        request_timeout: Duration,
        protobuf: bool,
        max_message: usize,
        bound: Bound,
        include_credentials: bool,
    ) -> Result<Self, String> {
        let content_type = if protobuf { PROTOBUF } else { JSON };
        let emulation = Emulation::new(
            fetch.clone(),
            url,
            emulation_endpoint,
            request_timeout,
            content_type,
            include_credentials,
        )?;
        // Dropped with the attempt, the guard aborts the request.
        let (abort, response) = start(
            &fetch,
            url,
            &[("Accept", content_type), ("Content-Type", content_type)],
            &initial_frame,
            include_credentials,
        );
        let response: Response = response?
            .await
            .map_err(|error| js_message(&error))?
            .unchecked_into();
        // A redirect not followed hides its status (Fetch, "opaque-redirect
        // filtered response"): reported as natively a 3xx is (Block 25, D8).
        if response.type_() == ResponseType::Opaqueredirect {
            return Err(format!("HTTP redirect{CAPTIVE_PORTAL}"));
        }
        let status = response.status();
        // CORS-safelisted, so readable from another origin (D8).
        let content_type_header = response
            .headers()
            .get("content-type")
            .ok()
            .flatten()
            .unwrap_or_default();
        let portal = captive_portal(status, &content_type_header);
        if !(200..300).contains(&status) {
            return Err(with_portal(format!("HTTP status {status}"), portal));
        }
        // A web page instead of the stream (Block 25, D8).
        if portal {
            return Err(with_portal(
                format!("HTTP status {status}, content type {content_type_header:?}"),
                true,
            ));
        }
        let body = response
            .body()
            .ok_or_else(|| format!("HTTP status {status} without a body"))?;
        let (body, through) = match readable_stream {
            Some(shim) => {
                let (stream, through) = self::through(shim, &body)?;
                (stream, Some(through))
            }
            None => (body, None),
        };
        let decoder = if protobuf {
            Decoder::protobuf(max_message)
        } else {
            Decoder::lines(max_message)
        };
        let state = Rc::new(RefCell::new(State::default()));
        let body = wasm_streams::ReadableStream::from_raw(body).into_stream();
        let (reading, handle) =
            abortable(read_body(through, body, decoder, bound, Rc::clone(&state)));
        spawn_local(async move {
            let _ = reading.await;
        });
        Ok(Self {
            state,
            emulation,
            _reading: Reading(handle),
            _abort: abort,
            written: VecDeque::new(),
            read_next: false,
        })
    }

    /// Sends frame `id`, the body of an emulation request started now,
    /// which [`HttpStream::recv`] reports written once answered. A request
    /// that fails ends the transport when [`HttpStream::recv`] reports it.
    pub(in crate::adapter) fn send(&mut self, id: FrameId, frame: Vec<u8>) {
        self.emulation.send(id, frame);
    }

    /// Closes the transport: its stream and every emulation request in
    /// flight are aborted (D8).
    pub(in crate::adapter) fn close(self) {
        drop(self);
    }

    /// The next message from the server, the end of the transport, or a
    /// frame written. Without `reading`, only frames written, a failed
    /// emulation request and an overflow are reported: messages stay in the
    /// inbox. Cancel-safe.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        let Self {
            state,
            emulation,
            written,
            read_next,
            ..
        } = self;
        std::future::poll_fn(|cx| {
            let mut state = state.borrow_mut();
            if state.overflowed {
                return Poll::Ready(Inbound::Overflow);
            }
            // Written reports and messages take turns, as on a websocket.
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::future::{Either, select};
    use futures_util::{Stream, StreamExt};
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{HttpStream, emulation_url};
    use crate::adapter::body::CAPTIVE_PORTAL;
    use crate::adapter::browser::websocket::{Bound, Inbound};
    use crate::adapter::platform;
    use crate::machine::{FrameId, TransportKind};
    use crate::{Client, ClientEvent, Config, SubscriptionEvent};

    const URL: &str = "http://127.0.0.1:8000/connection/http_stream";
    const LIMIT: usize = 1 << 20;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const WAIT: Duration = Duration::from_secs(5);
    const BOUND: Bound = Bound {
        messages: 1 << 10,
        bytes: 1 << 20,
    };
    /// The JSON lines the body of [`fake_fetch`] gives, after which a read
    /// waits for good.
    const LINES: f64 = 4.0;

    /// A `fetch` standing in for the page's, with what it saw in `state`:
    /// `calls`, each `{ url, init }`, and `pulls`, the reads of the stream
    /// body, each of which gives one JSON line until [`LINES`] were given,
    /// and then never answers. The stream response is of
    /// type `stream_type`; emulation responses have status `emulation`, or
    /// never come when it is 0.
    fn fake_fetch(stream_type: &str, emulation: u16) -> (Function, Object) {
        let state = Object::new();
        let make = Function::new_with_args(
            "state, streamType, emulation",
            "state.calls = [];
             state.pulls = 0;
             const lines = 4;
             return (url, init) => {
               state.calls.push({ url, init });
               if (url.endsWith('/emulation')) {
                 if (emulation === 0) return new Promise(() => {});
                 return Promise.resolve({ type: 'basic', status: emulation, headers: new Headers() });
               }
               const body = new ReadableStream({
                 pull(controller) {
                   if (state.pulls >= lines) return new Promise(() => {});
                   state.pulls += 1;
                   controller.enqueue(new TextEncoder().encode('{}\\n'));
                 },
               }, { highWaterMark: 0 });
               const headers = new Headers({ 'content-type': 'application/json' });
               return Promise.resolve({ type: streamType, status: streamType === 'basic' ? 200 : 0, headers, body });
             };",
        );
        let fetch = make
            .call3(
                &JsValue::NULL,
                &state,
                &JsValue::from_str(stream_type),
                &JsValue::from(emulation),
            )
            .expect("a fake fetch")
            .unchecked_into();
        (fetch, state)
    }

    fn get(object: &JsValue, path: &[&str]) -> JsValue {
        path.iter().fold(object.clone(), |value, name| {
            Reflect::get(&value, &JsValue::from_str(name)).expect("a property")
        })
    }

    /// The `init` of call `n` of the fake fetch.
    fn init(state: &Object, n: u32) -> JsValue {
        let calls: js_sys::Array = get(state, &["calls"]).unchecked_into();
        get(&calls.get(n), &["init"])
    }

    fn aborted(state: &Object, n: u32) -> bool {
        get(&init(state, n), &["signal", "aborted"]).is_truthy()
    }

    fn pulls(state: &Object) -> f64 {
        get(state, &["pulls"]).as_f64().expect("a count")
    }

    async fn open(fetch: Function) -> HttpStream {
        HttpStream::open(
            fetch,
            None,
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            false,
        )
        .await
        .unwrap_or_else(|reason| panic!("not opened: {reason}"))
    }

    /// What `recv` gives within `millis`, if anything.
    async fn recv_within(http: &mut HttpStream, reading: bool, millis: u64) -> Option<Inbound> {
        let timer = platform::sleep(Duration::from_millis(millis));
        match select(std::pin::pin!(http.recv(reading)), std::pin::pin!(timer)).await {
            Either::Left((inbound, _)) => Some(inbound),
            Either::Right(_) => None,
        }
    }

    // Block 44, D8: as centrifuge-js calls it, with a plain `init`.
    #[wasm_bindgen_test]
    async fn requests_are_posts_of_their_frame_naming_their_encoding() {
        let (fetch, state) = fake_fetch("basic", 200);
        let mut http = HttpStream::open(
            fetch,
            None,
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            true,
            LIMIT,
            BOUND,
            false,
        )
        .await
        .unwrap();
        http.send(FrameId::for_tests(1), vec![1, 2]);

        let stream = init(&state, 0);
        let emulation = init(&state, 1);
        for (init, accept) in [
            (&stream, Some("application/octet-stream")),
            (&emulation, None),
        ] {
            assert_eq!(get(init, &["method"]), "POST");
            assert_eq!(get(init, &["mode"]), "cors");
            assert_eq!(get(init, &["credentials"]), "same-origin");
            assert_eq!(get(init, &["redirect"]), "manual");
            assert_eq!(
                get(init, &["headers", "Content-Type"]),
                "application/octet-stream"
            );
            assert_eq!(
                get(init, &["headers", "Accept"]).as_string().as_deref(),
                accept
            );
        }
        let body: js_sys::Uint8Array = get(&emulation, &["body"]).unchecked_into();
        assert_eq!(body.to_vec(), [1, 2]);
        let calls: js_sys::Array = get(&state, &["calls"]).unchecked_into();
        assert_eq!(
            get(&calls.get(1), &["url"]),
            "http://127.0.0.1:8000/emulation"
        );
    }

    #[wasm_bindgen_test]
    async fn closing_aborts_the_stream_and_a_pending_emulation_request() {
        let (fetch, state) = fake_fetch("basic", 0);
        let mut http = open(fetch).await;
        http.send(FrameId::for_tests(1), b"{}".to_vec());
        assert!(!aborted(&state, 0) && !aborted(&state, 1));

        http.close();

        assert!(aborted(&state, 0), "the stream request");
        assert!(aborted(&state, 1), "the emulation request");
    }

    #[wasm_bindgen_test]
    async fn dropping_aborts_the_stream_and_a_pending_emulation_request() {
        let (fetch, state) = fake_fetch("basic", 0);
        let mut http = open(fetch).await;
        http.send(FrameId::for_tests(1), b"{}".to_vec());

        drop(http);

        assert!(aborted(&state, 0) && aborted(&state, 1));
    }

    /// A `fetch` whose stream body runs `start(controller)` over `encode`.
    fn fetch_of(start: &str) -> Function {
        Function::new_no_args(&format!(
            "return (url, init) => {{
               const encode = (text) => new TextEncoder().encode(text);
               const body = new ReadableStream({{ start(controller) {{ {start} }} }});
               const headers = new Headers({{ 'content-type': 'application/json' }});
               return Promise.resolve({{ type: 'basic', status: 200, headers, body }});
             }};"
        ))
        .call0(&JsValue::NULL)
        .expect("a fetch")
        .unchecked_into()
    }

    async fn open_bounded(fetch: Function, limit: usize, bound: Bound) -> HttpStream {
        HttpStream::open(
            fetch,
            None,
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            limit,
            bound,
            false,
        )
        .await
        .unwrap_or_else(|reason| panic!("not opened: {reason}"))
    }

    fn empty_frame() -> Inbound {
        Inbound::Frame(b"{}".to_vec())
    }

    // Block 44, D7, D8: the body is read whether or not the core reads, and
    // without `recv` being polled at all, as while an `Inline` handler runs.
    #[wasm_bindgen_test]
    async fn the_body_is_read_while_the_core_is_not_reading() {
        let (fetch, state) = fake_fetch("basic", 0);
        let mut http = open(fetch).await;

        platform::sleep(Duration::from_millis(100)).await;

        assert_eq!(pulls(&state), LINES);
        assert_eq!(recv_within(&mut http, false, 50).await, None);
        for _ in 0..4 {
            assert_eq!(
                recv_within(&mut http, true, 1000).await,
                Some(empty_frame())
            );
        }
    }

    // Block 44, D8: the stream's end waits behind the messages before it.
    #[wasm_bindgen_test]
    async fn the_end_of_the_stream_is_read_after_the_messages_before_it() {
        let fetch = fetch_of("controller.enqueue(encode('{}\\n{}\\n')); controller.close();");
        let mut http = open_bounded(fetch, LIMIT, BOUND).await;

        assert_eq!(recv_within(&mut http, false, 100).await, None);

        for _ in 0..2 {
            assert_eq!(
                recv_within(&mut http, true, 1000).await,
                Some(empty_frame())
            );
        }
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Closed {
                code: None,
                reason: "stream ended".to_owned(),
            })
        );
    }

    // Block 44, D8: a failure waits behind the messages before it too.
    #[wasm_bindgen_test]
    async fn a_failed_stream_is_read_after_the_messages_before_it() {
        let fetch = fetch_of(
            "controller.enqueue(encode('{}\\n')); \
             setTimeout(() => controller.error(new Error('body failed')), 10);",
        );
        let mut http = open_bounded(fetch, LIMIT, BOUND).await;

        assert_eq!(recv_within(&mut http, false, 100).await, None);

        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(empty_frame())
        );
        let Some(Inbound::Closed { code: None, reason }) = recv_within(&mut http, true, 1000).await
        else {
            panic!("not the end of the stream");
        };
        assert!(reason.contains("body failed"), "{reason}");
    }

    // Block 44, D8; Block 29, D3: so does a message over the size limit.
    #[wasm_bindgen_test]
    async fn a_message_over_the_limit_is_read_after_the_messages_before_it() {
        let fetch = fetch_of(&format!(
            "controller.enqueue(encode('{{}}\\n{}\\n'));",
            "x".repeat(64)
        ));
        let mut http = open_bounded(fetch, 16, BOUND).await;

        assert_eq!(recv_within(&mut http, false, 100).await, None);

        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(empty_frame())
        );
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::TooLarge)
        );
    }

    // Block 44, D7: past the bound the stream ends, holding nothing, and
    // that is reported though the core does not read.
    #[wasm_bindgen_test]
    async fn an_overflow_ends_the_stream_and_holds_nothing() {
        let (fetch, state) = fake_fetch("basic", 0);
        let bound = Bound {
            messages: 2,
            bytes: 1 << 20,
        };
        let mut http = open_bounded(fetch, LIMIT, bound).await;

        assert_eq!(
            recv_within(&mut http, false, 1000).await,
            Some(Inbound::Overflow)
        );

        assert_eq!(pulls(&state), 3.0, "the reading stopped at the overflow");
        assert!(http.state.borrow().inbox.is_empty());
    }

    // Codex, #209: a chunk can hold more messages than the inbox while the
    // client is reading all of them.
    #[wasm_bindgen_test]
    async fn a_chunk_larger_than_the_inbox_is_no_overflow_while_the_client_reads() {
        let (fetch, state) = paced_fetch();
        let bound = Bound {
            messages: 1,
            bytes: 1 << 20,
        };
        let mut http = open_bounded(fetch, LIMIT, bound).await;
        push(&state, &format!("{}{}{}", line(1), line(2), line(3)));

        for _ in 0..3 {
            assert!(
                matches!(
                    recv_within(&mut http, true, 1000).await,
                    Some(Inbound::Frame(_))
                ),
                "a message of the chunk"
            );
        }

        assert!(!http.state.borrow().overflowed);
    }

    // Block 44, D8: the reading ends with the transport, also over a body
    // that ignores the abort.
    #[wasm_bindgen_test]
    async fn dropping_the_transport_ends_the_reading_of_a_body_ignoring_the_abort() {
        let (fetch, state) = paced_fetch();
        let http = open(fetch).await;
        push(&state, &line(1));
        platform::sleep(Duration::from_millis(50)).await;
        assert_eq!(served(&state), 1.0);

        drop(http);
        platform::sleep(Duration::from_millis(50)).await;

        assert!(
            get(&state, &["cancelled"]).is_truthy(),
            "the body was read on"
        );
    }

    // Block 25, D8: a redirect not followed is an opaque one.
    #[wasm_bindgen_test]
    async fn a_redirect_is_reported_as_a_captive_portal() {
        let (fetch, _) = fake_fetch("opaqueredirect", 0);

        let opened = HttpStream::open(
            fetch,
            None,
            URL,
            Vec::new(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            false,
        )
        .await;

        let reason = opened.err().expect("not opened");
        assert!(reason.ends_with(CAPTIVE_PORTAL), "{reason}");
    }

    // Block 8: 413 ends only its commands.
    #[wasm_bindgen_test]
    async fn a_rejected_emulation_request_keeps_the_transport() {
        let (fetch, _) = fake_fetch("basic", 413);
        let mut http = open(fetch).await;
        http.send(FrameId::for_tests(1), b"big".to_vec());

        assert_eq!(
            recv_within(&mut http, false, 1000).await,
            Some(Inbound::Rejected(b"big".to_vec()))
        );
        assert_eq!(
            recv_within(&mut http, false, 1000).await,
            Some(Inbound::Written(FrameId::for_tests(1)))
        );
    }

    // Block 8: a 404 or 5xx closes the transport.
    #[wasm_bindgen_test]
    async fn a_failed_emulation_request_ends_the_transport() {
        for status in [404, 503] {
            let (fetch, _) = fake_fetch("basic", status);
            let mut http = open(fetch).await;
            http.send(FrameId::for_tests(1), b"{}".to_vec());

            assert_eq!(
                recv_within(&mut http, false, 1000).await,
                Some(Inbound::Closed {
                    code: None,
                    reason: format!("emulation request failed: HTTP status {status}"),
                })
            );
        }
    }

    // Block 44, D13: credentials included on the stream and emulation
    // requests alike.
    #[wasm_bindgen_test]
    async fn included_credentials_go_on_every_request() {
        let (fetch, state) = fake_fetch("basic", 200);
        let mut http = HttpStream::open(
            fetch,
            None,
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            true,
        )
        .await
        .unwrap();
        http.send(FrameId::for_tests(1), b"{}".to_vec());

        assert_eq!(get(&init(&state, 0), &["credentials"]), "include");
        assert_eq!(get(&init(&state, 1), &["credentials"]), "include");
    }

    /// A `ReadableStream` shim counting the `pull` calls of the streams it
    /// builds in `state.shimPulls`, and the streams in `state.shimmed`.
    fn counting_readable_stream(state: &Object) -> Function {
        Function::new_with_args(
            "state",
            "state.shimPulls = 0;
             state.shimmed = 0;
             return function (source, strategy) {
               state.shimmed += 1;
               const pull = source.pull;
               return new ReadableStream({
                 pull(controller) { state.shimPulls += 1; return pull(controller); },
                 cancel(reason) { return source.cancel(reason); },
               }, strategy);
             };",
        )
        .call1(&JsValue::NULL, state)
        .expect("a ReadableStream shim")
        .unchecked_into()
    }

    // Block 44, D8, D12: through the shim's stream the body is read whether
    // or not the core reads, and its chunks reach the inbox unchanged.
    #[wasm_bindgen_test]
    async fn through_the_readable_stream_shim_the_body_is_read_while_the_core_is_not_reading() {
        let (fetch, state) = fake_fetch("basic", 0);
        let shim = counting_readable_stream(&state);
        let mut http = HttpStream::open(
            fetch,
            Some(&shim),
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            false,
        )
        .await
        .unwrap();
        assert_eq!(get(&state, &["shimmed"]), 1);

        platform::sleep(Duration::from_millis(100)).await;

        assert_eq!(pulls(&state), LINES);
        assert!(
            get(&state, &["shimPulls"]).as_f64().unwrap() >= LINES,
            "the shim's stream was read"
        );
        assert_eq!(recv_within(&mut http, false, 50).await, None);
        for _ in 0..4 {
            assert_eq!(
                recv_within(&mut http, true, 1000).await,
                Some(empty_frame())
            );
        }
    }

    // The chunks go through unchanged, and the body's end ends the stream.
    #[wasm_bindgen_test]
    async fn the_readable_stream_shim_carries_the_body_to_its_end() {
        let fetch: Function = Function::new_no_args(
            "return (url, init) => {
               const body = new ReadableStream({
                 start(controller) {
                   controller.enqueue(new TextEncoder().encode('{\"a\":'));
                   controller.enqueue(new TextEncoder().encode('1}\\n'));
                   controller.close();
                 },
               });
               const headers = new Headers({ 'content-type': 'application/json' });
               return Promise.resolve({ type: 'basic', status: 200, headers, body });
             };",
        )
        .call0(&JsValue::NULL)
        .unwrap()
        .unchecked_into();
        let state = Object::new();
        let shim = counting_readable_stream(&state);
        let mut http = HttpStream::open(
            fetch,
            Some(&shim),
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Frame(br#"{"a":1}"#.to_vec()))
        );
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Closed {
                code: None,
                reason: "stream ended".to_owned(),
            })
        );
    }

    // A shim that throws fails the attempt.
    #[wasm_bindgen_test]
    async fn a_readable_stream_shim_that_throws_fails_the_attempt() {
        let (fetch, _) = fake_fetch("basic", 0);
        let shim = Function::new_no_args("throw new Error('no streams here');");

        let opened = HttpStream::open(
            fetch,
            Some(&shim),
            URL,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            false,
            LIMIT,
            BOUND,
            false,
        )
        .await;

        let reason = opened.err().expect("not opened");
        assert!(reason.contains("no streams here"), "{reason}");
    }

    /// A `fetch` standing in for a server over `http_stream`, each body chunk
    /// served only when a read asks for it, one per read, as a transport
    /// that only reads when polled: `state.push(text)` queues a chunk, and
    /// `state.served` counts those read. Commands of the stream request and
    /// of emulation requests are answered on the stream.
    fn paced_fetch() -> (Function, Object) {
        let state = Object::new();
        let make = Function::new_with_args(
            "state",
            "state.served = 0;
             const queue = [];
             let wake = null;
             state.push = (text) => {
               queue.push(new TextEncoder().encode(text));
               if (wake) { const waking = wake; wake = null; waking(); }
             };
             state.pushLater = (text, millis) => setTimeout(() => state.push(text), millis);
             const answer = (frame) => {
               for (const line of frame.split('\\n')) {
                 if (line === '') continue;
                 const command = JSON.parse(line);
                 if (command.connect) {
                   state.push(JSON.stringify({ id: command.id, connect: { client: 'c', session: 's', node: 'n' } }) + '\\n');
                 } else if (command.subscribe) {
                   state.push(JSON.stringify({ id: command.id, subscribe: {} }) + '\\n');
                 }
               }
             };
             return (url, init) => {
               const text = new TextDecoder().decode(init.body);
               if (url.endsWith('/emulation')) {
                 answer(JSON.parse(text).data);
                 return Promise.resolve({ type: 'basic', status: 200, headers: new Headers() });
               }
               answer(text);
               const body = new ReadableStream({
                 async pull(controller) {
                   while (queue.length === 0) {
                     await new Promise((resolve) => { wake = resolve; });
                   }
                   if (state.cancelled) return;
                   state.served += 1;
                   controller.enqueue(queue.shift());
                 },
                 cancel() { state.cancelled = true; },
               }, { highWaterMark: 0 });
               const headers = new Headers({ 'content-type': 'application/json' });
               return Promise.resolve({ type: 'basic', status: 200, headers, body });
             };",
        );
        let fetch = make
            .call1(&JsValue::NULL, &state)
            .expect("a paced fetch")
            .unchecked_into();
        (fetch, state)
    }

    fn push(state: &Object, text: &str) {
        let push: Function = get(state, &["push"]).unchecked_into();
        push.call1(&JsValue::NULL, &JsValue::from_str(text))
            .expect("a chunk queued");
    }

    fn push_later(state: &Object, text: &str, millis: u32) {
        let later: Function = get(state, &["pushLater"]).unchecked_into();
        later
            .call2(
                &JsValue::NULL,
                &JsValue::from_str(text),
                &JsValue::from(millis),
            )
            .expect("a chunk scheduled");
    }

    fn served(state: &Object) -> f64 {
        get(state, &["served"]).as_f64().expect("a count")
    }

    fn line(n: u32) -> String {
        format!("{{\"n\":{n}}}\n")
    }

    fn frame(n: u32) -> Inbound {
        Inbound::Frame(format!("{{\"n\":{n}}}").into_bytes())
    }

    // Block 44, D7, D8: what the body brings while the core does not read
    // waits in the inbox, in order, and a chunk arriving later is read.
    #[wasm_bindgen_test]
    async fn messages_read_while_the_core_is_not_reading_wait_in_the_inbox() {
        let (fetch, state) = paced_fetch();
        let mut http = open(fetch).await;
        // The stream request's frame is `{}`, which the fetch does not answer.
        push(&state, &line(1));
        assert_eq!(recv_within(&mut http, true, 1000).await, Some(frame(1)));

        push(&state, &line(2));
        push(&state, &line(3));
        assert_eq!(recv_within(&mut http, false, 100).await, None);
        assert_eq!(served(&state), 3.0, "the body was read on");

        assert_eq!(recv_within(&mut http, true, 1000).await, Some(frame(2)));
        assert_eq!(recv_within(&mut http, true, 1000).await, Some(frame(3)));

        push_later(&state, &line(4), 20);
        assert_eq!(recv_within(&mut http, true, 1000).await, Some(frame(4)));
    }

    /// A client over a [`paced_fetch`] with a read-ahead of `read_ahead`
    /// messages, connected and subscribed to "news"; neither stream is read
    /// further.
    async fn subscribed_over_paced_fetch(
        read_ahead: usize,
    ) -> (
        Client,
        Object,
        impl Stream<Item = ClientEvent> + Unpin,
        impl Stream<Item = SubscriptionEvent> + Unpin,
    ) {
        let (fetch, state) = paced_fetch();
        let client = Client::builder_with_transports([(TransportKind::HttpStream, URL.to_owned())])
            .config(Config::default().read_ahead_messages(read_ahead))
            .fetch(fetch)
            .build()
            .expect("a client");
        let subscription = client.new_subscription("news").expect("a subscription");
        let mut events = client.events();
        let mut subscription_events = subscription.events();
        client.connect();
        subscription.subscribe();
        loop {
            match next_within(&mut events, WAIT).await {
                ClientEvent::Connected { .. } => break,
                ClientEvent::Connecting { .. } => {}
                other => panic!("{other:?}"),
            }
        }
        loop {
            match next_within(&mut subscription_events, WAIT).await {
                SubscriptionEvent::Subscribed(_) => break,
                SubscriptionEvent::Subscribing { .. } => {}
                other => panic!("{other:?}"),
            }
        }
        (client, state, events, subscription_events)
    }

    fn publication(n: u32) -> String {
        format!("{{\"push\":{{\"channel\":\"news\",\"pub\":{{\"data\":{n}}}}}}}\n")
    }

    // Block 44, D7: the whole client over a body served one chunk per read:
    // a stream not read fills the read-ahead while the body is read on into
    // the inbox, and taking the events gives every publication in order.
    #[wasm_bindgen_test]
    async fn a_client_not_taking_its_events_reads_on_and_delivers_every_publication() {
        const COUNT: u32 = 1000;
        let (client, state, _events, mut subscription_events) =
            subscribed_over_paced_fetch(600).await;

        for n in 1..=COUNT {
            push(&state, &publication(n));
        }
        // The two replies, then every publication.
        let everything = f64::from(COUNT) + 2.0;
        for _ in 0..300 {
            if served(&state) >= everything {
                break;
            }
            platform::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(served(&state), everything);
        assert_eq!(client.read_ahead().0, 600);

        for n in 1..=COUNT {
            match next_within(&mut subscription_events, WAIT).await {
                SubscriptionEvent::Publication(publication) => {
                    assert_eq!(publication.data, n.to_string().into_bytes());
                }
                other => panic!("publication {n}: {other:?}"),
            }
        }
    }

    // Block 44, D7: past the read-ahead and the inbox the client reconnects
    // with code 7, as over websocket and sse.
    #[wasm_bindgen_test]
    async fn a_client_not_taking_its_events_reconnects_with_code_7_past_its_inbox() {
        let (_client, state, mut events, _subscription_events) =
            subscribed_over_paced_fetch(16).await;

        for n in 1..=100 {
            push(&state, &publication(n));
        }

        loop {
            if let ClientEvent::Connecting { code, reason } = next_within(&mut events, WAIT).await {
                assert_eq!((code, reason.as_str()), (7, "read-ahead overflow"));
                break;
            }
        }
    }

    async fn next_within<E>(events: &mut (impl Stream<Item = E> + Unpin), wait: Duration) -> E {
        let timer = platform::sleep(wait);
        match select(events.next(), std::pin::pin!(timer)).await {
            Either::Left((Some(event), _)) => event,
            Either::Left((None, _)) => panic!("the stream ended"),
            Either::Right(_) => panic!("no event within {wait:?}"),
        }
    }

    #[wasm_bindgen_test]
    fn emulation_requests_go_to_the_stream_server_unless_configured() {
        assert_eq!(
            emulation_url("https://example.com:8443/prefix/http_stream?a=b#c", None).as_deref(),
            Ok("https://example.com:8443/emulation")
        );
        assert_eq!(
            emulation_url(URL, Some("https://other.example/x/emulation")).as_deref(),
            Ok("https://other.example/x/emulation")
        );
        assert!(emulation_url(URL, Some("not a url")).is_err());
    }
}
