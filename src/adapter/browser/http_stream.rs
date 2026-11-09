//! `http_stream` transport of the browser adapter, over the browser's
//! `fetch` or the application's shim of it (Block 44, D8, D12), with
//! bidirectional emulation as natively (Block 8): the response to the
//! request opening the stream carries the replies; commands go in requests
//! to the emulation endpoint of the same server, as they do over sse (D9).
//! The body is read with wasm-streams, which reads it only when polled, so
//! a full read-ahead holds the server back (Block 15; Block 44, D7), also
//! through a stream built with the application's `ReadableStream` (D12).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::{Either, select};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt};
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use wasm_streams::readable::IntoStream;
use web_sys::{AbortController, ReadableStreamDefaultReader, Response, ResponseType};

use super::websocket::{Inbound, js_message};
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
/// unchanged, with `highWaterMark` 0, so the shim's stream reads only when
/// the transport polls it and a full read-ahead still holds the server back
/// (D7). Its `cancel` cancels the body.
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

/// An open `http_stream` connection.
pub(in crate::adapter) struct HttpStream {
    emulation: Emulation,
    body: IntoStream<'static>,
    /// The listeners of the `ReadableStream` shim's stream, if `body` reads
    /// one; after `body`, which cancels it when dropped.
    _through: Option<Through>,
    /// Aborts the stream request when the transport closes or is dropped;
    /// after `body`, which it errors.
    _abort: Option<Abort>,
    decoder: Decoder,
    /// Messages decoded but not received yet.
    frames: VecDeque<Vec<u8>>,
    /// Why the stream cannot go on, reported once `frames` are received.
    failed: Option<String>,
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
    /// server. Requests include credentials when `include_credentials`
    /// (D13).
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
        Ok(Self {
            emulation,
            body: wasm_streams::ReadableStream::from_raw(body).into_stream(),
            _through: through,
            _abort: abort,
            decoder: if protobuf {
                Decoder::protobuf(max_message)
            } else {
                Decoder::lines(max_message)
            },
            frames: VecDeque::new(),
            failed: None,
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

    /// The next message from the server, or the end of the transport: the
    /// stream ended or failed, or an emulation request failed. Without
    /// `reading` the body is not polled, so the browser stops reading it
    /// (Block 15). Cancel-safe.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        loop {
            // Written reports and messages take turns, as on a websocket.
            if !self.read_next
                && let Some(id) = self.written.pop_front()
            {
                self.read_next = true;
                return Inbound::Written(id);
            }
            if reading {
                if let Some(frame) = self.frames.pop_front() {
                    self.read_next = false;
                    return Inbound::Frame(frame);
                }
                if let Some(reason) = self.failed.take() {
                    // Over `Config::max_message_bytes` (Block 29, D3).
                    if reason == TOO_LARGE {
                        return Inbound::TooLarge;
                    }
                    return Inbound::Closed { code: None, reason };
                }
            }
            if let Some(id) = self.written.pop_front() {
                self.read_next = true;
                return Inbound::Written(id);
            }
            tokio::select! {
                // Not reading, the body waits: a full read-ahead stops the
                // reading, never the emulation requests (Block 15).
                chunk = self.body.next(), if reading => match chunk {
                    Some(Ok(chunk)) => {
                        let Some(chunk) = chunk.dyn_ref::<Uint8Array>() else {
                            return Inbound::Closed {
                                code: None,
                                reason: "a response body chunk that is not bytes".to_owned(),
                            };
                        };
                        if let Err(reason) = self.decoder.push(&chunk.to_vec(), &mut self.frames) {
                            self.failed = Some(reason);
                        }
                    }
                    Some(Err(error)) => {
                        return Inbound::Closed { code: None, reason: js_message(&error) };
                    }
                    None => {
                        return Inbound::Closed {
                            code: None,
                            reason: "stream ended".to_owned(),
                        };
                    }
                },
                (id, answered) = std::future::poll_fn(|cx| self.emulation.poll_answered(cx)) => {
                    return report(id, answered, &mut self.written);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::future::{Either, select};
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{HttpStream, emulation_url};
    use crate::adapter::body::CAPTIVE_PORTAL;
    use crate::adapter::browser::websocket::Inbound;
    use crate::adapter::platform;
    use crate::machine::FrameId;

    const URL: &str = "http://127.0.0.1:8000/connection/http_stream";
    const LIMIT: usize = 1 << 20;
    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A `fetch` standing in for the page's, with what it saw in `state`:
    /// `calls`, each `{ url, init }`, and `pulls`, the reads of the stream
    /// body, each of which gives one JSON line. The stream response is of
    /// type `stream_type`; emulation responses have status `emulation`, or
    /// never come when it is 0.
    fn fake_fetch(stream_type: &str, emulation: u16) -> (Function, Object) {
        let state = Object::new();
        let make = Function::new_with_args(
            "state, streamType, emulation",
            "state.calls = [];
             state.pulls = 0;
             return (url, init) => {
               state.calls.push({ url, init });
               if (url.endsWith('/emulation')) {
                 if (emulation === 0) return new Promise(() => {});
                 return Promise.resolve({ type: 'basic', status: emulation, headers: new Headers() });
               }
               const body = new ReadableStream({
                 pull(controller) {
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

    // Block 15; Block 44, D7: not reading, the body is not read.
    #[wasm_bindgen_test]
    async fn the_body_is_read_only_while_reading() {
        let (fetch, state) = fake_fetch("basic", 0);
        let mut http = open(fetch).await;
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
        );
        let read = pulls(&state);

        assert_eq!(recv_within(&mut http, false, 200).await, None);

        assert_eq!(pulls(&state), read);
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
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

    // Block 44, D12: through the shim's stream, the body is still read only
    // while the transport reads.
    #[wasm_bindgen_test]
    async fn through_the_readable_stream_shim_the_body_is_read_only_while_reading() {
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
            false,
        )
        .await
        .unwrap();
        assert_eq!(get(&state, &["shimmed"]), 1);
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
        );
        let read = (pulls(&state), get(&state, &["shimPulls"]).as_f64().unwrap());
        assert!(read.1 >= 1.0, "the shim's stream was read");

        assert_eq!(recv_within(&mut http, false, 200).await, None);

        assert_eq!(
            (pulls(&state), get(&state, &["shimPulls"]).as_f64().unwrap()),
            read
        );
        assert_eq!(
            recv_within(&mut http, true, 1000).await,
            Some(Inbound::Frame(b"{}".to_vec()))
        );
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
            false,
        )
        .await;

        let reason = opened.err().expect("not opened");
        assert!(reason.contains("no streams here"), "{reason}");
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
