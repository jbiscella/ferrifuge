//! `http_stream` transport of the browser adapter, over the browser's
//! `fetch` (Block 44, D8), with bidirectional emulation as natively (Block
//! 8): the response to the request opening the stream carries the replies;
//! commands go in requests to the emulation endpoint of the same server.
//! The body is read with wasm-streams, which reads it only when polled, so
//! a full read-ahead holds the server back (Block 15; Block 44, D7).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::{Either, select};
use futures_util::stream::FuturesUnordered;
use js_sys::{Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_streams::readable::IntoStream;
use web_sys::{AbortController, Response, ResponseType};

use super::websocket::{Inbound, js_message};
use crate::adapter::body::{CAPTIVE_PORTAL, Decoder, TOO_LARGE, captive_portal, with_portal};
use crate::adapter::platform;
use crate::machine::FrameId;

/// Path of the emulation endpoint (centrifuge-js `emulationEndpoint`).
const EMULATION_PATH: &str = "/emulation";
const JSON: &str = "application/json";
/// The media type of Protobuf requests and streams, exactly (Block 27, D3).
const PROTOBUF: &str = "application/octet-stream";

/// The page's `fetch` (D8; its shim comes with D12).
///
/// # Errors
///
/// When the page has none.
pub(in crate::adapter) fn global_fetch() -> Result<Function, String> {
    Reflect::get(&js_sys::global(), &JsValue::from_str("fetch"))
        .ok()
        .and_then(|fetch| fetch.dyn_into::<Function>().ok())
        .ok_or_else(|| "the page has no fetch function".to_owned())
}

/// Aborts its request when dropped, as reqwest's `AbortGuard`: a request
/// ends with the transport that made it (D8).
struct Abort(AbortController);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Starts `fetch(url, init)` as centrifuge-js calls it, `init` a plain
/// object, so that no global `Request` or `Headers` is needed (D8): a POST
/// of `body` with `headers`, credentials of the same origin only (D13
/// comes with Block 44f), and redirects not followed. The guard aborting
/// it, and its response.
fn start(
    fetch: &Function,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
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
        .and_then(|()| set(&init, "credentials", &JsValue::from_str("same-origin")))
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
enum Sent {
    Accepted,
    /// Rejected with HTTP 413, with the frame it carried.
    Rejected(Vec<u8>),
}

/// An open `http_stream` connection.
pub(in crate::adapter) struct HttpStream {
    fetch: Function,
    emulation_url: String,
    /// Time allowed for an emulation request, as for a command's reply.
    request_timeout: Duration,
    /// The media type of emulation requests: JSON's or Protobuf's.
    content_type: &'static str,
    body: IntoStream<'static>,
    /// Aborts the stream request when the transport closes or is dropped;
    /// after `body`, which it errors.
    _abort: Option<Abort>,
    decoder: Decoder,
    /// Messages decoded but not received yet.
    frames: VecDeque<Vec<u8>>,
    /// Why the stream cannot go on, reported once `frames` are received.
    failed: Option<String>,
    /// Each holds its request's guard: dropped with the transport, it
    /// aborts the request (D8).
    requests: FuturesUnordered<Request>,
    /// Frames whose request was answered and not yet reported by
    /// [`HttpStream::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a message comes first next.
    read_next: bool,
}

impl HttpStream {
    /// Opens the stream at `url` with `fetch`, sending `initial_frame`, the
    /// encoded connect command, with the request. Commands then go to
    /// `emulation_endpoint`, by default `/emulation` on the stream's server.
    pub(in crate::adapter) async fn open(
        fetch: Function,
        url: &str,
        initial_frame: Vec<u8>,
        emulation_endpoint: Option<&str>,
        request_timeout: Duration,
        protobuf: bool,
        max_message: usize,
    ) -> Result<Self, String> {
        let emulation_url = emulation_url(url, emulation_endpoint)?;
        let content_type = if protobuf { PROTOBUF } else { JSON };
        // Dropped with the attempt, the guard aborts the request.
        let (abort, response) = start(
            &fetch,
            url,
            &[("Accept", content_type), ("Content-Type", content_type)],
            &initial_frame,
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
        Ok(Self {
            fetch,
            emulation_url,
            request_timeout,
            content_type,
            body: wasm_streams::ReadableStream::from_raw(body).into_stream(),
            _abort: abort,
            decoder: if protobuf {
                Decoder::protobuf(max_message)
            } else {
                Decoder::lines(max_message)
            },
            frames: VecDeque::new(),
            failed: None,
            requests: FuturesUnordered::new(),
            written: VecDeque::new(),
            read_next: false,
        })
    }

    /// Sends frame `id`, the body of an emulation request started now,
    /// which [`HttpStream::recv`] reports written once answered. A request
    /// that fails ends the transport when [`HttpStream::recv`] reports it.
    pub(in crate::adapter) fn send(&mut self, id: FrameId, frame: Vec<u8>) {
        let (abort, response) = start(
            &self.fetch,
            &self.emulation_url,
            &[("Content-Type", self.content_type)],
            &frame,
        );
        let timeout = self.request_timeout;
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
                // Irrefutable: a refutable pattern would disable this branch
                // after a success until the stream yields (tokio `select!`).
                Some((id, result)) = self.requests.next(), if !self.requests.is_empty() => {
                    match result {
                        Err(reason) => return Inbound::Closed { code: None, reason },
                        // Answered, so written; its commands fail first.
                        Ok(Sent::Rejected(frame)) => {
                            self.written.push_back(id);
                            return Inbound::Rejected(frame);
                        }
                        Ok(Sent::Accepted) => return Inbound::Written(id),
                    }
                }
                else => std::future::pending::<()>().await,
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
        HttpStream::open(fetch, URL, b"{}".to_vec(), None, TIMEOUT, false, LIMIT)
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
        let mut http = HttpStream::open(fetch, URL, b"{}".to_vec(), None, TIMEOUT, true, LIMIT)
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

        let opened = HttpStream::open(fetch, URL, Vec::new(), None, TIMEOUT, false, LIMIT).await;

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
