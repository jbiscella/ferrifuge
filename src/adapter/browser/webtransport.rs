//! WebTransport in the browser (Block 48, D17): the browser's
//! `WebTransport`, one bidirectional stream of its session, as
//! centrifuge-js 63ed73e `transport_webtransport.ts` opens it. The
//! bindings are declared here, for the members centrifuge-js uses: web-sys
//! has them only behind `--cfg=web_sys_unstable_apis`, which every
//! application would have to set.
//!
//! The stream is read as `http_stream`'s body is, through D7's decoders
//! into the bounded inbox (Block 44, D7, D8); frames go through the
//! writer of the stream, JSON frames with one `\n` appended, and count as
//! written when the writer's promise resolves (D8).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use futures_util::future::{abortable, select};
use js_sys::{Promise, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::{JsValue, wasm_bindgen};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{ReadableStream, WritableStream, WritableStreamDefaultWriter};

use super::http_stream::{Reading, State, read_body};
use super::websocket::{Bound, Inbound, js_message};
use crate::adapter::body::Decoder;
use crate::adapter::platform::{self, Instant, Sleep};
use crate::machine::FrameId;

#[wasm_bindgen]
extern "C" {
    /// The page's `WebTransport` (D17).
    #[wasm_bindgen(js_name = WebTransport)]
    #[derive(Clone)]
    type Session;

    /// `new WebTransport(url)`; it throws for a URL it refuses.
    #[wasm_bindgen(constructor, catch, js_class = "WebTransport")]
    fn new(url: &str) -> Result<Session, JsValue>;

    /// Resolves once the session is established.
    #[wasm_bindgen(method, getter)]
    fn ready(this: &Session) -> Promise;

    /// Settles once the session is closed; rejects when it was not closed
    /// cleanly.
    #[wasm_bindgen(method, getter)]
    fn closed(this: &Session) -> Promise;

    #[wasm_bindgen(method, js_name = createBidirectionalStream)]
    fn create_bidirectional_stream(this: &Session) -> Promise;

    #[wasm_bindgen(method, catch)]
    fn close(this: &Session) -> Result<(), JsValue>;

    /// A `WebTransportBidirectionalStream`.
    type BidirectionalStream;

    #[wasm_bindgen(method, getter)]
    fn readable(this: &BidirectionalStream) -> ReadableStream;

    #[wasm_bindgen(method, getter)]
    fn writable(this: &BidirectionalStream) -> WritableStream;
}

/// The session URL for the configured endpoint: with Protobuf, its query
/// gains `cf_protocol=protobuf`, as centrifuge-js adds it
/// (`transport_webtransport.ts:45-47`; D2).
fn session_url(url: &str, protobuf: bool) -> String {
    if !protobuf {
        return url.to_owned();
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}cf_protocol=protobuf")
}

/// An open WebTransport session and its stream.
pub(in crate::adapter) struct WebTransport {
    session: Session,
    writer: WritableStreamDefaultWriter,
    json: bool,
    state: Rc<RefCell<State>>,
    /// Stops the task reading the stream when the transport closes or is
    /// dropped.
    _reading: Reading,
    /// Frames handed to the writer whose promise has not resolved, oldest
    /// first, with when each was queued.
    pending: VecDeque<(FrameId, JsFuture, Instant)>,
    /// Bounds the oldest pending write: it exists only while a write is
    /// pending (D17, Block 14's idle page).
    deadline: Option<Pin<Box<Sleep>>>,
    timeout: Duration,
    /// Frames whose write resolved and not yet reported.
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a message comes first next.
    read_next: bool,
    /// A write that failed or timed out: the end of the transport, reported
    /// after the messages already in the inbox when reading (D9).
    failed: Option<Inbound>,
}

impl WebTransport {
    /// Opens a session at `url` with the page's `WebTransport`, waits until
    /// it is ready, and opens its bidirectional stream. A constructor that
    /// throws fails the attempt with its message. The caller bounds it with
    /// the handshake timeout. A message over `max_message` bytes ends the
    /// transport ([`Inbound::TooLarge`]), and so does one that would take the
    /// inbox past `bound` ([`Inbound::Overflow`]).
    pub(in crate::adapter) async fn open(
        url: &str,
        protobuf: bool,
        timeout: Duration,
        max_message: usize,
        bound: Bound,
    ) -> Result<Self, String> {
        let session = Session::new(&session_url(url, protobuf)).map_err(|e| js_message(&e))?;
        // Closed when the attempt is dropped before it opened.
        let guard = CloseOnDrop(Some(session.clone()));
        JsFuture::from(session.ready())
            .await
            .map_err(|error| js_message(&error))?;
        let stream: BidirectionalStream = JsFuture::from(session.create_bidirectional_stream())
            .await
            .map_err(|error| js_message(&error))?
            .unchecked_into();
        let writer = stream
            .writable()
            .get_writer()
            .map_err(|error| js_message(&error))?;
        let decoder = if protobuf {
            Decoder::protobuf(max_message)
        } else {
            Decoder::lines(max_message)
        };
        let state = Rc::new(RefCell::new(State::default()));
        let readable = wasm_streams::ReadableStream::from_raw(stream.readable()).into_stream();
        // A session that closes ends the transport even if its stream has
        // not ended, after the messages decoded before (D17, D9): the reader
        // waits for both.
        let closed = JsFuture::from(session.closed());
        let (reading, handle) = abortable(read_body(
            None,
            readable,
            decoder,
            bound,
            Rc::clone(&state),
            Some(closed),
        ));
        spawn_local(async move {
            let _ = reading.await;
        });
        let mut guard = guard;
        guard.0 = None;
        Ok(Self {
            session,
            writer,
            json: !protobuf,
            state,
            _reading: Reading(handle),
            pending: VecDeque::new(),
            deadline: None,
            timeout,
            written: VecDeque::new(),
            read_next: false,
            failed: None,
        })
    }

    /// Hands frame `id` to the stream's writer, JSON with one `\n` after
    /// it (D7); [`WebTransport::recv`] reports it written once the
    /// writer's promise resolves.
    pub(in crate::adapter) fn send(&mut self, id: FrameId, mut frame: Vec<u8>) {
        if self.json {
            frame.push(b'\n');
        }
        let chunk = Uint8Array::from(frame.as_slice());
        let write = JsFuture::from(self.writer.write_with_chunk(&chunk));
        self.pending.push_back((id, write, Instant::now()));
    }

    /// Closes the writer, then the session, within `Config::timeout`
    /// (D17, D11).
    pub(in crate::adapter) async fn close(self) {
        let timeout = gloo_timers::future::TimeoutFuture::new(
            u32::try_from(self.timeout.as_millis()).unwrap_or(u32::MAX),
        );
        let closing = async {
            let _ = JsFuture::from(self.writer.close()).await;
            let _ = self.session.close();
        };
        let _ = select(std::pin::pin!(closing), timeout).await;
        // Past the timeout, the session is closed without waiting.
        let _ = self.session.close();
    }

    /// The next message from the server, the end of the transport, or a
    /// frame written. A write that fails, or that has not resolved within
    /// `Config::timeout` of being queued, ends the transport (D17). Without
    /// `reading`, only frames written, a failed or late write and an
    /// overflow are reported. Cancel-safe.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        let Self {
            state,
            pending,
            deadline,
            timeout,
            written,
            read_next,
            failed,
            ..
        } = self;
        std::future::poll_fn(|cx| {
            if state.borrow().overflowed {
                return Poll::Ready(Inbound::Overflow);
            }
            while failed.is_none()
                && let Some((id, write, _)) = pending.front_mut()
            {
                match Pin::new(write).poll(cx) {
                    Poll::Ready(Ok(_)) => {
                        written.push_back(*id);
                        pending.pop_front();
                        *deadline = None;
                    }
                    Poll::Ready(Err(error)) => {
                        *failed = Some(Inbound::Closed {
                            code: None,
                            reason: js_message(&error),
                        });
                    }
                    Poll::Pending => break,
                }
            }
            if failed.is_none()
                && let Some((_, _, queued)) = pending.front()
            {
                let due = *queued + *timeout;
                let timer = deadline.get_or_insert_with(|| Box::pin(platform::sleep(*timeout)));
                if timer.deadline() != due {
                    timer.as_mut().reset(due);
                }
                if timer.as_mut().poll(cx).is_ready() {
                    *failed = Some(Inbound::Closed {
                        code: None,
                        reason: "write timeout".to_owned(),
                    });
                }
            }
            let mut shared = state.borrow_mut();
            // A failed write ends the transport after the messages already
            // decoded, a disconnect push among them (D9); without reading,
            // at once (Block 15).
            if failed.is_some() {
                if reading && let Some(item) = shared.inbox.pop() {
                    return Poll::Ready(item);
                }
                return Poll::Ready(failed.take().expect("checked"));
            }
            // Written reports and messages take turns, as on a websocket.
            let frame_first = reading && !shared.inbox.is_empty() && *read_next;
            if !frame_first && let Some(id) = written.pop_front() {
                *read_next = true;
                return Poll::Ready(Inbound::Written(id));
            }
            if reading && let Some(item) = shared.inbox.pop() {
                *read_next = false;
                return Poll::Ready(item);
            }
            shared.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

/// The driver drops a transport that ended (a write timeout, an overflow,
/// a message over the limit) without closing it: its session is closed
/// here, as natively the endpoint goes with the transport (D11).
impl Drop for WebTransport {
    fn drop(&mut self) {
        let _ = self.session.close();
    }
}

/// Closes a session whose attempt was dropped before it opened.
struct CloseOnDrop(Option<Session>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(session) = &self.0 {
            let _ = session.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use futures_util::future::{Either, select};
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{WebTransport, session_url};
    use crate::adapter::browser::websocket::{Bound, Inbound};
    use crate::adapter::platform;
    use crate::machine::FrameId;

    const URL: &str = "https://127.0.0.1:8443/connection/webtransport";
    const LIMIT: usize = 1 << 20;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const BOUND: Bound = Bound {
        messages: 1 << 10,
        bytes: 1 << 20,
    };

    // D2: Protobuf adds `cf_protocol=protobuf` to the query, keeping one
    // given; JSON adds nothing.
    #[wasm_bindgen_test]
    fn protobuf_adds_its_query_parameter() {
        assert_eq!(session_url(URL, false), URL);
        assert_eq!(
            session_url(URL, true),
            format!("{URL}?cf_protocol=protobuf")
        );
        assert_eq!(
            session_url(&format!("{URL}?a=1"), true),
            format!("{URL}?a=1&cf_protocol=protobuf")
        );
    }

    /// Runs `body` with a `WebTransport` on `globalThis` standing in for
    /// the page's, given the object where it keeps what it saw: `url`,
    /// `chunks` (each chunk written, as an array of bytes) and `controller`
    /// (of the stream's readable side). With `hold`, no write resolves;
    /// with `throws`, the constructor throws "refused".
    async fn with_fake<F: Future<Output = ()>>(
        hold: bool,
        throws: bool,
        body: impl FnOnce(Object) -> F,
    ) {
        let global = js_sys::global();
        let key = JsValue::from_str("WebTransport");
        let saved = Reflect::get(&global, &key).expect("the global");
        let state = Object::new();
        let make = Function::new_with_args(
            "state, hold, throws",
            "state.chunks = [];
             state.closeCalls = 0;
             return class {
               constructor(url) {
                 if (throws) throw new Error('refused');
                 state.url = url;
                 this.ready = Promise.resolve();
                 this.closed = new Promise((resolve) => { state.closeSession = resolve; });
               }
               createBidirectionalStream() {
                 const readable = new ReadableStream({ start(c) { state.controller = c; } });
                 const writable = new WritableStream({
                   write(chunk) {
                     state.chunks.push(Array.from(chunk));
                     if (state.reject) return Promise.reject(new Error('stream error'));
                     return hold ? new Promise(() => {}) : undefined;
                   },
                 });
                 return Promise.resolve({ readable, writable });
               }
               close() { state.closeCalls += 1; }
             };",
        );
        let fake = make
            .call3(&JsValue::NULL, &state, &hold.into(), &throws.into())
            .expect("a fake WebTransport");
        Reflect::set(&global, &key, &fake).expect("installed");
        body(state).await;
        Reflect::set(&global, &key, &saved).expect("restored");
    }

    fn call(state: &Object, code: &str, text: &str) {
        Function::new_with_args("state, text", code)
            .call2(&JsValue::NULL, state, &JsValue::from_str(text))
            .expect("the fake answers");
    }

    /// Bytes on the stream's readable side.
    fn push(state: &Object, text: &str) {
        call(
            state,
            "state.controller.enqueue(new TextEncoder().encode(text))",
            text,
        );
    }

    fn chunks(state: &Object) -> Vec<Vec<u8>> {
        let chunks: js_sys::Array = Reflect::get(state, &JsValue::from_str("chunks"))
            .expect("chunks")
            .into();
        chunks
            .iter()
            .map(|chunk| {
                js_sys::Array::from(&chunk)
                    .iter()
                    .map(|byte| byte.as_f64().expect("a byte") as u8)
                    .collect()
            })
            .collect()
    }

    async fn open(protobuf: bool, timeout: Duration) -> Result<WebTransport, String> {
        WebTransport::open(URL, protobuf, timeout, LIMIT, BOUND).await
    }

    /// What `recv` gives within `millis`, if anything.
    async fn recv_within(transport: &mut WebTransport, millis: u64) -> Option<Inbound> {
        let timer = platform::sleep(Duration::from_millis(millis));
        match select(std::pin::pin!(transport.recv(true)), std::pin::pin!(timer)).await {
            Either::Left((inbound, _)) => Some(inbound),
            Either::Right(_) => None,
        }
    }

    // D17, D7: a JSON frame goes to the writer with one `\n`, a Protobuf
    // frame as it is, the session URL asking for Protobuf; each counts as
    // written once the writer's promise resolved.
    #[wasm_bindgen_test]
    async fn frames_are_written_through_the_writer() {
        for (protobuf, frame, written) in [
            (false, b"{}".to_vec(), b"{}\n".to_vec()),
            (true, vec![1, 2], vec![1, 2]),
        ] {
            with_fake(false, false, |state| async move {
                let mut transport = open(protobuf, TIMEOUT).await.expect("opened");

                transport.send(FrameId::for_tests(1), frame);
                let inbound = recv_within(&mut transport, 2000).await;

                assert!(
                    matches!(inbound, Some(Inbound::Written(id)) if id == FrameId::for_tests(1)),
                    "{inbound:?}"
                );
                assert_eq!(chunks(&state), [written]);
                let url = Reflect::get(&state, &JsValue::from_str("url"))
                    .expect("url")
                    .as_string()
                    .expect("a URL");
                assert_eq!(url, session_url(URL, protobuf));
            })
            .await;
        }
    }

    // D17, D8: a write the stream has not taken within the timeout ends the
    // transport.
    #[wasm_bindgen_test]
    async fn a_write_not_taken_within_the_timeout_ends_the_transport() {
        with_fake(true, false, |_| async {
            let mut transport = open(false, Duration::from_millis(200))
                .await
                .expect("opened");

            transport.send(FrameId::for_tests(1), b"{}".to_vec());
            let inbound = recv_within(&mut transport, 2000).await;

            assert!(
                matches!(&inbound, Some(Inbound::Closed { code: None, reason }) if reason == "write timeout"),
                "{inbound:?}"
            );
        })
        .await;
    }

    // D17, D9: the messages read before the end of the stream come first,
    // then the end, with no code.
    #[wasm_bindgen_test]
    async fn the_end_of_the_stream_comes_after_the_messages_before_it() {
        with_fake(false, false, |state| async move {
            let mut transport = open(false, TIMEOUT).await.expect("opened");

            push(&state, "{\"a\":1}\n{\"b\":2}\n");
            call(&state, "state.controller.close()", "");
            let mut seen = Vec::new();
            for _ in 0..3 {
                seen.push(recv_within(&mut transport, 2000).await);
            }

            assert!(
                matches!(
                    seen.as_slice(),
                    [
                        Some(Inbound::Frame(a)),
                        Some(Inbound::Frame(b)),
                        Some(Inbound::Closed { code: None, .. }),
                    ] if a == b"{\"a\":1}" && b == b"{\"b\":2}"
                ),
                "{seen:?}"
            );
        })
        .await;
    }

    // D17, D9 (Codex on #232): a session that closes while the reader
    // still holds messages it decoded, waiting for room in the inbox, ends
    // the transport only after all of them.
    #[wasm_bindgen_test]
    async fn a_closed_session_ends_the_transport_after_every_message_decoded() {
        with_fake(false, false, |state| async move {
            let small = Bound {
                messages: 2,
                bytes: 1 << 20,
            };
            let mut transport = WebTransport::open(URL, false, TIMEOUT, LIMIT, small)
                .await
                .expect("opened");

            push(
                &state,
                "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n{\"n\":5}\n",
            );
            call(&state, "state.closeSession()", "");
            let mut frames = 0;
            let end = loop {
                match recv_within(&mut transport, 2000).await {
                    Some(Inbound::Frame(_)) => frames += 1,
                    other => break other,
                }
            };

            assert_eq!(frames, 5);
            assert!(
                matches!(&end, Some(Inbound::Closed { code: None, .. })),
                "{end:?}"
            );
        })
        .await;
    }

    // D11 (Codex on #232): the driver drops a transport that ended without
    // closing it; its session is closed then.
    #[wasm_bindgen_test]
    async fn a_dropped_transport_closes_its_session() {
        with_fake(false, false, |state| async move {
            let transport = open(false, TIMEOUT).await.expect("opened");

            drop(transport);

            let calls = Reflect::get(&state, &JsValue::from_str("closeCalls"))
                .expect("closeCalls")
                .as_f64();
            assert!(calls.is_some_and(|calls| calls >= 1.0), "{calls:?}");
        })
        .await;
    }

    // D9 (Codex on #232): a write that fails, or is not taken within the
    // timeout, ends the transport only after the messages already in the
    // inbox, a disconnect push among them; without reading, at once.
    #[wasm_bindgen_test]
    async fn the_messages_decoded_before_a_failed_write_come_first() {
        for (hold, timeout, reason) in [
            (false, TIMEOUT, "stream error"),
            (true, Duration::from_millis(200), "write timeout"),
        ] {
            with_fake(hold, false, |state| async move {
                let mut transport = open(false, timeout).await.expect("opened");
                push(
                    &state,
                    "{}\n{\"push\":{\"disconnect\":{\"code\":3500,\"reason\":\"bye\"}}}\n",
                );
                // The reader admits both to the inbox.
                platform::sleep(Duration::from_millis(50)).await;
                // A write that fails, or with `hold` one never taken.
                if !hold {
                    Reflect::set(&state, &JsValue::from_str("reject"), &JsValue::TRUE)
                        .expect("set");
                }

                transport.send(FrameId::for_tests(1), b"{}".to_vec());
                let mut seen = Vec::new();
                for _ in 0..3 {
                    seen.push(recv_within(&mut transport, 2000).await);
                }

                assert!(
                    matches!(
                        seen.as_slice(),
                        [
                            Some(Inbound::Frame(ping)),
                            Some(Inbound::Frame(push)),
                            Some(Inbound::Closed { code: None, reason: end }),
                        ] if ping == b"{}" && push.starts_with(b"{\"push\"") && end.contains(reason)
                    ),
                    "{reason}: {seen:?}"
                );
            })
            .await;
        }
    }

    #[wasm_bindgen_test]
    async fn without_reading_a_failed_write_ends_the_transport_at_once() {
        with_fake(false, false, |state| async move {
            let mut transport = open(false, TIMEOUT).await.expect("opened");
            push(&state, "{}\n");
            platform::sleep(Duration::from_millis(50)).await;
            Reflect::set(&state, &JsValue::from_str("reject"), &JsValue::TRUE).expect("set");

            transport.send(FrameId::for_tests(1), b"{}".to_vec());
            let timer = platform::sleep(Duration::from_millis(2000));
            let inbound =
                match select(std::pin::pin!(transport.recv(false)), std::pin::pin!(timer)).await {
                    Either::Left((inbound, _)) => Some(inbound),
                    Either::Right(_) => None,
                };

            assert!(
                matches!(&inbound, Some(Inbound::Closed { code: None, reason }) if reason.contains("stream error")),
                "{inbound:?}"
            );
        })
        .await;
    }

    // D17: a constructor that throws fails the attempt with its message.
    #[wasm_bindgen_test]
    async fn a_constructor_that_throws_fails_the_attempt() {
        with_fake(false, true, |_| async {
            let opened = open(false, TIMEOUT).await;

            assert!(
                matches!(&opened, Err(message) if message.contains("refused")),
                "{:?}",
                opened.err()
            );
        })
        .await;
    }
}
