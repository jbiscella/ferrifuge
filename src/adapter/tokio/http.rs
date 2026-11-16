//! HTTP-streaming transports of the tokio adapter: `http_stream` and sse,
//! with bidirectional emulation as in centrifuge-js
//! `transport_http_stream.ts` and `transport_sse.ts`. The response to the
//! request opening the stream carries the replies; commands go in requests
//! to the emulation endpoint of the same server.
// Block 8.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream::{BoxStream, FuturesUnordered};
use mediatype::names;
use reqwest::Url;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap};

#[cfg(test)]
use super::super::body::CAPTIVE_PORTAL;
use super::super::body::{
    Decoder, Emulated, FailureKind, Sent, TransportFailure, captive_portal, classify_emulation,
    emulation_url, is_media_type, report,
};
use super::super::shared::lock;
use super::net::{Jar, Network};
use super::proxy::PROXY_AUTHENTICATION_REQUIRED;
use super::tls::WithDeadline;
use super::transport::Inbound;
use crate::sans_io::{FrameId, TransportKind};

const JSON: &str = "application/json";
/// The media type of Protobuf requests and streams, exactly: Centrifugo
/// compares the whole value (centrifuge `handler_http_stream.go:73`,
/// `emulation.go:72`).
// Block 27, D3.
const PROTOBUF: &str = "application/octet-stream";
/// The application's real HTTP headers, shared by the client handle, which
/// replaces them, and every request, which reads them when it starts. Its
/// `Debug` output shows the names only.
// Blocks 17, 32.
#[derive(Clone, Default)]
pub(crate) struct HeaderSet(Arc<Mutex<Arc<HeaderMap>>>);

impl HeaderSet {
    pub(crate) fn new(headers: HeaderMap) -> Self {
        Self(Arc::new(Mutex::new(Arc::new(headers))))
    }

    /// The headers a request starting now carries.
    pub(crate) fn current(&self) -> HeaderMap {
        let headers = Arc::clone(&lock(&self.0));
        (*headers).clone()
    }

    /// Replaces the whole set, for the requests started from now on.
    pub(crate) fn replace(&self, headers: HeaderMap) {
        *lock(&self.0) = Arc::new(headers);
    }
}

/// A header set holding `name: value`, for the redaction test (Block 60,
/// D2): its values are secrets its `Debug` must not show.
#[cfg(test)]
pub(crate) fn header_set_sample(name: &'static str, value: &str) -> HeaderSet {
    let mut headers = HeaderMap::new();
    headers.insert(name, value.parse().expect("a header value"));
    HeaderSet::new(headers)
}

impl std::fmt::Debug for HeaderSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers = Arc::clone(&lock(&self.0));
        f.debug_list().entries(headers.keys()).finish()
    }
}

/// An emulation request in flight; its failure ends the transport.
type Request = Pin<Box<dyn Future<Output = (FrameId, Result<Sent, TransportFailure>)> + Send>>;

/// An open `http_stream` or sse connection.
pub(crate) struct HttpTransport {
    /// Client for the emulation endpoint, whose scheme may differ from the
    /// stream's when it is configured.
    emulation_http: reqwest::Client,
    emulation_url: Url,
    /// Time allowed for an emulation request, as for a command's reply.
    request_timeout: Duration,
    /// The application's HTTP headers, read for every request.
    // Blocks 17, 32.
    headers: HeaderSet,
    /// The cookie jar, read and filled by every request.
    // Block 43.
    jar: Jar,
    /// The media type of emulation requests: JSON's or Protobuf's.
    content_type: &'static str,
    body: BoxStream<'static, Result<Vec<u8>, TransportFailure>>,
    decoder: Decoder,
    /// Messages decoded but not received yet.
    frames: VecDeque<Vec<u8>>,
    /// Why the stream cannot go on, reported once `frames` are received.
    failed: Option<TransportFailure>,
    requests: FuturesUnordered<Request>,
    /// Frames whose request was answered and not yet reported by
    /// [`HttpTransport::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a message comes first next.
    read_next: bool,
}

impl HttpTransport {
    /// Opens the stream at `url`, sending `initial_frame`, the encoded
    /// connect command, with the request. Commands then go to
    /// `emulation_endpoint`, by default `/emulation` on the stream's server.
    #[expect(clippy::too_many_arguments, reason = "each is the transport's own")]
    pub(crate) async fn open(
        kind: TransportKind,
        url: &str,
        initial_frame: Vec<u8>,
        emulation_endpoint: Option<&str>,
        request_timeout: Duration,
        headers: HeaderSet,
        network: &Network,
        protobuf: bool,
        max_message: usize,
    ) -> Result<Self, TransportFailure> {
        let mut url = Url::parse(url).map_err(|e| TransportFailure::other(e.to_string()))?;
        let emulation_url = emulation_url::<Url>(url.as_str(), emulation_endpoint)?;
        // The HTTP client asks the same decision for each request; a proxy
        // it cannot use must fail the attempt here rather than send the
        // requests directly (Block 23, D2).
        network.decide(&url)?;
        network.decide(&emulation_url)?;
        let http = network.http_client(url.scheme() == "https")?;
        let sse = kind == TransportKind::Sse;
        // Protobuf only over http_stream: building refuses it with sse
        // (Block 27, D3).
        let requests_type = if protobuf { PROTOBUF } else { JSON };
        // The jar sees the URL before the connect command joins it: the
        // sse one would carry the token (Block 43, D4).
        let stream_url = url.to_string();
        let mut stream_headers = headers.current();
        network.add_cookies(&stream_url, &mut stream_headers);
        let request = if sse {
            url.query_pairs_mut()
                .append_pair("cf_connect", &String::from_utf8_lossy(&initial_frame));
            http.get(url)
                .headers(stream_headers)
                .header(ACCEPT, "text/event-stream")
        } else {
            http.post(url)
                .headers(stream_headers)
                .header(ACCEPT, requests_type)
                .header(CONTENT_TYPE, requests_type)
                .body(initial_frame)
        };
        // Without the URL: the sse one carries the connect command, token
        // included, which must not reach error events.
        let response = request.send().await.map_err(request_error)?;
        let status = response.status();
        // Whatever the status but a proxy's 407 (Block 43, D2, D5).
        network.store_cookies(&stream_url, status.as_u16(), response.headers());
        if status == reqwest::StatusCode::PROXY_AUTHENTICATION_REQUIRED {
            return Err(proxy_authentication());
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let portal = captive_portal(status.as_u16(), content_type);
        if !status.is_success() {
            return Err(TransportFailure::status(
                status.as_u16(),
                format!("HTTP status {status}"),
                portal,
            ));
        }
        // A web page instead of the stream (Block 25, D8): read as frames,
        // it would only make the client reconnect, without an error.
        if portal {
            return Err(TransportFailure::status(
                status.as_u16(),
                format!("HTTP status {status}, content type {content_type:?}"),
                true,
            ));
        }
        // What EventSource accepts (WHATWG HTML, "Server-sent events").
        if sse && (status != reqwest::StatusCode::OK || !is_event_stream(content_type)) {
            return Err(TransportFailure::other(format!(
                "not an event stream: HTTP status {status}, content type {content_type:?}"
            )));
        }
        let body = response
            .bytes_stream()
            .map(|chunk| {
                chunk.map(|bytes| bytes.to_vec()).map_err(|e| {
                    let e = e.without_url();
                    let message = e.to_string();
                    failure_of(&e, message)
                })
            })
            .boxed();
        Ok(Self {
            emulation_http: network.http_client(emulation_url.scheme() == "https")?,
            emulation_url,
            request_timeout,
            headers,
            jar: network.jar(),
            content_type: requests_type,
            body,
            decoder: if protobuf {
                Decoder::protobuf(max_message)
            } else {
                Decoder::new(sse, max_message)
            },
            frames: VecDeque::new(),
            failed: None,
            requests: FuturesUnordered::new(),
            written: VecDeque::new(),
            read_next: false,
        })
    }

    /// Sends frame `id`, the body of an emulation request, which
    /// [`HttpTransport::recv`] reports written once answered. A request that
    /// fails ends the transport when [`HttpTransport::recv`] reports it.
    pub(crate) fn send(&mut self, id: FrameId, frame: Vec<u8>) {
        // Kept to name the commands of a rejected request.
        let sent = frame.clone();
        // The set as it is now: replaced while open, it applies to the next
        // request (Block 32, D2); and the jar's cookies as they are now
        // (Block 43, D2).
        let mut headers = self.headers.current();
        let url = self.emulation_url.to_string();
        self.jar.add(&url, &mut headers);
        let request = self
            .emulation_http
            .post(self.emulation_url.clone())
            .headers(headers)
            .header(CONTENT_TYPE, self.content_type)
            .body(frame);
        let timeout = self.request_timeout;
        // The request's one deadline: its timer ends there, and it is the
        // thread's handshake deadline on every poll of the request, so a
        // connection the request starts verifies within it. Past what the
        // clock counts, there is none, as a timer that far never fires.
        // Block 64, D1.
        let deadline = Instant::now().checked_add(timeout);
        let jar = self.jar.clone();
        let exchange = async move {
            let answered = async {
                let outcome = match deadline {
                    // The request is polled before the timer: a check
                    // failing at the deadline fails the request with its
                    // own error (tokio `time/timeout.rs`, `poll`).
                    Some(deadline) => {
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline),
                            request.send(),
                        )
                        .await
                    }
                    None => Ok(request.send().await),
                };
                let response = outcome
                    .map_err(|_| {
                        TransportFailure::new(
                            FailureKind::Timeout,
                            format!("emulation request timed out after {timeout:?}"),
                        )
                    })?
                    .map_err(|e| {
                        let failure = request_error(e);
                        // A request failing at or after its deadline ran out
                        // of time, whatever reqwest's error says (Block 64,
                        // D2).
                        let late = deadline.is_some_and(|deadline| Instant::now() >= deadline);
                        TransportFailure {
                            kind: if late {
                                FailureKind::Timeout
                            } else {
                                failure.kind
                            },
                            message: format!("emulation request failed: {}", failure.message),
                        }
                    })?;
                let status = response.status();
                jar.store(&url, status.as_u16(), response.headers());
                if status == reqwest::StatusCode::PROXY_AUTHENTICATION_REQUIRED {
                    return Err(proxy_authentication());
                }
                // A redirect is not followed (see `http_client`).
                match classify_emulation(status.as_u16(), false) {
                    Emulated::Failed => Err(TransportFailure::new(
                        FailureKind::Http(status.as_u16()),
                        format!("emulation request failed: HTTP status {status}"),
                    )),
                    Emulated::Rejected => Ok(Sent::Rejected(sent)),
                    Emulated::Accepted => Ok(Sent::Accepted),
                }
            };
            (id, answered.await)
        };
        self.requests
            .push(Box::pin(WithDeadline::new(deadline, exchange)));
    }

    /// Closes the stream, and lets the emulation requests in flight finish,
    /// each within its own timeout: what the client sent before closing is
    /// not dropped, as a websocket's close writes its queued frames first.
    pub(crate) async fn close(self) {
        let Self { body, requests, .. } = self;
        drop(body);
        requests.for_each(|_| async {}).await;
    }

    /// The next message from the server, or the end of the transport: the
    /// stream ended or failed, or an emulation request failed. Cancel-safe.
    pub(crate) async fn recv(&mut self, reading: bool) -> Inbound {
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
                if let Some(failure) = self.failed.take() {
                    // Over `Config::max_message_bytes` (Block 29, D3).
                    if failure.kind == FailureKind::TooLarge {
                        return Inbound::TooLarge;
                    }
                    return Inbound::Closed {
                        code: None,
                        reason: failure.message,
                    };
                }
            }
            if let Some(id) = self.written.pop_front() {
                self.read_next = true;
                return Inbound::Written(id);
            }
            tokio::select! {
                // Not reading, the stream waits: a full read-ahead stops the
                // reading, never the emulation requests (Block 15).
                chunk = self.body.next(), if reading => match chunk {
                    Some(Ok(chunk)) => {
                        if let Err(failure) = self.decoder.push(&chunk, &mut self.frames) {
                            self.failed = Some(failure);
                        }
                    }
                    Some(Err(failure)) => {
                        return Inbound::Closed {
                            code: None,
                            reason: failure.message,
                        };
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
                // Answered, so written; a rejected one's commands fail first.
                Some((id, result)) = self.requests.next(), if !self.requests.is_empty() => {
                    return report(id, result, &mut self.written);
                }
                else => std::future::pending::<()>().await,
            }
        }
    }
}

/// Whether `content_type` is the event-stream media type, whatever its case
/// and parameters.
fn is_event_stream(content_type: &str) -> bool {
    is_media_type(content_type, names::TEXT, names::EVENT_STREAM)
}

/// The message of a failed request, without its URL, which may carry the
/// connect command and its token (sse) or a credential. reqwest's own
/// message stops at "error sending request": its sources follow, and a
/// proxy refusing the credentials says so.
// Block 21, D5; Block 23, D5.
fn request_error(error: reqwest::Error) -> TransportFailure {
    let error = error.without_url();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        let text = cause.to_string();
        // hyper-util's `TunnelError::ProxyAuthRequired` (`tunnel.rs:241`).
        if text == "tunnel error: proxy authorization required" {
            return proxy_authentication();
        }
        message.push_str(": ");
        message.push_str(&text);
        source = cause.source();
    }
    failure_of(&error, message)
}

/// The failure of `error`, of the kind it is, with `message`.
fn failure_of(error: &reqwest::Error, message: String) -> TransportFailure {
    let kind = if error.is_timeout() {
        FailureKind::Timeout
    } else {
        io_kind(error).map_or(FailureKind::Other, FailureKind::Io)
    };
    TransportFailure::new(kind, message)
}

/// The kind of the I/O error among `error`'s sources, if any.
pub(in crate::adapter) fn io_kind(
    error: &(dyn std::error::Error + 'static),
) -> Option<std::io::ErrorKind> {
    let mut source = Some(error);
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            return Some(io.kind());
        }
        source = cause.source();
    }
    None
}

/// A proxy's refusal of the credentials, or its demand for some.
fn proxy_authentication() -> TransportFailure {
    TransportFailure::new(
        FailureKind::ProxyAuthentication,
        PROXY_AUTHENTICATION_REQUIRED,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);
    const LIMIT: usize = 64 << 20;

    /// Serves `response`, after `delay`, to the first request, then keeps
    /// the connection open; the URL of `path` on it.
    async fn serve_once(response: &'static [u8], path: &str, delay: Duration) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}{path}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // The request, sent in one go: answered after it is read.
            let _ = socket.read(&mut [0; 4096]).await;
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        url
    }

    /// As [`serve_once`] without a delay: answers once `release` fires, so
    /// that a test orders the answer by an event it has observed rather
    /// than by time.
    async fn serve_on_release(
        response: &'static [u8],
        path: &str,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}{path}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = socket.read(&mut [0; 4096]).await;
            let _ = release.await;
            let _ = socket.write_all(response).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        url
    }

    /// Messages and outcome of decoding `body` split at `cuts`, with a
    /// small limit so that the error path is reached too.
    fn decode_chunked(
        sse: bool,
        body: &[u8],
        cuts: &[u16],
    ) -> (Vec<Vec<u8>>, Result<(), TransportFailure>) {
        let mut decoder = Decoder::new(sse, 256);
        let mut messages = VecDeque::new();
        let mut rest = body;
        for cut in cuts {
            let (chunk, after) = rest.split_at(usize::from(*cut).min(rest.len()));
            rest = after;
            if let Err(error) = decoder.push(chunk, &mut messages) {
                return (messages.into(), Err(error));
            }
        }
        // Whatever the cuts left, however long.
        let result = decoder.push(rest, &mut messages);
        (messages.into(), result)
    }

    /// Fuzz target (Block 14e): any body, split anywhere, decodes to the
    /// same messages and outcome as in one chunk, and never panics.
    /// Coverage-guided with `cargo bolero test
    /// adapter::http::tests::fuzz_http_stream_decoder --engine libfuzzer`.
    /// What a Protobuf stream splitter of a small limit gives for `body`
    /// cut at `cuts`.
    fn split_chunked(body: &[u8], cuts: &[u16]) -> (Vec<Vec<u8>>, Result<(), TransportFailure>) {
        let mut decoder = Decoder::protobuf(256);
        let mut messages = VecDeque::new();
        let mut rest = body;
        for cut in cuts {
            let (chunk, after) = rest.split_at(usize::from(*cut).min(rest.len()));
            rest = after;
            if let Err(error) = decoder.push(chunk, &mut messages) {
                return (messages.into(), Err(error));
            }
        }
        let result = decoder.push(rest, &mut messages);
        (messages.into(), result)
    }

    // Block 27, D3: the in-house splitter, fuzzed as Block 0 requires.
    #[test]
    fn fuzz_protobuf_stream_decoder() {
        bolero::check!()
            .with_type::<(Vec<u8>, Vec<u16>)>()
            .for_each(|(body, cuts)| {
                let split = split_chunked(body, cuts);
                assert_eq!(split, split_chunked(body, &[]));
                // Each message is one reply with its prefix, within the
                // limit.
                for message in &split.0 {
                    let mut bytes = &message[..];
                    let length = prost::decode_length_delimiter(&mut bytes).expect("a prefix");
                    assert_eq!(bytes.len(), length);
                    assert!(length <= 256);
                }
            });
    }

    #[test]
    fn fuzz_http_stream_decoder() {
        bolero::check!()
            .with_type::<(Vec<u8>, Vec<u16>)>()
            .for_each(|(body, cuts)| {
                assert_eq!(
                    decode_chunked(false, body, cuts),
                    decode_chunked(false, body, &[])
                );
            });
    }

    /// As [`fuzz_http_stream_decoder`], for sse.
    #[test]
    fn fuzz_sse_decoder() {
        bolero::check!()
            .with_type::<(Vec<u8>, Vec<u16>)>()
            .for_each(|(body, cuts)| {
                assert_eq!(
                    decode_chunked(true, body, cuts),
                    decode_chunked(true, body, &[])
                );
            });
    }

    fn decode(sse: bool, chunks: &[&[u8]]) -> Vec<String> {
        let mut decoder = Decoder::new(sse, LIMIT);
        let mut messages = VecDeque::new();
        for chunk in chunks {
            decoder.push(chunk, &mut messages).unwrap();
        }
        messages
            .into_iter()
            .map(|message| String::from_utf8(message).unwrap())
            .collect()
    }

    #[test]
    fn emulation_requests_go_to_the_stream_server_unless_configured() {
        let stream = "https://example.com:8443/connection/sse?cf_connect=x#f";

        let default = emulation_url::<Url>(stream, None).unwrap();
        let configured =
            emulation_url::<Url>(stream, Some("https://example.com/centrifugo/emulation")).unwrap();

        assert_eq!(default.as_str(), "https://example.com:8443/emulation");
        assert_eq!(
            configured.as_str(),
            "https://example.com/centrifugo/emulation"
        );
        assert!(emulation_url::<Url>(stream, Some("not a url")).is_err());
    }

    #[tokio::test]
    async fn a_failed_sse_request_does_not_reveal_the_connect_command() {
        let connect = br#"{"id":1,"connect":{"token":"secret-token"}}"#.to_vec();

        let result = HttpTransport::open(
            TransportKind::Sse,
            "http://127.0.0.1:1/connection/sse",
            connect,
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;

        let Err(message) = result else {
            panic!("connected to a closed port");
        };
        assert!(!message.message.contains("secret"), "{message}");
    }

    #[test]
    fn http_stream_lines_split_over_chunks_are_joined() {
        let messages = decode(false, &[b"{\"id\":1,\"con", b"nect\":{}}\n{", b"}\n"]);

        assert_eq!(messages, [r#"{"id":1,"connect":{}}"#, "{}"]);
    }

    #[test]
    fn several_http_stream_lines_in_one_chunk_are_split_and_blank_ones_skipped() {
        let messages = decode(false, &[b"{}\r\n\n  \n{\"push\":{}}\n{\"id\""]);

        assert_eq!(messages, ["{}", r#"{"push":{}}"#]);
    }

    #[test]
    fn sse_events_split_over_chunks_are_joined() {
        let messages = decode(true, &[b"\ndata: {\"id\"", b":1}\n", b"\r\ndata: {}\n\n"]);

        assert_eq!(messages, [r#"{"id":1}"#, "{}"]);
    }

    #[test]
    fn sse_lines_may_end_with_cr_lf_or_crlf_across_chunks() {
        let messages = decode(
            true,
            &[b"data: {\"a\":1}\r", b"\rdata: {}\r", b"\n\r\ndata: 2\n\n"],
        );

        assert_eq!(messages, [r#"{"a":1}"#, "{}", "2"]);
    }

    #[test]
    fn a_leading_byte_order_mark_is_ignored_once() {
        let messages = decode(
            true,
            &[b"\xEF\xBB", b"\xBFdata: {}\n\n\xEF\xBB\xBFdata: x\n\n"],
        );

        // A later mark is part of the line: the field is unknown.
        assert_eq!(messages, ["{}"]);
    }

    /// The messages `chunks` give a Protobuf stream splitter of `limit`.
    fn split_protobuf(
        chunks: &[&[u8]],
        limit: usize,
    ) -> (Vec<Vec<u8>>, Result<(), TransportFailure>) {
        let mut decoder = Decoder::protobuf(limit);
        let mut messages = VecDeque::new();
        for chunk in chunks {
            if let Err(error) = decoder.push(chunk, &mut messages) {
                return (messages.into(), Err(error));
            }
        }
        (messages.into(), Ok(()))
    }

    // Block 27, D3.
    #[test]
    fn protobuf_replies_split_over_chunks_inside_their_prefix_too() {
        // A 300-byte reply: its length takes two bytes, 0xac 0x02.
        let reply: Vec<u8> = [0xac, 0x02].into_iter().chain([7; 300]).collect();

        let (messages, result) = split_protobuf(
            &[&reply[..1], &reply[1..2], &reply[2..150], &reply[150..]],
            1000,
        );

        assert!(result.is_ok());
        assert_eq!(messages, [reply]);
    }

    // Block 27, D3: a zero length is the server's ping, never skipped.
    #[test]
    fn several_protobuf_replies_in_one_chunk_a_ping_among_them() {
        let (messages, result) =
            split_protobuf(&[&[0x01, 0x08, 0x00, 0x02, 0x08, 0x05, 0x03]], 1000);

        assert!(result.is_ok());
        assert_eq!(
            messages,
            [vec![0x01, 0x08], vec![0x00], vec![0x02, 0x08, 0x05]]
        );
    }

    // Block 27, D3: the length is checked before its bytes arrive.
    #[test]
    fn a_protobuf_reply_over_the_limit_fails_before_its_bytes() {
        let (messages, result) = split_protobuf(&[&[0x01, 0x08, 0x09]], 8);

        assert_eq!(messages, [vec![0x01, 0x08]]);
        assert_eq!(result, Err(TransportFailure::too_large()));
    }

    // Block 27, D3: a prefix still continuing waits; one too long fails.
    #[test]
    fn a_protobuf_length_waits_for_its_bytes_and_fails_when_too_long() {
        let (waiting, pending) = split_protobuf(&[&[0x80; 9]], usize::MAX);
        let (failed, too_long) = split_protobuf(&[&[0x80; 9], &[0x80]], usize::MAX);

        assert!(waiting.is_empty() && pending.is_ok());
        assert!(failed.is_empty() && too_long.is_err());
    }

    /// Serves every request: an endless stream to the stream request, 204
    /// to the others; sends each request's head.
    async fn capture_requests() -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
        capture_requests_setting("", "").await
    }

    /// As [`capture_requests`], the stream response and the emulation
    /// responses carrying the header lines `stream` and `emulation`, each
    /// ending with CRLF, or empty.
    async fn capture_requests_setting(
        stream: &'static str,
        emulation: &'static str,
    ) -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/connection/http_stream",
            listener.local_addr().unwrap()
        );
        let (heads, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let heads = heads.clone();
                tokio::spawn(async move {
                    // Requests in turn on one connection, as a pooled
                    // client sends them.
                    let mut pending = Vec::new();
                    let mut buffer = [0; 4096];
                    loop {
                        let end = loop {
                            if let Some(at) =
                                pending.windows(4).position(|window| window == b"\r\n\r\n")
                            {
                                break at + 4;
                            }
                            match socket.read(&mut buffer).await {
                                Ok(0) | Err(_) => return,
                                Ok(read) => pending.extend_from_slice(&buffer[..read]),
                            }
                        };
                        let head = String::from_utf8_lossy(&pending[..end]).to_lowercase();
                        let length: usize = head
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .and_then(|value| value.trim().parse().ok())
                            .unwrap_or(0);
                        while pending.len() < end + length {
                            match socket.read(&mut buffer).await {
                                Ok(0) | Err(_) => return,
                                Ok(read) => pending.extend_from_slice(&buffer[..read]),
                            }
                        }
                        pending.drain(..end + length);
                        let stream_request = head.starts_with("post /connection");
                        let answer = if stream_request {
                            format!("HTTP/1.1 200 OK\r\n{stream}transfer-encoding: chunked\r\n\r\n")
                        } else {
                            format!(
                                "HTTP/1.1 204 No Content\r\n{emulation}content-length: 0\r\n\r\n"
                            )
                        };
                        let _ = heads.send(head);
                        let _ = socket.write_all(answer.as_bytes()).await;
                        if stream_request {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            return;
                        }
                    }
                });
            }
        });
        (url, received)
    }

    // Block 27, D3: Centrifugo compares the media type exactly.
    #[tokio::test]
    async fn requests_name_their_encoding_exactly() {
        for (protobuf, media_type) in [(false, JSON), (true, PROTOBUF)] {
            let (url, mut heads) = capture_requests().await;
            let mut transport = HttpTransport::open(
                TransportKind::HttpStream,
                &url,
                b"connect".to_vec(),
                None,
                TIMEOUT,
                HeaderSet::default(),
                &Network::default(),
                protobuf,
                LIMIT,
            )
            .await
            .expect("an open stream");
            let stream = heads.recv().await.expect("the stream request");

            transport.send(FrameId::for_tests(1), b"command".to_vec());
            let emulation = tokio::select! {
                head = heads.recv() => head.expect("the emulation request"),
                _ = transport.recv(true) => panic!("the transport ended"),
            };

            for head in [&stream, &emulation] {
                assert!(
                    head.contains(&format!("content-type: {media_type}\r\n")),
                    "{head}"
                );
            }
            assert!(
                stream.contains(&format!("accept: {media_type}\r\n")),
                "{stream}"
            );
        }
    }

    /// A network with a memory cookie jar, and the jar.
    #[cfg(feature = "cookies")]
    fn with_a_jar() -> (Network, Arc<crate::MemoryJar>) {
        let jar = Arc::new(crate::MemoryJar::new());
        let shared: Arc<dyn crate::CookieJar> = Arc::clone(&jar) as _;
        (Network::default().with_cookie_jar(Jar(Some(shared))), jar)
    }

    // Block 43, D2 and D3: the stream request carries the jar's cookies, and
    // the cookies its response and each emulation response set ride on the
    // emulation requests after them.
    #[cfg(feature = "cookies")]
    #[tokio::test]
    async fn the_stream_and_emulation_requests_carry_and_keep_cookies() {
        use crate::CookieJar as _;

        let (url, mut heads) = capture_requests_setting(
            "set-cookie: session=s1; Path=/\r\n",
            "set-cookie: lb=e1; Path=/\r\n",
        )
        .await;
        let (network, jar) = with_a_jar();
        let seed = reqwest::header::HeaderValue::from_static("seed=j1; Path=/");
        jar.set_cookies(&mut std::iter::once(&seed), &Url::parse(&url).unwrap());
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &url,
            b"connect".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &network,
            false,
            LIMIT,
        )
        .await
        .expect("an open stream");
        let stream = heads.recv().await.expect("the stream request");
        let mut emulation = Vec::new();
        for n in 1..=2 {
            transport.send(FrameId::for_tests(n), b"command".to_vec());
            // Answered, its cookie stored, before the next request is built.
            loop {
                match tokio::time::timeout(Duration::from_secs(5), transport.recv(true)).await {
                    Ok(Inbound::Written(id)) if id == FrameId::for_tests(n) => break,
                    Ok(Inbound::Written(_)) => {}
                    other => panic!("not written: {other:?}"),
                }
            }
            emulation.push(heads.recv().await.expect("an emulation request"));
        }

        assert_eq!(cookie_pairs(&stream), ["seed=j1"], "{stream}");
        assert_eq!(
            cookie_pairs(&emulation[0]),
            ["seed=j1", "session=s1"],
            "{}",
            emulation[0]
        );
        assert_eq!(
            cookie_pairs(&emulation[1]),
            ["lb=e1", "seed=j1", "session=s1"],
            "{}",
            emulation[1]
        );
    }

    /// The pairs of the one Cookie header of a request `head`, sorted: the
    /// jar orders its cookies as it pleases (Block 43, out of scope).
    #[cfg(feature = "cookies")]
    fn cookie_pairs(head: &str) -> Vec<String> {
        let lines: Vec<&str> = head
            .lines()
            .filter_map(|line| line.strip_prefix("cookie: "))
            .collect();
        assert_eq!(lines.len(), 1, "one Cookie header: {head}");
        let mut pairs: Vec<String> = lines[0].split("; ").map(str::to_owned).collect();
        pairs.sort();
        pairs
    }

    // Block 43, D5: a proxy's 407 sets no cookie for the server.
    #[cfg(feature = "cookies")]
    #[tokio::test]
    async fn a_407_stream_response_sets_no_cookie() {
        use crate::CookieJar as _;

        let url = serve_once(
            b"HTTP/1.1 407 Proxy Authentication Required\r\nset-cookie: p=x; Path=/\r\ncontent-length: 0\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        let (network, jar) = with_a_jar();
        let result = HttpTransport::open(
            TransportKind::HttpStream,
            &url,
            b"connect".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &network,
            false,
            LIMIT,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(jar.cookies(&Url::parse(&url).unwrap()), None);
    }

    // Block 27, D1 and D5: what the maintained sse decoder changes.
    #[test]
    fn sse_data_that_is_not_utf8_arrives_with_replacement_characters() {
        let mut decoder = Decoder::new(true, 256);
        let mut messages = VecDeque::new();

        decoder
            .push(b"data: {\"a\":\"\xff\"}\n\n", &mut messages)
            .expect("an event");

        assert_eq!(messages, ["{\"a\":\"\u{fffd}\"}".as_bytes().to_vec()]);
    }

    // Block 27, D1 and D5: comment and unknown-field lines are skipped
    // without being buffered.
    #[test]
    fn a_long_sse_comment_or_unknown_field_does_not_end_the_transport() {
        let mut decoder = Decoder::new(true, 16);
        let mut messages = VecDeque::new();
        let long = "x".repeat(1000);

        let comment = decoder.push(format!(":{long}\n").as_bytes(), &mut messages);
        let unknown = decoder.push(format!("note: {long}\n").as_bytes(), &mut messages);
        let event = decoder.push(b"data: {}\n\n", &mut messages);

        assert!(comment.is_ok() && unknown.is_ok() && event.is_ok());
        assert_eq!(messages, [b"{}".to_vec()]);
    }

    // Block 27, D1 and D2: the bounds still hold.
    #[test]
    fn a_message_over_the_limit_still_ends_the_transport() {
        let long = "x".repeat(100);
        for (sse, body) in [
            (true, format!("data: {long}\n\n")),
            (false, format!("{long}\n")),
            // A line that never ends.
            (false, long.clone()),
        ] {
            let mut decoder = Decoder::new(sse, 16);
            let mut messages = VecDeque::new();

            let pushed = decoder.push(body.as_bytes(), &mut messages);

            assert!(pushed.is_err(), "sse {sse}: {body}");
            assert!(messages.is_empty());
        }
    }

    // Block 27, D4 and D5: RFC 9110 media types, parsed by mediatype.
    #[test]
    fn media_types_are_parsed_as_rfc_9110_writes_them() {
        assert!(is_event_stream("text/event-stream ; charset=utf-8"));
        assert!(is_event_stream("TEXT/Event-Stream"));
        // A parameter without a value, which RFC 9110 forbids.
        assert!(!is_event_stream("text/event-stream; charset"));
        assert!(captive_portal(200, "Text/HTML; charset=iso-8859-1"));
        assert!(!captive_portal(200, "not a media type"));
        // A structured suffix makes another media type (Codex, #127).
        assert!(!is_event_stream("text/event-stream+json"));
        assert!(!captive_portal(200, "text/html+json"));
    }

    #[test]
    fn the_sse_media_type_is_compared_without_case_or_parameters() {
        assert!(is_event_stream("text/event-stream"));
        assert!(is_event_stream(" Text/Event-Stream ; charset=utf-8"));
        assert!(!is_event_stream("text/event-streaming"));
        assert!(!is_event_stream("application/json"));
        assert!(!is_event_stream(""));
    }

    #[tokio::test]
    async fn a_redirected_emulation_request_ends_the_transport() {
        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        let redirecting = serve_once(
            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://example.com/emulation\r\ncontent-length: 0\r\n\r\n",
            "/emulation",
            Duration::ZERO,
        )
        .await;
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            Some(&redirecting),
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await
        .unwrap();
        transport.send(FrameId::for_tests(1), b"{}".to_vec());

        let inbound = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;

        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if reason.contains("307")),
            "{inbound:?}"
        );
    }

    #[tokio::test]
    async fn closing_lets_emulation_requests_in_flight_finish() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        // An emulation endpoint that notes the request it read, after a
        // delay that outlasts the close below.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let emulation = format!("http://{}/emulation", listener.local_addr().unwrap());
        let received = Arc::new(AtomicBool::new(false));
        let noted = Arc::clone(&received);
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let read = socket.read(&mut [0; 4096]).await.unwrap_or(0);
            noted.store(read > 0, Ordering::SeqCst);
            let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\n\r\n").await;
        });
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await
        .unwrap();
        transport.emulation_url = Url::parse(&emulation).unwrap();
        transport.send(FrameId::for_tests(2), b"{\"send\":{}}".to_vec());

        tokio::time::timeout(TIMEOUT, transport.close())
            .await
            .expect("closed within the request timeout");

        assert!(received.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_failed_emulation_request_after_a_successful_one_ends_the_transport() {
        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        // The failing one answers only once the test has seen the
        // successful one reported written: ordered by what the transport
        // received, not by what a server sent or by a delay.
        let ok = serve_once(
            b"HTTP/1.1 204 No Content\r\n\r\n",
            "/emulation",
            Duration::ZERO,
        )
        .await;
        let (release, released) = tokio::sync::oneshot::channel();
        let failing = serve_on_release(
            b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n",
            "/emulation",
            released,
        )
        .await;
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await
        .unwrap();
        transport.emulation_url = Url::parse(&ok).unwrap();
        transport.send(FrameId::for_tests(3), b"{}".to_vec());
        transport.emulation_url = Url::parse(&failing).unwrap();
        transport.send(FrameId::for_tests(4), b"{}".to_vec());

        // The stream stays silent: the answered request is reported
        // written, then only the failed one, answered after that, can end
        // it.
        let written = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;
        let _ = release.send(());
        let inbound = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;

        assert_eq!(written, Ok(Inbound::Written(FrameId::for_tests(3))));
        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if reason.contains("503")),
            "{inbound:?}"
        );
    }

    #[tokio::test]
    async fn a_rejected_emulation_request_keeps_the_transport() {
        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        // E.g. a body too large: only this command is rejected.
        let rejecting = serve_once(
            b"HTTP/1.1 413 Payload Too Large\r\ncontent-length: 0\r\n\r\n",
            "/emulation",
            Duration::ZERO,
        )
        .await;
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            Some(&rejecting),
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await
        .unwrap();
        transport.send(FrameId::for_tests(5), br#"{"id":2}"#.to_vec());

        let inbound = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;

        // Reported with its frame, not as the end of the transport.
        assert_eq!(inbound, Ok(Inbound::Rejected(br#"{"id":2}"#.to_vec())));
    }

    #[tokio::test]
    async fn closing_the_transport_aborts_its_emulation_requests() {
        use tokio::io::AsyncReadExt;

        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        // Reads the request, never answers it, and reports the connection
        // closed by the client.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hung = format!("http://{}/emulation", listener.local_addr().unwrap());
        let (aborted, request_aborted) = tokio::sync::oneshot::channel();
        let (read, request_read) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let mut read = Some(read);
            while socket.read(&mut buffer).await.is_ok_and(|bytes| bytes > 0) {
                if let Some(read) = read.take() {
                    let _ = read.send(());
                }
            }
            let _ = aborted.send(());
        });
        // Longer than the test: only the close can end the request.
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            Some(&hung),
            Duration::from_secs(60),
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await
        .unwrap();
        transport.send(FrameId::for_tests(6), b"{}".to_vec());
        // Until the server has read the request: the transport sends it
        // while it waits for its next message.
        let mut request_read = request_read;
        loop {
            tokio::select! {
                _ = transport.recv(true) => {}
                read = &mut request_read => break read.expect("the request read"),
            }
        }

        // How the driver closes an HTTP transport.
        drop(transport);

        let closed = tokio::time::timeout(TIMEOUT, request_aborted).await;
        assert!(matches!(closed, Ok(Ok(()))), "{closed:?}");
    }

    #[test]
    fn colonless_sse_fields_have_an_empty_value() {
        let messages = decode(
            true,
            &[b"event: notice\nevent\ndata: 1\n\ndata\n\ndata\ndata: 2\n\n"],
        );

        // A bare event resets the type; bare data alone dispatches nothing.
        assert_eq!(messages, ["1", "\n2"]);
    }

    #[test]
    fn only_sse_events_of_the_default_type_are_delivered() {
        let messages = decode(
            true,
            &[b"event: notice\ndata: 1\n\nevent: message\ndata: 2\n\ndata: 3\n\nevent:\ndata: 4\n\n"],
        );

        assert_eq!(messages, ["2", "3", "4"]);
    }

    /// Answers of a captive portal, and one that is not (Block 25, D8).
    const PORTAL_ANSWERS: [(&[u8], bool); 4] = [
        (
            b"HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: 13\r\n\r\n<html></html>",
            true,
        ),
        (
            b"HTTP/1.1 302 Found\r\nlocation: http://portal.example/\r\ncontent-length: 0\r\n\r\n",
            true,
        ),
        (
            b"HTTP/1.1 511 Network Authentication Required\r\ncontent-type: text/html\r\ncontent-length: 0\r\n\r\n",
            true,
        ),
        (
            b"HTTP/1.1 403 Forbidden\r\ncontent-type: text/html\r\ncontent-length: 0\r\n\r\n",
            false,
        ),
    ];

    #[tokio::test]
    async fn an_open_answered_by_a_web_page_names_a_captive_portal() {
        for kind in [TransportKind::HttpStream, TransportKind::Sse] {
            for (answer, portal) in PORTAL_ANSWERS {
                let url = serve_once(answer, "/connection/stream", Duration::ZERO).await;

                let result = HttpTransport::open(
                    kind,
                    &url,
                    b"{}".to_vec(),
                    None,
                    TIMEOUT,
                    HeaderSet::default(),
                    &Network::default(),
                    false,
                    LIMIT,
                )
                .await;

                let Err(message) = result else {
                    panic!("{kind:?} opened on {}", String::from_utf8_lossy(answer));
                };
                assert_eq!(
                    message.message.ends_with(CAPTIVE_PORTAL),
                    portal,
                    "{kind:?}: {message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_redirecting_endpoint_fails_the_attempt() {
        let url = serve_once(
            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://example.com/\r\ncontent-length: 0\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;

        let result = HttpTransport::open(
            TransportKind::HttpStream,
            &url,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;

        let Err(message) = result else {
            panic!("followed the redirect");
        };
        assert!(message.message.contains("307"), "{message}");
    }

    #[tokio::test]
    async fn an_sse_response_that_is_not_an_event_stream_fails_the_attempt() {
        let url = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
            "/connection/sse",
            Duration::ZERO,
        )
        .await;

        let result = HttpTransport::open(
            TransportKind::Sse,
            &url,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;

        let Err(message) = result else {
            panic!("accepted a JSON response as an event stream");
        };
        assert!(message.message.contains("not an event stream"), "{message}");
    }

    #[test]
    fn sse_comments_and_other_fields_are_ignored_and_data_lines_joined() {
        let messages = decode(
            true,
            &[b": keep-alive\n\nid: 7\ndata:{}\ndata: {}\nretry: 1\n\n"],
        );

        assert_eq!(messages, ["{}\n{}"]);
    }

    #[test]
    fn a_message_past_the_limit_fails_after_the_complete_ones() {
        for (sse, chunk) in [
            (false, &b"{}\n0123456789"[..]),
            (true, &b"data: {}\n\ndata: 0123\ndata: 456789"[..]),
        ] {
            let mut decoder = Decoder::new(sse, 8);
            let mut messages = VecDeque::new();

            let result = decoder.push(chunk, &mut messages);

            assert!(result.is_err(), "sse {sse}");
            assert_eq!(messages, [b"{}".to_vec()], "sse {sse}");
        }
    }

    /// A decoder's name, a new one, a message at its limit and one over it.
    type DecoderCase = (&'static str, fn() -> Decoder, Vec<u8>, Vec<u8>);

    // Block 29, D2: each decoder takes a message at the limit and refuses
    // one byte more.
    #[test]
    fn each_decoder_takes_a_message_at_the_limit_and_refuses_one_over() {
        let at = "x".repeat(16);
        let over = "x".repeat(17);
        let at_reply: Vec<u8> = [16].into_iter().chain([7; 16]).collect();
        let over_reply: Vec<u8> = [17].into_iter().chain([7; 17]).collect();
        let cases: [DecoderCase; 3] = [
            (
                "lines",
                || Decoder::new(false, 16),
                format!("{at}\n").into(),
                format!("{over}\n").into(),
            ),
            (
                "sse",
                || Decoder::new(true, 16),
                format!("data: {at}\n\n").into(),
                format!("data: {over}\n\n").into(),
            ),
            ("protobuf", || Decoder::protobuf(16), at_reply, over_reply),
        ];
        for (name, decoder, at, over) in cases {
            let mut taken = VecDeque::new();
            let mut refused = VecDeque::new();

            let at_limit = decoder().push(&at, &mut taken);
            let over_limit = decoder().push(&over, &mut refused);

            assert!(at_limit.is_ok() && taken.len() == 1, "{name}: {at_limit:?}");
            assert_eq!(over_limit, Err(TransportFailure::too_large()), "{name}");
            assert!(refused.is_empty(), "{name}");
        }
    }

    // Block 29, D3: a message over the limit ends the stream as too large.
    #[tokio::test]
    async fn a_stream_message_over_the_limit_is_reported_as_too_large() {
        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n13\r\n{\"a\":\"0123456789\"}\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            8,
        )
        .await
        .expect("an open stream");

        assert_eq!(transport.recv(true).await, Inbound::TooLarge);
    }

    #[test]
    fn an_unfinished_message_is_held_back() {
        assert_eq!(decode(false, &[b"{\"id\":1}"]), Vec::<String>::new());
        assert_eq!(decode(true, &[b"data: {}\n"]), Vec::<String>::new());
    }

    /// The message of the failure `answer` gives an open of `kind`, or,
    /// with `emulation`, the emulation request it answers.
    async fn failure_of(kind: TransportKind, answer: &'static [u8], emulation: bool) -> String {
        const STREAM: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n";
        let (stream, configured) = if emulation {
            let configured = serve_once(answer, "/emulation", Duration::ZERO).await;
            (
                serve_once(STREAM, "/connection/http_stream", Duration::ZERO).await,
                Some(configured),
            )
        } else {
            (
                serve_once(answer, "/connection/stream", Duration::ZERO).await,
                None,
            )
        };
        let opened = HttpTransport::open(
            kind,
            &stream,
            b"{}".to_vec(),
            configured.as_deref(),
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;
        if !emulation {
            let Err(failure) = opened else {
                panic!("opened on {}", String::from_utf8_lossy(answer));
            };
            return failure.to_string();
        }
        let mut transport = opened.expect("an open stream");
        transport.send(FrameId::for_tests(1), b"{}".to_vec());
        match tokio::time::timeout(TIMEOUT, transport.recv(true)).await {
            Ok(Inbound::Closed { reason, .. }) => reason,
            other => panic!("{other:?}"),
        }
    }

    // Block 61, D5: the failures typed, their messages are today's. Each
    // row pins the exact message of a representative failure.
    #[tokio::test]
    async fn the_messages_of_transport_failures_are_unchanged() {
        let over_limit = |mut decoder: Decoder, chunk: &[u8]| {
            decoder
                .push(chunk, &mut VecDeque::new())
                .unwrap_err()
                .to_string()
        };
        let bad_url = HttpTransport::open(
            TransportKind::HttpStream,
            "not a url",
            Vec::new(),
            None,
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;
        let bad_emulation_url = HttpTransport::open(
            TransportKind::HttpStream,
            "http://127.0.0.1:1/connection/http_stream",
            Vec::new(),
            Some("not a url"),
            TIMEOUT,
            HeaderSet::default(),
            &Network::default(),
            false,
            LIMIT,
        )
        .await;
        let rows: [(&str, String, &str); 5] = [
            (
                "a line over the limit",
                over_limit(Decoder::lines(4), b"12345\n"),
                "message larger than the size limit",
            ),
            (
                "a Protobuf reply over the limit",
                over_limit(Decoder::protobuf(4), &[5]),
                "message larger than the size limit",
            ),
            (
                "an sse event over the limit",
                over_limit(Decoder::new(true, 4), b"data: 12345\n\n"),
                "message larger than the size limit",
            ),
            (
                "an endpoint that is not a URL",
                bad_url.err().expect("a failure").to_string(),
                "relative URL without a base",
            ),
            (
                "an emulation endpoint that is not a URL",
                bad_emulation_url.err().expect("a failure").to_string(),
                "relative URL without a base",
            ),
        ];

        for (name, message, expected) in rows {
            assert_eq!(message, expected, "{name}");
        }
    }

    // Block 61, D5: as above, for the answers of a server.
    #[tokio::test]
    async fn the_messages_of_http_failures_are_unchanged() {
        let rows: [(&str, String, &str); 6] = [
            (
                "an open answered 404",
                failure_of(
                    TransportKind::HttpStream,
                    b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n",
                    false,
                )
                .await,
                "HTTP status 404 Not Found",
            ),
            (
                "an open answered by a web page",
                failure_of(
                    TransportKind::HttpStream,
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/html\r\ncontent-length: 0\r\n\r\n",
                    false,
                )
                .await,
                "HTTP status 200 OK, content type \"text/html\"; a captive portal may be answering",
            ),
            (
                "an open redirected",
                failure_of(
                    TransportKind::HttpStream,
                    b"HTTP/1.1 307 Temporary Redirect\r\nLocation: https://example.com/\r\ncontent-length: 0\r\n\r\n",
                    false,
                )
                .await,
                "HTTP status 307 Temporary Redirect; a captive portal may be answering",
            ),
            (
                "an sse open answered with JSON",
                failure_of(
                    TransportKind::Sse,
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
                    false,
                )
                .await,
                "not an event stream: HTTP status 200 OK, content type \"application/json\"",
            ),
            (
                "an open answered 407",
                failure_of(
                    TransportKind::HttpStream,
                    b"HTTP/1.1 407 Proxy Authentication Required\r\ncontent-length: 0\r\n\r\n",
                    false,
                )
                .await,
                "proxy authentication required",
            ),
            (
                "an emulation request answered 503",
                failure_of(
                    TransportKind::HttpStream,
                    b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n",
                    true,
                )
                .await,
                "emulation request failed: HTTP status 503 Service Unavailable",
            ),
        ];

        for (name, message, expected) in rows {
            assert_eq!(message, expected, "{name}");
        }
    }

    /// The set-up of Block 64's tests: an `http_stream` transport whose
    /// stream is served plain and whose emulation endpoint is the local TLS
    /// server, verified through the slow stub of the operating system's
    /// verifier, with requests allowed `timeout`. A command's connection is
    /// then the first TLS connection of its settings, so it cannot resume.
    async fn slow_command_connection(
        timeout: Duration,
    ) -> (HttpTransport, Arc<super::super::tls::tests::Slow>) {
        use super::super::net::Network;
        use super::super::proxy::Proxy;
        use super::super::tls::tests::{Slow, tls_server};
        use super::super::tls::{Tls, Trust};

        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        let emulation = format!("https://127.0.0.1:{}/emulation", tls_server().await);
        let slow = Arc::new(Slow::default());
        let network = Network::new(
            Tls::new(Trust::OffThread(Arc::clone(&slow) as _)),
            Proxy::none(),
        );
        let transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            Some(&emulation),
            timeout,
            HeaderSet::default(),
            &network,
            false,
            LIMIT,
        )
        .await
        .expect("an open stream");
        (transport, slow)
    }

    /// The bound of Block 64's failed request, and of another task's timer.
    const COMMAND_BOUND: Duration = Duration::from_millis(1500);

    /// On a current-thread runtime, a slow check of a command's new
    /// connection holds the runtime until the request's timeout only: the
    /// transport ends with the verification's timeout, and another task's
    /// timer fires within the same bound (Block 64, D1 and D2).
    #[tokio::test]
    async fn a_slow_check_of_a_commands_connection_ends_at_the_requests_timeout() {
        use super::super::tls::VERIFICATION_TIMEOUT;
        use std::time::Instant;

        let (mut transport, slow) = slow_command_connection(Duration::from_secs(1)).await;
        let start = Instant::now();
        let timer = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Instant::now()
        });

        transport.send(FrameId::for_tests(1), b"{}".to_vec());
        let inbound = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;
        let ended = start.elapsed();
        let ticked = timer.await.expect("the timer") - start;

        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if reason.contains(VERIFICATION_TIMEOUT)),
            "{inbound:?} after {ended:?}"
        );
        assert!(ended < COMMAND_BOUND, "{ended:?}");
        assert!(ticked < COMMAND_BOUND, "{ticked:?}");
        assert_eq!(slow.started().len(), 1);
    }

    /// The request a check outlasted fails as a timeout, whatever reqwest's
    /// error says (Block 64, D2; Block 61, D5).
    #[tokio::test]
    async fn a_command_request_past_its_deadline_fails_as_a_timeout() {
        use super::super::tls::VERIFICATION_TIMEOUT;

        let (mut transport, _slow) = slow_command_connection(Duration::from_secs(1)).await;

        transport.send(FrameId::for_tests(1), b"{}".to_vec());
        let (_, result) = tokio::time::timeout(TIMEOUT, transport.requests.next())
            .await
            .expect("the request ends")
            .expect("a request in flight");

        let failure = result.err().expect("a failed request");
        assert!(
            failure.message.contains(VERIFICATION_TIMEOUT),
            "{failure:?}"
        );
        assert_eq!(failure.kind, FailureKind::Timeout, "{failure:?}");
    }

    /// A request allowed longer than the check waits for it: its failure,
    /// the TLS server's closing, comes after the check, without the
    /// verification's timeout (Block 64, D1).
    #[tokio::test]
    async fn a_command_request_allowed_longer_than_the_check_waits_for_it() {
        use super::super::tls::VERIFICATION_TIMEOUT;
        use super::super::tls::tests::SLOW_CHECK;
        use std::time::Instant;

        let (mut transport, slow) = slow_command_connection(Duration::from_secs(3)).await;
        let start = Instant::now();

        transport.send(FrameId::for_tests(1), b"{}".to_vec());
        let inbound = tokio::time::timeout(TIMEOUT, transport.recv(true)).await;
        let ended = start.elapsed();

        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if !reason.contains(VERIFICATION_TIMEOUT)),
            "{inbound:?} after {ended:?}"
        );
        assert!(ended >= SLOW_CHECK, "{ended:?}");
        assert_eq!(slow.started().len(), 1);
    }
}
