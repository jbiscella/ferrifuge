//! HTTP-streaming transports of the tokio adapter (Block 8): `http_stream`
//! and sse, with bidirectional emulation as in centrifuge-js
//! `transport_http_stream.ts` and `transport_sse.ts`. The response to the
//! request opening the stream carries the replies; commands go in requests
//! to the emulation endpoint of the same server.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::{BoxStream, FuturesUnordered};
use reqwest::Url;
use reqwest::header::{ACCEPT, CONTENT_TYPE};

use super::transport::{Inbound, plain_tls_config, tls_config};
use crate::machine::TransportKind;

/// Path of the emulation endpoint (centrifuge-js `emulationEndpoint`).
const EMULATION_PATH: &str = "/emulation";
const JSON: &str = "application/json";
/// Largest message accepted from a stream: the websocket transport's limit
/// (tungstenite's default `max_message_size`). A server sending more without
/// ending a message would otherwise grow the buffer without bound.
const MAX_MESSAGE: usize = 64 << 20;

/// An emulation request in flight; its failure ends the transport.
type Request = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// An open `http_stream` or sse connection.
pub(crate) struct HttpTransport {
    /// Client for the emulation endpoint, whose scheme may differ from the
    /// stream's when it is configured.
    emulation_http: reqwest::Client,
    emulation_url: Url,
    /// Time allowed for an emulation request, as for a command's reply.
    request_timeout: Duration,
    body: BoxStream<'static, Result<Vec<u8>, String>>,
    decoder: Decoder,
    /// Messages decoded but not received yet.
    frames: VecDeque<Vec<u8>>,
    /// Why the stream cannot go on, reported once `frames` are received.
    failed: Option<String>,
    requests: FuturesUnordered<Request>,
}

impl HttpTransport {
    /// Opens the stream at `url`, sending `initial_frame`, the encoded
    /// connect command, with the request. Commands then go to
    /// `emulation_endpoint`, by default `/emulation` on the stream's server.
    pub(crate) async fn open(
        kind: TransportKind,
        url: &str,
        initial_frame: Vec<u8>,
        emulation_endpoint: Option<&str>,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        let mut url = Url::parse(url).map_err(|e| e.to_string())?;
        let http = http_client(url.scheme() == "https")?;
        let emulation_url = emulation_url(&url, emulation_endpoint)?;
        let sse = kind == TransportKind::Sse;
        let request = if sse {
            url.query_pairs_mut()
                .append_pair("cf_connect", &String::from_utf8_lossy(&initial_frame));
            http.get(url).header(ACCEPT, "text/event-stream")
        } else {
            http.post(url)
                .header(ACCEPT, JSON)
                .header(CONTENT_TYPE, JSON)
                .body(initial_frame)
        };
        // Without the URL: the sse one carries the connect command, token
        // included, which must not reach error events.
        let response = request
            .send()
            .await
            .map_err(|e| e.without_url().to_string())?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("HTTP status {status}"));
        }
        // What EventSource accepts (WHATWG HTML, "Server-sent events").
        if sse {
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            if status != reqwest::StatusCode::OK || !is_event_stream(content_type) {
                return Err(format!(
                    "not an event stream: HTTP status {status}, content type {content_type:?}"
                ));
            }
        }
        let body = response
            .bytes_stream()
            .map(|chunk| {
                chunk
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| e.without_url().to_string())
            })
            .boxed();
        Ok(Self {
            emulation_http: http_client(emulation_url.scheme() == "https")?,
            emulation_url,
            request_timeout,
            body,
            decoder: Decoder::new(sse, MAX_MESSAGE),
            frames: VecDeque::new(),
            failed: None,
            requests: FuturesUnordered::new(),
        })
    }

    /// Sends `frame`, the body of an emulation request. A request that fails
    /// ends the transport when [`HttpTransport::recv`] reports it.
    pub(crate) fn send(&mut self, frame: Vec<u8>) {
        let request = self
            .emulation_http
            .post(self.emulation_url.clone())
            .header(CONTENT_TYPE, JSON)
            .body(frame);
        let timeout = self.request_timeout;
        self.requests.push(Box::pin(async move {
            let response = tokio::time::timeout(timeout, request.send())
                .await
                .map_err(|_| format!("emulation request timed out after {timeout:?}"))?
                .map_err(|e| format!("emulation request failed: {e}"))?;
            let status = response.status();
            // A server without the session's node (404) or failing (5xx)
            // cannot answer the command; other statuses reject only it
            // (centrifuge-js `send`). A redirect is not followed (see
            // `http_client`), so its command never arrived either.
            if status == reqwest::StatusCode::NOT_FOUND
                || status.is_server_error()
                || status.is_redirection()
            {
                Err(format!("emulation request failed: HTTP status {status}"))
            } else {
                Ok(())
            }
        }));
    }

    /// The next message from the server, or the end of the transport: the
    /// stream ended or failed, or an emulation request failed. Cancel-safe.
    pub(crate) async fn recv(&mut self) -> Inbound {
        loop {
            if let Some(frame) = self.frames.pop_front() {
                return Inbound::Frame(frame);
            }
            if let Some(reason) = self.failed.take() {
                return Inbound::Closed { code: None, reason };
            }
            tokio::select! {
                chunk = self.body.next() => match chunk {
                    Some(Ok(chunk)) => {
                        if let Err(reason) = self.decoder.push(&chunk, &mut self.frames) {
                            self.failed = Some(reason);
                        }
                    }
                    Some(Err(reason)) => return Inbound::Closed { code: None, reason },
                    None => {
                        return Inbound::Closed {
                            code: None,
                            reason: "stream ended".to_owned(),
                        };
                    }
                },
                // Irrefutable: a refutable pattern would disable this branch
                // after a success until the stream yields (tokio `select!`).
                Some(result) = self.requests.next(), if !self.requests.is_empty() => {
                    if let Err(reason) = result {
                        return Inbound::Closed { code: None, reason };
                    }
                }
            }
        }
    }
}

/// Whether `content_type` is the event-stream media type, whatever its case
/// and parameters.
fn is_event_stream(content_type: &str) -> bool {
    let essence = content_type.split(';').next().unwrap_or_default().trim();
    essence.eq_ignore_ascii_case("text/event-stream")
}

/// Where emulation requests go: `configured`, absolute, or `/emulation` on
/// the server of `stream` (centrifuge-js `emulationEndpoint`).
fn emulation_url(stream: &Url, configured: Option<&str>) -> Result<Url, String> {
    if let Some(endpoint) = configured {
        return Url::parse(endpoint).map_err(|e| e.to_string());
    }
    let mut url = stream.clone();
    url.set_path(EMULATION_PATH);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// The HTTP client for `https://` endpoints or for plain ones, built once
/// per process with the client's TLS configuration.
fn http_client(secure: bool) -> Result<reqwest::Client, String> {
    static SECURE: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    static PLAIN: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    let client = if secure { &SECURE } else { &PLAIN };
    client
        .get_or_init(|| {
            let tls = if secure {
                (*tls_config()?).clone()
            } else {
                plain_tls_config()
            };
            reqwest::Client::builder()
                .tls_backend_preconfigured(tls)
                // The TLS setup and the emulation endpoint follow from the
                // configured URL: a redirect elsewhere fails the attempt,
                // like an unreachable endpoint, rather than splitting the
                // stream and the commands between two servers.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())
        })
        .clone()
}

/// Splits a response body into the messages it carries: one JSON reply per
/// line for `http_stream`, the data of each event for sse. Network chunks
/// do not follow message boundaries, so a partial line waits for the next.
#[derive(Debug, Default)]
struct Decoder {
    sse: bool,
    /// Largest message, in bytes.
    limit: usize,
    /// The line being received.
    line: Vec<u8>,
    /// Data lines of the sse event being received, newline-separated.
    data: Option<Vec<u8>>,
    /// Type of the sse event being received, when named.
    event: Option<Vec<u8>>,
    /// The last byte was a CR ending an sse line.
    after_cr: bool,
    /// A line was ended already: the stream's first line may start with a
    /// byte order mark.
    past_first_line: bool,
}

impl Decoder {
    fn new(sse: bool, limit: usize) -> Self {
        Self {
            sse,
            limit,
            ..Self::default()
        }
    }

    /// Adds `chunk`, appending each message it completes to `messages`.
    ///
    /// # Errors
    ///
    /// When a message grows past the limit; the messages completed before
    /// it are appended still.
    fn push(&mut self, chunk: &[u8], messages: &mut VecDeque<Vec<u8>>) -> Result<(), String> {
        for &byte in chunk {
            // The LF of a CRLF whose CR ended an sse line already.
            let after_cr = std::mem::take(&mut self.after_cr);
            match byte {
                // An event stream may end lines with CR, LF or CRLF (WHATWG
                // HTML, "Parsing an event stream").
                b'\r' if self.sse => {
                    let line = std::mem::take(&mut self.line);
                    self.end_line(line, messages);
                    self.after_cr = true;
                }
                b'\n' if after_cr => {}
                b'\n' => {
                    let mut line = std::mem::take(&mut self.line);
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    self.end_line(line, messages);
                }
                _ => self.line.push(byte),
            }
            let pending = self.line.len() + self.data.as_ref().map_or(0, Vec::len);
            if pending > self.limit {
                return Err(format!("message larger than {} bytes", self.limit));
            }
        }
        Ok(())
    }

    fn end_line(&mut self, mut line: Vec<u8>, messages: &mut VecDeque<Vec<u8>>) {
        // One leading byte order mark is ignored (WHATWG HTML, "Parsing an
        // event stream").
        if self.sse && !std::mem::replace(&mut self.past_first_line, true) {
            line = line
                .strip_prefix(b"\xEF\xBB\xBF")
                .map_or(line.clone(), <[u8]>::to_vec);
        }
        if !self.sse {
            // Blank lines carry nothing, e.g. keep-alives of an intermediary
            // (centrifuge-js `decodeReplies`).
            if !line.iter().all(u8::is_ascii_whitespace) {
                messages.push_back(line);
            }
            return;
        }
        // Event stream format (WHATWG HTML, "Interpreting an event stream"):
        // a blank line ends an event; a line starting with a colon is a
        // comment; otherwise the field name ends at the first colon, and a
        // line without one is a field with an empty value. Of the fields,
        // data and event matter; id and retry carry nothing for the client.
        if line.is_empty() {
            let event = self.event.take();
            let data = self.data.take();
            // Only events of the default type, with data, reach the
            // reference transport (centrifuge-js `onmessage`, WHATWG
            // "dispatch the event").
            if event.is_none_or(|event| event.is_empty() || event == b"message")
                && let Some(data) = data.filter(|data| !data.is_empty())
            {
                messages.push_back(data);
            }
            return;
        }
        if line.first() == Some(&b':') {
            return;
        }
        let (field, value) = match line.iter().position(|&byte| byte == b':') {
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            }
            None => (&line[..], &[][..]),
        };
        match field {
            b"event" => self.event = Some(value.to_vec()),
            b"data" => match &mut self.data {
                Some(data) => {
                    data.push(b'\n');
                    data.extend_from_slice(value);
                }
                None => self.data = Some(value.to_vec()),
            },
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);

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

    fn decode(sse: bool, chunks: &[&[u8]]) -> Vec<String> {
        let mut decoder = Decoder::new(sse, MAX_MESSAGE);
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
        let stream = Url::parse("https://example.com:8443/connection/sse?cf_connect=x").unwrap();

        let default = emulation_url(&stream, None).unwrap();
        let configured =
            emulation_url(&stream, Some("https://example.com/centrifugo/emulation")).unwrap();

        assert_eq!(default.as_str(), "https://example.com:8443/emulation");
        assert_eq!(
            configured.as_str(),
            "https://example.com/centrifugo/emulation"
        );
        assert!(emulation_url(&stream, Some("not a url")).is_err());
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
        )
        .await;

        let Err(message) = result else {
            panic!("connected to a closed port");
        };
        assert!(!message.contains("secret"), "{message}");
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
        )
        .await
        .unwrap();
        transport.send(b"{}".to_vec());

        let inbound = tokio::time::timeout(TIMEOUT, transport.recv()).await;

        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if reason.contains("307")),
            "{inbound:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_emulation_request_after_a_successful_one_ends_the_transport() {
        let stream = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
            "/connection/http_stream",
            Duration::ZERO,
        )
        .await;
        let ok = serve_once(
            b"HTTP/1.1 204 No Content\r\n\r\n",
            "/emulation",
            Duration::ZERO,
        )
        .await;
        // Answers after the successful one.
        let failing = serve_once(
            b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n",
            "/emulation",
            Duration::from_millis(500),
        )
        .await;
        let mut transport = HttpTransport::open(
            TransportKind::HttpStream,
            &stream,
            b"{}".to_vec(),
            None,
            TIMEOUT,
        )
        .await
        .unwrap();
        transport.emulation_url = Url::parse(&ok).unwrap();
        transport.send(b"{}".to_vec());
        transport.emulation_url = Url::parse(&failing).unwrap();
        transport.send(b"{}".to_vec());

        // The stream stays silent: only the failed request can end it.
        let inbound = tokio::time::timeout(TIMEOUT, transport.recv()).await;

        assert!(
            matches!(&inbound, Ok(Inbound::Closed { reason, .. }) if reason.contains("503")),
            "{inbound:?}"
        );
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
        )
        .await;

        let Err(message) = result else {
            panic!("followed the redirect");
        };
        assert!(message.contains("307"), "{message}");
    }

    #[tokio::test]
    async fn an_sse_response_that_is_not_an_event_stream_fails_the_attempt() {
        let url = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
            "/connection/sse",
            Duration::ZERO,
        )
        .await;

        let result =
            HttpTransport::open(TransportKind::Sse, &url, b"{}".to_vec(), None, TIMEOUT).await;

        let Err(message) = result else {
            panic!("accepted a JSON response as an event stream");
        };
        assert!(message.contains("not an event stream"), "{message}");
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

    #[test]
    fn an_unfinished_message_is_held_back() {
        assert_eq!(decode(false, &[b"{\"id\":1}"]), Vec::<String>::new());
        assert_eq!(decode(true, &[b"data: {}\n"]), Vec::<String>::new());
    }
}
