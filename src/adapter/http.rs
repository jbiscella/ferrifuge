//! HTTP-streaming transports of the tokio adapter (Block 8): `http_stream`
//! and sse, with bidirectional emulation as in centrifuge-js
//! `transport_http_stream.ts` and `transport_sse.ts`. The response to the
//! request opening the stream carries the replies; commands go in requests
//! to the emulation endpoint of the same server.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

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
    http: reqwest::Client,
    emulation_url: Url,
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
    /// connect command, with the request.
    pub(crate) async fn open(
        kind: TransportKind,
        url: &str,
        initial_frame: Vec<u8>,
    ) -> Result<Self, String> {
        let mut url = Url::parse(url).map_err(|e| e.to_string())?;
        let http = http_client(url.scheme() == "https")?;
        let mut emulation_url = url.clone();
        emulation_url.set_path(EMULATION_PATH);
        emulation_url.set_query(None);
        emulation_url.set_fragment(None);
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
        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("HTTP status {status}"));
        }
        let body = response
            .bytes_stream()
            .map(|chunk| chunk.map(|bytes| bytes.to_vec()).map_err(|e| e.to_string()))
            .boxed();
        Ok(Self {
            http,
            emulation_url,
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
            .http
            .post(self.emulation_url.clone())
            .header(CONTENT_TYPE, JSON)
            .body(frame);
        self.requests.push(Box::pin(async move {
            let response = request
                .send()
                .await
                .map_err(|e| format!("emulation request failed: {e}"))?;
            let status = response.status();
            // A server without the session's node (404) or failing (5xx)
            // cannot answer the command; other statuses reject only it
            // (centrifuge-js `send`).
            if status == reqwest::StatusCode::NOT_FOUND || status.is_server_error() {
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
                Some(Err(reason)) = self.requests.next(), if !self.requests.is_empty() => {
                    return Inbound::Closed { code: None, reason };
                }
            }
        }
    }
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
            if byte == b'\n' {
                let mut line = std::mem::take(&mut self.line);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                self.end_line(line, messages);
            } else {
                self.line.push(byte);
            }
            let pending = self.line.len() + self.data.as_ref().map_or(0, Vec::len);
            if pending > self.limit {
                return Err(format!("message larger than {} bytes", self.limit));
            }
        }
        Ok(())
    }

    fn end_line(&mut self, line: Vec<u8>, messages: &mut VecDeque<Vec<u8>>) {
        if !self.sse {
            // Blank lines carry nothing, e.g. keep-alives of an intermediary
            // (centrifuge-js `decodeReplies`).
            if !line.iter().all(u8::is_ascii_whitespace) {
                messages.push_back(line);
            }
            return;
        }
        // Event stream format: a blank line ends an event; of the fields,
        // only data matters (comments, event, id and retry carry nothing).
        if line.is_empty() {
            messages.extend(self.data.take());
        } else if let Some(value) = line.strip_prefix(b"data:") {
            let value = value.strip_prefix(b" ").unwrap_or(value);
            match &mut self.data {
                Some(data) => {
                    data.push(b'\n');
                    data.extend_from_slice(value);
                }
                None => self.data = Some(value.to_vec()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sse_comments_and_other_fields_are_ignored_and_data_lines_joined() {
        let messages = decode(
            true,
            &[b": keep-alive\n\nevent: x\nid: 7\ndata:{}\ndata: {}\nretry: 1\n\n"],
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
