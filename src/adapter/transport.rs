//! Websocket transport of the tokio adapter.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};

use crate::machine::{FrameId, FrameType};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// An open websocket connection to the server.
///
/// Writing never blocks the caller: frames are queued by [`WebSocket::send`]
/// and written while [`WebSocket::recv`] waits for the server, so a server
/// that stops reading cannot freeze the client (Block 8).
pub(crate) struct WebSocket {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Frames not yet handed to the stream, with their id and the time each
    /// was queued.
    queue: VecDeque<(FrameId, Message, Instant)>,
    /// The frame being written and when it was queued; none when no write
    /// is under way.
    writing: Option<(FrameId, Instant)>,
    /// Frames written and not yet reported by [`WebSocket::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a read comes first next.
    read_next: bool,
    /// Fires [`WebSocket::timeout`] after the oldest frame not yet written
    /// was queued.
    deadline: Pin<Box<Sleep>>,
    /// Bound on writing a frame, and on closing (`Config::timeout`).
    timeout: Duration,
}

/// What the server sent, as far as the client protocol is concerned.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Inbound {
    /// A frame of client protocol messages.
    Frame(Vec<u8>),
    /// The connection ended; `code` is absent when no close frame was received.
    Closed { code: Option<u16>, reason: String },
    /// An emulation request carrying this frame was rejected with HTTP 413:
    /// only its commands failed, the transport stays open.
    Rejected(Vec<u8>),
    /// The frame was written: flushed to the socket, or its emulation
    /// request answered.
    Written(FrameId),
}

impl WebSocket {
    /// Opens a connection to `url` (`ws://` or `wss://`), asking for
    /// `subprotocol` if any. A frame not written within `timeout` of being
    /// queued ends the connection, and closing it takes at most `timeout`.
    pub(crate) async fn open(
        url: &str,
        subprotocol: Option<&str>,
        timeout: Duration,
        headers: &reqwest::header::HeaderMap,
    ) -> Result<Self, String> {
        // Plain ws:// must work on hosts without platform certificates.
        let secure = url
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("wss://"));
        let connector = if secure {
            Connector::Rustls(tls_config()?)
        } else {
            Connector::Plain
        };
        let mut request = url.into_client_request().map_err(|e| e.to_string())?;
        // The application's, which leave out those the handshake sets.
        for (name, value) in headers {
            request.headers_mut().insert(name.clone(), value.clone());
        }
        if let Some(subprotocol) = subprotocol {
            let value = HeaderValue::from_str(subprotocol).map_err(|e| e.to_string())?;
            request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
        }
        // tungstenite fails the handshake unless the server accepts the
        // subprotocol asked for.
        let (stream, _) = tokio_tungstenite::connect_async_tls_with_config(
            request,
            Some(websocket_config()),
            true,
            Some(connector),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(Self {
            stream,
            queue: VecDeque::new(),
            writing: None,
            written: VecDeque::new(),
            read_next: false,
            deadline: Box::pin(tokio::time::sleep(timeout)),
            timeout,
        })
    }

    /// Queues frame `id` of encoded commands, to go out as a text or binary
    /// message while [`WebSocket::recv`] runs, which reports it once written.
    pub(crate) fn send(
        &mut self,
        id: FrameId,
        frame: Vec<u8>,
        frame_type: FrameType,
    ) -> Result<(), String> {
        let message = match frame_type {
            FrameType::Text => Message::text(String::from_utf8(frame).map_err(|e| e.to_string())?),
            FrameType::Binary => Message::binary(frame),
        };
        self.queue.push_back((id, message, Instant::now()));
        Ok(())
    }

    /// Writes the queued frames while waiting for the next frame, for a
    /// frame written, or for the end of the connection, which a failed
    /// write, or a frame not written in time, also is. Without `reading`, it only writes, until a write
    /// fails or misses its deadline: a full read-ahead stops the reading,
    /// never the writing (Block 15). Cancel-safe: dropping the future loses
    /// no message.
    pub(crate) async fn recv(&mut self, reading: bool) -> Inbound {
        std::future::poll_fn(|cx| self.poll_recv(cx, reading)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>, reading: bool) -> Poll<Inbound> {
        let lost = Inbound::Closed {
            code: None,
            reason: String::new(),
        };
        if let Poll::Ready(Err(_)) = self.poll_write(cx, true) {
            return Poll::Ready(lost);
        }
        if let Some(queued) = self.oldest_unwritten() {
            let deadline = queued + self.timeout;
            if self.deadline.deadline() != deadline {
                self.deadline.as_mut().reset(deadline);
            }
            if self.deadline.as_mut().poll(cx).is_ready() {
                return Poll::Ready(lost);
            }
        }
        // Written reports and reads take turns: with a write queue that is
        // always backlogged, each report admits another frame, which must
        // not keep the server's messages waiting.
        if !self.read_next
            && let Some(id) = self.written.pop_front()
        {
            self.read_next = true;
            return Poll::Ready(Inbound::Written(id));
        }
        if reading && let Poll::Ready(inbound) = self.poll_read(cx) {
            self.read_next = false;
            return Poll::Ready(inbound.unwrap_or(lost));
        }
        if let Some(id) = self.written.pop_front() {
            self.read_next = true;
            return Poll::Ready(Inbound::Written(id));
        }
        Poll::Pending
    }

    /// The next frame or close from the server; `None` when the connection
    /// was lost.
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Option<Inbound>> {
        loop {
            let message = match ready!(self.stream.poll_next_unpin(cx)) {
                Some(Ok(message)) => message,
                Some(Err(Error::Capacity(_))) => {
                    return Poll::Ready(Some(Inbound::Closed {
                        code: None,
                        reason: TOO_LARGE.to_owned(),
                    }));
                }
                _ => return Poll::Ready(None),
            };
            match message {
                Message::Text(text) => {
                    return Poll::Ready(Some(Inbound::Frame(text.as_bytes().to_vec())));
                }
                Message::Binary(bytes) => {
                    return Poll::Ready(Some(Inbound::Frame(bytes.to_vec())));
                }
                Message::Close(frame) => return Poll::Ready(Some(closed(frame))),
                // Websocket-level keepalive is answered by tungstenite itself.
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    /// Writes the queued frames one at a time, each flushed before the next
    /// is handed to the stream, so that a written frame is one the network
    /// took. Ready once every queued frame is written. With `on_time`, a
    /// frame queued longer than the timeout is never written: the write
    /// fails instead, even if the socket could take it now.
    fn poll_write(&mut self, cx: &mut Context<'_>, on_time: bool) -> Poll<Result<(), Error>> {
        loop {
            if on_time
                && self
                    .oldest_unwritten()
                    .is_some_and(|queued| queued.elapsed() >= self.timeout)
            {
                return Poll::Ready(Err(Error::Io(std::io::ErrorKind::TimedOut.into())));
            }
            if self.writing.is_some() {
                let flushed = self.stream.poll_flush_unpin(cx);
                note_stall(&flushed);
                ready!(flushed)?;
                if let Some((id, _)) = self.writing.take() {
                    self.written.push_back(id);
                }
                continue;
            }
            if self.queue.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let ready = self.stream.poll_ready_unpin(cx);
            note_stall(&ready);
            ready!(ready)?;
            let (id, message, queued) = self.queue.pop_front().expect("the queue is not empty");
            self.stream.start_send_unpin(message)?;
            self.writing = Some((id, queued));
        }
    }

    /// Writes the queued frames, then closes the connection, within the
    /// timeout; past it, the connection is aborted. Failures are ignored: the
    /// connection is abandoned either way.
    pub(crate) async fn close(mut self) {
        let timeout = self.timeout;
        let closing = tokio::time::timeout(timeout, async {
            std::future::poll_fn(|cx| self.poll_write(cx, false)).await?;
            self.stream.close(None).await
        })
        .await;
        if closing.is_err() {
            self.abort();
        }
    }

    /// When the oldest frame not yet written was queued.
    fn oldest_unwritten(&self) -> Option<Instant> {
        self.writing
            .map(|(_, queued)| queued)
            .or(self.queue.front().map(|(_, _, queued)| *queued))
    }

    /// Drops the connection with a TCP reset, discarding the data the server
    /// never read, rather than leaving the system to keep trying to send it.
    fn abort(self) {
        let tcp = match self.stream.get_ref() {
            MaybeTlsStream::Plain(tcp) => Some(tcp),
            MaybeTlsStream::Rustls(tls) => Some(tls.get_ref().0),
            _ => None,
        };
        if let Some(tcp) = tcp {
            let _ = tcp.set_zero_linger();
        }
    }
}

/// Writes that found a connection unable to take more, across the process.
#[cfg(feature = "test-util")]
static STALLED_WRITES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many times so far in this process a websocket write found the
/// connection unable to take more, so that tests can wait for a stalled
/// write instead of guessing how much the system buffers. Only with the
/// `test-util` feature.
#[cfg(feature = "test-util")]
pub fn stalled_websocket_writes() -> usize {
    STALLED_WRITES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Counts a write that `poll` shows stalled, for [`stalled_websocket_writes`].
#[cfg(feature = "test-util")]
fn note_stall<T>(poll: &Poll<T>) {
    if poll.is_pending() {
        STALLED_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(not(feature = "test-util"))]
fn note_stall<T>(_: &Poll<T>) {}

/// Limits of a websocket connection: a frame may be as large as a message
/// (64 MiB, tungstenite's default and the HTTP transports' limit), since
/// Centrifugo sends each reply, a recovery's included, as one frame;
/// tungstenite's default frame limit of 16 MiB would refuse a large one
/// and close the connection on every attempt.
fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE))
        .max_frame_size(Some(MAX_MESSAGE))
}

/// The reason of [`Inbound::Closed`] when the server sent a message larger
/// than the size limit, on any transport (Block 21: a server anomaly).
pub(super) const TOO_LARGE: &str = "message larger than the size limit";

/// Largest message, and frame, accepted from the server.
const MAX_MESSAGE: usize = crate::config::MAX_MESSAGE_BYTES;

/// TLS settings of an HTTP client that only ever speaks plain `http://`:
/// no root certificates, so it works on hosts without platform ones, and
/// still the build's provider, as the HTTP client takes no default one.
pub(super) fn plain_tls_config() -> ClientConfig {
    ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("the provider supports the default protocol versions")
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth()
}

#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
compile_error!(
    "the tokio adapter needs a TLS crypto provider: enable the `ring` (default), `aws-lc-rs` or `fips` feature"
);

/// The crypto provider chosen by the build's features (Block 6). Features
/// are additive, so the strongest enabled one wins: fips > aws-lc-rs > ring.
#[cfg(feature = "fips")]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::default_fips_provider()
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "fips")))]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// TLS settings for `wss://` and `https://`, built once per process.
///
/// The crypto provider is set explicitly rather than taken from the process
/// default: when another crate also enables another rustls provider, rustls
/// cannot pick a default and panics.
pub(super) fn tls_config() -> Result<Arc<ClientConfig>, String> {
    tls_setup().map(|(config, _)| config)
}

/// [`tls_config`], and whether it is kept: a read of the platform's root
/// certificates that reported errors serves the attempt but is read again
/// by the next one (Block 6, review item C7).
pub(super) fn tls_setup() -> Result<(Arc<ClientConfig>, bool), String> {
    static CONFIG: BuildOnce<Arc<ClientConfig>> = BuildOnce::new();
    CONFIG.get(|| {
        let found = load_native_certs();
        roots_config(found.certs, found.errors.is_empty())
    })
}

/// The TLS settings trusting `certs`, kept when `complete`: read without
/// errors. Unreadable certificates are skipped, as browsers do.
fn roots_config(
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    complete: bool,
) -> Result<(Arc<ClientConfig>, bool), String> {
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err("no platform root certificates found".to_owned());
    }
    let config = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((Arc::new(config), complete))
}

/// A value built at first use and shared by the process; a build that
/// fails, or that its builder says not to keep, is not kept, so the next use
/// builds again (Block 6, review item C7).
pub(super) struct BuildOnce<T>(OnceLock<T>);

impl<T: Clone> BuildOnce<T> {
    pub(super) const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// The value, and whether it is kept: built by `build`, which also says
    /// whether to keep it, unless an earlier build was kept. Two first uses
    /// racing may both build; one value is kept.
    pub(super) fn get(
        &self,
        build: impl FnOnce() -> Result<(T, bool), String>,
    ) -> Result<(T, bool), String> {
        if let Some(value) = self.0.get() {
            return Ok((value.clone(), true));
        }
        let (value, keep) = build()?;
        if !keep {
            return Ok((value, false));
        }
        Ok((self.0.get_or_init(|| value).clone(), true))
    }
}

fn closed(frame: Option<CloseFrame>) -> Inbound {
    match frame {
        Some(frame) => Inbound::Closed {
            code: Some(frame.code.into()),
            reason: frame.reason.to_string(),
        },
        None => Inbound::Closed {
            code: None,
            reason: String::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderMap;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Starts a server that echoes one text message, then closes with `code`.
    async fn echo_then_close(code: u16, reason: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let message = ws.next().await.unwrap().unwrap();
            ws.send(message).await.unwrap();
            ws.close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            }))
            .await
            .unwrap();
        });
        format!("ws://{address}")
    }

    /// The next inbound that is not a written report.
    async fn next_inbound(ws: &mut WebSocket) -> Inbound {
        loop {
            match ws.recv(true).await {
                Inbound::Written(_) => {}
                inbound => return inbound,
            }
        }
    }

    #[tokio::test]
    async fn sends_and_receives_frames_then_reports_close_code() {
        let url = echo_then_close(3001, "shutdown").await;
        let mut ws = WebSocket::open(&url, None, TIMEOUT, &HeaderMap::new())
            .await
            .unwrap();

        ws.send(
            FrameId::for_tests(1),
            b"{\"id\":1}".to_vec(),
            FrameType::Text,
        )
        .unwrap();
        let written = ws.recv(true).await;
        let echoed = next_inbound(&mut ws).await;
        let closed = next_inbound(&mut ws).await;

        // Reported once flushed, before the server could answer it.
        assert_eq!(written, Inbound::Written(FrameId::for_tests(1)));
        assert_eq!(echoed, Inbound::Frame(b"{\"id\":1}".to_vec()));
        assert_eq!(
            closed,
            Inbound::Closed {
                code: Some(3001),
                reason: "shutdown".into()
            }
        );
    }

    #[tokio::test]
    async fn a_message_read_comes_before_the_next_written_report() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Echoes each message.
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                ws.send(message).await.unwrap();
            }
        });
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, TIMEOUT, &HeaderMap::new())
            .await
            .unwrap();
        ws.send(FrameId::for_tests(1), b"1".to_vec(), FrameType::Text)
            .unwrap();
        let first = ws.recv(true).await;
        // Its echo arrives; then a second frame is written at once.
        tokio::time::sleep(Duration::from_millis(100)).await;
        ws.send(FrameId::for_tests(2), b"2".to_vec(), FrameType::Text)
            .unwrap();

        let next = ws.recv(true).await;
        let after = ws.recv(true).await;

        assert_eq!(first, Inbound::Written(FrameId::for_tests(1)));
        assert_eq!(next, Inbound::Frame(b"1".to_vec()));
        assert_eq!(after, Inbound::Written(FrameId::for_tests(2)));
    }

    #[tokio::test]
    async fn frames_go_out_as_text_or_binary_messages_by_type() {
        // The server answers each message with its kind.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                let kind = match message {
                    Message::Text(_) => "text",
                    Message::Binary(_) => "binary",
                    _ => continue,
                };
                ws.send(Message::text(kind)).await.unwrap();
            }
        });
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, TIMEOUT, &HeaderMap::new())
            .await
            .unwrap();

        ws.send(FrameId::for_tests(2), b"{}".to_vec(), FrameType::Text)
            .unwrap();
        let text = next_inbound(&mut ws).await;
        ws.send(FrameId::for_tests(3), vec![0xff, 0x00], FrameType::Binary)
            .unwrap();
        let binary = next_inbound(&mut ws).await;

        assert_eq!(text, Inbound::Frame(b"text".to_vec()));
        assert_eq!(binary, Inbound::Frame(b"binary".to_vec()));
    }

    #[tokio::test]
    async fn a_close_past_the_timeout_resets_a_connection_the_server_stopped_reading() {
        // The server reads nothing until the client has given up closing.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (closed, read_now) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            read_now.await.unwrap();
            let mut buffer = vec![0; 1 << 16];
            loop {
                match ws.get_mut().read(&mut buffer).await {
                    Ok(0) => return Ok(()),
                    Ok(_) => {}
                    Err(error) => return Err(error.kind()),
                }
            }
        });
        let timeout = Duration::from_millis(200);
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, timeout, &HeaderMap::new())
            .await
            .unwrap();
        // Frames are queued until one is not written in time: how much the
        // system buffers varies (Windows took a single 32 MiB frame whole).
        let mut queued = 0;
        let stalled = loop {
            ws.send(FrameId::for_tests(4), vec![b'x'; 1 << 20], FrameType::Text)
                .unwrap();
            queued += 1 << 20;
            assert!(queued <= 1 << 30, "the system buffered 1 GiB");
            if let Ok(inbound) =
                tokio::time::timeout(Duration::from_millis(10), next_inbound(&mut ws)).await
            {
                break inbound;
            }
        };

        ws.close().await;
        closed.send(()).unwrap();
        let ended = tokio::time::timeout(TIMEOUT, server)
            .await
            .expect("the server's read ends")
            .unwrap();

        assert_eq!(
            stalled,
            Inbound::Closed {
                code: None,
                reason: String::new()
            }
        );
        // A plain close would deliver what the system had buffered, then
        // end the stream; a reset ends it with an error.
        assert_eq!(ended, Err(std::io::ErrorKind::ConnectionReset));
    }

    #[tokio::test]
    async fn a_frame_still_queued_past_the_timeout_is_not_written_late() {
        // The server reads everything and reports the first text message.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                if let Message::Text(text) = message {
                    return Some(text.to_string());
                }
            }
            None
        });
        let timeout = Duration::from_millis(100);
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, timeout, &HeaderMap::new())
            .await
            .unwrap();
        ws.send(FrameId::for_tests(5), b"late".to_vec(), FrameType::Text)
            .unwrap();
        // The client is not polled until the frame is overdue, while the
        // socket could take it at any time.
        tokio::time::sleep(timeout * 2).await;

        let overdue = tokio::time::timeout(TIMEOUT, next_inbound(&mut ws)).await;
        drop(ws);
        let received = server.await.unwrap();

        assert_eq!(
            overdue,
            Ok(Inbound::Closed {
                code: None,
                reason: String::new()
            })
        );
        assert_eq!(received, None);
    }

    // A recovery reply of thousands of publications comes as one frame:
    // tungstenite's default frame limit (16 MiB) closed the connection.
    #[tokio::test]
    async fn a_frame_larger_than_16_mib_is_received() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::text("x".repeat(20 << 20))).await.unwrap();
            let _ = ws.next().await;
        });
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, TIMEOUT, &HeaderMap::new())
            .await
            .unwrap();

        let received = ws.recv(true).await;

        assert!(
            matches!(&received, Inbound::Frame(data) if data.len() == 20 << 20),
            "{:?}",
            std::mem::discriminant(&received)
        );
    }

    #[tokio::test]
    async fn open_fails_on_refused_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result =
            WebSocket::open(&format!("ws://{address}"), None, TIMEOUT, &HeaderMap::new()).await;

        assert!(result.is_err());
    }

    // Review item C7: a transient failure (a CA bundle being replaced, no
    // file descriptor left) must not break every later secure connection.
    #[test]
    fn a_failed_tls_setup_is_retried() {
        let setup = BuildOnce::new();

        let failed = setup.get(|| Err("no platform root certificates found".to_owned()));
        let retried = setup.get(|| Ok((1, true)));

        assert!(failed.is_err());
        assert_eq!(retried, Ok((1, true)));
    }

    #[test]
    fn a_tls_setup_its_builder_does_not_keep_is_built_again() {
        let setup = BuildOnce::new();

        let first = setup.get(|| Ok((1, false)));
        let second = setup.get(|| Ok((2, true)));
        let third = setup.get(|| Ok((3, true)));

        assert_eq!(
            (first, second, third),
            (Ok((1, false)), Ok((2, true)), Ok((2, true)))
        );
    }

    #[test]
    fn root_certificates_read_with_errors_serve_the_attempt_but_are_not_kept() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .unwrap()
            .cert
            .der()
            .clone();

        let partial = roots_config(vec![certificate.clone()], false);
        let complete = roots_config(vec![certificate], true);

        assert!(matches!(partial, Ok((_, false))));
        assert!(matches!(complete, Ok((_, true))));
    }

    #[test]
    fn a_built_tls_setup_is_reused() {
        let setup = BuildOnce::new();
        let builds = std::cell::Cell::new(0);
        let build = || {
            builds.set(builds.get() + 1);
            Ok((builds.get(), true))
        };

        let first = setup.get(build);
        let second = setup.get(build);

        assert_eq!(
            (first, second, builds.get()),
            (Ok((1, true)), Ok((1, true)), 1)
        );
    }

    #[test]
    fn tls_config_builds_even_with_several_rustls_providers_enabled() {
        // Test builds enable aws-lc-rs too (through reqwest), which made the
        // process-default provider ambiguous and panic.
        let config = tls_config();

        assert!(config.is_ok(), "{config:?}");
    }

    /// The build's strongest provider feature selects the provider
    /// (Block 6): fips > aws-lc-rs > ring.
    #[test]
    fn tls_config_uses_the_build_provider() {
        let config = tls_config().expect("TLS configuration");

        let key_provider = format!("{:?}", config.crypto_provider().key_provider);

        let expected = if cfg!(feature = "aws-lc-rs") {
            "AwsLcRs"
        } else {
            "Ring"
        };
        assert_eq!(key_provider, expected);
        // rustls also checks TLS-level FIPS requirements here, beyond the
        // provider's primitives.
        assert_eq!(config.fips(), cfg!(feature = "fips"));
    }

    /// A dependency update must not switch the FIPS build to another
    /// module generation unnoticed (Block 6: AWS-LC-FIPS 4.x).
    #[cfg(feature = "fips")]
    #[test]
    fn fips_build_links_the_4x_fips_module() {
        // The FIPS module generation, not the AWS-LC release
        // (`awslc_version`), which aws-lc-rs documents as unfit for it.
        let generation = aws_lc_rs::fips_version();

        assert_eq!(generation, Some(4), "FIPS module generation");
        assert!(aws_lc_rs::try_fips_mode().is_ok(), "not in FIPS mode");
    }
}
