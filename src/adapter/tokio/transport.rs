//! Websocket transport of the tokio adapter.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, Sleep};

use super::head::HeadReader;
use super::net::Network;
use super::proxy::{Stream, dial};
use crate::machine::{FrameId, FrameType};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{TlsError, UrlError};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::stream::Mode;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// An open websocket connection to the server.
///
/// Writing never blocks the caller: frames are queued by [`WebSocket::send`]
/// and written while [`WebSocket::recv`] waits for the server, so a server
/// that stops reading cannot freeze the client (Block 8).
pub(crate) struct WebSocket {
    stream: WebSocketStream<HeadReader<MaybeTlsStream<Stream>>>,
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
    /// The server sent a message over the size limit, which was refused;
    /// the connection is to be dropped (Block 29, D3).
    TooLarge,
    /// An emulation request carrying this frame was rejected with HTTP 413:
    /// only its commands failed, the transport stays open.
    Rejected(Vec<u8>),
    /// The frame was written: flushed to the socket, or its emulation
    /// request answered.
    Written(FrameId),
}

/// The message of a failed upgrade, naming a captive portal when its page
/// answered instead of the server (Block 25, D8). The cookies a refused
/// upgrade's response sets are kept (Block 43, D2).
fn handshake_error(
    error: &tokio_tungstenite::tungstenite::Error,
    url: &str,
    network: &Network,
) -> String {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = error {
        network.store_cookies(url, response.status().as_u16(), response.headers());
    }
    let portal = match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            super::super::body::captive_portal(response.status().as_u16(), content_type)
        }
        _ => false,
    };
    super::super::body::with_portal(error.to_string(), portal)
}

/// `stream` with TLS to the server for a `wss://` `request`, as
/// tokio-tungstenite's `client_async_tls_with_config` would make it, with
/// `tls`, the client's settings (Blocks 6 and 22); as it is for `ws://`.
async fn secure(
    stream: Stream,
    request: &tokio_tungstenite::tungstenite::handshake::client::Request,
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
) -> Result<MaybeTlsStream<Stream>, Error> {
    match tokio_tungstenite::tungstenite::client::uri_mode(request.uri())? {
        Mode::Plain => Ok(MaybeTlsStream::Plain(stream)),
        Mode::Tls => {
            // Built before dialing for every wss:// endpoint; without it,
            // as tokio-tungstenite without a TLS connector (reviewer, #253).
            let config = tls.ok_or(Error::Url(UrlError::TlsFeatureNotEnabled))?;
            let host = request
                .uri()
                .host()
                .ok_or(Error::Url(UrlError::NoHostName))?;
            // rustls takes an IPv6 address without its brackets.
            let host = host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host);
            let name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|_| Error::Tls(TlsError::InvalidDnsName))?
                .to_owned();
            tokio_rustls::TlsConnector::from(config)
                .connect(name, stream)
                .await
                .map(MaybeTlsStream::Rustls)
                .map_err(Error::Io)
        }
    }
}

/// The websocket handshake of `request` over `stream`, the head of the
/// server's answer read whole first (Block 50b's finding: see `head`).
async fn upgrade<S: AsyncRead + AsyncWrite + Unpin>(
    request: tokio_tungstenite::tungstenite::handshake::client::Request,
    stream: S,
    max_message: usize,
) -> Result<
    (
        WebSocketStream<HeadReader<S>>,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    Error,
> {
    tokio_tungstenite::client_async_with_config(
        request,
        HeadReader::new(stream),
        Some(websocket_config(max_message)),
    )
    .await
}

impl WebSocket {
    /// Opens a connection to `url` (`ws://` or `wss://`), asking for
    /// `subprotocol` if any. A frame not written within `timeout` of being
    /// queued ends the connection, and closing it takes at most `timeout`.
    /// A message or frame over `max_message` bytes ends it too, reported as
    /// [`Inbound::TooLarge`].
    pub(crate) async fn open(
        url: &str,
        subprotocol: Option<&str>,
        timeout: Duration,
        headers: &reqwest::header::HeaderMap,
        network: &Network,
        max_message: usize,
    ) -> Result<Self, String> {
        // Plain ws:// must work on hosts without platform certificates.
        let secure_url = url
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("wss://"));
        let tls = if secure_url {
            Some(network.tls.config()?)
        } else {
            None
        };
        let parsed = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
        // The stream to the server's host: the application's connector's
        // (Block 37, D4), or through the proxy the setting decides (Block
        // 23, D3); TLS to the server comes after, over either.
        let stream = if let Some(connector) = network.connector() {
            Stream::Connected(super::connector::connect(connector, &parsed).await?)
        } else {
            let proxy = network.decide(&parsed)?;
            dial(&parsed, proxy.as_ref(), &network.tls).await?
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
        // With the application's Cookie, in one header (Block 43, D3).
        network.add_cookies(url, request.headers_mut());
        let stream = secure(stream, &request, tls)
            .await
            .map_err(|error| handshake_error(&error, url, network))?;
        // tungstenite fails the handshake unless the server accepts the
        // subprotocol asked for.
        let (stream, response) = upgrade(request, stream, max_message)
            .await
            .map_err(|error| handshake_error(&error, url, network))?;
        network.store_cookies(url, response.status().as_u16(), response.headers());
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
                Some(Err(Error::Capacity(_))) => return Poll::Ready(Some(Inbound::TooLarge)),
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
        let tcp = match self.stream.get_ref().get_ref() {
            MaybeTlsStream::Plain(stream) => stream.tcp(),
            MaybeTlsStream::Rustls(tls) => tls.get_ref().0.tcp(),
            _ => None,
        };
        if let Some(tcp) = tcp {
            let _ = tcp.set_zero_linger();
        }
    }
}

/// Writes that found a connection unable to take more, across the process.
#[cfg(feature = "__test-hooks")]
static STALLED_WRITES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many times so far in this process a websocket write found the
/// connection unable to take more, so that tests can wait for a stalled
/// write instead of guessing how much the system buffers. Only with the
/// `__test-hooks` feature.
#[cfg(feature = "__test-hooks")]
pub fn stalled_websocket_writes() -> usize {
    STALLED_WRITES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Counts a write that `poll` shows stalled, for [`stalled_websocket_writes`].
#[cfg(feature = "__test-hooks")]
fn note_stall<T>(poll: &Poll<T>) {
    if poll.is_pending() {
        STALLED_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(not(feature = "__test-hooks"))]
fn note_stall<T>(_: &Poll<T>) {}

/// Limits of a websocket connection: a frame may be as large as a message
/// (`Config::max_message_bytes`, also the HTTP transports' limit), since
/// Centrifugo sends each reply, a recovery's included, as one frame;
/// tungstenite's default frame limit of 16 MiB would refuse a large one
/// and close the connection on every attempt.
fn websocket_config(max_message: usize) -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(max_message))
        .max_frame_size(Some(max_message))
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
    const LIMIT: usize = 64 << 20;

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
        let mut ws = WebSocket::open(
            &url,
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
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

    /// A server sending one text message of each length in `lengths`.
    async fn sending(lengths: &'static [usize]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for length in lengths {
                let _ = ws.send(Message::text("x".repeat(*length))).await;
            }
            // Kept open: only the limit ends the connection.
            let _ = ws.next().await;
        });
        format!("ws://{address}")
    }

    // Block 29, D2, D3: a message at the limit is read, one over it is
    // refused and reported as too large.
    #[tokio::test]
    async fn a_message_over_the_limit_is_refused_and_one_at_it_read() {
        let url = sending(&[16, 17]).await;
        let mut ws = WebSocket::open(
            &url,
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            16,
        )
        .await
        .unwrap();

        let at = next_inbound(&mut ws).await;
        let over = next_inbound(&mut ws).await;

        assert_eq!(at, Inbound::Frame(vec![b'x'; 16]));
        assert_eq!(over, Inbound::TooLarge);
    }

    #[tokio::test]
    async fn a_message_read_comes_before_the_next_written_report() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Echoes each message, telling when the first echo was sent.
        let (echoed, first_echoed) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let mut echoed = Some(echoed);
            while let Some(Ok(message)) = ws.next().await {
                ws.send(message).await.unwrap();
                if let Some(echoed) = echoed.take() {
                    let _ = echoed.send(());
                }
            }
        });
        let mut ws = WebSocket::open(
            &format!("ws://{address}"),
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
        .await
        .unwrap();
        ws.send(FrameId::for_tests(1), b"1".to_vec(), FrameType::Text)
            .unwrap();
        let first = ws.recv(true).await;
        // Its echo is sent, and this end's reactor has seen it arrive: a
        // second frame is written then, at once.
        first_echoed.await.expect("the first echo");
        let MaybeTlsStream::Plain(stream) = ws.stream.get_ref().get_ref() else {
            unreachable!("a ws:// connection is plain");
        };
        let tcp = stream.tcp().expect("a direct connection");
        tokio::time::timeout(TIMEOUT, tcp.readable())
            .await
            .expect("the first echo readable here")
            .unwrap();
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
        let mut ws = WebSocket::open(
            &format!("ws://{address}"),
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
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
        let mut ws = WebSocket::open(
            &format!("ws://{address}"),
            None,
            timeout,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
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
        let mut ws = WebSocket::open(
            &format!("ws://{address}"),
            None,
            timeout,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
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
        let mut ws = WebSocket::open(
            &format!("ws://{address}"),
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
        .await
        .unwrap();

        let received = ws.recv(true).await;

        assert!(
            matches!(&received, Inbound::Frame(data) if data.len() == 20 << 20),
            "{:?}",
            std::mem::discriminant(&received)
        );
    }

    /// Answers the next connection's upgrade with `answer`, raw.
    async fn answer_upgrade(answer: &'static [u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = socket.read(&mut [0; 4096]).await;
            let _ = socket.write_all(answer).await;
            let _ = socket.shutdown().await;
        });
        format!("ws://{address}/connection/websocket")
    }

    // Block 25, D8.
    #[tokio::test]
    async fn an_upgrade_answered_by_a_web_page_names_a_captive_portal() {
        let answers: [(&'static [u8], bool); 4] = [
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
        for (answer, portal) in answers {
            let url = answer_upgrade(answer).await;

            let result = WebSocket::open(
                &url,
                None,
                TIMEOUT,
                &HeaderMap::new(),
                &Network::default(),
                LIMIT,
            )
            .await;

            let Err(message) = result else {
                panic!("upgraded on {}", String::from_utf8_lossy(answer));
            };
            assert_eq!(
                message.ends_with(crate::adapter::body::CAPTIVE_PORTAL),
                portal,
                "{message}"
            );
        }
    }

    /// A stream that reads at most `.1` bytes at a time, as from a network
    /// that delivers the bytes in segments that small.
    struct SmallReads(tokio::io::DuplexStream, usize);

    impl AsyncRead for SmallReads {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let mut small = vec![0; self.1.min(buf.remaining())];
            let mut read = tokio::io::ReadBuf::new(&mut small);
            ready!(Pin::new(&mut self.0).poll_read(cx, &mut read))?;
            buf.put_slice(read.filled());
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for SmallReads {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    /// An unmasked text frame "hello", as a server sends it.
    const HELLO: [u8; 7] = [0x81, 0x05, b'h', b'e', b'l', b'l', b'o'];

    /// The 101 answer to the upgrade `request` with `extra` bytes of
    /// padding in a header.
    fn switching(request: &str, extra: usize) -> String {
        let key = request
            .lines()
            .find_map(|line| {
                line.strip_prefix("Sec-WebSocket-Key: ")
                    .or_else(|| line.strip_prefix("sec-websocket-key: "))
            })
            .expect("a key");
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let padding = "x".repeat(extra);
        format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\nX-Padding: {padding}\r\n\r\n"
        )
    }

    /// Reads the upgrade request from `server`, up to the end of its head.
    async fn read_request(server: &mut tokio::io::DuplexStream) -> Option<String> {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0; 1];
            if server.read(&mut byte).await.unwrap() == 0 {
                return None;
            }
            request.extend_from_slice(&byte);
        }
        Some(String::from_utf8(request).unwrap())
    }

    /// Answers the upgrade request read from `server` with a 101 carrying
    /// `extra` header bytes, and in the same write a text frame "hello".
    async fn answer_with_a_frame(mut server: tokio::io::DuplexStream, extra: usize) {
        use tokio::io::AsyncWriteExt;
        let Some(request) = read_request(&mut server).await else {
            return;
        };
        // With reads of 7 bytes, the padding puts the end of the head 3
        // bytes into a read, so that read carries bytes of the frame too.
        let base = switching(&request, 0).len();
        let extra = extra + (3 + 7 - (base + extra) % 7) % 7;
        let answer = switching(&request, extra);
        let _ = server
            .write_all(&[answer.as_bytes(), &HELLO].concat())
            .await;
        let _ = server.flush().await;
        // Kept open while the client reads.
        tokio::time::sleep(TIMEOUT).await;
    }

    // Block 50b's finding: tungstenite fails a handshake whose head takes
    // more than 64 small reads ("Attack attempt detected"); read whole
    // first, it completes, and a frame sent with the head arrives, also
    // when the read that ends the head carries bytes of the frame, which
    // tungstenite then hands to the connection (reviewer, #253, (a)).
    #[tokio::test]
    async fn a_head_read_in_small_reads_completes_the_upgrade_and_keeps_the_frame_after_it() {
        // One byte; 7 bytes with 1 KiB of padding; whole reads.
        for (size, extra) in [(1, 0), (7, 1 << 10), (usize::MAX, 1 << 10)] {
            let (client, server) = tokio::io::duplex(1 << 20);
            tokio::spawn(answer_with_a_frame(server, extra));
            let request = "ws://localhost/connection/websocket"
                .into_client_request()
                .unwrap();

            let (mut ws, response) =
                tokio::time::timeout(TIMEOUT, upgrade(request, SmallReads(client, size), LIMIT))
                    .await
                    .expect("the upgrade within the timeout")
                    .unwrap_or_else(|error| panic!("reads of {size}: the upgrade: {error:?}"));

            assert_eq!(response.status(), 101);
            let message = tokio::time::timeout(TIMEOUT, ws.next())
                .await
                .expect("the frame within the timeout");
            assert_eq!(
                message.unwrap().unwrap(),
                Message::text("hello"),
                "reads of {size}"
            );
        }
    }

    // Block 50b's finding (reviewer, #253, (b)): a head that does not end
    // is handed over at 64 KiB, and tungstenite refuses it; without the
    // bound the reader would wait for its end until the timeout.
    #[tokio::test]
    async fn a_head_that_does_not_end_is_refused_at_64_kib() {
        use tokio::io::AsyncWriteExt;
        let (client, mut server) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            if read_request(&mut server).await.is_none() {
                return;
            }
            let start = b"HTTP/1.1 101 Switching Protocols\r\nX-Padding: ";
            let _ = server.write_all(start).await;
            let _ = server.write_all(&vec![b'x'; 70 << 10]).await;
            // Open, and no blank line ever.
            tokio::time::sleep(TIMEOUT * 2).await;
        });
        let request = "ws://localhost/connection/websocket"
            .into_client_request()
            .unwrap();

        let error = tokio::time::timeout(TIMEOUT, upgrade(request, client, LIMIT))
            .await
            .expect("refused within the timeout")
            .map(drop)
            .expect_err("a head over 64 KiB refused");

        assert!(matches!(error, Error::AttackAttempt), "{error}");
    }

    // Block 50b's finding (reviewer, (c)): a head over 64 KiB is handed to
    // tungstenite, which still refuses it.
    #[tokio::test]
    async fn a_head_over_64_kib_still_fails_the_upgrade() {
        for one_byte in [false, true] {
            let (client, server) = tokio::io::duplex(1 << 20);
            tokio::spawn(answer_with_a_frame(server, 70 << 10));
            let request = "ws://localhost/connection/websocket"
                .into_client_request()
                .unwrap();

            let upgraded = if one_byte {
                tokio::time::timeout(TIMEOUT, upgrade(request, SmallReads(client, 1), LIMIT))
                    .await
                    .map(|result| result.map(drop))
            } else {
                tokio::time::timeout(TIMEOUT, upgrade(request, client, LIMIT))
                    .await
                    .map(|result| result.map(drop))
            };

            let error = upgraded
                .expect("an answer within the timeout")
                .expect_err("a head over 64 KiB refused");
            assert!(
                matches!(error, Error::AttackAttempt),
                "one byte at a time {one_byte}: {error}"
            );
        }
    }

    // Block 50b's finding (reviewer, (c)): a head the end of the stream
    // cuts is handed over, and tungstenite reports the end.
    #[tokio::test]
    async fn a_head_cut_by_the_end_of_the_stream_fails_the_upgrade() {
        use tokio::io::AsyncWriteExt;
        let (client, mut server) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            let mut request = [0; 4096];
            let _ = server.read(&mut request).await;
            let _ = server.write_all(b"HTTP/1.1 101 Switching").await;
            drop(server);
        });
        let request = "ws://localhost/connection/websocket"
            .into_client_request()
            .unwrap();

        let error = tokio::time::timeout(TIMEOUT, upgrade(request, client, LIMIT))
            .await
            .expect("an answer within the timeout")
            .map(drop)
            .expect_err("a head cut short refused");

        assert!(
            matches!(
                error,
                Error::Protocol(
                    tokio_tungstenite::tungstenite::error::ProtocolError::HandshakeIncomplete
                )
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn open_fails_on_refused_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result = WebSocket::open(
            &format!("ws://{address}"),
            None,
            TIMEOUT,
            &HeaderMap::new(),
            &Network::default(),
            LIMIT,
        )
        .await;

        assert!(result.is_err());
    }
}
