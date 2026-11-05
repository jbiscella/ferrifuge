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

use crate::machine::FrameType;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// An open websocket connection to the server.
///
/// Writing never blocks the caller: frames are queued by [`WebSocket::send`]
/// and written while [`WebSocket::recv`] waits for the server, so a server
/// that stops reading cannot freeze the client (Block 8).
pub(crate) struct WebSocket {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Frames not yet handed to the stream, with the time each was queued.
    queue: VecDeque<(Message, Instant)>,
    /// When the frame being written was queued; none when no write is under way.
    writing: Option<Instant>,
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
}

impl WebSocket {
    /// Opens a connection to `url` (`ws://` or `wss://`), asking for
    /// `subprotocol` if any. A frame not written within `timeout` of being
    /// queued ends the connection, and closing it takes at most `timeout`.
    pub(crate) async fn open(
        url: &str,
        subprotocol: Option<&str>,
        timeout: Duration,
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
        if let Some(subprotocol) = subprotocol {
            let value = HeaderValue::from_str(subprotocol).map_err(|e| e.to_string())?;
            request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
        }
        // tungstenite fails the handshake unless the server accepts the
        // subprotocol asked for.
        let (stream, _) =
            tokio_tungstenite::connect_async_tls_with_config(request, None, true, Some(connector))
                .await
                .map_err(|e| e.to_string())?;
        Ok(Self {
            stream,
            queue: VecDeque::new(),
            writing: None,
            deadline: Box::pin(tokio::time::sleep(timeout)),
            timeout,
        })
    }

    /// Queues one frame of encoded commands, to go out as a text or binary
    /// message while [`WebSocket::recv`] runs.
    pub(crate) fn send(&mut self, frame: Vec<u8>, frame_type: FrameType) -> Result<(), String> {
        let message = match frame_type {
            FrameType::Text => Message::text(String::from_utf8(frame).map_err(|e| e.to_string())?),
            FrameType::Binary => Message::binary(frame),
        };
        self.queue.push_back((message, Instant::now()));
        Ok(())
    }

    /// Writes the queued frames while waiting for the next frame or for the
    /// end of the connection, which a failed write, or a frame not written
    /// in time, also is. Cancel-safe: dropping the future loses no message.
    pub(crate) async fn recv(&mut self) -> Inbound {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Inbound> {
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
        loop {
            let Some(Ok(message)) = ready!(self.stream.poll_next_unpin(cx)) else {
                return Poll::Ready(lost);
            };
            match message {
                Message::Text(text) => {
                    return Poll::Ready(Inbound::Frame(text.as_bytes().to_vec()));
                }
                Message::Binary(bytes) => return Poll::Ready(Inbound::Frame(bytes.to_vec())),
                Message::Close(frame) => return Poll::Ready(closed(frame)),
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
                ready!(self.stream.poll_flush_unpin(cx))?;
                self.writing = None;
                continue;
            }
            if self.queue.is_empty() {
                return Poll::Ready(Ok(()));
            }
            ready!(self.stream.poll_ready_unpin(cx))?;
            let (message, queued) = self.queue.pop_front().expect("the queue is not empty");
            self.stream.start_send_unpin(message)?;
            self.writing = Some(queued);
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
            .or(self.queue.front().map(|(_, queued)| *queued))
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
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            // Unreadable platform certificates are skipped, as browsers do.
            let (added, _) = roots.add_parsable_certificates(load_native_certs().certs);
            if added == 0 {
                return Err("no platform root certificates found".to_owned());
            }
            let config = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| e.to_string())?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(Arc::new(config))
        })
        .clone()
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

    #[tokio::test]
    async fn sends_and_receives_frames_then_reports_close_code() {
        let url = echo_then_close(3001, "shutdown").await;
        let mut ws = WebSocket::open(&url, None, TIMEOUT).await.unwrap();

        ws.send(b"{\"id\":1}".to_vec(), FrameType::Text).unwrap();
        let echoed = ws.recv().await;
        let closed = ws.recv().await;

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
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, TIMEOUT)
            .await
            .unwrap();

        ws.send(b"{}".to_vec(), FrameType::Text).unwrap();
        let text = ws.recv().await;
        ws.send(vec![0xff, 0x00], FrameType::Binary).unwrap();
        let binary = ws.recv().await;

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
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, timeout)
            .await
            .unwrap();
        // Frames are queued until one is not written in time: how much the
        // system buffers varies (Windows took a single 32 MiB frame whole).
        let mut queued = 0;
        let stalled = loop {
            ws.send(vec![b'x'; 1 << 20], FrameType::Text).unwrap();
            queued += 1 << 20;
            assert!(queued <= 1 << 30, "the system buffered 1 GiB");
            if let Ok(inbound) = tokio::time::timeout(Duration::from_millis(10), ws.recv()).await {
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
        let mut ws = WebSocket::open(&format!("ws://{address}"), None, timeout)
            .await
            .unwrap();
        ws.send(b"late".to_vec(), FrameType::Text).unwrap();
        // The client is not polled until the frame is overdue, while the
        // socket could take it at any time.
        tokio::time::sleep(timeout * 2).await;

        let overdue = tokio::time::timeout(TIMEOUT, ws.recv()).await;
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

    #[tokio::test]
    async fn open_fails_on_refused_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result = WebSocket::open(&format!("ws://{address}"), None, TIMEOUT).await;

        assert!(result.is_err());
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
