//! Websocket transport of the tokio adapter.

use std::sync::{Arc, OnceLock};

use futures_util::{SinkExt, StreamExt};
use rustls::crypto::ring;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

/// An open websocket connection to the server.
pub(crate) struct WebSocket {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

/// What the server sent, as far as the client protocol is concerned.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Inbound {
    /// A frame of client protocol messages.
    Frame(Vec<u8>),
    /// The connection ended; `code` is absent when no close frame was received.
    Closed { code: Option<u16>, reason: String },
}

impl WebSocket {
    /// Opens a connection to `url` (`ws://` or `wss://`).
    pub(crate) async fn open(url: &str) -> Result<Self, String> {
        // Plain ws:// must work on hosts without platform certificates.
        let secure = url
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("wss://"));
        let connector = if secure {
            Connector::Rustls(tls_config()?)
        } else {
            Connector::Plain
        };
        let (stream, _) =
            tokio_tungstenite::connect_async_tls_with_config(url, None, true, Some(connector))
                .await
                .map_err(|e| e.to_string())?;
        Ok(Self { stream })
    }

    /// Sends one frame of JSON-encoded commands.
    pub(crate) async fn send(&mut self, frame: Vec<u8>) -> Result<(), String> {
        let text = String::from_utf8(frame).map_err(|e| e.to_string())?;
        self.stream
            .send(Message::text(text))
            .await
            .map_err(|e| e.to_string())
    }

    /// Waits for the next frame or for the end of the connection.
    /// Cancel-safe: dropping the future loses no message.
    pub(crate) async fn recv(&mut self) -> Inbound {
        loop {
            let Some(Ok(message)) = self.stream.next().await else {
                return Inbound::Closed {
                    code: None,
                    reason: String::new(),
                };
            };
            match message {
                Message::Text(text) => return Inbound::Frame(text.as_bytes().to_vec()),
                Message::Binary(bytes) => return Inbound::Frame(bytes.to_vec()),
                Message::Close(frame) => return closed(frame),
                // Websocket-level keepalive is answered by tungstenite itself.
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    /// Closes the connection, ignoring failures: it is abandoned either way.
    pub(crate) async fn close(mut self) {
        let _ = self.stream.close(None).await;
    }
}

/// TLS settings for `wss://`, built once per process.
///
/// The crypto provider is set explicitly rather than taken from the process
/// default: when another crate also enables rustls's aws-lc-rs provider,
/// rustls cannot pick a default and panics.
fn tls_config() -> Result<Arc<ClientConfig>, String> {
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            // Unreadable platform certificates are skipped, as browsers do.
            let (added, _) = roots.add_parsable_certificates(load_native_certs().certs);
            if added == 0 {
                return Err("no platform root certificates found".to_owned());
            }
            let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
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
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

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
        let mut ws = WebSocket::open(&url).await.unwrap();

        ws.send(b"{\"id\":1}".to_vec()).await.unwrap();
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
    async fn open_fails_on_refused_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result = WebSocket::open(&format!("ws://{address}")).await;

        assert!(result.is_err());
    }

    #[test]
    fn tls_config_builds_even_with_several_rustls_providers_enabled() {
        // Test builds enable aws-lc-rs too (through reqwest), which made the
        // process-default provider ambiguous and panic.
        let config = tls_config();

        assert!(config.is_ok(), "{config:?}");
    }
}
