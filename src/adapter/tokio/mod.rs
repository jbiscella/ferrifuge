//! What the tokio driver needs beyond the shared adapter layer: its
//! transports, TLS, proxies, HTTP clients, connector, cookies and Android
//! support (Block 44, D2).

#[cfg(target_os = "android")]
pub mod android;
pub(super) mod connector;
#[cfg(feature = "cookies")]
pub(super) mod cookies;
pub(super) mod http;
pub(super) mod net;
pub(super) mod proxy;
pub(super) mod tls;
pub(super) mod transport;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ::tokio::runtime::Handle;
use ::tokio::task::JoinHandle;

use self::http::{HeaderSet, HttpTransport};
use self::net::Network;
use self::transport::{Inbound, WebSocket};
use super::log::Log;
use crate::machine::{FrameId, FrameType, TransportKind};

/// A transport being opened.
pub(in crate::adapter) type Opening =
    Pin<Box<dyn Future<Output = Result<Transport, String>> + Send>>;

/// What the driver gives to open a transport.
pub(in crate::adapter) struct Open {
    pub(in crate::adapter) kind: TransportKind,
    /// The endpoint's index, for the records.
    pub(in crate::adapter) endpoint: usize,
    pub(in crate::adapter) url: String,
    /// Websocket subprotocol of the configured encoding, if it needs one.
    pub(in crate::adapter) subprotocol: Option<&'static str>,
    pub(in crate::adapter) timeout: Duration,
    pub(in crate::adapter) max_message: usize,
    pub(in crate::adapter) headers: HeaderSet,
    pub(in crate::adapter) network: Arc<Network>,
    /// Where commands over emulated transports go, when not the default.
    pub(in crate::adapter) emulation: Option<String>,
    /// The commands an emulated transport's stream request carries.
    pub(in crate::adapter) initial_frame: Option<Vec<u8>>,
}

/// Opens a transport of `open.kind`, its attempt recorded in `log`.
pub(in crate::adapter) fn open(open: Open, log: &Log) -> Opening {
    let Open {
        kind,
        endpoint,
        url,
        subprotocol,
        timeout,
        max_message,
        headers,
        network,
        emulation,
        initial_frame,
    } = open;
    // The attempt's proxy, decided once (Block 23, D1); the transport
    // opening it gets the same decision.
    network.new_attempt();
    let decision = reqwest::Url::parse(&url)
        .map_err(|e| e.to_string())
        .and_then(|parsed| network.decide(&parsed));
    log.attempt(kind, endpoint, &decision, network.connector().is_some());
    match kind {
        TransportKind::WebSocket => Box::pin(async move {
            let headers = headers.current();
            WebSocket::open(&url, subprotocol, timeout, &headers, &network, max_message)
                .await
                .map(|websocket| Transport::WebSocket(Box::new(websocket)))
        }),
        TransportKind::HttpStream | TransportKind::Sse => {
            let protobuf = subprotocol.is_some();
            Box::pin(async move {
                HttpTransport::open(
                    kind,
                    &url,
                    initial_frame.unwrap_or_default(),
                    emulation.as_deref(),
                    timeout,
                    headers,
                    &network,
                    protobuf,
                    max_message,
                )
                .await
                .map(|http| Transport::Http(Box::new(http)))
            })
        }
    }
}

/// An open transport of any kind.
pub(in crate::adapter) enum Transport {
    // Boxed: the transports differ much in size, and one is held at a time.
    WebSocket(Box<WebSocket>),
    Http(Box<HttpTransport>),
}

impl Transport {
    /// Queues `frame`; it is written while [`Transport::recv`] runs.
    pub(in crate::adapter) fn send(
        &mut self,
        id: FrameId,
        frame: Vec<u8>,
        frame_type: FrameType,
    ) -> Result<(), String> {
        match self {
            Self::WebSocket(websocket) => websocket.send(id, frame, frame_type),
            // Failures end the transport through `recv`.
            Self::Http(http) => {
                http.send(id, frame);
                Ok(())
            }
        }
    }

    /// Cancel-safe. Without `reading`, only writes, as Block 15's full
    /// read-ahead requires.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv(reading).await,
            Self::Http(http) => http.recv(reading).await,
        }
    }

    /// Closes the transport on a task of its own, bounded by
    /// `Config::timeout` (Block 8); the task, which the driver awaits when it
    /// stops (Block 33, D1).
    pub(in crate::adapter) fn close(self, runtime: &Handle) -> JoinHandle<()> {
        match self {
            Self::WebSocket(websocket) => runtime.spawn(websocket.close()),
            Self::Http(http) => runtime.spawn(http.close()),
        }
    }
}
