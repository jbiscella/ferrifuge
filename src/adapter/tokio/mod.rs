//! What the tokio driver needs beyond the shared adapter layer: its
//! transports, TLS, proxies, HTTP clients, connector, cookies and Android
//! support.
// Block 44, D2.

#[cfg(target_os = "android")]
pub mod android;
pub(super) mod connector;
#[cfg(feature = "cookies")]
pub(super) mod cookies;
mod head;
pub(super) mod http;
pub(super) mod net;
pub(super) mod options;
pub(super) mod proxy;
pub(super) mod tls;
pub(super) mod transport;
#[cfg(feature = "unstable-webtransport")]
pub(super) mod webtransport;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ::tokio::runtime::Handle;
use ::tokio::task::JoinHandle;

use self::http::{HeaderSet, HttpTransport};
use self::net::Network;
pub(in crate::adapter) use self::options::{DriverPart, Options, SharedPart, Started};
pub(in crate::adapter) use self::transport::Inbound;
use self::transport::WebSocket;
#[cfg(feature = "unstable-webtransport")]
use self::webtransport::WebTransport;
use super::body::TransportFailure;
use super::log::Log;
use crate::sans_io::{FrameId, FrameType, TransportKind};

/// A transport being opened.
pub(in crate::adapter) type Opening =
    Pin<Box<dyn Future<Output = Result<Transport, TransportFailure>> + Send>>;

/// What the driver gives to open a transport.
pub(in crate::adapter) struct Open {
    pub(in crate::adapter) kind: TransportKind,
    /// The endpoint's index, for the records.
    pub(in crate::adapter) endpoint: usize,
    pub(in crate::adapter) url: String,
    /// The websocket subprotocols offered, and the encoding (Block 67).
    pub(in crate::adapter) offer: super::subprotocols::Offer,
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
        offer,
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
        .map_err(|e| TransportFailure::other(e.to_string()))
        .and_then(|parsed| network.decide(&parsed));
    #[cfg(feature = "unstable-webtransport")]
    let alpn = (kind == TransportKind::WebTransport).then(|| network.alpn.first(endpoint).name());
    #[cfg(not(feature = "unstable-webtransport"))]
    let alpn = None;
    log.attempt(
        kind,
        endpoint,
        &decision,
        network.connector().is_some(),
        alpn,
    );
    match kind {
        TransportKind::WebSocket => Box::pin(async move {
            let headers = headers.current();
            WebSocket::open(&url, &offer, timeout, &headers, &network, max_message)
                .await
                .map(|websocket| Transport::WebSocket(Box::new(websocket)))
        }),
        TransportKind::HttpStream | TransportKind::Sse => {
            let protobuf = offer.protobuf;
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
        #[cfg(feature = "unstable-webtransport")]
        TransportKind::WebTransport => Box::pin(async move {
            let headers = headers.current();
            let target = webtransport::Target {
                url: &url,
                protobuf: offer.protobuf,
                timeout,
                headers: &headers,
                max_message,
                endpoint,
            };
            WebTransport::open(target, &network)
                .await
                .map(|webtransport| Transport::WebTransport(Box::new(webtransport)))
        }),
        // Refused by `ClientBuilder::build` without the native transport
        // (Block 48, D1).
        #[cfg(all(feature = "unstable", not(feature = "unstable-webtransport")))]
        TransportKind::WebTransport => Box::pin(async move {
            let _ = (url, headers, network, max_message, timeout);
            Err(TransportFailure::other(NO_WEBTRANSPORT))
        }),
    }
}

/// The message of a WebTransport endpoint in a build without the native
/// transport.
// Block 48, D1.
#[cfg(all(feature = "unstable", not(feature = "unstable-webtransport")))]
pub(in crate::adapter) const NO_WEBTRANSPORT: &str =
    "the webtransport transport needs the `unstable-webtransport` feature";

/// An open transport of any kind.
pub(in crate::adapter) enum Transport {
    // Boxed: the transports differ much in size, and one is held at a time.
    WebSocket(Box<WebSocket>),
    Http(Box<HttpTransport>),
    #[cfg(feature = "unstable-webtransport")]
    #[expect(clippy::enum_variant_names, reason = "the transport's name")]
    WebTransport(Box<WebTransport>),
}

impl Transport {
    /// Queues `frame`; it is written while [`Transport::recv`] runs.
    pub(in crate::adapter) fn send(
        &mut self,
        id: FrameId,
        frame: Vec<u8>,
        frame_type: FrameType,
    ) -> Result<(), TransportFailure> {
        match self {
            Self::WebSocket(websocket) => websocket.send(id, frame, frame_type),
            // Failures end the transport through `recv`.
            Self::Http(http) => {
                http.send(id, frame);
                Ok(())
            }
            #[cfg(feature = "unstable-webtransport")]
            Self::WebTransport(webtransport) => {
                webtransport.send(id, frame);
                Ok(())
            }
        }
    }

    /// Cancel-safe. Without `reading`, only writes, as a full read-ahead
    /// requires.
    // Block 15.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv(reading).await,
            Self::Http(http) => http.recv(reading).await,
            #[cfg(feature = "unstable-webtransport")]
            Self::WebTransport(webtransport) => webtransport.recv(reading).await,
        }
    }

    /// Closes the transport on a task of its own, bounded by
    /// `Config::timeout`; the task, which the driver awaits when it stops.
    // Block 8; Block 33, D1.
    pub(in crate::adapter) fn close(self, runtime: &Handle) -> JoinHandle<()> {
        match self {
            Self::WebSocket(websocket) => runtime.spawn(websocket.close()),
            Self::Http(http) => runtime.spawn(http.close()),
            #[cfg(feature = "unstable-webtransport")]
            Self::WebTransport(webtransport) => runtime.spawn(webtransport.close()),
        }
    }
}
