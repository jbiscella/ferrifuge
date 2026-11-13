//! WebTransport transport of the tokio adapter (Block 48): one
//! bidirectional stream of an HTTP/3 session over QUIC, through wtransport
//! and quinn, with the client's own TLS settings and the build's provider.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use reqwest::Url;
use rustls::ClientConfig as TlsConfig;
use rustls::crypto::tls13::{Hkdf, OkmBlock};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};
use tracing::debug;
use wtransport::config::{DnsLookupFuture, DnsResolver};
use wtransport::endpoint::ConnectOptions;
use wtransport::endpoint::endpoint_side::Client as ClientSide;
use wtransport::error::{ConnectingError, ConnectionError};
use wtransport::quinn;
use wtransport::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt};

use super::super::body::{Decoder, TOO_LARGE};
use super::net::Network;
use super::transport::Inbound;
use crate::machine::FrameId;

/// QUIC settings of every WebTransport connection (D12), read back by a
/// unit test: about 4 MiB of buffers per connection at most.
const STREAM_RECEIVE_WINDOW: u32 = 1 << 20;
const RECEIVE_WINDOW: u32 = 2 << 20;
const SEND_WINDOW: u64 = 1 << 20;
/// The server opens no bidirectional stream.
const MAX_CONCURRENT_BIDI_STREAMS: u32 = 0;
/// The least RFC 9114 section 6.2 allows: control and two QPACK streams.
const MAX_CONCURRENT_UNI_STREAMS: u32 = 3;
/// Without datagram support Centrifugo closes the connection.
const DATAGRAM_RECEIVE_BUFFER_SIZE: usize = 64 << 10;
/// Measured by quinn from the last packet received.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// quinn's default; the smaller of the two peers' applies.
const MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The ALPN protocol of HTTP/3 (RFC 9114 section 3.1).
const H3: &[u8] = b"h3";

/// A local QUIC error 0x178: a crypto error with TLS alert 120,
/// `no_application_protocol`, as rustls raises when the server selected no
/// application protocol (RFC 9001 section 8.1). The only failure that starts
/// D5's retry without ALPN.
const NO_APPLICATION_PROTOCOL: u64 = 0x178;

/// The read buffer of the stream.
const READ_CHUNK: usize = 16 << 10;

/// The message of an attempt for which a proxy is decided (D13).
pub(in crate::adapter) const THROUGH_PROXY: &str = "webtransport cannot go through a proxy";

/// The message of a session the server refused (D6).
const SESSION_REJECTED: &str = "webtransport session rejected by the server";

/// An open WebTransport session and its one bidirectional stream.
///
/// Writing never blocks the caller: frames are queued by
/// [`WebTransport::send`] and written while [`WebTransport::recv`] waits for
/// the server, as on a websocket (D8).
pub(crate) struct WebTransport {
    /// The attempt's own endpoint: one UDP socket and its driver (D11).
    endpoint: Endpoint<ClientSide>,
    connection: Connection,
    send: SendStream,
    recv: RecvStream,
    /// JSON frames end with `\n`; Protobuf frames go as they are (D7).
    json: bool,
    /// Block 27's splitters, bounded by `max_message_bytes` (D7).
    decoder: Decoder,
    /// Messages decoded and not yet reported.
    messages: VecDeque<Vec<u8>>,
    /// How the stream ended, reported after the messages before it (D9).
    end: Option<Inbound>,
    read_buffer: Box<[u8]>,
    /// Frames not yet handed to the stream, with their id and the time each
    /// was queued.
    queue: VecDeque<(FrameId, Vec<u8>, Instant)>,
    /// The frame being written, how much of it the stream has taken, and
    /// when it was queued.
    writing: Option<(FrameId, Vec<u8>, usize, Instant)>,
    /// Frames written and not yet reported by [`WebTransport::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a read comes first next.
    read_next: bool,
    /// Fires after the oldest frame not yet written was queued.
    deadline: Pin<Box<Sleep>>,
    /// Bound on writing a frame, and on closing (`Config::timeout`).
    timeout: Duration,
}

/// What a WebTransport open needs besides the network.
pub(in crate::adapter) struct Target<'a> {
    pub(in crate::adapter) url: &'a str,
    pub(in crate::adapter) protobuf: bool,
    pub(in crate::adapter) timeout: Duration,
    pub(in crate::adapter) headers: &'a http::HeaderMap,
    pub(in crate::adapter) max_message: usize,
}

/// The ALPN list one handshake offers (D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::adapter) enum Alpn {
    H3,
    None,
}

impl Alpn {
    /// As the attempt's records name it (D5, D18).
    pub(in crate::adapter) fn name(self) -> &'static str {
        match self {
            Self::H3 => "h3",
            Self::None => "none",
        }
    }

    fn protocols(self) -> Vec<Vec<u8>> {
        match self {
            Self::H3 => vec![H3.to_vec()],
            Self::None => Vec::new(),
        }
    }
}

impl WebTransport {
    /// Opens a session at `target.url` and its bidirectional stream: the
    /// host resolved, a UDP socket of its family bound, the QUIC handshake
    /// offering `h3` and, if the server selected no application protocol,
    /// once more offering none, the HTTP/3 SETTINGS, the extended CONNECT
    /// with the real headers, and the stream (D5, D6). The caller bounds it
    /// with the handshake timeout.
    pub(crate) async fn open(target: Target<'_>, network: &Network) -> Result<Self, String> {
        let url = session_url(target.url, target.protobuf)?;
        if network.connector().is_some() {
            // Refused by `ClientBuilder::build` (D14).
            return Err("a connector carries the websocket only".to_owned());
        }
        // Block 23 D2's decision, asked as `https`; a proxy fails the
        // attempt before any datagram (D13).
        if let Some(proxy) = network.decide(&url)? {
            return Err(format!("{THROUGH_PROXY}: {}", proxy.shown()));
        }
        let (host, address) = resolve(&url).await?;
        let tls = network.tls.config()?;
        let options = connect_options(&url, target.headers, network)?;
        let mut alpn = Alpn::H3;
        let (endpoint, connection) = loop {
            let config = client_config(&tls, alpn, &host, address)?;
            let endpoint = Endpoint::client(config).map_err(|e| e.to_string())?;
            match endpoint.connect(options.clone()).await {
                Ok(connection) => break (endpoint, connection),
                Err(error) if alpn == Alpn::H3 && retries_without_alpn(&error) => {
                    debug!(
                        transport = "webtransport",
                        alpn = Alpn::None.name(),
                        "the server selected no application protocol: handshake again"
                    );
                    alpn = Alpn::None;
                }
                Err(error) => return Err(connecting_error(&error)),
            }
        };
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| e.to_string())?
            .await
            .map_err(|e| e.to_string())?;
        let decoder = decoder(target.protobuf, target.max_message);
        Ok(Self {
            endpoint,
            connection,
            send,
            recv,
            json: !target.protobuf,
            decoder,
            messages: VecDeque::new(),
            end: None,
            read_buffer: vec![0; READ_CHUNK].into_boxed_slice(),
            queue: VecDeque::new(),
            writing: None,
            written: VecDeque::new(),
            read_next: false,
            deadline: Box::pin(tokio::time::sleep(target.timeout)),
            timeout: target.timeout,
        })
    }

    /// Queues frame `id` of encoded commands, to be written while
    /// [`WebTransport::recv`] runs, which reports it once the stream has
    /// taken all of it.
    pub(crate) fn send(&mut self, id: FrameId, mut frame: Vec<u8>) {
        frame_bytes(&mut frame, self.json);
        self.queue.push_back((id, frame, Instant::now()));
    }

    /// Writes the queued frames while waiting for the next message, for a
    /// frame written, or for the end of the stream, which a failed write or
    /// a frame not written in time also is. Without `reading`, it only
    /// writes (Block 15). Cancel-safe.
    pub(crate) async fn recv(&mut self, reading: bool) -> Inbound {
        std::future::poll_fn(|cx| self.poll_recv(cx, reading)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>, reading: bool) -> Poll<Inbound> {
        if let Poll::Ready(Err(_)) = self.poll_write(cx, true) {
            return Poll::Ready(lost());
        }
        if let Some(queued) = self.oldest_unwritten() {
            let deadline = queued + self.timeout;
            if self.deadline.deadline() != deadline {
                self.deadline.as_mut().reset(deadline);
            }
            if self.deadline.as_mut().poll(cx).is_ready() {
                return Poll::Ready(lost());
            }
        }
        // Written reports and messages take turns, as on a websocket.
        if !self.read_next
            && let Some(id) = self.written.pop_front()
        {
            self.read_next = true;
            return Poll::Ready(Inbound::Written(id));
        }
        if reading && let Poll::Ready(inbound) = self.poll_read(cx) {
            self.read_next = false;
            return Poll::Ready(inbound);
        }
        if let Some(id) = self.written.pop_front() {
            self.read_next = true;
            return Poll::Ready(Inbound::Written(id));
        }
        Poll::Pending
    }

    /// The next message, then, once every message before it was reported,
    /// the end of the stream (D9).
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Inbound> {
        loop {
            if let Some(message) = self.messages.pop_front() {
                return Poll::Ready(Inbound::Frame(message));
            }
            if let Some(end) = self.end.take() {
                // Reported once; a later read finds the stream ended.
                self.end = Some(lost());
                return Poll::Ready(end);
            }
            let mut buffer = ReadBuf::new(&mut self.read_buffer);
            match ready!(Pin::new(&mut self.recv).poll_read(cx, &mut buffer)) {
                Ok(()) if buffer.filled().is_empty() => self.end = Some(lost()),
                Ok(()) => {
                    let chunk = buffer.filled();
                    if let Err(reason) = self.decoder.push(chunk, &mut self.messages) {
                        // Over `Config::max_message_bytes` (Block 29, D3).
                        self.end = Some(if reason == TOO_LARGE {
                            Inbound::TooLarge
                        } else {
                            Inbound::Closed { code: None, reason }
                        });
                    }
                }
                // A reset, the session's close, the connection's loss: no
                // code (D9).
                Err(_) => self.end = Some(lost()),
            }
        }
    }

    /// Hands the queued frames to the stream one at a time; ready once every
    /// queued frame is taken. With `on_time`, a frame queued longer than the
    /// timeout is never written: the write fails instead (D8).
    fn poll_write(&mut self, cx: &mut Context<'_>, on_time: bool) -> Poll<std::io::Result<()>> {
        loop {
            if on_time
                && self
                    .oldest_unwritten()
                    .is_some_and(|queued| queued.elapsed() >= self.timeout)
            {
                return Poll::Ready(Err(std::io::ErrorKind::TimedOut.into()));
            }
            if let Some((id, frame, taken, _)) = &mut self.writing {
                if *taken == frame.len() {
                    self.written.push_back(*id);
                    self.writing = None;
                    continue;
                }
                let accepted = ready!(Pin::new(&mut self.send).poll_write(cx, &frame[*taken..]))?;
                if accepted == 0 {
                    return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
                }
                *taken += accepted;
                continue;
            }
            let Some((id, frame, queued)) = self.queue.pop_front() else {
                return Poll::Ready(Ok(()));
            };
            self.writing = Some((id, frame, 0, queued));
        }
    }

    /// When the oldest frame not yet written was queued.
    fn oldest_unwritten(&self) -> Option<Instant> {
        self.writing
            .as_ref()
            .map(|(_, _, _, queued)| *queued)
            .or(self.queue.front().map(|(_, _, queued)| *queued))
    }

    /// Writes the queued frames and finishes the send stream, then closes
    /// the connection with code 0 and waits for the endpoint to go idle, all
    /// within the timeout; past it, the endpoint is dropped (D11).
    pub(crate) async fn close(mut self) {
        let timeout = self.timeout;
        let _ = tokio::time::timeout(timeout, async {
            let _ = std::future::poll_fn(|cx| self.poll_write(cx, false)).await;
            let _ = self.send.finish().await;
            self.connection.close(VarInt::from_u32(0), b"");
            self.endpoint.wait_idle().await;
        })
        .await;
    }
}

/// The end of the stream with no code: code 1 "transport closed" (D9).
fn lost() -> Inbound {
    Inbound::Closed {
        code: None,
        reason: String::new(),
    }
}

/// Block 27's splitters, bounded by `max_message_bytes`: `http_stream`'s
/// body without the emulation envelope (D7).
fn decoder(protobuf: bool, max_message: usize) -> Decoder {
    if protobuf {
        Decoder::protobuf(max_message)
    } else {
        Decoder::lines(max_message)
    }
}

/// Appends the `\n` the server reads JSON lines by; a Protobuf frame of
/// varint-delimited commands goes as it is (D7).
fn frame_bytes(frame: &mut Vec<u8>, json: bool) {
    if json {
        frame.push(b'\n');
    }
}

/// The session URL: `endpoint`, with `cf_protocol=protobuf` appended to its
/// query for Protobuf (D2).
fn session_url(endpoint: &str, protobuf: bool) -> Result<Url, String> {
    let mut url = Url::parse(endpoint).map_err(|e| e.to_string())?;
    if url.scheme() != "https" {
        // Refused by `ClientBuilder::build` (D2).
        return Err("webtransport needs an https:// endpoint".to_owned());
    }
    if protobuf {
        url.query_pairs_mut().append_pair("cf_protocol", "protobuf");
    }
    Ok(url)
}

/// The host of `url` and the first address it resolves to: no Happy
/// Eyeballs, as wtransport does (D6).
async fn resolve(url: &Url) -> Result<(String, SocketAddr), String> {
    let host = url
        .host_str()
        .ok_or_else(|| "webtransport needs an endpoint with a host".to_owned())?;
    // An IPv6 literal comes in brackets.
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
        .to_owned();
    let port = url.port_or_known_default().unwrap_or(443);
    let address = match host.parse::<std::net::IpAddr>() {
        Ok(ip) => SocketAddr::new(ip, port),
        Err(_) => tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| format!("cannot resolve {host}: {e}"))?
            .next()
            .ok_or_else(|| format!("cannot resolve {host}: no address"))?,
    };
    Ok((host, address))
}

/// The CONNECT request's options: the real HTTP headers (Blocks 17, 32)
/// and the cookie jar's `Cookie` (Block 43), as names and values (D14).
fn connect_options(
    url: &Url,
    headers: &http::HeaderMap,
    network: &Network,
) -> Result<ConnectOptions, String> {
    let mut headers = headers.clone();
    network.add_cookies(url.as_str(), &mut headers);
    let mut options = ConnectOptions::builder(url.as_str());
    for (name, value) in &headers {
        // Checked by `ClientBuilder::build`; never the value in a message.
        let value = value
            .to_str()
            .map_err(|_| format!("HTTP header {:?} is not visible ASCII", name.as_str()))?;
        options = options.add_header(name.as_str(), value);
    }
    Ok(options.build())
}

/// The QUIC client of one handshake offering `alpn`: a copy of the
/// client's TLS settings, checked as quinn will use it, wtransport's
/// builder finished with quinn's configuration, bound to `address`'s family,
/// resolving to `address`, with a reset key of the configuration's provider
/// (D4, D5, D6, D12, D15).
fn client_config(
    tls: &Arc<TlsConfig>,
    alpn: Alpn,
    host: &str,
    address: SocketAddr,
) -> Result<ClientConfig, String> {
    let tls = quic_tls(tls, alpn);
    check_quic(&tls, host)?;
    let reset_key = ResetKey::new(&tls)?;
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| unfit_for_quic(&e.to_string()))?;
    let mut quic = quinn::ClientConfig::new(Arc::new(crypto));
    quic.transport_config(Arc::new(transport_config()));
    let bind = match address {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let mut config = ClientConfig::builder()
        .with_bind_address(bind)
        .build_with_quic_config(quic);
    config.set_dns_resolver(Resolved(address));
    *config.quic_endpoint_config_mut() = quinn::EndpointConfig::new(Arc::new(reset_key));
    Ok(config)
}

/// A copy of `tls` offering `alpn`, the application's own left unchanged
/// (D4, D5; Block 49 row 53).
pub(in crate::adapter) fn quic_tls(tls: &TlsConfig, alpn: Alpn) -> Arc<TlsConfig> {
    let mut copy = tls.clone();
    copy.alpn_protocols = alpn.protocols();
    Arc::new(copy)
}

/// Fails unless QUIC can use `tls`: quinn's initial cipher suite, and a
/// TLS 1.3 QUIC session, the call quinn-proto would otherwise unwrap under
/// the endpoint's lock and abort the process with (D4).
pub(in crate::adapter) fn check_quic(tls: &Arc<TlsConfig>, host: &str) -> Result<(), String> {
    quinn::crypto::rustls::QuicClientConfig::try_from(Arc::clone(tls))
        .map_err(|e| unfit_for_quic(&e.to_string()))?;
    let name = ServerName::try_from(host.to_owned()).map_err(|e| e.to_string())?;
    rustls::quic::ClientConnection::new(
        Arc::clone(tls),
        rustls::quic::Version::V1,
        name,
        Vec::new(),
    )
    .map(drop)
    .map_err(|e| unfit_for_quic(&e.to_string()))
}

/// The message of a TLS configuration QUIC cannot use (D4).
fn unfit_for_quic(cause: &str) -> String {
    format!(
        "the TLS configuration cannot carry webtransport, which needs TLS 1.3 with TLS13_AES_128_GCM_SHA256: {cause}"
    )
}

/// quinn's transport settings of D12, set explicitly.
fn transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config
        .stream_receive_window(STREAM_RECEIVE_WINDOW.into())
        .receive_window(RECEIVE_WINDOW.into())
        .send_window(SEND_WINDOW)
        .max_concurrent_bidi_streams(MAX_CONCURRENT_BIDI_STREAMS.into())
        .max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into())
        .datagram_receive_buffer_size(Some(DATAGRAM_RECEIVE_BUFFER_SIZE))
        .keep_alive_interval(Some(KEEP_ALIVE_INTERVAL))
        .max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(MAX_IDLE_TIMEOUT).expect("30 s is a valid idle timeout"),
        ));
    config
}

/// Whether a failed handshake offering `h3` starts D5's retry: only the
/// local QUIC error 0x178, raised when the server selected no application
/// protocol.
fn retries_without_alpn(error: &ConnectingError) -> bool {
    matches!(
        error,
        ConnectingError::ConnectionError(ConnectionError::QuicProto(proto))
            if proto.to_string().ends_with(&format!("(code: {NO_APPLICATION_PROTOCOL})"))
    )
}

/// The message of a failed open (D6).
fn connecting_error(error: &ConnectingError) -> String {
    match error {
        ConnectingError::SessionRejected => SESSION_REJECTED.to_owned(),
        other => other.to_string(),
    }
}

/// wtransport's resolver, given the address Ferrifuge resolved, so that the
/// socket bound and the address dialled are of one family (D6).
#[derive(Debug)]
struct Resolved(SocketAddr);

impl DnsResolver for Resolved {
    fn resolve(&self, _host: &str) -> Pin<Box<dyn DnsLookupFuture>> {
        let address = self.0;
        Box::pin(async move { Ok(Some(address)) })
    }
}

/// The endpoint's stateless-reset key: 32 bytes of the configuration
/// provider's `secure_random`, signed through its TLS 1.3 SHA-256 HKDF, so
/// that no key of a QUIC function comes from outside the provider (D15).
struct ResetKey {
    hkdf: &'static dyn Hkdf,
    key: OkmBlock,
}

/// Length of the reset key and of its signatures (SHA-256).
const RESET_KEY_LEN: usize = 32;

impl ResetKey {
    fn new(tls: &TlsConfig) -> Result<Self, String> {
        let provider = tls.crypto_provider();
        let hkdf = provider
            .cipher_suites
            .iter()
            .find_map(|suite| match suite {
                rustls::SupportedCipherSuite::Tls13(suite)
                    if suite.common.suite == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256 =>
                {
                    Some(suite.hkdf_provider)
                }
                _ => None,
            })
            .ok_or_else(|| unfit_for_quic("no TLS13_AES_128_GCM_SHA256 suite"))?;
        let mut key = [0; RESET_KEY_LEN];
        provider
            .secure_random
            .fill(&mut key)
            .map_err(|_| "the crypto provider gave no random bytes".to_owned())?;
        Ok(Self {
            hkdf,
            key: OkmBlock::new(&key),
        })
    }
}

impl quinn::crypto::HmacKey for ResetKey {
    fn sign(&self, data: &[u8], signature_out: &mut [u8]) {
        let tag = self.hkdf.hmac_sign(&self.key, data);
        let tag = tag.as_ref();
        let length = signature_out.len().min(tag.len());
        signature_out[..length].copy_from_slice(&tag[..length]);
    }

    fn signature_len(&self) -> usize {
        RESET_KEY_LEN
    }

    // quinn-proto 0.11.19 never calls it: reset tokens are compared with
    // its own constant-time `ResetToken` equality.
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), quinn::crypto::CryptoError> {
        let tag = self.hkdf.hmac_sign(&self.key, data);
        let differences = tag.as_ref().iter().zip(signature).fold(
            u8::from(tag.as_ref().len() != signature.len()),
            |acc, (a, b)| acc | (a ^ b),
        );
        if differences == 0 {
            Ok(())
        } else {
            Err(quinn::crypto::CryptoError)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use wtransport::config::ServerConfigBuilder;
    use wtransport::config::states::WantsTransportConfigServer;

    use super::super::proxy::Proxy;
    use super::super::tls::{Tls, Trust, crypto_provider, tests::private_chain};
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);
    const LIMIT: usize = 64 << 20;

    /// A raw configuration with the build's provider, trusting `root`,
    /// offering `alpn`.
    fn raw(root: Option<&CertificateDer<'static>>, alpn: &[&[u8]]) -> Arc<TlsConfig> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(root) = root {
            roots.add(root.clone()).expect("a root");
        }
        let mut config = TlsConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        Arc::new(config)
    }

    /// A configuration QUIC cannot use: TLS 1.2 only, or TLS 1.3 without
    /// `TLS13_AES_128_GCM_SHA256`.
    fn unfit(tls12: bool) -> Arc<TlsConfig> {
        let mut provider = crypto_provider();
        let versions: &[&rustls::SupportedProtocolVersion] = if tls12 {
            &[&rustls::version::TLS12]
        } else {
            provider
                .cipher_suites
                .retain(|suite| suite.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
            &[&rustls::version::TLS13]
        };
        Arc::new(
            TlsConfig::builder_with_provider(Arc::new(provider))
                .with_protocol_versions(versions)
                .expect("protocol versions")
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    // D4, D5; Block 49 row 53: each kind of TLS settings, copied, offers
    // `h3`, then none; the application's own is left as it was.
    #[test]
    fn the_copy_of_each_tls_setting_offers_h3_then_none_and_leaves_it_unchanged() {
        let (ca, _, _) = private_chain();
        let application = raw(Some(&ca), &[b"h2", b"http/1.1"]);
        let kinds = [
            Tls::new(Trust::Platform),
            Tls::new(Trust::ExtraRoots(vec![ca].into())),
            Tls::new(Trust::Raw(Arc::clone(&application))),
        ];
        for tls in kinds {
            let config = tls.config().expect("TLS settings");
            let before = config.alpn_protocols.clone();

            let first = quic_tls(&config, Alpn::H3);
            let retry = quic_tls(&config, Alpn::None);

            assert_eq!(first.alpn_protocols, [b"h3".to_vec()]);
            assert_eq!(retry.alpn_protocols, Vec::<Vec<u8>>::new());
            assert_eq!(config.alpn_protocols, before);
        }
        assert_eq!(
            application.alpn_protocols,
            [b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    // D4: both checks refuse what quinn-proto would unwrap.
    #[test]
    fn a_configuration_quic_cannot_use_fails_both_checks() {
        for tls12 in [true, false] {
            let checked = check_quic(&quic_tls(&unfit(tls12), Alpn::H3), "localhost");

            let error = checked.expect_err("refused");
            assert!(error.contains("TLS 1.3"), "{error}");
            assert!(error.contains("webtransport"), "{error}");
        }
        assert_eq!(
            check_quic(&quic_tls(&raw(None, &[]), Alpn::H3), "localhost"),
            Ok(())
        );
    }

    /// A UDP socket of 127.0.0.1 counting what it receives, never answering.
    async fn counting_socket() -> (u16, Arc<AtomicUsize>) {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let received = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&received);
        tokio::spawn(async move {
            let mut buffer = vec![0; 65536];
            while socket.recv_from(&mut buffer).await.is_ok() {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        });
        (port, received)
    }

    async fn open(
        url: &str,
        network: &Network,
        headers: &http::HeaderMap,
    ) -> Result<WebTransport, String> {
        WebTransport::open(
            Target {
                url,
                protobuf: false,
                timeout: TIMEOUT,
                headers,
                max_message: LIMIT,
            },
            network,
        )
        .await
    }

    // D4: an attempt handed an unfit configuration, which `build()` would
    // have refused, fails without a datagram, and the process lives.
    #[tokio::test]
    async fn an_attempt_with_an_unfit_configuration_sends_no_datagram() {
        let (port, received) = counting_socket().await;
        for tls12 in [true, false] {
            let network = Network::new(Tls::new(Trust::Raw(unfit(tls12))), Proxy::none());

            let opened = open(
                &format!("https://127.0.0.1:{port}/connection/webtransport"),
                &network,
                &http::HeaderMap::new(),
            )
            .await;

            assert!(matches!(&opened, Err(error) if error.contains("TLS 1.3")));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(received.load(Ordering::SeqCst), 0);
    }

    fn quic_error(code: u64) -> ConnectingError {
        ConnectingError::ConnectionError(ConnectionError::from(
            quinn::ConnectionError::TransportError(
                quinn::TransportErrorCode::crypto(u8::try_from(code - 0x100).expect("an alert"))
                    .into(),
            ),
        ))
    }

    // D5: only the local crypto error 0x178 starts the retry; the peer's
    // 0x78 (no ALPN offered to a strict server), an untrusted certificate
    // (0x12a), a refused session or a timeout do not.
    #[test]
    fn the_retry_starts_only_on_the_local_no_application_protocol_error() {
        let peer_closed = ConnectingError::ConnectionError(ConnectionError::from(
            quinn::ConnectionError::ConnectionClosed(quinn::ConnectionClose {
                error_code: quinn::TransportErrorCode::crypto(0x78),
                frame_type: None,
                reason: bytes::Bytes::new(),
            }),
        ));

        assert!(retries_without_alpn(&quic_error(0x178)));
        assert!(!retries_without_alpn(&quic_error(0x12a)));
        assert!(!retries_without_alpn(&peer_closed));
        assert!(!retries_without_alpn(&ConnectingError::SessionRejected));
        assert!(!retries_without_alpn(&ConnectingError::ConnectionError(
            ConnectionError::TimedOut
        )));
    }

    // D12: the QUIC settings, read back from the configuration.
    #[test]
    fn the_quic_settings_are_set_explicitly() {
        let shown = format!("{:?}", transport_config());

        for field in [
            "max_concurrent_bidi_streams: 0,",
            "max_concurrent_uni_streams: 3,",
            "stream_receive_window: 1048576,",
            "receive_window: 2097152,",
            "send_window: 1048576,",
            "datagram_receive_buffer_size: Some(65536),",
            "keep_alive_interval: Some(15s),",
            "max_idle_timeout: Some(30000),",
        ] {
            assert!(shown.contains(field), "{field} in {shown}");
        }
    }

    // D7.
    #[test]
    fn a_json_frame_gets_one_newline_and_a_protobuf_frame_none() {
        let mut json = br#"{"id":1}"#.to_vec();
        let mut protobuf = vec![2, 8, 1];

        frame_bytes(&mut json, true);
        frame_bytes(&mut protobuf, false);

        assert_eq!(json, b"{\"id\":1}\n");
        assert_eq!(protobuf, [2, 8, 1]);
    }

    // D7: the reader is Block 27's splitters, http_stream's code.
    #[test]
    fn the_reader_is_built_on_the_stream_decoders() {
        assert_eq!(format!("{:?}", decoder(false, LIMIT)), "Decoder::Lines");
        assert_eq!(format!("{:?}", decoder(true, LIMIT)), "Decoder::Varint");
        let mut messages = VecDeque::new();
        decoder(false, 4)
            .push(b"{}\n\r\n{}\r\n", &mut messages)
            .expect("lines");
        assert_eq!(messages, [b"{}".to_vec(), b"{}".to_vec()]);
        assert_eq!(
            decoder(false, 4).push(b"{\"a\":1}\n", &mut messages),
            Err(TOO_LARGE.to_owned())
        );
    }

    // D15: the reset key's signatures are the configuration provider's
    // HMAC-SHA256 over its own random key.
    #[test]
    fn the_reset_key_signs_through_the_configuration_provider() {
        let tls = raw(None, &[]);
        let key = ResetKey::new(&tls).expect("a reset key");
        let provider_hkdf = tls
            .crypto_provider()
            .cipher_suites
            .iter()
            .find_map(|suite| match suite {
                rustls::SupportedCipherSuite::Tls13(suite)
                    if suite.common.suite == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256 =>
                {
                    Some(suite.hkdf_provider)
                }
                _ => None,
            })
            .expect("the suite");
        let mut signature = [0; RESET_KEY_LEN];

        quinn::crypto::HmacKey::sign(&key, b"a connection id", &mut signature);

        assert!(std::ptr::addr_eq(key.hkdf, provider_hkdf));
        assert_eq!(
            signature.as_slice(),
            provider_hkdf
                .hmac_sign(&key.key, b"a connection id")
                .as_ref()
        );
        assert_eq!(quinn::crypto::HmacKey::signature_len(&key), 32);
        assert!(quinn::crypto::HmacKey::verify(&key, b"a connection id", &signature).is_ok());
        assert!(quinn::crypto::HmacKey::verify(&key, b"another id", &signature).is_err());
        // Two keys draw their own bytes.
        let other = ResetKey::new(&tls).expect("a reset key");
        let mut other_signature = [0; RESET_KEY_LEN];
        quinn::crypto::HmacKey::sign(&other, b"a connection id", &mut other_signature);
        assert_ne!(signature, other_signature);
    }

    // D15, Block 6: the fips build's WebTransport configuration is FIPS.
    #[cfg(feature = "fips")]
    #[test]
    fn the_webtransport_configuration_of_the_fips_build_is_fips() {
        let tls = Tls::new(Trust::Platform).config().expect("TLS settings");

        assert!(quic_tls(&tls, Alpn::H3).fips());
        assert!(quic_tls(&tls, Alpn::None).fips());
    }

    /// A WebTransport server on `localhost` presenting `chain`, whose TLS
    /// configuration offers `alpn`: the URL of its session, and how many
    /// QUIC handshakes it saw. Each session's stream is echoed.
    async fn server(
        chain: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
        alpn: &[&[u8]],
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Vec<HashMapHeaders>>>,
    ) {
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3")
            .with_no_client_auth()
            .with_single_cert(vec![chain], key)
            .expect("server configuration");
        tls.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        // Where the client's resolver takes `localhost` first (D6).
        let local = tokio::net::lookup_host(("localhost", 0))
            .await
            .expect("localhost")
            .next()
            .expect("an address");
        let builder: ServerConfigBuilder<WantsTransportConfigServer> =
            wtransport::ServerConfig::builder()
                .with_bind_address(local)
                .with_custom_tls(tls);
        let endpoint = wtransport::Endpoint::server(builder.build()).expect("a server");
        let port = endpoint.local_addr().expect("its address").port();
        let handshakes = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&handshakes);
        let headers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&headers);
        tokio::spawn(async move {
            loop {
                let incoming = endpoint.accept().await;
                counted.fetch_add(1, Ordering::SeqCst);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let Ok(request) = incoming.await else {
                        return;
                    };
                    seen.lock().unwrap().push(request.headers().clone());
                    let Ok(connection) = request.accept().await else {
                        return;
                    };
                    let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                        return;
                    };
                    let _ = tokio::io::copy(&mut recv, &mut send).await;
                    let _ = send.finish().await;
                    let _ = connection.closed().await;
                });
            }
        });
        let url = format!("https://localhost:{port}/connection/webtransport");
        (url, handshakes, headers)
    }

    type HashMapHeaders = std::collections::HashMap<String, String>;

    /// The next message the transport reports, past its written reports.
    async fn next_message(transport: &mut WebTransport) -> Inbound {
        loop {
            match transport.recv(true).await {
                Inbound::Written(_) => {}
                inbound => return inbound,
            }
        }
    }

    // D5: a server requiring `h3` is reached on the attempt's first
    // handshake; D7: a JSON frame goes out as a line and comes back as one
    // message; D14: the real headers go with the CONNECT request.
    #[tokio::test]
    async fn a_server_requiring_h3_is_reached_on_the_first_handshake() {
        let (ca, chain, key) = private_chain();
        let network = Network::new(Tls::new(Trust::ExtraRoots(vec![ca].into())), Proxy::none());
        let mut headers = http::HeaderMap::new();
        headers.insert("x-probe", http::HeaderValue::from_static("b48"));
        let (url, handshakes, seen) = server(chain, key, &[b"h3"]).await;

        let mut transport = open(&url, &network, &headers).await.expect("opened");
        transport.send(FrameId::for_tests(1), br#"{"id":1}"#.to_vec());
        let echoed = tokio::time::timeout(TIMEOUT, next_message(&mut transport))
            .await
            .expect("an echo");

        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
        assert_eq!(echoed, Inbound::Frame(br#"{"id":1}"#.to_vec()));
        assert_eq!(
            seen.lock().unwrap()[0].get("x-probe").map(String::as_str),
            Some("b48")
        );
        transport.close().await;
    }

    // D5: the retry needs the local 0x178 of a server that selected
    // nothing, as Centrifugo's (the @A scenarios). A rustls server with no
    // protocol refuses the offered `h3` itself, in QUIC (rustls 0.23.46
    // `server/hs.rs:121-136`): that peer's 0x178 starts no retry.
    #[tokio::test]
    async fn a_server_refusing_the_offered_alpn_itself_gets_no_second_handshake() {
        let (ca, chain, key) = private_chain();
        let network = Network::new(Tls::new(Trust::ExtraRoots(vec![ca].into())), Proxy::none());
        let (url, handshakes, _) = server(chain, key, &[]).await;

        let opened = open(&url, &network, &http::HeaderMap::new()).await;

        assert!(
            matches!(&opened, Err(error) if error.contains("aborted by peer")),
            "{:?}",
            opened.err()
        );
        assert_eq!(handshakes.load(Ordering::SeqCst), 1);
    }

    // D9: the messages before the end of the stream come first, then the
    // end, with no code.
    #[tokio::test]
    async fn the_end_of_the_stream_comes_after_the_messages_before_it() {
        let (ca, chain, key) = private_chain();
        let network = Network::new(Tls::new(Trust::ExtraRoots(vec![ca].into())), Proxy::none());
        let (url, _, _) = server(chain, key, &[b"h3"]).await;
        let mut transport = open(&url, &network, &http::HeaderMap::new())
            .await
            .expect("opened");
        transport.send(FrameId::for_tests(1), b"{\"a\":1}\n{\"b\":2}".to_vec());
        // The server echoes, then finishes its side once the client did.
        let _ = std::future::poll_fn(|cx| transport.poll_write(cx, false)).await;
        transport.send.finish().await.expect("finished");

        let first = next_message(&mut transport).await;
        let second = next_message(&mut transport).await;
        let end = next_message(&mut transport).await;

        assert_eq!(first, Inbound::Frame(b"{\"a\":1}".to_vec()));
        assert_eq!(second, Inbound::Frame(b"{\"b\":2}".to_vec()));
        assert_eq!(end, lost());
    }

    /// Every field of every record and span, as text (D18).
    #[derive(Clone, Default)]
    struct Capture(Arc<std::sync::Mutex<Vec<String>>>);

    struct Fields<'a>(&'a mut String);

    impl tracing::field::Visit for Fields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write as _;
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut text = format!("{} {}", event.metadata().level(), event.metadata().target());
            event.record(&mut Fields(&mut text));
            self.0.lock().unwrap().push(text);
        }

        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut text = format!("span {}", attributes.metadata().target());
            attributes.record(&mut Fields(&mut text));
            self.0.lock().unwrap().push(text);
        }
    }

    // D18: no record of wtransport or quinn, at TRACE, holds the value of a
    // header the CONNECT request carries.
    #[test]
    fn no_trace_record_holds_a_header_value() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let (ca, chain, key) = private_chain();
        // The server on a thread and runtime of its own, outside the
        // subscriber: wtransport's server logs the request's headers at
        // DEBUG (`driver/mod.rs:606`), a client never.
        let (started, server_ready) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                started.send(server(chain, key, &[b"h3"]).await).unwrap();
                std::future::pending::<()>().await;
            });
        });
        let (url, _, seen) = server_ready.recv().expect("the server");
        // One thread, so that the client endpoint's tasks log to the
        // subscriber.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let network =
                    Network::new(Tls::new(Trust::ExtraRoots(vec![ca].into())), Proxy::none());
                let mut headers = http::HeaderMap::new();
                headers.insert(
                    "x-secret",
                    http::HeaderValue::from_static("a-header-value-of-the-test"),
                );
                let mut transport = open(&url, &network, &headers).await.expect("opened");
                transport.send(FrameId::for_tests(1), br#"{"id":1}"#.to_vec());
                let _ = tokio::time::timeout(TIMEOUT, next_message(&mut transport)).await;
                transport.close().await;
                assert_eq!(seen.lock().unwrap().len(), 1);
            });
        });

        let records = capture.0.lock().unwrap();
        assert!(
            records.iter().any(|record| record.contains("quinn")),
            "no record of quinn captured"
        );
        let leaked: Vec<_> = records
            .iter()
            .filter(|record| record.contains("a-header-value-of-the-test"))
            .collect();
        assert!(leaked.is_empty(), "{leaked:?}");
    }

    // D13: a proxy fails the attempt before any datagram.
    #[tokio::test]
    async fn a_proxy_fails_the_attempt_without_a_datagram() {
        let (port, received) = counting_socket().await;
        let target = super::super::proxy::ProxyTarget::new("http://127.0.0.1:9").expect("a proxy");
        let network = Network::new(Tls::new(Trust::Platform), Proxy::all(target));

        let opened = open(
            &format!("https://127.0.0.1:{port}/connection/webtransport"),
            &network,
            &http::HeaderMap::new(),
        )
        .await;

        assert!(
            matches!(&opened, Err(error) if error == &format!("{THROUGH_PROXY}: http://127.0.0.1:9")),
            "{:?}",
            opened.err()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(received.load(Ordering::SeqCst), 0);
    }
}
