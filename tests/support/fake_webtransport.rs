//! Fake WebTransport server for @D scenarios (Block 48, D19): a wtransport
//! server presenting the test CA's certificate (Block 6), whose sessions run the
//! script engine of [`super::fake_server`]. Each session's bidirectional
//! stream is bridged to a websocket connection to a [`FakeServer`], cut at
//! D7's frames: JSON lines each ended by `\n`, Protobuf commands each
//! prefixed with a varint length. So every script of the fake websocket
//! server (pings, pushes, publications) runs unchanged, and what only a
//! WebTransport server does is a switch of this one: the ALPN it selects,
//! the handshakes it counted, a stream it stops reading or ends, a session
//! it refuses.
//!
//! The ALPN: with `h3`, a rustls configuration with `["h3"]`. With none, as
//! Centrifugo v6.10.1, which selects no application protocol: a plain
//! rustls server refuses a client offering one in QUIC (rustls 0.23.46
//! `server/hs.rs:121-136`), so [`AlpnSwitch`] wraps quinn's crypto. A
//! client offering no ALPN gets a configuration without one; a client
//! offering one gets a configuration with `["h3"]` whose `EncryptedExtensions`
//! go out without the ALPN extension, so the client fails locally with
//! 0x178, as against Centrifugo (rustls checks ALPN on `EncryptedExtensions`,
//! before `CertificateVerify` and Finished, `client/tls13.rs:498-508`). The
//! wrapper edits plaintext handshake messages before quinn protects them,
//! and does no cryptography.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ferrifuge::rustls;
use ferrifuge::rustls::pki_types::pem::PemObject;
use ferrifuge::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use quinn_proto::TransportError;
use quinn_proto::transport_parameters::TransportParameters;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use wtransport::quinn;
use wtransport::quinn::crypto::{
    self, ExportKeyingMaterialError, HeaderKey, KeyPair, Keys, PacketKey, UnsupportedVersion,
};
use wtransport::quinn::{ConnectionId, Side};

use super::fake_server::FakeServer;
use super::{protobuf, tls};

/// The flow-control window of the stream the server reads (D19): small,
/// so a client whose writes the server stops reading stalls soon.
pub(crate) const STREAM_WINDOW: u32 = 16 * 1024;

/// TLS's extension type of ALPN (RFC 7301).
const ALPN_EXTENSION: u16 = 16;
/// TLS handshake message types (RFC 8446 §4).
const CLIENT_HELLO: u8 = 1;
const ENCRYPTED_EXTENSIONS: u8 = 8;

/// An instruction to the current session.
enum Control {
    /// Writes these bytes, then ends the stream in the same write; with
    /// `close`, then closes the session too, as Centrifugo does.
    End { bytes: Vec<u8>, close: bool },
    /// Writes these bytes in three writes, a pause between them.
    WriteSplit(Vec<u8>),
    /// Stops reading the stream.
    StopReading,
}

#[derive(Default)]
struct State {
    /// QUIC connection attempts the endpoint was offered.
    handshakes: usize,
    /// The address each of them came from, in order.
    remotes: Vec<std::net::SocketAddr>,
    /// Instructions to the latest session, if one is open.
    current: Option<mpsc::UnboundedSender<Control>>,
    /// Every new stream is ended before the connect command is answered.
    end_before_connect: bool,
    /// Every session request is refused (404).
    refuse: bool,
    /// When the latest session's stream was finished by the client.
    stream_finished_at: Option<Instant>,
    /// When the latest session's QUIC connection closed.
    connection_closed_at: Option<Instant>,
}

pub(crate) struct FakeWebTransport {
    port: u16,
    state: Arc<Mutex<State>>,
    no_alpn: Arc<AtomicBool>,
    server: JoinHandle<()>,
}

impl FakeWebTransport {
    /// A server on `localhost`, selecting `h3` or, with `no_alpn`, none,
    /// whose sessions are bridged to `fake`.
    pub(crate) async fn start(fake: &FakeServer, no_alpn: bool) -> Self {
        // The test CA's certificate, which `SSL_CERT_FILE` makes the
        // platform trust (Block 6).
        let (cert, key) = tls::server_pem();
        let chain: Vec<_> = CertificateDer::pem_slice_iter(cert.as_bytes())
            .collect::<Result<_, _>>()
            .expect("the server's chain");
        let key = PrivateKeyDer::from_pem_slice(key.as_bytes()).expect("its key");
        let without = rustls::ServerConfig::builder_with_provider(Arc::new(tls::provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server configuration");
        let mut with_h3 = without.clone();
        with_h3.alpn_protocols = vec![b"h3".to_vec()];
        let mut transport = quinn::TransportConfig::default();
        transport.stream_receive_window(STREAM_WINDOW.into());
        // Where the client's resolver takes `localhost` first (D6).
        let local = tokio::net::lookup_host(("localhost", 0))
            .await
            .expect("localhost")
            .next()
            .expect("an address");
        let mut config = wtransport::ServerConfig::builder()
            .with_bind_address(local)
            .with_custom_tls_and_transport(with_h3.clone(), transport)
            .build();
        let no_alpn = Arc::new(AtomicBool::new(no_alpn));
        config.quic_config_mut().crypto = Arc::new(AlpnSwitch {
            h3: Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(with_h3).expect("QUIC TLS"),
            ),
            none: Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(without).expect("QUIC TLS"),
            ),
            no_alpn: Arc::clone(&no_alpn),
        });
        let endpoint = wtransport::Endpoint::server(config).expect("a WebTransport server");
        let port = endpoint.local_addr().expect("its address").port();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        let websocket = format!("ws://{}/connection/websocket", fake.address());
        let server = tokio::spawn(async move {
            loop {
                let incoming = endpoint.accept().await;
                {
                    let mut state = lock(&shared);
                    state.handshakes += 1;
                    state.remotes.push(incoming.remote_address());
                }
                tokio::spawn(serve(incoming, Arc::clone(&shared), websocket.clone()));
            }
        });
        Self {
            port,
            state,
            no_alpn,
            server,
        }
    }

    /// The WebTransport endpoint to configure a client with.
    pub(crate) fn url(&self) -> String {
        format!("https://localhost:{}/connection/webtransport", self.port)
    }

    /// QUIC connection attempts counted so far.
    pub(crate) fn handshakes(&self) -> usize {
        lock(&self.state).handshakes
    }

    /// The address each QUIC connection attempt came from, in order.
    pub(crate) fn remotes(&self) -> Vec<std::net::SocketAddr> {
        lock(&self.state).remotes.clone()
    }

    /// From the next handshake on, selects `h3` or, with `no_alpn`, none.
    pub(crate) fn select_no_alpn(&self, no_alpn: bool) {
        self.no_alpn.store(no_alpn, Ordering::SeqCst);
    }

    /// Ends every new stream before the connect command is answered.
    pub(crate) fn end_streams_before_connect(&self) {
        lock(&self.state).end_before_connect = true;
    }

    /// Refuses every session request from now on.
    pub(crate) fn refuse_sessions(&self) {
        lock(&self.state).refuse = true;
    }

    /// Writes `bytes`, then ends the current stream in the same write; with
    /// `close`, closes the session after it.
    pub(crate) fn end_stream(&self, bytes: Vec<u8>, close: bool) {
        self.control(Control::End { bytes, close });
    }

    /// Writes `bytes` over the current stream in three writes.
    pub(crate) fn write_split(&self, bytes: Vec<u8>) {
        self.control(Control::WriteSplit(bytes));
    }

    /// Stops reading the current stream: once its window is full, the
    /// client's writes stall.
    pub(crate) fn stop_reading(&self) {
        self.control(Control::StopReading);
    }

    /// When the current session's stream was finished by the client, and
    /// when its connection closed.
    pub(crate) fn ends(&self) -> (Option<Instant>, Option<Instant>) {
        let state = lock(&self.state);
        (state.stream_finished_at, state.connection_closed_at)
    }

    fn control(&self, control: Control) {
        lock(&self.state)
            .current
            .as_ref()
            .expect("no session on the fake WebTransport server")
            .send(control)
            .unwrap_or_else(|_| panic!("the fake WebTransport session is gone"));
    }
}

impl std::fmt::Debug for FakeWebTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeWebTransport")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Drop for FakeWebTransport {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn lock(state: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A JSON disconnect push with `code` and `reason`, as a line.
pub(crate) fn disconnect_push(code: u32, reason: &str) -> Vec<u8> {
    let mut line =
        serde_json::json!({"push": {"disconnect": {"code": code, "reason": reason}}}).to_string();
    line.push('\n');
    line.into_bytes()
}

/// A publication push on `channel` whose message is exactly `size` bytes,
/// framed as D7 frames it: a JSON line with its `\n`, or a Protobuf reply
/// after its varint length.
pub(crate) fn sized_publication(channel: &str, size: usize, protobuf: bool) -> Vec<u8> {
    if !protobuf {
        let prefix = format!(r#"{{"push":{{"channel":"{channel}","pub":{{"data":""#);
        let mut line = super::fake_server::sized_json(&prefix, "\"}}}", size);
        line.push('\n');
        return line.into_bytes();
    }
    let reply = |data: usize| protobuf::Reply {
        push: Some(protobuf::Push {
            channel: channel.to_owned(),
            r#pub: Some(protobuf::Publication {
                data: vec![b'x'; data],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let data = (0..size)
        .rev()
        .find(|&data| reply(data).encoded_len() == size)
        .expect("a payload that makes the size");
    reply(data).encode_length_delimited_to_vec()
}

/// Serves one QUIC connection: its session, then its stream bridged to the
/// fake websocket server at `websocket`.
#[allow(clippy::too_many_lines)] // One select over both sides and the controls.
async fn serve(
    incoming: wtransport::endpoint::IncomingSession,
    state: Arc<Mutex<State>>,
    websocket: String,
) {
    let Ok(request) = incoming.await else {
        return;
    };
    if lock(&state).refuse {
        request.not_found().await;
        return;
    }
    let protobuf = request.path().contains("cf_protocol=protobuf");
    let Ok(connection) = request.accept().await else {
        return;
    };
    let Ok((mut send, mut recv)) = connection.accept_bi().await else {
        return;
    };
    let (controls, mut commands) = mpsc::unbounded_channel();
    let end_before_connect = {
        let mut state = lock(&state);
        state.current = Some(controls);
        state.stream_finished_at = None;
        state.connection_closed_at = None;
        state.end_before_connect
    };
    if end_before_connect {
        let _ = send.finish().await;
        let _ = connection.closed().await;
        return;
    }
    let mut request = websocket
        .into_client_request()
        .expect("a websocket request");
    if protobuf {
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(protobuf::SUBPROTOCOL),
        );
    }
    let Ok((ws, _)) = tokio_tungstenite::connect_async(request).await else {
        return;
    };
    let (mut sink, mut stream) = ws.split();
    let mut buffer = vec![0; 64 * 1024];
    let mut read = Vec::new();
    let mut reading = true;
    loop {
        tokio::select! {
            got = recv.read(&mut buffer), if reading => {
                // The client finished its stream (D11), or reset it.
                let Ok(Some(n)) = got else {
                    lock(&state).stream_finished_at = Some(Instant::now());
                    reading = false;
                    let _ = sink.close().await;
                    continue;
                };
                read.extend_from_slice(&buffer[..n]);
                let frame = complete_frames(&mut read, protobuf);
                if !frame.is_empty() {
                    let message = if protobuf {
                        Message::binary(frame)
                    } else {
                        Message::text(String::from_utf8_lossy(&frame).into_owned())
                    };
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
            },
            message = stream.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    let mut line = text.as_bytes().to_vec();
                    line.push(b'\n');
                    if send.write_all(&line).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    if send.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                // The fake websocket server closed its side.
                Some(Err(_)) | None => {
                    let _ = send.finish().await;
                    break;
                }
            },
            control = commands.recv() => match control {
                Some(Control::End { bytes, close }) => {
                    let _ = send.write_all(&bytes).await;
                    let _ = send.finish().await;
                    if close {
                        connection.close(wtransport::VarInt::from_u32(0), b"");
                    }
                    break;
                }
                Some(Control::WriteSplit(bytes)) => {
                    let third = bytes.len().div_ceil(3);
                    for part in bytes.chunks(third) {
                        if send.write_all(part).await.is_err() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
                Some(Control::StopReading) => reading = false,
                None => break,
            },
            _ = connection.closed() => break,
        }
    }
    let _ = sink.close().await;
    let _ = connection.closed().await;
    lock(&state).connection_closed_at = Some(Instant::now());
}

/// The complete frames at the start of `read`, removed from it: whole
/// JSON lines without their last `\n`, or whole varint-delimited Protobuf
/// commands.
fn complete_frames(read: &mut Vec<u8>, protobuf: bool) -> Vec<u8> {
    let end = if protobuf {
        let mut at = 0;
        loop {
            let Some((length, prefix)) = varint(&read[at..]) else {
                break at;
            };
            let Some(next) = at
                .checked_add(prefix)
                .and_then(|start| start.checked_add(usize::try_from(length).ok()?))
                .filter(|&next| next <= read.len())
            else {
                break at;
            };
            at = next;
        }
    } else {
        read.iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(0, |at| at + 1)
    };
    let rest = read.split_off(end);
    let mut frames = std::mem::replace(read, rest);
    if !protobuf && frames.last() == Some(&b'\n') {
        frames.pop();
    }
    frames
}

/// A varint at the start of `bytes`, and its length; none if incomplete.
fn varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (index, &byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    None
}

/// quinn's server crypto choosing, per handshake, the ALPN the fake server
/// selects (module documentation).
struct AlpnSwitch {
    h3: Arc<dyn crypto::ServerConfig>,
    none: Arc<dyn crypto::ServerConfig>,
    /// Read when a handshake starts.
    no_alpn: Arc<AtomicBool>,
}

impl crypto::ServerConfig for AlpnSwitch {
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<Keys, UnsupportedVersion> {
        self.h3.initial_keys(version, dst_cid)
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        self.h3.retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn crypto::Session> {
        if !self.no_alpn.load(Ordering::SeqCst) {
            return Arc::clone(&self.h3).start_session(version, params);
        }
        Box::new(NoAlpnSession {
            h3: Some(Arc::clone(&self.h3).start_session(version, params)),
            none: Some(Arc::clone(&self.none).start_session(version, params)),
            hello: Vec::new(),
            strip: false,
        })
    }
}

/// A server handshake selecting no ALPN: both sessions until the
/// `ClientHello` shows whether the client offers ALPN, then one of them.
struct NoAlpnSession {
    /// Kept for a client offering ALPN.
    h3: Option<Box<dyn crypto::Session>>,
    /// Kept for a client offering none.
    none: Option<Box<dyn crypto::Session>>,
    /// The `ClientHello`'s bytes, until it is complete.
    hello: Vec<u8>,
    /// The `EncryptedExtensions` still to go out without ALPN.
    strip: bool,
}

impl NoAlpnSession {
    fn undecided(&self) -> bool {
        self.h3.is_some() && self.none.is_some()
    }

    fn inner(&self) -> &dyn crypto::Session {
        self.none
            .as_deref()
            .or(self.h3.as_deref())
            .expect("one session kept")
    }

    fn inner_mut(&mut self) -> &mut dyn crypto::Session {
        if let Some(none) = self.none.as_mut() {
            return none.as_mut();
        }
        self.h3.as_mut().expect("one session kept").as_mut()
    }
}

impl crypto::Session for NoAlpnSession {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> Keys {
        self.inner().initial_keys(dst_cid, side)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        self.inner().handshake_data()
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner().peer_identity()
    }

    fn early_crypto(&self) -> Option<(Box<dyn HeaderKey>, Box<dyn PacketKey>)> {
        self.inner().early_crypto()
    }

    fn early_data_accepted(&self) -> Option<bool> {
        self.inner().early_data_accepted()
    }

    fn is_handshaking(&self) -> bool {
        self.inner().is_handshaking()
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        if !self.undecided() {
            return self.inner_mut().read_handshake(buf);
        }
        self.hello.extend_from_slice(buf);
        let Some(length) = Reader::new(&self.hello).message_length() else {
            return Ok(false);
        };
        if self.hello.len() < length {
            return Ok(false);
        }
        // A ClientHello that does not parse goes to the configuration
        // without ALPN, whose rustls refuses it.
        if offers_alpn(&self.hello[..length]) == Some(true) {
            self.none = None;
            self.strip = true;
        } else {
            self.h3 = None;
        }
        let hello = std::mem::take(&mut self.hello);
        self.inner_mut().read_handshake(&hello)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        if self.undecided() {
            return Ok(None);
        }
        self.inner().transport_parameters()
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        let start = buf.len();
        let keys = self.inner_mut().write_handshake(buf);
        if self.strip && buf.len() > start {
            let written = buf.split_off(start);
            match without_alpn(&written) {
                Some(edited) => {
                    self.strip = false;
                    buf.extend_from_slice(&edited);
                }
                None => buf.extend_from_slice(&written),
            }
        }
        keys
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn PacketKey>>> {
        self.inner_mut().next_1rtt_keys()
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner().is_valid_retry(orig_dst_cid, header, payload)
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        self.inner().export_keying_material(output, label, context)
    }
}

/// Checked reads of a TLS handshake message.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(count)?;
        let taken = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(taken)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|bytes| bytes[0])
    }

    fn u16(&mut self) -> Option<u16> {
        self.take(2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u24(&mut self) -> Option<usize> {
        self.take(3).map(|bytes| {
            usize::from(bytes[0]) << 16 | usize::from(bytes[1]) << 8 | usize::from(bytes[2])
        })
    }

    /// The whole length of the handshake message starting here, header
    /// included.
    fn message_length(mut self) -> Option<usize> {
        self.u8()?;
        self.u24()?.checked_add(4)
    }

    /// Skips a vector with a length prefix of `prefix` bytes.
    fn skip_vector(&mut self, prefix: usize) -> Option<()> {
        let length = match prefix {
            1 => usize::from(self.u8()?),
            _ => usize::from(self.u16()?),
        };
        self.take(length).map(|_| ())
    }

    /// The extensions of the extension block starting here, each as its
    /// type and its whole bytes.
    fn extensions(&mut self) -> Option<Vec<(u16, &'a [u8])>> {
        let length = usize::from(self.u16()?);
        let mut block = Reader::new(self.take(length)?);
        let mut extensions = Vec::new();
        while block.at < block.bytes.len() {
            let start = block.at;
            let kind = block.u16()?;
            let data = usize::from(block.u16()?);
            block.take(data)?;
            extensions.push((kind, &block.bytes[start..block.at]));
        }
        Some(extensions)
    }
}

/// Whether the `ClientHello` `hello` offers ALPN; none if it does not parse.
fn offers_alpn(hello: &[u8]) -> Option<bool> {
    let mut reader = Reader::new(hello);
    if reader.u8()? != CLIENT_HELLO {
        return None;
    }
    reader.u24()?;
    // legacy_version, random, legacy_session_id, cipher_suites,
    // legacy_compression_methods (RFC 8446 §4.1.2).
    reader.take(2 + 32)?;
    reader.skip_vector(1)?;
    reader.skip_vector(2)?;
    reader.skip_vector(1)?;
    let extensions = reader.extensions()?;
    Some(extensions.iter().any(|(kind, _)| *kind == ALPN_EXTENSION))
}

/// `messages` with the ALPN extension removed from the `EncryptedExtensions`
/// at their start; none if they do not start with one that parses.
fn without_alpn(messages: &[u8]) -> Option<Vec<u8>> {
    let mut reader = Reader::new(messages);
    if reader.u8()? != ENCRYPTED_EXTENSIONS {
        return None;
    }
    let length = reader.u24()?;
    let end = length.checked_add(4)?;
    let mut body = Reader::new(messages.get(4..end)?);
    let kept: Vec<u8> = body
        .extensions()?
        .into_iter()
        .filter(|(kind, _)| *kind != ALPN_EXTENSION)
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect();
    let kept_length = u16::try_from(kept.len()).ok()?;
    let body_length = u32::try_from(kept.len() + 2).ok()?;
    let mut edited = vec![ENCRYPTED_EXTENSIONS];
    edited.extend_from_slice(&body_length.to_be_bytes()[1..]);
    edited.extend_from_slice(&kept_length.to_be_bytes());
    edited.extend_from_slice(&kept);
    edited.extend_from_slice(messages.get(end..)?);
    Some(edited)
}
