//! The WebTransport relay of the @W harness (Block 48, D17, D19): a
//! wtransport server on `localhost` with the test CA's certificate,
//! offering `h3`, as a browser requires. Centrifugo up to v6.10.3 selects no
//! application protocol, which a browser refuses (Chrome:
//! `NO_APPLICATION_PROTOCOL`), so each session the browser opens here is
//! opened again at Centrifugo's WebTransport endpoint, on the same path and
//! query, with D5's rule (`h3`, then none), and its bidirectional stream is
//! copied both ways. On a switch it refuses every session.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use base64::Engine as _;
use ferrifuge::rustls;
use ferrifuge::rustls::pki_types::pem::PemObject;
use ferrifuge::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::task::JoinHandle;
use wtransport::quinn;

use super::tls;

/// What the relay's switch and counter share with the harness.
#[derive(Default)]
pub(crate) struct RelayState {
    refuse: AtomicBool,
    /// Sessions the relay accepted from browsers.
    sessions: AtomicUsize,
}

pub(crate) struct WebTransportRelay {
    port: u16,
    spki: String,
    state: Arc<RelayState>,
    server: JoinHandle<()>,
}

impl WebTransportRelay {
    /// A relay to the WebTransport endpoint at `upstream`, `host:port` over
    /// UDP of a Centrifugo serving the test CA's certificate.
    pub(crate) async fn start(upstream: String) -> Self {
        let (cert, key) = tls::server_pem();
        let chain: Vec<_> = CertificateDer::pem_slice_iter(cert.as_bytes())
            .collect::<Result<_, _>>()
            .expect("the relay's chain");
        let spki = spki_sha256(&key);
        let key = PrivateKeyDer::from_pem_slice(key.as_bytes()).expect("its key");
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(tls::provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("the relay's TLS configuration");
        config.alpn_protocols = vec![b"h3".to_vec()];
        let local = tokio::net::lookup_host(("localhost", 0))
            .await
            .expect("localhost")
            .next()
            .expect("an address");
        let endpoint = wtransport::Endpoint::server(
            wtransport::ServerConfig::builder()
                .with_bind_address(local)
                .with_custom_tls(config)
                .build(),
        )
        .expect("the WebTransport relay");
        let port = endpoint.local_addr().expect("its address").port();
        let state = Arc::new(RelayState::default());
        let shared = Arc::clone(&state);
        let server = tokio::spawn(async move {
            loop {
                let incoming = endpoint.accept().await;
                tokio::spawn(relay(incoming, Arc::clone(&shared), upstream.clone()));
            }
        });
        Self {
            port,
            spki,
            state,
            server,
        }
    }

    /// The relay's WebTransport endpoint.
    pub(crate) fn url(&self) -> String {
        format!("https://localhost:{}/connection/webtransport", self.port)
    }

    /// `host:port` Chrome forces QUIC on.
    pub(crate) fn origin(&self) -> String {
        format!("localhost:{}", self.port)
    }

    /// The base64 SHA-256 of the relay certificate's public key, as
    /// Chrome's `--ignore-certificate-errors-spki-list` takes it.
    pub(crate) fn spki_sha256(&self) -> &str {
        &self.spki
    }

    pub(crate) fn state(&self) -> Arc<RelayState> {
        Arc::clone(&self.state)
    }
}

impl Drop for WebTransportRelay {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl RelayState {
    /// The relay's commands, under the HTTP relay's control path:
    /// `webtransport-refuse/on` refuses every session until
    /// `webtransport-refuse/off`, `webtransport-sessions` gives how many it
    /// accepted.
    pub(crate) fn control(&self, command: &str) -> Option<String> {
        match command {
            "webtransport-refuse/on" | "webtransport-refuse/off" => {
                self.refuse
                    .store(command.ends_with("/on"), Ordering::SeqCst);
                Some(String::new())
            }
            "webtransport-sessions" => Some(self.sessions.load(Ordering::SeqCst).to_string()),
            _ => None,
        }
    }
}

/// The base64 SHA-256 of the `SubjectPublicKeyInfo` of `key_pem`'s public
/// key, through the build's provider.
fn spki_sha256(key_pem: &str) -> String {
    let key = rcgen::KeyPair::from_pem(key_pem).expect("the relay's key");
    let suite = tls::provider()
        .cipher_suites
        .into_iter()
        .find_map(|suite| match suite {
            rustls::SupportedCipherSuite::Tls13(suite)
                if suite.common.suite == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256 =>
            {
                Some(suite)
            }
            _ => None,
        })
        .expect("TLS13_AES_128_GCM_SHA256");
    let digest = suite
        .common
        .hash_provider
        .hash(&rcgen::PublicKeyData::subject_public_key_info(&key));
    base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
}

/// Opens a session at `upstream`, `127.0.0.1:port`, on `path`, offering
/// `h3` then none (D5). The certificate names 127.0.0.1; `localhost` may
/// resolve to `::1` first, where Docker maps nothing.
async fn open_upstream(upstream: &str, path: &str) -> Option<wtransport::Connection> {
    for alpn in [vec![b"h3".to_vec()], Vec::new()] {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(tls::ca_der()).expect("the test CA");
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(tls::provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3")
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = alpn;
        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(config).expect("QUIC TLS");
        let endpoint = wtransport::Endpoint::client(
            wtransport::ClientConfig::builder()
                .with_bind_address(std::net::SocketAddr::from(([0, 0, 0, 0], 0)))
                .build_with_quic_config(quinn::ClientConfig::new(Arc::new(quic))),
        )
        .ok()?;
        if let Ok(connection) = endpoint.connect(format!("https://{upstream}{path}")).await {
            return Some(connection);
        }
    }
    None
}

/// Relays one browser session.
async fn relay(
    incoming: wtransport::endpoint::IncomingSession,
    state: Arc<RelayState>,
    upstream: String,
) {
    let Ok(request) = incoming.await else {
        return;
    };
    if state.refuse.load(Ordering::SeqCst) {
        request.not_found().await;
        return;
    }
    let Some(server) = open_upstream(&upstream, request.path()).await else {
        request.not_found().await;
        return;
    };
    let Ok(browser) = request.accept().await else {
        return;
    };
    state.sessions.fetch_add(1, Ordering::SeqCst);
    let Ok((mut to_browser, mut from_browser)) = browser.accept_bi().await else {
        return;
    };
    let Ok(opening) = server.open_bi().await else {
        return;
    };
    let Ok((mut to_server, mut from_server)) = opening.await else {
        return;
    };
    let up = async {
        let _ = tokio::io::copy(&mut from_browser, &mut to_server).await;
        let _ = to_server.finish().await;
    };
    let down = async {
        let _ = tokio::io::copy(&mut from_server, &mut to_browser).await;
        let _ = to_browser.finish().await;
    };
    tokio::select! {
        ((), ()) = async { tokio::join!(up, down) } => {}
        _ = browser.closed() => {}
        _ = server.closed() => {}
    }
    browser.close(wtransport::VarInt::from_u32(0), b"");
    server.close(wtransport::VarInt::from_u32(0), b"");
}
