//! Block 48, D18: no record of wtransport or quinn, at TRACE, holds the
//! value of a header the client's WebTransport CONNECT request carries. In
//! its own binary, which sets the process's `tracing` subscriber: with a
//! subscriber scoped to one test, tests running alongside left tracing's
//! cached callsite interest of quinn's records at "never" (seen in about
//! one run of three).
#![cfg(feature = "unstable-webtransport")]

mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferrifuge::machine::TransportKind;
use ferrifuge::rustls;
use ferrifuge::rustls::pki_types::pem::PemObject;
use ferrifuge::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use ferrifuge::{Client, Config, Proxy};
use support::tls;
use tracing_subscriber::layer::SubscriberExt as _;

/// The value of the header the test looks for.
const SECRET: &str = "a-header-value-of-the-test";
/// The thread of the server, whose records are left out: wtransport's
/// server logs the request's headers at DEBUG (`driver/mod.rs:606`).
const SERVER_THREAD: &str = "webtransport-server";

/// Every field of every record and span of the client, as text.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<String>>>);

struct Fields<'a>(&'a mut String);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}

impl Capture {
    fn keep(&self, text: String) {
        if std::thread::current().name() != Some(SERVER_THREAD) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(text);
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut text = format!("{} {}", event.metadata().level(), event.metadata().target());
        event.record(&mut Fields(&mut text));
        self.keep(text);
    }

    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut text = format!("span {}", attributes.metadata().target());
        attributes.record(&mut Fields(&mut text));
        self.keep(text);
    }
}

/// A WebTransport server on `localhost` presenting the private test CA's
/// certificate, on a thread of its own: its URL, and whether it has
/// accepted a session.
fn server() -> (String, Arc<AtomicBool>) {
    let private = tls::private();
    let chain: Vec<_> = CertificateDer::pem_slice_iter(private.server_pem.as_bytes())
        .collect::<Result<_, _>>()
        .expect("the server's chain");
    let key = PrivateKeyDer::from_pem_slice(private.key_pem.as_bytes()).expect("its key");
    let accepted = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&accepted);
    let (started, ready) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(SERVER_THREAD.to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let mut config =
                    rustls::ServerConfig::builder_with_provider(Arc::new(tls::provider()))
                        .with_protocol_versions(&[&rustls::version::TLS13])
                        .expect("TLS 1.3")
                        .with_no_client_auth()
                        .with_single_cert(chain, key)
                        .expect("server configuration");
                config.alpn_protocols = vec![b"h3".to_vec()];
                // Where the client takes `localhost` first (D6).
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
                .expect("a server");
                let port = endpoint.local_addr().expect("its address").port();
                started
                    .send(format!("https://localhost:{port}/connection/webtransport"))
                    .unwrap();
                loop {
                    let incoming = endpoint.accept().await;
                    let seen = Arc::clone(&seen);
                    tokio::spawn(async move {
                        let Ok(request) = incoming.await else {
                            return;
                        };
                        let Ok(connection) = request.accept().await else {
                            return;
                        };
                        seen.store(true, Ordering::SeqCst);
                        let _ = connection.accept_bi().await;
                        let _ = connection.closed().await;
                    });
                }
            });
        })
        .expect("the server thread");
    (ready.recv().expect("the server's URL"), accepted)
}

#[tokio::test]
async fn no_client_record_holds_a_header_value() {
    let capture = Capture::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
        .expect("the only subscriber");
    let (url, accepted) = server();
    let client = Client::builder_with_transports([(TransportKind::WebTransport, url)])
        .config(Config::default())
        .root_certificates([tls::private().ca.clone()])
        .proxy(Proxy::none())
        .http_headers([("x-secret", SECRET)])
        .build()
        .expect("a client");

    client.connect();
    let session = tokio::time::timeout(Duration::from_secs(10), async {
        while !accepted.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Some more records of the open stream.
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.disconnect();
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(session.is_ok(), "no session");
    let records = capture
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    for crate_name in ["quinn", "wtransport"] {
        assert!(
            records.iter().any(|record| record.contains(crate_name)),
            "no record of {crate_name} captured"
        );
    }
    let leaked: Vec<_> = records
        .iter()
        .filter(|record| record.contains(SECRET))
        .collect();
    assert!(leaked.is_empty(), "{leaked:?}");
}
