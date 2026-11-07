//! Steps of Block 6, TLS crypto provider, and Block 22, TLS verification.

use std::sync::Arc;

use cucumber::{given, then};
use ferrifuge::machine::TransportKind;
use ferrifuge::rustls;
use ferrifuge::rustls::pki_types::CertificateDer;
use ferrifuge::{Client, ClientBuilder, ClientEvent, ErrorKind};

use super::{ClientWorld, expect_event};
use crate::SERVER_A;
use crate::support::centrifugo::Centrifugo;
use crate::support::tls;

/// An endpoint nothing listens on.
const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

/// Replaces the @A server the scenario started with one serving TLS. The
/// client connects to it directly: the server certificate names `localhost`.
#[given("a server serving TLS with a certificate from the test CA")]
pub(super) async fn a_tls_server(world: &mut ClientWorld) {
    let (cert, key) = tls::server_pem();
    world.relay = None;
    world.server = Some(Centrifugo::start_tls(SERVER_A, &cert, &key).await);
}

#[given("a client configured with the server's wss endpoint")]
fn a_client_with_the_wss_endpoint(world: &mut ClientWorld) {
    let endpoint = world
        .server
        .as_ref()
        .expect("this step needs a server: tag the scenario @A")
        .websocket_url();
    world.new_client(endpoint);
}

// Block 22: TLS verification.

/// The build's crypto provider, as the library chooses it (Block 6).
#[cfg(feature = "fips")]
fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::default_fips_provider()
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "fips")))]
fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(not(feature = "aws-lc-rs"))]
fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// A raw configuration trusting `roots` only, with the build's provider,
/// offering `alpn`.
fn raw_config(roots: &[CertificateDer<'static>], alpn: &[&str]) -> Arc<rustls::ClientConfig> {
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store.add(root.clone()).expect("a root");
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(provider()))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(store)
        .with_no_client_auth();
    config.alpn_protocols = alpn
        .iter()
        .map(|protocol| protocol.as_bytes().to_vec())
        .collect();
    Arc::new(config)
}

fn private_ca() -> CertificateDer<'static> {
    tls::private().ca.clone()
}

/// Builds the scenario's client for `endpoints`, with `tls` applied.
fn tls_client(
    world: &mut ClientWorld,
    endpoints: Vec<(TransportKind, String)>,
    tls: impl FnOnce(ClientBuilder) -> ClientBuilder,
) {
    world.transports = endpoints.iter().map(|(kind, _)| *kind).collect();
    let builder = Client::builder_with_transports(endpoints).config(world.client_config());
    world.build_client(tls(builder));
}

fn tls_server(world: &ClientWorld) -> &Centrifugo {
    world
        .server
        .as_ref()
        .expect("this step needs a server: tag the scenario @A")
}

#[given(
    regex = r"^a server serving TLS with a certificate from a private CA the platform does not trust(?:, with http_stream and sse enabled)?$"
)]
async fn a_private_tls_server(world: &mut ClientWorld) {
    let private = tls::private();
    world.relay = None;
    world.server =
        Some(Centrifugo::start_tls(SERVER_A, &private.server_pem, &private.key_pem).await);
}

#[given(
    regex = r"^a client configured with the server's wss endpoint and (that CA as an extra root certificate|no extra root certificate)$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_extra_roots(world: &mut ClientWorld, roots: String) {
    let url = tls_server(world).websocket_url();
    let extra = roots.starts_with("that CA");
    tls_client(world, vec![(TransportKind::WebSocket, url)], |builder| {
        if extra {
            builder.root_certificates([private_ca()])
        } else {
            builder
        }
    });
}

#[given(
    regex = r"^a client configured with an unreachable websocket endpoint, only the server's https (\w+) endpoint, and that CA as an extra root certificate$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_over_https_with_extra_roots(world: &mut ClientWorld, transport: String) {
    let server = tls_server(world);
    let (kind, url) = match transport.as_str() {
        "http_stream" => (TransportKind::HttpStream, server.http_stream_url()),
        "sse" => (TransportKind::Sse, server.sse_url()),
        other => panic!("an HTTPS transport, not {other}"),
    };
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE.to_owned()),
        (kind, url),
    ];
    tls_client(world, endpoints, |builder| {
        builder.root_certificates([private_ca()])
    });
}

#[given(
    regex = r#"^a client configured with only the server's (\w+) endpoint and a raw rustls configuration trusting only that CA, offering ALPN "h2" and "http/1.1"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_a_raw_configuration(world: &mut ClientWorld, transport: String) {
    let server = tls_server(world);
    let endpoint = match transport.as_str() {
        "websocket" => (TransportKind::WebSocket, server.websocket_url()),
        "http_stream" => (TransportKind::HttpStream, server.http_stream_url()),
        other => panic!("a transport of the scenario, not {other}"),
    };
    let raw = raw_config(&[private_ca()], &["h2", "http/1.1"]);
    tls_client(world, vec![endpoint], |builder| builder.tls_config(raw));
}

#[given(
    "a client configured with the server's wss endpoint and a raw rustls configuration trusting only a private CA the platform does not trust"
)]
fn a_client_whose_raw_configuration_trusts_another_ca(world: &mut ClientWorld) {
    let url = tls_server(world).websocket_url();
    let raw = raw_config(&[private_ca()], &[]);
    tls_client(world, vec![(TransportKind::WebSocket, url)], |builder| {
        builder.tls_config(raw)
    });
}

#[then(
    r#"the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted"#
)]
async fn an_untrusted_certificate_error(world: &mut ClientWorld) {
    expect_event(world, "connecting").await;
    let event = expect_event(world, "error").await;
    let ClientEvent::Error(error) = &event else {
        unreachable!("expect_event checked the name");
    };
    // rustls's `Error::InvalidCertificate`, through tungstenite.
    assert!(
        error.message.contains("invalid peer certificate"),
        "{event:?}"
    );
}

#[given("client options with an extra root certificate and a raw rustls configuration")]
fn options_with_roots_and_a_raw_configuration(world: &mut ClientWorld) {
    world.pending_builder = Some(
        Client::builder("wss://localhost/connection/websocket")
            .root_certificates([private_ca()])
            .tls_config(raw_config(&[], &[])),
    );
}

#[given(
    "client options with a valid extra root certificate followed by one whose bytes are not a certificate"
)]
fn options_with_an_unparsable_root(world: &mut ClientWorld) {
    world.pending_builder = Some(
        Client::builder("wss://localhost/connection/websocket").root_certificates([
            private_ca(),
            CertificateDer::from(b"not a certificate".to_vec()),
        ]),
    );
}

#[then(
    regex = r"^building fails with a configuration error naming (both|extra root certificate (\d+))$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn building_fails_naming(world: &mut ClientWorld, naming: String, index: String) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    if naming == "both" {
        assert!(
            error.message.contains("extra root certificates")
                && error.message.contains("TLS configuration"),
            "{error:?}"
        );
    } else {
        assert!(
            error
                .message
                .contains(&format!("extra root certificate {index} ")),
            "{error:?}"
        );
    }
}
