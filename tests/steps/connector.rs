//! Steps of Block 37b: an application's connector gives the websocket its
//! stream (@D over the fake server and a TLS listener in front of it; @E
//! for the build checks).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then};
use ferrifuge::machine::TransportKind;
use ferrifuge::{Client, ClientBuilder, ClientEvent, ErrorKind, Protocol, Proxy, codes};

use super::ClientWorld;
use crate::support::tls;

const TOLERANCE: Duration = Duration::from_secs(1);

/// What a 37b scenario's connector saw, and the TLS listener it reaches.
#[derive(Debug, Default)]
pub(crate) struct Connectors {
    /// The endpoints the connector was given, in order.
    endpoints: Arc<Mutex<Vec<String>>>,
    /// The future of the first call was dropped.
    dropped: Arc<AtomicBool>,
    /// The address of the TLS listener in front of the fake server.
    tls_listener: Option<String>,
    /// The handshake timeout of the scenario's client.
    handshake: Option<Duration>,
}

/// What the connector does on its first call.
#[derive(Clone, Copy)]
enum First {
    Connect,
    Fail,
    Panic,
    Hang,
}

/// A connector that records the endpoints it is given and opens a TCP
/// connection to `target`, but on its first call does as `first` says.
fn connector(
    world: &ClientWorld,
    target: String,
    first: First,
) -> impl Fn(
    ferrifuge::http::Uri,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = std::io::Result<tokio::net::TcpStream>> + Send>,
> + Send
+ Sync
+ 'static {
    let endpoints = Arc::clone(&world.connectors.endpoints);
    let dropped = Arc::clone(&world.connectors.dropped);
    let calls = Arc::new(AtomicUsize::new(0));
    move |endpoint: ferrifuge::http::Uri| {
        endpoints.lock().unwrap().push(endpoint.to_string());
        let call = calls.fetch_add(1, Ordering::SeqCst);
        let target = target.clone();
        let dropped = Arc::clone(&dropped);
        Box::pin(async move {
            match (call, first) {
                (0, First::Fail) => Err(std::io::Error::other("tunnel down")),
                (0, First::Panic) => panic!("a connector that panics"),
                (0, First::Hang) => {
                    let _guard = DropFlag(dropped);
                    std::future::pending::<()>().await;
                    unreachable!("never finishes")
                }
                _ => tokio::net::TcpStream::connect(target).await,
            }
        })
    }
}

/// Sets its flag when dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn built(world: &mut ClientWorld, builder: ClientBuilder) {
    world.build_client(builder);
}

#[given("the fake server")]
fn the_fake_server(world: &mut ClientWorld) {
    let _ = world.fake();
}

#[given(
    regex = r#"^a client configured with the (JSON|Protobuf) protocol and the endpoint "([^"]+)", with a connector that opens a TCP connection to the fake server and records the endpoints it is given$"#
)]
fn a_client_with_a_connector(world: &mut ClientWorld, protocol: String, endpoint: String) {
    world.protocol = if protocol == "Protobuf" {
        Protocol::Protobuf
    } else {
        Protocol::Json
    };
    let connector = connector(world, world.fake().address(), First::Connect);
    let builder = Client::builder(endpoint)
        .config(world.client_config())
        .connector(connector);
    built(world, builder);
}

#[given(
    regex = r#"^a client configured with the endpoint "([^"]+)", with a connector that (returns the error "tunnel down"|panics) on its first call and opens a TCP connection to the fake server on later calls$"#
)]
fn a_client_with_a_failing_connector(world: &mut ClientWorld, endpoint: String, fails: String) {
    let first = if fails == "panics" {
        First::Panic
    } else {
        First::Fail
    };
    let connector = connector(world, world.fake().address(), first);
    let builder = Client::builder(endpoint)
        .config(world.client_config())
        .connector(connector);
    built(world, builder);
}

#[given(
    regex = r#"^a client configured with the endpoint "([^"]+)" and a handshake timeout of (\d+) ms, with a connector whose first call never finishes and whose later calls open a TCP connection to the fake server$"#
)]
fn a_client_with_a_hanging_connector(world: &mut ClientWorld, endpoint: String, millis: u64) {
    let handshake = Duration::from_millis(millis);
    world.connectors.handshake = Some(handshake);
    let connector = connector(world, world.fake().address(), First::Hang);
    let mut config = world.client_config();
    config.handshake_timeout = handshake;
    let builder = Client::builder(endpoint)
        .config(config)
        .connector(connector);
    built(world, builder);
}

#[given(
    regex = r#"^the fake server behind a TLS listener whose certificate for "centrifugo.invalid" is issued by (the test CA|the private CA)$"#
)]
async fn a_tls_listener(world: &mut ClientWorld, issuer: String) {
    let config = if issuer == "the test CA" {
        tls::server_config()
    } else {
        private_server_config()
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("an address").to_string();
    let target = world.fake().address();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let (acceptor, target) = (acceptor.clone(), target.clone());
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                if let Ok(mut server) = tokio::net::TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut server).await;
                }
            });
        }
    });
    world.connectors.tls_listener = Some(address);
}

/// The private CA's server certificate, which the platform does not trust.
fn private_server_config() -> Arc<rustls::ServerConfig> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let private = tls::private();
    let chain =
        vec![CertificateDer::from_pem_slice(private.server_pem.as_bytes()).expect("a certificate")];
    let key = PrivateKeyDer::from_pem_slice(private.key_pem.as_bytes()).expect("a key");
    Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(tls::provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server configuration"),
    )
}

#[given(
    regex = r#"^a client configured with the endpoint "(wss://[^"]+)" and the test CA as an extra root certificate, with a connector that opens a TCP connection to that listener$"#
)]
fn a_client_over_tls(world: &mut ClientWorld, endpoint: String) {
    let listener = world
        .connectors
        .tls_listener
        .clone()
        .expect("a TLS listener");
    let connector = connector(world, listener, First::Connect);
    let builder = Client::builder(endpoint)
        .config(world.client_config())
        .root_certificates([tls::ca_der()])
        .connector(connector);
    built(world, builder);
}

#[then(regex = r#"^the connector was called once, with "([^"]+)"$"#)]
fn called_once_with(world: &mut ClientWorld, endpoint: String) {
    assert_eq!(*world.connectors.endpoints.lock().unwrap(), [endpoint]);
}

#[then("the connector was called twice")]
async fn called_twice(world: &mut ClientWorld) {
    // The second call may still be on its way when the event is read.
    let calls = || world.connectors.endpoints.lock().unwrap().len();
    let deadline = Instant::now() + Duration::from_secs(5);
    while calls() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(calls(), 2);
}

#[then(regex = r"^the fake server received a connect command in the (JSON|Protobuf) encoding$")]
fn connect_in_encoding(world: &mut ClientWorld, protocol: String) {
    if protocol == "Protobuf" {
        let received = world.fake().received_protobuf_with_frames();
        assert!(
            received
                .iter()
                .any(|(command, _)| command.connect.is_some()),
            "{received:?}"
        );
        assert_eq!(world.fake().received(), Vec::<serde_json::Value>::new());
        // Each upgrade asked for the Protobuf subprotocol.
        let upgrades = world.fake().protobuf_upgrades();
        assert!(
            !upgrades.is_empty() && upgrades.iter().all(|asked| *asked),
            "{upgrades:?}"
        );
    } else {
        let received = world.fake().received();
        assert!(
            received
                .iter()
                .any(|command| command["connect"].is_object()),
            "{received:?}"
        );
        let upgrades = world.fake().protobuf_upgrades();
        assert!(upgrades.iter().all(|asked| !asked), "{upgrades:?}");
    }
}

#[then(r#"the client emits "connected" again"#)]
async fn connected_again(world: &mut ClientWorld) {
    loop {
        let event = super::next_event_after_transport_errors(world).await;
        match event.name() {
            "connecting" => {}
            "connected" => return,
            _ => panic!("{event:?}"),
        }
    }
}

#[then(
    regex = r#"^the client emits "connecting", then an "error" event of kind transport whose message contains "([^"]+)", then "connected"$"#
)]
async fn connecting_error_connected(world: &mut ClientWorld, message: String) {
    let connecting = world.next_event().await;
    assert_eq!(connecting.name(), "connecting", "{connecting:?}");
    let error = world.next_event().await;
    let ClientEvent::Error(error) = &error else {
        panic!("an error event: {error:?}");
    };
    assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
    assert!(error.message.contains(&message), "{error:?}");
    let connected = world.next_event().await;
    assert_eq!(connected.name(), "connected", "{connected:?}");
}

#[then(regex = r"^the client's statistics count (\d+) failed attempts?$")]
fn failed_attempts(world: &mut ClientWorld, count: u64) {
    assert_eq!(world.client().statistics().failed_attempts, count);
}

#[then(
    regex = r#"^within the configured handshake timeout plus tolerance the client emits an "error" event of kind transport with message "([^"]+)"$"#
)]
async fn error_within_the_handshake_timeout(world: &mut ClientWorld, message: String) {
    let started = Instant::now();
    let handshake = world.connectors.handshake.expect("a handshake timeout");
    loop {
        let event = world.next_event().await;
        match &event {
            ClientEvent::Connecting { .. } => {}
            ClientEvent::Error(error) => {
                assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
                assert_eq!(error.message, message, "{error:?}");
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(
        started.elapsed() <= handshake + TOLERANCE,
        "{:?}",
        started.elapsed()
    );
}

#[then("the future of the connector's first call has been dropped")]
fn first_call_dropped(world: &mut ClientWorld) {
    assert!(world.connectors.dropped.load(Ordering::SeqCst));
}

#[then(regex = r#"^the client then emits "connected" over transport "(\w+)"$"#)]
async fn then_connected_over(world: &mut ClientWorld, transport: String) {
    let event = super::next_event_after_transport_errors(world).await;
    let ClientEvent::Connected {
        transport: over, ..
    } = &event
    else {
        panic!("a connected event: {event:?}");
    };
    assert_eq!(over, &transport);
}

#[then(
    regex = r#"^the client emits an "error" event of kind transport naming an invalid peer certificate, and the fake server receives no command$"#
)]
async fn invalid_certificate(world: &mut ClientWorld) {
    loop {
        let event = world.next_event().await;
        match &event {
            ClientEvent::Connecting { .. } => {}
            ClientEvent::Error(error) => {
                assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
                assert!(
                    error.message.contains("invalid peer certificate"),
                    "{error:?}"
                );
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(world.fake().received(), Vec::<serde_json::Value>::new());
}

// @E: the build checks.

#[given(regex = r"^a client builder with a connector and (.+)$")]
fn a_builder_with_a_connector(world: &mut ClientWorld, options: String) {
    const WS: &str = "ws://centrifugo.invalid/connection/websocket";
    let endpoints = match options.as_str() {
        "a websocket endpoint, then an http_stream endpoint as a fallback" => vec![
            (TransportKind::WebSocket, WS.to_owned()),
            (
                TransportKind::HttpStream,
                "http://centrifugo.invalid/connection/http_stream".to_owned(),
            ),
        ],
        "an sse endpoint" => vec![(
            TransportKind::Sse,
            "http://centrifugo.invalid/connection/sse".to_owned(),
        )],
        _ => vec![(TransportKind::WebSocket, WS.to_owned())],
    };
    let connector = |_: ferrifuge::http::Uri| async {
        Err::<tokio::net::TcpStream, _>(std::io::Error::other("never called"))
    };
    let builder = Client::builder_with_transports(endpoints).connector(connector);
    let builder = match options.as_str() {
        "a websocket endpoint and the proxy setting none" => builder.proxy(Proxy::none()),
        "a websocket endpoint and the system proxy setting" => builder.proxy(Proxy::system()),
        _ => builder,
    };
    world.pending_builder = Some(builder);
    world.build_error = None;
}

#[then("the client is built")]
fn the_client_was_built(world: &mut ClientWorld) {
    assert!(world.build_error.is_none(), "{:?}", world.build_error);
}

fn configuration_error(world: &ClientWorld) -> &ferrifuge::ClientError {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert_eq!(error.code, codes::error::BAD_CONFIGURATION, "{error:?}");
    assert!(error.message.contains("connector"), "{error:?}");
    error
}

#[then(
    regex = r#"^building fails with a configuration error naming the connector and transport "(\w+)"$"#
)]
fn fails_naming_the_transport(world: &mut ClientWorld, transport: String) {
    let error = configuration_error(world);
    assert!(error.message.contains(&transport), "{error:?}");
    assert_eq!(error.transport.as_deref(), Some(transport.as_str()));
}

#[then("building fails with a configuration error naming the connector and the proxy setting")]
fn fails_naming_the_proxy(world: &mut ClientWorld) {
    let error = configuration_error(world);
    assert!(error.message.contains("proxy setting"), "{error:?}");
}
