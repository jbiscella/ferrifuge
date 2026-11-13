//! Steps of Block 48: WebTransport (unstable), natively. The @A server is
//! Centrifugo over TLS with HTTP/3 (D19); the untagged scenarios bind a
//! local UDP socket.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, SubscriptionOptions, TransportKind};
use ferrifuge::rustls;
use ferrifuge::{Client, ClientEvent, Config, ErrorKind, Proxy, ProxyTarget, SubscriptionState};
use serde_json::json;

use super::batching::{BatchRun, run};
use super::subscriptions::{create, expect_subscription_event, publish, subscribe};
use super::transports::{configure, server};
use super::{ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, expect_event};
use crate::support::centrifugo::Centrifugo;
use crate::support::fake_webtransport::FakeWebTransport;
use crate::support::tls;
use crate::{SERVER_A, SERVER_B};

/// What a Block 48 scenario holds.
#[derive(Debug, Default)]
pub(crate) struct WebTransportWorld {
    /// The local UDP socket of an untagged scenario: its port, and the
    /// datagrams it received.
    socket: Option<(u16, Arc<AtomicUsize>)>,
    /// The fake WebTransport server of a @D scenario (48b).
    pub(super) fake: Option<FakeWebTransport>,
    /// The client of the @E scenario and how its connections end (48b).
    pub(super) plan: super::webtransport_fake::NoCodePlan,
}

/// Starts the scenario's server over TLS with HTTP/3, WebTransport on or
/// off, with `env` besides `SERVER_A`'s, and drops the TCP relay: the client
/// connects directly, as the certificate names `localhost` and 127.0.0.1.
async fn http3_server(world: &mut ClientWorld, env: &[(&str, &str)], webtransport: bool) {
    let (cert, key) = tls::server_pem();
    let env: Vec<_> = SERVER_A
        .iter()
        .copied()
        .chain(env.iter().copied())
        .collect();
    world.relay = None;
    world.server = Some(Centrifugo::start_http3(&env, &cert, &key, webtransport).await);
}

/// The WebTransport endpoint of the scenario's server, started first with
/// HTTP/3 and WebTransport when the scenario's server has none.
pub(super) async fn webtransport_endpoint(world: &mut ClientWorld) -> String {
    if !world.server.as_ref().is_some_and(Centrifugo::serves_http3) {
        http3_server(world, &[], true).await;
    }
    server(world).webtransport_url()
}

#[given(
    "a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled"
)]
async fn a_webtransport_server(world: &mut ClientWorld) {
    http3_server(world, &[], true).await;
}

#[given(
    "a server serving TLS with a certificate from the test CA, with HTTP/3 enabled and WebTransport disabled"
)]
async fn an_http3_server_without_webtransport(world: &mut ClientWorld) {
    http3_server(world, &[], false).await;
}

#[given(
    "a server with HTTP/3 and WebTransport enabled, a ping interval of 2 s and a pong timeout of 1 s"
)]
async fn a_pinging_webtransport_server(world: &mut ClientWorld) {
    http3_server(
        world,
        &[
            ("CENTRIFUGO_CLIENT_PING_INTERVAL", "2s"),
            ("CENTRIFUGO_CLIENT_PONG_TIMEOUT", "1s"),
        ],
        true,
    )
    .await;
}

/// `SERVER_A` without its anonymous connections: a connection token is
/// required.
#[given(
    "a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled, that requires a connection token"
)]
async fn a_webtransport_server_requiring_a_token(world: &mut ClientWorld) {
    let (cert, key) = tls::server_pem();
    let env: Vec<_> = SERVER_A
        .iter()
        .copied()
        .filter(|(name, _)| *name != "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN")
        .collect();
    world.relay = None;
    world.server = Some(Centrifugo::start_http3(&env, &cert, &key, true).await);
}

#[given(
    "a server serving TLS with a certificate from a private CA the platform does not trust, with HTTP/3 and WebTransport enabled"
)]
async fn a_private_webtransport_server(world: &mut ClientWorld) {
    let private = tls::private();
    world.relay = None;
    world.server =
        Some(Centrifugo::start_http3(SERVER_A, &private.server_pem, &private.key_pem, true).await);
}

#[given("a client configured with only the server's WebTransport endpoint")]
async fn a_client_with_only_webtransport(world: &mut ClientWorld) {
    let endpoint = webtransport_endpoint(world).await;
    configure(world, vec![(TransportKind::WebTransport, endpoint)]);
}

#[given("a client configured with the server's WebTransport and wss endpoints")]
fn a_client_with_webtransport_then_wss(world: &mut ClientWorld) {
    let server = server(world);
    let endpoints = vec![
        (TransportKind::WebTransport, server.webtransport_url()),
        (TransportKind::WebSocket, server.websocket_url()),
    ];
    configure(world, endpoints);
}

#[given(
    "a client with an invalid connection token, configured with the server's WebTransport and wss endpoints"
)]
fn a_client_with_an_invalid_token(world: &mut ClientWorld) {
    let server = server(world);
    let endpoints = vec![
        (TransportKind::WebTransport, server.webtransport_url()),
        (TransportKind::WebSocket, server.websocket_url()),
    ];
    world.transports = endpoints.iter().map(|(kind, _)| *kind).collect();
    let config = world.client_config().token("an invalid token");
    world.build_client(Client::builder_with_transports(endpoints).config(config));
}

/// A port of 127.0.0.1 bound and closed: nothing listens on it.
fn closed_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .expect("a UDP port")
        .port()
}

#[given(
    "a client with a handshake timeout of 2 s, configured with a WebTransport endpoint on a UDP port where nothing listens and the server's wss endpoint"
)]
fn a_client_with_an_unreachable_webtransport(world: &mut ClientWorld) {
    world.limits.handshake_timeout = Some(Duration::from_secs(2));
    let port = closed_udp_port();
    let endpoints = vec![
        (
            TransportKind::WebTransport,
            format!("https://127.0.0.1:{port}/connection/webtransport"),
        ),
        (TransportKind::WebSocket, server(world).websocket_url()),
    ];
    configure(world, endpoints);
}

#[then(
    regex = r#"^(?:then )?the client emits an "error" event for the failed transport attempt over transport "(\w+)"$"#
)]
async fn a_failed_attempt_over(world: &mut ClientWorld, transport: String) {
    let event = loop {
        match world.next_event().await {
            ClientEvent::Connecting { .. } => {}
            event => break event,
        }
    };
    let ClientEvent::Error(error) = &event else {
        panic!("expected an error event, got {event:?}");
    };
    assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
    assert_eq!(
        error.transport.as_deref(),
        Some(transport.as_str()),
        "{error:?}"
    );
    // Seen again by the next step, which checks the order of the attempts.
    world.put_back_events.push_front(event);
}

#[when(
    regex = r#"^the client subscribes to channel "(\w+)" and the server API publishes (\{.*\}) to channel "(\w+)"$"#
)]
async fn subscribes_and_the_server_publishes(
    world: &mut ClientWorld,
    channel: String,
    data: String,
    to: String,
) {
    create(world, &channel);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
    publish(world, &to, &data, None).await;
}

#[when(regex = r#"^the server API disconnects the client with code (\d+) and reason "([^"]+)"$"#)]
async fn the_server_api_disconnects_with(world: &mut ClientWorld, code: u32, reason: String) {
    let client_id = world.client_id.clone().expect("a connected client");
    server(world)
        .api()
        .call(
            "disconnect",
            json!({
                "user": "",
                "client": client_id,
                "disconnect": {"code": code, "reason": reason},
            }),
        )
        .await
        .expect("server API disconnect");
    world.sent_disconnect = Some((code, reason));
}

// The TLS settings (Block 22).

#[given(
    regex = r"^a client configured with only the server's WebTransport endpoint and (that CA as an extra root certificate|no extra root certificate)$"
)]
fn a_client_with_trust(world: &mut ClientWorld, trust: String) {
    let endpoint = server(world).webtransport_url();
    world.transports = vec![TransportKind::WebTransport];
    let builder = Client::builder_with_transports([(TransportKind::WebTransport, endpoint)])
        .config(world.client_config());
    let builder = if trust.starts_with("that CA") {
        builder.root_certificates([tls::private().ca.clone()])
    } else {
        builder
    };
    world.build_client(builder);
}

// The real HTTP headers (Block 17), through the connect proxy (@B).

#[given(
    regex = r#"^a server with HTTP/3 and WebTransport enabled and a connect proxy that receives header "([^"]+)"$"#
)]
async fn a_webtransport_server_with_a_proxy(world: &mut ClientWorld, header: String) {
    let proxy_env = world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .centrifugo_env();
    let headers = header.to_ascii_lowercase();
    let env: Vec<_> = proxy_env
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .filter(|(name, _)| *name != "CENTRIFUGO_CLIENT_PROXY_CONNECT_HTTP_HEADERS")
        .chain(SERVER_B.iter().copied())
        .chain([(
            "CENTRIFUGO_CLIENT_PROXY_CONNECT_HTTP_HEADERS",
            headers.as_str(),
        )])
        .collect();
    let (cert, key) = tls::server_pem();
    world.relay = None;
    world.server = Some(Centrifugo::start_http3(&env, &cert, &key, true).await);
}

#[given(
    regex = r#"^a client configured with only the server's WebTransport endpoint and the HTTP header "([^"]+)" set to "([^"]+)"$"#
)]
fn a_client_with_a_header(world: &mut ClientWorld, name: String, value: String) {
    let endpoint = server(world).webtransport_url();
    world.transports = vec![TransportKind::WebTransport];
    let builder = Client::builder_with_transports([(TransportKind::WebTransport, endpoint)])
        .config(world.client_config())
        .http_headers([(name, value)]);
    world.build_client(builder);
}

#[then(regex = r#"^the connect proxy received the header "([^"]+)" with value "([^"]+)"$"#)]
async fn the_proxy_received_the_header(world: &mut ClientWorld, name: String, value: String) {
    let proxy = world.proxy.as_ref().expect("the connect proxy");
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let headers = proxy.request_headers(&name);
            if !headers.is_empty() {
                return headers;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a connect request");
    assert!(
        received
            .iter()
            .all(|got| got.as_deref() == Some(value.as_str())),
        "{name}: {received:?}"
    );
}

// @E: the core over WebTransport (D10).

#[given(
    r#"a client configured with a WebTransport endpoint and two subscriptions in state "subscribing" that need no callback"#
)]
fn a_webtransport_core_with_two_subscriptions(world: &mut ClientWorld) {
    let mut run = BatchRun::of(
        Machine::new(Config::default())
            .with_transports(vec![TransportKind::WebTransport])
            .expect("one transport"),
    );
    for channel in ["news", "sport"] {
        let id = run
            .machine
            .add_subscription(channel.to_owned(), SubscriptionOptions::default())
            .expect("a new channel");
        run.handle(Input::Subscribe(id));
        run.subscriptions.push(id);
    }
    world.batching.run = Some(run);
}

#[when("the WebTransport transport opens")]
fn the_webtransport_transport_opens(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::Connect);
    assert!(
        run.actions[run.mark..].iter().any(|action| matches!(
            action,
            Action::OpenTransport {
                kind: TransportKind::WebTransport,
                initial_frame: None,
                ..
            }
        )),
        "{:?}",
        &run.actions[run.mark..]
    );
    run.handle(Input::TransportOpened(run.transport.expect("an attempt")));
}

#[then(
    "the first frame sent carries the connect command followed by the two subscribe commands, in registry order"
)]
fn connect_then_both_subscribes(world: &mut ClientWorld) {
    let run = run(world);
    let frames = run.frames();
    let first = frames.first().expect("a frame sent");
    let kinds: Vec<String> = first
        .1
        .iter()
        .map(|command| match command["subscribe"]["channel"].as_str() {
            Some(channel) => format!("subscribe {channel}"),
            None if command["connect"].is_object() => "connect".to_owned(),
            None => command.to_string(),
        })
        .collect();
    assert_eq!(kinds, ["connect", "subscribe news", "subscribe sport"]);
}

#[then(r#"no subscription reaches "subscribed" before the connect reply is handled"#)]
fn none_subscribed_before_the_connect_reply(world: &mut ClientWorld) {
    let run = run(world);
    for id in run.subscriptions.clone() {
        assert_eq!(
            run.machine.subscription_state(id),
            Some(SubscriptionState::Subscribing)
        );
    }
    // The subscribe replies first, then the connect reply: a subscribe is
    // handled only after the connect reply (Block 36, D6).
    let sent = run.frames();
    let commands = &sent.first().expect("a frame sent").1;
    let id = |index: usize| commands[index]["id"].as_u64().expect("a command id");
    run.frame(&json!({"id": id(0), "connect": {"client": "c", "session": "s", "node": "n"}}));
    run.frame(&json!({"id": id(1), "subscribe": {}}));
    run.frame(&json!({"id": id(2), "subscribe": {}}));
    for id in run.subscriptions.clone() {
        assert_eq!(
            run.machine.subscription_state(id),
            Some(SubscriptionState::Subscribed)
        );
    }
}

// Build-time refusals (D2, D4, D14).

#[given(regex = r#"^client options with transport "webtransport" and endpoint "([^"]*)"$"#)]
fn options_with_a_webtransport_endpoint(world: &mut ClientWorld, endpoint: String) {
    world.pending_builder = Some(Client::builder_with_transports([(
        TransportKind::WebTransport,
        endpoint,
    )]));
}

#[then("building fails with a configuration error naming webtransport and https")]
fn fails_naming_webtransport_and_https(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert!(
        error.message.contains("webtransport") && error.message.contains("https"),
        "{error:?}"
    );
    assert_eq!(
        error.transport.as_deref(),
        Some("webtransport"),
        "{error:?}"
    );
}

#[given(
    regex = r"^client options with a WebTransport endpoint and a raw rustls configuration (offering TLS 1.2 only|offering TLS 1.3 without TLS13_AES_128_GCM_SHA256)$"
)]
fn options_with_an_unfit_configuration(world: &mut ClientWorld, configuration: String) {
    let mut provider = tls::provider();
    let versions: &[&rustls::SupportedProtocolVersion] = if configuration.contains("1.2") {
        &[&rustls::version::TLS12]
    } else {
        provider
            .cipher_suites
            .retain(|suite| suite.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
        &[&rustls::version::TLS13]
    };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(versions)
        .expect("protocol versions")
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    world.pending_builder = Some(
        Client::builder_with_transports([(
            TransportKind::WebTransport,
            "https://localhost:8443/connection/webtransport".to_owned(),
        )])
        .tls_config(Arc::new(config)),
    );
}

#[then("building fails with a configuration error naming TLS 1.3 and webtransport")]
fn fails_naming_tls13(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert!(
        error.message.contains("TLS 1.3") && error.message.contains("webtransport"),
        "{error:?}"
    );
}

#[given("client options with a connector and a WebTransport endpoint")]
fn options_with_a_connector(world: &mut ClientWorld) {
    let connector = |_: ferrifuge::http::Uri| async {
        Err::<tokio::net::TcpStream, _>(std::io::Error::other("never called"))
    };
    world.pending_builder = Some(
        Client::builder_with_transports([(
            TransportKind::WebTransport,
            "https://localhost:8443/connection/webtransport".to_owned(),
        )])
        .connector(connector),
    );
}

#[then("building fails with a configuration error naming the connector and webtransport")]
fn fails_naming_the_connector(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert!(
        error.message.contains("connector") && error.message.contains("webtransport"),
        "{error:?}"
    );
}

// Untagged: a local UDP socket (D19).

#[given(
    regex = r"^a UDP socket on 127.0.0.1 that (?:counts the datagrams it receives|receives and never answers)$"
)]
async fn a_udp_socket(world: &mut ClientWorld) {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("a UDP socket");
    let port = socket.local_addr().expect("its address").port();
    let received = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&received);
    tokio::spawn(async move {
        let mut buffer = vec![0; 65536];
        while socket.recv_from(&mut buffer).await.is_ok() {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });
    world.webtransport.socket = Some((port, received));
}

fn socket(world: &ClientWorld) -> (u16, &AtomicUsize) {
    let (port, received) = world.webtransport.socket.as_ref().expect("a UDP socket");
    (*port, received)
}

fn socket_endpoint(world: &ClientWorld) -> String {
    format!(
        "https://127.0.0.1:{}/connection/webtransport",
        socket(world).0
    )
}

#[given(
    r#"a client configured with only a WebTransport endpoint at that socket's port and the proxy "http://127.0.0.1:9" for every endpoint"#
)]
fn a_client_with_a_proxy(world: &mut ClientWorld) {
    let endpoint = socket_endpoint(world);
    world.transports = vec![TransportKind::WebTransport];
    let proxy = Proxy::all(ProxyTarget::new("http://127.0.0.1:9").expect("a proxy"));
    let builder = Client::builder_with_transports([(TransportKind::WebTransport, endpoint)])
        .config(world.client_config())
        .proxy(proxy);
    world.build_client(builder);
}

#[given(
    "a client with a handshake timeout of 1 s and a timeout of 10 s, configured with only a WebTransport endpoint at that socket's port"
)]
fn a_client_with_a_silent_endpoint(world: &mut ClientWorld) {
    world.limits.handshake_timeout = Some(Duration::from_secs(1));
    world.limits.timeout = Some(Duration::from_secs(10));
    let endpoint = socket_endpoint(world);
    configure(world, vec![(TransportKind::WebTransport, endpoint)]);
}

#[then(
    regex = r#"^the client emits an "error" event for the failed transport attempt whose message contains "([^"]+)"$"#
)]
async fn a_failed_attempt_containing(world: &mut ClientWorld, part: String) {
    expect_event(world, "connecting").await;
    let event = expect_event(world, "error").await;
    let ClientEvent::Error(error) = &event else {
        unreachable!("expect_event checked the name");
    };
    assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
    assert!(error.message.contains(&part), "{error:?}");
}

#[then("the socket received no datagram")]
async fn no_datagram(world: &mut ClientWorld) {
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(socket(world).1.load(Ordering::SeqCst), 0);
}

#[then("the socket received at least one datagram")]
fn some_datagram(world: &mut ClientWorld) {
    assert!(socket(world).1.load(Ordering::SeqCst) > 0);
}
