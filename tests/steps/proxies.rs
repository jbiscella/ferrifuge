//! Steps of Block 23, proxies.

use std::sync::{Arc, Mutex};

use base64::Engine as _;
use cucumber::{given, then, when};
use ferrifuge::machine::TransportKind;
use ferrifuge::{Client, ClientError, ClientEvent, Proxy, ProxyTarget};

use super::{ClientWorld, expect_event};
use crate::support::forward_proxy::{Carried, Mode, TestProxy};
use crate::support::logs;
use crate::support::tls;

/// The proxy's credentials in the scenarios that require them.
const USER: &str = "u";
const PASSWORD: &str = "pw-7c41";
/// A host that never resolves (RFC 6761): an endpoint only a proxy reaches.
const UNREACHABLE_HOST: &str = "centrifugo.invalid";

/// What a Block 23 scenario holds.
#[derive(Default)]
pub(crate) struct Proxies {
    proxy: Option<TestProxy>,
    /// The proxy's scheme: http, https, socks5 or socks5h.
    kind: String,
    /// The password the client was given.
    password: String,
    /// `host:port` of the endpoint the client asks the proxy for.
    expected: String,
    /// The endpoints a proxy function was asked about.
    asked: Arc<Mutex<Vec<String>>>,
    /// A proxy target made in the scenario.
    made: Option<Result<ProxyTarget, ClientError>>,
    /// The error event of a failed attempt.
    error: Option<ClientError>,
}

fn proxies(world: &mut ClientWorld) -> &mut Proxies {
    &mut world.proxies
}

fn proxy(world: &ClientWorld) -> &TestProxy {
    world.proxies.proxy.as_ref().expect("a test proxy")
}

/// The proxy as the client is given it, with the scenario's credentials
/// when it requires them.
fn target(world: &ClientWorld) -> ProxyTarget {
    let proxies = &world.proxies;
    let address = proxy(world).address();
    let target = ProxyTarget::new(&format!("{}://{address}", proxies.kind)).expect("a proxy");
    if proxies.password.is_empty() {
        target
    } else {
        target.basic_auth(USER, &proxies.password)
    }
}

/// The address of the scenario's server: the fake server, or Centrifugo.
fn server_address(world: &ClientWorld) -> String {
    match (&world.server, &world.fake) {
        (Some(server), _) => server.address().to_owned(),
        (None, Some(fake)) => fake.address(),
        (None, None) => panic!("a server"),
    }
}

fn port_of(address: &str) -> u16 {
    address
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .expect("host:port")
}

#[given(
    regex = r"^an? (HTTP|http|https|socks5|socks5h) proxy requiring credentials in front of (?:the fake server|it)$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn a_proxy_requiring_credentials(world: &mut ClientWorld, kind: String) {
    let target = server_address(world);
    let kind = kind.to_ascii_lowercase();
    let proxy = if kind.starts_with("socks5") {
        TestProxy::socks5(target, (USER.to_owned(), PASSWORD.to_owned())).await
    } else {
        let credentials = Some(format!("{USER}:{PASSWORD}"));
        TestProxy::http(target, kind == "https", credentials, Mode::Carry).await
    };
    let proxies = proxies(world);
    proxies.proxy = Some(proxy);
    proxies.kind = kind;
    PASSWORD.clone_into(&mut proxies.password);
}

#[given("an HTTP proxy in front of the fake server")]
async fn an_http_proxy(world: &mut ClientWorld) {
    let target = server_address(world);
    let proxies = proxies(world);
    proxies.proxy = Some(TestProxy::http(target, false, None, Mode::Carry).await);
    "http".clone_into(&mut proxies.kind);
}

#[given("an HTTP proxy answering every request with 407")]
async fn a_refusing_proxy(world: &mut ClientWorld) {
    let proxies = proxies(world);
    proxies.proxy =
        Some(TestProxy::http("127.0.0.1:1".to_owned(), false, None, Mode::Refuse).await);
    "http".clone_into(&mut proxies.kind);
}

/// Builds the scenario's client for `endpoints` through `proxy`, trusting
/// the test CA for an https:// proxy.
fn build(world: &mut ClientWorld, endpoints: Vec<(TransportKind, String)>, proxy: Proxy) {
    world.transports = endpoints.iter().map(|(kind, _)| *kind).collect();
    let mut builder = Client::builder_with_transports(endpoints)
        .config(world.client_config())
        .proxy(proxy);
    if world.proxies.kind == "https" {
        builder = builder.root_certificates([tls::ca_der()]);
    }
    world.build_client(builder);
}

/// An endpoint of `scheme` only the proxy can reach, with the server's
/// port: `centrifugo.invalid`, or for socks5, whose DNS is the client's, a
/// port of 127.0.0.1 where nothing listens.
fn unreachable_endpoint(world: &mut ClientWorld, scheme: &str, path: &str) -> String {
    let host = if world.proxies.kind == "socks5" {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        let port = listener.local_addr().expect("address").port();
        drop(listener);
        format!("127.0.0.1:{port}")
    } else {
        format!("{UNREACHABLE_HOST}:{}", port_of(&server_address(world)))
    };
    world.proxies.expected.clone_from(&host);
    format!("{scheme}://{host}{path}")
}

fn captured(world: &mut ClientWorld) {
    let number = world.client().log_number();
    logs::register(number);
}

#[given(
    "a client configured with a websocket endpoint only that proxy can reach, and that proxy with its credentials, its log records captured"
)]
fn a_client_through_the_proxy(world: &mut ClientWorld) {
    let endpoint = unreachable_endpoint(world, "ws", "/connection/websocket");
    let proxy = Proxy::all(target(world));
    build(world, vec![(TransportKind::WebSocket, endpoint)], proxy);
    captured(world);
}

fn transport_kind(transport: &str) -> (TransportKind, &'static str) {
    match transport {
        "websocket" => (TransportKind::WebSocket, "/connection/websocket"),
        "http_stream" => (TransportKind::HttpStream, "/connection/http_stream"),
        "sse" => (TransportKind::Sse, "/connection/sse"),
        other => panic!("a transport, not {other}"),
    }
}

#[given(
    regex = r"^a client configured with only (a|an http) (\w+) endpoint only that proxy can reach, and that proxy with its credentials$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_one_endpoint_through_the_proxy(
    world: &mut ClientWorld,
    plain: String,
    transport: String,
) {
    let (kind, path) = transport_kind(&transport);
    let scheme = match (plain.as_str(), kind) {
        ("an http", _) => "http",
        (_, TransportKind::WebSocket) => "wss",
        _ => "https",
    };
    let endpoint = unreachable_endpoint(world, scheme, path);
    let proxy = Proxy::all(target(world));
    // Centrifugo's certificate, from the test CA, names the host.
    build(world, vec![(kind, endpoint)], proxy);
}

#[given(
    regex = r#"^a client configured with only the (\w+) endpoint "([^"]+)" and that proxy with the password "([^"]+)", its log records captured$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_whose_proxy_refuses(
    world: &mut ClientWorld,
    transport: String,
    endpoint: String,
    password: String,
) {
    world.proxies.password = password;
    let (kind, _) = transport_kind(&transport);
    let proxy = Proxy::all(target(world));
    build(world, vec![(kind, endpoint)], proxy);
    captured(world);
}

#[given(
    "a client configured with the fake server's websocket endpoint and that proxy, the fake server's host in its no-proxy list"
)]
fn a_client_with_a_no_proxy_list(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    let proxy = Proxy::all(target(world)).no_proxy("127.0.0.1");
    build(world, vec![(TransportKind::WebSocket, endpoint)], proxy);
}

#[given(
    "a client configured with a websocket endpoint only that proxy can reach, and a proxy function choosing that proxy for it"
)]
fn a_client_with_a_proxy_function(world: &mut ClientWorld) {
    let endpoint = unreachable_endpoint(world, "ws", "/connection/websocket");
    let chosen = target(world);
    let wanted = format!("ws://{}", world.proxies.expected);
    let asked = Arc::clone(&world.proxies.asked);
    let proxy = Proxy::custom(move |endpoint| {
        asked.lock().unwrap().push(endpoint.to_owned());
        (endpoint == wanted).then(|| chosen.clone())
    });
    build(world, vec![(TransportKind::WebSocket, endpoint)], proxy);
}

#[given("a client connected directly to the fake server, with no proxy")]
async fn a_client_connected_directly(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    build(
        world,
        vec![(TransportKind::WebSocket, endpoint)],
        Proxy::none(),
    );
    world.client().connect();
    expect_event(world, "connecting").await;
    expect_event(world, "connected").await;
}

#[when("set_proxy is called with that proxy")]
fn set_proxy_is_called(world: &mut ClientWorld) {
    let proxy = Proxy::all(target(world));
    world.client().set_proxy(proxy);
}

#[then(
    regex = r"^the proxy tunnelled the client's connection to that endpoint's host and port(, with the credentials)?$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_proxy_tunnelled_the_connection(world: &mut ClientWorld, credentials: String) {
    let log = proxy(world).log();
    let expected = &world.proxies.expected;
    let wanted = (!credentials.is_empty()).then(|| format!("{USER}:{PASSWORD}"));
    assert!(
        log.iter().any(|carriage| {
            matches!(carriage.carried, Carried::Tunnel | Carried::Socks)
                && carriage.destination == *expected
                && carriage.credentials == wanted
        }),
        "{log:?}, expected {expected}"
    );
}

#[then(
    "the proxy tunnelled every connection of the client to that endpoint's host and port, with the credentials"
)]
fn the_proxy_tunnelled_every_connection(world: &mut ClientWorld) {
    let log = proxy(world).log();
    let expected = &world.proxies.expected;
    let credentials = Some(format!("{USER}:{PASSWORD}"));
    assert!(!log.is_empty(), "nothing went through the proxy");
    assert!(
        log.iter().all(|carriage| {
            matches!(carriage.carried, Carried::Tunnel | Carried::Socks)
                && carriage.destination == *expected
                && carriage.credentials == credentials
        }),
        "{log:?}, expected {expected}"
    );
}

#[then("the proxy forwarded every request of the client to the server, with the credentials")]
fn the_proxy_forwarded_every_request(world: &mut ClientWorld) {
    let log = proxy(world).log();
    let expected = &world.proxies.expected;
    let credentials = Some(format!("{USER}:{PASSWORD}"));
    assert!(!log.is_empty(), "nothing went through the proxy");
    assert!(
        log.iter().all(|carriage| {
            carriage.carried == Carried::Absolute
                && carriage.destination == *expected
                && carriage.credentials == credentials
        }),
        "{log:?}, expected {expected}"
    );
}

#[then("the proxy carried nothing")]
fn the_proxy_carried_nothing(world: &mut ClientWorld) {
    let log = proxy(world).log();
    assert!(log.is_empty(), "{log:?}");
}

#[then("the proxy function was asked once, for that endpoint's scheme, host and port")]
fn the_function_was_asked_once(world: &mut ClientWorld) {
    let asked = world.proxies.asked.lock().unwrap().clone();
    assert_eq!(asked, [format!("ws://{}", world.proxies.expected)]);
}

#[then("the proxy tunnelled exactly one connection, the client's new one, to the fake server")]
fn the_proxy_tunnelled_one_connection(world: &mut ClientWorld) {
    let log = proxy(world).log();
    let relay = world.server_endpoint();
    let authority = relay
        .split_once("://")
        .and_then(|(_, rest)| rest.split('/').next())
        .expect("an endpoint");
    assert!(
        matches!(log.as_slice(), [carriage] if carriage.carried == Carried::Tunnel && carriage.destination == authority),
        "{log:?}"
    );
}

#[then("a DEBUG record in the client's span names the attempt's proxy by scheme, host and port")]
fn a_record_names_the_proxy(world: &mut ClientWorld) {
    let number = world.client().log_number();
    let shown = format!("{}://{}", world.proxies.kind, proxy(world).address());
    let records = logs::of(number);
    assert!(
        records
            .iter()
            .any(|record| record.level == tracing::Level::DEBUG
                && record.field("proxy") == Some(shown.as_str())),
        "no record names {shown}"
    );
}

/// Fails if a record kept, or `extra`, shows `password`, as text or in the
/// Basic encoding of the scenario's user and it.
fn not_shown(password: &str, extra: &[String]) {
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{password}"));
    for text in logs::all()
        .iter()
        .map(logs::Record::text)
        .chain(extra.iter().cloned())
    {
        assert!(
            !text.contains(password) && !text.contains(&basic),
            "the password in {text}"
        );
    }
}

#[then("no record contains the proxy's password, as text or in its Basic encoding")]
fn no_record_contains_the_password(world: &mut ClientWorld) {
    not_shown(&world.proxies.password, &[]);
}

#[then(
    r#"the client emits an "error" event for the failed transport attempt, saying proxy authentication is required"#
)]
async fn a_proxy_authentication_error(world: &mut ClientWorld) {
    expect_event(world, "connecting").await;
    let event = expect_event(world, "error").await;
    let ClientEvent::Error(error) = &event else {
        unreachable!("expect_event checked the name");
    };
    assert!(
        error.message.contains("proxy authentication required"),
        "{event:?}"
    );
    world.proxies.error = Some(error.clone());
}

#[then(regex = r#"^no record or error contains "([^"]+)", as text or in its Basic encoding$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn no_record_or_error_contains(world: &mut ClientWorld, password: String) {
    let error = world
        .proxies
        .error
        .as_ref()
        .map(|error| format!("{error:?}"))
        .into_iter()
        .collect::<Vec<_>>();
    not_shown(&password, &error);
}

#[when(regex = r#"^a proxy target is made from "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_proxy_target_is_made(world: &mut ClientWorld, url: String) {
    world.proxies.made = Some(ProxyTarget::new(&url));
}

#[then(regex = r#"^it fails with a configuration error naming the scheme "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_fails_naming_the_scheme(world: &mut ClientWorld, scheme: String) {
    let made = world.proxies.made.as_ref().expect("a proxy target made");
    let Err(error) = made else {
        panic!("made: {made:?}");
    };
    assert_eq!(error.kind, ferrifuge::ErrorKind::Configuration, "{error:?}");
    assert!(
        error.message.contains(&format!("proxy scheme {scheme} ")),
        "{error:?}"
    );
}

#[then(regex = r#"^the error does not contain "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_error_does_not_contain(world: &mut ClientWorld, secret: String) {
    let made = world.proxies.made.as_ref().expect("a proxy target made");
    assert!(!format!("{made:?}").contains(&secret), "{made:?}");
}
