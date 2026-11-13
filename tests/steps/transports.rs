//! Steps of Block 8: HTTP-streaming transports and fallback.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportKind};
use ferrifuge::{Client, ClientEvent, ErrorKind, Protocol, SubscriptionState, TokenError, codes};
use serde_json::json;

use super::tls::a_tls_server;
use super::token::with_expiring_token;
use super::{
    ClientWorld, EVENT_TIMEOUT, UNREACHABLE_ENDPOINT, expect_event,
    next_event_after_transport_errors, test_config,
};
use crate::support::centrifugo::Centrifugo;
use crate::support::http_proxy::HttpProxy;
use crate::support::relay::{CountingRelay, Mode};
use crate::{SERVER_A, SERVER_B};

const UNREACHABLE_HTTP_STREAM: &str = "http://127.0.0.1:1/connection/http_stream";
const UNREACHABLE_SSE: &str = "http://127.0.0.1:1/connection/sse";
/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// Server ping interval of the scenario making the client send a pong.
const PING_INTERVAL: Duration = Duration::from_secs(2);
/// Size of the subscription tokens that fill the stalled connection: more
/// than the client's socket and the relay's receive window take once the
/// relay stops reading (about 2.6 MiB measured on Linux loopback), and
/// small enough to encode quickly in a debug build on a slow runner.
const STALLING_TOKEN_SIZE: usize = 8 << 20;
/// Lifetime of the subscription tokens, in seconds, after which the client
/// sends the large ones in `sub_refresh` commands.
const STALLING_TOKEN_TTL: u32 = 2;
/// Channels whose large `sub_refresh` commands stall the connection.
/// Several, because Windows takes a single large write whole, however
/// large: a later one stalls there.
const STALLING_CHANNELS: [&str; 4] = ["stall", "stall2", "stall3", "stall4"];

pub(super) fn kind(name: &str) -> TransportKind {
    match name {
        "websocket" => TransportKind::WebSocket,
        "http_stream" => TransportKind::HttpStream,
        "sse" => TransportKind::Sse,
        #[cfg(feature = "unstable-webtransport")]
        "webtransport" => TransportKind::WebTransport,
        other => panic!("unknown transport {other}"),
    }
}

pub(super) fn server(world: &ClientWorld) -> &Centrifugo {
    world
        .server
        .as_ref()
        .expect("this step needs a server: tag the scenario @A or @B")
}

pub(super) fn server_url(server: &Centrifugo, kind: TransportKind) -> String {
    match kind {
        TransportKind::HttpStream => server.http_stream_url(),
        TransportKind::Sse => server.sse_url(),
        #[cfg(feature = "unstable-webtransport")]
        TransportKind::WebTransport => server.webtransport_url(),
        _ => server.websocket_url(),
    }
}

/// Creates the scenario's client for `endpoints`, tried in order. It
/// connects to the server directly: emulation requests open connections of
/// their own, which a counting relay would take for new transports.
pub(super) fn configure(world: &mut ClientWorld, endpoints: Vec<(TransportKind, String)>) {
    world.transports = endpoints.iter().map(|(kind, _)| *kind).collect();
    let builder = Client::builder_with_transports(endpoints).config(world.client_config());
    let builder = world.connect_data.build(builder);
    world.build_client(builder);
}

/// Connects the scenario's client and waits until it is connected; failed
/// attempts of a fallback may come first.
pub(super) async fn connect_and_wait(world: &mut ClientWorld) {
    world.client().connect();
    expect_event(world, "connecting").await;
    match next_event_after_transport_errors(world).await {
        ClientEvent::Connected { client_id, .. } => world.client_id = Some(client_id),
        other => panic!("expected connected, got {other:?}"),
    }
}

/// The @A and @B servers enable them (`SERVER_A`, `SERVER_B` in `bdd.rs`).
#[given("a server with http_stream and sse enabled")]
fn a_server_with_http_transports(world: &mut ClientWorld) {
    server(world);
}

#[given(
    "a server serving TLS with a certificate from the test CA, with http_stream and sse enabled"
)]
async fn a_tls_server_with_http_transports(world: &mut ClientWorld) {
    a_tls_server(world).await;
}

#[given(
    regex = r"^a server with http_stream and sse enabled, a ping interval of (\d+) s and a pong timeout of (\d+) s$"
)]
async fn a_server_pinging(world: &mut ClientWorld, interval: u32, timeout: u32) {
    let (interval, timeout) = (format!("{interval}s"), format!("{timeout}s"));
    let env: Vec<_> = SERVER_A
        .iter()
        .copied()
        .chain([
            ("CENTRIFUGO_CLIENT_PING_INTERVAL", interval.as_str()),
            ("CENTRIFUGO_CLIENT_PONG_TIMEOUT", timeout.as_str()),
        ])
        .collect();
    world.relay = None;
    world.server = Some(Centrifugo::start(&env).await);
}

#[given(
    regex = r"^a server with http_stream and sse enabled, whose http_stream endpoint is behind a relay that (accepts but never answers|is down)$"
)]
async fn a_server_behind_a_relay(world: &mut ClientWorld, mode: String) {
    let mode = if mode == "is down" {
        Mode::Down
    } else {
        Mode::Silent
    };
    let address = server(world).address().to_owned();
    world.relay = Some(CountingRelay::start_in(address, mode).await);
}

#[given(
    "a client configured with the Protobuf protocol, an unreachable websocket endpoint and the server's http_stream endpoint"
)]
fn a_protobuf_client_falling_back(world: &mut ClientWorld) {
    world.protocol = ferrifuge::Protocol::Protobuf;
    let http_stream = server_url(server(world), TransportKind::HttpStream);
    configure(
        world,
        vec![
            (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
            (TransportKind::HttpStream, http_stream),
        ],
    );
}

#[given(
    "a client configured with an unreachable websocket endpoint and the server's http_stream and sse endpoints"
)]
fn a_client_falling_back_to_http_stream(world: &mut ClientWorld) {
    let server = server(world);
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
        (TransportKind::HttpStream, server.http_stream_url()),
        (TransportKind::Sse, server.sse_url()),
    ];
    configure(world, endpoints);
}

#[given(
    regex = r"^a client configured with an unreachable websocket endpoint and only the server's https (\w+) endpoint$"
)]
fn a_client_falling_back_over_https(world: &mut ClientWorld, transport: String) {
    let url = server_url(server(world), kind(&transport));
    assert!(url.starts_with("https://"), "{url}");
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
        (kind(&transport), url),
    ];
    configure(world, endpoints);
}

#[given(
    "a client configured with unreachable websocket and http_stream endpoints and the server's sse endpoint"
)]
fn a_client_falling_back_to_sse(world: &mut ClientWorld) {
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
        (
            TransportKind::HttpStream,
            UNREACHABLE_HTTP_STREAM.to_owned(),
        ),
        (TransportKind::Sse, server(world).sse_url()),
    ];
    configure(world, endpoints);
}

#[given(
    regex = r"^a client configured with an unreachable websocket endpoint, the relayed http_stream endpoint and (the server's|an unreachable) sse endpoint$"
)]
pub(super) fn a_client_with_a_relayed_http_stream(world: &mut ClientWorld, sse: String) {
    let sse = if sse == "the server's" {
        server(world).sse_url()
    } else {
        UNREACHABLE_SSE.to_owned()
    };
    let relay = world.relay.as_ref().expect("a relay");
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
        (TransportKind::HttpStream, relay.http_stream_url()),
        (TransportKind::Sse, sse),
    ];
    configure(world, endpoints);
}

#[given("a client configured with unreachable websocket, http_stream and sse endpoints")]
fn a_client_with_every_transport_unreachable(world: &mut ClientWorld) {
    let endpoints = vec![
        (TransportKind::WebSocket, UNREACHABLE_ENDPOINT.to_owned()),
        (
            TransportKind::HttpStream,
            UNREACHABLE_HTTP_STREAM.to_owned(),
        ),
        (TransportKind::Sse, UNREACHABLE_SSE.to_owned()),
    ];
    configure(world, endpoints);
}

/// The websocket is a counting relay that is down, so the scenario can tell
/// whether the client went back to it.
#[given(
    r#"a client configured with a websocket endpoint behind a relay that is down and the server's http_stream and sse endpoints, connected over transport "http_stream""#
)]
async fn a_client_after_an_unreachable_websocket(world: &mut ClientWorld) {
    let address = server(world).address().to_owned();
    let websocket = CountingRelay::start_in(address, Mode::Down).await;
    let endpoints = vec![
        (TransportKind::WebSocket, websocket.websocket_url()),
        (TransportKind::HttpStream, server(world).http_stream_url()),
        (TransportKind::Sse, server(world).sse_url()),
    ];
    configure(world, endpoints);
    connect_and_wait(world).await;
    let attempts = websocket.connections();
    world.websocket_relay = Some((websocket, attempts));
}

#[then("the client made no connection attempt to the websocket endpoint after the disconnect")]
fn no_websocket_attempt(world: &mut ClientWorld) {
    let (relay, before) = world.websocket_relay.as_ref().expect("the websocket relay");
    assert_eq!(relay.connections(), *before);
}

/// Through the scenario's HTTP proxy when an earlier step set one.
#[given(
    regex = r#"^a client (configured with the Protobuf protocol )?connected over transport "(\w+)"$"#
)]
pub(super) async fn a_client_connected_over(
    world: &mut ClientWorld,
    protobuf: String,
    transport: String,
) {
    if !protobuf.is_empty() {
        world.protocol = ferrifuge::Protocol::Protobuf;
    }
    let kind = kind(&transport);
    // Over the scenario's HTTP/3 server, started at once when it has none
    // (Block 48).
    #[cfg(feature = "unstable-webtransport")]
    if kind == TransportKind::WebTransport {
        super::webtransport::webtransport_endpoint(world).await;
    }
    let url = match &world.http_proxy {
        Some(proxy) if kind.is_emulated() => {
            format!("http://{}/connection/{}", proxy.address(), kind.name())
        }
        _ => server_url(server(world), kind),
    };
    configure(world, vec![(kind, url)]);
    connect_and_wait(world).await;
}

#[given(r#"a client connected over transport "sse" using a connection token with expiration"#)]
async fn a_client_connected_over_sse_with_expiring_token(world: &mut ClientWorld) {
    world.transports = vec![TransportKind::Sse];
    let endpoints = [(TransportKind::Sse, server(world).sse_url())];
    let builder = with_expiring_token(world, Client::builder_with_transports(endpoints));
    world.build_client(builder);
    connect_and_wait(world).await;
}

/// The client sends commands through the emulation endpoint; the next one
/// is its pong to a server ping, so the server pings often.
#[given(
    r#"a client connected over transport "http_stream" through a proxy that rejects the next emulation request"#
)]
async fn a_client_behind_an_http_proxy(world: &mut ClientWorld) {
    let http_proxy = pinging_server_behind_an_http_proxy(world).await;
    let url = format!("http://{}/connection/http_stream", http_proxy.address());
    configure(world, vec![(TransportKind::HttpStream, url)]);
    connect_and_wait(world).await;
    http_proxy.reject_next_emulation();
    world.http_proxy = Some(http_proxy);
    world.emulation_rejection = true;
}

/// Restarts the @B server pinging every [`PING_INTERVAL`], so the client
/// sends commands (its pongs) through the emulation endpoint, and puts an
/// HTTP proxy in front of it.
pub(super) async fn pinging_server_behind_an_http_proxy(world: &mut ClientWorld) -> HttpProxy {
    let proxy_env = world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .centrifugo_env();
    let ping = format!("{}s", PING_INTERVAL.as_secs());
    let env: Vec<_> = proxy_env
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .chain(SERVER_B.iter().copied())
        .chain([
            ("CENTRIFUGO_CLIENT_PING_INTERVAL", ping.as_str()),
            ("CENTRIFUGO_CLIENT_PONG_TIMEOUT", "1s"),
        ])
        .collect();
    world.relay = None;
    world.server = Some(Centrifugo::start(&env).await);
    HttpProxy::start(server(world).address().to_owned()).await
}

/// Its pong to the next server ping: rejected when a rejection is
/// programmed, relayed otherwise.
#[when("the client sends a command")]
async fn the_client_sends_a_command(world: &mut ClientWorld) {
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    let rejecting = proxy.rejection_programmed();
    let sent = tokio::time::timeout(PING_INTERVAL + TOLERANCE, async {
        loop {
            let relayed = proxy
                .requests_with_header("")
                .iter()
                .any(|(path, _)| path == "/emulation");
            if proxy.rejected() || (!rejecting && relayed) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(sent.is_ok(), "no emulation request reached the proxy");
}

#[then(
    regex = r#"^within the configured timeout plus tolerance the client emits "connected" over transport "(\w+)"$"#
)]
async fn within_the_timeout_connected_over(world: &mut ClientWorld, transport: String) {
    connected_over_within(world, &transport, test_config().timeout + TOLERANCE).await;
}

/// Waits at most `limit` for "connected" over `transport`, past failed
/// attempts.
pub(super) async fn connected_over_within(
    world: &mut ClientWorld,
    transport: &str,
    limit: Duration,
) {
    let event = tokio::time::timeout(limit, async {
        loop {
            match world.next_event().await {
                // Failed attempts, and the event of connect() itself.
                ClientEvent::Error(error) if error.kind == ErrorKind::Transport => {}
                ClientEvent::Connecting { code, .. }
                    if code == codes::connecting::CONNECT_CALLED => {}
                event => return event,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("not connected within {limit:?}"));
    let ClientEvent::Connected { transport: got, .. } = &event else {
        panic!("expected connected, got {event:?}");
    };
    assert_eq!(got, transport);
}

#[when("the relay comes up after every transport has failed once")]
async fn the_relay_comes_up(world: &mut ClientWorld) {
    let transports = world.transports.clone();
    let mut failed = Vec::new();
    let all_failed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while !transports
            .iter()
            .all(|kind| failed.contains(&kind.name().to_owned()))
        {
            if let ClientEvent::Error(error) = world.next_event().await
                && let Some(transport) = error.transport
            {
                failed.push(transport);
            }
        }
    })
    .await;
    assert!(all_failed.is_ok(), "not every transport failed: {failed:?}");
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
}

#[given("the connect proxy will leave the next connect unanswered")]
fn proxy_leaves_next_connect_unanswered(world: &mut ClientWorld) {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .hold_next();
}

#[given("a client configured with the server's websocket and http_stream endpoints")]
fn a_client_with_websocket_and_http_stream(world: &mut ClientWorld) {
    let server = server(world);
    let endpoints = vec![
        (TransportKind::WebSocket, server.websocket_url()),
        (TransportKind::HttpStream, server.http_stream_url()),
    ];
    configure(world, endpoints);
}

#[given("a client configured with websocket and http_stream endpoints")]
fn a_machine_with_websocket_and_http_stream(world: &mut ClientWorld) {
    world.transports = vec![TransportKind::WebSocket, TransportKind::HttpStream];
}

/// Drives the sans-IO core: the websocket opens, its connect reply never
/// comes, and the connect timeout fires; records the transports opened.
#[when("the websocket opens and no connect reply arrives within the configured timeout")]
fn websocket_opens_without_reply(world: &mut ClientWorld) {
    let mut machine = Machine::new(test_config())
        .with_transports(world.transports.clone())
        .expect("valid transports");
    let mut actions = machine.handle(Input::Connect);
    while world.opened.len() < 2 {
        let open = actions.iter().find_map(|action| match action {
            Action::OpenTransport {
                transport, kind, ..
            } => Some((*transport, *kind)),
            _ => None,
        });
        if let Some((transport, kind)) = open {
            world.opened.push((kind, Duration::ZERO));
            if world.opened.len() == 1 {
                machine.handle(Input::TransportOpened(transport));
                actions = machine.handle(Input::TimerFired(Timer::ConnectTimeout));
            }
        } else if actions
            .iter()
            .any(|action| matches!(action, Action::SetTimer(Timer::Reconnect, _)))
        {
            actions = machine.handle(Input::TimerFired(Timer::Reconnect));
        } else {
            panic!("no further attempt after {:?}: {actions:?}", world.opened);
        }
    }
}

#[then("the next attempt opens the http_stream transport")]
fn next_attempt_opens_http_stream(world: &mut ClientWorld) {
    assert_eq!(
        world
            .opened
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>(),
        [TransportKind::WebSocket, TransportKind::HttpStream],
    );
}

#[given("a client configured with websocket, http_stream and sse endpoints")]
fn a_machine_with_three_transports(world: &mut ClientWorld) {
    world.transports = vec![
        TransportKind::WebSocket,
        TransportKind::HttpStream,
        TransportKind::Sse,
    ];
}

/// Drives the sans-IO core through two sequences of failed attempts,
/// recording each transport opened and the reconnect delay before it.
#[when("connect is called and every transport attempt fails")]
fn every_attempt_fails(world: &mut ClientWorld) {
    let mut machine = Machine::new(test_config())
        .with_transports(world.transports.clone())
        .expect("valid transports");
    let mut actions = machine.handle(Input::Connect);
    let mut delay = Duration::ZERO;
    while world.opened.len() < 2 * world.transports.len() {
        let open = actions.iter().find_map(|action| match action {
            Action::OpenTransport {
                transport, kind, ..
            } => Some((*transport, *kind)),
            _ => None,
        });
        let reconnect = actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        });
        if let Some((transport, kind)) = open {
            world.opened.push((kind, delay));
            delay = Duration::ZERO;
            actions = machine.handle(Input::TransportFailed {
                transport,
                message: "refused".to_owned(),
            });
        } else if let Some(reconnect) = reconnect {
            delay = reconnect;
            actions = machine.handle(Input::TimerFired(Timer::Reconnect));
        } else {
            panic!("no further attempt after {:?}: {actions:?}", world.opened);
        }
    }
}

#[then("the transports of the first sequence are tried without delay")]
fn first_sequence_without_delay(world: &mut ClientWorld) {
    let first = &world.opened[..world.transports.len()];
    let kinds: Vec<_> = first.iter().map(|(kind, _)| *kind).collect();
    assert_eq!(kinds, world.transports, "{:?}", world.opened);
    assert!(
        first.iter().all(|(_, delay)| delay.is_zero()),
        "{:?}",
        world.opened
    );
}

#[then("the next sequence starts only after the reconnect backoff delay")]
fn next_sequence_after_backoff(world: &mut ClientWorld) {
    let (kind, delay) = world.opened[world.transports.len()];
    assert_eq!(kind, world.transports[0], "{:?}", world.opened);
    assert!(
        delay >= test_config().min_reconnect_delay,
        "{:?}",
        world.opened
    );
}

#[given("a client configured with http_stream and sse endpoints")]
fn a_machine_with_emulated_transports(world: &mut ClientWorld) {
    world.transports = vec![TransportKind::HttpStream, TransportKind::Sse];
}

/// Drives the sans-IO core: the first transport opens and the server
/// answers its connect with a temporary error; records the next transport
/// opened and the reconnect delay before it.
#[when("the http_stream transport opens and the server answers its connect with a temporary error")]
fn the_server_answers_with_a_temporary_error(world: &mut ClientWorld) {
    let mut machine = Machine::new(test_config())
        .with_transports(world.transports.clone())
        .expect("valid transports");
    let actions = machine.handle(Input::Connect);
    let (transport, connect) = actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport {
                transport,
                initial_frame: Some(frame),
                ..
            } => Some((*transport, frame.clone())),
            _ => None,
        })
        .expect("an emulated transport to open, carrying the connect");
    let command: serde_json::Value = serde_json::from_slice(&connect).expect("a JSON connect");
    let id = &command["id"];
    machine.handle(Input::TransportOpened(transport));
    let mut actions = machine.handle(Input::Frame {
        transport,
        data: format!(r#"{{"id":{id},"error":{{"code":100,"message":"busy","temporary":true}}}}"#)
            .into_bytes(),
    });
    let mut delay = Duration::ZERO;
    loop {
        if let Some(kind) = actions.iter().find_map(|action| match action {
            Action::OpenTransport { kind, .. } => Some(*kind),
            _ => None,
        }) {
            world.opened.push((kind, delay));
            return;
        }
        delay = actions
            .iter()
            .find_map(|action| match action {
                Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no further attempt: {actions:?}"));
        actions = machine.handle(Input::TimerFired(Timer::Reconnect));
    }
}

#[then("the next attempt opens the http_stream transport again, after the reconnect backoff delay")]
fn the_same_transport_after_backoff(world: &mut ClientWorld) {
    let (kind, delay) = world.opened[0];
    assert_eq!(kind, TransportKind::HttpStream, "{:?}", world.opened);
    assert!(
        delay >= test_config().min_reconnect_delay,
        "{:?}",
        world.opened
    );
}

#[given(regex = r"^client options with the Protobuf protocol and an? (\w+) endpoint$")]
fn client_options_with_protobuf(world: &mut ClientWorld, transport: String) {
    world.pending_builder = Some(
        Client::builder_with_transports([(kind(&transport), UNREACHABLE_HTTP_STREAM.to_owned())])
            .config(test_config().protocol(Protocol::Protobuf)),
    );
}

#[when("the client is built")]
fn the_client_is_built(world: &mut ClientWorld) {
    let builder = world.pending_builder.take().expect("client options");
    world.build_error = builder.build().err();
}

#[then(regex = r"^building fails with a configuration error naming Protobuf and (\w+)$")]
fn building_fails(world: &mut ClientWorld, transport: String) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert_eq!(error.code, codes::error::BAD_CONFIGURATION, "{error:?}");
    assert!(
        error.message.contains("Protobuf") && error.message.contains(&transport),
        "{error:?}"
    );
    assert_eq!(error.transport.as_deref(), Some(transport.as_str()));
}

/// Before the server stops reading, the client subscribes with a token
/// callback whose next token, sent in a `sub_refresh` when the first
/// expires, is larger than the connection can buffer. A `sub_refresh`
/// without a reply is retried (Block 10) rather than ending the connection,
/// unlike a subscribe (code 3), so only the stalled write can end it.
#[given("the fake server stops reading from the connection")]
async fn the_fake_server_stops_reading(world: &mut ClientWorld) {
    for channel in STALLING_CHANNELS {
        let calls = Arc::clone(&world.token_calls);
        let subscription = world
            .client()
            .subscription_builder(channel)
            .get_token(move |channel: String| {
                let mut calls = calls.lock().unwrap();
                let first = !calls.contains(&channel);
                calls.push(channel);
                let token = if first {
                    "first".to_owned()
                } else {
                    "t".repeat(STALLING_TOKEN_SIZE)
                };
                std::future::ready(Ok::<_, TokenError>(token))
            })
            .build()
            .expect("no subscription yet");
        world
            .fake()
            .answer_a_subscribe_with(json!({"expires": true, "ttl": STALLING_TOKEN_TTL}));
        subscription.subscribe();
        world.named_subscriptions.push(subscription);
    }
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world
            .named_subscriptions
            .iter()
            .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(subscribed.is_ok(), "{:?}", world.named_subscriptions);
    world.stalled_writes = ferrifuge::stalled_websocket_writes();
    world.fake().stop_reading();
    world
        .relay
        .as_ref()
        .expect("the fake server's relay")
        .stop_reading();
}

/// Waits for a websocket write to stall, which the client's write deadline
/// then counts from at the latest.
#[when("the client sends more data than the connection can buffer")]
#[given("the client has sent more data than the connection can buffer")]
async fn the_client_sends_more_than_can_be_buffered(world: &mut ClientWorld) {
    let before = world.stalled_writes;
    let limit = Duration::from_secs(STALLING_TOKEN_TTL.into()) + EVENT_TIMEOUT;
    let stalled = tokio::time::timeout(limit, async {
        while ferrifuge::stalled_websocket_writes() == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let large_tokens = world.token_calls.lock().unwrap().len() - STALLING_CHANNELS.len();
    let states: Vec<_> = world
        .named_subscriptions
        .iter()
        .map(ferrifuge::Subscription::state)
        .collect();
    assert!(
        stalled.is_ok(),
        "no websocket write stalled within {limit:?}: {large_tokens} large tokens fetched, \
         subscriptions {states:?}, client {:?}",
        world.client().state()
    );
}

#[then(r#"the client emits "disconnected" before the configured timeout"#)]
async fn disconnected_before_the_timeout(world: &mut ClientWorld) {
    let limit = test_config().timeout;
    let event = tokio::time::timeout(limit, world.next_event())
        .await
        .unwrap_or_else(|_| panic!("no event within {limit:?}"));
    assert!(
        matches!(event, ClientEvent::Disconnected { .. }),
        "{event:?}"
    );
}

#[then("the fake server sees the connection closed within the configured timeout plus tolerance")]
async fn the_fake_server_sees_the_connection_closed(world: &mut ClientWorld) {
    let limit = test_config().timeout + TOLERANCE;
    let closed = tokio::time::timeout(limit, async {
        while world.fake().closed_connections() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "the connection is still open after {limit:?}"
    );
}
