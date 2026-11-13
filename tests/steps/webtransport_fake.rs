//! Steps of Block 48b: WebTransport against the fake WebTransport server
//! (@D, D19), and the core's backoff after a WebTransport connection ended
//! with no code (@E, D9).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportId, TransportKind};
use ferrifuge::{ClientEvent, Config};

use super::ClientWorld;
use super::backoff::frame_ids;
use super::transports::{configure, connect_and_wait};
use crate::support::fake_server::{Announce, FakeServer};
use crate::support::fake_webtransport::{
    FakeWebTransport, STREAM_WINDOW, disconnect_push, sized_publication,
};

/// Margin over the configured timeout of a step that waits for it.
const TOLERANCE: Duration = Duration::from_secs(1);
/// The reconnect-advised code the fake server ends a session with.
const RECONNECT: (u32, &str) = (3001, "shutdown");
/// Runs of the @E scenario's plan, whose delays are compared with the
/// bounds of an attempt: full jitter, so a delay of attempt 3 above
/// attempt 2's upper bound comes in half the runs.
const RUNS: usize = 200;

/// Starts the scenario's fake WebTransport server, selecting `h3` or, with
/// `no_alpn`, none, over a fake websocket server that runs its scripts.
/// There is no TCP relay: the client dials the fake server directly.
async fn start(world: &mut ClientWorld, no_alpn: bool) {
    let fake = FakeServer::start(Announce::default()).await;
    world.webtransport.fake = Some(FakeWebTransport::start(&fake, no_alpn).await);
    world.fake = Some(fake);
    world.relay = None;
}

fn fake_webtransport(world: &ClientWorld) -> &FakeWebTransport {
    world
        .webtransport
        .fake
        .as_ref()
        .expect("a fake WebTransport server started by an earlier step")
}

/// Configures the scenario's client with only the fake server's
/// WebTransport endpoint, starting a fake server selecting `h3` if the
/// scenario has none.
async fn configure_fake(world: &mut ClientWorld) {
    if world.webtransport.fake.is_none() {
        start(world, false).await;
    }
    let url = fake_webtransport(world).url();
    configure(world, vec![(TransportKind::WebTransport, url)]);
}

/// Connects the configured client and waits until it is connected over
/// WebTransport.
async fn connect_over_webtransport(world: &mut ClientWorld) {
    connect_and_wait(world).await;
    assert_eq!(
        world.client().statistics().transport,
        Some(TransportKind::WebTransport)
    );
}

#[given(
    regex = r#"^a fake WebTransport server whose TLS configuration offers (no application protocol|only "h3")$"#
)]
async fn a_fake_webtransport_server(world: &mut ClientWorld, offers: String) {
    start(world, offers == "no application protocol").await;
}

#[given("a client configured with only the fake server's WebTransport endpoint")]
async fn a_client_with_the_fake_endpoint(world: &mut ClientWorld) {
    configure_fake(world).await;
}

#[then(regex = r"^the fake server counted (\d+) QUIC handshakes?$")]
async fn the_fake_server_counted(world: &mut ClientWorld, handshakes: usize) {
    // A handshake more, which the step must not miss, would come at once.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fake_webtransport(world).handshakes(), handshakes);
}

#[when("the fake server ends the session after a disconnect push with a reconnect-advised code")]
async fn ends_the_session_with_a_reconnect_code(world: &mut ClientWorld) {
    let (code, reason) = RECONNECT;
    fake_webtransport(world).end_stream(disconnect_push(code, reason), true);
    // The client reconnects: its disconnect and the new attempt.
    let event = world.next_event().await;
    assert!(
        matches!(&event, ClientEvent::Connecting { code: got, .. } if *got == code),
        "{event:?}"
    );
}

#[given(
    regex = r"^a client connected to the fake WebTransport server after (\d+) QUIC handshakes$"
)]
async fn connected_after_handshakes(world: &mut ClientWorld, handshakes: usize) {
    configure_fake(world).await;
    connect_over_webtransport(world).await;
    assert_eq!(fake_webtransport(world).handshakes(), handshakes);
}

#[when(r#"the fake server's TLS configuration changes to offer only "h3""#)]
fn the_fake_server_requires_h3(world: &mut ClientWorld) {
    fake_webtransport(world).select_no_alpn(false);
}

#[given("a client connected to the fake WebTransport server")]
async fn a_client_connected_to_the_fake(world: &mut ClientWorld) {
    configure_fake(world).await;
    connect_over_webtransport(world).await;
}

#[given(regex = r"^a client with a timeout of (\d+) s connected to the fake WebTransport server$")]
async fn a_client_with_a_timeout_connected(world: &mut ClientWorld, seconds: u64) {
    world.timeout = Some(Duration::from_secs(seconds));
    a_client_connected_to_the_fake(world).await;
}

#[given(
    regex = r"^a client with a maximum message size of (\d+) bytes and the (JSON|Protobuf) protocol, connected to the fake WebTransport server$"
)]
async fn a_client_with_a_limit_connected(world: &mut ClientWorld, bytes: usize, name: String) {
    super::limits::set_limit(world, bytes, &name);
    a_client_connected_to_the_fake(world).await;
}

#[when(
    regex = r#"^the fake server writes a disconnect push with code (\d+) and reason "([^"]*)" and ends the stream in the same write$"#
)]
fn a_push_and_the_end(world: &mut ClientWorld, code: u32, reason: String) {
    fake_webtransport(world).end_stream(disconnect_push(code, &reason), false);
}

#[when("the fake server ends the stream without a disconnect push")]
fn the_end_without_a_push(world: &mut ClientWorld) {
    fake_webtransport(world).end_stream(Vec::new(), false);
}

#[given(
    r#"a client configured with the fake WebTransport server's endpoint and the fake websocket server's endpoint, connected over transport "webtransport""#
)]
async fn a_client_with_both_fake_endpoints(world: &mut ClientWorld) {
    start(world, false).await;
    let endpoints = vec![
        (TransportKind::WebTransport, fake_webtransport(world).url()),
        (
            TransportKind::WebSocket,
            format!("ws://{}/connection/websocket", world.fake().address()),
        ),
    ];
    configure(world, endpoints);
    connect_over_webtransport(world).await;
}

#[given("the fake WebTransport server ends every new stream before answering the connect")]
fn ends_new_streams(world: &mut ClientWorld) {
    fake_webtransport(world).end_streams_before_connect();
}

#[when(
    "the fake WebTransport server ends the session after a disconnect push with a reconnect-advised code"
)]
async fn the_webtransport_server_ends_the_session(world: &mut ClientWorld) {
    ends_the_session_with_a_reconnect_code(world).await;
}

#[given("the fake server stops reading the stream")]
fn stops_reading(world: &mut ClientWorld) {
    fake_webtransport(world).stop_reading();
}

#[when("the client sends more data than the stream's flow-control window")]
fn sends_more_than_the_window(world: &mut ClientWorld) {
    // Eight messages of twice the window, as JSON strings.
    let size = 2 * usize::try_from(STREAM_WINDOW).expect("a small window");
    for _ in 0..8 {
        let data = format!("\"{}\"", "x".repeat(size));
        drop(tokio::spawn(world.client().send(data)));
    }
}

#[when("the fake server writes a publication push of 1025 bytes, split over three writes")]
fn an_oversized_push(world: &mut ClientWorld) {
    let protobuf = world.protocol == ferrifuge::Protocol::Protobuf;
    fake_webtransport(world).write_split(sized_publication("news", 1025, protobuf));
}

#[then(
    "the fake server sees the stream finished and the QUIC connection closed within the configured timeout plus tolerance"
)]
async fn the_fake_server_sees_the_session_end(world: &mut ClientWorld) {
    let start = Instant::now();
    let limit = world.configured_timeout() + TOLERANCE;
    let ended = tokio::time::timeout(limit, async {
        loop {
            if let (Some(finished), Some(closed)) = fake_webtransport(world).ends() {
                return (finished, closed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let (finished, closed) = ended.unwrap_or_else(|_| {
        panic!(
            "after {limit:?}, stream finished and connection closed: {:?}",
            fake_webtransport(world).ends()
        )
    });
    assert!(
        finished <= closed,
        "the connection closed before the stream was finished"
    );
    assert!(closed.duration_since(start) <= limit);
}

/// The @E scenario's client: its options and how each connection ends.
#[derive(Clone, Debug, Default)]
pub(crate) struct NoCodePlan {
    config: Config,
    /// For each connection in turn, whether a server ping is read on it
    /// before its transport ends with no code.
    pings: Vec<bool>,
}

impl NoCodePlan {
    /// Plays the plan on a fresh machine: the reconnect delay scheduled
    /// after each connection.
    fn delays(&self) -> Vec<Duration> {
        let mut machine = Machine::new(self.config.clone())
            .with_transports(vec![TransportKind::WebTransport])
            .expect("one transport");
        let mut actions = machine.handle(Input::Connect);
        let mut delays = Vec::new();
        for &ping in &self.pings {
            let transport = opened(&actions);
            let sent = machine.handle(Input::TransportOpened(transport));
            let connect = connect_id(&sent);
            let frame = |machine: &mut Machine, json: String| {
                machine.handle(Input::Frame {
                    transport,
                    data: json.into_bytes(),
                })
            };
            frame(
                &mut machine,
                format!(r#"{{"id":{connect},"connect":{{"client":"c","ping":25,"pong":true}}}}"#),
            );
            if ping {
                frame(&mut machine, "{}".to_owned());
            }
            actions = machine.handle(Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            });
            delays.push(reconnect_delay(&actions));
            actions = machine.handle(Input::TimerFired(Timer::Reconnect));
        }
        delays
    }
}

fn opened(actions: &[Action]) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport to open")
}

fn connect_id(actions: &[Action]) -> u64 {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(frame),
            _ => None,
        })
        .flat_map(|frame| frame_ids(frame, "connect"))
        .last()
        .expect("a connect command")
}

fn reconnect_delay(actions: &[Action]) -> Duration {
    actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .expect("a reconnect is scheduled")
}

/// The delays the reconnect backoff may give attempt `attempt`, counted
/// from 1 (Block 2: full jitter over `[min, min + min * 2^(attempt - 1)]`,
/// capped at max).
fn bounds(config: &Config, attempt: u32) -> (Duration, Duration) {
    let (min, max) = (config.min_reconnect_delay, config.max_reconnect_delay);
    let ceiling = max.min(min * 2u32.pow(attempt - 1));
    (min, max.min(min + ceiling))
}

fn plan(world: &mut ClientWorld) -> &mut NoCodePlan {
    &mut world.webtransport.plan
}

/// The delays scheduled after connection `index` of the plan, over
/// [`RUNS`] runs.
fn sampled(world: &mut ClientWorld, index: usize) -> Vec<Duration> {
    let plan = plan(world).clone();
    (0..RUNS).map(|_| plan.delays()[index]).collect()
}

/// Asserts that every delay lies within the bounds of `attempt` and that
/// the largest one is above the upper bound of the attempt before it.
fn within_attempt(config: &Config, delays: &[Duration], attempt: u32) {
    let (lower, upper) = bounds(config, attempt);
    for delay in delays {
        assert!(
            (lower..=upper).contains(delay),
            "{delay:?} is outside [{lower:?}, {upper:?}], the bounds of attempt {attempt}"
        );
    }
    if attempt > 1 {
        let (_, below) = bounds(config, attempt - 1);
        let largest = delays.iter().max().expect("delays sampled");
        assert!(
            *largest > below,
            "no delay of {} above {below:?}, the bound of attempt {}",
            delays.len(),
            attempt - 1
        );
    }
}

#[given(
    regex = r"^a client with a minimum reconnect delay of (\d+) s and a maximum of (\d+) s, configured with a WebTransport endpoint$"
)]
fn a_planned_client(world: &mut ClientWorld, min: u64, max: u64) {
    *plan(world) = NoCodePlan {
        config: Config::default()
            .min_reconnect_delay(Duration::from_secs(min))
            .max_reconnect_delay(Duration::from_secs(max)),
        pings: Vec::new(),
    };
}

#[when(
    "three times in a row the transport opens, the connect reply arrives and the transport ends with no code before a server ping is read"
)]
fn three_ends_before_a_ping(world: &mut ClientWorld) {
    plan(world).pings.extend([false; 3]);
}

#[then("the third reconnect delay lies within the backoff bounds of attempt 3")]
fn the_third_delay(world: &mut ClientWorld) {
    let delays = sampled(world, 2);
    within_attempt(&plan(world).config.clone(), &delays, 3);
}

#[when("the next connection reads a server ping before its transport ends with no code")]
fn a_ping_then_the_end(world: &mut ClientWorld) {
    plan(world).pings.push(true);
}

#[then("the reconnect delay after it lies within the backoff bounds of attempt 1")]
fn the_delay_after_it(world: &mut ClientWorld) {
    let last = plan(world).pings.len() - 1;
    let delays = sampled(world, last);
    within_attempt(&plan(world).config.clone(), &delays, 1);
}
