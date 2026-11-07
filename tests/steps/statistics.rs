//! Steps of Block 31: statistics and delta statistics.

use cucumber::{given, then, when};
use ferrifuge::machine::SubscriptionOptions;
use ferrifuge::machine::{
    Action, EventId, FrameId, Input, Machine, Timer, TransportId, TransportKind,
};
use ferrifuge::write_fossil_delta;
use ferrifuge::{State, Statistics, SubscriptionEvent};
use serde_json::{Value, json};

use super::{ClientWorld, test_config};

const CHANNEL: &str = "news";

/// What a Block 31 scenario records.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    /// The sans-IO core of an @E scenario.
    core: Option<Machine>,
    transport: Option<TransportId>,
    /// The core's statistics before the step that reads a frame, the
    /// frame's size, and the pong it sent with its size.
    before: Option<Statistics>,
    frame_bytes: u64,
    pong: Option<(FrameId, u64)>,
    /// Sizes of the payloads pushed, and of the deltas among them.
    payloads: Vec<u64>,
    deltas: Vec<u64>,
}

fn core(world: &mut ClientWorld) -> &mut Machine {
    world.stats.core.as_mut().expect("a client core")
}

fn emitted(actions: &[Action]) -> Vec<EventId> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Emit(id, _) | Action::EmitSubscription(id, _, _) => Some(*id),
            _ => None,
        })
        .collect()
}

/// Gives the core `input`, then takes every event it and the events taken
/// emit, as an application reading its streams does (Block 15).
fn handle_taking(machine: &mut Machine, input: Input) -> Vec<Action> {
    let actions = machine.handle(input);
    let mut pending = emitted(&actions);
    while let Some(id) = pending.pop() {
        pending.extend(emitted(&machine.handle(Input::EventTaken(id))));
    }
    actions
}

fn opened(actions: &[Action], kind: TransportKind) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport {
                transport, kind: k, ..
            } if *k == kind => Some(*transport),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no {kind:?} attempt in {actions:?}"))
}

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

#[given("a client core configured with websocket and http_stream endpoints")]
fn a_core_with_two_transports(world: &mut ClientWorld) {
    let mut machine = Machine::new(test_config())
        .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
        .expect("transports");
    machine.handle(Input::StreamOpened(None));
    world.stats.core = Some(machine);
}

#[when("connect is called and the websocket attempt fails")]
fn connect_and_websocket_fails(world: &mut ClientWorld) {
    let machine = core(world);
    let actions = handle_taking(machine, Input::Connect);
    let websocket = opened(&actions, TransportKind::WebSocket);
    handle_taking(
        machine,
        Input::TransportFailed {
            transport: websocket,
            message: "refused".to_owned(),
        },
    );
}

#[when("the http_stream attempt connects")]
fn http_stream_connects(world: &mut ClientWorld) {
    let machine = core(world);
    // The next transport of the first sequence goes at once.
    let actions = handle_taking(machine, Input::TimerFired(Timer::Reconnect));
    let transport = opened(&actions, TransportKind::HttpStream);
    handle_taking(machine, Input::TransportOpened(transport));
    handle_taking(
        machine,
        frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
        ),
    );
    assert_eq!(machine.state(), State::Connected);
    world.stats.transport = Some(transport);
}

#[when("the server pushes 2 publications on a subscribed channel, which the application takes")]
fn two_publications_taken(world: &mut ClientWorld) {
    let transport = world.stats.transport.expect("connected");
    let machine = core(world);
    let id = machine
        .add_subscription(CHANNEL.to_owned(), SubscriptionOptions::default())
        .expect("a subscription");
    handle_taking(machine, Input::StreamOpened(Some(id)));
    handle_taking(machine, Input::Subscribe(id));
    handle_taking(machine, frame(transport, r#"{"id":2,"subscribe":{}}"#));
    for _ in 0..2 {
        handle_taking(
            machine,
            frame(
                transport,
                r#"{"push":{"channel":"news","pub":{"data":{}}}}"#,
            ),
        );
    }
}

#[then(
    regex = r"^the core's statistics report (\d+) connect, (\d+) failed attempt and (\d+) publications delivered$"
)]
fn core_reports(world: &mut ClientWorld, connects: u64, failed: u64, delivered: u64) {
    let statistics = core(world).statistics();
    assert_eq!(
        (
            statistics.connects,
            statistics.failed_attempts,
            statistics.publications_delivered
        ),
        (connects, failed, delivered),
        "{statistics:?}"
    );
}

#[then(regex = r#"^they report transport "(\w+)" and no connected-since time$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn core_reports_transport(world: &mut ClientWorld, transport: String) {
    let statistics = core(world).statistics();
    assert_eq!(
        statistics.transport.map(TransportKind::name),
        Some(transport.as_str())
    );
    assert_eq!(statistics.connected_since, None);
}

#[given("a client core connected over websocket, whose server expects pongs")]
fn a_core_expecting_pongs(world: &mut ClientWorld) {
    let mut machine = Machine::new(test_config());
    let actions = machine.handle(Input::Connect);
    let transport = opened(&actions, TransportKind::WebSocket);
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","ping":25,"pong":true}}"#,
    ));
    world.stats.core = Some(machine);
    world.stats.transport = Some(transport);
}

#[when("the core reads a frame holding a ping and a publication push")]
fn reads_a_ping_and_a_push(world: &mut ClientWorld) {
    let transport = world.stats.transport.expect("connected");
    let data = "{}\n{\"push\":{\"channel\":\"news\",\"pub\":{\"data\":{}}}}";
    let machine = core(world);
    let before = machine.statistics();
    let actions = machine.handle(frame(transport, data));
    let pong = actions.iter().find_map(|action| match action {
        Action::Send { id, frame, .. } => Some((*id, frame.len() as u64)),
        _ => None,
    });
    world.stats.before = Some(before);
    world.stats.frame_bytes = data.len() as u64;
    world.stats.pong = pong;
}

#[when("it sends the pong, which the transport has not yet reported written")]
fn it_sends_the_pong(world: &mut ClientWorld) {
    assert!(world.stats.pong.is_some(), "no pong sent");
}

#[then(
    "the core's statistics report 2 more messages in, the frame's bytes in, and nothing more out"
)]
fn more_in_nothing_out(world: &mut ClientWorld) {
    let before = world.stats.before.clone().expect("read");
    let frame_bytes = world.stats.frame_bytes;
    let now = core(world).statistics();
    assert_eq!(now.messages_in, before.messages_in + 2);
    assert_eq!(now.bytes_in, before.bytes_in + frame_bytes);
    assert_eq!(
        (now.messages_out, now.bytes_out),
        (before.messages_out, before.bytes_out)
    );
}

#[then(
    "when the transport reports the pong written, they report 1 more message out and the pong's bytes out"
)]
fn pong_written(world: &mut ClientWorld) {
    let before = world.stats.before.clone().expect("read");
    let (pong, bytes) = world.stats.pong.expect("a pong");
    let transport = world.stats.transport.expect("connected");
    let machine = core(world);
    machine.handle(Input::FrameWritten {
        transport,
        frame: pong,
    });
    let now = machine.statistics();
    assert_eq!(now.messages_out, before.messages_out + 1);
    assert_eq!(now.bytes_out, before.bytes_out + bytes);
}

#[then(
    regex = r"^its statistics report state .disconnected., no transport, no connected-since time and every counter 0$"
)]
fn new_client_statistics(world: &mut ClientWorld) {
    let statistics = world.client().statistics();
    assert_eq!(statistics.state, State::Disconnected);
    assert_eq!(statistics.transport, None);
    assert_eq!(statistics.connected_since, None);
    assert_eq!(
        (
            statistics.connects,
            statistics.failed_attempts,
            statistics.bytes_in,
            statistics.bytes_out,
            statistics.messages_in,
            statistics.messages_out,
            statistics.publications_delivered,
            statistics.subscriptions,
            statistics.server_subscriptions,
        ),
        (0, 0, 0, 0, 0, 0, 0, 0, 0),
        "{statistics:?}"
    );
}

#[when(regex = r#"^the fake server pushes 3 publications on channel "news"$"#)]
fn pushes_three(world: &mut ClientWorld) {
    for offset in 1..=3 {
        world.fake().publish(CHANNEL, offset);
    }
}

#[when("the subscription receives them")]
async fn the_subscription_receives_them(world: &mut ClientWorld) {
    let mut received = 0;
    while received < 3 {
        if let SubscriptionEvent::Publication(_) = world.next_subscription_event(false).await {
            received += 1;
        }
    }
}

#[then(
    regex = r#"^the client's statistics report (\d+) connects?, (\d+) failed attempts, (\d+) publications delivered, (\d+) subscription and transport "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn session_statistics(
    world: &mut ClientWorld,
    connects: u64,
    failed: u64,
    delivered: u64,
    subscriptions: usize,
    transport: String,
) {
    let statistics = world.client().statistics();
    assert_eq!(
        (
            statistics.connects,
            statistics.failed_attempts,
            statistics.publications_delivered,
            statistics.subscriptions,
        ),
        (connects, failed, delivered, subscriptions),
        "{statistics:?}"
    );
    assert_eq!(
        statistics.transport.map(TransportKind::name),
        Some(transport.as_str())
    );
}

#[then(
    "they report as many bytes and messages in as the fake server sent, and as many bytes and commands out as it received"
)]
async fn traffic_as_the_fake_server_sees_it(world: &mut ClientWorld) {
    // The server's count follows its writes and reads, which the client's
    // own reports may trail by a scheduling round.
    let deadline = tokio::time::Instant::now() + super::EVENT_TIMEOUT;
    loop {
        let statistics = world.client().statistics();
        let client = (
            statistics.bytes_in,
            statistics.messages_in,
            statistics.bytes_out,
            statistics.messages_out,
        );
        let (sent, received) = (world.fake().sent_traffic(), world.fake().received_traffic());
        let server = (sent.0, sent.1, received.0, received.1);
        if client == server {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "client {client:?}, fake server {server:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[then(regex = r"^the client's statistics report (\d+) connects? and (\d+) failed attempts$")]
fn connects_and_failures(world: &mut ClientWorld, connects: u64, failed: u64) {
    let statistics = world.client().statistics();
    assert_eq!(
        (statistics.connects, statistics.failed_attempts),
        (connects, failed),
        "{statistics:?}"
    );
}

#[then("they report a connected-since time after the fake server's disconnect")]
fn connected_since_after_disconnect(world: &mut ClientWorld) {
    let since = world
        .client()
        .statistics()
        .connected_since
        .expect("connected");
    let closed = world.fake().closed_at().expect("a disconnect");
    assert!(since >= closed, "{since:?} before {closed:?}");
}

#[then(regex = r"^the client's statistics report (\d+) publications? delivered$")]
fn publications_delivered(world: &mut ClientWorld, delivered: u64) {
    assert_eq!(
        world.client().statistics().publications_delivered,
        delivered
    );
}

/// A 1 KiB payload, `n` changing a few of its last bytes: the test
/// writer's deltas copy what the payloads share and carry the rest.
fn payload(n: u8) -> Vec<u8> {
    let mut payload = vec![b'a'; 1024];
    payload[1000] = b'0' + n;
    payload[1010] = b'0' + n;
    payload
}

fn as_json_string(payload: &[u8]) -> Value {
    Value::String(String::from_utf8(payload.to_vec()).expect("ASCII payloads"))
}

#[when(
    regex = r#"^the fake server pushes 3 publications of 1 KiB that differ in a few bytes to "news"$"#
)]
fn pushes_three_similar(world: &mut ClientWorld) {
    let delta_on = world.delta.negotiated == Some(true);
    let mut base: Option<Vec<u8>> = None;
    for n in 0..3 {
        let target = payload(n);
        let publication = match &base {
            Some(base) if delta_on => {
                let delta = write_fossil_delta(base, &target);
                world.stats.deltas.push(delta.len() as u64);
                json!({"data": as_json_string(&delta), "delta": true})
            }
            _ => json!({"data": as_json_string(&target)}),
        };
        world.stats.payloads.push(target.len() as u64);
        world
            .fake()
            .send(&json!({"push": {"channel": CHANNEL, "pub": publication}}).to_string());
        base = Some(target);
    }
}

#[then(
    regex = r"^its delta statistics report (\d+) publications, (\d+) full payload and (\d+) delta payloads$"
)]
fn delta_counts(world: &mut ClientWorld, publications: u64, full: u64, deltas: u64) {
    let statistics = world.subscription().delta_statistics();
    assert_eq!(
        (
            statistics.publications,
            statistics.full_payloads,
            statistics.delta_payloads
        ),
        (publications, full, deltas),
        "{statistics:?}"
    );
}

#[then(
    "they report as bytes received the full payload's size plus the 2 deltas' sizes, and as bytes decoded the 3 payloads' sizes"
)]
fn delta_bytes(world: &mut ClientWorld) {
    let statistics = world.subscription().delta_statistics();
    let received = world.stats.payloads[0] + world.stats.deltas.iter().sum::<u64>();
    let decoded: u64 = world.stats.payloads.iter().sum();
    assert_eq!(statistics.bytes_received, received, "{statistics:?}");
    assert_eq!(statistics.bytes_decoded, decoded, "{statistics:?}");
}

#[then("they report a compression ratio above 0.5")]
fn ratio_above_half(world: &mut ClientWorld) {
    let ratio = world.subscription().delta_statistics().compression_ratio();
    assert!(ratio > 0.5, "{ratio}");
}

#[then("its delta statistics report every counter 0 and a compression ratio of 0")]
fn delta_zero(world: &mut ClientWorld) {
    let statistics = world.subscription().delta_statistics();
    assert_eq!(statistics, ferrifuge::DeltaStatistics::default());
    assert!(statistics.compression_ratio().abs() < f64::EPSILON);
}
