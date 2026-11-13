//! Steps of Block 29: the handshake timeout and the maximum incoming
//! message size.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportKind};
use ferrifuge::{Client, ClientEvent, Config, ErrorKind, Protocol, codes};
use prost::Message as _;

use super::transports::{a_client_with_a_relayed_http_stream, configure, connected_over_within};
use super::{ClientWorld, UNREACHABLE_ENDPOINT, test_config};
use crate::support::fake_server::sized_json;
use crate::support::protobuf;
use crate::support::relay::{CountingRelay, Mode};

/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// Slack on the life of an attempt that never answers, past the handshake
/// timeout: half the 3 s between the scenarios' 2 s handshake timeout and
/// the default 5 s `timeout`, so that a client bounding the open with
/// `timeout` still fails.
const OPEN_TOLERANCE: Duration = Duration::from_millis(2500);
/// The channel of the sized publications.
const CHANNEL: &str = "news";

/// The options a Block 29 scenario sets before its client is created.
#[derive(Debug, Default)]
pub(crate) struct Limits {
    pub(super) handshake_timeout: Option<Duration>,
    pub(super) timeout: Option<Duration>,
    max_message_bytes: Option<usize>,
    /// The sans-IO core of the @E scenario.
    run: Option<OpenRun>,
}

impl Limits {
    /// Applies the options set so far to `config`.
    pub(crate) fn configure(&self, config: &mut Config) {
        if let Some(timeout) = self.handshake_timeout {
            config.handshake_timeout = timeout;
        }
        if let Some(timeout) = self.timeout {
            config.timeout = timeout;
        }
        if let Some(bytes) = self.max_message_bytes {
            config.max_message_bytes = bytes;
        }
    }
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// Sets the clients' maximum message size and their protocol, `JSON` or
/// `Protobuf`.
pub(super) fn set_limit(world: &mut ClientWorld, bytes: usize, name: &str) {
    world.limits.max_message_bytes = Some(bytes);
    world.protocol = protocol(name);
}

fn protocol(name: &str) -> Protocol {
    match name {
        "JSON" => Protocol::Json,
        "Protobuf" => Protocol::Protobuf,
        _ => panic!("unknown protocol {name}"),
    }
}

#[given(
    regex = r"^a client with a handshake timeout of (\d+) s, configured with an unreachable websocket endpoint, the relayed http_stream endpoint and the server's sse endpoint$"
)]
fn a_client_with_a_handshake_timeout_and_a_relayed_http_stream(
    world: &mut ClientWorld,
    seconds: u64,
) {
    world.limits.handshake_timeout = Some(secs(seconds));
    a_client_with_a_relayed_http_stream(world, "the server's".to_owned());
}

#[then(
    regex = r#"^within the configured handshake timeout plus tolerance the client emits "connected" over transport "(\w+)"$"#
)]
async fn within_the_handshake_timeout_connected_over(world: &mut ClientWorld, transport: String) {
    let handshake = world.limits.handshake_timeout.expect("a handshake timeout");
    let held = world
        .relay
        .as_ref()
        .is_some_and(|relay| relay.is(Mode::Silent));
    if !held {
        // Shorter than `timeout`: a client bounding the open with it fails.
        assert!(handshake + TOLERANCE < test_config().timeout);
        connected_over_within(world, &transport, handshake + TOLERANCE).await;
        return;
    }
    // A relay holds the attempt that never answers: what is bounded is that
    // attempt's own life, from the relay accepting its connection to the
    // client closing it, not the time to "connected", which also counts the
    // next transport's open and connect reply on a real server ("not
    // connected within 3s" with the sse attempt still under way, concurrency
    // 8). Still shorter than `timeout`, so a client bounding the open with
    // it fails.
    assert!(handshake + OPEN_TOLERANCE < test_config().timeout);
    connected_over_within(world, &transport, super::EVENT_TIMEOUT).await;
    let relay = world.relay.as_ref().expect("checked above");
    let lifetime = tokio::time::timeout(super::EVENT_TIMEOUT, async {
        loop {
            if let Some(Some(lifetime)) = relay.silent_lifetimes().first() {
                return *lifetime;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the held attempt closed by the client");
    assert!(
        lifetime < handshake + OPEN_TOLERANCE,
        "the attempt the relay held lasted {lifetime:?}, past the {handshake:?} handshake timeout"
    );
}

#[given(
    regex = r"^a client with a handshake timeout of (\d+) s and a timeout of (\d+) s, configured with a websocket endpoint behind a relay that accepts but never answers$"
)]
async fn a_client_behind_a_silent_relay(world: &mut ClientWorld, handshake: u64, timeout: u64) {
    world.limits.handshake_timeout = Some(secs(handshake));
    world.limits.timeout = Some(secs(timeout));
    // Silent: the target is never dialled.
    let relay = CountingRelay::start_in("127.0.0.1:1".to_owned(), Mode::Silent).await;
    let endpoint = relay.websocket_url();
    world.relay = Some(relay);
    configure(world, vec![(TransportKind::WebSocket, endpoint)]);
}

/// The next event past the "connecting" of `connect()`, which must be the
/// error of the failed attempt, with a message ending with `ending`.
pub(super) async fn failed_attempt_ending_with(world: &mut ClientWorld, ending: &str) {
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
    assert!(error.message.ends_with(ending), "{error:?}");
}

#[then(
    regex = r#"^within (\d+) s plus tolerance the client emits an "error" event for the failed transport attempt whose message ends with "([^"]+)"$"#
)]
async fn within_the_handshake_timeout_an_error(
    world: &mut ClientWorld,
    seconds: u64,
    ending: String,
) {
    let limit = secs(seconds) + TOLERANCE;
    tokio::time::timeout(limit, failed_attempt_ending_with(world, &ending))
        .await
        .unwrap_or_else(|_| panic!("no failed attempt within {limit:?}"));
}

/// The sans-IO core under a clock of its own: the timers it sets, by
/// deadline, and the transports it opens.
#[derive(Debug)]
struct OpenRun {
    machine: Machine,
    now: Duration,
    timers: Vec<(Timer, Duration)>,
    opened: Vec<(ferrifuge::machine::TransportId, TransportKind)>,
    failed: bool,
    /// When the websocket opened.
    opened_at: Duration,
}

impl OpenRun {
    fn handle(&mut self, input: Input) {
        for action in self.machine.handle(input) {
            match action {
                Action::SetTimer(timer, after) => {
                    self.timers.retain(|(set, _)| *set != timer);
                    self.timers.push((timer, self.now + after));
                }
                Action::CancelTimer(timer) => self.timers.retain(|(set, _)| *set != timer),
                Action::OpenTransport {
                    transport, kind, ..
                } => self.opened.push((transport, kind)),
                Action::Emit(_, ClientEvent::Error(_)) => self.failed = true,
                _ => {}
            }
        }
    }

    /// Lets time run to `until`, firing the timers due by then in order.
    fn advance_to(&mut self, until: Duration) {
        loop {
            let due = self
                .timers
                .iter()
                .filter(|(_, at)| *at <= until)
                .min_by_key(|(_, at)| *at)
                .copied();
            let Some((timer, at)) = due else {
                break;
            };
            self.timers.retain(|(set, _)| *set != timer);
            self.now = at;
            self.handle(Input::TimerFired(timer));
        }
        self.now = until;
    }
}

#[given(
    regex = r"^a client with a handshake timeout of (\d+) s and a timeout of (\d+) s, configured with websocket and http_stream endpoints$"
)]
fn a_core_with_both_timeouts(world: &mut ClientWorld, handshake: u64, timeout: u64) {
    let config = test_config()
        .handshake_timeout(secs(handshake))
        .timeout(secs(timeout));
    let machine = Machine::new(config)
        .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
        .expect("transports");
    world.limits.run = Some(OpenRun {
        machine,
        now: Duration::ZERO,
        timers: Vec::new(),
        opened: Vec::new(),
        failed: false,
        opened_at: Duration::ZERO,
    });
}

fn run(world: &mut ClientWorld) -> &mut OpenRun {
    world.limits.run.as_mut().expect("a core")
}

#[when(regex = r"^the websocket opens after (\d+) ms and no connect reply arrives$")]
fn the_websocket_opens_after(world: &mut ClientWorld, millis: u64) {
    let run = run(world);
    run.handle(Input::Connect);
    let (websocket, kind) = run.opened[0];
    assert_eq!(kind, TransportKind::WebSocket);
    run.advance_to(Duration::from_millis(millis));
    assert!(!run.failed, "failed before it opened: {run:?}");
    run.opened_at = run.now;
    run.handle(Input::TransportOpened(websocket));
}

/// The moment `seconds` after the websocket opened.
fn after_open(run: &OpenRun, seconds: f64) -> Duration {
    run.opened_at + Duration::from_secs_f64(seconds)
}

#[then(regex = r"^([\d.]+) s after it opened the websocket attempt has not failed$")]
fn not_failed_after(world: &mut ClientWorld, seconds: f64) {
    let run = run(world);
    let until = after_open(run, seconds);
    run.advance_to(until);
    assert!(!run.failed, "{run:?}");
    assert_eq!(run.opened.len(), 1, "{run:?}");
}

#[then(regex = r"^([\d.]+) s after it opened the next attempt opens the http_stream transport$")]
fn then_http_stream_opens(world: &mut ClientWorld, seconds: f64) {
    let run = run(world);
    let until = after_open(run, seconds);
    let just_before = until
        .checked_sub(Duration::from_millis(1))
        .expect("after the start");
    run.advance_to(just_before);
    assert!(!run.failed, "failed early: {run:?}");
    // The deadline fires, and the next attempt of the first sequence goes
    // at once.
    run.advance_to(until);
    assert!(run.failed, "{run:?}");
    assert_eq!(
        run.opened.last().map(|(_, kind)| *kind),
        Some(TransportKind::HttpStream),
        "{run:?}"
    );
}

#[given(
    regex = r"^a client with a maximum message size of (\d+) bytes and the (JSON|Protobuf) protocol, connected to the fake server$"
)]
async fn a_limited_client_connected(world: &mut ClientWorld, bytes: usize, name: String) {
    set_limit(world, bytes, &name);
    super::codes::a_client_connected_to_the_fake_server(world).await;
}

#[given(
    regex = r#"^a client with a maximum message size of (\d+) bytes and the (JSON|Protobuf) protocol, subscribed to channel "news" on the fake server$"#
)]
async fn a_limited_client_subscribed(world: &mut ClientWorld, bytes: usize, name: String) {
    set_limit(world, bytes, &name);
    super::subscriptions::a_client_subscribed(world).await;
}

/// Sends a publication on `CHANNEL` in one websocket message of exactly
/// `size` bytes, in the scenario's encoding; the data it carries.
fn send_sized_publication(world: &mut ClientWorld, size: usize) -> Vec<u8> {
    if world.protocol == Protocol::Json {
        let prefix = format!(r#"{{"push":{{"channel":"{CHANNEL}","pub":{{"data":""#);
        let message = sized_json(&prefix, "\"}}}", size);
        // The data: the JSON string, its quotes included.
        let data = message.as_bytes()[prefix.len() - 1..message.len() - 3].to_vec();
        world.fake().send(&message);
        return data;
    }
    // The data grows until the whole frame, its length prefix included,
    // has the size: no varint length crosses a boundary near 1 KiB.
    for length in 0..size {
        let reply = protobuf::Reply {
            push: Some(protobuf::Push {
                channel: CHANNEL.to_owned(),
                r#pub: Some(protobuf::Publication {
                    data: vec![b'x'; length],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        if reply.encoded_len() + prost::length_delimiter_len(reply.encoded_len()) == size {
            world.fake().send_protobuf(&[reply]);
            return vec![b'x'; length];
        }
    }
    panic!("no Protobuf publication of {size} bytes");
}

#[when(regex = r"^the fake server sends a publication push in a websocket message of (\d+) bytes$")]
fn the_fake_server_sends_a_sized_push(world: &mut ClientWorld, size: usize) {
    send_sized_publication(world, size);
}

#[when(
    regex = r#"^the fake server sends a publication on channel "news" in a websocket message of exactly (\d+) bytes$"#
)]
fn the_fake_server_sends_an_exact_publication(world: &mut ClientWorld, size: usize) {
    let data = send_sized_publication(world, size);
    world.last_raw_data = Some(data);
}

#[then("the subscription receives that publication")]
async fn the_subscription_receives_it(world: &mut ClientWorld) {
    let publication = super::subscriptions::next_publication(world, false).await;
    assert_eq!(
        Some(&publication.data),
        world.last_raw_data.as_ref(),
        "{publication:?}"
    );
}

#[given(
    regex = r"^a client with a maximum message size of (\d+) bytes, configured with the fake server's endpoint$"
)]
fn a_limited_client(world: &mut ClientWorld, bytes: usize) {
    world.limits.max_message_bytes = Some(bytes);
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
}

#[given(
    regex = r"^the fake server answers the first connect command with a websocket message of (\d+) bytes$"
)]
fn the_fake_server_answers_oversized(world: &mut ClientWorld, size: usize) {
    world.fake().answer_next_connect_sized(size);
}

#[given(regex = r"^client options with a maximum message size of (\d+)$")]
fn client_options_with_a_maximum_message_size(world: &mut ClientWorld, bytes: usize) {
    world.pending_builder =
        Some(Client::builder(UNREACHABLE_ENDPOINT).config(test_config().max_message_bytes(bytes)));
}

#[then("building fails with a configuration error naming the maximum message size")]
fn building_fails_naming_the_size(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert_eq!(error.code, codes::error::BAD_CONFIGURATION, "{error:?}");
    assert!(error.message.contains("max_message_bytes"), "{error:?}");
}
