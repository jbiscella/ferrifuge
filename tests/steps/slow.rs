//! Steps of Block 15d: the reconnect backoff after 3008 "slow" (@E).
//!
//! The sans-IO core is the controlled clock: a step plans how each
//! connection ends, and the sampling step plays the plan on fresh machines,
//! reading the reconnect delay scheduled after the last connection.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::Config;
use ferrifuge::machine::{
    Action, Input, Machine, SubscriptionOptions, Timer, TransportId, TransportKind,
};

use super::ClientWorld;
use super::backoff::{UnderTest, command_ids, frame_ids};

/// Code and reason the server closes a slow client with.
const SLOW: (u16, &str) = (3008, "slow");

/// The scenario's client and how each of its connections ends.
#[derive(Clone)]
pub(crate) struct SlowPlan {
    config: Config,
    kind: TransportKind,
    connections: Vec<Connection>,
}

/// How one connection ends, after its connect reply.
#[derive(Clone, Copy)]
struct Connection {
    /// The subscribe reply for "chat" arrives first.
    chat_reply: bool,
    /// A server ping is read first.
    ping: bool,
    /// The connection ends by a Disconnect push, not a close code.
    push: bool,
    code: u16,
    reason: &'static str,
}

impl Connection {
    fn slow(chat_reply: bool) -> Self {
        Self {
            chat_reply,
            ping: false,
            push: false,
            code: SLOW.0,
            reason: SLOW.1,
        }
    }
}

impl SlowPlan {
    /// Plays the plan on a fresh machine: the reconnect delay scheduled
    /// after the last connection.
    fn last_delay(&self) -> Duration {
        let mut machine = Machine::new(self.config.clone())
            .with_transports(vec![self.kind])
            .expect("one transport");
        for channel in ["news", "chat"] {
            let id = machine
                .add_subscription(channel.to_owned(), SubscriptionOptions::default())
                .expect("a new channel");
            machine.handle(Input::Subscribe(id));
        }
        let mut actions = machine.handle(Input::Connect);
        let mut delay = None;
        for connection in &self.connections {
            let transport = opened(&actions);
            let mut sent = actions;
            sent.extend(machine.handle(Input::TransportOpened(transport)));
            let connect = connect_id(&sent);
            let frame = |machine: &mut Machine, json: String| {
                machine.handle(Input::Frame {
                    transport,
                    data: json.into_bytes(),
                })
            };
            let replied = frame(
                &mut machine,
                format!(r#"{{"id":{connect},"connect":{{"client":"c","ping":25,"pong":true}}}}"#),
            );
            if connection.chat_reply {
                // Subscribe commands go in channel order: "chat" is the
                // second, with the connect command over websocket (Block 36,
                // D5) or after its reply.
                let chat = [
                    command_ids(&sent, "subscribe"),
                    command_ids(&replied, "subscribe"),
                ]
                .concat()[1];
                frame(&mut machine, format!(r#"{{"id":{chat},"subscribe":{{}}}}"#));
            }
            if connection.ping {
                frame(&mut machine, "{}".to_owned());
            }
            actions = if connection.push {
                frame(
                    &mut machine,
                    format!(
                        r#"{{"push":{{"disconnect":{{"code":{},"reason":"{}"}}}}}}"#,
                        connection.code, connection.reason
                    ),
                )
            } else {
                machine.handle(Input::TransportClosed {
                    transport,
                    code: Some(connection.code),
                    reason: connection.reason.to_owned(),
                })
            };
            delay = Some(reconnect_delay(&actions));
            actions = machine.handle(Input::TimerFired(Timer::Reconnect));
        }
        delay.expect("at least one connection")
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

/// Id of the connect command: sent on the websocket, or carried by the
/// request that opens an emulated transport.
fn connect_id(actions: &[Action]) -> u64 {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. }
            | Action::OpenTransport {
                initial_frame: Some(frame),
                ..
            } => Some(frame),
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

fn plan(world: &mut ClientWorld) -> &mut SlowPlan {
    world
        .slow
        .as_mut()
        .expect("a client planned by an earlier step")
}

#[given(
    regex = r#"^a client with minReconnectDelay (\d+) ms and maxReconnectDelay (\d+) ms, subscribed to channels "news" and "chat" over (websocket|http_stream)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_subscribed_client(world: &mut ClientWorld, min: u64, max: u64, transport: String) {
    let config = Config::default()
        .min_reconnect_delay(Duration::from_millis(min))
        .max_reconnect_delay(Duration::from_millis(max));
    world.backoff = Some(UnderTest::Reconnect(config.clone()));
    world.slow = Some(SlowPlan {
        config,
        kind: match transport.as_str() {
            "websocket" => TransportKind::WebSocket,
            _ => TransportKind::HttpStream,
        },
        connections: Vec::new(),
    });
}

#[when(
    regex = r#"^the server closes (\d+) connections in a row with code 3008 "slow", each after its connect reply( and the subscribe reply for "chat")?, before any server ping$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn closes_in_a_row(world: &mut ClientWorld, closes: usize, chat: String) {
    let connection = Connection::slow(!chat.is_empty());
    plan(world)
        .connections
        .extend(std::iter::repeat_n(connection, closes));
}

#[when(
    regex = r#"^the server ends (\d+) connections in a row with a Disconnect push of code 3008 "slow", each after its connect reply and the subscribe reply for "chat", before any server ping$"#
)]
fn pushes_in_a_row(world: &mut ClientWorld, closes: usize) {
    let connection = Connection {
        push: true,
        ..Connection::slow(true)
    };
    plan(world)
        .connections
        .extend(std::iter::repeat_n(connection, closes));
}

#[when(
    r#"the next connection reads a server ping after its connect reply, then the server closes it with code 3008 "slow""#
)]
fn ping_then_slow(world: &mut ClientWorld) {
    plan(world).connections.push(Connection {
        ping: true,
        ..Connection::slow(false)
    });
}

#[when(
    r#"the server closes the next connection with code 3001 "shutdown" after its connect reply, before any server ping"#
)]
fn another_code(world: &mut ClientWorld) {
    plan(world).connections.push(Connection {
        code: 3001,
        reason: "shutdown",
        ..Connection::slow(false)
    });
}

#[when(
    regex = r"^the reconnect delay after (?:the last of them|that close) is computed (\d+) times$"
)]
async fn delay_after_last_computed(world: &mut ClientWorld, times: usize) {
    let plan = plan(world).clone();
    world.computed_delays =
        super::off_the_runner(move || (0..times).map(|_| plan.last_delay()).collect()).await;
    world.delays_by_attempt = false;
}

#[then(regex = r"^the largest computed delay is more than (\d+) ms$")]
fn largest_delay_is_more_than(world: &mut ClientWorld, millis: u64) {
    let largest = world.computed_delays.iter().max().expect("delays computed");
    assert!(
        *largest > Duration::from_millis(millis),
        "largest of {} delays: {largest:?}",
        world.computed_delays.len()
    );
}
