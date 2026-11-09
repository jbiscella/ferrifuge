//! Steps of Block 46a: publish debouncing on stream subscriptions. @E
//! scenarios drive the sans-IO core with `Machine::with_publish_debounce`;
//! @D scenarios run a tokio client against the fake server, which advises
//! the interval; @A runs against a real server, which cannot send it
//! (Centrifugo OSS has no such option).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, CallId, DebounceTimer, Input, Machine, SubscriptionId, SubscriptionOptions,
    TransportKind,
};
use ferrifuge::{
    CallError, CallRequest, Client, Config, EventDelivery, Subscription, SubscriptionEvent,
    SubscriptionState,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::ClientWorld;
use super::batching::{BatchRun, run};

const CHANNEL: &str = "cursors:a";

/// How long a step waits for what it expects.
const WAIT: Duration = Duration::from_secs(5);

type Published = Result<(), CallError>;

/// What a Block 46a scenario holds.
#[derive(Default)]
pub(crate) struct Debouncing {
    /// The debounce timers fired so far, in order (@E).
    fired: Vec<DebounceTimer>,
    /// The publishes of a @D scenario, each awaited in a task of its own,
    /// with its data, in the order they were made.
    publishes: Vec<(Value, JoinHandle<Published>)>,
    /// What the handler of the Thread scenario did.
    handler: Arc<HandlerRecord>,
    /// The second client of the @A scenario, and the publications it took.
    second: Option<Client>,
    publications: Arc<Mutex<Vec<Value>>>,
    /// The runner stalled so long before the step that should end the
    /// window that its timer may have ended it first: the scenario's
    /// premise did not hold, and that is not the client's fault (as Block
    /// 13's scenarios do on a stalled runner).
    stalled: bool,
}

/// How long before the 300 ms window ends a step still counts as before it.
const STALL_MARGIN: Duration = Duration::from_millis(50);
const WINDOW: Duration = Duration::from_millis(300);

#[derive(Default)]
struct HandlerRecord {
    /// The handler's outcome per publication offset, in order.
    results: Mutex<Vec<(u64, Published)>>,
    /// The scenario's subscription, once built.
    subscription: Mutex<Option<Subscription>>,
}

// @E: the core.

fn sent_names(run: &BatchRun, from: usize) -> Vec<String> {
    run.frames_since(from)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .filter(|command| command["publish"].is_object())
        .map(|command| {
            command["publish"]["data"]
                .as_str()
                .expect("a string as data")
                .to_owned()
        })
        .collect()
}

/// The publish frames sent after action `from`, each with its data names,
/// in order.
fn frames_of_names(run: &BatchRun, from: usize) -> Vec<Vec<String>> {
    run.frames_since(from)
        .iter()
        .map(|(_, commands)| {
            commands
                .iter()
                .filter(|command| command["publish"].is_object())
                .map(|command| {
                    command["publish"]["data"]
                        .as_str()
                        .expect("a string as data")
                        .to_owned()
                })
                .collect::<Vec<_>>()
        })
        .filter(|names: &Vec<String>| !names.is_empty())
        .collect()
}

/// The command id of the publish frame carrying data `name`.
fn command_of(run: &BatchRun, name: &str) -> u64 {
    run.frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .find(|command| command["publish"]["data"].as_str() == Some(name))
        .and_then(|command| command["id"].as_u64())
        .unwrap_or_else(|| panic!("no publish of {name:?} was sent"))
}

/// The debounce timers set after action `from`, with their delays.
fn timers_set(run: &BatchRun, from: usize) -> Vec<(DebounceTimer, Duration)> {
    run.actions[from..]
        .iter()
        .filter_map(|action| match action {
            Action::SetDebounceTimer(timer, delay) => Some((*timer, *delay)),
            _ => None,
        })
        .collect()
}

/// The debounce timers still running.
fn open_timers(world: &mut ClientWorld) -> Vec<DebounceTimer> {
    let fired = world.debouncing.fired.clone();
    let run = run(world);
    let mut open: Vec<DebounceTimer> = Vec::new();
    for action in &run.actions {
        match action {
            Action::SetDebounceTimer(timer, _) => open.push(*timer),
            Action::CancelDebounceTimer(timer) => open.retain(|running| running != timer),
            _ => {}
        }
    }
    open.retain(|timer| !fired.contains(timer));
    open
}

fn settled_since(run: &BatchRun, from: usize) -> Vec<(CallId, Published)> {
    run.actions[from..]
        .iter()
        .filter_map(|action| match action {
            Action::CallSettled { call, result } => Some((*call, result.clone().map(drop))),
            _ => None,
        })
        .collect()
}

/// The transport and the machine after the connect exchange, flushing
/// around it when automatic.
fn connect(run: &mut BatchRun, automatic: bool) {
    run.handle(Input::Connect);
    let transport = run.transport.expect("an attempt");
    run.handle(Input::TransportOpened(transport));
    if automatic {
        run.handle(Input::Flush);
    }
    let id = run
        .frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .find(|command| command["connect"].is_object())
        .and_then(|command| command["id"].as_u64())
        .expect("a connect command");
    run.frame(&json!({"id": id, "connect": {"client": "c"}}));
}

/// Subscribes `channel`, its subscribe reply advising `interval`.
fn subscribe(run: &mut BatchRun, channel: &str, interval: u32, automatic: bool) -> SubscriptionId {
    let id = run
        .machine
        .add_subscription(channel.to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    run.subscriptions.push(id);
    run.handle(Input::Subscribe(id));
    if automatic {
        run.handle(Input::Flush);
    }
    let command = run
        .frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .rfind(|command| command["subscribe"]["channel"].as_str() == Some(channel))
        .and_then(|command| command["id"].as_u64())
        .expect("a subscribe command");
    run.frame(&json!({"id": command, "subscribe": {"publish_debounce": interval}}));
    if automatic {
        run.handle(Input::Flush);
    }
    id
}

/// A core connected over websocket, subscribed to `CHANNEL`, its reply
/// advising `interval`, the steps' mark after it.
fn core(config: Config, interval: u32) -> BatchRun {
    let mut run = BatchRun::of(
        Machine::new(config)
            .with_publish_debounce()
            .with_transports(vec![TransportKind::WebSocket])
            .expect("one transport"),
    );
    connect(&mut run, false);
    subscribe(&mut run, CHANNEL, interval, false);
    run.mark = run.actions.len();
    run
}

/// Starts a publish of data `"name"` on subscription `which`, named `name`
/// in the run's calls.
fn publish(run: &mut BatchRun, which: usize, name: &str) -> CallId {
    let id = run.machine.add_subscription_call(
        run.subscriptions[which],
        CallRequest::Publish {
            channel: String::new(),
            data: format!("\"{name}\"").into_bytes(),
        },
    );
    run.calls.push((name.to_owned(), id));
    run.handle(Input::Call(id));
    id
}

/// Fires the oldest running debounce timer.
fn fire(world: &mut ClientWorld) {
    let open = open_timers(world);
    let timer = *open.first().expect("a running debounce timer");
    world.debouncing.fired.push(timer);
    run(world).handle(Input::DebounceTimerFired(timer));
}

fn mark(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
}

fn one_debounce_timer_since_mark(run: &BatchRun, delay: Duration) {
    let timers = timers_set(run, run.mark);
    assert_eq!(
        timers.iter().map(|(_, at)| *at).collect::<Vec<_>>(),
        [delay],
        "{:?}",
        run.actions
    );
}

fn duration_of(number: u64, unit: &str) -> Duration {
    match unit {
        "ms" => Duration::from_millis(number),
        "s" => Duration::from_secs(number),
        unit => panic!("an unknown unit {unit}"),
    }
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" whose subscribe reply advised an interval of (\d+) ms$"#
)]
fn a_core_with_an_interval(world: &mut ClientWorld, interval: u32) {
    world.batching.run = Some(core(Config::default(), interval));
}

#[when(
    regex = r#"^publishes "a", "b" and "c" are started on the subscription, in that order, without replies$"#
)]
fn three_publishes(world: &mut ClientWorld) {
    let run = run(world);
    for name in ["a", "b", "c"] {
        publish(run, 0, name);
    }
}

#[then(
    regex = r#"^the actions contain one send, of publish "a", and one debounce timer of (\d+) ms$"#
)]
fn one_send_of_a(world: &mut ClientWorld, delay: u64) {
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), ["a"], "{:?}", run.actions);
    one_debounce_timer_since_mark(run, Duration::from_millis(delay));
}

#[then(
    regex = r#"^when the debounce timer fires, the actions contain one send, of publish "c", and one new debounce timer of (\d+) ms$"#
)]
fn the_firing_sends_c(world: &mut ClientWorld, delay: u64) {
    mark(world);
    fire(world);
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), ["c"], "{:?}", run.actions);
    one_debounce_timer_since_mark(run, Duration::from_millis(delay));
}

#[then(
    regex = r#"^when the reply to publish "a" is read, the publish "a" settles Ok and no other$"#
)]
fn the_reply_to_a(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    let command = command_of(run, "a");
    run.frame(&json!({"id": command, "publish": {}}));
    assert_eq!(settled_since(run, run.mark), [(run.named("a"), Ok(()))]);
}

#[then(
    regex = r#"^when the reply to the trailing send is read, the publishes "b" and "c" settle Ok, in that order, and no other$"#
)]
fn the_reply_to_the_trailing_send(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    let command = command_of(run, "c");
    run.frame(&json!({"id": command, "publish": {}}));
    assert_eq!(
        settled_since(run, run.mark),
        [(run.named("b"), Ok(())), (run.named("c"), Ok(()))]
    );
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" with an interval of (\d+) ms, with one publish sent and its reply read$"#
)]
fn a_core_with_one_publish_answered(world: &mut ClientWorld, interval: u32) {
    let mut run = core(Config::default(), interval);
    publish(&mut run, 0, "a");
    let command = command_of(&run, "a");
    run.frame(&json!({"id": command, "publish": {}}));
    world.batching.run = Some(run);
}

#[when("the debounce timer fires")]
fn the_timer_fires(world: &mut ClientWorld) {
    mark(world);
    fire(world);
}

#[then("the actions contain no send and no debounce timer")]
fn no_send_no_timer(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), Vec::<String>::new());
    assert_eq!(timers_set(run, run.mark), []);
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" with an interval of (\d+) ms, with publishes "a" and "b" started$"#
)]
fn a_core_with_a_and_b(world: &mut ClientWorld, interval: u32) {
    let mut run = core(Config::default(), interval);
    publish(&mut run, 0, "a");
    publish(&mut run, 0, "b");
    world.batching.run = Some(run);
}

#[when(regex = r#"^the publish "(\w+)" is started$"#)]
fn the_publish_is_started(world: &mut ClientWorld, name: String) {
    publish(run(world), 0, &name);
}

#[when("the debounce timer fires again")]
fn the_timer_fires_again(world: &mut ClientWorld) {
    mark(world);
    fire(world);
}

#[then(regex = r#"^the sends in order were "a", "b" and "c"$"#)]
fn the_sends_in_order(world: &mut ClientWorld) {
    assert_eq!(sent_names(run(world), 0), ["a", "b", "c"]);
}

#[then("the last firing left no debounce timer")]
fn no_timer_left(world: &mut ClientWorld) {
    let open = open_timers(world);
    let run = run(world);
    assert_eq!(timers_set(run, run.mark), [], "{:?}", run.actions);
    assert_eq!(open, []);
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" with an interval of (\d+) ms, with a publish in a window$"#
)]
fn a_core_with_a_publish_in_a_window(world: &mut ClientWorld, interval: u32) {
    let mut run = core(Config::default(), interval);
    publish(&mut run, 0, "a");
    publish(&mut run, 0, "b");
    world.batching.run = Some(run);
}

#[when("the subscription is unsubscribed and the window's timer then fires")]
fn unsubscribed_then_fired(world: &mut ClientWorld) {
    let timer = open_timers(world)[0];
    let run = run(world);
    let id = run.subscriptions[0];
    run.handle(Input::Unsubscribe(id));
    run.mark = run.actions.len();
    run.handle(Input::DebounceTimerFired(timer));
}

#[then("the firing produces no action")]
fn the_firing_does_nothing(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(&run.actions[run.mark..], &[], "{:?}", run.actions);
}

#[given(
    regex = r#"^a core configured with a call timeout of (\d+) s, subscribed to channel "cursors:a" with an advised interval of (\d+) ms$"#
)]
fn a_core_with_a_clamped_interval(world: &mut ClientWorld, seconds: u64, interval: u32) {
    world.batching.run = Some(core(
        Config::default().timeout(Duration::from_secs(seconds)),
        interval,
    ));
}

#[when("one publish is started on the subscription")]
fn one_publish(world: &mut ClientWorld) {
    publish(run(world), 0, "a");
}

#[then(regex = r"^the actions contain a debounce timer of (\d+) (ms|s)$")]
fn a_timer_of(world: &mut ClientWorld, number: u64, unit: String) {
    let run = run(world);
    one_debounce_timer_since_mark(run, duration_of(number, &unit));
}

#[given(
    regex = r#"^the debounce timer fired and the trailing send of "(\w+)" was handed to the transport$"#
)]
fn the_trailing_send_went(world: &mut ClientWorld, name: String) {
    mark(world);
    fire(world);
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), [name.as_str()]);
}

/// The transport of the core of a Block 46 scenario is lost; `calls.rs`
/// owns the step text.
pub(super) fn transport_lost(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    let transport = run.transport.expect("a transport");
    run.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
}

#[then(regex = r#"^the publish "(\w+)" settles with code (\d+)$"#)]
fn the_publish_settles_with(world: &mut ClientWorld, name: String, code: u32) {
    let run = run(world);
    let id = run.named(&name);
    let settled = settled_since(run, run.mark);
    let (_, result) = settled
        .iter()
        .find(|(call, _)| *call == id)
        .unwrap_or_else(|| panic!("{name} did not settle: {settled:?}"));
    assert_eq!(result.as_ref().err().map(|error| error.code), Some(code));
}

#[given(
    regex = r#"^a core with a write queue of (\d+) bytes, subscribed to channel "cursors:a" with an interval of (\d+) ms, its queue filled by an unwritten call$"#
)]
fn a_core_with_a_full_queue(world: &mut ClientWorld, bytes: usize, interval: u32) {
    let mut run = core(Config::default().write_queue_bytes(bytes), interval);
    // Larger than the queue: it fits alone (Block 13), and fills it.
    let pad = "x".repeat(bytes);
    run.call(
        "fill",
        CallRequest::Rpc {
            method: "fill".to_owned(),
            data: format!("\"{pad}\"").into_bytes(),
        },
    );
    world.batching.run = Some(run);
}

#[given(regex = r#"^the publishes "(\w+)" and "(\w+)" started, "(\w+)" waiting for room$"#)]
fn two_publishes_a_waiting_for_room(
    world: &mut ClientWorld,
    a: String,
    b: String,
    waiting: String,
) {
    let run = run(world);
    publish(run, 0, &a);
    publish(run, 0, &b);
    assert_eq!(a, waiting);
    // Past the filling call, nothing was sent.
    assert_eq!(sent_names(run, run.mark), Vec::<String>::new());
    run.mark = run.actions.len();
}

#[then(regex = r#"^the trailing send of "\w+" waits for room behind "\w+", in order$"#)]
fn the_trailing_send_waits_for_room(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), Vec::<String>::new());
    // The firing opened the next window nevertheless (D7).
    assert_eq!(timers_set(run, run.mark).len(), 1);
}

#[then(regex = r#"^when the queue drains, "(\w+)" is sent then "(\w+)"$"#)]
fn the_queue_drains(world: &mut ClientWorld, first: String, second: String) {
    let run = run(world);
    run.mark = run.actions.len();
    let (transport, frame) = run
        .actions
        .iter()
        .find_map(|action| match action {
            Action::Send {
                transport,
                id,
                frame,
                ..
            } if String::from_utf8_lossy(frame).contains("\"fill\"") => Some((*transport, *id)),
            _ => None,
        })
        .expect("the filling call's frame");
    run.handle(Input::FrameWritten { transport, frame });
    assert_eq!(sent_names(run, run.mark), [first, second]);
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" with an interval of (\d+) ms and an open batch$"#
)]
fn a_core_with_an_open_batch(world: &mut ClientWorld, interval: u32) {
    let mut run = core(Config::default(), interval);
    run.handle(Input::StartBatching);
    run.mark = run.actions.len();
    world.batching.run = Some(run);
}

#[given(regex = r#"^the publishes "(\w+)" and "(\w+)" started$"#)]
fn two_publishes(world: &mut ClientWorld, a: String, b: String) {
    let run = run(world);
    publish(run, 0, &a);
    publish(run, 0, &b);
}

#[then("the actions contain a new debounce timer and no send")]
fn a_new_timer_and_no_send(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), Vec::<String>::new());
    assert_eq!(timers_set(run, run.mark).len(), 1, "{:?}", run.actions);
}

#[when("the batch stops")]
fn the_batch_stops(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StopBatching);
}

#[then(regex = r#"^the frames sent carry "(\w+)" then "(\w+)", in that order$"#)]
fn the_frames_carry(world: &mut ClientWorld, first: String, second: String) {
    let run = run(world);
    assert_eq!(sent_names(run, run.mark), [first, second]);
}

#[given(
    regex = r#"^a core with automatic batching, subscribed to channel "cursors:a" with an interval of (\d+) ms, and subscribed to a second channel with the same interval, each with a first publish sent and a later publish in its window$"#
)]
fn a_core_with_automatic_batching(world: &mut ClientWorld, interval: u32) {
    let mut run = BatchRun::of(
        Machine::new(Config::default())
            .with_publish_debounce()
            .with_automatic_batching()
            .with_transports(vec![TransportKind::WebSocket])
            .expect("one transport"),
    );
    connect(&mut run, true);
    subscribe(&mut run, CHANNEL, interval, true);
    subscribe(&mut run, "cursors:b", interval, true);
    for (which, first, later) in [(0, "a1", "a2"), (1, "b1", "b2")] {
        publish(&mut run, which, first);
        run.handle(Input::Flush);
        publish(&mut run, which, later);
    }
    run.handle(Input::Flush);
    let first = frames_of_names(&run, 0);
    assert_eq!(first, [["a1"], ["b1"]], "{first:?}");
    run.mark = run.actions.len();
    world.batching.run = Some(run);
}

#[when("both debounce timers fire")]
fn both_timers_fire(world: &mut ClientWorld) {
    let open = open_timers(world);
    assert_eq!(open.len(), 2, "{open:?}");
    mark(world);
    for timer in open {
        world.debouncing.fired.push(timer);
        run(world).handle(Input::DebounceTimerFired(timer));
    }
}

#[then("no frame is sent before the core is flushed")]
fn no_frame_before_the_flush(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(frames_of_names(run, run.mark), Vec::<Vec<String>>::new());
}

#[when("the core is flushed")]
fn the_core_is_flushed(world: &mut ClientWorld) {
    run(world).handle(Input::Flush);
}

#[then("one frame carries both trailing sends")]
fn one_frame_carries_both(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(frames_of_names(run, run.mark), [["a2", "b2"]]);
}

#[given(
    regex = r#"^a core subscribed to channel "cursors:a" with an interval of (\d+) ms and publish "a" sent$"#
)]
fn a_core_with_a_sent(world: &mut ClientWorld, interval: u32) {
    let mut run = core(Config::default(), interval);
    publish(&mut run, 0, "a");
    run.mark = run.actions.len();
    world.batching.run = Some(run);
}

#[given(regex = r"^the publishes (\w+(?: and \w+)*) started, waiting in the window$")]
fn publishes_in_the_window(world: &mut ClientWorld, waiting: String) {
    let run = run(world);
    for name in waiting.split(" and ") {
        publish(run, 0, name);
    }
    assert_eq!(sent_names(run, run.mark), Vec::<String>::new());
}

#[when(regex = r"^the publish (\w+) is abandoned$")]
fn the_publish_is_abandoned(world: &mut ClientWorld, name: String) {
    let run = run(world);
    let id = run.named(&name);
    run.handle(Input::CallAbandoned(id));
}

#[then(regex = r"^the firing sends (\w+)$")]
fn the_firing_sends(world: &mut ClientWorld, sent: String) {
    let run = run(world);
    let expected: Vec<&str> = if sent == "nothing" {
        vec![]
    } else {
        vec![sent.as_str()]
    };
    assert_eq!(sent_names(run, run.mark), expected, "{:?}", run.actions);
}

// @D: a tokio client against the fake server.

/// Waits until `condition` holds, for at most [`WAIT`].
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// The world's client, connected to the fake server.
async fn connected_client(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    super::transports::connect_and_wait(world).await;
}

/// The world's subscription to `CHANNEL`, not yet subscribed.
fn new_subscription(world: &mut ClientWorld) -> Subscription {
    let subscription = world
        .client()
        .new_subscription(CHANNEL)
        .expect("a new channel");
    world.track_subscription(subscription.clone());
    subscription
}

async fn subscribed(subscription: &Subscription) {
    let handle = subscription.clone();
    wait_until("the subscription to be subscribed", move || {
        handle.state() == SubscriptionState::Subscribed
    })
    .await;
}

/// A client subscribed to `CHANNEL` on the fake server, advising `interval`.
async fn a_subscribed_client(world: &mut ClientWorld, interval: Option<u32>) {
    world.fake().advise_publish_debounce(interval);
    connected_client(world).await;
    let subscription = new_subscription(world);
    subscription.subscribe();
    subscribed(&subscription).await;
}

/// The JSON data of `{"n":1}`-style literals, separated by commas or " and ".
fn data_list(text: &str) -> Vec<Value> {
    text.split("},")
        .flat_map(|piece| piece.split("} and "))
        .map(|piece| {
            let piece = piece.trim();
            let piece = if piece.ends_with('}') {
                piece.to_owned()
            } else {
                format!("{piece}}}")
            };
            serde_json::from_str(&piece).unwrap_or_else(|e| panic!("{piece}: {e}"))
        })
        .collect()
}

/// Makes a publish of `data` on the world's subscription, kept in a task.
fn start_publish(world: &mut ClientWorld, data: Value) {
    let future = world.subscription().publish(data.to_string().into_bytes());
    world
        .debouncing
        .publishes
        .push((data, tokio::spawn(future)));
}

/// The commands the fake server received with data `data`.
fn received_of(world: &ClientWorld, data: &Value) -> Vec<(Value, Instant)> {
    world
        .fake()
        .commands_with_times("publish")
        .into_iter()
        .filter(|(command, _)| command["publish"]["data"] == *data)
        .collect()
}

/// When the fake server received a publish of `data`, waiting for it.
async fn received_at(world: &ClientWorld, data: &Value) -> Instant {
    let at = std::sync::Mutex::new(None);
    wait_until("the publish to reach the fake server", || {
        let received = received_of(world, data);
        *at.lock().unwrap() = received.first().map(|(_, at)| *at);
        !received.is_empty()
    })
    .await;
    at.into_inner().unwrap().expect("checked above")
}

/// The result of the publish made with `data`, waiting for it.
async fn outcome_of(world: &mut ClientWorld, data: &Value) -> Published {
    let index = world
        .debouncing
        .publishes
        .iter()
        .position(|(of, _)| of == data)
        .unwrap_or_else(|| panic!("no publish of {data} was made"));
    let (_, handle) = world.debouncing.publishes.remove(index);
    tokio::time::timeout(WAIT, handle)
        .await
        .expect("a settled publish")
        .expect("the task of the publish")
}

fn is_finished(world: &ClientWorld, data: &Value) -> bool {
    world
        .debouncing
        .publishes
        .iter()
        .find(|(of, _)| of == data)
        .unwrap_or_else(|| panic!("no publish of {data} was made"))
        .1
        .is_finished()
}

#[given(
    regex = r#"^a client connected to the fake server, whose subscribe replies to channel "cursors:a" advise an interval of (\d+) ms and are held$"#
)]
async fn a_client_with_held_subscribe_replies(world: &mut ClientWorld, interval: u32) {
    world.fake().advise_publish_debounce(Some(interval));
    world.fake().ignore_next_subscribe();
    connected_client(world).await;
}

#[when(regex = r#"^a subscription to channel "cursors:a" publishes (.+) while subscribing$"#)]
async fn publishes_while_subscribing(world: &mut ClientWorld, data: String) {
    let subscription = new_subscription(world);
    subscription.subscribe();
    let fake_ready = world.fake();
    let probe = || {
        fake_ready
            .commands_with_times("subscribe")
            .iter()
            .any(|(command, _)| command["subscribe"]["channel"] == CHANNEL)
    };
    wait_until("the subscribe command", probe).await;
    for data in data_list(&data) {
        start_publish(world, data);
    }
}

#[when("the fake server answers the subscribe command")]
async fn the_fake_server_answers_the_subscribe(world: &mut ClientWorld) {
    let (command, _) = world
        .fake()
        .commands_with_times("subscribe")
        .into_iter()
        .find(|(command, _)| command["subscribe"]["channel"] == CHANNEL)
        .expect("a subscribe command");
    world
        .fake()
        .send(&json!({"id": command["id"], "subscribe": {"publish_debounce": 300}}).to_string());
    subscribed(world.subscription()).await;
}

#[then(regex = r"^the fake server receives one publish, with data (\{.*\})$")]
async fn receives_one_publish(world: &mut ClientWorld, data: String) {
    let data = data_list(&data).remove(0);
    received_at(world, &data).await;
    assert_eq!(
        world.fake().commands_with_times("publish").len(),
        1,
        "only the leading publish at once"
    );
}

#[then(
    regex = r"^it receives exactly one more, with data (\{.*\}), no sooner than (\d+) ms and within (\d+) s of the first$"
)]
async fn receives_one_more(world: &mut ClientWorld, data: String, floor: u64, ceiling: u64) {
    let data = data_list(&data).remove(0);
    let at = received_at(world, &data).await;
    let first = world.fake().commands_with_times("publish")[0].1;
    let gap = at.duration_since(first);
    assert!(gap >= Duration::from_millis(floor), "{gap:?}");
    assert!(gap <= Duration::from_secs(ceiling), "{gap:?}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        world.fake().commands_with_times("publish").len(),
        2,
        "exactly one more"
    );
}

#[given(
    regex = r#"^a client subscribed to channel "cursors:a" on the fake server, whose subscribe reply has (.+)$"#
)]
async fn a_client_whose_reply_has(world: &mut ClientWorld, reply: String) {
    let interval = if reply == "no publish_debounce" {
        None
    } else {
        Some(0)
    };
    a_subscribed_client(world, interval).await;
}

#[when(regex = r"^the subscription publishes (\{.+) without waiting$")]
fn publishes_without_waiting(world: &mut ClientWorld, data: String) {
    for data in data_list(&data.replace(" and ", ", ")) {
        start_publish(world, data);
    }
}

#[then(
    regex = r"^the fake server has received (\d+) publish commands, in order, within (\d+) (ms|s)$"
)]
async fn received_in_order(world: &mut ClientWorld, count: usize, number: u64, unit: String) {
    received_n_in_order(world, count, duration_of(number, &unit)).await;
}

#[then(regex = r"^the fake server has received (\d+) publish commands within (\d+) (ms|s)$")]
async fn received_commands(world: &mut ClientWorld, count: usize, number: u64, unit: String) {
    received_n_in_order(world, count, duration_of(number, &unit)).await;
}

async fn received_n_in_order(world: &mut ClientWorld, count: usize, within: Duration) {
    let started = Instant::now();
    let fake = world.fake();
    let reached = || fake.commands_with_times("publish").len() >= count;
    wait_until("the publish commands", reached).await;
    assert!(started.elapsed() <= within, "{:?}", started.elapsed());
    // None was merged or sent in another order: the numbers ascend.
    let numbers: Vec<u64> = fake
        .commands_with_times("publish")
        .iter()
        .map(|(command, _)| command["publish"]["data"]["n"].as_u64().unwrap_or(0))
        .collect();
    assert_eq!(
        numbers,
        (1..=count as u64).collect::<Vec<_>>(),
        "{numbers:?}"
    );
}

#[given(
    regex = r#"^a client subscribed to channel "cursors:a" on the fake server with an advised interval of (\d+) ms$"#
)]
async fn a_client_with_an_interval(world: &mut ClientWorld, interval: u32) {
    a_subscribed_client(world, Some(interval)).await;
}

#[given("a resubscribe of the channel whose reply advises none")]
async fn a_resubscribe_advising_none(world: &mut ClientWorld) {
    world.fake().advise_publish_debounce(None);
    let subscription = world.subscription().clone();
    subscription.unsubscribe();
    wait_until("the subscription to end", || {
        subscription.state() == SubscriptionState::Unsubscribed
    })
    .await;
    subscription.subscribe();
    subscribed(&subscription).await;
}

#[given(
    regex = r#"^a client subscribed to channel "cursors:a" on the fake server with an advised interval of (\d+) ms, which holds its publish replies$"#
)]
async fn a_client_holding_publish_replies(world: &mut ClientWorld, interval: u32) {
    world.fake().hold_publish_replies();
    a_subscribed_client(world, Some(interval)).await;
}

#[when(regex = r"^the subscription publishes (.+) and each publish is awaited in its own task$")]
fn publishes_each_in_a_task(world: &mut ClientWorld, data: String) {
    for data in data_list(&data) {
        start_publish(world, data);
    }
}

#[then(
    regex = r#"^the fake server receives \{"n":1\} at once and answers it, and only that publish completes$"#
)]
async fn the_first_is_answered(world: &mut ClientWorld) {
    let first = json!({"n": 1});
    received_at(world, &first).await;
    assert!(world.fake().answer_held_publish());
    outcome_of(world, &first).await.expect("the first publish");
    assert!(!is_finished(world, &json!({"n": 2})));
    assert!(!is_finished(world, &json!({"n": 3})));
}

#[then(regex = r"^no sooner than (\d+) ms after the first the fake server receives (\{.*\})$")]
async fn received_no_sooner(world: &mut ClientWorld, floor: u64, data: String) {
    let data = data_list(&data).remove(0);
    let at = received_at(world, &data).await;
    let first = world.fake().commands_with_times("publish")[0].1;
    let gap = at.duration_since(first);
    assert!(gap >= Duration::from_millis(floor), "{gap:?}");
}

#[then(
    regex = r"^while the fake server holds its reply, the publishes of (.+) have not completed$"
)]
async fn not_completed_while_held(world: &mut ClientWorld, data: String) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    for data in data_list(&data) {
        assert!(!is_finished(world, &data), "{data} completed");
    }
}

#[then("once the fake server answers it, both complete with success")]
async fn once_answered_both_succeed(world: &mut ClientWorld) {
    assert!(world.fake().answer_held_publish());
    for n in [2, 3] {
        outcome_of(world, &json!({"n": n}))
            .await
            .expect("a superseded publish");
    }
}

#[given(
    regex = r#"^the fake server refuses publish commands of data (\{.*\}) with error (\d+) and message "([^"]*)"$"#
)]
fn refuses_with_message(world: &mut ClientWorld, data: String, code: u32, message: String) {
    world
        .fake()
        .refuse_publishes_of(data_list(&data).remove(0), code, &message);
}

#[given(regex = r"^the fake server refuses publish commands of data (\{.*\}) with error (\d+)$")]
fn refuses(world: &mut ClientWorld, data: String, code: u32) {
    world
        .fake()
        .refuse_publishes_of(data_list(&data).remove(0), code, "refused");
}

#[when(regex = r"^the subscription publishes (.+), each publish awaited in its own task$")]
fn publishes_each_awaited(world: &mut ClientWorld, data: String) {
    for data in data_list(&data) {
        start_publish(world, data);
    }
}

#[then(
    regex = r#"^the publishes of (\{.*\}) and (\{.*\}) fail with code (\d+) and message "([^"]*)"$"#
)]
async fn fail_with_code_and_message(
    world: &mut ClientWorld,
    first: String,
    second: String,
    code: u32,
    message: String,
) {
    for data in [first, second] {
        let error = outcome_of(world, &data_list(&data).remove(0))
            .await
            .expect_err("the publish succeeded");
        assert_eq!((error.code, error.message), (code, message.clone()));
    }
}

#[then(regex = r"^the publish of (\{.*\}) succeeds$")]
async fn the_publish_succeeds(world: &mut ClientWorld, data: String) {
    outcome_of(world, &data_list(&data).remove(0))
        .await
        .expect("the publish");
}

#[then(regex = r"^the publish of (\{.*\}) fails with code (\d+)$")]
async fn the_publish_fails(world: &mut ClientWorld, data: String, code: u32) {
    let outcome = outcome_of(world, &data_list(&data).remove(0)).await;
    if world.debouncing.stalled {
        return;
    }
    let error = outcome.expect_err("the publish succeeded");
    assert_eq!(error.code, code, "{error:?}");
}

#[then(regex = r"^the publish of (\{.*\}) succeeds after the interval$")]
async fn the_publish_succeeds_after_the_interval(world: &mut ClientWorld, data: String) {
    let data = data_list(&data).remove(0);
    let at = received_at(world, &data).await;
    let first = world.fake().commands_with_times("publish")[0].1;
    assert!(at.duration_since(first) >= Duration::from_millis(200));
    outcome_of(world, &data).await.expect("the publish");
}

#[when(regex = r#"^the client calls publish on "cursors:a" with (.+) and (.+) without waiting$"#)]
fn the_client_publishes(world: &mut ClientWorld, first: String, second: String) {
    for data in [first, second] {
        let data = data_list(&data).remove(0);
        let future = world
            .client()
            .publish(CHANNEL, data.to_string().into_bytes());
        world
            .debouncing
            .publishes
            .push((data, tokio::spawn(future)));
    }
}

#[given(
    regex = r#"^the publish of \{"n":1\} completed, and the publish of (\{.*\}) waits in the window$"#
)]
async fn one_completed_one_waiting(world: &mut ClientWorld, data: String) {
    let first = json!({"n": 1});
    start_publish(world, first.clone());
    received_at(world, &first).await;
    outcome_of(world, &first).await.expect("the first publish");
    let second = data_list(&data).remove(0);
    start_publish(world, second.clone());
    assert!(received_of(world, &second).is_empty(), "it was sent");
}

#[when(regex = r"^(.+) before the interval ends$")]
async fn the_cause(world: &mut ClientWorld, cause: String) {
    note_stall(world);
    let unsubscribe_push = |code: u32| {
        json!({"push": {"channel": CHANNEL, "unsubscribe": {"code": code, "reason": "server"}}})
            .to_string()
    };
    match cause.as_str() {
        "the fake server closes the connection with code 3005" => {
            world.fake().close(3005, "advises reconnect");
        }
        "the fake server sends an unsubscribe push with code 2500" => {
            world.fake().send(&unsubscribe_push(2500));
        }
        "the fake server sends an unsubscribe push with code 2000" => {
            world.fake().send(&unsubscribe_push(2000));
        }
        "unsubscribe is called on the subscription" => world.subscription().unsubscribe(),
        "disconnect is called" => world.client().disconnect(),
        "close is called" => world.client().close().await,
        cause => panic!("an unknown cause: {cause}"),
    }
}

#[then(regex = r"^the fake server receives no publish command of data (\{.*\}) within (\d+) ms$")]
async fn no_publish_within(world: &mut ClientWorld, data: String, ms: u64) {
    let data = data_list(&data).remove(0);
    tokio::time::sleep(Duration::from_millis(ms)).await;
    assert!(
        world.debouncing.stalled || received_of(world, &data).is_empty(),
        "it was sent"
    );
}

/// Whether the time since the first publish reached the fake server
/// leaves too little of the window for the step to come before its end.
fn note_stall(world: &mut ClientWorld) {
    let first = world.fake().commands_with_times("publish")[0].1;
    if first.elapsed() + STALL_MARGIN >= WINDOW {
        eprintln!(
            "runner stall: the step ran {:?} after the first publish, too late for the 300 ms window",
            first.elapsed()
        );
        world.debouncing.stalled = true;
    }
}

#[when("the fake server closes the connection with code 3005")]
fn the_fake_server_closes(world: &mut ClientWorld) {
    note_stall(world);
    world.fake().close(3005, "advises reconnect");
}

#[when("the client is subscribed again on a new connection")]
async fn subscribed_again(world: &mut ClientWorld) {
    let fake = world.fake();
    wait_until("a second connection", || fake.connections() >= 2).await;
    subscribed(world.subscription()).await;
}

#[then(regex = r"^the publish of (\{.*\}) has failed with code (\d+)$")]
async fn has_failed_with(world: &mut ClientWorld, data: String, code: u32) {
    let outcome = outcome_of(world, &data_list(&data).remove(0)).await;
    if world.debouncing.stalled {
        return;
    }
    let error = outcome.expect_err("the publish succeeded");
    assert_eq!(error.code, code, "{error:?}");
}

#[then(
    regex = r"^the fake server receives no publish command of data (\{.*\}) within (\d+) s of the new subscribe$"
)]
async fn no_publish_after_the_new_subscribe(world: &mut ClientWorld, data: String, seconds: u64) {
    let data = data_list(&data).remove(0);
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    assert!(
        world.debouncing.stalled || received_of(world, &data).is_empty(),
        "it was resent"
    );
}

/// The handler's outcome for the publish of offset `n`.
fn handler_outcome(world: &ClientWorld, n: u64) -> Option<Published> {
    world
        .debouncing
        .handler
        .results
        .lock()
        .unwrap()
        .iter()
        .find(|(offset, _)| *offset == n)
        .map(|(_, result)| result.clone())
}

/// Blocks the thread on `future`, as a handler waiting on a call does.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(Unpark(std::thread::current())).into();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

#[given(
    regex = r#"^a client with a Thread handler and a subscription to channel "cursors:a" on the fake server with an advised interval of (\d+) ms$"#
)]
async fn a_client_with_a_thread_handler(world: &mut ClientWorld, interval: u32) {
    world.fake().advise_publish_debounce(Some(interval));
    let builder = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .event_delivery(EventDelivery::Thread);
    world.build_client(builder);
    super::transports::connect_and_wait(world).await;
    let record = Arc::clone(&world.debouncing.handler);
    let subscription = world
        .client()
        .subscription_builder(CHANNEL)
        .handler(move |event: SubscriptionEvent| {
            let SubscriptionEvent::Publication(publication) = event else {
                return;
            };
            let offset = publication.offset.unwrap_or(0);
            let subscription = record
                .subscription
                .lock()
                .unwrap()
                .clone()
                .expect("the subscription");
            let result = block_on(subscription.publish(format!("{{\"n\":{offset}}}")));
            record.results.lock().unwrap().push((offset, result));
        })
        .build()
        .expect("no subscription to the channel yet");
    *world.debouncing.handler.subscription.lock().unwrap() = Some(subscription.clone());
    world.subscription = Some(subscription.clone());
    subscription.subscribe();
    subscribed(&subscription).await;
}

#[given(
    regex = r#"^the handler, running on a publication event, has published \{"n":1\} and it completed$"#
)]
async fn the_handler_published_the_first(world: &mut ClientWorld) {
    world.fake().publish(CHANNEL, 1);
    let record = Arc::clone(&world.debouncing.handler);
    wait_until("the handler's first publish", || {
        record.results.lock().unwrap().iter().any(|(n, _)| *n == 1)
    })
    .await;
    handler_outcome(world, 1)
        .expect("checked above")
        .expect("the handler's first publish");
}

#[when(regex = r#"^the handler awaits a publish of \{"n":2\}, which waits in the window$"#)]
fn the_handler_awaits_the_second(world: &mut ClientWorld) {
    world.fake().hold_publish_replies();
    world.fake().publish(CHANNEL, 2);
}

#[then(
    regex = r#"^the fake server receives the publish of \{"n":2\} no sooner than (\d+) ms after the first$"#
)]
async fn receives_the_handlers_second(world: &mut ClientWorld, floor: u64) {
    let second = json!({"n": 2});
    let at = received_at(world, &second).await;
    let first = world.fake().commands_with_times("publish")[0].1;
    let gap = at.duration_since(first);
    assert!(gap >= Duration::from_millis(floor), "{gap:?}");
}

#[then("once the fake server answers it, the handler's publish returns success")]
async fn the_handlers_publish_returns(world: &mut ClientWorld) {
    let fake = world.fake();
    wait_until("the held publish", || fake.answer_held_publish()).await;
    let record = Arc::clone(&world.debouncing.handler);
    wait_until("the handler's second publish", || {
        record.results.lock().unwrap().iter().any(|(n, _)| *n == 2)
    })
    .await;
    handler_outcome(world, 2)
        .expect("checked above")
        .expect("the handler's second publish");
}

// @A: a real server, which has no such option and sends no interval.

#[given(
    "a subscription to a channel on a real server configured with client_publish_debounce_interval on its namespace"
)]
async fn a_server_with_the_option(world: &mut ClientWorld) {
    let server = crate::support::centrifugo::Centrifugo::start_with_config(
        r#"{
          "client": {"insecure": true},
          "http_stream": {"enabled": true},
          "channel": {"namespaces": [{"name": "debounced", "client_publish_debounce_interval": "50ms"}]}
        }"#,
    )
    .await;
    let endpoint = server.websocket_url();
    world.server = Some(server);
    let config = world.client_config();
    let second = Client::new(endpoint.clone(), config.clone()).expect("a valid endpoint");
    world.build_client(Client::builder(endpoint).config(config));
    super::transports::connect_and_wait(world).await;
    let channel = "debounced:a";
    let subscription = world
        .client()
        .new_subscription(channel)
        .expect("a new channel");
    world.track_subscription(subscription.clone());
    subscription.subscribe();
    subscribed(&subscription).await;
    second.connect();
    let of_second = second.new_subscription(channel).expect("a new channel");
    let mut events = of_second.events();
    let seen = Arc::clone(&world.debouncing.publications);
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if let SubscriptionEvent::Publication(publication) = event {
                let data: Value =
                    serde_json::from_slice(&publication.data).expect("JSON publication data");
                seen.lock().unwrap().push(data);
            }
        }
    });
    of_second.subscribe();
    subscribed(&of_second).await;
    world.named_subscriptions.push(of_second);
    world.debouncing.second = Some(second);
}

#[when("the subscription publishes 3 values in a row without waiting")]
fn publishes_3_values(world: &mut ClientWorld) {
    for n in 1..=3 {
        start_publish(world, json!({"n": n}));
    }
}

#[then("all 3 publications reach a second client's subscription, in order")]
async fn all_3_reach_the_second(world: &mut ClientWorld) {
    for data in [json!({"n": 1}), json!({"n": 2}), json!({"n": 3})] {
        outcome_of(world, &data).await.expect("the publish");
    }
    let seen = Arc::clone(&world.debouncing.publications);
    wait_until("3 publications", || seen.lock().unwrap().len() >= 3).await;
    assert_eq!(
        *seen.lock().unwrap(),
        [json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]
    );
}
