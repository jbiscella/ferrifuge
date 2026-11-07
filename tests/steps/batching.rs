//! Steps of Block 18: batches. @E scenarios drive the sans-IO core on a
//! simulated clock; @D scenarios run a tokio client against the fake server.

use std::collections::BTreeMap;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, CallId, CallTimer, Input, Machine, SubscriptionId, SubscriptionOptions,
    SubscriptionTimer, Timer, TransportId,
};
use ferrifuge::{
    Batch, CallError, CallReply, CallRequest, ClientEvent, Config, HistoryOptions, Protocol,
    TokenError,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::ClientWorld;

/// What a Block 18 scenario holds.
#[derive(Default)]
pub(crate) struct Batching {
    /// The sans-IO core of an @E scenario.
    pub(super) run: Option<BatchRun>,
    /// The open batches of a @D scenario, by name ("" for the only one).
    batches: BTreeMap<String, Batch>,
    /// Calls of a @D or @B scenario, in the order they were made, each
    /// giving its error code when it fails.
    calls: Vec<JoinHandle<Result<(), u32>>>,
    /// Commands the fake server had received when the batch started.
    received_at_start: usize,
}

/// A timer the simulated clock runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Key {
    Client(Timer),
    Call(CallId, CallTimer),
    Subscription(SubscriptionId, SubscriptionTimer),
}

/// A connected machine on a simulated clock.
pub(crate) struct BatchRun {
    pub(super) machine: Machine,
    pub(super) transport: Option<TransportId>,
    now: Duration,
    timers: Vec<(Duration, Key)>,
    /// Every action, in order.
    pub(super) actions: Vec<Action>,
    /// Where the actions of the current batch start.
    mark: usize,
    /// Calls by name, in the order they were made.
    calls: Vec<(String, CallId)>,
    pub(super) settled: BTreeMap<CallId, Result<CallReply, CallError>>,
    subscriptions: Vec<SubscriptionId>,
    /// Transports closed so far.
    closed: Vec<TransportId>,
}

impl BatchRun {
    pub(super) fn new(config: Config, token_source: bool) -> Self {
        let machine = if token_source {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        Self {
            machine,
            transport: None,
            now: Duration::ZERO,
            timers: Vec::new(),
            actions: Vec::new(),
            mark: 0,
            calls: Vec::new(),
            settled: BTreeMap::new(),
            subscriptions: Vec::new(),
            closed: Vec::new(),
        }
    }

    pub(super) fn handle(&mut self, input: Input) {
        for action in self.machine.handle(input) {
            match &action {
                Action::OpenTransport { transport, .. } => self.transport = Some(*transport),
                Action::SetTimer(timer, delay) => self.set(Key::Client(*timer), *delay),
                Action::CancelTimer(timer) => self.cancel(Key::Client(*timer)),
                Action::ShortenTimer(timer, by) => self.shorten(Key::Client(*timer), *by),
                Action::ShortenSubscriptionTimer(id, timer, by) => {
                    self.shorten(Key::Subscription(*id, *timer), *by);
                }
                Action::SetCallTimer(call, timer, delay) => {
                    self.set(Key::Call(*call, *timer), *delay);
                }
                Action::CancelCallTimer(call, timer) => self.cancel(Key::Call(*call, *timer)),
                Action::SetSubscriptionTimer(id, timer, delay) => {
                    self.set(Key::Subscription(*id, *timer), *delay);
                }
                Action::CancelSubscriptionTimer(id, timer) => {
                    self.cancel(Key::Subscription(*id, *timer));
                }
                Action::CallSettled { call, result } => {
                    self.settled.insert(*call, result.clone());
                }
                _ => {}
            }
            self.actions.push(action);
        }
    }

    fn set(&mut self, key: Key, delay: Duration) {
        self.cancel(key);
        self.timers.push((self.now + delay, key));
    }

    fn cancel(&mut self, key: Key) {
        self.timers.retain(|(_, of)| *of != key);
    }

    /// A running timer fires `by` earlier, but not before now; one not
    /// running is ignored (Block 25, D3).
    fn shorten(&mut self, key: Key, by: Duration) {
        let now = self.now;
        for (at, _) in self.timers.iter_mut().filter(|(_, of)| *of == key) {
            *at = at.saturating_sub(by).max(now);
        }
    }

    /// How long until the timer `key` fires, if it runs.
    pub(super) fn due(&self, key: Key) -> Option<Duration> {
        self.timers
            .iter()
            .find(|(_, of)| *of == key)
            .map(|(at, _)| at.saturating_sub(self.now))
    }

    /// Lets `by` of simulated time pass, firing the timers due.
    pub(super) fn advance(&mut self, by: Duration) {
        let until = self.now + by;
        while let Some((index, &(at, key))) = self
            .timers
            .iter()
            .enumerate()
            .filter(|(_, (at, _))| *at <= until)
            .min_by_key(|(_, (at, _))| *at)
        {
            self.timers.remove(index);
            self.now = at;
            self.handle(match key {
                Key::Client(timer) => Input::TimerFired(timer),
                Key::Call(call, timer) => Input::CallTimerFired(call, timer),
                Key::Subscription(id, timer) => Input::SubscriptionTimerFired(id, timer),
            });
        }
        self.now = until;
    }

    /// Opens the transport and answers the connect command with `result`.
    pub(super) fn connect(&mut self, result: &Value) {
        let transport = self.transport.expect("a transport opened");
        self.handle(Input::TransportOpened(transport));
        // Over an emulated transport the connect command opened it.
        let initial = self.actions.iter().rev().find_map(|action| match action {
            Action::OpenTransport {
                initial_frame: Some(frame),
                ..
            } => commands_of(frame).into_iter().next(),
            _ => None,
        });
        let id = self
            .frames_since(0)
            .iter()
            .rev()
            .flat_map(|(_, commands)| commands.clone())
            .chain(initial)
            .find(|command| command["connect"].is_object())
            .and_then(|command| command["id"].as_u64())
            .expect("a connect command");
        self.frame(&json!({"id": id, "connect": result}));
    }

    pub(super) fn frame(&mut self, value: &Value) {
        self.handle(Input::Frame {
            transport: self.transport.expect("a transport"),
            data: value.to_string().into_bytes(),
        });
    }

    /// The frames sent after action `from`, each with its commands.
    pub(super) fn frames_since(&self, from: usize) -> Vec<(TransportId, Vec<Value>)> {
        self.actions[from..]
            .iter()
            .filter_map(|action| match action {
                Action::Send {
                    transport, frame, ..
                } => Some((*transport, commands_of(frame))),
                _ => None,
            })
            .collect()
    }

    /// The frames sent since the batch started.
    fn frames(&self) -> Vec<(TransportId, Vec<Value>)> {
        self.frames_since(self.mark)
    }

    pub(super) fn call(&mut self, name: &str, request: CallRequest) -> CallId {
        let id = self.machine.add_call(request);
        self.calls.push((name.to_owned(), id));
        self.handle(Input::Call(id));
        id
    }

    pub(super) fn rpc(&mut self, name: &str) -> CallId {
        self.call(
            name,
            CallRequest::Rpc {
                method: name.to_owned(),
                data: b"{}".to_vec(),
            },
        )
    }

    fn named(&self, name: &str) -> CallId {
        self.calls
            .iter()
            .find(|(of, _)| of == name)
            .map(|(_, id)| *id)
            .expect("a call of that name")
    }
}

/// The JSON commands of a frame, unwrapping an emulation request.
fn commands_of(frame: &[u8]) -> Vec<Value> {
    let text = String::from_utf8_lossy(frame);
    let commands: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON"))
        .collect();
    match commands.as_slice() {
        [request] if request.get("session").is_some() => commands_of(
            request["data"]
                .as_str()
                .expect("the commands of an emulation request")
                .as_bytes(),
        ),
        _ => commands,
    }
}

pub(super) fn connected_run(config: Config) -> BatchRun {
    let mut run = BatchRun::new(config, false);
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c"}));
    run
}

pub(super) fn run(world: &mut ClientWorld) -> &mut BatchRun {
    world
        .batching
        .run
        .as_mut()
        .expect("an @E batching scenario")
}

/// Whether an @E batching scenario drives the core.
pub(crate) fn drives_a_machine(world: &ClientWorld) -> bool {
    world.batching.run.is_some()
}

/// "a connected client" in a scenario without a server: the core.
pub(crate) fn a_connected_core(world: &mut ClientWorld) {
    world.batching.run = Some(connected_run(Config::default()));
}

pub(crate) fn elapse(world: &mut ClientWorld, seconds: u64) {
    run(world).advance(Duration::from_secs(seconds));
}

pub(crate) fn server_ping(world: &mut ClientWorld) {
    run(world).frame(&json!({}));
}

pub(crate) fn disconnect(world: &mut ClientWorld) {
    run(world).handle(Input::Disconnect);
}

/// An rpc whose command encodes to `size` bytes once numbered `id`.
fn rpc_of_size(id: u64, size: usize) -> CallRequest {
    let base = json!({"id": id, "rpc": {"method": "m", "data": ""}})
        .to_string()
        .len();
    let pad = "x".repeat(size.checked_sub(base).expect("a larger size"));
    CallRequest::Rpc {
        method: "m".to_owned(),
        data: format!("\"{pad}\"").into_bytes(),
    }
}

/// Makes calls whose commands encode to `sizes`, in order, numbered from
/// the next command id (the connect command took 1).
fn calls_of_sizes(run: &mut BatchRun, sizes: &[usize]) {
    for (n, size) in sizes.iter().enumerate() {
        let id = 2 + n as u64;
        run.call(&format!("c{n}"), rpc_of_size(id, *size));
    }
}

fn sizes(list: &str) -> Vec<usize> {
    list.split(", ")
        .map(|size| size.trim().parse().expect("a size"))
        .collect()
}

#[given(regex = r"^a connected client with a batch frame cap of (\d+) bytes$")]
fn a_client_with_a_frame_cap(world: &mut ClientWorld, cap: usize) {
    world.batching.run = Some(connected_run(Config {
        batch_frame_bytes: cap,
        ..Config::default()
    }));
}

#[given(regex = r"^a connected client with a write queue of (\d+) bytes and nothing unwritten$")]
fn a_client_with_a_write_queue(world: &mut ClientWorld, bytes: usize) {
    world.batching.run = Some(connected_run(Config {
        write_queue_bytes: bytes,
        ..Config::default()
    }));
}

#[given(regex = r"^a connected client with a timeout of (\d+) s$")]
fn a_client_with_a_timeout(world: &mut ClientWorld, seconds: u64) {
    world.batching.run = Some(connected_run(Config {
        timeout: Duration::from_secs(seconds),
        ..Config::default()
    }));
}

#[given(
    regex = r"^a connected client with a timeout of (\d+) s and a subscription not subscribed$"
)]
fn a_client_with_a_subscription(world: &mut ClientWorld, seconds: u64) {
    let mut run = connected_run(Config {
        timeout: Duration::from_secs(seconds),
        ..Config::default()
    });
    let id = run
        .machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    run.subscriptions.push(id);
    world.batching.run = Some(run);
}

#[given(
    "a connected client with a token callback, whose connect reply asked for pongs and whose connection token expires"
)]
fn a_client_with_a_token_callback(world: &mut ClientWorld) {
    let mut run = BatchRun::new(
        Config {
            token: "t0".to_owned(),
            ..Config::default()
        },
        true,
    );
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c", "ping": 25, "pong": true, "expires": true, "ttl": 10}));
    world.batching.run = Some(run);
}

#[when(
    regex = r"^the application starts a batch, makes calls whose commands encode to ([\d, ]+) bytes, and stops the batch$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_of_sizes(world: &mut ClientWorld, list: String) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StartBatching);
    calls_of_sizes(run, &sizes(&list));
    run.handle(Input::StopBatching);
}

#[when(
    regex = r"^the application starts a batch, makes five calls whose commands encode to (\d+) bytes each, and stops the batch$"
)]
fn a_batch_of_five(world: &mut ClientWorld, size: usize) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StartBatching);
    calls_of_sizes(run, &[size; 5]);
    run.handle(Input::StopBatching);
}

#[then(
    regex = r"^the client sends frames carrying ([\d, ]+) calls, in the order the calls were made$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn frames_carrying(world: &mut ClientWorld, list: String) {
    let run = run(world);
    let frames = run.frames();
    let counts: Vec<usize> = frames.iter().map(|(_, commands)| commands.len()).collect();
    assert_eq!(counts, sizes(&list), "{frames:?}");
    let ids: Vec<u64> = frames
        .iter()
        .flat_map(|(_, commands)| commands.iter().filter_map(|command| command["id"].as_u64()))
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "{frames:?}");
}

#[then(regex = r"^no frame carrying several commands exceeds (\d+) bytes$")]
fn no_frame_exceeds(world: &mut ClientWorld, cap: usize) {
    let run = run(world);
    for action in &run.actions[run.mark..] {
        if let Action::Send { frame, .. } = action {
            let commands = String::from_utf8_lossy(frame).lines().count();
            assert!(commands < 2 || frame.len() <= cap, "{} bytes", frame.len());
        }
    }
}

#[then("the client sends one frame carrying the first three calls")]
fn one_frame_of_three(world: &mut ClientWorld) {
    let run = run(world);
    let frames = run.frames();
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].1.len(), 3, "{frames:?}");
}

#[then("the client sends nothing more until that frame is reported written")]
fn nothing_more(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.frames().len(), 1);
    let frame = run.actions[run.mark..]
        .iter()
        .find_map(|action| match action {
            Action::Send { id, .. } => Some(*id),
            _ => None,
        })
        .expect("a frame");
    let transport = run.transport.expect("a transport");
    run.handle(Input::FrameWritten { transport, frame });
}

#[then("then the fourth and fifth calls are sent, each in its own frame")]
fn fourth_and_fifth(world: &mut ClientWorld) {
    let run = run(world);
    let frames = run.frames();
    let counts: Vec<usize> = frames.iter().map(|(_, commands)| commands.len()).collect();
    assert_eq!(counts, [3, 1, 1], "{frames:?}");
}

#[when(regex = r#"^the application starts a batch and calls RPC "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_with_an_rpc(world: &mut ClientWorld, name: String) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StartBatching);
    run.rpc(&name);
}

#[then(regex = r"^the call fails with error code (\d+)$")]
fn the_call_fails_with(world: &mut ClientWorld, code: u32) {
    let run = run(world);
    let (_, call) = run.calls.last().expect("a call");
    let result = run.settled.get(call).expect("settled");
    assert!(
        matches!(result, Err(error) if error.code == code),
        "{result:?}"
    );
}

#[then("the client has sent no frame since the batch started")]
fn no_frame_since(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.frames(), Vec::new());
}

#[when("the application starts a batch and calls subscribe on the subscription")]
fn a_batch_with_a_subscribe(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StartBatching);
    let id = run.subscriptions[0];
    run.handle(Input::Subscribe(id));
}

#[then(regex = r#"^the client emits "connecting" with code (\d+) and reason "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn connecting_with_reason(world: &mut ClientWorld, code: u32, reason: String) {
    let run = run(world);
    assert!(
        run.actions.iter().any(|action| matches!(
            action,
            Action::Emit(_, ClientEvent::Connecting { code: got, reason: why })
                if *got == code && *why == reason
        )),
        "{:?}",
        run.actions
    );
}

#[when("the application starts a batch")]
fn a_batch_starts(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::StartBatching);
}

#[when(regex = r"^the application calls send with (\{.*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_application_calls_send(world: &mut ClientWorld, data: String) {
    run(world).call(
        "send",
        CallRequest::Send {
            data: data.into_bytes(),
        },
    );
}

#[when("the connection token's refresh time comes and the token callback answers")]
fn the_refresh_comes(world: &mut ClientWorld) {
    let run = run(world);
    run.advance(Duration::from_secs(10));
    let fetch = run
        .actions
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::FetchToken(fetch) => Some(*fetch),
            _ => None,
        })
        .expect("a token fetch for the refresh");
    run.handle(Input::TokenFetched {
        fetch,
        result: Ok::<_, TokenError>("t1".to_owned()),
    });
}

#[then(
    "the client sends a pong, the send command and a refresh command, each in its own frame, before the batch is stopped"
)]
fn own_commands_not_held(world: &mut ClientWorld) {
    let run = run(world);
    let frames = run.frames();
    assert!(
        frames.iter().all(|(_, commands)| commands.len() == 1),
        "{frames:?}"
    );
    let commands: Vec<&Value> = frames.iter().flat_map(|(_, commands)| commands).collect();
    assert!(
        commands.iter().any(|command| **command == json!({})),
        "pong: {frames:?}"
    );
    assert!(
        commands.iter().any(|command| command["send"].is_object()),
        "send: {frames:?}"
    );
    assert!(
        commands
            .iter()
            .any(|command| command["refresh"].is_object()),
        "refresh: {frames:?}"
    );
}

#[then("the send call returns")]
fn the_send_call_returns(world: &mut ClientWorld) {
    let run = run(world);
    let call = run.named("send");
    assert!(
        matches!(run.settled.get(&call), Some(Ok(_))),
        "{:?}",
        run.settled
    );
}

#[when(regex = r#"^connect is called and the client reaches state "connected"$"#)]
fn connect_and_connected(world: &mut ClientWorld) {
    let run = run(world);
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c2"}));
}

#[then("the client has sent no rpc command")]
fn no_rpc_command(world: &mut ClientWorld) {
    let run = run(world);
    let frames = run.frames_since(0);
    assert!(
        !frames
            .iter()
            .flat_map(|(_, commands)| commands)
            .any(|command| command["rpc"].is_object()),
        "{frames:?}"
    );
}

#[when("the transport closes")]
fn the_transport_closes(world: &mut ClientWorld) {
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.closed.push(transport);
    run.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: "lost".to_owned(),
    });
}

#[then("the call has not failed")]
fn the_call_has_not_failed(world: &mut ClientWorld) {
    let run = run(world);
    let (_, call) = run.calls.last().expect("a call");
    assert!(!run.settled.contains_key(call), "{:?}", run.settled);
}

#[when(regex = r#"^the client reaches state "connected" on a new transport$"#)]
fn connected_on_a_new_transport(world: &mut ClientWorld) {
    let run = run(world);
    // Until the reconnect opens a new transport, well within the held
    // call's deadline.
    for _ in 0..50 {
        if run
            .transport
            .is_some_and(|transport| !run.closed.contains(&transport))
        {
            break;
        }
        run.advance(Duration::from_millis(100));
    }
    assert!(
        run.transport
            .is_some_and(|transport| !run.closed.contains(&transport)),
        "no new transport opened"
    );
    run.connect(&json!({"client": "c2"}));
}

#[then("the rpc command is sent on the new transport")]
fn rpc_on_the_new_transport(world: &mut ClientWorld) {
    let run = run(world);
    let transport = run.transport.expect("a transport");
    let frames = run.frames_since(0);
    assert!(
        frames.iter().any(|(on, commands)| *on == transport
            && commands.iter().any(|command| command["rpc"].is_object())),
        "{frames:?}"
    );
}

#[then("no rpc command was sent on the closed transport")]
fn no_rpc_on_the_closed_transport(world: &mut ClientWorld) {
    let run = run(world);
    let closed = run.closed.clone();
    let frames = run.frames_since(0);
    assert!(
        !frames.iter().any(|(on, commands)| closed.contains(on)
            && commands.iter().any(|command| command["rpc"].is_object())),
        "{frames:?}"
    );
}

#[when(regex = r#"^the application starts batches A and B and calls RPC "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn two_batches(world: &mut ClientWorld, name: String) {
    let run = run(world);
    run.handle(Input::StartBatching);
    run.handle(Input::StartBatching);
    run.mark = run.actions.len();
    run.rpc(&name);
}

#[when(regex = r"^batch (A|B) is stopped$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn batch_is_stopped(world: &mut ClientWorld, batch: String) {
    // Either stops one of the two batches: the core counts them.
    drop(batch);
    run(world).handle(Input::StopBatching);
}

#[then(regex = r#"^the client sends one frame carrying RPC "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn one_frame_with(world: &mut ClientWorld, name: String) {
    let run = run(world);
    let frames = run.frames();
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].1.len(), 1, "{frames:?}");
    assert_eq!(frames[0].1[0]["rpc"]["method"], name, "{frames:?}");
    run.mark = run.actions.len();
}

#[when(regex = r#"^the application calls RPC "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_application_calls_rpc(world: &mut ClientWorld, name: String) {
    let run = run(world);
    run.mark = run.actions.len();
    run.rpc(&name);
}

#[then("the client sends no frame")]
fn the_client_sends_no_frame(world: &mut ClientWorld) {
    assert_eq!(run(world).frames(), Vec::new());
}

// @D: the tokio client against the fake server.

fn start_batch(world: &mut ClientWorld) {
    world.batching.received_at_start =
        world.fake().received().len() + world.fake().received_protobuf_with_frames().len();
    let batch = world.client().start_batching();
    world.batching.batches.insert(String::new(), batch);
}

fn spawn_call<F, T>(world: &mut ClientWorld, call: F)
where
    F: std::future::Future<Output = Result<T, CallError>> + Send + 'static,
{
    world.batching.calls.push(tokio::spawn(async move {
        call.await.map(|_| ()).map_err(|error| error.code)
    }));
}

#[when(
    regex = r#"^the application starts a batch, calls RPC "(\w+)", publishes (\{.*\}) to channel "(\w+)" and requests the history of channel "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_of_calls(
    world: &mut ClientWorld,
    method: String,
    data: String,
    channel: String,
    history: String,
) {
    start_batch(world);
    let rpc = world.client().rpc(method, b"{}".to_vec());
    spawn_call(world, rpc);
    let publish = world.client().publish(channel, data.into_bytes());
    spawn_call(world, publish);
    let history = world.client().history(history, HistoryOptions::default());
    spawn_call(world, history);
}

#[when(regex = r"^(\d+) ms elapse$")]
async fn milliseconds_elapse(_world: &mut ClientWorld, millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

#[then("the fake server has received no command since the batch started")]
fn no_command_since(world: &mut ClientWorld) {
    let now = world.fake().received().len() + world.fake().received_protobuf_with_frames().len();
    assert_eq!(now, world.batching.received_at_start);
}

#[when(regex = r"^the batch is (stopped|dropped)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_batch_is_ended(world: &mut ClientWorld, how: String) {
    if drives_a_machine(world) {
        run(world).handle(Input::StopBatching);
        return;
    }
    let batch = world.batching.batches.remove("").expect("an open batch");
    if how == "stopped" {
        batch.stop();
    } else {
        drop(batch);
    }
}

/// Waits until the fake server received `count` JSON commands since the
/// batch started; those commands with their frames.
async fn received_since(world: &ClientWorld, count: usize) -> Vec<(Value, usize)> {
    let start = world.batching.received_at_start;
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let received = world.fake().received_with_frames();
            if received.len() >= start + count {
                return received[start..].to_vec();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    got.expect("the commands")
}

#[then(
    "the fake server receives one frame carrying the rpc, publish and history commands, in that order"
)]
async fn one_frame_of_calls(world: &mut ClientWorld) {
    let received = received_since(world, 3).await;
    let methods: Vec<&str> = received
        .iter()
        .map(|(command, _)| {
            ["rpc", "publish", "history"]
                .into_iter()
                .find(|method| command[*method].is_object())
                .unwrap_or("other")
        })
        .collect();
    assert_eq!(methods, ["rpc", "publish", "history"], "{received:?}");
    assert!(
        received.iter().all(|(_, frame)| *frame == received[0].1),
        "{received:?}"
    );
}

#[then(regex = r"^(?:each call returns|both calls return) the fake server's reply$")]
async fn each_call_returns(world: &mut ClientWorld) {
    for call in std::mem::take(&mut world.batching.calls) {
        let result = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("the call settles")
            .expect("the call task");
        assert_eq!(result, Ok(()), "a call failed");
    }
}

#[given("a client connected to the fake server, with three subscriptions not subscribed")]
async fn a_client_with_three_subscriptions(world: &mut ClientWorld) {
    super::connection::a_connected_client(world).await;
    for channel in ["a", "b", "c"] {
        let subscription = world
            .client()
            .new_subscription(channel)
            .expect("a new channel");
        world.named_subscriptions.push(subscription);
    }
}

#[when("the application starts a batch and calls subscribe on the three subscriptions")]
fn a_batch_of_subscribes(world: &mut ClientWorld) {
    start_batch(world);
    for subscription in &world.named_subscriptions {
        subscription.subscribe();
    }
}

#[then("the fake server receives one frame carrying the three subscribe commands, in that order")]
async fn one_frame_of_subscribes(world: &mut ClientWorld) {
    let received = received_since(world, 3).await;
    let channels: Vec<&str> = received
        .iter()
        .map(|(command, _)| command["subscribe"]["channel"].as_str().unwrap_or("other"))
        .collect();
    assert_eq!(channels, ["a", "b", "c"], "{received:?}");
    assert!(
        received.iter().all(|(_, frame)| *frame == received[0].1),
        "{received:?}"
    );
}

#[then(r#"the three subscriptions reach state "subscribed""#)]
async fn three_subscribed(world: &mut ClientWorld) {
    let reached =
        tokio::time::timeout(Duration::from_secs(5), async {
            while !world.named_subscriptions.iter().all(|subscription| {
                subscription.state() == ferrifuge::SubscriptionState::Subscribed
            }) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    assert!(reached.is_ok(), "not all subscribed");
}

#[given("a client configured with the Protobuf protocol, connected to the fake server")]
async fn a_protobuf_client(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    super::connection::a_connected_client(world).await;
}

#[when(
    regex = r#"^the application starts a batch, calls RPC "(\w+)" and RPC "(\w+)", and stops the batch$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_of_two_rpcs(world: &mut ClientWorld, first: String, second: String) {
    start_batch(world);
    for method in [first, second] {
        let rpc = world.client().rpc(method, b"\x01".to_vec());
        spawn_call(world, rpc);
    }
    world.batching.batches.remove("").expect("the batch").stop();
}

#[then(
    "the fake server receives one binary frame carrying the two rpc commands, varint-delimited, in that order"
)]
async fn one_binary_frame(world: &mut ClientWorld) {
    let start = world.batching.received_at_start;
    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let received = world.fake().received_protobuf_with_frames();
            if received.len() >= start + 2 {
                return received[start..].to_vec();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("two Protobuf commands");
    let methods: Vec<String> = received
        .iter()
        .map(|(command, _)| {
            command
                .rpc
                .as_ref()
                .map(|rpc| rpc.method.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(methods, ["a", "b"], "{received:?}");
    assert_eq!(received[0].1, received[1].1, "{received:?}");
}

// 18b: emulated transports and the HTTP 413 resend.

#[given("a client connected over an emulated transport, with a subscription not subscribed")]
fn an_emulated_client(world: &mut ClientWorld) {
    let mut run = BatchRun::new(Config::default(), false);
    run.machine = Machine::new(Config::default())
        .with_transports(vec![ferrifuge::machine::TransportKind::HttpStream])
        .expect("an emulated transport");
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c", "session": "s", "node": "n"}));
    let id = run
        .machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    run.subscriptions.push(id);
    world.batching.run = Some(run);
}

#[given(
    regex = r#"^the application started a batch, called RPC "(\w+)" and subscribe on the subscription, and stopped the batch$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_of_rpc_and_subscribe(world: &mut ClientWorld, name: String) {
    let run = run(world);
    run.handle(Input::StartBatching);
    run.rpc(&name);
    let id = run.subscriptions[0];
    run.handle(Input::Subscribe(id));
    run.mark = run.actions.len();
    run.handle(Input::StopBatching);
}

/// The last frame sent, raw, with the commands it carries.
fn last_frame(run: &BatchRun) -> (ferrifuge::machine::FrameId, Vec<u8>, Vec<Value>) {
    run.actions
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::Send { id, frame, .. } => Some((*id, frame.clone(), commands_of(frame))),
            _ => None,
        })
        .expect("a frame sent")
}

/// The server refuses `frame` with HTTP 413: the transport reports it
/// rejected, then written, as the HTTP transport does.
fn refuse(run: &mut BatchRun, id: ferrifuge::machine::FrameId, frame: Vec<u8>) {
    let transport = run.transport.expect("a transport");
    run.handle(Input::SendRejected { transport, frame });
    run.handle(Input::FrameWritten {
        transport,
        frame: id,
    });
}

#[when("the server refuses the request carrying both commands with HTTP 413")]
fn refuse_both(world: &mut ClientWorld) {
    let run = run(world);
    let (id, frame, commands) = last_frame(run);
    assert_eq!(commands.len(), 2, "{commands:?}");
    run.mark = run.actions.len();
    refuse(run, id, frame);
}

#[then(regex = r#"^the call has not failed and the subscription is "(\w+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn not_failed_and_subscription(world: &mut ClientWorld, state: String) {
    let run = run(world);
    let (_, call) = run.calls.last().expect("a call");
    assert!(!run.settled.contains_key(call), "{:?}", run.settled);
    let id = run.subscriptions[0];
    let got = run.machine.subscription_state(id).expect("a subscription");
    assert_eq!(format!("{got:?}").to_lowercase(), state);
}

#[then(
    regex = r#"^the client sends two requests, carrying RPC "(\w+)" and then the subscribe command$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn two_requests(world: &mut ClientWorld, name: String) {
    let run = run(world);
    let frames = run.frames();
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert_eq!(frames[0].1.len(), 1, "{frames:?}");
    assert_eq!(frames[0].1[0]["rpc"]["method"], name, "{frames:?}");
    assert_eq!(frames[1].1.len(), 1, "{frames:?}");
    assert!(frames[1].1[0]["subscribe"].is_object(), "{frames:?}");
}

#[when(
    regex = r#"^the server refuses the request carrying RPC "(\w+)" with HTTP 413 and answers the subscribe command$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn refuse_the_rpc(world: &mut ClientWorld, name: String) {
    let run = run(world);
    let sent: Vec<(ferrifuge::machine::FrameId, Vec<u8>, Vec<Value>)> = run.actions[run.mark..]
        .iter()
        .filter_map(|action| match action {
            Action::Send { id, frame, .. } => Some((*id, frame.clone(), commands_of(frame))),
            _ => None,
        })
        .collect();
    let (rpc_id, rpc_frame, _) = sent
        .iter()
        .find(|(_, _, commands)| commands[0]["rpc"]["method"] == name)
        .cloned()
        .expect("the rpc request");
    let subscribe = sent
        .iter()
        .find(|(_, _, commands)| commands[0]["subscribe"].is_object())
        .and_then(|(_, _, commands)| commands[0]["id"].as_u64())
        .expect("the subscribe request");
    run.mark = run.actions.len();
    refuse(run, rpc_id, rpc_frame);
    run.frame(&json!({"id": subscribe, "subscribe": {}}));
}

pub(crate) fn subscription_subscribed(world: &mut ClientWorld) {
    let run = run(world);
    let id = run.subscriptions[0];
    assert_eq!(
        run.machine.subscription_state(id),
        Some(ferrifuge::SubscriptionState::Subscribed)
    );
}

#[then("the transport stays open")]
fn the_transport_stays_open(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        !run.actions
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{:?}",
        run.actions
    );
}

#[given(
    regex = r#"^a client subscribed to channel "(\w+)" over transport "(\w+)" through an HTTP proxy answering HTTP 413 to every emulation request over (\d+) bytes$"#
)]
async fn a_client_behind_a_refusing_proxy(
    world: &mut ClientWorld,
    channel: String,
    transport: String,
    limit: usize,
) {
    let kind = super::transports::kind(&transport);
    let http_proxy = crate::support::http_proxy::HttpProxy::start(
        super::transports::server(world).address().to_owned(),
    )
    .await;
    http_proxy.refuse_emulation_over(limit);
    let url = format!("http://{}/connection/{}", http_proxy.address(), kind.name());
    super::transports::configure(world, vec![(kind, url)]);
    super::transports::connect_and_wait(world).await;
    world.http_proxy = Some(http_proxy);
    let subscription = world
        .client()
        .new_subscription(channel)
        .expect("a new channel");
    world.track_subscription(subscription);
    world
        .subscription
        .as_ref()
        .expect("the subscription")
        .subscribe();
    super::subscriptions::expect_subscription_event(world, "subscribing", false).await;
    super::subscriptions::expect_subscription_event(world, "subscribed", false).await;
}

#[when(
    regex = r#"^the application starts a batch, publishes a (\d+)-byte JSON string and then (\{.*\}) to channel "(\w+)", and stops the batch$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_batch_of_publishes(world: &mut ClientWorld, size: usize, data: String, channel: String) {
    let batch = world.client().start_batching();
    let large = format!("\"{}\"", "x".repeat(size - 2)).into_bytes();
    let first = world.client().publish(channel.clone(), large);
    spawn_call(world, first);
    let second = world.client().publish(channel, data.into_bytes());
    spawn_call(world, second);
    batch.stop();
}

#[then("the HTTP proxy refused one request carrying both publish commands")]
async fn refused_one_with_both(world: &mut ClientWorld) {
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let both = proxy
                .refused()
                .iter()
                .filter(|body| {
                    commands_of(body)
                        .iter()
                        .filter(|c| c["publish"].is_object())
                        .count()
                        == 2
                })
                .count();
            if both > 0 {
                return both;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a refused request");
    assert_eq!(found, 1);
}

/// The result of call `index`, made in order.
async fn call_result(world: &mut ClientWorld, index: usize) -> Result<(), u32> {
    let call = &mut world.batching.calls[index];
    tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("the call settles")
        .expect("the call task")
}

#[then(regex = r"^the first publish fails with error code (\d+)$")]
async fn the_first_publish_fails(world: &mut ClientWorld, code: u32) {
    assert_eq!(call_result(world, 0).await, Err(code));
}

#[then("the second publish succeeds")]
async fn the_second_publish_succeeds(world: &mut ClientWorld) {
    assert_eq!(call_result(world, 1).await, Ok(()));
}

#[then(regex = r"^the subscription receives the publication (\{.*\}) and no other$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn receives_only(world: &mut ClientWorld, data: String) {
    let expected: Value = serde_json::from_str(&data).expect("JSON data");
    let event = super::subscriptions::expect_subscription_event(world, "publication", false).await;
    let ferrifuge::SubscriptionEvent::Publication(publication) = &event else {
        unreachable!("checked the name");
    };
    let got: Value = serde_json::from_slice(&publication.data).expect("JSON data");
    assert_eq!(got, expected);
    let more = tokio::time::timeout(
        Duration::from_millis(500),
        super::subscriptions::expect_subscription_event(world, "publication", false),
    )
    .await;
    assert!(more.is_err(), "another publication");
}

// 18c: the subscribe commands of a connect reply in one batch.

#[given("a client with three subscriptions subscribed, connected to the fake server")]
async fn three_subscribed_on_the_fake_server(world: &mut ClientWorld) {
    super::connection::a_connected_client(world).await;
    for channel in ["a", "b", "c"] {
        let subscription = world
            .client()
            .new_subscription(channel)
            .expect("a new channel");
        subscription.subscribe();
        world.named_subscriptions.push(subscription);
    }
    three_subscribed(world).await;
    world.batching.received_at_start = world.fake().received().len();
}

#[then(
    "after the next connect reply the fake server receives one frame carrying the three subscribe commands"
)]
async fn resubscribes_in_one_frame(world: &mut ClientWorld) {
    let start = world.batching.received_at_start;
    let subscribes = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let subscribes: Vec<(Value, usize)> = world.fake().received_with_frames()[start..]
                .iter()
                .filter(|(command, _)| command["subscribe"].is_object())
                .cloned()
                .collect();
            if subscribes.len() >= 3 {
                return subscribes;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("three subscribe commands");
    assert_eq!(subscribes.len(), 3, "{subscribes:?}");
    assert!(
        subscribes
            .iter()
            .all(|(_, frame)| *frame == subscribes[0].1),
        "{subscribes:?}"
    );
}

/// A subscription whose subscribe command encodes to `size` bytes once
/// numbered `id`.
fn subscription_of_size(run: &mut BatchRun, id: u64, size: usize) {
    let base = json!({"id": id, "subscribe": {"channel": ""}})
        .to_string()
        .len();
    let channel = "c".repeat(size.checked_sub(base).expect("a larger size"));
    let sub = run
        .machine
        .add_subscription(
            format!("{id}{}", &channel[1..]),
            SubscriptionOptions::default(),
        )
        .expect("a new channel");
    run.handle(Input::Subscribe(sub));
    run.subscriptions.push(sub);
}

#[given(
    regex = r"^a client with a batch frame cap of (\d+) bytes and five subscriptions whose subscribe commands encode to (\d+) bytes each$"
)]
fn five_subscriptions_of_size(world: &mut ClientWorld, cap: usize, size: usize) {
    let mut run = BatchRun::new(
        Config {
            batch_frame_bytes: cap,
            ..Config::default()
        },
        false,
    );
    // The connect command is 1, the subscribe commands 2 to 6.
    for id in 2..7 {
        subscription_of_size(&mut run, id, size);
    }
    world.batching.run = Some(run);
}

#[when("connect is called and the server answers the connect command")]
fn connect_and_answer(world: &mut ClientWorld) {
    let run = run(world);
    run.handle(Input::Connect);
    run.mark = run.actions.len();
    run.connect(&json!({"client": "c"}));
}

#[then("the client sends two frames, carrying three and then two subscribe commands")]
fn two_frames_of_subscribes(world: &mut ClientWorld) {
    let run = run(world);
    let counts: Vec<usize> = run
        .frames()
        .iter()
        .map(|(_, commands)| {
            commands
                .iter()
                .filter(|command| command["subscribe"].is_object())
                .count()
        })
        .filter(|count| *count > 0)
        .collect();
    assert_eq!(counts, [3, 2], "{:?}", run.frames());
}

#[given(r#"a client with a subscription in state "subscribing""#)]
fn a_subscribing_subscription(world: &mut ClientWorld) {
    let mut run = BatchRun::new(Config::default(), false);
    let id = run
        .machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    run.handle(Input::Subscribe(id));
    run.subscriptions.push(id);
    world.batching.run = Some(run);
}

#[given("the application has started a batch")]
fn the_application_has_started_a_batch(world: &mut ClientWorld) {
    run(world).handle(Input::StartBatching);
}

#[then("the client sends the subscribe command while the batch is open")]
fn subscribe_sent_while_open(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        run.frames()
            .iter()
            .flat_map(|(_, commands)| commands)
            .any(|command| command["subscribe"].is_object()),
        "{:?}",
        run.frames()
    );
}
