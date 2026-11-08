//! Steps of Block 36a: automatic command batching. @E scenarios drive the
//! sans-IO core with automatic batching, feeding `Input::Flush` as the
//! steps say; @D and @A scenarios run a tokio client, whose driver
//! flushes on each iteration.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, TransportKind};
use ferrifuge::{CallRequest, Config};
use serde_json::{Value, json};

use super::ClientWorld;
use super::batching::{BatchRun, calls_of_sizes, run, sizes};

/// A core over `kind`, with automatic batching when `automatic`, its
/// connect command flushed and answered with `result`; the steps' mark is
/// after it.
fn connected_core(config: Config, automatic: bool, kind: TransportKind, result: Value) -> BatchRun {
    let machine = Machine::new(config);
    let machine = if automatic {
        machine.with_automatic_batching()
    } else {
        machine
    };
    let mut run = BatchRun::of(machine.with_transports(vec![kind]).expect("one transport"));
    run.handle(Input::Connect);
    if automatic && kind == TransportKind::WebSocket {
        // The connect command waits in the buffer for the flush.
        let transport = run.transport.expect("an attempt");
        run.handle(Input::TransportOpened(transport));
        run.handle(Input::Flush);
        let id = run
            .frames_since(0)
            .iter()
            .flat_map(|(_, commands)| commands.clone())
            .find(|command| command["connect"].is_object())
            .and_then(|command| command["id"].as_u64())
            .expect("a connect command");
        run.frame(&json!({"id": id, "connect": result}));
    } else {
        run.connect(&result);
    }
    run.handle(Input::Flush);
    run.mark = run.actions.len();
    run
}

fn websocket(world: &mut ClientWorld, config: Config, automatic: bool) {
    world.batching.run = Some(connected_core(
        config,
        automatic,
        TransportKind::WebSocket,
        json!({"client": "c"}),
    ));
}

/// The frames sent since the mark, each with its commands; the mark moves
/// past them.
fn take_frames(world: &mut ClientWorld) -> Vec<Vec<Value>> {
    let run = run(world);
    let frames = run
        .frames()
        .into_iter()
        .map(|(_, commands)| commands)
        .collect();
    run.mark = run.actions.len();
    frames
}

/// What a command is, as the scenarios name it.
fn kind(command: &Value) -> String {
    for method in ["rpc", "send", "publish", "subscribe", "connect"] {
        if command[method].is_object() {
            return if method == "rpc" {
                format!(
                    "rpc {}",
                    command["rpc"]["method"].as_str().unwrap_or_default()
                )
            } else {
                method.to_owned()
            };
        }
    }
    if command.as_object().is_some_and(serde_json::Map::is_empty) {
        "pong".to_owned()
    } else {
        command.to_string()
    }
}

fn kinds(frame: &[Value]) -> Vec<String> {
    frame.iter().map(kind).collect()
}

#[given(regex = r"^a client core (with|without) automatic batching, connected$")]
fn a_core(world: &mut ClientWorld, with: String) {
    websocket(world, Config::default(), with == "with");
}

#[given(
    "a client core with automatic batching, connected over websocket, whose connect reply asked for pongs"
)]
fn a_core_with_pongs(world: &mut ClientWorld) {
    world.batching.run = Some(connected_core(
        Config::default(),
        true,
        TransportKind::WebSocket,
        json!({"client": "c", "ping": 25, "pong": true}),
    ));
}

#[given(
    regex = r"^a client core with automatic batching, connected, with a batch frame cap of (\d+) bytes$"
)]
fn a_core_with_a_cap(world: &mut ClientWorld, cap: usize) {
    websocket(world, Config::default().batch_frame_bytes(cap), true);
}

#[given(
    regex = r"^a client core with automatic batching, connected, with a write queue of (\d+) bytes and nothing unwritten$"
)]
fn a_core_with_a_write_queue(world: &mut ClientWorld, bytes: usize) {
    websocket(world, Config::default().write_queue_bytes(bytes), true);
}

#[given("a client core with automatic batching, connected over an emulated transport")]
fn an_emulated_core(world: &mut ClientWorld) {
    world.batching.run = Some(connected_core(
        Config::default(),
        true,
        TransportKind::HttpStream,
        json!({"client": "c", "session": "s", "node": "n"}),
    ));
}

/// What a Block 36a scenario holds.
#[derive(Debug, Default)]
pub(crate) struct AutoBatching {
    /// The actions of each of two calls, and of the last flush.
    first: Vec<Action>,
    second: Vec<Action>,
    flushed: Vec<Action>,
    /// The frame a step saw sent, for the transport to report written.
    frame: Option<ferrifuge::machine::FrameId>,
    /// The emulation request a step saw sent, for the server to refuse.
    request: Option<Vec<u8>>,
    /// The runtime of a @D scenario's client.
    single: Option<Single>,
    /// The outcome of an awaited call, how long it took, and how many
    /// commands the fake server had before it.
    awaited: Option<(Result<(), u32>, std::time::Duration, usize)>,
    /// The frames the counting server had read before a step's calls.
    frames_before: Option<u64>,
}

#[when(regex = r#"^the application calls RPC "(\w+)", then RPC "(\w+)"$"#)]
fn two_rpcs(world: &mut ClientWorld, first: String, second: String) {
    let run = run(world);
    let before = run.actions.len();
    run.rpc(&first);
    let middle = run.actions.len();
    run.rpc(&second);
    let (first, second) = (
        run.actions[before..middle].to_vec(),
        run.actions[middle..].to_vec(),
    );
    world.auto_batching.first = first;
    world.auto_batching.second = second;
}

#[when(
    regex = r#"^the application calls RPC "(\w+)" and RPC "(\w+)" and the owner feeds a flush$"#
)]
fn two_rpcs_and_a_flush(world: &mut ClientWorld, first: String, second: String) {
    let run = run(world);
    run.rpc(&first);
    run.rpc(&second);
    run.handle(Input::Flush);
}

#[when("the owner feeds a flush")]
fn a_flush(world: &mut ClientWorld) {
    let run = run(world);
    let before = run.actions.len();
    run.handle(Input::Flush);
    let flushed = run.actions[before..].to_vec();
    world.auto_batching.flushed = flushed;
}

#[then("the core has sent no frame")]
fn no_frame(world: &mut ClientWorld) {
    let frames = run(world).frames();
    assert_eq!(frames, Vec::new());
}

#[then("the core sends nothing")]
fn sends_nothing(world: &mut ClientWorld) {
    no_frame(world);
}

#[then(
    regex = r#"^the core sent a frame carrying RPC "(\w+)" among the actions of the first call, and one carrying RPC "(\w+)" among those of the second$"#
)]
fn sent_by_each_call(world: &mut ClientWorld, first: String, second: String) {
    for (actions, name) in [
        (&world.auto_batching.first, first),
        (&world.auto_batching.second, second),
    ] {
        let frames: Vec<Vec<String>> = actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(kinds(&super::batching::commands_of(frame))),
                _ => None,
            })
            .collect();
        assert_eq!(frames, [[format!("rpc {name}")]], "{actions:?}");
    }
    let run = run(world);
    run.mark = run.actions.len();
}

#[then("the flush produces no action")]
fn the_flush_does_nothing(world: &mut ClientWorld) {
    assert_eq!(world.auto_batching.flushed, Vec::new());
}

#[then(regex = r#"^the core sends one frame carrying RPC "(\w+)" and RPC "(\w+)"$"#)]
fn one_frame_of_two(world: &mut ClientWorld, first: String, second: String) {
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(frames, [[format!("rpc {first}"), format!("rpc {second}")]]);
}

#[then(regex = r#"^the core sends one frame carrying RPC "(\w+)"$"#)]
fn one_frame_of_one(world: &mut ClientWorld, name: String) {
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(frames, [[format!("rpc {name}")]]);
}

#[when("the core reads a server ping")]
fn a_ping(world: &mut ClientWorld) {
    run(world).frame(&json!({}));
}

#[when(
    regex = r#"^the application calls RPC "(\w+)", then send with (\{.*\}), then publishes (\{.*\}) to channel "(\w+)"$"#
)]
fn rpc_send_publish(
    world: &mut ClientWorld,
    name: String,
    send: String,
    publish: String,
    channel: String,
) {
    let run = run(world);
    run.rpc(&name);
    run.call(
        "send",
        CallRequest::Send {
            data: send.into_bytes(),
        },
    );
    run.call(
        "publish",
        CallRequest::Publish {
            channel,
            data: publish.into_bytes(),
        },
    );
}

#[then(
    "the core sends one frame carrying the pong, the rpc, the send and the publish commands, in that order"
)]
fn pong_rpc_send_publish(world: &mut ClientWorld) {
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(frames, [["pong", "rpc a", "send", "publish"]]);
}

#[when(
    regex = r"^the application makes calls whose commands encode to ([\d, ]+) bytes and the owner feeds a flush$"
)]
fn calls_of_sizes_and_a_flush(world: &mut ClientWorld, list: String) {
    let run = run(world);
    calls_of_sizes(run, &sizes(&list));
    run.handle(Input::Flush);
}

#[then(
    regex = r"^the core sends frames carrying ([\d, ]+) calls, in the order the calls were made$"
)]
fn frames_carrying(world: &mut ClientWorld, list: String) {
    let run = run(world);
    let frames = run.frames();
    let counts: Vec<usize> = frames.iter().map(|(_, commands)| commands.len()).collect();
    assert_eq!(counts, sizes(&list), "{frames:?}");
    let ids: Vec<u64> = frames
        .iter()
        .flat_map(|(_, commands)| commands.iter().filter_map(|command| command["id"].as_u64()))
        .collect();
    assert!(ids.is_sorted(), "{frames:?}");
}

#[when(
    regex = r"^the application makes five calls whose commands encode to (\d+) bytes each and the owner feeds a flush$"
)]
fn five_calls_and_a_flush(world: &mut ClientWorld, size: usize) {
    let run = run(world);
    calls_of_sizes(run, &[size; 5]);
    run.handle(Input::Flush);
}

/// The frame of the last "sends one frame" step, which the transport
/// reports written.
#[then("the core sends one frame carrying the first three calls")]
fn the_first_three(world: &mut ClientWorld) {
    let run = run(world);
    let sent: Vec<_> = run.actions[run.mark..]
        .iter()
        .filter_map(|action| match action {
            Action::Send { id, .. } => Some(*id),
            _ => None,
        })
        .collect();
    let frames = take_frames(world);
    assert_eq!(frames.len(), 1, "{frames:?}");
    let ids: Vec<u64> = frames[0]
        .iter()
        .filter_map(|command| command["id"].as_u64())
        .collect();
    assert_eq!(ids, [2, 3, 4], "{frames:?}");
    world.auto_batching.frame = sent.first().copied();
}

#[when("the transport reports that frame written and the owner feeds a flush")]
fn written_and_flushed(world: &mut ClientWorld) {
    let frame = world.auto_batching.frame.expect("a frame sent");
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.handle(Input::FrameWritten { transport, frame });
    run.handle(Input::Flush);
}

#[then("the core sends one frame carrying the fourth and fifth calls")]
fn the_fourth_and_fifth(world: &mut ClientWorld) {
    let frames = take_frames(world);
    assert_eq!(frames.len(), 1, "{frames:?}");
    let ids: Vec<u64> = frames[0]
        .iter()
        .filter_map(|command| command["id"].as_u64())
        .collect();
    assert_eq!(ids, [5, 6], "{frames:?}");
}

#[then("the core sends one emulation request carrying both rpc commands")]
fn one_request_of_two(world: &mut ClientWorld) {
    let run = run(world);
    let request = run.actions[run.mark..]
        .iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        })
        .expect("a request");
    world.auto_batching.request = Some(request);
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(frames, [["rpc a", "rpc b"]]);
}

#[when("the server refuses that request with HTTP 413 and the owner feeds a flush")]
fn refused_and_flushed(world: &mut ClientWorld) {
    let frame = world.auto_batching.request.take().expect("a request");
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.handle(Input::SendRejected { transport, frame });
    run.handle(Input::Flush);
}

#[then(
    regex = r#"^the core sends two emulation requests, carrying RPC "(\w+)" and then RPC "(\w+)"$"#
)]
fn two_requests(world: &mut ClientWorld, first: String, second: String) {
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(
        frames,
        [[format!("rpc {first}")], [format!("rpc {second}")]]
    );
}

#[then("neither call has failed")]
fn neither_failed(world: &mut ClientWorld) {
    let run = run(world);
    assert!(run.settled.values().all(Result::is_ok), "{:?}", run.settled);
}

#[when(regex = r#"^the application calls RPC "(\w+)" and RPC "(\w+)", then abandons RPC "(\w+)"$"#)]
fn two_rpcs_one_abandoned(
    world: &mut ClientWorld,
    first: String,
    second: String,
    abandoned: String,
) {
    let run = run(world);
    run.rpc(&first);
    run.rpc(&second);
    let id = run.named(&abandoned);
    run.handle(Input::CallAbandoned(id));
}

#[then(regex = r#"^the core sends one frame carrying RPC "(\w+)" and no rpc command for "(\w+)"$"#)]
fn one_frame_without(world: &mut ClientWorld, sent: String, abandoned: String) {
    let frames: Vec<Vec<String>> = take_frames(world)
        .iter()
        .map(|frame| kinds(frame))
        .collect();
    assert_eq!(frames, [[format!("rpc {sent}")]]);
    assert!(!frames.concat().contains(&format!("rpc {abandoned}")));
}

// @D: a tokio client on a current-thread runtime, against the fake server.

/// A current-thread runtime driven by a thread of its own, so that a step
/// can block it while it makes calls: the client's driver then runs only
/// once they are all made.
#[derive(Debug)]
pub(crate) struct Single {
    handle: tokio::runtime::Handle,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Single {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime");
        let handle = runtime.handle().clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let _ = runtime.block_on(stopped);
        });
        Self {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Blocks the runtime's thread until the guard is dropped.
    fn pause(&self) -> std::sync::mpsc::Sender<()> {
        let (started, running) = std::sync::mpsc::channel();
        let (resume, paused) = std::sync::mpsc::channel::<()>();
        self.handle.spawn(async move {
            let _ = started.send(());
            // Blocking on purpose: nothing else on this runtime may run.
            let _ = paused.recv();
        });
        running.recv().expect("the runtime runs the pause");
        resume
    }
}

impl Drop for Single {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The scenario's client, built on a [`Single`] runtime, connected to the
/// fake server, which announces no pings.
async fn a_single_runtime_client(world: &mut ClientWorld, config: Config) {
    world
        .restart_fake(crate::support::fake_server::Announce::default())
        .await;
    let single = Single::start();
    let builder = ferrifuge::Client::builder(world.server_endpoint())
        .config(config)
        .runtime(single.handle.clone());
    world.auto_batching.single = Some(single);
    world.build_client(builder);
    super::transports::connect_and_wait(world).await;
}

/// The fake server's commands from `start` on, once `count` came, each
/// with the frame it arrived in.
async fn received_from(world: &ClientWorld, start: usize, count: usize) -> Vec<(Value, usize)> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let received = world.fake().received_with_frames();
            if received.len() >= start + count {
                return received[start..].to_vec();
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the commands")
}

#[given(
    regex = r"^a client on a current-thread runtime with a timeout of (\d+) s, connected to the fake server, which sends no pings$"
)]
async fn a_client_on_a_single_runtime(world: &mut ClientWorld, seconds: u64) {
    let mut config = world.client_config();
    config.timeout = std::time::Duration::from_secs(seconds);
    a_single_runtime_client(world, config).await;
}

#[given(
    "a client on a current-thread runtime, connected to the fake server, with three subscriptions not subscribed"
)]
async fn a_single_runtime_client_with_subscriptions(world: &mut ClientWorld) {
    let config = world.client_config();
    a_single_runtime_client(world, config).await;
    for channel in ["a", "b", "c"] {
        let subscription = world
            .client()
            .new_subscription(channel)
            .expect("a new channel");
        world.named_subscriptions.push(subscription);
    }
}

#[when(
    regex = r#"^the application makes RPC "(\w+)", publishes (\{.*\}) to channel "(\w+)" and requests the history of channel "(\w+)", without yielding between the calls$"#
)]
fn a_burst_of_calls(
    world: &mut ClientWorld,
    method: String,
    data: String,
    channel: String,
    history: String,
) {
    world.batching.received_at_start = world.fake().received().len();
    let single = world
        .auto_batching
        .single
        .as_ref()
        .expect("a current-thread runtime");
    let paused = single.pause();
    let rpc = world.client().rpc(method, b"{}".to_vec());
    let publish = world.client().publish(channel, data.into_bytes());
    let history = world
        .client()
        .history(history, ferrifuge::HistoryOptions::default());
    drop(paused);
    super::batching::spawn_call(world, rpc);
    super::batching::spawn_call(world, publish);
    super::batching::spawn_call(world, history);
}

#[when(
    "the application calls subscribe on the three subscriptions, without yielding between the calls"
)]
fn a_burst_of_subscribes(world: &mut ClientWorld) {
    world.batching.received_at_start = world.fake().received().len();
    let single = world
        .auto_batching
        .single
        .as_ref()
        .expect("a current-thread runtime");
    let paused = single.pause();
    for subscription in &world.named_subscriptions {
        subscription.subscribe();
    }
    drop(paused);
}

#[then(
    "the fake server receives one frame carrying the three subscribe commands, in the order of the calls"
)]
async fn one_frame_of_three_subscribes(world: &mut ClientWorld) {
    let received = received_from(world, world.batching.received_at_start, 3).await;
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

#[when(regex = r#"^the application then calls RPC "(\w+)" and awaits it$"#)]
async fn then_one_rpc(world: &mut ClientWorld, method: String) {
    let start = world.fake().received().len();
    let made = std::time::Instant::now();
    let result = world.client().rpc(method, b"{}".to_vec()).await;
    world.auto_batching.awaited = Some((
        result.map(|_| ()).map_err(|error| error.code),
        made.elapsed(),
        start,
    ));
}

#[then(
    regex = r"^the call returns the fake server's reply within (\d+) s, its command alone in its frame$"
)]
async fn returned_alone(world: &mut ClientWorld, seconds: u64) {
    let (result, took, start) = world.auto_batching.awaited.expect("an awaited call");
    assert_eq!(result, Ok(()));
    assert!(took < std::time::Duration::from_secs(seconds), "{took:?}");
    let received = received_from(world, start, 1).await;
    let frame = received[0].1;
    let all = world.fake().received_with_frames();
    let in_frame = all.iter().filter(|(_, of)| *of == frame).count();
    assert_eq!(in_frame, 1, "{received:?}");
}

#[given("a client configured with the fake server's endpoint, not connected")]
fn a_client_not_connected(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
}

#[when(regex = r#"^the application calls RPC "(\w+)", RPC "(\w+)" and RPC "(\w+)"$"#)]
fn three_rpcs(world: &mut ClientWorld, a: String, b: String, c: String) {
    for method in [a, b, c] {
        let rpc = world.client().rpc(method, b"{}".to_vec());
        super::batching::spawn_call(world, rpc);
    }
}

#[then(
    "after the connect reply the fake server receives one frame carrying the three rpc commands, in the order of the calls"
)]
async fn one_frame_of_three_rpcs(world: &mut ClientWorld) {
    let received = received_from(world, 0, 4).await;
    assert!(received[0].0["connect"].is_object(), "{received:?}");
    let rpcs = &received[1..];
    let methods: Vec<&str> = rpcs
        .iter()
        .map(|(command, _)| command["rpc"]["method"].as_str().unwrap_or("other"))
        .collect();
    assert_eq!(methods, ["a", "b", "c"], "{received:?}");
    assert!(
        rpcs.iter().all(|(_, frame)| *frame == rpcs[0].1) && rpcs[0].1 != received[0].1,
        "{received:?}"
    );
}

// @A: a Centrifugo server of its own, counting the frames it reads.

/// The @A server with Prometheus metrics, pinging rarely, so that no pong
/// joins the frames a scenario counts.
async fn a_counting_server(world: &mut ClientWorld) -> String {
    let mut env: Vec<(&str, &str)> = crate::SERVER_A.to_vec();
    env.extend([
        ("CENTRIFUGO_PROMETHEUS_ENABLED", "true"),
        ("CENTRIFUGO_CLIENT_PING_INTERVAL", "120s"),
        ("CENTRIFUGO_CLIENT_PONG_TIMEOUT", "10s"),
    ]);
    let server = crate::support::centrifugo::Centrifugo::start(&env).await;
    let endpoint = server.websocket_url();
    world.server = Some(server);
    endpoint
}

#[given(
    regex = r#"^a Centrifugo server of its own with Prometheus metrics, and a client on a current-thread runtime subscribed to channel "(\w+)" on it$"#
)]
async fn a_client_on_a_counting_server(world: &mut ClientWorld, channel: String) {
    let endpoint = a_counting_server(world).await;
    let single = Single::start();
    let builder = ferrifuge::Client::builder(endpoint)
        .config(world.client_config())
        .runtime(single.handle.clone());
    world.auto_batching.single = Some(single);
    world.build_client(builder);
    super::transports::connect_and_wait(world).await;
    let subscription = world
        .client()
        .new_subscription(channel)
        .expect("a new channel");
    world.track_subscription(subscription);
    super::subscriptions::subscribe(world).await;
    super::subscriptions::expect_subscription_event(world, "subscribed", false).await;
}

#[when(
    regex = r#"^the application publishes (\{.*\}), (\{.*\}) and (\{.*\}) to channel "(\w+)", without yielding between the calls$"#
)]
async fn a_burst_of_publishes(
    world: &mut ClientWorld,
    a: String,
    b: String,
    c: String,
    channel: String,
) {
    let before = world
        .server
        .as_ref()
        .expect("the counting server")
        .websocket_frames_received()
        .await;
    world.auto_batching.frames_before = Some(before);
    let single = world
        .auto_batching
        .single
        .as_ref()
        .expect("a current-thread runtime");
    let paused = single.pause();
    let publishes: Vec<_> = [a, b, c]
        .into_iter()
        .map(|data| world.client().publish(channel.clone(), data.into_bytes()))
        .collect();
    drop(paused);
    for publish in publishes {
        super::batching::spawn_call(world, publish);
    }
}

#[then("each publish succeeds")]
async fn each_publish_succeeds(world: &mut ClientWorld) {
    for call in std::mem::take(&mut world.batching.calls) {
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), call)
            .await
            .expect("the publish settles")
            .expect("the publish task");
        assert_eq!(result, Ok(()), "a publish failed");
    }
}

#[then(
    regex = r"^the subscription receives the publications (\{.*\}), (\{.*\}) and (\{.*\}), in that order$"
)]
async fn receives_the_three(world: &mut ClientWorld, a: String, b: String, c: String) {
    for expected in [a, b, c] {
        let publication = super::subscriptions::next_publication(world, false).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&publication.data).expect("JSON"),
            serde_json::from_str::<Value>(&expected).expect("JSON")
        );
    }
}

#[then(
    regex = r"^the server's count of websocket frames received grew by exactly (\d+) during the publishes$"
)]
async fn frames_grew_by(world: &mut ClientWorld, count: u64) {
    let before = world.auto_batching.frames_before.expect("a count before");
    let after = world
        .server
        .as_ref()
        .expect("the counting server")
        .websocket_frames_received()
        .await;
    assert_eq!(after - before, count);
}
