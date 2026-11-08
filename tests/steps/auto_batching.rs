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
