//! Steps of Block 21, logging.

use cucumber::{given, then, when};
use ferrifuge::codes;
use ferrifuge::machine::{Action, Anomaly, Input};
use ferrifuge::{ClientEvent, Config};
use serde_json::json;

use super::ClientWorld;
use super::batching::{connected_run, run};

/// What a Block 21 scenario holds.
#[derive(Default)]
pub(crate) struct Logging {
    /// Where the actions of the step under test start.
    mark: usize,
    /// The call of an @E scenario.
    call: Option<ferrifuge::machine::CallId>,
    /// Index of the anomaly a step found among the actions.
    anomaly: Option<usize>,
}

#[given("a machine connected over a websocket transport")]
fn a_connected_machine(world: &mut ClientWorld) {
    world.batching.run = Some(connected_run(Config::default()));
    world.logging.mark = run(world).actions.len();
}

#[given("a machine connected over a websocket transport, with an RPC call sent")]
fn a_connected_machine_with_a_call(world: &mut ClientWorld) {
    a_connected_machine(world);
    world.logging.call = Some(run(world).rpc("rpc"));
    world.logging.mark = run(world).actions.len();
}

#[when(regex = r"^it is given a frame of (\d+) bytes that is not client protocol data$")]
fn given_a_frame_that_is_no_data(world: &mut ClientWorld, bytes: usize) {
    let data = b"not-json-7f3a".to_vec();
    assert_eq!(data.len(), bytes, "the scenario's frame length");
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.handle(Input::Frame { transport, data });
}

#[when("it is given the reply to that call carrying a history result")]
fn given_a_reply_of_another_kind(world: &mut ClientWorld) {
    let run = run(world);
    let id = run
        .frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .find(|command| command["rpc"].is_object())
        .and_then(|command| command["id"].as_u64())
        .expect("the RPC command");
    let transport = run.transport.expect("a transport");
    let reply = json!({"id": id, "history": {"publications": [], "epoch": "e", "offset": 0}});
    run.handle(Input::Frame {
        transport,
        data: reply.to_string().into_bytes(),
    });
}

/// The index, among all actions, of the one anomaly reported since the
/// step that set the mark, checked by `expected`.
fn anomaly(world: &mut ClientWorld, expected: impl Fn(&Anomaly) -> bool) -> usize {
    let mark = world.logging.mark;
    let run = run(world);
    let anomalies: Vec<(usize, &Anomaly)> = run
        .actions
        .iter()
        .enumerate()
        .skip(mark)
        .filter_map(|(index, action)| match action {
            Action::Anomaly(anomaly) => Some((index, anomaly)),
            _ => None,
        })
        .collect();
    assert!(
        matches!(anomalies.as_slice(), [(_, anomaly)] if expected(anomaly)),
        "{anomalies:?}"
    );
    anomalies[0].0
}

#[then(
    regex = r"^its actions report an anomaly naming that transport and a length of (\d+) bytes$"
)]
fn reports_an_undecodable_frame(world: &mut ClientWorld, length: usize) {
    let transport = run(world).transport.expect("a transport");
    world.logging.anomaly = Some(anomaly(world, |anomaly| {
        *anomaly
            == Anomaly::UndecodableFrame {
                transport,
                bytes: length,
            }
    }));
}

#[then(
    regex = r#"^its actions report an anomaly naming that transport and the command kind "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn reports_a_reply_without_result(world: &mut ClientWorld, kind: String) {
    let transport = run(world).transport.expect("a transport");
    world.logging.anomaly = Some(anomaly(world, |anomaly| {
        *anomaly
            == Anomaly::ReplyWithoutResult {
                transport,
                command: match kind.as_str() {
                    "rpc" => "rpc",
                    other => panic!("a command kind of the scenarios, not {other}"),
                },
            }
    }));
}

/// The actions after the anomaly the previous step found.
fn after_the_anomaly(world: &mut ClientWorld) -> Vec<Action> {
    let from = world.logging.anomaly.expect("an anomaly reported") + 1;
    run(world).actions[from..].to_vec()
}

fn emits_connecting(actions: &[Action], code: u32, reason: &str) -> bool {
    actions.iter().any(|action| {
        matches!(action, Action::Emit(_, ClientEvent::Connecting { code: of, reason: why })
            if *of == code && why == reason)
    })
}

#[then(
    regex = r#"^then they close the transport and emit "connecting" with code (\d+) and reason "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn then_they_reconnect(world: &mut ClientWorld, code: u32, reason: String) {
    let transport = run(world).transport;
    let actions = after_the_anomaly(world);
    assert!(
        actions
            .iter()
            .any(|action| Some(action) == transport.map(Action::CloseTransport).as_ref()),
        "{actions:?}"
    );
    assert!(emits_connecting(&actions, code, &reason), "{actions:?}");
}

#[then(
    regex = r#"^then the call fails with a "connection closed" error and the machine emits "connecting" with code (\d+)$"#
)]
fn then_the_call_fails(world: &mut ClientWorld, code: u32) {
    let call = world.logging.call.expect("a call sent");
    let result = run(world).settled.get(&call).cloned();
    assert!(
        matches!(&result, Some(Err(error)) if error.code == codes::error::CONNECTION_CLOSED),
        "{result:?}"
    );
    let actions = after_the_anomaly(world);
    assert!(
        actions.iter().any(|action| matches!(action,
            Action::Emit(_, ClientEvent::Connecting { code: of, .. }) if *of == code)),
        "{actions:?}"
    );
}
