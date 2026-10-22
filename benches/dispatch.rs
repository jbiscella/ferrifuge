//! Instruction counts of dispatching one publication (Block 14, `@P`), with
//! gungraun on Valgrind. `tests/perf_gates.rs` gates on them.

// The gungraun macros generate undocumented items.
#![allow(missing_docs)]

use std::hint::black_box;

use ferrifuge::Config;
use ferrifuge::machine::{Action, Input, Machine, SubscriptionOptions, TransportId};
use gungraun::{library_benchmark, library_benchmark_group, main};

/// Payload of the publication, so that its JSON data is 1 KiB.
const TEXT_LEN: usize = 1024 - r#"{"t":""}"#.len();

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// A connected machine with `count` subscribed channels, and a publication
/// to the channel subscribed last: a scan of the subscriptions reaches it
/// last.
fn subscribed(count: usize) -> (Machine, Input) {
    let mut machine = Machine::new(Config::default());
    let transport = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport to open");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    for index in 0..count {
        let id = machine
            .add_subscription(format!("ch{index}"), SubscriptionOptions::default())
            .expect("a new channel");
        for action in machine.handle(Input::Subscribe(id)) {
            let Action::Send { frame: sent, .. } = action else {
                continue;
            };
            for command in String::from_utf8(sent).expect("JSON").lines() {
                let command: serde_json::Value = serde_json::from_str(command).expect("JSON");
                if command.get("subscribe").is_some() {
                    let reply = format!(r#"{{"id":{},"subscribe":{{}}}}"#, command["id"]);
                    machine.handle(frame(transport, &reply));
                }
            }
        }
    }
    let text = "x".repeat(TEXT_LEN);
    let channel = count - 1;
    let publication =
        format!(r#"{{"push":{{"channel":"ch{channel}","pub":{{"data":{{"t":"{text}"}}}}}}}}"#);
    (machine, frame(transport, &publication))
}

/// Drops the machine and the actions outside the measurement: dropping the
/// subscriptions would otherwise count, and grow with their number.
fn teardown((machine, actions): (Machine, Vec<Action>)) {
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::EmitSubscription(..))),
        "the publication was not dispatched"
    );
    drop((machine, actions));
}

#[library_benchmark]
#[bench::one_channel(setup = subscribed, args = (1), teardown = teardown)]
#[bench::ten_thousand_channels(setup = subscribed, args = (10_000), teardown = teardown)]
fn dispatch((mut machine, publication): (Machine, Input)) -> (Machine, Vec<Action>) {
    let actions = machine.handle(black_box(publication));
    black_box((machine, actions))
}

library_benchmark_group!(name = publications, benchmarks = [dispatch]);

main!(library_benchmark_groups = publications);
