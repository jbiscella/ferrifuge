//! A delta declaring a target larger than the inbound message limit fails
//! before anything of that size is allocated (Block 19, D6; `@P`): the
//! sans-IO core, subscribed with delta on, handles a 20-byte delta declaring
//! one byte more than 64 MiB while dhat counts the live heap. Fast and
//! deterministic, so it runs with every `cargo test`.

// @P scenario: A delta declaring more than the inbound message limit fails before allocating

use ferrifuge::machine::{Action, Input, Machine, SubscriptionOptions, TransportId};
use ferrifuge::{Config, DeltaFormat, ErrorKind, SubscriptionEvent};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const MIB: usize = 1 << 20;

/// 20 bytes: a header declaring 64 MiB + 1 (`40001` in the delta's base-64
/// digits, 4 × 64^4 + 1), a copy of as many bytes, and a trailer.
const OVERSIZED: &[u8] = b"40001\n40001@0,00000;";

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

fn main() {
    let _profiler = dhat::Profiler::builder().testing().build();
    assert_eq!(OVERSIZED.len(), 20);

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
    let id = machine
        .add_subscription(
            "news".to_owned(),
            SubscriptionOptions::default().delta(DeltaFormat::Fossil),
        )
        .expect("a new channel");
    machine.handle(Input::Subscribe(id));
    machine.handle(frame(transport, r#"{"id":2,"subscribe":{"delta":true}}"#));
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":"{\"n\":1}"}}}"#,
    ));
    let delta = serde_json::to_string(std::str::from_utf8(OVERSIZED).expect("ASCII"))
        .expect("a JSON string");
    let push = format!(r#"{{"push":{{"channel":"news","pub":{{"data":{delta},"delta":true}}}}}}"#);

    let before = dhat::HeapStats::get();
    let actions = machine.handle(frame(transport, &push));
    let after = dhat::HeapStats::get();

    assert!(
        actions.iter().any(|action| matches!(
            action,
            Action::EmitSubscription(_, _, SubscriptionEvent::Error(error))
                if error.kind == ErrorKind::Delta && error.code == 15
        )),
        "{actions:?}"
    );
    // The peak while it was handled: never the declared 64 MiB.
    let peak = if after.max_bytes > before.max_bytes {
        after.max_bytes - before.curr_bytes
    } else {
        0
    };
    println!("@P report: live bytes added while handling the oversized delta: {peak}");
    assert!(peak < MIB, "{peak} bytes");
}
