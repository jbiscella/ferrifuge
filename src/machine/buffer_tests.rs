//! Block 36a: the output buffer and `Input::Flush`, on the core.

use super::*;
use crate::{CallReply, CallRequest};

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// The frames among `actions`, as text.
fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
            _ => None,
        })
        .collect()
}

/// A machine with automatic batching over `kinds`, connected with
/// `connect` as the connect reply's result, its connect frame flushed.
fn connected(config: Config, kinds: Vec<TransportKind>, connect: &str) -> (Machine, TransportId) {
    let mut machine = Machine::new(config)
        .with_automatic_batching()
        .with_transports(kinds)
        .expect("transports");
    let transport = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(Input::Flush);
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":1,"connect":{connect}}}"#),
    ));
    machine.handle(Input::Flush);
    (machine, transport)
}

fn websocket() -> (Machine, TransportId) {
    connected(
        Config::default(),
        vec![TransportKind::WebSocket],
        r#"{"client":"c","ping":25,"pong":true}"#,
    )
}

fn call(machine: &mut Machine, request: CallRequest) -> (CallId, Vec<Action>) {
    let id = machine.add_call(request);
    let actions = machine.handle(Input::Call(id));
    (id, actions)
}

fn rpc(method: &str) -> CallRequest {
    CallRequest::Rpc {
        method: method.to_owned(),
        data: b"{}".to_vec(),
    }
}

#[test]
fn the_buffer_keeps_the_order_the_core_produced_its_commands_in() {
    let (mut machine, transport) = websocket();
    let mut before = machine.handle(frame(transport, "{}"));
    before.extend(call(&mut machine, rpc("a")).1);
    before.extend(
        call(
            &mut machine,
            CallRequest::Send {
                data: br#"{"n":1}"#.to_vec(),
            },
        )
        .1,
    );
    before.extend(
        call(
            &mut machine,
            CallRequest::Publish {
                channel: "news".to_owned(),
                data: br#"{"n":2}"#.to_vec(),
            },
        )
        .1,
    );

    let flushed = machine.handle(Input::Flush);

    assert_eq!(sent(&before), Vec::<String>::new());
    assert_eq!(
        sent(&flushed),
        [concat!(
            "{}\n",
            r#"{"id":2,"rpc":{"method":"a","data":{}}}"#,
            "\n",
            r#"{"send":{"data":{"n":1}}}"#,
            "\n",
            r#"{"id":3,"publish":{"channel":"news","data":{"n":2}}}"#,
        )]
    );
}

#[test]
fn a_send_settles_after_its_frame_is_handed_over() {
    let (mut machine, _) = websocket();
    let (id, made) = call(
        &mut machine,
        CallRequest::Send {
            data: b"{}".to_vec(),
        },
    );

    let flushed = machine.handle(Input::Flush);

    assert!(
        !made
            .iter()
            .any(|action| matches!(action, Action::CallSettled { .. })),
        "{made:?}"
    );
    let send = flushed
        .iter()
        .position(|action| matches!(action, Action::Send { .. }));
    let settled = flushed.iter().position(|action| {
        matches!(action, Action::CallSettled { call, result: Ok(CallReply::Send) } if *call == id)
    });
    assert!(send.is_some() && settled > send, "{flushed:?}");
}

#[test]
fn a_flush_with_nothing_buffered_or_without_automatic_batching_does_nothing() {
    let (mut machine, _) = websocket();
    let mut plain = Machine::new(Config::default());
    let transport = plain
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt");
    let opened = plain.handle(Input::TransportOpened(transport));

    assert_eq!(machine.handle(Input::Flush), []);
    assert_eq!(sent(&opened).len(), 1, "{opened:?}");
    assert_eq!(plain.handle(Input::Flush), []);
}

#[test]
fn a_call_abandoned_or_settled_before_the_flush_is_not_sent() {
    let (mut machine, _) = websocket();
    let (abandoned, _) = call(&mut machine, rpc("a"));
    call(&mut machine, rpc("b"));
    machine.handle(Input::CallAbandoned(abandoned));

    let flushed = sent(&machine.handle(Input::Flush));

    assert_eq!(flushed, [r#"{"id":3,"rpc":{"method":"b","data":{}}}"#]);
}

/// The room of a buffered call abandoned before the flush goes at once
/// to the call waiting for it (Codex on #150).
#[test]
fn a_call_abandoned_before_the_flush_frees_its_room_at_once() {
    let config = Config::default().write_queue_bytes(60);
    let (mut machine, _) = connected(config, vec![TransportKind::WebSocket], r#"{"client":"c"}"#);
    let (abandoned, _) = call(&mut machine, rpc("a"));
    // The first call fills the queue: the second waits for room.
    call(&mut machine, rpc("b"));

    machine.handle(Input::CallAbandoned(abandoned));
    let flushed = sent(&machine.handle(Input::Flush));

    assert_eq!(flushed, [r#"{"id":3,"rpc":{"method":"b","data":{}}}"#]);
}

/// A buffered call that settles before the flush, by its timeout, leaves
/// the buffer and its room too.
#[test]
fn a_call_timed_out_before_the_flush_leaves_the_buffer() {
    let config = Config::default().write_queue_bytes(60);
    let (mut machine, _) = connected(config, vec![TransportKind::WebSocket], r#"{"client":"c"}"#);
    let (timed_out, _) = call(&mut machine, rpc("a"));
    call(&mut machine, rpc("b"));

    let settled = machine.handle(Input::CallTimerFired(timed_out, CallTimer::Reply));
    let flushed = sent(&machine.handle(Input::Flush));

    assert!(
        settled.iter().any(|action| matches!(
            action,
            Action::CallSettled { call, result: Err(error) }
                if *call == timed_out && error.code == codes::error::TIMEOUT
        )),
        "{settled:?}"
    );
    assert_eq!(flushed, [r#"{"id":3,"rpc":{"method":"b","data":{}}}"#]);
}

#[test]
fn teardown_drops_the_buffer_and_fails_its_calls_with_connection_closed() {
    let (mut machine, transport) = websocket();
    let (rpc_call, _) = call(&mut machine, rpc("a"));
    let (send_call, _) = call(
        &mut machine,
        CallRequest::Send {
            data: b"{}".to_vec(),
        },
    );

    let closed = machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".into(),
    });
    let flushed = machine.handle(Input::Flush);

    for id in [rpc_call, send_call] {
        assert!(
            closed.iter().any(|action| matches!(
                action,
                Action::CallSettled { call, result: Err(error) }
                    if *call == id && error.code == codes::error::CONNECTION_CLOSED
            )),
            "{closed:?}"
        );
    }
    assert_eq!(sent(&flushed), Vec::<String>::new());
}

#[test]
fn the_calls_of_a_flushed_frame_hold_the_write_queue_until_it_is_written() {
    let config = Config::default().write_queue_bytes(100);
    let (mut machine, _) = connected(config, vec![TransportKind::WebSocket], r#"{"client":"c"}"#);
    call(&mut machine, rpc("a"));
    call(&mut machine, rpc("b"));
    let first = machine.handle(Input::Flush);
    let id = first
        .iter()
        .find_map(|action| match action {
            Action::Send { id, .. } => Some(*id),
            _ => None,
        })
        .expect("a frame");
    // Both calls fill the queue: a third waits for room.
    call(&mut machine, rpc("c"));
    let waiting = machine.handle(Input::Flush);
    let transport = machine.transport.expect("a transport");

    machine.handle(Input::FrameWritten {
        transport,
        frame: id,
    });
    let after = machine.handle(Input::Flush);

    assert_eq!(sent(&first).len(), 1, "{first:?}");
    assert_eq!(sent(&waiting), Vec::<String>::new());
    assert_eq!(sent(&after), [r#"{"id":4,"rpc":{"method":"c","data":{}}}"#]);
}

#[test]
fn commands_resent_after_http_413_go_alone() {
    let (mut machine, transport) = connected(
        Config::default(),
        vec![TransportKind::HttpStream],
        r#"{"client":"c","session":"s","node":"n"}"#,
    );
    call(&mut machine, rpc("a"));
    call(&mut machine, rpc("b"));
    let request = machine
        .handle(Input::Flush)
        .into_iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame),
            _ => None,
        })
        .expect("one request");

    machine.handle(Input::SendRejected {
        transport,
        frame: request,
    });
    let resent = sent(&machine.handle(Input::Flush));

    assert_eq!(resent.len(), 2, "{resent:?}");
    assert!(resent[0].contains(r#"\"method\":\"a\""#), "{resent:?}");
    assert!(resent[1].contains(r#"\"method\":\"b\""#), "{resent:?}");
}

#[test]
fn the_debug_output_of_a_flush_names_it() {
    assert_eq!(format!("{:?}", Input::Flush), "Flush");
}

/// The size a frame is computed to have equals the length of the frame
/// encoded, for both encodings, over websocket and emulated (Block 36,
/// D1).
#[test]
fn a_computed_frame_size_is_the_encoded_length() {
    bolero::check!()
        .with_type::<(bool, bool, Vec<(String, Vec<u8>)>)>()
        .for_each(|(protobuf, emulated, calls)| {
            let mut machine = Machine::new(Config::default());
            if *protobuf {
                machine.codec = crate::codec::Codec::Protobuf;
            }
            if *emulated {
                machine.emulation = Some(("sess\"ion".to_owned(), "nöde".to_owned()));
            }
            let commands: Vec<Command> = calls
                .iter()
                .take(20)
                .zip(1..)
                .map(|((method, data), id)| Command {
                    id,
                    method: crate::wire::Method::Rpc(crate::wire::RpcRequest {
                        method: method.clone(),
                        // JSON data must be JSON: a string of the bytes.
                        data: if *protobuf {
                            data.clone()
                        } else {
                            serde_json::to_vec(&String::from_utf8_lossy(data)).unwrap()
                        },
                    }),
                })
                .collect();
            if commands.is_empty() {
                return;
            }
            let sum: usize = commands
                .iter()
                .map(|command| {
                    machine.piece_size(&machine.codec.encode(std::slice::from_ref(command)))
                })
                .sum();

            assert_eq!(
                machine.frame_bytes(commands.len(), sum),
                machine.frame_of(&commands).len()
            );
        });
}
