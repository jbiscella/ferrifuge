//! Block 31: the core's counters after scripted inputs, one counter per
//! assertion.

use super::*;
use crate::{DeltaFormat, ServerSubscriptionEvent};

const CONNECT_REPLY: &str = r#"{"id":1,"connect":{"client":"c"}}"#;

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// The transport `actions` open.
fn opened(actions: &[Action]) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt")
}

/// The frames `actions` send.
fn sends(actions: &[Action]) -> Vec<(FrameId, Vec<u8>)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { id, frame, .. } => Some((*id, frame.clone())),
            _ => None,
        })
        .collect()
}

/// A machine whose websocket attempt is open, its connect command sent.
fn opening(machine: &mut Machine) -> TransportId {
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));
    transport
}

fn connected() -> (Machine, TransportId) {
    let mut machine = Machine::new(Config::default());
    let transport = opening(&mut machine);
    machine.handle(frame(transport, CONNECT_REPLY));
    (machine, transport)
}

/// How each ending of an attempt counts (D1).
#[test]
fn every_failed_ending_of_an_attempt_counts_once() {
    type Ending = fn(&mut Machine, TransportId);
    let endings: [(&str, Ending); 8] = [
        ("an opening that fails", |machine, _| {
            let transport = opened(&machine.handle(Input::Reconnect));
            machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            });
        }),
        ("a close before the connect reply", |machine, transport| {
            machine.handle(Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            });
        }),
        ("a connect timeout", |machine, _| {
            machine.handle(Input::TimerFired(Timer::ConnectTimeout));
        }),
        ("a message too large", |machine, transport| {
            machine.handle(Input::MessageTooLarge(transport));
        }),
        ("a temporary connect error", |machine, transport| {
            machine.handle(frame(
                transport,
                r#"{"id":1,"error":{"code":100,"message":"busy","temporary":true}}"#,
            ));
        }),
        ("a terminal connect error", |machine, transport| {
            machine.handle(frame(
                transport,
                r#"{"id":1,"error":{"code":101,"message":"unauthorized"}}"#,
            ));
        }),
        ("a disconnect push", |machine, transport| {
            machine.handle(frame(
                transport,
                r#"{"push":{"disconnect":{"code":3001,"reason":"shutdown"}}}"#,
            ));
        }),
        ("a close code from 3000", |machine, transport| {
            machine.handle(Input::TransportClosed {
                transport,
                code: Some(4500),
                reason: "terminal".into(),
            });
        }),
    ];
    for (name, end) in endings {
        let mut machine = Machine::new(Config::default());
        let transport = opening(&mut machine);
        // The opening-failure case starts its own attempt first, by
        // reconnect(), which is not counted.
        end(&mut machine, transport);
        assert_eq!(machine.statistics().failed_attempts, 1, "{name}");
        assert_eq!(machine.statistics().connects, 0, "{name}");
    }
}

#[test]
fn a_handshake_timeout_counts_as_a_failed_attempt() {
    let mut machine = Machine::new(Config::default());
    machine.handle(Input::Connect);

    machine.handle(Input::TimerFired(Timer::ConnectTimeout));

    assert_eq!(machine.statistics().failed_attempts, 1);
}

#[test]
fn an_attempt_ended_by_disconnect_or_reconnect_is_not_counted() {
    for input in [
        Input::Disconnect,
        Input::Reconnect,
        Input::DriverDropped { panicked: false },
    ] {
        let mut machine = Machine::new(Config::default());
        opening(&mut machine);

        machine.handle(input.clone());

        assert_eq!(machine.statistics().failed_attempts, 0, "{input:?}");
    }
}

#[test]
fn a_lost_connection_is_not_a_failed_attempt_and_its_reconnect_counts() {
    let (mut machine, transport) = connected();
    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let again = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)));
    machine.handle(Input::TransportOpened(again));

    machine.handle(frame(again, r#"{"id":2,"connect":{"client":"c"}}"#));

    assert_eq!(machine.statistics().connects, 2);
    assert_eq!(machine.statistics().failed_attempts, 0);
    assert_eq!(
        machine.statistics().transport,
        Some(TransportKind::WebSocket)
    );
}

#[test]
fn a_ping_counts_in_when_read_and_its_pong_out_when_written() {
    let mut machine = Machine::new(Config::default());
    let transport = opening(&mut machine);
    machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","ping":25,"pong":true}}"#,
    ));
    let before = machine.statistics();

    let read = machine.handle(frame(transport, "{}"));
    let [(pong, bytes)] = &sends(&read)[..] else {
        panic!("{read:?}");
    };
    let unwritten = machine.statistics();
    machine.handle(Input::FrameWritten {
        transport,
        frame: *pong,
    });
    let written = machine.statistics();

    assert_eq!(unwritten.messages_in, before.messages_in + 1);
    assert_eq!(unwritten.bytes_in, before.bytes_in + 2);
    assert_eq!(unwritten.messages_out, before.messages_out);
    assert_eq!(written.messages_out, before.messages_out + 1);
    assert_eq!(written.bytes_out, before.bytes_out + bytes.len() as u64);
}

#[test]
fn a_frame_of_an_abandoned_transport_is_not_counted() {
    let (mut machine, transport) = connected();
    machine.handle(Input::Disconnect);
    let before = machine.statistics();

    machine.handle(frame(transport, "{}"));

    assert_eq!(machine.statistics().bytes_in, before.bytes_in);
    assert_eq!(machine.statistics().messages_in, before.messages_in);
}

#[test]
fn an_undecodable_frame_counts_its_bytes_and_no_message() {
    let (mut machine, transport) = connected();
    let before = machine.statistics();

    machine.handle(frame(transport, "not json"));

    assert_eq!(machine.statistics().bytes_in, before.bytes_in + 8);
    assert_eq!(machine.statistics().messages_in, before.messages_in);
}

#[test]
fn messages_after_a_disconnect_push_in_one_frame_count_in() {
    let (mut machine, transport) = connected();
    let before = machine.statistics();

    machine.handle(frame(
        transport,
        "{\"push\":{\"disconnect\":{\"code\":3001,\"reason\":\"r\"}}}\n{\"push\":{\"channel\":\"news\",\"pub\":{\"data\":{}}}}",
    ));

    assert_eq!(machine.statistics().messages_in, before.messages_in + 2);
}

#[test]
fn a_frame_whose_transport_ends_before_it_is_written_counts_nowhere() {
    let (mut machine, transport) = connected();
    let id = machine.add_subscription("news".into(), SubscriptionOptions::default());
    let before = machine.statistics();
    let sent = machine.handle(Input::Subscribe(id.unwrap()));
    let [(frame_id, _)] = &sends(&sent)[..] else {
        panic!("{sent:?}");
    };

    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    machine.handle(Input::FrameWritten {
        transport,
        frame: *frame_id,
    });

    assert_eq!(machine.statistics().bytes_out, before.bytes_out);
    assert_eq!(machine.statistics().messages_out, before.messages_out);
    assert_eq!(machine.counters.unwritten(), 0);
}

#[test]
fn a_frame_of_three_commands_counts_three_out_and_its_bytes_once() {
    let mut machine = Machine::new(Config::default());
    for channel in ["a", "b", "c"] {
        let id = machine
            .add_subscription(channel.into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(id));
    }
    let transport = opening(&mut machine);
    let before = machine.statistics();

    let replied = machine.handle(frame(transport, CONNECT_REPLY));
    let [(frame_id, bytes)] = &sends(&replied)[..] else {
        panic!("one frame of subscribes: {replied:?}");
    };
    machine.handle(Input::FrameWritten {
        transport,
        frame: *frame_id,
    });

    assert_eq!(machine.statistics().messages_out, before.messages_out + 3);
    assert_eq!(
        machine.statistics().bytes_out,
        before.bytes_out + bytes.len() as u64
    );
}

#[test]
fn an_emulated_transports_connect_command_counts_when_it_opens() {
    let mut machine = Machine::new(Config::default())
        .with_transports(vec![TransportKind::HttpStream])
        .unwrap();
    let connect = machine.handle(Input::Connect);
    let initial = connect
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { initial_frame, .. } => initial_frame.clone(),
            _ => None,
        })
        .expect("the connect command in the request");
    let transport = opened(&connect);
    assert_eq!(machine.statistics().messages_out, 0);

    machine.handle(Input::TransportOpened(transport));

    assert_eq!(machine.statistics().messages_out, 1);
    assert_eq!(machine.statistics().bytes_out, initial.len() as u64);
}

#[test]
fn a_request_refused_with_413_counts_once_and_each_command_sent_again() {
    let mut machine = Machine::new(Config::default())
        .with_transports(vec![TransportKind::HttpStream])
        .unwrap();
    for channel in ["a", "b"] {
        let id = machine
            .add_subscription(channel.into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(id));
    }
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));
    let replied = machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
    ));
    let [(refused, request)] = &sends(&replied)[..] else {
        panic!("{replied:?}");
    };
    let before = machine.statistics();

    let again = machine.handle(Input::SendRejected {
        transport,
        frame: request.clone(),
    });
    machine.handle(Input::FrameWritten {
        transport,
        frame: *refused,
    });
    let resent = sends(&again);
    for (id, _) in &resent {
        machine.handle(Input::FrameWritten {
            transport,
            frame: *id,
        });
    }

    let resent_bytes: u64 = resent.iter().map(|(_, frame)| frame.len() as u64).sum();
    assert_eq!(resent.len(), 2, "{again:?}");
    assert_eq!(
        machine.statistics().messages_out,
        before.messages_out + 2 + 2
    );
    assert_eq!(
        machine.statistics().bytes_out,
        before.bytes_out + request.len() as u64 + resent_bytes
    );
}

/// Takes every event `actions` emit, and those taking them releases, as an
/// application reading its streams does (Block 15).
fn take_all(machine: &mut Machine, actions: Vec<Action>) {
    let mut pending = actions;
    while !pending.is_empty() {
        let ids: Vec<EventId> = pending
            .iter()
            .filter_map(|action| match action {
                Action::Emit(id, _) | Action::EmitSubscription(id, _, _) => Some(*id),
                _ => None,
            })
            .collect();
        pending = ids
            .into_iter()
            .flat_map(|id| machine.handle(Input::EventTaken(id)))
            .collect();
    }
}

/// A connected machine with subscription "news" subscribed, its stream
/// open when `stream`, every event so far taken.
fn subscribed(stream: bool) -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected();
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    if stream {
        machine.handle(Input::StreamOpened(Some(id)));
    }
    let subscribing = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, subscribing);
    let subscribed = machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));
    take_all(&mut machine, subscribed);
    (machine, transport, id)
}

fn publication_ids(actions: &[Action]) -> Vec<EventId> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSubscription(event, _, SubscriptionEvent::Publication(_))
            | Action::Emit(
                event,
                ClientEvent::ServerSubscription {
                    event: ServerSubscriptionEvent::Publication(_),
                    ..
                },
            ) => Some(*event),
            _ => None,
        })
        .collect()
}

const PUSH: &str = r#"{"push":{"channel":"news","pub":{"data":{}}}}"#;

#[test]
fn publications_count_once_taken_and_never_withdrawn() {
    let (mut machine, transport, id) = subscribed(true);
    let first = publication_ids(&machine.handle(frame(transport, PUSH)));
    // Held behind the first until it is taken (Block 15).
    machine.handle(frame(transport, PUSH));
    assert_eq!(machine.statistics().publications_delivered, 0, "not taken");

    let second = publication_ids(&machine.handle(Input::EventTaken(first[0])));
    machine.handle(Input::Unsubscribe(id));

    assert_eq!(second.len(), 1, "released, then withdrawn");
    assert_eq!(machine.statistics().publications_delivered, 1);
}

#[test]
fn joins_and_leaves_taken_are_not_publications() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_subscription(
            "news".into(),
            SubscriptionOptions::default().join_leave(true),
        )
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let subscribing = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, subscribing);
    let subscribed = machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));
    take_all(&mut machine, subscribed);

    let joined = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","join":{"info":{"client":"x","user":"u"}}}}"#,
    ));
    assert!(
        joined.iter().any(|action| matches!(
            action,
            Action::EmitSubscription(_, _, SubscriptionEvent::Join(_))
        )),
        "{joined:?}"
    );
    take_all(&mut machine, joined);

    assert_eq!(machine.statistics().publications_delivered, 0);
}

#[test]
fn publications_with_no_stream_count_at_once() {
    let (mut machine, transport, _) = subscribed(false);

    machine.handle(frame(transport, PUSH));

    assert_eq!(machine.statistics().publications_delivered, 1);
}

#[test]
fn server_side_publications_count_once_taken() {
    let mut machine = Machine::new(Config::default());
    machine.handle(Input::StreamOpened(None));
    let connecting = machine.handle(Input::Connect);
    let transport = opened(&connecting);
    take_all(&mut machine, connecting);
    machine.handle(Input::TransportOpened(transport));
    let connected = machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{}}}}"#,
    ));
    take_all(&mut machine, connected);
    let push = r#"{"push":{"channel":"alerts","pub":{"data":{}}}}"#;
    let ids = publication_ids(&machine.handle(frame(transport, push)));
    assert_eq!(machine.statistics().publications_delivered, 0);

    machine.handle(Input::EventTaken(ids[0]));

    assert_eq!(machine.statistics().publications_delivered, 1);
    assert_eq!(machine.statistics().server_subscriptions, 1);
}

/// A connected machine whose subscription "news" negotiated fossil delta.
fn with_delta() -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected();
    let id = machine
        .add_subscription(
            "news".into(),
            SubscriptionOptions::default().delta(DeltaFormat::Fossil),
        )
        .unwrap();
    machine.handle(Input::Subscribe(id));
    machine.handle(frame(transport, r#"{"id":2,"subscribe":{"delta":true}}"#));
    (machine, transport, id)
}

fn delta(machine: &Machine, id: SubscriptionId) -> crate::DeltaStatistics {
    machine.delta_statistics(id).expect("in the registry")
}

#[test]
fn delta_counts_full_and_delta_payloads_as_carried_and_rebuilt() {
    let (mut machine, transport, id) = with_delta();
    let base = "abcdefghijklmnopqrstuvwxyz";
    let target = "abcdefghijklmnopqrstuvwxyZ";
    let patch = String::from_utf8(crate::write_fossil_delta(
        base.as_bytes(),
        target.as_bytes(),
    ))
    .unwrap();
    let full = format!(
        r#"{{"push":{{"channel":"news","pub":{{"data":{}}}}}}}"#,
        serde_json::to_string(base).unwrap()
    );
    let patched = format!(
        r#"{{"push":{{"channel":"news","pub":{{"data":{},"delta":true}}}}}}"#,
        serde_json::to_string(&patch).unwrap()
    );

    machine.handle(frame(transport, &full));
    machine.handle(frame(transport, &patched));

    let statistics = delta(&machine, id);
    assert_eq!(statistics.publications, 2);
    assert_eq!(statistics.full_payloads, 1);
    assert_eq!(statistics.delta_payloads, 1);
    assert_eq!(statistics.bytes_received, (base.len() + patch.len()) as u64);
    assert_eq!(statistics.bytes_decoded, (base.len() + target.len()) as u64);
}

#[test]
fn a_failed_delta_is_not_counted_and_the_counters_outlive_the_error() {
    let (mut machine, transport, id) = with_delta();
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":"\"a\""}}}"#,
    ));

    // A delta whose checksum is wrong.
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":"2\n2:{}0;","delta":true}}}"#,
    ));

    let statistics = delta(&machine, id);
    assert_eq!(statistics.publications, 1);
    assert_eq!(statistics.delta_payloads, 0);
}

#[test]
fn a_subscription_without_delta_and_a_removed_one_report_zeros() {
    let (mut machine, transport, id) = subscribed(false);
    machine.handle(frame(transport, PUSH));

    assert_eq!(delta(&machine, id), crate::DeltaStatistics::default());
    machine.handle(Input::RemoveSubscription(id));
    assert_eq!(machine.delta_statistics(id), None);
}

#[test]
fn recovered_publications_count_in_delta_statistics() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_subscription(
            "news".into(),
            SubscriptionOptions::default().delta(DeltaFormat::Fossil),
        )
        .unwrap();
    machine.handle(Input::Subscribe(id));

    machine.handle(frame(
        transport,
        r#"{"id":2,"subscribe":{"delta":true,"recovered":true,"was_recovering":true,"publications":[{"data":"\"ab\"","offset":1}]}}"#,
    ));

    assert_eq!(delta(&machine, id).publications, 1);
    assert_eq!(delta(&machine, id).bytes_received, 4);
}

#[test]
fn the_compression_ratio_is_zero_without_bytes_and_can_be_negative() {
    let mut statistics = crate::DeltaStatistics::default();
    assert!(statistics.compression_ratio().abs() < f64::EPSILON);

    statistics.count(true, 30, 10);

    assert!(statistics.compression_ratio() < 0.0);
}

#[test]
fn a_new_machine_reports_zeros() {
    let machine = Machine::new(Config::default());

    let statistics = machine.statistics();

    assert_eq!(statistics.state, State::Disconnected);
    assert_eq!(statistics.transport, None);
    assert_eq!(statistics.connected_since, None);
    assert_eq!(
        (
            statistics.connects,
            statistics.failed_attempts,
            statistics.bytes_in,
            statistics.bytes_out,
            statistics.messages_in,
            statistics.messages_out,
            statistics.publications_delivered,
        ),
        (0, 0, 0, 0, 0, 0, 0)
    );
}
