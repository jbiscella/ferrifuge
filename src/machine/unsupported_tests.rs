//! Block 44, D12: a transport its owner cannot open is skipped in the
//! round, on the core (@E).

use super::*;

const CONNECT_REPLY: &str = r#"{"id":1,"connect":{"client":"c"}}"#;

/// The transport `actions` open, with its kind.
fn opened(actions: &[Action]) -> Option<(TransportId, TransportKind)> {
    actions.iter().find_map(|action| match action {
        Action::OpenTransport {
            transport, kind, ..
        } => Some((*transport, *kind)),
        _ => None,
    })
}

/// The id of the connect command an emulated transport's opening carries.
fn connect_id(actions: &[Action]) -> u64 {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport {
                initial_frame: Some(frame),
                ..
            } => serde_json::from_slice::<serde_json::Value>(frame)
                .ok()
                .and_then(|command| command["id"].as_u64()),
            _ => None,
        })
        .expect("a connect command")
}

fn reconnect_delay(actions: &[Action]) -> Option<Duration> {
    actions.iter().find_map(|action| match action {
        Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
        _ => None,
    })
}

fn errors(actions: &[Action]) -> Vec<&ClientError> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Emit(_, ClientEvent::Error(error)) => Some(error),
            _ => None,
        })
        .collect()
}

fn machine(kinds: &[TransportKind]) -> Machine {
    Machine::new(Config::default())
        .with_transports(kinds.to_vec())
        .expect("valid transports")
}

const ALL: [TransportKind; 3] = [
    TransportKind::WebSocket,
    TransportKind::Sse,
    TransportKind::HttpStream,
];

#[test]
fn an_unsupported_transport_is_skipped_without_an_error_or_a_failed_attempt() {
    let mut machine = machine(&ALL);
    let (websocket, _) = opened(&machine.handle(Input::Connect)).expect("an attempt");
    machine.handle(Input::TransportFailed {
        transport: websocket,
        message: "refused".into(),
    });
    let (sse, kind) =
        opened(&machine.handle(Input::TimerFired(Timer::Reconnect))).expect("the next attempt");
    assert_eq!(kind, TransportKind::Sse);

    let actions = machine.handle(Input::TransportUnsupported(sse));

    let (http_stream, kind) = opened(&actions).expect("the next transport at once");
    assert_eq!(kind, TransportKind::HttpStream);
    assert!(errors(&actions).is_empty(), "{actions:?}");
    assert_eq!(reconnect_delay(&actions), None);
    let id = connect_id(&actions);
    machine.handle(Input::TransportOpened(http_stream));
    machine.handle(Input::Frame {
        transport: http_stream,
        data: format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#).into_bytes(),
    });
    assert_eq!(machine.state(), State::Connected);
    assert_eq!(machine.statistics().failed_attempts, 1);
}

#[test]
fn an_attempt_finding_no_supported_transport_fails_once() {
    let mut machine = machine(&ALL);
    let mut transport = opened(&machine.handle(Input::Connect))
        .expect("an attempt")
        .0;
    let mut kinds = vec![TransportKind::WebSocket];
    let mut actions;
    loop {
        actions = machine.handle(Input::TransportUnsupported(transport));
        match opened(&actions) {
            Some((next, kind)) => {
                assert!(errors(&actions).is_empty(), "{actions:?}");
                kinds.push(kind);
                transport = next;
            }
            None => break,
        }
    }

    assert_eq!(kinds, ALL);
    let errors = errors(&actions);
    assert_eq!(errors.len(), 1, "{actions:?}");
    assert_eq!(errors[0].message, "no supported transport found");
    assert_eq!(errors[0].code, codes::error::TRANSPORT_CLOSED);
    assert_eq!(errors[0].transport, None);
    assert!(reconnect_delay(&actions).is_some(), "{actions:?}");
    assert_eq!(machine.state(), State::Connecting);
    assert_eq!(machine.statistics().failed_attempts, 1);
}

#[test]
fn the_next_attempt_skips_again_from_where_the_round_stands() {
    let mut machine = machine(&ALL);
    let mut transport = opened(&machine.handle(Input::Connect)).unwrap().0;
    for _ in 0..ALL.len() {
        if let Some((next, _)) = opened(&machine.handle(Input::TransportUnsupported(transport))) {
            transport = next;
        }
    }

    let (_, kind) = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)))
        .expect("the next attempt after the delay");

    assert_eq!(kind, TransportKind::WebSocket);
}

#[test]
fn a_skip_reported_for_an_abandoned_transport_does_nothing() {
    let mut machine = machine(&ALL);
    let (websocket, _) = opened(&machine.handle(Input::Connect)).unwrap();
    machine.handle(Input::TransportFailed {
        transport: websocket,
        message: "refused".into(),
    });

    let actions = machine.handle(Input::TransportUnsupported(websocket));

    assert!(actions.is_empty(), "{actions:?}");
}

#[test]
fn skipping_a_chosen_transport_moves_to_the_next() {
    let mut machine = machine(&[TransportKind::WebSocket, TransportKind::HttpStream]);
    let (websocket, _) = opened(&machine.handle(Input::Connect)).unwrap();
    machine.handle(Input::TransportOpened(websocket));
    machine.handle(Input::Frame {
        transport: websocket,
        data: CONNECT_REPLY.as_bytes().to_vec(),
    });
    machine.handle(Input::TransportClosed {
        transport: websocket,
        code: None,
        reason: String::new(),
    });
    let (again, kind) = opened(&machine.handle(Input::TimerFired(Timer::Reconnect))).unwrap();
    assert_eq!(kind, TransportKind::WebSocket);

    let (_, kind) =
        opened(&machine.handle(Input::TransportUnsupported(again))).expect("the next transport");

    assert_eq!(kind, TransportKind::HttpStream);
}

#[test]
fn a_skipped_emulated_transport_counts_no_opening_request() {
    let mut machine = machine(&[TransportKind::Sse, TransportKind::WebSocket]);
    let (sse, kind) = opened(&machine.handle(Input::Connect)).expect("an attempt");
    assert_eq!(kind, TransportKind::Sse);
    let (websocket, kind) =
        opened(&machine.handle(Input::TransportUnsupported(sse))).expect("the next transport");
    assert_eq!(kind, TransportKind::WebSocket);

    machine.handle(Input::TransportOpened(websocket));

    // The websocket's connect frame counts once written; the sse request
    // that never went out counts never.
    assert_eq!(machine.statistics().bytes_out, 0);
    assert_eq!(machine.statistics().messages_out, 0);
}
