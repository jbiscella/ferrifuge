//! Block 48 on the core: WebTransport takes websocket's paths (D10, D20),
//! but for the rules of D9 and D7.

use super::*;

/// What the owner reports back for `actions`: each frame sent, written.
fn written(transport: TransportId, actions: &[Action]) -> Vec<Input> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { id, .. } => Some(Input::FrameWritten {
                transport,
                frame: *id,
            }),
            _ => None,
        })
        .collect()
}

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

fn opened(actions: &[Action]) -> Option<TransportId> {
    actions.iter().find_map(|action| match action {
        Action::OpenTransport { transport, .. } => Some(*transport),
        _ => None,
    })
}

/// The actions of a machine over `kind`, as text with the transport's
/// names erased, for a session: a connect with a subscription sent along,
/// a publication, server pings, a call, a lost transport and its
/// reconnect, then a terminal disconnect.
fn session(kind: TransportKind) -> Vec<String> {
    let mut machine = Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(48))
        .with_transports(vec![kind])
        .expect("one transport");
    let id = machine
        .add_subscription(
            "news".to_owned(),
            crate::machine::SubscriptionOptions::default(),
        )
        .expect("a new channel");
    let mut log = Vec::new();
    let mut handle = |machine: &mut Machine, input: Input| {
        let actions = machine.handle(input);
        log.push(format!("{actions:?}"));
        actions
    };
    handle(&mut machine, Input::Subscribe(id));
    let mut transport = opened(&handle(&mut machine, Input::Connect)).expect("an attempt");
    for round in 0..2 {
        let actions = handle(&mut machine, Input::TransportOpened(transport));
        for input in written(transport, &actions) {
            handle(&mut machine, input);
        }
        handle(
            &mut machine,
            frame(
                transport,
                r#"{"id":1,"connect":{"client":"c","session":"s","node":"n","ping":25,"pong":true}}"#,
            ),
        );
        handle(&mut machine, frame(transport, r#"{"id":2,"subscribe":{}}"#));
        let actions = handle(
            &mut machine,
            frame(
                transport,
                r#"{"push":{"channel":"news","pub":{"data":{"n":1}}}}"#,
            ),
        );
        for action in actions {
            if let Action::Emit(event, _) = action {
                handle(&mut machine, Input::EventTaken(event));
            }
        }
        let actions = handle(&mut machine, frame(transport, "{}"));
        for input in written(transport, &actions) {
            handle(&mut machine, input);
        }
        if round == 0 {
            handle(
                &mut machine,
                Input::TransportClosed {
                    transport,
                    code: None,
                    reason: String::new(),
                },
            );
            let actions = handle(&mut machine, Input::TimerFired(Timer::Reconnect));
            transport = opened(&actions).expect("a reconnect");
        }
    }
    handle(
        &mut machine,
        Input::TransportClosed {
            transport,
            code: Some(3500),
            reason: "invalid token".to_owned(),
        },
    );
    log.into_iter()
        .map(|actions| {
            actions
                .replace("WebTransport", "<kind>")
                .replace("WebSocket", "<kind>")
                .replace("webtransport", "<kind>")
                .replace("websocket", "<kind>")
        })
        .collect()
}

// D20: over the same inputs, the core's actions for a WebTransport
// connection are those for a websocket one, step by step, but for the kind.
#[test]
fn webtransport_takes_the_websocket_paths_of_the_core() {
    let websocket = session(TransportKind::WebSocket);
    let webtransport = session(TransportKind::WebTransport);

    let all = websocket.concat();
    for part in [
        "Connected",
        "Publication",
        "Subscribed",
        "Connecting",
        "Disconnected",
    ] {
        assert!(all.contains(part), "the session has no {part}: {all}");
    }
    assert_eq!(websocket.len(), webtransport.len());
    for (step, (websocket, webtransport)) in websocket.iter().zip(&webtransport).enumerate() {
        assert_eq!(websocket, webtransport, "step {step}");
    }
}

/// The events of a first attempt over `kind`, followed by websocket, whose
/// stream ends without a code once open, before the server answered.
fn ended_before_an_answer(kind: TransportKind) -> (Vec<ClientEvent>, TransportKind) {
    let mut machine = Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(48))
        .with_transports(vec![kind, TransportKind::WebSocket])
        .expect("two transports");
    let transport = opened(&machine.handle(Input::Connect)).expect("an attempt");
    machine.handle(Input::TransportOpened(transport));
    let actions = machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let mut events: Vec<ClientEvent> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Emit(_, event) => Some(event.clone()),
            _ => None,
        })
        .collect();
    let actions = machine.handle(Input::TimerFired(Timer::Reconnect));
    let next = actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { kind, .. } => Some(*kind),
            _ => None,
        })
        .expect("the next attempt");
    events.retain(|event| !matches!(event, ClientEvent::Connecting { .. }));
    (events, next)
}

// D9, the first rule: a WebTransport stream that ends before the server
// answered on it is a failed attempt, with its error event; a websocket
// closed so is not reported (centrifuge-js 63ed73e reports only a
// transport that never opened, `centrifuge.ts:1327-1340`). Both give way
// to the next transport.
#[test]
fn a_webtransport_ending_before_an_answer_is_a_reported_failed_attempt() {
    let (websocket, after_websocket) = ended_before_an_answer(TransportKind::WebSocket);
    let (webtransport, after_webtransport) = ended_before_an_answer(TransportKind::WebTransport);

    assert_eq!(websocket, []);
    assert!(
        matches!(
            webtransport.as_slice(),
            [ClientEvent::Error(ClientError { kind: ErrorKind::Transport, transport: Some(name), .. })]
                if name == "webtransport"
        ),
        "{webtransport:?}"
    );
    assert_eq!(after_websocket, TransportKind::WebSocket);
    assert_eq!(after_webtransport, TransportKind::WebSocket);
}

// D7, D18: `bytes_out` counts the `\n` after each JSON frame over
// WebTransport, and nothing more over websocket or with Protobuf.
#[test]
fn bytes_out_counts_the_newline_of_a_json_frame_over_webtransport() {
    let mut counted = Vec::new();
    for (kind, protocol) in [
        (TransportKind::WebSocket, crate::Protocol::Json),
        (TransportKind::WebTransport, crate::Protocol::Json),
        (TransportKind::WebTransport, crate::Protocol::Protobuf),
    ] {
        let mut machine = Machine::new(Config::default().protocol(protocol))
            .with_transports(vec![kind])
            .expect("one transport");
        let transport = opened(&machine.handle(Input::Connect)).expect("an attempt");
        let actions = machine.handle(Input::TransportOpened(transport));
        let sent: usize = actions
            .iter()
            .map(|action| match action {
                Action::Send { frame, .. } => frame.len(),
                _ => 0,
            })
            .sum();
        for input in written(transport, &actions) {
            machine.handle(input);
        }
        counted.push(machine.statistics().bytes_out - sent as u64);
    }

    assert_eq!(counted, [0, 1, 0]);
}

/// Over `[kind, HttpStream]`: a connection over `kind`, lost with no code,
/// then the next attempt's stream over `kind` ending with no code before
/// the server answered. The events of that end, and the next attempt's
/// transport and delay.
fn chosen_then_ended_before_an_answer(
    kind: TransportKind,
) -> (Vec<ClientEvent>, TransportKind, Duration) {
    let mut machine = Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(48))
        .with_transports(vec![kind, TransportKind::HttpStream])
        .expect("two transports");
    let transport = opened(&machine.handle(Input::Connect)).expect("an attempt");
    let actions = machine.handle(Input::TransportOpened(transport));
    let connect = actions
        .iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        })
        .expect("the connect command");
    let id = serde_json::from_slice::<serde_json::Value>(&connect).expect("JSON")["id"].clone();
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#),
    ));
    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let transport =
        opened(&machine.handle(Input::TimerFired(Timer::Reconnect))).expect("a reconnect");
    machine.handle(Input::TransportOpened(transport));
    let actions = machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let events = actions
        .iter()
        .filter_map(|action| match action {
            Action::Emit(_, ClientEvent::Connecting { .. }) => None,
            Action::Emit(_, event) => Some(event.clone()),
            _ => None,
        })
        .collect();
    let delay = actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .expect("a reconnect scheduled");
    let next = machine
        .handle(Input::TimerFired(Timer::Reconnect))
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { kind, .. } => Some(*kind),
            _ => None,
        })
        .expect("the next attempt");
    (events, next, delay)
}

// D9, the second rule: a chosen WebTransport whose stream ends before the
// server answered gives way, with its error event, to the next transport at
// once; a chosen websocket is kept, after the backoff (Block 8).
#[test]
fn a_chosen_webtransport_ending_before_an_answer_gives_way_at_once() {
    let (websocket, after_websocket, websocket_delay) =
        chosen_then_ended_before_an_answer(TransportKind::WebSocket);
    let (webtransport, after_webtransport, webtransport_delay) =
        chosen_then_ended_before_an_answer(TransportKind::WebTransport);

    assert_eq!(websocket, []);
    assert_eq!(after_websocket, TransportKind::WebSocket);
    assert!(websocket_delay > Duration::ZERO);
    assert!(
        matches!(
            webtransport.as_slice(),
            [ClientEvent::Error(ClientError { kind: ErrorKind::Transport, transport: Some(name), .. })]
                if name == "webtransport"
        ),
        "{webtransport:?}"
    );
    assert_eq!(after_webtransport, TransportKind::HttpStream);
    assert_eq!(webtransport_delay, Duration::ZERO);
}

/// The reconnect delay after the third connection over `kind` that got its
/// connect reply and ended with no code before a server ping, with the
/// backoff of 1 s to 1000 s and the machine's seed `seed`.
fn third_delay_without_a_ping(kind: TransportKind, seed: u64) -> Duration {
    let config = Config::default()
        .min_reconnect_delay(Duration::from_secs(1))
        .max_reconnect_delay(Duration::from_secs(1000));
    let mut machine = Machine::with_rng(config, false, fastrand::Rng::with_seed(seed))
        .with_transports(vec![kind])
        .expect("one transport");
    let mut actions = machine.handle(Input::Connect);
    let mut delay = Duration::ZERO;
    for _ in 0..3 {
        let transport = opened(&actions).expect("an attempt");
        let sent = machine.handle(Input::TransportOpened(transport));
        let connect = sent
            .iter()
            .find_map(|action| match action {
                Action::Send { frame, .. } => Some(frame.clone()),
                _ => None,
            })
            .expect("the connect command");
        let id = serde_json::from_slice::<serde_json::Value>(&connect).expect("JSON")["id"].clone();
        machine.handle(frame(
            transport,
            &format!(r#"{{"id":{id},"connect":{{"client":"c","ping":25,"pong":true}}}}"#),
        ));
        let ended = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        delay = ended
            .iter()
            .find_map(|action| match action {
                Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
                _ => None,
            })
            .expect("a reconnect scheduled");
        actions = machine.handle(Input::TimerFired(Timer::Reconnect));
    }
    delay
}

// D9, "Backoff after 3008" extended: over WebTransport, the third such end
// waits as attempt 3 (up to 1 s + 4 s); over websocket each connect reply
// resets the backoff, so attempt 1 (up to 1 s + 1 s).
#[test]
fn a_webtransport_end_without_a_code_before_a_ping_keeps_the_backoff_growing() {
    let seeds = 0..100;
    let websocket: Vec<_> = seeds
        .clone()
        .map(|seed| third_delay_without_a_ping(TransportKind::WebSocket, seed))
        .collect();
    let webtransport: Vec<_> = seeds
        .map(|seed| third_delay_without_a_ping(TransportKind::WebTransport, seed))
        .collect();

    assert!(
        websocket
            .iter()
            .all(|delay| *delay <= Duration::from_secs(2)),
        "{websocket:?}"
    );
    assert!(
        webtransport
            .iter()
            .all(|delay| *delay <= Duration::from_secs(5)),
        "{webtransport:?}"
    );
    assert!(
        webtransport
            .iter()
            .any(|delay| *delay > Duration::from_secs(3)),
        "{webtransport:?}"
    );
}
