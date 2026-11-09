//! Block 44, D10 and D11: going offline and a page entering the
//! back/forward cache, on the core (@E).

use super::*;

const CONNECT_REPLY: &str = r#"{"id":1,"connect":{"client":"c"}}"#;

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

/// The transport `actions` open, with its kind.
fn opened(actions: &[Action]) -> Option<(TransportId, TransportKind)> {
    actions.iter().find_map(|action| match action {
        Action::OpenTransport {
            transport, kind, ..
        } => Some((*transport, *kind)),
        _ => None,
    })
}

/// The reconnect delay `actions` set.
fn reconnect_delay(actions: &[Action]) -> Option<Duration> {
    actions.iter().find_map(|action| match action {
        Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
        _ => None,
    })
}

fn events(actions: &[Action]) -> Vec<&ClientEvent> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Emit(_, event) => Some(event),
            _ => None,
        })
        .collect()
}

fn connected(config: Config) -> (Machine, TransportId) {
    let mut machine = Machine::new(config);
    let (transport, _) = opened(&machine.handle(Input::Connect)).expect("an attempt");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, CONNECT_REPLY));
    assert_eq!(machine.state(), State::Connected);
    (machine, transport)
}

fn config(min: u64, jitter: u64) -> Config {
    Config {
        min_reconnect_delay: Duration::from_millis(min),
        reconnect_jitter_window: Duration::from_millis(jitter),
        ..Config::default()
    }
}

#[test]
fn offline_while_connected_closes_the_transport_and_waits_the_backoff() {
    let (mut machine, transport) = connected(Config::default());

    let actions = machine.handle(Input::Offline);

    assert!(
        actions.contains(&Action::CloseTransport(transport)),
        "{actions:?}"
    );
    assert_eq!(
        events(&actions),
        [&ClientEvent::Connecting {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: "transport closed".into(),
        }]
    );
    let delay = reconnect_delay(&actions).expect("a reconnect delay");
    assert!(
        (Duration::from_millis(500)..=Duration::from_millis(1000)).contains(&delay),
        "{delay:?}"
    );
    assert_eq!(opened(&actions), None);
}

#[test]
fn offline_while_connected_adds_the_jitter_window() {
    for seed in 0..50 {
        let (machine, _) = connected(config(500, 100_000));
        let mut machine = machine.with_seed(seed);

        let delay = reconnect_delay(&machine.handle(Input::Offline)).expect("a delay");

        assert!(delay <= Duration::from_millis(101_000), "{delay:?}");
    }
}

#[test]
fn page_cached_while_connected_reconnects_with_its_own_reason() {
    let (mut machine, transport) = connected(Config::default());

    let actions = machine.handle(Input::PageCached);

    assert!(
        actions.contains(&Action::CloseTransport(transport)),
        "{actions:?}"
    );
    assert_eq!(
        events(&actions),
        [&ClientEvent::Connecting {
            code: codes::connecting::TRANSPORT_CLOSED,
            reason: "page entered back/forward cache".into(),
        }]
    );
    assert!(reconnect_delay(&actions).is_some());
}

#[test]
fn offline_while_connecting_closes_the_attempt_without_an_event() {
    let mut machine = Machine::new(Config::default())
        .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
        .unwrap();
    let (transport, kind) = opened(&machine.handle(Input::Connect)).expect("an attempt");
    assert_eq!(kind, TransportKind::WebSocket);

    let actions = machine.handle(Input::Offline);

    assert!(
        actions.contains(&Action::CloseTransport(transport)),
        "{actions:?}"
    );
    assert!(events(&actions).is_empty(), "{actions:?}");
    assert_eq!(machine.state(), State::Connecting);
    let delay = reconnect_delay(&actions).expect("a reconnect delay");
    assert!(delay >= Duration::from_millis(500), "{delay:?}");
    assert_eq!(machine.statistics().failed_attempts, 0);
    // The round is unchanged: websocket again, not http_stream.
    let next = machine.handle(Input::TimerFired(Timer::Reconnect));
    assert_eq!(
        opened(&next).map(|(_, kind)| kind),
        Some(TransportKind::WebSocket)
    );
}

#[test]
fn offline_while_connecting_drops_a_connect_token_fetch() {
    let mut machine = Machine::with_token_source(Config::default());
    let fetch = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::FetchToken(id) => Some(*id),
            _ => None,
        })
        .expect("a token fetch");

    let actions = machine.handle(Input::Offline);

    assert!(events(&actions).is_empty(), "{actions:?}");
    assert!(reconnect_delay(&actions).is_some(), "{actions:?}");
    let late = machine.handle(Input::TokenFetched {
        fetch,
        result: Ok("t".into()),
    });
    assert_eq!(opened(&late), None, "{late:?}");
}

#[test]
fn offline_while_waiting_the_reconnect_delay_changes_nothing() {
    let (mut machine, _) = connected(Config::default());
    machine.handle(Input::Offline);

    assert_eq!(machine.handle(Input::Offline), []);
    assert_eq!(machine.handle(Input::PageCached), []);
}

#[test]
fn offline_while_disconnected_does_nothing() {
    let mut machine = Machine::new(Config::default());

    assert_eq!(machine.handle(Input::Offline), []);
    assert_eq!(machine.handle(Input::PageCached), []);
    assert_eq!(machine.state(), State::Disconnected);
}

#[test]
fn online_after_going_offline_reconnects_at_once_without_a_jitter_window() {
    let (mut machine, _) = connected(config(20_000, 0));
    machine.handle(Input::Offline);

    let actions = machine.handle(Input::Reconnect);

    assert!(opened(&actions).is_some(), "{actions:?}");
    assert!(events(&actions).is_empty(), "{actions:?}");
}

#[test]
fn online_after_going_offline_keeps_the_jitter_window() {
    let delays: Vec<Duration> = (0..100)
        .map(|seed| {
            let (machine, _) = connected(config(20_000, 1000));
            let mut machine = machine.with_seed(seed);
            machine.handle(Input::Offline);
            let actions = machine.handle(Input::Reconnect);
            // A draw of 0 starts the attempt at once.
            if opened(&actions).is_some() {
                Duration::ZERO
            } else {
                reconnect_delay(&actions).expect("a jitter delay")
            }
        })
        .collect();

    assert!(
        delays
            .iter()
            .all(|delay| *delay <= Duration::from_millis(1000))
    );
    assert!(delays.iter().any(|delay| *delay != delays[0]));
}

#[test]
fn online_once_the_attempt_after_a_loss_started_reconnects_at_once() {
    let (mut machine, _) = connected(config(500, 100_000));
    machine.handle(Input::Offline);
    machine.handle(Input::TimerFired(Timer::Reconnect));

    let actions = machine.handle(Input::Reconnect);

    assert!(opened(&actions).is_some(), "{actions:?}");
}
