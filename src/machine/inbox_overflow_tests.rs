//! Block 44, D7: a browser transport's inbox overflowing, on the core (@E).

use super::*;
use crate::SubscriptionState;

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

fn opened(actions: &[Action]) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("an attempt")
}

/// Opens the transport `actions` start and answers its connect command:
/// the transport, and the commands sent on it.
fn connect(machine: &mut Machine, actions: &[Action]) -> (TransportId, Vec<String>) {
    let transport = opened(actions);
    let mut commands = sent(actions);
    commands.extend(sent(&machine.handle(Input::TransportOpened(transport))));
    let id = commands
        .iter()
        .filter_map(|command| serde_json::from_str::<serde_json::Value>(command).ok())
        .find(|command| command.get("connect").is_some())
        .and_then(|command| command["id"].as_u64())
        .expect("a connect command");
    commands.extend(sent(&machine.handle(frame(
        transport,
        &format!(r#"{{"id":{id},"connect":{{"client":"c","ping":25,"pong":true}}}}"#),
    ))));
    (transport, commands)
}

/// The commands `actions` send, one JSON text each.
fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
            _ => None,
        })
        .flat_map(|frame| frame.lines().map(str::to_owned).collect::<Vec<_>>())
        .collect()
}

/// A connected machine whose subscription to recoverable "news" is
/// subscribed, its stream open and read up to `subscribed`.
fn subscribed() -> (Machine, TransportId, SubscriptionId) {
    let mut machine = Machine::new(Config::default());
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let actions = machine.handle(Input::Connect);
    let (transport, _) = connect(&mut machine, &actions);
    machine.handle(Input::Subscribe(id));
    let replied = machine.handle(frame(
        transport,
        r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":0}}"#,
    ));
    for action in replied {
        if let Action::EmitSubscription(event, ..) | Action::Emit(event, _) = action {
            machine.handle(Input::EventTaken(event));
        }
    }
    (machine, transport, id)
}

fn publication_ids(actions: &[Action]) -> Vec<EventId> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSubscription(event, _, SubscriptionEvent::Publication(_)) => Some(*event),
            _ => None,
        })
        .collect()
}

fn reconnect_delay(actions: &[Action]) -> Duration {
    actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .expect("a reconnect timer")
}

#[test]
fn an_overflow_reconnects_with_code_7_and_withdraws_the_events_not_taken() {
    let (mut machine, transport, id) = subscribed();
    let publication = publication_ids(&machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":{},"offset":1}}}"#,
    )))[0];

    let overflowed = machine.handle(Input::InboxOverflow(transport));

    assert_eq!(connecting(&overflowed), Some((7, "read-ahead overflow")));
    assert_eq!(codes::connecting::READ_AHEAD_OVERFLOW, 7);
    assert!(
        overflowed.contains(&Action::Withdraw(vec![publication])),
        "{overflowed:?}"
    );
    assert!(
        overflowed.contains(&Action::CloseTransport(transport)),
        "{overflowed:?}"
    );
    assert_eq!(machine.state(), State::Connecting);
    // The position did not move: the next subscribe recovers from offset 0.
    let reconnecting = machine.handle(Input::TimerFired(Timer::Reconnect));
    let (_, commands) = connect(&mut machine, &reconnecting);
    let subscribe = commands
        .into_iter()
        .find(|frame| frame.contains(r#""subscribe""#))
        .expect("a subscribe command");
    assert!(
        subscribe.contains(r#""recover":true"#) && subscribe.contains(r#""epoch":"e""#),
        "{subscribe}"
    );
    assert!(!subscribe.contains(r#""offset":1"#), "{subscribe}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribing)
    );
}

/// The code and reason of the `connecting` event among `actions`.
fn connecting(actions: &[Action]) -> Option<(u32, &str)> {
    actions.iter().find_map(|action| match action {
        Action::Emit(_, ClientEvent::Connecting { code, reason }) => Some((*code, reason.as_str())),
        _ => None,
    })
}

#[test]
fn an_overflow_is_handled_before_the_messages_held() {
    let (mut machine, transport, _) = subscribed();
    // Not taken: the next message waits in the read-ahead.
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":{},"offset":1}}}"#,
    ));
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":{},"offset":2}}}"#,
    ));
    assert_eq!(machine.read_ahead().0, 1);

    let overflowed = machine.handle(Input::InboxOverflow(transport));

    assert_eq!(connecting(&overflowed), Some((7, "read-ahead overflow")));
    assert_eq!(machine.read_ahead().0, 0);
}

#[test]
fn an_overflow_of_a_transport_abandoned_does_nothing() {
    let (mut machine, transport, _) = subscribed();
    machine.handle(Input::Reconnect);

    assert_eq!(machine.handle(Input::InboxOverflow(transport)), vec![]);
}

/// The reconnect delay after `connections` connections, each ending by an
/// overflow after its connect reply, the last one after a server ping when
/// `ping`, for the generator seeded with `seed`.
fn last_delay(seed: u64, connections: usize, ping: bool) -> Duration {
    let mut machine = Machine::new(Config::default());
    machine.rng = fastrand::Rng::with_seed(seed);
    let mut actions = machine.handle(Input::Connect);
    let mut delay = None;
    for connection in 0..connections {
        let (transport, _) = connect(&mut machine, &actions);
        if ping && connection + 1 == connections {
            machine.handle(frame(transport, "{}"));
        }
        let overflowed = machine.handle(Input::InboxOverflow(transport));
        delay = Some(reconnect_delay(&overflowed));
        actions = machine.handle(Input::TimerFired(Timer::Reconnect));
    }
    delay.expect("a connection")
}

// As after a 3008 close (Block 15, "Backoff after 3008"): without a server
// ping the backoff keeps growing, past attempt 0's bound of 1000 ms; a ping
// read lets the connect reply's reset stand.
#[test]
fn an_overflow_before_a_server_ping_keeps_the_backoff_growing() {
    let bound = Duration::from_millis(1000);
    let grown = (0..50).map(|seed| last_delay(seed, 6, false)).max();
    let reset = (0..50).map(|seed| last_delay(seed, 6, true)).max();

    assert!(grown > Some(bound), "{grown:?}");
    assert!(reset <= Some(bound), "{reset:?}");
}
