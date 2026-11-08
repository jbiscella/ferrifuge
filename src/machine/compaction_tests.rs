//! Block 35b: numeric channel ids, on the core.

use super::*;
use crate::{ServerSubscriptionEvent, SubscriptionState};

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

fn connect(machine: &mut Machine, input: Input, command: u32) -> TransportId {
    let transport = opened(&machine.handle(input));
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{command},"connect":{{"client":"c","subs":{{"alerts":{{}}}}}}}}"#),
    ));
    transport
}

/// A machine connected with a server-side subscription to "alerts", and a
/// client-side subscription to "news" with channel compaction, subscribed
/// with id 7.
fn subscribed() -> (Machine, TransportId, SubscriptionId) {
    let mut machine = Machine::new(Config::default());
    let transport = connect(&mut machine, Input::Connect, 1);
    let id = machine
        .add_subscription(
            "news".into(),
            SubscriptionOptions::default().channel_compaction(true),
        )
        .unwrap();
    machine.handle(Input::Subscribe(id));
    machine.handle(frame(transport, r#"{"id":2,"subscribe":{"id":7}}"#));
    (machine, transport, id)
}

/// The pushes `actions` deliver, by kind, and whether to a client-side
/// subscription.
fn delivered(actions: &[Action]) -> Vec<(&'static str, bool)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSubscription(_, _, SubscriptionEvent::Publication(_)) => {
                Some(("publication", true))
            }
            Action::EmitSubscription(_, _, SubscriptionEvent::Join(_)) => Some(("join", true)),
            Action::EmitSubscription(_, _, SubscriptionEvent::Leave(_)) => Some(("leave", true)),
            Action::Emit(
                _,
                ClientEvent::ServerSubscription {
                    event:
                        ServerSubscriptionEvent::Publication(_)
                        | ServerSubscriptionEvent::Join(_)
                        | ServerSubscriptionEvent::Leave(_),
                    ..
                },
            ) => Some(("server-side", false)),
            _ => None,
        })
        .collect()
}

const PUB_7: &str = r#"{"push":{"id":7,"pub":{"data":{}}}}"#;

fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8(frame.clone()).unwrap()),
            _ => None,
        })
        .collect()
}

#[test]
fn the_subscribe_command_requests_channel_compaction_beside_reject_unrecovered() {
    let mut machine = Machine::new(Config::default());
    connect(&mut machine, Input::Connect, 1);
    let options = SubscriptionOptions::default().channel_compaction(true);
    let plain = machine
        .add_subscription("a".into(), options.clone())
        .unwrap();
    let with_state = machine
        .add_subscription("b".into(), options.state_source())
        .unwrap();

    let first = sent(&machine.handle(Input::Subscribe(plain)));
    let fetch = machine
        .handle(Input::Subscribe(with_state))
        .iter()
        .find_map(|action| match action {
            Action::FetchSubscriptionState { fetch, .. } => Some(*fetch),
            _ => None,
        })
        .expect("a getState fetch");
    let second = sent(&machine.handle(Input::SubscriptionStateFetched {
        fetch,
        result: Ok(StreamPosition {
            offset: 1,
            epoch: "e".into(),
        }),
    }));

    assert_eq!(first, [r#"{"id":2,"subscribe":{"channel":"a","flag":1}}"#]);
    assert_eq!(second.len(), 1);
    assert!(second[0].contains(r#""flag":3"#), "{second:?}");
}

#[test]
fn pushes_with_the_id_of_a_subscription_reach_it() {
    let (mut machine, transport, _) = subscribed();

    let actions = machine.handle(frame(
        transport,
        concat!(
            r#"{"push":{"id":7,"pub":{"data":{}}}}"#,
            "\n",
            r#"{"push":{"id":7,"join":{"info":{"client":"c2"}}}}"#,
            "\n",
            r#"{"push":{"id":7,"leave":{"info":{"client":"c2"}}}}"#,
        ),
    ));

    assert_eq!(
        delivered(&actions),
        [("publication", true), ("join", true), ("leave", true)]
    );
}

#[test]
fn a_push_with_an_unknown_id_is_dropped_and_not_given_to_server_side_subscriptions() {
    let (mut machine, transport, _) = subscribed();

    let actions = machine.handle(frame(
        transport,
        concat!(
            r#"{"push":{"id":8,"pub":{"data":{}}}}"#,
            "\n",
            r#"{"push":{"id":8,"join":{"info":{"client":"c2"}}}}"#,
        ),
    ));

    assert_eq!(delivered(&actions), []);
}

#[test]
fn a_push_with_a_channel_is_routed_by_channel() {
    let (mut machine, transport, _) = subscribed();

    let actions = machine.handle(frame(
        transport,
        concat!(
            r#"{"push":{"id":7,"channel":"alerts","pub":{"data":{}}}}"#,
            "\n",
            r#"{"push":{"channel":"news","pub":{"data":{}}}}"#,
        ),
    ));

    assert_eq!(
        delivered(&actions),
        [("server-side", false), ("publication", true)]
    );
}

#[test]
fn an_id_is_dropped_once_its_subscription_unsubscribes() {
    let (mut machine, transport, id) = subscribed();
    machine.handle(Input::Unsubscribe(id));
    machine.handle(frame(transport, r#"{"id":3,"unsubscribe":{}}"#));
    machine.handle(Input::Subscribe(id));
    // Subscribed again, the server giving no id this time.
    machine.handle(frame(transport, r#"{"id":4,"subscribe":{}}"#));

    let actions = machine.handle(frame(transport, PUB_7));

    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
    assert_eq!(delivered(&actions), []);
}

#[test]
fn an_id_is_dropped_once_its_subscription_resubscribes_with_another() {
    let (mut machine, transport, _) = subscribed();
    // A resubscribe-advised unsubscribe, then a new subscribe reply.
    let resubscribe = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"r"}}}"#,
    ));
    assert_eq!(
        sent(&resubscribe),
        [r#"{"id":3,"subscribe":{"channel":"news","flag":1}}"#]
    );
    machine.handle(frame(transport, r#"{"id":3,"subscribe":{"id":9}}"#));

    let old = machine.handle(frame(transport, PUB_7));
    let new = machine.handle(frame(transport, r#"{"push":{"id":9,"pub":{"data":{}}}}"#));

    assert_eq!(delivered(&old), []);
    assert_eq!(delivered(&new), [("publication", true)]);
}

#[test]
fn an_id_given_to_another_subscription_names_the_new_holder() {
    let (mut machine, transport, _) = subscribed();
    let other = machine
        .add_subscription(
            "sport".into(),
            SubscriptionOptions::default().channel_compaction(true),
        )
        .unwrap();
    machine.handle(Input::Subscribe(other));
    machine.handle(frame(transport, r#"{"id":3,"subscribe":{"id":7}}"#));

    let actions = machine.handle(frame(transport, PUB_7));

    assert!(
        actions.iter().any(|action| matches!(
            action,
            Action::EmitSubscription(_, to, SubscriptionEvent::Publication(_)) if *to == other
        )),
        "{actions:?}"
    );
}

#[test]
fn ids_are_forgotten_when_the_connection_ends() {
    let (mut machine, transport, id) = subscribed();
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".into(),
    });
    let transport = connect(&mut machine, Input::TimerFired(Timer::Reconnect), 3);
    // The resubscribe, answered without an id.
    machine.handle(frame(transport, r#"{"id":4,"subscribe":{}}"#));

    let actions = machine.handle(frame(transport, PUB_7));

    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
    assert_eq!(delivered(&actions), []);
}

#[test]
fn a_removed_subscription_leaves_no_id_behind() {
    let (mut machine, transport, id) = subscribed();

    machine.handle(Input::RemoveSubscription(id));

    assert!(machine.subscriptions_by_compact_id_is_empty());
    assert_eq!(delivered(&machine.handle(frame(transport, PUB_7))), []);
}
