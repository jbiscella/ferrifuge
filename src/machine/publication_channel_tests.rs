//! Block 35c: a publication's own channel and recovered publications, on
//! the core.

use super::*;
use crate::{ServerSubscriptionEvent, Subscribed};

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

/// Opens a transport with `input` and answers connect command `command`
/// with `result`.
fn connect(machine: &mut Machine, input: Input, command: u32, result: &str) -> TransportId {
    let transport = opened(&machine.handle(input));
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{command},"connect":{result}}}"#),
    ));
    transport
}

fn sent(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(String::from_utf8(frame.clone()).unwrap()),
            _ => None,
        })
        .collect()
}

/// The subscribed events of `actions`, client-side and server-side.
fn subscribed(actions: &[Action]) -> Vec<Subscribed> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSubscription(_, _, SubscriptionEvent::Subscribed(subscribed))
            | Action::Emit(
                _,
                ClientEvent::ServerSubscription {
                    event: ServerSubscriptionEvent::Subscribed(subscribed),
                    ..
                },
            ) => Some(subscribed.clone()),
            _ => None,
        })
        .collect()
}

/// A machine connected, with a subscription to "news" subscribed with
/// `result`; no stream is open, so publications are taken at once.
fn subscribed_with(result: &str) -> (Machine, TransportId, SubscriptionId, Vec<Action>) {
    let mut machine = Machine::new(Config::default());
    let transport = connect(&mut machine, Input::Connect, 1, r#"{"client":"c"}"#);
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::Subscribe(id));
    let actions = machine.handle(frame(
        transport,
        &format!(r#"{{"id":2,"subscribe":{result}}}"#),
    ));
    (machine, transport, id, actions)
}

#[test]
fn a_publication_of_another_channel_moves_no_position() {
    let (mut machine, transport, _, _) =
        subscribed_with(r#"{"recoverable":true,"positioned":true,"epoch":"e","offset":0}"#);
    machine.handle(frame(
        transport,
        concat!(
            r#"{"push":{"channel":"news","pub":{"data":{},"offset":1}}}"#,
            "\n",
            r#"{"push":{"channel":"news","pub":{"data":{},"offset":5,"epoch":"x","channel":"news.sport"}}}"#,
        ),
    ));
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".into(),
    });
    let transport = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)));
    machine.handle(Input::TransportOpened(transport));

    let resubscribe =
        sent(&machine.handle(frame(transport, r#"{"id":3,"connect":{"client":"c"}}"#)));

    assert_eq!(
        resubscribe,
        [r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"e","offset":1}}"#]
    );
}

/// The publications of a subscribe reply not yet taken when the
/// connection ends leave the position at their baseline (Block 15).
#[test]
fn a_reply_publication_of_another_channel_is_not_the_baseline() {
    let mut machine = Machine::new(Config::default());
    let transport = connect(&mut machine, Input::Connect, 1, r#"{"client":"c"}"#);
    let id = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    machine.handle(Input::Subscribe(id));
    let replied = machine.handle(frame(
        transport,
        concat!(
            r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":3,"publications":["#,
            r#"{"data":{},"offset":9,"channel":"news.sport"},{"data":{},"offset":3}]}}"#
        ),
    ));
    // Every event but the publications is taken.
    for action in replied {
        if let Action::EmitSubscription(event, _, kind) = action
            && !matches!(kind, SubscriptionEvent::Publication(_))
        {
            machine.handle(Input::EventTaken(event));
        }
    }
    machine.handle(Input::Disconnect);

    let reconnected = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(reconnected));
    let resubscribe =
        sent(&machine.handle(frame(reconnected, r#"{"id":3,"connect":{"client":"c"}}"#)));

    assert_eq!(
        resubscribe,
        [r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"e","offset":2}}"#]
    );
}

/// The reply's position is reached at the last publication of the
/// subscription's own stream, not at a later one of another channel (Codex
/// on PR #147).
#[test]
fn a_last_reply_publication_of_another_channel_does_not_reach_the_reply_position() {
    let (mut machine, transport, _, _) = subscribed_with(concat!(
        r#"{"recoverable":true,"epoch":"e","offset":10,"publications":["#,
        r#"{"data":{},"offset":7},{"data":{},"channel":"news.sport"}]}"#
    ));
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3001),
        reason: "closed by the test".into(),
    });
    let transport = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)));
    machine.handle(Input::TransportOpened(transport));

    let resubscribe = sent(&machine.handle(frame(
        transport,
        r#"{"id":3,"connect":{"client":"c"}}"#,
    )));

    assert_eq!(
        resubscribe,
        [r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"e","offset":7}}"#]
    );
}

#[test]
fn the_subscribed_event_says_whether_publications_follow() {
    let (_, _, _, with) = subscribed_with(r#"{"publications":[{"data":{},"offset":1}]}"#);
    let (_, _, _, without) = subscribed_with("{}");

    let flags: Vec<bool> = subscribed(&with)
        .into_iter()
        .chain(subscribed(&without))
        .map(|subscribed| subscribed.has_recovered_publications)
        .collect();

    assert_eq!(flags, [true, false]);
}

#[test]
fn a_server_side_subscribed_event_says_whether_publications_follow() {
    let mut machine = Machine::new(Config::default());
    let transport = opened(&machine.handle(Input::Connect));
    machine.handle(Input::TransportOpened(transport));
    let connected = machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","subs":{"alerts":{"publications":[{"data":{}}]},"quiet":{}}}}"#,
    ));
    let pushed = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"later","subscribe":{"recoverable":true}}}"#,
    ));

    let flags: Vec<bool> = subscribed(&connected)
        .into_iter()
        .chain(subscribed(&pushed))
        .map(|subscribed| subscribed.has_recovered_publications)
        .collect();

    // "alerts" then "quiet", in the registry's order, then the push.
    assert_eq!(flags, [true, false, false]);
}
