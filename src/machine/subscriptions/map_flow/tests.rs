//! Block 45: map subscriptions on the core (@E).

use std::num::NonZeroUsize;

use serde_json::Value;

use super::*;
use crate::Config;
use crate::machine::{EventId, Input, SubscriptionOptions, SubscriptionTimer, TransportId};

/// A machine connected over a transport.
fn connected() -> (Machine, TransportId) {
    let mut machine = Machine::new(Config::default());
    let transport = opened(&machine.handle(Input::Connect)).expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    (machine, transport)
}

fn opened(actions: &[Action]) -> Option<TransportId> {
    actions.iter().find_map(|action| match action {
        Action::OpenTransport { transport, .. } => Some(*transport),
        _ => None,
    })
}

fn frame(transport: TransportId, json: &str) -> Input {
    Input::Frame {
        transport,
        data: json.as_bytes().to_vec(),
    }
}

fn options(page_size: u32) -> MapSubscriptionOptions {
    let mut options = MapSubscriptionOptions::new(MapKind::Map);
    options.page_size = page_size;
    options
}

/// The commands sent among `actions`, decoded.
fn sent(actions: &[Action]) -> Vec<Value> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(frame),
            _ => None,
        })
        .flat_map(|frame| {
            std::str::from_utf8(frame)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The one subscribe command sent among `actions`.
fn subscribe_sent(actions: &[Action]) -> Value {
    let mut subscribes = sent(actions)
        .into_iter()
        .filter(|command| command.get("subscribe").is_some());
    let command = subscribes.next().expect("a subscribe command");
    assert!(subscribes.next().is_none(), "{actions:?}");
    command
}

fn unsubscribes(actions: &[Action]) -> usize {
    sent(actions)
        .iter()
        .filter(|command| command.get("unsubscribe").is_some())
        .count()
}

/// The map subscription events among `actions`, with their ids.
fn events(actions: &[Action]) -> Vec<(EventId, MapSubscriptionEvent)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitMapSubscription(event_id, _, event) => Some((*event_id, event.clone())),
            _ => None,
        })
        .collect()
}

fn names(actions: &[Action]) -> Vec<&'static str> {
    events(actions)
        .iter()
        .map(|(_, event)| event.name())
        .collect()
}

fn sync_keys(actions: &[Action]) -> Vec<String> {
    events(actions)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Sync(entries) => {
                Some(entries.into_iter().map(|entry| entry.key).collect())
            }
            _ => None,
        })
        .expect("a sync")
}

fn id_of(command: &Value) -> u64 {
    command["id"].as_u64().unwrap()
}

/// A JSON entry or publication with `key`, `data` and `offset`.
fn entry(key: &str, data: u32, offset: u64) -> String {
    format!(r#"{{"key":"{key}","data":{{"v":{data}}},"offset":{offset}}}"#)
}

fn removal(key: &str, offset: u64) -> String {
    format!(r#"{{"key":"{key}","removed":true,"offset":{offset}}}"#)
}

/// A map subscription to "scores" subscribed on a connected machine: the
/// machine, its transport, the subscription and its first command.
fn subscribing(page_size: u32) -> (Machine, TransportId, SubscriptionId, Value) {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), options(page_size))
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    (machine, transport, id, first)
}

#[test]
fn the_first_request_asks_for_the_first_state_page() {
    let (_, _, _, first) = subscribing(2);

    let subscribe = &first["subscribe"];
    assert_eq!(subscribe["type"], 1);
    assert_eq!(subscribe["phase"], 2);
    assert_eq!(subscribe["limit"], 2);
    for absent in [
        "cursor",
        "epoch",
        "offset",
        "recover",
        "positioned",
        "recoverable",
    ] {
        assert!(subscribe.get(absent).is_none(), "{first}");
    }
}

#[test]
fn a_page_size_of_0_sends_no_limit() {
    let (_, _, _, first) = subscribing(0);

    assert!(first["subscribe"].get("limit").is_none(), "{first}");
}

#[test]
fn the_kinds_are_sent_as_their_types() {
    for (kind, wire) in [
        (MapKind::Map, 1),
        (MapKind::Clients, 2),
        (MapKind::Users, 3),
    ] {
        let (mut machine, _) = connected();
        let id = machine
            .add_map_subscription("m".into(), MapSubscriptionOptions::new(kind))
            .unwrap();

        let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

        assert_eq!(first["subscribe"]["type"], wire);
    }
}

#[test]
fn a_load_pages_the_state_then_the_stream_then_syncs() {
    let (mut machine, transport, id, first) = subscribing(2);
    machine.handle(Input::StreamOpened(Some(id)));

    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5,"state":[{},{}]}}}}"#,
            id_of(&first),
            entry("a", 1, 1),
            entry("b", 1, 2)
        ),
    )));
    let third = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"epoch":"e1","offset":5,"state":[{}]}}}}"#,
            id_of(&second),
            entry("c", 1, 3)
        ),
    )));
    let fourth = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":1,"epoch":"e1","offset":7,"publications":[{},{}]}}}}"#,
            id_of(&third),
            entry("a", 2, 6),
            removal("b", 7)
        ),
    )));
    let live = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"recoverable":true,"epoch":"e1","offset":9,"publications":[{},{}]}}}}"#,
            id_of(&fourth),
            entry("d", 1, 8),
            entry("b", 2, 9)
        ),
    ));

    let second = &second["subscribe"];
    assert_eq!(
        (
            &second["phase"],
            &second["cursor"],
            &second["epoch"],
            &second["offset"],
            &second["limit"]
        ),
        (
            &Value::from(2),
            &Value::from("c1"),
            &Value::from("e1"),
            &Value::from(5),
            &Value::from(2)
        )
    );
    let third = &third["subscribe"];
    assert_eq!(
        (&third["phase"], &third["epoch"], &third["offset"]),
        (&Value::from(1), &Value::from("e1"), &Value::from(5))
    );
    assert!(third.get("cursor").is_none());
    assert_eq!(fourth["subscribe"]["offset"], 7);
    assert_eq!(names(&live), ["subscribed", "sync"]);
    // The last value wins, a removal deletes, a key set again goes last.
    assert_eq!(sync_keys(&live), ["a", "c", "d", "b"]);
    let entries = events(&live)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Sync(entries) => Some(entries),
            _ => None,
        })
        .unwrap();
    assert_eq!(entries[0].data, br#"{"v":2}"#);
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
}

#[test]
fn every_request_carries_the_token_and_only_the_first_the_data() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.common.token = "t1".into();
    options.common.data = Some(br#"{"room":1}"#.to_vec());
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    )));

    assert_eq!(first["subscribe"]["token"], "t1");
    assert_eq!(first["subscribe"]["data"], serde_json::json!({"room":1}));
    assert_eq!(second["subscribe"]["token"], "t1");
    assert!(second["subscribe"].get("data").is_none(), "{second}");
}

#[test]
fn each_request_has_the_reply_deadline_of_a_subscribe() {
    let (mut machine, transport, id, first) = subscribing(2);

    let actions = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    ));

    assert!(
        actions.contains(&Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
            Config::default().timeout
        )),
        "{actions:?}"
    );
}

#[test]
fn an_epoch_change_during_the_load_unsubscribes_then_loads_again() {
    let (mut machine, transport, _, first) = subscribing(2);
    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5,"state":[{}]}}}}"#,
            id_of(&first),
            entry("a", 1, 1)
        ),
    )));

    let changed = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"epoch":"e2","offset":1,"state":[{}]}}}}"#,
            id_of(&second),
            entry("b", 1, 1)
        ),
    ));
    let unsubscribe = sent(&changed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    let again = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    )));

    assert!(
        sent(&changed)
            .iter()
            .all(|command| command.get("subscribe").is_none())
    );
    let again = &again["subscribe"];
    assert_eq!(again["phase"], 2);
    for absent in ["cursor", "epoch", "offset"] {
        assert!(again.get(absent).is_none(), "{again}");
    }
}

#[test]
fn a_reply_without_the_map_type_ends_the_subscription_with_code_6() {
    let (mut machine, transport, id, first) = subscribing(2);
    machine.handle(Input::StreamOpened(Some(id)));

    let actions = machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"subscribe":{{}}}}"#, id_of(&first)),
    ));

    assert_eq!(unsubscribes(&actions), 1);
    assert_eq!(
        events(&actions)
            .into_iter()
            .map(|(_, event)| event)
            .collect::<Vec<_>>(),
        [MapSubscriptionEvent::Unsubscribed {
            code: codes::unsubscribed::MAP_NOT_SUPPORTED,
            reason: "map subscriptions not supported".into()
        }]
    );
    assert!(!actions.iter().any(|action| matches!(
        action,
        Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, _)
    )));
}

/// The answer `reply`, to the first request of a new load, makes the
/// client reconnect with an anomaly reported.
fn faults(reply: &str) {
    let (mut machine, transport, _, first) = subscribing(2);

    let actions = machine.handle(frame(
        transport,
        &reply.replace("ID", &id_of(&first).to_string()),
    ));

    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Anomaly(Anomaly::UnexpectedMapReply { .. }))),
        "{reply}: {actions:?}"
    );
    assert_eq!(machine.state(), State::Connecting, "{reply}");
}

#[test]
fn a_phase_the_request_does_not_allow_is_a_protocol_fault() {
    faults(r#"{"id":ID,"subscribe":{"type":1,"phase":1,"epoch":"e1","offset":5}}"#);
    faults(r#"{"id":ID,"subscribe":{"type":1,"phase":3,"epoch":"e1","offset":5}}"#);
}

#[test]
fn an_entry_without_a_key_is_a_protocol_fault() {
    faults(
        r#"{"id":ID,"subscribe":{"type":1,"phase":2,"cursor":"c1","epoch":"e1","state":[{"data":{}}]}}"#,
    );
}

#[test]
fn a_state_page_repeating_its_cursor_is_a_protocol_fault() {
    let (mut machine, transport, _, first) = subscribing(2);
    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    )));

    let actions = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&second)
        ),
    ));

    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Anomaly(Anomaly::UnexpectedMapReply { .. })))
    );
}

#[test]
fn a_stream_request_answered_with_a_state_page_is_a_protocol_fault() {
    let (mut machine, transport, _, first) = subscribing(2);
    let stream = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    )));
    assert_eq!(stream["subscribe"]["phase"], 1);

    let actions = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"x","epoch":"e1","offset":5}}}}"#,
            id_of(&stream)
        ),
    ));

    assert_eq!(machine.state(), State::Connecting, "{actions:?}");
}

#[test]
fn pages_past_the_bound_end_the_subscription_with_code_5() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.max_state_bytes = NonZeroUsize::new(300).unwrap();
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let page = |id: u64, cursor: &str| {
        format!(
            r#"{{"id":{id},"subscribe":{{"type":1,"phase":2,"cursor":"{cursor}","epoch":"e1","offset":5,"state":[{{"key":"k{cursor}","data":"{}"}}]}}}}"#,
            "x".repeat(150)
        )
    };
    let second = subscribe_sent(&machine.handle(frame(transport, &page(id_of(&first), "c1"))));

    let ended = machine.handle(frame(transport, &page(id_of(&second), "c2")));

    assert_eq!(unsubscribes(&ended), 1);
    assert!(
        sent(&ended)
            .iter()
            .all(|command| command.get("subscribe").is_none())
    );
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
    assert!(
        events(&ended)
            .iter()
            .any(|(_, event)| matches!(event, MapSubscriptionEvent::Unsubscribed { code: 5, .. }))
    );
}

// Codex, #198: a live reply's state goes into the same `Sync`.
#[test]
fn a_live_reply_past_the_bound_ends_the_subscription_with_code_5() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.max_state_bytes = NonZeroUsize::new(300).unwrap();
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let ended = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":0,"epoch":"e1","offset":5,"state":[{{"key":"k","data":"{}"}}]}}}}"#,
            id_of(&first),
            "x".repeat(400)
        ),
    ));

    assert_eq!(unsubscribes(&ended), 1);
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
    let events = events(&ended);
    assert!(
        events
            .iter()
            .any(|(_, event)| matches!(event, MapSubscriptionEvent::Unsubscribed { code: 5, .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|(_, event)| matches!(event, MapSubscriptionEvent::Sync(..))),
        "{events:?}"
    );
}

#[test]
fn a_page_is_handled_while_another_subscriptions_publication_is_held() {
    let (mut machine, transport) = connected();
    let news = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(news)));
    let subscribe = subscribe_sent(&machine.handle(Input::Subscribe(news)));
    for action in machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"subscribe":{{}}}}"#, id_of(&subscribe)),
    )) {
        if let Action::EmitSubscription(event, ..) = action {
            machine.handle(Input::EventTaken(event));
        }
    }
    // A publication not taken holds the messages read after it.
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":{}}}}"#,
    ));
    let id = machine
        .add_map_subscription("scores".into(), options(2))
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let paged = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    ));
    let second = subscribe_sent(&paged);
    let live = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"epoch":"e1","offset":5}}}}"#,
            id_of(&second)
        ),
    ));

    assert_eq!(second["subscribe"]["cursor"], "c1");
    // The live reply makes events: it waits its turn.
    assert!(events(&live).is_empty(), "{live:?}");
}

// Codex, #198: the last state page has no cursor, like the first request.
#[test]
fn a_single_cursorless_state_page_is_handled_while_another_subscriptions_publication_is_held() {
    let (mut machine, transport) = connected();
    let news = machine
        .add_subscription("news".into(), SubscriptionOptions::default())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(news)));
    let subscribe = subscribe_sent(&machine.handle(Input::Subscribe(news)));
    for action in machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"subscribe":{{}}}}"#, id_of(&subscribe)),
    )) {
        if let Action::EmitSubscription(event, ..) = action {
            machine.handle(Input::EventTaken(event));
        }
    }
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"news","pub":{"data":{}}}}"#,
    ));
    let id = machine
        .add_map_subscription("scores".into(), options(2))
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let paged = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"epoch":"e1","offset":5,"state":[{}]}}}}"#,
            id_of(&first),
            entry("a", 1, 4)
        ),
    ));

    let stream = subscribe_sent(&paged);
    assert_eq!(stream["subscribe"]["phase"], 1, "{paged:?}");
}

// Codex, #198: an error reply to an abandoned page request still leaves the
// server's reservation, which an unsubscribe undoes.
#[test]
fn an_error_to_a_page_request_abandoned_by_unsubscribe_is_followed_by_an_unsubscribe() {
    let (mut machine, transport, id, first) = subscribing(2);
    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    )));
    let unsubscribed = machine.handle(Input::Unsubscribe(id));
    assert_eq!(unsubscribes(&unsubscribed), 0, "{unsubscribed:?}");

    let answered = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"error":{{"code":113,"message":"concurrent pagination","temporary":true}}}}"#,
            id_of(&second)
        ),
    ));

    assert_eq!(unsubscribes(&answered), 1, "{answered:?}");
}

#[test]
fn a_page_read_before_the_connect_reply_is_handled_right_after_it() {
    let mut machine = Machine::new(Config::default());
    machine.handle(Input::StreamOpened(None));
    let id = machine
        .add_map_subscription("scores".into(), options(2))
        .unwrap();
    machine.handle(Input::Subscribe(id));
    let transport = opened(&machine.handle(Input::Connect)).unwrap();
    // The first request goes with the connect command (Block 36).
    let opened = machine.handle(Input::TransportOpened(transport));
    let first = sent(&opened)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("the first request with the connect command");

    // The client's stream holds the connected event: the page goes ahead.
    let actions = machine.handle(frame(
        transport,
        &format!(
            "{}\n{}",
            r#"{"id":1,"connect":{"client":"c"}}"#,
            format_args!(
                r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
                id_of(&first)
            )
        ),
    ));

    assert_eq!(subscribe_sent(&actions)["subscribe"]["cursor"], "c1");
}

/// A map subscription synced at epoch "e1" and offset 7 with a stream
/// taking its events: the machine, its transport and the subscription.
fn synced() -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport, id, first) = subscribing(2);
    machine.handle(Input::StreamOpened(Some(id)));
    let live = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"recoverable":true,"epoch":"e1","offset":7,"state":[{}]}}}}"#,
            id_of(&first),
            entry("a", 1, 7)
        ),
    ));
    for (event, _) in events(&live) {
        machine.handle(Input::EventTaken(event));
    }
    (machine, transport, id)
}

#[test]
fn a_sync_taken_sets_the_position_and_updates_move_it_once_taken() {
    let (mut machine, transport, id) = synced();
    assert_eq!(
        machine.subscription_position(id),
        Some(StreamPosition {
            offset: 7,
            epoch: "e1".into()
        })
    );

    let pushed = machine.handle(frame(
        transport,
        &format!(
            r#"{{"push":{{"channel":"scores","pub":{}}}}}"#,
            entry("b", 1, 8)
        ),
    ));
    let (update, event) = events(&pushed).pop().expect("an update");
    let delivered = machine.statistics().publications_delivered;

    assert_eq!(event.name(), "update");
    assert_eq!(machine.subscription_position(id).unwrap().offset, 7);
    machine.handle(Input::EventTaken(update));
    assert_eq!(machine.subscription_position(id).unwrap().offset, 8);
    assert_eq!(machine.statistics().publications_delivered, delivered + 1);
}

#[test]
fn a_removal_is_an_update_without_data() {
    let (mut machine, transport, _) = synced();

    let pushed = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"scores","pub":{"key":"a","removed":true,"data":"","offset":8}}}"#,
    ));

    let Some((_, MapSubscriptionEvent::Update(update))) = events(&pushed).pop() else {
        panic!("{pushed:?}");
    };
    assert!(update.removed);
    assert_eq!(update.data, b"");
}

#[test]
fn a_sync_counts_as_no_publication_delivered() {
    let (machine, _, _) = synced();

    assert_eq!(machine.statistics().publications_delivered, 0);
}

#[test]
fn a_sync_not_taken_when_the_subscription_leaves_subscribed_is_withdrawn() {
    let (mut machine, transport, id, first) = subscribing(2);
    machine.handle(Input::StreamOpened(Some(id)));
    let live = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"recoverable":true,"epoch":"e1","offset":7}}}}"#,
            id_of(&first)
        ),
    ));
    let sync = events(&live)
        .into_iter()
        .find(|(_, event)| event.name() == "sync")
        .unwrap()
        .0;

    let actions = machine.handle(Input::Unsubscribe(id));

    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Withdraw(ids) if ids.contains(&sync))),
        "{actions:?}"
    );
    assert_eq!(machine.subscription_position(id), None);
}

#[test]
fn unsubscribing_during_a_load_sends_the_unsubscribe_once_the_page_is_read() {
    let (mut machine, transport, id, first) = subscribing(2);

    let unsubscribed = machine.handle(Input::Unsubscribe(id));
    let answered = machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    ));

    assert_eq!(unsubscribes(&unsubscribed), 0);
    assert_eq!(unsubscribes(&answered), 1);
    assert!(
        sent(&answered)
            .iter()
            .all(|command| command.get("subscribe").is_none())
    );
}

#[test]
fn a_map_subscription_cannot_share_a_channel_with_a_stream_subscription() {
    let mut machine = Machine::new(Config::default());
    machine
        .add_subscription("a".into(), SubscriptionOptions::default())
        .unwrap();
    machine
        .add_map_subscription("b".into(), options(0))
        .unwrap();

    assert_eq!(
        machine.add_map_subscription("a".into(), options(0)),
        Err(SubscriptionError::AlreadyExists {
            channel: "a".into()
        })
    );
    assert_eq!(
        machine.add_subscription("b".into(), SubscriptionOptions::default()),
        Err(SubscriptionError::AlreadyExists {
            channel: "b".into()
        })
    );
}

#[test]
fn a_lost_connection_ends_the_load_and_the_next_one_starts_from_scratch() {
    let (mut machine, transport, _, first) = subscribing(2);
    machine.handle(frame(
        transport,
        &format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5}}}}"#,
            id_of(&first)
        ),
    ));
    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let transport =
        opened(&machine.handle(Input::TimerFired(crate::machine::Timer::Reconnect))).unwrap();

    let opened_again = machine.handle(Input::TransportOpened(transport));

    let again = sent(&opened_again)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe with the connect command");
    assert_eq!(again["subscribe"]["phase"], 2);
    assert!(again["subscribe"].get("cursor").is_none());
}

// Recovery and resync (Block 45c, D6).

/// Closes `transport` and opens the next, answering its connect: the
/// transport, and the subscribe command sent on it, if any.
fn reconnect(machine: &mut Machine, transport: TransportId) -> (TransportId, Option<Value>) {
    machine.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    let transport =
        opened(&machine.handle(Input::TimerFired(crate::machine::Timer::Reconnect))).unwrap();
    let mut actions = machine.handle(Input::TransportOpened(transport));
    let connect = sent(&actions)
        .into_iter()
        .find(|command| command.get("connect").is_some())
        .expect("a connect command");
    actions.extend(machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"connect":{{"client":"c"}}}}"#, id_of(&connect)),
    )));
    let subscribe = sent(&actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some());
    (transport, subscribe)
}

fn subscribe_reply(command: &Value, fields: &str) -> String {
    format!(r#"{{"id":{},"subscribe":{{{fields}}}}}"#, id_of(command))
}

fn error_reply(command: &Value, code: u32, temporary: bool) -> String {
    format!(
        r#"{{"id":{},"error":{{"code":{code},"message":"m","temporary":{temporary}}}}}"#,
        id_of(command)
    )
}

#[test]
fn a_subscribe_after_a_taken_sync_recovers_in_the_stream_phase() {
    let (mut machine, transport, _) = synced();

    let (_, subscribe) = reconnect(&mut machine, transport);

    let request = &subscribe.expect("a subscribe")["subscribe"];
    assert_eq!(
        (
            &request["phase"],
            &request["recover"],
            &request["epoch"],
            &request["offset"]
        ),
        (
            &Value::from(1),
            &Value::from(true),
            &Value::from("e1"),
            &Value::from(7)
        ),
        "{request}"
    );
}

#[test]
fn a_recovered_load_emits_its_catch_up_as_updates_moving_the_position_once_taken() {
    let (mut machine, transport, id) = synced();
    let (transport, subscribe) = reconnect(&mut machine, transport);
    let subscribe = subscribe.unwrap();
    // A stream page first, then the live reply with the rest; the reply's
    // offset lies past the last update (a tag filter skipped one).
    let page = machine.handle(frame(
        transport,
        &subscribe_reply(
            &subscribe,
            &format!(
                r#""type":1,"phase":1,"epoch":"e1","offset":8,"publications":[{}]"#,
                entry("b", 1, 8)
            ),
        ),
    ));
    let next = subscribe_sent(&page);
    assert_eq!(next["subscribe"]["recover"], true);
    assert_eq!(next["subscribe"]["offset"], 8);
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &next,
            &format!(
                r#""type":1,"recoverable":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":10,"publications":[{}]"#,
                removal("a", 9)
            ),
        ),
    ));

    let emitted = events(&live);
    let names: Vec<_> = emitted.iter().map(|(_, event)| event.name()).collect();
    assert_eq!(names, ["subscribed", "update", "update"]);
    let MapSubscriptionEvent::Subscribed(subscribed) = &emitted[0].1 else {
        unreachable!();
    };
    assert!(subscribed.has_recovered_publications);
    // Before the catch-up until its updates are taken.
    assert_eq!(machine.subscription_position(id).unwrap().offset, 7);
    machine.handle(Input::EventTaken(emitted[1].0));
    assert_eq!(machine.subscription_position(id).unwrap().offset, 8);
    machine.handle(Input::EventTaken(emitted[2].0));
    assert_eq!(machine.subscription_position(id).unwrap().offset, 10);
}

#[test]
fn a_recovery_answered_live_without_catch_up_sets_the_reply_position() {
    let (mut machine, transport, id) = synced();
    let (transport, subscribe) = reconnect(&mut machine, transport);

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &subscribe.unwrap(),
            r#""type":1,"recoverable":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":9"#,
        ),
    ));

    assert_eq!(names(&live), ["subscribed"]);
    assert_eq!(machine.subscription_position(id).unwrap().offset, 9);
}

#[test]
fn a_recovery_not_completed_syncs_and_keeps_no_position_until_taken() {
    let (mut machine, transport, id) = synced();
    let (transport, subscribe) = reconnect(&mut machine, transport);

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &subscribe.unwrap(),
            &format!(
                r#""type":1,"recoverable":true,"was_recovering":true,"epoch":"e1","offset":9,"publications":[{}]"#,
                entry("b", 1, 8)
            ),
        ),
    ));

    assert_eq!(names(&live), ["subscribed", "sync"]);
    assert_eq!(sync_keys(&live), ["b"]);
    assert_eq!(machine.subscription_position(id), None);
}

#[test]
fn error_112_from_scratch_drops_the_position_and_loads_after_the_backoff() {
    let (mut machine, transport, id) = synced();
    let (transport, subscribe) = reconnect(&mut machine, transport);

    let failed = machine.handle(frame(
        transport,
        &error_reply(&subscribe.unwrap(), 112, false),
    ));

    assert!(events(&failed).is_empty(), "{failed:?}");
    assert_eq!(machine.subscription_position(id), None);
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribing)
    );
    assert!(
        failed.iter().any(|action| matches!(
            action,
            Action::SetSubscriptionTimer(of, SubscriptionTimer::Resubscribe, _) if *of == id
        )),
        "{failed:?}"
    );
    let again = machine.handle(Input::SubscriptionTimerFired(
        id,
        SubscriptionTimer::Resubscribe,
    ));
    let request = &subscribe_sent(&again)["subscribe"];
    assert_eq!(request["phase"], 2);
    assert!(request.get("epoch").is_none(), "{request}");
}

#[test]
fn error_112_with_the_fatal_strategy_unsubscribes_with_its_code() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.unrecoverable_strategy = UnrecoverableStrategy::Fatal;
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            r#""type":1,"recoverable":true,"epoch":"e1","offset":7"#,
        ),
    ));
    let (transport, subscribe) = reconnect(&mut machine, transport);

    let failed = machine.handle(frame(
        transport,
        &error_reply(&subscribe.unwrap(), 112, false),
    ));

    assert_eq!(
        events(&failed).pop().map(|(_, event)| event),
        Some(MapSubscriptionEvent::Unsubscribed {
            code: 112,
            reason: "m".into()
        })
    );
    assert!(
        !failed.iter().any(|action| matches!(
            action,
            Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, _)
        )),
        "{failed:?}"
    );
}

// Codex, #200: without this the load would recover from the old epoch again.
#[test]
fn an_epoch_change_during_a_recovering_load_loads_the_state_from_scratch() {
    let (mut machine, transport, _) = synced();
    let (transport, recovering) = reconnect(&mut machine, transport);
    let recovering = recovering.expect("a subscribe");
    assert_eq!(recovering["subscribe"]["phase"], 1, "{recovering}");

    let changed = machine.handle(frame(
        transport,
        &subscribe_reply(
            &recovering,
            r#""type":1,"phase":1,"epoch":"other","offset":1"#,
        ),
    ));
    let unsubscribe = sent(&changed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    let again = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    )));

    let again = &again["subscribe"];
    assert_eq!(again["phase"], 2, "{again}");
    for absent in ["recover", "epoch", "offset"] {
        assert!(again.get(absent).is_none(), "{again}");
    }
}

#[test]
fn an_epoch_change_with_the_fatal_strategy_unsubscribes_with_112() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.unrecoverable_strategy = UnrecoverableStrategy::Fatal;
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let page = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            r#""type":1,"phase":2,"cursor":"c1","epoch":"e1","offset":5"#,
        ),
    ));

    let changed = machine.handle(frame(
        transport,
        &subscribe_reply(
            &subscribe_sent(&page),
            r#""type":1,"phase":2,"cursor":"c2","epoch":"e2","offset":6"#,
        ),
    ));

    assert_eq!(unsubscribes(&changed), 1);
    assert_eq!(
        events(&changed).pop().map(|(_, event)| event),
        Some(MapSubscriptionEvent::Unsubscribed {
            code: 112,
            reason: "unrecoverable position".into()
        })
    );
    assert!(
        sent(&changed)
            .iter()
            .all(|command| command.get("subscribe").is_none())
    );
}

#[test]
fn unsubscribe_2502_drops_the_position_and_the_token() {
    let (mut machine, transport) = connected();
    let mut options = options(2);
    options.common = SubscriptionOptions::default().token("t0".into());
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    assert_eq!(first["subscribe"]["token"], "t0");
    machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            r#""type":1,"recoverable":true,"epoch":"e1","offset":7"#,
        ),
    ));

    let invalidated = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"scores","unsubscribe":{"code":2502,"reason":"state invalidated"}}}"#,
    ));

    let request = &subscribe_sent(&invalidated)["subscribe"];
    assert_eq!(request["phase"], 2);
    for absent in ["epoch", "recover", "token"] {
        assert!(request.get(absent).is_none(), "{request}");
    }
}

#[test]
fn disconnect_3014_drops_a_maps_position_rather_than_making_it_unmatchable() {
    let (mut machine, transport, id) = synced();

    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3014),
        reason: "state invalidated".into(),
    });

    assert_eq!(machine.subscription_position(id), None);
}

#[test]
fn setting_a_tag_filter_drops_the_position() {
    let (mut machine, transport, id) = synced();

    machine.handle(Input::SetSubscriptionTagFilter(
        id,
        Some(crate::TagFilter::eq("t", "red")),
    ));
    let (_, subscribe) = reconnect(&mut machine, transport);

    assert_eq!(machine.subscription_position(id), None);
    assert_eq!(subscribe.unwrap()["subscribe"]["phase"], 2);
}

#[test]
fn a_temporary_error_after_a_page_unsubscribes_then_recovers_after_the_backoff() {
    let (mut machine, transport, id) = synced();
    let (transport, subscribe) = reconnect(&mut machine, transport);
    let page = machine.handle(frame(
        transport,
        &subscribe_reply(
            &subscribe.unwrap(),
            &format!(
                r#""type":1,"phase":1,"epoch":"e1","offset":8,"publications":[{}]"#,
                entry("b", 1, 8)
            ),
        ),
    ));

    let failed = machine.handle(frame(
        transport,
        &error_reply(&subscribe_sent(&page), 113, true),
    ));
    let unsubscribe = sent(&failed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    assert_eq!(names(&failed), ["error"]);
    // Taken, so that the unsubscribe reply is handled (Block 15).
    for (event, _) in events(&failed) {
        machine.handle(Input::EventTaken(event));
    }
    let waiting = machine.handle(Input::SubscriptionTimerFired(
        id,
        SubscriptionTimer::Resubscribe,
    ));
    assert!(sent(&waiting).is_empty(), "{waiting:?}");
    let answered = machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    ));

    let request = &subscribe_sent(&answered)["subscribe"];
    assert_eq!(
        (&request["phase"], &request["offset"]),
        (&Value::from(1), &Value::from(7))
    );
}

#[test]
fn an_ephemeral_map_keeps_no_position() {
    let (mut machine, transport, id, first) = subscribing(2);
    machine.handle(frame(
        transport,
        &subscribe_reply(&first, r#""type":1,"epoch":"e1","offset":7"#),
    ));
    machine.handle(frame(
        transport,
        &format!(
            r#"{{"push":{{"channel":"scores","pub":{}}}}}"#,
            entry("b", 1, 8)
        ),
    ));

    let (_, subscribe) = reconnect(&mut machine, transport);

    assert_eq!(machine.subscription_position(id), None);
    assert_eq!(subscribe.unwrap()["subscribe"]["phase"], 2);
}

// --- Per-key delta (Block 45, D9) ------------------------------------------

fn value(key: &str, version: u32) -> Vec<u8> {
    format!("{}{version}", key.repeat(40)).into_bytes()
}

/// A JSON string holding `bytes`, as data travels with delta on.
fn wrapped(bytes: &[u8]) -> String {
    serde_json::to_string(std::str::from_utf8(bytes).unwrap()).unwrap()
}

fn whole(key: &str, version: u32, offset: u64) -> String {
    format!(
        r#"{{"key":"{key}","data":{},"offset":{offset}}}"#,
        wrapped(&value(key, version))
    )
}

/// The delta from `key`'s value at version `from` to version `to`.
fn delta(key: &str, from: u32, to: u32, offset: u64) -> String {
    format!(
        r#"{{"key":"{key}","delta":true,"data":{},"offset":{offset}}}"#,
        wrapped(&crate::fossil::write(&value(key, from), &value(key, to)))
    )
}

fn push(publication: &str) -> String {
    format!(r#"{{"push":{{"channel":"scores","pub":{publication}}}}}"#)
}

fn delta_options() -> MapSubscriptionOptions {
    let mut options = options(2);
    options.common = SubscriptionOptions::default().delta(crate::DeltaFormat::Fossil);
    options
}

/// The keys with a base, sorted; `None` while delta is off.
fn bases(machine: &Machine, id: SubscriptionId) -> Option<Vec<String>> {
    let map = machine.subscriptions.by_id[&id].map.as_ref().unwrap();
    map.bases.as_ref().map(|bases| {
        let mut keys: Vec<String> = bases.keys().cloned().collect();
        keys.sort();
        keys
    })
}

fn updates(actions: &[Action]) -> Vec<MapUpdate> {
    events(actions)
        .into_iter()
        .filter_map(|(_, event)| match event {
            MapSubscriptionEvent::Update(update) => Some(update),
            _ => None,
        })
        .collect()
}

fn take_all(machine: &mut Machine, actions: &[Action]) {
    for (event, _) in events(actions) {
        machine.handle(Input::EventTaken(event));
    }
}

fn delta_error(actions: &[Action]) -> Option<crate::ClientError> {
    events(actions)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Error(error) if error.kind == crate::ErrorKind::Delta => {
                Some(error)
            }
            _ => None,
        })
}

/// A map subscription with delta on, whose stream is open and whose sync
/// of "a" and "b", both at version 1, was taken at epoch "e1" and offset 2.
fn delta_synced() -> (Machine, TransportId, SubscriptionId) {
    let (machine, transport) = connected();
    delta_synced_on(machine, transport)
}

fn delta_synced_on(
    mut machine: Machine,
    transport: TransportId,
) -> (Machine, TransportId, SubscriptionId) {
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":2,"state":[{},{}]"#,
                whole("a", 1, 1),
                whole("b", 1, 2)
            ),
        ),
    ));
    take_all(&mut machine, &live);
    (machine, transport, id)
}

#[test]
fn every_request_of_a_load_asks_for_delta() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let second = subscribe_sent(&machine.handle(frame(
        transport,
        &subscribe_reply(&first, r#""type":1,"phase":2,"epoch":"e1","offset":2"#),
    )));

    assert_eq!(first["subscribe"]["delta"], "fossil", "{first}");
    assert_eq!(second["subscribe"]["delta"], "fossil", "{second}");
}

#[test]
fn delta_and_a_tag_filter_are_refused_together() {
    let (mut machine, _) = connected();
    let mut options = delta_options();
    options.common = options.common.tag_filter(crate::TagFilter::eq("t", "red"));

    assert_eq!(
        machine.add_map_subscription("scores".into(), options),
        Err(SubscriptionError::TagFilterWithDelta)
    );
}

fn page_then_live(delta: bool) -> Vec<u8> {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let stream = subscribe_sent(&machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            r#""type":1,"phase":2,"epoch":"e1","offset":1,"state":[{"key":"a","data":"{\"x\":1}","offset":1}]"#,
        ),
    )));
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &stream,
            &format!(r#""type":1,"recoverable":true,"delta":{delta},"epoch":"e1","offset":1"#),
        ),
    ));
    events(&live)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Sync(entries) => Some(entries[0].data.clone()),
            _ => None,
        })
        .expect("a sync")
}

#[test]
fn page_data_is_unwrapped_only_when_the_live_reply_says_delta_is_on() {
    assert_eq!(page_then_live(true), br#"{"x":1}"#);
    assert_eq!(page_then_live(false), br#""{\"x\":1}""#);
}

#[test]
fn a_catch_up_delta_applies_to_its_keys_state_entry_in_the_sync() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let stream = subscribe_sent(&machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"phase":2,"epoch":"e1","offset":1,"state":[{}]"#,
                whole("a", 1, 1)
            ),
        ),
    )));

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &stream,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":2,"publications":[{}]"#,
                delta("a", 1, 2, 2)
            ),
        ),
    ));

    let entries = events(&live)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Sync(entries) => Some(entries),
            _ => None,
        })
        .expect("a sync");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].data, value("a", 2));
    assert_eq!(bases(&machine, id), Some(vec!["a".to_owned()]));
}

#[test]
fn each_key_has_its_own_chain_and_the_statistics_count_full_and_delta_payloads() {
    let (mut machine, transport, id) = delta_synced();

    let first = machine.handle(frame(transport, &push(&delta("a", 1, 2, 3))));
    take_all(&mut machine, &first);
    let second = machine.handle(frame(transport, &push(&delta("b", 1, 2, 4))));
    take_all(&mut machine, &second);
    let third = machine.handle(frame(transport, &push(&delta("a", 2, 3, 5))));

    assert_eq!(updates(&first)[0].data, value("a", 2));
    assert_eq!(updates(&second)[0].data, value("b", 2));
    assert_eq!(updates(&third)[0].data, value("a", 3));
    let statistics = machine.delta_statistics(id).unwrap();
    assert_eq!(statistics.full_payloads, 2);
    assert_eq!(statistics.delta_payloads, 3);
}

#[test]
fn a_removal_deletes_its_keys_base_and_the_next_value_comes_whole() {
    let (mut machine, transport, id) = delta_synced();

    let removed = machine.handle(frame(
        transport,
        &push(r#"{"key":"a","removed":true,"data":"","offset":3}"#),
    ));
    assert!(updates(&removed)[0].removed);
    assert_eq!(bases(&machine, id), Some(vec!["b".to_owned()]));
    take_all(&mut machine, &removed);

    let set = machine.handle(frame(transport, &push(&whole("a", 5, 4))));

    assert_eq!(updates(&set)[0].data, value("a", 5));
    assert_eq!(
        bases(&machine, id),
        Some(vec!["a".to_owned(), "b".to_owned()])
    );
}

#[test]
fn a_delta_after_a_removal_fails_and_keeps_the_other_keys_bases() {
    let (mut machine, transport, id) = delta_synced();
    let removed = machine.handle(frame(
        transport,
        &push(r#"{"key":"a","removed":true,"data":"","offset":3}"#),
    ));
    take_all(&mut machine, &removed);

    let failed = machine.handle(frame(transport, &push(&delta("a", 1, 2, 4))));

    let error = delta_error(&failed).expect("a delta error");
    assert_eq!(error.code, codes::error::DELTA);
    assert!(updates(&failed).is_empty(), "{failed:?}");
    assert!(
        events(&failed).iter().any(|(_, event)| matches!(
            event,
            MapSubscriptionEvent::Subscribing { code, .. }
                if *code == codes::subscribing::DELTA_ERROR
        )),
        "{failed:?}"
    );
    assert_eq!(unsubscribes(&failed), 1, "{failed:?}");
    assert_eq!(bases(&machine, id), Some(vec!["b".to_owned()]));
}

#[test]
fn a_corrupt_delta_drops_only_its_keys_base_and_resubscribes_recovering() {
    let (mut machine, transport, id) = delta_synced();

    let failed = machine.handle(frame(
        transport,
        &push(r#"{"key":"a","delta":true,"data":"not a delta","offset":3}"#),
    ));

    assert!(delta_error(&failed).is_some(), "{failed:?}");
    assert_eq!(bases(&machine, id), Some(vec!["b".to_owned()]));
    take_all(&mut machine, &failed);
    let unsubscribe = sent(&failed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    ));
    let again = machine.handle(Input::SubscriptionTimerFired(
        id,
        SubscriptionTimer::Resubscribe,
    ));
    let request = &subscribe_sent(&again)["subscribe"];
    assert_eq!(request["phase"], 1, "{request}");
    assert_eq!(request["recover"], true, "{request}");
    assert_eq!(bases(&machine, id), Some(vec!["b".to_owned()]));
}

#[test]
fn a_recovering_resubscribe_keeps_the_bases_of_every_key() {
    let (mut machine, transport, id) = delta_synced();
    let (transport, recovering) = reconnect(&mut machine, transport);
    let recovering = recovering.expect("a subscribe");
    assert_eq!(recovering["subscribe"]["phase"], 1, "{recovering}");
    assert_eq!(
        bases(&machine, id),
        Some(vec!["a".to_owned(), "b".to_owned()])
    );

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &recovering,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":4,"publications":[{},{}]"#,
                whole("b", 2, 3),
                delta("b", 2, 3, 4)
            ),
        ),
    ));
    let caught_up = updates(&live);
    assert_eq!(caught_up[0].data, value("b", 2));
    assert_eq!(caught_up[1].data, value("b", 3));
    take_all(&mut machine, &live);

    // "a" was not in the catch-up: its next delta is against the value held
    // before the reconnect.
    let pushed = machine.handle(frame(transport, &push(&delta("a", 1, 2, 5))));

    assert_eq!(updates(&pushed)[0].data, value("a", 2));
}

/// Answers `request`, the first of a load from scratch, live with delta on
/// and no entry, then pushes a delta of "a": it must fail for want of a
/// base.
fn assert_no_base_after_a_load_from_scratch(
    machine: &mut Machine,
    transport: TransportId,
    id: SubscriptionId,
    request: &Value,
) {
    assert_eq!(request["subscribe"]["phase"], 2, "{request}");
    assert_eq!(bases(machine, id), None);
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            request,
            r#""type":1,"recoverable":true,"delta":true,"epoch":"e2","offset":9"#,
        ),
    ));
    take_all(machine, &live);
    assert_eq!(bases(machine, id), Some(vec![]));

    let failed = machine.handle(frame(transport, &push(&delta("a", 1, 2, 10))));

    assert!(delta_error(&failed).is_some(), "{failed:?}");
}

#[test]
fn error_112_drops_the_bases_and_the_next_load_starts_none() {
    let (mut machine, transport, id) = delta_synced();
    let (transport, recovering) = reconnect(&mut machine, transport);

    machine.handle(frame(
        transport,
        &error_reply(&recovering.expect("a subscribe"), 112, false),
    ));

    assert_eq!(bases(&machine, id), None);
    let again = machine.handle(Input::SubscriptionTimerFired(
        id,
        SubscriptionTimer::Resubscribe,
    ));
    assert_no_base_after_a_load_from_scratch(&mut machine, transport, id, &subscribe_sent(&again));
}

#[test]
fn error_112_with_the_fatal_strategy_drops_the_bases_too() {
    let (mut machine, transport) = connected();
    let mut options = delta_options();
    options.unrecoverable_strategy = UnrecoverableStrategy::Fatal;
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":1,"state":[{}]"#,
                whole("a", 1, 1)
            ),
        ),
    ));
    take_all(&mut machine, &live);
    let (transport, recovering) = reconnect(&mut machine, transport);

    machine.handle(frame(
        transport,
        &error_reply(&recovering.expect("a subscribe"), 112, false),
    ));

    assert_eq!(bases(&machine, id), None);
}

#[test]
fn an_epoch_change_during_a_recovering_load_drops_the_bases() {
    let (mut machine, transport, id) = delta_synced();
    let (transport, recovering) = reconnect(&mut machine, transport);

    let changed = machine.handle(frame(
        transport,
        &subscribe_reply(
            &recovering.expect("a subscribe"),
            r#""type":1,"phase":1,"epoch":"other","offset":1"#,
        ),
    ));

    assert_eq!(bases(&machine, id), None);
    let unsubscribe = sent(&changed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    let again = subscribe_sent(&machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    )));
    assert_no_base_after_a_load_from_scratch(&mut machine, transport, id, &again);
}

#[test]
fn unsubscribe_2502_makes_the_next_load_start_with_no_base() {
    let (mut machine, transport, id) = delta_synced();

    let invalidated = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"scores","unsubscribe":{"code":2502,"reason":"state invalidated"}}}"#,
    ));

    take_all(&mut machine, &invalidated);
    let request = subscribe_sent(&invalidated);
    assert_no_base_after_a_load_from_scratch(&mut machine, transport, id, &request);
}

#[test]
fn disconnect_3014_makes_the_next_load_start_with_no_base() {
    let (mut machine, transport, id) = delta_synced();
    machine.handle(Input::TransportClosed {
        transport,
        code: Some(3014),
        reason: "state invalidated".into(),
    });
    let transport =
        opened(&machine.handle(Input::TimerFired(crate::machine::Timer::Reconnect))).unwrap();
    let mut actions = machine.handle(Input::TransportOpened(transport));
    let connect = sent(&actions)
        .into_iter()
        .find(|command| command.get("connect").is_some())
        .expect("a connect command");
    actions.extend(machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"connect":{{"client":"c"}}}}"#, id_of(&connect)),
    )));

    let request = sent(&actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe");

    assert_no_base_after_a_load_from_scratch(&mut machine, transport, id, &request);
}

#[test]
fn a_sync_withdrawn_before_it_was_taken_makes_the_next_load_start_with_no_base() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let subscribed = machine.handle(Input::Subscribe(id));
    let first = subscribe_sent(&subscribed);
    take_all(&mut machine, &subscribed);
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":1,"state":[{}]"#,
                whole("a", 1, 1)
            ),
        ),
    ));
    for (event, what) in events(&live) {
        if !matches!(what, MapSubscriptionEvent::Sync(_)) {
            machine.handle(Input::EventTaken(event));
        }
    }
    assert_eq!(bases(&machine, id), Some(vec!["a".to_owned()]));

    // The application never took the sync: no position to recover from.
    let unsubscribed = machine.handle(Input::Unsubscribe(id));
    let unsubscribe = sent(&unsubscribed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe");
    take_all(&mut machine, &unsubscribed);
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"unsubscribe":{{}}}}"#, id_of(&unsubscribe)),
    ));
    let subscribing = machine.handle(Input::Subscribe(id));
    let request = subscribe_sent(&subscribing);
    take_all(&mut machine, &subscribing);

    assert_no_base_after_a_load_from_scratch(&mut machine, transport, id, &request);
}

#[test]
fn a_delta_failing_in_the_live_reply_gives_no_sync_and_unsubscribes() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let failed = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":1,"publications":[{}]"#,
                delta("a", 1, 2, 1)
            ),
        ),
    ));

    assert!(delta_error(&failed).is_some(), "{failed:?}");
    assert!(
        !events(&failed)
            .iter()
            .any(|(_, event)| matches!(event, MapSubscriptionEvent::Sync(_))),
        "{failed:?}"
    );
    assert_eq!(unsubscribes(&failed), 1, "{failed:?}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribing)
    );
}

#[test]
fn a_delta_rebuilding_past_the_inbound_limit_fails_and_keeps_no_base() {
    let limit = 60;
    let mut machine = Machine::new(Config::default().max_message_bytes(limit));
    let transport = opened(&machine.handle(Input::Connect)).unwrap();
    let connecting = machine.handle(Input::TransportOpened(transport));
    let connect = sent(&connecting)
        .into_iter()
        .find(|command| command.get("connect").is_some())
        .expect("a connect command");
    machine.handle(frame(
        transport,
        &format!(r#"{{"id":{},"connect":{{"client":"c"}}}}"#, id_of(&connect)),
    ));
    let (mut machine, transport, id) = delta_synced_on(machine, transport);
    let big = "a".repeat(limit + 1).into_bytes();
    let patch = wrapped(&crate::fossil::write(&value("a", 1), &big));

    let failed = machine.handle(frame(
        transport,
        &push(&format!(
            r#"{{"key":"a","delta":true,"data":{patch},"offset":3}}"#
        )),
    ));

    let error = delta_error(&failed).expect("a delta error");
    assert!(error.message.contains("inbound message limit"), "{error:?}");
    assert_eq!(bases(&machine, id), Some(vec!["b".to_owned()]));
}

/// A value of `len` bytes that fossil's copies can repeat.
fn long_value(len: usize) -> Vec<u8> {
    (0..len)
        .map(|n| b'a' + u8::try_from((n * n + n / 7) % 26).unwrap())
        .collect()
}

fn whole_bytes(key: &str, data: &[u8], offset: u64) -> String {
    format!(
        r#"{{"key":"{key}","data":{},"offset":{offset}}}"#,
        wrapped(data)
    )
}

fn delta_bytes(key: &str, from: &[u8], to: &[u8], offset: u64) -> String {
    format!(
        r#"{{"key":"{key}","delta":true,"data":{},"offset":{offset}}}"#,
        wrapped(&crate::fossil::write(from, to))
    )
}

/// A delta rebuilding `base` written out `times` times, in a few bytes.
fn expanding(key: &str, base: &[u8], times: usize, offset: u64) -> String {
    format!(
        r#"{{"key":"{key}","delta":true,"data":{},"offset":{offset}}}"#,
        wrapped(&crate::fossil::write_repeats(base, times))
    )
}

fn bounded_delta(machine: &mut Machine, max_state_bytes: usize) -> (SubscriptionId, Value) {
    let mut options = delta_options();
    options.max_state_bytes = NonZeroUsize::new(max_state_bytes).unwrap();
    let id = machine
        .add_map_subscription("scores".into(), options)
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));
    (id, first)
}

fn ended_with_code_5(actions: &[Action]) {
    assert_eq!(unsubscribes(actions), 1, "{actions:?}");
    let events = events(actions);
    assert!(
        events
            .iter()
            .any(|(_, event)| matches!(event, MapSubscriptionEvent::Unsubscribed { code: 5, .. })),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|(_, event)| matches!(
            event,
            MapSubscriptionEvent::Sync(..) | MapSubscriptionEvent::Update(..)
        )),
        "{events:?}"
    );
}

// Codex, #202: a superseded value is freed as the load is rebuilt, so only
// what the state holds counts.
#[test]
fn values_a_load_superseded_do_not_count_toward_the_bound() {
    let (mut machine, transport) = connected();
    let (_, first) = bounded_delta(&mut machine, 3000);
    let mut current = long_value(1000);
    let mut publications = Vec::new();
    for version in 0..20_u8 {
        let mut next = current.clone();
        next[0] = b'A' + version;
        publications.push(delta_bytes("a", &current, &next, u64::from(version) + 2));
        current = next;
    }

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":21,"state":[{}],"publications":[{}]"#,
                whole_bytes("a", &long_value(1000), 1),
                publications.join(",")
            ),
        ),
    ));

    assert_eq!(unsubscribes(&live), 0, "{live:?}");
    let synced = events(&live)
        .into_iter()
        .find_map(|(_, event)| match event {
            MapSubscriptionEvent::Sync(entries) => Some(entries),
            _ => None,
        })
        .expect("a sync");
    assert_eq!(synced.len(), 1);
    assert_eq!(synced[0].data, current);
}

// Codex, #202: small deltas can rebuild far more than the replies carried.
#[test]
fn a_state_rebuilt_past_the_bound_ends_the_subscription_with_code_5() {
    let (mut machine, transport) = connected();
    let (id, first) = bounded_delta(&mut machine, 1500);
    let base = long_value(200);

    let ended = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":4,"state":[{},{}],"publications":[{},{}]"#,
                whole_bytes("a", &base, 1),
                whole_bytes("b", &base, 2),
                expanding("a", &base, 5, 3),
                expanding("b", &base, 5, 4)
            ),
        ),
    ));

    ended_with_code_5(&ended);
    assert_eq!(bases(&machine, id), None);
}

// Codex, #202: a completed recovery's updates are all kept, so they all count.
#[test]
fn a_catch_up_rebuilt_past_the_bound_ends_the_subscription_and_forgets_the_position() {
    let (mut machine, transport) = connected();
    let (id, first) = bounded_delta(&mut machine, 1500);
    let base = long_value(200);
    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":1,"state":[{}]"#,
                whole_bytes("a", &base, 1)
            ),
        ),
    ));
    take_all(&mut machine, &live);
    assert!(machine.subscription_position(id).is_some());
    let (transport, recovering) = reconnect(&mut machine, transport);
    let recovering = recovering.expect("a subscribe");
    let big = base.repeat(5);
    let mut other = big.clone();
    other[0] = b'Z';

    let ended = machine.handle(frame(
        transport,
        &subscribe_reply(
            &recovering,
            &format!(
                r#""type":1,"recoverable":true,"delta":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":3,"publications":[{},{}]"#,
                expanding("a", &base, 5, 2),
                delta_bytes("a", &big, &other, 3)
            ),
        ),
    ));

    ended_with_code_5(&ended);
    assert_eq!(bases(&machine, id), None);
    assert_eq!(machine.subscription_position(id), None);
}

#[test]
fn a_delta_while_delta_is_off_fails() {
    let (mut machine, transport, id) = synced();
    assert_eq!(bases(&machine, id), None);

    let failed = machine.handle(frame(transport, &push(&delta("a", 1, 2, 8))));

    assert!(delta_error(&failed).is_some(), "{failed:?}");
    assert!(updates(&failed).is_empty(), "{failed:?}");
}

#[test]
fn an_entry_without_a_value_stays_empty_with_delta_on() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_map_subscription("scores".into(), delta_options())
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let first = subscribe_sent(&machine.handle(Input::Subscribe(id)));

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &first,
            r#""type":1,"recoverable":true,"delta":true,"epoch":"e1","offset":1,"state":[{"key":"u1","offset":1}]"#,
        ),
    ));

    assert_eq!(sync_keys(&live), ["u1"]);
    assert!(delta_error(&live).is_none(), "{live:?}");
}

#[test]
fn a_live_reply_with_delta_off_ends_the_chains_and_data_stays_as_received() {
    let (mut machine, transport, id) = delta_synced();
    let (transport, recovering) = reconnect(&mut machine, transport);

    let live = machine.handle(frame(
        transport,
        &subscribe_reply(
            &recovering.expect("a subscribe"),
            r#""type":1,"recoverable":true,"was_recovering":true,"recovered":true,"epoch":"e1","offset":2"#,
        ),
    ));
    take_all(&mut machine, &live);

    assert_eq!(bases(&machine, id), None);
    let pushed = machine.handle(frame(
        transport,
        &push(r#"{"key":"a","data":{"v":2},"offset":3}"#),
    ));
    assert!(delta_error(&pushed).is_none(), "{pushed:?}");
    assert_eq!(updates(&pushed)[0].data, br#"{"v":2}"#);
}
