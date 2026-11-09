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
    assert!(update.data.is_empty());
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
