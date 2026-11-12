//! Block 47d: per-key delta bases of shared poll subscriptions, their
//! bound, and the deltas in flight across an untrack (@E).

use std::num::NonZeroUsize;

use super::*;

/// A machine connected over a transport with `config`.
fn connected_with(config: Config) -> (Machine, TransportId) {
    let mut machine = Machine::new(config);
    let transport = opened(&machine.handle(Input::Connect)).expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
    (machine, transport)
}

fn delta_options(bound: usize) -> SharedPollSubscriptionOptions {
    let mut options = options(false);
    options.common = SubscriptionOptions::default().delta(crate::DeltaFormat::Fossil);
    options.max_delta_bases_bytes = NonZeroUsize::new(bound).unwrap();
    options
}

/// Answers the subscribe command among `actions` with delta on.
fn answer_with_delta(
    machine: &mut Machine,
    transport: TransportId,
    actions: &[Action],
) -> Vec<Action> {
    let command = sent(actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe command");
    let answered = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command["id"], "subscribe": {"type": 4, "delta": true}})
            .to_string(),
    ));
    take_all(machine, &answered);
    answered
}

/// A shared poll subscription to "items" with delta, its bases bounded by
/// `bound`, subscribed with delta on over a machine with `config`, its
/// stream open, tracking `keys` under a signature the server accepted.
fn tracking_with(
    config: Config,
    bound: usize,
    keys: &[&str],
) -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected_with(config);
    let id = machine
        .add_shared_poll_subscription("items".into(), delta_options(bound))
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let actions = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, &actions);
    answer_with_delta(&mut machine, transport, &actions);
    let items: Vec<(&str, u64)> = keys.iter().map(|key| (*key, 0)).collect();
    let tracked = track(&mut machine, id, &items, "s");
    for request in requests(&tracked, 1) {
        machine.handle(reply_to(transport, &request));
    }
    (machine, transport, id)
}

fn tracking(keys: &[&str]) -> (Machine, TransportId, SubscriptionId) {
    tracking_with(Config::default(), 64 << 20, keys)
}

/// The value `n` of a key, as JSON text.
fn value(n: u64) -> String {
    serde_json::json!({"v": format!("value {n} of a key that changes a little at a time")})
        .to_string()
}

/// A publication of `key` at `version` as a track reply item or a push:
/// `payload` whole, a JSON string with delta on (Block 19, D7).
fn whole(key: &str, version: u64, payload: &str) -> Value {
    serde_json::json!({"key": key, "data": payload, "version": version})
}

/// A publication of `key` at `version` as a fossil delta from `from` to
/// `to`.
fn delta(key: &str, version: u64, from: &str, to: &str) -> Value {
    let patch = String::from_utf8(crate::write_fossil_delta(from.as_bytes(), to.as_bytes()))
        .expect("an ASCII delta");
    serde_json::json!({"key": key, "data": patch, "version": version, "delta": true})
}

fn pushed(transport: TransportId, publication: &Value) -> Input {
    frame(
        transport,
        &serde_json::json!({"push": {"channel": "items", "pub": publication}}).to_string(),
    )
}

/// Pushes `publications` in turn, taking every event: the actions.
fn push_all(machine: &mut Machine, transport: TransportId, publications: &[Value]) -> Vec<Action> {
    let mut actions = Vec::new();
    for publication in publications {
        let handled = machine.handle(pushed(transport, publication));
        take_all(machine, &handled);
        actions.extend(handled);
    }
    actions
}

/// The delta errors among `actions`.
fn delta_errors(actions: &[Action]) -> Vec<u32> {
    events(actions)
        .into_iter()
        .filter_map(|(_, event)| match event {
            SharedPollSubscriptionEvent::Error(error) if error.kind == ErrorKind::Delta => {
                Some(error.code)
            }
            _ => None,
        })
        .collect()
}

fn values(actions: &[Action]) -> Vec<(String, Vec<u8>)> {
    updates(actions)
        .into_iter()
        .map(|(_, update)| (update.key, update.data))
        .collect()
}

// D10: `delta` asks for fossil delta on every subscribe.
#[test]
fn the_subscribe_command_asks_for_delta_when_built_with_it() {
    let (mut machine, _) = connected();
    let with = machine
        .add_shared_poll_subscription("with".into(), delta_options(1024))
        .unwrap();
    let without = machine
        .add_shared_poll_subscription("without".into(), options(false))
        .unwrap();

    let with = sent(&machine.handle(Input::Subscribe(with)));
    let without = sent(&machine.handle(Input::Subscribe(without)));

    assert_eq!(with[0]["subscribe"]["delta"], "fossil", "{with:?}");
    assert!(
        without[0]["subscribe"].get("delta").is_none(),
        "{without:?}"
    );
}

// D10, D13: each key's deltas apply to that key's last value, and the
// statistics count each value received with delta on.
#[test]
fn deltas_are_decoded_per_key_and_counted() {
    let (mut machine, transport, id) = tracking(&["a", "b"]);

    let actions = push_all(
        &mut machine,
        transport,
        &[
            whole("a", 1, &value(1)),
            whole("b", 1, &value(10)),
            delta("a", 2, &value(1), &value(2)),
            delta("b", 2, &value(10), &value(11)),
        ],
    );

    assert_eq!(
        values(&actions),
        [
            ("a".to_owned(), value(1).into_bytes()),
            ("b".to_owned(), value(10).into_bytes()),
            ("a".to_owned(), value(2).into_bytes()),
            ("b".to_owned(), value(11).into_bytes()),
        ]
    );
    let statistics = machine.delta_statistics(id).unwrap();
    assert_eq!(
        (
            statistics.publications,
            statistics.full_payloads,
            statistics.delta_payloads
        ),
        (4, 2, 2)
    );
}

/// Gives key "a" a base, then ends it as `end` does, then tracks it again:
/// the actions of a delta for "a" against the base it had.
fn a_delta_after(end: &str) -> Vec<Action> {
    let (mut machine, transport, id) = tracking(&["a", "b"]);
    push_all(&mut machine, transport, &[whole("a", 1, &value(1))]);
    match end {
        "untrack" => {
            let untracked = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
            machine.handle(reply_to(transport, &requests(&untracked, 2)[0]));
        }
        "removal" => {
            push_all(
                &mut machine,
                transport,
                &[serde_json::json!({"key": "a", "removed": true})],
            );
        }
        "refused" => {
            // A track of "a" the server refuses for good untracks it (D7).
            let tracked = track(&mut machine, id, &[("a", 0)], "bad");
            let request = &requests(&tracked, 1)[0];
            let refused = machine.handle(frame(
                transport,
                &serde_json::json!({"id": request["id"], "error": {"code": 103, "message": "permission denied"}})
                    .to_string(),
            ));
            take_all(&mut machine, &refused);
            let untrack = &requests(&refused, 2)[0];
            machine.handle(reply_to(transport, untrack));
        }
        other => unreachable!("{other}"),
    }
    let again = track(&mut machine, id, &[("a", 0)], "s2");
    machine.handle(reply_to(transport, &requests(&again, 1)[0]));
    machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))))
}

// D10: a key's base goes when the key stops being tracked.
#[test]
fn a_base_goes_with_its_key() {
    for end in ["untrack", "removal", "refused"] {
        let actions = a_delta_after(end);

        assert_eq!(delta_errors(&actions), [codes::error::DELTA], "{end}");
        assert_eq!(values(&actions), [], "{end}");
    }
}

// D10, D13: every base goes when the subscription leaves `subscribed`, so
// none outlives a subscribe: after a reconnect, a 2500 and a 2502.
#[test]
fn no_base_outlives_a_subscribe() {
    for how in ["reconnect", "2500", "2502"] {
        let (mut machine, transport, id) = tracking(&["a"]);
        push_all(&mut machine, transport, &[whole("a", 1, &value(1))]);
        let transport = match how {
            "reconnect" => {
                let closed = machine.handle(Input::Disconnect);
                take_all(&mut machine, &closed);
                let opening = machine.handle(Input::Connect);
                take_all(&mut machine, &opening);
                let transport = opened(&opening).expect("a new transport");
                let mut actions = machine.handle(Input::TransportOpened(transport));
                let connect = sent(&actions)
                    .into_iter()
                    .find(|command| command.get("connect").is_some())
                    .expect("a connect command");
                actions.extend(
                    machine.handle(frame(
                        transport,
                        &serde_json::json!({"id": connect["id"], "connect": {"client": "c"}})
                            .to_string(),
                    )),
                );
                take_all(&mut machine, &actions);
                answer_with_delta(&mut machine, transport, &actions);
                transport
            }
            code => {
                let resubscribe = machine.handle(frame(
                    transport,
                    &format!(
                        r#"{{"push":{{"channel":"items","unsubscribe":{{"code":{code},"reason":"r"}}}}}}"#
                    ),
                ));
                take_all(&mut machine, &resubscribe);
                answer_with_delta(&mut machine, transport, &resubscribe);
                transport
            }
        };
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Subscribed),
            "{how}"
        );

        let actions = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));

        assert_eq!(delta_errors(&actions), [codes::error::DELTA], "{how}");
    }
}

// D10: a delta for a key with no base is dropped while an untrack request
// of that key awaits its reply, and is a failure once none does.
#[test]
fn a_no_base_delta_is_dropped_only_while_its_untrack_is_unanswered() {
    let (mut machine, transport, id) = tracking(&["a"]);
    push_all(&mut machine, transport, &[whole("a", 1, &value(1))]);
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    let again = track(&mut machine, id, &[("a", 0)], "s2");

    let dropped = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));

    assert!(events(&dropped).is_empty(), "{dropped:?}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
    assert_eq!(machine.delta_statistics(id).unwrap().delta_payloads, 0);

    // The untrack answered, the track too: the next value comes whole.
    machine.handle(reply_to(transport, &requests(&untracked, 2)[0]));
    machine.handle(reply_to(transport, &requests(&again, 1)[0]));
    let whole_again = push_all(&mut machine, transport, &[whole("a", 9, &value(9))]);
    assert_eq!(
        values(&whole_again),
        [("a".to_owned(), value(9).into_bytes())]
    );

    // With no untrack unanswered, a delta with no base fails.
    let (mut machine, transport, _) = tracking(&["a"]);
    let failed = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert_eq!(delta_errors(&failed), [codes::error::DELTA]);
}

// D10, D12: an untrack still queued behind another request over an
// emulated transport counts as unanswered.
#[test]
fn a_queued_untrack_counts_as_unanswered() {
    let (mut machine, transport, id) = subscribed_emulated_with_delta();
    let tracked = track(&mut machine, id, &[("a", 0)], "s");
    machine.handle(reply_to(transport, &emulated_requests(&tracked, 1)[0]));
    push_all(&mut machine, transport, &[whole("a", 1, &value(1))]);
    // In flight, unanswered: what follows waits behind it.
    let in_flight = track(&mut machine, id, &[("b", 0)], "s2");
    assert_eq!(emulated_requests(&in_flight, 1).len(), 1);
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    assert!(emulated_requests(&untracked, 2).is_empty(), "queued");
    track(&mut machine, id, &[("a", 0)], "s3");

    let dropped = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));

    assert!(events(&dropped).is_empty(), "{dropped:?}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
}

fn subscribed_emulated_with_delta() -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected_emulated();
    let id = machine
        .add_shared_poll_subscription("items".into(), delta_options(64 << 20))
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let actions = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, &actions);
    let subscribe = sent_emulated(&actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe command");
    let answered = machine.handle(frame(
        transport,
        &serde_json::json!({"id": subscribe["id"], "subscribe": {"type": 4, "delta": true}})
            .to_string(),
    ));
    take_all(&mut machine, &answered);
    (machine, transport, id)
}

/// A JSON string of `size` bytes, quotes included.
fn sized(size: usize) -> String {
    format!("\"{}\"", "x".repeat(size - 2))
}

/// Whether `actions` end the subscription with `unsubscribed` 7 after an
/// unsubscribe command.
fn ended_by_the_bound(actions: &[Action]) -> bool {
    let unsubscribed = events(actions).into_iter().any(|(_, event)| {
        matches!(
            event,
            SharedPollSubscriptionEvent::Unsubscribed { code, ref reason }
                if code == codes::unsubscribed::SHARED_POLL_DELTA_BASES_TOO_LARGE
                    && reason == "shared poll delta bases too large"
        )
    });
    let command = sent(actions)
        .iter()
        .any(|command| command["unsubscribe"]["channel"] == "items");
    unsubscribed && command
}

// D11: the bases together stay within `max_delta_bases_bytes`; a key whose
// base would pass it ends the subscription with `unsubscribed` 7, its
// value not delivered, and it does not subscribe again.
#[test]
fn bases_past_their_bound_end_the_subscription() {
    let (mut machine, transport, id) = tracking_with(Config::default(), 100, &["a", "b"]);
    let first = push_all(&mut machine, transport, &[whole("a", 1, &sized(60))]);
    assert!(!ended_by_the_bound(&first));

    let past = push_all(&mut machine, transport, &[whole("b", 1, &sized(60))]);

    assert!(ended_by_the_bound(&past), "{past:?}");
    assert_eq!(values(&past), []);
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
    assert!(
        !past.iter().any(|action| matches!(
            action,
            Action::SetSubscriptionTimer(_, crate::machine::SubscriptionTimer::Resubscribe, _)
        )),
        "no resubscribe: {past:?}"
    );
}

// D11: what the bound counts: a base replaced counts once, and an untrack
// or a subscribe frees what it held.
#[test]
fn the_bound_counts_each_base_held_once() {
    // A replaced base.
    let (mut machine, transport, _) = tracking_with(Config::default(), 100, &["a"]);
    let replaced = push_all(
        &mut machine,
        transport,
        &[whole("a", 1, &sized(60)), whole("a", 2, &sized(60))],
    );
    assert!(!ended_by_the_bound(&replaced), "{replaced:?}");
    assert_eq!(values(&replaced).len(), 2);

    // An untracked key's base.
    let (mut machine, transport, id) = tracking_with(Config::default(), 100, &["a", "b"]);
    push_all(&mut machine, transport, &[whole("a", 1, &sized(60))]);
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    machine.handle(reply_to(transport, &requests(&untracked, 2)[0]));
    let freed = push_all(&mut machine, transport, &[whole("b", 1, &sized(60))]);
    assert!(!ended_by_the_bound(&freed), "{freed:?}");

    // The bases of an earlier subscribe.
    let (mut machine, transport, _) = tracking_with(Config::default(), 100, &["a"]);
    push_all(&mut machine, transport, &[whole("a", 1, &sized(60))]);
    let resubscribe = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"r"}}}"#,
    ));
    take_all(&mut machine, &resubscribe);
    answer_with_delta(&mut machine, transport, &resubscribe);
    let after = push_all(&mut machine, transport, &[whole("a", 2, &sized(60))]);
    assert!(!ended_by_the_bound(&after), "{after:?}");
}

// D9, D10, Block 15: the items of one track reply are decoded before any
// is emitted. A delta failure, or the bound passed, emits none of the
// values, which leaving `subscribed` would withdraw within the message,
// but keeps a removal, which is never withdrawn.
#[test]
fn a_failing_track_reply_emits_only_its_removals() {
    for failure in ["no base", "bound"] {
        let (mut machine, transport, id) = tracking_with(Config::default(), 100, &["a", "c"]);
        let tracked = track(&mut machine, id, &[("b", 0)], "s2");
        let last = match failure {
            "no base" => delta("b", 1, &value(0), &value(1)),
            _ => whole("b", 1, &sized(60)),
        };

        let reply = machine.handle(frame(
            transport,
            &serde_json::json!({"id": requests(&tracked, 1)[0]["id"], "sub_refresh": {"items": [
                whole("a", 1, &sized(60)),
                {"key": "c", "removed": true},
                last,
            ]}})
            .to_string(),
        ));

        let withdrawn: Vec<EventId> = reply
            .iter()
            .filter_map(|action| match action {
                Action::Withdraw(ids) => Some(ids.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let emitted = updates(&reply);
        assert_eq!(
            emitted
                .iter()
                .map(|(_, update)| (update.key.as_str(), update.removed))
                .collect::<Vec<_>>(),
            [("c", true)],
            "{failure}: {reply:?}"
        );
        assert!(
            emitted
                .iter()
                .all(|(event_id, _)| !withdrawn.contains(event_id)),
            "{failure}"
        );
        let kept = machine.shared_poll_tracked_keys(id).unwrap();
        assert!(!kept.contains(&"c".to_owned()), "{failure}: {kept:?}");
    }
}

// Block 19, D6: the values one message builds stay within the inbound
// limit together, as they would sent whole.
#[test]
fn the_values_of_one_reply_stay_within_the_inbound_limit() {
    let config = Config::default().max_message_bytes(600);
    let (mut machine, transport, id) = tracking_with(config, 64 << 20, &["a"]);
    let base = sized(250);
    push_all(&mut machine, transport, &[whole("a", 1, &base)]);
    let tracked = track(&mut machine, id, &[("a", 0)], "s2");

    let reply = machine.handle(frame(
        transport,
        &serde_json::json!({"id": requests(&tracked, 1)[0]["id"], "sub_refresh": {"items": [
            delta("a", 2, &base, &base),
            delta("a", 3, &base, &base),
            delta("a", 4, &base, &base),
        ]}})
        .to_string(),
    ));

    assert_eq!(delta_errors(&reply), [codes::error::DELTA], "{reply:?}");
    assert_eq!(values(&reply), []);
}
