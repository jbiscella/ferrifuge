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

// D10: only an untrack request counts, of this subscription and of the
// current `subscribed` period: a track of the key in flight, another
// subscription's untrack of the same key, or an untrack sent before the
// last subscribe leaves a delta with no base a failure.
#[test]
fn only_an_unanswered_untrack_of_this_period_counts() {
    // A track of the key in flight, unanswered.
    let (mut machine, transport, id) = tracking(&["a"]);
    track(&mut machine, id, &[("a", 0)], "s2");
    let failed = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert_eq!(delta_errors(&failed), [codes::error::DELTA], "a track");

    // Another subscription's untrack of the same key, unanswered.
    let (mut machine, transport, _) = tracking(&["a"]);
    let other = machine
        .add_shared_poll_subscription("other".into(), options(false))
        .unwrap();
    let subscribing = machine.handle(Input::Subscribe(other));
    take_all(&mut machine, &subscribing);
    answer_subscribe(&mut machine, transport, &subscribing);
    let tracked = track(&mut machine, other, &[("a", 0)], "s");
    machine.handle(reply_to(transport, &requests(&tracked, 1)[0]));
    let untracked = machine.handle(Input::UntrackKeys(other, vec!["a".into()]));
    assert_eq!(requests(&untracked, 2).len(), 1);
    let failed = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert_eq!(delta_errors(&failed), [codes::error::DELTA], "another's");

    // An untrack sent before a resubscribe, unanswered.
    let (mut machine, transport, id) = tracking(&["a"]);
    machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    let resubscribe = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"r"}}}"#,
    ));
    take_all(&mut machine, &resubscribe);
    answer_with_delta(&mut machine, transport, &resubscribe);
    let again = track(&mut machine, id, &[("a", 0)], "s2");
    machine.handle(reply_to(transport, &requests(&again, 1)[0]));
    let failed = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert_eq!(
        delta_errors(&failed),
        [codes::error::DELTA],
        "an earlier period's"
    );
}

/// Disconnects while `untrack` is called, then connects again, answering
/// the resubscribe with delta on: the new transport and the replay's
/// actions.
fn away_and_back(
    machine: &mut Machine,
    id: SubscriptionId,
    untrack: &[&str],
) -> (TransportId, Vec<Action>) {
    let closed = machine.handle(Input::Disconnect);
    take_all(machine, &closed);
    machine.handle(Input::UntrackKeys(
        id,
        untrack.iter().map(|key| (*key).to_owned()).collect(),
    ));
    let opening = machine.handle(Input::Connect);
    take_all(machine, &opening);
    let transport = opened(&opening).expect("a new transport");
    let mut actions = machine.handle(Input::TransportOpened(transport));
    let connect = sent(&actions)
        .into_iter()
        .find(|command| command.get("connect").is_some())
        .expect("a connect command");
    actions.extend(machine.handle(frame(
        transport,
        &serde_json::json!({"id": connect["id"], "connect": {"client": "c"}}).to_string(),
    )));
    take_all(machine, &actions);
    let replay = answer_with_delta(machine, transport, &actions);
    (transport, replay)
}

/// k1 and k2 tracked under one signature; k1 untracked while away, so the
/// replay carries it inline (D7), its frame answered and a whole value of
/// k1 dropped (D9); then k1 tracked again, that request unanswered: the
/// machine, its transport, the subscription and that request.
fn tracked_again_after_an_inline_untrack() -> (Machine, TransportId, SubscriptionId, Value) {
    let (mut machine, _, id) = tracking(&["k1", "k2"]);
    let (transport, replay) = away_and_back(&mut machine, id, &["k1"]);
    let frames = requests(&replay, 1);
    assert_eq!(frames.len(), 1, "{replay:?}");
    assert_eq!(
        frames[0]["sub_refresh"]["untrack"],
        serde_json::json!(["k1"])
    );
    machine.handle(reply_to(transport, &frames[0]));
    let dropped = push_all(&mut machine, transport, &[whole("k1", 1, &value(1))]);
    assert_eq!(values(&dropped), []);
    let again = track(&mut machine, id, &[("k1", 0)], "s2");
    let request = requests(&again, 1)[0].clone();
    (machine, transport, id, request)
}

// D7, D10: the server applies a frame's inline untrack after it tracked the
// key, delivered its value and answered (centrifuge `handleTrack`), so a
// delta of a key tracked again since may follow while that key has no
// base: dropped, until a later track request carrying the key is answered.
#[test]
fn a_delta_after_an_inline_untrack_is_dropped_until_the_key_is_tracked_again() {
    let (mut machine, transport, id, request) = tracked_again_after_an_inline_untrack();

    let dropped = machine.handle(pushed(transport, &delta("k1", 2, &value(1), &value(2))));

    assert!(events(&dropped).is_empty(), "{dropped:?}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
    machine.handle(reply_to(transport, &request));
    let delivered = push_all(&mut machine, transport, &[whole("k1", 3, &value(3))]);
    assert_eq!(
        values(&delivered),
        [("k1".to_owned(), value(3).into_bytes())]
    );
}

// D10: once a later track request carrying the key is answered, a delta of
// the key with no base is Block 19's failure again.
#[test]
fn an_answered_track_ends_the_inline_untrack() {
    let (mut machine, transport, _, request) = tracked_again_after_an_inline_untrack();
    machine.handle(reply_to(transport, &request));

    let failed = machine.handle(pushed(transport, &delta("k1", 2, &value(1), &value(2))));

    assert_eq!(delta_errors(&failed), [codes::error::DELTA], "{failed:?}");
}

// D10: an inline untrack counts in its `subscribed` period only: one of an
// earlier period does not hold a delta, and a track request of an earlier
// period answered late does not end one of this period.
#[test]
fn an_inline_untrack_counts_in_its_period_only() {
    // An earlier period's.
    let (mut machine, _, id) = tracking(&["k1", "k2"]);
    let (transport, replay) = away_and_back(&mut machine, id, &["k1"]);
    machine.handle(reply_to(transport, &requests(&replay, 1)[0]));
    // Only k1's signature is left once k2 goes; the next replay carries k1
    // alone, not answered.
    machine.handle(Input::UntrackKeys(id, vec!["k2".into()]));
    let again = track(&mut machine, id, &[("k1", 0)], "s2");
    assert_eq!(requests(&again, 1).len(), 1);
    let resubscribe = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"r"}}}"#,
    ));
    take_all(&mut machine, &resubscribe);
    let replay = answer_with_delta(&mut machine, transport, &resubscribe);
    assert!(
        requests(&replay, 1)
            .iter()
            .all(|frame| frame["sub_refresh"].get("untrack").is_none()),
        "{replay:?}"
    );
    let failed = machine.handle(pushed(transport, &delta("k1", 2, &value(1), &value(2))));
    assert_eq!(
        delta_errors(&failed),
        [codes::error::DELTA],
        "an earlier period's"
    );

    // A track request of an earlier period, answered late.
    let (mut machine, transport, id) = tracking(&["k1", "k2"]);
    let late = requests(&track(&mut machine, id, &[("k1", 0)], "s2"), 1)[0].clone();
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["k1".into()]));
    machine.handle(reply_to(transport, &requests(&untracked, 2)[0]));
    let resubscribe = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"r"}}}"#,
    ));
    take_all(&mut machine, &resubscribe);
    let replay = answer_with_delta(&mut machine, transport, &resubscribe);
    let frames = requests(&replay, 1);
    assert_eq!(
        frames[0]["sub_refresh"]["untrack"],
        serde_json::json!(["k1"])
    );
    machine.handle(reply_to(transport, &frames[0]));
    machine.handle(reply_to(transport, &late));
    track(&mut machine, id, &[("k1", 0)], "s3");
    let dropped = machine.handle(pushed(transport, &delta("k1", 2, &value(1), &value(2))));
    assert!(events(&dropped).is_empty(), "a late reply: {dropped:?}");
}

/// The keys of the untrack requests among `actions`.
fn untracked_keys(actions: &[Action]) -> Vec<String> {
    requests(actions, 2)
        .iter()
        .flat_map(|request| {
            request["sub_refresh"]["untrack"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .map(|key| key.as_str().unwrap().to_owned())
        .collect()
}

/// Answers every request among `actions`: the actions of the answers.
fn answer_all(machine: &mut Machine, transport: TransportId, actions: &[Action]) -> Vec<Action> {
    let mut answered = Vec::new();
    for kind in [1, 2] {
        for request in requests(actions, kind) {
            answered.extend(machine.handle(reply_to(transport, &request)));
        }
    }
    answered
}

/// A subscription to "items" with delta and a `get_signature` callback,
/// subscribed with delta on.
fn signing_with_delta() -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected();
    let mut options = delta_options(64 << 20);
    options.signature_source = true;
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let actions = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, &actions);
    answer_with_delta(&mut machine, transport, &actions);
    (machine, transport, id)
}

/// `track([first, second])`, `first` untracked while the callback runs
/// (its untrack answered), then the callback's signature over both: the
/// track request, which carries `first` inline (D6, D7), unanswered.
fn signed_with_one_untracked(
    machine: &mut Machine,
    transport: TransportId,
    id: SubscriptionId,
    first: &str,
    second: &str,
) -> Value {
    let asked = machine.handle(Input::TrackKeys(id, vec![first.into(), second.into()]));
    let (request, _) = fetches(&asked)[0].clone();
    let untracked = machine.handle(Input::UntrackKeys(id, vec![first.into()]));
    answer_all(machine, transport, &untracked);
    let signed = machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Ok(crate::SharedPollSignature::new(
            vec![first.into(), second.into()],
            "s",
        )),
    });
    let frames = requests(&signed, 1);
    assert_eq!(frames.len(), 1, "{signed:?}");
    assert_eq!(
        frames[0]["sub_refresh"]["untrack"],
        serde_json::json!([first])
    );
    frames[0].clone()
}

// Codex on #221 (D10): an inline untrack of a key no held signature covers
// any more does not stay forever: fresh keys churned through the race of
// D6 leave a bounded set behind.
#[test]
fn inline_untracks_of_keys_gone_stay_bounded() {
    let (mut machine, transport, id) = subscribed(true, true);
    for n in 0..50 {
        let (first, second) = (format!("a{n}"), format!("b{n}"));
        let frame = signed_with_one_untracked(&mut machine, transport, id, &first, &second);
        let answered = machine.handle(reply_to(transport, &frame));
        answer_all(&mut machine, transport, &answered);
        let untracked = machine.handle(Input::UntrackKeys(id, vec![second]));
        answer_all(&mut machine, transport, &untracked);
    }

    let flow = machine.subscriptions.by_id[&id]
        .shared_poll
        .as_ref()
        .unwrap();
    assert!(
        flow.inline_untracked.len() <= 2,
        "{} inline untracks held",
        flow.inline_untracked.len()
    );
}

// Codex on #221 (D10): once no held signature covers a key untracked
// inline and its frame is answered, the next reply sends the key in an
// untrack request,
// whose reply comes after the server applied the inline untrack (centrifuge
// `handleUntrack` waits for the track requests in progress, and that
// frame's reply was read before it was sent). Until then a delta of the
// key tracked again is dropped; after it, one with no base is a failure.
#[test]
fn an_inline_untrack_of_a_key_gone_becomes_an_untrack_request() {
    let (mut machine, transport, id) = signing_with_delta();
    let frame = signed_with_one_untracked(&mut machine, transport, id, "a", "b");
    let answered = machine.handle(reply_to(transport, &frame));
    assert!(
        !untracked_keys(&answered).contains(&"a".to_owned()),
        "b holds its signature"
    );
    answer_all(&mut machine, transport, &answered);
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["b".into()]));

    // Once b's untrack is answered, a is in no held signature.
    let answered = answer_all(&mut machine, transport, &untracked);

    let untrack: Vec<Value> = requests(&answered, 2)
        .into_iter()
        .filter(|request| request["sub_refresh"]["untrack"] == serde_json::json!(["a"]))
        .collect();
    assert_eq!(untrack.len(), 1, "{answered:?}");
    track(&mut machine, id, &[("a", 0)], "s4");
    let dropped = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert!(events(&dropped).is_empty(), "{dropped:?}");
    machine.handle(reply_to(transport, &untrack[0]));
    let failed = machine.handle(pushed(transport, &delta("a", 2, &value(1), &value(2))));
    assert_eq!(delta_errors(&failed), [codes::error::DELTA], "{failed:?}");
}

// Codex on #221 (D10): not while the frame carrying the key inline awaits
// its reply: an untrack handled before that frame would prove nothing.
#[test]
fn an_inline_untrack_waits_for_its_frame_before_becoming_an_untrack_request() {
    let (mut machine, transport, id) = signing_with_delta();
    let frame = signed_with_one_untracked(&mut machine, transport, id, "a", "b");
    let untracked = machine.handle(Input::UntrackKeys(id, vec!["b".into()]));
    answer_all(&mut machine, transport, &untracked);

    let other = track(&mut machine, id, &[("c", 0)], "s3");
    let before = machine.handle(reply_to(transport, &requests(&other, 1)[0]));
    let after = machine.handle(reply_to(transport, &frame));

    assert!(
        !untracked_keys(&before).contains(&"a".to_owned()),
        "{before:?}"
    );
    assert!(
        untracked_keys(&after).contains(&"a".to_owned()),
        "{after:?}"
    );
}

// Codex on #221 (D9, D10): a whole value at the version held, which the
// server sends again after a track from version 0, is no update, but it is
// what the server's next delta of the key applies to: it becomes the base.
// A delta at the version held changes no base.
#[test]
fn a_whole_duplicate_becomes_the_base_and_a_delta_duplicate_does_not() {
    let (mut machine, transport, id) = tracking(&["a"]);
    push_all(&mut machine, transport, &[whole("a", 5, &value(5))]);
    let (transport, replay) = away_and_back(&mut machine, id, &[]);
    answer_all(&mut machine, transport, &replay);
    let again = track(&mut machine, id, &[("a", 0)], "s2");
    answer_all(&mut machine, transport, &again);

    let counted = machine.delta_statistics(id).unwrap();
    let duplicates = push_all(
        &mut machine,
        transport,
        &[
            delta("a", 5, &value(4), &value(5)),
            whole("a", 5, &value(5)),
        ],
    );
    assert!(events(&duplicates).is_empty(), "{duplicates:?}");
    assert_eq!(
        machine.delta_statistics(id).unwrap(),
        counted,
        "no duplicate counts"
    );
    let next = push_all(
        &mut machine,
        transport,
        &[delta("a", 6, &value(5), &value(6))],
    );

    assert_eq!(delta_errors(&next), Vec::<u32>::new(), "{next:?}");
    assert_eq!(values(&next), [("a".to_owned(), value(6).into_bytes())]);
}

// Codex on #221 (D10): only this subscription's frames in flight hold an
// inline untrack back; another subscription's frame naming the same key
// inline does not.
#[test]
fn another_subscriptions_frame_holds_no_inline_untrack_back() {
    let (mut machine, transport, id) = signing_with_delta();
    let other = machine
        .add_shared_poll_subscription("other".into(), options(true))
        .unwrap();
    let subscribing = machine.handle(Input::Subscribe(other));
    take_all(&mut machine, &subscribing);
    answer_subscribe(&mut machine, transport, &subscribing);
    signed_with_one_untracked(&mut machine, transport, other, "a", "b");
    let frame = signed_with_one_untracked(&mut machine, transport, id, "a", "b");
    let answered = machine.handle(reply_to(transport, &frame));
    answer_all(&mut machine, transport, &answered);

    let untracked = machine.handle(Input::UntrackKeys(id, vec!["b".into()]));
    let answered = answer_all(&mut machine, transport, &untracked);

    assert!(
        requests(&answered, 2).iter().any(|request| {
            request["sub_refresh"]["channel"] == "items"
                && request["sub_refresh"]["untrack"] == serde_json::json!(["a"])
        }),
        "{answered:?}"
    );
}
