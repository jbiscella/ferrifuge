//! Block 47: shared poll subscriptions on the core (@E).

use serde_json::Value;

use super::*;
use crate::Config;
use crate::codec::Codec;
use crate::machine::{EventId, Input, SubscriptionOptions, TransportId};

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

/// The track (1) or untrack (2) requests among `actions`.
fn requests(actions: &[Action], kind: i64) -> Vec<Value> {
    sent(actions)
        .into_iter()
        .filter(|command| command["sub_refresh"]["type"] == kind)
        .collect()
}

fn events(actions: &[Action]) -> Vec<(EventId, SharedPollSubscriptionEvent)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::EmitSharedPollSubscription(event_id, _, event) => {
                Some((*event_id, event.clone()))
            }
            _ => None,
        })
        .collect()
}

fn updates(actions: &[Action]) -> Vec<(EventId, SharedPollUpdate)> {
    events(actions)
        .into_iter()
        .filter_map(|(event_id, event)| match event {
            SharedPollSubscriptionEvent::Update(update) => Some((event_id, update)),
            _ => None,
        })
        .collect()
}

fn fetches(actions: &[Action]) -> Vec<(FetchId, Vec<String>)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::FetchSignature { request, keys, .. } => Some((*request, keys.clone())),
            _ => None,
        })
        .collect()
}

/// The items of a track request, as (key, version), 0 for none.
fn items(request: &Value) -> Vec<(String, u64)> {
    request["sub_refresh"]["track"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|batch| batch["items"].as_array().unwrap().clone())
        .map(|item| {
            (
                item["key"].as_str().unwrap().to_owned(),
                item["version"].as_u64().unwrap_or(0),
            )
        })
        .collect()
}

fn options(signature_source: bool) -> SharedPollSubscriptionOptions {
    SharedPollSubscriptionOptions {
        signature_source,
        ..SharedPollSubscriptionOptions::default()
    }
}

/// Answers the one subscribe command among `actions` as a shared poll
/// subscription's.
fn answer_subscribe(
    machine: &mut Machine,
    transport: TransportId,
    actions: &[Action],
) -> Vec<Action> {
    let command = sent(actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe command");
    machine.handle(frame(
        transport,
        &serde_json::json!({"id": command["id"], "subscribe": {"type": 4}}).to_string(),
    ))
}

/// Takes every event among `actions`, as a stream read at once would.
fn take_all(machine: &mut Machine, actions: &[Action]) {
    for action in actions {
        if let Action::EmitSharedPollSubscription(event_id, ..) | Action::Emit(event_id, _) = action
        {
            machine.handle(Input::EventTaken(*event_id));
        }
    }
}

/// A shared poll subscription to "items" subscribed on a connected
/// machine, with its stream open when `stream`, its events taken.
fn subscribed(signature_source: bool, stream: bool) -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(signature_source))
        .unwrap();
    if stream {
        machine.handle(Input::StreamOpened(Some(id)));
    }
    let actions = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, &actions);
    let answered = answer_subscribe(&mut machine, transport, &actions);
    take_all(&mut machine, &answered);
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Subscribed)
    );
    (machine, transport, id)
}

fn track(
    machine: &mut Machine,
    id: SubscriptionId,
    items: &[(&str, u64)],
    signature: &str,
) -> Vec<Action> {
    machine.handle(Input::TrackKeysWithSignature(
        id,
        items
            .iter()
            .map(|(key, version)| SharedPollTrackItem::new(*key, *version))
            .collect(),
        signature.into(),
    ))
}

fn push(transport: TransportId, key: &str, version: u64) -> Input {
    frame(
        transport,
        &serde_json::json!({"push": {"channel": "items", "pub": {"key": key, "data": {"v": version}, "version": version}}})
            .to_string(),
    )
}

/// Disconnects and connects again, a client-side reason that withdraws
/// what was not taken, answering the resubscribe: the actions of the reply,
/// which hold the replay.
fn reconnect(machine: &mut Machine, _transport: TransportId) -> (TransportId, Vec<Action>) {
    let closed = machine.handle(Input::Disconnect);
    take_all(machine, &closed);
    let actions = machine.handle(Input::Connect);
    take_all(machine, &actions);
    let transport = opened(&actions).expect("a new transport");
    // The resubscribe goes with the connect command (Block 36, D5).
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
    let subscribe = sent(&actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a resubscribe");
    let replay = machine.handle(frame(
        transport,
        &serde_json::json!({"id": subscribe["id"], "subscribe": {"type": 4}}).to_string(),
    ));
    take_all(machine, &replay);
    (transport, replay)
}

// D4; delta waits for 47d's per-key bases (D10), as map subscriptions
// did before 45d: asked for or not, the subscribe carries none.
#[test]
fn the_subscribe_command_has_type_4_and_nothing_a_stream_subscription_adds() {
    let (mut machine, _) = connected();
    let mut options = options(false);
    options.common = SubscriptionOptions::default()
        .data(b"{}".to_vec())
        .positioned(true)
        .recoverable(true)
        .join_leave(true)
        .channel_compaction(true)
        .delta(crate::DeltaFormat::Fossil);
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();

    let subscribe = &sent(&machine.handle(Input::Subscribe(id)))[0]["subscribe"];

    assert_eq!(subscribe["type"], 4);
    assert_eq!(subscribe["data"], serde_json::json!({}));
    for absent in [
        "positioned",
        "recoverable",
        "join_leave",
        "flag",
        "recover",
        "epoch",
        "offset",
        "delta",
    ] {
        assert!(subscribe.get(absent).is_none(), "{absent} in {subscribe}");
    }
}

// D4: a server before v6.8.0 grants a stream subscription.
#[test]
fn a_reply_without_type_4_unsubscribes_with_code_8() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(false))
        .unwrap();
    let command = sent(&machine.handle(Input::Subscribe(id)))[0].clone();

    let actions = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command["id"], "subscribe": {}}).to_string(),
    ));

    assert!(
        sent(&actions)
            .iter()
            .any(|c| c["unsubscribe"]["channel"] == "items")
    );
    assert!(events(&actions).iter().any(|(_, event)| matches!(
        event,
        SharedPollSubscriptionEvent::Unsubscribed { code: 8, reason } if reason == NOT_SUPPORTED_REASON
    )));
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
}

#[test]
fn track_and_untrack_send_their_requests_and_empty_sets_send_nothing() {
    let (mut machine, _, id) = subscribed(true, false);

    let tracked = requests(&track(&mut machine, id, &[("a", 3), ("b", 0)], "sig1"), 1);
    let untracked = requests(&machine.handle(Input::UntrackKeys(id, vec!["a".into()])), 2);
    let empty_track = machine.handle(Input::TrackKeys(id, Vec::new()));
    let empty_untrack = machine.handle(Input::UntrackKeys(id, Vec::new()));
    let empty_signed = machine.handle(Input::TrackKeysWithSignature(id, Vec::new(), "s".into()));

    assert_eq!(tracked.len(), 1);
    assert_eq!(tracked[0]["sub_refresh"]["track"][0]["signature"], "sig1");
    assert_eq!(items(&tracked[0]), [("a".into(), 3), ("b".into(), 0)]);
    assert_eq!(
        untracked[0]["sub_refresh"]["untrack"],
        serde_json::json!(["a"])
    );
    for actions in [empty_track, empty_untrack, empty_signed] {
        assert!(
            sent(&actions).is_empty() && fetches(&actions).is_empty(),
            "{actions:?}"
        );
    }
    assert_eq!(machine.shared_poll_tracked_keys(id).unwrap(), ["b"]);
}

// D8: the signatures held go first, at the versions held, then the keys
// without one are signed.
#[test]
fn the_replay_sends_the_signatures_held_then_signs_the_others() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(true))
        .unwrap();
    track(&mut machine, id, &[("a", 5)], "sig1");
    track(&mut machine, id, &[("a", 2)], "sig2");
    let before = machine.handle(Input::TrackKeys(id, vec!["b".into()]));
    assert!(fetches(&before).is_empty(), "not subscribed: only stored");

    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);

    let tracks = requests(&replay, 1);
    assert_eq!(tracks.len(), 1);
    assert_eq!(
        tracks[0]["sub_refresh"]["track"].as_array().unwrap().len(),
        1
    );
    assert_eq!(tracks[0]["sub_refresh"]["track"][0]["signature"], "sig2");
    assert_eq!(items(&tracks[0]), [("a".into(), 5)]);
    assert_eq!(
        fetches(&replay)
            .iter()
            .map(|(_, keys)| keys.clone())
            .collect::<Vec<_>>(),
        [vec!["b".to_owned()]]
    );
}

#[test]
fn a_signature_fetched_is_tracked_and_kept_for_the_next_replay() {
    let (mut machine, transport, id) = subscribed(true, false);
    let (request, keys) = fetches(&machine.handle(Input::TrackKeys(id, vec!["a".into()])))
        .pop()
        .unwrap();

    let actions = machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Ok(crate::SharedPollSignature::new(keys, "sig1")),
    });
    let (_, replay) = reconnect(&mut machine, transport);

    assert_eq!(
        requests(&actions, 1)[0]["sub_refresh"]["track"][0]["signature"],
        "sig1"
    );
    assert_eq!(
        requests(&replay, 1)[0]["sub_refresh"]["track"][0]["signature"],
        "sig1"
    );
    assert!(
        fetches(&replay).is_empty(),
        "no new signature for a key held"
    );
}

// I13: an answer to a request abandoned by leaving `subscribed` changes
// nothing.
#[test]
fn a_stale_signature_answer_and_a_stale_track_reply_change_nothing() {
    let (mut machine, transport, id) = subscribed(true, true);
    let (request, keys) = fetches(&machine.handle(Input::TrackKeys(id, vec!["a".into()])))
        .pop()
        .unwrap();
    let track_command = requests(&track(&mut machine, id, &[("a", 0)], "sig0"), 1)[0]["id"].clone();
    let (transport, _) = reconnect(&mut machine, transport);

    let answered = machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Ok(crate::SharedPollSignature::new(keys, "late")),
    });
    let replied = machine.handle(frame(
        transport,
        &serde_json::json!({"id": track_command, "sub_refresh": {"items": [{"key": "a", "data": {}, "version": 7}]}})
            .to_string(),
    ));

    assert!(requests(&answered, 1).is_empty(), "{answered:?}");
    assert!(updates(&replied).is_empty(), "{replied:?}");
}

// D5, Block 15: a version moves only when its update is taken, so an
// update withdrawn by a disconnect is asked for again.
#[test]
fn a_version_moves_only_when_its_update_is_taken() {
    let (mut machine, transport, id) = subscribed(false, true);
    track(&mut machine, id, &[("a", 0)], "sig1");
    let pushed = updates(&machine.handle(push(transport, "a", 2)));
    assert_eq!(pushed.len(), 1);

    let (transport, replay) = reconnect(&mut machine, transport);
    assert_eq!(items(&requests(&replay, 1)[0]), [("a".into(), 0)]);

    let again = updates(&machine.handle(push(transport, "a", 2)));
    machine.handle(Input::EventTaken(again[0].0));
    let (_, replay) = reconnect(&mut machine, transport);
    assert_eq!(items(&requests(&replay, 1)[0]), [("a".into(), 2)]);
}

// Block 41, D3: an update handed to a handler moved the version already.
#[test]
fn a_replay_uses_the_version_of_an_update_handed_to_a_handler() {
    let (mut machine, transport, id) = subscribed(false, true);
    track(&mut machine, id, &[("a", 0)], "sig1");
    let pushed = updates(&machine.handle(push(transport, "a", 3)));
    machine.handle(Input::EventHanded(pushed[0].0));
    let lost = machine.handle(Input::Disconnect);
    take_all(&mut machine, &lost);
    // The handler returns once disconnected: its update was not withdrawn.
    machine.handle(Input::EventTaken(pushed[0].0));

    let (_, replay) = reconnect(&mut machine, transport);

    assert_eq!(items(&requests(&replay, 1)[0]), [("a".into(), 3)]);
}

// D9: a removal ends its key when handled and is never withdrawn; a key
// tracked again after it stays tracked.
#[test]
fn a_removal_ends_its_key_at_once_and_is_never_withdrawn() {
    let (mut machine, transport, id) = subscribed(false, true);
    track(&mut machine, id, &[("a", 0)], "sig1");
    let removal = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","pub":{"key":"a","removed":true}}}"#,
    ));
    let (removal_id, update) = updates(&removal).pop().unwrap();
    assert!(update.removed && update.version.is_none() && update.data.is_empty());
    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        Vec::<String>::new()
    );

    let lost = machine.handle(Input::Disconnect);
    track(&mut machine, id, &[("a", 0)], "sig2");

    assert!(
        !lost
            .iter()
            .any(|action| matches!(action, Action::Withdraw(ids) if ids.contains(&removal_id))),
        "{lost:?}"
    );
    assert_eq!(machine.shared_poll_tracked_keys(id).unwrap(), ["a"]);
}

// D9: what is not tracked, and a version not newer than the one held or
// handled in the same message, are dropped.
#[test]
fn untracked_keys_and_old_versions_are_dropped() {
    let (mut machine, transport, id) = subscribed(false, false);
    let command = requests(&track(&mut machine, id, &[("a", 5)], "sig1"), 1)[0]["id"].clone();

    let reply = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command, "sub_refresh": {"items": [
            {"key": "a", "data": {}, "version": 6},
            {"key": "a", "data": {}, "version": 6},
            {"key": "z", "data": {}, "version": 1},
        ]}})
        .to_string(),
    ));
    let old = machine.handle(push(transport, "a", 5));
    let empty_key = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","pub":{"data":{},"version":9}}}"#,
    ));

    let got: Vec<(String, Option<u64>)> = updates(&reply)
        .into_iter()
        .map(|(_, update)| (update.key, update.version))
        .collect();
    assert_eq!(got, [("a".into(), Some(6))]);
    assert!(updates(&old).is_empty() && updates(&empty_key).is_empty());
}

// D7: a refused track untracks its keys, on the server too, each with a
// removal; 109 and temporary errors only report.
#[test]
fn a_refused_track_untracks_its_keys_and_others_only_report() {
    for (error, refused) in [
        (
            serde_json::json!({"code": 103, "message": "permission denied"}),
            true,
        ),
        (
            serde_json::json!({"code": 106, "message": "limit exceeded"}),
            true,
        ),
        (
            serde_json::json!({"code": 109, "message": "token expired"}),
            false,
        ),
        (
            serde_json::json!({"code": 500, "message": "busy", "temporary": true}),
            false,
        ),
        (serde_json::json!({"code": 1, "message": "timeout"}), false),
    ] {
        let (mut machine, transport, id) = subscribed(false, false);
        let command =
            requests(&track(&mut machine, id, &[("a", 0), ("b", 0)], "s"), 1)[0]["id"].clone();

        let actions = machine.handle(frame(
            transport,
            &serde_json::json!({"id": command, "error": error}).to_string(),
        ));

        let got = events(&actions);
        assert!(
            matches!(&got[0].1, SharedPollSubscriptionEvent::Error(e) if e.kind == ErrorKind::Track && Some(u64::from(e.code)) == error["code"].as_u64()),
            "{got:?}"
        );
        let removed: Vec<String> = updates(&actions).into_iter().map(|(_, u)| u.key).collect();
        let untracked = requests(&actions, 2);
        if refused {
            assert_eq!(removed, ["a", "b"]);
            assert_eq!(
                untracked[0]["sub_refresh"]["untrack"],
                serde_json::json!(["a", "b"])
            );
            assert_eq!(
                machine.shared_poll_tracked_keys(id).unwrap(),
                [] as [std::string::String; 0]
            );
        } else {
            assert!(
                removed.is_empty() && untracked.is_empty(),
                "{error}: {actions:?}"
            );
            assert_eq!(machine.shared_poll_tracked_keys(id).unwrap(), ["a", "b"]);
        }
    }
}

#[test]
fn an_untrack_error_is_reported_as_such() {
    let (mut machine, transport, id) = subscribed(false, true);
    track(&mut machine, id, &[("a", 0)], "s");
    let command =
        requests(&machine.handle(Input::UntrackKeys(id, vec!["a".into()])), 2)[0]["id"].clone();

    let actions = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command, "error": {"code": 107, "message": "bad request"}})
            .to_string(),
    ));

    assert!(matches!(
        &events(&actions)[..],
        [(_, SharedPollSubscriptionEvent::Error(error))] if error.kind == ErrorKind::Untrack && error.code == 107
    ));
}

#[test]
fn unsubscribe_forgets_the_keys_and_the_signatures() {
    let (mut machine, transport, id) = subscribed(false, false);
    track(&mut machine, id, &[("a", 0)], "sig1");

    let unsubscribed = machine.handle(Input::Unsubscribe(id));
    let unsubscribe = sent(&unsubscribed)[0]["id"].clone();
    machine.handle(frame(
        transport,
        &serde_json::json!({"id": unsubscribe, "unsubscribe": {}}).to_string(),
    ));
    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);

    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        [] as [std::string::String; 0]
    );
    assert_eq!(requests(&replay, 1), [] as [serde_json::Value; 0]);
}

// D7: each untrack request is within the budget by its worst-case cost,
// and the cost bounds the bytes of every encoding (a property test).
#[test]
fn the_untrack_cost_bounds_every_encoding() {
    let mut rng = fastrand::Rng::with_seed(47);
    let alphabet: Vec<char> = "ab\"\\\n\t\u{1}é€😀/<>".chars().collect();
    for _ in 0..200 {
        let key = |rng: &mut fastrand::Rng| -> String {
            (0..rng.usize(0..40))
                .map(|_| alphabet[rng.usize(..alphabet.len())])
                .collect()
        };
        let channel = key(&mut rng);
        let keys: Vec<String> = (0..rng.usize(1..60)).map(|_| key(&mut rng)).collect();
        let budget = rng.usize(300..3000);
        let requests = SharedPollFlow::untrack_requests(&channel, keys.clone(), budget);
        assert_eq!(requests.concat(), keys);
        for request in requests {
            let cost = FRAME_COST
                + escaped_twice(&channel)
                + request
                    .iter()
                    .map(|key| escaped_twice(key) + UNTRACK_KEY_COST)
                    .sum::<usize>();
            assert!(request.len() == 1 || cost <= budget, "{cost} over {budget}");
            let command = Command {
                id: u32::MAX,
                method: Method::SubRefresh(SubRefreshRequest {
                    channel: channel.clone(),
                    kind: UNTRACK,
                    untrack: request,
                    ..SubRefreshRequest::default()
                }),
            };
            let session = "0".repeat(36);
            for codec in [Codec::Json, Codec::Protobuf] {
                let encoded = codec.encode(std::slice::from_ref(&command));
                let emulated = codec.emulation_request(&session, &session, &encoded);
                assert!(
                    encoded.len() <= cost,
                    "{codec:?}: {} over {cost}",
                    encoded.len()
                );
                assert!(
                    emulated.len() <= cost,
                    "{codec:?}: {} over {cost}",
                    emulated.len()
                );
            }
        }
    }
}

#[test]
fn an_untrack_over_the_budget_is_split() {
    let (mut machine, _) = connected();
    let mut options = options(false);
    options.max_track_frame_bytes = std::num::NonZeroUsize::new(1000).unwrap();
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    machine.handle(Input::StreamOpened(Some(id)));
    let transport = machine.transport.unwrap();
    let actions = machine.handle(Input::Subscribe(id));
    answer_subscribe(&mut machine, transport, &actions);
    let keys: Vec<String> = (0..100).map(|n| format!("{n:0>20}")).collect();

    let untracked = requests(&machine.handle(Input::UntrackKeys(id, keys.clone())), 2);

    assert!(untracked.len() > 1);
    let sent: Vec<String> = untracked
        .iter()
        .flat_map(|request| {
            request["sub_refresh"]["untrack"]
                .as_array()
                .unwrap()
                .clone()
        })
        .map(|key| key.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(sent, keys);
}

// D16: a signature is a credential.
#[test]
fn no_debug_output_shows_a_signature() {
    const SECRET: &str = "s1gn4tur3-s3cr3t";
    let (mut machine, _, id) = subscribed(true, false);
    let input =
        Input::TrackKeysWithSignature(id, vec![SharedPollTrackItem::new("a", 0)], SECRET.into());
    let fetched = Input::SignatureFetched {
        subscription: id,
        request: FetchId(0),
        result: Ok(crate::SharedPollSignature::new(vec!["a".into()], SECRET)),
    };
    let batch = TrackBatch {
        signature: Secret(SECRET.into()),
        items: Vec::new(),
    };
    let printed = format!("{input:?} {fetched:?} {batch:?}");
    let actions = machine.handle(input);

    let printed = format!("{printed} {actions:?}");
    assert!(!printed.contains(SECRET), "{printed}");
    assert!(
        sent(&actions)[0].to_string().contains(SECRET),
        "sent on the wire"
    );
}
