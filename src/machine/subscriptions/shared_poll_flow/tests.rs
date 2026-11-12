//! Block 47: shared poll subscriptions on the core (@E).

use serde_json::Value;

use super::*;
use crate::Config;
use crate::TokenError;
use crate::codec::Codec;
use crate::machine::{EventId, FetchId, Input, SubscriptionOptions, TransportId};

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

/// A machine connected over an emulated transport (`http_stream`).
fn connected_emulated() -> (Machine, TransportId) {
    let mut machine = Machine::new(Config::default())
        .with_transports(vec![crate::machine::TransportKind::HttpStream])
        .expect("an emulated transport");
    let transport = opened(&machine.handle(Input::Connect)).expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(frame(
        transport,
        r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
    ));
    (machine, transport)
}

/// The commands the emulation requests among `actions` carry, decoded.
fn sent_emulated(actions: &[Action]) -> Vec<Value> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Codec::Json.emulation_frame(frame),
            _ => None,
        })
        .flat_map(|frame| {
            String::from_utf8(frame)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn emulated_requests(actions: &[Action], kind: i64) -> Vec<Value> {
    sent_emulated(actions)
        .into_iter()
        .filter(|command| command["sub_refresh"]["type"] == kind)
        .collect()
}

/// A shared poll subscription to "items" subscribed over an emulated
/// transport, its stream open and its events taken.
fn subscribed_emulated() -> (Machine, TransportId, SubscriptionId) {
    let (mut machine, transport) = connected_emulated();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(false))
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
        &serde_json::json!({"id": subscribe["id"], "subscribe": {"type": 4}}).to_string(),
    ));
    take_all(&mut machine, &answered);
    (machine, transport, id)
}

fn reply_to(transport: TransportId, command: &Value) -> Input {
    frame(
        transport,
        &serde_json::json!({"id": command["id"], "sub_refresh": {}}).to_string(),
    )
}

// D12: over an emulated transport a subscription's requests go one at a
// time, in the order made, each waiting for its reply.
#[test]
fn emulated_track_and_untrack_requests_go_one_at_a_time() {
    let (mut machine, transport, id) = subscribed_emulated();

    let tracked = emulated_requests(&track(&mut machine, id, &[("a", 0)], "sig1"), 1);
    let held = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    let released = machine.handle(reply_to(transport, &tracked[0]));

    assert_eq!(tracked.len(), 1);
    assert!(emulated_requests(&held, 2).is_empty(), "{held:?}");
    let untracked = emulated_requests(&released, 2);
    assert_eq!(untracked.len(), 1);
    assert_eq!(
        untracked[0]["sub_refresh"]["untrack"],
        serde_json::json!(["a"])
    );
}

// D12: a reply deadline passing over emulation reconnects with connecting
// 3 "shared poll request timeout"; a reply read stops it.
#[test]
fn an_emulated_request_without_a_reply_reconnects_with_code_3() {
    let (mut machine, transport, id) = subscribed_emulated();
    machine.handle(Input::StreamOpened(None));
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let timer = actions
        .iter()
        .find_map(|action| match action {
            Action::SetSharedPollTimer(timer, delay) => Some((*timer, *delay)),
            _ => None,
        })
        .expect("a reply deadline");
    assert_eq!(timer.1, Config::default().timeout);

    let fired = machine.handle(Input::SharedPollTimerFired(timer.0));

    assert!(
        fired.iter().any(|action| matches!(
            action,
            Action::Emit(_, crate::ClientEvent::Connecting { code: 3, reason })
                if reason == "shared poll request timeout"
        )),
        "{fired:?}"
    );

    let (mut machine, transport2, id) = subscribed_emulated();
    let _ = transport;
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let request = emulated_requests(&actions, 1)[0].clone();
    let timer = actions
        .iter()
        .find_map(|action| match action {
            Action::SetSharedPollTimer(timer, _) => Some(*timer),
            _ => None,
        })
        .unwrap();
    machine.handle(reply_to(transport2, &request));
    let late = machine.handle(Input::SharedPollTimerFired(timer));
    assert!(
        !late
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{late:?}"
    );
}

// D12: the next subscribe waits until the requests in flight are answered;
// a queued request of the earlier subscription is dropped.
#[test]
fn an_emulated_resubscribe_waits_for_the_requests_in_flight() {
    let (mut machine, transport, id) = subscribed_emulated();
    let request = emulated_requests(&track(&mut machine, id, &[("a", 0)], "sig1"), 1)[0].clone();
    machine.handle(Input::UntrackKeys(id, vec!["b".into()]));

    let resubscribing = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"resubscribe"}}}"#,
    ));
    take_all(&mut machine, &resubscribing);
    let answered = machine.handle(reply_to(transport, &request));

    assert!(
        !sent_emulated(&resubscribing)
            .iter()
            .any(|command| command.get("subscribe").is_some()),
        "{resubscribing:?}"
    );
    let commands = sent_emulated(&answered);
    assert!(
        commands
            .iter()
            .any(|command| command.get("subscribe").is_some()),
        "{answered:?}"
    );
    assert!(
        emulated_requests(&answered, 2).is_empty(),
        "the earlier untrack waits"
    );
    // Once subscribed again, the earlier subscription's untrack is dropped
    // and the replay goes first.
    take_all(&mut machine, &answered);
    let subscribe = commands
        .iter()
        .find(|command| command.get("subscribe").is_some())
        .unwrap();
    let replay = machine.handle(frame(
        transport,
        &serde_json::json!({"id": subscribe["id"], "subscribe": {"type": 4}}).to_string(),
    ));
    assert!(emulated_requests(&replay, 2).is_empty(), "{replay:?}");
    assert_eq!(emulated_requests(&replay, 1).len(), 1, "{replay:?}");
}

/// A shared poll subscription to "items" with a frame budget of `budget`,
/// tracking keys `k0`.. under one signature per `per_batch` keys before
/// subscribing, then subscribed: the machine, transport, id and the
/// actions of the subscribe reply, which hold the replay.
fn replaying(
    budget: usize,
    keys: usize,
    per_batch: usize,
) -> (Machine, TransportId, SubscriptionId, Vec<Action>) {
    let (mut machine, transport) = connected();
    let mut options = options(false);
    options.max_track_frame_bytes = std::num::NonZeroUsize::new(budget).unwrap();
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    let names: Vec<String> = (0..keys).map(|n| format!("k{n:03}")).collect();
    for (number, batch) in names.chunks(per_batch).enumerate() {
        let items: Vec<(&str, u64)> = batch.iter().map(|key| (key.as_str(), u64::MAX)).collect();
        track(&mut machine, id, &items, &format!("sig{number}"));
    }
    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);
    (machine, transport, id, replay)
}

// D7: the replay's batches are split over frames within the budget, sent
// one after the other.
#[test]
fn a_replay_over_the_budget_goes_in_frames_one_after_the_other() {
    let (mut machine, transport, _, replay) = replaying(1000, 30, 5);

    let first = requests(&replay, 1);
    assert_eq!(first.len(), 1, "one frame at a time");
    let mut frames = vec![first[0].clone()];
    loop {
        let next = requests(
            &machine.handle(reply_to(transport, frames.last().unwrap())),
            1,
        );
        match next.len() {
            0 => break,
            1 => frames.push(next[0].clone()),
            more => panic!("{more} frames at once"),
        }
    }
    assert!(frames.len() > 1);
    let mut keys = Vec::new();
    for request in &frames {
        assert!(request.to_string().len() <= 1000, "{request}");
        keys.extend(items(request).into_iter().map(|(key, _)| key));
    }
    let expected: Vec<String> = (0..30).map(|n| format!("k{n:03}")).collect();
    assert_eq!(keys, expected);
}

// D7: a permanent error on a replay frame untracks the keys of every frame
// not accepted, the rest never sent.
#[test]
fn a_refused_replay_frame_untracks_the_keys_of_the_frames_not_sent() {
    let (mut machine, transport, id, replay) = replaying(1000, 30, 5);
    let first = requests(&replay, 1)[0].clone();

    let refused = machine.handle(frame(
        transport,
        &serde_json::json!({"id": first["id"], "error": {"code": 103, "message": "permission denied"}})
            .to_string(),
    ));

    assert!(requests(&refused, 1).is_empty(), "{refused:?}");
    assert_eq!(updates(&refused).len(), 30);
    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        [] as [std::string::String; 0]
    );
}

// D6: a batch over the budget is refused before it enters the library or
// the wire: error 12 of kind Track, its keys removed.
#[test]
fn a_batch_over_the_budget_is_refused_before_it_is_sent() {
    let (mut machine, transport) = connected();
    let mut options = options(false);
    options.max_track_frame_bytes = std::num::NonZeroUsize::new(1000).unwrap();
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    let keys: Vec<String> = (0..100).map(|n| format!("{n:020}")).collect();
    let items: Vec<(&str, u64)> = keys.iter().map(|key| (key.as_str(), 0)).collect();

    let refused = track(&mut machine, id, &items, "sig1");
    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);

    assert!(
        matches!(
            &events(&refused)[0].1,
            SharedPollSubscriptionEvent::Error(error)
                if error.kind == ErrorKind::Track && error.code == codes::error::BAD_CONFIGURATION
        ),
        "{refused:?}"
    );
    assert_eq!(updates(&refused).len(), 100);
    assert_eq!(sent(&refused), [] as [serde_json::Value; 0]);
    assert!(requests(&replay, 1).is_empty(), "not kept for the replay");
    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        [] as [std::string::String; 0]
    );
}

// D7: the worst-case cost of a track request bounds its bytes in every
// encoding, versions up to u64::MAX included (a property test), and each
// frame of a replay keeps within the budget.
#[test]
fn the_track_cost_bounds_every_encoding() {
    let mut rng = fastrand::Rng::with_seed(4747);
    let alphabet: Vec<char> = "ab\"\\\n\t\u{1}é€😀/<>".chars().collect();
    let text = |rng: &mut fastrand::Rng, max: usize| -> String {
        (0..rng.usize(0..max))
            .map(|_| alphabet[rng.usize(..alphabet.len())])
            .collect()
    };
    for _ in 0..200 {
        let channel = text(&mut rng, 40);
        let batches: Vec<TrackBatch> = (0..rng.usize(1..8))
            .map(|_| TrackBatch {
                signature: Secret(text(&mut rng, 120)),
                items: (0..rng.usize(1..12))
                    .map(|_| KeyedItem {
                        key: text(&mut rng, 40),
                        version: if rng.bool() { u64::MAX } else { rng.u64(..) },
                    })
                    .collect(),
            })
            .collect();
        let budget = rng.usize(400..6000);
        let frames = SharedPollFlow::track_frames(&channel, batches.clone(), budget);
        assert_eq!(frames.concat(), batches);
        for frame in frames {
            let cost = SharedPollFlow::track_cost(&channel, &frame);
            assert!(frame.len() == 1 || cost <= budget, "{cost} over {budget}");
            let command = Command {
                id: u32::MAX,
                method: Method::SubRefresh(SubRefreshRequest {
                    channel: channel.clone(),
                    kind: TRACK,
                    track: frame,
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

// D12 with Block 15: a reply read but held behind an event not taken stops
// its request's deadline, so the deadline passing then reconnects nothing.
#[test]
fn a_reply_read_behind_an_event_not_taken_stops_the_deadline() {
    let (mut machine, transport, id) = subscribed_emulated();
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let request = emulated_requests(&actions, 1)[0].clone();
    let timer = actions
        .iter()
        .find_map(|action| match action {
            Action::SetSharedPollTimer(timer, _) => Some(*timer),
            _ => None,
        })
        .unwrap();
    // An update not taken holds the messages read after it.
    let pushed = updates(&machine.handle(push(transport, "a", 1)));
    assert_eq!(pushed.len(), 1);

    let read = machine.handle(reply_to(transport, &request));
    let fired = machine.handle(Input::SharedPollTimerFired(timer));

    assert!(
        read.contains(&Action::CancelSharedPollTimer(timer)),
        "{read:?}"
    );
    assert!(
        !fired
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{fired:?}"
    );
}

// Codex on #218, 1 (D12): a subscription recreated after a removal waits
// for the removed one's requests still in flight on the channel, not only
// for the unsubscribe.
#[test]
fn a_recreated_subscription_waits_for_the_removed_ones_requests() {
    let (mut machine, transport, id) = subscribed_emulated();
    let request = emulated_requests(&track(&mut machine, id, &[("a", 0)], "sig1"), 1)[0].clone();
    let removed = machine.handle(Input::RemoveSubscription(id));
    let unsubscribe = sent_emulated(&removed)
        .into_iter()
        .find(|command| command.get("unsubscribe").is_some())
        .expect("an unsubscribe command");
    let again = machine
        .add_shared_poll_subscription("items".into(), options(false))
        .unwrap();
    machine.handle(Input::Subscribe(again));

    let unsubscribed = machine.handle(frame(
        transport,
        &serde_json::json!({"id": unsubscribe["id"], "unsubscribe": {}}).to_string(),
    ));
    let answered = machine.handle(reply_to(transport, &request));

    let subscribes = |actions: &[Action]| {
        sent_emulated(actions)
            .into_iter()
            .filter(|command| command.get("subscribe").is_some())
            .count()
    };
    assert_eq!(subscribes(&unsubscribed), 0, "{unsubscribed:?}");
    assert_eq!(subscribes(&answered), 1, "{answered:?}");
}

// Codex on #218, 2 (D5): unsubscribe() forgets the keys and signatures even
// when the subscription never subscribed.
#[test]
fn unsubscribe_before_any_subscribe_forgets_the_keys() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(false))
        .unwrap();
    track(&mut machine, id, &[("a", 3)], "sig1");

    machine.handle(Input::Unsubscribe(id));
    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);

    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(requests(&replay, 1), Vec::<Value>::new());
}

// Codex on #218, 3 (Block 18 D6, Block 8): a request of several commands
// refused with HTTP 413 resends a track request alone; one refused alone
// ends the subscription with unsubscribed 3, as any sub_refresh.
#[test]
fn a_track_request_refused_with_413_is_resent_alone_then_unsubscribes() {
    let (mut machine, transport, id) = subscribed_emulated();
    machine.handle(Input::StreamOpened(Some(id)));
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let body = actions
        .iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        })
        .unwrap();
    let line = String::from_utf8(Codec::Json.emulation_frame(&body).unwrap()).unwrap();
    let batched = Codec::Json.emulation_request(
        "s",
        "n",
        format!("{line}\n{}", r#"{"id":4242,"rpc":{"data":{}}}"#).as_bytes(),
    );

    let resent = machine.handle(Input::SendRejected {
        transport,
        frame: batched,
    });
    let resent_body = resent
        .iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        })
        .expect("the track request resent alone");
    assert_eq!(emulated_requests(&resent, 1).len(), 1, "{resent:?}");

    let refused = machine.handle(Input::SendRejected {
        transport,
        frame: resent_body,
    });

    assert!(
        events(&refused).iter().any(|(_, event)| matches!(
            event,
            SharedPollSubscriptionEvent::Unsubscribed { code: 3, .. }
        )),
        "{refused:?}"
    );
    assert!(
        sent_emulated(&refused)
            .iter()
            .any(|command| command.get("unsubscribe").is_some()),
        "{refused:?}"
    );
}

// Codex on #218, 4 (D9, Block 15): a delta among a track reply's items
// fails before any of the reply's updates is emitted, so no withdrawn
// update holds the next messages.
#[test]
fn a_delta_in_a_track_reply_holds_no_message() {
    let (mut machine, transport, id) = subscribed(false, true);
    let command =
        requests(&track(&mut machine, id, &[("a", 0), ("b", 0)], "s"), 1)[0]["id"].clone();

    let reply = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command, "sub_refresh": {"items": [
            {"key": "a", "data": {"v": 1}, "version": 1},
            {"key": "b", "data": "patch", "version": 1, "delta": true},
        ]}})
        .to_string(),
    ));
    // The application takes what reaches its stream: never a withdrawn
    // event.
    let withdrawn: Vec<EventId> = reply
        .iter()
        .filter_map(|action| match action {
            Action::Withdraw(ids) => Some(ids.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    for (event_id, _) in events(&reply) {
        if !withdrawn.contains(&event_id) {
            machine.handle(Input::EventTaken(event_id));
        }
    }

    assert!(!machine.events_hold_inbound(), "{reply:?}");
}

mod replay;
mod signatures;
