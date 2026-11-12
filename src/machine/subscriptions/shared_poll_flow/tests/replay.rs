//! Block 47c: the replay of a subscribe, epochs, and the retries of a
//! failing track (@E).

use std::collections::HashSet;
use std::time::Duration;

use super::*;

/// The timers set among `actions`, with their delays.
fn timers_set(actions: &[Action]) -> Vec<(SharedPollTimer, Duration)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::SetSharedPollTimer(timer, delay) => Some((*timer, *delay)),
            _ => None,
        })
        .collect()
}

/// The retry timers set among `actions`: those not of `Config::timeout`,
/// a request's reply deadline.
fn retries_set(actions: &[Action]) -> Vec<(SharedPollTimer, Duration)> {
    timers_set(actions)
        .into_iter()
        .filter(|(_, delay)| *delay != Config::default().timeout)
        .collect()
}

fn deadlines_set(actions: &[Action]) -> Vec<SharedPollTimer> {
    timers_set(actions)
        .into_iter()
        .filter(|(_, delay)| *delay == Config::default().timeout)
        .map(|(timer, _)| timer)
        .collect()
}

fn cancelled(actions: &[Action]) -> Vec<SharedPollTimer> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::CancelSharedPollTimer(timer) => Some(*timer),
            _ => None,
        })
        .collect()
}

fn errors(actions: &[Action]) -> Vec<ClientError> {
    events(actions)
        .into_iter()
        .filter_map(|(_, event)| match event {
            SharedPollSubscriptionEvent::Error(error) => Some(error),
            _ => None,
        })
        .collect()
}

/// The subscribe command among `actions`, answered with `result` as its
/// `subscribe`: the actions of the reply, which hold the replay.
fn subscribe_answered(
    machine: &mut Machine,
    transport: TransportId,
    actions: &[Action],
    result: &Value,
) -> Vec<Action> {
    let subscribe = sent(actions)
        .into_iter()
        .find(|command| command.get("subscribe").is_some())
        .expect("a subscribe command");
    let replay = machine.handle(frame(
        transport,
        &serde_json::json!({"id": subscribe["id"], "subscribe": result}).to_string(),
    ));
    take_all(machine, &replay);
    replay
}

/// Disconnects and connects again, answering the resubscribe with
/// `result`: the actions of the reply, which hold the replay.
fn reconnected(machine: &mut Machine, result: &Value) -> (TransportId, Vec<Action>) {
    let closed = machine.handle(Input::Disconnect);
    take_all(machine, &closed);
    let actions = machine.handle(Input::Connect);
    take_all(machine, &actions);
    let transport = opened(&actions).expect("a new transport");
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
    let replay = subscribe_answered(machine, transport, &actions, result);
    (transport, replay)
}

/// The server unsubscribes with 2500 and the resubscribe is answered with
/// `result`: the actions of the reply.
fn resubscribed_by_the_server(
    machine: &mut Machine,
    transport: TransportId,
    result: &Value,
) -> Vec<Action> {
    let actions = machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","unsubscribe":{"code":2500,"reason":"insufficient state"}}}"#,
    ));
    take_all(machine, &actions);
    subscribe_answered(machine, transport, &actions, result)
}

/// The version the replay among `actions` carries for its only key.
fn replayed_version(actions: &[Action]) -> u64 {
    let tracks = requests(actions, 1);
    assert_eq!(tracks.len(), 1, "{actions:?}");
    let items = items(&tracks[0]);
    assert_eq!(items.len(), 1, "{items:?}");
    items[0].1
}

// D8: a held version goes to 0 when the reply's epoch differs from the one
// kept, an absent one being "", but not on the first subscribe after
// `unsubscribed`, when none is kept.
#[test]
fn the_epoch_rule_resets_the_versions_on_any_change_but_the_first() {
    let (mut machine, transport) = connected();
    let id = machine
        .add_shared_poll_subscription("items".into(), options(false))
        .unwrap();
    track(&mut machine, id, &[("a", 5)], "sig1");

    let actions = machine.handle(Input::Subscribe(id));
    let replay = subscribe_answered(
        &mut machine,
        transport,
        &actions,
        &serde_json::json!({"type": 4, "epoch": "e1"}),
    );
    assert_eq!(replayed_version(&replay), 5, "none kept: kept as given");

    let (_, replay) = reconnected(&mut machine, &serde_json::json!({"type": 4, "epoch": "e1"}));
    assert_eq!(replayed_version(&replay), 5, "the same epoch");

    let (transport, replay) = reconnected(&mut machine, &serde_json::json!({"type": 4}));
    assert_eq!(replayed_version(&replay), 0, "an absent epoch is \"\"");

    let pushed = updates(&machine.handle(push(transport, "a", 7)));
    machine.handle(Input::EventTaken(pushed[0].0));
    let (transport, replay) = reconnected(&mut machine, &serde_json::json!({"type": 4}));
    assert_eq!(replayed_version(&replay), 7, "\"\" again");

    // The first epoch a backend reports, after an empty one.
    let replay = resubscribed_by_the_server(
        &mut machine,
        transport,
        &serde_json::json!({"type": 4, "epoch": "e2"}),
    );
    assert_eq!(replayed_version(&replay), 0, "from \"\" to e2");

    // unsubscribe() forgets the epoch with the keys.
    let unsubscribed = machine.handle(Input::Unsubscribe(id));
    let unsubscribe = sent(&unsubscribed)[0]["id"].clone();
    machine.handle(frame(
        transport,
        &serde_json::json!({"id": unsubscribe, "unsubscribe": {}}).to_string(),
    ));
    track(&mut machine, id, &[("a", 4)], "sig3");
    let actions = machine.handle(Input::Subscribe(id));
    let replay = subscribe_answered(
        &mut machine,
        transport,
        &actions,
        &serde_json::json!({"type": 4, "epoch": "e3"}),
    );
    assert_eq!(replayed_version(&replay), 4, "cold again after unsubscribe");
}

/// Answers every track and untrack request in turn, each once sent, until
/// none is left: the commands sent, with the bytes of each frame.
fn answer_all(
    machine: &mut Machine,
    transport: TransportId,
    first: &[Action],
) -> Vec<(Value, usize)> {
    let mut sent_all = Vec::new();
    let mut pending: std::collections::VecDeque<Value> = std::collections::VecDeque::new();
    let mut record = |actions: &[Action], pending: &mut std::collections::VecDeque<Value>| {
        for action in actions {
            if let Action::Send { frame, .. } = action {
                let text = std::str::from_utf8(frame).unwrap();
                for line in text.lines() {
                    let command: Value = serde_json::from_str(line).unwrap();
                    if command.get("sub_refresh").is_some() {
                        pending.push_back(command.clone());
                        sent_all.push((command, frame.len()));
                    }
                }
            }
        }
    };
    record(first, &mut pending);
    while let Some(command) = pending.pop_front() {
        let actions = machine.handle(reply_to(transport, &command));
        record(&actions, &mut pending);
    }
    sent_all
}

/// Keys `k000`.. in `signatures` signatures of `per_signature` keys each,
/// tracked before subscribing, all but the first of each then untracked;
/// the replay of the subscribe answered with a budget of `budget`.
fn mostly_untracked(
    budget: usize,
    signatures: usize,
    per_signature: usize,
    width: usize,
) -> (
    Machine,
    TransportId,
    SubscriptionId,
    Vec<Action>,
    Vec<Vec<String>>,
) {
    let (mut machine, transport) = connected();
    let mut options = options(false);
    options.max_track_frame_bytes = std::num::NonZeroUsize::new(budget).unwrap();
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    let mut groups = Vec::new();
    for number in 0..signatures {
        let keys: Vec<String> = (0..per_signature)
            .map(|n| format!("{:0>width$}", number * per_signature + n))
            .collect();
        let items: Vec<(&str, u64)> = keys.iter().map(|key| (key.as_str(), 0)).collect();
        let refused = track(&mut machine, id, &items, &format!("sig{number}"));
        assert!(errors(&refused).is_empty(), "{:?}", errors(&refused));
        machine.handle(Input::UntrackKeys(id, keys[1..].to_vec()));
        groups.push(keys);
    }
    let actions = machine.handle(Input::Subscribe(id));
    let replay = answer_subscribe(&mut machine, transport, &actions);
    (machine, transport, id, replay, groups)
}

/// The keys a track request's batches carry.
fn keys_of(request: &Value) -> HashSet<String> {
    items(request).into_iter().map(|(key, _)| key).collect()
}

/// The keys of a request's `untrack` field.
fn untrack_of(request: &Value) -> Vec<String> {
    request["sub_refresh"]["untrack"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|key| key.as_str().unwrap().to_owned())
        .collect()
}

// D7, D8 (47c): a key since untracked goes in the `untrack` field of the
// frame that carries its batch, whichever frame that is; a frame takes a
// batch with its untracks when both fit, else the batch starts the next
// frame. By the worst-case costs, a frame is 267 bytes, a batch of five
// keys 340 and its four untracks 64. At 1050 bytes a second batch fits a
// frame by itself (1011) but not with its untracks (1075): one per frame.
// At 1300 three batches fit by themselves (1287), two with their untracks
// and not three (1479): two per frame.
#[test]
fn each_replay_frame_untracks_the_keys_of_its_own_batches() {
    for (budget, per_frame) in [(1050, 1), (1300, 2)] {
        let (mut machine, transport, _, replay, groups) = mostly_untracked(budget, 6, 5, 4);

        let commands = answer_all(&mut machine, transport, &replay);

        let tracks: Vec<&Value> = commands
            .iter()
            .map(|(command, _)| command)
            .filter(|command| command["sub_refresh"]["type"] == 1)
            .collect();
        assert_eq!(tracks.len(), 6 / per_frame, "{budget}: {tracks:?}");
        for (number, request) in tracks.iter().enumerate() {
            let mut expected: Vec<String> = groups[number * per_frame..(number + 1) * per_frame]
                .iter()
                .flat_map(|keys| keys[1..].to_vec())
                .collect();
            let mut untracked = untrack_of(request);
            expected.sort();
            untracked.sort();
            assert_eq!(untracked, expected, "{budget}, frame {number}: {request}");
        }
        assert!(
            commands
                .iter()
                .all(|(command, size)| command["sub_refresh"]["type"] == 1 && *size <= budget),
            "{budget}: {commands:?}"
        );
    }
}

// D7, D8 (47c): a mostly untracked large key set (1800 keys of 44 bytes
// under three signatures, one key of each kept) cannot put its untracks in
// the frames of their batches: each frame stays within the budget, and
// what does not fit is untracked by untrack requests once its frame is
// answered. Inline, it would be a frame of about 120 KB, closed with 1009.
#[test]
fn a_replay_of_mostly_untracked_keys_stays_within_the_budget() {
    let (mut machine, transport, id, replay, groups) = mostly_untracked(60_000, 3, 600, 44);

    let commands = answer_all(&mut machine, transport, &replay);

    let mut untracked: Vec<String> = Vec::new();
    let mut tracked_by: Vec<HashSet<String>> = Vec::new();
    // Each key untracked by an untrack request, with the track frames sent
    // before it.
    let mut after: Vec<(String, usize)> = Vec::new();
    for (command, size) in &commands {
        assert!(*size <= 60_000, "a frame of {size} bytes");
        let doubly = serde_json::to_string(&command.to_string()).unwrap().len();
        assert!(
            doubly <= 60_000 - 128,
            "{doubly} bytes inside an emulation request"
        );
        match command["sub_refresh"]["type"].as_i64() {
            Some(1) => {
                let keys = keys_of(command);
                for key in untrack_of(command) {
                    assert!(
                        keys.contains(&key),
                        "{key} untracked by a frame not tracking it"
                    );
                    untracked.push(key);
                }
                tracked_by.push(keys);
            }
            Some(2) => {
                for key in untrack_of(command) {
                    after.push((key.clone(), tracked_by.len()));
                    untracked.push(key);
                }
            }
            other => panic!("{other:?}"),
        }
    }
    // After the frame that tracked the key, and no later frame tracks it
    // again.
    for (key, frames) in &after {
        let carrying: Vec<usize> = (0..tracked_by.len())
            .filter(|frame| tracked_by[*frame].contains(key))
            .collect();
        assert!(
            carrying.iter().all(|frame| frame < frames),
            "{key} untracked before a frame tracking it"
        );
    }
    assert_eq!(tracked_by.len(), 3, "one frame per signature");
    let mut expected: Vec<String> = groups.iter().flat_map(|keys| keys[1..].to_vec()).collect();
    untracked.sort();
    expected.sort();
    assert_eq!(untracked, expected);
    assert_eq!(machine.state(), State::Connected);
    assert_eq!(
        machine.shared_poll_tracked_keys(id).unwrap(),
        groups
            .iter()
            .map(|keys| keys[0].clone())
            .collect::<Vec<_>>()
    );
}

/// The error reply with `code` (and `temporary`) to `command`.
fn failing(transport: TransportId, command: &Value, code: u32, temporary: bool) -> Input {
    frame(
        transport,
        &serde_json::json!({"id": command["id"], "error": {"code": code, "message": "no", "temporary": temporary}})
            .to_string(),
    )
}

// D7 (47c): a temporary error or one below 100 replays the subscription's
// signatures after 1 s to 15 s, growing with each failure in a row and
// starting over once a track is accepted.
#[test]
fn a_temporary_failure_replays_after_a_growing_backoff() {
    let (mut machine, transport, id) = subscribed(false, false);
    track(&mut machine, id, &[("a", 0)], "sig1");
    let mut command = requests(&track(&mut machine, id, &[("b", 3)], "sig2"), 1)[0].clone();
    let mut delays = Vec::new();
    for attempt in 0..8 {
        let (code, temporary) = if attempt % 2 == 0 {
            (500, true)
        } else {
            (1, false)
        };
        let actions = machine.handle(failing(transport, &command, code, temporary));
        let error = &errors(&actions)[0];
        assert_eq!(
            (error.kind, error.code),
            (ErrorKind::Track, code),
            "{error:?}"
        );
        assert!(
            updates(&actions).is_empty() && requests(&actions, 2).is_empty(),
            "{actions:?}"
        );
        let retries = retries_set(&actions);
        assert_eq!(retries.len(), 1, "{actions:?}");
        let (timer, delay) = retries[0];
        assert!(
            (Duration::from_secs(1)..=Duration::from_secs(15)).contains(&delay),
            "{delay:?}"
        );
        delays.push(delay);
        let fired = machine.handle(Input::SharedPollTimerFired(timer));
        let replay = requests(&fired, 1);
        assert_eq!(replay.len(), 1, "{fired:?}");
        assert_eq!(items(&replay[0]), [("a".into(), 0), ("b".into(), 3)]);
        command = replay[0].clone();
    }
    assert!(delays[0] <= Duration::from_secs(2), "{delays:?}");
    assert!(
        delays.iter().any(|delay| *delay > Duration::from_secs(2)),
        "no backoff: {delays:?}"
    );

    machine.handle(reply_to(transport, &command));
    let command = requests(&track(&mut machine, id, &[("c", 0)], "sig3"), 1)[0].clone();
    let actions = machine.handle(failing(transport, &command, 500, true));
    let (_, delay) = retries_set(&actions)[0];
    assert!(
        delay <= Duration::from_secs(2),
        "not started over: {delay:?}"
    );
}

// D7 (47c): the retry ends when the subscription leaves `subscribed`, a
// firing then does nothing, and the next failure starts from 1 s.
#[test]
fn a_replay_retry_ends_when_the_subscription_leaves_subscribed() {
    let (mut machine, transport, id) = subscribed(false, false);
    let mut command = requests(&track(&mut machine, id, &[("a", 0)], "sig1"), 1)[0].clone();
    for _ in 0..6 {
        let actions = machine.handle(failing(transport, &command, 500, true));
        let timer = retries_set(&actions)[0].0;
        command = requests(&machine.handle(Input::SharedPollTimerFired(timer)), 1)[0].clone();
    }
    let actions = machine.handle(failing(transport, &command, 500, true));
    let timer = retries_set(&actions)[0].0;

    let lost = machine.handle(Input::Disconnect);
    assert!(cancelled(&lost).contains(&timer), "{lost:?}");
    let (transport, replay) = reconnected(&mut machine, &serde_json::json!({"type": 4}));
    let fired = machine.handle(Input::SharedPollTimerFired(timer));
    assert!(requests(&fired, 1).is_empty(), "{fired:?}");

    let command = requests(&replay, 1)[0].clone();
    let actions = machine.handle(failing(transport, &command, 500, true));
    let (timer, delay) = retries_set(&actions)[0];
    assert!(
        delay <= Duration::from_secs(2),
        "not started over: {delay:?}"
    );
    let removed = machine.handle(Input::RemoveSubscription(id));
    assert!(cancelled(&removed).contains(&timer), "{removed:?}");
}

// D7 (47c): over websocket, a track request without a reply within
// `Config::timeout` reports error 1 and replays after the backoff; an
// untrack only reports it; a late reply changes nothing; a reply stops the
// deadline.
#[test]
fn a_request_without_a_reply_on_websocket_reports_a_timeout_and_replays() {
    let (mut machine, transport, id) = subscribed(false, false);
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let command = requests(&actions, 1)[0].clone();
    let deadline = deadlines_set(&actions);
    assert_eq!(deadline.len(), 1, "{actions:?}");

    let fired = machine.handle(Input::SharedPollTimerFired(deadline[0]));
    let error = &errors(&fired)[0];
    assert_eq!(
        (error.kind, error.code),
        (ErrorKind::Track, codes::error::TIMEOUT)
    );
    assert_eq!(error.message, "timeout");
    assert_eq!(machine.state(), State::Connected);
    let (retry, delay) = retries_set(&fired)[0];
    assert!(delay <= Duration::from_secs(2), "{delay:?}");
    let late = machine.handle(frame(
        transport,
        &serde_json::json!({"id": command["id"], "sub_refresh": {"items": [{"key": "a", "data": {}, "version": 4}]}})
            .to_string(),
    ));
    assert!(updates(&late).is_empty(), "{late:?}");
    let replay = requests(&machine.handle(Input::SharedPollTimerFired(retry)), 1);
    assert_eq!(items(&replay[0]), [("a".into(), 0)]);

    let answered = machine.handle(reply_to(transport, &replay[0]));
    assert!(
        !cancelled(&answered).is_empty(),
        "the deadline stops: {answered:?}"
    );

    let actions = machine.handle(Input::UntrackKeys(id, vec!["a".into()]));
    let deadline = deadlines_set(&actions)[0];
    let fired = machine.handle(Input::SharedPollTimerFired(deadline));
    let error = &errors(&fired)[0];
    assert_eq!(
        (error.kind, error.code),
        (ErrorKind::Untrack, codes::error::TIMEOUT)
    );
    assert!(retries_set(&fired).is_empty(), "{fired:?}");
}

// I13 (47c): the reply deadline of a request made before the subscription
// last became `subscribed` changes nothing when it passes.
#[test]
fn the_deadline_of_an_earlier_subscribe_changes_nothing() {
    let (mut machine, transport, id) = subscribed(false, false);
    let actions = track(&mut machine, id, &[("a", 0)], "sig1");
    let deadline = deadlines_set(&actions)[0];
    resubscribed_by_the_server(&mut machine, transport, &serde_json::json!({"type": 4}));

    let fired = machine.handle(Input::SharedPollTimerFired(deadline));

    assert!(errors(&fired).is_empty(), "{fired:?}");
    assert!(timers_set(&fired).is_empty(), "{fired:?}");
    let _ = id;
}

// D7 (47c): one replay retry per subscription: a second failure replaces
// the timer of the first; close() ends it, and a firing then does nothing.
#[test]
fn one_replay_retry_per_subscription_and_close_ends_it() {
    let (mut machine, transport, id) = subscribed(false, false);
    let first = requests(&track(&mut machine, id, &[("a", 0)], "sig1"), 1)[0].clone();
    let second = requests(&track(&mut machine, id, &[("b", 0)], "sig2"), 1)[0].clone();

    let (timer, _) = retries_set(&machine.handle(failing(transport, &first, 500, true)))[0];
    let replaced = machine.handle(failing(transport, &second, 1, false));
    let (again, _) = retries_set(&replaced)[0];
    assert!(cancelled(&replaced).contains(&timer), "{replaced:?}");
    assert!(
        requests(&machine.handle(Input::SharedPollTimerFired(timer)), 1).is_empty(),
        "the replaced timer does nothing"
    );

    let closed = machine.handle(Input::Close);
    assert!(cancelled(&closed).contains(&again), "{closed:?}");
    let fired = machine.handle(Input::SharedPollTimerFired(again));
    assert!(sent(&fired).is_empty(), "{fired:?}");
}

// D7, D8 (47c): a failure on a later frame of a replay stops the frames
// after it; a temporary one replays from the first frame after the
// backoff, keeping every key.
#[test]
fn a_temporary_failure_of_a_later_replay_frame_replays_from_the_start() {
    let (mut machine, transport, id, replay) = replaying(1000, 30, 5);
    let first = requests(&replay, 1)[0].clone();
    let second = requests(&machine.handle(reply_to(transport, &first)), 1)[0].clone();

    let failed = machine.handle(failing(transport, &second, 500, true));
    assert!(
        requests(&failed, 1).is_empty(),
        "the rest stops: {failed:?}"
    );
    let (timer, _) = retries_set(&failed)[0];
    let again = requests(&machine.handle(Input::SharedPollTimerFired(timer)), 1);

    assert_eq!(again.len(), 1, "one frame at a time");
    assert_eq!(items(&again[0]), items(&first), "from the first frame");
    assert_eq!(machine.shared_poll_tracked_keys(id).unwrap().len(), 30);
}

// D8 (47c): an epoch that changes while a replay's frames are in flight
// abandons that replay: its replies change nothing, its frames not sent
// never go, and the new replay tracks every key once, from version 0.
#[test]
fn an_epoch_flip_during_a_replay_starts_it_over_from_version_0() {
    let (mut machine, transport, _, replay) = replaying(1000, 30, 5);
    let old = requests(&replay, 1)[0].clone();

    let flipped = resubscribed_by_the_server(
        &mut machine,
        transport,
        &serde_json::json!({"type": 4, "epoch": "e9"}),
    );
    let late = machine.handle(reply_to(transport, &old));
    assert!(requests(&late, 1).is_empty(), "{late:?}");

    let mut frames = requests(&flipped, 1);
    assert_eq!(frames.len(), 1, "{flipped:?}");
    loop {
        let next = requests(
            &machine.handle(reply_to(transport, frames.last().unwrap())),
            1,
        );
        if next.is_empty() {
            break;
        }
        frames.extend(next);
    }
    let carried: Vec<(String, u64)> = frames.iter().flat_map(items).collect();
    let expected: Vec<(String, u64)> = (0..30).map(|n| (format!("k{n:03}"), 0)).collect();
    assert_eq!(carried, expected);
}
