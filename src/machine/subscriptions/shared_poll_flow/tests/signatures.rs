//! Block 47b: signatures the client asks for and refreshes (@E).

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

fn timers_cancelled(actions: &[Action]) -> Vec<SharedPollTimer> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::CancelSharedPollTimer(timer) => Some(*timer),
            _ => None,
        })
        .collect()
}

/// The refresh timer settings among `actions`, as the owner runs them
/// (D7): the delays asked for, and whether one was cancelled.
fn refreshes_set(actions: &[Action], id: SubscriptionId) -> Vec<Duration> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::SetSharedPollRefresh(of, delay) if *of == id => Some(*delay),
            _ => None,
        })
        .collect()
}

fn refresh_cancelled(actions: &[Action], id: SubscriptionId) -> bool {
    actions
        .iter()
        .any(|action| matches!(action, Action::CancelSharedPollRefresh(of) if *of == id))
}

/// The one refresh delay set among `actions`.
fn the_refresh(actions: &[Action], id: SubscriptionId) -> Duration {
    let set = refreshes_set(actions, id);
    assert_eq!(set.len(), 1, "{actions:?}");
    assert!(
        timers_set(actions).is_empty(),
        "no other timer: {actions:?}"
    );
    set[0]
}

/// A refresh timer as the owner runs it: one per subscription, firing at
/// the earliest deadline asked for, on a clock the test moves.
#[derive(Default)]
struct RefreshTimer {
    now: Duration,
    deadline: Option<Duration>,
}

impl RefreshTimer {
    fn run(&mut self, actions: &[Action], id: SubscriptionId) {
        for action in actions {
            match action {
                Action::SetSharedPollRefresh(of, delay) if *of == id => {
                    let at = self.now + *delay;
                    self.deadline = Some(self.deadline.map_or(at, |set| set.min(at)));
                }
                Action::CancelSharedPollRefresh(of) if *of == id => self.deadline = None,
                _ => {}
            }
        }
    }
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

fn names(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|key| (*key).to_owned()).collect()
}

/// The callback answers `request` with `keys` signed by `signature`.
fn signed(
    machine: &mut Machine,
    id: SubscriptionId,
    request: FetchId,
    keys: &[&str],
    signature: &str,
) -> Vec<Action> {
    machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Ok(crate::SharedPollSignature::new(names(keys), signature)),
    })
}

fn failed(machine: &mut Machine, id: SubscriptionId, request: FetchId) -> Vec<Action> {
    machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Err(TokenError::Failed("backend down".into())),
    })
}

/// The reply to `command` with `result` as its `sub_refresh`.
fn answered(transport: TransportId, command: &Value, result: &Value) -> Input {
    frame(
        transport,
        &serde_json::json!({"id": command["id"], "sub_refresh": result}).to_string(),
    )
}

/// The error reply with `code` to `command`.
fn refused(transport: TransportId, command: &Value, code: u32) -> Input {
    frame(
        transport,
        &serde_json::json!({"id": command["id"], "error": {"code": code, "message": "no"}})
            .to_string(),
    )
}

fn the_fetch(actions: &[Action]) -> (FetchId, Vec<String>) {
    let mut fetches = fetches(actions);
    assert_eq!(fetches.len(), 1, "{actions:?}");
    fetches.pop().unwrap()
}

fn the_timer(actions: &[Action]) -> (SharedPollTimer, Duration) {
    let mut timers = timers_set(actions);
    assert_eq!(timers.len(), 1, "{actions:?}");
    timers.pop().unwrap()
}

fn the_track(actions: &[Action]) -> Value {
    let mut tracks = requests(actions, 1);
    assert_eq!(tracks.len(), 1, "{actions:?}");
    tracks.pop().unwrap()
}

fn tracked_keys(machine: &Machine, id: SubscriptionId) -> Vec<String> {
    machine.shared_poll_tracked_keys(id).unwrap()
}

// D6: without a callback, track(keys) reports error 14 of kind Track and
// keeps its keys; the replay of the next subscribe reports it again.
#[test]
fn a_key_set_without_a_callback_reports_error_14() {
    let (mut machine, transport, id) = subscribed(false, false);

    let actions = machine.handle(Input::TrackKeys(id, vec!["a".into()]));
    let (_, replay) = reconnect(&mut machine, transport);

    for actions in [&actions, &replay] {
        let got = errors(actions);
        assert_eq!(got.len(), 1, "{actions:?}");
        assert_eq!(
            (got[0].kind, got[0].code, got[0].message.as_str()),
            (
                ErrorKind::Track,
                14,
                "get_signature callback required for track(keys)"
            )
        );
        assert!(requests(actions, 1).is_empty() && fetches(actions).is_empty());
    }
    assert_eq!(tracked_keys(&machine, id), ["a"]);
}

// D6: a key asked for and not returned is revoked: a removal, an untrack,
// and the keys returned tracked.
#[test]
fn a_key_the_callback_does_not_return_is_revoked() {
    let (mut machine, _, id) = subscribed(true, false);
    let (request, keys) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a", "b"]))));
    assert_eq!(keys, ["a", "b"]);

    let actions = signed(&mut machine, id, request, &["a"], "sig1");

    let removed: Vec<(String, bool)> = updates(&actions)
        .into_iter()
        .map(|(_, update)| (update.key, update.removed))
        .collect();
    assert_eq!(removed, [("b".to_owned(), true)]);
    assert_eq!(
        requests(&actions, 2)[0]["sub_refresh"]["untrack"],
        serde_json::json!(["b"])
    );
    let track = the_track(&actions);
    assert_eq!(items(&track), [("a".into(), 0)]);
    assert_eq!(tracked_keys(&machine, id), ["a"]);
}

// D6: a key untracked while the callback runs goes in the batch, which
// the signature covers, and in the request's untrack field; the signature
// is kept for the next replay.
#[test]
fn a_key_untracked_while_signing_goes_in_the_batch_and_its_untrack() {
    let (mut machine, transport, id) = subscribed(true, false);
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a", "b"]))));
    machine.handle(Input::UntrackKeys(id, names(&["a"])));

    let actions = signed(&mut machine, id, request, &["a", "b"], "sig1");

    let track = the_track(&actions);
    assert_eq!(items(&track), [("a".into(), 0), ("b".into(), 0)]);
    assert_eq!(track["sub_refresh"]["untrack"], serde_json::json!(["a"]));
    assert!(updates(&actions).is_empty(), "nothing revoked");
    let (_, replay) = reconnect(&mut machine, transport);
    assert_eq!(
        requests(&replay, 1)[0]["sub_refresh"]["track"][0]["signature"],
        "sig1"
    );
    assert!(fetches(&replay).is_empty(), "{replay:?}");
}

// D6, D7: a failing callback reports error 14 (of kind Track on the track
// path, SignatureRefresh on its retries) and retries after Block 2's
// backoff between 5 s and 30 s, growing with each failure and starting
// over after a success.
#[test]
fn a_failing_callback_retries_with_a_growing_backoff() {
    let (mut machine, _, id) = subscribed(true, false);
    let (mut request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a"]))));
    let mut delays = Vec::new();
    for attempt in 0..8 {
        let actions = failed(&mut machine, id, request);
        let error = &errors(&actions)[0];
        let kind = if attempt == 0 {
            ErrorKind::Track
        } else {
            ErrorKind::SignatureRefresh
        };
        assert_eq!((error.kind, error.code), (kind, 14), "{error:?}");
        assert!(requests(&actions, 1).is_empty(), "{actions:?}");
        let (timer, delay) = the_timer(&actions);
        assert!(
            (Duration::from_secs(5)..=Duration::from_secs(30)).contains(&delay),
            "{delay:?}"
        );
        delays.push(delay);
        let (next, keys) = the_fetch(&machine.handle(Input::SharedPollTimerFired(timer)));
        assert_eq!(keys, ["a"]);
        request = next;
    }
    assert!(delays[0] <= Duration::from_secs(10), "{delays:?}");
    assert!(
        delays.iter().any(|delay| *delay > Duration::from_secs(10)),
        "no backoff: {delays:?}"
    );

    let actions = signed(&mut machine, id, request, &["a"], "sig1");
    assert_eq!(items(&the_track(&actions)), [("a".into(), 0)]);
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["b"]))));
    let (_, delay) = the_timer(&failed(&mut machine, id, request));
    assert!(
        delay <= Duration::from_secs(10),
        "not started over: {delay:?}"
    );
}

// D6: Unauthorized means the user may not use the subscription any more.
#[test]
fn an_unauthorized_callback_unsubscribes_with_code_1() {
    let (mut machine, _, id) = subscribed(true, false);
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a"]))));

    let actions = machine.handle(Input::SignatureFetched {
        subscription: id,
        request,
        result: Err(TokenError::Unauthorized),
    });

    assert!(events(&actions).iter().any(|(_, event)| matches!(
        event,
        SharedPollSubscriptionEvent::Unsubscribed { code: 1, reason } if reason == "unauthorized"
    )));
    assert!(
        sent(&actions)
            .iter()
            .any(|command| command["unsubscribe"]["channel"] == "items")
    );
    assert!(errors(&actions).is_empty(), "{actions:?}");
    assert_eq!(
        machine.subscription_state(id),
        Some(SubscriptionState::Unsubscribed)
    );
}

// D7: replies with `expires` make the refresh run at the earliest
// deadline seen, one timer per subscription, however many replies with
// ever shorter ttls come; when it comes, every tracked key is signed again,
// and the new signature replaces every one held whose tracked keys it
// covers.
#[test]
fn an_expiring_signature_is_refreshed_at_the_earliest_deadline() {
    let (mut machine, transport, id) = subscribed(true, false);
    let mut owner = RefreshTimer::default();
    // Signatures under one exp: each reply a second later says 1 s less.
    for (second, key) in ["a", "b", "c", "d", "e", "f"].iter().enumerate() {
        owner.now = Duration::from_secs(second as u64);
        let command = the_track(&track(
            &mut machine,
            id,
            &[(*key, 0)],
            &format!("sig-{key}"),
        ));
        let ttl = 60 - second as u64;
        let actions = machine.handle(answered(
            transport,
            &command,
            &serde_json::json!({"expires": true, "ttl": ttl}),
        ));
        assert_eq!(the_refresh(&actions, id), Duration::from_secs(ttl));
        owner.run(&actions, id);
    }
    // A later deadline moves nothing.
    let command = the_track(&track(&mut machine, id, &[("g", 0)], "sig-g"));
    let later = machine.handle(answered(
        transport,
        &command,
        &serde_json::json!({"expires": true, "ttl": 600}),
    ));
    owner.run(&later, id);
    assert_eq!(owner.deadline, Some(Duration::from_secs(60)));
    // A signature held for "a" with a key since untracked: the refresh
    // replaces it, though it does not cover "x".
    the_track(&track(&mut machine, id, &[("a", 0), ("x", 0)], "sig-ax"));
    machine.handle(Input::UntrackKeys(id, names(&["x"])));

    let fired = machine.handle(Input::SharedPollRefreshFired(id));
    let (request, keys) = the_fetch(&fired);
    let all = ["a", "b", "c", "d", "e", "f", "g"];
    assert_eq!(keys, all);
    let actions = signed(&mut machine, id, request, &all, "sig-all");
    let track = the_track(&actions);
    assert_eq!(track["sub_refresh"]["track"][0]["signature"], "sig-all");

    let (_, replay) = reconnect(&mut machine, transport);
    let replayed = &requests(&replay, 1)[0]["sub_refresh"]["track"];
    assert_eq!(replayed.as_array().unwrap().len(), 1, "{replayed}");
    assert_eq!(replayed[0]["signature"], "sig-all");
}

// D7: a refresh run for a 109 ends the timer armed for an expiry; a
// firing with none armed does nothing.
#[test]
fn a_refresh_ends_the_armed_timer_and_a_stray_firing_does_nothing() {
    let (mut machine, transport, id) = subscribed(true, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0)], "sig-a"));
    the_refresh(
        &machine.handle(answered(
            transport,
            &command,
            &serde_json::json!({"expires": true, "ttl": 60}),
        )),
        id,
    );
    let command = the_track(&track(&mut machine, id, &[("b", 0)], "sig-b"));

    let refreshed = machine.handle(refused(transport, &command, 109));

    assert!(refresh_cancelled(&refreshed, id), "{refreshed:?}");
    let (request, _) = the_fetch(&refreshed);
    the_track(&signed(&mut machine, id, request, &["a", "b"], "sig-ab"));
    let stray = machine.handle(Input::SharedPollRefreshFired(id));
    assert!(fetches(&stray).is_empty(), "{stray:?}");
}

// D6: without a callback, refreshing is the application's job.
#[test]
fn without_a_callback_no_refresh_is_scheduled() {
    let (mut machine, transport, id) = subscribed(false, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0)], "sig"));

    let actions = machine.handle(answered(
        transport,
        &command,
        &serde_json::json!({"expires": true, "ttl": 5}),
    ));

    assert!(
        timers_set(&actions).is_empty() && refreshes_set(&actions, id).is_empty(),
        "{actions:?}"
    );
}

// D7: a signature accepted within the grace period is refreshed after
// 1 s; replies without ttl in a row back off up to 15 s, and a reply with
// a ttl starts over.
#[test]
fn replies_without_ttl_in_a_row_back_off_from_1_s() {
    let (mut machine, transport, id) = subscribed(true, false);
    let mut command = the_track(&track(&mut machine, id, &[("a", 0)], "sig0"));
    let mut delays = Vec::new();
    for _ in 0..8 {
        let delay = the_refresh(
            &machine.handle(answered(
                transport,
                &command,
                &serde_json::json!({"expires": true}),
            )),
            id,
        );
        delays.push(delay);
        let (request, _) = the_fetch(&machine.handle(Input::SharedPollRefreshFired(id)));
        command = the_track(&signed(&mut machine, id, request, &["a"], "sig"));
    }
    assert_eq!(delays[0], Duration::from_secs(1), "{delays:?}");
    assert!(
        delays
            .iter()
            .all(|delay| (Duration::from_secs(1)..=Duration::from_secs(15)).contains(delay)),
        "{delays:?}"
    );
    assert!(
        delays.iter().any(|delay| *delay > Duration::from_secs(2)),
        "no backoff: {delays:?}"
    );

    let delay = the_refresh(
        &machine.handle(answered(
            transport,
            &command,
            &serde_json::json!({"expires": true, "ttl": 10}),
        )),
        id,
    );
    assert_eq!(delay, Duration::from_secs(10));
    let (request, _) = the_fetch(&machine.handle(Input::SharedPollRefreshFired(id)));
    let command = the_track(&signed(&mut machine, id, request, &["a"], "sig"));
    let delay = the_refresh(
        &machine.handle(answered(
            transport,
            &command,
            &serde_json::json!({"expires": true}),
        )),
        id,
    );
    assert_eq!(delay, Duration::from_secs(1), "started over");
}

// D7: 109 refreshes the signatures at once; a 109 to the refresh's own
// track request fails the refresh, which is retried after 5 s to 30 s,
// growing until a refresh is accepted.
#[test]
fn a_109_refreshes_at_once_and_a_109_to_a_refresh_backs_off() {
    let (mut machine, transport, id) = subscribed(true, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0)], "old"));

    let actions = machine.handle(refused(transport, &command, 109));
    assert_eq!(
        errors(&actions)
            .iter()
            .map(|e| (e.kind, e.code))
            .collect::<Vec<_>>(),
        [(ErrorKind::Track, 109)]
    );
    let (mut request, keys) = the_fetch(&actions);
    assert_eq!(keys, ["a"]);
    let mut delays = Vec::new();
    for _ in 0..8 {
        let command = the_track(&signed(&mut machine, id, request, &["a"], "expired"));
        let actions = machine.handle(refused(transport, &command, 109));
        assert_eq!(errors(&actions)[0].code, 109);
        assert!(fetches(&actions).is_empty(), "not at once: {actions:?}");
        let (timer, delay) = the_timer(&actions);
        assert!(
            (Duration::from_secs(5)..=Duration::from_secs(30)).contains(&delay),
            "{delay:?}"
        );
        delays.push(delay);
        request = the_fetch(&machine.handle(Input::SharedPollTimerFired(timer))).0;
    }
    assert!(
        delays.iter().any(|delay| *delay > Duration::from_secs(10)),
        "no backoff: {delays:?}"
    );

    // Accepted: the next failure starts the backoff over.
    let command = the_track(&signed(&mut machine, id, request, &["a"], "fresh"));
    machine.handle(answered(transport, &command, &serde_json::json!({})));
    let command = the_track(&track(&mut machine, id, &[("b", 0)], "old"));
    let (request, _) = the_fetch(&machine.handle(refused(transport, &command, 109)));
    let command = the_track(&signed(&mut machine, id, request, &["a", "b"], "expired"));
    let (_, delay) = the_timer(&machine.handle(refused(transport, &command, 109)));
    assert!(
        delay <= Duration::from_secs(10),
        "not started over: {delay:?}"
    );
}

// D7: one refresh at a time; a key it does not cover runs another once it
// ends.
#[test]
fn a_refresh_runs_one_at_a_time_and_again_for_keys_it_did_not_cover() {
    let (mut machine, transport, id) = subscribed(true, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0)], "old-a"));
    let (request, _) = the_fetch(&machine.handle(refused(transport, &command, 109)));
    let command = the_track(&track(&mut machine, id, &[("b", 0)], "old-b"));

    let second = machine.handle(refused(transport, &command, 109));
    assert!(fetches(&second).is_empty(), "one at a time: {second:?}");

    let actions = signed(&mut machine, id, request, &["a"], "new-a");
    assert_eq!(items(&the_track(&actions)), [("a".into(), 0)]);
    let (_, keys) = the_fetch(&actions);
    assert_eq!(keys, ["a", "b"]);
}

/// The worst-case cost of a batch of `keys` under a signature of 128
/// bytes, in a track request on "items" (D6).
fn chunk_cost(keys: &[String]) -> usize {
    SharedPollFlow::track_cost(
        "items",
        [&TrackBatch {
            signature: Secret("s".repeat(128)),
            items: keys
                .iter()
                .map(|key| KeyedItem {
                    key: key.clone(),
                    version: u64::MAX,
                })
                .collect(),
        }],
    )
}

/// A subscription with a frame budget of 1000 bytes, subscribed, which
/// tracked 30 keys: the machine, transport, id and the first fetch.
fn tracking_many() -> (Machine, TransportId, SubscriptionId, Vec<Action>) {
    let (mut machine, transport) = connected();
    let mut options = options(true);
    options.max_track_frame_bytes = std::num::NonZeroUsize::new(1000).unwrap();
    let id = machine
        .add_shared_poll_subscription("items".into(), options)
        .unwrap();
    let actions = machine.handle(Input::Subscribe(id));
    take_all(&mut machine, &actions);
    let answered = answer_subscribe(&mut machine, transport, &actions);
    take_all(&mut machine, &answered);
    let keys: Vec<String> = (0..30).map(|n| format!("k{n:03}")).collect();
    let fetched = machine.handle(Input::TrackKeys(id, keys));
    (machine, transport, id, fetched)
}

// D6, D7: a key set whose batch would pass the budget is signed in chunks,
// one at a time, each within the budget at any version; the result is
// tracked in frames within the budget, one after the other.
#[test]
fn a_key_set_over_the_budget_is_signed_in_chunks() {
    let (mut machine, transport, id, mut fetched) = tracking_many();
    let mut asked: Vec<Vec<String>> = Vec::new();
    let mut actions = Vec::new();
    while let [(request, keys)] = fetches(&fetched).as_slice() {
        assert!(chunk_cost(keys) <= 1000, "{} keys", keys.len());
        assert!(requests(&fetched, 1).is_empty(), "before the last chunk");
        asked.push(keys.clone());
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        fetched = signed(
            &mut machine,
            id,
            *request,
            &keys,
            &format!("sig{}", asked.len()),
        );
        actions.clone_from(&fetched);
    }
    assert!(asked.len() > 1, "{asked:?}");
    assert_eq!(asked.concat(), tracked_keys(&machine, id));

    let mut frames = vec![the_track(&actions)];
    loop {
        let next = requests(
            &machine.handle(answered(
                transport,
                frames.last().unwrap(),
                &serde_json::json!({}),
            )),
            1,
        );
        match next.len() {
            0 => break,
            1 => frames.push(next[0].clone()),
            more => panic!("{more} frames at once"),
        }
    }
    let mut keys = Vec::new();
    for frame in &frames {
        assert!(frame.to_string().len() <= 1000, "{frame}");
        keys.extend(items(frame).into_iter().map(|(key, _)| key));
    }
    assert_eq!(keys, tracked_keys(&machine, id));
}

// D6: a chunk that fails fails the whole request; the retry asks again
// from the first chunk.
#[test]
fn a_failing_chunk_fails_the_whole_request() {
    let (mut machine, _, id, fetched) = tracking_many();
    let (first, first_keys) = the_fetch(&fetched);
    let keys: Vec<&str> = first_keys.iter().map(String::as_str).collect();
    let (second, _) = the_fetch(&signed(&mut machine, id, first, &keys, "sig1"));

    let actions = failed(&mut machine, id, second);

    assert_eq!(errors(&actions)[0].code, 14);
    assert!(requests(&actions, 1).is_empty(), "{actions:?}");
    let (timer, _) = the_timer(&actions);
    let (_, again) = the_fetch(&machine.handle(Input::SharedPollTimerFired(timer)));
    assert_eq!(again, first_keys);
}

// D6: a signature whose batch would pass the budget (more keys than asked
// for) is refused before it is sent, and retried as a failed fetch.
#[test]
fn a_signature_over_the_budget_is_refused_and_retried() {
    let (mut machine, _, id, _) = tracking_many();
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a"]))));
    let many: Vec<String> = (0..40).map(|n| format!("x{n:03}")).collect();
    let many: Vec<&str> = std::iter::once("a")
        .chain(many.iter().map(String::as_str))
        .collect();

    let actions = signed(&mut machine, id, request, &many, "sig");

    assert!(requests(&actions, 1).is_empty(), "{actions:?}");
    let got = errors(&actions);
    assert_eq!((got[0].kind, got[0].code), (ErrorKind::Track, 12));
    assert!(updates(&actions).is_empty(), "nothing revoked");
    the_timer(&actions);
    assert!(tracked_keys(&machine, id).contains(&"a".to_owned()));
}

// D7: keys of a track request no longer tracked when its reply comes, as
// a key removed meanwhile, are untracked on the server.
#[test]
fn a_track_reply_untracks_the_keys_no_longer_tracked() {
    let (mut machine, transport, id) = subscribed(false, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0), ("b", 0)], "sig"));
    machine.handle(frame(
        transport,
        r#"{"push":{"channel":"items","pub":{"key":"a","removed":true}}}"#,
    ));

    let actions = machine.handle(answered(transport, &command, &serde_json::json!({})));

    let untracks = requests(&actions, 2);
    assert_eq!(untracks.len(), 1, "{actions:?}");
    assert_eq!(
        untracks[0]["sub_refresh"]["untrack"],
        serde_json::json!(["a"])
    );
    assert_eq!(tracked_keys(&machine, id), ["b"]);
}

// I13: what a subscription asked for or scheduled before it left
// `subscribed` changes nothing after: an answer, a chunk's answer, a
// refresh or a retry timer; a new subscribe cancels the timers, as do
// unsubscribe() and a removal.
#[test]
fn what_was_asked_before_leaving_subscribed_changes_nothing() {
    // A refresh timer and a retry timer, then a resubscribe.
    let (mut machine, transport, id) = subscribed(true, false);
    let command = the_track(&track(&mut machine, id, &[("a", 0)], "sig"));
    the_refresh(
        &machine.handle(answered(
            transport,
            &command,
            &serde_json::json!({"expires": true, "ttl": 60}),
        )),
        id,
    );
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["b"]))));
    let (retry, _) = the_timer(&failed(&mut machine, id, request));
    let lost = machine.handle(Input::Disconnect);
    take_all(&mut machine, &lost);
    // Leaving `subscribed` stops them.
    assert!(refresh_cancelled(&lost, id), "{lost:?}");
    assert!(timers_cancelled(&lost).contains(&retry), "{lost:?}");
    let (_, replay) = reconnect(&mut machine, transport);
    assert!(refreshes_set(&replay, id).is_empty(), "{replay:?}");
    for fired in [
        machine.handle(Input::SharedPollRefreshFired(id)),
        machine.handle(Input::SharedPollTimerFired(retry)),
    ] {
        assert!(fetches(&fired).is_empty(), "{fired:?}");
    }

    // A chunked request, its second chunk answered after a reconnect.
    let (mut machine, transport, id, fetched) = tracking_many();
    let (first, keys) = the_fetch(&fetched);
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let (second, second_keys) = the_fetch(&signed(&mut machine, id, first, &keys, "sig1"));
    let (_, replay) = reconnect(&mut machine, transport);
    let second_keys: Vec<&str> = second_keys.iter().map(String::as_str).collect();
    let late = signed(&mut machine, id, second, &second_keys, "sig2");
    assert!(
        requests(&late, 1).is_empty() && fetches(&late).is_empty(),
        "{late:?}"
    );
    assert!(!fetches(&replay).is_empty(), "the replay asks again");

    // unsubscribe() and a removal cancel the timers.
    for remove in [false, true] {
        let (mut machine, transport, id) = subscribed(true, false);
        let command = the_track(&track(&mut machine, id, &[("a", 0)], "sig"));
        the_refresh(
            &machine.handle(answered(
                transport,
                &command,
                &serde_json::json!({"expires": true, "ttl": 60}),
            )),
            id,
        );
        let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["b"]))));
        let (retry, _) = the_timer(&failed(&mut machine, id, request));
        let actions = machine.handle(if remove {
            Input::RemoveSubscription(id)
        } else {
            Input::Unsubscribe(id)
        });
        assert!(refresh_cancelled(&actions, id), "{actions:?}");
        assert!(timers_cancelled(&actions).contains(&retry), "{actions:?}");
    }
}

// I13: an answer that comes while the subscription is not subscribed, the
// connection lost, revokes nothing and tracks nothing; the next subscribe
// asks again.
#[test]
fn an_answer_while_not_subscribed_changes_nothing() {
    let (mut machine, transport, id) = subscribed(true, false);
    let (request, _) = the_fetch(&machine.handle(Input::TrackKeys(id, names(&["a", "b"]))));
    let lost = machine.handle(Input::Disconnect);
    take_all(&mut machine, &lost);

    let late = signed(&mut machine, id, request, &["a"], "sig1");

    assert!(updates(&late).is_empty(), "{late:?}");
    assert_eq!(tracked_keys(&machine, id), ["a", "b"]);
    let (_, replay) = reconnect(&mut machine, transport);
    assert_eq!(the_fetch(&replay).1, ["a", "b"]);
}

// D6: a chunk is sized for the longest signature received, at least 128
// bytes: a backend signing with 300 bytes gets one refusal for the first
// chunk, sized before any was known, then chunks its signatures fit.
#[test]
fn chunks_are_sized_for_the_longest_signature_received() {
    let (mut machine, transport, id, fetched) = tracking_many();
    let long = "s".repeat(300);
    let (request, keys) = the_fetch(&fetched);
    let names: Vec<&str> = keys.iter().map(String::as_str).collect();
    let refused = signed(&mut machine, id, request, &names, &long);
    assert_eq!(errors(&refused)[0].code, 12, "the first chunk: {refused:?}");
    let (timer, _) = the_timer(&refused);

    let mut fetched = machine.handle(Input::SharedPollTimerFired(timer));
    let mut actions = Vec::new();
    let mut chunks = 0;
    while let [(request, keys)] = fetches(&fetched).as_slice() {
        chunks += 1;
        let names: Vec<&str> = keys.iter().map(String::as_str).collect();
        fetched = signed(&mut machine, id, *request, &names, &long);
        assert!(errors(&fetched).is_empty(), "chunk {chunks}: {fetched:?}");
        actions.clone_from(&fetched);
    }
    assert!(chunks > 1);
    let mut frames = vec![the_track(&actions)];
    loop {
        let next = requests(
            &machine.handle(answered(
                transport,
                frames.last().unwrap(),
                &serde_json::json!({}),
            )),
            1,
        );
        match next.as_slice() {
            [] => break,
            [frame] => frames.push(frame.clone()),
            more => panic!("{} frames at once", more.len()),
        }
    }
    let mut tracked = Vec::new();
    for frame in &frames {
        assert!(frame.to_string().len() <= 1000, "{frame}");
        tracked.extend(items(frame).into_iter().map(|(key, _)| key));
    }
    assert_eq!(tracked, tracked_keys(&machine, id));
}
