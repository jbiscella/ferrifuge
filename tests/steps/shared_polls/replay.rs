//! Block 47c: keys kept across reconnects, replayed with the signatures
//! and versions held, epochs, and the retries of a failing track.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::{ErrorKind, SharedPollSubscriptionEvent, SharedPollTrackItem, SubscriptionState};
use serde_json::{Value, json};

use super::super::connection::a_connected_client;
use super::super::delivery::wait_until;
use super::super::{ClientWorld, QUIET_PERIOD};
use super::{
    a_shared_poll_subscription, backend, events_within, items_of, next_of, next_update, nth_track,
    server_api, signature_calls, signing_with, sub_refreshes, subscribed, track_requests,
    track_validly, tracked_on_the_fake,
};
use crate::support::http_proxy::HttpProxy;
use crate::support::relay::Mode;

/// The subscriptions of the 47c scenarios on the fake server, subscribed;
/// false for a description it does not know.
pub(super) async fn a_replaying_subscription(
    world: &mut ClientWorld,
    channel: &str,
    how: &str,
) -> bool {
    let calls = Arc::clone(&world.shared_polls.signature_calls);
    match how {
        r#" subscribed, tracking "a" with signature "sig1" from get_signature"# => {
            a_shared_poll_subscription(world, channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig1"))
            });
            subscribed(world).await;
            world.shared_poll().track(["a"]);
            nth_track(world, channel, 1).await;
        }
        r#" subscribed, tracking "a" and "b" under signature "sig1""# => {
            a_shared_poll_subscription(world, channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 0), ("b", 0)]).await;
        }
        r#" subscribed, tracking "a" under signature "sig1""# => {
            a_shared_poll_subscription(world, channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 0)]).await;
        }
        r#" subscribed, tracking "a" under signature "sig1", with a get_signature callback"# => {
            a_shared_poll_subscription(world, channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig2"))
            });
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 0)]).await;
        }
        r#" subscribed at epoch "e1", tracking "a" at version 9"# => {
            world
                .fake()
                .answer_a_subscribe_with(json!({"type": 4, "epoch": "e1"}));
            a_shared_poll_subscription(world, channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 9)]).await;
        }
        r#" subscribed with no epoch, tracking "a" at version 9"# => {
            a_shared_poll_subscription(world, channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 9)]).await;
        }
        r#" with get_signature that signs what it is given with "sig1", subscribed, whose stream was taken"# =>
        {
            a_shared_poll_subscription(world, channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig1"))
            });
            subscribed(world).await;
        }
        r#" with a get_signature callback, subscribed, tracking "a", whose stream was taken"# => {
            a_shared_poll_subscription(world, channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig1"))
            });
            subscribed(world).await;
            world.shared_poll().track(["a"]);
            nth_track(world, channel, 1).await;
        }
        r#" tracking "a" at version 2, whose stream has been read up to "subscribed""# => {
            // The stream itself, read only by the steps: what they do not
            // read is not taken (Block 15).
            let subscription = world
                .client()
                .shared_poll_subscription_builder(channel)
                .build()
                .unwrap();
            world.shared_polls.events = Some(Box::pin(subscription.events()));
            world.shared_polls.subscription = Some(subscription);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 2)]).await;
        }
        _ => return false,
    }
    true
}

/// Waits until the subscription is no longer subscribed, past its events.
async fn subscribing(world: &mut ClientWorld) {
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribing { .. } => return,
            SharedPollSubscriptionEvent::Update(_) => {}
            other => panic!("expected subscribing, got {other:?}"),
        }
    }
}

/// Waits until the subscription is subscribed again, past its events.
async fn subscribed_again(world: &mut ClientWorld) {
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(_) => return,
            SharedPollSubscriptionEvent::Subscribing { .. }
            | SharedPollSubscriptionEvent::Update(_) => {}
            other => panic!("expected subscribed, got {other:?}"),
        }
    }
}

/// Marks the track requests received so far, so that the next is the
/// replay's.
fn mark(world: &mut ClientWorld) {
    let channel = world.shared_poll().channel().to_owned();
    world.shared_polls.sub_refreshes_before = track_requests(world, &channel).len();
}

/// Closes the transport while the relay holds the client's next
/// connection, and waits until the subscription is subscribing: a step can
/// act while the client is away, however slow the runner.
async fn closed_and_held(world: &mut ClientWorld) {
    mark(world);
    world.relay.as_ref().expect("a relay").set_mode(Mode::Hold);
    assert!(world.fake().close_current(3001, "reconnect"));
    subscribing(world).await;
    assert_eq!(world.shared_poll().state(), SubscriptionState::Subscribing);
}

#[when(
    regex = r#"^the transport closes and the client reconnects and the subscribe is answered(?: with epoch "(\w+)")?$"#
)]
async fn reconnects_and_answered(world: &mut ClientWorld, epoch: String) {
    mark(world);
    if !epoch.is_empty() {
        world
            .fake()
            .answer_a_subscribe_with(json!({"type": 4, "epoch": epoch}));
    }
    assert!(world.fake().close_current(3001, "reconnect"));
    subscribing(world).await;
    subscribed_again(world).await;
    // The client's own events of that reconnect, so that a later step sees
    // only what follows.
    while !matches!(
        world.next_event().await,
        ferrifuge::ClientEvent::Connected { .. }
    ) {}
}

#[when(regex = r#"^the transport closes and untrack is called with key "(\w+)"$"#)]
async fn untracked_while_away(world: &mut ClientWorld, key: String) {
    closed_and_held(world).await;
    world.shared_poll().untrack([key]);
}

#[when(regex = r#"^the client is not subscribed and track is called with key "(\w+)"$"#)]
async fn tracked_while_away(world: &mut ClientWorld, key: String) {
    closed_and_held(world).await;
    world.shared_poll().track([key]);
}

#[when("the client reconnects and the subscribe is answered")]
async fn the_client_comes_back(world: &mut ClientWorld) {
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
    subscribed_again(world).await;
}

/// The replay's track request `n` (from 1).
async fn replayed(world: &ClientWorld, n: usize) -> Value {
    let channel = world.shared_poll().channel().to_owned();
    nth_track(world, &channel, world.shared_polls.sub_refreshes_before + n).await
}

#[then(regex = r#"^the replay's track request carries signature "(\w+)" and item "(\w+)"$"#)]
async fn replay_with_signature(world: &mut ClientWorld, signature: String, key: String) {
    let replay = replayed(world, 1).await;
    assert_eq!(
        replay["track"][0]["signature"],
        signature.as_str(),
        "{replay}"
    );
    assert_eq!(items_of(&replay), [(key, None)], "{replay}");
}

#[then("get_signature was called once")]
fn called_once(world: &mut ClientWorld) {
    assert_eq!(
        signature_calls(world).len(),
        1,
        "{:?}",
        signature_calls(world)
    );
}

#[then(
    regex = r#"^the replay's track request carries items "(\w+)" and "(\w+)" under signature "(\w+)" and the untrack field "(\w+)"$"#
)]
async fn replay_with_an_untrack(
    world: &mut ClientWorld,
    first: String,
    second: String,
    signature: String,
    untracked: String,
) {
    let replay = replayed(world, 1).await;
    assert_eq!(
        replay["track"].as_array().map(Vec::len),
        Some(1),
        "{replay}"
    );
    assert_eq!(
        replay["track"][0]["signature"],
        signature.as_str(),
        "{replay}"
    );
    let keys: Vec<String> = items_of(&replay).into_iter().map(|(key, _)| key).collect();
    assert_eq!(keys, [first, second], "{replay}");
    assert_eq!(replay["untrack"], json!([untracked]), "{replay}");
}

#[then(regex = r#"^the replay's first track request carries item "(\w+)" under "(\w+)"$"#)]
async fn first_replay_request(world: &mut ClientWorld, key: String, signature: String) {
    let replay = replayed(world, 1).await;
    assert_eq!(
        replay["track"].as_array().map(Vec::len),
        Some(1),
        "{replay}"
    );
    assert_eq!(
        replay["track"][0]["signature"],
        signature.as_str(),
        "{replay}"
    );
    assert_eq!(items_of(&replay), [(key, None)], "{replay}");
}

#[then(regex = r#"^get_signature is called with key "(\w+)" only$"#)]
async fn signed_only(world: &mut ClientWorld, key: String) {
    let calls = Arc::clone(&world.shared_polls.signature_calls);
    wait_until("a get_signature call", || !calls.lock().unwrap().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(signature_calls(world), [vec![key]]);
}

#[then(regex = r#"^a second track request carries item "(\w+)"$"#)]
async fn second_replay_request(world: &mut ClientWorld, key: String) {
    let request = replayed(world, 2).await;
    assert_eq!(request["track"][0]["signature"], "sig2", "{request}");
    assert_eq!(items_of(&request), [(key, None)], "{request}");
}

#[then(regex = r#"^the replay's track request carries item "(\w+)" with no version$"#)]
async fn replay_from_version_0(world: &mut ClientWorld, key: String) {
    let replay = replayed(world, 1).await;
    assert_eq!(items_of(&replay), [(key, None)], "{replay}");
}

#[then(regex = r#"^the replay's track request carries item "(\w+)" at version (\d+)$"#)]
async fn replay_at_version(world: &mut ClientWorld, key: String, version: u64) {
    let replay = replayed(world, 1).await;
    assert_eq!(items_of(&replay), [(key, Some(version))], "{replay}");
}

#[when(
    regex = r#"^the fake server unsubscribes it with code (\d+) and answers the next subscribe with epoch "(\w+)"$"#
)]
async fn unsubscribed_by_the_server(world: &mut ClientWorld, code: u32, epoch: String) {
    mark(world);
    let channel = world.shared_poll().channel().to_owned();
    world
        .fake()
        .answer_a_subscribe_with(json!({"type": 4, "epoch": epoch}));
    world.fake().send(
        &json!({"push": {"channel": channel, "unsubscribe": {"code": code, "reason": "insufficient state"}}})
            .to_string(),
    );
    let event = world.next_shared_poll_event().await;
    assert!(
        matches!(event, SharedPollSubscriptionEvent::Subscribing { code: got, .. } if got == code),
        "expected subscribing with {code}, got {event:?}"
    );
    subscribed_again(world).await;
}

#[given(
    regex = r#"^track_with_signature was called with key "(\w+)" at version (\d+) and signature "(\w+)" before subscribe$"#
)]
fn tracked_before_subscribe(world: &mut ClientWorld, key: String, version: u64, signature: String) {
    assert_eq!(world.shared_poll().state(), SubscriptionState::Unsubscribed);
    world
        .shared_poll()
        .track_with_signature([SharedPollTrackItem::new(key, version)], signature);
}

#[when(regex = r#"^subscribe is called and the subscribe is answered with epoch "(\w+)"$"#)]
async fn subscribed_at_epoch(world: &mut ClientWorld, epoch: String) {
    mark(world);
    world
        .fake()
        .answer_a_subscribe_with(json!({"type": 4, "epoch": epoch}));
    subscribed(world).await;
}

// --- A replay within the frame budget ---------------------------------

#[given(regex = r"^track is called with (\d+) keys of (\d+) bytes each$")]
async fn tracked_sized(world: &mut ClientWorld, count: usize, size: usize) {
    super::track_sized(world, count, size);
    let channel = world.shared_poll().channel().to_owned();
    wait_until("every key tracked", || {
        track_requests(world, &channel)
            .iter()
            .map(|command| items_of(&command["sub_refresh"]).len())
            .sum::<usize>()
            >= count
    })
    .await;
}

#[given(
    regex = r"^the fake server has pushed every key at version (\d+) and each update was taken$"
)]
async fn every_key_pushed(world: &mut ClientWorld, version: u64) {
    let channel = world.shared_poll().channel().to_owned();
    let keys = world.shared_polls.keys.clone();
    for chunk in keys.chunks(100) {
        let frame: Vec<String> = chunk
            .iter()
            .map(|key| {
                json!({"push": {"channel": channel, "pub": {"key": key, "data": {}, "version": version}}})
                    .to_string()
            })
            .collect();
        world.fake().send(&frame.join("\n"));
    }
    let events = world.shared_polls.events.as_mut().expect("a stream");
    for key in &keys {
        let update = next_update(events).await;
        assert_eq!((&update.key, update.version), (key, Some(version)));
    }
}

/// The replay's track requests once they carry every key, with their
/// frames.
async fn replay_frames(world: &ClientWorld) -> Vec<(Value, usize)> {
    let channel = world.shared_poll().channel().to_owned();
    let count = world.shared_polls.keys.len();
    let before = world.shared_polls.sub_refreshes_before;
    let replay = |world: &ClientWorld| -> Vec<(Value, usize)> {
        world
            .fake()
            .received_with_frames()
            .into_iter()
            .filter(|(command, _)| {
                command["sub_refresh"]["channel"] == channel.as_str()
                    && command["sub_refresh"]["type"] == 1
            })
            .skip(before)
            .collect()
    };
    wait_until("the replay of every key", || {
        replay(world)
            .iter()
            .map(|(command, _)| items_of(&command["sub_refresh"]).len())
            .sum::<usize>()
            >= count
    })
    .await;
    replay(world)
}

#[then(
    regex = r"^every sub_refresh frame the fake server receives is at most (\d+) bytes as sent$"
)]
async fn every_frame_within(world: &mut ClientWorld, limit: usize) {
    replay_frames(world).await;
    let channel = world.shared_poll().channel().to_owned();
    for (command, frame) in world.fake().received_with_frames() {
        if command["sub_refresh"]["channel"] == channel.as_str() {
            let size = world.fake().frame_size(frame);
            assert!(size <= limit, "a frame of {size} bytes");
        }
    }
}

#[then(regex = r"^the replay's track requests together carry every key at version (\d+)$")]
async fn replay_carries_every_key(world: &mut ClientWorld, version: u64) {
    let frames = replay_frames(world).await;
    assert!(frames.len() > 1, "{} replay frame", frames.len());
    let mut carried: Vec<(String, Option<u64>)> = frames
        .iter()
        .flat_map(|(command, _)| items_of(&command["sub_refresh"]))
        .collect();
    let mut expected: Vec<(String, Option<u64>)> = world
        .shared_polls
        .keys
        .iter()
        .map(|key| (key.clone(), Some(version)))
        .collect();
    carried.sort();
    expected.sort();
    assert_eq!(carried, expected);
}

// --- unsubscribe --------------------------------------------------------

#[when("unsubscribe is called and the fake server answers it")]
async fn unsubscribed(world: &mut ClientWorld) {
    world.shared_poll().unsubscribe();
    let event = world.next_shared_poll_event().await;
    assert_eq!(event.name(), "unsubscribed", "{event:?}");
    let channel = world.shared_poll().channel().to_owned();
    let fake = world.fake();
    wait_until("the unsubscribe command", || {
        fake.received()
            .iter()
            .any(|command| command["unsubscribe"]["channel"] == channel.as_str())
    })
    .await;
}

#[when("subscribe is called and the subscribe is answered")]
async fn subscribe_answered(world: &mut ClientWorld) {
    let channel = world.shared_poll().channel().to_owned();
    world.shared_polls.sub_refreshes_before = sub_refreshes(world, &channel).len();
    subscribed(world).await;
}

#[then("tracked_keys is empty and the fake server receives no sub_refresh within 1 s")]
async fn nothing_tracked(world: &mut ClientWorld) {
    assert_eq!(world.shared_poll().tracked_keys(), Vec::<String>::new());
    tokio::time::sleep(QUIET_PERIOD).await;
    let channel = world.shared_poll().channel().to_owned();
    let later: Vec<Value> = sub_refreshes(world, &channel)
        .into_iter()
        .skip(world.shared_polls.sub_refreshes_before)
        .collect();
    assert_eq!(later, Vec::<Value>::new());
}

// --- A failing track ----------------------------------------------------

#[when(
    regex = r#"^track_with_signature is called with key "(\w+)" and the fake server answers it with error (\d+) and temporary (true|false)$"#
)]
async fn track_answered_with_an_error(
    world: &mut ClientWorld,
    key: String,
    code: u32,
    temporary: String,
) {
    let channel = world.shared_poll().channel().to_owned();
    world.fake().answer_a_sub_refresh_with(json!({"error": {
        "code": code, "message": "refused by the test", "temporary": temporary == "true",
    }}));
    mark(world);
    // The backoff runs from the reply, which comes after this request:
    // measuring from here, not from when the error is taken, never makes
    // a correct delay look short on a slow runner.
    world.shared_polls.at = Some(Instant::now());
    world
        .shared_poll()
        .track_with_signature([SharedPollTrackItem::new(key, 0)], "sigb");
    nth_track(world, &channel, world.shared_polls.sub_refreshes_before + 1).await;
}

#[then(regex = r"^the stream yields an error with code (\d+)$")]
async fn an_error(world: &mut ClientWorld, code: u32) {
    let SharedPollSubscriptionEvent::Error(error) = world.next_shared_poll_event().await else {
        panic!("expected an error");
    };
    assert_eq!(
        (error.code, error.kind),
        (code, ErrorKind::Track),
        "{error:?}"
    );
}

#[then("get_signature is called for the tracked keys")]
async fn signed_again(world: &mut ClientWorld) {
    let calls = Arc::clone(&world.shared_polls.signature_calls);
    wait_until("a second get_signature call", || {
        calls.lock().unwrap().len() >= 2
    })
    .await;
    assert_eq!(signature_calls(world)[1], ["a", "b"]);
}

#[then(regex = r"^after the 1 s to 15 s backoff the replay's track request carries key (\w+)$")]
async fn replayed_after_the_backoff(world: &mut ClientWorld, key: String) {
    let since = world.shared_polls.at.expect("the failing track sent");
    let request = tokio::time::timeout(Duration::from_secs(4), replayed(world, 2))
        .await
        .expect("a replay within 4 s");
    let after = since.elapsed();
    assert!(
        after >= Duration::from_millis(900),
        "replayed after {after:?}"
    );
    let keys: Vec<String> = items_of(&request).into_iter().map(|(key, _)| key).collect();
    assert!(keys.contains(&key), "{request}");
    assert!(keys.contains(&"a".to_owned()), "{request}");
}

#[then(regex = r"^the fake server receives an untrack of key (\w+) and tracked_keys is (\w+)$")]
async fn untracked_and_kept(world: &mut ClientWorld, key: String, kept: String) {
    let channel = world.shared_poll().channel().to_owned();
    wait_until("an untrack request", || {
        sub_refreshes(world, &channel)
            .iter()
            .any(|command| command["sub_refresh"]["type"] == 2)
    })
    .await;
    let untrack = sub_refreshes(world, &channel)
        .into_iter()
        .find(|command| command["sub_refresh"]["type"] == 2)
        .expect("found above");
    assert_eq!(untrack["sub_refresh"]["untrack"], json!([key]), "{untrack}");
    assert_eq!(world.shared_poll().tracked_keys(), [kept]);
}

// --- Events not taken ---------------------------------------------------

#[given(regex = r#"^the fake server has pushed "(\w+)" (at version (\d+)|as removed), not taken$"#)]
async fn pushed_not_taken(world: &mut ClientWorld, key: String, how: String, version: String) {
    let channel = world.shared_poll().channel().to_owned();
    let publication = if how == "as removed" {
        json!({"key": key, "removed": true})
    } else {
        let version: u64 = version.parse().expect("a version");
        json!({"key": key, "data": {"v": version}, "version": version})
    };
    let read = world.client().statistics().messages_in;
    world
        .fake()
        .send(&json!({"push": {"channel": channel, "pub": publication}}).to_string());
    super::reply_read(world.client(), read).await;
}

#[when("disconnect is called, then connect is called and the subscribe is answered")]
async fn disconnect_then_connect(world: &mut ClientWorld) {
    mark(world);
    world.client().disconnect();
    world.client().connect();
    let channel = world.shared_poll().channel().to_owned();
    nth_track(world, &channel, world.shared_polls.sub_refreshes_before + 1).await;
}

#[then(
    regex = r#"^the stream yields "subscribing", "subscribed" and no update at version (\d+) before the fake server sends it again$"#
)]
async fn not_redelivered_before_sent_again(world: &mut ClientWorld, version: u64) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let first = next_of(events).await;
    assert_eq!(first.name(), "subscribing", "{first:?}");
    let second = next_of(events).await;
    assert_eq!(second.name(), "subscribed", "{second:?}");
    let later = events_within(events, QUIET_PERIOD).await;
    assert!(later.is_empty(), "{later:?}");
    super::push(world, "a", version);
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!((update.key.as_str(), update.version), ("a", Some(version)));
}

#[then(
    regex = r#"^the stream yields an update of "(\w+)" with removed true before "subscribing"$"#
)]
async fn removal_before_subscribing(world: &mut ClientWorld, key: String) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let first = next_of(events).await;
    let SharedPollSubscriptionEvent::Update(update) = first else {
        panic!("expected an update, got {first:?}");
    };
    assert_eq!(
        (update.key, update.removed, update.data, update.version),
        (key, true, Vec::new(), None)
    );
    let second = next_of(events).await;
    assert_eq!(second.name(), "subscribing", "{second:?}");
}

// --- A real server ------------------------------------------------------

#[given(
    regex = r#"^a connected client with a shared poll subscription to "([^"]+)" subscribed, tracking "a" and "b", whose updates of "a" at version (\d+) and "b" at version (\d+) were taken$"#
)]
async fn tracking_two_taken(world: &mut ClientWorld, channel: String, at: u64, then: u64) {
    a_connected_client(world).await;
    a_shared_poll_subscription(world, &channel, |b| b);
    subscribed(world).await;
    track_validly(world, &["a", "b"]).await;
    for (key, version) in [("a", at), ("b", then)] {
        server_api(world)
            .shared_poll_publish(&channel, key, version, json!({"v": version}))
            .await
            .expect("server API shared_poll_publish");
        let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
        assert_eq!((update.key.as_str(), update.version), (key, Some(version)));
    }
}

#[when("the server API disconnects the client with a reconnect-advised code")]
async fn the_server_api_disconnects(world: &mut ClientWorld) {
    super::super::connection::server_disconnects_with_reconnect_code(world).await;
}

#[when(regex = r#"^the client reconnects and the shared poll subscription becomes "subscribed"$"#)]
async fn becomes_subscribed_again(world: &mut ClientWorld) {
    subscribing(world).await;
    subscribed_again(world).await;
}

#[then("no update is yielded within 1 s without a new publish")]
async fn no_update_within(world: &mut ClientWorld) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let seen = events_within(events, QUIET_PERIOD).await;
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event, SharedPollSubscriptionEvent::Update(_))),
        "{seen:?}"
    );
}

#[then(
    regex = r#"^the server API's publish of "(\w+)" at version (\d+) yields an update of "(\w+)" with version (\d+)$"#
)]
async fn publish_yields(world: &mut ClientWorld, key: String, at: u64, of: String, version: u64) {
    let channel = world.shared_poll().channel().to_owned();
    server_api(world)
        .shared_poll_publish(&channel, &key, at, json!({"v": at}))
        .await
        .expect("server API shared_poll_publish");
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!((update.key, update.version), (of, Some(version)));
}

// --- The epoch flip (@B) ------------------------------------------------

#[given(
    regex = r#"^a client over http_stream with a shared poll subscription to "([^"]+)" tracking "a" with a valid signature, whose backend has reported epoch "(\w+)" and whose update of "a" at version (\d+) was taken$"#
)]
async fn tracking_at_an_epoch(
    world: &mut ClientWorld,
    channel: String,
    epoch: String,
    version: u64,
) {
    backend(world).shared_poll_item(&channel, "a", json!({"data": {"v": 1}, "version": version}));
    backend(world).shared_poll_epoch(&channel, &epoch);
    // Over http_stream through the HTTP proxy, which keeps the commands the
    // client sends.
    let server = world.server.as_ref().expect("a Centrifugo server");
    let http_proxy = HttpProxy::start(server.address().to_owned()).await;
    let url = format!("http://{}/connection/http_stream", http_proxy.address());
    world.http_proxy = Some(http_proxy);
    super::super::transports::configure(
        world,
        vec![(ferrifuge::machine::TransportKind::HttpStream, url)],
    );
    super::super::transports::connect_and_wait(world).await;
    a_shared_poll_subscription(world, &channel, |b| b);
    subscribed(world).await;
    track_validly(world, &["a"]).await;
    // The first epoch the backend reports makes the server unsubscribe its
    // subscribers with 2500; the value comes after the resubscribe.
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let mut flipped = false;
    let mut again = false;
    loop {
        match next_of(events).await {
            SharedPollSubscriptionEvent::Subscribing { code: 2500, .. } => flipped = true,
            SharedPollSubscriptionEvent::Subscribed(_) if flipped => again = true,
            SharedPollSubscriptionEvent::Update(update)
                if again && update.key == "a" && update.version == Some(version) =>
            {
                break;
            }
            SharedPollSubscriptionEvent::Error(error) => panic!("{error:?}"),
            _ => {}
        }
    }
}

/// The commands the client sent through the HTTP proxy.
fn commands_sent(world: &ClientWorld) -> Vec<Value> {
    world
        .http_proxy
        .as_ref()
        .expect("an HTTP proxy")
        .emulation_bodies()
        .iter()
        .flat_map(|body| super::super::batching::commands_of(body))
        .collect()
}

#[when(
    regex = r#"^the backend's refresh reports epoch "(\w+)" and key "(\w+)" at version (\d+) with data (\{.*\})$"#
)]
fn the_backend_flips(
    world: &mut ClientWorld,
    epoch: String,
    key: String,
    version: u64,
    data: String,
) {
    world.shared_polls.sub_refreshes_before = commands_sent(world).len();
    let channel = world.shared_poll().channel().to_owned();
    let data: Value = serde_json::from_str(&data).unwrap();
    // The value first: a lower version in the same epoch changes nothing.
    backend(world).shared_poll_item(&channel, &key, json!({"data": data, "version": version}));
    backend(world).shared_poll_epoch(&channel, &epoch);
}

#[then(regex = r#"^the stream yields "subscribing" with code (\d+), then "subscribed"$"#)]
async fn subscribing_then_subscribed(world: &mut ClientWorld, code: u32) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let first = next_of(events).await;
    assert!(
        matches!(first, SharedPollSubscriptionEvent::Subscribing { code: got, .. } if got == code),
        "{first:?}"
    );
    let second = next_of(events).await;
    assert_eq!(second.name(), "subscribed", "{second:?}");
}

#[then(regex = r#"^the next track request carries key "(\w+)" with no version$"#)]
async fn next_track_from_version_0(world: &mut ClientWorld, key: String) {
    let before = world.shared_polls.sub_refreshes_before;
    let mut track = None;
    wait_until("a track request", || {
        track = commands_sent(world)
            .into_iter()
            .skip(before)
            .find(|command| command["sub_refresh"]["type"] == 1);
        track.is_some()
    })
    .await;
    let track = track.expect("found")["sub_refresh"].clone();
    assert_eq!(items_of(&track), [(key, None)], "{track}");
}

#[then(regex = r#"^the stream yields an update of "(\w+)" with data (\{.*\})$"#)]
async fn an_update_with(world: &mut ClientWorld, key: String, data: String) {
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!(update.key, key);
    assert_eq!(
        serde_json::from_slice::<Value>(&update.data).unwrap(),
        serde_json::from_str::<Value>(&data).unwrap()
    );
}
