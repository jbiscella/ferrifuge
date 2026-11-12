//! Block 47d: per-key delta compression of shared poll subscriptions, its
//! memory bound, and the statistics.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    DeltaFormat, ErrorKind, SharedPollSubscriptionEvent, SharedPollTrackItem, SubscriptionState,
};
use serde_json::{Value, json};

use super::super::connection::a_connected_client;
use super::super::delivery::wait_until;
use super::super::{ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD};
use super::{
    a_shared_poll_subscription, backend, events_within, json_of, next_update, nth_track, push,
    sub_refreshes, subscribed, track_requests, track_validly, tracked_on_the_fake,
};

/// The value a scenario's key "a" holds on the client: its JSON text and
/// its version, which the next delta applies to.
fn base(world: &ClientWorld) -> (String, u64) {
    world
        .shared_polls
        .base
        .clone()
        .expect("a value decoded before")
}

/// Pushes `key` at `version`, `payload` whole: with delta on the data is a
/// JSON string holding the JSON text (Block 19, D7).
fn push_whole(world: &ClientWorld, key: &str, version: u64, payload: &str) {
    let channel = world.shared_poll().channel().to_owned();
    world.fake().send(
        &json!({"push": {"channel": channel, "pub": {
            "key": key, "data": payload, "version": version,
        }}})
        .to_string(),
    );
}

/// Pushes `key` at `version` as a fossil delta from `from` to `to`.
fn push_delta(world: &ClientWorld, key: &str, version: u64, from: &str, to: &str) {
    let channel = world.shared_poll().channel().to_owned();
    let delta = String::from_utf8(ferrifuge::write_fossil_delta(
        from.as_bytes(),
        to.as_bytes(),
    ))
    .expect("an ASCII delta");
    world.fake().send(
        &json!({"push": {"channel": channel, "pub": {
            "key": key, "data": delta, "version": version, "delta": true,
        }}})
        .to_string(),
    );
}

/// A value of `n`, long enough that a delta between two of them is short.
fn value(n: u64) -> String {
    json!({"v": format!("value {n} of a key whose value changes a little at a time")}).to_string()
}

/// The subscriptions of the 47d scenarios on the fake server; false for a
/// description it does not know.
pub(super) async fn a_delta_subscription(
    world: &mut ClientWorld,
    channel: &str,
    how: &str,
) -> bool {
    match how {
        r#" with delta, subscribed, tracking "a", which has decoded one delta on key "a""#
        | r#" with delta, subscribed, tracking "a" under signature "sig1", which has decoded one delta on key "a", whose stream was taken"# =>
        {
            with_delta(world, channel, |b| b).await;
            decoded_one_delta(world).await;
        }
        r#" with delta and max_delta_bases_bytes 1024, subscribed, tracking "a", whose stream was taken"# =>
        {
            with_delta(world, channel, |builder| {
                builder.max_delta_bases_bytes(std::num::NonZeroUsize::new(1024).unwrap())
            })
            .await;
        }
        r#" with delta, subscribed, tracking "a" under signature "sig1", whose stream was taken"# =>
        {
            with_delta(world, channel, |b| b).await;
        }
        r#" subscribed, tracking "a""# => {
            a_shared_poll_subscription(world, channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, channel, vec![("a", 0)]).await;
        }
        _ => return false,
    }
    true
}

/// A subscription to `channel` with delta, built by `configure`, which the
/// fake server subscribes with delta on, tracking "a" under "sig1".
async fn with_delta(
    world: &mut ClientWorld,
    channel: &str,
    configure: impl FnOnce(
        ferrifuge::SharedPollSubscriptionBuilder,
    ) -> ferrifuge::SharedPollSubscriptionBuilder,
) {
    world.fake().delta_on(channel);
    a_shared_poll_subscription(world, channel, |builder| {
        configure(builder.delta(DeltaFormat::Fossil))
    });
    subscribed_with_delta(world).await;
    tracked_on_the_fake(world, channel, vec![("a", 0)]).await;
}

/// Subscribes and waits until subscribed, which the server must report
/// with delta on.
async fn subscribed_with_delta(world: &mut ClientWorld) {
    world.shared_poll().subscribe();
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(subscribed) => {
                assert!(subscribed.delta, "delta not on: {subscribed:?}");
                return;
            }
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected subscribed, got {other:?}"),
        }
    }
}

/// Key "a" comes whole at version 1, then as a delta at version 2; both
/// updates are taken, with the values the fake server built.
async fn decoded_one_delta(world: &mut ClientWorld) {
    push_whole(world, "a", 1, &value(1));
    push_delta(world, "a", 2, &value(1), &value(2));
    let events = world.shared_polls.events.as_mut().expect("a stream");
    for n in [1, 2] {
        let update = next_update(events).await;
        assert_eq!(
            (update.key.as_str(), update.version, update.data),
            ("a", Some(n), value(n).into_bytes()),
        );
    }
    world.shared_polls.base = Some((value(2), 2));
}

// --- Bases dropped at each subscribe --------------------------------------

#[when(
    "the transport closes and the client reconnects and the subscribe is answered with delta true"
)]
async fn reconnects_with_delta(world: &mut ClientWorld) {
    assert!(world.fake().close_current(3001, "reconnect"));
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(subscribed) => {
                assert!(subscribed.delta, "delta not on: {subscribed:?}");
                break;
            }
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected subscribed again, got {other:?}"),
        }
    }
    while !matches!(
        world.next_event().await,
        ferrifuge::ClientEvent::Connected { .. }
    ) {}
}

#[when(regex = r#"^the fake server pushes a delta for "(\w+)" against the former base$"#)]
fn a_delta_against_the_former_base(world: &mut ClientWorld, key: String) {
    let (former, version) = base(world);
    push_delta(world, &key, version + 1, &former, &value(version + 1));
}

#[then(
    regex = r#"^the stream yields an error of kind Delta with code (\d+) and "subscribing" with code (\d+)$"#
)]
async fn a_delta_error_then_subscribing(world: &mut ClientWorld, code: u32, subscribing: u32) {
    match world.next_shared_poll_event().await {
        SharedPollSubscriptionEvent::Error(error) => {
            assert_eq!(
                (error.kind, error.code),
                (ErrorKind::Delta, code),
                "{error:?}"
            );
        }
        other => panic!("expected a delta error, got {other:?}"),
    }
    match world.next_shared_poll_event().await {
        SharedPollSubscriptionEvent::Subscribing { code: got, reason } => {
            assert_eq!(got, subscribing, "{reason}");
        }
        other => panic!("expected subscribing, got {other:?}"),
    }
}

// --- A delta in flight when its key was tracked again ---------------------

#[when(
    regex = r#"^untrack is called with key "(\w+)" and track_with_signature is called with key "(\w+)" and signature "(\w+)"$"#
)]
async fn untracked_and_tracked_again(
    world: &mut ClientWorld,
    key: String,
    again: String,
    signature: String,
) {
    let channel = world.shared_poll().channel().to_owned();
    // Neither is answered until a later step says so.
    world.fake().answer_a_sub_refresh_with(Value::Null);
    world.fake().answer_a_sub_refresh_with(Value::Null);
    let before = sub_refreshes(world, &channel).len();
    world.shared_poll().untrack([key]);
    world
        .shared_poll()
        .track_with_signature([SharedPollTrackItem::new(again, 0)], signature);
    wait_until("the untrack and the track requests", || {
        sub_refreshes(world, &channel).len() >= before + 2
    })
    .await;
    world.shared_polls.held = sub_refreshes(world, &channel)
        .into_iter()
        .skip(before)
        .map(|command| command["id"].clone())
        .collect();
}

#[when(
    regex = r#"^the fake server, before answering the untrack, pushes a delta for "(\w+)" against the former base$"#
)]
fn a_delta_before_the_untrack_reply(world: &mut ClientWorld, key: String) {
    a_delta_against_the_former_base(world, key);
}

#[then(regex = r#"^the stream yields no error and no update of "(\w+)" within 1 s$"#)]
async fn nothing_within_a_second(world: &mut ClientWorld, key: String) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let seen = events_within(events, QUIET_PERIOD).await;
    for event in &seen {
        match event {
            SharedPollSubscriptionEvent::Error(error) => panic!("an error: {error:?}"),
            SharedPollSubscriptionEvent::Update(update) if update.key == key => {
                panic!("an update of {key}: {update:?}");
            }
            _ => {}
        }
    }
}

#[then(regex = r#"^the shared poll subscription stays "subscribed"$"#)]
fn stays_subscribed(world: &mut ClientWorld) {
    assert_eq!(world.shared_poll().state(), SubscriptionState::Subscribed);
}

#[when(
    regex = r#"^the fake server answers the untrack and the track and pushes "(\w+)" whole at version (\d+)$"#
)]
fn answers_and_pushes_whole(world: &mut ClientWorld, key: String, version: u64) {
    let held = std::mem::take(&mut world.shared_polls.held);
    assert_eq!(held.len(), 2, "the untrack and the track held");
    for id in held {
        world
            .fake()
            .send(&json!({"id": id, "sub_refresh": {}}).to_string());
    }
    push_whole(world, &key, version, &value(version));
}

#[then(regex = r#"^the stream yields an update of "(\w+)" with version (\d+)$"#)]
async fn an_update_at(world: &mut ClientWorld, key: String, version: u64) {
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!(
        (update.key.as_str(), update.version, update.data),
        (key.as_str(), Some(version), value(version).into_bytes())
    );
}

// --- The bound ------------------------------------------------------------

#[when(regex = r#"^the fake server pushes "(\w+)" with (\d+) bytes of data$"#)]
fn pushes_sized(world: &mut ClientWorld, key: String, size: usize) {
    // A JSON string of `size` bytes, quotes included.
    let payload = format!("\"{}\"", "x".repeat(size - 2));
    push_whole(world, &key, 1, &payload);
}

// --- A delta that cannot be applied ---------------------------------------

#[when(regex = r#"^the fake server pushes a delta for key "(\w+)" with no base$"#)]
fn a_delta_with_no_base(world: &mut ClientWorld, key: String) {
    world.shared_polls.sub_refreshes_before =
        track_requests(world, world.shared_poll().channel()).len();
    push_delta(world, &key, 1, &value(0), &value(1));
}

#[then(
    regex = r#"^after the resubscribe backoff the fake server receives a subscribe and a track request with signature "(\w+)" and item "(\w+)"$"#
)]
async fn resubscribed_and_tracked(world: &mut ClientWorld, signature: String, key: String) {
    let channel = world.shared_poll().channel().to_owned();
    let fake = world.fake();
    wait_until("a second subscribe", || {
        fake.received()
            .iter()
            .filter(|command| command["subscribe"]["channel"] == channel.as_str())
            .count()
            >= 2
    })
    .await;
    let request = nth_track(world, &channel, world.shared_polls.sub_refreshes_before + 1).await;
    let batches = request["track"].as_array().cloned().unwrap_or_default();
    assert_eq!(batches.len(), 1, "{request}");
    assert_eq!(batches[0]["signature"], signature.as_str(), "{request}");
    assert_eq!(super::items_of(&request), [(key, None)], "{request}");
}

// --- Statistics -----------------------------------------------------------

#[when(regex = r#"^the fake server pushes "(\w+)" at version (\d+) and it is taken$"#)]
async fn pushed_and_taken(world: &mut ClientWorld, key: String, version: u64) {
    push(world, &key, version);
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!((update.key, update.version), (key, Some(version)));
}

#[then(
    regex = r"^the client's statistics report (\d+) subscription and (\d+) publication delivered$"
)]
async fn statistics_report(world: &mut ClientWorld, subscriptions: usize, delivered: u64) {
    let client = world.client().clone();
    wait_until("the publication counted", || {
        client.statistics().publications_delivered >= delivered
    })
    .await;
    let statistics = client.statistics();
    assert_eq!(
        (statistics.subscriptions, statistics.publications_delivered),
        (subscriptions, delivered),
        "{statistics:?}"
    );
}

// --- A real server's delta (@B) -------------------------------------------

#[given(
    regex = r#"^a client with a shared poll subscription to "([^"]+)" with delta, subscribed, tracking "a" with a valid signature, whose stream was taken$"#
)]
async fn a_delta_client(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    a_shared_poll_subscription(world, &channel, |builder| {
        builder.delta(DeltaFormat::Fossil)
    });
    subscribed_with_delta(world).await;
    track_validly(world, &["a"]).await;
}

#[when(
    regex = r#"^the backend's refresh answers key "(\w+)" at version (\d+) with data (\{[^}]*\})( and that previous data)?$"#
)]
async fn the_backend_answers_at(
    world: &mut ClientWorld,
    key: String,
    version: u64,
    data: String,
    previous: String,
) {
    let channel = world.shared_poll().channel().to_owned();
    let data: Value = serde_json::from_str(&data).unwrap();
    let mut item = json!({"data": data, "version": version});
    if !previous.is_empty() {
        let (former, _) = base(world);
        item["prev_data"] = serde_json::from_str(&former).unwrap();
    }
    let backend = backend(world);
    backend.shared_poll_item(&channel, &key, item);
    // Polled with this answer before the next step changes it: each
    // version reaches the server, so the next can go as a delta.
    let asked = backend.shared_poll_requests().len();
    wait_until("a refresh with this value", || {
        backend.shared_poll_requests().len() > asked
    })
    .await;
    world.shared_polls.base = Some((data.to_string(), version));
}

#[then(regex = r#"^the stream yields updates of "(\w+)" with data (\{[^}]*\}) and (\{[^}]*\})$"#)]
async fn yields_both_values(world: &mut ClientWorld, key: String, first: String, second: String) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    for data in [first, second] {
        let update =
            tokio::time::timeout(EVENT_TIMEOUT + Duration::from_secs(5), next_update(events))
                .await
                .expect("an update in time");
        assert_eq!(update.key, key);
        assert_eq!(
            json_of(&update.data),
            serde_json::from_str::<Value>(&data).unwrap(),
            "{update:?}"
        );
    }
}

#[then(regex = r"^the subscription's delta_statistics counts (one|\d+) deltas?$")]
fn counts_deltas(world: &mut ClientWorld, count: String) {
    let count = if count == "one" {
        1
    } else {
        count.parse().unwrap()
    };
    let statistics = world.shared_poll().delta_statistics();
    assert_eq!(statistics.delta_payloads, count, "{statistics:?}");
}
