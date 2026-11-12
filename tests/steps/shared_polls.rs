//! Block 47: shared poll subscriptions (`unstable`).

// Cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    Client, DeltaFormat, ErrorKind, MapKind, Protocol, SharedPollSignature, SharedPollSubscription,
    SharedPollSubscriptionEvent, SharedPollTrackItem, SharedPollUpdate, State, SubscriptionState,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::delivery::wait_until;
use super::{ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, recorded};
use crate::support::token::{shared_poll_signature, subscription_token};

type Events = Pin<Box<dyn Stream<Item = SharedPollSubscriptionEvent> + Send>>;

/// The keys and signatures a `get_signature` callback was called with.
type Signed = Arc<Mutex<Vec<Vec<String>>>>;

/// What a Block 47 scenario holds.
#[derive(Default)]
pub(crate) struct SharedPolls {
    subscription: Option<SharedPollSubscription>,
    events: Option<Events>,
    /// The other client of a scenario, its subscription and its stream.
    other: Option<(Client, SharedPollSubscription, Events)>,
    /// The keys each `get_signature` call was given.
    signature_calls: Signed,
    token_calls: Arc<AtomicUsize>,
    /// The track and untrack requests the fake server had received before
    /// a step closed the transport.
    sub_refreshes_before: usize,
    /// The keys a step tracked, for a later step to name.
    keys: Vec<String>,
    /// The subscribe command a step checked.
    subscribe: Option<Value>,
    /// Messages the client had read when a step sent a request.
    read: u64,
}

impl ClientWorld {
    fn shared_poll(&self) -> &SharedPollSubscription {
        self.shared_polls
            .subscription
            .as_ref()
            .expect("a shared poll subscription")
    }

    /// Makes `subscription` the scenario's, recording its events.
    fn track_shared_poll(&mut self, subscription: SharedPollSubscription) {
        self.shared_polls.events = Some(recorded(subscription.events()));
        self.shared_polls.subscription = Some(subscription);
    }

    async fn next_shared_poll_event(&mut self) -> SharedPollSubscriptionEvent {
        let events = self
            .shared_polls
            .events
            .as_mut()
            .expect("a shared poll subscription's stream");
        next_of(events).await
    }

    /// The anonymous user of @A clients, the proxy's of @B ones.
    fn shared_poll_user(&self) -> &'static str {
        if self.proxy.is_some() {
            crate::support::proxy::USER
        } else {
            ""
        }
    }
}

async fn next_of(events: &mut Events) -> SharedPollSubscriptionEvent {
    tokio::time::timeout(EVENT_TIMEOUT, events.next())
        .await
        .unwrap_or_else(|_| panic!("no shared poll subscription event within {EVENT_TIMEOUT:?}"))
        .expect("the shared poll subscription's stream ended")
}

/// The events `events` yields within `period`.
async fn events_within(events: &mut Events, period: Duration) -> Vec<SharedPollSubscriptionEvent> {
    let mut seen = Vec::new();
    let _ = tokio::time::timeout(period, async {
        while let Some(event) = events.next().await {
            seen.push(event);
        }
    })
    .await;
    seen
}

/// The next update `events` yields, past lifecycle events.
async fn next_update(events: &mut Events) -> SharedPollUpdate {
    loop {
        match next_of(events).await {
            SharedPollSubscriptionEvent::Update(update) => return update,
            SharedPollSubscriptionEvent::Subscribing { .. }
            | SharedPollSubscriptionEvent::Subscribed(_) => {}
            other => panic!("expected an update, got {other:?}"),
        }
    }
}

fn quoted(list: &str) -> Vec<String> {
    list.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

fn json_of(data: &[u8]) -> Value {
    serde_json::from_slice(data).unwrap_or_else(|e| panic!("not JSON ({e}): {data:?}"))
}

/// A `get_signature` callback signing what it is given with `signature`, and
/// recording each call.
fn signing_with(
    calls: &Signed,
    signature: &'static str,
) -> impl Fn(Vec<String>) -> std::future::Ready<Result<SharedPollSignature, ferrifuge::TokenError>>
+ Send
+ Sync
+ 'static {
    let calls = Arc::clone(calls);
    move |keys: Vec<String>| {
        calls.lock().unwrap().push(keys.clone());
        std::future::ready(Ok(SharedPollSignature::new(keys, signature)))
    }
}

// --- Registry -------------------------------------------------------------

#[given(
    regex = r#"^a new client with a (shared poll subscription|map subscription) to channel "([^"]+)"$"#
)]
fn a_new_client_with(world: &mut ClientWorld, what: String, channel: String) {
    world.new_client(super::UNREACHABLE_ENDPOINT.to_owned());
    if what == "map subscription" {
        world
            .client()
            .new_map_subscription(channel, MapKind::Map)
            .unwrap();
    } else {
        world
            .client()
            .new_shared_poll_subscription(channel)
            .unwrap();
    }
}

#[when(regex = r#"^a shared poll subscription to channel "([^"]+)" is created$"#)]
fn a_shared_poll_subscription_is_created(world: &mut ClientWorld, channel: String) {
    let created = world
        .client()
        .new_shared_poll_subscription(channel)
        .map(|subscription| world.shared_polls.subscription = Some(subscription));
    world.maps.created = Some(created);
}

#[then(regex = r#"^the shared poll subscription state is "(\w+)"$"#)]
fn the_state_is(world: &mut ClientWorld, state: String) {
    assert_eq!(
        format!("{:?}", world.shared_poll().state()).to_lowercase(),
        state
    );
}

#[then(
    regex = r#"^the client's shared poll subscriptions are "([^"]+)" and its subscriptions are none$"#
)]
fn the_registry_lists(world: &mut ClientWorld, channel: String) {
    let listed: Vec<String> = world
        .client()
        .shared_poll_subscriptions()
        .iter()
        .map(|subscription| subscription.channel().to_owned())
        .collect();
    assert_eq!(listed, std::slice::from_ref(&channel));
    assert_eq!(world.client().subscriptions(), []);
    assert!(world.client().get_subscription(&channel).is_none());
    assert!(world.client().get_map_subscription(&channel).is_none());
    assert_eq!(
        world
            .client()
            .get_shared_poll_subscription(&channel)
            .as_ref(),
        Some(world.shared_poll())
    );
}

#[then(regex = r"^tracked_keys is (empty|.+)$")]
fn tracked_keys_are(world: &mut ClientWorld, keys: String) {
    let expected = if keys == "empty" {
        Vec::new()
    } else {
        quoted(&keys)
    };
    assert_eq!(world.shared_poll().tracked_keys(), expected);
}

#[when("the shared poll subscription is removed")]
fn the_subscription_is_removed(world: &mut ClientWorld) {
    let subscription = world.shared_poll().clone();
    world
        .client()
        .remove_shared_poll_subscription(&subscription);
}

#[then("the client's shared poll subscriptions are none")]
fn none_left(world: &mut ClientWorld) {
    assert_eq!(world.client().shared_poll_subscriptions(), []);
}

// --- The fake server ------------------------------------------------------

/// The track and untrack requests the fake server received for `channel`.
fn sub_refreshes(world: &ClientWorld, channel: &str) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter(|command| {
            command["sub_refresh"]["channel"] == channel
                && matches!(command["sub_refresh"]["type"].as_i64(), Some(1 | 2))
        })
        .collect()
}

fn track_requests(world: &ClientWorld, channel: &str) -> Vec<Value> {
    sub_refreshes(world, channel)
        .into_iter()
        .filter(|command| command["sub_refresh"]["type"] == 1)
        .collect()
}

/// Waits for track request `n` (from 1) for `channel`.
async fn nth_track(world: &ClientWorld, channel: &str, n: usize) -> Value {
    let waited = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(command) = track_requests(world, channel).into_iter().nth(n - 1) {
                return command["sub_refresh"].clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    waited.unwrap_or_else(|_| panic!("no track request {n} for {channel}"))
}

/// The items of a track request's batches, as (key, version), a missing
/// version as `None`.
fn items_of(request: &Value) -> Vec<(String, Option<u64>)> {
    request["track"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|batch| batch["items"].as_array().cloned().unwrap_or_default())
        .map(|item| {
            (
                item["key"].as_str().unwrap_or_default().to_owned(),
                item["version"].as_u64(),
            )
        })
        .collect()
}

/// The subscription to `channel` on the client, built by `configure`, its
/// stream taken.
fn a_shared_poll_subscription(
    world: &mut ClientWorld,
    channel: &str,
    configure: impl FnOnce(
        ferrifuge::SharedPollSubscriptionBuilder,
    ) -> ferrifuge::SharedPollSubscriptionBuilder,
) {
    let subscription = configure(world.client().shared_poll_subscription_builder(channel))
        .build()
        .unwrap();
    world.track_shared_poll(subscription);
}

/// Subscribes the scenario's subscription and waits until it is
/// subscribed, its events taken.
async fn subscribed(world: &mut ClientWorld) {
    world.shared_poll().subscribe();
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(_) => return,
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected subscribed, got {other:?}"),
        }
    }
}

/// Tracks `items` with a signature the fake server accepts, and waits for
/// its track request.
async fn tracked_on_the_fake(world: &mut ClientWorld, channel: &str, items: Vec<(&str, u64)>) {
    let before = track_requests(world, channel).len();
    world.shared_poll().track_with_signature(
        items
            .into_iter()
            .map(|(key, version)| SharedPollTrackItem::new(key, version)),
        "sig1",
    );
    nth_track(world, channel, before + 1).await;
}

#[given(
    regex = r#"^a client connected to the fake server, with a shared poll subscription to "(\w+)"(.*)$"#
)]
async fn a_client_with_a_subscription(world: &mut ClientWorld, channel: String, how: String) {
    a_connected_client(world).await;
    let calls = Arc::clone(&world.shared_polls.signature_calls);
    match how.as_str() {
        "" | " whose stream was taken" => a_shared_poll_subscription(world, &channel, |b| b),
        " that has subscribed" | " subscribed, whose stream was taken" => {
            a_shared_poll_subscription(world, &channel, |b| b);
            subscribed(world).await;
        }
        r#" subscribed, tracking key "a""# | r#" tracking "a", whose stream was taken"# => {
            a_shared_poll_subscription(world, &channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, &channel, vec![("a", 0)]).await;
        }
        r#" tracking "a" at version 5, whose stream was taken"# => {
            a_shared_poll_subscription(world, &channel, |b| b);
            subscribed(world).await;
            tracked_on_the_fake(world, &channel, vec![("a", 5)]).await;
        }
        r#" with get_signature that signs what it is given with "sig1""# => {
            a_shared_poll_subscription(world, &channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig1"))
            });
        }
        " with a get_signature callback that has subscribed" => {
            a_shared_poll_subscription(world, &channel, |builder| {
                builder.get_signature(signing_with(&calls, "sig1"))
            });
            subscribed(world).await;
        }
        " subscribed and 1800 keys of 44 bytes each tracked" => {
            a_shared_poll_subscription(world, &channel, |b| b);
            subscribed(world).await;
            let keys: Vec<String> = (0..1800).map(|n| format!("{n:0>44}")).collect();
            // In batches the frame budget holds, as a backend signs them.
            for (number, batch) in keys.chunks(500).enumerate() {
                world.shared_poll().track_with_signature(
                    batch
                        .iter()
                        .map(|key| SharedPollTrackItem::new(key.clone(), 0)),
                    format!("sig{number}"),
                );
                nth_track(world, &channel, number + 1).await;
            }
            world.shared_polls.keys = keys;
        }
        other => panic!("no such shared poll subscription: {other}"),
    }
}

#[given(
    regex = r#"^a shared poll subscription to channel "(\w+)" with data \{"a":1\}, delta and a token$"#
)]
fn a_subscription_with_options(world: &mut ClientWorld, channel: String) {
    a_shared_poll_subscription(world, &channel, |builder| {
        builder
            .data(br#"{"a":1}"#.to_vec())
            .delta(DeltaFormat::Fossil)
            .token("t1")
    });
}

#[when("subscribe is called on the shared poll subscription")]
fn subscribe_is_called(world: &mut ClientWorld) {
    world.shared_poll().subscribe();
}

#[then(
    regex = r#"^the subscribe command for "(\w+)" has type 4, the data \{"a":1\}, delta "fossil" and the token$"#
)]
async fn the_subscribe_command(world: &mut ClientWorld, channel: String) {
    let fake = world.fake();
    wait_until("the subscribe command", || {
        fake.received()
            .iter()
            .any(|command| command["subscribe"]["channel"] == channel.as_str())
    })
    .await;
    let subscribe = fake
        .received()
        .into_iter()
        .find(|command| command["subscribe"]["channel"] == channel.as_str())
        .expect("found above")["subscribe"]
        .clone();
    assert_eq!(subscribe["type"], 4, "{subscribe}");
    assert_eq!(subscribe["data"], json!({"a": 1}), "{subscribe}");
    assert_eq!(subscribe["delta"], "fossil", "{subscribe}");
    assert_eq!(subscribe["token"], "t1", "{subscribe}");
    world.shared_polls.subscribe = Some(subscribe);
}

#[then("it has no recover, epoch, offset, positioned, recoverable, join_leave, flag or tf field")]
fn no_other_field(world: &mut ClientWorld) {
    let subscribe = world
        .shared_polls
        .subscribe
        .clone()
        .expect("a subscribe command");
    for field in [
        "recover",
        "epoch",
        "offset",
        "positioned",
        "recoverable",
        "join_leave",
        "flag",
        "tf",
    ] {
        assert!(subscribe.get(field).is_none(), "{field} in {subscribe}");
    }
}

#[when(
    regex = r#"^track_with_signature is called with (keys? .+?) and (signature "[^"]+"|a valid signature)$"#
)]
fn track_with_signature_is_called(world: &mut ClientWorld, items: String, signature: String) {
    let items = parse_items(&items);
    let channel = world.shared_poll().channel().to_owned();
    let signature = if let Some(quoted_signature) = signature.strip_prefix("signature ") {
        quoted_signature.trim_matches('"').to_owned()
    } else {
        let keys: Vec<&str> = items.iter().map(|(key, _)| key.as_str()).collect();
        shared_poll_signature(&channel, world.shared_poll_user(), &keys, None)
    };
    world.shared_poll().track_with_signature(
        items
            .into_iter()
            .map(|(key, version)| SharedPollTrackItem::new(key, version)),
        signature,
    );
}

/// `key "a" at version 3`, `keys "a" at version 3 and "b" at version 0`,
/// `keys "a", "b" and "c"`: the keys, at version 0 unless given.
fn parse_items(text: &str) -> Vec<(String, u64)> {
    let mut items = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('"') {
        let after = &rest[start + 1..];
        let end = after.find('"').expect("a closing quote");
        let key = after[..end].to_owned();
        rest = &after[end + 1..];
        let version = rest
            .trim_start()
            .strip_prefix("at version ")
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|number| number.parse().ok())
            .unwrap_or(0);
        items.push((key, version));
    }
    items
}

#[then(
    regex = r#"^the fake server receives a sub_refresh for "(\w+)" with type 1 and one batch with signature "(\w+)", item "a" at version 3 and item "b" with no version$"#
)]
async fn receives_the_batch(world: &mut ClientWorld, channel: String, signature: String) {
    let request = nth_track(world, &channel, 1).await;
    assert_eq!(
        request["track"].as_array().map(Vec::len),
        Some(1),
        "{request}"
    );
    assert_eq!(request["track"][0]["signature"], signature.as_str());
    assert_eq!(
        items_of(&request),
        [("a".to_owned(), Some(3)), ("b".to_owned(), None)]
    );
    assert!(request.get("token").is_none(), "{request}");
}

#[when(regex = r#"^track is called with key "(\w+)" before subscribe$"#)]
fn track_before_subscribe(world: &mut ClientWorld, key: String) {
    assert_eq!(world.shared_poll().state(), SubscriptionState::Unsubscribed);
    world.shared_poll().track([key]);
}

#[then("get_signature has not been called")]
fn not_called(world: &mut ClientWorld) {
    assert!(
        world
            .shared_polls
            .signature_calls
            .lock()
            .unwrap()
            .is_empty()
    );
}

#[then(
    regex = r#"^when subscribe is called and the subscribe is answered, get_signature is called with key "(\w+)"$"#
)]
async fn called_once_subscribed(world: &mut ClientWorld, key: String) {
    subscribed(world).await;
    let calls = Arc::clone(&world.shared_polls.signature_calls);
    wait_until("a get_signature call", || !calls.lock().unwrap().is_empty()).await;
    assert_eq!(*calls.lock().unwrap(), [vec![key]]);
}

#[then(
    regex = r#"^the fake server receives a sub_refresh for "(\w+)" with type 1, signature "(\w+)" and item "(\w+)"$"#
)]
async fn receives_a_signed_item(
    world: &mut ClientWorld,
    channel: String,
    signature: String,
    key: String,
) {
    let request = nth_track(world, &channel, 1).await;
    assert_eq!(request["track"][0]["signature"], signature.as_str());
    assert_eq!(items_of(&request), [(key, None)]);
}

#[when("the transport closes and the client reconnects and subscribes again")]
async fn the_transport_closes(world: &mut ClientWorld) {
    let channel = world.shared_poll().channel().to_owned();
    // The track requests made before are answered once read.
    let made = track_requests(world, &channel).len();
    wait_until("the track requests", || {
        track_requests(world, &channel).len() >= made
    })
    .await;
    world.shared_polls.sub_refreshes_before = made;
    assert!(world.fake().close_current(3001, "reconnect"));
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(_) => break,
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected a resubscribe, got {other:?}"),
        }
    }
}

#[then(regex = r#"^the replay's track request carries key "(\w+)" at version (\d+)$"#)]
async fn the_replay_carries(world: &mut ClientWorld, key: String, version: u64) {
    let channel = world.shared_poll().channel().to_owned();
    let replay = nth_track(world, &channel, world.shared_polls.sub_refreshes_before + 1).await;
    assert_eq!(items_of(&replay), [(key, Some(version))], "{replay}");
}

#[when("track is called with no keys and untrack is called with no keys")]
fn empty_sets(world: &mut ClientWorld) {
    world.shared_poll().track(Vec::<String>::new());
    world.shared_poll().untrack(Vec::<String>::new());
}

#[then("the fake server receives no sub_refresh and get_signature is not called within 1 s")]
async fn nothing_within_a_second(world: &mut ClientWorld) {
    tokio::time::sleep(QUIET_PERIOD).await;
    let channel = world.shared_poll().channel().to_owned();
    assert_eq!(sub_refreshes(world, &channel), Vec::<Value>::new());
    assert!(
        world
            .shared_polls
            .signature_calls
            .lock()
            .unwrap()
            .is_empty()
    );
}

#[when("untrack is called with those keys")]
fn untrack_those(world: &mut ClientWorld) {
    let keys = world.shared_polls.keys.clone();
    world.shared_poll().untrack(keys);
}

#[then(
    regex = r"^the fake server receives several sub_refresh frames with type 2, each at most (\d+) bytes as sent, together covering all (\d+) keys, each once$"
)]
async fn several_untrack_frames(world: &mut ClientWorld, limit: usize, count: usize) {
    let channel = world.shared_poll().channel().to_owned();
    let untracked = |world: &ClientWorld| -> Vec<(Value, usize)> {
        world
            .fake()
            .received_with_frames()
            .into_iter()
            .filter(|(command, _)| {
                command["sub_refresh"]["channel"] == channel.as_str()
                    && command["sub_refresh"]["type"] == 2
            })
            .collect()
    };
    wait_until("every key untracked", || {
        untracked(world)
            .iter()
            .map(|(command, _)| {
                command["sub_refresh"]["untrack"]
                    .as_array()
                    .map_or(0, Vec::len)
            })
            .sum::<usize>()
            >= count
    })
    .await;
    let requests = untracked(world);
    assert!(requests.len() > 1, "{} untrack request", requests.len());
    let mut keys: Vec<String> = Vec::new();
    for (command, frame) in &requests {
        let size = world.fake().frame_size(*frame);
        assert!(size <= limit, "a frame of {size} bytes");
        keys.extend(
            command["sub_refresh"]["untrack"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap().to_owned()),
        );
    }
    let mut expected = world.shared_polls.keys.clone();
    keys.sort();
    expected.sort();
    assert_eq!(keys, expected);
}

#[then("the client stays connected")]
async fn stays_connected(world: &mut ClientWorld) {
    let events = world.events_during(QUIET_PERIOD).await;
    assert!(
        events
            .iter()
            .all(|event| event.name() != "connecting" && event.name() != "disconnected"),
        "{events:?}"
    );
    assert_eq!(world.client().state(), State::Connected);
}

#[when(
    regex = r#"^the fake server pushes key "(\w+)" at version (\d+) and then key "(\w+)" at version (\d+)$"#
)]
fn pushes_two(world: &mut ClientWorld, first: String, at: u64, second: String, then: u64) {
    push(world, &first, at);
    push(world, &second, then);
}

#[when(regex = r#"^the fake server pushes "(\w+)" at version (\d+)$"#)]
fn pushes_one(world: &mut ClientWorld, key: String, version: u64) {
    push(world, &key, version);
}

/// Pushes `key` at `version`, its data `{"v": version}`.
fn push(world: &ClientWorld, key: &str, version: u64) {
    let channel = world.shared_poll().channel().to_owned();
    world.fake().send(
        &json!({"push": {"channel": channel, "pub": {"key": key, "data": {"v": version}, "version": version}}})
            .to_string(),
    );
}

#[then(regex = r#"^the stream yields exactly one update of "(\w+)" with version (\d+)$"#)]
async fn exactly_one_update(world: &mut ClientWorld, key: String, version: u64) {
    exactly_one(world, &key, version).await;
}

#[then(regex = r#"^the stream yields exactly one update, of "(\w+)" at version (\d+)$"#)]
async fn exactly_one_update_of(world: &mut ClientWorld, key: String, version: u64) {
    exactly_one(world, &key, version).await;
}

async fn exactly_one(world: &mut ClientWorld, key: &str, version: u64) {
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!(
        (update.key.as_str(), update.version),
        (key, Some(version)),
        "{update:?}"
    );
    let later = events_within(
        world.shared_polls.events.as_mut().expect("a stream"),
        QUIET_PERIOD,
    )
    .await;
    assert!(later.is_empty(), "{later:?}");
}

#[when(
    regex = r#"^track_with_signature is called with key "a" and "b" and the fake server answers with items "a" at version 2 and "b" at version 3$"#
)]
async fn tracked_and_answered_with_items(world: &mut ClientWorld) {
    let channel = world.shared_poll().channel().to_owned();
    world
        .fake()
        .answer_a_sub_refresh_with(json!({"sub_refresh": {"items": [
            {"key": "a", "data": {"v": 2}, "version": 2},
            {"key": "b", "data": {"v": 3}, "version": 3},
        ]}}));
    tracked_on_the_fake(world, &channel, vec![("a", 0), ("b", 0)]).await;
}

#[then(
    regex = r#"^the stream yields updates of "a" version 2, "b" version 3 and "a" version 4, in that order$"#
)]
async fn three_updates(world: &mut ClientWorld) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let mut got = Vec::new();
    for _ in 0..3 {
        let update = next_update(events).await;
        got.push((update.key, update.version));
    }
    assert_eq!(
        got,
        [
            ("a".to_owned(), Some(2)),
            ("b".to_owned(), Some(3)),
            ("a".to_owned(), Some(4))
        ]
    );
}

#[when("subscribe is called and the fake server answers with a subscribe result of type 0")]
fn answered_as_a_stream(world: &mut ClientWorld) {
    world.fake().answer_a_subscribe_with(json!({}));
    world.shared_poll().subscribe();
}

#[then(regex = r#"^the stream yields "unsubscribed" with code (\d+)(?: and reason "([^"]+)")?$"#)]
async fn yields_unsubscribed(world: &mut ClientWorld, code: u32, reason: String) {
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Unsubscribed {
                code: got,
                reason: why,
            } => {
                assert_eq!(got, code, "{why}");
                if !reason.is_empty() {
                    assert_eq!(why, reason);
                }
                return;
            }
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected unsubscribed, got {other:?}"),
        }
    }
}

// --- A real server --------------------------------------------------------

#[given(
    regex = r#"^a connected client with a shared poll subscription to "([^"]+)"(?: (whose stream was taken, subscribed|subscribed, whose stream was taken|that has subscribed, tracking "a" and "b"))?$"#
)]
async fn a_connected_client_with(world: &mut ClientWorld, channel: String, how: String) {
    a_connected_client(world).await;
    a_shared_poll_subscription(world, &channel, |b| b);
    if how.is_empty() {
        return;
    }
    if how == "whose stream was taken, subscribed" {
        // Its lifecycle events stay in the stream, for the scenario to see.
        world.shared_poll().subscribe();
        tokio::time::timeout(EVENT_TIMEOUT, world.shared_poll().ready())
            .await
            .expect("subscribed in time")
            .expect("subscribed");
        return;
    }
    subscribed(world).await;
    if how.contains("tracking") {
        track_validly(world, &["a", "b"]).await;
    }
}

/// Tracks `keys` with a valid signature, and waits until the server's
/// reply is read: the client reads one message more.
async fn track_validly(world: &mut ClientWorld, keys: &[&str]) {
    let channel = world.shared_poll().channel().to_owned();
    let signature = shared_poll_signature(&channel, world.shared_poll_user(), keys, None);
    let read = world.client().statistics().messages_in;
    world.shared_poll().track_with_signature(
        keys.iter().map(|key| SharedPollTrackItem::new(*key, 0)),
        signature,
    );
    reply_read(world.client(), read).await;
}

/// Waits until `client` has read a message more than `read`.
async fn reply_read(client: &Client, read: u64) {
    wait_until("the reply", || client.statistics().messages_in > read).await;
}

fn server_api(world: &ClientWorld) -> &crate::support::centrifugo::ServerApi {
    world.server.as_ref().expect("a Centrifugo server").api()
}

#[when(
    regex = r#"^the server API publishes key "(\w+)" of "([^"]+)" at version (\d+) with data (\{.*\})$"#
)]
async fn the_server_api_publishes(
    world: &mut ClientWorld,
    key: String,
    channel: String,
    version: u64,
    data: String,
) {
    let data: Value = serde_json::from_str(&data).unwrap();
    server_api(world)
        .shared_poll_publish(&channel, &key, version, data)
        .await
        .expect("server API shared_poll_publish");
}

#[when(
    regex = r#"^the server API publishes key "(\w+)" at version (\d+) and key "(\w+)" at version (\d+)$"#
)]
async fn the_server_api_publishes_two(
    world: &mut ClientWorld,
    first: String,
    at: u64,
    second: String,
    then: u64,
) {
    let channel = world.shared_poll().channel().to_owned();
    for (key, version) in [(first, at), (second, then)] {
        server_api(world)
            .shared_poll_publish(&channel, &key, version, json!({"v": version}))
            .await
            .expect("server API shared_poll_publish");
    }
}

#[when(
    regex = r#"^track_with_signature is called with key "a" at version 0 and a valid signature for "a"$"#
)]
async fn tracked_validly(world: &mut ClientWorld) {
    track_validly(world, &["a"]).await;
}

#[then(
    regex = r#"^the shared poll subscription's stream yields "subscribing", "subscribed", an update of "a" with data \{"v":2\} and version 2, then an update of "a" with data \{"v":4\} and version 4$"#
)]
async fn yields_two_versions(world: &mut ClientWorld) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let subscribing = next_of(events).await;
    assert_eq!(subscribing.name(), "subscribing", "{subscribing:?}");
    let subscribed = next_of(events).await;
    assert_eq!(subscribed.name(), "subscribed", "{subscribed:?}");
    let first = next_of(events).await;
    let second = next_of(events).await;
    for (event, version) in [(first, 2), (second, 4)] {
        let SharedPollSubscriptionEvent::Update(update) = event else {
            panic!("expected an update, got {event:?}");
        };
        assert_eq!(update.key, "a");
        assert_eq!(update.version, Some(version));
        assert_eq!(json_of(&update.data), json!({"v": version}));
    }
}

#[given(
    regex = r#"^a server with http_stream and sse enabled, and a client with the (JSON|Protobuf) protocol connected over transport "(\w+)" with a shared poll subscription to "([^"]+)"$"#
)]
async fn a_client_over_a_transport(
    world: &mut ClientWorld,
    protocol: String,
    transport: String,
    channel: String,
) {
    world.protocol = if protocol == "Protobuf" {
        Protocol::Protobuf
    } else {
        Protocol::Json
    };
    super::transports::a_client_connected_over(world, String::new(), transport).await;
    a_shared_poll_subscription(world, &channel, |b| b);
}

#[when(
    "subscribe is called on the shared poll subscription and key \"a\" is tracked with a valid signature"
)]
async fn subscribed_and_tracked(world: &mut ClientWorld) {
    subscribed(world).await;
    track_validly(world, &["a"]).await;
}

#[then(
    regex = r#"^the shared poll subscription's stream yields an update of "(\w+)" with data (\{.*\}) and version (\d+)$"#
)]
async fn yields_an_update(world: &mut ClientWorld, key: String, data: String, version: u64) {
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!(update.key, key);
    assert_eq!(update.version, Some(version));
    assert_eq!(
        json_of(&update.data),
        serde_json::from_str::<Value>(&data).unwrap()
    );
}

#[when(regex = r#"^untrack is called with key "(\w+)"$"#)]
fn untrack_is_called(world: &mut ClientWorld, key: String) {
    world.shared_polls.read = world.client().statistics().messages_in;
    world.shared_poll().untrack([key]);
}

#[when("the untrack reply is received")]
async fn the_untrack_reply(world: &mut ClientWorld) {
    reply_read(world.client(), world.shared_polls.read).await;
}

#[then(regex = r#"^the stream yields an update of "(\w+)" and no update of "(\w+)" within 2 s$"#)]
async fn only_one_key(world: &mut ClientWorld, key: String, not: String) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let seen = events_within(events, Duration::from_secs(2)).await;
    let keys: Vec<&str> = seen
        .iter()
        .filter_map(|event| match event {
            SharedPollSubscriptionEvent::Update(update) => Some(update.key.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(keys, [key.as_str()], "{seen:?}");
    assert!(!keys.contains(&not.as_str()));
}

#[then(
    regex = r#"^the stream yields an error with code (\d+)(?: of kind Track)? and (?:an update|updates) of ((?:"\w+"(?:, | and )?)+) with removed true$"#
)]
async fn an_error_and_removals(world: &mut ClientWorld, code: u32, keys: String) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let SharedPollSubscriptionEvent::Error(error) = next_of(events).await else {
        panic!("expected an error first");
    };
    assert_eq!(
        (error.code, error.kind),
        (code, ErrorKind::Track),
        "{error:?}"
    );
    let mut removed = Vec::new();
    for _ in quoted(&keys) {
        let update = next_update(events).await;
        assert!(
            update.removed && update.data.is_empty() && update.version.is_none(),
            "{update:?}"
        );
        removed.push(update.key);
    }
    assert_eq!(removed, quoted(&keys));
}

#[given(
    regex = r#"^a connected client with a shared poll subscription to "([^"]+)" with a get_token callback returning tokens with a (\d+) s lifetime$"#
)]
async fn with_a_token_callback(world: &mut ClientWorld, channel: String, lifetime: u64) {
    a_connected_client(world).await;
    let calls = Arc::clone(&world.shared_polls.token_calls);
    a_shared_poll_subscription(world, &channel, move |builder| {
        builder.get_token(move |channel| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(subscription_token(
                    "",
                    &channel,
                    Duration::from_secs(lifetime),
                ))
            }
        })
    });
}

#[then(r#"the subscribe carries the token and the subscription becomes "subscribed""#)]
async fn becomes_subscribed(world: &mut ClientWorld) {
    // The namespace refuses a subscribe without a valid token.
    loop {
        match world.next_shared_poll_event().await {
            SharedPollSubscriptionEvent::Subscribed(_) => break,
            SharedPollSubscriptionEvent::Subscribing { .. } => {}
            other => panic!("expected subscribed, got {other:?}"),
        }
    }
    assert_eq!(world.shared_polls.token_calls.load(Ordering::SeqCst), 1);
}

#[then(
    regex = r#"^within (\d+) s get_token has been called at least twice and the subscription stays "subscribed"$"#
)]
async fn refreshed(world: &mut ClientWorld, seconds: u64) {
    let events = world.shared_polls.events.as_mut().expect("a stream");
    let seen = events_within(events, Duration::from_secs(seconds)).await;
    assert_eq!(seen, [], "events while refreshing");
    let calls = world.shared_polls.token_calls.load(Ordering::SeqCst);
    assert!(calls >= 2, "{calls} get_token calls");
    assert_eq!(world.shared_poll().state(), SubscriptionState::Subscribed);
}

// --- The test backend (@B) -----------------------------------------------

fn backend(world: &ClientWorld) -> &crate::support::proxy::ProxyBackend {
    world
        .proxy
        .as_ref()
        .expect("the test backend: tag the scenario @B")
}

#[given(regex = r#"^the backend's refresh answers key "(\w+)" of "([^"]+)" with data (\{.*\})$"#)]
fn the_backend_answers(world: &mut ClientWorld, key: String, channel: String, data: String) {
    let data: Value = serde_json::from_str(&data).unwrap();
    backend(world).shared_poll_item(&channel, &key, json!({"data": data}));
}

#[given(
    regex = r#"^a client and another client each with a shared poll subscription to "([^"]+)" subscribed, each tracking "a" with a valid signature$"#
)]
async fn two_clients_tracking(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    a_shared_poll_subscription(world, &channel, |b| b);
    subscribed(world).await;
    track_validly(world, &["a"]).await;
    let other = Client::builder(world.server_endpoint())
        .config(super::test_config())
        .build()
        .unwrap();
    let subscription = other.new_shared_poll_subscription(channel.clone()).unwrap();
    let mut events = recorded(subscription.events());
    other.connect();
    subscription.subscribe();
    loop {
        if let SharedPollSubscriptionEvent::Subscribed(_) = next_of(&mut events).await {
            break;
        }
    }
    let read = other.statistics().messages_in;
    subscription.track_with_signature(
        [SharedPollTrackItem::new("a", 0)],
        shared_poll_signature(&channel, world.shared_poll_user(), &["a"], None),
    );
    reply_read(&other, read).await;
    world.shared_polls.other = Some((other, subscription, events));
}

#[then(
    regex = r#"^each shared poll subscription's stream yields an update of "a" with data (\{.*\}) and a version greater than 0$"#
)]
async fn each_yields(world: &mut ClientWorld, data: String) {
    each_yields_data(world, &data, true).await;
}

async fn each_yields_data(world: &mut ClientWorld, data: &str, versioned: bool) {
    let expected: Value = serde_json::from_str(data).unwrap();
    let (_, _, other) = world.shared_polls.other.as_mut().expect("another client");
    let theirs = next_update(other).await;
    let ours = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    for update in [ours, theirs] {
        assert_eq!(update.key, "a");
        assert_eq!(json_of(&update.data), expected);
        if versioned {
            assert!(
                update.version.is_some_and(|version| version > 0),
                "{update:?}"
            );
        }
    }
}

#[then(
    regex = r#"^when the backend's refresh answers key "a" with data (\{.*\}), each stream yields an update of "a" with data (\{.*\})$"#
)]
async fn a_new_value(world: &mut ClientWorld, data: String, expected: String) {
    let channel = world.shared_poll().channel().to_owned();
    let value: Value = serde_json::from_str(&data).unwrap();
    backend(world).shared_poll_item(&channel, "a", json!({"data": value}));
    each_yields_data(world, &expected, false).await;
}

#[then(
    regex = r#"^the backend's refresh requests for "([^"]+)" never name "a" more than once per request$"#
)]
fn once_per_request(world: &mut ClientWorld, channel: String) {
    let requests = backend(world).shared_poll_requests();
    let requests: Vec<&Value> = requests
        .iter()
        .filter(|request| request["channel"] == channel.as_str())
        .collect();
    assert!(!requests.is_empty(), "no refresh request");
    for request in requests {
        let named = request["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| item["key"] == "a")
            .count();
        assert!(named <= 1, "{request}");
    }
}

#[given(
    regex = r#"^a client with a shared poll subscription to "([^"]+)" subscribed, tracking "a" with a valid signature, whose update of "a" was taken$"#
)]
async fn tracking_with_a_taken_update(world: &mut ClientWorld, channel: String) {
    backend(world).shared_poll_item(&channel, "a", json!({"data": {"v": 1}}));
    a_connected_client(world).await;
    a_shared_poll_subscription(world, &channel, |b| b);
    subscribed(world).await;
    track_validly(world, &["a"]).await;
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!((update.key.as_str(), update.removed), ("a", false));
}

#[when(regex = r#"^the backend's refresh answers key "(\w+)" with removed true$"#)]
fn the_backend_removes(world: &mut ClientWorld, key: String) {
    let channel = world.shared_poll().channel().to_owned();
    backend(world).shared_poll_item(&channel, &key, json!({"removed": true}));
}

#[then(
    regex = r#"^the stream yields an update of "(\w+)" with removed true, empty data and no version$"#
)]
async fn yields_a_removal(world: &mut ClientWorld, key: String) {
    let update = next_update(world.shared_polls.events.as_mut().expect("a stream")).await;
    assert_eq!(
        (update.key, update.removed, update.data, update.version),
        (key, true, Vec::new(), None)
    );
}
