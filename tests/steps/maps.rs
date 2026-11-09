//! Block 45: map subscriptions (`unstable`).

// Cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    CallError, Client, ClientEvent, MapKind, MapSubscription, MapSubscriptionEvent, Protocol,
    Subscription, SubscriptionError, SubscriptionEvent, SubscriptionState,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::connection::a_connected_client;
use super::delivery::wait_until;
use super::{ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, recorded};
use crate::support::fake_server::sized_json;
use crate::support::token::{connection_token, subscription_token};

type MapEvents = Pin<Box<dyn Stream<Item = MapSubscriptionEvent> + Send>>;

/// What a Block 45 scenario holds.
#[derive(Default)]
pub(crate) struct Maps {
    subscription: Option<MapSubscription>,
    events: Option<MapEvents>,
    /// The other client of a scenario, and its map subscription.
    other: Option<(Client, MapSubscription, MapEvents)>,
    /// The second client of a presence scenario: its id, and its
    /// subscription to the stream channel.
    second: Option<(Client, String, Subscription)>,
    created: Option<Result<(), SubscriptionError>>,
    calls: Vec<JoinHandle<Result<(), CallError>>>,
    token_calls: Arc<AtomicUsize>,
    token_calls_at_sync: Option<usize>,
    /// Subscribe commands for the channel the scenario answered by hand.
    answered: usize,
    /// The key of the last update a step checked.
    key: Option<String>,
}

fn kind(name: &str) -> MapKind {
    match name {
        "map" => MapKind::Map,
        "clients" => MapKind::Clients,
        "users" => MapKind::Users,
        other => panic!("no map kind {other}"),
    }
}

impl ClientWorld {
    fn map(&self) -> &MapSubscription {
        self.maps.subscription.as_ref().expect("a map subscription")
    }

    /// Makes `subscription` the scenario's, recording its events.
    fn track_map(&mut self, subscription: MapSubscription) {
        self.maps.events = Some(recorded(subscription.events()));
        self.maps.subscription = Some(subscription);
    }

    async fn next_map_event(&mut self) -> MapSubscriptionEvent {
        let events = self
            .maps
            .events
            .as_mut()
            .expect("a map subscription's stream");
        tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .unwrap_or_else(|_| panic!("no map subscription event within {EVENT_TIMEOUT:?}"))
            .expect("the map subscription's stream ended")
    }

    async fn next_map_event_named(&mut self, name: &str) -> MapSubscriptionEvent {
        let event = self.next_map_event().await;
        assert_eq!(event.name(), name, "{event:?}");
        event
    }
}

/// The events `events` yields within `QUIET_PERIOD`.
async fn quiet_events(events: &mut MapEvents) -> Vec<MapSubscriptionEvent> {
    let mut seen = Vec::new();
    while let Ok(Some(event)) = tokio::time::timeout(QUIET_PERIOD, events.next()).await {
        seen.push(event);
    }
    seen
}

fn entry_keys(event: &MapSubscriptionEvent) -> Vec<String> {
    let MapSubscriptionEvent::Sync(entries) = event else {
        panic!("expected a sync, got {event:?}");
    };
    entries.iter().map(|entry| entry.key.clone()).collect()
}

fn quoted(list: &str) -> Vec<String> {
    list.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

fn server_api(world: &ClientWorld) -> &crate::support::centrifugo::ServerApi {
    world.server.as_ref().expect("a Centrifugo server").api()
}

async fn map_publish(world: &ClientWorld, channel: &str, key: &str, data: Value) {
    server_api(world)
        .call(
            "map_publish",
            json!({"channel": channel, "key": key, "data": data}),
        )
        .await
        .expect("server API map_publish");
}

async fn map_remove(world: &ClientWorld, channel: &str, key: &str) {
    server_api(world)
        .call("map_remove", json!({"channel": channel, "key": key}))
        .await
        .expect("server API map_remove");
}

// --- Registry -------------------------------------------------------------

#[given(
    regex = r#"^a new client with a (subscription|map subscription of kind "(\w+)") to channel "(\w+:[^"]+)"$"#
)]
fn a_new_client_with(world: &mut ClientWorld, what: String, of: String, channel: String) {
    world.new_client(super::UNREACHABLE_ENDPOINT.to_owned());
    if what == "subscription" {
        world.client().new_subscription(channel).unwrap();
    } else {
        world
            .client()
            .new_map_subscription(channel, kind(&of))
            .unwrap();
    }
}

#[when(
    regex = r#"^a (subscription|map subscription of kind "(\w+)") to channel "(\w+:[^"]+)" is created$"#
)]
fn one_is_created(world: &mut ClientWorld, what: String, of: String, channel: String) {
    let created = if what == "subscription" {
        world.client().new_subscription(channel).map(drop)
    } else {
        world
            .client()
            .new_map_subscription(channel, kind(&of))
            .map(|subscription| world.maps.subscription = Some(subscription))
    };
    world.maps.created = Some(created);
}

#[then(regex = r#"^creating it fails with "([^"]+)"$"#)]
fn creating_it_fails_with(world: &mut ClientWorld, message: String) {
    let created = world.maps.created.take().expect("a creation");
    assert_eq!(created.expect_err("a failure").to_string(), message);
}

#[then(regex = r#"^the map subscription state is "(\w+)"$"#)]
fn the_map_subscription_state(world: &mut ClientWorld, state: String) {
    assert_eq!(format!("{:?}", world.map().state()).to_lowercase(), state);
}

#[then(regex = r#"^the client's map subscriptions are "([^"]+)" and its subscriptions are none$"#)]
fn the_registry_lists(world: &mut ClientWorld, channel: String) {
    let maps: Vec<String> = world
        .client()
        .map_subscriptions()
        .iter()
        .map(|subscription| subscription.channel().to_owned())
        .collect();
    assert_eq!(maps, std::slice::from_ref(&channel));
    assert_eq!(world.client().subscriptions(), []);
    assert!(world.client().get_subscription(&channel).is_none());
    assert_eq!(
        world.client().get_map_subscription(&channel).as_ref(),
        Some(world.map())
    );
}

#[when("the map subscription is removed")]
fn the_map_subscription_is_removed(world: &mut ClientWorld) {
    let subscription = world.map().clone();
    world.client().remove_map_subscription(&subscription);
}

#[then("the client's map subscriptions are none")]
fn no_map_subscriptions(world: &mut ClientWorld) {
    assert_eq!(world.client().map_subscriptions(), []);
}

#[then(regex = r#"^a subscription to channel "(\w+)" can be created$"#)]
fn a_subscription_can_be_created(world: &mut ClientWorld, channel: String) {
    world.client().new_subscription(channel).expect("created");
}

// --- The fake server ------------------------------------------------------

/// The subscribe commands for `channel` the fake server received.
fn subscribes(world: &ClientWorld, channel: &str) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command["subscribe"]["channel"] == channel)
        .collect()
}

fn unsubscribes(world: &ClientWorld, channel: &str) -> usize {
    world
        .fake()
        .received()
        .iter()
        .filter(|command| command["unsubscribe"]["channel"] == channel)
        .count()
}

/// Waits for the `n`th subscribe command (from 1) for `channel`.
async fn nth_subscribe(world: &ClientWorld, channel: &str, n: usize) -> Value {
    let waited = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(command) = subscribes(world, channel).into_iter().nth(n - 1) {
                return command;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    waited.unwrap_or_else(|_| panic!("no subscribe command {n} for {channel}"))
}

/// Answers the next subscribe command for "scores" not answered yet with
/// `result`.
async fn answer_next(world: &mut ClientWorld, result: Value) {
    world.maps.answered += 1;
    let command = nth_subscribe(world, "scores", world.maps.answered).await;
    world
        .fake()
        .send(&json!({"id": command["id"], "subscribe": result}).to_string());
}

fn entry(key: &str, data: Value, offset: u64) -> Value {
    json!({"key": key, "data": data, "offset": offset})
}

/// A map subscription to `channel` on the client, built with `page_size`,
/// `max_state_bytes`, a token and data as given, its stream taken; the
/// fake server leaves its requests to the steps.
fn a_scripted_map_subscription(
    world: &mut ClientWorld,
    channel: &str,
    configure: impl FnOnce(ferrifuge::MapSubscriptionBuilder) -> ferrifuge::MapSubscriptionBuilder,
) {
    world.fake().ignore_subscribes();
    let subscription = configure(
        world
            .client()
            .map_subscription_builder(channel, MapKind::Map),
    )
    .build()
    .unwrap();
    world.track_map(subscription);
}

#[given(
    regex = r#"^a client connected to the fake server, with a map subscription to "(\w+)"(.*)$"#
)]
async fn a_client_with_a_map_subscription(world: &mut ClientWorld, channel: String, how: String) {
    a_connected_client(world).await;
    match how.as_str() {
        "" | " whose stream was taken" => a_scripted_map_subscription(world, &channel, |b| b),
        " whose max_state_bytes is 1024 and whose stream was taken" => {
            a_scripted_map_subscription(world, &channel, |builder| {
                builder.max_state_bytes(std::num::NonZeroUsize::new(1024).unwrap())
            });
        }
        r#" with token "t1" and data {"room":1}"# => {
            a_scripted_map_subscription(world, &channel, |builder| {
                builder.token("t1").data(br#"{"room":1}"#.to_vec())
            });
        }
        " that has synced" => {
            a_scripted_map_subscription(world, &channel, |b| b);
            world.map().subscribe();
            answer_next(world, json!({"type": 1, "epoch": "e1", "offset": 1})).await;
            world.next_map_event_named("subscribing").await;
            world.next_map_event_named("subscribed").await;
            world.next_map_event_named("sync").await;
        }
        " whose first page is not answered yet" => {
            a_scripted_map_subscription(world, &channel, |b| b);
            world.map().subscribe();
            nth_subscribe(world, &channel, 1).await;
        }
        " whose first page was answered with a cursor" => {
            a_scripted_map_subscription(world, &channel, |b| b);
            world.map().subscribe();
            answer_next(
                world,
                json!({"type": 1, "phase": 2, "cursor": "c1", "epoch": "e1", "offset": 5}),
            )
            .await;
            nth_subscribe(world, &channel, 2).await;
        }
        " whose first state request is in flight" => {
            a_scripted_map_subscription(world, &channel, |b| b);
            world.map().subscribe();
            nth_subscribe(world, &channel, 1).await;
        }
        " whose stream request is in flight" => {
            a_scripted_map_subscription(world, &channel, |b| b);
            world.map().subscribe();
            answer_next(
                world,
                json!({"type": 1, "phase": 2, "epoch": "e1", "offset": 5}),
            )
            .await;
            nth_subscribe(world, &channel, 2).await;
        }
        other => panic!("no such map subscription: {other}"),
    }
}

#[given(
    regex = r#"^a map subscription of kind "map" to channel "(\w+)" with page size (\d+), whose stream was taken$"#
)]
fn a_map_subscription_with_a_page_size(world: &mut ClientWorld, channel: String, size: u32) {
    a_scripted_map_subscription(world, &channel, |builder| builder.page_size(size));
}

#[when("subscribe is called on the map subscription")]
fn subscribe_is_called(world: &mut ClientWorld) {
    world.map().subscribe();
}

#[when("unsubscribe is called on the map subscription")]
fn unsubscribe_is_called(world: &mut ClientWorld) {
    world.map().unsubscribe();
}

#[when(
    regex = r#"^(?:the fake server )?answers the (?:first|next) request with phase 2, (?:entries|entry) ((?:"\w+"(?:, | and )?)+), (?:cursor "(\w+)"|no cursor)(?:, | and )epoch "(\w+)"(?: and offset (\d+))?$"#
)]
async fn answers_a_state_page(
    world: &mut ClientWorld,
    entries: String,
    cursor: String,
    epoch: String,
    offset: String,
) {
    let state: Vec<Value> = quoted(&entries)
        .iter()
        .zip(1..)
        .map(|(key, offset)| entry(key, json!({"key": key}), offset))
        .collect();
    let mut result = json!({
        "type": 1,
        "phase": 2,
        "epoch": epoch,
        "offset": offset.parse::<u64>().unwrap_or(1),
        "state": state,
    });
    if !cursor.is_empty() {
        result["cursor"] = json!(cursor);
    }
    answer_next(world, result).await;
}

#[when(
    regex = r#"^answers the next request live with a publication setting "(\w+)" at offset (\d+), epoch "(\w+)" and offset (\d+)$"#
)]
async fn answers_live(world: &mut ClientWorld, key: String, at: u64, epoch: String, offset: u64) {
    answer_next(
        world,
        json!({
            "type": 1, "recoverable": true, "epoch": epoch, "offset": offset,
            "publications": [entry(&key, json!({"key": key}), at)],
        }),
    )
    .await;
}

#[then(
    r#"the fake server received, for "scores", a subscribe with type 1, phase 2 and limit 2 and no cursor, then one with phase 2, limit 2, cursor "c1", epoch "e1" and offset 5, then one with phase 1, limit 2, epoch "e1" and offset 5"#
)]
fn the_three_requests(world: &mut ClientWorld) {
    let requests: Vec<Value> = subscribes(world, "scores")
        .into_iter()
        .map(|command| command["subscribe"].clone())
        .collect();
    assert_eq!(requests.len(), 3, "{requests:?}");
    let [first, second, third] = &requests[..] else {
        unreachable!();
    };
    assert_eq!(
        (&first["type"], &first["phase"], &first["limit"]),
        (&json!(1), &json!(2), &json!(2))
    );
    assert!(first.get("cursor").is_none(), "{first}");
    assert_eq!(
        (
            &second["phase"],
            &second["limit"],
            &second["cursor"],
            &second["epoch"],
            &second["offset"]
        ),
        (&json!(2), &json!(2), &json!("c1"), &json!("e1"), &json!(5))
    );
    assert_eq!(
        (
            &third["phase"],
            &third["limit"],
            &third["epoch"],
            &third["offset"]
        ),
        (&json!(1), &json!(2), &json!("e1"), &json!(5))
    );
    assert!(third.get("cursor").is_none(), "{third}");
}

#[then(
    regex = r#"^the map subscription's stream yields "subscribing", "subscribed", then a sync with entries ((?:"\w+"(?:, | and )?)+) in that order$"#
)]
async fn yields_a_sync_with(world: &mut ClientWorld, keys: String) {
    world.next_map_event_named("subscribing").await;
    world.next_map_event_named("subscribed").await;
    let sync = world.next_map_event_named("sync").await;
    assert_eq!(entry_keys(&sync), quoted(&keys));
}

#[when(regex = r#"^a map subscription to channel "(\w+)" with page size (\d+) is subscribed$"#)]
fn a_map_subscription_is_subscribed(world: &mut ClientWorld, channel: String, size: u32) {
    a_scripted_map_subscription(world, &channel, |builder| builder.page_size(size));
    world.map().subscribe();
}

#[then(regex = r#"^the first subscribe command for "(\w+)" has type 1, phase 2 and no limit$"#)]
async fn the_first_has_no_limit(world: &mut ClientWorld, channel: String) {
    let first = nth_subscribe(world, &channel, 1).await;
    let subscribe = &first["subscribe"];
    assert_eq!(
        (&subscribe["type"], &subscribe["phase"]),
        (&json!(1), &json!(2))
    );
    assert!(subscribe.get("limit").is_none(), "{subscribe}");
}

#[when(
    r#"the fake server answers with state pages holding "a" = 1, "b" = 1 and "c" = 1, then a stream page with "a" = 2 at offset 6 and the removal of "b" at offset 7, then live with "d" = 1 at offset 8"#
)]
async fn answers_with_changes(world: &mut ClientWorld) {
    answer_next(
        world,
        json!({"type": 1, "phase": 2, "cursor": "c1", "epoch": "e1", "offset": 5,
            "state": [entry("a", json!(1), 1), entry("b", json!(1), 2)]}),
    )
    .await;
    answer_next(
        world,
        json!({"type": 1, "phase": 2, "epoch": "e1", "offset": 5,
            "state": [entry("c", json!(1), 3)]}),
    )
    .await;
    answer_next(
        world,
        json!({"type": 1, "phase": 1, "epoch": "e1", "offset": 7,
            "publications": [entry("a", json!(2), 6), {"key": "b", "removed": true, "offset": 7}]}),
    )
    .await;
    answer_next(
        world,
        json!({"type": 1, "recoverable": true, "epoch": "e1", "offset": 8,
            "publications": [entry("d", json!(1), 8)]}),
    )
    .await;
}

#[then(
    r#"the map subscription's stream yields "subscribing", "subscribed" with recovered false, then a sync with exactly "a" = 2, "c" = 1 and "d" = 1, in that order"#
)]
async fn yields_the_changed_sync(world: &mut ClientWorld) {
    world.next_map_event_named("subscribing").await;
    let MapSubscriptionEvent::Subscribed(subscribed) =
        world.next_map_event_named("subscribed").await
    else {
        unreachable!();
    };
    assert!(!subscribed.recovered);
    let MapSubscriptionEvent::Sync(entries) = world.next_map_event_named("sync").await else {
        unreachable!();
    };
    let got: Vec<(String, Vec<u8>)> = entries
        .into_iter()
        .map(|entry| (entry.key, entry.data))
        .collect();
    assert_eq!(
        got,
        [
            ("a".to_owned(), b"2".to_vec()),
            ("c".to_owned(), b"1".to_vec()),
            ("d".to_owned(), b"1".to_vec())
        ]
    );
}

#[then("it yields no update")]
async fn yields_no_update(world: &mut ClientWorld) {
    let events = world.maps.events.as_mut().expect("a stream");
    let later = quiet_events(events).await;
    assert!(later.is_empty(), "{later:?}");
}

#[then(
    regex = r#"^once it is answered, the next subscribe command for "(\w+)" has phase 2, no cursor, no epoch and no offset$"#
)]
async fn the_load_starts_again(world: &mut ClientWorld, channel: String) {
    let again = nth_subscribe(world, &channel, 3).await;
    let unsubscribe = world
        .fake()
        .received()
        .into_iter()
        .position(|command| command["unsubscribe"]["channel"] == channel.as_str());
    let position = world
        .fake()
        .received()
        .into_iter()
        .position(|command| command["id"] == again["id"]);
    assert!(
        unsubscribe < position,
        "the subscribe went before the unsubscribe"
    );
    let subscribe = &again["subscribe"];
    assert_eq!(subscribe["phase"], 2);
    for absent in ["cursor", "epoch", "offset"] {
        assert!(subscribe.get(absent).is_none(), "{subscribe}");
    }
}

#[then(
    regex = r#"^when the fake server answers it live with entry "(\w+)" and epoch "(\w+)", the stream yields "subscribing", "subscribed" and a sync with exactly "(\w+)"$"#
)]
async fn answered_live_after_the_restart(
    world: &mut ClientWorld,
    key: String,
    epoch: String,
    exactly: String,
) {
    answer_next(
        world,
        json!({"type": 1, "recoverable": true, "epoch": epoch, "offset": 1,
            "state": [entry(&key, json!(1), 1)]}),
    )
    .await;
    world.next_map_event_named("subscribing").await;
    world.next_map_event_named("subscribed").await;
    let sync = world.next_map_event_named("sync").await;
    assert_eq!(entry_keys(&sync), [exactly]);
}

#[when(regex = r#"^the map subscription publishes (\{.*\}) to key "(\w+)"$"#)]
fn the_map_subscription_publishes(world: &mut ClientWorld, data: String, key: String) {
    let call = world.map().publish(key, data.into_bytes());
    world.maps.calls.push(tokio::spawn(call));
}

#[then("the fake server has received no publish command 1 s later")]
async fn no_publish_yet(world: &mut ClientWorld) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(world.fake().commands_with_times("publish").len(), 0);
}

#[then(
    regex = r#"^when the fake server answers the page live, it receives a publish with type 1, key "(\w+)" and data (\{.*\})$"#
)]
async fn the_publish_follows(world: &mut ClientWorld, key: String, data: String) {
    answer_next(world, json!({"type": 1, "epoch": "e1", "offset": 1})).await;
    let fake = world.fake();
    wait_until("a publish command", || {
        !fake.commands_with_times("publish").is_empty()
    })
    .await;
    let (publish, _) = fake.commands_with_times("publish").remove(0);
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        (
            &publish["publish"]["type"],
            &publish["publish"]["key"],
            &publish["publish"]["data"]
        ),
        (&json!(1), &json!(key), &data)
    );
}

#[when("subscribe is called and the fake server answers two state pages and then live")]
async fn two_pages_then_live(world: &mut ClientWorld) {
    world.map().subscribe();
    answer_next(
        world,
        json!({"type": 1, "phase": 2, "cursor": "c1", "epoch": "e1", "offset": 5}),
    )
    .await;
    answer_next(
        world,
        json!({"type": 1, "phase": 2, "cursor": "c2", "epoch": "e1", "offset": 5}),
    )
    .await;
    answer_next(world, json!({"type": 1, "epoch": "e1", "offset": 5})).await;
    world.next_map_event_named("subscribing").await;
    world.next_map_event_named("subscribed").await;
}

#[then(regex = r#"^each of the three subscribe commands carries token "(\w+)"$"#)]
fn each_carries_the_token(world: &mut ClientWorld, token: String) {
    let requests = subscribes(world, "scores");
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|command| command["subscribe"]["token"] == token.as_str()),
        "{requests:?}"
    );
}

#[then(regex = r"^only the first carries data (\{.*\})$")]
fn only_the_first_carries_data(world: &mut ClientWorld, data: String) {
    let requests = subscribes(world, "scores");
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(requests[0]["subscribe"]["data"], data);
    assert!(
        requests[1..]
            .iter()
            .all(|command| command["subscribe"].get("data").is_none()),
        "{requests:?}"
    );
}

#[when("the fake server does not answer the second page within the configured timeout")]
fn the_second_page_is_not_answered(_world: &mut ClientWorld) {}

#[when("the fake server answers it with a subscribe result without type and without phase")]
async fn answers_as_a_stream_subscription(world: &mut ClientWorld) {
    answer_next(world, json!({})).await;
}

#[then(
    regex = r#"^the map subscription emits "unsubscribed" with code (\d+) and reason "([^"]+)", and no sync$"#
)]
async fn emits_unsubscribed_and_no_sync(world: &mut ClientWorld, code: u32, reason: String) {
    world.next_map_event_named("subscribing").await;
    let event = world.next_map_event_named("unsubscribed").await;
    assert_eq!(event, MapSubscriptionEvent::Unsubscribed { code, reason });
    let events = world.maps.events.as_mut().expect("a stream");
    let later = quiet_events(events).await;
    assert!(
        later.iter().all(|event| event.name() != "sync"),
        "{later:?}"
    );
}

#[then(
    regex = r#"^2 s later the fake server has received no further subscribe command for "(\w+)"$"#
)]
async fn no_further_subscribe(world: &mut ClientWorld, channel: String) {
    let before = subscribes(world, &channel).len();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(subscribes(world, &channel).len(), before);
}

#[when(regex = r"^the fake server answers it with phase (\d+)$")]
async fn answers_with_phase(world: &mut ClientWorld, phase: u32) {
    let mut result = json!({"type": 1, "phase": phase, "epoch": "e1", "offset": 5});
    if phase == 2 {
        result["cursor"] = json!("c9");
    }
    answer_next(world, result).await;
}

#[when("the fake server answers each request with a state page of 600 bytes and a cursor")]
async fn answers_each_with_600_bytes(world: &mut ClientWorld) {
    // The client asks for no page after the second, past its bound.
    for page in 1..=2 {
        world.maps.answered += 1;
        let command = nth_subscribe(world, "scores", world.maps.answered).await;
        let prefix = format!(
            r#"{{"id":{},"subscribe":{{"type":1,"phase":2,"cursor":"c{page}","epoch":"e1","offset":5,"state":[{{"key":"k{page}","data":""#,
            command["id"]
        );
        world.fake().send(&sized_json(&prefix, r#""}]}}"#, 600));
    }
}

#[then(
    regex = r#"^after the second page the fake server receives an unsubscribe command for "(\w+)" and no further subscribe command$"#
)]
async fn unsubscribed_after_the_second_page(world: &mut ClientWorld, channel: String) {
    wait_until("an unsubscribe command", || {
        unsubscribes(world, &channel) > 0
    })
    .await;
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(subscribes(world, &channel).len(), 2);
}

#[then(
    regex = r#"^the map subscription's stream yields "subscribing", then "unsubscribed" with code (\d+) and reason "([^"]+)", and no sync$"#
)]
async fn yields_unsubscribed_and_no_sync(world: &mut ClientWorld, code: u32, reason: String) {
    emits_unsubscribed_and_no_sync(world, code, reason).await;
}

#[then(
    regex = r#"^the fake server receives an unsubscribe command for "(\w+)" and no further subscribe command$"#
)]
async fn an_unsubscribe_and_no_subscribe(world: &mut ClientWorld, channel: String) {
    wait_until("an unsubscribe command", || {
        unsubscribes(world, &channel) > 0
    })
    .await;
    let before = subscribes(world, &channel).len();
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(subscribes(world, &channel).len(), before);
    assert_eq!(before, 1);
}

#[when("the fake server answers the page with a cursor")]
async fn answers_the_page_with_a_cursor(world: &mut ClientWorld) {
    answer_next(
        world,
        json!({"type": 1, "phase": 2, "cursor": "c1", "epoch": "e1", "offset": 5}),
    )
    .await;
}

// Pages are handled while another subscription's stream is not read.

#[given(
    regex = r#"^a client connected to the fake server, with a subscription to "(\w+)" whose stream has been read up to "subscribed" and is not read after$"#
)]
async fn a_subscription_not_read(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    let subscription = world.client().new_subscription(channel).unwrap();
    let mut events = subscription.events();
    subscription.subscribe();
    loop {
        let event = tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .expect("subscribed in time")
            .expect("a stream");
        if matches!(event, SubscriptionEvent::Subscribed(_)) {
            break;
        }
    }
    world.delivery.subscription_events = Some(Box::pin(events));
    world.subscription = Some(subscription);
}

#[given(regex = r#"^the fake server has pushed (\d+) publications to "(\w+)"$"#)]
async fn pushed_publications(world: &mut ClientWorld, count: u64, channel: String) {
    for offset in 1..=count {
        world.fake().publish(&channel, offset);
    }
    let subscription = world.subscription.clone().expect("a subscription");
    // The first is handled and holds the others, which wait read.
    wait_until("the publications held", || subscription.held_events() >= 1).await;
}

#[given(regex = r#"^a map subscription to "(\w+)" whose stream was taken$"#)]
fn a_map_subscription_whose_stream_was_taken(world: &mut ClientWorld, channel: String) {
    a_scripted_map_subscription(world, &channel, |b| b);
}

#[when("the fake server answers its first page with a cursor")]
async fn answers_its_first_page(world: &mut ClientWorld) {
    answers_the_page_with_a_cursor(world).await;
}

#[then(regex = r#"^the client sends the second page request for "(\w+)" within 1 s$"#)]
async fn the_second_page_request(world: &mut ClientWorld, channel: String) {
    let second = tokio::time::timeout(Duration::from_secs(1), nth_subscribe(world, &channel, 2))
        .await
        .expect("the second page asked within 1 s");
    assert_eq!(second["subscribe"]["cursor"], "c1");
}

#[then(
    r#"when the fake server answers it live, the map subscription's stream yields nothing beyond "subscribing" within 1 s"#
)]
async fn the_live_reply_waits(world: &mut ClientWorld) {
    answer_next(world, json!({"type": 1, "epoch": "e1", "offset": 5})).await;
    let events = world.maps.events.as_mut().expect("a stream");
    let seen = quiet_events(events).await;
    let names: Vec<_> = seen.iter().map(MapSubscriptionEvent::name).collect();
    assert_eq!(names, ["subscribing"]);
}

#[then(
    regex = r#"^once the (\d+) publications of "(\w+)" are taken, the map subscription's stream yields "subscribed" and a sync$"#
)]
async fn once_taken(world: &mut ClientWorld, count: usize, channel: String) {
    assert_eq!(
        world.subscription.as_ref().map(Subscription::channel),
        Some(channel.as_str())
    );
    let events = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    for _ in 0..count {
        let event = tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .expect("a publication in time")
            .expect("a stream");
        assert_eq!(event.name(), "publication");
    }
    world.next_map_event_named("subscribed").await;
    world.next_map_event_named("sync").await;
}

// --- The real server ------------------------------------------------------

/// A client of the scenario's server with a map subscription of `kind` to
/// `channel`, connected and subscribed, its sync taken: the client, the
/// subscription, its stream and the sync.
async fn synced_client(
    world: &mut ClientWorld,
    channel: &str,
    kind: MapKind,
) -> (Client, MapSubscription, MapEvents, MapSubscriptionEvent) {
    let client = Client::builder(world.server_endpoint())
        .config(super::test_config())
        .build()
        .unwrap();
    let subscription = client.new_map_subscription(channel, kind).unwrap();
    let mut events = recorded(subscription.events());
    client.connect();
    subscription.subscribe();
    let sync = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let event = events.next().await.expect("a stream");
            match event {
                MapSubscriptionEvent::Sync(_) => return event,
                MapSubscriptionEvent::Unsubscribed { .. } | MapSubscriptionEvent::Error(_) => {
                    panic!("{event:?}")
                }
                _ => {}
            }
        }
    })
    .await
    .expect("a sync in time");
    (client, subscription, events, sync)
}

/// Makes the synced map subscription of `channel` the scenario's, on the
/// scenario's client.
async fn the_clients_synced_map(world: &mut ClientWorld, channel: &str, kind: MapKind) {
    let (client, subscription, events, _) = synced_client(world, channel, kind).await;
    world.client = Some(client);
    world.maps.subscription = Some(subscription);
    world.maps.events = Some(events);
}

#[given(
    regex = r#"^a connected client and another client with a map subscription to "([^"]+)" that has synced$"#
)]
async fn a_client_and_another_with_a_map(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    let (client, subscription, events, _) = synced_client(world, &channel, MapKind::Map).await;
    world.maps.other = Some((client, subscription, events));
}

/// The next update `events` yields, the events before it skipped.
async fn next_update(events: &mut MapEvents) -> ferrifuge::MapUpdate {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let MapSubscriptionEvent::Update(update) = events.next().await.expect("a stream") {
                return update;
            }
        }
    })
    .await
    .expect("an update in time")
}

#[then(
    regex = r#"^the other client's map subscription emits an update with key "(\w+)", data (\{.*\}), removed false and info naming the publishing client$"#
)]
async fn the_other_gets_the_update(world: &mut ClientWorld, key: String, data: String) {
    let publisher = world.client_id.clone().expect("the publishing client's id");
    let (_, _, events) = world.maps.other.as_mut().expect("the other client");
    let update = next_update(events).await;
    assert_eq!(update.key, key);
    assert_eq!(update.data, data.into_bytes());
    assert!(!update.removed);
    assert_eq!(update.info.expect("info").client, publisher);
}

#[given(regex = r#"^the server API has set key "(\w+)" of "([^"]+)" to (\{.*\})$"#)]
async fn the_server_api_has_set_a_key(
    world: &mut ClientWorld,
    key: String,
    channel: String,
    data: String,
) {
    map_publish(world, &channel, &key, serde_json::from_str(&data).unwrap()).await;
}

#[given(regex = r#"^a client with a map subscription to "([^"]+)" that has synced$"#)]
async fn a_client_with_a_synced_map(world: &mut ClientWorld, channel: String) {
    the_clients_synced_map(world, &channel, MapKind::Map).await;
}

#[then(
    regex = r#"^the map subscription emits an update with key "(\w+)", removed true and empty data$"#
)]
async fn a_removal(world: &mut ClientWorld, key: String) {
    let update = next_update(world.maps.events.as_mut().expect("a stream")).await;
    assert_eq!((update.key.as_str(), update.removed), (key.as_str(), true));
    assert!(update.data.is_empty(), "{update:?}");
}

#[given(
    regex = r#"^the server API has set keys "k1" to "k5" of "([^"]+)" to \{"v":1\} to \{"v":5\}$"#
)]
async fn the_server_api_has_set_five_keys(world: &mut ClientWorld, channel: String) {
    for n in 1..=5 {
        map_publish(world, &channel, &format!("k{n}"), json!({"v": n})).await;
    }
}

#[given(
    regex = r#"^a client with a map subscription of kind "(\w+)" to "([^"]+)" with page size (\d+), whose stream was taken$"#
)]
fn a_client_with_a_map_subscription_of_kind(
    world: &mut ClientWorld,
    of: String,
    channel: String,
    size: u32,
) {
    world.new_client(world.server_endpoint());
    let subscription = world
        .client()
        .map_subscription_builder(channel, kind(&of))
        .page_size(size)
        .build()
        .unwrap();
    world.track_map(subscription);
}

#[when("the client connects and subscribes the map subscription")]
fn the_client_connects_and_subscribes(world: &mut ClientWorld) {
    world.client().connect();
    world.map().subscribe();
}

#[when(
    regex = r#"^the server API sets key "(\w+)" of "([^"]+)" to (\{.*\}) once the subscription is subscribed$"#
)]
async fn sets_once_subscribed(world: &mut ClientWorld, key: String, channel: String, data: String) {
    tokio::time::timeout(EVENT_TIMEOUT, world.map().ready())
        .await
        .expect("subscribed in time")
        .expect("subscribed");
    map_publish(world, &channel, &key, serde_json::from_str(&data).unwrap()).await;
}

#[when(regex = r#"^the server API removes key "(\w+)" of "([^"]+)"$"#)]
async fn removes_a_key(world: &mut ClientWorld, key: String, channel: String) {
    map_remove(world, &channel, &key).await;
}

#[then(
    regex = r#"^the map subscription's stream yields "subscribing", "subscribed" with recoverable true and recovered false, a sync with keys "k1" to "k5" and their data$"#
)]
async fn yields_the_five_keys(world: &mut ClientWorld) {
    world.next_map_event_named("subscribing").await;
    let MapSubscriptionEvent::Subscribed(subscribed) =
        world.next_map_event_named("subscribed").await
    else {
        unreachable!();
    };
    assert!(
        subscribed.recoverable && !subscribed.recovered,
        "{subscribed:?}"
    );
    let MapSubscriptionEvent::Sync(entries) = world.next_map_event_named("sync").await else {
        unreachable!();
    };
    let got: Vec<(String, Value)> = entries
        .iter()
        .map(|entry| {
            (
                entry.key.clone(),
                serde_json::from_slice(&entry.data).unwrap(),
            )
        })
        .collect();
    let mut expected: Vec<(String, Value)> = (1..=5)
        .map(|n| (format!("k{n}"), json!({"v": n})))
        .collect();
    let mut got_sorted = got.clone();
    got_sorted.sort_by(|a, b| a.0.cmp(&b.0));
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got_sorted, expected);
}

#[then(
    regex = r#"^then an update with key "(\w+)", data (\{.*\}) and offset (\d+), then an update with key "(\w+)" and removed true$"#
)]
async fn then_two_updates(
    world: &mut ClientWorld,
    key: String,
    data: String,
    offset: u64,
    removed: String,
) {
    let first = world.next_map_event_named("update").await;
    let MapSubscriptionEvent::Update(first) = first else {
        unreachable!();
    };
    assert_eq!(first.key, key);
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&first.data).unwrap(), data);
    assert_eq!(first.offset, Some(offset));
    let MapSubscriptionEvent::Update(second) = world.next_map_event_named("update").await else {
        unreachable!();
    };
    assert_eq!(
        (second.key.as_str(), second.removed),
        (removed.as_str(), true)
    );
}

#[given(
    regex = r#"^a server with http_stream and sse enabled, and the server API has set keys "a", "b" and "c" of "([^"]+)"$"#
)]
async fn three_keys(world: &mut ClientWorld, channel: String) {
    for key in ["a", "b", "c"] {
        map_publish(world, &channel, key, json!({"key": key})).await;
    }
}

#[given(
    regex = r#"^a client with the (JSON|Protobuf) protocol connected over transport "(\w+)" with a map subscription to "([^"]+)" with page size (\d+)$"#
)]
async fn a_client_over_a_transport(
    world: &mut ClientWorld,
    protocol: String,
    transport: String,
    channel: String,
    size: u32,
) {
    world.protocol = if protocol == "Protobuf" {
        Protocol::Protobuf
    } else {
        Protocol::Json
    };
    super::transports::a_client_connected_over(world, String::new(), transport).await;
    let subscription = world
        .client()
        .map_subscription_builder(channel, MapKind::Map)
        .page_size(size)
        .build()
        .unwrap();
    world.track_map(subscription);
}

#[then(regex = r#"^the map subscription emits a sync with keys "a", "b" and "c"$"#)]
async fn emits_a_sync_with_three_keys(world: &mut ClientWorld) {
    let sync = loop {
        let event = world.next_map_event().await;
        if event.name() == "sync" {
            break event;
        }
        assert!(
            matches!(
                event,
                MapSubscriptionEvent::Subscribing { .. } | MapSubscriptionEvent::Subscribed(_)
            ),
            "{event:?}"
        );
    };
    assert_eq!(entry_keys(&sync), ["a", "b", "c"]);
}

#[then(
    regex = r#"^when the server API sets key "(\w+)" of "([^"]+)", the map subscription emits an update of "(\w+)"$"#
)]
async fn an_update_of(world: &mut ClientWorld, key: String, channel: String, of: String) {
    map_publish(world, &channel, &key, json!({"key": key})).await;
    let update = next_update(world.maps.events.as_mut().expect("a stream")).await;
    assert_eq!(update.key, of);
}

#[given(regex = r#"^two clients each with a map subscription to "([^"]+)" that has synced$"#)]
async fn two_clients_synced(world: &mut ClientWorld, channel: String) {
    the_clients_synced_map(world, &channel, MapKind::Map).await;
    let (client, subscription, events, _) = synced_client(world, &channel, MapKind::Map).await;
    world.maps.other = Some((client, subscription, events));
}

#[when(regex = r#"^the first client's map subscription publishes (\{.*\}) to key "(\w+)"$"#)]
async fn the_first_publishes(world: &mut ClientWorld, data: String, key: String) {
    let outcome = tokio::time::timeout(EVENT_TIMEOUT, world.map().publish(key, data.into_bytes()))
        .await
        .expect("the publish settled in time");
    world.maps.calls.push(tokio::spawn(async move { outcome }));
}

#[when(regex = r#"^then removes key "(\w+)"$"#)]
async fn then_removes(world: &mut ClientWorld, key: String) {
    let outcome = tokio::time::timeout(EVENT_TIMEOUT, world.map().remove(key))
        .await
        .expect("the remove settled in time");
    world.maps.calls.push(tokio::spawn(async move { outcome }));
}

#[then("both calls succeed")]
async fn both_calls_succeed(world: &mut ClientWorld) {
    for call in world.maps.calls.drain(..) {
        call.await
            .expect("the call's task")
            .expect("the call succeeded");
    }
}

#[then(
    regex = r#"^each map subscription emits an update with key "(\w+)" and data (\{.*\}), then an update with key "(\w+)" and removed true$"#
)]
async fn each_gets_both_updates(world: &mut ClientWorld, key: String, data: String, again: String) {
    let data: Value = serde_json::from_str(&data).unwrap();
    let (_, _, other) = world.maps.other.as_mut().expect("the other client");
    for events in [world.maps.events.as_mut().expect("a stream"), other] {
        let set = next_update(events).await;
        assert_eq!(set.key, key);
        assert_eq!(serde_json::from_slice::<Value>(&set.data).unwrap(), data);
        let removed = next_update(events).await;
        assert_eq!(
            (removed.key.as_str(), removed.removed),
            (again.as_str(), true)
        );
    }
}

#[given(
    regex = r#"^a client with a map subscription of kind "(\w+)" to "([^"]+)" that has synced with no entry$"#
)]
async fn a_presence_map(world: &mut ClientWorld, of: String, channel: String) {
    let (client, subscription, events, sync) = synced_client(world, &channel, kind(&of)).await;
    assert!(entry_keys(&sync).is_empty(), "{sync:?}");
    world.client = Some(client);
    world.maps.subscription = Some(subscription);
    world.maps.events = Some(events);
}

#[when(regex = r#"^a second client of user "(\w+)" subscribes to "([^"]+)"$"#)]
async fn a_second_client_subscribes(world: &mut ClientWorld, user: String, channel: String) {
    let client = Client::builder(world.server_endpoint())
        .config(super::test_config().token(connection_token(&user, Duration::from_secs(300))))
        .build()
        .unwrap();
    let mut events = client.events();
    client.connect();
    let id = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(ClientEvent::Connected { client_id, .. }) = events.next().await {
                return client_id;
            }
        }
    })
    .await
    .expect("connected in time");
    let subscription = client.new_subscription(channel).unwrap();
    subscription.subscribe();
    tokio::time::timeout(EVENT_TIMEOUT, subscription.ready())
        .await
        .expect("subscribed in time")
        .expect("subscribed");
    world.maps.second = Some((client, id, subscription));
}

#[then(
    regex = r#"^the map subscription emits an update whose key is the second client's id and whose info names user "(\w+)"$"#
)]
async fn the_clients_update(world: &mut ClientWorld, user: String) {
    let id = world
        .maps
        .second
        .as_ref()
        .expect("a second client")
        .1
        .clone();
    let update = next_update(world.maps.events.as_mut().expect("a stream")).await;
    assert_eq!(update.key, id);
    assert_eq!(update.info.expect("info").user, user);
    world.maps.key = Some(id);
}

#[then(
    regex = r#"^when the second client unsubscribes from "([^"]+)", the map subscription emits an update with that key and removed true$"#
)]
async fn the_client_leaves(world: &mut ClientWorld, channel: String) {
    let second = &world.maps.second.as_ref().expect("a second client").2;
    assert_eq!(second.channel(), channel);
    second.unsubscribe();
    let update = next_update(world.maps.events.as_mut().expect("a stream")).await;
    assert_eq!(Some(&update.key), world.maps.key.as_ref());
    assert!(update.removed);
}

#[then(
    regex = r#"^the map subscription emits an update with key "(\w+)", empty data and no info$"#
)]
async fn the_users_update(world: &mut ClientWorld, key: String) {
    let update = next_update(world.maps.events.as_mut().expect("a stream")).await;
    assert_eq!(update.key, key);
    assert!(update.data.is_empty(), "{update:?}");
    assert_eq!(update.info, None);
}

#[then(
    regex = r#"^when the second client disconnects, the map subscription emits an update with key "(\w+)" and removed true within (\d+) s$"#
)]
async fn the_user_leaves(world: &mut ClientWorld, key: String, within: u64) {
    world
        .maps
        .second
        .as_ref()
        .expect("a second client")
        .0
        .disconnect();
    let events = world.maps.events.as_mut().expect("a stream");
    let update = tokio::time::timeout(Duration::from_secs(within), next_update(events))
        .await
        .expect("the removal in time");
    assert_eq!((update.key.as_str(), update.removed), (key.as_str(), true));
}

#[when(regex = r#"^a (subscription|map subscription of kind "(\w+)") to "([^"]+)" is subscribed$"#)]
fn one_is_subscribed(world: &mut ClientWorld, what: String, of: String, channel: String) {
    if what == "subscription" {
        let subscription = world.client().new_subscription(channel).unwrap();
        let events = recorded(subscription.events());
        subscription.subscribe();
        world.delivery.subscription_events = Some(events);
        world.subscription = Some(subscription);
    } else {
        let subscription = world
            .client()
            .new_map_subscription(channel, kind(&of))
            .unwrap();
        world.track_map(subscription);
        world.map().subscribe();
    }
}

#[then(regex = r#"^it emits "unsubscribed" with code (\d+) and reason "([^"]+)"$"#)]
async fn it_emits_unsubscribed(world: &mut ClientWorld, code: u32, reason: String) {
    if world.maps.subscription.is_some() {
        let event = tokio::time::timeout(EVENT_TIMEOUT, async {
            loop {
                let event = world.next_map_event().await;
                if event.name() == "unsubscribed" {
                    return event;
                }
            }
        })
        .await
        .expect("unsubscribed in time");
        assert_eq!(event, MapSubscriptionEvent::Unsubscribed { code, reason });
    } else {
        let events = world
            .delivery
            .subscription_events
            .as_mut()
            .expect("the subscription's stream");
        let event = tokio::time::timeout(EVENT_TIMEOUT, async {
            loop {
                let event = events.next().await.expect("a stream");
                if event.name() == "unsubscribed" {
                    return event;
                }
            }
        })
        .await
        .expect("unsubscribed in time");
        assert_eq!(event, SubscriptionEvent::Unsubscribed { code, reason });
    }
}

#[given(
    regex = r#"^a client whose map subscription to "([^"]+)" has a token callback giving tokens that expire after (\d+) s$"#
)]
fn a_map_subscription_with_tokens(world: &mut ClientWorld, channel: String, lifetime: u64) {
    world.new_client(world.server_endpoint());
    let calls = Arc::clone(&world.maps.token_calls);
    let subscription = world
        .client()
        .map_subscription_builder(channel, MapKind::Map)
        .get_token(move |channel| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(subscription_token(
                    "",
                    &channel,
                    Duration::from_secs(lifetime),
                ))
            }
        })
        .build()
        .unwrap();
    world.track_map(subscription);
}

#[then("the map subscription emits a sync")]
async fn emits_a_sync(world: &mut ClientWorld) {
    loop {
        let event = world.next_map_event().await;
        match event {
            MapSubscriptionEvent::Sync(_) => break,
            MapSubscriptionEvent::Unsubscribed { .. } => panic!("{event:?}"),
            _ => {}
        }
    }
    world.maps.token_calls_at_sync = Some(world.maps.token_calls.load(Ordering::SeqCst));
}

#[then("the token callback was called once before the first subscribe command")]
fn called_once_before(world: &mut ClientWorld) {
    // The sync answers the first subscribe, which needs the token.
    assert_eq!(world.maps.token_calls_at_sync, Some(1));
}

#[then(
    regex = r#"^(\d+) s later the map subscription is still "subscribed" and the token callback was called twice$"#
)]
async fn still_subscribed_later(world: &mut ClientWorld, seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    assert_eq!(world.map().state(), SubscriptionState::Subscribed);
    let calls = world.maps.token_calls.load(Ordering::SeqCst);
    assert_eq!(calls, 2, "token callback calls {seconds} s after the sync");
    let events = world.maps.events.as_mut().expect("a stream");
    let later = quiet_events(events).await;
    assert!(
        later.iter().all(|event| !matches!(
            event,
            MapSubscriptionEvent::Subscribing { .. } | MapSubscriptionEvent::Unsubscribed { .. }
        )),
        "{later:?}"
    );
}

#[given(regex = r#"^a client with a map subscription to "([^"]+)" that has synced (\d+) keys$"#)]
async fn synced_with_keys(world: &mut ClientWorld, channel: String, count: u32) {
    for n in 1..=count {
        map_publish(world, &channel, &format!("k{n}"), json!({"n": n})).await;
    }
    a_connected_client(world).await;
    let subscription = world
        .client()
        .new_map_subscription(channel, MapKind::Map)
        .unwrap();
    world.track_map(subscription);
    world.map().subscribe();
    loop {
        if let MapSubscriptionEvent::Sync(entries) = world.next_map_event().await {
            assert_eq!(entries.len(), usize::try_from(count).unwrap());
            break;
        }
    }
}

#[when(
    regex = r#"^the server API sets (\d+) keys of "([^"]+)" and the application takes both updates$"#
)]
async fn sets_keys_and_takes(world: &mut ClientWorld, count: u32, channel: String) {
    for n in 1..=count {
        map_publish(world, &channel, &format!("new{n}"), json!({"n": n})).await;
    }
    for _ in 0..count {
        world.next_map_event_named("update").await;
    }
}

#[then(
    regex = r"^the client's statistics count (\d+) subscription and (\d+) publications delivered$"
)]
async fn statistics_count(world: &mut ClientWorld, subscriptions: usize, delivered: u64) {
    let client = world.client();
    wait_until("the updates counted", || {
        client.statistics().publications_delivered == delivered
    })
    .await;
    assert_eq!(client.statistics().subscriptions, subscriptions);
}
