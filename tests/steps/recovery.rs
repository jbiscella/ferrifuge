//! Steps of Block 11: stream positions and recovery.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{StreamPosition, Subscribed, SubscriptionEvent};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::subscriptions::{
    create, expect_subscription_event, has_json_data, next_publication, subscribe,
};
use super::transports::server;
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::relay::Mode;

/// Channel of the @A server's `rec` namespace: history and forced recovery.
const RECOVERABLE: &str = "rec:news";
/// The fake server's channel.
const CHANNEL: &str = "news";
/// Disconnect code making the client reconnect: a server API one for @A,
/// a close code in the protocol's reconnect range for @D (Block 3).
const RECONNECT_CODE: u32 = 4000;
const RECONNECT_CLOSE: u16 = 3001;
/// Stream position of the fake server's recoverable channel.
const FAKE_EPOCH: &str = "fake-epoch";
const FAKE_OFFSET: u64 = 3;
/// Epoch of a stream that started again.
const NEW_EPOCH: &str = "new-epoch";
/// How long a step waits for a publication that must not arrive.
const NO_PUBLICATION_PERIOD: Duration = Duration::from_millis(500);

/// What the recovery steps of a scenario keep.
#[derive(Debug, Default)]
pub(crate) struct Recovery {
    /// Publications made through the server API: data and position.
    published: Vec<(String, StreamPosition)>,
    /// Data of the publications the subscription must receive next.
    expected: Vec<String>,
    /// The stream position the client must recover from next.
    position: Option<StreamPosition>,
    /// Subscribe commands the fake server had received before the step
    /// making the client resubscribe.
    subscribes_before: usize,
}

fn position(offset: u64, epoch: &str) -> StreamPosition {
    StreamPosition {
        offset,
        epoch: epoch.to_owned(),
    }
}

/// Publishes `count` publications to the recoverable channel through the
/// server API; the subscription must receive them next.
async fn publish(world: &mut ClientWorld, count: usize) {
    for _ in 0..count {
        let data = json!({"n": world.recovery.published.len() + 1}).to_string();
        let result = server(world)
            .api()
            .call(
                "publish",
                json!({"channel": RECOVERABLE, "data": serde_json::from_str::<Value>(&data).unwrap()}),
            )
            .await
            .expect("publish");
        let at = position(
            result["offset"].as_u64().expect("an offset"),
            result["epoch"].as_str().expect("an epoch"),
        );
        world.recovery.published.push((data.clone(), at));
        world.recovery.expected.push(data);
    }
}

/// Disconnects the client with a reconnect code while the relay refuses
/// connections: the client stays away until [`let_the_client_back`].
async fn disconnect_and_hold(world: &mut ClientWorld) {
    world.relay.as_ref().expect("a relay").set_mode(Mode::Down);
    let client_id = world.client_id.clone().expect("a connected client");
    server(world)
        .api()
        .call(
            "disconnect",
            json!({
                "user": "",
                "client": client_id,
                "disconnect": {"code": RECONNECT_CODE, "reason": "reconnect advised by the test"},
            }),
        )
        .await
        .expect("server API disconnect");
    expect_subscription_event(world, "subscribing", false).await;
}

fn let_the_client_back(world: &ClientWorld) {
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
}

/// The subscription's next "subscribed" event; state events may come
/// first, a publication may not.
async fn next_subscribed(world: &mut ClientWorld) -> Subscribed {
    loop {
        match world.next_subscription_event(false).await {
            SubscriptionEvent::Subscribed(subscribed) => return subscribed,
            SubscriptionEvent::Publication(publication) => {
                panic!("a publication before \"subscribed\": {publication:?}")
            }
            _ => {}
        }
    }
}

/// The subscription receives the expected publications in order, and no
/// other.
async fn receive_exactly_the_expected(world: &mut ClientWorld) {
    let expected = std::mem::take(&mut world.recovery.expected);
    for data in &expected {
        let publication = next_publication(world, false).await;
        assert!(
            has_json_data(&publication, data),
            "expected {data}, got {publication:?}"
        );
    }
    let later = world
        .subscription_events_during(NO_PUBLICATION_PERIOD)
        .await;
    assert!(
        !later
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{later:?}"
    );
}

/// Subscribe commands the fake server has received.
fn subscribes(world: &ClientWorld) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter_map(|message| {
            let subscribe = &message["subscribe"];
            subscribe.is_object().then(|| subscribe.clone())
        })
        .collect()
}

/// The fake server's subscribe request number `index` (from 0), once
/// received.
async fn subscribe_request(world: &ClientWorld, index: usize) -> Value {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(subscribe) = subscribes(world).get(index) {
                return subscribe.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no subscribe command number {index} within {EVENT_TIMEOUT:?}"))
}

/// The client's next subscribe command asks to recover from the position
/// the scenario expects.
async fn assert_next_subscribe_recovers(world: &ClientWorld) {
    let expected = world.recovery.position.clone().expect("a position");
    let subscribe = subscribe_request(world, world.recovery.subscribes_before).await;
    assert_eq!(subscribe["channel"], CHANNEL, "{subscribe}");
    assert_eq!(subscribe["recover"], true, "{subscribe}");
    assert_eq!(
        (
            subscribe["epoch"].as_str().unwrap_or_default(),
            subscribe["offset"].as_u64().unwrap_or_default(),
        ),
        (expected.epoch.as_str(), expected.offset),
        "{subscribe}"
    );
}

/// A subscribe result granting recovery, with a stream position.
fn recoverable_result(at: &StreamPosition) -> Value {
    json!({"recoverable": true, "positioned": true, "epoch": at.epoch, "offset": at.offset})
}

/// Subscribes to the fake server's channel, which its reply makes
/// recoverable at a stream position.
async fn subscribed_to_the_recoverable_fake_channel(world: &mut ClientWorld) {
    let at = position(FAKE_OFFSET, FAKE_EPOCH);
    world
        .fake()
        .answer_a_subscribe_with(recoverable_result(&at));
    world.recovery.position = Some(at);
    create(world, CHANNEL);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

/// Makes the fake server close the connection with a reconnect code,
/// noting how many subscribe commands came before.
fn fake_server_disconnects(world: &mut ClientWorld) {
    world.recovery.subscribes_before = subscribes(world).len();
    world
        .fake()
        .close(RECONNECT_CLOSE, "reconnect advised by the test");
}

#[when("a subscription to a channel with history and recovery is subscribed")]
async fn subscribed_to_a_recoverable_channel(world: &mut ClientWorld) {
    create(world, RECOVERABLE);
    subscribe(world).await;
}

#[then(
    regex = r#"^(?:then )?the subscription emits "subscribed" with was_recovering (true|false) and recovered (true|false)$"#
)]
async fn subscribed_with_recovery(
    world: &mut ClientWorld,
    was_recovering: String,
    recovered: String,
) {
    let subscribed = next_subscribed(world).await;
    assert_eq!(
        (subscribed.was_recovering, subscribed.recovered),
        (was_recovering == "true", recovered == "true"),
        "{subscribed:?}"
    );
}

#[given("a client subscribed to a channel with history and recovery")]
async fn a_client_subscribed_to_a_recoverable_channel(world: &mut ClientWorld) {
    a_connected_client(world).await;
    create(world, RECOVERABLE);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given("the subscription has received 1 live publication")]
async fn received_a_live_publication(world: &mut ClientWorld) {
    publish(world, 1).await;
    receive_exactly_the_expected(world).await;
}

#[given("the client is disconnected with a reconnect-advised code")]
async fn disconnected_with_a_reconnect_code(world: &mut ClientWorld) {
    disconnect_and_hold(world).await;
}

#[when("2 publications are made to the channel while the client reconnects")]
async fn publications_while_reconnecting(world: &mut ClientWorld) {
    publish(world, 2).await;
    let_the_client_back(world);
}

#[when("2 publications are made to the channel")]
async fn two_publications(world: &mut ClientWorld) {
    publish(world, 2).await;
}

#[then(
    regex = r"^the subscription receives exactly (?:those 2|the 2 later) publications in order$"
)]
async fn receives_exactly_those(world: &mut ClientWorld) {
    receive_exactly_the_expected(world).await;
}

#[when("subscribe is called on the subscription again")]
fn subscribe_again(world: &mut ClientWorld) {
    world.subscription().subscribe();
}

#[given("a channel with history and recovery holding 3 publications")]
async fn a_channel_holding_publications(world: &mut ClientWorld) {
    publish(world, 3).await;
}

#[given("a subscription to the channel with the since option at the position of the first one")]
fn a_subscription_since_the_first(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    let since = world.recovery.published[0].1.clone();
    // The first one is not recovered: the later ones are.
    world.recovery.expected.remove(0);
    let subscription = world
        .client()
        .subscription_builder(RECOVERABLE)
        .since(since)
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
}

#[given("the channel history expires while the client is disconnected")]
async fn history_expires_while_disconnected(world: &mut ClientWorld) {
    disconnect_and_hold(world).await;
    // A publication the client misses, then gone from the history.
    publish(world, 1).await;
    world.recovery.expected.clear();
    server(world)
        .api()
        .call("history_remove", json!({"channel": RECOVERABLE}))
        .await
        .expect("history_remove");
}

#[when("the client resubscribes")]
fn the_client_resubscribes(world: &mut ClientWorld) {
    let_the_client_back(world);
}

#[when(
    regex = r#"^a subscription to channel "news" with the positioned and recoverable options is subscribed(?: and the fake server grants (both|positioned only|recoverable only|neither))?$"#
)]
async fn subscribed_with_the_options(world: &mut ClientWorld, granted: String) {
    if !granted.is_empty() {
        let (positioned, recoverable) = match granted.as_str() {
            "both" => (true, true),
            "positioned only" => (true, false),
            "recoverable only" => (false, true),
            _ => (false, false),
        };
        // As Centrifugo: a position only with a capability granted.
        let result = if positioned || recoverable {
            let at = position(FAKE_OFFSET, FAKE_EPOCH);
            world.recovery.position = Some(at.clone());
            json!({"positioned": positioned, "recoverable": recoverable, "epoch": at.epoch, "offset": at.offset})
        } else {
            json!({})
        };
        world.fake().answer_a_subscribe_with(result);
    }
    let subscription = world
        .client()
        .subscription_builder(CHANNEL)
        .positioned(true)
        .recoverable(true)
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
}

#[then(r#"the fake server received a subscribe command for "news" requesting positioned and recoverable"#)]
async fn requested_positioned_and_recoverable(world: &mut ClientWorld) {
    let subscribe = subscribe_request(world, 0).await;
    assert_eq!(
        (
            &subscribe["channel"],
            &subscribe["positioned"],
            &subscribe["recoverable"]
        ),
        (&json!(CHANNEL), &json!(true), &json!(true)),
        "{subscribe}"
    );
}

#[then(
    regex = r#"^the subscription emits "subscribed" reporting positioned (true|false), recoverable (true|false) and the fake server's stream position$"#
)]
async fn subscribed_reporting_the_capabilities(
    world: &mut ClientWorld,
    positioned: String,
    recoverable: String,
) {
    let subscribed = next_subscribed(world).await;
    assert_eq!(
        (
            subscribed.positioned,
            subscribed.recoverable,
            subscribed.stream_position.clone()
        ),
        (
            positioned == "true",
            recoverable == "true",
            world.recovery.position.clone()
        ),
        "{subscribed:?}"
    );
}

#[given(r#"a subscription to recoverable channel "news" whose subscribe reply carries a stream position"#)]
async fn a_recoverable_subscription_with_a_position(world: &mut ClientWorld) {
    subscribed_to_the_recoverable_fake_channel(world).await;
}

#[given(r#"a client subscribed to recoverable channel "news" on the fake server"#)]
async fn a_client_subscribed_to_the_recoverable_channel(world: &mut ClientWorld) {
    a_connected_client(world).await;
    subscribed_to_the_recoverable_fake_channel(world).await;
}

#[when(
    regex = r"^the fake server disconnects the client with a reconnect-advised code(?: before any publication| again)?$"
)]
fn the_fake_server_disconnects(world: &mut ClientWorld) {
    fake_server_disconnects(world);
}

#[then(
    regex = r#"^the next subscribe command for "news" (?:asks to recover from (?:that|the new) stream position|carries the new epoch and that publication's offset|carries the offset of the second recovered publication)$"#
)]
async fn the_next_subscribe_recovers(world: &mut ClientWorld) {
    assert_next_subscribe_recovers(world).await;
}

#[when("the fake server pushes a publication with a new epoch")]
async fn a_publication_with_a_new_epoch(world: &mut ClientWorld) {
    world.recovery.position = Some(position(1, NEW_EPOCH));
    let push = json!({"push": {"channel": CHANNEL, "pub": {"data": {"n": 1}, "offset": 1, "epoch": NEW_EPOCH}}});
    world.fake().send(&push.to_string());
    // Applications see it too, to save positions they can resume from.
    let publication = next_publication(world, false).await;
    assert_eq!(
        publication.epoch.as_deref(),
        Some(NEW_EPOCH),
        "{publication:?}"
    );
}

#[given("the client recovered 2 publications after a reconnect")]
async fn recovered_two_publications(world: &mut ClientWorld) {
    let mut result = recoverable_result(&position(FAKE_OFFSET, FAKE_EPOCH));
    result["was_recovering"] = json!(true);
    result["recovered"] = json!(true);
    result["publications"] = json!([
        {"data": {"n": 4}, "offset": FAKE_OFFSET + 1},
        {"data": {"n": 5}, "offset": FAKE_OFFSET + 2},
    ]);
    world.fake().answer_a_subscribe_with(result);
    world.recovery.position = Some(position(FAKE_OFFSET + 2, FAKE_EPOCH));
    world.recovery.expected = vec![r#"{"n":4}"#.to_owned(), r#"{"n":5}"#.to_owned()];
    fake_server_disconnects(world);
    let subscribed = next_subscribed(world).await;
    assert!(subscribed.recovered, "{subscribed:?}");
    receive_exactly_the_expected(world).await;
}

#[given(
    "a resubscribe after a reconnect that the fake server answered with recovered false and a new stream position"
)]
async fn a_failed_recovery(world: &mut ClientWorld) {
    let at = position(9, NEW_EPOCH);
    let mut result = recoverable_result(&at);
    result["was_recovering"] = json!(true);
    world.fake().answer_a_subscribe_with(result);
    world.recovery.position = Some(at);
    fake_server_disconnects(world);
    let subscribed = next_subscribed(world).await;
    assert!(
        subscribed.was_recovering && !subscribed.recovered,
        "{subscribed:?}"
    );
}
