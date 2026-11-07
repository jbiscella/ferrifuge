//! Steps of Block 9: client-side subscriptions.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, Input, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer, TransportId,
};
use ferrifuge::{
    ClientEvent, DataError, Protocol, Publication, StateError, StreamPosition, SubscriptionEvent,
    SubscriptionState, TokenError,
};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::transports::{configure, connect_and_wait, server};
use super::{ClientWorld, EVENT_TIMEOUT, test_config};
use crate::support::http_proxy::HttpProxy;

const CHANNEL: &str = "news";
/// How long a step waits for a publication that must not arrive.
const NO_PUBLICATION_PERIOD: Duration = Duration::from_millis(500);
/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// Data of the publication a frame carries after a disconnect push.
const STALE: &str = r#"{"stale":true}"#;

fn json_bytes(text: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::from_str::<Value>(text).expect("JSON in the step"))
        .expect("serializable")
}

fn hex_bytes(text: &str) -> Vec<u8> {
    text.split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("hex byte"))
        .collect()
}

/// Whether `publication` carries `expected` JSON, whatever its layout.
pub(super) fn has_json_data(publication: &Publication, expected: &str) -> bool {
    serde_json::from_slice::<Value>(&publication.data).ok()
        == serde_json::from_str::<Value>(expected).ok()
}

pub(super) fn create(world: &mut ClientWorld, channel: &str) {
    let subscription = world
        .client()
        .new_subscription(channel)
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
}

/// Subscribes the scenario's subscription and consumes the "subscribing"
/// event `subscribe()` emits at once, so later steps see the server's answer.
pub(super) async fn subscribe(world: &mut ClientWorld) {
    world.subscription().subscribe();
    expect_subscription_event(world, "subscribing", false).await;
}

pub(super) async fn expect_subscription_event(
    world: &mut ClientWorld,
    expected: &str,
    new: bool,
) -> SubscriptionEvent {
    let event = world.next_subscription_event(new).await;
    assert_eq!(event.name(), expected, "{event:?}");
    event
}

/// Waits until the subscription (or the new one) is subscribed.
async fn reach_subscribed(world: &mut ClientWorld, new: bool) {
    let subscription = if new {
        world.new_subscription.clone()
    } else {
        world.subscription.clone()
    }
    .expect("a subscription");
    let reached = tokio::time::timeout(EVENT_TIMEOUT, async {
        while subscription.state() != SubscriptionState::Subscribed {
            world.next_subscription_event(new).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "not subscribed within {EVENT_TIMEOUT:?}: {:?}",
        subscription.state()
    );
}

pub(super) async fn next_publication(world: &mut ClientWorld, new: bool) -> Publication {
    loop {
        if let SubscriptionEvent::Publication(publication) =
            world.next_subscription_event(new).await
        {
            return publication;
        }
    }
}

/// Publishes `data` (JSON) to `channel` through the server API; the
/// offset the server reports is kept.
async fn publish(world: &mut ClientWorld, channel: &str, data: &str, tags: Option<Value>) {
    let mut params = json!({
        "channel": channel,
        "data": serde_json::from_str::<Value>(data).expect("JSON data"),
    });
    if let Some(tags) = tags {
        params["tags"] = tags;
    }
    let result = server(world)
        .api()
        .call("publish", params)
        .await
        .expect("publish");
    world.published_offset = result["offset"].as_u64();
}

async fn a_client_subscribed(world: &mut ClientWorld) {
    let connected = world
        .client
        .as_ref()
        .is_some_and(|client| client.state() == ferrifuge::State::Connected);
    if !connected {
        a_connected_client(world).await;
    }
    create(world, CHANNEL);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[when(regex = r#"^a subscription to channel "(\w+)" is created$"#)]
fn a_subscription_is_created(world: &mut ClientWorld, channel: String) {
    create(world, &channel);
}

#[when(regex = r#"^a subscription to channel "(\w+)" is created and subscribed$"#)]
fn a_subscription_is_created_and_subscribed(world: &mut ClientWorld, channel: String) {
    create(world, &channel);
    world.subscription().subscribe();
}

#[when(regex = r#"^a subscription to channel "(\w+)" is subscribed$"#)]
async fn a_subscription_is_subscribed(world: &mut ClientWorld, channel: String) {
    create(world, &channel);
    subscribe(world).await;
}

#[when(regex = r#"^a subscription to channel "(\w+)" is subscribed on a connected client$"#)]
async fn a_subscription_on_a_connected_client(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    create(world, &channel);
    subscribe(world).await;
}

#[then(regex = r#"^the subscription state is "(\w+)"$"#)]
fn the_subscription_state_is(world: &mut ClientWorld, expected: String) {
    assert_eq!(world.subscription().state().as_str(), expected);
}

#[then(regex = r#"^(?:then )?the subscription emits "(\w+)"$"#)]
async fn the_subscription_emits(world: &mut ClientWorld, expected: String) {
    if let Some(run) = &world.subscription_machine {
        assert!(
            run.events().iter().any(|event| event.name() == expected),
            "{:?}",
            run.actions
        );
        return;
    }
    expect_subscription_event(world, &expected, false).await;
}

#[then(regex = r#"^the subscription reaches state "subscribed"(?: on it)?$"#)]
async fn the_subscription_reaches_subscribed(world: &mut ClientWorld) {
    if super::batching::drives_a_machine(world) {
        super::batching::subscription_subscribed(world);
        return;
    }
    reach_subscribed(world, false).await;
}

/// After the server disconnected the client: the subscription goes back to
/// subscribing first, which the step waits for, so it cannot pass on the
/// state from before the disconnect.
#[when(r#"the subscription reaches state "subscribed" again"#)]
async fn the_subscription_is_subscribed_again(world: &mut ClientWorld) {
    expect_subscription_event(world, "subscribing", false).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given(regex = r#"^a client subscribed to channel "news"(?: with history)?$"#)]
async fn a_client_subscribed_to_news(world: &mut ClientWorld) {
    a_client_subscribed(world).await;
}

#[given(
    regex = r#"^a client (configured with the Protobuf protocol )?connected over transport "(\w+)" and subscribed to channel "news"$"#
)]
async fn a_client_over_a_transport_subscribed(
    world: &mut ClientWorld,
    protobuf: String,
    transport: String,
) {
    if !protobuf.is_empty() {
        world.protocol = Protocol::Protobuf;
    }
    let kind = super::transports::kind(&transport);
    let url = super::transports::server_url(server(world), kind);
    configure(world, vec![(kind, url)]);
    connect_and_wait(world).await;
    a_client_subscribed(world).await;
}

#[given(r#"a client configured with the Protobuf protocol subscribed to channel "news""#)]
async fn a_protobuf_client_subscribed(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    a_client_subscribed(world).await;
}

#[when(regex = r#"^the server API publishes (\{.*\}) to channel "(\w+)"$"#)]
async fn the_server_api_publishes(world: &mut ClientWorld, data: String, channel: String) {
    publish(world, &channel, &data, None).await;
}

#[when(regex = r#"^the server API publishes (\{.*\}) to channel "(\w+)" with tags (\{.*\})$"#)]
async fn the_server_api_publishes_with_tags(
    world: &mut ClientWorld,
    data: String,
    channel: String,
    tags: String,
) {
    let tags = serde_json::from_str(&tags).expect("JSON tags");
    publish(world, &channel, &data, Some(tags)).await;
}

#[when(regex = r#"^the server API publishes the bytes ([0-9a-f ]+) to channel "(\w+)"$"#)]
async fn the_server_api_publishes_bytes(world: &mut ClientWorld, bytes: String, channel: String) {
    let encoded = base64::engine::general_purpose::STANDARD.encode(hex_bytes(&bytes));
    server(world)
        .api()
        .call("publish", json!({"channel": channel, "b64data": encoded}))
        .await
        .expect("publish");
}

#[then(regex = r"^the subscription receives a publication with data (\{.*\})$")]
async fn receives_a_publication_with_data(world: &mut ClientWorld, data: String) {
    let publication = next_publication(world, false).await;
    assert!(has_json_data(&publication, &data), "{publication:?}");
}

#[then(regex = r"^the subscription receives a publication with tags (\{.*\})$")]
async fn receives_a_publication_with_tags(world: &mut ClientWorld, tags: String) {
    let publication = next_publication(world, false).await;
    let expected: Value = serde_json::from_str(&tags).expect("JSON tags");
    assert_eq!(
        serde_json::to_value(&publication.tags).expect("tags"),
        expected,
        "{publication:?}"
    );
    world.last_publication = Some(publication);
}

#[then("the publication carries the channel's next stream offset")]
fn the_publication_carries_the_offset(world: &mut ClientWorld) {
    let publication = world.last_publication.as_ref().expect("a publication");
    assert!(
        world.published_offset.is_some(),
        "the server reported no offset"
    );
    assert_eq!(
        publication.offset, world.published_offset,
        "{publication:?}"
    );
}

#[then(regex = r"^the subscription receives a publication with the bytes ([0-9a-f ]+)$")]
async fn receives_a_publication_with_bytes(world: &mut ClientWorld, bytes: String) {
    let publication = next_publication(world, false).await;
    assert_eq!(publication.data, hex_bytes(&bytes));
}

#[when("unsubscribe is called on the subscription")]
fn unsubscribe_is_called(world: &mut ClientWorld) {
    world.unsubscribed_at = Some(Instant::now());
    world.subscription().unsubscribe();
}

#[when("unsubscribe is called on the subscription before the subscribe is answered")]
fn unsubscribe_before_the_answer(world: &mut ClientWorld) {
    assert_eq!(
        world.subscription().state(),
        SubscriptionState::Subscribing,
        "the subscribe was answered already"
    );
    world.subscription().unsubscribe();
}

#[when("subscribe is called on the subscription again at once")]
fn subscribe_again_at_once(world: &mut ClientWorld) {
    world.subscription().subscribe();
}

#[then(regex = r#"^the subscription emits "unsubscribed" with code (\d+)$"#)]
async fn the_subscription_emits_unsubscribed_with_code(world: &mut ClientWorld, code: u32) {
    if let Some(run) = &world.subscription_machine {
        assert!(
            run.events().iter().any(|event| matches!(
                event,
                SubscriptionEvent::Unsubscribed { code: got, .. } if *got == code
            )),
            "{:?}",
            run.actions
        );
        return;
    }
    let event = expect_subscription_event(world, "unsubscribed", false).await;
    assert!(
        matches!(&event, SubscriptionEvent::Unsubscribed { code: got, .. } if *got == code),
        "{event:?}"
    );
}

#[then(
    regex = r#"^(?:then )?the subscription emits "unsubscribed" with code (\d+) and reason "(.*)"$"#
)]
async fn unsubscribed_with_code_and_reason(world: &mut ClientWorld, code: u32, reason: String) {
    if let Some(run) = &world.subscription_machine {
        let expected = SubscriptionEvent::Unsubscribed { code, reason };
        assert!(run.events().contains(&&expected), "{:?}", run.actions);
        return;
    }
    let event = expect_subscription_event(world, "unsubscribed", false).await;
    assert_eq!(
        event,
        SubscriptionEvent::Unsubscribed { code, reason },
        "{event:?}"
    );
}

/// Waits until the server has no subscriber to `channel` left (server API
/// `channels`): the client's unsubscribe reached it. The subscription drops
/// publications once unsubscribed, so its events alone cannot show that.
async fn assert_no_server_subscriber(world: &ClientWorld, channel: &str) {
    let gone = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let result = server(world)
                .api()
                .call("channels", json!({"pattern": channel}))
                .await
                .expect("server API channels");
            if result["channels"].get(channel).is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        gone.is_ok(),
        "the server still has a subscriber to {channel}"
    );
}

/// Publishes `{"late":true}` and checks the subscription receives nothing.
async fn assert_no_publication(world: &mut ClientWorld) {
    publish(world, CHANNEL, r#"{"late":true}"#, None).await;
    let events = world
        .subscription_events_during(NO_PUBLICATION_PERIOD)
        .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{events:?}"
    );
}

#[then(regex = r#"^a publication to channel "news" is not received$"#)]
async fn a_publication_is_not_received(world: &mut ClientWorld) {
    assert_no_server_subscriber(world, CHANNEL).await;
    assert_no_publication(world).await;
}

#[then(
    regex = r#"^a publication to channel "news" after the subscribe was answered is not received$"#
)]
async fn a_late_publication_is_not_received(world: &mut ClientWorld) {
    // The proxy answers the subscribe after 1 s; the deferred unsubscribe
    // follows its reply.
    tokio::time::sleep(Duration::from_secs(1) + TOLERANCE).await;
    // The client undoes only a subscription the server made: an unsubscribe
    // relayed shows the subscribe was answered with success, and the race
    // happened.
    let relayed = world
        .http_proxy
        .as_ref()
        .expect("the slow subscribe proxy step sets an HTTP proxy")
        .unsubscribes_relayed();
    assert_eq!(relayed, 1, "unsubscribe commands the server accepted");
    assert_no_server_subscriber(world, CHANNEL).await;
    assert_no_publication(world).await;
}

#[then(regex = r#"^a publication to channel "news" is received$"#)]
async fn a_publication_is_received(world: &mut ClientWorld) {
    publish(world, CHANNEL, r#"{"after":true}"#, None).await;
    let publication = next_publication(world, false).await;
    assert!(
        has_json_data(&publication, r#"{"after":true}"#),
        "{publication:?}"
    );
}

#[then(regex = r#"^a publication to channel "news" made after both requests settled is received$"#)]
async fn a_publication_after_settling_is_received(world: &mut ClientWorld) {
    a_publication_is_received(world).await;
}

#[given(regex = r#"^a new client with a subscription to channel "(\w+)"$"#)]
fn a_new_client_with_a_subscription(world: &mut ClientWorld, channel: String) {
    world.new_client(super::UNREACHABLE_ENDPOINT.to_owned());
    create(world, &channel);
}

#[when(regex = r#"^a second subscription to channel "(\w+)" is created$"#)]
fn a_second_subscription_is_created(world: &mut ClientWorld, channel: String) {
    world.duplicate = world.client().new_subscription(channel).err();
}

#[then("creating it fails")]
fn creating_it_fails(world: &mut ClientWorld) {
    let duplicate = world.duplicate.as_ref().expect("creating it succeeded");
    assert_eq!(
        duplicate,
        &ferrifuge::SubscriptionError::AlreadyExists {
            channel: CHANNEL.to_owned()
        }
    );
}

#[given(
    regex = r#"^the subscribe proxy answers channel "(\w+)" with error code (\d+) and message "(.*)"$"#
)]
fn the_subscribe_proxy_answers_with_an_error(
    world: &mut ClientWorld,
    channel: String,
    code: u32,
    message: String,
) {
    proxy(world).subscribe_error(&channel, code, &message);
}

#[given(regex = r#"^the subscribe proxy answers channel "(\w+)" with data (\{.*\})$"#)]
fn the_subscribe_proxy_answers_with_data(world: &mut ClientWorld, channel: String, data: String) {
    let data: Value = serde_json::from_str(&data).expect("JSON data");
    proxy(world).subscribe_data(&channel, &data);
}

/// The client then connects through an HTTP proxy, which records the
/// unsubscribe commands the server accepts.
#[given("a server with http_stream enabled and a subscribe proxy that answers after 1 s")]
async fn a_slow_subscribe_proxy(world: &mut ClientWorld) {
    proxy(world).subscribe_delay(CHANNEL, Duration::from_secs(1));
    world.http_proxy = Some(HttpProxy::start(server(world).address().to_owned()).await);
}

/// The proxy records every subscribe request.
#[given("a subscribe proxy recording requests")]
fn a_recording_subscribe_proxy(world: &mut ClientWorld) {
    proxy(world);
}

fn proxy(world: &ClientWorld) -> &crate::support::proxy::ProxyBackend {
    world
        .proxy
        .as_ref()
        .expect("this step needs the proxy backend: tag the scenario @B")
}

#[then(regex = r#"^the subscription emits "subscribed" with data (\{.*\})$"#)]
async fn subscribed_with_data(world: &mut ClientWorld, data: String) {
    let event = expect_subscription_event(world, "subscribed", false).await;
    let SubscriptionEvent::Subscribed(subscribed) = &event else {
        unreachable!("checked the name");
    };
    let got = subscribed
        .data
        .as_deref()
        .map(serde_json::from_slice::<Value>);
    assert_eq!(
        got.and_then(Result::ok),
        serde_json::from_str::<Value>(&data).ok(),
        "{event:?}"
    );
}

#[given(regex = r#"^a client subscribed to channel "news" with subscription data (\{.*\})$"#)]
async fn a_client_subscribed_with_data(world: &mut ClientWorld, data: String) {
    a_connected_client(world).await;
    let subscription = world
        .client()
        .subscription_builder(CHANNEL)
        .data(json_bytes(&data))
        .build()
        .expect("no subscription yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given(
    regex = r#"^a client subscribed to channel "news" with a data callback returning (\{.*\}) then (\{.*\})$"#
)]
async fn a_client_subscribed_with_a_data_callback(
    world: &mut ClientWorld,
    first: String,
    then: String,
) {
    a_connected_client(world).await;
    let answers = Arc::new(Mutex::new(VecDeque::from([
        json_bytes(&first),
        json_bytes(&then),
    ])));
    let subscription = world
        .client()
        .subscription_builder(CHANNEL)
        .get_data(move |_| {
            let data = answers.lock().unwrap().pop_front().expect("two calls only");
            async move { Ok(data) }
        })
        .build()
        .expect("no subscription yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

fn recorded_data(world: &ClientWorld) -> Vec<Value> {
    proxy(world)
        .subscribe_requests()
        .into_iter()
        .filter(|request| request["channel"] == CHANNEL)
        .map(|request| request["data"].clone())
        .collect()
}

#[then(regex = r"^the subscribe proxy recorded data (\{.*\}) on both subscribe requests$")]
fn the_proxy_recorded_data_twice(world: &mut ClientWorld, data: String) {
    let expected: Value = serde_json::from_str(&data).expect("JSON data");
    assert_eq!(recorded_data(world), [expected.clone(), expected]);
}

#[then(regex = r"^the subscribe proxy recorded data (\{.*\}) then (\{.*\})$")]
fn the_proxy_recorded_data_in_order(world: &mut ClientWorld, first: String, then: String) {
    let first: Value = serde_json::from_str(&first).expect("JSON data");
    let then: Value = serde_json::from_str(&then).expect("JSON data");
    assert_eq!(recorded_data(world), [first, then]);
}

#[given(
    regex = r#"^a connected client with subscriptions to channels "(\w+)" and "(\w+)" using one data callback$"#
)]
async fn subscriptions_with_one_data_callback(world: &mut ClientWorld, a: String, b: String) {
    a_connected_client(world).await;
    let calls = Arc::clone(&world.data_calls);
    let callback = move |channel: String| {
        calls.lock().unwrap().push(channel);
        async { Ok(b"{}".to_vec()) }
    };
    for channel in [a, b] {
        let subscription = world
            .client()
            .subscription_builder(channel)
            .get_data(callback.clone())
            .build()
            .expect("no subscription yet");
        world.named_subscriptions.push(subscription);
    }
}

#[when("both subscriptions are subscribed")]
async fn both_subscriptions_are_subscribed(world: &mut ClientWorld) {
    for subscription in &world.named_subscriptions {
        subscription.subscribe();
    }
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world
            .named_subscriptions
            .iter()
            .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(subscribed.is_ok(), "{:?}", world.named_subscriptions);
}

#[then(
    regex = r#"^the data callback was called once for channel "(\w+)" and once for channel "(\w+)"$"#
)]
fn the_data_callback_was_called_per_channel(world: &mut ClientWorld, a: String, b: String) {
    let mut calls = world.data_calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(calls, [a, b]);
}

#[given(regex = r#"^a new client with subscriptions to channels "(\w+)" and "(\w+)"$"#)]
fn a_new_client_with_two_subscriptions(world: &mut ClientWorld, a: String, b: String) {
    world.new_client(super::UNREACHABLE_ENDPOINT.to_owned());
    create(world, &a);
    let b = world
        .client()
        .new_subscription(b)
        .expect("no subscription yet");
    world.named_subscriptions.push(b);
}

#[then(regex = r#"^looking up channel "(\w+)" returns its subscription$"#)]
fn looking_up_returns_it(world: &mut ClientWorld, channel: String) {
    assert_eq!(
        world.client().get_subscription(&channel).as_ref(),
        Some(world.subscription())
    );
}

#[then(regex = r#"^looking up channel "(\w+)" returns none$"#)]
fn looking_up_returns_none(world: &mut ClientWorld, channel: String) {
    assert_eq!(world.client().get_subscription(&channel), None);
}

#[then(regex = r#"^the client lists the subscriptions to "(\w+)" and "(\w+)"$"#)]
fn the_client_lists_them(world: &mut ClientWorld, a: String, b: String) {
    let mut channels: Vec<_> = world
        .client()
        .subscriptions()
        .iter()
        .map(|subscription| subscription.channel().to_owned())
        .collect();
    channels.sort();
    assert_eq!(channels, [a, b]);
}

#[when("the subscription is removed from the client")]
fn the_subscription_is_removed(world: &mut ClientWorld) {
    world.unsubscribed_at = Some(Instant::now());
    let subscription = world.subscription().clone();
    world.client().remove_subscription(&subscription);
}

#[when(regex = r#"^a new subscription to channel "(\w+)" is created and subscribed at once$"#)]
fn a_new_subscription_at_once(world: &mut ClientWorld, channel: String) {
    let subscription = world
        .client()
        .new_subscription(channel)
        .expect("the removed subscription left the registry");
    world.track_new_subscription(subscription);
    world
        .new_subscription
        .as_ref()
        .expect("tracked")
        .subscribe();
}

/// Also checks the order: the delayed unsubscribe settled first, so the
/// subscription is subscribed no sooner than its delay.
async fn subscribed_after_the_delayed_unsubscribe(world: &mut ClientWorld, new: bool) {
    reach_subscribed(world, new).await;
    let elapsed = world
        .unsubscribed_at
        .expect("an unsubscribe was requested")
        .elapsed();
    assert!(
        elapsed >= DELAYED_UNSUBSCRIBE,
        "subscribed {elapsed:?} after the unsubscribe, before it settled"
    );
}

/// Delay the HTTP proxy adds to the next unsubscribe emulation request.
const DELAYED_UNSUBSCRIBE: Duration = Duration::from_secs(1);

#[then(
    regex = r#"^the new subscription reaches state "subscribed" after the delayed unsubscribe is settled$"#
)]
async fn the_new_subscription_is_subscribed_after(world: &mut ClientWorld) {
    subscribed_after_the_delayed_unsubscribe(world, true).await;
}

#[then(
    regex = r#"^the subscription reaches state "subscribed" after the delayed unsubscribe is settled$"#
)]
async fn the_subscription_is_subscribed_after(world: &mut ClientWorld) {
    subscribed_after_the_delayed_unsubscribe(world, false).await;
}

#[then(regex = r#"^a publication to channel "news" is received by the new subscription$"#)]
async fn the_new_subscription_receives_a_publication(world: &mut ClientWorld) {
    publish(world, CHANNEL, r#"{"after":true}"#, None).await;
    let publication = next_publication(world, true).await;
    assert!(
        has_json_data(&publication, r#"{"after":true}"#),
        "{publication:?}"
    );
}

#[given(
    regex = r#"^a client connected over transport "http_stream" through a proxy that delays the next unsubscribe emulation request by 1 s$"#
)]
async fn a_client_behind_a_delaying_proxy(world: &mut ClientWorld) {
    let http_proxy = HttpProxy::start(server(world).address().to_owned()).await;
    http_proxy.delay_next_unsubscribe(DELAYED_UNSUBSCRIBE);
    let url = format!("http://{}/connection/http_stream", http_proxy.address());
    configure(
        world,
        vec![(ferrifuge::machine::TransportKind::HttpStream, url)],
    );
    connect_and_wait(world).await;
    world.http_proxy = Some(http_proxy);
}

#[given(regex = r#"^a client subscribed to channel "news" on the fake server$"#)]
async fn a_client_subscribed_on_the_fake_server(world: &mut ClientWorld) {
    a_client_subscribed(world).await;
}

/// From code 2500 the subscription subscribes again: the fake server leaves
/// that subscribe unanswered, so the subscribing state can be observed; a
/// later step answers it.
#[when(regex = r"^the fake server unsubscribes the subscription with code (\d+)$")]
fn the_fake_server_unsubscribes(world: &mut ClientWorld, code: u32) {
    if code >= 2500 {
        world.fake().ignore_next_subscribe();
    }
    let reason = format!("fake server unsubscribe {code}");
    let push =
        json!({"push": {"channel": CHANNEL, "unsubscribe": {"code": code, "reason": reason}}});
    world.fake().send(&push.to_string());
    world.sent_unsubscribe = Some((code, reason));
}

#[then(regex = r#"^the subscription emits "(\w+)" with code (\d+) and the reason sent$"#)]
async fn emits_with_the_code_and_reason_sent(world: &mut ClientWorld, state: String, code: u32) {
    let (_, reason) = world.sent_unsubscribe.clone().expect("an unsubscribe sent");
    let event = expect_subscription_event(world, &state, false).await;
    let expected = match state.as_str() {
        "unsubscribed" => SubscriptionEvent::Unsubscribed { code, reason },
        "subscribing" => SubscriptionEvent::Subscribing { code, reason },
        other => panic!("no {other} event carries a code"),
    };
    assert_eq!(event, expected);
}

#[then(
    r#"then the subscription emits "subscribed" after the fake server answers the new subscribe command"#
)]
async fn subscribed_again_after_the_new_subscribe(world: &mut ClientWorld) {
    let new_subscribe = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let subscribes: Vec<_> = world
                .fake()
                .received()
                .into_iter()
                .filter(|message| message["subscribe"].is_object())
                .collect();
            if let [_, new] = subscribes.as_slice() {
                return new["id"].clone();
            }
            assert!(subscribes.len() < 2, "{subscribes:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a new subscribe command");
    world
        .fake()
        .send(&json!({"id": new_subscribe, "subscribe": {}}).to_string());
    expect_subscription_event(world, "subscribed", false).await;
}

#[when(
    regex = r#"^a subscription to channel "(\w+)" is subscribed and the fake server answers with 1 initial publication$"#
)]
async fn subscribed_with_an_initial_publication(world: &mut ClientWorld, channel: String) {
    world
        .fake()
        .answer_next_subscribe_with(&[json!({"data": {"initial": 1}, "offset": 1})]);
    create(world, &channel);
    subscribe(world).await;
}

#[then("then the subscription receives that publication")]
async fn receives_that_publication(world: &mut ClientWorld) {
    let publication = next_publication(world, false).await;
    assert!(
        has_json_data(&publication, r#"{"initial":1}"#),
        "{publication:?}"
    );
}

#[when(
    regex = r"^the fake server pushes a publication whose data is the non-canonical JSON (\{.*\})$"
)]
fn the_fake_server_pushes_non_canonical_json(world: &mut ClientWorld, data: String) {
    world.fake().send(&format!(
        r#"{{"push":{{"channel":"{CHANNEL}","pub":{{"data":{data}}}}}}}"#
    ));
    world.last_raw_data = Some(data.into_bytes());
}

#[then("the subscription receives a publication whose data is exactly those bytes")]
async fn receives_exactly_those_bytes(world: &mut ClientWorld) {
    let publication = next_publication(world, false).await;
    assert_eq!(
        Some(&publication.data),
        world.last_raw_data.as_ref(),
        "{:?}",
        String::from_utf8_lossy(&publication.data)
    );
}

#[given("a client connected to the fake server that never answers the first subscribe command")]
async fn a_fake_server_ignoring_the_first_subscribe(world: &mut ClientWorld) {
    world.fake().ignore_next_subscribe();
    a_connected_client(world).await;
}

#[then(
    regex = r#"^within the configured timeout plus tolerance the client emits "connecting" with code (\d+)$"#
)]
async fn within_the_timeout_connecting_with_code(world: &mut ClientWorld, code: u32) {
    let limit = test_config().timeout + TOLERANCE;
    let event = tokio::time::timeout(limit, world.next_event())
        .await
        .unwrap_or_else(|_| panic!("no event within {limit:?}"));
    assert!(
        matches!(&event, ClientEvent::Connecting { code: got, .. } if *got == code),
        "{event:?}"
    );
}

#[when(
    "the fake server sends one frame holding a reconnect-advised disconnect push followed by a publication on channel \"news\""
)]
fn a_frame_with_a_disconnect_then_a_publication(world: &mut ClientWorld) {
    let code = 3001;
    world.sent_disconnect = Some((code, "frame test".to_owned()));
    world.fake().send(&format!(
        "{}\n{}",
        json!({"push": {"disconnect": {"code": code, "reason": "frame test"}}}),
        format_args!(r#"{{"push":{{"channel":"{CHANNEL}","pub":{{"data":{STALE}}}}}}}"#),
    ));
}

#[then("the client emits \"connecting\" with that code")]
async fn the_client_emits_connecting_with_that_code(world: &mut ClientWorld) {
    let (code, _) = world.sent_disconnect.clone().expect("a disconnect sent");
    let event = world.next_event().await;
    assert!(
        matches!(&event, ClientEvent::Connecting { code: got, .. } if *got == code),
        "{event:?}"
    );
}

#[then("the subscription receives no publication from that frame")]
async fn no_publication_from_that_frame(world: &mut ClientWorld) {
    let events = world
        .subscription_events_during(NO_PUBLICATION_PERIOD)
        .await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Publication(publication) if has_json_data(publication, STALE)
        )),
        "{events:?}"
    );
}

/// The sans-IO core of an @E scenario with one subscription, connected by
/// a step. It answers the data and token callbacks from scripted answers,
/// and the server's subscribe and `sub_refresh` replies from scripted
/// reply fields; a command without one stays unanswered. Timers other than
/// an answered resubscribe are fired by steps.
pub(crate) struct MachineRun {
    machine: Machine,
    pub(crate) subscription: SubscriptionId,
    transport: Option<TransportId>,
    /// Every action the machine returned, in order.
    pub(crate) actions: Vec<Action>,
    answers: VecDeque<Result<Vec<u8>, DataError>>,
    pub(crate) token_answers: VecDeque<Result<String, TokenError>>,
    /// Answers of the getState callback, in order.
    pub(crate) state_answers: VecDeque<Result<StreamPosition, StateError>>,
    /// Data callback calls answered so far.
    pub(crate) fetches: usize,
    /// Channels the token callback was called with, in order.
    pub(crate) token_calls: Vec<String>,
    /// Callback calls in order: "token", "data" or "state".
    pub(crate) calls: Vec<&'static str>,
    /// Delay of the resubscribe timer before the latest fetch, if any.
    pub(crate) delay_before_fetch: Option<Duration>,
    /// Reply fields for the next subscribe commands, e.g. `"subscribe":{}`.
    pub(crate) subscribe_answers: VecDeque<String>,
    /// Reply fields for the next `sub_refresh` commands.
    pub(crate) sub_refresh_answers: VecDeque<String>,
    /// Unanswered subscribe and `sub_refresh` commands, by id.
    pending_subscribe: Option<u64>,
    pending_sub_refresh: Option<u64>,
    /// Index into `actions` from which a later step looks.
    pub(crate) mark: usize,
}

impl MachineRun {
    /// A subscription with a data callback answered from `answers`.
    fn new(answers: Vec<Result<Vec<u8>, DataError>>) -> Self {
        Self::with(
            SubscriptionOptions::default().data_source(),
            answers,
            Vec::new(),
        )
    }

    pub(crate) fn with(
        options: SubscriptionOptions,
        answers: Vec<Result<Vec<u8>, DataError>>,
        token_answers: Vec<Result<String, TokenError>>,
    ) -> Self {
        let mut machine = Machine::new(test_config());
        let subscription = machine
            .add_subscription(CHANNEL.to_owned(), options)
            .expect("no subscription yet");
        Self {
            machine,
            subscription,
            transport: None,
            actions: Vec::new(),
            answers: answers.into(),
            token_answers: token_answers.into(),
            state_answers: VecDeque::new(),
            fetches: 0,
            token_calls: Vec::new(),
            calls: Vec::new(),
            delay_before_fetch: None,
            subscribe_answers: VecDeque::new(),
            sub_refresh_answers: VecDeque::new(),
            pending_subscribe: None,
            pending_sub_refresh: None,
            mark: 0,
        }
    }

    /// Feeds `input`, then answers callbacks and commands and fires
    /// resubscribe timers while answers remain, until the machine waits on
    /// something else.
    pub(crate) fn handle(&mut self, input: Input) {
        let mut pending = VecDeque::from([input]);
        while let Some(input) = pending.pop_front() {
            let actions = self.machine.handle(input);
            for action in &actions {
                match action {
                    Action::FetchSubscriptionData { fetch, .. } => {
                        self.calls.push("data");
                        if let Some(result) = self.answers.pop_front() {
                            self.fetches += 1;
                            pending.push_back(Input::SubscriptionDataFetched {
                                fetch: *fetch,
                                result,
                            });
                        }
                    }
                    Action::FetchSubscriptionToken { channel, fetch, .. } => {
                        self.calls.push("token");
                        self.token_calls.push(channel.clone());
                        if let Some(result) = self.token_answers.pop_front() {
                            pending.push_back(Input::SubscriptionTokenFetched {
                                fetch: *fetch,
                                result,
                            });
                        }
                    }
                    Action::FetchSubscriptionState { fetch, .. } => {
                        self.calls.push("state");
                        if let Some(result) = self.state_answers.pop_front() {
                            pending.push_back(Input::SubscriptionStateFetched {
                                fetch: *fetch,
                                result,
                            });
                        }
                    }
                    Action::SetSubscriptionTimer(id, SubscriptionTimer::Resubscribe, delay)
                        if !self.answers.is_empty()
                            || !self.token_answers.is_empty()
                            || !self.state_answers.is_empty() =>
                    {
                        self.delay_before_fetch = Some(*delay);
                        pending.push_back(Input::SubscriptionTimerFired(
                            *id,
                            SubscriptionTimer::Resubscribe,
                        ));
                    }
                    Action::Send { frame, .. } => {
                        for command in commands(frame) {
                            let id = command["id"].as_u64().unwrap_or_default();
                            if command.get("subscribe").is_some() {
                                match self.subscribe_answers.pop_front() {
                                    Some(fields) => pending.push_back(self.reply(id, &fields)),
                                    None => self.pending_subscribe = Some(id),
                                }
                            } else if command.get("sub_refresh").is_some() {
                                match self.sub_refresh_answers.pop_front() {
                                    Some(fields) => pending.push_back(self.reply(id, &fields)),
                                    None => self.pending_sub_refresh = Some(id),
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            self.actions.extend(actions);
        }
    }

    fn reply(&self, id: u64, fields: &str) -> Input {
        Input::Frame {
            transport: self.transport.expect("connected"),
            data: format!("{{\"id\":{id},{fields}}}").into_bytes(),
        }
    }

    /// Answers the unanswered subscribe command with `fields`.
    pub(crate) fn answer_subscribe(&mut self, fields: &str) {
        let id = self
            .pending_subscribe
            .take()
            .expect("a subscribe command in flight");
        let reply = self.reply(id, fields);
        self.handle(reply);
    }

    pub(crate) fn subscribing(&self) -> bool {
        self.pending_subscribe.is_some()
    }

    pub(crate) fn fire(&mut self, timer: SubscriptionTimer) {
        let subscription = self.subscription;
        self.handle(Input::SubscriptionTimerFired(subscription, timer));
    }

    /// Connects the machine over a transport answering connect at once.
    pub(crate) fn connect(&mut self) {
        let actions = self.machine.handle(Input::Connect);
        let transport = actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a transport to open");
        self.transport = Some(transport);
        self.machine.handle(Input::TransportOpened(transport));
        self.machine.handle(Input::Frame {
            transport,
            data: br#"{"id":1,"connect":{"client":"c"}}"#.to_vec(),
        });
        assert_eq!(self.machine.state(), ferrifuge::State::Connected);
    }

    pub(crate) fn state(&self) -> Option<SubscriptionState> {
        self.machine.subscription_state(self.subscription)
    }

    pub(crate) fn events(&self) -> Vec<&SubscriptionEvent> {
        self.actions
            .iter()
            .filter_map(|action| match action {
                Action::EmitSubscription(_, _, event) => Some(event),
                _ => None,
            })
            .collect()
    }

    /// Frames sent, as text.
    fn sent(&self) -> Vec<String> {
        self.actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect()
    }

    /// The `method` requests sent from action `from` on, in order.
    pub(crate) fn requests(&self, method: &str, from: usize) -> Vec<Value> {
        self.actions[from..]
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(commands(frame)),
                _ => None,
            })
            .flatten()
            .filter_map(|command| command.get(method).cloned())
            .collect()
    }

    /// Delays of the `kind` timers set from action `from` on, in order.
    pub(crate) fn timers(&self, kind: SubscriptionTimer, from: usize) -> Vec<Duration> {
        self.actions[from..]
            .iter()
            .filter_map(|action| match action {
                Action::SetSubscriptionTimer(_, timer, delay) if *timer == kind => Some(*delay),
                _ => None,
            })
            .collect()
    }
}

/// The commands of a JSON frame.
fn commands(frame: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(frame)
        .lines()
        .map(|line| serde_json::from_str(line).expect("a JSON command"))
        .collect()
}

#[given("a subscription whose data callback fails once")]
fn a_data_callback_failing_once(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::new(vec![
        Err(DataError::Failed("backend down".to_owned())),
        Ok(br#"{"n":2}"#.to_vec()),
    ]));
}

#[given(r#"a subscription whose data callback signals "unauthorized""#)]
fn a_data_callback_denying_access(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::new(vec![Err(DataError::Unauthorized)]));
}

#[when("the subscription is subscribed on a connected client")]
async fn subscribed_on_a_connected_machine(world: &mut ClientWorld) {
    // A real client in @A scenarios, e.g. one subscribing with `since`.
    if world.subscription_machine.is_none() {
        world.client().connect();
        super::expect_event(world, "connecting").await;
        super::expect_event(world, "connected").await;
        subscribe(world).await;
        return;
    }
    let run = world.subscription_machine.as_mut().expect("@E machine");
    run.connect();
    let subscription = run.subscription;
    run.handle(Input::Subscribe(subscription));
}

#[then(r#"the subscription emits an "error" event"#)]
fn the_subscription_emits_an_error(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    assert!(
        run.events()
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Error(_))),
        "{:?}",
        run.actions
    );
}

#[then("the data callback is called again after the resubscribe backoff delay")]
fn the_data_callback_is_called_again(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    assert_eq!(run.fetches, 2, "{:?}", run.actions);
    let delay = run.delay_before_fetch.expect("a resubscribe delay");
    // The resubscribe backoff defaults to centrifuge-js's: from 500 ms.
    assert!(delay >= Duration::from_millis(500), "{delay:?}");
}

#[then("the subscribe command carries the data the second call returned")]
fn the_subscribe_command_carries_the_data(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    let subscribes: Vec<_> = run
        .sent()
        .into_iter()
        .filter(|frame| frame.contains(r#""subscribe""#))
        .collect();
    assert_eq!(subscribes.len(), 1, "{subscribes:?}");
    let command: Value = serde_json::from_str(&subscribes[0]).expect("JSON command");
    assert_eq!(
        command["subscribe"],
        json!({"channel": CHANNEL, "data": {"n": 2}}),
        "{command}"
    );
}

#[then("no subscribe command is sent")]
fn no_subscribe_command(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    assert!(
        !run.sent()
            .iter()
            .any(|frame| frame.contains(r#""subscribe""#)),
        "{:?}",
        run.sent()
    );
}

#[then("no resubscribe is scheduled")]
fn no_resubscribe_is_scheduled(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    assert!(
        !run.actions.iter().any(|action| matches!(
            action,
            Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, _)
        )),
        "{:?}",
        run.actions
    );
}
