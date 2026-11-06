//! Steps of Block 11: stream positions and recovery.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::machine::SubscriptionOptions;
use ferrifuge::{
    ClientEvent, StateError, StreamPosition, Subscribed, SubscriptionEvent, SubscriptionState,
};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::connection::a_connected_client;
use super::subscriptions::{
    MachineRun, create, expect_subscription_event, has_json_data, next_publication, subscribe,
};
use super::transports::server;
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::proxy;
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
pub(super) const FAKE_EPOCH: &str = "fake-epoch";
pub(super) const FAKE_OFFSET: u64 = 3;
/// Epoch of a stream that started again.
const NEW_EPOCH: &str = "new-epoch";
/// How long a step waits for a publication that must not arrive.
const NO_PUBLICATION_PERIOD: Duration = Duration::from_millis(500);

/// What the recovery steps of a scenario keep.
#[derive(Debug)]
pub(crate) struct Recovery {
    /// Publications made through the server API: data and position.
    published: Vec<(String, StreamPosition)>,
    /// Data of the publications the subscription must receive next.
    expected: Vec<String>,
    /// The stream position the client must recover from next.
    position: Option<StreamPosition>,
    /// Subscribe commands the fake server had received before the step
    /// making the client resubscribe.
    pub(super) subscribes_before: usize,
    /// Connect commands the fake server had received when the scenario
    /// last disconnected the client.
    pub(super) connects_before: usize,
    /// The channel the server API publishes to, when not [`RECOVERABLE`]:
    /// e.g. one with fossil delta (Block 19).
    pub(super) channel: Option<&'static str>,
    /// When the getState callback of the scenario's client was called.
    state_calls: Arc<Mutex<Vec<Instant>>>,
    /// The position it answers with.
    state_answer: Arc<Mutex<Option<StreamPosition>>>,
    /// Permits to answer: a call waits for one, so a step can look at the
    /// subscription while getState is running.
    state_gate: Arc<Semaphore>,
}

impl Default for Recovery {
    fn default() -> Self {
        Self {
            published: Vec::new(),
            expected: Vec::new(),
            position: None,
            subscribes_before: 0,
            connects_before: 0,
            channel: None,
            state_calls: Arc::default(),
            state_answer: Arc::default(),
            state_gate: Arc::new(Semaphore::new(0)),
        }
    }
}

pub(super) fn position(offset: u64, epoch: &str) -> StreamPosition {
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
        let channel = world.recovery.channel.unwrap_or(RECOVERABLE);
        let result = server(world)
            .api()
            .call(
                "publish",
                json!({"channel": channel, "data": serde_json::from_str::<Value>(&data).unwrap()}),
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
pub(super) async fn disconnect_and_hold(world: &mut ClientWorld) {
    world.relay.as_ref().expect("a relay").set_mode(Mode::Down);
    let client_id = world.client_id.clone().expect("a connected client");
    // Connections accepted by the proxy belong to its user.
    let user = if world.proxy.is_some() {
        proxy::USER
    } else {
        ""
    };
    server(world)
        .api()
        .call(
            "disconnect",
            json!({
                "user": user,
                "client": client_id,
                "disconnect": {"code": RECONNECT_CODE, "reason": "reconnect advised by the test"},
            }),
        )
        .await
        .expect("server API disconnect");
    // The old connection is gone before the scenario publishes, so nothing
    // arrives live: a client-side subscription says so, else the client.
    if world.subscription.is_some() {
        expect_subscription_event(world, "subscribing", false).await;
    } else {
        tokio::time::timeout(EVENT_TIMEOUT, async {
            while !matches!(world.next_event().await, ClientEvent::Connecting { .. }) {}
        })
        .await
        .expect("the client reconnecting in time");
    }
}

pub(super) fn let_the_client_back(world: &ClientWorld) {
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
}

/// The subscription's next "subscribed" event; state events may come
/// first, a publication may not.
pub(super) async fn next_subscribed(world: &mut ClientWorld) -> Subscribed {
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
pub(super) async fn subscribe_request(world: &ClientWorld, index: usize) -> Value {
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

/// Notes how many subscribe and connect commands the fake server received
/// before the scenario disconnects the client, so that later steps find the
/// next ones.
pub(super) fn note_commands_before_disconnect(world: &mut ClientWorld) {
    world.recovery.subscribes_before = subscribes(world).len();
    world.recovery.connects_before = connects(world).len();
}

/// The fake server's connect command number `index` (from 0), once it
/// arrives within [`EVENT_TIMEOUT`].
pub(super) async fn connect_request(world: &ClientWorld, index: usize) -> Value {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(connect) = connects(world).get(index) {
                return connect.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no connect command number {index} within {EVENT_TIMEOUT:?}"))
}

/// A subscribe result granting recovery, with a stream position.
fn recoverable_result(at: &StreamPosition) -> Value {
    json!({"recoverable": true, "positioned": true, "epoch": at.epoch, "offset": at.offset})
}

/// Subscribes to the fake server's channel, which its reply makes
/// recoverable at a stream position.
pub(super) async fn subscribed_to_the_recoverable_fake_channel(world: &mut ClientWorld) {
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
pub(super) fn fake_server_disconnects(world: &mut ClientWorld) {
    note_commands_before_disconnect(world);
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

#[when(
    regex = r"^2 publications (?:that differ in a few bytes from it )?are made to the channel(?:, as deltas,)? while the client reconnects$"
)]
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

/// The top of the recoverable channel's stream.
async fn stream_top(world: &ClientWorld) -> StreamPosition {
    let result = server(world)
        .api()
        .call("history", json!({"channel": RECOVERABLE, "limit": 0}))
        .await
        .expect("history");
    position(
        result["offset"].as_u64().unwrap_or_default(),
        result["epoch"].as_str().expect("an epoch"),
    )
}

/// Makes the scenario's subscription to the recoverable channel, with a
/// getState callback answering `state_answer` once a permit lets it.
fn a_subscription_with_get_state(world: &mut ClientWorld) {
    let recovery = &world.recovery;
    let (calls, answer, gate) = (
        Arc::clone(&recovery.state_calls),
        Arc::clone(&recovery.state_answer),
        Arc::clone(&recovery.state_gate),
    );
    let subscription = world
        .client()
        .subscription_builder(RECOVERABLE)
        .get_state(move |_channel| {
            let (calls, answer, gate) =
                (Arc::clone(&calls), Arc::clone(&answer), Arc::clone(&gate));
            async move {
                calls.lock().unwrap().push(Instant::now());
                gate.acquire().await.expect("the gate stays open").forget();
                let answer = answer.lock().unwrap().clone();
                answer.ok_or_else(|| StateError("no position to answer".to_owned()))
            }
        })
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
}

fn state_calls(world: &ClientWorld) -> Vec<Instant> {
    world.recovery.state_calls.lock().unwrap().clone()
}

/// Lets the getState callback answer `at` once.
fn answer_state_with(world: &ClientWorld, at: StreamPosition) {
    *world.recovery.state_answer.lock().unwrap() = Some(at);
    world.recovery.state_gate.add_permits(1);
}

#[given("a subscription with a getState callback to a channel with history and recovery")]
async fn a_get_state_subscription_to_a_channel_with_publications(world: &mut ClientWorld) {
    publish(world, 3).await;
    // getState gives the position of the first: the later ones are
    // recovered.
    world.recovery.expected.remove(0);
    let first = world.recovery.published[0].1.clone();
    answer_state_with(world, first);
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    a_subscription_with_get_state(world);
}

#[then("the getState callback is called once before the subscribe command")]
async fn get_state_called_before_the_subscribe(world: &mut ClientWorld) {
    let proxy = world
        .proxy
        .as_ref()
        .expect("a subscribe proxy: tag the scenario @B");
    let first_subscribe = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let requests = proxy.subscribe_requests();
            if let Some(index) = requests
                .iter()
                .position(|request| request["channel"] == RECOVERABLE)
            {
                return proxy.subscribe_times()[index];
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a subscribe request within the timeout");
    let calls = state_calls(world);
    assert_eq!(calls.len(), 1, "getState calls: {calls:?}");
    assert!(calls[0] < first_subscribe, "getState after the subscribe");
}

/// The subscribe carried the position getState gave: Centrifugo recovered
/// from it, which needs that offset and epoch (the subscribe proxy does not
/// see positions; verified, Block 11).
#[then("the subscribe command carries the stream position getState returned")]
async fn the_subscribe_carries_the_get_state_position(world: &mut ClientWorld) {
    let subscribed = next_subscribed(world).await;
    assert!(
        subscribed.was_recovering && subscribed.recovered,
        "{subscribed:?}"
    );
    receive_exactly_the_expected(world).await;
}

#[given("a client subscribed with a getState callback to a channel with history and recovery")]
async fn a_client_subscribed_with_get_state(world: &mut ClientWorld) {
    a_connected_client(world).await;
    let top = stream_top(world).await;
    answer_state_with(world, top);
    a_subscription_with_get_state(world);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[then("the getState callback is called a second time")]
async fn get_state_called_again(world: &mut ClientWorld) {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        while state_calls(world).len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("getState calls: {:?}", state_calls(world)));
}

/// getState is still running: it waits for a permit.
#[then(r#"until then the subscription stays "subscribing" and receives no publication"#)]
async fn subscribing_until_then(world: &mut ClientWorld) {
    let events = world
        .subscription_events_during(Duration::from_millis(100))
        .await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Subscribed(_) | SubscriptionEvent::Publication(_)
        )),
        "{events:?}"
    );
    assert_eq!(world.subscription().state(), SubscriptionState::Subscribing);
}

/// getState answers the stream's top: a recovery from it succeeds, which
/// it could not from the position the history lost.
#[then("the next subscribe command carries the stream position it returned")]
async fn the_next_subscribe_carries_the_new_position(world: &mut ClientWorld) {
    let top = stream_top(world).await;
    answer_state_with(world, top);
    let subscribed = next_subscribed(world).await;
    assert!(
        subscribed.was_recovering && subscribed.recovered,
        "{subscribed:?}"
    );
    // The next step looks at it too.
    world
        .put_back
        .push_front(SubscriptionEvent::Subscribed(subscribed));
}

#[given("a subscription with the since option and a getState callback")]
fn a_machine_with_since_and_get_state(world: &mut ClientWorld) {
    let options = SubscriptionOptions::default()
        .since(position(5, "since-epoch"))
        .state_source();
    let mut run = MachineRun::with(options, Vec::new(), Vec::new());
    run.state_answers.push_back(Ok(position(9, "state-epoch")));
    world.subscription_machine = Some(run);
}

#[then("the getState callback is not called")]
fn get_state_not_called(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    assert!(!run.calls.contains(&"state"), "{:?}", run.actions);
}

/// The first subscribe command of an @E machine asks to recover from `at`.
fn first_subscribe_recovers_from(run: &MachineRun, at: &StreamPosition) -> Value {
    let subscribe = run
        .requests("subscribe", 0)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no subscribe command: {:?}", run.actions));
    assert_eq!(subscribe["recover"], true, "{subscribe}");
    assert_eq!(
        (
            subscribe["epoch"].as_str().unwrap_or_default(),
            subscribe["offset"].as_u64().unwrap_or_default(),
        ),
        (at.epoch.as_str(), at.offset),
        "{subscribe}"
    );
    subscribe
}

#[then("the subscribe command asks to recover from the since position")]
fn the_subscribe_recovers_from_since(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    first_subscribe_recovers_from(run, &position(5, "since-epoch"));
}

#[given("a subscription with a getState callback that fails once")]
fn a_machine_with_get_state_failing_once(world: &mut ClientWorld) {
    let options = SubscriptionOptions::default().state_source();
    let mut run = MachineRun::with(options, Vec::new(), Vec::new());
    run.state_answers.extend([
        Err(StateError("backend down".to_owned())),
        Ok(position(9, "state-epoch")),
    ]);
    world.subscription_machine = Some(run);
}

#[then("getState is called again after the resubscribe backoff delay")]
fn get_state_called_again_after_the_backoff(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    let calls = run.calls.iter().filter(|call| **call == "state").count();
    assert_eq!(calls, 2, "{:?}", run.actions);
    let delay = run.delay_before_fetch.expect("a resubscribe delay");
    // The resubscribe backoff defaults to centrifuge-js's: from 500 ms.
    assert!(delay >= Duration::from_millis(500), "{delay:?}");
}

#[then("the subscribe command carries the stream position the second call returned")]
fn the_subscribe_carries_the_second_position(world: &mut ClientWorld) {
    let run = world.subscription_machine.as_ref().expect("@E machine");
    let subscribe = first_subscribe_recovers_from(run, &position(9, "state-epoch"));
    // With getState, a recovery that fails is rejected rather than
    // answered unrecovered (client.proto, SubscribeRequest.flag).
    assert_eq!(subscribe["flag"], 2, "{subscribe}");
    assert_eq!(run.requests("subscribe", 0).len(), 1, "{:?}", run.actions);
}

/// Subscribes to the fake server's recoverable channel with `builder`'s
/// options: the subscription has a stream position.
async fn subscribed_on_the_fake_server_with(
    world: &mut ClientWorld,
    builder: ferrifuge::SubscriptionBuilder,
) {
    let at = position(FAKE_OFFSET, FAKE_EPOCH);
    world
        .fake()
        .answer_a_subscribe_with(recoverable_result(&at));
    let subscription = builder.build().expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given(r#"a client subscribed to channel "news" on the fake server using a subscription token callback"#)]
async fn subscribed_with_a_token_callback(world: &mut ClientWorld) {
    a_connected_client(world).await;
    let calls = Arc::clone(&world.token_calls);
    let builder = world
        .client()
        .subscription_builder(CHANNEL)
        .get_token(move |channel| {
            let calls = Arc::clone(&calls);
            async move {
                let mut calls = calls.lock().unwrap();
                calls.push(channel);
                Ok(format!("t{}", calls.len()))
            }
        });
    subscribed_on_the_fake_server_with(world, builder).await;
}

#[given(
    r#"a client subscribed to channel "news" on the fake server using a fixed subscription token"#
)]
async fn subscribed_with_a_fixed_token(world: &mut ClientWorld) {
    a_connected_client(world).await;
    let builder = world
        .client()
        .subscription_builder(CHANNEL)
        .token("fixed-subscription-token");
    subscribed_on_the_fake_server_with(world, builder).await;
}

/// The subscribe command after the first, which the unsubscribe push made
/// the client send.
async fn the_resubscribe(world: &ClientWorld) -> Value {
    let first = subscribe_request(world, 0).await;
    assert!(first["recover"].is_null(), "{first}");
    subscribe_request(world, 1).await
}

fn assert_no_position(subscribe: &Value) {
    for field in ["recover", "epoch", "offset"] {
        assert!(subscribe[field].is_null(), "{subscribe}");
    }
}

#[then("the subscription resubscribes with the new token and without a stream position")]
async fn resubscribes_with_the_new_token_without_position(world: &mut ClientWorld) {
    let subscribe = the_resubscribe(world).await;
    assert_eq!(subscribe["token"], "t2", "{subscribe}");
    assert_no_position(&subscribe);
}

#[then("the subscription resubscribes without a token and without a stream position")]
async fn resubscribes_without_token_or_position(world: &mut ClientWorld) {
    let subscribe = the_resubscribe(world).await;
    assert!(subscribe["token"].is_null(), "{subscribe}");
    assert_no_position(&subscribe);
}

#[given(
    "a client using a fixed connection token and no token callback, connected to the fake server"
)]
async fn a_client_with_a_fixed_connection_token(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    let mut config = world.client_config();
    "fixed-connection-token".clone_into(&mut config.token);
    world.build_client(ferrifuge::Client::builder(endpoint).config(config));
    world.client().connect();
    super::expect_event(world, "connecting").await;
    super::expect_event(world, "connected").await;
}

/// Connect commands the fake server has received.
pub(super) fn connects(world: &ClientWorld) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter_map(|message| {
            let connect = &message["connect"];
            connect.is_object().then(|| connect.clone())
        })
        .collect()
}

#[then("the next connect command carries no token")]
async fn the_next_connect_carries_no_token(world: &mut ClientWorld) {
    let connects = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let connects = connects(world);
            if connects.len() >= 2 {
                return connects;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a second connect command within the timeout");
    assert_eq!(
        connects[0]["token"], "fixed-connection-token",
        "{connects:?}"
    );
    assert!(connects[1]["token"].is_null(), "{connects:?}");
}
