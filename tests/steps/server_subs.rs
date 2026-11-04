//! Steps of Block 12, server-side subscriptions.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    ClientEvent, ServerSubscriptionEvent, StreamPosition, SubscriptionEvent, SubscriptionState,
};
use serde_json::{Value, json};

use super::recovery::{
    FAKE_EPOCH, FAKE_OFFSET, connect_request, disconnect_and_hold, fake_server_disconnects,
    let_the_client_back, position, subscribe_request, subscribed_to_the_recoverable_fake_channel,
};
use super::subscriptions::{create, has_json_data};
use super::transports::{configure, connect_and_wait, kind, server, server_url};
use super::{ClientWorld, EVENT_TIMEOUT, expect_event};

/// How long a step waits to be sure an event does not come.
const QUIET: Duration = Duration::from_millis(500);

/// The epoch no stream has, of a position invalidated by 3014.
const SENTINEL_EPOCH: &str = "_";

/// State of a Block 12 scenario.
#[derive(Debug, Default)]
pub(crate) struct ServerSide {
    /// Data of the publication the client must receive next.
    expected: Option<Value>,
    /// Data of the publications the client must receive next, in order.
    expected_in_order: Vec<Value>,
    /// The stream position the client must recover from next.
    position: Option<StreamPosition>,
    /// Number of the publications the scenario made.
    published: usize,
    /// Calls of the connection token callback.
    token_calls: Arc<AtomicUsize>,
}

#[given(regex = r#"^the connect proxy subscribes the connection to channel "(\w+)"$"#)]
fn proxy_grants(world: &mut ClientWorld, channel: String) {
    proxy(world).grant(&[&channel]);
}

#[given("the connect proxy will subscribe the next connection to no channel")]
fn proxy_grants_nothing(world: &mut ClientWorld) {
    proxy(world).grant(&[]);
}

#[given(
    regex = r#"^the fake server grants a server-side subscription to channel "(\w+)" at connect$"#
)]
fn fake_grants(world: &mut ClientWorld, channel: String) {
    world
        .fake()
        .grant_server_subscriptions(json!({ channel: {} }));
}

#[given(
    regex = r#"^the fake server grants a server-side subscription to channel "(\w+)" with 1 initial publication and recovered false at connect$"#
)]
fn fake_grants_with_a_publication(world: &mut ClientWorld, channel: String) {
    let data = json!({"initial": 1});
    // `recovered` false is left out, as the server does.
    world.fake().grant_server_subscriptions(json!({
        channel: {"publications": [{"data": data, "offset": 1}]},
    }));
    world.server_side.expected = Some(data);
}

#[given(regex = r#"^a client with an unsubscribed client-side subscription to channel "(\w+)"$"#)]
fn client_with_unsubscribed_subscription(world: &mut ClientWorld, channel: String) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    create(world, &channel);
}

#[given("the client is connected to the fake server")]
async fn client_is_connected(world: &mut ClientWorld) {
    connect(world).await;
}

#[when("a client connects to the fake server")]
async fn a_client_connects(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    connect(world).await;
}

#[when(regex = r#"^a client connects over transport "(\w+)"$"#)]
async fn a_client_connects_over(world: &mut ClientWorld, transport: String) {
    let kind = kind(&transport);
    let url = server_url(server(world), kind);
    configure(world, vec![(kind, url)]);
    connect_and_wait(world).await;
}

#[when(regex = r#"^the fake server pushes a publication on channel "(\w+)"$"#)]
fn fake_pushes_publication(world: &mut ClientWorld, channel: String) {
    let data = json!({"pushed": 1});
    push_publication(world, &channel, &data);
    world.server_side.expected = Some(data);
}

#[when("the client-side subscription is subscribed")]
fn client_side_subscribes(world: &mut ClientWorld) {
    world.subscription().subscribe();
}

#[then(regex = r#"^(?:then )?the client emits a server-side "(\w+)" event for channel "(\w+)"$"#)]
async fn emits_server_side_event(world: &mut ClientWorld, name: String, channel: String) {
    next_server_side_event(world, &channel, |event| event.name() == name).await;
}

#[then(
    regex = r#"^the client receives a server-side publication on channel "(\w+)" with data (\{.*\})$"#
)]
async fn receives_server_side_publication(world: &mut ClientWorld, channel: String, data: String) {
    expect_server_side_publication(world, &channel, &data).await;
}

#[then("the client receives it as a server-side publication")]
async fn receives_it_as_server_side(world: &mut ClientWorld) {
    let data = expected(world);
    expect_server_side_publication(world, "alerts", &data).await;
}

#[then(regex = r"^(?:then )?the client receives that publication as a server-side publication$")]
async fn receives_that_publication(world: &mut ClientWorld) {
    let data = expected(world);
    expect_server_side_publication(world, "alerts", &data).await;
}

#[then(r#"the client-side subscription receives nothing and stays "unsubscribed""#)]
async fn client_side_receives_nothing(world: &mut ClientWorld) {
    let events = world.subscription_events_during(QUIET).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{events:?}"
    );
    assert_eq!(
        world.subscription().state(),
        SubscriptionState::Unsubscribed
    );
}

#[then(r#"it reaches state "subscribed""#)]
async fn it_reaches_subscribed(world: &mut ClientWorld) {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        while !matches!(
            world.next_subscription_event(false).await,
            SubscriptionEvent::Subscribed(_)
        ) {}
    })
    .await
    .expect("subscribed in time");
}

#[then(regex = r#"^the next publication on channel "(\w+)" goes to the client-side subscription$"#)]
async fn next_publication_goes_client_side(world: &mut ClientWorld, channel: String) {
    let data = json!({"pushed": 2});
    push_publication(world, &channel, &data);
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let SubscriptionEvent::Publication(publication) =
                world.next_subscription_event(false).await
            {
                return publication;
            }
        }
    })
    .await
    .expect("a client-side publication in time");
    assert!(has_json_data(&received, &data.to_string()), "{received:?}");
    let events = world.events_during(QUIET).await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(_),
                ..
            }
        )),
        "{events:?}"
    );
}

#[then(regex = r#"^the server-side registry still lists channel "(\w+)"$"#)]
fn registry_lists(world: &mut ClientWorld, channel: String) {
    let channels = world.client().server_subscriptions();
    assert!(channels.contains(&channel), "{channels:?}");
}

#[given(
    regex = r#"^the fake server grants a server-side subscription to channel "(\w+)" with data (\{.*\}) at connect$"#
)]
fn fake_grants_with_data(world: &mut ClientWorld, channel: String, data: String) {
    let data: Value = serde_json::from_str(&data).expect("JSON data");
    world
        .fake()
        .grant_server_subscriptions(json!({ channel: {"data": data} }));
}

#[given(regex = r#"^the server API subscribes the client to channel "(\w+)"$"#)]
#[when(regex = r#"^the server API subscribes the client to channel "(\w+)"$"#)]
async fn server_api_subscribes(world: &mut ClientWorld, channel: String) {
    server_api(world, "subscribe", &channel).await;
}

#[when(regex = r#"^the server API unsubscribes the client from channel "(\w+)"$"#)]
async fn server_api_unsubscribes(world: &mut ClientWorld, channel: String) {
    server_api(world, "unsubscribe", &channel).await;
}

#[when(
    regex = r#"^the fake server pushes a server-side subscription to channel "(\w+)" with data (\{.*\})$"#
)]
fn fake_pushes_subscription(world: &mut ClientWorld, channel: String, data: String) {
    let data: Value = serde_json::from_str(&data).expect("JSON data");
    world
        .fake()
        .send(&json!({"push": {"channel": channel, "subscribe": {"data": data}}}).to_string());
}

#[then(
    regex = r#"^(?:then )?the client emits a server-side "subscribed" event for channel "(\w+)" with data (\{.*\})$"#
)]
async fn emits_subscribed_with_data(world: &mut ClientWorld, channel: String, data: String) {
    let event = next_server_side_event(world, &channel, |event| event.name() == "subscribed").await;
    let ServerSubscriptionEvent::Subscribed(subscribed) = &event else {
        unreachable!("only a subscribed event is wanted");
    };
    let got = subscribed
        .data
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
    assert_eq!(got, serde_json::from_str::<Value>(&data).ok(), "{event:?}");
}

#[then(regex = r#"^a publication to channel "(\w+)" is received as a server-side publication$"#)]
async fn publication_received_server_side(world: &mut ClientWorld, channel: String) {
    let data = json!({"via": "server API"});
    server(world)
        .api()
        .call("publish", json!({"channel": channel, "data": data}))
        .await
        .expect("server API publish");
    expect_server_side_publication(world, &channel, &data.to_string()).await;
}

/// Calls the server API `method` (`subscribe` or `unsubscribe`) for the
/// scenario's connection and `channel`.
async fn server_api(world: &ClientWorld, method: &str, channel: &str) {
    // Connections accepted by the proxy belong to its user, others are anonymous.
    let user = if world.proxy.is_some() {
        crate::support::proxy::USER
    } else {
        ""
    };
    let client_id = world.client_id.clone().expect("a connected client");
    server(world)
        .api()
        .call(
            method,
            json!({"user": user, "client": client_id, "channel": channel}),
        )
        .await
        .unwrap_or_else(|error| panic!("server API {method}: {error:?}"));
}

// Block 12c: positions and recovery.

#[given(
    regex = r#"^the connect proxy subscribes the connection to channel "(\w+)" with history and recovery$"#
)]
fn proxy_grants_recoverable(world: &mut ClientWorld, channel: String) {
    proxy(world).grant_recoverable(&[&channel]);
}

// The channel it grants again was subscribed recoverable, so it grants it
// recoverable: a recovery needs that on the server side too.
#[given(regex = r#"^the connect proxy will subscribe the next connection to channel "(\w+)"$"#)]
fn proxy_grants_again(world: &mut ClientWorld, channel: String) {
    proxy(world).grant_recoverable(&[&channel]);
}

#[given(regex = r#"^the server API subscribes the client to recoverable channel "(\w+)"$"#)]
async fn server_api_subscribes_recoverable(world: &mut ClientWorld, channel: String) {
    let client_id = world.client_id.clone().expect("a connected client");
    server(world)
        .api()
        .call(
            "subscribe",
            json!({
                "user": api_user(world),
                "client": client_id,
                "channel": channel,
                "override": {"force_recovery": {"value": true}},
            }),
        )
        .await
        .expect("server API subscribe");
    next_server_side_event(world, &channel, |event| event.name() == "subscribed").await;
}

#[given(regex = r#"^the client has received 1 live server-side publication on channel "(\w+)"$"#)]
async fn received_a_live_server_side_publication(world: &mut ClientWorld, channel: String) {
    publish(world, &channel, 1).await;
    receive_in_order(world, &channel).await;
}

#[when("the client is disconnected with a reconnect-advised code")]
async fn disconnected_with_a_reconnect_code(world: &mut ClientWorld) {
    disconnect_and_hold(world).await;
}

#[when(regex = r#"^2 publications are made to channel "(\w+)" while the client reconnects$"#)]
async fn publications_while_reconnecting(world: &mut ClientWorld, channel: String) {
    publish(world, &channel, 2).await;
    let_the_client_back(world);
}

#[when(regex = r#"^2 publications are made to channel "(\w+)"$"#)]
async fn two_publications(world: &mut ClientWorld, channel: String) {
    publish(world, &channel, 2).await;
}

#[then(
    regex = r#"^the client emits a server-side "subscribed" event for channel "(\w+)" reporting recoverable true, was_recovering true, recovered true and the new stream position$"#
)]
async fn subscribed_recovered(world: &mut ClientWorld, channel: String) {
    let event = next_server_side_event(world, &channel, |event| event.name() == "subscribed").await;
    let ServerSubscriptionEvent::Subscribed(subscribed) = &event else {
        unreachable!("only a subscribed event is wanted");
    };
    assert!(
        subscribed.recoverable && subscribed.was_recovering && subscribed.recovered,
        "{event:?}"
    );
    // Centrifugo reports the position recovered from: the publications
    // that follow move it (Block 12 Verified paragraph).
    let expected = world.server_side.position.clone().expect("a position");
    assert_eq!(
        subscribed.stream_position.as_ref(),
        Some(&expected),
        "{event:?}"
    );
}

#[then("the client receives exactly those 2 server-side publications in order")]
async fn receives_those_two(world: &mut ClientWorld) {
    receive_in_order(world, "alerts").await;
}

#[given(
    regex = r#"^the fake server grants a recoverable server-side subscription to channel "(\w+)" with a stream position at connect$"#
)]
fn fake_grants_recoverable(world: &mut ClientWorld, channel: String) {
    let subs = recoverable_subs(world, &channel);
    world.fake().grant_server_subscriptions(subs);
}

#[given(
    regex = r#"^the fake server grants a recoverable server-side subscription to channel "(\w+)" with a stream position at the first connect only$"#
)]
fn fake_grants_recoverable_once(world: &mut ClientWorld, channel: String) {
    let subs = recoverable_subs(world, &channel);
    world.fake().grant_server_subscriptions_next(subs);
}

#[given("the fake server will grant no server-side subscription on later connects")]
fn fake_grants_nothing_later(world: &mut ClientWorld) {
    world.fake().grant_server_subscriptions(json!({}));
}

#[when(
    regex = r#"^the fake server pushes a recoverable server-side subscription to channel "(\w+)" with a stream position$"#
)]
async fn fake_pushes_recoverable_subscription(world: &mut ClientWorld, channel: String) {
    let at = position(FAKE_OFFSET, FAKE_EPOCH);
    world.fake().send(
        &json!({"push": {"channel": channel, "subscribe": {
            "recoverable": true, "positioned": true, "epoch": at.epoch, "offset": at.offset,
        }}})
        .to_string(),
    );
    world.server_side.position = Some(at);
    next_server_side_event(world, &channel, |event| event.name() == "subscribed").await;
}

#[given(
    regex = r#"^the client recovered 2 server-side publications on channel "(\w+)" after a reconnect$"#
)]
async fn recovered_two_server_side_publications(world: &mut ClientWorld, channel: String) {
    let offset = FAKE_OFFSET;
    world
        .fake()
        .grant_server_subscriptions_next(json!({ channel.clone(): {
            "recoverable": true, "positioned": true, "epoch": FAKE_EPOCH, "offset": offset,
            "was_recovering": true, "recovered": true,
            "publications": [
                {"data": {"n": 4}, "offset": offset + 1},
                {"data": {"n": 5}, "offset": offset + 2},
            ],
        }}));
    world.server_side.expected_in_order = vec![json!({"n": 4}), json!({"n": 5})];
    world.server_side.position = Some(position(offset + 2, FAKE_EPOCH));
    fake_server_disconnects(world);
    receive_in_order(world, &channel).await;
}

#[then(regex = r#"^the next connect command carries no entry for "(\w+)"$"#)]
async fn next_connect_carries_no_entry(world: &mut ClientWorld, channel: String) {
    let connect = next_connect(world).await;
    assert!(connect["subs"].get(&channel).is_none(), "{connect}");
}

#[then(
    regex = r#"^the next connect command asks to recover "(\w+)" from (?:that stream position|the offset of the second recovered publication)$"#
)]
async fn next_connect_recovers(world: &mut ClientWorld, channel: String) {
    let expected = world.server_side.position.clone().expect("a position");
    assert_next_connect_recovers(world, &channel, &expected).await;
}

#[then(regex = r#"^the next connect command asks to recover "(\w+)" from offset 0 and epoch "_"$"#)]
async fn next_connect_recovers_from_the_sentinel(world: &mut ClientWorld, channel: String) {
    assert_next_connect_recovers(world, &channel, &position(0, SENTINEL_EPOCH)).await;
}

#[when(
    regex = r#"^the fake server pushes an unsubscribe for channel "(\w+)" with code (\d+) and reason "([^"]*)"$"#
)]
fn fake_pushes_unsubscribe(world: &mut ClientWorld, channel: String, code: u32, reason: String) {
    world.fake().send(
        &json!({"push": {"channel": channel, "unsubscribe": {"code": code, "reason": reason}}})
            .to_string(),
    );
}

#[then(
    regex = r#"^the client emits a server-side "unsubscribed" event for channel "(\w+)" with code (\d+) and reason "([^"]*)"$"#
)]
async fn unsubscribed_with(world: &mut ClientWorld, channel: String, code: u32, reason: String) {
    let event =
        next_server_side_event(world, &channel, |event| event.name() == "unsubscribed").await;
    assert_eq!(
        event,
        ServerSubscriptionEvent::Unsubscribed { code, reason },
        "{event:?}"
    );
}

#[then(
    regex = r#"^no (second server-side "unsubscribed"|further server-side) event for channel "(\w+)" is emitted$"#
)]
async fn no_more_server_side_events(world: &mut ClientWorld, which: String, channel: String) {
    let only_unsubscribed = which.starts_with("second");
    let events = world.events_during(QUIET).await;
    let unexpected: Vec<_> = events
        .iter()
        .filter(|event| match event {
            ClientEvent::ServerSubscription { channel: of, event } => {
                *of == channel && (!only_unsubscribed || event.name() == "unsubscribed")
            }
            _ => false,
        })
        .collect();
    assert!(unexpected.is_empty(), "{unexpected:?}");
}

// Block 11's "Connection state invalidation clears the token and
// invalidates every position", which needs server-side positions.

#[given("a client using a connection token callback, connected to the fake server")]
async fn client_with_token_callback(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    let calls = Arc::clone(&world.server_side.token_calls);
    let builder = ferrifuge::Client::builder(endpoint)
        .config(super::test_config())
        .get_token(move || {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(format!("token-{call}")) }
        });
    world.build_client(builder);
    connect(world).await;
}

#[given(regex = r#"^a client-side subscription to channel "news" with a stream position$"#)]
async fn client_side_subscription_with_a_position(world: &mut ClientWorld) {
    subscribed_to_the_recoverable_fake_channel(world).await;
}

#[given(
    regex = r#"^a server-side subscription to channel "(\w+)" with a stream position from the connect reply$"#
)]
async fn server_side_subscription_from_the_connect_reply(world: &mut ClientWorld, channel: String) {
    // The client is connected already: a reconnect brings the reply.
    let subs = recoverable_subs(world, &channel);
    world.fake().grant_server_subscriptions(subs);
    fake_server_disconnects(world);
    next_server_side_event(world, &channel, |event| event.name() == "subscribed").await;
}

#[then("the connection token callback is called before the next connect")]
async fn token_callback_before_the_next_connect(world: &mut ClientWorld) {
    let connect = next_connect(world).await;
    // The first connect used the token of the first call; the one after
    // 3014 carries a token the callback gave since.
    let call: usize = connect["token"]
        .as_str()
        .and_then(|token| token.strip_prefix("token-"))
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("a token from the callback: {connect}"));
    assert!(call >= 2, "{connect}");
}

#[then(
    regex = r#"^the next subscribe command for "news" asks to recover from offset 0 and epoch "_"$"#
)]
async fn next_subscribe_recovers_from_the_sentinel(world: &mut ClientWorld) {
    let subscribe = subscribe_request(world, world.recovery.subscribes_before).await;
    assert_eq!(subscribe["recover"], true, "{subscribe}");
    assert_eq!(subscribe["epoch"], SENTINEL_EPOCH, "{subscribe}");
    assert_eq!(
        subscribe["offset"].as_u64().unwrap_or_default(),
        0,
        "{subscribe}"
    );
}

fn api_user(world: &ClientWorld) -> &'static str {
    // Connections accepted by the proxy belong to its user, others are anonymous.
    if world.proxy.is_some() {
        crate::support::proxy::USER
    } else {
        ""
    }
}

/// Publishes `count` numbered publications to `channel` through the server
/// API; the client must receive them next, in order, from the position
/// before them.
async fn publish(world: &mut ClientWorld, channel: &str, count: usize) {
    for _ in 0..count {
        world.server_side.published += 1;
        let data = json!({"n": world.server_side.published});
        let result = server(world)
            .api()
            .call("publish", json!({"channel": channel, "data": data}))
            .await
            .expect("server API publish");
        if world.server_side.expected_in_order.is_empty() {
            // Where a recovery of these starts from: before the first.
            let offset = result["offset"].as_u64().expect("an offset");
            let epoch = result["epoch"].as_str().expect("an epoch");
            world.server_side.position = Some(position(offset - 1, epoch));
        }
        world.server_side.expected_in_order.push(data);
    }
}

/// Receives exactly the expected publications of `channel`, in order.
async fn receive_in_order(world: &mut ClientWorld, channel: &str) {
    for data in std::mem::take(&mut world.server_side.expected_in_order) {
        expect_server_side_publication(world, channel, &data.to_string()).await;
    }
}

/// Subs granting `channel` recoverable at the fake stream position, noted
/// as the one to recover from.
fn recoverable_subs(world: &mut ClientWorld, channel: &str) -> Value {
    let at = position(FAKE_OFFSET, FAKE_EPOCH);
    world.server_side.position = Some(at.clone());
    json!({ channel: {
        "recoverable": true, "positioned": true, "epoch": at.epoch, "offset": at.offset,
    }})
}

/// The connect command of the reconnect that followed the scenario's last
/// disconnect.
async fn next_connect(world: &ClientWorld) -> Value {
    connect_request(world, world.recovery.connects_before).await
}

async fn assert_next_connect_recovers(world: &ClientWorld, channel: &str, from: &StreamPosition) {
    let connect = next_connect(world).await;
    let sub = &connect["subs"][channel];
    assert_eq!(sub["recover"], true, "{connect}");
    assert_eq!(
        (
            sub["epoch"].as_str().unwrap_or_default(),
            sub["offset"].as_u64().unwrap_or_default(),
        ),
        (from.epoch.as_str(), from.offset),
        "{connect}"
    );
}

fn proxy(world: &ClientWorld) -> &crate::support::proxy::ProxyBackend {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
}

fn expected(world: &ClientWorld) -> String {
    world
        .server_side
        .expected
        .as_ref()
        .expect("a publication the scenario made")
        .to_string()
}

/// Connects the scenario's client to the fake server and waits until it is
/// connected.
async fn connect(world: &mut ClientWorld) {
    world.client().connect();
    expect_event(world, "connecting").await;
    let ClientEvent::Connected { client_id, .. } = expect_event(world, "connected").await else {
        unreachable!("expect_event checked the name");
    };
    world.client_id = Some(client_id);
}

fn push_publication(world: &ClientWorld, channel: &str, data: &Value) {
    world
        .fake()
        .send(&json!({"push": {"channel": channel, "pub": {"data": data}}}).to_string());
}

/// The next event of the server-side subscription to `channel` that is
/// `wanted`, within [`EVENT_TIMEOUT`]. Earlier events are skipped: e.g. the
/// `subscribed` emitted at connect, which a shared connect step leaves.
async fn next_server_side_event(
    world: &mut ClientWorld,
    channel: &str,
    wanted: impl Fn(&ServerSubscriptionEvent) -> bool,
) -> ServerSubscriptionEvent {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let ClientEvent::ServerSubscription { channel: of, event } = world.next_event().await
                && of == channel
                && wanted(&event)
            {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no such server-side event for {channel} within {EVENT_TIMEOUT:?}"))
}

async fn expect_server_side_publication(world: &mut ClientWorld, channel: &str, data: &str) {
    let is_publication =
        |event: &ServerSubscriptionEvent| matches!(event, ServerSubscriptionEvent::Publication(_));
    if let ServerSubscriptionEvent::Publication(publication) =
        next_server_side_event(world, channel, is_publication).await
    {
        assert!(has_json_data(&publication, data), "{publication:?}");
    }
}
