//! Steps of Block 19: Fossil delta compression. @D scenarios push deltas
//! made by the test-only writer from the fake server; @A scenarios use a
//! Centrifugo namespace with fossil delta on.

use cucumber::{given, then, when};
use ferrifuge::{DeltaFormat, SubscriptionEvent, write_fossil_delta};
use serde_json::{Value, json};

use super::ClientWorld;
use super::connection::a_connected_client;
use super::subscriptions::{expect_subscription_event, next_publication, subscribe};
use super::transports::server;

/// The fake server's channel.
const CHANNEL: &str = "news";

/// Channels of the @A server's `delta` namespace: history, recovery and
/// fossil delta on every publication (`tests/bdd.rs`, `SERVER_A`).
const DELTA_CHANNEL: &str = "delta:news";

/// What a Block 19 scenario holds.
#[derive(Default)]
pub(crate) struct Delta {
    /// The payloads the subscription must receive, in order.
    expected: Vec<Vec<u8>>,
    /// The last whole payload pushed: what the next delta applies to.
    base: Vec<u8>,
    /// Bytes the relay had sent to the client before the publications
    /// whose size a step measures.
    bytes_before: usize,
    /// Whether the last `subscribed` event reported delta on.
    negotiated: Option<bool>,
}

/// Subscribes the scenario's subscription to `channel` with the fossil
/// delta option, the fake server answering with `result`, and returns the
/// `subscribed` event's delta flag.
async fn subscribed_with_delta(world: &mut ClientWorld, channel: &str) -> bool {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .delta(DeltaFormat::Fossil)
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    let SubscriptionEvent::Subscribed(subscribed) =
        expect_subscription_event(world, "subscribed", false).await
    else {
        unreachable!("expect_subscription_event checked the name");
    };
    subscribed.delta
}

#[given(
    regex = r#"^a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on$"#
)]
async fn subscribed_on_the_fake_server(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world.fake().answer_a_subscribe_with(json!({"delta": true}));
    assert!(subscribed_with_delta(world, CHANNEL).await);
}

/// The data of a publication of a subscription with delta on, with the
/// JSON protocol: `payload` as a JSON string (`client.proto`,
/// `Publication.data`).
fn as_json_string(payload: &[u8]) -> Value {
    Value::String(String::from_utf8(payload.to_vec()).expect("UTF-8 payloads in these steps"))
}

/// Pushes `payload` whole from the fake server on the scenario's channel.
fn push_whole(world: &mut ClientWorld, payload: &[u8]) {
    let push = json!({"push": {"channel": CHANNEL, "pub": {"data": as_json_string(payload)}}});
    world.fake().send(&push.to_string());
    world.delta.base = payload.to_vec();
    world.delta.expected.push(payload.to_vec());
}

/// Pushes a delta turning the last payload into `target`.
fn push_delta_to(world: &mut ClientWorld, target: &[u8]) {
    let delta = write_fossil_delta(&world.delta.base, target);
    let push = json!({"push": {"channel": CHANNEL, "pub": {"data": as_json_string(&delta), "delta": true}}});
    world.fake().send(&push.to_string());
    world.delta.base = target.to_vec();
    world.delta.expected.push(target.to_vec());
}

#[when("the fake server pushes a whole publication and then a delta publication against it")]
fn a_whole_publication_then_a_delta(world: &mut ClientWorld) {
    push_whole(
        world,
        br#"{"text":"the quick brown fox jumps over the lazy dog","n":1}"#,
    );
    push_delta_to(
        world,
        br#"{"text":"the quick red fox jumps over the lazy dog","n":2}"#,
    );
}

#[then(
    regex = r"^the subscription receives (?:the whole payload and then the payload the delta builds|exactly the bytes .* and then the bytes the delta builds)$"
)]
async fn receives_the_expected_payloads(world: &mut ClientWorld) {
    for expected in std::mem::take(&mut world.delta.expected) {
        let publication = next_publication(world, false).await;
        assert_eq!(
            String::from_utf8_lossy(&publication.data),
            String::from_utf8_lossy(&expected)
        );
    }
}

#[when(regex = r#"^a subscription to channel "(\w+)" with the fossil delta option is subscribed$"#)]
async fn a_delta_subscription_is_subscribed(world: &mut ClientWorld, channel: String) {
    subscribed_with_delta(world, &channel).await;
}

#[then(
    regex = r#"^the fake server received (\d+) subscribe commands for "(\w+)", each asking for fossil delta$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn every_subscribe_asks_for_delta(world: &mut ClientWorld, count: usize, channel: String) {
    let subscribes: Vec<Value> = world
        .fake()
        .received()
        .into_iter()
        .filter(|message| message["subscribe"]["channel"] == channel.as_str())
        .collect();
    assert_eq!(subscribes.len(), count, "{subscribes:?}");
    assert!(
        subscribes
            .iter()
            .all(|message| message["subscribe"]["delta"] == "fossil"),
        "{subscribes:?}"
    );
}

#[when(
    r#"the fake server pushes a whole publication whose data is the JSON string of the non-canonical JSON {"b": 1.0,  "a":"é<"}, its é and < sent as \u escapes"#
)]
fn a_whole_publication_with_escapes(world: &mut ClientWorld) {
    let payload = r#"{"b": 1.0,  "a":"é<"}"#;
    // The JSON string by hand, so that é and < travel as \u escapes.
    let data = r#""{\"b\": 1.0,  \"a\":\"é<\"}""#;
    assert_eq!(
        serde_json::from_str::<String>(data).expect("a JSON string"),
        payload
    );
    let push = format!(r#"{{"push":{{"channel":"{CHANNEL}","pub":{{"data":{data}}}}}}}"#);
    world.fake().send(&push);
    world.delta.base = payload.as_bytes().to_vec();
    world.delta.expected.push(payload.as_bytes().to_vec());
}

#[when("then a delta publication against it, as a JSON string")]
fn then_a_delta_against_it(world: &mut ClientWorld) {
    push_delta_to(world, r#"{"b": 1.0,  "a":"é<", "c":3}"#.as_bytes());
}

#[when(
    "a subscription with the fossil delta option to a channel allowing fossil delta is subscribed"
)]
async fn a_delta_subscription_on_the_server(world: &mut ClientWorld) {
    world.delta.negotiated = Some(subscribed_with_delta(world, DELTA_CHANNEL).await);
}

#[then(r#"the subscription emits "subscribed" reporting delta true"#)]
fn subscribed_reporting_delta(world: &mut ClientWorld) {
    assert_eq!(
        world.delta.negotiated,
        Some(true),
        "the subscribed event reported no delta"
    );
}

#[given(
    regex = r"^a client configured with the (JSON|Protobuf) protocol, connected through a relay counting bytes, subscribed with the fossil delta option to a channel with history allowing fossil delta and delta publishing$"
)]
async fn a_client_subscribed_to_a_delta_channel(world: &mut ClientWorld, protocol: String) {
    if protocol == "Protobuf" {
        world.protocol = ferrifuge::Protocol::Protobuf;
    }
    a_connected_client(world).await;
    world.delta.negotiated = Some(subscribed_with_delta(world, DELTA_CHANNEL).await);
}

#[given(r#"the subscription emitted "subscribed" reporting delta true"#)]
fn the_subscription_reported_delta(world: &mut ClientWorld) {
    subscribed_reporting_delta(world);
}

/// A payload of 4 KiB that differs from the others of its series in `n`.
fn large_payload(n: usize) -> String {
    json!({"blob": "x".repeat(4000), "n": n}).to_string()
}

#[when(
    "the server API publishes, as deltas, 3 payloads of 4 KiB that differ in a few bytes to the channel"
)]
async fn publishes_three_large_payloads(world: &mut ClientWorld) {
    world.delta.expected.clear();
    for n in 1..=3 {
        let payload = large_payload(n);
        let before = world.relay.as_ref().expect("a relay").bytes_to_clients();
        server(world)
            .api()
            .call(
                "publish",
                json!({"channel": DELTA_CHANNEL, "data": serde_json::from_str::<Value>(&payload).expect("JSON"), "delta": true}),
            )
            .await
            .expect("publish");
        if n == 1 {
            // The first goes whole: measure from once it reached the client.
            let relay = world.relay.as_ref().expect("a relay");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while relay.bytes_to_clients() < before + payload.len() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the first publication never arrived"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            world.delta.bytes_before = relay.bytes_to_clients();
        }
        world.delta.expected.push(payload.into_bytes());
    }
}

#[then("the subscription receives exactly those 3 payloads in order")]
async fn receives_the_three_payloads(world: &mut ClientWorld) {
    let expected = std::mem::take(&mut world.delta.expected);
    for payload in &expected {
        let publication = next_publication(world, false).await;
        let received: Value = serde_json::from_slice(&publication.data).expect("JSON data");
        let sent: Value = serde_json::from_slice(payload).expect("JSON payload");
        assert_eq!(received, sent);
    }
}

#[then("the client read fewer than 2 KiB for the last 2 publications")]
fn fewer_than_two_kib(world: &mut ClientWorld) {
    let read = world.relay.as_ref().expect("a relay").bytes_to_clients() - world.delta.bytes_before;
    assert!(read < 2048, "{read} bytes");
}
