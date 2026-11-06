//! Steps of Block 19: Fossil delta compression. @D scenarios push deltas
//! made by the test-only writer from the fake server; @A scenarios use a
//! Centrifugo namespace with fossil delta on.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    ClientEvent, DeltaFormat, ErrorKind, ServerSubscriptionEvent, StreamPosition,
    SubscriptionEvent, write_fossil_delta,
};
use serde_json::{Value, json};

use super::ClientWorld;
use super::connection::a_connected_client;
use super::delivery::wait_until;
use super::recovery::{FAKE_EPOCH, next_subscribed, note_commands_before_disconnect};
use super::server_subs::assert_next_connect_recovers;
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

// Block 19b: failures.

/// The fake server's recoverable stream, as the delta scenarios use it.
const EPOCH: &str = "delta-epoch";

/// A whole payload and the next one, sharing a prefix and a suffix, so that
/// the writer's delta copies both and inserts what differs.
const BASE: &str = r#"{"n":1,"text":"the quick brown fox jumps over the lazy dog"}"#;
const NEXT: &str = r#"{"n":2,"text":"the quick brown fox jumps over the lazy dog"}"#;

/// A subscribe reply of the recoverable stream at `offset`, delta on.
fn recoverable_delta_result(offset: u64) -> Value {
    json!({"delta": true, "recoverable": true, "positioned": true, "epoch": EPOCH, "offset": offset})
}

/// A publication of the scenario's channel: `payload` whole, or as a
/// delta, at `offset`.
fn publication(payload: &[u8], delta: bool, offset: u64) -> Value {
    let mut publication = json!({"data": as_json_string(payload), "offset": offset});
    if delta {
        publication["delta"] = json!(true);
    }
    publication
}

fn push(world: &ClientWorld, channel: &str, publication: Value) {
    world
        .fake()
        .send(&json!({"push": {"channel": channel, "pub": publication}}).to_string());
}

/// Subscribes with delta to the fake server's recoverable channel, then
/// takes the whole publication at offset 1 from the subscription's stream.
async fn subscribed_and_took_offset_1(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world
        .fake()
        .answer_a_subscribe_with(recoverable_delta_result(0));
    assert!(subscribed_with_delta(world, CHANNEL).await);
    push(world, CHANNEL, publication(BASE.as_bytes(), false, 1));
    let taken = next_publication(world, false).await;
    assert_eq!(taken.data, BASE.as_bytes());
    world.delta.base = BASE.as_bytes().to_vec();
}

#[given(
    regex = r#"^a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, which turned delta on, whose subscription's stream has taken the whole publication at offset 1$"#
)]
async fn subscribed_and_took_the_first(world: &mut ClientWorld) {
    subscribed_and_took_offset_1(world).await;
}

/// The writer's delta from `base` to `target`, then broken as `fault` says.
fn broken_delta(base: &[u8], target: &[u8], fault: &str) -> Vec<u8> {
    let delta = String::from_utf8(write_fossil_delta(base, target)).expect("an ASCII delta");
    // The writer ends with the suffix copy, then the trailer: "...,<sum>;".
    let last_comma = delta.rfind(',').expect("a copy of the common suffix");
    let broken = match fault {
        "whose checksum is wrong" => format!("{}0;", &delta[..=last_comma]),
        "that has no trailer" => delta[..=last_comma].to_owned(),
        "that builds fewer bytes than it declares" => {
            // Without the suffix copy: the segment between the last two
            // commas' predecessors, i.e. after the insert.
            let suffix_start = delta[..last_comma]
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '~' || c == '@'))
                .expect("a segment before the suffix copy")
                + 1;
            format!("{}{}", &delta[..suffix_start], &delta[last_comma + 1..])
        }
        other => panic!("no such fault: {other}"),
    };
    broken.into_bytes()
}

#[when(
    regex = r"^the fake server pushes, at offset 2, a delta (whose checksum is wrong|that copies past the end of the previous data|that builds fewer bytes than it declares|that has no trailer)$"
)]
fn pushes_a_broken_delta(world: &mut ClientWorld, fault: String) {
    // The resubscribe recovers the publication at offset 2 whole.
    let mut result = recoverable_delta_result(2);
    result["was_recovering"] = json!(true);
    result["recovered"] = json!(true);
    result["publications"] = json!([publication(NEXT.as_bytes(), false, 2)]);
    world.fake().answer_a_subscribe_with(result);
    world.delta.expected = vec![NEXT.as_bytes().to_vec()];
    let delta = if fault == "that copies past the end of the previous data" {
        // Made against a longer base than the client holds: its suffix
        // copy reads past the end of the client's.
        let padding = "x".repeat(32);
        write_fossil_delta(
            format!("{BASE}{padding}").as_bytes(),
            format!("{NEXT}{padding}").as_bytes(),
        )
    } else {
        broken_delta(BASE.as_bytes(), NEXT.as_bytes(), &fault)
    };
    push(world, CHANNEL, publication(&delta, true, 2));
}

#[then(
    regex = r#"^(?:then )?the subscription emits an "error" event of kind "delta" with code (\d+)$"#
)]
async fn subscription_emits_a_delta_error(world: &mut ClientWorld, code: u32) {
    let event = expect_subscription_event(world, "error", false).await;
    let SubscriptionEvent::Error(error) = event else {
        unreachable!("expect_subscription_event checked the name");
    };
    assert_eq!(
        (error.kind, error.code),
        (ErrorKind::Delta, code),
        "{error:?}"
    );
}

#[then(regex = r#"^then the subscription emits "subscribing" with code (\d+)$"#)]
async fn subscription_emits_subscribing_with(world: &mut ClientWorld, code: u32) {
    let event = expect_subscription_event(world, "subscribing", false).await;
    let SubscriptionEvent::Subscribing { code: got, .. } = event else {
        unreachable!("expect_subscription_event checked the name");
    };
    assert_eq!(got, code);
}

#[then(regex = r#"^the fake server receives an unsubscribe command for "(\w+)"$"#)]
async fn receives_an_unsubscribe(world: &mut ClientWorld, channel: String) {
    let fake = world.fake();
    wait_until("an unsubscribe command", || {
        fake.received()
            .iter()
            .any(|command| command["unsubscribe"]["channel"] == channel.as_str())
    })
    .await;
}

#[then("the subscription receives no publication at offset 2 before that subscribe is answered")]
async fn no_publication_before_the_resubscribe(world: &mut ClientWorld) {
    loop {
        match world.next_subscription_event(false).await {
            SubscriptionEvent::Subscribed(_) => return,
            SubscriptionEvent::Publication(publication) => {
                panic!("a publication before the resubscribe: {publication:?}")
            }
            _ => {}
        }
    }
}

#[then(
    "when the fake server answers it with the publication at offset 2 recovered whole, the subscription receives it exactly once"
)]
async fn the_recovered_publication_exactly_once(world: &mut ClientWorld) {
    let publication = next_publication(world, false).await;
    assert_eq!(
        (publication.offset, publication.data.as_slice()),
        (Some(2), NEXT.as_bytes())
    );
    let later = world
        .subscription_events_during(Duration::from_millis(500))
        .await;
    assert!(
        !later
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{later:?}"
    );
}

#[when("the fake server pushes a delta publication before any whole publication")]
fn a_delta_before_any_whole(world: &mut ClientWorld) {
    let delta = write_fossil_delta(b"{}", NEXT.as_bytes());
    push(world, CHANNEL, publication(&delta, true, 0));
}

#[then("the subscription receives no publication")]
async fn receives_no_publication(world: &mut ClientWorld) {
    let events = world
        .subscription_events_during(Duration::from_secs(1))
        .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{events:?}"
    );
}

#[given(
    regex = r#"^a client subscribed to channel "news" on the fake server (with the fossil delta option, which did not turn delta on|without the delta option)$"#
)]
async fn subscribed_without_delta_on(world: &mut ClientWorld, how: String) {
    a_connected_client(world).await;
    let builder = world.client().subscription_builder(CHANNEL);
    let builder = if how.starts_with("with the fossil delta option") {
        builder.delta(DeltaFormat::Fossil)
    } else {
        builder
    };
    let subscription = builder.build().expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    let SubscriptionEvent::Subscribed(subscribed) =
        expect_subscription_event(world, "subscribed", false).await
    else {
        unreachable!("expect_subscription_event checked the name");
    };
    assert!(!subscribed.delta);
}

#[when("the fake server pushes a delta publication")]
fn pushes_a_delta(world: &mut ClientWorld) {
    let delta = write_fossil_delta(BASE.as_bytes(), NEXT.as_bytes());
    push(world, CHANNEL, publication(&delta, true, 0));
}

#[given(
    r#"a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on, and has received a whole publication"#
)]
async fn subscribed_with_a_whole_publication(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world.fake().answer_a_subscribe_with(json!({"delta": true}));
    // The resubscribe after the scenario's disconnect, delta on too.
    world.fake().answer_a_subscribe_with(json!({"delta": true}));
    assert!(subscribed_with_delta(world, CHANNEL).await);
    push(world, CHANNEL, publication(BASE.as_bytes(), false, 0));
    let taken = next_publication(world, false).await;
    assert_eq!(taken.data, BASE.as_bytes());
    world.delta.base = BASE.as_bytes().to_vec();
}

#[when(r#"the subscription reaches state "subscribed" again, the fake server turning delta on"#)]
async fn subscribed_again_with_delta(world: &mut ClientWorld) {
    loop {
        if let SubscriptionEvent::Subscribed(subscribed) =
            world.next_subscription_event(false).await
        {
            assert!(subscribed.delta, "{subscribed:?}");
            return;
        }
    }
}

#[when(
    "the fake server pushes a delta publication against the publication received before the disconnect"
)]
fn a_delta_against_the_old_chain(world: &mut ClientWorld) {
    let delta = write_fossil_delta(&world.delta.base, NEXT.as_bytes());
    push(world, CHANNEL, publication(&delta, true, 0));
}

#[given(
    regex = r#"^a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, whose subscription's stream has taken the publication at offset 1$"#
)]
async fn subscribed_for_a_broken_recovery(world: &mut ClientWorld) {
    subscribed_and_took_offset_1(world).await;
    // The resubscribe after the scenario's disconnect recovers offsets 2 and
    // 3, the one at 3 a delta whose checksum is wrong. Queued now: the
    // reconnect follows the disconnect within the test's 50 ms delay.
    let third = r#"{"n":3,"text":"the quick brown fox jumps over the lazy dog"}"#;
    let mut result = recoverable_delta_result(3);
    result["was_recovering"] = json!(true);
    result["recovered"] = json!(true);
    result["publications"] = json!([
        publication(NEXT.as_bytes(), false, 2),
        publication(
            &broken_delta(NEXT.as_bytes(), third.as_bytes(), "whose checksum is wrong"),
            true,
            3
        ),
    ]);
    world.fake().answer_a_subscribe_with(result);
}

#[when(
    "answers the next subscribe with delta on and the publications at offsets 2 and 3 recovered, the one at 3 a delta whose checksum is wrong"
)]
fn the_next_subscribe_is_answered_with_a_broken_recovery(_world: &mut ClientWorld) {
    // Queued by the Given: the reconnect may come before this step runs.
}

#[then(
    r#"the subscription's stream yields "subscribing", "subscribed", an "error" of kind "delta", and "subscribing" with code 2"#
)]
async fn the_stream_yields_the_failure(world: &mut ClientWorld) {
    expect_subscription_event(world, "subscribing", false).await;
    expect_subscription_event(world, "subscribed", false).await;
    subscription_emits_a_delta_error(world, 15).await;
    subscription_emits_subscribing_with(world, 2).await;
    // The next subscribe is the one after the failure: the third, after the
    // first and the reconnect's (Codex on PR #100). Counted, not noted now:
    // a slow runner may have sent it before this step takes the events.
    world.recovery.subscribes_before = 2;
}

#[given(
    regex = r"^a client connected to the fake server that has taken the server-side publication at offset 1$"
)]
async fn connected_and_took_a_server_side_publication(world: &mut ClientWorld) {
    a_connected_client(world).await;
    push(
        world,
        SERVER_CHANNEL,
        json!({"data": {"n": 1}, "offset": 1}),
    );
    loop {
        if let ClientEvent::ServerSubscription {
            event: ServerSubscriptionEvent::Publication(publication),
            ..
        } = world.next_event().await
        {
            assert_eq!(publication.offset, Some(1));
            break;
        }
    }
}

/// The server-side subscription's channel.
const SERVER_CHANNEL: &str = "alerts";

#[when(regex = r#"^the fake server pushes a delta publication at offset 2 on channel "alerts"$"#)]
fn a_server_side_delta(world: &mut ClientWorld) {
    note_commands_before_disconnect(world);
    let delta = write_fossil_delta(br#"{"n":1}"#, br#"{"n":2}"#);
    push(
        world,
        SERVER_CHANNEL,
        json!({"data": as_json_string(&delta), "offset": 2, "delta": true}),
    );
}

#[then(regex = r#"^the client emits an "error" event of kind "delta" with code (\d+)$"#)]
async fn client_emits_a_delta_error(world: &mut ClientWorld, code: u32) {
    loop {
        match world.next_event().await {
            ClientEvent::Error(error) => {
                assert_eq!(
                    (error.kind, error.code),
                    (ErrorKind::Delta, code),
                    "{error:?}"
                );
                return;
            }
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(publication),
                ..
            } => panic!("a server-side publication before the error: {publication:?}"),
            _ => {}
        }
    }
}

#[then(regex = r#"^then the client emits "connecting" with code (\d+)$"#)]
async fn then_the_client_emits_connecting_with(world: &mut ClientWorld, code: u32) {
    loop {
        match world.next_event().await {
            ClientEvent::Connecting { code: got, .. } => {
                assert_eq!(got, code);
                return;
            }
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(publication),
                ..
            } => panic!("a server-side publication before connecting: {publication:?}"),
            _ => {}
        }
    }
}

#[then("no server-side publication at offset 2 is yielded")]
async fn no_server_side_publication_at_2(world: &mut ClientWorld) {
    let events = world.events_during(Duration::from_millis(500)).await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(publication),
                ..
            } if publication.offset == Some(2)
        )),
        "{events:?}"
    );
}

#[then(regex = r#"^the next connect command asks to recover "(\w+)" from offset (\d+)$"#)]
async fn next_connect_recovers_from_offset(world: &mut ClientWorld, channel: String, offset: u64) {
    // The epoch of the fake server's grant.
    let from = StreamPosition {
        offset,
        epoch: FAKE_EPOCH.to_owned(),
    };
    assert_next_connect_recovers(world, &channel, &from).await;
}

// Block 19c: recovery.

#[given(
    "a client subscribed with the fossil delta option to a channel with history and recovery allowing fossil delta and delta publishing"
)]
async fn subscribed_to_the_recoverable_delta_channel(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world.recovery.channel = Some(DELTA_CHANNEL);
    world.delta.negotiated = Some(subscribed_with_delta(world, DELTA_CHANNEL).await);
}

#[then(
    r#"the subscription emits "subscribed" with was_recovering true and recovered true, reporting delta true"#
)]
async fn recovered_with_delta(world: &mut ClientWorld) {
    let subscribed = next_subscribed(world).await;
    assert!(
        subscribed.was_recovering && subscribed.recovered && subscribed.delta,
        "{subscribed:?}"
    );
}
