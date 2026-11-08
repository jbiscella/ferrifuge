//! Steps of Block 35b: channel compaction. @D scenarios give numeric
//! channel ids from the fake server; the @A scenario checks that a server
//! without compaction ignores the request.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, ServerSubscriptionEvent, SubscriptionEvent};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::subscriptions::{expect_subscription_event, subscribe};
use super::{ClientWorld, QUIET_PERIOD};

const CHANNEL: &str = "news";

/// Subscribes the scenario's client to `channel` with channel compaction,
/// and with join and leave pushes when `join_leave` is set, up to
/// `subscribed`.
async fn subscribe_compacted(world: &mut ClientWorld, channel: &str, join_leave: bool) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .channel_compaction(true)
        .join_leave(join_leave)
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[when(regex = r#"^a subscription to channel "(\w+)" with channel compaction is subscribed$"#)]
async fn a_compacted_subscription(world: &mut ClientWorld, channel: String) {
    subscribe_compacted(world, &channel, false).await;
}

#[then(
    regex = r#"^the fake server received (\d+) subscribe commands for "(\w+)", each requesting channel compaction$"#
)]
fn subscribes_requesting_compaction(world: &mut ClientWorld, count: usize, channel: String) {
    let flags: Vec<Value> = world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command["subscribe"]["channel"] == channel.as_str())
        .map(|command| command["subscribe"]["flag"].clone())
        .collect();
    assert_eq!(flags.len(), count, "{flags:?}");
    assert!(
        flags
            .iter()
            .all(|flag| flag.as_i64().is_some_and(|flag| flag & 1 == 1)),
        "{flags:?}"
    );
}

/// The fake server answers the first subscribe with id 7 and, for the
/// scenario that reconnects, the resubscribe with id 9.
#[given(
    regex = r#"^a client subscribed to channel "news" on the fake server with channel compaction( and join and leave pushes)?, the fake server giving it id 7$"#
)]
async fn subscribed_with_id_7(world: &mut ClientWorld, join_leave: String) {
    a_connected_client(world).await;
    world.fake().answer_a_subscribe_with(json!({"id": 7}));
    world.fake().answer_a_subscribe_with(json!({"id": 9}));
    subscribe_compacted(world, CHANNEL, !join_leave.is_empty()).await;
}

/// A push of `kind` naming its subscription by `id` alone, its data or
/// client naming the id too; a publication's data is kept for Block 29's
/// "the subscription receives that publication".
fn push_by_id(world: &mut ClientWorld, kind: &str, id: u64) {
    let body = match kind {
        "publication" => {
            let data = json!({"id": id});
            world.last_raw_data = Some(data.to_string().into_bytes());
            json!({"pub": {"data": data}})
        }
        kind => json!({kind: {"info": {"client": format!("c{id}"), "user": "u"}}}),
    };
    let mut push = json!({"id": id});
    push.as_object_mut()
        .unwrap()
        .extend(body.as_object().unwrap().clone());
    world.fake().send(&json!({ "push": push }).to_string());
}

#[when(regex = r"^the fake server pushes a (publication|join|leave) with id (\d+) and no channel$")]
fn pushes_one_by_id(world: &mut ClientWorld, kind: String, id: u64) {
    push_by_id(world, &kind, id);
}

#[when(
    regex = r"^the fake server pushes a publication with id (\d+) and no channel, then one with id (\d+) and no channel$"
)]
fn pushes_two_by_id(world: &mut ClientWorld, first: u64, second: u64) {
    push_by_id(world, "publication", first);
    push_by_id(world, "publication", second);
}

/// "the subscription receives that publication" is Block 29's step.
#[then(regex = r"^the subscription receives that (join|leave)$")]
async fn receives_that(world: &mut ClientWorld, kind: String) {
    let event = loop {
        if let event @ (SubscriptionEvent::Publication(_)
        | SubscriptionEvent::Join(_)
        | SubscriptionEvent::Leave(_)) = world.next_subscription_event(false).await
        {
            break event;
        }
    };
    match (kind.as_str(), &event) {
        ("join", SubscriptionEvent::Join(info)) | ("leave", SubscriptionEvent::Leave(info)) => {
            assert_eq!(info.client, "c7");
        }
        _ => panic!("expected a {kind}, got {event:?}"),
    }
}

#[then(
    regex = r"^the subscription receives exactly one publication, the one pushed with id (\d+)$"
)]
async fn receives_only_the_one(world: &mut ClientWorld, id: u64) {
    let publication = loop {
        if let SubscriptionEvent::Publication(publication) =
            world.next_subscription_event(false).await
        {
            break publication;
        }
    };
    assert_eq!(publication.data, json!({"id": id}).to_string().as_bytes());
    let later = world.subscription_events_during(QUIET_PERIOD).await;
    assert!(
        !later
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{later:?}"
    );
}

#[then("the client receives no server-side publication")]
async fn no_server_side_publication(world: &mut ClientWorld) {
    let events = world.events_during(QUIET_PERIOD).await;
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

/// The fake server's answer with id 9 was queued by the Given step, before
/// the reconnect could send the resubscribe.
#[when(r#"the subscription reaches state "subscribed" again, the fake server giving it id 9"#)]
async fn subscribed_again_with_id_9(world: &mut ClientWorld) {
    expect_subscription_event(world, "subscribing", false).await;
    expect_subscription_event(world, "subscribed", false).await;
    let ids: Vec<Value> = world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command["subscribe"]["channel"] == CHANNEL)
        .map(|command| command["id"].clone())
        .collect();
    assert_eq!(ids.len(), 2, "{ids:?}");
}

#[given(regex = r#"^a client subscribed to channel "(\w+)" with channel compaction$"#)]
async fn subscribed_on_a_server_without_compaction(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    subscribe_compacted(world, &channel, false).await;
}
