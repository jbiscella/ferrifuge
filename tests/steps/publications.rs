//! Steps of Block 39: a publications-only stream.
//!
//! The scenario's publications stream sits in `world.delivery` as Block
//! 15's subscription streams do, each publication wrapped back into
//! `SubscriptionEvent::Publication`, so that Block 15's steps read it; the
//! wrapping takes nothing more than the stream itself does.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{Subscription, SubscriptionEvent, SubscriptionState};
use futures_util::StreamExt as _;
use serde_json::json;

use super::connection::a_connected_client;
use super::delivery::{
    QUIET, next_of, next_subscription_event, offset, offsets, subscribed, wait_until,
};
use super::{ClientWorld, SubscriptionEvents, recorded};

/// `subscription`'s publications stream, its items wrapped as events.
fn publications_of(subscription: &Subscription) -> SubscriptionEvents {
    Box::pin(
        subscription
            .publications()
            .map(SubscriptionEvent::Publication),
    )
}

/// The offset of a wrapped publication, or what else the stream yielded.
fn offset_of(event: &SubscriptionEvent) -> u64 {
    match event {
        SubscriptionEvent::Publication(publication) => offset(publication),
        event => panic!("a publications stream yielded {event:?}"),
    }
}

async fn wait_subscribed(subscription: &Subscription) {
    wait_until("subscribed", || {
        subscription.state() == SubscriptionState::Subscribed
    })
    .await;
}

#[given(
    regex = r#"^a client connected to the fake server, with a subscription to channel "(\w+)"( with the join_leave option)? whose publications stream was taken(?: before subscribe is called)?( and is read| and is not read)?$"#
)]
async fn a_subscription_with_its_publications_stream(
    world: &mut ClientWorld,
    channel: String,
    join_leave: String,
    read: String,
) {
    a_connected_client(world).await;
    let subscription = world
        .client()
        .subscription_builder(channel)
        .join_leave(!join_leave.is_empty())
        .build()
        .expect("no subscription to the channel yet");
    let stream = publications_of(&subscription);
    world.delivery.subscription_events = Some(if read == " and is read" {
        recorded(stream)
    } else {
        stream
    });
    world.subscription = Some(subscription);
}

#[given("the subscription is subscribed")]
async fn the_subscription_is_subscribed(world: &mut ClientWorld) {
    let subscription = world.subscription().clone();
    subscription.subscribe();
    wait_subscribed(&subscription).await;
}

#[when(
    regex = r#"^the fake server sends a publication at offset (\d+), a join, a leave, then a publication at offset (\d+) to "(\w+)"$"#
)]
fn sends_publications_and_presence(
    world: &mut ClientWorld,
    first: u64,
    last: u64,
    channel: String,
) {
    let fake = world.fake();
    fake.publish(&channel, first);
    for kind in ["join", "leave"] {
        fake.send(
            &json!({"push": {"channel": channel, kind: {"info": {"client": "c2", "user": "u2"}}}})
                .to_string(),
        );
    }
    fake.publish(&channel, last);
}

#[when(regex = r#"^the client has resubscribed to "(\w+)" on the new connection$"#)]
async fn resubscribed(world: &mut ClientWorld, channel: String) {
    let fake = world.fake();
    let subscribes = || {
        fake.received()
            .iter()
            .filter(|command| command["subscribe"]["channel"] == channel.as_str())
            .count()
    };
    wait_until("a second subscribe command", || subscribes() >= 2).await;
    wait_subscribed(world.subscription()).await;
}

#[when(regex = r#"^the fake server sends a publication at offset (\d+) to "(\w+)"$"#)]
fn sends_a_publication(world: &mut ClientWorld, at: u64, channel: String) {
    world.fake().publish(&channel, at);
}

#[then(
    regex = r"^the publications stream has yielded exactly the publications at offsets ([\d, and]+), in order$"
)]
async fn has_yielded_exactly(world: &mut ClientWorld, list: String) {
    let expected = offsets(&list);
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the publications stream");
    let mut yielded = Vec::new();
    for _ in &expected {
        yielded.push(offset_of(&next_of(stream).await.expect("not ended")));
    }
    assert_eq!(yielded, expected);
    let more = tokio::time::timeout(QUIET, stream.next()).await;
    assert!(more.is_err(), "it yielded more: {more:?}");
}

#[given(
    regex = r#"^a client connected to the fake server, subscribed to channel "(\w+)", whose subscription's stream is read$"#
)]
async fn a_client_with_a_read_subscription(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    let (subscription, events) = subscribed(world, &channel).await;
    world
        .delivery
        .channels
        .insert(channel, (subscription, Some(recorded(events))));
}

#[given(
    regex = r#"^a subscription to channel "(\w+)" whose publications stream was taken before subscribe is called and is not read$"#
)]
fn a_subscription_whose_publications_are_not_read(world: &mut ClientWorld, channel: String) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .build()
        .expect("no subscription to the channel yet");
    world.delivery.subscription_events = Some(publications_of(&subscription));
    world.subscription = Some(subscription);
}

#[when(regex = r#"^the application subscribes to "(\w+)"$"#)]
fn the_application_subscribes(world: &mut ClientWorld, channel: String) {
    assert_eq!(world.subscription().channel(), channel);
    world.subscription().subscribe();
}

#[when(
    regex = r#"^the fake server has answered the subscribe command for "(\w+)", then sent a publication to "(\w+)"$"#
)]
async fn answered_then_sent(world: &mut ClientWorld, channel: String, other: String) {
    let fake = world.fake();
    wait_until("the subscribe answered", || {
        fake.subscribe_replies()
            .iter()
            .any(|(of, _)| *of == channel)
    })
    .await;
    fake.publish(&other, 1);
}

#[then(regex = r#"^1 s later the stream of "(\w+)" has yielded no publication$"#)]
async fn no_publication_a_second_later(world: &mut ClientWorld, channel: String) {
    let stream = world
        .delivery
        .channels
        .get_mut(&channel)
        .and_then(|(_, events)| events.as_mut())
        .expect("its stream");
    let item = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
    assert!(item.is_err(), "it yielded {item:?}");
}

#[then(
    regex = r#"^when the application reads the publications stream of "(\w+)" for 100 ms, it yields nothing$"#
)]
async fn the_publications_stream_yields_nothing(world: &mut ClientWorld, channel: String) {
    assert_eq!(world.subscription().channel(), channel);
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the publications stream");
    let item = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(item.is_err(), "it yielded {item:?}");
}

#[then(regex = r#"^then the stream of "(\w+)" yields that publication$"#)]
async fn then_the_stream_yields(world: &mut ClientWorld, channel: String) {
    let stream = world
        .delivery
        .channels
        .get_mut(&channel)
        .and_then(|(_, events)| events.as_mut())
        .expect("its stream");
    let event = next_of(stream).await.expect("not ended");
    assert!(
        matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == 1),
        "{event:?}"
    );
}

#[given(
    regex = r#"^a client subscribed to recoverable channel "(\w+)" on the fake server, whose publications stream was taken before subscribe is called$"#
)]
async fn a_recoverable_subscription(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    world.fake().recover_from_history(&channel);
    a_subscription_whose_publications_are_not_read(world, channel);
    let subscription = world.subscription().clone();
    subscription.subscribe();
    wait_subscribed(&subscription).await;
}

#[given(
    regex = r"^the application has taken only the publication at offset (\d+) from the publications stream$"
)]
async fn taken_only_from_publications(world: &mut ClientWorld, expected: u64) {
    let event = next_subscription_event(world).await;
    assert_eq!(offset_of(&event), expected);
}

#[then(
    regex = r"^the publications stream yields the publications? at offsets? ([\d, and]+)(?:, in order)?$"
)]
async fn the_publications_stream_yields(world: &mut ClientWorld, list: String) {
    let mut yielded = Vec::new();
    for _ in offsets(&list) {
        yielded.push(match world.delivery.second_subscription_events.as_mut() {
            // After a take-over, the new stream is the publications stream.
            Some(stream) => offset_of(&next_of(stream).await.expect("not ended")),
            None => offset_of(&next_subscription_event(world).await),
        });
    }
    assert_eq!(yielded, offsets(&list));
}

#[when("the application calls the subscription's publications()")]
fn calls_publications(world: &mut ClientWorld) {
    world.delivery.second_subscription_events = Some(publications_of(world.subscription()));
}

#[when("the application calls the subscription's events()")]
fn calls_events(world: &mut ClientWorld) {
    world.delivery.second_subscription_events = Some(Box::pin(world.subscription().events()));
}

#[then("the publications stream's next item is its end")]
async fn the_publications_stream_ends(world: &mut ClientWorld) {
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the publications stream");
    let item = next_of(stream).await;
    assert!(item.is_none(), "it yielded {item:?}");
}

#[then(
    regex = r#"^the new stream yields "subscribing", "subscribed", and the publications at offsets ([\d, and]+), in order$"#
)]
async fn the_new_stream_yields_all(world: &mut ClientWorld, list: String) {
    let stream = world
        .delivery
        .second_subscription_events
        .as_mut()
        .expect("the new stream");
    let mut seen = Vec::new();
    for _ in 0..2 + offsets(&list).len() {
        seen.push(match next_of(stream).await.expect("not ended") {
            SubscriptionEvent::Publication(publication) => offset(&publication).to_string(),
            event => event.name().to_owned(),
        });
    }
    let mut expected = vec!["subscribing".to_owned(), "subscribed".to_owned()];
    expected.extend(offsets(&list).iter().map(u64::to_string));
    assert_eq!(seen, expected);
}

#[given(
    regex = r#"^a client subscribed to channel "(\w+)" on the fake server, whose publications stream was taken once it was subscribed and is not read$"#
)]
async fn a_subscription_whose_stream_is_taken_once_subscribed(
    world: &mut ClientWorld,
    channel: String,
) {
    a_connected_client(world).await;
    let subscription = world
        .client()
        .subscription_builder(channel)
        .build()
        .expect("no subscription to the channel yet");
    subscription.subscribe();
    wait_subscribed(&subscription).await;
    world.delivery.subscription_events = Some(publications_of(&subscription));
    world.subscription = Some(subscription);
}

#[when(
    regex = r#"^the fake server answers the subscribe command, then sends a publication at offset (\d+) to "(\w+)"$"#
)]
async fn answers_then_sends(world: &mut ClientWorld, at: u64, channel: String) {
    wait_subscribed(world.subscription()).await;
    world.fake().publish(&channel, at);
}

#[then(regex = r"^the publications stream yields the publication at offset (\d+), and no other$")]
async fn yields_only(world: &mut ClientWorld, expected: u64) {
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the publications stream");
    assert_eq!(
        offset_of(&next_of(stream).await.expect("not ended")),
        expected
    );
    let more = tokio::time::timeout(QUIET, stream.next()).await;
    assert!(more.is_err(), "it yielded more: {more:?}");
}

#[when("the subscription is removed")]
fn the_subscription_is_removed(world: &mut ClientWorld) {
    let subscription = world.subscription().clone();
    world.client().remove_subscription(&subscription);
}

#[when("the client is closed")]
async fn the_client_is_closed(world: &mut ClientWorld) {
    world.client().close().await;
}
