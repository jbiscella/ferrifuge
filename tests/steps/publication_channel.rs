//! Steps of Block 35c: a publication's own channel and the
//! recovered-publications flag.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use cucumber::{then, when};
use ferrifuge::{ClientEvent, ServerSubscriptionEvent};
use serde_json::json;

use super::ClientWorld;
use super::recovery::{FAKE_EPOCH, assert_next_subscribe_recovers, next_subscribed, position};
use super::subscriptions::next_publication;

const CHANNEL: &str = "news";

/// Pushes a publication for [`CHANNEL`] at `offset`, with `own` as its own
/// channel when set, and takes it from the subscription's stream.
async fn push_and_take(world: &mut ClientWorld, offset: u64, own: Option<&str>) -> Option<String> {
    let mut publication = json!({"data": {"n": offset}, "offset": offset});
    if let Some(own) = own {
        publication["channel"] = json!(own);
    }
    world
        .fake()
        .send(&json!({"push": {"channel": CHANNEL, "pub": publication}}).to_string());
    next_publication(world, false).await.channel
}

#[when(
    r#"the fake server pushes a publication for channel "news" whose own channel is "news.sport", then one with no own channel"#
)]
async fn pushes_one_with_and_one_without(world: &mut ClientWorld) {
    let first = push_and_take(world, 1, Some("news.sport")).await;
    let second = push_and_take(world, 2, None).await;
    world.publication_channels = vec![first, second];
}

#[then(
    r#"the subscription receives a publication whose channel is "news.sport", then one with no channel"#
)]
fn receives_with_and_without(world: &mut ClientWorld) {
    assert_eq!(
        world.publication_channels,
        [Some("news.sport".to_owned()), None]
    );
}

/// Both are taken, so the first moves the position and the second would.
#[when(
    r#"the fake server pushes a publication for channel "news" at offset 1, then one whose own channel is "news.sport" at offset 5"#
)]
async fn pushes_at_1_then_another_channel_at_5(world: &mut ClientWorld) {
    assert_eq!(push_and_take(world, 1, None).await, None);
    assert_eq!(
        push_and_take(world, 5, Some("news.sport")).await.as_deref(),
        Some("news.sport")
    );
    world.recovery.position = Some(position(1, FAKE_EPOCH));
}

#[then(r#"the next subscribe command for "news" asks to recover from offset 1 in the same epoch"#)]
async fn recovers_from_offset_1(world: &mut ClientWorld) {
    assert_next_subscribe_recovers(world).await;
}

#[then(
    regex = r#"^the subscription emits "subscribed" again with has_recovered_publications (true|false)$"#
)]
async fn subscribed_again_with_the_flag(world: &mut ClientWorld, flag: String) {
    let subscribed = next_subscribed(world).await;
    assert!(subscribed.was_recovering, "{subscribed:?}");
    assert_eq!(
        subscribed.has_recovered_publications,
        flag == "true",
        "{subscribed:?}"
    );
}

#[then(
    regex = r#"^the client emits a server-side "subscribed" event for channel "(\w+)" with has_recovered_publications (true|false)$"#
)]
async fn server_side_subscribed_with_the_flag(
    world: &mut ClientWorld,
    channel: String,
    flag: String,
) {
    let subscribed = loop {
        if let ClientEvent::ServerSubscription {
            channel: got,
            event: ServerSubscriptionEvent::Subscribed(subscribed),
        } = world.next_event().await
            && got == channel
        {
            break subscribed;
        }
    };
    assert_eq!(
        subscribed.has_recovered_publications,
        flag == "true",
        "{subscribed:?}"
    );
}
