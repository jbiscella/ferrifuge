//! Steps of Block 35a: tag filters.

use cucumber::{given, then, when};
use ferrifuge::{DeltaFormat, Protocol, SubscriptionError, SubscriptionEvent, TagFilter};
use serde_json::{Value, json};

use super::subscriptions::{expect_subscription_event, publish};
use super::{ClientWorld, QUIET_PERIOD};

/// The channel of the @A scenario: allowed tag filters, as every channel
/// without namespace of the @A server.
const FILTERED: &str = "filtered";

/// A Block 35a scenario's filter and the outcome of building with it.
#[derive(Debug, Default)]
pub(crate) struct TagFilters {
    filter: Option<TagFilter>,
    delta: bool,
    built: Option<Result<(), SubscriptionError>>,
}

/// `key eq value` or `key in a, b`, as the scenarios write them.
fn filter(text: &str) -> (TagFilter, Value) {
    if let Some((key, values)) = text.split_once(" in ") {
        let values: Vec<String> = values.split(", ").map(str::to_owned).collect();
        let json = json!({"key": key, "cmp": "in", "vals": values});
        return (TagFilter::is_in(key, values), json);
    }
    let (key, value) = text.split_once(" eq ").expect("key eq value");
    (
        TagFilter::eq(key, value),
        json!({"key": key, "cmp": "eq", "val": value}),
    )
}

/// Subscribes the scenario's client to `channel` with `filter`, up to
/// `subscribed`.
async fn subscribe_with(world: &mut ClientWorld, channel: &str, filter: TagFilter) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .tag_filter(filter)
        .build()
        .expect("a valid filter");
    world.track_subscription(subscription);
    world.subscription().subscribe();
    expect_subscription_event(world, "subscribing", false).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given(
    regex = r"^a client configured with the (JSON|Protobuf) protocol, subscribed to a channel allowing tag filters with the filter (.+)$"
)]
async fn a_client_subscribed_with_a_filter(
    world: &mut ClientWorld,
    protocol: String,
    text: String,
) {
    world.protocol = if protocol == "JSON" {
        Protocol::Json
    } else {
        Protocol::Protobuf
    };
    super::connection::a_connected_client(world).await;
    subscribe_with(world, FILTERED, filter(&text).0).await;
}

#[when(
    regex = r#"^the server API publishes to the channel with tags \{"ticker":"BTC"\}, then \{"ticker":"XRP"\}, then \{"ticker":"ETH"\}$"#
)]
async fn publishes_three_tagged(world: &mut ClientWorld) {
    for (n, ticker) in ["BTC", "XRP", "ETH"].into_iter().enumerate() {
        let data = json!({"n": n}).to_string();
        publish(world, FILTERED, &data, Some(json!({"ticker": ticker}))).await;
    }
}

#[then("the subscription receives exactly the publications tagged BTC and ETH, in order")]
async fn receives_btc_and_eth(world: &mut ClientWorld) {
    let mut tickers = Vec::new();
    while tickers.len() < 2 {
        if let SubscriptionEvent::Publication(publication) =
            world.next_subscription_event(false).await
        {
            tickers.push(publication.tags.get("ticker").cloned().unwrap_or_default());
        }
    }
    assert_eq!(tickers, ["BTC", "ETH"]);
    let later = world.subscription_events_during(QUIET_PERIOD).await;
    assert!(
        !later
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{later:?}"
    );
}

#[when(regex = r#"^a subscription to channel "(\w+)" with the filter (.+) is subscribed$"#)]
async fn a_filtered_subscription(world: &mut ClientWorld, channel: String, text: String) {
    subscribe_with(world, &channel, filter(&text).0).await;
}

#[when(regex = r"^the tag filter of the subscription is set to (.+)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_filter_is_set(world: &mut ClientWorld, text: String) {
    world
        .subscription()
        .set_tag_filter(Some(filter(&text).0))
        .expect("a valid filter");
}

/// The `tf` of every subscribe command for `channel` the fake server got.
fn subscribe_filters(world: &ClientWorld, channel: &str) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command["subscribe"]["channel"] == channel)
        .map(|command| command["subscribe"]["tf"].clone())
        .collect()
}

#[then(
    regex = r#"^the fake server received 2 subscribe commands for "(\w+)", each carrying the filter (.+)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn two_subscribes_carrying(world: &mut ClientWorld, channel: String, text: String) {
    let expected = filter(&text).1;
    assert_eq!(
        subscribe_filters(world, &channel),
        [expected.clone(), expected]
    );
}

#[then(
    regex = r#"^the fake server received exactly 2 subscribe commands for "(\w+)", the first carrying the filter (.+) and the second (.+)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn two_subscribes_then(world: &mut ClientWorld, channel: String, first: String, second: String) {
    assert_eq!(
        subscribe_filters(world, &channel),
        [filter(&first).1, filter(&second).1]
    );
}

#[given("subscription options with the fossil delta option and a tag filter")]
fn options_with_delta_and_a_filter(world: &mut ClientWorld) {
    world.tag_filters.filter = Some(TagFilter::eq("ticker", "BTC"));
    world.tag_filters.delta = true;
}

#[given(
    regex = r#"^subscription options with the tag filter (a set comparison on "ticker" with no value|an equality with an empty key)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn options_with_an_invalid_filter(world: &mut ClientWorld, which: String) {
    world.tag_filters.filter = Some(if which.starts_with("a set comparison") {
        TagFilter::is_in("ticker", Vec::<String>::new())
    } else {
        TagFilter::eq("", "BTC")
    });
}

#[when("the subscription is built")]
fn the_subscription_is_built(world: &mut ClientWorld) {
    world.new_client(super::UNREACHABLE_ENDPOINT.to_owned());
    let mut builder = world.client().subscription_builder("news");
    if world.tag_filters.delta {
        builder = builder.delta(DeltaFormat::Fossil);
    }
    let filter = world.tag_filters.filter.clone().expect("a filter");
    world.tag_filters.built = Some(builder.tag_filter(filter).build().map(|_| ()));
}

#[then("building fails because a tag filter cannot be combined with delta")]
fn fails_with_delta(world: &mut ClientWorld) {
    assert_eq!(
        world.tag_filters.built,
        Some(Err(SubscriptionError::TagFilterWithDelta))
    );
}

#[then("setting a tag filter on a subscription with the fossil delta option fails the same way")]
fn setting_fails_with_delta(world: &mut ClientWorld) {
    let subscription = world
        .client()
        .subscription_builder("delta")
        .delta(DeltaFormat::Fossil)
        .build()
        .expect("a delta subscription");
    assert_eq!(
        subscription.set_tag_filter(Some(TagFilter::eq("ticker", "BTC"))),
        Err(SubscriptionError::TagFilterWithDelta)
    );
}

#[then("building fails because the tag filter is invalid")]
fn fails_invalid(world: &mut ClientWorld) {
    assert!(
        matches!(
            world.tag_filters.built,
            Some(Err(SubscriptionError::InvalidTagFilter { .. }))
        ),
        "{:?}",
        world.tag_filters.built
    );
}

#[then("setting that tag filter on a built subscription fails the same way")]
fn setting_fails_invalid(world: &mut ClientWorld) {
    let subscription = world
        .client()
        .subscription_builder("plain")
        .build()
        .expect("a subscription");
    let filter = world.tag_filters.filter.clone();
    assert!(
        matches!(
            subscription.set_tag_filter(filter),
            Err(SubscriptionError::InvalidTagFilter { .. })
        ),
        "{:?}",
        world.tag_filters.filter
    );
}
