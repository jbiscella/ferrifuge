//! Block 15: event delivery and backpressure. These steps read the streams
//! the way an application does, one event at a time, instead of through
//! the world's recorded streams, which take every event as it comes.

// Cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::collections::HashMap;
use std::task::Poll;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{
    Client, ClientEvent, Publication, ServerSubscriptionEvent, State, Subscription,
    SubscriptionEvent,
};
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::{ClientWorld, EVENT_TIMEOUT, Events, SubscriptionEvents};
use crate::support::fake_server::HISTORY_EPOCH;

/// How long a step watches a stream for events that must not come.
const QUIET: Duration = Duration::from_millis(500);

/// The streams of a Block 15 scenario, read by its steps.
#[derive(Default)]
pub(crate) struct Delivery {
    /// The client's stream, taken before `connect()`.
    client_events: Option<Events>,
    /// The client's stream from a second `events()` call.
    second_client_events: Option<Events>,
    /// The stream of the scenario's subscription.
    subscription_events: Option<SubscriptionEvents>,
    /// The subscription's stream from a second `events()` call.
    second_subscription_events: Option<SubscriptionEvents>,
    /// Further subscriptions, with their streams, by channel.
    channels: HashMap<String, (Subscription, Option<SubscriptionEvents>)>,
    /// Offsets the fake server pushed, and those the application took.
    sent: Vec<u64>,
    taken: Vec<u64>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("sent", &self.sent)
            .field("taken", &self.taken)
            .finish_non_exhaustive()
    }
}

fn offsets(list: &str) -> Vec<u64> {
    list.split([',', ' '])
        .filter_map(|word| word.parse().ok())
        .collect()
}

fn offset(publication: &Publication) -> u64 {
    publication.offset.expect("the fake server sends offsets")
}

/// Waits for `ready` to hold, or fails with `what`.
async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(EVENT_TIMEOUT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{what} within {EVENT_TIMEOUT:?}");
}

/// The next item of `stream`, `None` once it ended.
async fn next_of<E>(stream: &mut (impl futures_util::Stream<Item = E> + Unpin)) -> Option<E> {
    tokio::time::timeout(EVENT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("no item within {EVENT_TIMEOUT:?}"))
}

/// The next event of the scenario's subscription stream, recording the
/// publications the application takes.
async fn next_subscription_event(world: &mut ClientWorld) -> SubscriptionEvent {
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    let event = next_of(stream).await.expect("the stream has not ended");
    if let SubscriptionEvent::Publication(publication) = &event {
        world.delivery.taken.push(offset(publication));
    }
    event
}

/// The next event of the client's stream taken before `connect()`.
async fn next_client_event(world: &mut ClientWorld) -> ClientEvent {
    let stream = world
        .delivery
        .client_events
        .as_mut()
        .expect("the client's stream");
    next_of(stream).await.expect("the stream has not ended")
}

/// Builds the scenario's client without the world's recorded stream, and
/// takes its stream, as an application does before `connect()`.
fn a_client_with_its_stream(world: &mut ClientWorld) {
    let client = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .build()
        .expect("a valid client configuration");
    world.delivery.client_events = Some(Box::pin(client.events()));
    world.client = Some(client);
}

/// Subscribes to `channel`, reading its stream up to `subscribed`.
async fn subscribed(world: &mut ClientWorld, channel: &str) -> (Subscription, SubscriptionEvents) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .build()
        .expect("no subscription to the channel yet");
    let mut events: SubscriptionEvents = Box::pin(subscription.events());
    subscription.subscribe();
    loop {
        match next_of(&mut events)
            .await
            .expect("the stream has not ended")
        {
            SubscriptionEvent::Subscribed(_) => break,
            SubscriptionEvent::Subscribing { .. } => {}
            event => panic!("{event:?} before subscribed"),
        }
    }
    (subscription, events)
}

#[given(
    regex = r#"^a client subscribed to (recoverable )?channel "(\w+)" on the fake server, whose subscription's stream has been read up to "subscribed"(?: and is not read after)?$"#
)]
async fn a_subscription_read_up_to_subscribed(
    world: &mut ClientWorld,
    recoverable: String,
    channel: String,
) {
    a_connected_client(world).await;
    if !recoverable.is_empty() {
        world.fake().recover_from_history(&channel);
    }
    let (subscription, events) = subscribed(world, &channel).await;
    world.subscription = Some(subscription);
    world.delivery.subscription_events = Some(events);
}

#[given(regex = r#"^a client subscribed to channels "(\w+)" and "(\w+)" on the fake server$"#)]
async fn a_client_subscribed_to_two_channels(
    world: &mut ClientWorld,
    first: String,
    second: String,
) {
    a_connected_client(world).await;
    for channel in [first, second] {
        let (subscription, events) = subscribed(world, &channel).await;
        world
            .delivery
            .channels
            .insert(channel, (subscription, Some(events)));
    }
}

#[given(
    regex = r#"^the fake server grants a recoverable server-side subscription to channel "(\w+)" at offset 0 at connect$"#
)]
async fn a_recoverable_server_side_subscription(world: &mut ClientWorld, channel: String) {
    world.fake().grant_server_subscriptions(json!({
        channel.clone(): {"recoverable": true, "positioned": true, "epoch": HISTORY_EPOCH, "offset": 0},
    }));
    a_client_with_its_stream(world);
    world.client().connect();
    loop {
        if let ClientEvent::ServerSubscription {
            channel: of,
            event: ServerSubscriptionEvent::Subscribed(_),
        } = next_client_event(world).await
            && of == channel
        {
            break;
        }
    }
}

#[given(
    "a client whose stream was taken before connect is called and is not read, connected to the fake server"
)]
async fn a_client_whose_stream_is_not_read(world: &mut ClientWorld) {
    a_client_with_its_stream(world);
    world.client().connect();
    let client = world.client();
    wait_until("connected", || client.state() == State::Connected).await;
}

#[given(regex = r#"^the fake server has sent publications at offsets ([\d, and]+) to "(\w+)"$"#)]
fn the_fake_server_has_sent(world: &mut ClientWorld, list: String, channel: String) {
    for offset in offsets(&list) {
        world.fake().publish(&channel, offset);
        world.delivery.sent.push(offset);
    }
}

#[given(
    regex = r"^the application has taken only the publication at offset (\d+) from the subscription's stream$"
)]
async fn taken_only(world: &mut ClientWorld, expected: u64) {
    let event = next_subscription_event(world).await;
    assert!(
        matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == expected),
        "{event:?}"
    );
}

#[given(
    regex = r"^the application has read the client's stream up to the server-side publication at offset (\d+), and no further$"
)]
async fn client_stream_read_up_to(world: &mut ClientWorld, expected: u64) {
    loop {
        if let ClientEvent::ServerSubscription {
            event: ServerSubscriptionEvent::Publication(publication),
            ..
        } = next_client_event(world).await
        {
            assert_eq!(offset(&publication), expected, "{publication:?}");
            return;
        }
    }
}

#[given(
    regex = r"^the client holds (?:the publications at offsets ([\d, and]+)|them) not yet taken$"
)]
async fn the_subscription_holds(world: &mut ClientWorld, list: String) {
    let expected = if list.is_empty() {
        world.delivery.sent.len() - world.delivery.taken.len()
    } else {
        offsets(&list).len()
    };
    let subscription = world.subscription();
    wait_until(&format!("{expected} events held"), || {
        subscription.held_events() == expected
    })
    .await;
}

#[given(
    regex = r"^the client holds the server-side publications at offsets ([\d, and]+) not yet taken$"
)]
async fn the_client_holds(world: &mut ClientWorld, list: String) {
    let expected = offsets(&list).len();
    let client = world.client();
    wait_until(&format!("{expected} events held"), || {
        client.held_events() == expected
    })
    .await;
}

#[given(regex = r#"^the client holds a publication to "(\w+)" not yet taken$"#)]
async fn holds_a_publication(world: &mut ClientWorld, channel: String) {
    world.fake().publish(&channel, 1);
    world.delivery.sent.push(1);
    let subscription = world.subscription();
    wait_until("1 event held", || subscription.held_events() == 1).await;
}

/// Polls the stream once with nothing to take, then drops that read, as
/// a `select!` branch losing the race would.
#[given("the application has started reading the subscription's stream")]
async fn started_reading(world: &mut ClientWorld) {
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    let mut read = stream.next();
    let polled = std::future::poll_fn(|cx| Poll::Ready(read.poll_unpin(cx))).await;
    assert!(polled.is_pending(), "{polled:?}");
}

#[when("the application cancels that read before polling it again")]
fn cancels_the_read(_world: &mut ClientWorld) {
    // The read started by the Given step was dropped when that step ended.
}

#[when(regex = r"^the application calls the (client|subscription)'s events\(\) again$")]
fn calls_events_again(world: &mut ClientWorld, of: String) {
    if of == "client" {
        world.delivery.second_client_events = Some(Box::pin(world.client().events()));
    } else {
        world.delivery.second_subscription_events = Some(Box::pin(world.subscription().events()));
    }
}

#[given(
    regex = r#"^the application dropped the stream of "(\w+)" and reads the stream of "(\w+)"$"#
)]
fn dropped_one_stream(world: &mut ClientWorld, dropped: String, read: String) {
    let channels = &mut world.delivery.channels;
    channels.get_mut(&dropped).expect("that subscription").1 = None;
    assert!(channels[&read].1.is_some());
}

#[when(regex = r#"^the fake server sends (\d+) publications to "(\w+)", then one to "(\w+)"$"#)]
fn sends_many_then_one(world: &mut ClientWorld, count: u64, many: String, one: String) {
    for offset in 1..=count {
        world.fake().publish(&many, offset);
    }
    world.fake().publish(&one, 1);
}

#[when(
    regex = r"^the fake server answers the subscribe command with the publications at offsets ([\d, and]+) recovered$"
)]
async fn answers_with_recovered(world: &mut ClientWorld, list: String) {
    let channel = world.subscription().channel().to_owned();
    let fake = world.fake();
    let recovering = || {
        fake.subscribe_replies()
            .into_iter()
            .rev()
            .find(|(of, result)| *of == channel && result["was_recovering"] == true)
    };
    wait_until("a recovering subscribe answered", || recovering().is_some()).await;
    let (_, result) = recovering().expect("the reply");
    let recovered: Vec<u64> = result["publications"]
        .as_array()
        .map(|publications| {
            publications
                .iter()
                .filter_map(|publication| publication["offset"].as_u64())
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(recovered, offsets(&list), "{result}");
}

/// The last command of `kind` the fake server received.
fn last_command(world: &ClientWorld, kind: &str, channel: Option<&str>) -> Value {
    world
        .fake()
        .received()
        .into_iter()
        .rev()
        .find(|command| {
            command[kind].is_object()
                && channel.is_none_or(|channel| command[kind]["channel"] == channel)
        })
        .unwrap_or_else(|| panic!("no {kind} command"))
}

#[then(regex = r"^that subscribe command asked to recover from offset (\d+)$")]
fn subscribe_recovered_from(world: &mut ClientWorld, expected: u64) {
    let channel = world.subscription().channel().to_owned();
    let subscribe = &last_command(world, "subscribe", Some(&channel))["subscribe"];
    assert_eq!(
        (&subscribe["recover"], subscribe["offset"].as_u64()),
        (&json!(true), Some(expected)),
        "{subscribe}"
    );
}

#[then(regex = r#"^that connect command asks to recover "(\w+)" from offset (\d+)$"#)]
async fn connect_recovers_from(world: &mut ClientWorld, channel: String, expected: u64) {
    let fake = world.fake();
    let connects = || {
        fake.received()
            .into_iter()
            .filter(|command| command["connect"].is_object())
            .count()
    };
    wait_until("a second connect command", || connects() >= 2).await;
    let sub = &last_command(world, "connect", None)["connect"]["subs"][&channel];
    assert_eq!(
        (
            &sub["recover"],
            sub["offset"].as_u64(),
            sub["epoch"].as_str()
        ),
        (&json!(true), Some(expected), Some(HISTORY_EPOCH)),
        "{sub}"
    );
}

#[then(
    regex = r#"^the subscription's stream yields, in order, "subscribing", "subscribed", and the publications at offsets ([\d, and]+)$"#
)]
async fn yields_in_order(world: &mut ClientWorld, list: String) {
    let mut seen = Vec::new();
    for _ in 0..2 + offsets(&list).len() {
        seen.push(match next_subscription_event(world).await {
            SubscriptionEvent::Publication(publication) => offset(&publication).to_string(),
            event => event.name().to_owned(),
        });
    }
    let mut expected = vec!["subscribing".to_owned(), "subscribed".to_owned()];
    expected.extend(offsets(&list).iter().map(u64::to_string));
    assert_eq!(seen, expected);
}

#[then("no publication is yielded twice")]
async fn no_publication_twice(world: &mut ClientWorld) {
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    while let Ok(Some(event)) = tokio::time::timeout(QUIET, stream.next()).await {
        if let SubscriptionEvent::Publication(publication) = event {
            world.delivery.taken.push(offset(&publication));
        }
    }
    let mut taken = world.delivery.taken.clone();
    taken.sort_unstable();
    taken.dedup();
    assert_eq!(
        taken.len(),
        world.delivery.taken.len(),
        "{:?}",
        world.delivery.taken
    );
}

#[then(r#"the client's stream yields no server-side publication before "connected""#)]
async fn no_server_side_publication_before_connected(world: &mut ClientWorld) {
    let mut before = Vec::new();
    loop {
        let event = next_client_event(world).await;
        if matches!(event, ClientEvent::Connected { .. }) {
            break;
        }
        before.push(event);
    }
    assert!(
        !before.iter().any(|event| matches!(
            event,
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(_),
                ..
            }
        )),
        "{before:?}"
    );
}

#[then(r#"the subscription's stream yields "unsubscribed" and no publication"#)]
async fn yields_unsubscribed_and_no_publication(world: &mut ClientWorld) {
    let event = next_subscription_event(world).await;
    assert!(
        matches!(event, SubscriptionEvent::Unsubscribed { .. }),
        "{event:?}"
    );
    let stream = world
        .delivery
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    while let Ok(Some(event)) = tokio::time::timeout(QUIET, stream.next()).await {
        assert!(
            !matches!(event, SubscriptionEvent::Publication(_)),
            "{event:?}"
        );
    }
}

#[then("the first stream's next item is its end")]
async fn the_first_stream_ends(world: &mut ClientWorld) {
    let delivery = &mut world.delivery;
    let ended = if delivery.second_subscription_events.is_some() {
        let stream = delivery
            .subscription_events
            .as_mut()
            .expect("the first stream");
        next_of(stream).await.map(|event| format!("{event:?}"))
    } else {
        let stream = delivery.client_events.as_mut().expect("the first stream");
        next_of(stream).await.map(|event| format!("{event:?}"))
    };
    assert_eq!(ended, None);
}

#[then(regex = r"^the new stream yields the publications at offsets ([\d, and]+)$")]
async fn the_new_stream_yields_publications(world: &mut ClientWorld, list: String) {
    let stream = world
        .delivery
        .second_subscription_events
        .as_mut()
        .expect("the new stream");
    for expected in offsets(&list) {
        let event = next_of(stream).await.expect("the stream has not ended");
        assert!(
            matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == expected),
            "{event:?}"
        );
    }
}

#[then(r#"the new stream yields "connecting", then "connected""#)]
async fn the_new_stream_yields_connecting_then_connected(world: &mut ClientWorld) {
    let stream = world
        .delivery
        .second_client_events
        .as_mut()
        .expect("the new stream");
    let mut names = Vec::new();
    for _ in 0..2 {
        names.push(
            next_of(stream)
                .await
                .expect("the stream has not ended")
                .name()
                .to_owned(),
        );
    }
    assert_eq!(names, ["connecting", "connected"]);
}

#[then("the subscription's stream yields that publication")]
async fn yields_that_publication(world: &mut ClientWorld) {
    let event = next_subscription_event(world).await;
    assert!(
        matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == 1),
        "{event:?}"
    );
}

#[then(regex = r#"^the stream of "(\w+)" yields that publication$"#)]
async fn the_stream_of_yields(world: &mut ClientWorld, channel: String) {
    let stream = world
        .delivery
        .channels
        .get_mut(&channel)
        .expect("that subscription")
        .1
        .as_mut()
        .expect("its stream");
    let event = next_of(stream).await.expect("the stream has not ended");
    assert!(
        matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == 1),
        "{event:?}"
    );
}
