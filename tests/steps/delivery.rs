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
use super::ping::time_elapses;
use super::{ClientWorld, EVENT_TIMEOUT, Events, SubscriptionEvents};
use crate::support::centrifugo::Centrifugo;
use crate::support::fake_server::{Announce, HISTORY_EPOCH};
use crate::support::relay::CountingRelay;

/// How long a step watches a stream for events that must not come.
pub(super) const QUIET: Duration = Duration::from_millis(500);

/// The streams of a Block 15 scenario, read by its steps.
#[derive(Default)]
pub(crate) struct Delivery {
    /// The client's stream, taken before `connect()`.
    client_events: Option<Events>,
    /// The client's stream from a second `events()` call.
    second_client_events: Option<Events>,
    /// The stream of the scenario's subscription.
    pub(super) subscription_events: Option<SubscriptionEvents>,
    /// The subscription's stream from a second `events()` call.
    pub(super) second_subscription_events: Option<SubscriptionEvents>,
    /// Further subscriptions, with their streams, by channel.
    pub(super) channels: HashMap<String, (Subscription, Option<SubscriptionEvents>)>,
    /// Offsets the fake server pushed, and those the application took.
    pub(super) sent: Vec<u64>,
    pub(super) taken: Vec<u64>,
    /// Options of the scenario's client, besides the world's.
    read_ahead_bytes: Option<usize>,
    timeout: Option<Duration>,
    /// A configuration a step creates a client with.
    config: Option<ferrifuge::Config>,
    /// Events the application read in one go.
    yielded: Vec<SubscriptionEvent>,
    /// The core of a 15c scenario.
    unread: Option<UnreadCore>,
}

impl Delivery {
    /// Applies the scenario's options to the client's `config`.
    pub(crate) fn configure(&self, config: &mut ferrifuge::Config) {
        if let Some(bytes) = self.read_ahead_bytes {
            config.read_ahead_bytes = bytes;
        }
        if let Some(timeout) = self.timeout {
            config.timeout = timeout;
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("sent", &self.sent)
            .field("taken", &self.taken)
            .finish_non_exhaustive()
    }
}

pub(super) fn offsets(list: &str) -> Vec<u64> {
    list.split([',', ' '])
        .filter_map(|word| word.parse().ok())
        .collect()
}

pub(super) fn offset(publication: &Publication) -> u64 {
    publication.offset.expect("the fake server sends offsets")
}

/// Waits for `ready` to hold, or fails with `what`.
pub(super) async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(EVENT_TIMEOUT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{what} within {EVENT_TIMEOUT:?}");
}

/// The next item of `stream`, `None` once it ended.
pub(super) async fn next_of<E>(
    stream: &mut (impl futures_util::Stream<Item = E> + Unpin),
) -> Option<E> {
    tokio::time::timeout(EVENT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("no item within {EVENT_TIMEOUT:?}"))
}

/// The next event of the scenario's subscription stream, recording the
/// publications the application takes.
pub(super) async fn next_subscription_event(world: &mut ClientWorld) -> SubscriptionEvent {
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
pub(super) fn a_client_with_its_stream(world: &mut ClientWorld) {
    let client = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .build()
        .expect("a valid client configuration");
    world.delivery.client_events = Some(Box::pin(client.events()));
    world.client = Some(client);
}

/// Subscribes to `channel`, reading its stream up to `subscribed`.
pub(super) async fn subscribed(
    world: &mut ClientWorld,
    channel: &str,
) -> (Subscription, SubscriptionEvents) {
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

#[given(
    regex = r#"^the fake server has sent publications at offsets ([\d, and]+) to "(\w+)"(, then closed the connection with a reconnect-advised code)?$"#
)]
fn the_fake_server_has_sent(world: &mut ClientWorld, list: String, channel: String, close: String) {
    for offset in offsets(&list) {
        world.fake().publish(&channel, offset);
        world.delivery.sent.push(offset);
    }
    if !close.is_empty() {
        // A close code in the protocol's reconnect range (Block 3).
        world.fake().close(3001, "reconnect");
    }
}

/// They count as sent, for "the client holds them not yet taken".
#[given(regex = r#"^the fake server has pushed a join and a leave for channel "(\w+)"$"#)]
fn the_fake_server_has_pushed_a_join_and_a_leave(world: &mut ClientWorld, channel: String) {
    for kind in ["join", "leave"] {
        world.fake().send(
            &serde_json::json!({"push": {"channel": channel, kind: {"info": {"client": "c2", "user": "u2"}}}})
                .to_string(),
        );
        world.delivery.sent.push(0);
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
    let (client, subscription) = (world.client(), world.subscription());
    wait_until(&format!("{expected} publications held"), || {
        subscription.held_events() + client.read_ahead().0 == expected
    })
    .await;
}

#[given(
    regex = r"^the client holds the server-side publications at offsets ([\d, and]+) not yet taken$"
)]
async fn the_client_holds(world: &mut ClientWorld, list: String) {
    let expected = offsets(&list).len();
    let client = world.client();
    wait_until(&format!("{expected} publications held"), || {
        client.held_events() + client.read_ahead().0 == expected
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

#[then(
    regex = r#"^the subscription's stream yields "unsubscribed" and no (?:publication|"join" or "leave" event)$"#
)]
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
            !matches!(
                event,
                SubscriptionEvent::Publication(_)
                    | SubscriptionEvent::Join(_)
                    | SubscriptionEvent::Leave(_)
            ),
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

// Block 15, sub-increment 15b: the bounded read-ahead.

/// How long the read-ahead must stay as it is to count as stopped.
const STOPPED: Duration = Duration::from_secs(1);

fn size(amount: u64, unit: &str) -> usize {
    let amount = usize::try_from(amount).expect("a size that fits");
    match unit {
        "KiB" => amount << 10,
        "MiB" => amount << 20,
        _ => amount,
    }
}

/// Reads the subscription's stream until it has yielded `count`
/// publications; they are offsets 1 to `count`, in order, each once.
async fn read_publications_in_order(world: &mut ClientWorld, count: u64) {
    let mut offsets = Vec::new();
    while offsets.len() < usize::try_from(count).expect("a count that fits") {
        if let SubscriptionEvent::Publication(publication) = next_subscription_event(world).await {
            offsets.push(offset(&publication));
        }
    }
    assert!(
        offsets.iter().copied().eq(1..=count),
        "not offsets 1 to {count} in order: {:?}",
        &offsets[..offsets.len().min(20)]
    );
}

#[given(
    regex = r#"^a client configured with a read-ahead of (\d+) (bytes|KiB|MiB), subscribed to channel "(\w+)" on the fake server$"#
)]
async fn a_client_with_a_read_ahead(
    world: &mut ClientWorld,
    amount: u64,
    unit: String,
    channel: String,
) {
    world.delivery.read_ahead_bytes = Some(size(amount, &unit));
    a_subscription_read_up_to_subscribed(world, String::new(), channel).await;
}

#[given(
    regex = r#"^a client with a timeout of (\d+) s, subscribed to channel "(\w+)" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after$"#
)]
async fn a_client_with_a_timeout(world: &mut ClientWorld, seconds: u64, channel: String) {
    world.delivery.timeout = Some(Duration::from_secs(seconds));
    a_subscription_read_up_to_subscribed(world, String::new(), channel).await;
}

#[given(
    regex = r"^a client connected to the fake server, which pings every (\d+) s and expects pongs$"
)]
async fn connected_to_a_pinging_fake_server(world: &mut ClientWorld, seconds: u32) {
    world
        .restart_fake(Announce {
            ping: seconds,
            pong: true,
            ..Announce::default()
        })
        .await;
    // Pinging before the connection: the first ping then comes within one
    // period of the connect reply, however long the step takes to read it.
    world.fake().ping_every(Duration::from_secs(seconds.into()));
    a_connected_client(world).await;
}

#[given(
    regex = r"^a client with maxServerPingDelay (\d+) ms, connected to the fake server pinging every (\d+) s$"
)]
async fn connected_with_a_ping_delay(world: &mut ClientWorld, millis: u64, seconds: u32) {
    world.max_server_ping_delay = Some(Duration::from_millis(millis));
    world
        .restart_fake(Announce {
            ping: seconds,
            ..Announce::default()
        })
        .await;
    // Pinging before the connection: the first ping then comes within one
    // period of the connect reply, however long the step takes to read it.
    world.fake().ping_every(Duration::from_secs(seconds.into()));
    a_connected_client(world).await;
}

#[given(
    regex = r#"^the client is subscribed to channel "(\w+)", whose subscription's stream has been read up to "subscribed" and is not read after$"#
)]
async fn the_client_is_subscribed(world: &mut ClientWorld, channel: String) {
    let (subscription, events) = subscribed(world, &channel).await;
    world.subscription = Some(subscription);
    world.delivery.subscription_events = Some(events);
}

#[given(regex = r#"^the fake server has sent (?:(\d+) publications|a publication) to "(\w+)"$"#)]
fn the_fake_server_has_sent_some(world: &mut ClientWorld, count: String, channel: String) {
    let count: u64 = count.parse().unwrap_or(1);
    for offset in 1..=count {
        world.fake().publish(&channel, offset);
        world.delivery.sent.push(offset);
    }
}

#[given(
    regex = r#"^the fake server has sent publications of (\d+) (bytes|KiB) to "(\w+)" until the client stopped reading$"#
)]
async fn sent_until_the_client_stopped_reading(
    world: &mut ClientWorld,
    amount: u64,
    unit: String,
    channel: String,
) {
    let limit = world.client_config().read_ahead_messages;
    for offset in 1..=u64::try_from(limit).expect("a limit that fits") * 4 {
        world
            .fake()
            .publish_sized(&channel, offset, size(amount, &unit));
    }
    let client = world.client();
    wait_until("the read-ahead full", || client.read_ahead().0 >= limit).await;
}

#[when(
    regex = r#"^the fake server (?:starts sending|sends) (\d+) publications? of (\d+) (bytes|KiB|MiB) to "(\w+)"$"#
)]
fn starts_sending(world: &mut ClientWorld, count: u64, amount: u64, unit: String, channel: String) {
    for offset in 1..=count {
        world
            .fake()
            .publish_sized(&channel, offset, size(amount, &unit));
        world.delivery.sent.push(offset);
    }
}

#[when(regex = r#"^the fake server sends a publication of (\d+) (bytes|KiB|MiB) to "(\w+)"$"#)]
fn sends_one_publication(world: &mut ClientWorld, amount: u64, unit: String, channel: String) {
    starts_sending(world, 1, amount, unit, channel);
}

#[when(regex = r#"^the application subscribes to channel "(\w+)"$"#)]
fn subscribes_to(world: &mut ClientWorld, channel: String) {
    let subscription = world
        .client()
        .subscription_builder(&channel)
        .build()
        .expect("no subscription to the channel yet");
    let events: SubscriptionEvents = Box::pin(subscription.events());
    subscription.subscribe();
    world
        .delivery
        .channels
        .insert(channel, (subscription, Some(events)));
}

#[when(regex = r"^(\d+) s elapse with the fake server pinging every \d+ s$")]
async fn elapse_while_pinging(world: &mut ClientWorld, seconds: u64) {
    time_elapses(world, seconds).await;
}

#[then("the client has opened no new connection")]
fn no_new_connection(world: &mut ClientWorld) {
    assert_eq!(world.elapsed_new_connections, 0);
}

#[then(regex = r#"^the client has not emitted "connecting" with code (\d+)$"#)]
fn not_connecting_with(world: &mut ClientWorld, code: u32) {
    assert!(
        !world.elapsed_events.iter().any(|event| matches!(
            event,
            ClientEvent::Connecting { code: of, .. } if *of == code
        )),
        // The new connections tell whether the reconnect happened in the
        // interval or before it, its events read only in the interval.
        "{:?}, {} new connections in the interval",
        world.elapsed_events,
        world.elapsed_new_connections
    );
}

#[then("the client has answered every ping")]
fn answered_every_ping(world: &mut ClientWorld) {
    let fake = world.fake();
    let pongs = fake
        .received()
        .iter()
        .filter(|command| command.as_object().is_some_and(serde_json::Map::is_empty))
        .count();
    // The last ping may still be on its way back.
    assert!(
        pongs + 1 >= fake.pings(),
        "{pongs} pongs to {} pings",
        fake.pings()
    );
}

#[then(
    regex = r#"^when the application reads the subscription's stream, it yields the publications at offsets ([\d, and]+), then "subscribing"$"#
)]
async fn yields_publications_then_subscribing(world: &mut ClientWorld, list: String) {
    for expected in offsets(&list) {
        let event = next_subscription_event(world).await;
        assert!(
            matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == expected),
            "{event:?}"
        );
    }
    let event = next_subscription_event(world).await;
    assert_eq!(event.name(), "subscribing", "{event:?}");
}

#[then(regex = r#"^the next subscribe command for "(\w+)" asks to recover from offset (\d+)$"#)]
async fn next_subscribe_recovers_from(world: &mut ClientWorld, channel: String, expected: u64) {
    let fake = world.fake();
    // Only those after the point a step noted, e.g. a disconnect: an
    // earlier subscribe cannot answer for "the next" (Codex on PR #100).
    let before = world.recovery.subscribes_before;
    let recovering = || {
        fake.received()
            .into_iter()
            .filter(|command| command["subscribe"].is_object())
            .skip(before)
            .any(|command| {
                let subscribe = &command["subscribe"];
                subscribe["channel"] == channel.as_str()
                    && subscribe["recover"] == true
                    && subscribe["offset"].as_u64() == Some(expected)
            })
    };
    wait_until("a subscribe recovering from that offset", recovering).await;
}

#[then(regex = r"^within (\d+) s the fake server has received a second connect command$")]
async fn second_connect_within(world: &mut ClientWorld, seconds: u64) {
    let fake = world.fake();
    let connects = || {
        fake.received()
            .into_iter()
            .filter(|command| command["connect"].is_object())
            .count()
    };
    let waited = tokio::time::timeout(Duration::from_secs(seconds), async {
        while connects() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{} connect commands", connects());
}

#[then(regex = r#"^(\d+) s later the client's state is "(\w+)"$"#)]
async fn state_later(world: &mut ClientWorld, seconds: u64, expected: String) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    assert_eq!(
        format!("{:?}", world.client().state()).to_lowercase(),
        expected
    );
}

#[then(
    regex = r#"^when the application reads the client's stream, it yields ((?:"\w+"(?:, )?)+)$"#
)]
async fn client_stream_yields(world: &mut ClientWorld, list: String) {
    let expected: Vec<&str> = list
        .split(", ")
        .map(|name| name.trim_matches('"'))
        .collect();
    let mut names = Vec::new();
    for _ in 0..expected.len() {
        names.push(next_client_event(world).await.name().to_owned());
    }
    assert_eq!(names, expected);
}

#[then(regex = r"^the client stops reading with (\d+) messages in its read-ahead$")]
async fn stops_reading_with_messages(world: &mut ClientWorld, expected: usize) {
    let client = world.client();
    wait_until(&format!("{expected} messages in the read-ahead"), || {
        client.read_ahead().0 == expected
    })
    .await;
    tokio::time::sleep(STOPPED).await;
    assert_eq!(client.read_ahead().0, expected);
}

#[then(regex = r"^1 s later its read-ahead still holds (\d+) messages$")]
async fn still_holds_messages(world: &mut ClientWorld, expected: usize) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(world.client().read_ahead().0, expected);
}

#[then(
    regex = r"^the client stops reading with more than (\d+) MiB and at most (\d+) MiB in its read-ahead$"
)]
async fn stops_reading_with_bytes(world: &mut ClientWorld, more_than: usize, at_most: usize) {
    let client = world.client();
    wait_until(
        &format!("more than {more_than} MiB in the read-ahead"),
        || client.read_ahead().1 > more_than << 20,
    )
    .await;
    tokio::time::sleep(STOPPED).await;
    assert!(
        client.read_ahead().1 <= at_most << 20,
        "{:?}",
        client.read_ahead()
    );
}

#[then(regex = r"^1 s later its read-ahead still holds at most (\d+) MiB$")]
async fn still_holds_at_most(world: &mut ClientWorld, at_most: usize) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let held = world.client().read_ahead();
    assert!(held.1 <= at_most << 20, "{held:?}");
}

#[then(
    regex = r"^when the application reads the subscription's stream, it yields the (\d+) publications in order, each once$"
)]
async fn yields_all_in_order(world: &mut ClientWorld, count: u64) {
    read_publications_in_order(world, count).await;
}

#[then("the subscription's stream yields it")]
async fn the_subscription_stream_yields_it(world: &mut ClientWorld) {
    let event = next_subscription_event(world).await;
    assert!(
        matches!(&event, SubscriptionEvent::Publication(publication) if offset(publication) == 1),
        "{event:?}"
    );
}

#[then(
    regex = r#"^when the application reads the stream of "(\w+)", the stream of "(\w+)" yields "subscribed"$"#
)]
async fn reading_one_releases_the_other(world: &mut ClientWorld, read: String, other: String) {
    let event = next_subscription_event(world).await;
    assert!(
        matches!(event, SubscriptionEvent::Publication(_)),
        "reading {read}: {event:?}"
    );
    let stream = world
        .delivery
        .channels
        .get_mut(&other)
        .expect("that subscription")
        .1
        .as_mut()
        .expect("its stream");
    loop {
        match next_of(stream).await.expect("the stream has not ended") {
            SubscriptionEvent::Subscribed(_) => return,
            SubscriptionEvent::Subscribing { .. } => {}
            event => panic!("{event:?} before subscribed"),
        }
    }
}

#[then("the fake server has received a refresh command")]
fn received_a_refresh(world: &mut ClientWorld) {
    assert!(
        world
            .fake()
            .received()
            .iter()
            .any(|command| command["refresh"].is_object()),
        "commands received: {:?}; client {:?}, read-ahead {:?}",
        world
            .fake()
            .received()
            .iter()
            .map(|command| command
                .as_object()
                .and_then(|command| command.keys().find(|key| *key != "id").cloned())
                .unwrap_or_else(|| "pong".to_owned()))
            .collect::<Vec<_>>(),
        world.client().state(),
        world.client().read_ahead()
    );
}

#[given(
    regex = r"^a Config whose (?:read-ahead limit of (messages|bytes)|write queue limit) is 0$"
)]
fn a_config_with_a_zero_limit(world: &mut ClientWorld, limit: String) {
    let mut config = ferrifuge::Config::default();
    match limit.as_str() {
        "messages" => config.read_ahead_messages = 0,
        "bytes" => config.read_ahead_bytes = 0,
        // Block 13d.
        _ => config.write_queue_bytes = 0,
    }
    world.delivery.config = Some(config);
}

#[when("a client is created")]
fn a_client_is_created(world: &mut ClientWorld) {
    let config = world.delivery.config.clone().expect("a Config");
    match Client::new("ws://127.0.0.1:1/connection/websocket", config) {
        Ok(client) => world.client = Some(client),
        Err(error) => world.build_error = Some(error),
    }
}

#[then("Client::new returns an error")]
fn client_new_returns_an_error(world: &mut ClientWorld) {
    let error = world.build_error.as_ref().expect("an error");
    assert_eq!(error.kind, ferrifuge::ErrorKind::Configuration, "{error:?}");
}

#[given(
    regex = r#"^a server whose channel "\w+" keeps a history of (\d+) publications and recovers up to (\d+), with a client queue of (\d+) MiB$"#
)]
async fn a_server_with_a_long_history(
    world: &mut ClientWorld,
    history: u64,
    recovers: u64,
    queue: usize,
) {
    if let Some(server) = world.server.take() {
        server.stop().await;
    }
    let (history, recovers, queue) = (
        history.to_string(),
        recovers.to_string(),
        (queue << 20).to_string(),
    );
    let env: Vec<(&str, &str)> = crate::SERVER_A
        .iter()
        .copied()
        .filter(|(name, _)| !name.starts_with("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY"))
        .chain([
            (
                "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE",
                history.as_str(),
            ),
            ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s"),
            (
                "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_FORCE_RECOVERY",
                "true",
            ),
            (
                "CENTRIFUGO_CLIENT_RECOVERY_MAX_PUBLICATION_LIMIT",
                recovers.as_str(),
            ),
            ("CENTRIFUGO_CLIENT_QUEUE_MAX_SIZE", queue.as_str()),
        ])
        .collect();
    let server = Centrifugo::start(&env).await;
    world.relay = Some(CountingRelay::start(server.address().to_owned()).await);
    world.server = Some(server);
}

#[given(
    regex = r#"^a client subscribed to "(\w+)", whose subscription's stream has been read up to "subscribed" and is not read after$"#
)]
async fn a_client_subscribed_on_the_server(world: &mut ClientWorld, channel: String) {
    a_connected_client(world).await;
    the_client_is_subscribed(world, channel).await;
}

#[when(regex = r#"^the server API publishes (\d+) publications of (\d+) KiB to "(\w+)"$"#)]
async fn the_server_api_publishes(world: &mut ClientWorld, count: u64, kib: u64, channel: String) {
    let server = world.server.as_ref().expect("a Centrifugo server");
    let pad = "x".repeat(size(kib, "KiB"));
    for n in 1..=count {
        server
            .api()
            .call(
                "publish",
                json!({"channel": channel, "data": {"n": n, "pad": pad}}),
            )
            .await
            .expect("publish");
    }
}

#[when(
    regex = r"^the application then reads the subscription's stream until it has (\d+) publications$"
)]
async fn reads_until(world: &mut ClientWorld, count: u64) {
    world.delivery.sent = (1..=count).collect();
    let mut events = Vec::new();
    let mut publications = 0;
    while publications < usize::try_from(count).expect("a count that fits") {
        let stream = world
            .delivery
            .subscription_events
            .as_mut()
            .expect("the subscription's stream");
        let Ok(Some(event)) = tokio::time::timeout(EVENT_TIMEOUT, stream.next()).await else {
            let client = world.client();
            panic!(
                "stalled after {publications} publications, last events {:?}; client {:?}, read-ahead {:?}, held {}",
                events
                    .iter()
                    .rev()
                    .take(3)
                    .map(SubscriptionEvent::name)
                    .collect::<Vec<_>>(),
                client.state(),
                client.read_ahead(),
                world.subscription().held_events(),
            );
        };
        if matches!(event, SubscriptionEvent::Publication(_)) {
            publications += 1;
        }
        events.push(event);
    }
    world.delivery.yielded = events;
}

#[then(regex = r"^they are the offsets 1 to (\d+), in order, each exactly once$")]
fn offsets_in_order(world: &mut ClientWorld, count: u64) {
    let offsets: Vec<u64> = world
        .delivery
        .yielded
        .iter()
        .filter_map(|event| match event {
            SubscriptionEvent::Publication(publication) => Some(offset(publication)),
            _ => None,
        })
        .collect();
    assert!(
        offsets.iter().copied().eq(1..=count),
        "{:?}",
        &offsets[..offsets.len().min(20)]
    );
}

#[then(r#"between them the stream yielded "subscribing", then "subscribed" with recovered true"#)]
fn yielded_a_recovery(world: &mut ClientWorld) {
    let yielded = &world.delivery.yielded;
    let subscribing = yielded
        .iter()
        .position(|event| event.name() == "subscribing")
        .expect("a subscribing event");
    let recovered = yielded[subscribing..].iter().any(
        |event| matches!(event, SubscriptionEvent::Subscribed(subscribed) if subscribed.recovered),
    );
    assert!(
        recovered,
        "{:?}",
        yielded
            .iter()
            .map(SubscriptionEvent::name)
            .collect::<Vec<_>>()
    );
}

#[given(
    regex = r"^a client connected to the fake server with a connection token that expires in (\d+) s$"
)]
async fn connected_with_an_expiring_token(world: &mut ClientWorld, seconds: u32) {
    world
        .restart_fake(Announce {
            ttl: seconds,
            ..Announce::default()
        })
        .await;
    let builder =
        super::token::with_expiring_token(world, Client::builder(world.server_endpoint()));
    world.build_client(builder);
    world.client().connect();
    let client = world.client();
    wait_until("connected", || client.state() == State::Connected).await;
}

// Block 15, sub-increment 15c: errors past 256 untaken events.

/// The sans-IO core of a 15c scenario, whose streams are open and never
/// read: every event it emits stays untaken.
pub(crate) struct UnreadCore {
    machine: ferrifuge::machine::Machine,
    transport: Option<ferrifuge::machine::TransportId>,
    subscription: Option<ferrifuge::machine::SubscriptionId>,
    /// The events emitted, in order: what reading the stream yields.
    client_events: Vec<ClientEvent>,
    subscription_events: Vec<SubscriptionEvent>,
    /// The data callback's fetch awaiting its answer.
    fetch: Option<ferrifuge::machine::FetchId>,
    /// The commands sent, encoded.
    sent: Vec<Vec<u8>>,
}

impl UnreadCore {
    fn new() -> Self {
        Self {
            machine: ferrifuge::machine::Machine::new(super::test_config()),
            transport: None,
            subscription: None,
            client_events: Vec::new(),
            subscription_events: Vec::new(),
            fetch: None,
            sent: Vec::new(),
        }
    }

    fn handle(&mut self, input: ferrifuge::machine::Input) {
        use ferrifuge::machine::Action;
        for action in self.machine.handle(input) {
            match action {
                Action::Emit(_, event) => self.client_events.push(event),
                Action::EmitSubscription(_, _, event) => self.subscription_events.push(event),
                Action::OpenTransport { transport, .. } => self.transport = Some(transport),
                Action::FetchSubscriptionData { fetch, .. } => self.fetch = Some(fetch),
                Action::Send { frame, .. } => self.sent.push(frame),
                _ => {}
            }
        }
    }

    /// The id of the last command sent.
    fn last_command_id(&self) -> u64 {
        let frame = self.sent.last().expect("a command sent");
        let command: Value = serde_json::from_slice(frame).expect("a JSON command");
        command["id"].as_u64().expect("an id")
    }

    /// Opens the transport just requested and answers the connect.
    fn connects(&mut self) {
        use ferrifuge::machine::Input;
        let transport = self.transport.expect("a transport to open");
        self.handle(Input::TransportOpened(transport));
        let id = self.last_command_id();
        self.handle(Input::Frame {
            transport,
            data: format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#).into_bytes(),
        });
    }
}

fn unread_core(world: &mut ClientWorld) -> &mut UnreadCore {
    world.delivery.unread.as_mut().expect("the scenario's core")
}

/// The names yielded: runs of `error` counted, e.g. `["connecting", "255
/// error", "connected"]`.
fn names_with_errors_counted<'a>(names: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut errors = 0;
    for name in names {
        if name == "error" {
            errors += 1;
            continue;
        }
        if errors > 0 {
            out.push(format!("{errors} error"));
            errors = 0;
        }
        out.push(name.to_owned());
    }
    if errors > 0 {
        out.push(format!("{errors} error"));
    }
    out
}

#[given(
    "a client whose stream was taken before connect is called and is not read, and whose server is unreachable"
)]
fn an_unread_client(world: &mut ClientWorld) {
    let mut core = UnreadCore::new();
    core.handle(ferrifuge::machine::Input::StreamOpened(None));
    world.delivery.unread = Some(core);
}

#[when(regex = r"^(\d+) connection attempts fail$")]
fn connection_attempts_fail(world: &mut ClientWorld, attempts: usize) {
    use ferrifuge::machine::{Input, Timer};
    let core = unread_core(world);
    core.handle(Input::Connect);
    for attempt in 0..attempts {
        if attempt > 0 {
            core.handle(Input::TimerFired(Timer::Reconnect));
        }
        let transport = core.transport.expect("a transport to open");
        core.handle(Input::TransportFailed {
            transport,
            message: "refused".into(),
        });
    }
}

#[when("the server becomes reachable and the client connects")]
fn the_server_becomes_reachable(world: &mut ClientWorld) {
    let core = unread_core(world);
    core.handle(ferrifuge::machine::Input::TimerFired(
        ferrifuge::machine::Timer::Reconnect,
    ));
    core.connects();
}

#[given(
    "a connected client with a subscription whose stream was taken before subscribe is called and is not read, and whose get_data callback fails"
)]
fn an_unread_subscription(world: &mut ClientWorld) {
    use ferrifuge::machine::{Input, SubscriptionOptions};
    let mut core = UnreadCore::new();
    core.handle(Input::Connect);
    core.connects();
    let mut options = SubscriptionOptions::default();
    options.data_source = true;
    let id = core
        .machine
        .add_subscription("news".into(), options)
        .expect("a new subscription");
    core.subscription = Some(id);
    core.handle(Input::StreamOpened(Some(id)));
    world.delivery.unread = Some(core);
}

#[when(regex = r"^(\d+) subscribe attempts fail$")]
fn subscribe_attempts_fail(world: &mut ClientWorld, attempts: usize) {
    use ferrifuge::machine::{Input, SubscriptionTimer};
    let core = unread_core(world);
    let id = core.subscription.expect("the subscription");
    core.handle(Input::Subscribe(id));
    for attempt in 0..attempts {
        if attempt > 0 {
            core.handle(Input::SubscriptionTimerFired(
                id,
                SubscriptionTimer::Resubscribe,
            ));
        }
        let fetch = core.fetch.take().expect("a data fetch");
        core.handle(Input::SubscriptionDataFetched {
            fetch,
            result: Err(ferrifuge::DataError::Failed("backend down".into())),
        });
    }
}

#[when("the get_data callback succeeds and the subscription subscribes")]
fn the_data_callback_succeeds(world: &mut ClientWorld) {
    use ferrifuge::machine::{Input, SubscriptionTimer};
    let core = unread_core(world);
    let id = core.subscription.expect("the subscription");
    core.handle(Input::SubscriptionTimerFired(
        id,
        SubscriptionTimer::Resubscribe,
    ));
    let fetch = core.fetch.take().expect("a data fetch");
    core.handle(Input::SubscriptionDataFetched {
        fetch,
        result: Ok(b"{}".to_vec()),
    });
    let command = core.last_command_id();
    let transport = core.transport.expect("the transport");
    core.handle(Input::Frame {
        transport,
        data: format!(r#"{{"id":{command},"subscribe":{{}}}}"#).into_bytes(),
    });
}

#[then(regex = r#"^reading the stream yields "(\w+)", (\d+) error events, then "(\w+)"$"#)]
fn the_client_stream_yields(world: &mut ClientWorld, first: String, errors: usize, last: String) {
    let core = unread_core(world);
    let names = names_with_errors_counted(core.client_events.iter().map(ClientEvent::name));
    assert_eq!(names, [first, format!("{errors} error"), last]);
}

#[then(
    regex = r#"^reading the subscription's stream yields "(\w+)", (\d+) error events, then "(\w+)"$"#
)]
fn the_subscription_stream_yields_errors(
    world: &mut ClientWorld,
    first: String,
    errors: usize,
    last: String,
) {
    let core = unread_core(world);
    let names =
        names_with_errors_counted(core.subscription_events.iter().map(SubscriptionEvent::name));
    assert_eq!(names, [first, format!("{errors} error"), last]);
}
