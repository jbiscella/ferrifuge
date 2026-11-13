//! Steps of Block 37c: the real client against the in-memory server
//! double (@D), and the double against Centrifugo (@A).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::test_util::{Command, Server};
use ferrifuge::{
    CallError, ClientEvent, Connecting, Connector, HistoryOptions, HistoryResult, PresenceResult,
    PresenceStatsResult, Protocol, ServerSubscriptionEvent, SubscriptionEvent,
};
use serde_json::Value;

use super::{ClientWorld, EVENT_TIMEOUT};

/// What a 37c scenario holds: the double, how often its connector was
/// called, and what the steps saw.
#[derive(Debug, Default)]
pub(crate) struct Doubles {
    server: Option<Server>,
    calls: Arc<AtomicUsize>,
    /// Holds the connector's new connections while set.
    hold: Arc<Hold>,
    /// The client id of the client's "connected" event.
    client_id: Option<String>,
    /// The outcome of the last call a step made that is not an RPC.
    outcome: Option<Outcome>,
}

#[derive(Debug)]
enum Outcome {
    Published(Result<(), CallError>),
    History(Result<HistoryResult, CallError>),
    Presence(Result<PresenceResult, CallError>),
    PresenceStats(Result<PresenceStatsResult, CallError>),
}

/// Holds the connections a connector opens while set, until a step
/// releases them: the step then acts while the client reconnects, however
/// slow the runner, rather than within a reconnect delay.
#[derive(Debug, Default)]
struct Hold {
    held: AtomicBool,
    released: tokio::sync::Notify,
}

impl Hold {
    async fn passed(&self) {
        loop {
            // Created before the check: a release in between is kept.
            let released = self.released.notified();
            if !self.held.load(Ordering::SeqCst) {
                return;
            }
            released.await;
        }
    }

    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.released.notify_waiters();
    }
}

/// A connector counting its calls once held ones pass, then giving the
/// double's.
struct Counting<C> {
    inner: C,
    calls: Arc<AtomicUsize>,
    hold: Arc<Hold>,
}

impl<C: Connector + Clone + 'static> Connector for Counting<C> {
    fn connect(&self, endpoint: ferrifuge::http::Uri) -> Connecting {
        let inner = self.inner.clone();
        let calls = Arc::clone(&self.calls);
        let hold = Arc::clone(&self.hold);
        Box::pin(async move {
            hold.passed().await;
            calls.fetch_add(1, Ordering::SeqCst);
            inner.connect(endpoint).await
        })
    }
}

fn server(world: &ClientWorld) -> &Server {
    world.doubles.server.as_ref().expect("a server double")
}

fn json(text: &str) -> Value {
    serde_json::from_str(text).expect("JSON")
}

fn data_of(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("JSON data")
}

/// Builds the scenario's client for the double, with `config`.
fn build(world: &mut ClientWorld, config: ferrifuge::Config) {
    let double = server(world).clone();
    let connector = Counting {
        inner: double.connector(),
        calls: Arc::clone(&world.doubles.calls),
        hold: Arc::clone(&world.doubles.hold),
    };
    let builder = ferrifuge::Client::builder(double.endpoint())
        .config(config)
        .connector(connector);
    world.build_client(builder);
}

/// Subscribes the scenario's subscription to `channel`, recoverable when
/// asked.
fn subscribe(world: &mut ClientWorld, channel: &str, recoverable: bool) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .recoverable(recoverable)
        .build()
        .expect("a new channel");
    world.track_subscription(subscription);
    world.subscription().subscribe();
}

/// Waits for the client's "connected" event, keeping its client id.
async fn connected(world: &mut ClientWorld) -> String {
    loop {
        match world.next_event().await {
            ClientEvent::Connecting { .. } => {}
            ClientEvent::Connected { client_id, .. } => {
                world.doubles.client_id = Some(client_id.clone());
                return client_id;
            }
            other => panic!("{other:?}"),
        }
    }
}

/// Waits for the subscription's "subscribed" event.
async fn subscribed(world: &mut ClientWorld) -> ferrifuge::Subscribed {
    loop {
        match world.next_subscription_event(false).await {
            SubscriptionEvent::Subscribing { .. } => {}
            SubscriptionEvent::Subscribed(details) => return details,
            other => panic!("{other:?}"),
        }
    }
}

#[given("a server double")]
fn a_server_double(world: &mut ClientWorld) {
    world.doubles.server = Some(Server::new());
}

#[given(
    regex = r"^a client built with the double's endpoint and connector and the (JSON|Protobuf) protocol$"
)]
fn a_client_with_a_protocol(world: &mut ClientWorld, protocol: String) {
    world.protocol = if protocol == "Protobuf" {
        Protocol::Protobuf
    } else {
        Protocol::Json
    };
    let config = world.client_config();
    build(world, config);
}

#[given("a client built with the double's endpoint and connector")]
fn a_client(world: &mut ClientWorld) {
    let config = world.client_config();
    build(world, config);
}

#[when(regex = r#"^connect is called and the application subscribes to channel "(\w+)"$"#)]
fn connect_and_subscribe(world: &mut ClientWorld, channel: String) {
    world.client().connect();
    subscribe(world, &channel, false);
}

#[then(
    regex = r#"^the client emits "connected" over transport "websocket", with a client id the double gave$"#
)]
async fn connected_with_an_id(world: &mut ClientWorld) {
    let client_id = connected(world).await;
    assert!(client_id.starts_with("double-"), "{client_id}");
}

#[when(regex = r#"^the double publishes (\{.*\}) to channel "([^"]+)"$"#)]
fn the_double_publishes(world: &mut ClientWorld, data: String, channel: String) {
    server(world).publish(channel, data.into_bytes());
}

#[then(regex = r"^the subscription receives a publication with data (\{.*\}) and offset (\d+)$")]
async fn a_publication_at(world: &mut ClientWorld, data: String, offset: u64) {
    let publication = super::subscriptions::next_publication(world, false).await;
    assert_eq!(data_of(&publication.data), json(&data));
    assert_eq!(publication.offset, Some(offset));
}

#[then(
    regex = r#"^the double received a connect command, then a subscribe command for "news", and no other command$"#
)]
async fn received_connect_and_subscribe(world: &mut ClientWorld) {
    // Nothing else follows: the double announces no pings.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let received = server(world).received();
    assert!(
        matches!(
            received.as_slice(),
            [Command::Connect { .. }, Command::Subscribe { channel, recover: None, .. }] if channel == "news"
        ),
        "{received:?}"
    );
}

#[given(
    regex = r#"^a client built with the double's endpoint and connector and a minimum reconnect delay of (\d+) ms, connected, with a recoverable subscription to "news" subscribed$"#
)]
async fn a_recoverable_subscriber(world: &mut ClientWorld, millis: u64) {
    let config = world
        .client_config()
        .min_reconnect_delay(Duration::from_millis(millis));
    build(world, config);
    world.client().connect();
    subscribe(world, "news", true);
    connected(world).await;
    subscribed(world).await;
    // The next connection waits until the step publishing during the
    // reconnect has published.
    world.doubles.hold.held.store(true, Ordering::SeqCst);
}

#[given(
    regex = r#"^the double published (\{.*\}) to channel "news", which the subscription received$"#
)]
async fn published_and_received(world: &mut ClientWorld, data: String) {
    let offset = server(world).publish("news", data.clone().into_bytes());
    a_publication_at(world, data, offset).await;
}

#[when(regex = r#"^the double disconnects the client with code (\d+) "([^"]+)"$"#)]
fn the_double_disconnects(world: &mut ClientWorld, code: u16, reason: String) {
    server(world).disconnect(code, &reason);
}

#[when(
    regex = r#"^the double publishes (\{.*\}) and then (\{.*\}) to channel "news" while the connector has been called once$"#
)]
fn publishes_while_reconnecting(world: &mut ClientWorld, first: String, second: String) {
    assert_eq!(world.doubles.calls.load(Ordering::SeqCst), 1);
    server(world).publish("news", first.into_bytes());
    server(world).publish("news", second.into_bytes());
    world.doubles.hold.release();
}

#[then(regex = r#"^the subscription emits "subscribing", then "subscribed" with recovered true$"#)]
async fn resubscribed_recovered(world: &mut ClientWorld) {
    let event = world.next_subscription_event(false).await;
    assert_eq!(event.name(), "subscribing", "{event:?}");
    let details = subscribed(world).await;
    assert!(details.was_recovering && details.recovered, "{details:?}");
}

#[then(
    regex = r"^the subscription receives the publications (\{.*\}) at offset (\d+) and (\{.*\}) at offset (\d+), in that order, each exactly once$"
)]
async fn two_publications(
    world: &mut ClientWorld,
    first: String,
    first_offset: u64,
    second: String,
    second_offset: u64,
) {
    a_publication_at(world, first, first_offset).await;
    a_publication_at(world, second, second_offset).await;
    let more = world
        .subscription_events_during(Duration::from_millis(300))
        .await;
    assert!(
        !more
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{more:?}"
    );
}

#[then(
    regex = r#"^the double received a second subscribe command for "news", asking to recover from offset (\d+)$"#
)]
fn a_second_subscribe(world: &mut ClientWorld, offset: u64) {
    let subscribes: Vec<Command> = server(world)
        .received()
        .into_iter()
        .filter(
            |command| matches!(command, Command::Subscribe { channel, .. } if channel == "news"),
        )
        .collect();
    let Some(Command::Subscribe {
        recover: Some(from),
        ..
    }) = subscribes.get(1)
    else {
        panic!("a second subscribe asking to recover: {subscribes:?}");
    };
    assert_eq!(from.offset, offset);
}

#[given(
    regex = r#"^a server double whose RPC handler answers method "echo" with the data it is given, and any other method with the error (\d+) "([^"]+)"$"#
)]
fn a_double_with_rpc(world: &mut ClientWorld, code: u32, message: String) {
    let double = Server::new();
    double.on_rpc(move |method, data| {
        if method == "echo" {
            Ok(data.to_vec())
        } else {
            Err(CallError::new(code, message.clone()))
        }
    });
    world.doubles.server = Some(double);
}

#[given(
    regex = r#"^a client built with the double's endpoint and connector, connected, with a subscription to "news" subscribed$"#
)]
async fn a_subscriber(world: &mut ClientWorld) {
    let config = world.client_config();
    build(world, config);
    world.client().connect();
    subscribe(world, "news", false);
    connected(world).await;
    subscribed(world).await;
}

#[given(regex = r#"^the double published (\{.*\}) to channel "news"$"#)]
fn the_double_published(world: &mut ClientWorld, data: String) {
    server(world).publish("news", data.into_bytes());
}

#[when(regex = r#"^the application calls RPC "(\w+)" with (\{.*\})$"#)]
fn an_rpc(world: &mut ClientWorld, method: String, data: String) {
    super::calls::call(world, &method, data.into_bytes());
}

#[when(regex = r#"^the application publishes (\{.*\}) to channel "news"$"#)]
async fn a_publish(world: &mut ClientWorld, data: String) {
    let published = world.client().publish("news", data.into_bytes()).await;
    world.doubles.outcome = Some(Outcome::Published(published));
}

#[when(regex = r#"^the application requests the history of "news" with limit (-?\d+)$"#)]
async fn a_history(world: &mut ClientWorld, limit: i32) {
    let history = world
        .client()
        .history("news", HistoryOptions::default().limit(limit))
        .await;
    world.doubles.outcome = Some(Outcome::History(history));
}

#[when(r#"the application requests the presence of "news""#)]
async fn a_presence(world: &mut ClientWorld) {
    let presence = world.client().presence("news").await;
    world.doubles.outcome = Some(Outcome::Presence(presence));
}

#[when(r#"the application requests the presence stats of "news""#)]
async fn presence_stats(world: &mut ClientWorld) {
    let stats = world.client().presence_stats("news").await;
    world.doubles.outcome = Some(Outcome::PresenceStats(stats));
}

#[then(regex = r#"^the call fails with code (\d+) and message "([^"]+)"$"#)]
async fn the_call_fails(world: &mut ClientWorld, code: u32, message: String) {
    let error = super::calls::last_error(world).await;
    assert_eq!(
        (error.code, error.message.as_str()),
        (code, message.as_str())
    );
}

#[then(
    regex = r"^the publish succeeds and the subscription receives a publication (\{.*\}) at offset (\d+)$"
)]
async fn published_and_delivered(world: &mut ClientWorld, data: String, offset: u64) {
    let Some(Outcome::Published(published)) = world.doubles.outcome.take() else {
        panic!("a publish");
    };
    published.expect("the publish succeeds");
    // The publications before it come first.
    loop {
        let publication = super::subscriptions::next_publication(world, false).await;
        if publication.offset == Some(offset) {
            assert_eq!(data_of(&publication.data), json(&data));
            return;
        }
    }
}

fn history(world: &mut ClientWorld) -> HistoryResult {
    let Some(Outcome::History(history)) = world.doubles.outcome.take() else {
        panic!("a history call");
    };
    history.expect("the history call succeeds")
}

#[then(
    regex = r"^the result holds the publication (\{.*\}) at offset (\d+), and its offset is (\d+)$"
)]
fn history_with_one(world: &mut ClientWorld, data: String, at: u64, offset: u64) {
    let history = history(world);
    assert_eq!(history.publications.len(), 1, "{history:?}");
    assert_eq!(data_of(&history.publications[0].data), json(&data));
    assert_eq!(history.publications[0].offset, Some(at));
    assert_eq!(history.offset, offset);
}

#[then(regex = r"^the result holds no publication, and its offset is (\d+)$")]
fn history_empty(world: &mut ClientWorld, offset: u64) {
    let history = history(world);
    assert!(history.publications.is_empty(), "{history:?}");
    assert_eq!(history.offset, offset);
}

#[then(r#"the result holds one client, with the client id of the client's "connected" event"#)]
fn presence_of_one(world: &mut ClientWorld) {
    let Some(Outcome::Presence(presence)) = world.doubles.outcome.take() else {
        panic!("a presence call");
    };
    let presence = presence.expect("the presence call succeeds");
    let client = world.doubles.client_id.clone().expect("a connected event");
    assert_eq!(presence.clients.keys().collect::<Vec<_>>(), [&client]);
}

#[then(regex = r"^the result counts (\d+) client and (\d+) user$")]
fn stats_count(world: &mut ClientWorld, clients: u32, users: u32) {
    let Some(Outcome::PresenceStats(stats)) = world.doubles.outcome.take() else {
        panic!("a presence stats call");
    };
    let stats = stats.expect("the presence stats call succeeds");
    assert_eq!((stats.num_clients, stats.num_users), (clients, users));
}

#[given(
    regex = r#"^a server double that refuses the next connect with the temporary error (\d+) "([^"]+)"$"#
)]
fn a_double_refusing_a_connect(world: &mut ClientWorld, code: u32, message: String) {
    let double = Server::new();
    double.refuse_next_connect(CallError::new(code, message).temporary(true));
    world.doubles.server = Some(double);
}

#[given(
    regex = r#"^a server double that refuses subscriptions to "(\w+)" with the error (\d+) "([^"]+)"$"#
)]
fn a_double_refusing_subscriptions(
    world: &mut ClientWorld,
    channel: String,
    code: u32,
    message: String,
) {
    let double = Server::new();
    double.refuse_subscriptions(channel, CallError::new(code, message));
    world.doubles.server = Some(double);
}

#[then(
    regex = r#"^the client emits "connecting", an "error" event of kind connect with code (\d+), then "connected", and the double received two connect commands$"#
)]
async fn refused_then_connected(world: &mut ClientWorld, code: u32) {
    let connecting = world.next_event().await;
    assert_eq!(connecting.name(), "connecting", "{connecting:?}");
    let error = world.next_event().await;
    let ClientEvent::Error(error) = &error else {
        panic!("an error event: {error:?}");
    };
    assert_eq!(
        (error.kind, error.code),
        (ferrifuge::ErrorKind::Connect, code),
        "{error:?}"
    );
    let connected = world.next_event().await;
    assert_eq!(connected.name(), "connected", "{connected:?}");
    let connects = server(world)
        .received()
        .iter()
        .filter(|command| matches!(command, Command::Connect { .. }))
        .count();
    assert_eq!(connects, 2);
}

#[then(
    regex = r#"^the client is "connected" and the subscription emits "unsubscribed" with code (\d+)$"#
)]
async fn connected_and_unsubscribed(world: &mut ClientWorld, code: u32) {
    connected(world).await;
    loop {
        match world.next_subscription_event(false).await {
            SubscriptionEvent::Subscribing { .. } => {}
            SubscriptionEvent::Unsubscribed { code: got, .. } => {
                assert_eq!(got, code);
                return;
            }
            other => panic!("{other:?}"),
        }
    }
}

#[given(
    regex = r"^a client built with the double's endpoint and connector and reconnect delays from (\d+) ms to (\d+) ms, connected$"
)]
async fn a_connected_client(world: &mut ClientWorld, min: u64, max: u64) {
    let config = world
        .client_config()
        .min_reconnect_delay(Duration::from_millis(min))
        .max_reconnect_delay(Duration::from_millis(max));
    build(world, config);
    world.client().connect();
    connected(world).await;
}

#[then(
    regex = r"^after 1 s the connector has been called (\d+) times and the double has (\d+) connection open$"
)]
async fn after_a_second(world: &mut ClientWorld, calls: usize, open: usize) {
    // Reached however late the runner is (CI health, finding 11: a fixed
    // 1 s sleep failed when the reconnect came later), then unchanged
    // until 1 s after the step started.
    let started = std::time::Instant::now();
    let counts = |world: &ClientWorld| {
        (
            world.doubles.calls.load(Ordering::SeqCst),
            server(world).connections(),
        )
    };
    let reached = tokio::time::timeout(super::EVENT_TIMEOUT, async {
        while counts(world) != (calls, open) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "connector calls and open connections: {:?}",
        counts(world)
    );
    tokio::time::sleep(Duration::from_secs(1).saturating_sub(started.elapsed())).await;
    assert_eq!(counts(world), (calls, open));
}

#[given(
    regex = r#"^a server double that grants the server-side subscription "([^"]+)" at connect$"#
)]
fn a_double_granting(world: &mut ClientWorld, channel: String) {
    let double = Server::new();
    double.grant_server_subscription(channel);
    world.doubles.server = Some(double);
}

#[then(r#"the client emits a server-side "subscribed" event for channel "personal:1""#)]
async fn server_side_subscribed(world: &mut ClientWorld) {
    loop {
        match world.next_event().await {
            ClientEvent::Connecting { .. } | ClientEvent::Connected { .. } => {}
            ClientEvent::ServerSubscription {
                channel,
                event: ServerSubscriptionEvent::Subscribed(_),
            } if channel == "personal:1" => return,
            other => panic!("{other:?}"),
        }
    }
}

#[then(
    regex = r#"^the client emits a server-side publication of "personal:1" with data (\{.*\}) and offset (\d+)$"#
)]
async fn server_side_publication(world: &mut ClientWorld, data: String, offset: u64) {
    let event = world.next_event().await;
    let ClientEvent::ServerSubscription {
        channel,
        event: ServerSubscriptionEvent::Publication(publication),
    } = &event
    else {
        panic!("a server-side publication: {event:?}");
    };
    assert_eq!(channel, "personal:1");
    assert_eq!(data_of(&publication.data), json(&data));
    assert_eq!(publication.offset, Some(offset));
}

#[when(regex = r"^the double sends the message (\{.*\})$")]
fn the_double_sends(world: &mut ClientWorld, data: String) {
    server(world).message(data.into_bytes());
}

#[then(regex = r#"^the client emits a "message" event with data (\{.*\})$"#)]
async fn a_message(world: &mut ClientWorld, data: String) {
    let event = tokio::time::timeout(EVENT_TIMEOUT, world.next_event())
        .await
        .expect("an event");
    let ClientEvent::Message(bytes) = &event else {
        panic!("a message event: {event:?}");
    };
    assert_eq!(data_of(bytes), json(&data));
}

// @A: the double and Centrifugo answer alike.

/// One side of the comparison: a client, its subscription to "news", and
/// what both yielded, read as they come so that none holds the client
/// back (Block 15).
pub(crate) struct Side {
    client: ferrifuge::Client,
    subscription: ferrifuge::Subscription,
    events: Arc<std::sync::Mutex<Vec<ClientEvent>>>,
    subscription_events: Arc<std::sync::Mutex<Vec<SubscriptionEvent>>>,
    history: Option<HistoryResult>,
}

impl std::fmt::Debug for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Side").finish_non_exhaustive()
    }
}

/// Reads `stream` into a shared list, on a task of its own.
fn recorded<E: Send + 'static>(
    stream: impl futures_util::Stream<Item = E> + Send + Unpin + 'static,
) -> Arc<std::sync::Mutex<Vec<E>>> {
    use futures_util::StreamExt as _;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let into = Arc::clone(&seen);
    let mut stream = stream;
    tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            into.lock().unwrap().push(event);
        }
    });
    seen
}

impl Side {
    fn new(builder: ferrifuge::ClientBuilder) -> Self {
        let client = builder.build().expect("a client");
        let events = recorded(client.events());
        let subscription = client
            .subscription_builder("news")
            .recoverable(true)
            .build()
            .expect("a new channel");
        let subscription_events = recorded(subscription.events());
        Self {
            client,
            subscription,
            events,
            subscription_events,
            history: None,
        }
    }

    /// Waits until `done` holds for its subscription's events.
    async fn until(&self, done: impl Fn(&[SubscriptionEvent]) -> bool) {
        let reached = tokio::time::timeout(EVENT_TIMEOUT, async {
            while !done(&self.subscription_events.lock().unwrap()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            reached.is_ok(),
            "{:?}",
            self.subscription_events.lock().unwrap()
        );
    }

    /// Waits until its client has emitted `count` "connected" events.
    async fn until_connected(&self, count: usize) {
        let connected = || {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, ClientEvent::Connected { .. }))
                .count()
        };
        let reached = tokio::time::timeout(EVENT_TIMEOUT, async {
            while connected() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(reached.is_ok(), "{:?}", self.events.lock().unwrap());
    }

    fn publications(&self) -> Vec<(Value, Option<u64>)> {
        self.subscription_events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                SubscriptionEvent::Publication(publication) => {
                    Some((data_of(&publication.data), publication.offset))
                }
                _ => None,
            })
            .collect()
    }

    /// Its events with what differs by design left out: epochs, client ids.
    fn normalized(&self) -> (Vec<String>, Vec<String>) {
        let client = self
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                ClientEvent::Connecting { code, reason } => format!("connecting {code} {reason}"),
                ClientEvent::Disconnected { code, reason } => {
                    format!("disconnected {code} {reason}")
                }
                ClientEvent::Error(error) => format!("error {:?} {}", error.kind, error.code),
                other => other.name().to_owned(),
            })
            .collect();
        let subscription = self
            .subscription_events
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                SubscriptionEvent::Subscribing { code, reason } => {
                    format!("subscribing {code} {reason}")
                }
                SubscriptionEvent::Subscribed(details) => format!(
                    "subscribed positioned {} recoverable {} was_recovering {} recovered {} publications {}",
                    details.positioned,
                    details.recoverable,
                    details.was_recovering,
                    details.recovered,
                    details.has_recovered_publications
                ),
                SubscriptionEvent::Publication(publication) => format!(
                    "publication {} at {:?}",
                    data_of(&publication.data),
                    publication.offset
                ),
                other => format!("{other:?}"),
            })
            .collect();
        (client, subscription)
    }
}

/// Both sides of the comparison: Centrifugo's, then the double's.
fn sides(world: &mut ClientWorld) -> &mut [Side] {
    &mut world.sides
}

#[given(
    regex = r#"^a Centrifugo server whose channel "news" keeps history, allows recovery and accepts publishes from clients, and a server double$"#
)]
async fn centrifugo_and_a_double(world: &mut ClientWorld) {
    let mut env: Vec<(&str, &str)> = crate::SERVER_A.to_vec();
    env.extend([
        (
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_RECOVERY",
            "true",
        ),
        (
            "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_POSITIONING",
            "true",
        ),
    ]);
    let server = crate::support::centrifugo::Centrifugo::start(&env).await;
    world.relay =
        Some(crate::support::relay::CountingRelay::start(server.address().to_owned()).await);
    world.server = Some(server);
    world.doubles.server = Some(Server::new());
}

#[given(
    regex = r#"^for each of them, a client with a minimum reconnect delay of (\d+) s and a connection token for user "(\w+)", connected to it, with a recoverable subscription to "news" subscribed$"#
)]
async fn a_client_for_each(world: &mut ClientWorld, seconds: u64, user: String) {
    let config = world
        .client_config()
        .min_reconnect_delay(Duration::from_secs(seconds))
        .token(crate::support::token::connection_token(
            &user,
            Duration::from_secs(3600),
        ));
    let centrifugo = ferrifuge::Client::builder(world.server_endpoint()).config(config.clone());
    let double = server(world).clone();
    let connector = Counting {
        inner: double.connector(),
        calls: Arc::clone(&world.doubles.calls),
        hold: Arc::clone(&world.doubles.hold),
    };
    let doubled = ferrifuge::Client::builder(double.endpoint())
        .config(config)
        .connector(connector);
    world.sides = vec![Side::new(centrifugo), Side::new(doubled)];
    for side in sides(world) {
        side.client.connect();
        side.subscription.subscribe();
    }
    for side in sides(world) {
        side.until(|events| {
            events
                .iter()
                .any(|event| matches!(event, SubscriptionEvent::Subscribed(_)))
        })
        .await;
    }
}

#[when(regex = r#"^each client publishes (\{.*\}) and then (\{.*\}) to channel "news"$"#)]
async fn each_publishes(world: &mut ClientWorld, first: String, second: String) {
    for side in sides(world) {
        for data in [&first, &second] {
            side.client
                .publish("news", data.clone().into_bytes())
                .await
                .expect("a publish");
        }
    }
}

#[when(regex = r#"^each client requests the history of "news" with limit (-?\d+)$"#)]
async fn each_requests_history(world: &mut ClientWorld, limit: i32) {
    for side in sides(world) {
        side.history = Some(
            side.client
                .history("news", HistoryOptions::default().limit(limit))
                .await
                .expect("a history"),
        );
    }
}

#[when(
    regex = r#"^each server disconnects its client with code (\d+) "(\w+)", Centrifugo through its server API$"#
)]
async fn each_server_disconnects(world: &mut ClientWorld, code: u16, reason: String) {
    // Both have received their own publications first.
    for side in sides(world) {
        side.until(|events| {
            events
                .iter()
                .filter(|event| matches!(event, SubscriptionEvent::Publication(_)))
                .count()
                >= 2
        })
        .await;
    }
    // The next connections wait until the step publishing during the
    // reconnect has published, however slow the runner.
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(crate::support::relay::Mode::Hold);
    world.doubles.hold.held.store(true, Ordering::SeqCst);
    world
        .server
        .as_ref()
        .expect("Centrifugo")
        .api()
        .call(
            "disconnect",
            serde_json::json!({"user": "u1", "disconnect": {"code": code, "reason": reason}}),
        )
        .await
        .expect("server API disconnect");
    server(world).disconnect(code, &reason);
}

#[when(
    regex = r#"^each server publishes (\{.*\}) to channel "news" before its client reconnects$"#
)]
async fn each_server_publishes(world: &mut ClientWorld, data: String) {
    world
        .server
        .as_ref()
        .expect("Centrifugo")
        .api()
        .call(
            "publish",
            serde_json::json!({"channel": "news", "data": json(&data)}),
        )
        .await
        .expect("server API publish");
    server(world).publish("news", data.into_bytes());
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(crate::support::relay::Mode::Forward);
    world.doubles.hold.release();
}

#[then(
    regex = r#"^both clients emit the same client and subscription events, with the same codes, in the same order, "subscribed" with recovered true included$"#
)]
async fn the_same_events(world: &mut ClientWorld) {
    for side in sides(world) {
        side.until(|events| {
            events
                .iter()
                .filter(|event| matches!(event, SubscriptionEvent::Publication(_)))
                .count()
                >= 3
        })
        .await;
        // Its client's events are recorded apart from its subscription's.
        side.until_connected(2).await;
    }
    let sides = sides(world);
    let (centrifugo, double) = (sides[0].normalized(), sides[1].normalized());
    assert_eq!(centrifugo, double);
    assert!(
        centrifugo
            .1
            .iter()
            .any(|event| event.contains("was_recovering true recovered true")),
        "{centrifugo:?}"
    );
}

#[then(
    regex = r"^both subscriptions receive the publications (\{.*\}), (\{.*\}) and (\{.*\}) at offsets (\d+), (\d+) and (\d+)$"
)]
fn both_receive(
    world: &mut ClientWorld,
    first: String,
    second: String,
    third: String,
    a: u64,
    b: u64,
    c: u64,
) {
    let expected = vec![
        (json(&first), Some(a)),
        (json(&second), Some(b)),
        (json(&third), Some(c)),
    ];
    for side in sides(world) {
        assert_eq!(side.publications(), expected);
    }
}

#[then(
    regex = r"^both history results hold (\{.*\}) at offset (\d+) and (\{.*\}) at offset (\d+), with offset (\d+)$"
)]
fn both_histories(
    world: &mut ClientWorld,
    first: String,
    a: u64,
    second: String,
    b: u64,
    offset: u64,
) {
    for side in sides(world) {
        let history = side.history.as_ref().expect("a history");
        let got: Vec<(Value, Option<u64>)> = history
            .publications
            .iter()
            .map(|publication| (data_of(&publication.data), publication.offset))
            .collect();
        assert_eq!(got, [(json(&first), Some(a)), (json(&second), Some(b))]);
        assert_eq!(history.offset, offset);
    }
}
