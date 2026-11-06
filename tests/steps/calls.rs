//! Steps of Block 13: client commands.

use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use std::future::Future;

use std::pin::Pin;

use ferrifuge::{
    CallError, Client, ClientEvent, Config, ErrorKind, HistoryOptions, HistoryResult,
    PresenceResult, PresenceStatsResult, State, StreamPosition, Subscription, SubscriptionEvent,
    codes,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::connection::a_connected_client;
use super::subscriptions::{create, expect_subscription_event, has_json_data, subscribe};
use super::{
    ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, next_event_after_transport_errors, test_config,
};
use crate::support::fake_server::Rpc;
use crate::support::relay::Mode;
use crate::support::token::{connection_token_with_info, subscription_token_with_info};

/// Margin for a call's timeout to fire and its failure to reach the step.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How long the connect proxy takes to accept a connection in the
/// scenarios calling while the client connects.
const CONNECTING_FOR: Duration = Duration::from_millis(500);

/// Calls of the scenario, in the order they were made, and what the steps
/// around them set up.
#[derive(Default)]
pub(crate) struct Calls {
    made: Vec<Call>,
    /// Positions of the publications a step made to the channel, in order.
    published: Vec<StreamPosition>,
    /// The second client of a scenario with two.
    second: Option<Second>,
}

/// A second client, subscribed, with its subscription's events.
struct Second {
    /// Kept so that it stays connected.
    _client: Client,
    client_id: String,
    _subscription: Subscription,
    events: Pin<Box<dyn Stream<Item = SubscriptionEvent> + Send>>,
}

struct Call {
    method: String,
    made_at: Instant,
    outcome: Outcome,
}

/// What a call returned.
#[derive(Debug, Clone)]
enum Returned {
    Data(Vec<u8>),
    Published,
    History(HistoryResult),
    Presence(PresenceResult),
    PresenceStats(PresenceStatsResult),
}

type Settled = Result<Returned, CallError>;

enum Outcome {
    Pending(JoinHandle<Settled>),
    Settled(Settled, Instant),
}

impl Drop for Calls {
    fn drop(&mut self) {
        for call in &self.made {
            if let Outcome::Pending(task) = &call.outcome {
                task.abort();
            }
        }
    }
}

/// Makes an RPC call now, awaited by a task of its own.
fn call(world: &mut ClientWorld, method: &str, data: Vec<u8>) {
    let future = world.client().rpc(method, data);
    track(
        world,
        method,
        async move { future.await.map(Returned::Data) },
    );
}

/// Keeps a call just made, `name`d for the steps, awaited by a task of its
/// own.
fn track(
    world: &mut ClientWorld,
    name: &str,
    call: impl Future<Output = Settled> + Send + 'static,
) {
    world.calls.made.push(Call {
        method: name.to_owned(),
        made_at: Instant::now(),
        outcome: Outcome::Pending(tokio::spawn(call)),
    });
}

/// The outcome of the call to `method` (the last call when `None`), once it
/// settles within `limit`, and how long after it was made.
async fn outcome(
    world: &mut ClientWorld,
    method: Option<&str>,
    limit: Duration,
) -> (Settled, Duration) {
    let call = match method {
        Some(method) => world
            .calls
            .made
            .iter_mut()
            .find(|call| call.method == method)
            .unwrap_or_else(|| panic!("no call to {method}")),
        None => world.calls.made.last_mut().expect("no call made"),
    };
    if let Outcome::Pending(task) = &mut call.outcome {
        let result = tokio::time::timeout(limit, task)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the call to {} did not settle within {limit:?}",
                    call.method
                )
            })
            .expect("the call's task panicked");
        call.outcome = Outcome::Settled(result, Instant::now());
    }
    let Outcome::Settled(result, at) = &call.outcome else {
        unreachable!("settled above");
    };
    (result.clone(), at.duration_since(call.made_at))
}

/// The call's error, failing on a result.
fn error(result: Settled) -> CallError {
    match result {
        Ok(returned) => panic!("the call returned {returned:?}"),
        Err(error) => error,
    }
}

/// The data an RPC returned, failing otherwise.
fn data(result: Settled) -> Vec<u8> {
    match result {
        Ok(Returned::Data(data)) => data,
        other => panic!("expected RPC data, got {other:?}"),
    }
}

/// `data` as JSON.
fn json_of(data: &[u8]) -> Value {
    serde_json::from_slice(data)
        .unwrap_or_else(|_| panic!("not JSON: {:?}", String::from_utf8_lossy(data)))
}

fn assert_rpc_proxy(world: &ClientWorld) {
    assert!(
        world.proxy.is_some(),
        "this step needs the RPC proxy: tag the scenario @B"
    );
}

/// The proxy's "echo" returns its input and "busy" fails with a temporary
/// error 1000 "try later" (`ProxyBackend`'s `/rpc` route).
#[given(
    regex = r#"^a connected client and an RPC proxy answering method "(?:echo" with its input|busy" with a temporary error with code 1000 and message "try later")$"#
)]
async fn a_connected_client_and_an_rpc_proxy(world: &mut ClientWorld) {
    assert_rpc_proxy(world);
    a_connected_client(world).await;
}

/// The connect proxy accepts connections only after a delay, so that a
/// call made right after `connect()` is made while the client connects.
#[given(r#"an RPC proxy answering method "echo" with its input"#)]
fn an_rpc_proxy(world: &mut ClientWorld) {
    assert_rpc_proxy(world);
    world
        .proxy
        .as_ref()
        .expect("the RPC proxy")
        .accept_after(CONNECTING_FOR);
}

#[given(
    regex = r#"^the client calls RPC "(\w+)"(?: with (\{.*\}))?(?: (before connect is called|before it is connected))?$"#
)]
#[when(
    regex = r#"^the client calls RPC "(\w+)"(?: with (\{.*\}))?(?: (before connect is called|before it is connected))?$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_calls_rpc(world: &mut ClientWorld, method: String, data: String, when: String) {
    match when.as_str() {
        "before connect is called" => assert_eq!(world.client().state(), State::Disconnected),
        "before it is connected" => assert_eq!(world.client().state(), State::Connecting),
        _ => {}
    }
    let data = if data.is_empty() {
        "{}".to_owned()
    } else {
        data
    };
    call(world, &method, data.into_bytes());
}

#[when(regex = r#"^the client calls RPC "(\w+)" and RPC "(\w+)" concurrently$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_calls_two_rpcs(world: &mut ClientWorld, first: String, second: String) {
    call(world, &first, b"{}".to_vec());
    call(world, &second, b"{}".to_vec());
}

#[when("connect is never called")]
fn connect_is_never_called(world: &mut ClientWorld) {
    assert_eq!(world.client().state(), State::Disconnected);
}

#[then(regex = r"^the call returns (\{.*\})$")]
async fn the_call_returns(world: &mut ClientWorld, expected: String) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let data = data(result);
    assert_eq!(
        json_of(&data),
        serde_json::from_str::<Value>(&expected).unwrap()
    );
}

#[then(regex = r#"^the "(\w+)" call returns the reply to "(\w+)"$"#)]
async fn the_call_returns_the_reply_to(world: &mut ClientWorld, method: String, to: String) {
    let (result, _) = outcome(world, Some(&method), EVENT_TIMEOUT).await;
    let data = data(result);
    assert_eq!(json_of(&data), json!({"reply_to": to}));
}

#[then("the call completes once the client is connected")]
async fn the_call_completes_once_connected(world: &mut ClientWorld) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let data = data(result);
    assert_eq!(json_of(&data), json!({}));
    assert_eq!(world.client().state(), State::Connected);
    let connected = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let ClientEvent::Connected { .. } = world.next_event().await {
                return;
            }
        }
    })
    .await;
    assert!(connected.is_ok(), "no connected event");
}

#[then(regex = r#"^the call fails with a temporary error with code (\d+) and message "(.*)"$"#)]
async fn the_call_fails_with_a_temporary_error(
    world: &mut ClientWorld,
    code: u32,
    message: String,
) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let error = error(result);
    assert_eq!(
        (error.code, error.message.as_str(), error.temporary),
        (code, message.as_str(), true),
        "{error:?}"
    );
}

/// The call fails with the timeout error once the configured timeout has
/// passed, not before.
#[given("the call fails with a timeout error")]
#[when("the call fails with a timeout error")]
#[then("the call fails with a timeout error within the configured timeout plus tolerance")]
#[then("the history call fails with a timeout error within the configured timeout plus tolerance")]
async fn the_call_times_out(world: &mut ClientWorld) {
    let timeout = test_config().timeout;
    let (result, after) = outcome(world, None, timeout + TOLERANCE).await;
    let error = error(result);
    assert_eq!(error.code, codes::error::TIMEOUT, "{error:?}");
    assert!(
        after >= timeout,
        "timed out after {after:?}, before {timeout:?}"
    );
}

#[then(
    regex = r#"^the call fails with a "(disconnected|connection closed|subscription unsubscribed)" error( before the configured timeout)?$"#
)]
async fn the_call_fails_with(world: &mut ClientWorld, message: String, early: String) {
    let timeout = test_config().timeout;
    let (result, after) = outcome(world, None, EVENT_TIMEOUT).await;
    let error = error(result);
    let code = match message.as_str() {
        "disconnected" => codes::error::DISCONNECTED,
        "connection closed" => codes::error::CONNECTION_CLOSED,
        _ => codes::error::SUBSCRIPTION_UNSUBSCRIBED,
    };
    assert_eq!(
        (error.code, error.message.as_str()),
        (code, message.as_str())
    );
    if !early.is_empty() {
        assert!(after < timeout, "failed after {after:?}");
    }
}

#[given(
    regex = r"^a client connected to the fake server that (answers commands in reverse order|never replies to commands)$"
)]
async fn a_client_connected_to_a_fake_server_answering(world: &mut ClientWorld, how: String) {
    world.fake().answer_rpc(match how.as_str() {
        "answers commands in reverse order" => Rpc::Reverse,
        _ => Rpc::Ignore,
    });
    a_connected_client(world).await;
}

#[given("a client configured with the fake server's endpoint behind a relay that is down")]
fn a_client_behind_a_relay_that_is_down(world: &mut ClientWorld) {
    world.relay.as_ref().expect("a relay").set_mode(Mode::Down);
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
}

/// The client is retrying: its next event, after the failed attempts, is
/// kept for the next step.
#[when("the relay comes up")]
async fn the_relay_comes_up(world: &mut ClientWorld) {
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
    loop {
        match next_event_after_transport_errors(world).await {
            ClientEvent::Connecting { code, .. } if code == codes::connecting::CONNECT_CALLED => {}
            event => {
                world.put_back_events.push_back(event);
                return;
            }
        }
    }
}

#[then("the fake server receives no RPC command")]
async fn the_fake_server_receives_no_rpc(world: &mut ClientWorld) {
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(world.fake().rpc_commands(), Vec::<Value>::new());
}

#[given("the fake server has received the RPC command")]
async fn the_fake_server_has_received_the_rpc(world: &mut ClientWorld) {
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world.fake().rpc_commands().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(received.is_ok(), "no RPC command reached the fake server");
}

#[given(regex = r"^the connect proxy answers after (\d+) s with disconnect code (\d+)$")]
fn the_connect_proxy_answers_after(world: &mut ClientWorld, seconds: u64, code: u32) {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .disconnect_next_after(Duration::from_secs(seconds), code, "go away");
}

#[then(regex = r#"^the client emits "disconnected" with code (\d+)$"#)]
async fn the_client_emits_disconnected_with(world: &mut ClientWorld, code: u32) {
    loop {
        match next_event_after_transport_errors(world).await {
            ClientEvent::Connecting { .. } => {}
            ClientEvent::Disconnected { code: got, .. } => {
                assert_eq!(got, code);
                return;
            }
            event => panic!("expected disconnected, got {event:?}"),
        }
    }
}

#[then(regex = r#"^the client reconnects over transport "(\w+)"$"#)]
async fn the_client_reconnects_over(world: &mut ClientWorld, transport: String) {
    let connecting = world.next_event().await;
    assert!(
        matches!(connecting, ClientEvent::Connecting { .. }),
        "{connecting:?}"
    );
    loop {
        match world.next_event().await {
            ClientEvent::Error(error) if error.kind == ErrorKind::Transport => {}
            ClientEvent::Connected { transport: got, .. } => {
                assert_eq!(got, transport);
                return;
            }
            event => panic!("expected connected, got {event:?}"),
        }
    }
}

/// User, connection info and channel info of the two clients' tokens.
const USERS: [(&str, &str, &str); 2] = [
    ("alice", r#"{"name":"alice"}"#, r#"{"seat":1}"#),
    ("bob", r#"{"name":"bob"}"#, r#"{"seat":2}"#),
];
/// The channel of the scenarios with two clients.
const CHANNEL: &str = "news";

/// A client of `user` connected to the scenario's server, with its
/// connection info in its token when `info` is set, and its client id.
async fn connected_as(world: &mut ClientWorld, index: usize, info: bool) -> (Client, String) {
    let (user, conn_info, _) = USERS[index];
    let conn_info: Value = if info {
        serde_json::from_str(conn_info).unwrap()
    } else {
        Value::Null
    };
    let config = Config {
        token: connection_token_with_info(user, &conn_info),
        ..test_config()
    };
    let client = Client::builder(world.server_endpoint())
        .config(config)
        .build()
        .expect("a valid client configuration");
    let mut events = client.events();
    client.connect();
    let client_id = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match events.next().await {
                Some(ClientEvent::Connected { client_id, .. }) => return client_id,
                Some(ClientEvent::Connecting { .. }) => {}
                event => panic!("expected connected, got {event:?}"),
            }
        }
    })
    .await
    .expect("connected in time");
    (client, client_id)
}

/// A subscription of `client` to [`CHANNEL`], with its channel info in its
/// token when `info` is set.
fn subscription_of(client: &Client, index: usize, info: bool) -> Subscription {
    let (user, _, chan_info) = USERS[index];
    let mut builder = client.subscription_builder(CHANNEL);
    if info {
        let chan_info: Value = serde_json::from_str(chan_info).unwrap();
        builder = builder.token(subscription_token_with_info(user, CHANNEL, &chan_info));
    }
    builder.build().expect("no subscription to the channel yet")
}

/// The scenario's client (the first) and a second one, each of its own
/// user, subscribed to [`CHANNEL`].
async fn two_clients(world: &mut ClientWorld, info: bool) {
    let (client, client_id) = connected_as(world, 0, info).await;
    world.events = Some(super::recorded(client.events()));
    world.client_id = Some(client_id);
    world.client = Some(client);
    let subscription = subscription_of(world.client(), 0, info);
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;

    let (client, client_id) = connected_as(world, 1, info).await;
    let subscription = subscription_of(&client, 1, info);
    let mut events = super::recorded(subscription.events());
    subscription.subscribe();
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while let Some(event) = events.next().await {
            if event.name() == "subscribed" {
                return;
            }
            assert_eq!(event.name(), "subscribing", "{event:?}");
        }
    })
    .await;
    assert!(subscribed.is_ok(), "the second client did not subscribe");
    world.calls.second = Some(Second {
        _client: client,
        client_id,
        _subscription: subscription,
        events,
    });
}

fn second(world: &mut ClientWorld) -> &mut Second {
    world.calls.second.as_mut().expect("a second client")
}

#[given(
    regex = r#"^two clients of different users with connection and channel info subscribed to (?:channel "news"|a channel with presence)$"#
)]
async fn two_clients_with_info(world: &mut ClientWorld) {
    two_clients(world, true).await;
}

#[given(
    regex = r"^two clients (?:of different users )?subscribed to a channel with (?:history and presence|presence)$"
)]
async fn two_clients_without_info(world: &mut ClientWorld) {
    two_clients(world, false).await;
}

/// The call settles before the step ends.
#[when(regex = r#"^the (?:first )?client publishes (\{.*\}) to channel "(\w+)"$"#)]
async fn the_client_publishes(world: &mut ClientWorld, data: String, channel: String) {
    let publish = world.client().publish(channel, data.into_bytes());
    track(world, "publish", async move {
        publish.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(matches!(result, Ok(Returned::Published)), "{result:?}");
}

#[when(regex = r"^the first client's subscription publishes (\{.*\})$")]
async fn the_subscription_publishes(world: &mut ClientWorld, data: String) {
    let publish = world.subscription().publish(data.into_bytes());
    track(world, "publish", async move {
        publish.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(matches!(result, Ok(Returned::Published)), "{result:?}");
}

/// The second client's next publication.
async fn second_publication(world: &mut ClientWorld) -> ferrifuge::Publication {
    let events = &mut second(world).events;
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match events.next().await {
                Some(SubscriptionEvent::Publication(publication)) => return publication,
                Some(_) => {}
                None => panic!("the second client's stream ended"),
            }
        }
    })
    .await
    .expect("a publication in time")
}

#[then(regex = r"^the second client's subscription receives a publication with data (\{.*\})$")]
async fn the_second_receives(world: &mut ClientWorld, data: String) {
    let publication = second_publication(world).await;
    assert!(has_json_data(&publication, &data), "{publication:?}");
}

#[then(
    "the second client's subscription receives a publication whose info names the first client, its user, its connection info and its channel info"
)]
async fn the_second_receives_the_publisher(world: &mut ClientWorld) {
    let publication = second_publication(world).await;
    let info = publication.info.expect("the publisher's info");
    let (user, conn_info, chan_info) = USERS[0];
    assert_eq!(Some(&info.client), world.client_id.as_ref());
    assert_eq!(info.user, user);
    assert_eq!(
        info.conn_info.as_deref().map(json_of),
        Some(serde_json::from_str(conn_info).unwrap())
    );
    assert_eq!(
        info.chan_info.as_deref().map(json_of),
        Some(serde_json::from_str(chan_info).unwrap())
    );
}

/// Makes a call of `kind` on the first client's subscription, or on the
/// client for [`CHANNEL`] when `on_client`, and returns its result.
async fn ask(world: &mut ClientWorld, kind: &str, on_client: bool) -> Returned {
    let call: Pin<Box<dyn Future<Output = Settled> + Send>> = match (kind, on_client) {
        ("history", false) => {
            let call = world
                .subscription()
                .history(HistoryOptions::default().limit(-1));
            Box::pin(async move { call.await.map(Returned::History) })
        }
        ("presence", false) => {
            let call = world.subscription().presence();
            Box::pin(async move { call.await.map(Returned::Presence) })
        }
        ("presence stats", false) => {
            let call = world.subscription().presence_stats();
            Box::pin(async move { call.await.map(Returned::PresenceStats) })
        }
        ("presence", true) => {
            let call = world.client().presence(CHANNEL);
            Box::pin(async move { call.await.map(Returned::Presence) })
        }
        ("presence stats", true) => {
            let call = world.client().presence_stats(CHANNEL);
            Box::pin(async move { call.await.map(Returned::PresenceStats) })
        }
        other => unreachable!("no call {other:?}"),
    };
    track(world, kind, call);
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    result.unwrap_or_else(|error| panic!("the {kind} call failed: {error}"))
}

#[then("the first client's subscription history ends with that publication")]
async fn the_history_ends_with_it(world: &mut ClientWorld) {
    let Returned::History(history) = ask(world, "history", false).await else {
        unreachable!("a history call");
    };
    let last = history.publications.last().expect("a publication");
    assert!(has_json_data(last, r#"{"text":"hi"}"#), "{history:?}");
}

#[then("the first client's subscription presence lists both clients")]
async fn the_presence_lists_both(world: &mut ClientWorld) {
    let Returned::Presence(presence) = ask(world, "presence", false).await else {
        unreachable!("a presence call");
    };
    let mut clients: Vec<String> = presence.clients.into_keys().collect();
    clients.sort();
    let mut expected = vec![
        world.client_id.clone().expect("the first client's id"),
        second(world).client_id.clone(),
    ];
    expected.sort();
    assert_eq!(clients, expected);
}

#[then(regex = r"^the first client's subscription presence stats count (\d+) clients$")]
async fn the_presence_stats_count(world: &mut ClientWorld, clients: u32) {
    let Returned::PresenceStats(stats) = ask(world, "presence stats", false).await else {
        unreachable!("a presence stats call");
    };
    assert_eq!(stats.num_clients, clients);
}

#[when(regex = r"^one of them requests the channel's (presence|presence stats)$")]
async fn one_of_them_requests(world: &mut ClientWorld, kind: String) {
    ask(world, &kind, true).await;
}

/// The last call's result.
async fn last_returned(world: &mut ClientWorld) -> Returned {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    result.unwrap_or_else(|error| panic!("the call failed: {error}"))
}

#[then("it receives both clients with their user, connection info and channel info")]
async fn it_receives_both_clients(world: &mut ClientWorld) {
    let Returned::Presence(presence) = last_returned(world).await else {
        panic!("not a presence result");
    };
    let ids = [
        world.client_id.clone().expect("the first client's id"),
        second(world).client_id.clone(),
    ];
    assert_eq!(presence.clients.len(), 2, "{presence:?}");
    for (id, (user, conn_info, chan_info)) in ids.iter().zip(USERS) {
        let client = &presence.clients[id];
        assert_eq!(
            (client.client.as_str(), client.user.as_str()),
            (id.as_str(), user)
        );
        assert_eq!(
            client.conn_info.as_deref().map(json_of),
            Some(serde_json::from_str(conn_info).unwrap())
        );
        assert_eq!(
            client.chan_info.as_deref().map(json_of),
            Some(serde_json::from_str(chan_info).unwrap())
        );
    }
}

#[then(regex = r"^it receives (\d+) clients and (\d+) users$")]
async fn it_receives_counts(world: &mut ClientWorld, clients: u32, users: u32) {
    let Returned::PresenceStats(stats) = last_returned(world).await else {
        panic!("not a presence stats result");
    };
    assert_eq!((stats.num_clients, stats.num_users), (clients, users));
}

#[given(regex = r"^a channel with history holding (\d+) publications$")]
async fn a_channel_with_history(world: &mut ClientWorld, count: u64) {
    for n in 1..=count {
        let result = world
            .server
            .as_ref()
            .expect("this step needs a server: tag the scenario @A")
            .api()
            .call("publish", json!({"channel": CHANNEL, "data": {"n": n}}))
            .await
            .expect("publish");
        world.calls.published.push(StreamPosition {
            offset: result["offset"].as_u64().expect("an offset"),
            epoch: result["epoch"].as_str().expect("an epoch").to_owned(),
        });
    }
}

/// Requests the history of [`CHANNEL`] from a client connected for it.
async fn request_history(world: &mut ClientWorld, options: HistoryOptions) {
    a_connected_client(world).await;
    let call = world.client().history(CHANNEL, options);
    track(world, "history", async move {
        call.await.map(Returned::History)
    });
}

#[when(
    regex = r"^a connected client requests the last (\d+) publications of the channel( in reverse order)?$"
)]
async fn requests_the_last(world: &mut ClientWorld, count: usize, reverse: String) {
    let limit = i32::try_from(count).unwrap();
    let options = if reverse.is_empty() {
        // Forward, the first publications come first (verified on
        // Centrifugo v6.10.1): the last ones follow the one before them.
        let published = &world.calls.published;
        let before = published[published.len() - count - 1].clone();
        HistoryOptions::default().limit(limit).since(before)
    } else {
        HistoryOptions::default().limit(limit).reverse(true)
    };
    request_history(world, options).await;
}

#[when("a connected client requests the channel's history since the position of the first one")]
async fn requests_since_the_first(world: &mut ClientWorld) {
    let first = world.calls.published[0].clone();
    request_history(world, HistoryOptions::default().limit(-1).since(first)).await;
}

#[when("a connected client requests the channel's history with limit 0")]
async fn requests_limit_0(world: &mut ClientWorld) {
    request_history(world, HistoryOptions::default()).await;
}

/// The history the last call returned.
async fn history_returned(world: &mut ClientWorld) -> HistoryResult {
    match last_returned(world).await {
        Returned::History(history) => history,
        other => panic!("not a history result: {other:?}"),
    }
}

/// Offsets of the publications of `history`, each checked against its
/// data (`{"n": offset}`, as published).
fn offsets(history: &HistoryResult) -> Vec<u64> {
    history
        .publications
        .iter()
        .map(|publication| {
            let offset = publication.offset.expect("an offset");
            assert!(
                has_json_data(publication, &json!({"n": offset}).to_string()),
                "{publication:?}"
            );
            offset
        })
        .collect()
}

#[then(regex = r"^it receives those (\d+) publications with their offsets$")]
async fn it_receives_the_last(world: &mut ClientWorld, count: usize) {
    let history = history_returned(world).await;
    let published = &world.calls.published;
    let expected: Vec<u64> = published[published.len() - count..]
        .iter()
        .map(|position| position.offset)
        .collect();
    assert_eq!(offsets(&history), expected);
}

#[then(regex = r"^it receives the (\d+) later publications and the stream's current position$")]
async fn it_receives_the_later(world: &mut ClientWorld, count: usize) {
    let history = history_returned(world).await;
    let published = &world.calls.published;
    let expected: Vec<u64> = published[1..]
        .iter()
        .map(|position| position.offset)
        .collect();
    assert_eq!(expected.len(), count);
    assert_eq!(offsets(&history), expected);
    let top = published.last().expect("publications");
    assert_eq!(
        (history.epoch.as_str(), history.offset),
        (top.epoch.as_str(), top.offset)
    );
}

#[then("it receives the third publication then the second")]
async fn it_receives_third_then_second(world: &mut ClientWorld) {
    let history = history_returned(world).await;
    let published = &world.calls.published;
    assert_eq!(
        offsets(&history),
        [published[2].offset, published[1].offset]
    );
}

#[then("it receives no publications and the stream's current epoch and offset")]
async fn it_receives_only_the_position(world: &mut ClientWorld) {
    let history = history_returned(world).await;
    let top = world.calls.published.last().expect("publications");
    assert_eq!(history.publications, []);
    assert_eq!(
        (history.epoch.as_str(), history.offset),
        (top.epoch.as_str(), top.offset)
    );
}

#[given("a client connected to the fake server that never answers subscribe commands")]
async fn a_fake_server_never_answering_subscribes(world: &mut ClientWorld) {
    world.fake().ignore_subscribes();
    a_connected_client(world).await;
}

#[given(
    regex = r"^a client connected to the fake server that answers subscribe commands after 1 s( with a permanent error)?$"
)]
async fn a_fake_server_answering_subscribes_late(world: &mut ClientWorld, error: String) {
    let error = (!error.is_empty()).then(|| json!({"code": 403, "message": "closed"}));
    world
        .fake()
        .answer_subscribes_after(Duration::from_secs(1), error);
    a_connected_client(world).await;
}

#[given(
    r#"a client connected to the fake server with an unsubscribed subscription to channel "news""#
)]
async fn a_fake_server_and_an_unsubscribed_subscription(world: &mut ClientWorld) {
    a_connected_client(world).await;
    create(world, CHANNEL);
}

#[when(regex = r"^its history is requested(?: at once)?$")]
fn its_history_is_requested(world: &mut ClientWorld) {
    let call = world.subscription().history(HistoryOptions::default());
    track(world, "history", async move {
        call.await.map(Returned::History)
    });
}

#[then(r#"the call fails at once with a "subscription unsubscribed" error"#)]
async fn the_call_fails_at_once_unsubscribed(world: &mut ClientWorld) {
    let (result, after) = outcome(world, None, EVENT_TIMEOUT).await;
    let error = error(result);
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            codes::error::SUBSCRIPTION_UNSUBSCRIBED,
            "subscription unsubscribed"
        )
    );
    assert!(after < Duration::from_millis(500), "failed after {after:?}");
}

#[then("the fake server receives no history command")]
async fn the_fake_server_receives_no_history(world: &mut ClientWorld) {
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(world.fake().commands_with_times("history"), []);
}

#[then("the fake server receives the history command only after it answered the subscribe command")]
async fn history_only_after_the_subscribe_answer(world: &mut ClientWorld) {
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some((_, at)) = world.fake().commands_with_times("history").first() {
                return *at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a history command in time");
    let answered = *world
        .fake()
        .subscribes_answered_at()
        .first()
        .expect("the subscribe command answered");
    assert!(
        received >= answered,
        "history received before the subscribe was answered"
    );
}

#[then("the history call completes")]
async fn the_history_call_completes(world: &mut ClientWorld) {
    history_returned(world).await;
}
