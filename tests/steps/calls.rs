//! Steps of Block 13: client commands.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use std::future::Future;

use std::pin::Pin;

use ferrifuge::{
    CallError, Client, ClientEvent, ErrorKind, HistoryOptions, HistoryResult, PresenceResult,
    PresenceStatsResult, Protocol, State, StreamPosition, Subscription, SubscriptionEvent, codes,
};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::connection::a_connected_client;
use super::subscriptions::{create, expect_subscription_event, has_json_data, subscribe};
use super::{
    ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, STALL_PROOF_TIMEOUT,
    next_event_after_transport_errors, test_config,
};
use crate::support::fake_server::{Rpc, protobuf_results};
use crate::support::protobuf;
use crate::support::relay::Mode;
use crate::support::token::{connection_token_with_info, subscription_token_with_info};

/// Margin for a call's timeout to fire and its failure to reach the step.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How long the connect proxy takes to accept a connection in the
/// scenarios calling while the client connects: 500 ms was too short on a
/// loaded CI runner (the client was connected when the step called).
const CONNECTING_FOR: Duration = Duration::from_secs(2);

/// Calls of the scenario, in the order they were made, and what the steps
/// around them set up.
#[derive(Default)]
pub(crate) struct Calls {
    made: Vec<Call>,
    /// Positions of the publications a step made to the channel, in order.
    published: Vec<StreamPosition>,
    /// The second client of a scenario with two.
    second: Option<Second>,
    /// The third client of a join and leave scenario.
    third: Option<Second>,
    /// The write-queue limit of the scenario's client (Block 13d, @D).
    pub(crate) write_queue_bytes: Option<usize>,
    /// The sans-IO core of an @E write-queue scenario.
    queue: Option<QueueRun>,
    /// When a step that should settle the calls at once (a disconnect, a
    /// lost transport) ran: tells a runner stall from a client that failed
    /// to settle (Windows flake, #138, #157, #174).
    pub(crate) settling_step_at: Option<Instant>,
}

/// A second client, subscribed, with its subscription's events.
struct Second {
    /// Kept so that it stays connected.
    _client: Client,
    client_id: String,
    subscription: Subscription,
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
    /// The task awaiting the call; it gives the outcome and when it came.
    Pending(JoinHandle<(Settled, Instant)>),
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
pub(super) fn call(world: &mut ClientWorld, method: &str, data: Vec<u8>) {
    let made_at = Instant::now();
    let future = world.client().rpc(method, data);
    track(world, method, made_at, async move {
        future.await.map(Returned::Data)
    });
}

/// Keeps a call made at `made_at`, `name`d for the steps, awaited by a
/// task of its own. `made_at` is read before the call is made: the driver,
/// on another thread, may arm its timeout before the call method returns.
fn track(
    world: &mut ClientWorld,
    name: &str,
    made_at: Instant,
    call: impl Future<Output = Settled> + Send + 'static,
) {
    world.calls.made.push(Call {
        method: name.to_owned(),
        made_at,
        // When the call settled, not when a step looks: a loaded runner may
        // run the next step late.
        outcome: Outcome::Pending(tokio::spawn(async move {
            let settled = call.await;
            (settled, Instant::now())
        })),
    });
}

/// The error the last call failed with (Block 37c).
pub(super) async fn last_error(world: &mut ClientWorld) -> CallError {
    match outcome(world, None, EVENT_TIMEOUT).await.0 {
        Err(error) => error,
        Ok(returned) => panic!("the call returned {returned:?}"),
    }
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
        let (result, at) = tokio::time::timeout(limit, task)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the call to {} did not settle within {limit:?}",
                    call.method
                )
            })
            .expect("the call's task panicked");
        call.outcome = Outcome::Settled(result, at);
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
    // A pong, sent every ping interval, could take a rejection programmed
    // for "the next emulation request" before the call: the call's own
    // request is the one rejected.
    if world.emulation_rejection
        && let Some(proxy) = world.http_proxy.as_ref()
    {
        proxy.reject_next_emulation_carrying(b"rpc");
    }
    call(world, &method, data.into_bytes());
    // A connect answer held for this step goes out once the call is made.
    if let Some(proxy) = world.proxy.as_ref() {
        proxy.release_held_connect();
    }
}

// Block 45a: map_publish and map_remove on the client. The key may be
// empty, which the server refuses.
#[cfg(feature = "unstable")]
#[when(
    regex = r#"^the client calls map_publish on "([^"]*)" with key "([^"]*)" and data (\{.*\})$"#
)]
fn the_client_calls_map_publish(
    world: &mut ClientWorld,
    channel: String,
    key: String,
    data: String,
) {
    let made_at = Instant::now();
    let future = world.client().map_publish(channel, key, data.into_bytes());
    track(world, "map_publish", made_at, async move {
        future.await.map(|()| Returned::Published)
    });
}

#[cfg(feature = "unstable")]
#[when(regex = r#"^the client calls map_remove on "([^"]*)" with key "([^"]*)"$"#)]
fn the_client_calls_map_remove(world: &mut ClientWorld, channel: String, key: String) {
    let made_at = Instant::now();
    let future = world.client().map_remove(channel, key);
    track(world, "map_remove", made_at, async move {
        future.await.map(|()| Returned::Published)
    });
}

// Block 45: a map write the server accepts.
#[then("the call succeeds")]
async fn the_call_succeeds(world: &mut ClientWorld) {
    if let Err(error) = outcome(world, None, EVENT_TIMEOUT).await.0 {
        panic!("the call failed: {error:?}");
    }
}

#[then(regex = r"^the call fails with code (\d+)$")]
async fn the_call_fails_with_code(world: &mut ClientWorld, code: u32) {
    assert_eq!(last_error(world).await.code, code);
}

#[when(regex = r#"^the client calls RPC "(\w+)" and RPC "(\w+)" concurrently$"#)]
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
#[then("the call fails with a timeout error")]
#[then("the call fails with a timeout error within the configured timeout plus tolerance")]
#[then("the history call fails with a timeout error within the configured timeout plus tolerance")]
async fn the_call_times_out(world: &mut ClientWorld) {
    if let Some(run) = &world.calls.queue {
        run.assert_failed(run.last(), codes::error::TIMEOUT);
        return;
    }
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
    regex = r#"^the call fails with a "(disconnected|connection closed|subscription unsubscribed|client closed)" error( before the configured timeout)?$"#
)]
async fn the_call_fails_with(world: &mut ClientWorld, message: String, early: String) {
    let timeout = world.configured_timeout();
    let made_at = world.calls.made.last().map(|call| call.made_at);
    let (result, after) = outcome(world, None, EVENT_TIMEOUT).await;
    // When the fake server read each RPC command, after the call: tells a
    // command written late from a step run late (Windows flake, #138, #157).
    let read_after: Vec<Duration> = match (&world.fake, made_at) {
        (Some(fake), Some(made_at)) => fake
            .commands_with_times("rpc")
            .iter()
            .map(|(_, at)| at.saturating_duration_since(made_at))
            .collect(),
        _ => Vec::new(),
    };
    let settling_step_after = made_at
        .zip(world.calls.settling_step_at)
        .map(|(made_at, at)| at.saturating_duration_since(made_at));
    let error = error(result);
    let code = match message.as_str() {
        "disconnected" => codes::error::DISCONNECTED,
        "connection closed" => codes::error::CONNECTION_CLOSED,
        "client closed" => codes::error::CLIENT_CLOSED,
        _ => codes::error::SUBSCRIPTION_UNSUBSCRIBED,
    };
    // The time tells a slow runner (a call made more than the timeout
    // before the step that settles it) from a client failing to settle it.
    assert_eq!(
        (error.code, error.message.as_str()),
        (code, message.as_str()),
        "settled {after:?} after the call was made; RPC commands read by the fake server {read_after:?} after it; the settling step ran {settling_step_after:?} after it"
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

/// Its scenarios settle the call in a later step, so their clients have
/// a call timeout no runner stall reaches ([`STALL_PROOF_TIMEOUT`], set
/// before the scenario runs by `bdd.rs`, as the client is created by an
/// earlier step that scenarios timing a call out share): at the 5 s
/// default, a stall between this step and the settling one let the
/// timeout settle the call first (Windows, #185: the disconnect ran
/// 6.02 s after the call).
pub(crate) const PENDING_CALL_STEP: &str = "the fake server has received the RPC command";

#[given("the fake server has received the RPC command")]
async fn the_fake_server_has_received_the_rpc(world: &mut ClientWorld) {
    assert_eq!(
        world.configured_timeout(),
        STALL_PROOF_TIMEOUT,
        "a scenario with this step runs with a call timeout no stall reaches"
    );
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world.fake().rpc_commands().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(received.is_ok(), "no RPC command reached the fake server");
    // The later steps need the call still pending: a runner so slow that
    // the call timed out by now cannot run the scenario (seen on Windows).
    if let Some(call) = world.calls.made.last() {
        let elapsed = call.made_at.elapsed();
        assert!(
            elapsed < STALL_PROOF_TIMEOUT,
            "the RPC command reached the fake server {elapsed:?} after the call"
        );
    }
}

/// The delay counts from the step that acts while the client connects
/// ("the client calls RPC … before it is connected", "ready is awaited on
/// the client"), which releases it: counted from the request, a runner
/// stalled for longer than the delay found the client disconnected
/// already (providers fips, #125). Until then the client waits for the
/// answer, under a connect timeout no stall reaches.
#[given(regex = r"^the connect proxy answers after (\d+) s with disconnect code (\d+)$")]
fn the_connect_proxy_answers_after(world: &mut ClientWorld, seconds: u64, code: u32) {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .disconnect_next_after_release(Duration::from_secs(seconds), code, "go away");
    world.timeout = Some(super::STALL_PROOF_TIMEOUT);
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

/// User, connection info and channel info of the clients' tokens.
const USERS: [(&str, &str, &str); 3] = [
    ("alice", r#"{"name":"alice"}"#, r#"{"seat":1}"#),
    ("bob", r#"{"name":"bob"}"#, r#"{"seat":2}"#),
    ("carol", r#"{"name":"carol"}"#, r#"{"seat":3}"#),
];
/// The channel of the scenarios with two clients.
const CHANNEL: &str = "news";

/// A client of `user` connected to the scenario's server with `protocol`,
/// with its connection info in its token when `info` is set, and its client
/// id.
async fn connected_as(
    world: &mut ClientWorld,
    index: usize,
    info: bool,
    protocol: Protocol,
) -> (Client, String) {
    let (user, conn_info, _) = USERS[index];
    let conn_info: Value = if info {
        serde_json::from_str(conn_info).unwrap()
    } else {
        Value::Null
    };
    let config = test_config()
        .token(connection_token_with_info(user, &conn_info))
        .protocol(protocol);
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
    let protocol = world.protocol;
    two_clients_of(world, info, protocol).await;
}

/// As [`two_clients`], the second client using `second` (Block 38).
pub(super) async fn two_clients_of(world: &mut ClientWorld, info: bool, second: Protocol) {
    let (client, client_id) = connected_as(world, 0, info, world.protocol).await;
    world.events = Some(super::recorded(client.events()));
    world.client_id = Some(client_id);
    world.client = Some(client);
    let subscription = subscription_of(world.client(), 0, info);
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;

    let (client, client_id) = connected_as(world, 1, info, second).await;
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
        subscription,
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
    let made_at = Instant::now();
    let publish = world.client().publish(channel, data.into_bytes());
    track(world, "publish", made_at, async move {
        publish.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(matches!(result, Ok(Returned::Published)), "{result:?}");
}

#[when(regex = r"^the first client's subscription publishes (\{.*\})$")]
async fn the_subscription_publishes(world: &mut ClientWorld, data: String) {
    let made_at = Instant::now();
    let publish = world.subscription().publish(data.into_bytes());
    track(world, "publish", made_at, async move {
        publish.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(matches!(result, Ok(Returned::Published)), "{result:?}");
}

/// The second client's next publication.
pub(super) async fn second_publication(world: &mut ClientWorld) -> ferrifuge::Publication {
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
    let made_at = Instant::now();
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
    track(world, kind, made_at, call);
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
    let made_at = Instant::now();
    let call = world.client().history(CHANNEL, options);
    track(world, "history", made_at, async move {
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
    let made_at = Instant::now();
    let call = world.subscription().history(HistoryOptions::default());
    track(world, "history", made_at, async move {
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

fn hex_bytes(text: &str) -> Vec<u8> {
    text.split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("hex byte"))
        .collect()
}

/// Waits until the fake server has received a Protobuf command `matching`.
async fn protobuf_command(
    world: &ClientWorld,
    matching: impl Fn(&protobuf::Command) -> bool,
) -> protobuf::Command {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(command) = world.fake().received_protobuf().into_iter().find(&matching) {
                return command;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the command reached the fake server")
}

/// The fake server answers every Protobuf RPC with
/// [`protobuf_results::RPC_DATA`], the bytes fe 00 ff.
#[given(
    r#"a client configured with the Protobuf protocol connected to the fake server that answers RPC "bin" with the bytes fe 00 ff"#
)]
async fn a_protobuf_client_and_a_fake_rpc(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    a_connected_client(world).await;
}

#[when(regex = r#"^the client calls RPC "(\w+)" with the bytes ([0-9a-f ]+)$"#)]
fn the_client_calls_rpc_with_bytes(world: &mut ClientWorld, method: String, bytes: String) {
    call(world, &method, hex_bytes(&bytes));
}

#[then(regex = r"^the fake server received a Protobuf RPC command with the bytes ([0-9a-f ]+)$")]
async fn the_fake_server_received_protobuf_rpc(world: &mut ClientWorld, bytes: String) {
    let command = protobuf_command(world, |command| command.rpc.is_some()).await;
    let rpc = command.rpc.expect("an RPC command");
    assert_eq!(rpc.data, hex_bytes(&bytes));
    assert_eq!(rpc.method, "bin");
}

#[then(regex = r"^the call returns the bytes ([0-9a-f ]+)$")]
async fn the_call_returns_bytes(world: &mut ClientWorld, bytes: String) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert_eq!(data(result), hex_bytes(&bytes));
}

#[given(r#"two clients configured with the Protobuf protocol subscribed to channel "news""#)]
async fn two_protobuf_clients(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    two_clients(world, false).await;
}

#[when(regex = r#"^the first client publishes the bytes ([0-9a-f ]+) to channel "(\w+)"$"#)]
async fn the_first_client_publishes_bytes(world: &mut ClientWorld, bytes: String, channel: String) {
    let made_at = Instant::now();
    let publish = world.client().publish(channel, hex_bytes(&bytes));
    track(world, "publish", made_at, async move {
        publish.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(matches!(result, Ok(Returned::Published)), "{result:?}");
}

#[then(
    regex = r"^the second client's subscription receives a publication with the bytes ([0-9a-f ]+)$"
)]
async fn the_second_receives_bytes(world: &mut ClientWorld, bytes: String) {
    let publication = second_publication(world).await;
    assert_eq!(publication.data, hex_bytes(&bytes));
}

#[when(regex = r"^the fake server sends a message push whose data is the non-canonical JSON (.*)$")]
fn the_fake_server_sends_a_message(world: &mut ClientWorld, data: String) {
    world
        .fake()
        .send(&format!(r#"{{"push":{{"message":{{"data":{data}}}}}}}"#));
    world.last_raw_data = Some(data.into_bytes());
}

#[when(regex = r"^the fake server sends a Protobuf message push with the bytes ([0-9a-f ]+)$")]
fn the_fake_server_sends_a_protobuf_message(world: &mut ClientWorld, bytes: String) {
    world.fake().send_protobuf(&[protobuf::Reply {
        push: Some(protobuf::Push {
            message: Some(protobuf::Message {
                data: hex_bytes(&bytes),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }]);
}

/// The data of the client's next "message" event.
async fn next_message(world: &mut ClientWorld) -> Vec<u8> {
    match world.next_event().await {
        ClientEvent::Message(data) => data,
        event => panic!("expected a message event, got {event:?}"),
    }
}

#[then(r#"the client emits a "message" event whose data is exactly those bytes"#)]
async fn the_client_emits_the_exact_message(world: &mut ClientWorld) {
    let data = next_message(world).await;
    assert_eq!(Some(data), world.last_raw_data);
}

#[then(regex = r#"^the client emits a "message" event with the bytes ([0-9a-f ]+)$"#)]
async fn the_client_emits_a_binary_message(world: &mut ClientWorld, bytes: String) {
    assert_eq!(next_message(world).await, hex_bytes(&bytes));
}

/// The call settles before the step ends.
#[when(regex = r"^the client sends (?:the bytes ([0-9a-f ]+)|(\{.*\}))$")]
async fn the_client_sends(world: &mut ClientWorld, bytes: String, json: String) {
    let data = if json.is_empty() {
        hex_bytes(&bytes)
    } else {
        json.into_bytes()
    };
    let made_at = Instant::now();
    let send = world.client().send(data);
    track(world, "send", made_at, async move {
        send.await.map(|()| Returned::Published)
    });
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    assert!(result.is_ok(), "{result:?}");
}

#[then(regex = r"^the fake server received a send command with data (\{.*\}) and no command id$")]
async fn the_fake_server_received_a_send(world: &mut ClientWorld, data: String) {
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some((command, _)) = world.fake().commands_with_times("send").pop() {
                return command;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a send command in time");
    assert_eq!(received.get("id"), None, "{received}");
    assert_eq!(
        received["send"]["data"],
        serde_json::from_str::<Value>(&data).unwrap()
    );
}

#[then(
    regex = r"^the fake server received a Protobuf send command with the bytes ([0-9a-f ]+) and no command id$"
)]
async fn the_fake_server_received_a_protobuf_send(world: &mut ClientWorld, bytes: String) {
    let command = protobuf_command(world, |command| command.send.is_some()).await;
    assert_eq!(command.id, 0);
    assert_eq!(
        command.send.expect("a send command").data,
        hex_bytes(&bytes)
    );
}

#[when(regex = r"^the client calls (rpc|history|presence|presence_stats)$")]
fn the_client_calls(world: &mut ClientWorld, command: String) {
    let client = world.client();
    let made_at = Instant::now();
    let call: Pin<Box<dyn Future<Output = Settled> + Send>> = match command.as_str() {
        "rpc" => {
            let call = client.rpc("bin", vec![0xff, 0x00, 0xfe]);
            Box::pin(async move { call.await.map(Returned::Data) })
        }
        "history" => {
            let call = client.history(CHANNEL, HistoryOptions::default().limit(1));
            Box::pin(async move { call.await.map(Returned::History) })
        }
        "presence" => {
            let call = client.presence(CHANNEL);
            Box::pin(async move { call.await.map(Returned::Presence) })
        }
        _ => {
            let call = client.presence_stats(CHANNEL);
            Box::pin(async move { call.await.map(Returned::PresenceStats) })
        }
    };
    track(world, &command, made_at, call);
}

#[then(
    regex = r"^the fake server received the (rpc|history|presence|presence_stats) command in Protobuf$"
)]
async fn the_fake_server_received_the_command(world: &mut ClientWorld, command: String) {
    let received = protobuf_command(world, |received| match command.as_str() {
        "rpc" => received.rpc.is_some(),
        "history" => received.history.is_some(),
        "presence" => received.presence.is_some(),
        _ => received.presence_stats.is_some(),
    })
    .await;
    match command.as_str() {
        "rpc" => assert_eq!(received.rpc.unwrap().data, [0xff, 0x00, 0xfe]),
        "history" => {
            let history = received.history.unwrap();
            assert_eq!((history.channel.as_str(), history.limit), (CHANNEL, 1));
        }
        "presence" => assert_eq!(received.presence.unwrap().channel, CHANNEL),
        _ => assert_eq!(received.presence_stats.unwrap().channel, CHANNEL),
    }
}

#[then(
    regex = r"^the call returns the fake server's Protobuf result for (rpc|history|presence|presence_stats)$"
)]
async fn the_call_returns_the_protobuf_result(world: &mut ClientWorld, command: String) {
    match (command.as_str(), last_returned(world).await) {
        ("rpc", Returned::Data(data)) => assert_eq!(data, protobuf_results::RPC_DATA),
        ("history", Returned::History(history)) => {
            let expected = protobuf_results::history();
            let publication = &history.publications[0];
            assert_eq!(history.publications.len(), 1);
            assert_eq!(publication.data, expected.publications[0].data);
            assert_eq!(publication.offset, Some(expected.publications[0].offset));
            assert_eq!(
                (history.epoch, history.offset),
                (expected.epoch, expected.offset)
            );
        }
        ("presence", Returned::Presence(presence)) => {
            let expected = protobuf_results::presence();
            assert_eq!(presence.clients.len(), expected.presence.len());
            for (id, info) in expected.presence {
                let client = &presence.clients[&id];
                assert_eq!(
                    (client.client.as_str(), client.user.as_str()),
                    (info.client.as_str(), info.user.as_str())
                );
                assert_eq!(client.conn_info.as_deref(), Some(&info.conn_info[..]));
                assert_eq!(client.chan_info.as_deref(), Some(&info.chan_info[..]));
            }
        }
        ("presence_stats", Returned::PresenceStats(stats)) => {
            let expected = protobuf_results::presence_stats();
            assert_eq!(
                (stats.num_clients, stats.num_users),
                (expected.num_clients, expected.num_users)
            );
        }
        (command, returned) => panic!("{command}: unexpected {returned:?}"),
    }
}

/// An @E write-queue scenario: the sans-IO core, connected, its publishes
/// and what they became. Timers fire when a step says so.
struct QueueRun {
    machine: ferrifuge::machine::Machine,
    transport: ferrifuge::machine::TransportId,
    /// Each publish: its size, call, fill character and frame once sent.
    publishes: Vec<Publish>,
    /// The actions of the last input.
    last: Vec<ferrifuge::machine::Action>,
    /// Calls settled so far, with their outcome.
    settled: Vec<(
        ferrifuge::machine::CallId,
        Result<ferrifuge::CallReply, CallError>,
    )>,
}

struct Publish {
    size: usize,
    call: ferrifuge::machine::CallId,
    /// Fills its data, telling its frame apart.
    fill: char,
    frame: Option<ferrifuge::machine::FrameId>,
}

/// Bytes of a JSON publish frame besides its data's characters.
const PUBLISH_OVERHEAD: usize = 50;

impl QueueRun {
    fn connected(write_queue_bytes: usize, pong: bool) -> Self {
        use ferrifuge::machine::{Action, Input, Machine};
        let mut machine = Machine::new(test_config().write_queue_bytes(write_queue_bytes));
        let transport = machine
            .handle(Input::Connect)
            .into_iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(transport),
                _ => None,
            })
            .expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));
        let mut run = Self {
            machine,
            transport,
            publishes: Vec::new(),
            last: Vec::new(),
            settled: Vec::new(),
        };
        run.connect_reply(1, pong);
        run
    }

    fn connect_reply(&mut self, id: u32, pong: bool) {
        let reply = json!({"id": id, "connect": {"client": "c", "ping": 25, "pong": pong}});
        self.frame(&reply.to_string());
        assert_eq!(self.machine.state(), State::Connected);
    }

    fn handle(&mut self, input: ferrifuge::machine::Input) {
        use ferrifuge::machine::Action;
        self.last = self.machine.handle(input);
        for action in &self.last {
            if let Action::CallSettled { call, result } = action {
                self.settled.push((*call, result.clone()));
            }
        }
        let sent = self.sent();
        for publish in &mut self.publishes {
            if publish.frame.is_none() {
                publish.frame = sent
                    .iter()
                    .find(|(_, text)| text.contains(&publish.fill.to_string().repeat(8)))
                    .map(|(id, _)| *id);
            }
        }
    }

    fn frame(&mut self, text: &str) {
        self.handle(ferrifuge::machine::Input::Frame {
            transport: self.transport,
            data: text.as_bytes().to_vec(),
        });
    }

    /// The frames the last input sent, with their ids.
    fn sent(&self) -> Vec<(ferrifuge::machine::FrameId, String)> {
        self.last
            .iter()
            .filter_map(|action| match action {
                ferrifuge::machine::Action::Send { id, frame, .. } => {
                    Some((*id, String::from_utf8_lossy(frame).into_owned()))
                }
                _ => None,
            })
            .collect()
    }

    /// Publishes about `size` bytes, its data filled with the next letter.
    fn publish(&mut self, size: usize) {
        let fill = char::from(b'a' + u8::try_from(self.publishes.len()).unwrap());
        let data = format!("\"{}\"", fill.to_string().repeat(size - PUBLISH_OVERHEAD));
        let call = self.machine.add_call(ferrifuge::CallRequest::Publish {
            channel: "news".into(),
            data: data.into_bytes(),
        });
        self.publishes.push(Publish {
            size,
            call,
            fill,
            frame: None,
        });
        self.handle(ferrifuge::machine::Input::Call(call));
    }

    fn by_size(&self, size: usize) -> &Publish {
        self.publishes
            .iter()
            .find(|publish| publish.size == size)
            .unwrap_or_else(|| panic!("no publish of {size} bytes"))
    }

    fn last(&self) -> ferrifuge::machine::CallId {
        self.publishes.last().expect("a publish").call
    }

    /// Whether the last input sent the publish of `size` bytes.
    fn sent_now(&self, size: usize) -> bool {
        let fill = self.by_size(size).fill.to_string().repeat(8);
        self.sent().iter().any(|(_, text)| text.contains(&fill))
    }

    fn publishes_sent_now(&self) -> Vec<String> {
        self.sent()
            .into_iter()
            .map(|(_, text)| text)
            .filter(|text| text.contains("publish"))
            .collect()
    }

    fn assert_failed(&self, call: ferrifuge::machine::CallId, code: u32) {
        let outcome = self
            .settled
            .iter()
            .find(|(settled, _)| *settled == call)
            .unwrap_or_else(|| panic!("{call:?} not settled: {:?}", self.last));
        assert!(
            matches!(&outcome.1, Err(error) if error.code == code),
            "{outcome:?}"
        );
    }

    /// Fires the timer `timer` of `call`, which the machine must have set.
    fn fire(&mut self, call: ferrifuge::machine::CallId, timer: ferrifuge::machine::CallTimer) {
        self.handle(ferrifuge::machine::Input::CallTimerFired(call, timer));
    }
}

fn queue(world: &mut ClientWorld) -> &mut QueueRun {
    world
        .calls
        .queue
        .as_mut()
        .expect("an @E write-queue scenario")
}

#[given(
    regex = r"^a connected client with a write queue of (\d+) bytes(, whose server expects pongs)?$"
)]
fn a_client_with_a_write_queue(world: &mut ClientWorld, bytes: usize, pongs: String) {
    world.calls.queue = Some(QueueRun::connected(bytes, !pongs.is_empty()));
}

#[given(regex = r"^a publish of (\d+) bytes sent and not yet written$")]
fn a_publish_sent(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    run.publish(size);
    assert!(run.sent_now(size), "not sent: {:?}", run.last);
}

#[given(regex = r"^a publish of (\d+) bytes waiting for room$")]
fn a_publish_waiting(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    run.publish(size);
    assert_eq!(run.publishes_sent_now(), Vec::<String>::new());
}

#[when(regex = r"^the client publishes (\d+) bytes(?: and then (\d+) bytes)?$")]
fn the_client_publishes_bytes(world: &mut ClientWorld, first: usize, second: String) {
    let run = queue(world);
    run.publish(first);
    if !second.is_empty() {
        let earlier = run.publishes_sent_now();
        run.publish(second.parse().unwrap());
        assert_eq!(earlier, Vec::<String>::new(), "the first publish was sent");
    }
}

#[then(regex = r"^no command is sent for (?:it|either)$")]
fn no_command_is_sent(world: &mut ClientWorld) {
    let run = queue(world);
    assert_eq!(
        run.publishes_sent_now(),
        Vec::<String>::new(),
        "{:?}",
        run.last
    );
}

#[when("the transport reports the first publish written")]
fn the_first_publish_is_written(world: &mut ClientWorld) {
    let run = queue(world);
    let frame = run.publishes[0].frame.expect("the first publish sent");
    let transport = run.transport;
    run.handle(ferrifuge::machine::Input::FrameWritten { transport, frame });
}

#[then("the second publish is sent")]
fn the_second_publish_is_sent(world: &mut ClientWorld) {
    let run = queue(world);
    let size = run.publishes[1].size;
    assert!(run.sent_now(size), "{:?}", run.last);
}

#[then(regex = r"^the (\d+)-byte publish is sent$")]
fn the_publish_is_sent(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    assert!(run.sent_now(size), "{:?}", run.last);
}

#[then(regex = r"^the (\d+)-byte publish is sent before the (\d+)-byte one$")]
fn sent_in_order(world: &mut ClientWorld, first: usize, second: usize) {
    let run = queue(world);
    let fills = [first, second].map(|size| run.by_size(size).fill.to_string().repeat(8));
    let sent = run.publishes_sent_now();
    let position = |fill: &String| sent.iter().position(|text| text.contains(fill));
    let (first, second) = (position(&fills[0]), position(&fills[1]));
    assert!(
        first.is_some() && second.is_some() && first < second,
        "{sent:?}"
    );
}

#[when("the configured timeout elapses")]
fn the_configured_timeout_elapses(world: &mut ClientWorld) {
    let run = queue(world);
    let call = run.last();
    run.fire(call, ferrifuge::machine::CallTimer::Room);
}

#[when("the server sends a ping")]
fn the_server_sends_a_ping(world: &mut ClientWorld) {
    if super::batching::drives_a_machine(world) {
        super::batching::server_ping(world);
        return;
    }
    queue(world).frame("{}");
}

#[then("the client sends a pong at once")]
fn the_client_sends_a_pong(world: &mut ClientWorld) {
    let run = queue(world);
    assert!(
        run.sent().iter().any(|(_, text)| text == "{}"),
        "{:?}",
        run.last
    );
}

#[when(regex = r"^the (\d+)-byte publish is abandoned$")]
fn the_publish_is_abandoned(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    let call = run.by_size(size).call;
    run.handle(ferrifuge::machine::Input::CallAbandoned(call));
}

#[when("the transport is lost")]
fn the_transport_is_lost(world: &mut ClientWorld) {
    // A Block 46 core.
    if world.batching.run.is_some() {
        super::debouncing::transport_lost(world);
        return;
    }
    let run = queue(world);
    let transport = run.transport;
    run.handle(ferrifuge::machine::Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    // Losing the transport keeps the waiting call's deadline: no new one.
    let waiting = run.last();
    assert!(
        !run.last.iter().any(|action| matches!(
            action,
            ferrifuge::machine::Action::SetCallTimer(call, ..) if *call == waiting
        )),
        "{:?}",
        run.last
    );
}

#[then(r#"the first publish fails with a "connection closed" error"#)]
fn the_first_publish_fails_closed(world: &mut ClientWorld) {
    let run = queue(world);
    run.assert_failed(run.publishes[0].call, codes::error::CONNECTION_CLOSED);
}

#[when(
    regex = r"^the configured timeout elapses since the (\d+)-byte publish was made, the client still connecting$"
)]
fn the_timeout_elapses_while_connecting(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    assert_eq!(run.machine.state(), State::Connecting);
    let call = run.by_size(size).call;
    run.fire(call, ferrifuge::machine::CallTimer::Room);
}

#[then(regex = r"^the (\d+)-byte publish fails with a timeout error$")]
fn the_publish_times_out(world: &mut ClientWorld, size: usize) {
    let run = queue(world);
    run.assert_failed(run.by_size(size).call, codes::error::TIMEOUT);
}

#[when("the client is connected again")]
fn the_client_is_connected_again(world: &mut ClientWorld) {
    use ferrifuge::machine::{Action, Input, Timer};
    let run = queue(world);
    run.handle(Input::TimerFired(Timer::Reconnect));
    let transport = run
        .last
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a new transport");
    run.transport = transport;
    run.handle(Input::TransportOpened(transport));
    let connect: Value = run
        .sent()
        .iter()
        .find_map(|(_, text)| serde_json::from_str::<Value>(text).ok())
        .expect("a connect command");
    let id = u32::try_from(connect["id"].as_u64().expect("a command id")).unwrap();
    run.connect_reply(id, false);
}

#[given(regex = r"^a client with a write queue of (\d+) bytes? connected to the fake server$")]
async fn a_client_with_a_small_write_queue(world: &mut ClientWorld, bytes: usize) {
    world.calls.write_queue_bytes = Some(bytes);
    a_connected_client(world).await;
}

#[when(r#"the client sends {"n":1}, {"n":2} and {"n":3} without awaiting them"#)]
fn the_client_sends_three(world: &mut ClientWorld) {
    for n in 1..=3 {
        let made_at = Instant::now();
        let send = world
            .client()
            .send(json!({"n": n}).to_string().into_bytes());
        track(world, &format!("send {n}"), made_at, async move {
            send.await.map(|()| Returned::Published)
        });
    }
}

#[then("every send completes before the configured timeout")]
async fn every_send_completes(world: &mut ClientWorld) {
    let timeout = test_config().timeout;
    for n in 1..=3 {
        let (result, after) = outcome(world, Some(&format!("send {n}")), EVENT_TIMEOUT).await;
        assert!(result.is_ok(), "send {n}: {result:?}");
        assert!(after < timeout, "send {n} took {after:?}");
    }
}

#[then("the fake server received the 3 send commands in that order")]
async fn the_fake_server_received_three_sends(world: &mut ClientWorld) {
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            let sends = world.fake().commands_with_times("send");
            if sends.len() >= 3 {
                return sends;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("3 send commands in time");
    let data: Vec<Value> = received
        .iter()
        .map(|(command, _)| command["send"]["data"].clone())
        .collect();
    assert_eq!(data, [json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]);
}

/// Another client of user `index`, subscribed to [`CHANNEL`], asking for
/// join and leave pushes when `join_leave` is set.
async fn another_client(world: &mut ClientWorld, index: usize, join_leave: bool) -> Second {
    let (client, client_id) = connected_as(world, index, false, world.protocol).await;
    let subscription = client
        .subscription_builder(CHANNEL)
        .join_leave(join_leave)
        .build()
        .expect("no subscription to the channel yet");
    let mut events = super::recorded(subscription.events());
    subscription.subscribe();
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while let Some(event) = events.next().await {
            if event.name() == "subscribed" {
                return;
            }
        }
    })
    .await;
    assert!(subscribed.is_ok(), "client {index} did not subscribe");
    Second {
        _client: client,
        client_id,
        subscription,
        events,
    }
}

#[given(
    regex = r"^a client subscribed (with|without) the join_leave option to a channel with join and leave events$"
)]
async fn a_client_subscribed_for_join_leave(world: &mut ClientWorld, with: String) {
    a_connected_client(world).await;
    let subscription = world
        .client()
        .subscription_builder(CHANNEL)
        .join_leave(with == "with")
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[given("a second client subscribed to the channel with the join_leave option")]
async fn a_second_client_with_join_leave(world: &mut ClientWorld) {
    let second = another_client(world, 1, true).await;
    world.calls.second = Some(second);
}

#[when("a second client subscribes to the channel")]
async fn a_second_client_subscribes(world: &mut ClientWorld) {
    let second = another_client(world, 1, false).await;
    world.calls.second = Some(second);
}

#[when("the second client unsubscribes from the channel")]
fn the_second_client_unsubscribes(world: &mut ClientWorld) {
    second(world).subscription.unsubscribe();
}

#[when("a third client subscribes to the channel and then unsubscribes")]
async fn a_third_client_comes_and_goes(world: &mut ClientWorld) {
    let third = another_client(world, 2, false).await;
    third.subscription.unsubscribe();
    world.calls.third = Some(third);
}

/// Waits for a join or leave event naming `client` among `events`,
/// skipping the others (a subscriber may get its own join).
async fn expect_join_or_leave(
    events: &mut Pin<Box<dyn Stream<Item = SubscriptionEvent> + Send>>,
    kind: &str,
    client: &str,
) {
    let found = tokio::time::timeout(EVENT_TIMEOUT, async {
        while let Some(event) = events.next().await {
            match (&event, kind) {
                (SubscriptionEvent::Join(info), "join")
                | (SubscriptionEvent::Leave(info), "leave")
                    if info.client == client =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(found.is_ok(), "no {kind} naming {client}");
}

#[then(regex = r#"^the subscription emits a "(join|leave)" event naming the second client$"#)]
async fn the_subscription_emits_join_or_leave(world: &mut ClientWorld, kind: String) {
    let client = second(world).client_id.clone();
    let events = world
        .subscription_events
        .as_mut()
        .expect("no subscription created yet");
    expect_join_or_leave(events, &kind, &client).await;
}

#[when(
    regex = r#"^the second client's subscription has emitted a "leave" event naming the third client$"#
)]
async fn the_second_saw_the_third_leave(world: &mut ClientWorld) {
    let third = world
        .calls
        .third
        .as_ref()
        .expect("a third client")
        .client_id
        .clone();
    let events = &mut second(world).events;
    expect_join_or_leave(events, "leave", &third).await;
}

#[then(r#"the first client's subscription has emitted no "join" or "leave" event"#)]
async fn the_first_saw_no_join_or_leave(world: &mut ClientWorld) {
    let seen = world.subscription_events_during(QUIET_PERIOD).await;
    assert!(
        !seen.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Join(_) | SubscriptionEvent::Leave(_)
        )),
        "{seen:?}"
    );
}

#[when(regex = r#"^a subscription to channel "(\w+)" with the join_leave option is subscribed$"#)]
async fn a_join_leave_subscription_is_subscribed(world: &mut ClientWorld, channel: String) {
    let subscription = world
        .client()
        .subscription_builder(channel)
        .join_leave(true)
        .build()
        .expect("no subscription to the channel yet");
    world.track_subscription(subscription);
    subscribe(world).await;
    expect_subscription_event(world, "subscribed", false).await;
}

#[then(
    regex = r#"^the fake server received (\d+) subscribe commands for "(\w+)", each requesting join_leave$"#
)]
fn the_fake_server_received_join_leave_subscribes(
    world: &mut ClientWorld,
    count: usize,
    channel: String,
) {
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
            .all(|message| message["subscribe"]["join_leave"] == true),
        "{subscribes:?}"
    );
}

/// The client info of the join and of the leave the fake server pushes.
fn pushed_info(kind: &str) -> Value {
    if kind == "join" {
        json!({"client": "c-join", "user": "u-join", "conn_info": {"j": 1}, "chan_info": {"k": 2}})
    } else {
        json!({"client": "c-leave", "user": "u-leave", "conn_info": {"j": 3}, "chan_info": {"k": 4}})
    }
}

fn push_join_or_leave(world: &ClientWorld, kind: &str, channel: &str) {
    world.fake().send(
        &json!({"push": {"channel": channel, kind: {"info": pushed_info(kind)}}}).to_string(),
    );
}

#[when(
    regex = r#"^the fake server pushes a join and then a leave of different clients for channel "(\w+)"$"#
)]
fn the_fake_server_pushes_join_and_leave(world: &mut ClientWorld, channel: String) {
    push_join_or_leave(world, "join", &channel);
    push_join_or_leave(world, "leave", &channel);
}

#[when(regex = r#"^the fake server pushes a (join|leave) for channel "(\w+)"$"#)]
fn the_fake_server_pushes(world: &mut ClientWorld, kind: String, channel: String) {
    push_join_or_leave(world, &kind, &channel);
}

#[then(
    regex = r#"^(?:then )?the client emits a server-side "(join|leave)" event for channel "(\w+)" with the pushed client info$"#
)]
async fn the_client_emits_a_server_side_join_or_leave(
    world: &mut ClientWorld,
    kind: String,
    channel: String,
) {
    let event = loop {
        if let ClientEvent::ServerSubscription {
            channel: got,
            event,
        } = world.next_event().await
            && got == channel
            && matches!(
                event,
                ferrifuge::ServerSubscriptionEvent::Join(_)
                    | ferrifuge::ServerSubscriptionEvent::Leave(_)
            )
        {
            break event;
        }
    };
    let (name, info) = match event {
        ferrifuge::ServerSubscriptionEvent::Join(info) => ("join", info),
        ferrifuge::ServerSubscriptionEvent::Leave(info) => ("leave", info),
        _ => unreachable!("matched above"),
    };
    assert_eq!(name, kind);
    let expected = pushed_info(&kind);
    assert_eq!(info.client, expected["client"]);
    assert_eq!(info.user, expected["user"]);
    assert_eq!(
        info.conn_info.as_deref().map(json_of),
        Some(expected["conn_info"].clone())
    );
    assert_eq!(
        info.chan_info.as_deref().map(json_of),
        Some(expected["chan_info"].clone())
    );
}

/// Without taking its events: a later step reads them.
#[when("the subscription is subscribed")]
async fn the_subscription_is_subscribed(world: &mut ClientWorld) {
    let subscription = world.subscription().clone();
    subscription.subscribe();
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while subscription.state() != ferrifuge::SubscriptionState::Subscribed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(subscribed.is_ok(), "not subscribed in time");
}

#[then(
    r#"the subscription's stream yields "subscribing", "subscribed" and "leave", and no "join" event"#
)]
async fn yields_subscribed_then_leave_only(world: &mut ClientWorld) {
    let mut names = Vec::new();
    for _ in 0..3 {
        names.push(world.next_subscription_event(false).await.name());
    }
    assert_eq!(names, ["subscribing", "subscribed", "leave"]);
    let seen = world.subscription_events_during(QUIET_PERIOD).await;
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Join(_))),
        "{seen:?}"
    );
}
