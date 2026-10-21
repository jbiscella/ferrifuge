//! Steps of Block 4: ping/pong.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, codes};
use serde_json::json;

use super::connection::a_connected_client;
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::fake_server::Announce;
use crate::support::relay::CountingRelay;

/// Ping interval announced when a scenario does not specify one: long enough
/// that only the pings a step sends arrive.
const QUIET_PING_INTERVAL: u32 = 25;
/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How long to watch for a pong that must not be sent.
const NO_PONG_PERIOD: Duration = Duration::from_millis(500);

#[given(
    regex = r"^a client connected to the fake server that announces pong is (expected|not expected)$"
)]
async fn connected_announcing_pong(world: &mut ClientWorld, expected: String) {
    world
        .restart_fake(Announce {
            ping: QUIET_PING_INTERVAL,
            pong: expected == "expected",
        })
        .await;
    a_connected_client(world).await;
}

#[given(regex = r"^a client with maxServerPingDelay (\d+) ms$")]
fn a_client_with_max_server_ping_delay(world: &mut ClientWorld, millis: u64) {
    world.max_server_ping_delay = Some(Duration::from_millis(millis));
}

#[given(regex = r"^connected to the fake server announcing a ping interval of (\d+) s$")]
async fn connected_announcing_ping_interval(world: &mut ClientWorld, seconds: u32) {
    world
        .restart_fake(Announce {
            ping: seconds,
            pong: true,
        })
        .await;
    a_connected_client(world).await;
}

#[given(regex = r"^connected to the fake server pinging every (\d+) s$")]
async fn connected_to_pinging_fake_server(world: &mut ClientWorld, seconds: u32) {
    connected_announcing_ping_interval(world, seconds).await;
    world.fake().ping_every(Duration::from_secs(seconds.into()));
}

#[when("the fake server sends a ping")]
fn the_fake_server_sends_a_ping(world: &mut ClientWorld) {
    world.fake().send("{}");
}

#[when("the fake server stops sending pings")]
fn the_fake_server_stops_sending_pings(world: &mut ClientWorld) {
    world.fake().stop_pinging();
}

#[when(regex = r"^(\d+) s elapse$")]
async fn time_elapses(world: &mut ClientWorld, seconds: u64) {
    let connections = world.relay.as_ref().map(CountingRelay::connections);
    world.elapsed_events = world.events_during(Duration::from_secs(seconds)).await;
    world.elapsed_new_connections = world
        .relay
        .as_ref()
        .map(CountingRelay::connections)
        .zip(connections)
        .map_or(0, |(after, before)| after - before);
}

#[then("the client sends a pong")]
async fn the_client_sends_a_pong(world: &mut ClientWorld) {
    let ponged = tokio::time::timeout(EVENT_TIMEOUT, async {
        while !received_pong(world) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(ponged.is_ok(), "no pong: {:?}", world.fake().received());
}

#[then("the client sends no pong")]
async fn the_client_sends_no_pong(world: &mut ClientWorld) {
    tokio::time::sleep(NO_PONG_PERIOD).await;
    assert!(!received_pong(world), "{:?}", world.fake().received());
}

#[then(regex = r#"^within (\d+) s plus tolerance the client emits "connecting"$"#)]
async fn within_the_client_emits_connecting(world: &mut ClientWorld, seconds: u64) {
    let limit = Duration::from_secs(seconds) + TOLERANCE;
    let event = tokio::time::timeout(limit, world.next_event())
        .await
        .unwrap_or_else(|_| panic!("no event within {limit:?}"));
    assert_eq!(
        event,
        ClientEvent::Connecting {
            code: codes::connecting::NO_PING,
            reason: "no ping".into()
        }
    );
}

#[then("the client opens a new connection")]
async fn the_client_opens_a_new_connection(world: &mut ClientWorld) {
    let reconnected = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world.fake().connections() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reconnected.is_ok(), "no second connection");
}

#[then(expr = "the client stays {string}")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_stays(world: &mut ClientWorld, expected: String) {
    assert!(
        world.elapsed_events.is_empty(),
        "unexpected events: {:?}",
        world.elapsed_events
    );
    assert_eq!(
        world.elapsed_new_connections, 0,
        "the client opened a new connection"
    );
    assert_eq!(world.client().state().as_str(), expected);
}

fn received_pong(world: &ClientWorld) -> bool {
    world.fake().received().contains(&json!({}))
}
