//! Steps of Block 1: client connection states.

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, ErrorKind, State, codes};
use serde_json::json;

use super::{ClientWorld, QUIET_PERIOD, UNREACHABLE_ENDPOINT};

#[given("a new client")]
fn a_new_client(world: &mut ClientWorld) {
    let endpoint = match world.server {
        Some(_) => world.server_endpoint(),
        None => UNREACHABLE_ENDPOINT.to_owned(),
    };
    world.new_client(endpoint);
}

#[given("a client configured with an unreachable endpoint")]
fn a_client_with_unreachable_endpoint(world: &mut ClientWorld) {
    world.new_client(UNREACHABLE_ENDPOINT.to_owned());
}

#[given("a connected client")]
async fn a_connected_client(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    world.client().connect();
    expect_event(world, "connecting").await;
    expect_event(world, "connected").await;
}

#[given("a client disconnected by an explicit disconnect call")]
async fn a_client_disconnected_explicitly(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world.client().disconnect();
    expect_event(world, "disconnected").await;
}

#[when("connect is called")]
fn connect_is_called(world: &mut ClientWorld) {
    world.client().connect();
}

#[when("disconnect is called")]
fn disconnect_is_called(world: &mut ClientWorld) {
    world.client().disconnect();
}

#[then(expr = "the client state is {string}")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_state_is(world: &mut ClientWorld, expected: String) {
    assert_eq!(world.client().state().as_str(), expected);
}

#[then(regex = r#"^(?:then )?the client emits "(\w+)"$"#)]
async fn the_client_emits(world: &mut ClientWorld, expected: String) {
    expect_event(world, &expected).await;
}

#[then(expr = "the client reaches state {string}")]
async fn the_client_reaches_state(world: &mut ClientWorld, expected: String) {
    while world.client().state().as_str() != expected {
        world.next_event().await;
    }
}

#[then("no further connect attempt is made")]
async fn no_further_connect_attempt(world: &mut ClientWorld) {
    let events = world.events_during(QUIET_PERIOD).await;

    assert!(events.is_empty(), "unexpected events: {events:?}");
    assert_eq!(world.client().state(), State::Disconnected);
    if let Some(server) = &world.server {
        let info = server.api().call("info", json!({})).await.expect("info");
        assert_eq!(info["nodes"][0]["num_clients"], 0, "{info}");
    }
}

#[then(r#"the client emits an "error" event for each failed transport attempt"#)]
async fn an_error_for_each_failed_attempt(world: &mut ClientWorld) {
    let mut events = world.events_during(QUIET_PERIOD).await;
    // The "connecting" that connect() itself emits (see "Successful connect").
    if let Some(ClientEvent::Connecting { code, .. }) = events.first()
        && *code == codes::connecting::CONNECT_CALLED
    {
        events.remove(0);
    }

    assert!(
        events.len() >= 2,
        "expected repeated attempts, got {events:?}"
    );
    for event in &events {
        let ClientEvent::Error(error) = event else {
            panic!("expected only error events after connecting, got {events:?}");
        };
        assert_eq!(error.kind, ErrorKind::Transport, "{error:?}");
        assert_eq!(error.code, codes::error::TRANSPORT_CLOSED, "{error:?}");
        assert_eq!(error.transport.as_deref(), Some("websocket"), "{error:?}");
    }
    assert_eq!(world.client().state(), State::Connecting);
}

async fn expect_event(world: &mut ClientWorld, expected: &str) -> ClientEvent {
    let event = world.next_event().await;
    assert_eq!(event.name(), expected, "{event:?}");
    event
}
