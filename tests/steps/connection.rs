//! Steps of Block 1: client connection states.

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, ErrorKind, State, codes};
use serde_json::json;

use super::{ClientWorld, QUIET_PERIOD, UNREACHABLE_ENDPOINT, expect_event};
use crate::support::proxy;
use crate::support::relay::CountingRelay;

/// Server disconnect code advising a reconnect (spec: 3000-3499, 4000-4499).
const RECONNECT_CODE: u32 = 4000;
/// Server disconnect code ending the connection for good (spec: other codes).
const TERMINAL_CODE: u32 = 4500;

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
pub(super) async fn a_connected_client(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    world.client().connect();
    expect_event(world, "connecting").await;
    let ClientEvent::Connected { client_id, .. } = expect_event(world, "connected").await else {
        unreachable!("expect_event checked the name");
    };
    world.client_id = Some(client_id);
}

#[given("the connect proxy will answer the next connect with a terminal disconnect code")]
fn proxy_answers_next_connect_with_terminal_code(world: &mut ClientWorld) {
    let reason = "terminal disconnect from the connect proxy";
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .disconnect_next(TERMINAL_CODE, reason);
    world.proxy_disconnect = Some((TERMINAL_CODE, reason.to_owned()));
}

#[when("the server disconnects the client with a reconnect-advised code")]
async fn server_disconnects_with_reconnect_code(world: &mut ClientWorld) {
    let reason = "reconnect advised by the test";
    // Connections accepted by the proxy belong to its user, others are anonymous.
    let user = if world.proxy.is_some() {
        proxy::USER
    } else {
        ""
    };
    let client_id = world.client_id.clone().expect("a connected client");
    world
        .server
        .as_ref()
        .expect("this step needs a server")
        .api()
        .call(
            "disconnect",
            json!({
                "user": user,
                "client": client_id,
                "disconnect": {"code": RECONNECT_CODE, "reason": reason},
            }),
        )
        .await
        .expect("server API disconnect");
    world.server_disconnect = Some((RECONNECT_CODE, reason.to_owned()));
}

#[given("a client disconnected by an explicit disconnect call")]
async fn a_client_disconnected_explicitly(world: &mut ClientWorld) {
    a_connected_client(world).await;
    world.client().disconnect();
    expect_event(world, "disconnected").await;
}

#[when("connect is called")]
fn connect_is_called(world: &mut ClientWorld) {
    // Some scenarios (e.g. Block 3's proxy outline) create no client first.
    if world.client.is_none() {
        let endpoint = world.server_endpoint();
        world.new_client(endpoint);
    }
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

#[then(regex = r#"^(?:then )?the client emits "(\w+)" with (that|the terminal) code and reason$"#)]
async fn the_client_emits_with_code_and_reason(
    world: &mut ClientWorld,
    expected: String,
    which: String,
) {
    let (code, reason) = match which.as_str() {
        "that" => world.server_disconnect.clone(),
        _ => world.proxy_disconnect.clone(),
    }
    .expect("a disconnect sent earlier in the scenario");

    let event = expect_event(world, &expected).await;

    let (ClientEvent::Connecting {
        code: got_code,
        reason: got_reason,
    }
    | ClientEvent::Disconnected {
        code: got_code,
        reason: got_reason,
    }) = &event
    else {
        panic!("{event:?} carries no code and reason");
    };
    assert_eq!((*got_code, got_reason.as_str()), (code, reason.as_str()));
}

#[then(expr = "the client reaches state {string}")]
async fn the_client_reaches_state(world: &mut ClientWorld, expected: String) {
    while world.client().state().as_str() != expected {
        world.next_event().await;
    }
}

#[then("no further connect attempt is made")]
async fn no_further_connect_attempt(world: &mut ClientWorld) {
    let connections = world.relay.as_ref().map(CountingRelay::connections);
    let attempts = world.proxy.as_ref().map(proxy::ConnectProxy::requests);
    let events = world.events_during(QUIET_PERIOD).await;

    assert!(events.is_empty(), "unexpected events: {events:?}");
    assert_eq!(
        world.relay.as_ref().map(CountingRelay::connections),
        connections,
        "the client opened another connection"
    );
    assert_eq!(
        world.proxy.as_ref().map(proxy::ConnectProxy::requests),
        attempts,
        "connect attempts reached the proxy"
    );
    assert_eq!(world.client().state(), State::Disconnected);
    // Not via the server's `info` statistics: they are republished about
    // every 3 s, so they lag behind the client (measured on v6.10.1).
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

#[then(regex = r#"^(?:then )?the client emits "connected" over transport "(\w+)"$"#)]
async fn the_client_emits_connected_over(world: &mut ClientWorld, expected: String) {
    let event = expect_event(world, "connected").await;
    let ClientEvent::Connected { transport, .. } = &event else {
        unreachable!("expect_event checked the name");
    };
    assert_eq!(transport, &expected, "{event:?}");
}
