//! Steps of Block 3: disconnect codes.

use cucumber::{given, then, when};
use ferrifuge::ClientEvent;
use serde_json::json;

use super::ClientWorld;
use super::connection::a_connected_client;
use crate::support::proxy;

#[given("a client connected to the fake server")]
async fn a_client_connected_to_the_fake_server(world: &mut ClientWorld) {
    a_connected_client(world).await;
}

#[given(regex = r"^the connect proxy answers with disconnect code (\d+)$")]
fn proxy_answers_with_code(world: &mut ClientWorld, code: u32) {
    let reason = format!("proxy disconnect {code}");
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .disconnect_next(code, &reason);
    world.sent_disconnect = Some((code, reason));
}

#[when(regex = r"^the server API disconnects the client with code (\d+)$")]
async fn server_api_disconnects_with_code(world: &mut ClientWorld, code: u32) {
    let reason = format!("server API disconnect {code}");
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
                "disconnect": {"code": code, "reason": reason},
            }),
        )
        .await
        .expect("server API disconnect");
    world.sent_disconnect = Some((code, reason));
}

#[when(regex = r"^the fake server disconnects the client with code (\d+)$")]
fn fake_server_disconnects_with_code(world: &mut ClientWorld, code: u16) {
    let reason = format!("fake server disconnect {code}");
    world
        .fake
        .as_ref()
        .expect("this step needs the fake server: tag the scenario @D")
        .close(code, &reason);
    world.sent_disconnect = Some((code.into(), reason));
}

/// Waits for the client's reaction to the disconnect the scenario sent.
///
/// A code sent while connected moves the client to "connecting" or
/// "disconnected" with that code. A reconnect code sent while the client
/// is still connecting (e.g. by the connect proxy) causes no state change,
/// as in centrifuge-js; it shows as the client connecting again on its
/// next attempt, so "connected" counts as having stayed in "connecting".
#[then(expr = "the client goes to state {string}")]
async fn the_client_goes_to_state(world: &mut ClientWorld, expected: String) {
    let (code, _) = world.sent_disconnect.clone().expect("a disconnect sent");
    loop {
        let event = world.next_event().await;
        let reached = match &event {
            ClientEvent::Connecting { code: got, .. } if *got == code => "connecting",
            ClientEvent::Disconnected { .. } => "disconnected",
            ClientEvent::Connected { .. } => "connecting",
            // E.g. the "connecting" that connect() itself emits.
            _ => continue,
        };
        assert_eq!(reached, expected, "{event:?}");
        world.state_event = Some(event);
        return;
    }
}

#[then(regex = r"^the event carries code (\d+) and the reason sent$")]
fn the_event_carries_code_and_reason(world: &mut ClientWorld, code: u32) {
    let (_, reason) = world.sent_disconnect.clone().expect("a disconnect sent");
    let event = world.state_event.clone().expect("a state event");
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
