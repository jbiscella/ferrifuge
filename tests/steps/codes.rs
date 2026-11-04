//! Steps of Block 3: disconnect codes.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, State};
use serde_json::json;

use super::connection::a_connected_client;
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::proxy;

#[given("a client connected to the fake server")]
pub(super) async fn a_client_connected_to_the_fake_server(world: &mut ClientWorld) {
    a_connected_client(world).await;
}

#[given(regex = r"^the connect proxy answers with disconnect code (\d+)$")]
fn proxy_answers_with_code(world: &mut ClientWorld, code: u32) {
    let reason = format!("proxy disconnect {code}");
    let proxy = world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B");
    proxy.disconnect_next(code, &reason);
    // A retry stays pending, so its connecting state can be observed.
    proxy.hold_next();
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
    super::recovery::note_commands_before_disconnect(world);
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
/// "disconnected" with that code. A code the connect proxy answers while the
/// client is still connecting changes no state for a reconnect code, as in
/// centrifuge-js: there the step waits for the client's retry, which the
/// proxy holds, and checks the client's actual state at that point.
#[then(expr = "the client goes to state {string}")]
async fn the_client_goes_to_state(world: &mut ClientWorld, expected: String) {
    let (code, _) = world.sent_disconnect.clone().expect("a disconnect sent");
    if world.proxy.is_some() && expected == "connecting" {
        let retried = tokio::time::timeout(EVENT_TIMEOUT, async {
            while world.proxy.as_ref().unwrap().requests() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(retried.is_ok(), "the client did not retry");
        assert_eq!(world.client().state(), State::Connecting);
        let events = world.ready_events();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ClientEvent::Disconnected { .. })),
            "{events:?}"
        );
        return;
    }
    loop {
        let event = world.next_event().await;
        let reached = match &event {
            ClientEvent::Connecting { code: got, .. } if *got == code => "connecting",
            // The final state: it must carry the code sent.
            ClientEvent::Disconnected { code: got, .. } => {
                assert_eq!(*got, code, "{event:?}");
                "disconnected"
            }
            ClientEvent::Connected { .. } => panic!("expected {expected}, got {event:?}"),
            // E.g. the "connecting" that connect() itself emits, errors,
            // server-side subscription events and events added later.
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

#[then("the fake server received the connect command as JSON")]
fn fake_server_received_json_connect(world: &mut ClientWorld) {
    // The fake server records only text frames that parse as JSON objects.
    let received = world.fake().received();
    assert!(
        received
            .first()
            .is_some_and(|first| first["connect"].is_object()),
        "{received:?}"
    );
}
