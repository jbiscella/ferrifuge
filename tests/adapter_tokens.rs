//! Connection token callbacks run by the tokio adapter, against the fake
//! server: what the unit tests of the sans-IO core cannot reach.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config, TokenError, codes};
use futures_util::{Stream, StreamExt};
use support::fake_server::{Announce, FakeServer};
use tokio::sync::oneshot;

/// A refresh callback still running when its connection is replaced keeps
/// running while the new connection refreshes, more than once: its denial
/// disconnects the client.
#[tokio::test]
async fn a_late_refresh_denial_disconnects_after_the_new_connection_refreshed() {
    let server = FakeServer::start(Announce {
        ttl: 1,
        ..Announce::default()
    })
    .await;
    let (deny, denied) = oneshot::channel::<()>();
    let denied = Arc::new(Mutex::new(Some(denied)));
    let calls = Arc::new(AtomicUsize::new(0));
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(Config::default().token("t0"))
        .get_token({
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                // The first call, the old connection's refresh, waits for
                // the test and then denies; later ones give a token.
                let denied = denied.lock().unwrap().take();
                async move {
                    match denied {
                        Some(denied) => {
                            let _ = denied.await;
                            Err(TokenError::Unauthorized)
                        }
                        None => Ok("t2".to_owned()),
                    }
                }
            }
        })
        .build()
        .unwrap();
    let mut events = client.events();

    client.connect();
    wait_for(&mut events, |event| {
        matches!(event, ClientEvent::Connected { .. })
    })
    .await;
    wait_until(|| calls.load(Ordering::SeqCst) == 1).await;
    server.close(3001, "reconnect");
    wait_for(&mut events, |event| {
        matches!(event, ClientEvent::Connected { .. })
    })
    .await;
    // Two refreshes of the new connection: the second fetch starts when no
    // other one is running.
    wait_until(|| calls.load(Ordering::SeqCst) == 3).await;
    // Fails when the old callback was dropped: nothing then denies.
    let _ = deny.send(());

    wait_for(&mut events, |event| {
        matches!(
            event,
            ClientEvent::Disconnected { code, .. } if *code == codes::disconnected::UNAUTHORIZED
        )
    })
    .await;
}

/// Waits up to 5 s for an event matching `wanted`, skipping others.
async fn wait_for(
    events: &mut (impl Stream<Item = ClientEvent> + Unpin),
    wanted: impl Fn(&ClientEvent) -> bool,
) {
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.next().await {
            if wanted(&event) {
                return;
            }
        }
        panic!("the event stream ended");
    })
    .await;
    assert!(found.is_ok(), "no matching event within 5 s");
}

/// Waits up to 5 s for `condition`.
async fn wait_until(condition: impl Fn() -> bool) {
    let met = tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(met.is_ok(), "condition not met within 5 s");
}
