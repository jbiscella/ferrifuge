//! Steps of Block 33: closing a client.

use std::time::{Duration, Instant};

use cucumber::{then, when};
use ferrifuge::{ClientEvent, HistoryOptions, ServerSubscriptionEvent, SubscriptionEvent, codes};
use futures_util::{FutureExt as _, StreamExt};

use super::{ClientWorld, EVENT_TIMEOUT, test_config};

/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How soon "at once" settles: the outcome is decided when the call is made.
/// "At once" for `close()`, which waits for the driver to end and so is
/// never settled at its first poll: well within the configured timeout
/// it must not wait out, with room for a loaded runner's scheduling (CI
/// health, finding 7: 500 ms).
const CLOSE_AT_ONCE: Duration = Duration::from_secs(2);

/// How long the last `close()` took, once it resolved.
#[derive(Debug, Default)]
pub(crate) struct Closing {
    took: Option<Duration>,
}

#[when("close is awaited")]
async fn close_is_awaited(world: &mut ClientWorld) {
    let limit = test_config().timeout + TOLERANCE;
    let started = Instant::now();
    let closing = world.client().close();
    tokio::time::timeout(limit, closing)
        .await
        .unwrap_or_else(|_| panic!("close() did not resolve within {limit:?}"));
    world.close.took = Some(started.elapsed());
}

#[then("close resolves")]
fn close_resolves(world: &mut ClientWorld) {
    assert!(world.close.took.is_some(), "close() not awaited");
}

#[then("close resolves at once")]
fn close_resolves_at_once(world: &mut ClientWorld) {
    let took = world.close.took.expect("close() awaited");
    assert!(took < CLOSE_AT_ONCE, "close() took {took:?}");
}

#[then("the client is closed")]
fn the_client_is_closed(world: &mut ClientWorld) {
    assert!(world.client().is_closed());
}

#[then("the subscription's stream ends")]
async fn the_subscriptions_stream_ends(world: &mut ClientWorld) {
    // Events queued before the close come first.
    while let Some(event) = world.next_subscription_item().await {
        assert!(
            matches!(event, SubscriptionEvent::Unsubscribed { .. }),
            "after the close: {event:?}"
        );
    }
}

#[then("the fake server received no unsubscribe command")]
fn no_unsubscribe_command(world: &mut ClientWorld) {
    let unsubscribes: Vec<_> = world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command.get("unsubscribe").is_some())
        .collect();
    assert!(unsubscribes.is_empty(), "{unsubscribes:?}");
}

#[then(
    regex = r#"^the client emits a server-side "unsubscribed" event for channel "(\w+)" with code (\d+)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn server_side_unsubscribed(world: &mut ClientWorld, channel: String, code: u32) {
    loop {
        match world.next_event().await {
            ClientEvent::ServerSubscription {
                channel: got,
                event: ServerSubscriptionEvent::Unsubscribed { code: got_code, .. },
            } if got == channel => {
                assert_eq!(got_code, code);
                return;
            }
            // Publications and the like queued before the close.
            ClientEvent::ServerSubscription { .. } => {}
            event => panic!("expected the server-side unsubscribed, got {event:?}"),
        }
    }
}

#[then(regex = r#"^then the client emits "disconnected" with code (\d+)$"#)]
async fn then_disconnected(world: &mut ClientWorld, code: u32) {
    let event = world.next_event().await;
    assert!(
        matches!(event, ClientEvent::Disconnected { code: got, .. } if got == code),
        "{event:?}"
    );
}

#[when("the client is dropped")]
fn the_client_is_dropped(world: &mut ClientWorld) {
    drop(world.client.take().expect("a client"));
}

/// Checks that `call` fails at once with "client closed": decided when it
/// is made, at its first poll, rather than within some wall time, which a
/// loaded runner exceeds (CI health, finding 7).
fn fails_at_once_closed<T: std::fmt::Debug>(
    call: impl std::future::Future<Output = Result<T, ferrifuge::CallError>>,
) {
    let outcome = std::pin::pin!(call)
        .now_or_never()
        .expect("the call was not settled at its first poll");
    let error = outcome.expect_err("the call succeeded");
    assert_eq!(
        (error.code, error.message.as_str()),
        (codes::error::CLIENT_CLOSED, "client closed")
    );
}

#[then(r#"the subscription's history call fails at once with a "client closed" error"#)]
fn the_history_fails_closed(world: &mut ClientWorld) {
    fails_at_once_closed(world.subscription().history(HistoryOptions::default()));
}

#[then(r#"an RPC call fails at once with a "client closed" error"#)]
fn an_rpc_fails_closed(world: &mut ClientWorld) {
    fails_at_once_closed(world.client().rpc("echo", b"{}".to_vec()));
}

#[then(r#"the client's stream ends with no "disconnected" event"#)]
async fn the_stream_ends_without_disconnected(world: &mut ClientWorld) {
    let events = world.events.as_mut().expect("no client created yet");
    let ended = tokio::time::timeout(EVENT_TIMEOUT, async {
        while let Some(event) = events.next().await {
            assert!(
                !matches!(event, ClientEvent::Disconnected { .. }),
                "{event:?}"
            );
        }
    })
    .await;
    assert!(ended.is_ok(), "the stream did not end");
}
