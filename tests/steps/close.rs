//! Steps of Block 33: closing a client.

use std::time::{Duration, Instant};

use cucumber::{then, when};
use ferrifuge::{ClientEvent, HistoryOptions, ServerSubscriptionEvent, SubscriptionEvent, codes};
use futures_util::StreamExt;

use super::{ClientWorld, EVENT_TIMEOUT, test_config};

/// Slack on timing assertions for scheduling on a loaded runner.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How soon "at once" settles: the outcome is decided when the call is made.
const AT_ONCE: Duration = Duration::from_millis(500);

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
    assert!(took < AT_ONCE, "close() took {took:?}");
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

/// Checks that `outcome`, of a call made `after` ago, failed at once with
/// "client closed".
fn failed_at_once_closed(outcome: Result<(), ferrifuge::CallError>, after: Duration) {
    let error = outcome.expect_err("the call succeeded");
    assert_eq!(
        (error.code, error.message.as_str()),
        (codes::error::CLIENT_CLOSED, "client closed")
    );
    assert!(after < AT_ONCE, "failed after {after:?}");
}

#[then(r#"the subscription's history call fails at once with a "client closed" error"#)]
async fn the_history_fails_closed(world: &mut ClientWorld) {
    let started = Instant::now();
    let history = world.subscription().history(HistoryOptions::default());
    let outcome = tokio::time::timeout(EVENT_TIMEOUT, history)
        .await
        .expect("settled")
        .map(|_| ());
    failed_at_once_closed(outcome, started.elapsed());
}

#[then(r#"an RPC call fails at once with a "client closed" error"#)]
async fn an_rpc_fails_closed(world: &mut ClientWorld) {
    let started = Instant::now();
    let rpc = world.client().rpc("echo", b"{}".to_vec());
    let outcome = tokio::time::timeout(EVENT_TIMEOUT, rpc)
        .await
        .expect("settled")
        .map(|_| ());
    failed_at_once_closed(outcome, started.elapsed());
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
