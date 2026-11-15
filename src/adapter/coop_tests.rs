//! Stream reads take part in tokio's cooperative scheduling on the tokio
//! adapter (Block 63, D1): a loop over a stream that a large recovery filled
//! gives its runtime a turn, loses no event when a read is dropped on such a
//! turn, and never waits for budget outside a tokio runtime.

use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt as _};

use crate::test_util::Server;
use crate::{Client, StreamPosition, Subscription, SubscriptionEvent};

/// The publications one subscribe reply recovers.
const RECOVERED: u64 = 1000;

/// The bound of the reading loop.
const BOUND: Duration = Duration::from_secs(5);

/// A client and its subscription to "news", whose stream is taken, on the
/// current runtime or on `runtime`; once the subscribe reply that recovers
/// [`RECOVERED`] publications from offset 0 is handled, all its events are
/// queued, none taken.
async fn recovered(
    server: &Server,
    runtime: Option<tokio::runtime::Handle>,
) -> (
    Client,
    Subscription,
    impl Stream<Item = SubscriptionEvent> + Unpin,
) {
    for n in 1..=RECOVERED {
        server.publish("news", format!(r#"{{"n":{n}}}"#).into_bytes());
    }
    let mut builder = Client::builder(server.endpoint()).connector(server.connector());
    if let Some(runtime) = runtime {
        builder = builder.runtime(runtime);
    }
    let client = builder.build().expect("a client");
    let subscription = client
        .subscription_builder("news")
        .since(StreamPosition::new(0, "double"))
        .build()
        .expect("a subscription");
    let events = subscription.events();
    client.connect();
    subscription.subscribe();
    // `subscribing` and `subscribed`, then the recovered publications.
    let queued = usize::try_from(RECOVERED).expect("fits") + 2;
    tokio::time::timeout(BOUND, async {
        while subscription.held_events() < queued {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the recovery queued");
    (client, subscription, events)
}

/// The offsets of the publications among `events`, which must be 1 to
/// [`RECOVERED`] in order, each once.
fn assert_recovered(offsets: &[u64]) {
    let expected: Vec<u64> = (1..=RECOVERED).collect();
    assert_eq!(offsets, expected, "the recovered publications, in order");
}

/// A loop that only awaits `next()` takes the whole recovery, while a task
/// beside it gets its turns.
#[tokio::test(flavor = "current_thread")]
async fn a_loop_over_a_large_recovery_gives_the_runtime_its_turns() {
    let server = Server::new();
    let (_client, _subscription, mut events) = recovered(&server, None).await;
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = tokio::spawn({
        let ticks = Arc::clone(&ticks);
        async move {
            loop {
                ticks.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        }
    });
    // The ticker's first turn, so that only the loop's turns count below.
    tokio::task::yield_now().await;
    let before = ticks.load(Ordering::Relaxed);
    let offsets = tokio::time::timeout(BOUND, async {
        let mut offsets = Vec::new();
        while offsets.len() < usize::try_from(RECOVERED).expect("fits") {
            match events.next().await {
                Some(SubscriptionEvent::Publication(publication)) => {
                    offsets.push(publication.offset.expect("an offset"));
                }
                Some(_) => {}
                None => panic!("the stream ended"),
            }
        }
        offsets
    })
    .await
    .expect("the recovery taken within the bound");
    let turns = ticks.load(Ordering::Relaxed) - before;
    ticker.abort();
    assert_recovered(&offsets);
    assert!(
        turns >= 7,
        "the ticker ran {turns} times while the loop took {RECOVERED} events"
    );
}

/// A `next()` dropped right after it returned `Pending` for budget, with
/// events queued, loses none of them.
#[tokio::test(flavor = "current_thread")]
async fn a_read_dropped_on_a_budget_turn_loses_no_event() {
    let server = Server::new();
    let (_client, subscription, mut events) = recovered(&server, None).await;
    let mut offsets = Vec::new();
    let mut dropped = 0;
    tokio::time::timeout(BOUND, async {
        while offsets.len() < usize::try_from(RECOVERED).expect("fits") {
            // One poll of a fresh read; a `Pending` drops it.
            let polled = std::future::poll_fn(|cx| {
                let mut next = events.next();
                Poll::Ready(Pin::new(&mut next).poll(cx))
            })
            .await;
            match polled {
                Poll::Ready(Some(SubscriptionEvent::Publication(publication))) => {
                    offsets.push(publication.offset.expect("an offset"));
                }
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => panic!("the stream ended"),
                Poll::Pending => {
                    assert!(
                        subscription.held_events() > 0,
                        "a Pending with nothing queued, after {} publications",
                        offsets.len()
                    );
                    dropped += 1;
                    // The read woke the task: it runs again after a turn.
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("the recovery taken within the bound");
    assert_recovered(&offsets);
    assert!(dropped > 0, "no read returned Pending for budget");
}

/// Native, unconstrained budget: outside any tokio runtime the budget never
/// runs out, and taking the whole recovery never returns `Pending`.
#[test]
fn outside_a_runtime_reads_never_wait_for_budget() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");
    let server = Server::new();
    let (client, subscription, mut events) =
        runtime.block_on(recovered(&server, Some(runtime.handle().clone())));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut offsets = Vec::new();
    while offsets.len() < usize::try_from(RECOVERED).expect("fits") {
        match Pin::new(&mut events).poll_next(&mut cx) {
            Poll::Ready(Some(SubscriptionEvent::Publication(publication))) => {
                offsets.push(publication.offset.expect("an offset"));
            }
            Poll::Ready(Some(_)) => {}
            Poll::Ready(None) => panic!("the stream ended"),
            Poll::Pending => panic!(
                "Pending outside a runtime, after {} publications",
                offsets.len()
            ),
        }
    }
    assert_recovered(&offsets);
    drop((events, subscription, client));
    runtime.shutdown_background();
}
