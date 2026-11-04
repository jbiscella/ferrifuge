//! Allocations per publication, end to end (Block 14, `@P`): a client
//! connected over websocket to the fake server receives 10000 publications
//! of 1 KiB on one of its 1, 100 or 10000 subscribed channels. dhat counts
//! the allocations; the fake server runs in a child process
//! (`support::perf_server`), so that only the client's are counted.
//!
//! Slow under dhat, so it runs only with `FERRIFUGE_PERF=1`, as the perf
//! CI job does: `FERRIFUGE_PERF=1 cargo test --release --test perf_alloc`.

// @P scenario: Allocations per publication do not depend on the number of subscriptions

mod support;

use std::env;
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config, SubscriptionEvent, SubscriptionState};
use futures_util::StreamExt;
use support::perf_server::{self, ChildServer};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Enables the measurement.
const RUN: &str = "FERRIFUGE_PERF";
const PUBLICATIONS: usize = 10_000;
const COUNTS: [usize; 3] = [1, 100, 10_000];

fn main() {
    if perf_server::serve_if_child() {
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("perf_alloc skipped: set {RUN}=1 to measure");
        return;
    }
    let _profiler = dhat::Profiler::builder().testing().build();
    let measured = COUNTS.map(|count| (count, allocations_per_publication(count)));
    for (count, per_publication) in measured {
        println!(
            "@P report: {per_publication:.2} allocations per 1 KiB publication \
             with {count} subscribed channels"
        );
    }
    let (_, baseline) = measured[0];
    for (count, per_publication) in measured {
        assert!(
            (per_publication - baseline).abs() < 1.0,
            "{per_publication:.2} allocations per publication with {count} channels, \
             {baseline:.2} with 1"
        );
    }
}

/// The channel the publications go to: the one subscribed last.
fn target(channels: usize) -> String {
    format!("ch{}", channels - 1)
}

fn allocations_per_publication(channels: usize) -> f64 {
    let mut server = ChildServer::start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client =
            Client::new(server.websocket_url(), Config::default()).expect("a valid endpoint");
        let mut client_events = client.events();
        client.connect();
        while !matches!(
            client_events.next().await,
            Some(ClientEvent::Connected { .. })
        ) {}
        drop(client_events);

        let mut subscriptions = Vec::with_capacity(channels);
        for index in 0..channels {
            let subscription = client
                .new_subscription(format!("ch{index}"))
                .expect("a new channel");
            subscription.subscribe();
            subscriptions.push(subscription);
        }
        let target = subscriptions.last().expect("a subscription");
        assert_eq!(target.channel(), self::target(channels));
        let mut events = target.events();
        while subscriptions
            .iter()
            .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let before = dhat::HeapStats::get().total_blocks;
        server.publish(&self::target(channels), PUBLICATIONS);
        let mut received = 0;
        while received < PUBLICATIONS {
            match events.next().await.expect("the subscription's events") {
                SubscriptionEvent::Publication(_) => received += 1,
                // Queued while subscribing, before the count started.
                SubscriptionEvent::Subscribing { .. } | SubscriptionEvent::Subscribed(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        let after = dhat::HeapStats::get().total_blocks;
        #[allow(clippy::cast_precision_loss)] // Counts far below 2^52.
        let per_publication = (after - before) as f64 / PUBLICATIONS as f64;
        per_publication
    })
}
