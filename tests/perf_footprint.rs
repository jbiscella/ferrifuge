//! Memory footprint of a client (Block 14, `@P`): live heap counted by dhat
//! from just before the client is created, with the fake server in a child
//! process (`support::perf_server`) so that only the client's memory
//! counts. Each scenario uses a fresh server and client.
//!
//! Slow under dhat, so it runs only with `FERRIFUGE_PERF=1`, as the perf
//! CI job does: `FERRIFUGE_PERF=1 cargo test --release --test perf_footprint`.

// Reported figures only: byte counts are far below 2^52.
#![allow(clippy::cast_precision_loss)]

// @P scenario: Idle footprint with ten subscriptions
// @P scenario: Marginal cost of a subscription
// @P scenario: Memory stays flat across reconnects

mod support;

use std::env;
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config, Subscription, SubscriptionState};
use futures_util::{Stream, StreamExt};
use support::perf_server::{self, ChildServer};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Enables the measurement.
const RUN: &str = "FERRIFUGE_PERF";
const KIB: usize = 1024;
/// How long a client counts as idle before its footprint is read.
const IDLE: Duration = Duration::from_secs(1);
const IDLE_FOOTPRINT_LIMIT: usize = 1024 * KIB;
const MORE_CHANNELS: usize = 1000;
const MARGINAL_LIMIT: usize = 2 * KIB;
const RECONNECT_CYCLES: usize = 10_000;
/// A disconnect code from the server that advises reconnecting (Block 3).
const RECONNECT_ADVISED: u16 = 3000;

fn main() {
    if perf_server::serve_if_child() {
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("perf_footprint skipped: set {RUN}=1 to measure");
        return;
    }
    let _profiler = dhat::Profiler::builder().testing().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        idle_footprint_with_ten_subscriptions().await;
        marginal_cost_of_a_subscription().await;
        memory_stays_flat_across_reconnects().await;
    });
}

fn live_bytes() -> usize {
    dhat::HeapStats::get().curr_bytes
}

/// A client connected to `server`.
async fn connected(server: &ChildServer, config: Config) -> Client {
    let client = Client::new(server.websocket_url(), config).expect("a valid endpoint");
    let mut events = client.events();
    client.connect();
    wait_connected(&mut events).await;
    client
}

async fn wait_connected(events: &mut (impl Stream<Item = ClientEvent> + Unpin)) {
    loop {
        match events.next().await.expect("the client's events") {
            ClientEvent::Connected { .. } => return,
            ClientEvent::Disconnected { code, reason } => panic!("disconnected: {code} {reason}"),
            _ => {}
        }
    }
}

/// Subscribes channels `ch<from>` to `ch<to - 1>`.
fn subscribe(client: &Client, channels: std::ops::Range<usize>) -> Vec<Subscription> {
    channels
        .map(|index| {
            let subscription = client
                .new_subscription(format!("ch{index}"))
                .expect("a new channel");
            subscription.subscribe();
            subscription
        })
        .collect()
}

async fn wait_subscribed(subscriptions: &[Subscription]) {
    while subscriptions
        .iter()
        .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn idle_footprint_with_ten_subscriptions() {
    let server = ChildServer::start();
    let before = live_bytes();
    let client = connected(&server, Config::default()).await;
    let subscriptions = subscribe(&client, 0..10);
    wait_subscribed(&subscriptions).await;
    tokio::time::sleep(IDLE).await;

    let footprint = live_bytes() - before;

    println!(
        "@P report: idle footprint with 10 subscriptions: {:.1} KiB (limit {} KiB)",
        footprint as f64 / KIB as f64,
        IDLE_FOOTPRINT_LIMIT / KIB
    );
    assert!(footprint <= IDLE_FOOTPRINT_LIMIT, "{footprint} bytes");
    drop((subscriptions, client));
}

async fn marginal_cost_of_a_subscription() {
    let server = ChildServer::start();
    let client = connected(&server, Config::default()).await;
    tokio::time::sleep(IDLE).await;
    let before = live_bytes();

    // The handles count too: an application holds them.
    let subscriptions = subscribe(&client, 0..MORE_CHANNELS);
    wait_subscribed(&subscriptions).await;
    tokio::time::sleep(IDLE).await;
    let per_subscription = (live_bytes() - before) / MORE_CHANNELS;

    println!(
        "@P report: marginal cost of a subscription: {per_subscription} bytes \
         (limit {MARGINAL_LIMIT})"
    );
    assert!(per_subscription <= MARGINAL_LIMIT);
    drop((subscriptions, client));
}

async fn memory_stays_flat_across_reconnects() {
    let mut server = ChildServer::start();
    // Reconnects at once, so that 10000 cycles take seconds, not hours.
    let config = Config {
        min_reconnect_delay: Duration::from_millis(1),
        max_reconnect_delay: Duration::from_millis(2),
        ..Config::default()
    };
    let client = connected(&server, config).await;
    let mut events = client.events();
    let subscriptions = subscribe(&client, 0..10);
    wait_subscribed(&subscriptions).await;

    let mut after_first = 0;
    for cycle in 1..=RECONNECT_CYCLES {
        server.close(RECONNECT_ADVISED);
        wait_connected(&mut events).await;
        wait_subscribed(&subscriptions).await;
        if cycle == 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            after_first = live_bytes();
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_last = live_bytes();

    let change = after_last.abs_diff(after_first);
    println!(
        "@P report: live heap after reconnect cycle 1: {after_first} bytes, after cycle \
         {RECONNECT_CYCLES}: {after_last} bytes ({:+.3}%, limit 1%)",
        (after_last as f64 - after_first as f64) * 100.0 / after_first as f64
    );
    assert!(change * 100 <= after_first, "{change} bytes apart");
    drop((subscriptions, events));
    drop(client);
}
