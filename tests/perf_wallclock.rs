//! Delivery latency and idle CPU of a client (Block 14, `@P`, reported
//! only, never gating): wall-clock figures depend on the machine. The fake
//! server runs in a child process (`support::perf_server`), so only the
//! client's CPU counts. `benches/wallclock.rs` measures the decode and
//! dispatch time of the same scenario.
//!
//! Runs only with `FERRIFUGE_PERF=1`, as the non-gating CI job does:
//! `FERRIFUGE_PERF=1 cargo test --release --test perf_wallclock`. The idle
//! period is 2 minutes; `FERRIFUGE_PERF_IDLE_SECS` changes it.

// @P scenario: Wall-clock cost per publication is reported
// @P scenario: Idle CPU is reported

mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs};

use ferrifuge::{
    Client, ClientEvent, Config, State, Subscription, SubscriptionEvent, SubscriptionState,
};
use futures_util::StreamExt;
use support::perf_server::{self, ChildServer};

/// Enables the measurement.
const RUN: &str = "FERRIFUGE_PERF";
/// Overrides the idle period, in seconds.
const IDLE_SECS: &str = "FERRIFUGE_PERF_IDLE_SECS";
const IDLE: Duration = Duration::from_secs(2 * 60);
const PUBLICATIONS: usize = 10_000;
const PER_SECOND: u32 = 1000;
/// The server's ping interval in the idle scenario, in seconds.
const PING: u32 = 25;
const CHANNELS: usize = 10;

fn main() {
    if perf_server::serve_if_child() {
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("perf_wallclock skipped: set {RUN}=1 to measure");
        return;
    }
    let idle = env::var(IDLE_SECS).map_or(IDLE, |seconds| {
        Duration::from_secs(seconds.parse().expect("an idle period in seconds"))
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        delivery_latency().await;
        idle_cpu(idle).await;
    });
}

/// A client connected to `server` with channels `ch0` to `ch9` subscribed.
async fn subscribed_client(server: &ChildServer) -> (Client, Vec<Subscription>) {
    let client = Client::new(server.websocket_url(), Config::default()).expect("a valid endpoint");
    let mut events = client.events();
    client.connect();
    loop {
        match events.next().await.expect("the client's events") {
            ClientEvent::Connected { .. } => break,
            ClientEvent::Disconnected { code, reason } => panic!("disconnected: {code} {reason}"),
            _ => {}
        }
    }
    let subscriptions: Vec<_> = (0..CHANNELS)
        .map(|index| {
            let subscription = client
                .new_subscription(format!("ch{index}"))
                .expect("a new channel");
            subscription.subscribe();
            subscription
        })
        .collect();
    while subscriptions
        .iter()
        .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    (client, subscriptions)
}

fn micros_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_micros()
}

async fn delivery_latency() {
    let mut server = ChildServer::start();
    let (client, subscriptions) = subscribed_client(&server).await;
    let target = subscriptions.last().expect("a subscription");
    let mut events = target.events();

    server.publish_paced(target.channel(), PUBLICATIONS, PER_SECOND);
    let mut latencies = Vec::with_capacity(PUBLICATIONS);
    while latencies.len() < PUBLICATIONS {
        // A broken run fails rather than waiting forever.
        let event = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .expect("a publication within 10 s")
            .expect("the subscription's events");
        let received = micros_since_epoch();
        let SubscriptionEvent::Publication(publication) = event else {
            continue;
        };
        let data: serde_json::Value = serde_json::from_slice(&publication.data).expect("JSON data");
        let sent = u128::from(data["sent"].as_u64().expect("a send time"));
        latencies.push(received.saturating_sub(sent));
    }
    latencies.sort_unstable();
    let at = |fraction: f64| {
        // Indexes of a sorted sample: far below 2^52.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let index = ((latencies.len() - 1) as f64 * fraction).round() as usize;
        latencies[index]
    };
    println!(
        "@P report: delivery latency of 1 KiB publications at {PER_SECOND}/s, \
         {CHANNELS} channels: p50 {} µs, p99 {} µs (reference 1 ms), max {} µs",
        at(0.5),
        at(0.99),
        latencies.last().copied().unwrap_or_default()
    );
    drop((subscriptions, client));
}

/// CPU time this process's threads have used, from each thread's
/// `/proc/self/task/<id>/schedstat` (nanoseconds; `/proc/self/stat` counts
/// in 10 ms ticks, too coarse for an idle client); `None` where there is no
/// such file (not Linux).
fn process_cpu() -> Option<Duration> {
    let mut total = 0;
    for task in fs::read_dir("/proc/self/task").ok()? {
        let schedstat = fs::read_to_string(task.ok()?.path().join("schedstat")).ok()?;
        total += schedstat.split_whitespace().next()?.parse::<u64>().ok()?;
    }
    Some(Duration::from_nanos(total))
}

async fn idle_cpu(idle: Duration) {
    let server = ChildServer::start_pinging(PING);
    let (client, subscriptions) = subscribed_client(&server).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let before = process_cpu();
    tokio::time::sleep(idle).await;
    let after = process_cpu();

    assert_eq!(
        client.state(),
        State::Connected,
        "the client stayed connected"
    );
    match before.zip(after) {
        Some((before, after)) => {
            let used = after.saturating_sub(before);
            println!(
                "@P report: idle CPU over {idle:?} with {CHANNELS} channels and pings every \
                 {PING} s: {used:?}, {:.4}% of a core (reference 0.1%)",
                used.as_secs_f64() * 100.0 / idle.as_secs_f64()
            );
        }
        None => println!("@P report: idle CPU not measured: no /proc/self/stat here"),
    }
    drop((subscriptions, client));
}
