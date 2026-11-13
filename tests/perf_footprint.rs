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
// @P scenario: A closed window frees its memory
//
// Block 48 (structural acceptance, @P): with `unstable-webtransport`, the
// live heap of an idle client connected to the fake WebTransport server is
// reported, not gated while WebTransport is unstable.

mod support;

use std::env;
use std::time::Duration;

use ferrifuge::machine::{
    Action, DebounceTimer, FrameId, Input, Machine, SubscriptionId, SubscriptionOptions,
    TransportId,
};
use ferrifuge::{CallRequest, Client, ClientEvent, Config, Subscription, SubscriptionState};
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
        #[cfg(feature = "unstable")]
        {
            marginal_cost_of_a_shared_poll_subscription().await;
            bytes_per_tracked_key().await;
        }
        memory_stays_flat_across_reconnects().await;
        #[cfg(feature = "unstable-webtransport")]
        idle_footprint_over_webtransport().await;
    });
    a_closed_window_frees_its_memory();
    #[cfg(feature = "unstable")]
    bytes_per_tracked_key_with_a_base();
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

/// Reported only (Block 48): QUIC's buffers are bounded by D12, and the
/// transport is unstable.
#[cfg(feature = "unstable-webtransport")]
async fn idle_footprint_over_webtransport() {
    support::tls::create();
    let server = ChildServer::start_webtransport();
    let before = live_bytes();
    let client = Client::builder_with_transports([(
        ferrifuge::machine::TransportKind::WebTransport,
        server.webtransport_url(),
    )])
    .config(Config::default())
    .root_certificates([support::tls::ca_der()])
    .proxy(ferrifuge::Proxy::none())
    .build()
    .expect("a valid client");
    let mut events = client.events();
    client.connect();
    wait_connected(&mut events).await;
    let subscriptions = subscribe(&client, 0..10);
    wait_subscribed(&subscriptions).await;
    tokio::time::sleep(IDLE).await;

    let footprint = live_bytes() - before;

    println!(
        "@P report: idle footprint over WebTransport with 10 subscriptions: {:.1} KiB (not a gate while unstable)",
        footprint as f64 / KIB as f64
    );
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

/// Block 47, D3: a shared poll subscription tracking no key costs what a
/// subscription does, its state boxed.
#[cfg(feature = "unstable")]
async fn marginal_cost_of_a_shared_poll_subscription() {
    let server = ChildServer::start();
    let client = connected(&server, Config::default()).await;
    tokio::time::sleep(IDLE).await;
    let before = live_bytes();

    let subscriptions: Vec<ferrifuge::SharedPollSubscription> = (0..MORE_CHANNELS)
        .map(|index| {
            let subscription = client
                .new_shared_poll_subscription(format!("poll{index}"))
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
    tokio::time::sleep(IDLE).await;
    let per_subscription = (live_bytes() - before) / MORE_CHANNELS;

    println!(
        "@P report: marginal cost of a shared poll subscription tracking no key: \
         {per_subscription} bytes (limit {MARGINAL_LIMIT})"
    );
    assert!(per_subscription <= MARGINAL_LIMIT);
    drop((subscriptions, client));
}

/// Block 47, D11: tracked keys cost memory per key, reported: 16-byte keys
/// under signatures of 80 bytes covering 100 keys each.
#[cfg(feature = "unstable")]
async fn bytes_per_tracked_key() {
    const KEYS: usize = 10_000;
    let server = ChildServer::start();
    let client = connected(&server, Config::default()).await;
    let subscription = client
        .new_shared_poll_subscription("poll")
        .expect("a new channel");
    tokio::time::sleep(IDLE).await;
    let before = live_bytes();

    // Not subscribed: kept for the next subscribe, nothing sent.
    for batch in 0..KEYS / 100 {
        subscription.track_with_signature(
            (0..100).map(|key| {
                ferrifuge::SharedPollTrackItem::new(format!("{:016}", batch * 100 + key), 0)
            }),
            "s".repeat(80),
        );
    }
    let per_key = (live_bytes() - before) / KEYS;

    println!("@P report: bytes per tracked shared poll key: {per_key}");
    drop((subscription, client));
}

/// Block 47, D11: with delta on, each tracked key also holds its last
/// value as its delta base; reported on the core, 16-byte keys whose values
/// are 64 bytes.
#[cfg(feature = "unstable")]
fn bytes_per_tracked_key_with_a_base() {
    const KEYS: usize = 10_000;
    const VALUE: usize = 64;
    let mut machine = Machine::new(Config::default());
    let transport = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(Input::Frame {
        transport,
        data: br#"{"id":1,"connect":{"client":"c"}}"#.to_vec(),
    });
    let mut options = ferrifuge::machine::SharedPollSubscriptionOptions::default();
    options.common = SubscriptionOptions::default().delta(ferrifuge::DeltaFormat::Fossil);
    let subscription = machine
        .add_shared_poll_subscription("poll".into(), options)
        .expect("a new channel");
    machine.handle(Input::Subscribe(subscription));
    machine.handle(Input::Frame {
        transport,
        data: br#"{"id":2,"subscribe":{"type":4,"delta":true}}"#.to_vec(),
    });
    let key = |number: usize| format!("{number:016}");
    for batch in 0..KEYS / 100 {
        machine.handle(Input::TrackKeysWithSignature(
            subscription,
            (0..100)
                .map(|number| ferrifuge::SharedPollTrackItem::new(key(batch * 100 + number), 0))
                .collect(),
            "s".repeat(80),
        ));
    }
    let value = format!("\"{}\"", "x".repeat(VALUE - 2));
    let before = live_bytes();

    for number in 0..KEYS {
        let push = serde_json::json!({"push": {"channel": "poll", "pub": {
            "key": key(number), "data": value, "version": 1,
        }}});
        machine.handle(Input::Frame {
            transport,
            data: push.to_string().into_bytes(),
        });
    }
    let per_key = (live_bytes() - before) / KEYS;

    println!(
        "@P report: bytes per tracked shared poll key for its delta base, with \
         {VALUE}-byte values: {per_key}"
    );
    drop(machine);
}

async fn memory_stays_flat_across_reconnects() {
    let mut server = ChildServer::start();
    // Reconnects at once, so that 10000 cycles take seconds, not hours.
    let config = Config::default()
        .min_reconnect_delay(Duration::from_millis(1))
        .max_reconnect_delay(Duration::from_millis(2));
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

/// Windows of the Block 46 scenario.
const WINDOWS: usize = 10_000;

/// A machine connected over websocket with `WINDOWS` subscriptions, each
/// advised an interval (Block 46), its transport and the subscriptions.
fn subscribed_core() -> (Machine, TransportId, Vec<SubscriptionId>) {
    let mut machine = Machine::new(Config::default()).with_publish_debounce();
    let transport = machine
        .handle(Input::Connect)
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport");
    machine.handle(Input::TransportOpened(transport));
    machine.handle(Input::Frame {
        transport,
        data: br#"{"id":1,"connect":{"client":"c"}}"#.to_vec(),
    });
    let mut subscriptions = Vec::with_capacity(WINDOWS);
    for number in 0..WINDOWS {
        let subscription = machine
            .add_subscription(format!("cursors:{number}"), SubscriptionOptions::default())
            .expect("a new channel");
        machine.handle(Input::Subscribe(subscription));
        subscriptions.push(subscription);
        let id = number + 2;
        machine.handle(Input::Frame {
            transport,
            data: format!(r#"{{"id":{id},"subscribe":{{"publish_debounce":100}}}}"#).into_bytes(),
        });
    }
    (machine, transport, subscriptions)
}

/// Answers the publish commands among `actions` and reports their frames
/// written; the debounce timers set.
fn answer_publishes(
    machine: &mut Machine,
    transport: TransportId,
    actions: Vec<Action>,
    timers: &mut Vec<DebounceTimer>,
) {
    let mut frames: Vec<(FrameId, Vec<u8>)> = Vec::new();
    for action in actions {
        match action {
            Action::Send { id, frame, .. } => frames.push((id, frame)),
            Action::SetDebounceTimer(timer, _) => timers.push(timer),
            _ => {}
        }
    }
    for (frame, bytes) in frames {
        machine.handle(Input::FrameWritten { transport, frame });
        let text = String::from_utf8(bytes).expect("a JSON frame");
        for line in text.lines() {
            let command: serde_json::Value = serde_json::from_str(line).expect("JSON");
            if command["publish"].is_object() {
                machine.handle(Input::Frame {
                    transport,
                    data: format!(r#"{{"id":{},"publish":{{}}}}"#, command["id"]).into_bytes(),
                });
            }
        }
    }
}

/// Every window opens, a 1 KiB value waiting in it, then closes: its
/// trailing send answered, its timer fired once more with nothing to send.
fn open_and_close_windows(
    machine: &mut Machine,
    transport: TransportId,
    subscriptions: &[SubscriptionId],
) {
    let value = format!("\"{}\"", "x".repeat(KIB));
    let mut timers = Vec::new();
    for subscription in subscriptions {
        for _ in 0..2 {
            let call = machine.add_subscription_call(
                *subscription,
                CallRequest::Publish {
                    channel: String::new(),
                    data: value.as_bytes().to_vec(),
                },
            );
            let actions = machine.handle(Input::Call(call));
            answer_publishes(machine, transport, actions, &mut timers);
        }
    }
    let fired = std::mem::take(&mut timers);
    for timer in fired {
        let actions = machine.handle(Input::DebounceTimerFired(timer));
        answer_publishes(machine, transport, actions, &mut timers);
    }
    let fired = std::mem::take(&mut timers);
    for timer in fired {
        let actions = machine.handle(Input::DebounceTimerFired(timer));
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::Send { .. })),
            "a window sent a value it did not hold"
        );
    }
}

fn a_closed_window_frees_its_memory() {
    let (mut machine, transport, subscriptions) = subscribed_core();
    // A first round grows the maps of the calls' commands to their working
    // size (Block 13): they keep their capacity, as for any burst of calls.
    open_and_close_windows(&mut machine, transport, &subscriptions);
    let before = live_bytes();

    open_and_close_windows(&mut machine, transport, &subscriptions);
    let after = live_bytes();

    println!(
        "@P report: live heap with {WINDOWS} debounce windows closed: \
         {after} bytes, {before} bytes before they opened"
    );
    assert_eq!(
        after,
        before,
        "the windows left {} bytes",
        after.abs_diff(before)
    );
    drop(machine);
}
