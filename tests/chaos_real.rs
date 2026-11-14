//! Real-server chaos (Block 51): seeded faults against a real Centrifugo
//! through Toxiproxy. Needs Docker, so it runs only with
//! `FERRIFUGE_REAL_CHAOS=1`, as the chaos-real workflow does with
//! `cargo test --test chaos_real`; `cargo test` and `--all-targets` leave
//! it out (D1).
//!
//! The binary is both sides (D1). As the parent it starts the environment
//! (D2), drives the faults (D3) and checks the invariants (D7) against the
//! server's own records. Re-executed with `FERRIFUGE_REAL_CHAOS_ROLE=client`
//! it is the child, which owns one `Client`, reads operations from stdin
//! and writes what the client does to stdout as NDJSON, each record with a
//! sequence number: a process a fault can stop without stopping its
//! checker.
//!
//! 51a: websocket, JSON, the memory engine; D3's toxics and the proxy
//! turned down. 51c adds D4: the server paused and restarted, the client
//! suspended, short-lived tokens; and D6's two restart scenarios, each run
//! once, the Redis one in an environment of its own. The outline's row
//! runs here but is not claimed until 51d, when every row and fault kind
//! is in.

// @P scenario: A failing real-server seed is a finding only when it fails twice
// @P scenario: A server restart with the memory engine reports recovered false
// @P scenario: A server restart with the Redis engine loses nothing
// @P scenario: A seed of the real-server runner fixes its operations
// @P scenario: The real-server runner reports a planted fault

// Counts and offsets are far below 2^52.
#![allow(clippy::cast_precision_loss)]

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::Write as _;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use support::invariants::{self, Lifecycle};
use support::real_server::{Engine, Environment, PROXY, Stream, configuration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

/// Enables the run.
const RUN: &str = "FERRIFUGE_REAL_CHAOS";
/// Makes the binary the child (D1).
const ROLE: &str = "FERRIFUGE_REAL_CHAOS_ROLE";
/// Runs one seed alone (D8).
const SEED_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_SEED";
/// The run's budget in seconds (D11: 1800 a row in the workflow).
const BUDGET_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_BUDGET_SECONDS";
/// Where a failing seed's output goes, for the workflow's artifact.
const ARTIFACTS_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_ARTIFACTS";
/// The child's endpoint, channels and user.
const ENDPOINT_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_ENDPOINT";
const CHANNELS_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_CHANNELS";
const USER_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_USER";
/// The ttls, in seconds, of the child's connection and subscription
/// tokens, `<connection>,<subscription>` (D4: 4 to 8 s each).
const TOKEN_TTL_VARIABLE: &str = "FERRIFUGE_REAL_CHAOS_TOKEN_TTL";

/// The budget when none is set: a short local run (reviewer, 51a).
const DEFAULT_BUDGET: Duration = Duration::from_secs(300);
/// A seed's operations stop after this, leaving the rest of its 120 s to
/// healing and the checks (D11).
const OPERATIONS_WITHIN: Duration = Duration::from_secs(45);
/// Quiescence after healing (D7, I4). The suspended time I4 adds is the
/// child's after healing, none, as suspensions hold the operations until
/// they end (reviewer, 51c).
const QUIESCENCE: Duration = Duration::from_secs(30);
/// What a restart is counted for, at least, when the operations window
/// decides whether an operation that holds the others still fits (D11).
const RESTART_TAKES: Duration = Duration::from_secs(5);
/// The longest restart of the run so far, in ms, from its `restart` to its
/// `restart … done` (reviewer, 51c).
static LONGEST_RESTART_MS: AtomicU64 = AtomicU64::new(0);

/// What a restart is counted for: [`RESTART_TAKES`], or the longest one
/// of the run so far.
fn restart_takes() -> Duration {
    RESTART_TAKES.max(Duration::from_millis(
        LONGEST_RESTART_MS.load(Ordering::SeqCst),
    ))
}

/// Whether an operation that holds the others for `holds`, started at
/// `elapsed` into the operations and after the fault in place holds for
/// its `remaining` time, ends within the window (D11, reviewer 51c).
fn fits(elapsed: Duration, remaining: Duration, holds: Duration) -> bool {
    elapsed + remaining + holds <= OPERATIONS_WITHIN
}
/// How long the channels API is polled (D7, I3): the server notices a dead
/// connection at its next missed pong.
const CHANNELS_WITHIN: Duration = Duration::from_secs(10);
/// The client's `Config::timeout`.
const CALL_TIMEOUT: Duration = Duration::from_secs(2);
/// I6: a call settles within 3 timeouts plus 1 s, as the adapter runner
/// checks it (Block 16, D7), measured in the child.
const CALL_BOUND: Duration = Duration::from_secs(7);
/// How long the runner waits for the child to answer: a state reached,
/// the calls after healing, its exit.
const CHILD_WITHIN: Duration = Duration::from_secs(10);
/// A seed's bound (D11): its operations, healing and checks.
const SEED_BOUND: Duration = Duration::from_secs(120);
/// The publisher's period: 20 publications a second in all (D5).
const PUBLISH_EVERY: Duration = Duration::from_millis(50);
/// The namespace of every channel (D5).
const NAMESPACE: &str = "recoverable";
/// At most this many channels a seed, besides the canary (D5: at most 10).
const MAX_CHANNELS: usize = 5;

// --- Operations ------------------------------------------------------------

/// A Toxiproxy toxic or the proxy turned down (D3), the server paused or
/// the client process suspended (D4): each held, then removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum FaultKind {
    Latency,
    Timeout,
    Slicer,
    ResetPeer,
    LimitData,
    Bandwidth,
    ProxyDown,
    ServerPause,
    ClientSuspension,
}

impl FaultKind {
    const ALL: [Self; 9] = [
        Self::Latency,
        Self::Timeout,
        Self::Slicer,
        Self::ResetPeer,
        Self::LimitData,
        Self::Bandwidth,
        Self::ProxyDown,
        Self::ServerPause,
        Self::ClientSuspension,
    ];

    /// Toxiproxy's name of the toxic; none for the proxy turned down.
    fn toxic(self) -> Option<&'static str> {
        Some(match self {
            Self::Latency => "latency",
            Self::Timeout => "timeout",
            Self::Slicer => "slicer",
            Self::ResetPeer => "reset_peer",
            Self::LimitData => "limit_data",
            Self::Bandwidth => "bandwidth",
            Self::ProxyDown | Self::ServerPause | Self::ClientSuspension => return None,
        })
    }
}

/// A fault and how long it holds before it is removed (D3: 500 ms–15 s).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Fault {
    kind: FaultKind,
    attributes: Value,
    /// The directions it acts on: both, but for `limit_data`.
    streams: Vec<String>,
    hold_ms: u64,
}

/// What a seed does, in order (D8: a seed fixes the operations, not their
/// timing).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Op {
    Subscribe {
        channel: usize,
    },
    Unsubscribe {
        channel: usize,
    },
    Disconnect,
    Connect,
    History {
        channel: usize,
    },
    Presence {
        channel: usize,
    },
    Fault(Fault),
    /// The Centrifugo container restarted (D4): gracefully, or killed and
    /// started.
    Restart {
        graceful: bool,
    },
    /// The child suspended, the server restarted meanwhile, the child
    /// resumed after `hold_ms` (D4, "Scheduling").
    SuspensionAcrossRestart {
        hold_ms: u64,
        graceful: bool,
    },
    Wait {
        ms: u64,
    },
}

/// What a seed fixes (D8): its channels, its tokens' ttl and its
/// operations.
#[derive(Clone, Debug, PartialEq)]
struct Plan {
    channels: usize,
    /// The ttls of the seed's connection and subscription tokens, each
    /// 4 to 8 s (D4, "Scheduling").
    connection_ttl_secs: u64,
    subscription_ttl_secs: u64,
    ops: Vec<Op>,
}

/// At most this many server restarts a seed (D4).
const MAX_RESTARTS: usize = 2;

/// A seed's plan.
fn operations(seed: u64) -> Plan {
    let mut rng = fastrand::Rng::with_seed(seed ^ 0x5eed_4ea1_c4a0_5051);
    let channels = rng.usize(2..=MAX_CHANNELS);
    let connection_ttl_secs = rng.u64(4..=8);
    let subscription_ttl_secs = rng.u64(4..=8);
    let mut ops: Vec<Op> = (0..channels)
        .map(|channel| Op::Subscribe { channel })
        .collect();
    let mut restarts = 0;
    for _ in 0..rng.usize(10..=18) {
        let channel = rng.usize(0..channels);
        ops.push(match rng.u32(0..100) {
            0..30 => {
                let fault = fault(&mut rng);
                // A seeded share of suspensions enclose a restart: an app
                // suspended across a deploy (D4, "Scheduling").
                if fault.kind == FaultKind::ClientSuspension
                    && restarts < MAX_RESTARTS
                    && rng.u32(0..3) == 0
                {
                    restarts += 1;
                    Op::SuspensionAcrossRestart {
                        hold_ms: fault.hold_ms,
                        graceful: rng.bool(),
                    }
                } else {
                    Op::Fault(fault)
                }
            }
            30..34 if restarts < MAX_RESTARTS => {
                restarts += 1;
                Op::Restart {
                    graceful: rng.bool(),
                }
            }
            34..42 => Op::Unsubscribe { channel },
            42..54 => Op::Subscribe { channel },
            54..62 => Op::Disconnect,
            62..72 => Op::Connect,
            72..80 => Op::History { channel },
            80..86 => Op::Presence { channel },
            _ => Op::Wait {
                ms: rng.u64(200..=3000),
            },
        });
    }
    Plan {
        channels,
        connection_ttl_secs,
        subscription_ttl_secs,
        ops,
    }
}

/// A fault drawn within D3's ranges.
fn fault(rng: &mut fastrand::Rng) -> Fault {
    let kind = FaultKind::ALL[rng.usize(0..FaultKind::ALL.len())];
    let attributes = match kind {
        FaultKind::Latency => json!({"latency": rng.u64(5..=400), "jitter": rng.u64(0..=100)}),
        FaultKind::Timeout => {
            let timeout = if rng.bool() { 0 } else { rng.u64(500..=5000) };
            json!({"timeout": timeout})
        }
        FaultKind::Slicer => {
            let size = rng.u64(16..=256);
            json!({"average_size": size, "size_variation": rng.u64(0..=size), "delay": rng.u64(0..=100)})
        }
        FaultKind::ResetPeer => json!({"timeout": rng.u64(0..=2000)}),
        FaultKind::LimitData => json!({"bytes": rng.u64(1024..=256 * 1024)}),
        FaultKind::Bandwidth => json!({"rate": rng.u64(1..=128)}),
        FaultKind::ProxyDown | FaultKind::ServerPause | FaultKind::ClientSuspension => json!({}),
    };
    let streams = match kind {
        FaultKind::LimitData => vec!["downstream".to_owned()],
        FaultKind::ProxyDown | FaultKind::ServerPause | FaultKind::ClientSuspension => vec![],
        _ => vec!["upstream".to_owned(), "downstream".to_owned()],
    };
    let hold_ms = match kind {
        FaultKind::ProxyDown => rng.u64(500..=10_000),
        // D4: a client suspension of 500 ms to 20 s.
        FaultKind::ClientSuspension => rng.u64(500..=20_000),
        _ => rng.u64(500..=15_000),
    };
    Fault {
        kind,
        attributes,
        streams,
        hold_ms,
    }
}

impl Op {
    /// How long the operation holds the others (D4, "Scheduling"): a
    /// suspension, a pause and a restart do, until they end.
    fn holds(&self, restart: Duration) -> Duration {
        match self {
            Self::Fault(Fault {
                kind: FaultKind::ServerPause | FaultKind::ClientSuspension,
                hold_ms,
                ..
            }) => Duration::from_millis(*hold_ms),
            Self::Restart { .. } => restart,
            Self::SuspensionAcrossRestart { hold_ms, .. } => {
                Duration::from_millis(*hold_ms) + restart
            }
            _ => Duration::ZERO,
        }
    }
}

// --- The child -------------------------------------------------------------

/// What the parent asks of the child, one JSON line each.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "do", rename_all = "snake_case")]
enum Command_ {
    Subscribe { channel: String },
    Unsubscribe { channel: String },
    Disconnect,
    Connect,
    History { channel: String },
    Presence { channel: String },
    Status,
    Close,
    Exit,
}

/// What the child writes, one JSON line each, with its sequence number.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Client {
        state: String,
        code: u32,
    },
    Sub {
        channel: String,
        state: String,
        was_recovering: bool,
        recovered: bool,
        epoch: Option<String>,
        offset: Option<u64>,
    },
    Pub {
        channel: String,
        offset: Option<u64>,
        epoch: Option<String>,
        data: String,
    },
    Call {
        call: String,
        took_ms: u64,
        /// When it was made and settled, by the wall clock (I6).
        started_ms: u64,
        ended_ms: u64,
        ok: bool,
        error: Option<String>,
    },
    Status {
        client: String,
        subs: BTreeMap<String, String>,
        pending: u64,
    },
    Panic {
        message: String,
    },
    /// A token callback was called: of the connection or a subscription;
    /// a refresh when that was established (D10), and whether it was
    /// called after `disconnect()`, before the next `connect()` (I5).
    Token {
        of: String,
        refresh: bool,
        off: bool,
    },
    /// The client's state left disconnected after `disconnect()`, before
    /// the next `connect()` (I5).
    LeftOff {
        state: String,
    },
    Closed,
}

/// A record and its sequence number.
#[derive(Debug, Serialize, Deserialize)]
struct Line {
    seq: u64,
    #[serde(flatten)]
    record: Record,
}

/// The child's writer: one thread writing records to stdout in order.
#[derive(Clone)]
struct Writer {
    lines: std::sync::mpsc::Sender<Record>,
}

impl Writer {
    fn start() -> Self {
        let (lines, records) = std::sync::mpsc::channel::<Record>();
        std::thread::spawn(move || {
            use std::io::Write as _;
            for (seq, record) in (1u64..).zip(records) {
                let line = serde_json::to_string(&Line { seq, record }).expect("a record");
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{line}");
                let _ = out.flush();
            }
        });
        Self { lines }
    }

    fn write(&self, record: Record) {
        let _ = self.lines.send(record);
    }
}

fn state_name(state: impl std::fmt::Debug) -> String {
    format!("{state:?}").to_lowercase()
}

fn child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(child_run());
}

async fn child_run() {
    let writer = Writer::start();
    // D1: the panic is recorded before the exit, on any thread.
    let hook_writer = writer.clone();
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        hook_writer.write(Record::Panic {
            message: info.to_string(),
        });
        std::thread::sleep(Duration::from_millis(100));
        default_hook(info);
    }));

    let endpoint = env::var(ENDPOINT_VARIABLE).expect("the child's endpoint");
    let user = env::var(USER_VARIABLE).expect("the child's user");
    let channels: Vec<String> = env::var(CHANNELS_VARIABLE)
        .expect("the child's channels")
        .split(',')
        .map(str::to_owned)
        .collect();
    let ttls: Vec<u64> = env::var(TOKEN_TTL_VARIABLE)
        .unwrap_or_default()
        .split(',')
        .filter_map(|ttl| ttl.parse().ok())
        .collect();
    let ttl = |n: usize| Duration::from_secs(ttls.get(n).copied().unwrap_or(3600));
    let (connection_ttl, subscription_ttl) = (ttl(0), ttl(1));
    // I5's flag (below), also read by the token callbacks.
    let off = Off::default();
    let client = child_client(endpoint, &user, connection_ttl, &writer, &off);

    forward_client_events(&client, &writer);
    let subscriptions =
        child_subscriptions(&client, &channels, &writer, &user, subscription_ttl, &off);
    // The canary, the last channel, is subscribed for the whole run (D7).
    if let Some(canary) = channels.last() {
        subscriptions[canary].subscribe();
    }
    client.connect();

    let mut commanded = commands_from_stdin();
    let pending = Arc::new(AtomicU64::new(0));
    // I5 (Block 16), watched where it is exact: from disconnect()'s
    // return until the next connect(), the client's own state stays
    // disconnected; the event stream, forwarded by another task, gives no
    // such boundary (Codex, #259).
    // The flag and the state it guards are read and written under one
    // lock: disconnect() and the flag set together, the flag cleared and
    // connect() together, and the watcher's flag and state read together,
    // so that a state sampled before disconnect() is never judged by the
    // flag set after it (Codex, #259).
    watch_off(&client, &off, &writer);
    while let Some(command) = commanded.recv().await {
        match command {
            Command_::Subscribe { channel } => subscriptions[&channel].subscribe(),
            Command_::Unsubscribe { channel } => subscriptions[&channel].unsubscribe(),
            Command_::Disconnect => off.disconnect(&client),
            Command_::Connect => off.connect(&client),
            Command_::History { channel } => {
                let call = client.history(channel, ferrifuge::HistoryOptions::default().limit(10));
                spawn_call(&writer, &pending, "history", async move {
                    call.await.map(drop).map_err(|e| e.to_string())
                });
            }
            Command_::Presence { channel } => {
                let call = client.presence(channel);
                spawn_call(&writer, &pending, "presence", async move {
                    call.await.map(drop).map_err(|e| e.to_string())
                });
            }
            Command_::Status => writer.write(Record::Status {
                client: state_name(client.state()),
                subs: subscriptions
                    .iter()
                    .map(|(name, sub)| (name.clone(), state_name(sub.state())))
                    .collect(),
                pending: pending.load(Ordering::SeqCst),
            }),
            Command_::Close => {
                client.close().await;
                writer.write(Record::Closed);
            }
            // Only once the parent has seen the server list none of the
            // client's channels: the exit would close a socket close()
            // left open (Codex, #259).
            Command_::Exit => {
                // The writer thread flushes before the exit.
                tokio::time::sleep(Duration::from_millis(200)).await;
                std::process::exit(0);
            }
        }
    }
}

/// The parent's commands, read from stdin by a thread of their own.
fn commands_from_stdin() -> mpsc::UnboundedReceiver<Command_> {
    let (commands, commanded) = mpsc::unbounded_channel::<Command_>();
    std::thread::spawn(move || {
        use std::io::BufRead as _;
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if let Ok(command) = serde_json::from_str(&line)
                && commands.send(command).is_err()
            {
                break;
            }
        }
    });
    commanded
}

/// I5's flag, set from `disconnect()`'s return until the next `connect()`,
/// in two forms: under a lock, read with the client's state by the
/// watcher, and as a mirror the token callbacks read without a lock, so
/// that no fetch goes unjudged while the watcher holds it (reviewer, 51c).
#[derive(Clone, Default)]
struct Off {
    wanted: Arc<Mutex<bool>>,
    mirror: Arc<AtomicBool>,
}

impl Off {
    /// `disconnect()`, then the flag set, under the lock.
    fn disconnect(&self, client: &ferrifuge::Client) {
        let mut off = self.wanted.lock().unwrap();
        client.disconnect();
        *off = true;
        self.mirror.store(true, Ordering::SeqCst);
    }

    /// The flag cleared, then `connect()`, under the lock.
    fn connect(&self, client: &ferrifuge::Client) {
        let mut off = self.wanted.lock().unwrap();
        *off = false;
        self.mirror.store(false, Ordering::SeqCst);
        client.connect();
    }

    /// Whether a token fetch now is one after `disconnect()`, before the
    /// next `connect()` (I5).
    fn fetched_off(&self) -> bool {
        self.mirror.load(Ordering::SeqCst)
    }
}

/// Whether a connection token fetch is a refresh: the client connected,
/// read from its own state when the callback runs, which can only count
/// fewer (reviewer, 51c). Before the client is set, none is.
fn connection_established(client: &OnceLock<ferrifuge::Client>) -> bool {
    client
        .get()
        .is_some_and(|client| client.state() == ferrifuge::State::Connected)
}

/// Whether a subscription token fetch is a refresh: the subscription
/// subscribed, by its own state.
fn subscription_established(subscription: &OnceLock<ferrifuge::Subscription>) -> bool {
    subscription.get().is_some_and(|subscription| {
        subscription.state() == ferrifuge::SubscriptionState::Subscribed
    })
}

/// The child's client, its connection tokens short-lived and refreshed
/// through the callback, each fetch recorded (D4).
fn child_client(
    endpoint: String,
    user: &str,
    ttl: Duration,
    writer: &Writer,
    off: &Off,
) -> ferrifuge::Client {
    let token_user = user.to_owned();
    let token_writer = writer.clone();
    let slot = Arc::new(OnceLock::new());
    let (token_slot, off) = (Arc::clone(&slot), off.clone());
    let client = ferrifuge::Client::builder(endpoint)
        .config(ferrifuge::Config::default().timeout(CALL_TIMEOUT))
        .get_token(move || {
            token_writer.write(Record::Token {
                of: "connection".to_owned(),
                refresh: connection_established(&token_slot),
                off: off.fetched_off(),
            });
            let token = support::token::connection_token(&token_user, ttl);
            async move { Ok(token) }
        })
        .build()
        .expect("a client");
    let _ = slot.set(client.clone());
    client
}

/// Writes [`Record::LeftOff`] when the client's own state leaves
/// disconnected while `off_wanted` (I5).
fn watch_off(client: &ferrifuge::Client, off: &Off, writer: &Writer) {
    let (client, off_wanted, writer) = (client.clone(), Arc::clone(&off.wanted), writer.clone());
    tokio::spawn(async move {
        let mut reported = false;
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let (wanted, state) = {
                let off = off_wanted.lock().unwrap();
                (*off, client.state())
            };
            let left = wanted && state != ferrifuge::State::Disconnected;
            if left && !reported {
                writer.write(Record::LeftOff {
                    state: state_name(state),
                });
            }
            reported = left;
        }
    });
}

/// Forwards the client's state events to `writer`.
fn forward_client_events(client: &ferrifuge::Client, writer: &Writer) {
    let mut events = client.events();
    let client_writer = writer.clone();
    tokio::spawn(async move {
        use futures_util::StreamExt;
        while let Some(event) = events.next().await {
            let (state, code) = match &event {
                ferrifuge::ClientEvent::Connecting { code, .. } => ("connecting", *code),
                ferrifuge::ClientEvent::Connected { .. } => ("connected", 0),
                ferrifuge::ClientEvent::Disconnected { code, .. } => ("disconnected", *code),
                _ => continue,
            };
            client_writer.write(Record::Client {
                state: state.to_owned(),
                code,
            });
        }
    });
}

/// The child's subscriptions, unsubscribed, each forwarding its events to
/// `writer`.
fn child_subscriptions(
    client: &ferrifuge::Client,
    channels: &[String],
    writer: &Writer,
    user: &str,
    ttl: Duration,
    off: &Off,
) -> BTreeMap<String, ferrifuge::Subscription> {
    let mut subscriptions = BTreeMap::new();
    for channel in channels {
        // Its own short-lived token, refreshed through the callback (D4).
        let (token_user, token_writer) = (user.to_owned(), writer.clone());
        let slot = Arc::new(OnceLock::new());
        let (token_slot, token_off) = (Arc::clone(&slot), off.clone());
        let subscription = client
            .subscription_builder(channel.clone())
            .get_token(move |channel| {
                token_writer.write(Record::Token {
                    of: "subscription".to_owned(),
                    refresh: subscription_established(&token_slot),
                    off: token_off.fetched_off(),
                });
                let token = support::token::subscription_token(&token_user, &channel, ttl);
                async move { Ok(token) }
            })
            .build()
            .expect("a subscription");
        let _ = slot.set(subscription.clone());
        let mut events = subscription.events();
        let sub_writer = writer.clone();
        let name = channel.clone();
        tokio::spawn(async move {
            use futures_util::StreamExt;
            while let Some(event) = events.next().await {
                use ferrifuge::SubscriptionEvent as E;
                let record = match event {
                    E::Subscribing { .. } => sub_record(&name, "subscribing", None),
                    E::Unsubscribed { .. } => sub_record(&name, "unsubscribed", None),
                    E::Subscribed(subscribed) => sub_record(&name, "subscribed", Some(&subscribed)),
                    E::Publication(publication) => Record::Pub {
                        channel: name.clone(),
                        offset: publication.offset,
                        epoch: publication.epoch,
                        data: String::from_utf8_lossy(&publication.data).into_owned(),
                    },
                    _ => continue,
                };
                sub_writer.write(record);
            }
        });
        subscriptions.insert(channel.clone(), subscription);
    }
    subscriptions
}

fn sub_record(channel: &str, state: &str, subscribed: Option<&ferrifuge::Subscribed>) -> Record {
    Record::Sub {
        channel: channel.to_owned(),
        state: state.to_owned(),
        was_recovering: subscribed.is_some_and(|s| s.was_recovering),
        recovered: subscribed.is_some_and(|s| s.recovered),
        epoch: subscribed.and_then(|s| s.stream_position.as_ref().map(|p| p.epoch.clone())),
        offset: subscribed.and_then(|s| s.stream_position.as_ref().map(|p| p.offset)),
    }
}

fn spawn_call(
    writer: &Writer,
    pending: &Arc<AtomicU64>,
    call: &'static str,
    settled: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
) {
    let writer = writer.clone();
    let pending = Arc::clone(pending);
    pending.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        let (start, started_ms) = (Instant::now(), unix_ms());
        let settled = settled.await;
        writer.write(Record::Call {
            call: call.to_owned(),
            took_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            started_ms,
            ended_ms: unix_ms(),
            ok: settled.is_ok(),
            error: settled.err(),
        });
        pending.fetch_sub(1, Ordering::SeqCst);
    });
}

/// The wall clock in ms since the epoch, one clock for the parent and the
/// child: a call's interval and a suspension's are compared on it (I6).
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

// --- The parent ------------------------------------------------------------

/// A fault planted in the tap or the runner, never in the client (D9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plant {
    None,
    /// The tap drops one delivered publication before the checker (I8),
    /// on every attempt or, for D8's check, on the first only.
    DropDelivery {
        every_attempt: bool,
    },
    /// The runner opens a second real connection as the run's user,
    /// subscribed to the canary (I3).
    SecondConnection,
}

/// An invariant a seed broke, with the operations up to it.
#[derive(Debug, Clone)]
struct Violation {
    seed: u64,
    /// D8's attempt: 0, or 1 for the rerun.
    attempt: u32,
    invariant: &'static str,
    detail: String,
    /// The operations done before the violation was seen.
    ops: Vec<Op>,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "seed {} broke {}: {}; operations up to it: {}",
            self.seed,
            self.invariant,
            self.detail,
            serde_json::to_string(&self.ops).unwrap_or_default()
        )
    }
}

/// What D8 makes of a seed.
#[derive(Debug)]
enum Outcome {
    Passed,
    /// One violation, not repeated by the rerun: logged, fails nothing.
    Unconfirmed(Violation),
    /// The same invariant twice: a finding, which fails the run.
    Finding(Violation),
}

/// Counts over the whole job (D10).
#[derive(Default)]
struct Counters {
    counts: BTreeMap<String, u64>,
}

impl Counters {
    fn add(&mut self, name: impl Into<String>) {
        *self.counts.entry(name.into()).or_default() += 1;
    }
}

/// Held by the publisher from its check of the pause to its publication's
/// reply, and taken by the checker once it has set the pause: after that,
/// no publication can start until the pause is lifted (Codex, #259).
type Gate = Arc<tokio::sync::Mutex<()>>;

/// The publications acknowledged: channel, epoch, offset and id.
type Published = Arc<Mutex<Vec<(String, String, u64, String)>>>;

/// The child process of a seed.
struct ChildProcess {
    process: Child,
    stdin: ChildStdin,
    records: mpsc::UnboundedReceiver<(u64, Record)>,
    /// Every line it wrote, for a failing seed's artifact.
    log: Arc<Mutex<Vec<String>>>,
}

impl ChildProcess {
    fn start(
        endpoint: &str,
        user: &str,
        channels: &[String],
        (connection, subscription): (u64, u64),
    ) -> Self {
        let mut process = Command::new(env::current_exe().expect("the runner"))
            .env(ROLE, "client")
            .env(TOKEN_TTL_VARIABLE, format!("{connection},{subscription}"))
            .env(ENDPOINT_VARIABLE, endpoint)
            .env(USER_VARIABLE, user)
            .env(CHANNELS_VARIABLE, channels.join(","))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("the child");
        let stdin = process.stdin.take().expect("the child's stdin");
        let stdout = process.stdout.take().expect("the child's stdout");
        let stderr = process.stderr.take().expect("the child's stderr");
        let log = Arc::new(Mutex::new(Vec::new()));
        let (sender, records) = mpsc::unbounded_channel();
        let out_log = Arc::clone(&log);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                out_log.lock().unwrap().push(line.clone());
                if let Ok(Line { seq, record }) = serde_json::from_str(&line) {
                    let _ = sender.send((seq, record));
                }
            }
        });
        let err_log = Arc::clone(&log);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                err_log.lock().unwrap().push(format!("stderr: {line}"));
            }
        });
        Self {
            process,
            stdin,
            records,
            log,
        }
    }

    async fn send(&mut self, command: &Command_) {
        let line = serde_json::to_string(command).expect("a command") + "\n";
        let _ = self.stdin.write_all(line.as_bytes()).await;
        let _ = self.stdin.flush().await;
    }
}

/// What the checker knows of a channel.
#[derive(Default)]
struct ChannelState {
    last: Option<Lifecycle>,
    /// Where its segment started: the offset of the first `subscribed`
    /// (D7); `None` before it.
    start: Option<u64>,
    epoch: Option<String>,
    /// The offsets and data delivered in the segment, in order.
    delivered: Vec<(u64, String)>,
    /// Whether the application wants it subscribed.
    wanted: bool,
    /// Set by a restart on the memory engine while the channel has a
    /// segment: the history's offsets and ids after the segment's start
    /// then, which the segment delivered a prefix of, until the next
    /// `subscribed` starts a new segment (D6, reviewer 51c).
    restarted: Option<Vec<(u64, String)>>,
}

/// One run of one seed.
struct SeedRun<'a> {
    environment: &'a Environment,
    seed: u64,
    attempt: u32,
    plant: Plant,
    ops: Vec<Op>,
    done: usize,
    channels: Vec<String>,
    canary: String,
    child: ChildProcess,
    client: Option<Lifecycle>,
    states: BTreeMap<String, ChannelState>,
    status: Option<Record>,
    /// The calls settled: which, how long, and their error if any.
    calls: Vec<(String, u64, Option<String>)>,
    panics: Vec<String>,
    closed: bool,
    dropped: bool,
    deliveries: u64,
    published: Published,
    paused: Arc<AtomicBool>,
    gate: Gate,
    /// A fault in place and when it is to be removed.
    fault: Option<(Fault, Instant)>,
    /// Whether the application wants the client connected.
    wanted_connected: bool,
    /// The child's suspensions, by the wall clock: from before its STOP to
    /// after its CONT, open while it is stopped (I6).
    suspensions: Vec<(u64, Option<u64>)>,
    /// The acknowledged publications checked against a history so far.
    acknowledged: usize,
    /// The server restarts so far (D4).
    restarts: usize,
    /// Each channel's expected deliveries, as compared: compared again
    /// once every record is taken (Codex, #259).
    expected: BTreeMap<String, Vec<(u64, String)>>,
}

impl SeedRun<'_> {
    fn violation(&self, invariant: &'static str, detail: impl Into<String>) -> Violation {
        Violation {
            seed: self.seed,
            attempt: self.attempt,
            invariant,
            detail: detail.into(),
            ops: self.ops[..self.done.min(self.ops.len())].to_vec(),
        }
    }

    /// Takes the child's records until `deadline`, checking each.
    async fn pump(&mut self, deadline: Instant) -> Result<(), Violation> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            match tokio::time::timeout(deadline - now, self.child.records.recv()).await {
                Ok(Some((_, record))) => self.take(record)?,
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    /// Takes the records waiting now.
    fn drain(&mut self) -> Result<(), Violation> {
        while let Ok((_, record)) = self.child.records.try_recv() {
            self.take(record)?;
        }
        Ok(())
    }

    fn take(&mut self, record: Record) -> Result<(), Violation> {
        match record {
            Record::Client { state, .. } => {
                let next = lifecycle(&state);
                // The client starts disconnected (Codex, #259).
                let last = self.client.unwrap_or(Lifecycle::Off);
                invariants::state_chain(last, next)
                    .map_err(|detail| self.violation("I1", format!("client: {detail}")))?;
                if last == Lifecycle::On && next == Lifecycle::Pending {
                    COUNTERS.lock().unwrap().add("reconnect");
                }
                self.client = Some(next);
            }
            Record::Sub {
                channel,
                state,
                was_recovering,
                recovered,
                epoch,
                offset,
            } => self.sub(&channel, &state, was_recovering, recovered, epoch, offset)?,
            Record::Pub {
                channel,
                offset,
                epoch,
                data,
            } => self.publication(&channel, offset, epoch.as_deref(), data)?,
            Record::Call {
                call,
                took_ms,
                started_ms,
                ended_ms,
                ok,
                error,
            } => {
                if let Some(error) = &error {
                    log(&format!("seed {} {call} failed: {error}", self.seed));
                }
                COUNTERS
                    .lock()
                    .unwrap()
                    .add(format!("call {call} {}", if ok { "ok" } else { "failed" }));
                // The time the child was stopped within the call is added
                // (D4, "Scheduling").
                let bound = CALL_BOUND + self.suspended_within(started_ms, ended_ms);
                invariants::call_settled(Duration::from_millis(took_ms), bound)
                    .map_err(|detail| self.violation("I6", format!("{call}: {detail}")))?;
                self.calls.push((call, took_ms, error));
            }
            record @ Record::Status { .. } => self.status = Some(record),
            Record::Panic { message } => self.panics.push(message),
            Record::Closed => self.closed = true,
            Record::Token { of, refresh, off } => {
                if off {
                    return Err(self.violation(
                        "I5",
                        format!("a {of} token fetched after disconnect(), before connect()"),
                    ));
                }
                // D10 counts refreshes: fetches while the connection or the
                // subscription is established (reviewer, 51c).
                if refresh {
                    COUNTERS.lock().unwrap().add(format!("token {of}"));
                }
            }
            Record::LeftOff { state } => {
                return Err(self.violation(
                    "I5",
                    format!("the client went {state} after disconnect(), before connect()"),
                ));
            }
        }
        Ok(())
    }

    fn sub(
        &mut self,
        channel: &str,
        state: &str,
        was_recovering: bool,
        recovered: bool,
        epoch: Option<String>,
        offset: Option<u64>,
    ) -> Result<(), Violation> {
        let next = lifecycle(state);
        // A subscription starts unsubscribed, as the adapter runner records
        // it (Codex, #259).
        let last = self
            .states
            .entry(channel.to_owned())
            .or_default()
            .last
            .unwrap_or(Lifecycle::Off);
        invariants::state_chain(last, next)
            .map_err(|detail| self.violation("I1", format!("{channel}: {detail}")))?;
        let entry = self.states.get_mut(channel).expect("its state");
        entry.last = Some(next);
        if next != Lifecycle::On {
            return Ok(());
        }
        if recovered {
            COUNTERS.lock().unwrap().add("recovered true");
        }
        // Every channel is positioned and recoverable (D5): a subscribed
        // without its position leaves no segment to check (Codex, #259).
        if offset.is_none() {
            return Err(self.violation(
                "I8",
                format!("{channel} subscribed without a stream position"),
            ));
        }
        let counted = channel != self.canary;
        let entry = self.states.get_mut(channel).expect("its state");
        let failure = match entry.start {
            // The first subscribed starts the channel's segment (D7).
            None => {
                entry.start = offset;
                entry.epoch = epoch;
                None
            }
            // A recovery continues it, in its epoch.
            Some(_) if recovered => (epoch != entry.epoch).then(|| {
                format!(
                    "{channel} recovered in epoch {epoch:?}, the segment's {:?}",
                    entry.epoch
                )
            }),
            // A failed recovery is allowed only after a restart on the
            // memory engine (D6): the segment delivered a prefix of the
            // history at the restart, and a new segment starts.
            Some(_) => match entry.restarted.take() {
                None => Some(format!(
                    "{channel} subscribed with was_recovering {was_recovering} and recovered false, no server restart"
                )),
                Some(_) if !was_recovering => Some(format!(
                    "{channel} subscribed after a restart with was_recovering false"
                )),
                Some(_) if epoch == entry.epoch => Some(format!(
                    "{channel} subscribed after a restart with recovered false in the segment's epoch {epoch:?}"
                )),
                Some(history) => {
                    let delivered: Vec<(u64, String)> = entry
                        .delivered
                        .iter()
                        .map(|(offset, data)| (*offset, id_of(data)))
                        .collect();
                    if history.starts_with(&delivered) {
                        entry.start = offset;
                        entry.epoch = epoch;
                        entry.delivered.clear();
                        if counted {
                            COUNTERS
                                .lock()
                                .unwrap()
                                .add("recovered false after a restart");
                        }
                        None
                    } else {
                        Some(format!(
                            "{channel}: before the restart, delivered {delivered:?} of the history {history:?}"
                        ))
                    }
                }
            },
        };
        failure.map_or(Ok(()), |detail| Err(self.violation("I8", detail)))
    }

    /// The time the child was stopped between `from` and `to`, by the wall
    /// clock (I6).
    fn suspended_within(&self, from: u64, to: u64) -> Duration {
        overlap(&self.suspensions, from, to, unix_ms())
    }

    fn publication(
        &mut self,
        channel: &str,
        offset: Option<u64>,
        epoch: Option<&str>,
        data: String,
    ) -> Result<(), Violation> {
        let Some(offset) = offset else {
            return Err(self.violation("I8", format!("{channel}: a publication without offset")));
        };
        self.deliveries += 1;
        // D9: the tap drops one delivered publication before the checker.
        if let Plant::DropDelivery { every_attempt } = self.plant
            && (every_attempt || self.attempt == 0)
            && !self.dropped
            && self.deliveries >= 5
        {
            self.dropped = true;
            return Ok(());
        }
        let segment = self
            .states
            .entry(channel.to_owned())
            .or_default()
            .epoch
            .clone();
        if segment.is_some() && epoch.is_some() && segment.as_deref() != epoch {
            return Err(self.violation(
                "I8",
                format!("{channel}: offset {offset} of epoch {epoch:?}, the segment's {segment:?}"),
            ));
        }
        self.states
            .get_mut(channel)
            .expect("its state")
            .delivered
            .push((offset, data));
        Ok(())
    }

    /// Sends `command` to the child.
    async fn command(&mut self, command: Command_) {
        self.child.send(&command).await;
    }

    /// The child's status, asked for and waited for.
    async fn status(&mut self) -> Result<Option<Record>, Violation> {
        self.status = None;
        self.command(Command_::Status).await;
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.status.is_none() && Instant::now() < deadline {
            self.pump(Instant::now() + Duration::from_millis(50))
                .await?;
        }
        Ok(self.status.take())
    }

    /// Applies `fault`, removing the one in place first if its time is up.
    async fn apply(&mut self, fault: Fault) -> Result<(), Violation> {
        if let Some((_, until)) = &self.fault {
            let until = *until;
            self.pump(until).await?;
            self.remove().await;
        }
        let toxics = self.environment.toxics();
        match fault.kind.toxic() {
            Some(toxic) => {
                for stream in &fault.streams {
                    let direction = if stream == "upstream" {
                        Stream::Upstream
                    } else {
                        Stream::Downstream
                    };
                    let name = format!("{toxic}-{stream}");
                    toxics
                        .add(PROXY, &name, toxic, direction, fault.attributes.clone())
                        .await
                        .unwrap_or_else(|error| panic!("{error}"));
                }
            }
            None => match fault.kind {
                FaultKind::ServerPause => self
                    .environment
                    .pause()
                    .await
                    .unwrap_or_else(|error| panic!("{error}")),
                FaultKind::ClientSuspension => {
                    self.suspensions.push((unix_ms(), None));
                    self.signal("STOP").await;
                }
                _ => toxics
                    .set_enabled(PROXY, false)
                    .await
                    .unwrap_or_else(|error| panic!("{error}")),
            },
        }
        self.record_fault(&format!(
            "fault {:?} {} for {} ms",
            fault.kind, fault.attributes, fault.hold_ms
        ));
        COUNTERS
            .lock()
            .unwrap()
            .add(format!("fault {:?}", fault.kind));
        let until = Instant::now() + Duration::from_millis(fault.hold_ms);
        self.fault = Some((fault, until));
        Ok(())
    }

    /// Removes the fault in place, if any.
    async fn remove(&mut self) {
        let Some((fault, _)) = self.fault.take() else {
            return;
        };
        let toxics = self.environment.toxics();
        match fault.kind.toxic() {
            Some(toxic) => {
                for stream in &fault.streams {
                    let _ = toxics.remove(PROXY, &format!("{toxic}-{stream}")).await;
                }
            }
            None => match fault.kind {
                FaultKind::ServerPause => {
                    let _ = self.environment.unpause().await;
                }
                FaultKind::ClientSuspension => {
                    self.signal("CONT").await;
                    if let Some((_, end @ None)) = self.suspensions.last_mut() {
                        *end = Some(unix_ms());
                    }
                }
                _ => {
                    let _ = toxics.set_enabled(PROXY, true).await;
                }
            },
        }
        self.record_fault(&format!("fault {:?} removed", fault.kind));
    }

    /// Sends `signal` (STOP or CONT) to the child process (D4).
    async fn signal(&self, signal: &str) {
        let Some(pid) = self.child.process.id() else {
            return;
        };
        let _ = Command::new("kill")
            .args([format!("-{signal}"), pid.to_string()])
            .status()
            .await;
    }

    /// Holds the operations until the fault in place ends, then removes
    /// it, also when a record taken meanwhile breaks an invariant: the
    /// child runs again before any exit path waits on it (D4,
    /// "Scheduling").
    async fn hold(&mut self) -> Result<(), Violation> {
        let Some((_, until)) = &self.fault else {
            return Ok(());
        };
        let until = *until;
        let held = self.pump(until).await;
        self.remove().await;
        held
    }

    /// The fault in place held for its seeded time, then removed.
    async fn end_fault(&mut self) -> Result<(), Violation> {
        if let Some((_, until)) = &self.fault {
            let until = *until;
            self.pump(until).await?;
            self.remove().await;
        }
        Ok(())
    }

    /// Restarts the server (D4), holding the operations until it is done.
    async fn restart(&mut self, graceful: bool) -> Result<(), Violation> {
        self.end_fault().await?;
        self.before_restart().await?;
        self.restart_server(graceful).await;
        self.paused.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// The child suspended, the server restarted meanwhile, the child
    /// resumed once the suspension's time is up (D4, "Scheduling").
    async fn suspension_across_restart(
        &mut self,
        hold_ms: u64,
        graceful: bool,
    ) -> Result<(), Violation> {
        self.end_fault().await?;
        // The round trip of the snapshot needs the child running.
        self.before_restart().await?;
        self.apply(Fault {
            kind: FaultKind::ClientSuspension,
            attributes: json!({}),
            streams: Vec::new(),
            hold_ms,
        })
        .await?;
        COUNTERS.lock().unwrap().add("suspension across restart");
        self.restart_server(graceful).await;
        self.paused.store(false, Ordering::SeqCst);
        self.hold().await
    }

    /// Restarts the server, counted and stamped as the faults are. One that
    /// does not answer within its bound is the environment's failure, not
    /// a finding: it ends the run (reviewer, 51c).
    async fn restart_server(&mut self, graceful: bool) {
        let kind = if graceful { "graceful" } else { "hard" };
        self.record_fault(&format!("restart {kind}"));
        let began = Instant::now();
        COUNTERS.lock().unwrap().add(format!("restart {kind}"));
        if let Err(error) = self.environment.restart(graceful).await {
            panic!("the environment: seed {}, the restart: {error}", self.seed);
        }
        self.restarts += 1;
        self.record_fault(&format!("restart {kind} done"));
        LONGEST_RESTART_MS.fetch_max(
            u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    /// Pauses the publisher: a publication in flight has its reply, and
    /// none starts until the pause is lifted (Codex, #259).
    async fn pause_publisher(&self) {
        self.paused.store(true, Ordering::SeqCst);
        drop(self.gate.lock().await);
    }

    /// A channel's history by the server API: its epoch, and the id at each
    /// offset.
    async fn history(&self, channel: &str) -> Result<(String, BTreeMap<u64, String>), Violation> {
        let history = self
            .environment
            .api()
            .call("history", json!({"channel": channel, "limit": -1}))
            .await
            .map_err(|error| {
                self.violation("I8", format!("{channel} history: {}", error.message))
            })?;
        let epoch = history["epoch"].as_str().unwrap_or_default().to_owned();
        let offsets = history["publications"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|publication| {
                let offset = publication["offset"].as_u64().unwrap_or(0);
                (offset, id_of(&publication["data"].to_string()))
            })
            .collect();
        Ok((epoch, offsets))
    }

    /// Every publication acknowledged since the last check is in its
    /// channel's history, by epoch and offset (I8, reviewer 51c).
    fn check_acknowledged(
        &mut self,
        histories: &BTreeMap<String, (String, BTreeMap<u64, String>)>,
    ) -> Result<(), Violation> {
        let published = self.published.lock().unwrap().clone();
        for (channel, epoch, offset, id) in &published[self.acknowledged..] {
            let Some((history_epoch, offsets)) = histories.get(channel) else {
                continue;
            };
            if epoch != history_epoch || offsets.get(offset) != Some(id) {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: acknowledged ({epoch}, {offset}) {id} not in the history, of epoch {history_epoch}"
                    ),
                ));
            }
        }
        self.acknowledged = published.len();
        Ok(())
    }

    /// Before a restart (D6), with the publisher paused: every publication
    /// acknowledged is in the history, and after a status round trip each
    /// channel's segment delivered a prefix of the history's offsets after
    /// its start. On the memory engine that history is kept for the
    /// `subscribed` the restart brings (reviewer, 51c).
    async fn before_restart(&mut self) -> Result<(), Violation> {
        self.pause_publisher().await;
        let mut histories = BTreeMap::new();
        for channel in self.channels.iter().chain([&self.canary]) {
            histories.insert(channel.clone(), self.history(channel).await?);
        }
        self.check_acknowledged(&histories)?;
        // Every record the child wrote before its answer is taken.
        self.status().await?;
        self.drain()?;
        let memory = self.environment.engine() == Engine::Memory;
        for (channel, (epoch, offsets)) in histories {
            let Some(state) = self.states.get(&channel) else {
                continue;
            };
            // An earlier restart's history stands until its subscribed.
            let (Some(start), None) = (state.start, &state.restarted) else {
                continue;
            };
            if state.epoch.as_deref() != Some(epoch.as_str()) {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: the history's epoch {epoch}, the segment's {:?}, before a restart",
                        state.epoch
                    ),
                ));
            }
            let history: Vec<(u64, String)> = offsets
                .range(start + 1..)
                .map(|(offset, id)| (*offset, id.clone()))
                .collect();
            let delivered: Vec<(u64, String)> = state
                .delivered
                .iter()
                .map(|(offset, data)| (*offset, id_of(data)))
                .collect();
            if !history.starts_with(&delivered) {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: before a restart, delivered {delivered:?} of the history {history:?}"
                    ),
                ));
            }
            if memory {
                self.states.get_mut(&channel).expect("its state").restarted = Some(history);
            }
        }
        Ok(())
    }

    /// Logs a fault added or removed, stamped with the wall clock, also in
    /// the seed's artifact: Toxiproxy logs no toxic, only the proxy turned
    /// down and up and the connections, so these records attribute a
    /// failure to a toxic (`VERIFY:` of Block 51).
    fn record_fault(&self, what: &str) {
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_millis());
        let line = format!("seed {} at {at} ms since the epoch: {what}", self.seed);
        log(&line);
        self.child
            .log
            .lock()
            .unwrap()
            .push(format!("runner: {line}"));
    }

    /// Removes a fault whose time is up.
    async fn expire(&mut self) {
        if self
            .fault
            .as_ref()
            .is_some_and(|(_, until)| Instant::now() >= *until)
        {
            self.remove().await;
        }
    }

    async fn run_ops(&mut self) -> Result<(), Violation> {
        let start = Instant::now();
        for index in 0..self.ops.len() {
            if start.elapsed() > OPERATIONS_WITHIN {
                break;
            }
            self.done = index + 1;
            let op = self.ops[index].clone();
            // An operation that holds the others runs only if it ends
            // within the window, the fault in place held out first; one
            // that does not is skipped, so that the seed keeps its bound
            // (D11, reviewer 51c).
            let holds = op.holds(restart_takes());
            let remaining = self.fault.as_ref().map_or(Duration::ZERO, |(_, until)| {
                until.saturating_duration_since(Instant::now())
            });
            if holds > Duration::ZERO && !fits(start.elapsed(), remaining, holds) {
                self.record_fault(&format!("held op skipped by the window: {op:?}"));
                COUNTERS
                    .lock()
                    .unwrap()
                    .add("held op skipped by the window");
                continue;
            }
            match op {
                Op::Subscribe { channel } => {
                    let name = self.channels[channel].clone();
                    self.states.entry(name.clone()).or_default().wanted = true;
                    self.command(Command_::Subscribe { channel: name }).await;
                }
                Op::Unsubscribe { channel } => {
                    let name = self.channels[channel].clone();
                    self.states.entry(name.clone()).or_default().wanted = false;
                    self.command(Command_::Unsubscribe { channel: name }).await;
                }
                Op::Disconnect => {
                    self.wanted_connected = false;
                    self.disconnect().await?;
                }
                Op::Connect => {
                    self.wanted_connected = true;
                    self.command(Command_::Connect).await;
                }
                Op::History { channel } => {
                    let channel = self.channels[channel].clone();
                    self.command(Command_::History { channel }).await;
                }
                Op::Presence { channel } => {
                    let channel = self.channels[channel].clone();
                    self.command(Command_::Presence { channel }).await;
                }
                Op::Fault(fault) => {
                    let holds = matches!(
                        fault.kind,
                        FaultKind::ServerPause | FaultKind::ClientSuspension
                    );
                    self.apply(fault).await?;
                    if holds {
                        self.hold().await?;
                    }
                }
                Op::Restart { graceful } => self.restart(graceful).await?,
                Op::SuspensionAcrossRestart { hold_ms, graceful } => {
                    self.suspension_across_restart(hold_ms, graceful).await?;
                }
                Op::Wait { ms } => {
                    self.pump(Instant::now() + Duration::from_millis(ms))
                        .await?;
                }
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
            self.expire().await;
        }
        Ok(())
    }

    /// I5: after `disconnect()`, the client makes no connection and the
    /// server lists none of it on the canary.
    async fn disconnect(&mut self) -> Result<(), Violation> {
        self.command(Command_::Disconnect).await;
        // disconnect() takes the client off, whatever it was doing, by the
        // client's own state, not a record that may be stale (Codex, #259).
        let deadline = Instant::now() + CHILD_WITHIN;
        loop {
            let status = self.status().await?;
            if let Some(Record::Status { client, .. }) = &status
                && client == "disconnected"
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err(self.violation(
                    "I5",
                    format!("disconnect() left the client as {status:?} after {CHILD_WITHIN:?}"),
                ));
            }
            self.pump(Instant::now() + Duration::from_millis(50))
                .await?;
        }
        // Until the next connect(), a client record leaving off is I5's
        // connection made ([`SeedRun::take`]); here, none stays open.
        let open = self.canary_clients(0).await?;
        invariants::cleanup_after_disconnect(0, usize::try_from(open).unwrap_or(usize::MAX))
            .map_err(|detail| self.violation("I5", detail))
    }

    /// The canary's `num_clients`, once at most `at_most` or
    /// [`CHANNELS_WITHIN`] has passed.
    async fn canary_clients(&mut self, at_most: u64) -> Result<u64, Violation> {
        let deadline = Instant::now() + CHANNELS_WITHIN;
        loop {
            let clients = self.listed().await?.get(&self.canary).copied().unwrap_or(0);
            if clients <= at_most || Instant::now() >= deadline {
                return Ok(clients);
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
        }
    }

    /// The seed's channels the server lists, with their `num_clients`.
    ///
    /// # Errors
    ///
    /// The channels API's error: an oracle that cannot answer is no
    /// evidence of cleanup (Codex, #259).
    async fn listed(&self) -> Result<BTreeMap<String, u64>, Violation> {
        let prefix = self.canary.trim_end_matches("canary");
        let result = self
            .environment
            .api()
            .call("channels", json!({"pattern": format!("{prefix}*")}))
            .await
            .map_err(|error| {
                self.violation(
                    "channels API",
                    format!("error {}: {}", error.code, error.message),
                )
            })?;
        let channels = result["channels"].as_object().ok_or_else(|| {
            self.violation(
                "channels API",
                format!("an answer without channels: {result}"),
            )
        })?;
        Ok(channels
            .iter()
            .map(|(name, info)| (name.clone(), info["num_clients"].as_u64().unwrap_or(0)))
            .collect())
    }

    /// Faults stop and the application wants to be connected (I4): within
    /// [`QUIESCENCE`] the client is connected, every channel it wants is
    /// subscribed and the others unsubscribed, and no call is pending.
    async fn heal(&mut self) -> Result<(), Violation> {
        // The last fault holds for its seeded time first (Codex, #259).
        self.end_fault().await?;
        let _ = self.environment.toxics().set_enabled(PROXY, true).await;
        self.wanted_connected = true;
        self.command(Command_::Connect).await;
        let deadline = Instant::now() + QUIESCENCE;
        loop {
            self.pump(Instant::now() + Duration::from_millis(300))
                .await?;
            let status = self.status().await?;
            if let Some(Record::Status {
                client,
                subs,
                pending,
            }) = &status
            {
                let quiescent = client == "connected"
                    && *pending == 0
                    && subs.iter().all(|(name, state)| {
                        let wanted =
                            *name == self.canary || self.states.get(name).is_some_and(|s| s.wanted);
                        state == if wanted { "subscribed" } else { "unsubscribed" }
                    });
                if quiescent {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(self.violation(
                    "I4",
                    format!("no quiescence {QUIESCENCE:?} after healing: {status:?}"),
                ));
            }
        }
    }

    /// Once the application's final states are checked (quiescence and
    /// the channels listing), every channel it left unsubscribed is
    /// subscribed again, so that each segment's end is the history's end:
    /// a delivery lost before an unsubscribe shows as a gap, while one the
    /// client had not yet taken is recovered (Codex, #259).
    async fn resubscribe_all(&mut self) -> Result<(), Violation> {
        for channel in self.channels.clone() {
            let state = self.states.entry(channel.clone()).or_default();
            if !state.wanted {
                state.wanted = true;
                self.command(Command_::Subscribe { channel }).await;
            }
        }
        let deadline = Instant::now() + QUIESCENCE;
        loop {
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
            let all = self.channels.iter().all(|channel| {
                self.states
                    .get(channel)
                    .is_some_and(|s| s.last == Some(Lifecycle::On))
            });
            if all {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.violation(
                    "I4",
                    format!("not every channel subscribed again within {QUIESCENCE:?}"),
                ));
            }
        }
    }

    /// After healing, history and presence are called once each on the
    /// canary, which is subscribed, and both must succeed (reviewer, 51a):
    /// a failure there is a finding.
    async fn calls_after_healing(&mut self) -> Result<(), Violation> {
        let before = self.calls.len();
        let canary = self.canary.clone();
        self.command(Command_::History {
            channel: canary.clone(),
        })
        .await;
        self.command(Command_::Presence { channel: canary }).await;
        let deadline = Instant::now() + CHILD_WITHIN;
        while self.calls.len() < before + 2 && Instant::now() < deadline {
            self.pump(Instant::now() + Duration::from_millis(50))
                .await?;
        }
        let settled = self.calls[before..].to_vec();
        if settled.len() < 2 {
            return Err(self.violation(
                "calls",
                format!(
                    "{} of the 2 calls after healing settled within {CHILD_WITHIN:?}",
                    settled.len()
                ),
            ));
        }
        for (call, _, error) in settled {
            if let Some(error) = error {
                return Err(
                    self.violation("calls", format!("{call} after healing failed: {error}"))
                );
            }
        }
        Ok(())
    }

    /// I3 by the channels API (D7): at most one client on the canary, and
    /// exactly the channels the client subscribed to.
    async fn check_connections(&mut self) -> Result<(), Violation> {
        let wanted: BTreeSet<String> = self
            .states
            .iter()
            .filter(|(_, state)| state.wanted)
            .map(|(name, _)| name.clone())
            .chain([self.canary.clone()])
            .collect();
        let deadline = Instant::now() + CHANNELS_WITHIN;
        loop {
            let listed = self.listed().await?;
            let canary = listed.get(&self.canary).copied().unwrap_or(0);
            let names: BTreeSet<String> = listed.keys().cloned().collect();
            if canary <= 1 && names == wanted {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.violation(
                    "I3",
                    format!(
                        "{canary} clients on the canary; listed {names:?}, subscribed {wanted:?}"
                    ),
                ));
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
        }
    }

    /// I8 by the server's own epochs and offsets (D7): with the publisher
    /// paused, the history of each channel holds every publication
    /// acknowledged since the last check, in its epoch, and each channel's
    /// segment, in the history's epoch, delivered exactly the history's offsets
    /// after its start, in order, each once, with the id at that offset;
    /// a channel still subscribed delivered up to the history's end.
    async fn check_deliveries(&mut self) -> Result<(), Violation> {
        self.pause_publisher().await;
        let mut histories = BTreeMap::new();
        for channel in &self.channels {
            histories.insert(channel.clone(), self.history(channel).await?);
        }
        self.check_acknowledged(&histories)?;
        for (channel, (epoch, offsets)) in histories {
            // By (epoch, offset): the segment is in the history's epoch.
            if let Some(state) = self.states.get(&channel)
                && state.start.is_some()
                && state.epoch.as_deref() != Some(epoch.as_str())
            {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: the history's epoch {epoch}, the segment's {:?}",
                        state.epoch
                    ),
                ));
            }
            let end = offsets.keys().next_back().copied().unwrap_or(0);
            // A subscribed channel delivers up to the end: waited for.
            let subscribed = self
                .states
                .get(&channel)
                .is_some_and(|state| state.wanted && state.last == Some(Lifecycle::On));
            if subscribed {
                let deadline = Instant::now() + CHANNELS_WITHIN;
                while self.delivered_up_to(&channel) < end && Instant::now() < deadline {
                    self.pump(Instant::now() + Duration::from_millis(100))
                        .await?;
                }
            }
            self.drain()?;
            let Some(state) = self.states.get(&channel) else {
                continue;
            };
            let Some(start) = state.start else {
                continue;
            };
            let expected: Vec<(u64, String)> = offsets
                .range(start + 1..)
                .map(|(offset, id)| (*offset, id.clone()))
                .collect();
            let delivered: Vec<(u64, String)> = state
                .delivered
                .iter()
                .map(|(offset, data)| (*offset, id_of(data)))
                .collect();
            self.expected.insert(channel.clone(), expected.clone());
            let whole = if subscribed {
                delivered == expected
            } else {
                expected.starts_with(&delivered)
            };
            if !whole {
                let first_wrong = delivered
                    .iter()
                    .zip(&expected)
                    .position(|(got, want)| got != want)
                    .unwrap_or(delivered.len().min(expected.len()));
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: from offset {start}, delivered {} of {} history offsets (subscribed: {subscribed}), first difference at {first_wrong}: delivered {:?}, history {:?}",
                        delivered.len(),
                        expected.len(),
                        delivered.get(first_wrong),
                        expected.get(first_wrong)
                    ),
                ));
            }
        }
        Ok(())
    }

    fn delivered_up_to(&self, channel: &str) -> u64 {
        self.states
            .get(channel)
            .and_then(|state| state.delivered.last().map(|(offset, _)| *offset))
            .or_else(|| self.states.get(channel).and_then(|state| state.start))
            .unwrap_or(0)
    }

    /// Every seed ends with `close()` and the channels API listing none of
    /// its channels (D10); a panic recorded or an exit that fails is I2.
    async fn close(&mut self) -> Result<(), Violation> {
        self.command(Command_::Close).await;
        let deadline = Instant::now() + CHILD_WITHIN;
        while !self.closed && Instant::now() < deadline {
            self.pump(Instant::now() + Duration::from_millis(50))
                .await?;
        }
        if !self.closed {
            return Err(self.violation(
                "close",
                format!("close() did not return within {CHILD_WITHIN:?}"),
            ));
        }
        // With the child alive, so that its exit cannot close what close()
        // left open.
        let deadline = Instant::now() + CHANNELS_WITHIN;
        loop {
            let listed = self.listed().await?;
            if listed.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(self.violation(
                    "close",
                    format!("after close the channels API lists {listed:?}"),
                ));
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
        }
        self.command(Command_::Exit).await;
        let exited = tokio::time::timeout(CHILD_WITHIN, self.child.process.wait()).await;
        // Every record the child wrote, through the end of its output: the
        // reader forwards them apart from the exit (Codex, #259).
        let deadline = Instant::now() + CHILD_WITHIN;
        loop {
            let now = Instant::now();
            match tokio::time::timeout(
                deadline.saturating_duration_since(now),
                self.child.records.recv(),
            )
            .await
            {
                Ok(Some((_, record))) => self.take(record)?,
                Ok(None) => break,
                Err(_) => {
                    return Err(self.violation(
                        "close",
                        format!(
                            "the child's output did not end within {CHILD_WITHIN:?} of its exit"
                        ),
                    ));
                }
            }
        }
        invariants::no_panic(self.panics.len())
            .map_err(|detail| self.violation("I2", format!("{detail}: {:?}", self.panics)))?;
        if let Err(error) = match exited {
            Ok(Ok(status)) if status.success() => Ok(()),
            other => Err(other),
        } {
            return Err(self.violation("I2", format!("the child ended with {error:?}")));
        }
        // Every record taken: a delivery that came after its channel was
        // compared, a late duplicate say, is seen too (Codex, #259).
        for (channel, expected) in &self.expected {
            let delivered: Vec<(u64, String)> = self.states[channel]
                .delivered
                .iter()
                .map(|(offset, data)| (*offset, id_of(data)))
                .collect();
            if &delivered != expected {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{channel}: {} deliveries once every record was taken, {} when compared",
                        delivered.len(),
                        expected.len()
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// The time of `suspensions` between `from` and `to`, an open one ending
/// at `now`, all in ms by the wall clock (I6).
fn overlap(suspensions: &[(u64, Option<u64>)], from: u64, to: u64, now: u64) -> Duration {
    Duration::from_millis(
        suspensions
            .iter()
            .map(|&(start, end)| end.unwrap_or(now).min(to).saturating_sub(start.max(from)))
            .sum(),
    )
}

/// The run-wide id a publication's data carries.
fn id_of(data: &str) -> String {
    serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|value| value["id"].as_str().map(str::to_owned))
        .unwrap_or_else(|| data.to_owned())
}

fn lifecycle(state: &str) -> Lifecycle {
    match state {
        "connected" | "subscribed" => Lifecycle::On,
        "connecting" | "subscribing" => Lifecycle::Pending,
        _ => Lifecycle::Off,
    }
}

static COUNTERS: std::sync::LazyLock<Mutex<Counters>> =
    std::sync::LazyLock::new(|| Mutex::new(Counters::default()));

fn log(line: &str) {
    println!("chaos_real: {line}");
}

/// The publisher (D5): 20 publications a second over the seed's channels,
/// round robin, each with a run-wide id, recording `(epoch, offset, id)`
/// from each reply; paused for the history snapshot.
fn publisher(
    environment: &Environment,
    channels: Vec<String>,
    id_prefix: String,
    published: Published,
    paused: Arc<AtomicBool>,
    gate: Gate,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let api = environment.api().clone();
    tokio::spawn(async move {
        let mut n = 0u64;
        let mut tick = tokio::time::interval(PUBLISH_EVERY);
        while !stop.load(Ordering::SeqCst) {
            tick.tick().await;
            let held = gate.lock().await;
            if paused.load(Ordering::SeqCst) {
                continue;
            }
            let channel = &channels[usize::try_from(n).unwrap_or(0) % channels.len()];
            let id = format!("{id_prefix}-{n}");
            let reply = api
                .call("publish", json!({"channel": channel, "data": {"id": id}}))
                .await;
            if let Ok(reply) = reply {
                let epoch = reply["epoch"].as_str().unwrap_or_default().to_owned();
                let offset = reply["offset"].as_u64().unwrap_or(0);
                published
                    .lock()
                    .unwrap()
                    .push((channel.clone(), epoch, offset, id));
            }
            drop(held);
            n += 1;
        }
    })
}

/// Runs of a seed so far, for channel names of their own.
static RUNS: AtomicU64 = AtomicU64::new(0);

/// Runs `seed` once (attempt `attempt` of D8), planting `plant`.
async fn run_seed(
    environment: &Environment,
    seed: u64,
    attempt: u32,
    plant: Plant,
) -> Result<(), (Violation, Vec<String>)> {
    let Plan {
        channels: count,
        connection_ttl_secs,
        subscription_ttl_secs,
        ops,
    } = operations(seed);
    // Channels of their own for every run of a seed, so that no history
    // is shared between runs.
    let nth = RUNS.fetch_add(1, Ordering::SeqCst);
    let base = format!(
        "{NAMESPACE}:{}-n{nth}-s{seed}-a{attempt}-",
        environment.run()
    );
    let mut channels: Vec<String> = (0..count).map(|n| format!("{base}c{n}")).collect();
    let canary = format!("{base}canary");
    let user = format!("chaos-{}", environment.run());
    let mut all = channels.clone();
    all.push(canary.clone());
    let child = ChildProcess::start(
        &environment.websocket_url(),
        &user,
        &all,
        (connection_ttl_secs, subscription_ttl_secs),
    );
    let published = Arc::new(Mutex::new(Vec::new()));
    let paused = Arc::new(AtomicBool::new(false));
    let gate: Gate = Arc::new(tokio::sync::Mutex::new(()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut sender_task = publisher(
        environment,
        channels.clone(),
        format!("{}-{seed}-{attempt}", environment.run()),
        Arc::clone(&published),
        Arc::clone(&paused),
        Arc::clone(&gate),
        Arc::clone(&stop),
    );
    let mut run = SeedRun {
        environment,
        seed,
        attempt,
        plant,
        ops,
        done: 0,
        channels: std::mem::take(&mut channels),
        canary,
        child,
        client: None,
        states: BTreeMap::new(),
        status: None,
        calls: Vec::new(),
        panics: Vec::new(),
        closed: false,
        dropped: false,
        deliveries: 0,
        published,
        paused,
        gate,
        fault: None,
        wanted_connected: true,
        expected: BTreeMap::new(),
        suspensions: Vec::new(),
        acknowledged: 0,
        restarts: 0,
    };
    let mut second = None;
    let checked = tokio::time::timeout(SEED_BOUND, async {
        run.run_ops().await?;
        run.heal().await?;
        run.calls_after_healing().await?;
        if plant == Plant::SecondConnection {
            second = Some(second_connection(environment, &user, &run.canary).await);
        }
        run.check_connections().await?;
        run.resubscribe_all().await?;
        run.check_deliveries().await?;
        run.close().await
    })
    .await;
    // D11: a seed is bounded at 120 s.
    let result = checked.unwrap_or_else(|_| {
        Err(run.violation("seed bound", format!("the seed ran past {SEED_BOUND:?}")))
    });
    let result = with_publisher(&run, result, &mut sender_task).await;
    stop.store(true, Ordering::SeqCst);
    sender_task.abort();
    if let Some(second) = second {
        second.close().await;
    }
    run.remove().await;
    let _ = environment.toxics().set_enabled(PROXY, true).await;
    let _ = run.child.process.start_kill();
    let log = run.child.log.lock().unwrap().clone();
    result.map_err(|violation| (violation, log))
}

/// The publisher ends only when stopped: one that ended before has failed,
/// and a seed without its workload cannot pass (Codex, #259).
async fn with_publisher(
    run: &SeedRun<'_>,
    result: Result<(), Violation>,
    sender_task: &mut tokio::task::JoinHandle<()>,
) -> Result<(), Violation> {
    match result {
        Ok(()) if sender_task.is_finished() => {
            let ended = sender_task.await;
            Err(run.violation(
                "publisher",
                format!("the publisher stopped during the seed: {ended:?}"),
            ))
        }
        Ok(()) if run.published.lock().unwrap().is_empty() => {
            Err(run.violation("publisher", "no publication was acknowledged"))
        }
        result => result,
    }
}

/// D9's planted I3 fault: a second real connection as the run's user,
/// subscribed to the canary.
async fn second_connection(
    environment: &Environment,
    user: &str,
    canary: &str,
) -> ferrifuge::Client {
    let token = support::token::connection_token(user, Duration::from_secs(3600));
    let client = ferrifuge::Client::new(
        environment.websocket_url(),
        ferrifuge::Config::default().token(token),
    )
    .expect("a second client");
    // The namespace allows no subscribe without a token (D4).
    let token_user = user.to_owned();
    let subscription = client
        .subscription_builder(canary.to_owned())
        .get_token(move |channel| {
            let token = support::token::subscription_token(
                &token_user,
                &channel,
                Duration::from_secs(3600),
            );
            async move { Ok(token) }
        })
        .build()
        .expect("its canary subscription");
    subscription.subscribe();
    client.connect();
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        while subscription.state() != ferrifuge::SubscriptionState::Subscribed {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    client
}

/// D8: a seed that violated an invariant is rerun once at once; only the
/// same invariant again is a finding.
async fn run_confirmed(environment: &Environment, seed: u64, plant: Plant) -> Outcome {
    let Err((first, log)) = run_seed(environment, seed, 0, plant).await else {
        return Outcome::Passed;
    };
    log_violation(&first, &log, environment).await;
    match run_seed(environment, seed, 1, plant).await {
        Err((second, log)) if second.invariant == first.invariant => {
            log_violation(&second, &log, environment).await;
            Outcome::Finding(second)
        }
        Err((second, log)) => {
            log_violation(&second, &log, environment).await;
            Outcome::Unconfirmed(first)
        }
        Ok(()) => Outcome::Unconfirmed(first),
    }
}

/// Writes a failing seed's child output and the containers' logs where
/// the workflow uploads them.
async fn log_violation(violation: &Violation, child_log: &[String], environment: &Environment) {
    log(&format!("violation: {violation}"));
    let dir = env::var_os(ARTIFACTS_VARIABLE).map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/chaos-real"),
        std::path::PathBuf::from,
    );
    let _ = std::fs::create_dir_all(&dir);
    let mut text = format!("{violation}\n\n==== child\n");
    for line in child_log {
        let _ = writeln!(text, "{line}");
    }
    text.push_str(&environment.logs().await);
    let _ = std::fs::write(
        dir.join(format!(
            "seed-{}-attempt-{}-{}.log",
            violation.seed, violation.attempt, violation.invariant
        )),
        text,
    );
}

/// "A seed of the real-server runner fixes its operations".
fn seeds_fix_their_operations() {
    assert_eq!(operations(7), operations(7), "seed 7 gives two lists");
    assert_ne!(operations(7), operations(8), "seeds 7 and 8 give one list");
    log("seed 7 gives the same operations twice, seed 8 others");
}

/// How long a restart scenario waits for a record: the restart, and the
/// reconnect after it, its backoff included.
const SCENARIO_WITHIN: Duration = Duration::from_secs(40);

/// A restart scenario's client: a child subscribed to one channel, with
/// tokens of an hour, and the publications it delivered.
struct RestartScenario {
    name: &'static str,
    child: ChildProcess,
    channel: String,
    /// Epoch and offset of each publication delivered, in order.
    delivered: Vec<(Option<String>, u64)>,
}

impl RestartScenario {
    /// The child, its channel subscribed as its canary, the first one.
    fn start(name: &'static str, environment: &Environment, channel: &str) -> Self {
        let user = format!("restart-{}", environment.run());
        let child = ChildProcess::start(
            &environment.websocket_url(),
            &user,
            &[channel.to_owned()],
            (3600, 3600),
        );
        Self {
            name,
            child,
            channel: channel.to_owned(),
            delivered: Vec::new(),
        }
    }

    /// The next record `wanted` takes, the publications before it kept.
    async fn until(&mut self, what: &str, mut wanted: impl FnMut(&Record) -> bool) -> Record {
        let deadline = Instant::now() + SCENARIO_WITHIN;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let record = match tokio::time::timeout(left, self.child.records.recv()).await {
                Ok(Some((_, record))) => record,
                Ok(None) => panic!("{}: the child ended before {what}", self.name),
                Err(elapsed) => panic!("{}: no {what}: {elapsed}", self.name),
            };
            if let Record::Pub { offset, epoch, .. } = &record {
                self.delivered
                    .push((epoch.clone(), offset.expect("a publication's offset")));
            }
            if wanted(&record) {
                return record;
            }
        }
    }

    /// The next `subscribed`: `was_recovering`, `recovered`, epoch and offset.
    async fn subscribed(&mut self) -> (bool, bool, Option<String>, Option<u64>) {
        match self
            .until(
                "subscribed",
                |record| matches!(record, Record::Sub { state, .. } if state == "subscribed"),
            )
            .await
        {
            Record::Sub {
                was_recovering,
                recovered,
                epoch,
                offset,
                ..
            } => (was_recovering, recovered, epoch, offset),
            _ => unreachable!("a subscribed record"),
        }
    }

    /// Publishes `n` through the server API: its epoch and offset.
    async fn publish(&self, environment: &Environment, n: u64) -> (String, u64) {
        let reply = environment
            .api()
            .call(
                "publish",
                json!({"channel": self.channel, "data": {"id": format!("p{n}")}}),
            )
            .await
            .unwrap_or_else(|error| panic!("{}: publication {n}: {}", self.name, error.message));
        (
            reply["epoch"].as_str().unwrap_or_default().to_owned(),
            reply["offset"].as_u64().unwrap_or(0),
        )
    }

    /// Waits until the publication at `offset` is delivered, among the
    /// deliveries from the `from`th on.
    async fn delivered_at(&mut self, from: usize, offset: u64) {
        if self.delivered[from.min(self.delivered.len())..]
            .iter()
            .any(|(_, at)| *at == offset)
        {
            return;
        }
        self.until(
            &format!("the publication at offset {offset}"),
            |record| matches!(record, Record::Pub { offset: Some(at), .. } if *at == offset),
        )
        .await;
    }

    /// The child closed and gone.
    async fn end(mut self) {
        self.child.send(&Command_::Close).await;
        let _ = tokio::time::timeout(
            CHILD_WITHIN,
            self.until("closed", |record| matches!(record, Record::Closed)),
        )
        .await;
        self.child.send(&Command_::Exit).await;
        let _ = tokio::time::timeout(CHILD_WITHIN, self.child.process.wait()).await;
        let _ = self.child.process.start_kill();
    }
}

/// "A server restart with the memory engine reports recovered false"
/// (D6): run once, without D8's rerun (reviewer, 51c).
async fn restart_with_memory(environment: &Environment) {
    let name = "A server restart with the memory engine reports recovered false";
    let channel = format!("{NAMESPACE}:{}-restart-memory", environment.run());
    let mut scenario = RestartScenario::start(name, environment, &channel);
    let (_, _, before, _) = scenario.subscribed().await;
    for n in 1..=3 {
        let (_, offset) = scenario.publish(environment, n).await;
        scenario.delivered_at(0, offset).await;
    }
    let mut restarts = 0;
    environment
        .restart(true)
        .await
        .unwrap_or_else(|error| panic!("the environment: {name}: {error}"));
    restarts += 1;
    let (was_recovering, recovered, epoch, position) = scenario.subscribed().await;
    assert!(
        was_recovering && !recovered && epoch.is_some() && epoch != before,
        "{name}: subscribed with was_recovering {was_recovering}, recovered {recovered}, epoch {epoch:?}, before the restart {before:?}"
    );
    // The offsets restart after that subscribed: the next publication
    // is the first delivered since, at the offset the server gave it, in
    // the subscribed's epoch: a publication carries an epoch only when the
    // epoch changes; it is checked when present.
    let since = scenario.delivered.len();
    let (published_epoch, offset) = scenario.publish(environment, 4).await;
    scenario.delivered_at(since, offset).await;
    let (delivered_epoch, delivered) = scenario.delivered[since].clone();
    assert!(
        delivered == offset
            && Some(offset) == position.map(|at| at + 1)
            && epoch.as_deref() == Some(published_epoch.as_str())
            && delivered_epoch
                .as_ref()
                .is_none_or(|at| Some(at) == epoch.as_ref()),
        "{name}: delivered ({delivered_epoch:?}, {delivered}); published ({published_epoch}, {offset}), subscribed at ({epoch:?}, {position:?})"
    );
    // The scenario's own restarts, not the job's counter (reviewer, 51c).
    assert_eq!(restarts, 1, "{name}: restarts");
    scenario.end().await;
    log(&format!("{name}: passed"));
}

/// "A server restart with the Redis engine loses nothing" (D6), in an
/// environment of its own: run once, without D8's rerun (reviewer, 51c).
async fn restart_with_redis(run: &str, engine: Engine) {
    let name = "A server restart with the Redis engine loses nothing";
    let environment = Environment::start(run, engine, &configuration(engine))
        .await
        .unwrap_or_else(|error| panic!("the environment: {name}: {error}"));
    let channel = format!("{NAMESPACE}:{}-restart-redis", environment.run());
    let mut scenario = RestartScenario::start(name, &environment, &channel);
    let (_, _, before, _) = scenario.subscribed().await;
    for n in 1..=3 {
        let (_, offset) = scenario.publish(&environment, n).await;
        scenario.delivered_at(0, offset).await;
    }
    let toxics = environment.toxics();
    toxics
        .set_enabled(PROXY, false)
        .await
        .unwrap_or_else(|error| panic!("the environment: {name}: {error}"));
    environment
        .restart(true)
        .await
        .unwrap_or_else(|error| panic!("the environment: {name}: {error}"));
    let (published_epoch, offset) = scenario.publish(&environment, 4).await;
    toxics
        .set_enabled(PROXY, true)
        .await
        .unwrap_or_else(|error| panic!("the environment: {name}: {error}"));
    let (_, recovered, epoch, _) = scenario.subscribed().await;
    assert!(recovered, "{name}: subscribed with recovered false");
    assert!(
        epoch == before && epoch.as_deref() == Some(published_epoch.as_str()),
        "{name}: epoch {epoch:?} after the restart, {before:?} before, publication 4 in {published_epoch}"
    );
    scenario.delivered_at(0, offset).await;
    // A duplicate would follow at once: a moment for it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    while let Ok((_, record)) = scenario.child.records.try_recv() {
        if let Record::Pub { offset, epoch, .. } = record {
            scenario
                .delivered
                .push((epoch, offset.expect("a publication's offset")));
        }
    }
    let offsets: Vec<u64> = scenario.delivered.iter().map(|(_, at)| *at).collect();
    assert_eq!(offsets, [1, 2, 3, 4], "{name}: the offsets delivered");
    scenario.end().await;
    log(&format!("{name}: passed"));
}

/// I6's allowance is the call's overlap with the suspensions, no more
/// (D4, "Scheduling").
fn suspensions_overlap_calls() {
    let suspensions = [(1_000, Some(3_000)), (5_000, Some(6_000)), (9_000, None)];
    for (from, to, overlapped) in [
        (0, 500, 0),
        (500, 2_000, 1_000),
        (2_000, 5_500, 1_500),
        (0, 10_000, 3_900),
        (3_000, 5_000, 0),
        (9_500, 9_800, 300),
    ] {
        assert_eq!(
            overlap(&suspensions, from, to, 9_900),
            Duration::from_millis(overlapped),
            "a call from {from} to {to} ms"
        );
    }
    log("a call's I6 bound adds its overlap with the suspensions");
}

/// An operation that holds the others fits the window only with the
/// fault in place held out first (reviewer, 51c, item 1).
fn holds_fit_the_window() {
    let s = Duration::from_secs;
    assert!(
        fits(s(30), s(0), s(10)),
        "30 s in, nothing in place, 10 s held"
    );
    assert!(fits(s(25), s(10), s(10)), "ending at 45 s");
    assert!(
        !fits(s(30), s(10), s(10)),
        "the fault in place held out first"
    );
    assert!(!fits(s(40), s(0), s(10)), "ending past 45 s");
    log("a held operation runs only if it ends within the window");
}

/// The child's token judgements without a server (reviewer, 51c, items 2
/// and 3): a fetch is a refresh only while the client or subscription is
/// established by its own state, and one after `disconnect()`, before the
/// next `connect()`, is seen.
async fn token_fetches_are_judged() {
    let client = ferrifuge::Client::builder("ws://127.0.0.1:9/connection/websocket")
        .build()
        .expect("a client");
    let slot = OnceLock::new();
    assert!(!connection_established(&slot), "no client set");
    let _ = slot.set(client.clone());
    assert!(!connection_established(&slot), "a client never connected");
    let subscription = client
        .new_subscription("recoverable:check")
        .expect("a subscription");
    let sub_slot = OnceLock::new();
    let _ = sub_slot.set(subscription.clone());
    subscription.subscribe();
    assert!(
        !subscription_established(&sub_slot),
        "a subscription not subscribed"
    );
    let off = Off::default();
    assert!(!off.fetched_off(), "before disconnect()");
    off.disconnect(&client);
    assert!(off.fetched_off(), "after disconnect()");
    off.connect(&client);
    assert!(!off.fetched_off(), "after connect()");
    off.disconnect(&client);
    client.close().await;
    log("token fetches are judged by the client's own state and I5's flag");
}

/// The budget's seeds, or `FERRIFUGE_REAL_CHAOS_SEED` alone.
async fn run_budget(environment: &Environment) -> Vec<Violation> {
    let budget = env::var(BUDGET_VARIABLE)
        .ok()
        .and_then(|seconds| seconds.parse().ok())
        .map_or(DEFAULT_BUDGET, Duration::from_secs);
    let only: Option<u64> = env::var(SEED_VARIABLE)
        .ok()
        .and_then(|seed| seed.parse().ok());
    let start = Instant::now();
    let mut findings = Vec::new();
    let mut seed = only.unwrap_or(0);
    loop {
        let outcome = run_confirmed(environment, seed, Plant::None).await;
        let mut counters = COUNTERS.lock().unwrap();
        counters.add("seeds");
        match outcome {
            Outcome::Passed => log(&format!("seed {seed} passed")),
            Outcome::Unconfirmed(violation) => {
                counters.add("unconfirmed");
                log(&format!("seed {seed} unconfirmed: {violation}"));
            }
            Outcome::Finding(violation) => {
                counters.add("findings");
                findings.push(violation);
            }
        }
        drop(counters);
        if only.is_some() || start.elapsed() >= budget {
            return findings;
        }
        seed += 1;
    }
}

/// "The real-server runner reports a planted fault", and D8's rerun.
async fn planted_faults_are_reported(environment: &Environment) {
    for (plant, invariant) in [
        (
            Plant::DropDelivery {
                every_attempt: true,
            },
            "I8",
        ),
        (Plant::SecondConnection, "I3"),
    ] {
        match run_confirmed(environment, 1_000, plant).await {
            Outcome::Finding(violation) if violation.invariant == invariant => {
                log(&format!("planted {plant:?} reported: {violation}"));
            }
            other => panic!("planted {plant:?}: expected a finding of {invariant}, got {other:?}"),
        }
    }
    // "A failing real-server seed is a finding only when it fails twice":
    // dropped on the first attempt alone, it is counted unconfirmed.
    match run_confirmed(
        environment,
        1_000,
        Plant::DropDelivery {
            every_attempt: false,
        },
    )
    .await
    {
        Outcome::Unconfirmed(violation) if violation.invariant == "I8" => {
            log(&format!(
                "a violation the rerun did not repeat is unconfirmed: {violation}"
            ));
        }
        other => panic!("a violation not repeated: expected it unconfirmed, got {other:?}"),
    }
}

fn main() {
    if env::var(ROLE).as_deref() == Ok("client") {
        child();
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("chaos_real skipped: set {RUN}=1, with Docker");
        return;
    }
    seeds_fix_their_operations();
    suspensions_overlap_calls();
    holds_fit_the_window();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let findings = runtime.block_on(async {
        token_fetches_are_judged().await;
        let run = format!("r{}", std::process::id());
        let scenarios = env::var_os(SEED_VARIABLE).is_none();
        // The Redis scenario's environment, gone before the run's own: both
        // publish the same host ports.
        if scenarios {
            restart_with_redis(&format!("{run}redis"), Engine::Redis).await;
        }
        let environment = Environment::start(&run, Engine::Memory, &configuration(Engine::Memory))
            .await
            .unwrap_or_else(|error| panic!("the environment: {error}"));
        if scenarios {
            restart_with_memory(&environment).await;
            planted_faults_are_reported(&environment).await;
        }
        run_budget(&environment).await
    });
    let counters = COUNTERS.lock().unwrap();
    for (name, count) in &counters.counts {
        println!("@P report {name}: {count}");
    }
    assert!(findings.is_empty(), "findings: {findings:#?}");
    // D10, and the reviewer's outcomes (51a): calls of each kind that
    // succeeded, reconnects and recoveries.
    let unseen: Vec<_> = [
        "call history ok",
        "call presence ok",
        "reconnect",
        "recovered true",
        "recovered false after a restart",
    ]
    .into_iter()
    .filter(|name| counters.counts.get(*name).copied().unwrap_or(0) == 0)
    .collect();
    assert!(
        unseen.is_empty() || env::var_os(SEED_VARIABLE).is_some(),
        "outcomes never counted: {unseen:?} (D10)"
    );
    // D10: every fault kind implemented reached, over the whole job.
    // D4's restarts, both kinds, and the token callbacks of both kinds.
    let missing: Vec<String> = FaultKind::ALL
        .iter()
        .map(|kind| format!("fault {kind:?}"))
        .chain(
            [
                "restart graceful",
                "restart hard",
                "suspension across restart",
                "token connection",
                "token subscription",
            ]
            .map(str::to_owned),
        )
        .filter(|name| !counters.counts.contains_key(name))
        .collect();
    assert!(
        missing.is_empty() || env::var_os(SEED_VARIABLE).is_some(),
        "fault kinds never applied: {missing:?}; a budget too small for them fails (D10)"
    );
}
