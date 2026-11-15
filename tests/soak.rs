//! Soak run (Block 52): hours of normal use against a real Centrifugo,
//! with a server-forced reconnect every period and every token refresh
//! due, keep the live heap, the timers and the pending calls flat and
//! deliver every publication once. Needs Docker, so it runs only with
//! `FERRIFUGE_SOAK=1`; `cargo test` and `--all-targets` leave it out.
//!
//! The binary is both sides, as Block 51's runner is (D1). The parent
//! starts Centrifugo (no Toxiproxy), publishes, forces the reconnects,
//! makes the calls and checks; re-executed with
//! `FERRIFUGE_SOAK_ROLE=client` it is the child, which owns one `Client`
//! with 10 subscriptions, counts its own heap with dhat, samples its timers
//! every second and answers checkpoints, writing NDJSON records.
//!
//! `FERRIFUGE_SOAK_SCALE` N divides the timeline (D12): a run with N other
//! than 1 is evidence of the checker only, and is refused on CI.

// @P scenario: Hours of normal use keep resources flat and delivery exact

// Counts and sizes are far below 2^52.
#![allow(clippy::cast_precision_loss)]

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use support::ndjson::{Line, Writer, connection_established, subscription_established};
use support::proxy::ProxyBackend;
use support::real_server::{Engine, Environment, configuration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Enables the soak.
const RUN: &str = "FERRIFUGE_SOAK";
/// D12's N.
const SCALE_VARIABLE: &str = "FERRIFUGE_SOAK_SCALE";
const ROLE: &str = "FERRIFUGE_SOAK_ROLE";
const ENDPOINT_VARIABLE: &str = "FERRIFUGE_SOAK_ENDPOINT";
const USER_VARIABLE: &str = "FERRIFUGE_SOAK_USER";
const CHANNELS_VARIABLE: &str = "FERRIFUGE_SOAK_CHANNELS";
/// The ttls, in seconds, of the child's connection and subscription
/// tokens, `<connection>,<subscription>`.
const TTL_VARIABLE: &str = "FERRIFUGE_SOAK_TTL";
/// Where the checkpoints and the summary are written.
const ARTIFACTS_VARIABLE: &str = "FERRIFUGE_SOAK_ARTIFACTS";

/// D1: 10 subscriptions.
const CHANNELS: usize = 10;
/// The namespace of every channel (Block 51 D5).
const NAMESPACE: &str = "recoverable";
/// D2: the measured run.
const DURATION: Duration = Duration::from_secs(4 * 3600);
/// D3: one publication per second on each channel.
const PUBLISH_EVERY: Duration = Duration::from_secs(1);
/// D4: the tokens' ttls.
const CONNECTION_TTL: Duration = Duration::from_secs(20 * 60);
const SUBSCRIPTION_TTL: Duration = Duration::from_secs(30 * 60);
/// D5: a server-forced reconnect every period, with an application code
/// the client reconnects on (Block 3).
const PERIOD: Duration = Duration::from_secs(5 * 60);
const RECONNECT_CODE: u32 = 4000;
/// A call whose transport closed before its reply (Block 13).
const CONNECTION_CLOSED: u32 = 11;
/// D5, D6: the checkpoints, offset from the reconnects.
const CHECKPOINT_OFFSET: Duration = Duration::from_secs(150);
/// D7: the warm-up, and the tolerance.
const WARM_UP: Duration = Duration::from_secs(30 * 60);
const TOLERANCE: f64 = 0.01;
const FLOOR: f64 = 64.0 * 1024.0;
/// D8: a call every period, history and presence in turn.
const CALL_EVERY: Duration = Duration::from_secs(30);
/// D8: how long a checkpoint waits for rest.
const REST_WITHIN: Duration = Duration::from_secs(30);
/// D8: how far past its deadline a listed timer may be.
const OVERDUE: Duration = Duration::from_secs(1);
/// The child's `Config::timeout`: its default (D8).
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the runner waits for the child or the server to answer.
const WITHIN: Duration = Duration::from_secs(30);
/// The scenario's counts (D4, D5).
const MIN_CONNECTION_REFRESHES: u64 = 3;
const MIN_SUBSCRIPTION_REFRESHES: u64 = 3;
const MIN_RECONNECTS: u64 = 40;
/// D3: the history the server keeps.
const HISTORY_SIZE: u64 = 2000;
const HISTORY_TTL: &str = "3600s";

// --- Records -----------------------------------------------------------------

/// What the parent asks of the child, one JSON line each.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "do", rename_all = "snake_case")]
enum Command_ {
    History {
        channel: String,
    },
    Presence {
        channel: String,
    },
    /// An RPC whose reply the proxy backend holds (D8).
    Held,
    Checkpoint,
    Close,
    Exit,
}

/// A timer as a checkpoint reports it.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Timer_ {
    name: String,
    age_ms: u64,
    due_in_ms: u64,
    overdue_ms: u64,
}

/// A pending call as a checkpoint reports it.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Call_ {
    id: String,
    age_ms: u64,
    registries: Vec<String>,
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
    },
    Call {
        call: String,
        ok: bool,
        /// The error's code, when it failed.
        code: Option<u32>,
        took_ms: u64,
    },
    /// A token fetched: a refresh when the connection or subscription was
    /// established.
    Token {
        of: String,
        refresh: bool,
    },
    /// A timer the client's or a subscription's state does not allow,
    /// listed in two samples a second apart (D8).
    TimerNotAllowed {
        name: String,
        client: String,
    },
    Checkpoint {
        at_rest: bool,
        waited_ms: u64,
        live_bytes: u64,
        rss_bytes: u64,
        client: String,
        subs: BTreeMap<String, String>,
        driver: Vec<Timer_>,
        core: Vec<Timer_>,
        calls: Vec<Call_>,
    },
    Panic {
        message: String,
    },
    Closed,
}

fn state_name(state: impl std::fmt::Debug) -> String {
    format!("{state:?}").to_lowercase()
}

/// D12's N, one of 1, 2, 5 and 10; refused on CI unless 1.
fn scale() -> u32 {
    let scale = env::var(SCALE_VARIABLE)
        .ok()
        .filter(|value| !value.is_empty())
        .map_or(1, |value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{SCALE_VARIABLE}: not a number: {value:?}"))
        });
    assert!(
        [1, 2, 5, 10].contains(&scale),
        "{SCALE_VARIABLE}: {scale} is not one of 1, 2, 5 and 10 (D12)"
    );
    assert!(
        scale == 1 || env::var_os("GITHUB_ACTIONS").is_none(),
        "{SCALE_VARIABLE}={scale}: a scaled run is evidence of the checker only, refused on CI (D12)"
    );
    scale
}

/// The timeline at scale `n` (D12).
#[derive(Clone, Copy, Debug, Serialize)]
struct Timeline {
    scale: u32,
    duration: Duration,
    period: Duration,
    checkpoint_offset: Duration,
    warm_up: Duration,
    call_every: Duration,
    connection_ttl: Duration,
    subscription_ttl: Duration,
}

impl Timeline {
    fn at(scale: u32) -> Self {
        Self {
            scale,
            duration: DURATION / scale,
            period: PERIOD / scale,
            checkpoint_offset: CHECKPOINT_OFFSET / scale,
            warm_up: WARM_UP / scale,
            call_every: CALL_EVERY / scale,
            connection_ttl: CONNECTION_TTL / scale,
            subscription_ttl: SUBSCRIPTION_TTL / scale,
        }
    }
}

// --- The child -----------------------------------------------------------------

fn child() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(child_run());
}

/// The live heap the child's allocator counts.
fn live_bytes() -> u64 {
    u64::try_from(dhat::HeapStats::get().curr_bytes).unwrap_or(u64::MAX)
}

/// The process's resident set, reported only (D6).
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

async fn child_run() {
    let writer: Writer<Record> = Writer::start();
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
    let ttls: Vec<u64> = env::var(TTL_VARIABLE)
        .expect("the child's ttls")
        .split(',')
        .filter_map(|ttl| ttl.parse().ok())
        .collect();
    let (connection_ttl, subscription_ttl) =
        (Duration::from_secs(ttls[0]), Duration::from_secs(ttls[1]));

    let slot = Arc::new(OnceLock::new());
    let (token_slot, token_writer, token_user) = (Arc::clone(&slot), writer.clone(), user.clone());
    let client = ferrifuge::Client::builder(endpoint)
        .get_token(move |_| {
            token_writer.write(Record::Token {
                of: "connection".to_owned(),
                refresh: connection_established(&token_slot),
            });
            let token = support::token::connection_token(&token_user, connection_ttl);
            async move { Ok(token) }
        })
        .build()
        .expect("a client");
    let _ = slot.set(client.clone());
    // Before anything sets a timer (D6).
    client.record_timers();
    forward_client_events(&client, &writer);
    let subscriptions = child_subscriptions(&client, &channels, &writer, &user, subscription_ttl);
    for subscription in subscriptions.values() {
        subscription.subscribe();
    }
    client.connect();
    sample_timers(&client, &subscriptions, &writer);

    let mut commands = commands_from_stdin();
    let pending = Arc::new(AtomicU64::new(0));
    while let Some(command) = commands.recv().await {
        match command {
            Command_::History { channel } => {
                let call = client.history(channel, ferrifuge::HistoryOptions::default().limit(1));
                spawn_call(
                    &writer,
                    &pending,
                    "history",
                    async move { call.await.map(drop) },
                );
            }
            Command_::Presence { channel } => {
                let call = client.presence(channel);
                spawn_call(&writer, &pending, "presence", async move {
                    call.await.map(drop)
                });
            }
            Command_::Held => {
                let call = client.rpc("held", b"{}".to_vec());
                spawn_call(
                    &writer,
                    &pending,
                    "held",
                    async move { call.await.map(drop) },
                );
            }
            Command_::Checkpoint => {
                let (client, subscriptions, writer) =
                    (client.clone(), subscriptions.clone(), writer.clone());
                tokio::spawn(async move { checkpoint(&client, &subscriptions, &writer).await });
            }
            Command_::Close => {
                client.close().await;
                writer.write(Record::Closed);
            }
            Command_::Exit => {
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

fn forward_client_events(client: &ferrifuge::Client, writer: &Writer<Record>) {
    let mut events = client.events();
    let writer = writer.clone();
    tokio::spawn(async move {
        use futures_util::StreamExt;
        while let Some(event) = events.next().await {
            let (state, code) = match &event {
                ferrifuge::ClientEvent::Connecting(ferrifuge::ConnectingContext {
                    code, ..
                }) => ("connecting", *code),
                ferrifuge::ClientEvent::Connected(ferrifuge::ConnectedContext { .. }) => {
                    ("connected", 0)
                }
                ferrifuge::ClientEvent::Disconnected(ferrifuge::DisconnectedContext {
                    code,
                    ..
                }) => ("disconnected", *code),
                _ => continue,
            };
            writer.write(Record::Client {
                state: state.to_owned(),
                code,
            });
        }
    });
}

fn child_subscriptions(
    client: &ferrifuge::Client,
    channels: &[String],
    writer: &Writer<Record>,
    user: &str,
    ttl: Duration,
) -> BTreeMap<String, ferrifuge::Subscription> {
    let mut subscriptions = BTreeMap::new();
    for channel in channels {
        let (token_user, token_writer) = (user.to_owned(), writer.clone());
        let slot = Arc::new(OnceLock::new());
        let token_slot = Arc::clone(&slot);
        let subscription = client
            .subscription_builder(channel.clone())
            .get_token(move |ferrifuge::SubscriptionTokenContext { channel, .. }| {
                token_writer.write(Record::Token {
                    of: "subscription".to_owned(),
                    refresh: subscription_established(&token_slot),
                });
                let token = support::token::subscription_token(&token_user, &channel, ttl);
                async move { Ok(token) }
            })
            .build()
            .expect("a subscription");
        let _ = slot.set(subscription.clone());
        let mut events = subscription.events();
        let (writer, name) = (writer.clone(), channel.clone());
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
                    },
                    _ => continue,
                };
                writer.write(record);
            }
        });
        subscriptions.insert(channel.clone(), subscription);
    }
    subscriptions
}

fn sub_record(
    channel: &str,
    state: &str,
    subscribed: Option<&ferrifuge::SubscribedContext>,
) -> Record {
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
    writer: &Writer<Record>,
    pending: &Arc<AtomicU64>,
    call: &'static str,
    settled: impl std::future::Future<Output = Result<(), ferrifuge::CallError>> + Send + 'static,
) {
    let (writer, pending) = (writer.clone(), Arc::clone(pending));
    pending.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        let start = Instant::now();
        let settled = settled.await;
        writer.write(Record::Call {
            call: call.to_owned(),
            ok: settled.is_ok(),
            code: settled.err().map(|error| error.code),
            took_ms: millis(start.elapsed()),
        });
        pending.fetch_sub(1, Ordering::SeqCst);
    });
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The kind of a timer's name: `client ServerPing` gives `ServerPing`.
fn kind_of(name: &str) -> &str {
    name.rsplit(' ').next().unwrap_or(name)
}

/// Whether the client's and the subscriptions' states allow `name` (D8).
/// A call's timers are allowed while it runs, a refresh's reply deadline
/// while connected, for the connection and each subscription alike; the connect deadline and the reconnect delay
/// only while connecting; a subscribe's only while a subscription is not
/// subscribed. Refresh timers run on across a reconnect (Block 25 D3).
fn allowed(name: &str, client: ferrifuge::State, all_subscribed: bool) -> bool {
    let kind = kind_of(name);
    if name.starts_with("call ") {
        return true;
    }
    if name.starts_with("client ") {
        return match kind {
            "Refresh" => true,
            "ServerPing" | "RefreshReply" => client == ferrifuge::State::Connected,
            "Reconnect" | "ConnectTimeout" => client == ferrifuge::State::Connecting,
            _ => false,
        };
    }
    if name.starts_with("subscription ") {
        return match kind {
            "Refresh" => true,
            // As the client's own: only a connection carries it (reviewer, 52a).
            "RefreshReply" => client == ferrifuge::State::Connected,
            "SubscribeReply" | "Resubscribe" | "UnsubscribeReply" => !all_subscribed,
            _ => false,
        };
    }
    false
}

/// D8: every second the child lists its timers; one its state does not
/// allow, listed in two samples in a row, is recorded once.
fn sample_timers(
    client: &ferrifuge::Client,
    subscriptions: &BTreeMap<String, ferrifuge::Subscription>,
    writer: &Writer<Record>,
) {
    let (client, subscriptions, writer) = (client.clone(), subscriptions.clone(), writer.clone());
    tokio::spawn(async move {
        let mut previous: BTreeSet<String> = BTreeSet::new();
        let mut reported: BTreeSet<String> = BTreeSet::new();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let state = client.state();
            let all_subscribed = subscriptions
                .values()
                .all(|s| s.state() == ferrifuge::SubscriptionState::Subscribed);
            let timers = client.armed_timers();
            let now: BTreeSet<String> = timers
                .driver
                .iter()
                .chain(&timers.core)
                .map(|timer| timer.name.clone())
                .filter(|name| !allowed(name, state, all_subscribed))
                .collect();
            for name in now.intersection(&previous) {
                if reported.insert(name.clone()) {
                    writer.write(Record::TimerNotAllowed {
                        name: name.clone(),
                        client: state_name(state),
                    });
                }
            }
            previous = now;
        }
    });
}

fn timers_of(list: &[ferrifuge::ArmedTimer]) -> Vec<Timer_> {
    list.iter()
        .map(|timer| Timer_ {
            name: timer.name.clone(),
            age_ms: millis(timer.age),
            due_in_ms: millis(timer.due_in),
            overdue_ms: millis(timer.overdue),
        })
        .collect()
}

/// The timers a client at rest runs (D8): the ping deadline, the
/// connection refresh and one refresh per subscription.
fn at_rest_timers(names: &BTreeSet<String>, subscriptions: usize) -> bool {
    let client: BTreeSet<&str> = names
        .iter()
        .filter(|name| name.starts_with("client "))
        .map(String::as_str)
        .collect();
    let refreshes = names
        .iter()
        .filter(|name| name.starts_with("subscription ") && kind_of(name) == "Refresh")
        .count();
    client == BTreeSet::from(["client Refresh", "client ServerPing"])
        && refreshes == subscriptions
        && names.len() == 2 + subscriptions
}

/// D6, D8: waits for rest at most [`REST_WITHIN`], then reads.
async fn checkpoint(
    client: &ferrifuge::Client,
    subscriptions: &BTreeMap<String, ferrifuge::Subscription>,
    writer: &Writer<Record>,
) {
    let start = Instant::now();
    let at_rest = loop {
        let timers = client.armed_timers();
        let driver: BTreeSet<String> = timers.driver.iter().map(|t| t.name.clone()).collect();
        let core: BTreeSet<String> = timers.core.iter().map(|t| t.name.clone()).collect();
        let rest = client.state() == ferrifuge::State::Connected
            && subscriptions
                .values()
                .all(|s| s.state() == ferrifuge::SubscriptionState::Subscribed)
            && client.pending_calls().is_empty()
            && driver == core
            && at_rest_timers(&core, subscriptions.len());
        if rest {
            break true;
        }
        if start.elapsed() >= REST_WITHIN {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let timers = client.armed_timers();
    writer.write(Record::Checkpoint {
        at_rest,
        waited_ms: millis(start.elapsed()),
        live_bytes: live_bytes(),
        rss_bytes: rss_bytes(),
        client: state_name(client.state()),
        subs: subscriptions
            .iter()
            .map(|(name, s)| (name.clone(), state_name(s.state())))
            .collect(),
        driver: timers_of(&timers.driver),
        core: timers_of(&timers.core),
        calls: client
            .pending_calls()
            .into_iter()
            .map(|call| Call_ {
                id: call.id,
                age_ms: millis(call.age),
                registries: call.registries.iter().map(|r| (*r).to_owned()).collect(),
            })
            .collect(),
    });
}

// --- The parent ------------------------------------------------------------------

/// The child process.
struct ChildProcess {
    process: Child,
    stdin: ChildStdin,
    records: mpsc::UnboundedReceiver<Record>,
}

impl ChildProcess {
    fn start(endpoint: &str, user: &str, channels: &[String], timeline: &Timeline) -> Self {
        let mut process = Command::new(env::current_exe().expect("the runner"))
            .env(ROLE, "client")
            .env(ENDPOINT_VARIABLE, endpoint)
            .env(USER_VARIABLE, user)
            .env(CHANNELS_VARIABLE, channels.join(","))
            .env(
                TTL_VARIABLE,
                format!(
                    "{},{}",
                    timeline.connection_ttl.as_secs(),
                    timeline.subscription_ttl.as_secs()
                ),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("the child");
        let stdin = process.stdin.take().expect("the child's stdin");
        let stdout = process.stdout.take().expect("the child's stdout");
        let (sender, records) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(Line { record, .. }) = serde_json::from_str::<Line<Record>>(&line) {
                    let _ = sender.send(record);
                }
            }
        });
        Self {
            process,
            stdin,
            records,
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
struct Channel {
    /// The offset of the first `subscribed`, and its epoch.
    start: Option<u64>,
    epoch: Option<String>,
    delivered: Vec<u64>,
    /// D9's oracle: every history read, accumulated by offset.
    history: BTreeMap<u64, String>,
    history_epoch: Option<String>,
    state: String,
}

/// A failure of the run, with what it broke.
#[derive(Debug)]
struct Failure(String);

type Checked = Result<(), Failure>;

fn fail(what: impl Into<String>) -> Checked {
    Err(Failure(what.into()))
}

/// The run's counts (D4, D5, D9).
#[derive(Default, Debug, Serialize)]
struct Counts {
    connection_refreshes: u64,
    subscription_refreshes: u64,
    reconnects: u64,
    unrecovered: u64,
    calls_ok: u64,
    calls_failed: u64,
    deliveries: u64,
    /// The publications acknowledged (D3).
    publications: u64,
    /// The RPCs held at a disconnect, and those that failed with the
    /// connection's loss (D8).
    held: u64,
    held_failed: u64,
}

/// A checkpoint as the checker keeps it.
#[derive(Debug, Serialize)]
struct Kept {
    at: Duration,
    live_bytes: u64,
    rss_bytes: u64,
}

struct Soak<'a> {
    environment: &'a Environment,
    /// The RPC proxy backend, which holds D8's RPC.
    backend: &'a ProxyBackend,
    timeline: Timeline,
    child: ChildProcess,
    channels: BTreeMap<String, Channel>,
    client: String,
    counts: Counts,
    checkpoints: Vec<Kept>,
    started: Instant,
    /// Every checkpoint record, for the artifact.
    artifact: Vec<Value>,
    published: Arc<Mutex<Vec<(String, String, u64)>>>,
    /// Set once the run calls `close()`, whose `disconnected` is expected
    /// (Codex, #264).
    closing: bool,
}

impl Soak<'_> {
    /// Takes the child's records until `deadline`, checking each.
    async fn pump(&mut self, deadline: Instant) -> Checked {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            match tokio::time::timeout(deadline - now, self.child.records.recv()).await {
                Ok(Some(record)) => self.take(record)?,
                Ok(None) => return fail("the child's output ended"),
                Err(_) => return Ok(()),
            }
        }
    }

    fn take(&mut self, record: Record) -> Checked {
        match record {
            Record::Client { state, code } => {
                if state == "connecting" && self.client == "connected" {
                    self.counts.reconnects += 1;
                    // D8: only D5's disconnects; a 3005 close or any other
                    // loss fails the run instead of healing silently.
                    if code != RECONNECT_CODE {
                        return fail(format!("a disconnect with code {code}, not D5's"));
                    }
                }
                // Only close()'s own disconnected, code 0 (reviewer, 52a).
                if state == "disconnected" && !(self.closing && code == 0) {
                    return fail(format!("the client went disconnected with code {code}"));
                }
                self.client = state;
            }
            Record::Sub {
                channel,
                state,
                was_recovering,
                recovered,
                epoch,
                offset,
            } => {
                let entry = self.channels.entry(channel.clone()).or_default();
                entry.state.clone_from(&state);
                if state == "subscribed" {
                    match entry.start {
                        None => {
                            entry.start = offset;
                            entry.epoch = epoch;
                        }
                        Some(_) if was_recovering && !recovered => {
                            self.counts.unrecovered += 1;
                        }
                        Some(_) if epoch != entry.epoch => {
                            return fail(format!(
                                "{channel} recovered into epoch {epoch:?}, the run's {:?}",
                                entry.epoch
                            ));
                        }
                        Some(_) => {}
                    }
                }
            }
            Record::Pub { channel, offset } => {
                let Some(offset) = offset else {
                    return fail(format!("{channel}: a publication without offset"));
                };
                self.counts.deliveries += 1;
                self.channels
                    .entry(channel)
                    .or_default()
                    .delivered
                    .push(offset);
            }
            Record::Call {
                ok,
                code,
                took_ms,
                call,
            } => {
                if ok {
                    self.counts.calls_ok += 1;
                } else {
                    self.counts.calls_failed += 1;
                }
                // D8: a held RPC, in flight when the connection closed,
                // fails with its loss, "connection closed".
                if call == "held" {
                    if code != Some(CONNECTION_CLOSED) {
                        return fail(format!(
                            "an RPC in flight at a disconnect settled with {code:?}, not connection closed (D8)"
                        ));
                    }
                    self.counts.held_failed += 1;
                }
                if Duration::from_millis(took_ms) > CALL_TIMEOUT + Duration::from_secs(1) {
                    return fail(format!("{call} took {took_ms} ms"));
                }
            }
            Record::Token { of, refresh } => {
                if refresh {
                    match of.as_str() {
                        "connection" => self.counts.connection_refreshes += 1,
                        _ => self.counts.subscription_refreshes += 1,
                    }
                }
            }
            Record::TimerNotAllowed { name, client } => {
                return fail(format!(
                    "timer {name} listed in two samples in a row while the client was {client} (D8)"
                ));
            }
            Record::Panic { message } => return fail(format!("the child panicked: {message}")),
            record @ Record::Checkpoint { .. } => self.checked_checkpoint(&record)?,
            Record::Closed => {}
        }
        Ok(())
    }

    /// D6, D8 on a checkpoint the child answered.
    fn checked_checkpoint(&mut self, record: &Record) -> Checked {
        let Record::Checkpoint {
            at_rest,
            live_bytes,
            rss_bytes,
            driver,
            core,
            calls,
            ..
        } = record
        else {
            return Ok(());
        };
        let at = self.started.elapsed();
        let mut value = serde_json::to_value(record).unwrap_or_default();
        value["at_ms"] = json!(millis(at));
        value["scale"] = json!(self.timeline.scale);
        self.artifact.push(value);
        log(&format!(
            "checkpoint at {:.1} min: live {live_bytes} B, rss {rss_bytes} B, at rest {at_rest}, {} timers, {} calls",
            at.as_secs_f64() / 60.0,
            core.len(),
            calls.len()
        ));
        let names =
            |list: &[Timer_]| -> BTreeSet<String> { list.iter().map(|t| t.name.clone()).collect() };
        if !at_rest {
            return fail(format!(
                "no rest within {REST_WITHIN:?} at {at:?}: driver {:?}, core {:?}, calls {calls:?}",
                names(driver),
                names(core)
            ));
        }
        if names(driver) != names(core) {
            return fail(format!(
                "the driver's timers {:?} are not the core's {:?}",
                names(driver),
                names(core)
            ));
        }
        for timer in driver.iter().chain(core) {
            if Duration::from_millis(timer.overdue_ms) > OVERDUE {
                return fail(format!(
                    "{} is {} ms past its deadline",
                    timer.name, timer.overdue_ms
                ));
            }
        }
        for call in calls {
            if Duration::from_millis(call.age_ms) > CALL_TIMEOUT {
                return fail(format!(
                    "call {} pending for {} ms, held by {:?}",
                    call.id, call.age_ms, call.registries
                ));
            }
        }
        self.checkpoints.push(Kept {
            at,
            live_bytes: *live_bytes,
            rss_bytes: *rss_bytes,
        });
        Ok(())
    }

    /// D6: a checkpoint, then the channels API and the history reads.
    async fn checkpoint(&mut self) -> Checked {
        let before = self.checkpoints.len();
        self.child.send(&Command_::Checkpoint).await;
        let deadline = Instant::now() + REST_WITHIN + WITHIN;
        while self.checkpoints.len() == before {
            if Instant::now() >= deadline {
                return fail("the child answered no checkpoint");
            }
            self.pump(Instant::now() + Duration::from_millis(100))
                .await?;
        }
        self.check_channels().await?;
        self.read_histories().await
    }

    /// The scenario: the channels API lists one client on each subscribed
    /// channel, polled as Block 51 D7 does.
    async fn check_channels(&mut self) -> Checked {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let listed = self.listed().await?;
            let wrong: Vec<_> = self
                .channels
                .keys()
                .filter(|name| listed.get(*name).copied() != Some(1))
                .map(|name| (name.clone(), listed.get(name).copied()))
                .collect();
            if wrong.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return fail(format!("the channels API's clients per channel: {wrong:?}"));
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
        }
    }

    async fn listed(&self) -> Result<BTreeMap<String, u64>, Failure> {
        let prefix = format!("{NAMESPACE}:soak-{}-", self.environment.run());
        let result = self
            .environment
            .api()
            .call("channels", json!({"pattern": format!("{prefix}*")}))
            .await
            .map_err(|error| Failure(format!("channels API: {}", error.message)))?;
        let channels = result["channels"]
            .as_object()
            .ok_or_else(|| Failure(format!("channels API without channels: {result}")))?;
        Ok(channels
            .iter()
            .map(|(name, info)| (name.clone(), info["num_clients"].as_u64().unwrap_or(0)))
            .collect())
    }

    /// D9: each channel's history, accumulated by offset; each read must
    /// overlap the accumulation, in the same epoch.
    async fn read_histories(&mut self) -> Checked {
        for name in self.channels.keys().cloned().collect::<Vec<_>>() {
            let history = self
                .environment
                .api()
                .call("history", json!({"channel": name, "limit": -1}))
                .await
                .map_err(|error| Failure(format!("{name} history: {}", error.message)))?;
            let epoch = history["epoch"].as_str().unwrap_or_default().to_owned();
            let read: BTreeMap<u64, String> = history["publications"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|publication| {
                    (
                        publication["offset"].as_u64().unwrap_or(0),
                        publication["data"]["id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                    )
                })
                .collect();
            let channel = self.channels.get_mut(&name).expect("its channel");
            if let Some(known) = &channel.history_epoch
                && *known != epoch
            {
                return fail(format!(
                    "oracle: {name}'s history moved from epoch {known} to {epoch}"
                ));
            }
            if let (Some(last), Some(first)) =
                (channel.history.keys().next_back(), read.keys().next())
                && first > last
            {
                return fail(format!(
                    "oracle: {name}'s history read starts at {first}, after the {last} accumulated: a gap"
                ));
            }
            channel.history_epoch = Some(epoch);
            channel.history.extend(read);
        }
        Ok(())
    }

    /// D8: a history (limit 1) and a presence call in turn.
    async fn call(&mut self, n: u64) {
        let names: Vec<String> = self.channels.keys().cloned().collect();
        let channel = names[usize::try_from(n).unwrap_or(0) % names.len()].clone();
        let command = if n.is_multiple_of(2) {
            Command_::History { channel }
        } else {
            Command_::Presence { channel }
        };
        self.child.send(&command).await;
    }

    /// D5: the server disconnects the run's user with D5's code, over a
    /// connection of its own: a pooled one the server closed while idle
    /// fails the request as "connection closed before message completed"
    /// (#147, #150, #151), and a disconnect is not retried, as a second
    /// could act on the next connection.
    async fn disconnect(&mut self, user: &str) -> Checked {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .timeout(WITHIN)
            .build()
            .map_err(|error| Failure(format!("disconnect: {error}")))?;
        let response = http
            .post(format!("{}/api/disconnect", self.environment.api_url()))
            .header("X-API-Key", support::centrifugo::API_KEY)
            .json(&json!({"user": user, "disconnect": {"code": RECONNECT_CODE, "reason": "soak"}}))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| Failure(format!("disconnect: {error}")))?;
        let body: Value = response
            .json()
            .await
            .map_err(|error| Failure(format!("disconnect: {error}")))?;
        match body.get("error") {
            Some(error) => fail(format!("disconnect: {error}")),
            None => Ok(()),
        }
    }

    /// D5 with D8's RPC: the child makes it, the backend holds it, the
    /// disconnect goes out, then the backend answers to no one; the RPC was
    /// in flight when the connection closed.
    async fn held_disconnect(&mut self, user: &str) -> Checked {
        let held = self.backend.held_rpcs();
        self.child.send(&Command_::Held).await;
        let deadline = Instant::now() + WITHIN;
        while self.backend.held_rpcs() <= held {
            if Instant::now() >= deadline {
                return fail("the backend received no held RPC (D8)");
            }
            self.pump(Instant::now() + Duration::from_millis(10))
                .await?;
        }
        self.counts.held += 1;
        let disconnected = self.disconnect(user).await;
        self.backend.release_held_rpcs();
        disconnected
    }

    /// D9 at the end: every channel delivered the accumulated history from
    /// its first `subscribed`, each offset once; every acknowledged
    /// publication is in it.
    async fn check_deliveries(&mut self) -> Checked {
        let deadline = Instant::now() + WITHIN;
        loop {
            let behind: Vec<String> = self
                .channels
                .iter()
                .filter(|(_, c)| {
                    c.history.keys().next_back().copied().unwrap_or(0)
                        > c.delivered.last().copied().unwrap_or(0)
                })
                .map(|(name, _)| name.clone())
                .collect();
            if behind.is_empty() || Instant::now() >= deadline {
                break;
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
        }
        let published = self.published.lock().unwrap().clone();
        for (name, epoch, offset) in &published {
            let channel = &self.channels[name];
            if channel.history_epoch.as_deref() != Some(epoch.as_str())
                || !channel.history.contains_key(offset)
            {
                return fail(format!(
                    "oracle: {name}'s acknowledged ({epoch}, {offset}) is not in the accumulated history"
                ));
            }
        }
        for (name, channel) in &self.channels {
            let Some(start) = channel.start else {
                return fail(format!("{name} was never subscribed"));
            };
            let expected: Vec<u64> = channel
                .history
                .range(start + 1..)
                .map(|(o, _)| *o)
                .collect();
            if channel.delivered != expected {
                let first_wrong = channel
                    .delivered
                    .iter()
                    .zip(&expected)
                    .position(|(got, want)| got != want)
                    .unwrap_or(channel.delivered.len().min(expected.len()));
                return fail(format!(
                    "{name}: delivered {} of {} history offsets from {start}, first difference at {first_wrong}: {:?} against {:?}",
                    channel.delivered.len(),
                    expected.len(),
                    channel.delivered.get(first_wrong),
                    expected.get(first_wrong)
                ));
            }
        }
        if self.counts.unrecovered > 0 {
            return fail(format!(
                "{} subscribed with was_recovering true and recovered false (D9)",
                self.counts.unrecovered
            ));
        }
        Ok(())
    }

    /// D7: the later median within 1% of the earlier, or 64 KiB.
    fn check_flat(&self) -> Checked {
        let after: Vec<&Kept> = self
            .checkpoints
            .iter()
            .filter(|kept| kept.at >= self.timeline.warm_up)
            .collect();
        if after.len() < 6 {
            return fail(format!(
                "{} checkpoints after the warm-up, 6 needed (D7)",
                after.len()
            ));
        }
        let median = |three: &[&Kept]| {
            let mut bytes: Vec<u64> = three.iter().map(|kept| kept.live_bytes).collect();
            bytes.sort_unstable();
            bytes[1] as f64
        };
        let earlier = median(&after[..3]);
        let later = median(&after[after.len() - 3..]);
        let allowed = (earlier * TOLERANCE).max(FLOOR);
        // Reported only: the least-squares slope over every checkpoint
        // after the warm-up, in bytes an hour.
        let points: Vec<(f64, f64)> = after
            .iter()
            .map(|kept| (kept.at.as_secs_f64() / 3600.0, kept.live_bytes as f64))
            .collect();
        let n = points.len() as f64;
        let (mean_x, mean_y) = (
            points.iter().map(|p| p.0).sum::<f64>() / n,
            points.iter().map(|p| p.1).sum::<f64>() / n,
        );
        let slope = points
            .iter()
            .map(|p| (p.0 - mean_x) * (p.1 - mean_y))
            .sum::<f64>()
            / points
                .iter()
                .map(|p| (p.0 - mean_x).powi(2))
                .sum::<f64>()
                .max(f64::MIN_POSITIVE);
        let rss: Vec<u64> = after.iter().map(|kept| kept.rss_bytes).collect();
        println!(
            "@P report live heap: earlier median {earlier:.0} B, later median {later:.0} B, allowed growth {allowed:.0} B, slope {slope:.0} B/h (scaled time ×{})",
            self.timeline.scale
        );
        println!("@P report rss bytes after the warm-up: {rss:?}");
        if later - earlier > allowed {
            return fail(format!(
                "the live heap grew from a median of {earlier:.0} B to {later:.0} B, more than {allowed:.0} B (D7)"
            ));
        }
        Ok(())
    }

    fn check_counts(&self) -> Checked {
        let counts = &self.counts;
        if counts.held_failed != counts.held {
            return fail(format!(
                "{} RPCs held at a disconnect, {} failed with the connection's loss (D8)",
                counts.held, counts.held_failed
            ));
        }
        if counts.connection_refreshes < MIN_CONNECTION_REFRESHES
            || counts.subscription_refreshes < MIN_SUBSCRIPTION_REFRESHES
            || counts.reconnects < MIN_RECONNECTS
        {
            return fail(format!(
                "the counts fall short of the scenario's: {counts:?}"
            ));
        }
        Ok(())
    }

    /// `close()`, and the channels API listing none of the run's channels.
    async fn close(&mut self) -> Checked {
        self.closing = true;
        self.child.send(&Command_::Close).await;
        let deadline = Instant::now() + WITHIN;
        loop {
            if Instant::now() >= deadline {
                return fail("the channels API still lists the run's channels after close()");
            }
            self.pump(Instant::now() + Duration::from_millis(200))
                .await?;
            if self.listed().await?.is_empty() {
                break;
            }
        }
        self.child.send(&Command_::Exit).await;
        let _ = tokio::time::timeout(WITHIN, self.child.process.wait()).await;
        Ok(())
    }
}

fn log(line: &str) {
    println!("soak: {line}");
}

/// D1's configuration: Block 51 D5's, with D3's history, D4's default
/// close delays, Centrifugo's default ping and pong, and an RPC proxy to
/// the runner's backend.
fn soak_configuration(backend: &ProxyBackend) -> Value {
    let mut config = configuration(Engine::Memory);
    // D8's RPC goes to the runner's backend, which holds it, under a timeout
    // above that hold.
    config["rpc"] = json!({
        "proxy": {"endpoint": format!("{}/rpc", backend.url()), "timeout": "10s"},
        "without_namespace": {"proxy_enabled": true}
    });
    let client = config["client"].as_object_mut().expect("a client section");
    for default in [
        "ping_interval",
        "pong_timeout",
        "expired_close_delay",
        "expired_sub_close_delay",
    ] {
        client.remove(default);
    }
    let namespace = &mut config["channel"]["namespaces"][0];
    namespace["history_size"] = json!(HISTORY_SIZE);
    namespace["history_ttl"] = json!(HISTORY_TTL);
    config
}

/// The publisher (D3): one publication a second on each channel, each with
/// a run-wide id, recording `(channel, epoch, offset)` from each reply.
///
/// A publish the server refuses ends it with that error, and a transport
/// failure is a panic of its task: the run checks its end, so that rejected
/// publications cannot leave both sides of the comparison (Codex, #264).
fn publisher(
    environment: &Environment,
    channels: Vec<String>,
    published: Arc<Mutex<Vec<(String, String, u64)>>>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    let api = environment.api().clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PUBLISH_EVERY);
        // Burst, tokio's default: a slow round is caught up, so the count
        // is deterministic (reviewer, 52a).
        assert_eq!(
            tick.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Burst
        );
        let mut n = 0u64;
        while !stop.load(Ordering::SeqCst) {
            tick.tick().await;
            for channel in &channels {
                let id = format!("p{n}");
                let reply = api
                    .call("publish", json!({"channel": channel, "data": {"id": id}}))
                    .await
                    .map_err(|error| {
                        format!(
                            "publish to {channel}: error {}: {}",
                            error.code, error.message
                        )
                    })?;
                published.lock().unwrap().push((
                    channel.clone(),
                    reply["epoch"].as_str().unwrap_or_default().to_owned(),
                    reply["offset"].as_u64().unwrap_or(0),
                ));
                n += 1;
            }
        }
        Ok(())
    })
}

/// The publisher's end: once stopped, within [`WITHIN`], without an error.
async fn publisher_ended(sender: &mut tokio::task::JoinHandle<Result<(), String>>) -> Checked {
    match tokio::time::timeout(WITHIN, &mut *sender).await {
        Err(_) => fail(format!("the publisher did not stop within {WITHIN:?}")),
        Ok(Err(error)) => fail(format!("the publisher's task: {error}")),
        Ok(Ok(Err(error))) => fail(format!("the publisher: {error}")),
        Ok(Ok(Ok(()))) => Ok(()),
    }
}

/// The publisher's volume (reviewer, 52a): a round of one publication per
/// channel each second of the run, less one round for the stop. Its ticker
/// catches up after a slow round, and every refusal or failure already
/// fails the run, so the count is exact.
fn check_volume(published: u64, duration: Duration) -> Checked {
    let channels = CHANNELS as u64;
    let required = (channels * duration.as_secs()).saturating_sub(channels);
    if published < required {
        return fail(format!(
            "{published} publications acknowledged, {required} required (D3)"
        ));
    }
    Ok(())
}

/// The run: its timeline, then the checks at the end; the run's state is
/// returned with the outcome, for the artifact.
async fn run<'a>(
    environment: &'a Environment,
    backend: &'a ProxyBackend,
    timeline: Timeline,
) -> (Soak<'a>, Checked) {
    let user = format!("soak-{}", environment.run());
    let channels: Vec<String> = (0..CHANNELS)
        .map(|n| format!("{NAMESPACE}:soak-{}-c{n}", environment.run()))
        .collect();
    let child = ChildProcess::start(
        &environment.direct_websocket_url(),
        &user,
        &channels,
        &timeline,
    );
    let published = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut sender = publisher(
        environment,
        channels.clone(),
        Arc::clone(&published),
        Arc::clone(&stop),
    );
    let mut soak = Soak {
        environment,
        backend,
        timeline,
        child,
        channels: channels
            .iter()
            .map(|name| (name.clone(), Channel::default()))
            .collect(),
        client: "disconnected".to_owned(),
        counts: Counts::default(),
        checkpoints: Vec::new(),
        started: Instant::now(),
        artifact: Vec::new(),
        published,
        closing: false,
    };
    let checked = async {
        let start = soak.started;
        let end = start + timeline.duration;
        let (mut next_call, mut calls) = (start + timeline.call_every, 0u64);
        let mut next_disconnect = start + timeline.period;
        let mut next_checkpoint = start + timeline.checkpoint_offset;
        loop {
            let next = next_call.min(next_disconnect).min(next_checkpoint).min(end);
            soak.pump(next).await?;
            let now = Instant::now();
            if now >= end {
                break;
            }
            // The publisher runs until stopped: one that ended has failed.
            if sender.is_finished() {
                return publisher_ended(&mut sender)
                    .await
                    .and_then(|()| fail("the publisher stopped during the run"));
            }
            if now >= next_checkpoint {
                soak.checkpoint().await?;
                next_checkpoint += timeline.period;
            }
            if now >= next_disconnect {
                soak.held_disconnect(&user).await?;
                next_disconnect += timeline.period;
            }
            if now >= next_call {
                soak.call(calls).await;
                calls += 1;
                next_call += timeline.call_every;
            }
        }
        stop.store(true, Ordering::SeqCst);
        // The publisher's last round has its replies; then the final read.
        publisher_ended(&mut sender).await?;
        soak.counts.publications = soak.published.lock().unwrap().len() as u64;
        check_volume(soak.counts.publications, timeline.duration)?;
        soak.read_histories().await?;
        soak.check_deliveries().await?;
        soak.check_flat()?;
        soak.check_counts()?;
        soak.close().await
    }
    .await;
    stop.store(true, Ordering::SeqCst);
    sender.abort();
    (soak, checked)
}

/// Writes the checkpoints and the summary where the workflow uploads them.
fn write_artifact(soak: &Soak<'_>, failure: Option<&Failure>) {
    let dir = env::var_os(ARTIFACTS_VARIABLE).map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/soak"),
        std::path::PathBuf::from,
    );
    let _ = std::fs::create_dir_all(&dir);
    let mut text = String::new();
    for checkpoint in &soak.artifact {
        text.push_str(&checkpoint.to_string());
        text.push('\n');
    }
    let summary = json!({
        "summary": true,
        "scale": soak.timeline.scale,
        "timeline": soak.timeline,
        "counts": soak.counts,
        "checkpoints": soak.checkpoints,
        "failure": failure.map(|f| f.0.clone()),
    });
    text.push_str(&summary.to_string());
    text.push('\n');
    let _ = std::fs::write(dir.join("soak-checkpoints.ndjson"), text);
}

fn main() {
    if env::var(ROLE).as_deref() == Ok("client") {
        child();
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("soak skipped: set {RUN}=1, with Docker");
        return;
    }
    let timeline = Timeline::at(scale());
    log(&format!("timeline {timeline:?}"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let outcome = runtime.block_on(async {
        let run_name = format!("s{}", std::process::id());
        let backend = ProxyBackend::start().await;
        let environment = Environment::start_direct(&run_name, &soak_configuration(&backend))
            .await
            .unwrap_or_else(|error| panic!("the environment: {error}"));
        let (soak, checked) = run(&environment, &backend, timeline).await;
        write_artifact(&soak, checked.as_ref().err());
        println!("@P report counts: {:?}", soak.counts);
        checked
    });
    println!(
        "@P report scale: {} ({})",
        timeline.scale,
        if timeline.scale == 1 {
            "evidence of the scenario"
        } else {
            "evidence of the checker only (D12)"
        }
    );
    if let Err(failure) = outcome {
        panic!("the soak failed: {}", failure.0);
    }
}
