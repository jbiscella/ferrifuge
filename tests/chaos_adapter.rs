//! Seeded chaos on the tokio client against the fake server (Block 16,
//! `@P`): random operations and server faults in real time, with the
//! invariants the adapter can check. A seed fixes the operations, not
//! their timing, so a failure is replayed, not always reproduced.

// @P scenario: Seeded chaos on the tokio client keeps the invariants
// @P scenario: The adapter runner reports a planted fault

mod support;

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ferrifuge::{
    Client, ClientEvent, Config, HistoryOptions, State, Subscription, SubscriptionEvent,
    SubscriptionState,
};
use futures_util::{Stream, StreamExt};
use support::fake_server::{Announce, FakeServer};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// Seeds every pull request runs (Block 16, "CI").
const SEEDS: Range<u64> = 0..20;
/// Operations of each seed.
const OPS: usize = 30;
/// Reruns one seed alone.
const SEED_VARIABLE: &str = "FERRIFUGE_CHAOS_SEED";
/// `Config::timeout` of the chaos configuration.
const TIMEOUT: Duration = Duration::from_secs(1);
/// I6: a call waits at most once per phase, plus a margin for real time.
const CALL_BOUND: Duration = Duration::from_secs(4);
/// How long a seed may take to reach quiescence once faults stop (I4).
const QUIESCENCE: Duration = Duration::from_secs(10);
/// No event for this long: the streams are quiet.
const QUIET: Duration = Duration::from_millis(300);
/// How long a read of a stream waits for its next event.
const READ_WAIT: Duration = Duration::from_millis(20);
/// After `disconnect()`, how long the runner watches for a new connection
/// (I5).
const AFTER_DISCONNECT: Duration = Duration::from_millis(1500);
const CHANNELS: [&str; 2] = ["news", "chat"];
/// Publications a recovery may bring back.
const RECOVERY_LIMIT: u64 = 8;
/// Close codes: reconnect-advised (3001, 4000, 3008, 3014) and terminal
/// (3500, 4500).
const CODES: [u16; 6] = [3001, 4000, 3008, 3014, 3500, 4500];

/// Panics on any thread but the tests' own (I2), counted by a hook.
static PANICS: AtomicUsize = AtomicUsize::new(0);

fn count_panics() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let test_thread = std::thread::current()
                .name()
                .is_some_and(|name| name.starts_with("seeded_") || name.starts_with("the_"));
            if !test_thread {
                PANICS.fetch_add(1, Ordering::SeqCst);
            }
            previous(info);
        }));
    });
}

/// Block 16, "Chaos configuration".
fn chaos_config() -> Config {
    Config {
        min_reconnect_delay: Duration::from_millis(50),
        max_reconnect_delay: Duration::from_millis(500),
        timeout: TIMEOUT,
        read_ahead_messages: 8,
        read_ahead_bytes: 4096,
        ..Config::default()
    }
}

/// An invariant a seed broke.
#[derive(Debug)]
struct Violation {
    seed: u64,
    invariant: &'static str,
    detail: String,
    /// The operations up to it.
    ops: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} broke {}: {}\nreplay: {SEED_VARIABLE}={} cargo test --test chaos_adapter\n\
             operations:\n  {}",
            self.seed,
            self.invariant,
            self.detail,
            self.seed,
            self.ops.join("\n  ")
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum Target {
    Client,
    Subscription(usize),
}

/// What the application or the server does next.
#[derive(Debug, Clone, Copy)]
enum Op {
    Connect,
    Disconnect,
    Subscribe(usize),
    Unsubscribe(usize),
    Call(usize),
    /// Reads up to this many events of a stream.
    Take(Target, usize),
    /// A read cancelled before its event came, or not.
    CancelledRead(Target),
    /// A second `events()` call takes over the stream.
    SecondEvents(Target),
    Pause(Duration),
    /// The server publishes this many publications to a channel.
    Publish(usize, u32),
    Expire(usize),
    Close(u16),
    FailConnects(usize),
}

impl Op {
    fn pick(rng: &mut fastrand::Rng) -> Self {
        let channel = rng.usize(..CHANNELS.len());
        let target = if rng.bool() {
            Target::Client
        } else {
            Target::Subscription(channel)
        };
        match rng.u32(0..100) {
            0..8 => Self::Connect,
            8..12 => Self::Disconnect,
            12..20 => Self::Subscribe(channel),
            20..24 => Self::Unsubscribe(channel),
            24..32 => Self::Call(rng.usize(..4)),
            32..48 => Self::Take(target, rng.usize(1..=10)),
            48..54 => Self::CancelledRead(target),
            54..58 => Self::SecondEvents(target),
            58..66 => Self::Pause(Duration::from_millis(rng.u64(50..=400))),
            66..82 => Self::Publish(channel, rng.u32(1..=5)),
            82..86 => Self::Expire(channel),
            86..96 => Self::Close(CODES[rng.usize(..CODES.len())]),
            _ => Self::FailConnects(rng.usize(1..=3)),
        }
    }
}

type Events<E> = Box<dyn Stream<Item = E> + Send + Unpin>;

/// A subscription as the application holds it.
struct Sub {
    subscription: Subscription,
    events: Events<SubscriptionEvent>,
    last_state: SubscriptionState,
    /// The offset delivered last, `None` until one is (I8).
    delivered: Option<u64>,
}

/// One seed's run.
struct Run {
    seed: u64,
    rng: fastrand::Rng,
    server: FakeServer,
    client: Client,
    events: Events<ClientEvent>,
    last_state: State,
    subs: Vec<Sub>,
    /// The last offset published per channel.
    published: [u64; CHANNELS.len()],
    calls: Vec<JoinHandle<Duration>>,
    ops: Vec<String>,
    counts: BTreeMap<&'static str, usize>,
}

type Step = Result<(), Violation>;

impl Run {
    async fn new(seed: u64, skip_recovered: bool) -> Self {
        let server = FakeServer::start(Announce::default()).await;
        for channel in CHANNELS {
            server.recover_from_history(channel);
        }
        server.limit_recovery(RECOVERY_LIMIT);
        if skip_recovered {
            server.skip_recovered_offset();
        }
        let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
            .config(chaos_config())
            .build()
            .expect("a valid configuration");
        let events: Events<ClientEvent> = Box::new(client.events());
        let subs = CHANNELS
            .iter()
            .map(|channel| {
                let subscription = client
                    .subscription_builder(*channel)
                    .positioned(true)
                    .recoverable(true)
                    .min_resubscribe_delay(Duration::from_millis(50))
                    .max_resubscribe_delay(Duration::from_millis(500))
                    .build()
                    .expect("a new channel");
                let events: Events<SubscriptionEvent> = Box::new(subscription.events());
                Sub {
                    subscription,
                    events,
                    last_state: SubscriptionState::Unsubscribed,
                    delivered: None,
                }
            })
            .collect();
        Self {
            seed,
            rng: fastrand::Rng::with_seed(seed ^ 0x5eed_ada9_7e40_0016),
            server,
            client,
            events,
            last_state: State::Disconnected,
            subs,
            published: [0; CHANNELS.len()],
            calls: Vec::new(),
            ops: Vec::new(),
            counts: BTreeMap::new(),
        }
    }

    fn violation(&self, invariant: &'static str, detail: impl Into<String>) -> Violation {
        Violation {
            seed: self.seed,
            invariant,
            detail: detail.into(),
            ops: self.ops.clone(),
        }
    }

    fn count(&mut self, name: &'static str) {
        *self.counts.entry(name).or_default() += 1;
    }

    async fn run(&mut self, ops: usize) -> Step {
        for n in 0..ops {
            let op = Op::pick(&mut self.rng);
            self.ops.push(format!("op {n}: {op:?}"));
            self.apply(op).await?;
            sleep(Duration::from_millis(self.rng.u64(0..=30))).await;
            self.check_panics()?;
        }
        self.ops.push("heal".to_owned());
        self.heal().await?;
        self.ops.push("disconnect".to_owned());
        self.after_disconnect().await?;
        self.check_panics()
    }

    async fn apply(&mut self, op: Op) -> Step {
        match op {
            Op::Connect => self.client.connect(),
            Op::Disconnect => self.client.disconnect(),
            Op::Subscribe(n) => self.subs[n].subscription.subscribe(),
            Op::Unsubscribe(n) => self.subs[n].subscription.unsubscribe(),
            Op::Call(kind) => self.call(kind),
            Op::Take(target, events) => {
                for _ in 0..events {
                    if !self.read(target, READ_WAIT).await? {
                        break;
                    }
                }
            }
            Op::CancelledRead(target) => {
                let wait = Duration::from_micros(self.rng.u64(0..=500));
                if !self.read(target, wait).await? {
                    self.count("cancelled read");
                }
            }
            Op::SecondEvents(target) => {
                match target {
                    Target::Client => self.events = Box::new(self.client.events()),
                    Target::Subscription(n) => {
                        let sub = &mut self.subs[n];
                        sub.events = Box::new(sub.subscription.events());
                    }
                }
                self.count("second events()");
            }
            Op::Pause(by) => sleep(by).await,
            Op::Publish(n, count) => {
                for _ in 0..count {
                    self.published[n] += 1;
                    let pad = self.rng.usize(0..=600);
                    self.server
                        .publish_sized(CHANNELS[n], self.published[n], pad);
                }
            }
            Op::Expire(n) => self.server.expire_history(CHANNELS[n]),
            Op::Close(code) => {
                self.server.close_current(code, "chaos");
            }
            Op::FailConnects(count) => self.server.fail_connects(count),
        }
        Ok(())
    }

    /// Makes a call in a task that reports how long it took to settle.
    fn call(&mut self, kind: usize) {
        let made = Instant::now();
        let call: std::pin::Pin<Box<dyn Future<Output = bool> + Send>> = match kind {
            0 => Box::pin({
                let call = self.client.rpc("m", b"{}".to_vec());
                async move { call.await.is_ok() }
            }),
            1 => Box::pin({
                let call = self.client.publish("news", b"{}".to_vec());
                async move { call.await.is_ok() }
            }),
            2 => Box::pin({
                let call = self.client.history("news", HistoryOptions::default());
                async move { call.await.is_ok() }
            }),
            _ => Box::pin({
                let call = self.client.presence("news");
                async move { call.await.is_ok() }
            }),
        };
        self.calls.push(tokio::spawn(async move {
            call.await;
            made.elapsed()
        }));
    }

    /// Reads the next event of `target` within `wait`; whether one came.
    async fn read(&mut self, target: Target, wait: Duration) -> Result<bool, Violation> {
        match target {
            Target::Client => match timeout(wait, self.events.next()).await {
                Ok(Some(event)) => self.client_event(&event).map(|()| true),
                Ok(None) | Err(_) => Ok(false),
            },
            Target::Subscription(n) => match timeout(wait, self.subs[n].events.next()).await {
                Ok(Some(event)) => self.subscription_event(n, &event).map(|()| true),
                Ok(None) | Err(_) => Ok(false),
            },
        }
    }

    /// I1 for the client.
    fn client_event(&mut self, event: &ClientEvent) -> Step {
        let state = match event {
            ClientEvent::Connecting { .. } => State::Connecting,
            ClientEvent::Connected { .. } => State::Connected,
            ClientEvent::Disconnected { .. } => State::Disconnected,
            _ => return Ok(()),
        };
        let last = std::mem::replace(&mut self.last_state, state);
        if last == state {
            return Err(self.violation("I1", format!("{state:?} twice in a row")));
        }
        if last == State::Disconnected && state == State::Connected {
            return Err(self.violation("I1", "disconnected went to connected"));
        }
        if last == State::Connected && state == State::Connecting {
            self.count("reconnect");
        }
        Ok(())
    }

    /// I1 and I8 for a subscription.
    fn subscription_event(&mut self, n: usize, event: &SubscriptionEvent) -> Step {
        let state = match event {
            SubscriptionEvent::Subscribing { .. } => SubscriptionState::Subscribing,
            SubscriptionEvent::Subscribed(subscribed) => {
                if subscribed.was_recovering && subscribed.recovered {
                    self.count("recovered true");
                }
                if !subscribed.recovered {
                    self.subs[n].delivered =
                        subscribed.stream_position.as_ref().map(|at| at.offset);
                }
                SubscriptionState::Subscribed
            }
            SubscriptionEvent::Unsubscribed { .. } => SubscriptionState::Unsubscribed,
            SubscriptionEvent::Publication(publication) => {
                let Some(offset) = publication.offset else {
                    return Ok(());
                };
                let sub = &mut self.subs[n];
                if let Some(last) = sub.delivered
                    && offset != last + 1
                {
                    return Err(self.violation(
                        "I8",
                        format!("{} delivered offset {offset} after {last}", CHANNELS[n]),
                    ));
                }
                sub.delivered = Some(offset);
                return Ok(());
            }
            _ => return Ok(()),
        };
        let last = std::mem::replace(&mut self.subs[n].last_state, state);
        if last == state {
            return Err(self.violation("I1", format!("{} {state:?} twice in a row", CHANNELS[n])));
        }
        if last == SubscriptionState::Unsubscribed && state == SubscriptionState::Subscribed {
            return Err(self.violation(
                "I1",
                format!("{} unsubscribed went to subscribed", CHANNELS[n]),
            ));
        }
        Ok(())
    }

    /// Reads every stream until none has had an event for [`QUIET`], or
    /// `deadline` passes: a client that never stops emitting must not hang
    /// the runner.
    async fn read_until_quiet(&mut self, deadline: Instant) -> Step {
        let mut quiet_since = Instant::now();
        while quiet_since.elapsed() < QUIET && Instant::now() < deadline {
            let mut any = self.read(Target::Client, READ_WAIT).await?;
            for n in 0..CHANNELS.len() {
                any |= self.read(Target::Subscription(n), READ_WAIT).await?;
            }
            if any {
                quiet_since = Instant::now();
            }
        }
        Ok(())
    }

    /// Faults stop and the application wants to be connected: within
    /// [`QUIESCENCE`] the client is connected, every subscription not
    /// unsubscribed is subscribed and every call settled (I4, I6); at most
    /// one connection is open (I3).
    async fn heal(&mut self) -> Step {
        self.server.fail_connects(0);
        let deadline = Instant::now() + QUIESCENCE;
        loop {
            // A terminal close the server sent before healing may be
            // handled only now, once the events before it are taken.
            if self.client.state() == State::Disconnected {
                self.client.connect();
            }
            self.read_until_quiet(deadline).await?;
            if self.quiescent() {
                break;
            }
            if Instant::now() > deadline {
                return Err(self.violation(
                    "I4",
                    format!(
                        "no quiescence {QUIESCENCE:?} after healing: client {:?}, subscriptions {:?}, {} calls unsettled",
                        self.last_state,
                        self.subs.iter().map(|sub| sub.last_state).collect::<Vec<_>>(),
                        self.calls.iter().filter(|call| !call.is_finished()).count()
                    ),
                ));
            }
        }
        // The last publication of a subscribed channel was delivered too:
        // no later offset would show it missing.
        for (n, sub) in self.subs.iter().enumerate() {
            if sub.last_state == SubscriptionState::Subscribed
                && sub.delivered != Some(self.published[n])
            {
                return Err(self.violation(
                    "I8",
                    format!(
                        "{} delivered up to {:?} at quiescence, published up to {}",
                        CHANNELS[n], sub.delivered, self.published[n]
                    ),
                ));
            }
        }
        for call in std::mem::take(&mut self.calls) {
            let took = call
                .await
                .map_err(|error| self.violation("I2", error.to_string()))?;
            if took > CALL_BOUND {
                return Err(self.violation("I6", format!("a call settled after {took:?}")));
            }
        }
        let open = self.server.connections() - self.server.closed_connections();
        if open > 1 {
            return Err(self.violation("I3", format!("{open} connections open")));
        }
        Ok(())
    }

    fn quiescent(&self) -> bool {
        self.last_state == State::Connected
            && self.client.state() == State::Connected
            && self.subs.iter().all(|sub| {
                matches!(
                    sub.last_state,
                    SubscriptionState::Unsubscribed | SubscriptionState::Subscribed
                ) && sub.subscription.state() == sub.last_state
            })
            && self.calls.iter().all(JoinHandle::is_finished)
    }

    /// I5: after `disconnect()`, no connection stays open and no new one
    /// is made.
    async fn after_disconnect(&mut self) -> Step {
        // Counted before: a connection made and closed while the events
        // are drained is one made after disconnect().
        let connections = self.server.connections();
        self.client.disconnect();
        self.read_until_quiet(Instant::now() + QUIESCENCE).await?;
        let deadline = Instant::now() + AFTER_DISCONNECT;
        while Instant::now() < deadline {
            self.read_until_quiet(deadline).await?;
        }
        let made = self.server.connections() - connections;
        let open = self.server.connections() - self.server.closed_connections();
        if made > 0 || open > 0 {
            return Err(self.violation(
                "I5",
                format!("{made} connections made and {open} open after disconnect()"),
            ));
        }
        Ok(())
    }

    fn check_panics(&self) -> Step {
        match PANICS.load(Ordering::SeqCst) {
            0 => Ok(()),
            panics => Err(self.violation("I2", format!("{panics} panics"))),
        }
    }
}

/// The seeds to run: `FERRIFUGE_CHAOS_SEED` alone when it is set.
fn seeds() -> Range<u64> {
    match std::env::var(SEED_VARIABLE) {
        Ok(seed) => {
            let seed: u64 = seed.trim().parse().expect("a seed");
            seed..seed + 1
        }
        Err(_) => SEEDS,
    }
}

/// Runs `seeds`, stopping at the first violation; the counts of every run.
async fn run_seeds(
    seeds: Range<u64>,
    skip_recovered: bool,
) -> Result<BTreeMap<&'static str, usize>, Violation> {
    count_panics();
    let mut counts = BTreeMap::new();
    for seed in seeds {
        let mut run = Run::new(seed, skip_recovered).await;
        run.run(OPS).await?;
        for (name, count) in run.counts {
            *counts.entry(name).or_default() += count;
        }
    }
    Ok(counts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_chaos_on_the_tokio_client_keeps_the_invariants() {
    let seeds = seeds();
    let counts = run_seeds(seeds.clone(), false)
        .await
        .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let missing: Vec<_> = [
        "reconnect",
        "recovered true",
        "second events()",
        "cancelled read",
    ]
    .into_iter()
    .filter(|name| counts.get(name).copied().unwrap_or(0) == 0)
    .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_adapter_runner_reports_a_planted_fault() {
    let violation = run_seeds(SEEDS, true)
        .await
        .expect_err("a recovery skipping an offset went unreported");
    assert_eq!(violation.invariant, "I8", "{violation}");
    assert!(
        violation
            .to_string()
            .contains(&format!("seed {}", violation.seed))
    );
    assert!(!violation.ops.is_empty(), "{violation}");
}

/// The main and nightly run (Block 16, "CI"): `FERRIFUGE_CHAOS_SEEDS` seeds
/// (500 by default) from a start seed drawn per run, or
/// `FERRIFUGE_CHAOS_START`; `FERRIFUGE_CHAOS_SEED` alone when set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long: main and nightly"]
async fn seeded_chaos_adapter_long_run() {
    let number = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .map_or(default, |value| value.trim().parse().expect("a number"))
    };
    let start = number("FERRIFUGE_CHAOS_START", fastrand::u64(..u64::MAX / 2));
    let count = number("FERRIFUGE_CHAOS_SEEDS", 500);
    let seeds = match std::env::var(SEED_VARIABLE) {
        Ok(_) => seeds(),
        Err(_) => start..start + count,
    };
    println!("chaos: adapter seeds {}..{}", seeds.start, seeds.end);
    if let Err(violation) = run_seeds(seeds, false).await {
        panic!("{violation}");
    }
}
