//! Seeded chaos on the tokio client against the fake server (Block 16,
//! `@P`): random operations and server faults in real time, with the
//! invariants the adapter can check. A seed fixes the operations, not
//! their timing, so a failure is replayed, not always reproduced.

// @P scenario: Seeded chaos on the tokio client keeps the invariants
// @P scenario: The adapter runner reports a planted fault
// @P scenario: Seeded chaos with batching on the tokio client keeps the invariants
// @P scenario: Seeded chaos with delta subscriptions on the tokio client keeps the invariants
// @P scenario: Seeded chaos on the tokio client batches automatically and keeps the invariants
// @P scenario: Seeded chaos with debounced publishes on the tokio client keeps the invariants
// @P scenario: Seeded chaos with handlers on the tokio client keeps the invariants

mod support;

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ferrifuge::{
    Client, ClientEvent, Config, DeltaFormat, EventDelivery, HistoryOptions, State, Subscription,
    SubscriptionEvent, SubscriptionState, WeakClient, codes,
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
/// I6: a call waits at most once per phase, plus a margin for real time:
/// 3 s, as this runs in the instrumented coverage job beside the binary's
/// other tests, and the time counts the call's task being scheduled. A
/// call that never settles still fails it.
const CALL_BOUND: Duration = Duration::from_secs(6);
/// I6: a publish of a burst may have waited for its subscription and in a
/// debounce window, 5 × in all, plus the margin (Block 46, D6); the adapter
/// cannot tell which did, so the bound is that of every publish of a burst.
const WINDOW_CALL_BOUND: Duration = Duration::from_secs(8);
/// A tick of real time, of which a debounce interval is 1 to 3 (Block 46,
/// D16): longer than the pause between two operations.
const TICK: u32 = 50;
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

/// The payload of a panic a handler raises on purpose (Block 49): the
/// hook tells it apart from an unexpected panic, which breaks I2.
struct PlantedPanic;

fn count_panics() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Planted: the run counts it itself, and it is no violation.
            if info.payload().downcast_ref::<PlantedPanic>().is_some() {
                return;
            }
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

/// What a seed's handlers do next, set by its operations, and what they
/// did (Block 49): per run, so that tests running at once share nothing.
#[derive(Default)]
struct Plan {
    /// The subscription's handler panics at its next publication, before
    /// taking it.
    panic: AtomicBool,
    /// The client's handler closes the client at its next event.
    close: AtomicBool,
    /// The subscription's handler unsubscribes it at its next event.
    unsubscribe: AtomicBool,
    /// How long each handler call takes, in milliseconds.
    slow: AtomicU64,
    panics: AtomicUsize,
    closes: AtomicUsize,
    unsubscribes: AtomicUsize,
    /// The client, for its handlers to reach without keeping it alive.
    client: std::sync::Mutex<Option<WeakClient>>,
}

impl Plan {
    fn upgrade(&self) -> Option<Client> {
        self.client
            .lock()
            .unwrap()
            .as_ref()
            .and_then(WeakClient::upgrade)
    }

    /// The start of a handler call: it takes its time, and panics when
    /// planted and `publication`.
    fn turn(&self, publication: bool) {
        let slow = self.slow.load(Ordering::SeqCst);
        if slow > 0 {
            std::thread::sleep(Duration::from_millis(slow));
        }
        if publication && self.panic.swap(false, Ordering::SeqCst) {
            self.panics.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(PlantedPanic);
        }
    }
}

/// A stream of the events a handler forwards.
fn forwarded<E: Send + 'static>() -> (tokio::sync::mpsc::UnboundedSender<E>, Events<E>) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let events = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    });
    // Fused: the runner reads an ended stream again, as a stream of the
    // client allows.
    (sender, Box::new(Box::pin(events).fuse()))
}

/// The client's handler: forwards each event, and closes the client when
/// planted.
fn client_handler(
    plan: &Arc<Plan>,
) -> (
    impl FnMut(ClientEvent) + Send + 'static,
    Events<ClientEvent>,
) {
    let (sender, events) = forwarded();
    let plan = Arc::clone(plan);
    let handler = move |event: ClientEvent| {
        plan.turn(false);
        if plan.close.swap(false, Ordering::SeqCst)
            && let Some(client) = plan.upgrade()
        {
            plan.closes.fetch_add(1, Ordering::SeqCst);
            // Closing has started; its future waits for nothing here.
            drop(client.close());
        }
        let _ = sender.send(event);
    };
    (handler, events)
}

/// The handler of the subscription to `channel`: forwards each event,
/// panics at a publication and unsubscribes it when planted.
fn subscription_handler(
    plan: &Arc<Plan>,
    channel: &'static str,
) -> (
    impl FnMut(SubscriptionEvent) + Send + 'static,
    Events<SubscriptionEvent>,
) {
    let (sender, events) = forwarded();
    let plan = Arc::clone(plan);
    let handler = move |event: SubscriptionEvent| {
        plan.turn(matches!(event, SubscriptionEvent::Publication(_)));
        if plan.unsubscribe.swap(false, Ordering::SeqCst)
            && let Some(subscription) = plan
                .upgrade()
                .and_then(|client| client.get_subscription(channel))
        {
            plan.unsubscribes.fetch_add(1, Ordering::SeqCst);
            subscription.unsubscribe();
        }
        let _ = sender.send(event);
    };
    (handler, events)
}

/// Block 16, "Chaos configuration".
fn chaos_config() -> Config {
    Config::default()
        .min_reconnect_delay(Duration::from_millis(50))
        .max_reconnect_delay(Duration::from_millis(500))
        .timeout(TIMEOUT)
        .read_ahead_messages(8)
        .read_ahead_bytes(4096)
        .batch_frame_bytes(1024)
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
    /// `reconnect()` (Block 25, D5).
    Reconnect,
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
    /// The application starts a batch (Block 18).
    StartBatching,
    /// It stops, or drops, its last batch.
    StopBatching,
    DropBatch,
    /// The server sends its next delta broken (Block 19).
    CorruptDelta,
    /// The application publishes this many times on the subscription to
    /// `CHANNELS[n]`, each publish awaited in its own task (Block 46, D16).
    Burst(usize, u32),
    /// A subscription's handler panics at its next publication (Block 49).
    PlantPanic,
    /// The client's handler closes the client at its next event.
    HandlerClose,
    /// A subscription's handler unsubscribes it at its next event.
    HandlerUnsubscribe,
    /// A handler takes over the stream of the subscription read so far.
    TakeOver,
    /// Each handler call takes this many milliseconds.
    Slow(u64),
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
            0..7 => Self::Connect,
            7..8 => Self::Reconnect,
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
            86..93 => Self::Close(CODES[rng.usize(..CODES.len())]),
            93..95 => Self::FailConnects(rng.usize(1..=3)),
            95..97 => Self::StartBatching,
            97..99 => Self::StopBatching,
            _ => Self::DropBatch,
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

/// A client of `server` and its subscriptions to `CHANNELS`, with their
/// streams; with `plan`, a `Thread` handler on the client and on the
/// subscription to `CHANNELS[0]`, each forwarding its events (Block 49).
fn build(
    server: &FakeServer,
    delta: bool,
    plan: Option<&Arc<Plan>>,
) -> (Client, Events<ClientEvent>, Vec<Sub>) {
    let mut builder = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(chaos_config());
    let mut client_events = None;
    if let Some(plan) = plan {
        let (handler, events) = client_handler(plan);
        builder = builder
            .event_delivery(EventDelivery::Thread)
            .handler(handler);
        client_events = Some(events);
    }
    let client = builder.build().expect("a valid configuration");
    if let Some(plan) = plan {
        *plan.client.lock().unwrap() = Some(client.downgrade());
    }
    let events: Events<ClientEvent> = client_events.unwrap_or_else(|| Box::new(client.events()));
    let subs = CHANNELS
        .iter()
        .enumerate()
        .map(|(n, channel)| {
            let builder = client
                .subscription_builder(*channel)
                .positioned(true)
                .recoverable(true)
                .min_resubscribe_delay(Duration::from_millis(50))
                .max_resubscribe_delay(Duration::from_millis(500));
            let mut builder = if delta {
                builder.delta(DeltaFormat::Fossil)
            } else {
                builder
            };
            // Every second subscription has a handler.
            let mut forwarded_events = None;
            if let Some(plan) = plan.filter(|_| n % 2 == 0) {
                let (handler, events) = subscription_handler(plan, channel);
                builder = builder.handler(handler);
                forwarded_events = Some(events);
            }
            let subscription = builder.build().expect("a new channel");
            let events: Events<SubscriptionEvent> =
                forwarded_events.unwrap_or_else(|| Box::new(subscription.events()));
            Sub {
                subscription,
                events,
                last_state: SubscriptionState::Unsubscribed,
                delivered: None,
            }
        })
        .collect();
    (client, events, subs)
}

/// What a run exercises beyond the plain client.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools)] // One switch per runner variant.
struct Variant {
    /// The server skips a recovered offset (the planted fault).
    skip_recovered: bool,
    /// Subscriptions ask for fossil delta (Block 19).
    delta: bool,
    /// Debounce intervals and publish bursts (Block 46).
    debounce: bool,
    /// Handlers on the client and on every second subscription (Block 49).
    handlers: bool,
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
    /// The padding of each publication made, by channel and offset: its
    /// payload, which every delivery must carry (I11).
    pads: BTreeMap<(usize, u64), usize>,
    /// Subscriptions ask for fossil delta, and the server breaks some
    /// deltas (Block 19).
    delta: bool,
    /// The server advises a debounce interval on some subscribes, and the
    /// application makes publish bursts (Block 46, D16).
    debounce: bool,
    /// Tasks awaiting calls, each with the bound of its settling (I6).
    calls: Vec<(JoinHandle<Duration>, Duration)>,
    /// The publishes of each burst made, by burst number (Block 46).
    bursts: Vec<u32>,
    /// Batches the application holds open.
    batches: Vec<ferrifuge::Batch>,
    /// With handlers, what they do next (Block 49).
    plan: Option<Arc<Plan>>,
    /// A handler took over the stream of `CHANNELS[1]`.
    taken_over: bool,
    ops: Vec<String>,
    counts: BTreeMap<&'static str, usize>,
}

type Step = Result<(), Violation>;

impl Run {
    async fn new(seed: u64, variant: Variant) -> Self {
        let Variant {
            skip_recovered,
            delta,
            debounce,
            handlers,
        } = variant;
        let server = FakeServer::start(Announce::default()).await;
        for channel in CHANNELS {
            server.recover_from_history(channel);
            if delta {
                server.delta_on(channel);
            }
        }
        server.limit_recovery(RECOVERY_LIMIT);
        if skip_recovered {
            server.skip_recovered_offset();
        }
        // Handlers take their time from the start, so that reconnects
        // meet events handed and not yet returned (Block 41, D3).
        let plan = handlers.then(|| {
            let plan = Plan::default();
            plan.slow.store(30, Ordering::SeqCst);
            Arc::new(plan)
        });
        let (client, events, subs) = build(&server, delta, plan.as_ref());
        Self {
            seed,
            rng: fastrand::Rng::with_seed(seed ^ 0x5eed_ada9_7e40_0016),
            server,
            client,
            events,
            last_state: State::Disconnected,
            subs,
            published: [0; CHANNELS.len()],
            pads: BTreeMap::new(),
            delta,
            debounce,
            calls: Vec::new(),
            bursts: Vec::new(),
            batches: Vec::new(),
            plan,
            taken_over: false,
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
            // Drawn only with delta, or with debouncing, so the other runs'
            // seeds keep their operations.
            let op = if self.delta && self.rng.u32(0..100) < 8 {
                Op::CorruptDelta
            } else if self.debounce && self.rng.u32(0..100) < 20 {
                Op::Burst(self.rng.usize(..CHANNELS.len()), self.rng.u32(2..=5))
            } else if self.plan.is_some() && self.rng.u32(0..100) < 20 {
                match self.rng.u32(0..100) {
                    0..35 => Op::PlantPanic,
                    35..47 => Op::HandlerClose,
                    47..70 => Op::HandlerUnsubscribe,
                    70..85 => Op::TakeOver,
                    _ => Op::Slow(self.rng.u64(0..=150)),
                }
            } else {
                Op::pick(&mut self.rng)
            };
            self.ops.push(format!("op {n}: {op:?}"));
            self.apply(op).await?;
            self.rebuild_if_closed();
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
            Op::Reconnect => self.client.reconnect(),
            Op::Subscribe(n) => {
                self.advise_interval();
                self.subs[n].subscription.subscribe();
            }
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
            // A stream taking over from a handler would lose what the handler
            // forwarded and the runner has not read: handlers taking over
            // streams are what Block 49 runs.
            Op::SecondEvents(target) if self.handled(target) => {}
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
                    self.pads.insert((n, self.published[n]), pad);
                    self.server
                        .publish_sized(CHANNELS[n], self.published[n], pad);
                }
            }
            Op::Expire(n) => self.server.expire_history(CHANNELS[n]),
            Op::Close(code) => {
                self.server.close_current(code, "chaos");
            }
            Op::FailConnects(count) => self.server.fail_connects(count),
            Op::StartBatching => {
                if self.batches.len() < 3 {
                    self.batches.push(self.client.start_batching());
                    self.count("batch started");
                }
            }
            Op::StopBatching => {
                if let Some(batch) = self.batches.pop() {
                    batch.stop();
                    self.count("batch stopped");
                }
            }
            Op::DropBatch => {
                if self.batches.pop().is_some() {
                    self.count("batch dropped");
                }
            }
            Op::CorruptDelta => self.server.corrupt_next_delta(),
            Op::Burst(n, count) => self.burst(n, count),
            Op::PlantPanic
            | Op::HandlerClose
            | Op::HandlerUnsubscribe
            | Op::TakeOver
            | Op::Slow(_) => {
                self.plan_handlers(op);
            }
        }
        Ok(())
    }

    /// Whether a handler owns `target`'s events (Block 49).
    fn handled(&self, target: Target) -> bool {
        self.plan.is_some()
            && match target {
                Target::Client => true,
                Target::Subscription(n) => n % 2 == 0 || (n == 1 && self.taken_over),
            }
    }

    /// Plans what the handlers do next (Block 49).
    fn plan_handlers(&mut self, op: Op) {
        let Some(plan) = self.plan.clone() else {
            return;
        };
        match op {
            Op::PlantPanic => plan.panic.store(true, Ordering::SeqCst),
            Op::HandlerClose => plan.close.store(true, Ordering::SeqCst),
            Op::HandlerUnsubscribe => plan.unsubscribe.store(true, Ordering::SeqCst),
            Op::Slow(ms) => plan.slow.store(ms, Ordering::SeqCst),
            Op::TakeOver if !self.taken_over => {
                let (handler, events) = subscription_handler(&plan, CHANNELS[1]);
                self.subs[1].subscription.set_handler(handler);
                self.subs[1].events = events;
                self.taken_over = true;
                self.count("handler take-over");
            }
            _ => {}
        }
    }

    /// A handler closed the client: the application makes a new one, as
    /// after its own `close()`.
    fn rebuild_if_closed(&mut self) {
        if !self.client.is_closed() {
            return;
        }
        let (client, events, subs) = build(&self.server, self.delta, self.plan.as_ref());
        self.client = client;
        self.events = events;
        self.subs = subs;
        self.last_state = State::Disconnected;
        self.taken_over = false;
        self.batches.clear();
        self.count("client made again");
    }

    /// With debouncing, the next subscribe replies advise an interval of 1
    /// to 3 ticks, or none (Block 46, D16).
    fn advise_interval(&mut self) {
        if self.debounce {
            let interval = (self.rng.u32(0..100) < 75).then(|| self.rng.u32(1..=3) * TICK);
            self.server.advise_publish_debounce(interval);
        }
    }

    /// `count` publishes on the subscription to `CHANNELS[n]`, numbered so
    /// that those merged can be counted, each awaited in its own task, on
    /// a subscription that wants to be subscribed so that bursts reach the
    /// windows (Block 46, D16).
    fn burst(&mut self, n: usize, count: u32) {
        self.advise_interval();
        self.count("publish burst");
        self.subs[n].subscription.subscribe();
        let burst = self.bursts.len();
        self.bursts.push(count);
        for publish in 0..count {
            let made = Instant::now();
            let data = serde_json::json!({"burst": burst, "publish": publish}).to_string();
            let call = self.subs[n].subscription.publish(data.into_bytes());
            let handle = tokio::spawn(async move {
                let _ = call.await;
                made.elapsed()
            });
            self.calls.push((handle, WINDOW_CALL_BOUND));
        }
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
        let handle = tokio::spawn(async move {
            call.await;
            made.elapsed()
        });
        self.calls.push((handle, CALL_BOUND));
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
            SubscriptionEvent::Subscribing { code, .. } => {
                if *code == codes::subscribing::DELTA_ERROR {
                    self.count("delta resubscribe");
                }
                SubscriptionState::Subscribing
            }
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
                // I11: the payload the server published at that offset.
                if let Some(pad) = self.pads.get(&(n, offset)) {
                    let published =
                        serde_json::json!({"n": offset, "pad": "x".repeat(*pad)}).to_string();
                    if publication.data != published.as_bytes() {
                        return Err(self.violation(
                            "I11",
                            format!(
                                "{} offset {offset} carries {}",
                                CHANNELS[n],
                                String::from_utf8_lossy(&publication.data)
                            ),
                        ));
                    }
                }
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
        // Healed: the handlers plan nothing more (Block 49).
        if let Some(plan) = &self.plan {
            plan.panic.store(false, Ordering::SeqCst);
            plan.close.store(false, Ordering::SeqCst);
            plan.unsubscribe.store(false, Ordering::SeqCst);
            plan.slow.store(0, Ordering::SeqCst);
        }
        // Healed: every batch is stopped (Block 18, D9).
        for batch in std::mem::take(&mut self.batches) {
            batch.stop();
        }
        let deadline = Instant::now() + QUIESCENCE;
        loop {
            // A handler may have closed the client before it healed.
            self.rebuild_if_closed();
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
                        self.calls.iter().filter(|(call, _)| !call.is_finished()).count()
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
        for (call, bound) in std::mem::take(&mut self.calls) {
            let took = call
                .await
                .map_err(|error| self.violation("I2", error.to_string()))?;
            if took > bound {
                return Err(self.violation("I6", format!("a call settled after {took:?}")));
            }
        }
        let open = self.open_connections(1).await;
        if open > 1 {
            return Err(self.violation("I3", format!("{open} connections open")));
        }
        Ok(())
    }

    /// The connections open on the fake server, once at most `at_most`
    /// are or [`QUIESCENCE`] has passed: the fake notices a connection the
    /// client closed when its task next runs, which a loaded runner delays.
    async fn open_connections(&self, at_most: usize) -> usize {
        let deadline = Instant::now() + QUIESCENCE;
        loop {
            let open = self.server.connections() - self.server.closed_connections();
            if open <= at_most || Instant::now() >= deadline {
                return open;
            }
            tokio::time::sleep(READ_WAIT).await;
        }
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
            && self.calls.iter().all(|(call, _)| call.is_finished())
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
        let open = self.open_connections(0).await;
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
    variant: Variant,
) -> Result<BTreeMap<&'static str, usize>, Violation> {
    count_panics();
    let mut counts = BTreeMap::new();
    for seed in seeds {
        let mut run = Run::new(seed, variant).await;
        run.run(OPS).await?;
        if let Some(plan) = &run.plan {
            for (name, count) in [
                ("handler panic", &plan.panics),
                ("close from a handler", &plan.closes),
                ("unsubscribe from a handler", &plan.unsubscribes),
            ] {
                *counts.entry(name).or_default() += count.load(Ordering::SeqCst);
            }
        }
        *counts.entry("corrupted delta").or_default() += run.server.corrupted_deltas();
        frames_of(&run, &mut counts);
        merged_of(&run, &mut counts);
        for (name, count) in run.counts {
            *counts.entry(name).or_default() += count;
        }
    }
    Ok(counts)
}

/// Counts the publishes of `run`'s bursts merged in a window: the fake
/// server never read them but read a later one of their burst (Block 46,
/// D3).
fn merged_of(run: &Run, counts: &mut BTreeMap<&'static str, usize>) {
    let read: std::collections::BTreeSet<(u64, u64)> = run
        .server
        .received_with_frames()
        .iter()
        .filter_map(|(command, _)| {
            let data = &command["publish"]["data"];
            Some((data["burst"].as_u64()?, data["publish"].as_u64()?))
        })
        .collect();
    for (burst, count) in run.bursts.iter().enumerate() {
        let burst = burst as u64;
        for publish in 0..u64::from(*count) {
            let later = publish + 1..u64::from(*count);
            if !read.contains(&(burst, publish))
                && later.into_iter().any(|at| read.contains(&(burst, at)))
            {
                *counts.entry("publish merged").or_default() += 1;
            }
        }
    }
}

/// Counts the frames of `run` the fake server read: those of several
/// commands in a seed that opened no batch, which automatic batching made
/// (Block 36a), and those whose connect command carries subscribe commands
/// (Block 36b).
fn frames_of(run: &Run, counts: &mut BTreeMap<&'static str, usize>) {
    let mut frames: BTreeMap<usize, Vec<serde_json::Value>> = BTreeMap::new();
    for (command, frame) in run.server.received_with_frames() {
        frames.entry(frame).or_default().push(command);
    }
    let batched = run.counts.contains_key("batch started");
    for commands in frames.values() {
        if commands.len() > 1 && !batched {
            *counts
                .entry("frame of several commands no batch held")
                .or_default() += 1;
        }
        if commands[0]["connect"].is_object()
            && commands
                .iter()
                .any(|command| command["subscribe"].is_object())
        {
            *counts
                .entry("connect frame carrying subscribe commands")
                .or_default() += 1;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_chaos_on_the_tokio_client_batches_automatically() {
    let seeds = seeds();
    let counts = run_seeds(seeds.clone(), Variant::default())
        .await
        .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let missing: Vec<_> = [
        "frame of several commands no batch held",
        "connect frame carrying subscribe commands",
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
async fn seeded_chaos_on_the_tokio_client_keeps_the_invariants() {
    let seeds = seeds();
    let counts = run_seeds(seeds.clone(), Variant::default())
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
        "batch stopped",
        "batch dropped",
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
    let violation = run_seeds(
        SEEDS,
        Variant {
            skip_recovered: true,
            ..Variant::default()
        },
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_chaos_with_delta_subscriptions_on_the_tokio_client_keeps_the_invariants() {
    let seeds = seeds();
    let counts = run_seeds(
        seeds.clone(),
        Variant {
            delta: true,
            ..Variant::default()
        },
    )
    .await
    .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let missing: Vec<_> = ["corrupted delta", "delta resubscribe"]
        .into_iter()
        .filter(|name| counts.get(name).copied().unwrap_or(0) == 0)
        .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_chaos_with_debounced_publishes_on_the_tokio_client_keeps_the_invariants() {
    let seeds = seeds();
    let counts = run_seeds(
        seeds.clone(),
        Variant {
            debounce: true,
            ..Variant::default()
        },
    )
    .await
    .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let missing: Vec<_> = ["reconnect", "publish burst", "publish merged"]
        .into_iter()
        .filter(|name| counts.get(name).copied().unwrap_or(0) == 0)
        .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
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
    let delta = std::env::var("FERRIFUGE_CHAOS_DELTA").is_ok();
    let debounce = std::env::var("FERRIFUGE_CHAOS_DEBOUNCE").is_ok();
    println!(
        "chaos: adapter seeds {}..{}, delta {delta}, debounce {debounce}",
        seeds.start, seeds.end
    );
    if let Err(violation) = run_seeds(
        seeds,
        Variant {
            delta,
            debounce,
            ..Variant::default()
        },
    )
    .await
    {
        panic!("{violation}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_chaos_with_handlers_on_the_tokio_client_keeps_the_invariants() {
    let seeds = seeds();
    let counts = run_seeds(
        seeds.clone(),
        Variant {
            handlers: true,
            ..Variant::default()
        },
    )
    .await
    .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let missing: Vec<_> = ["handler panic", "close from a handler", "handler take-over"]
        .into_iter()
        .filter(|name| counts.get(name).copied().unwrap_or(0) == 0)
        .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
}
