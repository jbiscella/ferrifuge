//! Seeded chaos on the sans-IO core (Block 16, `@P`): random operations
//! against a server simulated here, on a simulated clock, with the
//! invariants checked after every step. A seed fixes every choice, so a
//! failure reruns alone with `FERRIFUGE_CHAOS_SEED=<seed>`.

// @P scenario: Seeded chaos on the core keeps the connection invariants
// @P scenario: A seed of the core runner reproduces its run
// @P scenario: The core runner reports a planted connection fault
// @P scenario: Seeded chaos with subscriptions on the core keeps every invariant
// @P scenario: The core runner reports a planted delivery fault

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use ferrifuge::machine::{
    Action, CallId, CallTimer, EventId, FetchId, FrameId, Input, Machine, SubscriptionId,
    SubscriptionOptions, SubscriptionTimer, Timer, TransportId,
};
use ferrifuge::{
    CallRequest, ClientEvent, Config, HistoryOptions, ServerSubscriptionEvent, State, StateError,
    StreamPosition, SubscriptionEvent, SubscriptionState, TokenError, codes,
};
use serde_json::{Value, json};

/// Seeds every pull request runs (Block 16, "CI").
const SEEDS: Range<u64> = 0..300;
/// Operations of each seed.
const OPS: usize = 60;
/// Reruns one seed alone.
const SEED_VARIABLE: &str = "FERRIFUGE_CHAOS_SEED";
/// Operations of each seed of the long run.
const OPS_VARIABLE: &str = "FERRIFUGE_CHAOS_OPS";
/// `Config::timeout` of the chaos configuration.
const TIMEOUT: Duration = Duration::from_secs(1);
/// I4: how long after healing the client must be connected.
const LIVENESS: Duration = Duration::from_secs(3);
/// I6: a call waits at most once per phase (connection, room, reply).
const CALL_BOUND: Duration = Duration::from_secs(3);
/// Lines of the trace a violation shows before it.
const TAIL: usize = 40;
/// Interval of the simulated server's pings, in seconds.
const PING: u64 = 1;
/// Read-ahead limits of the chaos configuration.
const READ_AHEAD_MESSAGES: usize = 8;
const READ_AHEAD_BYTES: usize = 4096;
/// Client-side subscriptions; the second one reloads its state with
/// getState, so its failed recoveries are answered with error 112.
const CHANNELS: [&str; 2] = ["news", "chat"];
/// Channels the server subscribes the client to.
const SERVER_CHANNELS: [&str; 2] = ["alerts", "feed"];
/// Publications a recovery may bring back (Centrifugo
/// `client.recovery_max_publication_limit`).
const RECOVERY_LIMIT: u64 = 8;
const EPOCH: &str = "e";
/// `SubscribeRequest.flag` asking a failed recovery to be refused with
/// error 112 (`client.proto`).
const REJECT_UNRECOVERED: i64 = 1 << 1;

/// Reconnect and resubscribe delays of the chaos configuration.
const MIN_DELAY: Duration = Duration::from_millis(50);
const MAX_DELAY: Duration = Duration::from_millis(500);

/// Block 16, "Chaos configuration".
fn chaos_config() -> Config {
    Config {
        min_reconnect_delay: MIN_DELAY,
        max_reconnect_delay: MAX_DELAY,
        timeout: TIMEOUT,
        read_ahead_messages: READ_AHEAD_MESSAGES,
        read_ahead_bytes: READ_AHEAD_BYTES,
        ..Config::default()
    }
}

/// A fault planted outside the client, which the runner must report
/// (Block 16, "Checking the checker").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// A state event of the client dropped before the checker (I1).
    DropStateEvent,
    /// A transport the client closed kept open by the simulated network (I3).
    KeepClosedTransport,
    /// A simulated server that ignores connect commands from healing on (I4).
    IgnoreConnectWhenHealed,
    /// A timer the client cancelled after disconnect kept by the clock (I5).
    KeepCancelledTimer,
    /// A call outcome dropped before the checker (I6).
    DropCallOutcome,
    /// A subscribe reply that recovers with an offset skipped (I8).
    SkipRecovered,
    /// A connect reply that recovers a server-side subscription with an
    /// offset skipped (I8).
    SkipServerRecovered,
    /// A publication duplicated before the checker (I8).
    DuplicatePublication,
    /// A state event of a subscription dropped before the checker (I1).
    DropSubscriptionStateEvent,
    /// `wants_frames()` reported true to the checker while the read-ahead
    /// is full (I9).
    WantsFramesWhenFull,
}

/// An invariant a seed broke.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation {
    seed: u64,
    invariant: &'static str,
    /// Index of the operation during which it broke.
    op: usize,
    /// Operations the seed runs.
    of: usize,
    detail: String,
    /// The operations up to it.
    ops: Vec<String>,
    /// The last inputs and actions before it.
    tail: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} broke {} at operation {}: {}\n\
             rerun: {SEED_VARIABLE}={} {OPS_VARIABLE}={} cargo test --release --test chaos \
             seeded_chaos_long_run -- --ignored\n\
             operations:\n  {}\nlast inputs and actions:\n  {}",
            self.seed,
            self.invariant,
            self.op,
            self.detail,
            self.seed,
            self.of,
            self.ops.join("\n  "),
            self.tail.join("\n  ")
        )
    }
}

/// What the application or the server does next (Block 16, "Operations",
/// "Server behaviour").
#[derive(Debug, Clone, Copy)]
enum Op {
    Connect,
    Disconnect,
    Call(CallKind),
    /// Takes the next event of the client's stream, or of a subscription's.
    TakeNext(Target),
    Hold,
    Read,
    OpenStream(Target),
    DropStream(Target),
    Advance(Duration),
    /// The server closes the connection with a close code.
    Close(u16),
    /// The server sends a Disconnect push, then closes.
    Push(u16),
    /// The connection drops without a close code.
    Drop,
    /// The server stops pinging this connection.
    PingGap,
    /// Adds a client-side subscription to `CHANNELS[n]`.
    Add(usize),
    Subscribe(usize),
    Unsubscribe(usize),
    Remove(usize),
    /// The server publishes `count` publications to a channel, client-side
    /// (`CHANNELS`) or server-side (`SERVER_CHANNELS`).
    Publish(Channel, u32),
    /// The server's history of a channel expires.
    Expire(Channel),
    /// The server unsubscribes a client-side subscription with a code.
    UnsubscribePush(usize, u32),
    /// The server subscribes the client to `SERVER_CHANNELS[n]`, now and at
    /// later connects.
    ServerSubscribe(usize),
    /// The server unsubscribes the client from `SERVER_CHANNELS[n]`.
    ServerUnsubscribe(usize),
}

#[derive(Debug, Clone, Copy)]
enum CallKind {
    Rpc,
    Publish,
    History,
    Presence,
}

/// Whose stream an operation reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Client,
    /// The subscription to `CHANNELS[n]`.
    Subscription(usize),
}

#[derive(Debug, Clone, Copy)]
enum Channel {
    Client(usize),
    Server(usize),
}

impl Channel {
    fn name(self) -> &'static str {
        match self {
            Self::Client(n) => CHANNELS[n],
            Self::Server(n) => SERVER_CHANNELS[n],
        }
    }
}

/// Close and Disconnect push codes: reconnect-advised (3001, 4000, 3008
/// "slow", 3014 "state invalidated") and terminal (3500, 4500).
const CODES: [u16; 6] = [3001, 4000, 3008, 3014, 3500, 4500];
/// Unsubscribe push codes: unsubscribe (2000) and resubscribe (2500).
const UNSUBSCRIBE_CODES: [u32; 2] = [2000, 2500];

impl Op {
    /// The next operation; with `subscriptions`, those of subscriptions
    /// too. Without, the choices are those of the connection runner alone.
    fn pick(rng: &mut fastrand::Rng, subscriptions: bool) -> Self {
        if subscriptions && rng.u32(0..100) < 45 {
            return Self::pick_subscription(rng);
        }
        match rng.u32(0..100) {
            0..8 => Self::Connect,
            8..13 => Self::Disconnect,
            13..28 => Self::Call(match rng.u32(0..4) {
                0 => CallKind::Rpc,
                1 => CallKind::Publish,
                2 => CallKind::History,
                _ => CallKind::Presence,
            }),
            28..38 => Self::TakeNext(target(rng, subscriptions)),
            38..43 => Self::Hold,
            43..50 => Self::Read,
            50..55 => Self::OpenStream(target(rng, subscriptions)),
            55..59 => Self::DropStream(target(rng, subscriptions)),
            59..80 => Self::Advance(Duration::from_millis(rng.u64(100..=2000))),
            80..88 => Self::Close(CODES[rng.usize(..CODES.len())]),
            88..93 => Self::Push(CODES[rng.usize(..CODES.len())]),
            93..97 => Self::Drop,
            _ => Self::PingGap,
        }
    }

    fn pick_subscription(rng: &mut fastrand::Rng) -> Self {
        let client = rng.usize(..CHANNELS.len());
        let server = rng.usize(..SERVER_CHANNELS.len());
        let channel = if rng.bool() {
            Channel::Client(client)
        } else {
            Channel::Server(server)
        };
        match rng.u32(0..100) {
            0..10 => Self::Add(client),
            10..22 => Self::Subscribe(client),
            22..28 => Self::Unsubscribe(client),
            28..31 => Self::Remove(client),
            31..66 => Self::Publish(channel, rng.u32(1..=6)),
            66..72 => Self::Expire(channel),
            72..80 => Self::UnsubscribePush(
                client,
                UNSUBSCRIBE_CODES[rng.usize(..UNSUBSCRIBE_CODES.len())],
            ),
            80..90 => Self::ServerSubscribe(server),
            _ => Self::ServerUnsubscribe(server),
        }
    }

    /// Its name in the run counters.
    fn name(self) -> String {
        match self {
            Self::Call(kind) => format!("call {kind:?}"),
            Self::TakeNext(target) => format!("TakeNext {}", target.kind()),
            Self::OpenStream(target) => format!("OpenStream {}", target.kind()),
            Self::DropStream(target) => format!("DropStream {}", target.kind()),
            Self::Advance(_) => "advance".to_owned(),
            Self::Close(code) => format!("close {code}"),
            Self::Push(code) => format!("push {code}"),
            Self::Add(_) => "add".to_owned(),
            Self::Subscribe(_) => "subscribe".to_owned(),
            Self::Unsubscribe(_) => "unsubscribe".to_owned(),
            Self::Remove(_) => "remove".to_owned(),
            Self::Publish(channel, _) => format!("publish {}", channel.kind()),
            Self::Expire(channel) => format!("expire {}", channel.kind()),
            Self::UnsubscribePush(_, code) => format!("unsubscribe push {code}"),
            Self::ServerSubscribe(_) => "server subscribe".to_owned(),
            Self::ServerUnsubscribe(_) => "server unsubscribe".to_owned(),
            other => format!("{other:?}"),
        }
    }
}

impl Target {
    fn kind(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Subscription(_) => "subscription",
        }
    }
}

impl Channel {
    fn kind(self) -> &'static str {
        match self {
            Self::Client(_) => "client-side",
            Self::Server(_) => "server-side",
        }
    }
}

fn target(rng: &mut fastrand::Rng, subscriptions: bool) -> Target {
    if subscriptions && rng.bool() {
        Target::Subscription(rng.usize(..CHANNELS.len()))
    } else {
        Target::Client
    }
}

/// Something the simulated world does at a point of simulated time.
#[derive(Debug)]
enum Due {
    Timer(Key),
    /// Data or a close reaches the client's socket.
    Arrive(TransportId, Inbound),
    /// The server answers a command it got earlier, if the connection is
    /// still there.
    Reply(TransportId, Value),
    Opened(TransportId),
    Refused(TransportId),
    Written(TransportId, FrameId),
    Token(FetchId, Result<String, TokenError>),
    /// The application's getState callback answers for a subscription.
    State(FetchId, SubscriptionId, bool),
    /// The server pings the connection, if it still pings it.
    Ping(TransportId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    Client(Timer),
    Call(CallId, CallTimer),
    Subscription(SubscriptionId, SubscriptionTimer),
}

#[derive(Debug)]
enum Inbound {
    Frame(Vec<u8>),
    Close(Option<u16>, String),
}

/// A channel's stream on the simulated server: offsets `oldest + 1` to
/// `top` are in its history.
#[derive(Default, Clone, Copy)]
struct Stream {
    top: u64,
    oldest: u64,
}

/// The simulated server.
struct Server {
    /// The transport the server accepted last.
    current: Option<TransportId>,
    /// The connect reply went out on `current`.
    connected: bool,
    /// Channels on which `current` gets live publications.
    subscribed: BTreeSet<&'static str>,
    /// Whether it pings `current`.
    pinging: bool,
    /// Faults are over: the server answers every command at once.
    healed: bool,
    /// When the last message to each transport arrives, so that messages
    /// of one connection arrive in order.
    last_arrival: BTreeMap<u64, Duration>,
    streams: BTreeMap<&'static str, Stream>,
    /// Server-side subscriptions it grants at connect.
    granted: BTreeSet<&'static str>,
}

/// An object whose events the application reads from one stream.
type Of = Option<SubscriptionId>;

/// A subscription as I8 follows it: a removed and added subscription, or a
/// server-side one unsubscribed and granted again, is a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Sub {
    Client(SubscriptionId),
    Server(&'static str, u32),
}

/// An event emitted and not yet delivered, as the checker sees it.
#[derive(Debug, Clone)]
struct Tracked {
    sub: Option<Sub>,
    kind: Kind,
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Publication(Option<u64>),
    Subscribed {
        was_recovering: bool,
        recovered: bool,
    },
    Other,
}

/// What the checker has seen.
#[derive(Default)]
struct Checker {
    /// The last client state event emitted (I1).
    last_state: Option<State>,
    /// The last state event of each subscription (I1).
    sub_states: BTreeMap<Sub, SubscriptionState>,
    /// Transports opened and not yet closed (I3).
    open: BTreeSet<u64>,
    /// `disconnect()` was called, and `connect()` not since (I5).
    disconnected: bool,
    /// Calls not yet settled, with when they were made (I6).
    calls: BTreeMap<CallId, Duration>,
    /// Events emitted and not yet delivered (I8, I9).
    events: BTreeMap<EventId, Tracked>,
    /// The offset delivered last per subscription, `None` until one is or
    /// after a restart (I8).
    delivered: BTreeMap<Sub, Option<u64>>,
    /// The current incarnation of each server-side subscription (I8).
    generations: BTreeMap<&'static str, u32>,
}

/// One seed's run.
struct Run {
    seed: u64,
    fault: Option<Fault>,
    /// Whether the run uses subscriptions (16b) or the connection alone.
    subscriptions: bool,
    rng: fastrand::Rng,
    machine: Machine,
    now: Duration,
    /// Simulated events by time, then by when they were scheduled.
    agenda: BTreeMap<(Duration, u64), Due>,
    scheduled: u64,
    /// What reached the client's socket and waits to be read.
    inbox: VecDeque<(TransportId, Inbound)>,
    server: Server,
    /// The client-side subscriptions, by `CHANNELS` index.
    subs: [Option<SubscriptionId>; CHANNELS.len()],
    /// The application's open streams, and the events each has not taken.
    streams: BTreeMap<Of, VecDeque<EventId>>,
    /// The application takes every event as it comes.
    reading: bool,
    /// A stream being dropped: its events emitted meanwhile count as taken
    /// at once, as the adapter's outbox reports them.
    dropping: Option<Of>,
    /// Events to report taken once the current input's actions are done.
    taken_at_once: Vec<EventId>,
    checker: Checker,
    /// The operation being run.
    op: usize,
    /// Operations the seed runs.
    of: usize,
    /// Every operation, input and action, in order.
    trace: Vec<String>,
    /// How often each operation, server behaviour and outcome happened.
    counts: BTreeMap<String, usize>,
}

type Step = Result<(), Violation>;

impl Run {
    fn new(seed: u64, fault: Option<Fault>, subscriptions: bool) -> Self {
        Self {
            seed,
            fault,
            subscriptions,
            rng: fastrand::Rng::with_seed(seed ^ 0x5eed_c4a0_5000_0016),
            machine: Machine::with_token_source(chaos_config()).with_seed(seed),
            now: Duration::ZERO,
            agenda: BTreeMap::new(),
            scheduled: 0,
            inbox: VecDeque::new(),
            server: Server {
                current: None,
                connected: false,
                subscribed: BTreeSet::new(),
                pinging: true,
                healed: false,
                last_arrival: BTreeMap::new(),
                streams: BTreeMap::new(),
                granted: BTreeSet::new(),
            },
            subs: [None; CHANNELS.len()],
            streams: BTreeMap::new(),
            reading: false,
            dropping: None,
            taken_at_once: Vec::new(),
            checker: Checker::default(),
            op: 0,
            of: 0,
            trace: Vec::new(),
            counts: BTreeMap::new(),
        }
    }

    /// Runs `ops` operations, heals, and checks liveness.
    fn run(&mut self, ops: usize) -> Step {
        self.of = ops;
        for op in 0..ops {
            self.op = op;
            let next = Op::pick(&mut self.rng, self.subscriptions);
            self.trace.push(format!("op {op}: {next:?}"));
            self.count(next.name());
            self.apply(next)?;
            let pause = Duration::from_millis(self.rng.u64(0..=50));
            self.advance(pause)?;
        }
        self.op = ops;
        self.heal()
    }

    fn count(&mut self, name: impl Into<String>) {
        *self.counts.entry(name.into()).or_default() += 1;
    }

    fn violation(&self, invariant: &'static str, detail: impl Into<String>) -> Violation {
        Violation {
            seed: self.seed,
            invariant,
            op: self.op,
            of: self.of,
            detail: detail.into(),
            ops: self
                .trace
                .iter()
                .filter(|line| line.starts_with("op ") || line.starts_with("heal"))
                .cloned()
                .collect(),
            tail: self.trace[self.trace.len().saturating_sub(TAIL)..].to_vec(),
        }
    }

    fn of(&self, target: Target) -> Option<Of> {
        match target {
            Target::Client => Some(None),
            Target::Subscription(n) => self.subs[n].map(Some),
        }
    }

    fn apply(&mut self, op: Op) -> Step {
        match op {
            Op::Connect => {
                self.checker.disconnected = false;
                self.feed(Input::Connect)
            }
            Op::Disconnect => {
                self.checker.disconnected = true;
                self.feed(Input::Disconnect)
            }
            Op::Call(kind) => {
                let channel = "news".to_owned();
                let id = self.machine.add_call(match kind {
                    CallKind::Rpc => CallRequest::Rpc {
                        method: "m".to_owned(),
                        data: b"{}".to_vec(),
                    },
                    CallKind::Publish => CallRequest::Publish {
                        channel,
                        data: b"{}".to_vec(),
                    },
                    CallKind::History => CallRequest::History {
                        channel,
                        options: HistoryOptions::default(),
                    },
                    CallKind::Presence => CallRequest::Presence { channel },
                });
                self.checker.calls.insert(id, self.now);
                self.feed(Input::Call(id))
            }
            Op::TakeNext(target) => match self.of(target) {
                Some(of) => self.take_next(of),
                None => Ok(()),
            },
            Op::Hold => {
                self.reading = false;
                Ok(())
            }
            Op::Read => {
                self.reading = true;
                self.take_all()
            }
            Op::OpenStream(target) => match self.of(target) {
                Some(of) if !self.streams.contains_key(&of) => {
                    self.streams.insert(of, VecDeque::new());
                    self.feed(Input::StreamOpened(of))
                }
                _ => Ok(()),
            },
            Op::DropStream(target) => match self.of(target) {
                Some(of) => self.drop_stream(of),
                None => Ok(()),
            },
            Op::Advance(by) => self.advance(by),
            Op::Close(code) => {
                self.server_close(Some(code), "chaos");
                Ok(())
            }
            Op::Push(code) => {
                if let Some(transport) = self.server.current {
                    let push = json!({"push": {"disconnect": {"code": code, "reason": "chaos"}}});
                    self.arrive(transport, frame(&push));
                    self.server_close(Some(code), "chaos");
                }
                Ok(())
            }
            Op::Drop => {
                self.server_close(None, "");
                Ok(())
            }
            Op::PingGap => {
                self.server.pinging = false;
                Ok(())
            }
            subscription => self.apply_subscription(subscription),
        }
    }

    /// The operations of subscriptions.
    fn apply_subscription(&mut self, op: Op) -> Step {
        match op {
            Op::Add(n) => {
                if self.subs[n].is_some() {
                    return Ok(());
                }
                let mut options = SubscriptionOptions::default()
                    .positioned(true)
                    .recoverable(true)
                    .min_resubscribe_delay(MIN_DELAY)
                    .max_resubscribe_delay(MAX_DELAY);
                if n == 1 {
                    options = options.state_source();
                }
                let id = self
                    .machine
                    .add_subscription(CHANNELS[n].to_owned(), options)
                    .expect("a channel without a subscription");
                self.subs[n] = Some(id);
                self.checker
                    .sub_states
                    .insert(Sub::Client(id), SubscriptionState::Unsubscribed);
                Ok(())
            }
            Op::Subscribe(n) => match self.subs[n] {
                Some(id) => self.feed(Input::Subscribe(id)),
                None => Ok(()),
            },
            Op::Unsubscribe(n) => match self.subs[n] {
                Some(id) => self.feed(Input::Unsubscribe(id)),
                None => Ok(()),
            },
            Op::Remove(n) => match self.subs[n].take() {
                Some(id) => self.feed(Input::RemoveSubscription(id)),
                None => Ok(()),
            },
            Op::Publish(channel, count) => {
                for _ in 0..count {
                    self.publish(channel.name());
                }
                Ok(())
            }
            Op::Expire(channel) => {
                let stream = self.server.streams.entry(channel.name()).or_default();
                stream.oldest = stream.top;
                Ok(())
            }
            Op::UnsubscribePush(n, code) => {
                let channel = CHANNELS[n];
                if self.server.subscribed.remove(channel)
                    && let Some(transport) = self.server.current
                {
                    let push = json!({"push": {"channel": channel, "unsubscribe": {"code": code, "reason": "chaos"}}});
                    self.arrive(transport, frame(&push));
                }
                Ok(())
            }
            Op::ServerSubscribe(n) => {
                let channel = SERVER_CHANNELS[n];
                self.server.granted.insert(channel);
                if self.server.connected
                    && self.server.subscribed.insert(channel)
                    && let Some(transport) = self.server.current
                {
                    let top = self.server.streams.entry(channel).or_default().top;
                    let push = json!({"push": {"channel": channel, "subscribe": {
                        "recoverable": true, "positioned": true, "epoch": EPOCH, "offset": top,
                    }}});
                    self.arrive(transport, frame(&push));
                }
                Ok(())
            }
            Op::ServerUnsubscribe(n) => {
                let channel = SERVER_CHANNELS[n];
                self.server.granted.remove(channel);
                if self.server.subscribed.remove(channel)
                    && let Some(transport) = self.server.current
                {
                    let push = json!({"push": {"channel": channel, "unsubscribe": {"code": 2000, "reason": "server unsubscribe"}}});
                    self.arrive(transport, frame(&push));
                }
                Ok(())
            }
            other => unreachable!("not an operation of subscriptions: {other:?}"),
        }
    }

    /// The server publishes the next offset of `channel`, live to the
    /// connection subscribed to it; its data pads the read-ahead.
    fn publish(&mut self, channel: &'static str) {
        let stream = self.server.streams.entry(channel).or_default();
        stream.top += 1;
        let offset = stream.top;
        if self.server.subscribed.contains(channel)
            && let Some(transport) = self.server.current
        {
            let publication = publication(offset, self.rng.usize(0..=600));
            let push = json!({"push": {"channel": channel, "pub": publication}});
            self.arrive(transport, frame(&push));
        }
    }

    /// Faults stop, the application reads every event and wants to be
    /// connected: within [`LIVENESS`] the client is connected and every
    /// subscription not unsubscribed is subscribed (I4), and the calls made
    /// settle (I6). The last fault is over once what the server and the
    /// application's callbacks sent before healing has arrived.
    fn heal(&mut self) -> Step {
        self.trace.push("heal".to_owned());
        self.server.healed = true;
        self.server.pinging = true;
        self.reading = true;
        self.take_all()?;
        let sent_before = self.scheduled;
        while let Some(&(at, _)) = self
            .agenda
            .iter()
            .find(|((_, seq), due)| *seq <= sent_before && in_flight(due))
            .map(|(key, _)| key)
        {
            self.advance(at.saturating_sub(self.now))?;
        }
        self.trace.push("healed".to_owned());
        if self.machine.state() == State::Disconnected {
            self.checker.disconnected = false;
            self.feed(Input::Connect)?;
        }
        let deadline = self.now + LIVENESS;
        while self.not_live().is_some() {
            let Some(&(at, _)) = self.agenda.keys().next() else {
                break;
            };
            if at > deadline {
                break;
            }
            self.advance(at.saturating_sub(self.now))?;
        }
        if let Some(what) = self.not_live() {
            return Err(self.violation("I4", format!("{what} {LIVENESS:?} after healing")));
        }
        self.advance(CALL_BOUND)
    }

    /// What keeps the client from being live, if anything.
    fn not_live(&self) -> Option<String> {
        if self.machine.state() != State::Connected {
            return Some(format!("the client {:?}", self.machine.state()));
        }
        if self.streams.values().any(|untaken| !untaken.is_empty()) {
            return Some("events not taken".to_owned());
        }
        for id in self.subs.iter().flatten() {
            let state = self.machine.subscription_state(*id);
            if state != Some(SubscriptionState::Unsubscribed)
                && state != Some(SubscriptionState::Subscribed)
            {
                return Some(format!("{id:?} {state:?}"));
            }
        }
        for channel in self.machine.server_subscriptions() {
            let sub = self.server_sub(&channel);
            let state = self.checker.sub_states.get(&sub);
            if state != Some(&SubscriptionState::Subscribed) {
                return Some(format!("server-side {channel} {state:?}"));
            }
        }
        None
    }

    /// Lets `by` of simulated time pass, handling what falls due.
    fn advance(&mut self, by: Duration) -> Step {
        let until = self.now + by;
        while let Some(entry) = self.agenda.first_entry() {
            let (at, _) = *entry.key();
            if at > until {
                break;
            }
            let due = entry.remove();
            self.now = at;
            self.happen(due)?;
        }
        self.now = until;
        self.check_calls()
    }

    fn schedule(&mut self, after: Duration, due: Due) {
        self.scheduled += 1;
        self.agenda.insert((self.now + after, self.scheduled), due);
    }

    fn latency(&mut self) -> Duration {
        Duration::from_millis(self.rng.u64(0..=30))
    }

    fn happen(&mut self, due: Due) -> Step {
        match due {
            Due::Timer(Key::Client(timer)) => self.feed(Input::TimerFired(timer)),
            Due::Timer(Key::Call(call, timer)) => self.feed(Input::CallTimerFired(call, timer)),
            Due::Timer(Key::Subscription(id, timer)) => {
                self.feed(Input::SubscriptionTimerFired(id, timer))
            }
            Due::Arrive(transport, inbound) => {
                self.inbox.push_back((transport, inbound));
                self.pump()
            }
            Due::Reply(transport, command) => {
                if self.server.current == Some(transport) {
                    let reply = self.result(transport, &command);
                    self.arrive(transport, frame(&reply));
                }
                Ok(())
            }
            Due::Opened(transport) => {
                self.server.current = Some(transport);
                self.server.connected = false;
                self.server.subscribed.clear();
                self.feed(Input::TransportOpened(transport))
            }
            Due::Refused(transport) => {
                self.checker.open.remove(&id(transport));
                self.feed(Input::TransportFailed {
                    transport,
                    message: "refused".to_owned(),
                })
            }
            Due::Written(transport, frame) => self.feed(Input::FrameWritten { transport, frame }),
            Due::Token(fetch, result) => self.feed(Input::TokenFetched { fetch, result }),
            Due::State(fetch, id, loaded) => {
                let result = if loaded {
                    // The application loads its state as of the channel's
                    // current position: deliveries go on from there (I8).
                    let channel = self.channel_of(id);
                    let top = self.server.streams.entry(channel).or_default().top;
                    self.checker.delivered.insert(Sub::Client(id), Some(top));
                    Ok(StreamPosition {
                        offset: top,
                        epoch: EPOCH.to_owned(),
                    })
                } else {
                    Err(StateError("chaos".to_owned()))
                };
                self.feed(Input::SubscriptionStateFetched { fetch, result })
            }
            Due::Ping(transport) => {
                if self.server.current == Some(transport) && self.server.pinging {
                    self.arrive(transport, Inbound::Frame(b"{}".to_vec()));
                    self.schedule(Duration::from_secs(PING), Due::Ping(transport));
                }
                Ok(())
            }
        }
    }

    fn channel_of(&self, id: SubscriptionId) -> &'static str {
        CHANNELS
            .iter()
            .zip(&self.subs)
            .find(|(_, sub)| **sub == Some(id))
            .map_or(CHANNELS[1], |(channel, _)| channel)
    }

    /// The server's data or close reaches the client after the latency,
    /// after what the server sent it before.
    fn arrive(&mut self, transport: TransportId, inbound: Inbound) {
        let at = self.now + self.latency();
        let last = self.server.last_arrival.entry(id(transport)).or_default();
        let at = at.max(*last);
        *last = at;
        self.scheduled += 1;
        self.agenda
            .insert((at, self.scheduled), Due::Arrive(transport, inbound));
    }

    /// The server ends its current connection.
    fn server_close(&mut self, code: Option<u16>, reason: &str) {
        if let Some(transport) = self.server.current.take() {
            self.arrive(transport, Inbound::Close(code, reason.to_owned()));
        }
    }

    /// Reads what waits on the socket while the client wants frames.
    fn pump(&mut self) -> Step {
        while self.machine.wants_frames() {
            let Some((transport, inbound)) = self.inbox.pop_front() else {
                break;
            };
            match inbound {
                Inbound::Frame(data) => self.feed(Input::Frame { transport, data })?,
                Inbound::Close(code, reason) => {
                    self.checker.open.remove(&id(transport));
                    self.feed(Input::TransportClosed {
                        transport,
                        code,
                        reason,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn take_next(&mut self, of: Of) -> Step {
        match self.streams.get_mut(&of).and_then(VecDeque::pop_front) {
            Some(event) => {
                self.delivered(event)?;
                self.feed(Input::EventTaken(event))
            }
            None => Ok(()),
        }
    }

    fn take_all(&mut self) -> Step {
        let open: Vec<Of> = self.streams.keys().copied().collect();
        for of in open {
            while self
                .streams
                .get(&of)
                .is_some_and(|untaken| !untaken.is_empty())
            {
                self.take_next(of)?;
            }
        }
        Ok(())
    }

    fn drop_stream(&mut self, of: Of) -> Step {
        let Some(untaken) = self.streams.remove(&of) else {
            return Ok(());
        };
        self.dropping = Some(of);
        for event in untaken {
            self.delivered(event)?;
            self.feed(Input::EventTaken(event))?;
        }
        self.dropping = None;
        self.feed(Input::StreamClosed(of))
    }

    /// Feeds `input` to the machine, performs its actions, and checks the
    /// invariants.
    fn feed(&mut self, input: Input) -> Step {
        self.trace.push(format!("in {input:?}"));
        let machine = &mut self.machine;
        let actions = catch_unwind(AssertUnwindSafe(|| machine.handle(input)))
            .map_err(|panic| self.violation("I2", panic_message(&panic)))?;
        for action in actions {
            self.trace.push(format!("out {action:?}"));
            self.act(action)?;
        }
        for event in std::mem::take(&mut self.taken_at_once) {
            self.delivered(event)?;
            self.feed(Input::EventTaken(event))?;
        }
        if self.reading {
            self.take_all()?;
        }
        self.forget_removed_streams();
        self.check()?;
        self.pump()
    }

    /// A removed subscription's stream ends once it is read.
    fn forget_removed_streams(&mut self) {
        let subs = self.subs;
        self.streams.retain(|of, untaken| {
            of.is_none_or(|id| subs.contains(&Some(id))) || !untaken.is_empty()
        });
    }

    fn act(&mut self, action: Action) -> Step {
        match action {
            Action::OpenTransport { transport, .. } => {
                if self.checker.disconnected {
                    return Err(self.violation("I5", "a transport opened after disconnect()"));
                }
                if !self.checker.open.insert(id(transport)) {
                    return Err(self.violation("I3", "a transport opened twice"));
                }
                let after = self.latency();
                let refused = !self.server.healed && self.rng.u32(0..10) == 0;
                if refused {
                    self.count("refuse");
                    self.schedule(after, Due::Refused(transport));
                } else {
                    self.schedule(after, Due::Opened(transport));
                }
            }
            Action::CloseTransport(transport) => {
                // The planted fault leaks a transport the client closed while
                // it still wants a connection: I3 reports it at the next one.
                if self.fault != Some(Fault::KeepClosedTransport) || self.checker.disconnected {
                    self.checker.open.remove(&id(transport));
                }
                self.inbox.retain(|(of, _)| *of != transport);
                if self.server.current == Some(transport) {
                    self.server.current = None;
                }
            }
            Action::Send {
                transport,
                id: frame_id,
                frame,
                ..
            } => {
                let after = self.latency();
                self.schedule(after, Due::Written(transport, frame_id));
                if self.server.current == Some(transport) {
                    let text = String::from_utf8(frame).expect("JSON frames");
                    for command in text.lines() {
                        self.answer(transport, command);
                    }
                }
            }
            Action::SetTimer(timer, delay) => {
                if self.checker.disconnected {
                    return Err(self.violation("I5", format!("{timer:?} set after disconnect()")));
                }
                self.cancel(Key::Client(timer));
                self.schedule(delay, Due::Timer(Key::Client(timer)));
            }
            Action::CancelTimer(timer) => {
                let kept =
                    self.fault == Some(Fault::KeepCancelledTimer) && self.checker.disconnected;
                if !kept {
                    self.cancel(Key::Client(timer));
                }
            }
            Action::SetCallTimer(call, timer, delay) => {
                self.cancel(Key::Call(call, timer));
                self.schedule(delay, Due::Timer(Key::Call(call, timer)));
            }
            Action::CancelCallTimer(call, timer) => self.cancel(Key::Call(call, timer)),
            Action::SetSubscriptionTimer(id, timer, delay) => {
                if self.checker.disconnected {
                    return Err(
                        self.violation("I5", format!("{id:?} {timer:?} set after disconnect()"))
                    );
                }
                self.cancel(Key::Subscription(id, timer));
                self.schedule(delay, Due::Timer(Key::Subscription(id, timer)));
            }
            Action::CancelSubscriptionTimer(id, timer) => {
                self.cancel(Key::Subscription(id, timer));
            }
            Action::CallSettled { call, result } => {
                if self.fault != Some(Fault::DropCallOutcome)
                    && let Some(made) = self.checker.calls.remove(&call)
                    && self.now.saturating_sub(made) > CALL_BOUND
                {
                    return Err(self.violation(
                        "I6",
                        format!("{call:?} made at {made:?} settled at {:?}", self.now),
                    ));
                }
                if matches!(&result, Err(error) if error.code == codes::error::TIMEOUT) {
                    self.count("call timeout");
                }
            }
            other => self.act_application(other)?,
        }
        Ok(())
    }

    /// The actions for the application: events and its callbacks.
    fn act_application(&mut self, action: Action) -> Step {
        match action {
            Action::Emit(event, client_event) => self.emitted(event, None, &client_event)?,
            Action::EmitSubscription(event, id, sub_event) => {
                self.emitted_subscription(event, id, &sub_event)?;
            }
            Action::Withdraw(events) => {
                for event in events {
                    if self.checker.events.remove(&event).is_none() {
                        return Err(self.violation(
                            "I9",
                            format!("{event:?} withdrawn, not emitted or already taken"),
                        ));
                    }
                    for untaken in self.streams.values_mut() {
                        untaken.retain(|of| *of != event);
                    }
                    self.taken_at_once.retain(|of| *of != event);
                    self.count("withdrawal");
                }
            }
            Action::FetchToken(fetch) => {
                if self.checker.disconnected {
                    return Err(self.violation("I5", "a token fetch after disconnect()"));
                }
                let result = if self.server.healed {
                    Ok("token".to_owned())
                } else {
                    match self.rng.u32(0..20) {
                        0 => {
                            self.count("token unauthorized");
                            Err(TokenError::Unauthorized)
                        }
                        1..3 => {
                            self.count("token failed");
                            Err(TokenError::Failed("chaos".to_owned()))
                        }
                        _ => Ok("token".to_owned()),
                    }
                };
                let after = Duration::from_millis(self.rng.u64(0..=50));
                self.schedule(after, Due::Token(fetch, result));
            }
            Action::FetchSubscriptionState {
                subscription,
                fetch,
                ..
            } => {
                if self.checker.disconnected {
                    return Err(self.violation("I5", "a state fetch after disconnect()"));
                }
                let loaded = self.server.healed || self.rng.u32(0..10) > 0;
                if !loaded {
                    self.count("state failed");
                }
                let after = Duration::from_millis(self.rng.u64(0..=50));
                self.schedule(after, Due::State(fetch, subscription, loaded));
            }
            Action::CancelSubscriptionState(fetch) => {
                self.agenda
                    .retain(|_, due| !matches!(due, Due::State(of, ..) if *of == fetch));
            }
            other => panic!("an action the runner does not expect: {other:?}"),
        }
        Ok(())
    }

    fn cancel(&mut self, key: Key) {
        self.agenda
            .retain(|_, due| !matches!(due, Due::Timer(of) if *of == key));
    }

    /// The current incarnation of the server-side subscription to `channel`.
    fn server_sub(&self, channel: &str) -> Sub {
        let channel = SERVER_CHANNELS
            .into_iter()
            .find(|known| *known == channel)
            .expect("a server-side channel of the runner");
        Sub::Server(
            channel,
            self.checker.generations.get(channel).copied().unwrap_or(0),
        )
    }

    /// Keeps `event` for the application: on its stream if open, delivered
    /// at once otherwise (Block 15).
    fn keep(&mut self, event: EventId, of: Of, tracked: Tracked) -> Step {
        self.checker.events.insert(event, tracked);
        if let Some(untaken) = self.streams.get_mut(&of) {
            untaken.push_back(event);
            Ok(())
        } else if self.dropping == Some(of) {
            self.taken_at_once.push(event);
            Ok(())
        } else {
            self.delivered(event)
        }
    }

    /// I1 for the client, and for server-side subscriptions; their events
    /// go to the client's stream.
    fn emitted(&mut self, event: EventId, of: Of, client_event: &ClientEvent) -> Step {
        let state = match client_event {
            ClientEvent::Connecting { .. } => State::Connecting,
            ClientEvent::Connected { .. } => State::Connected,
            ClientEvent::Disconnected { .. } => State::Disconnected,
            ClientEvent::ServerSubscription {
                channel,
                event: server_event,
            } => {
                return self.emitted_server(event, channel, server_event);
            }
            _ => {
                return self.keep(
                    event,
                    of,
                    Tracked {
                        sub: None,
                        kind: Kind::Other,
                    },
                );
            }
        };
        self.keep(
            event,
            of,
            Tracked {
                sub: None,
                kind: Kind::Other,
            },
        )?;
        if self.fault == Some(Fault::DropStateEvent) && state == State::Connected {
            return Ok(());
        }
        let last = self.checker.last_state.replace(state);
        if last == Some(state) {
            return Err(self.violation("I1", format!("{state:?} emitted twice in a row")));
        }
        if last == Some(State::Disconnected) && state == State::Connected {
            return Err(self.violation("I1", "disconnected went to connected"));
        }
        if last == Some(State::Connected) && state == State::Connecting {
            self.count("reconnect");
        }
        Ok(())
    }

    fn emitted_server(
        &mut self,
        event: EventId,
        channel: &str,
        server_event: &ServerSubscriptionEvent,
    ) -> Step {
        let sub = self.server_sub(channel);
        let (state, kind) = match server_event {
            ServerSubscriptionEvent::Subscribing => {
                (Some(SubscriptionState::Subscribing), Kind::Other)
            }
            ServerSubscriptionEvent::Subscribed(subscribed) => (
                Some(SubscriptionState::Subscribed),
                Kind::Subscribed {
                    was_recovering: subscribed.was_recovering,
                    recovered: subscribed.recovered,
                },
            ),
            ServerSubscriptionEvent::Unsubscribed { .. } => {
                (Some(SubscriptionState::Unsubscribed), Kind::Other)
            }
            ServerSubscriptionEvent::Publication(publication) => {
                (None, Kind::Publication(publication.offset))
            }
            _ => (None, Kind::Other),
        };
        self.keep(
            event,
            None,
            Tracked {
                sub: Some(sub),
                kind,
            },
        )?;
        let Some(state) = state else {
            return Ok(());
        };
        self.sub_state(sub, state, false)?;
        if state == SubscriptionState::Unsubscribed {
            // Unsubscribed server-side subscriptions leave the registry: a
            // later grant is a new one.
            let Sub::Server(channel, generation) = sub else {
                unreachable!("a server-side subscription");
            };
            self.checker.generations.insert(channel, generation + 1);
        }
        Ok(())
    }

    fn emitted_subscription(
        &mut self,
        event: EventId,
        id: SubscriptionId,
        sub_event: &SubscriptionEvent,
    ) -> Step {
        let sub = Sub::Client(id);
        let (state, kind) = match sub_event {
            SubscriptionEvent::Subscribing { .. } => {
                (Some(SubscriptionState::Subscribing), Kind::Other)
            }
            SubscriptionEvent::Subscribed(subscribed) => (
                Some(SubscriptionState::Subscribed),
                Kind::Subscribed {
                    was_recovering: subscribed.was_recovering,
                    recovered: subscribed.recovered,
                },
            ),
            SubscriptionEvent::Unsubscribed { .. } => {
                (Some(SubscriptionState::Unsubscribed), Kind::Other)
            }
            SubscriptionEvent::Publication(publication) => {
                (None, Kind::Publication(publication.offset))
            }
            _ => (None, Kind::Other),
        };
        self.keep(
            event,
            Some(id),
            Tracked {
                sub: Some(sub),
                kind,
            },
        )?;
        match state {
            Some(state) => self.sub_state(sub, state, true),
            None => Ok(()),
        }
    }

    /// I1 for a subscription: each state event differs from the last, and
    /// a client-side one never goes from unsubscribed to subscribed.
    fn sub_state(&mut self, sub: Sub, state: SubscriptionState, client_side: bool) -> Step {
        if self.fault == Some(Fault::DropSubscriptionStateEvent)
            && client_side
            && state == SubscriptionState::Subscribed
        {
            return Ok(());
        }
        let last = self.checker.sub_states.insert(sub, state);
        if last == Some(state) {
            return Err(self.violation("I1", format!("{sub:?} {state:?} twice in a row")));
        }
        if client_side
            && last == Some(SubscriptionState::Unsubscribed)
            && state == SubscriptionState::Subscribed
        {
            return Err(self.violation("I1", format!("{sub:?} unsubscribed went to subscribed")));
        }
        if last == Some(SubscriptionState::Subscribed) && state == SubscriptionState::Subscribing {
            self.count("resubscribe");
        }
        Ok(())
    }

    /// The application has `event`: I8 follows the offsets it delivers.
    fn delivered(&mut self, event: EventId) -> Step {
        let Some(tracked) = self.checker.events.remove(&event) else {
            return Ok(());
        };
        let Some(sub) = tracked.sub else {
            return Ok(());
        };
        match tracked.kind {
            Kind::Publication(Some(offset)) => {
                self.delivered_offset(&sub, offset)?;
                if self.fault == Some(Fault::DuplicatePublication) {
                    self.delivered_offset(&sub, offset)?;
                }
            }
            Kind::Subscribed {
                was_recovering,
                recovered,
            } => {
                if was_recovering {
                    self.count(if recovered {
                        "recovered true"
                    } else {
                        "recovered false"
                    });
                }
                if !recovered {
                    // The stream restarts: the server could not bring
                    // back what was missed.
                    self.checker.delivered.insert(sub, None);
                }
            }
            Kind::Publication(None) | Kind::Other => {}
        }
        Ok(())
    }

    fn delivered_offset(&mut self, sub: &Sub, offset: u64) -> Step {
        let last = self.checker.delivered.entry(*sub).or_default();
        if let Some(last) = *last
            && offset != last + 1
        {
            return Err(self.violation(
                "I8",
                format!("{sub:?} delivered offset {offset} after {last}"),
            ));
        }
        *last = Some(offset);
        Ok(())
    }

    /// The invariants checked after every step.
    fn check(&mut self) -> Step {
        if self.checker.open.len() > 1 {
            return Err(
                self.violation("I3", format!("{} transports open", self.checker.open.len()))
            );
        }
        if self.checker.disconnected {
            if !self.checker.open.is_empty() {
                return Err(self.violation("I5", "a transport open after disconnect()"));
            }
            // A resubscribe backoff armed before disconnect() may elapse, and
            // a subscription timer set since is reported where it is set
            // (I5, Block 10).
            let timers: Vec<&Due> = self
                .agenda
                .values()
                .filter(|due| matches!(due, Due::Timer(Key::Client(_))))
                .collect();
            if !timers.is_empty() {
                return Err(self.violation("I5", format!("{timers:?} run after disconnect()")));
            }
        }
        let client_taken = self.streams.get(&None).is_none_or(VecDeque::is_empty);
        if let Some(last) = self.checker.last_state
            && client_taken
            && self.machine.state() != last
        {
            return Err(self.violation(
                "I1",
                format!(
                    "state() is {:?}, the last state event {last:?}",
                    self.machine.state()
                ),
            ));
        }
        for id in self.subs.iter().flatten() {
            let taken = self.streams.get(&Some(*id)).is_none_or(VecDeque::is_empty);
            let last = self.checker.sub_states.get(&Sub::Client(*id));
            let state = self.machine.subscription_state(*id);
            if taken && last.is_some() && state.as_ref() != last {
                return Err(self.violation(
                    "I1",
                    format!("{id:?} state is {state:?}, the last state event {last:?}"),
                ));
            }
        }
        self.check_read_ahead()
    }

    /// I9: the client stops reading when its read-ahead is full.
    fn check_read_ahead(&mut self) -> Step {
        let (messages, bytes) = self.machine.read_ahead();
        let full = messages >= READ_AHEAD_MESSAGES || bytes >= READ_AHEAD_BYTES;
        let wants =
            self.machine.wants_frames() || (full && self.fault == Some(Fault::WantsFramesWhenFull));
        if full && wants {
            return Err(self.violation(
                "I9",
                format!("reading with {messages} messages and {bytes} bytes held"),
            ));
        }
        if !self.machine.wants_frames() {
            self.count("full read-ahead");
        }
        Ok(())
    }

    /// I6: every call settles within [`CALL_BOUND`].
    fn check_calls(&self) -> Step {
        match self
            .checker
            .calls
            .iter()
            .find(|(_, made)| self.now.saturating_sub(**made) > CALL_BOUND)
        {
            Some((call, made)) => Err(self.violation(
                "I6",
                format!("{call:?} made at {made:?} unsettled at {:?}", self.now),
            )),
            None => Ok(()),
        }
    }

    /// The simulated server answers a command of the client, or not.
    fn answer(&mut self, transport: TransportId, command: &str) {
        let command: Value = serde_json::from_str(command).expect("a JSON command");
        let Some(id) = command.get("id").and_then(Value::as_u64) else {
            // A pong, or a send: no reply.
            return;
        };
        if let Some(channel) = command["unsubscribe"]["channel"].as_str() {
            self.server.subscribed.retain(|known| *known != channel);
        }
        let connect = command.get("connect").is_some();
        if connect && self.server.healed && self.fault == Some(Fault::IgnoreConnectWhenHealed) {
            return;
        }
        let mut after = Duration::ZERO;
        if !self.server.healed {
            match self.rng.u32(0..100) {
                0..70 => self.count("answer"),
                70..80 => {
                    self.count("ignore");
                    return;
                }
                80..88 => {
                    self.count("late answer");
                    after = Duration::from_millis(self.rng.u64(1000..=3000));
                }
                88..94 => {
                    self.count("temporary error");
                    return self.arrive(transport, frame(&error(id, 100, true)));
                }
                94..98 => {
                    self.count("permanent error");
                    return self.arrive(transport, frame(&error(id, 103, false)));
                }
                _ => {
                    self.count("error 109");
                    return self.arrive(transport, frame(&error(id, 109, false)));
                }
            }
        }
        self.schedule(after, Due::Reply(transport, command));
    }

    /// The server's reply to `command`, as it sends it now.
    fn result(&mut self, transport: TransportId, command: &Value) -> Value {
        let id = &command["id"];
        let ttl = self.rng.u64(2..=5);
        if let Some(connect) = command.get("connect") {
            self.server.connected = true;
            self.server.pinging = true;
            self.schedule(Duration::from_secs(PING), Due::Ping(transport));
            let granted: Vec<&'static str> = self.server.granted.iter().copied().collect();
            let mut subs = serde_json::Map::new();
            for channel in granted {
                self.server.subscribed.insert(channel);
                let skip = self.fault == Some(Fault::SkipServerRecovered);
                let result = self.recovery(channel, &connect["subs"][channel], skip);
                subs.insert(channel.to_owned(), result);
            }
            return json!({"id": id, "connect": {
                "client": "c", "ping": PING, "pong": true, "expires": true, "ttl": ttl,
                "subs": subs,
            }});
        }
        if let Some(subscribe) = command.get("subscribe") {
            let channel = CHANNELS
                .into_iter()
                .find(|known| subscribe["channel"] == *known)
                .expect("a client-side channel of the runner");
            let reject = subscribe["flag"].as_i64().unwrap_or(0) & REJECT_UNRECOVERED != 0;
            if reject && !self.recoverable(channel, subscribe) {
                self.count("error 112");
                return error(id.as_u64().unwrap_or(0), 112, false);
            }
            self.server.subscribed.insert(channel);
            let skip = self.fault == Some(Fault::SkipRecovered);
            return json!({"id": id, "subscribe": self.recovery(channel, subscribe, skip)});
        }
        let result = if command.get("refresh").is_some() {
            json!({"refresh": {"expires": true, "ttl": ttl}})
        } else if command.get("unsubscribe").is_some() {
            json!({"unsubscribe": {}})
        } else if command.get("rpc").is_some() {
            json!({"rpc": {"data": {}}})
        } else if command.get("publish").is_some() {
            json!({"publish": {}})
        } else if command.get("history").is_some() {
            json!({"history": {"publications": [], "epoch": EPOCH, "offset": 0}})
        } else if command.get("presence").is_some() {
            json!({"presence": {"presence": {}}})
        } else {
            panic!("a command the simulated server does not know: {command}");
        };
        let mut reply = result;
        reply["id"] = id.clone();
        reply
    }

    /// Whether the server can bring back what a subscription asking to
    /// recover missed.
    fn recoverable(&mut self, channel: &'static str, request: &Value) -> bool {
        let stream = *self.server.streams.entry(channel).or_default();
        let from = request["offset"].as_u64().unwrap_or(0);
        request["epoch"] == EPOCH
            && from >= stream.oldest
            && from <= stream.top
            && stream.top - from <= RECOVERY_LIMIT
    }

    /// A subscribe result for `channel`, recovering when `request` asks:
    /// the publications after its offset, or `recovered` false when they
    /// are gone or too many.
    fn recovery(&mut self, channel: &'static str, request: &Value, skip: bool) -> Value {
        let top = self.server.streams.entry(channel).or_default().top;
        let mut result = json!({
            "recoverable": true, "positioned": true, "epoch": EPOCH, "offset": top,
        });
        if request["recover"] == true {
            let recovered = self.recoverable(channel, request);
            result["was_recovering"] = json!(true);
            result["recovered"] = json!(recovered);
            if recovered {
                let from = request["offset"].as_u64().unwrap_or(0);
                let mut offsets: Vec<u64> = (from + 1..=top).collect();
                if skip && offsets.len() >= 2 {
                    offsets.remove(0);
                }
                let publications: Vec<Value> = offsets
                    .into_iter()
                    .map(|offset| publication(offset, self.rng.usize(0..=600)))
                    .collect();
                result["publications"] = json!(publications);
            }
        }
        result
    }
}

fn frame(value: &Value) -> Inbound {
    Inbound::Frame(value.to_string().into_bytes())
}

fn publication(offset: u64, pad: usize) -> Value {
    json!({"data": {"n": offset, "pad": "x".repeat(pad)}, "offset": offset})
}

/// Whether `due` is something the server or the application's callbacks
/// sent, on its way to the client.
fn in_flight(due: &Due) -> bool {
    matches!(
        due,
        Due::Arrive(..)
            | Due::Reply(..)
            | Due::Opened(_)
            | Due::Refused(_)
            | Due::Token(..)
            | Due::State(..)
    )
}

fn error(id: u64, code: u32, temporary: bool) -> Value {
    json!({"id": id, "error": {"code": code, "message": "chaos", "temporary": temporary}})
}

/// A transport's number, to keep transports in ordered sets.
fn id(transport: TransportId) -> u64 {
    let text = format!("{transport:?}");
    text.trim_start_matches("TransportId(")
        .trim_end_matches(')')
        .parse()
        .expect("TransportId(n)")
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .unwrap_or_else(|| "a panic".to_owned())
}

/// The seeds to run: `FERRIFUGE_CHAOS_SEED` alone when it is set.
fn seeds_from(variable: Option<String>, default: Range<u64>) -> Range<u64> {
    match variable {
        Some(seed) => {
            let seed: u64 = seed.trim().parse().expect("a seed");
            seed..seed + 1
        }
        None => default,
    }
}

/// Runs `seeds`, stopping at the first violation; the counts of every run.
fn run_seeds(
    seeds: Range<u64>,
    ops: usize,
    fault: Option<Fault>,
    subscriptions: bool,
) -> Result<BTreeMap<String, usize>, Violation> {
    let mut counts = BTreeMap::new();
    for seed in seeds {
        let mut run = Run::new(seed, fault, subscriptions);
        run.run(ops)?;
        for (name, count) in run.counts {
            *counts.entry(name).or_default() += count;
        }
    }
    Ok(counts)
}

/// Fails on any of `expected` counted 0 times.
fn assert_counted(counts: &BTreeMap<String, usize>, expected: &[String]) {
    let missing: Vec<_> = expected
        .iter()
        .filter(|name| counts.get(*name).copied().unwrap_or(0) == 0)
        .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
}

/// The operations and server behaviours of the connection runner.
fn connection_counts() -> Vec<String> {
    let mut expected: Vec<String> = [
        Op::Connect,
        Op::Disconnect,
        Op::TakeNext(Target::Client),
        Op::Hold,
        Op::Read,
        Op::OpenStream(Target::Client),
        Op::DropStream(Target::Client),
        Op::Advance(Duration::ZERO),
        Op::Drop,
        Op::PingGap,
    ]
    .into_iter()
    .map(Op::name)
    .collect();
    for kind in [
        CallKind::Rpc,
        CallKind::Publish,
        CallKind::History,
        CallKind::Presence,
    ] {
        expected.push(Op::Call(kind).name());
    }
    for code in CODES {
        expected.push(Op::Close(code).name());
        expected.push(Op::Push(code).name());
    }
    expected.extend(
        [
            "refuse",
            "answer",
            "ignore",
            "late answer",
            "temporary error",
            "permanent error",
            "error 109",
            "token failed",
            "token unauthorized",
            "reconnect",
            "call timeout",
        ]
        .map(str::to_owned),
    );
    expected
}

#[test]
fn seeded_chaos_on_the_core_keeps_the_connection_invariants() {
    let seeds = seeds_from(std::env::var(SEED_VARIABLE).ok(), SEEDS);
    let counts = run_seeds(seeds.clone(), OPS, None, false)
        .unwrap_or_else(|violation| panic!("{violation}"));
    if seeds == SEEDS {
        assert_counted(&counts, &connection_counts());
    }
}

#[test]
fn a_seed_of_the_core_runner_reproduces_its_run() {
    let trace = |seed| {
        let mut run = Run::new(seed, None, false);
        run.run(OPS)
            .unwrap_or_else(|violation| panic!("{violation}"));
        run.trace
    };
    let operations = |trace: &[String]| -> Vec<String> {
        trace
            .iter()
            .filter(|line| line.starts_with("op "))
            .cloned()
            .collect()
    };

    let first = trace(7);
    let second = trace(7);
    let other = trace(8);

    assert_eq!(operations(&first).len(), OPS);
    assert!(first.iter().any(|line| line.starts_with("in ")));
    assert!(first.iter().any(|line| line.starts_with("out ")));
    assert_eq!(first, second);
    assert_ne!(operations(&first), operations(&other));
}

/// Runs `fault` over the pull-request seeds: the first violation must be
/// of `invariant`, and the same when its seed reruns alone.
fn assert_reported(fault: Fault, invariant: &str, subscriptions: bool) {
    let violation = run_seeds(SEEDS, OPS, Some(fault), subscriptions)
        .err()
        .unwrap_or_else(|| panic!("{fault:?} went unreported"));
    assert_eq!(violation.invariant, invariant, "{fault:?}: {violation}");

    let alone = seeds_from(Some(violation.seed.to_string()), SEEDS);
    let again = run_seeds(alone, OPS, Some(fault), subscriptions).err();

    assert_eq!(again.as_ref(), Some(&violation), "{fault:?}");
}

#[test]
fn the_core_runner_reports_a_planted_connection_fault() {
    for (fault, invariant) in [
        (Fault::DropStateEvent, "I1"),
        (Fault::KeepClosedTransport, "I3"),
        (Fault::IgnoreConnectWhenHealed, "I4"),
        (Fault::KeepCancelledTimer, "I5"),
        (Fault::DropCallOutcome, "I6"),
    ] {
        assert_reported(fault, invariant, false);
    }
}

#[test]
fn seeded_chaos_with_subscriptions_on_the_core_keeps_every_invariant() {
    let seeds = seeds_from(std::env::var(SEED_VARIABLE).ok(), SEEDS);
    let counts =
        run_seeds(seeds.clone(), OPS, None, true).unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let mut expected = connection_counts();
    expected.extend(
        [
            Op::TakeNext(Target::Subscription(0)),
            Op::OpenStream(Target::Subscription(0)),
            Op::DropStream(Target::Subscription(0)),
            Op::Add(0),
            Op::Subscribe(0),
            Op::Unsubscribe(0),
            Op::Remove(0),
            Op::Publish(Channel::Client(0), 1),
            Op::Publish(Channel::Server(0), 1),
            Op::Expire(Channel::Client(0)),
            Op::Expire(Channel::Server(0)),
            Op::ServerSubscribe(0),
            Op::ServerUnsubscribe(0),
        ]
        .into_iter()
        .map(Op::name),
    );
    for code in UNSUBSCRIBE_CODES {
        expected.push(Op::UnsubscribePush(0, code).name());
    }
    expected.extend(
        [
            "state failed",
            "error 112",
            "resubscribe",
            "recovered true",
            "recovered false",
            "full read-ahead",
            "withdrawal",
        ]
        .map(str::to_owned),
    );
    assert_counted(&counts, &expected);
}

#[test]
fn the_core_runner_reports_a_planted_delivery_fault() {
    for (fault, invariant) in [
        (Fault::SkipRecovered, "I8"),
        (Fault::SkipServerRecovered, "I8"),
        (Fault::DuplicatePublication, "I8"),
        (Fault::DropSubscriptionStateEvent, "I1"),
        (Fault::WantsFramesWhenFull, "I9"),
    ] {
        assert_reported(fault, invariant, true);
    }
}

/// Prints the whole trace of `FERRIFUGE_CHAOS_SEED` (seed 0 without it),
/// of `FERRIFUGE_CHAOS_OPS` operations (60 without it), with
/// subscriptions: `cargo test --test chaos trace -- --ignored --nocapture`.
#[test]
#[ignore = "a debugging aid"]
fn trace() {
    let seed = seeds_from(std::env::var(SEED_VARIABLE).ok(), 0..1).start;
    let ops = std::env::var(OPS_VARIABLE)
        .ok()
        .map_or(OPS, |value| value.trim().parse().expect("a number"));
    let mut run = Run::new(seed, None, true);
    let outcome = run.run(ops);
    println!("{}", run.trace.join("\n"));
    if let Err(violation) = outcome {
        println!("{violation}");
    }
}

/// The main and nightly run (Block 16, "CI"): `FERRIFUGE_CHAOS_SEEDS`
/// seeds (10 000 by default) of `FERRIFUGE_CHAOS_OPS` operations (200),
/// with subscriptions, from a start seed drawn per run, or
/// `FERRIFUGE_CHAOS_START`; `FERRIFUGE_CHAOS_SEED` alone when set, which
/// reruns a failure.
#[test]
#[ignore = "long: main and nightly, in release"]
fn seeded_chaos_long_run() {
    let number = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .map_or(default, |value| value.trim().parse().expect("a number"))
    };
    let start = number("FERRIFUGE_CHAOS_START", fastrand::u64(..u64::MAX / 2));
    let seeds = number("FERRIFUGE_CHAOS_SEEDS", 10_000);
    let ops = usize::try_from(number(OPS_VARIABLE, 200)).expect("a count");
    let seeds = seeds_from(std::env::var(SEED_VARIABLE).ok(), start..start + seeds);
    println!(
        "chaos: seeds {}..{} of {ops} operations",
        seeds.start, seeds.end
    );
    if let Err(violation) = run_seeds(seeds, ops, None, true) {
        panic!("{violation}");
    }
}
