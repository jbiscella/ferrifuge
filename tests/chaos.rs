//! Seeded chaos on the sans-IO core (Block 16, `@P`): random operations
//! against a server simulated here, on a simulated clock, with the
//! invariants checked after every step. A seed fixes every choice, so a
//! failure reruns alone with `FERRIFUGE_CHAOS_SEED=<seed>`.

// @P scenario: Seeded chaos on the core keeps the connection invariants
// @P scenario: A seed of the core runner reproduces its run
// @P scenario: The core runner reports a planted connection fault

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use ferrifuge::machine::{
    Action, CallId, CallTimer, EventId, FetchId, FrameId, Input, Machine, Timer, TransportId,
};
use ferrifuge::{CallRequest, ClientEvent, Config, HistoryOptions, State, TokenError, codes};
use serde_json::Value;

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

/// What the application does next (Block 16, "Operations").
#[derive(Debug, Clone, Copy)]
enum Op {
    Connect,
    Disconnect,
    Call(CallKind),
    TakeNext,
    Hold,
    Read,
    OpenStream,
    DropStream,
    Advance(Duration),
    /// The server closes the connection with a close code.
    Close(u16),
    /// The server sends a Disconnect push, then closes.
    Push(u16),
    /// The connection drops without a close code.
    Drop,
    /// The server stops pinging this connection.
    PingGap,
}

#[derive(Debug, Clone, Copy)]
enum CallKind {
    Rpc,
    Publish,
    History,
    Presence,
}

/// Close and Disconnect push codes: reconnect-advised (3001, 4000, 3008
/// "slow", 3014 "state invalidated") and terminal (3500, 4500).
const CODES: [u16; 6] = [3001, 4000, 3008, 3014, 3500, 4500];

impl Op {
    fn pick(rng: &mut fastrand::Rng) -> Self {
        match rng.u32(0..100) {
            0..8 => Self::Connect,
            8..13 => Self::Disconnect,
            13..28 => Self::Call(match rng.u32(0..4) {
                0 => CallKind::Rpc,
                1 => CallKind::Publish,
                2 => CallKind::History,
                _ => CallKind::Presence,
            }),
            28..38 => Self::TakeNext,
            38..43 => Self::Hold,
            43..50 => Self::Read,
            50..55 => Self::OpenStream,
            55..59 => Self::DropStream,
            59..80 => Self::Advance(Duration::from_millis(rng.u64(100..=2000))),
            80..88 => Self::Close(CODES[rng.usize(..CODES.len())]),
            88..93 => Self::Push(CODES[rng.usize(..CODES.len())]),
            93..97 => Self::Drop,
            _ => Self::PingGap,
        }
    }

    /// Its name in the run counters.
    fn name(self) -> String {
        match self {
            Self::Call(kind) => format!("call {kind:?}"),
            Self::Advance(_) => "advance".to_owned(),
            Self::Close(code) => format!("close {code}"),
            Self::Push(code) => format!("push {code}"),
            other => format!("{other:?}"),
        }
    }
}

/// Something the simulated world does at a point of simulated time.
#[derive(Debug)]
enum Due {
    Timer(Key),
    /// Data or a close reaches the client's socket.
    Arrive(TransportId, Inbound),
    Opened(TransportId),
    Refused(TransportId),
    Written(TransportId, FrameId),
    Token(FetchId, Result<String, TokenError>),
    /// The server pings the connection, if it still pings it.
    Ping(TransportId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    Client(Timer),
    Call(CallId, CallTimer),
}

#[derive(Debug)]
enum Inbound {
    Frame(Vec<u8>),
    Close(Option<u16>, String),
}

/// The simulated server's side of a connection.
struct Server {
    /// The transport the server accepted last.
    current: Option<TransportId>,
    /// Whether it pings `current`.
    pinging: bool,
    /// Faults are over: the server answers every command at once.
    healed: bool,
}

/// What the checker has seen.
#[derive(Default)]
struct Checker {
    /// The last client state event emitted (I1).
    last_state: Option<State>,
    /// Transports opened and not yet closed (I3).
    open: BTreeSet<u64>,
    /// `disconnect()` was called, and `connect()` not since (I5).
    disconnected: bool,
    /// Calls not yet settled, with when they were made (I6).
    calls: BTreeMap<CallId, Duration>,
}

/// One seed's run.
struct Run {
    seed: u64,
    fault: Option<Fault>,
    rng: fastrand::Rng,
    machine: Machine,
    now: Duration,
    /// Simulated events by time, then by when they were scheduled.
    agenda: BTreeMap<(Duration, u64), Due>,
    scheduled: u64,
    /// What reached the client's socket and waits to be read.
    inbox: VecDeque<(TransportId, Inbound)>,
    server: Server,
    /// The application's stream of client events, if open, and the events
    /// it has not taken.
    stream: Option<VecDeque<EventId>>,
    /// The application takes every event as it comes.
    reading: bool,
    /// The stream is being dropped: events emitted meanwhile count as
    /// taken at once, as the adapter's outbox reports them.
    dropping: bool,
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
    fn new(seed: u64, fault: Option<Fault>) -> Self {
        Self {
            seed,
            fault,
            rng: fastrand::Rng::with_seed(seed ^ 0x5eed_c4a0_5000_0016),
            machine: Machine::with_token_source(chaos_config()).with_seed(seed),
            now: Duration::ZERO,
            agenda: BTreeMap::new(),
            scheduled: 0,
            inbox: VecDeque::new(),
            server: Server {
                current: None,
                pinging: true,
                healed: false,
            },
            stream: None,
            reading: false,
            dropping: false,
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
            let next = Op::pick(&mut self.rng);
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
            Op::TakeNext => match self.stream.as_mut().and_then(VecDeque::pop_front) {
                Some(event) => self.feed(Input::EventTaken(event)),
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
            Op::OpenStream if self.stream.is_none() => {
                self.stream = Some(VecDeque::new());
                self.feed(Input::StreamOpened(None))
            }
            Op::DropStream => match self.stream.take() {
                Some(untaken) => {
                    self.dropping = true;
                    for event in untaken {
                        self.feed(Input::EventTaken(event))?;
                    }
                    self.dropping = false;
                    self.feed(Input::StreamClosed(None))
                }
                None => Ok(()),
            },
            Op::OpenStream => Ok(()),
            Op::Advance(by) => self.advance(by),
            Op::Close(code) => {
                self.server_close(Some(code), "chaos");
                Ok(())
            }
            Op::Push(code) => {
                if let Some(transport) = self.server.current {
                    let push = format!(
                        r#"{{"push":{{"disconnect":{{"code":{code},"reason":"chaos"}}}}}}"#
                    );
                    self.arrive(transport, Inbound::Frame(push.into_bytes()));
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
        }
    }

    /// Faults stop, the application reads every event and wants to be
    /// connected: within [`LIVENESS`] the client is connected (I4), and the
    /// calls made settle (I6). The last fault is over once what the server
    /// and the token callback sent before healing has arrived.
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
        while !self.live() {
            let Some(&(at, _)) = self.agenda.keys().next() else {
                break;
            };
            if at > deadline {
                break;
            }
            self.advance(at.saturating_sub(self.now))?;
        }
        if !self.live() {
            return Err(self.violation(
                "I4",
                format!("{:?} {LIVENESS:?} after healing", self.machine.state()),
            ));
        }
        self.advance(CALL_BOUND)
    }

    fn live(&self) -> bool {
        self.machine.state() == State::Connected
            && self.stream.as_ref().is_none_or(VecDeque::is_empty)
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
            Due::Arrive(transport, inbound) => {
                self.inbox.push_back((transport, inbound));
                self.pump()
            }
            Due::Opened(transport) => {
                self.server.current = Some(transport);
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
            Due::Ping(transport) => {
                if self.server.current == Some(transport) && self.server.pinging {
                    self.arrive(transport, Inbound::Frame(b"{}".to_vec()));
                    self.schedule(Duration::from_secs(PING), Due::Ping(transport));
                }
                Ok(())
            }
        }
    }

    /// The server's data or close reaches the client after the latency.
    fn arrive(&mut self, transport: TransportId, inbound: Inbound) {
        let after = self.latency();
        self.schedule(after, Due::Arrive(transport, inbound));
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

    fn take_all(&mut self) -> Step {
        while let Some(event) = self.stream.as_mut().and_then(VecDeque::pop_front) {
            self.feed(Input::EventTaken(event))?;
        }
        Ok(())
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
            self.feed(Input::EventTaken(event))?;
        }
        if self.reading {
            self.take_all()?;
        }
        self.check()?;
        self.pump()
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
                if self.fault != Some(Fault::KeepClosedTransport) {
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
            Action::Emit(event, client_event) => self.emitted(event, &client_event)?,
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
            other => panic!("a subscription action without subscriptions: {other:?}"),
        }
        Ok(())
    }

    fn cancel(&mut self, key: Key) {
        self.agenda
            .retain(|_, due| !matches!(due, Due::Timer(of) if *of == key));
    }

    /// I1: each client state event differs from the last, and never goes
    /// from disconnected to connected.
    fn emitted(&mut self, event: EventId, client_event: &ClientEvent) -> Step {
        if let Some(stream) = &mut self.stream {
            stream.push_back(event);
        } else if self.dropping {
            self.taken_at_once.push(event);
        }
        let state = match client_event {
            ClientEvent::Connecting { .. } => State::Connecting,
            ClientEvent::Connected { .. } => State::Connected,
            ClientEvent::Disconnected { .. } => State::Disconnected,
            _ => return Ok(()),
        };
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

    /// The invariants checked after every step.
    fn check(&self) -> Step {
        if self.checker.open.len() > 1 {
            return Err(
                self.violation("I3", format!("{} transports open", self.checker.open.len()))
            );
        }
        if self.checker.disconnected {
            if !self.checker.open.is_empty() {
                return Err(self.violation("I5", "a transport open after disconnect()"));
            }
            if self
                .agenda
                .values()
                .any(|due| matches!(due, Due::Timer(Key::Client(_))))
            {
                return Err(self.violation("I5", "a client timer runs after disconnect()"));
            }
        }
        let all_taken = self.stream.as_ref().is_none_or(VecDeque::is_empty);
        if let Some(last) = self.checker.last_state
            && all_taken
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
        let connect = command.get("connect").is_some();
        if connect && self.server.healed && self.fault == Some(Fault::IgnoreConnectWhenHealed) {
            return;
        }
        let mut after = self.latency();
        let reply = if self.server.healed {
            self.result(id, &command)
        } else {
            match self.rng.u32(0..100) {
                0..70 => {
                    self.count("answer");
                    self.result(id, &command)
                }
                70..80 => {
                    self.count("ignore");
                    return;
                }
                80..88 => {
                    self.count("late answer");
                    after += Duration::from_millis(self.rng.u64(1000..=3000));
                    self.result(id, &command)
                }
                88..94 => {
                    self.count("temporary error");
                    error(id, 100, true)
                }
                94..98 => {
                    self.count("permanent error");
                    error(id, 103, false)
                }
                _ => {
                    self.count("error 109");
                    error(id, 109, false)
                }
            }
        };
        if connect && reply.contains(r#""connect""#) {
            self.server.pinging = true;
            self.schedule(after + Duration::from_secs(PING), Due::Ping(transport));
        }
        self.scheduled += 1;
        self.agenda.insert(
            (self.now + after, self.scheduled),
            Due::Arrive(transport, Inbound::Frame(reply.into_bytes())),
        );
    }

    fn result(&mut self, id: u64, command: &Value) -> String {
        let ttl = self.rng.u64(2..=5);
        let result = if command.get("connect").is_some() {
            format!(
                r#""connect":{{"client":"c","ping":{PING},"pong":true,"expires":true,"ttl":{ttl}}}"#
            )
        } else if command.get("refresh").is_some() {
            format!(r#""refresh":{{"expires":true,"ttl":{ttl}}}"#)
        } else if command.get("rpc").is_some() {
            r#""rpc":{"data":{}}"#.to_owned()
        } else if command.get("publish").is_some() {
            r#""publish":{}"#.to_owned()
        } else if command.get("history").is_some() {
            r#""history":{"publications":[],"epoch":"e","offset":0}"#.to_owned()
        } else if command.get("presence").is_some() {
            r#""presence":{"presence":{}}"#.to_owned()
        } else {
            panic!("a command the simulated server does not know: {command}");
        };
        format!(r#"{{"id":{id},{result}}}"#)
    }
}

/// Whether `due` is something the server or the token callback sent, on
/// its way to the client.
fn in_flight(due: &Due) -> bool {
    matches!(
        due,
        Due::Arrive(..) | Due::Opened(_) | Due::Refused(_) | Due::Token(..)
    )
}

fn error(id: u64, code: u32, temporary: bool) -> String {
    format!(r#"{{"id":{id},"error":{{"code":{code},"message":"chaos","temporary":{temporary}}}}}"#)
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
) -> Result<BTreeMap<String, usize>, Violation> {
    let mut counts = BTreeMap::new();
    for seed in seeds {
        let mut run = Run::new(seed, fault);
        run.run(ops)?;
        for (name, count) in run.counts {
            *counts.entry(name).or_default() += count;
        }
    }
    Ok(counts)
}

#[test]
fn seeded_chaos_on_the_core_keeps_the_connection_invariants() {
    let seeds = seeds_from(std::env::var(SEED_VARIABLE).ok(), SEEDS);
    let counts =
        run_seeds(seeds.clone(), OPS, None).unwrap_or_else(|violation| panic!("{violation}"));
    if seeds != SEEDS {
        return;
    }
    let mut expected: Vec<String> = [
        Op::Connect,
        Op::Disconnect,
        Op::TakeNext,
        Op::Hold,
        Op::Read,
        Op::OpenStream,
        Op::DropStream,
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
    let missing: Vec<_> = expected
        .iter()
        .filter(|name| counts.get(*name).copied().unwrap_or(0) == 0)
        .collect();
    assert!(
        missing.is_empty(),
        "counted 0 times: {missing:?}; counts {counts:?}"
    );
}

#[test]
fn a_seed_of_the_core_runner_reproduces_its_run() {
    let trace = |seed| {
        let mut run = Run::new(seed, None);
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

#[test]
fn the_core_runner_reports_a_planted_connection_fault() {
    for (fault, invariant) in [
        (Fault::DropStateEvent, "I1"),
        (Fault::KeepClosedTransport, "I3"),
        (Fault::IgnoreConnectWhenHealed, "I4"),
        (Fault::KeepCancelledTimer, "I5"),
        (Fault::DropCallOutcome, "I6"),
    ] {
        let violation = run_seeds(SEEDS, OPS, Some(fault))
            .err()
            .unwrap_or_else(|| panic!("{fault:?} went unreported"));
        assert_eq!(violation.invariant, invariant, "{fault:?}: {violation}");

        let alone = seeds_from(Some(violation.seed.to_string()), SEEDS);
        let again = run_seeds(alone, OPS, Some(fault)).err();

        assert_eq!(again.as_ref(), Some(&violation), "{fault:?}");
    }
}

/// Prints the whole trace of `FERRIFUGE_CHAOS_SEED` (seed 0 without it),
/// of `FERRIFUGE_CHAOS_OPS` operations (60 without it):
/// `cargo test --test chaos trace -- --ignored --nocapture`.
#[test]
#[ignore = "a debugging aid"]
fn trace() {
    let seed = seeds_from(std::env::var(SEED_VARIABLE).ok(), 0..1).start;
    let ops = std::env::var(OPS_VARIABLE)
        .ok()
        .map_or(OPS, |value| value.trim().parse().expect("a number"));
    let mut run = Run::new(seed, None);
    let outcome = run.run(ops);
    println!("{}", run.trace.join("\n"));
    if let Err(violation) = outcome {
        println!("{violation}");
    }
}

/// The main and nightly run (Block 16, "CI"): `FERRIFUGE_CHAOS_SEEDS`
/// seeds (10 000 by default) of `FERRIFUGE_CHAOS_OPS` operations (200),
/// from a start seed drawn per run, or `FERRIFUGE_CHAOS_START`;
/// `FERRIFUGE_CHAOS_SEED` alone when set, which reruns a failure.
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
    if let Err(violation) = run_seeds(seeds, ops, None) {
        panic!("{violation}");
    }
}
