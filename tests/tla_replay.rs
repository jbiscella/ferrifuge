//! Replays the traces centrifugal/centrifuge-rust generated from the
//! Centrifugal SDK TLA+ model into the sans-IO core, step by step (Block 42,
//! `@P`). The traces are data only, copied with their MIT notice under
//! `tests/fixtures/tla-traces/`; nothing else of that repository is read.
//!
//! A trace is one JSON object per line: its `constants`, then `states`, of
//! which the first holds every variable and each later one its step's `act`
//! and the variables it changed. Each step with an input is fed to a
//! `Machine`, and the model's whole state after it, with the steps that have
//! no input of their own (`Sdk_StartFlow`, `Sdk_SSSubscribed`), is compared
//! with what the core did and holds (D3, D4). A trace reaching a recorded
//! divergence (D5) is checked up to it and must differ there.

// @P scenario: Every model trace replays into the core up to its recorded divergence
// @P scenario: The replayer reports a planted mismatch

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ferrifuge::machine::{
    Action, FetchId, Input, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer, Timer,
    TransportId, TransportKind,
};
use ferrifuge::{
    CallRequest, ClientEvent, Config, DeltaFormat, ServerSubscriptionEvent, State, StateError,
    StreamPosition, SubscriptionEvent, SubscriptionState, TokenError,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Where the copied traces and their README are.
fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tla-traces")
}

/// Traces each recorded divergence reaches (Block 42, D5).
const RECORDED: [(Divergence, usize); 4] = [
    (Divergence::Dv1, 70),
    (Divergence::Dv2, 3),
    (Divergence::Dv3, 16),
    (Divergence::Dv4, 5),
];

/// Traces that reach no recorded divergence.
const CLEAN: usize = 577;

/// Every act of the model the replayer maps (D3).
const ACTS: [&str; 36] = [
    "App_Connect",
    "App_Disconnect",
    "App_Close",
    "App_Subscribe",
    "App_Unsubscribe",
    "App_Replace",
    "App_Call",
    "Env_CallTimeout",
    "Env_Open",
    "Env_Drop",
    "Env_WriteError",
    "Env_Timeout",
    "Env_TokenExpires",
    "Cb_ConnToken",
    "Cb_SubToken",
    "Cb_GetState",
    "Sdk_ReconnectTimer",
    "Sdk_ResubscribeTimer",
    "Sdk_ConnectReply",
    "Sdk_SubscribeReply",
    "Sdk_UnsubscribeReply",
    "Sdk_UnsubscribePush",
    "Sdk_DisconnectPush",
    "Sdk_Publication",
    "Sdk_SSDeliver",
    "Sdk_StartFlow",
    "Sdk_SSSubscribed",
    "Srv_Publish",
    "Srv_Connect",
    "Srv_Subscribe",
    "Srv_Trim",
    "Srv_UnsubPush",
    "Srv_Disconnect",
    "Srv_NewEpoch",
    "Srv_Lost",
    "Srv_Unsubscribe",
];

/// Acts that occur only past a recorded divergence, so are never
/// replayed: the live publication of `MC_not_recoverable.jsonl` line 29 and
/// the recovered server-side ones of `MC_large.jsonl` line 37 are in DV1
/// traces (D7).
const PAST_A_DIVERGENCE: [&str; 2] = ["Sdk_Publication", "Sdk_SSDeliver"];

/// A schedule of the model that differs from a recorded Ferrifuge decision
/// (Block 42, D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Divergence {
    /// Optimistic subscribes in the connect frame (Block 36, D5).
    Dv1,
    /// An unsubscribe waiting for the subscribe's reply over websocket
    /// (Block 9).
    Dv2,
    /// A 2502 drops the position (Block 11).
    Dv3,
    /// A failed emulation request of a subscribe reconnects (Block 8).
    Dv4,
}

impl Divergence {
    /// The value kind that must differ at the step (D5).
    fn kind(self) -> Kind {
        match self {
            Self::Dv1 | Self::Dv2 | Self::Dv3 => Kind::Commands,
            Self::Dv4 => Kind::Client,
        }
    }
}

/// What D4 compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Client,
    Subscriptions,
    ServerSubscriptions,
    Commands,
    Callbacks,
    Timers,
    Position,
}

impl Kind {
    const ALL: [Self; 7] = [
        Self::Client,
        Self::Subscriptions,
        Self::ServerSubscriptions,
        Self::Commands,
        Self::Callbacks,
        Self::Timers,
        Self::Position,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Client => "the client's state",
            Self::Subscriptions => "the subscription's state",
            Self::ServerSubscriptions => "the server-side subscription's state",
            Self::Commands => "the commands sent",
            Self::Callbacks => "the callbacks started",
            Self::Timers => "the timers",
            Self::Position => "the application's position",
        }
    }
}

/// A trace of the model, its states made whole.
#[derive(Clone)]
struct Trace {
    config: String,
    /// 1-based line of the trace in its file.
    line: usize,
    constants: Value,
    /// The step names, `Init` first.
    acts: Vec<String>,
    /// The whole state after each step.
    states: Vec<Value>,
}

impl Trace {
    fn parse(line: usize, text: &str) -> Self {
        let value: Value = serde_json::from_str(text).expect("a trace is a JSON object");
        let mut acts = Vec::new();
        let mut states = Vec::new();
        let mut whole = serde_json::Map::new();
        for step in value["states"].as_array().expect("states") {
            let step = step.as_object().expect("a state is an object");
            for (variable, value) in step {
                if variable != "act" {
                    whole.insert(variable.clone(), value.clone());
                }
            }
            acts.push(step["act"].as_str().expect("an act").to_owned());
            states.push(Value::Object(whole.clone()));
        }
        Self {
            config: value["config"].as_str().unwrap_or_default().to_owned(),
            line,
            constants: value["constants"].clone(),
            acts,
            states,
        }
    }

    fn ordered(&self) -> bool {
        self.constants["Ordered"] == true
    }

    fn constant(&self, name: &str) -> bool {
        self.constants[name] == true
    }

    fn names(&self, name: &str) -> Vec<String> {
        self.constants[name]
            .as_array()
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| name.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The first step reaching a recorded divergence, and which.
    fn divergence(&self) -> Option<(usize, Divergence)> {
        self.hits().into_iter().min()
    }

    /// Every recorded divergence the trace reaches.
    fn divergences(&self) -> BTreeSet<Divergence> {
        self.hits()
            .into_iter()
            .map(|(_, divergence)| divergence)
            .collect()
    }

    /// Each step reaching a recorded divergence, with the divergence.
    fn hits(&self) -> Vec<(usize, Divergence)> {
        let mut found = Vec::new();
        let mut keep = |step: usize, divergence: Divergence| found.push((step, divergence));
        let mut invalidated = BTreeSet::new();
        for step in 1..self.acts.len() {
            let before = &self.states[step - 1];
            let after = &self.states[step];
            let act = self.acts[step].as_str();
            let new = new_commands(before, after);
            match act {
                "Env_Open" if self.ordered() => {
                    let optimistic = channels(after, "sub").into_iter().any(|channel| {
                        let sub = &after["sub"][&channel];
                        sub["st"] == "subscribing"
                            && sub["tmr"] == false
                            && sub["unw"] == 0
                            && (!self.constant("SubToken") || sub["tok"].as_i64() > Some(0))
                            && !self.constant("GetState")
                    });
                    if optimistic {
                        keep(step, Divergence::Dv1);
                    }
                }
                "App_Unsubscribe" | "App_Replace"
                    if self.ordered()
                        && new.iter().any(|command| command["k"] == "unsub")
                        && list(before, "pend")
                            .iter()
                            .any(|pending| pending["k"] == "sub") =>
                {
                    keep(step, Divergence::Dv2);
                }
                "Sdk_UnsubscribePush" => {
                    let message = &list(before, "toC")[0];
                    if message["code"] == "invalidate" {
                        invalidated.insert(message["ch"].as_str().unwrap_or_default().to_owned());
                    }
                }
                "Env_WriteError" if !changed(before, after, "cl") => keep(step, Divergence::Dv4),
                _ => {}
            }
            if act == "Sdk_StartFlow" {
                for command in &new {
                    let channel = command["ch"].as_str().unwrap_or_default();
                    if command["k"] == "sub"
                        && command["rec"] == true
                        && command["ep"] == 0
                        && invalidated.contains(channel)
                    {
                        keep(step, Divergence::Dv3);
                    }
                }
            }
        }
        found
    }
}

/// The traces of every configuration, in file and line order.
fn traces() -> Vec<(String, Vec<Trace>)> {
    let dir = fixtures().join("upstream/traces");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the copied traces")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let text = std::fs::read_to_string(&path).expect("a trace file");
            let traces = text
                .lines()
                .enumerate()
                .map(|(index, line)| Trace::parse(index + 1, line))
                .collect();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, traces)
        })
        .collect()
}

fn list(state: &Value, variable: &str) -> Vec<Value> {
    state[variable].as_array().cloned().unwrap_or_default()
}

/// The channels of a variable keyed by channel; `[]` stands for none.
fn channels(state: &Value, variable: &str) -> Vec<String> {
    state[variable]
        .as_object()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

fn changed(before: &Value, after: &Value, variable: &str) -> bool {
    before[variable] != after[variable]
}

/// The commands the model sent between `before` and `after`: those of
/// `toS` with an id `before` did not hold.
fn new_commands(before: &Value, after: &Value) -> Vec<Value> {
    let known: BTreeSet<i64> = list(before, "toS")
        .iter()
        .filter_map(|command| command["id"].as_i64())
        .collect();
    list(after, "toS")
        .into_iter()
        .filter(|command| !command["id"].as_i64().is_some_and(|id| known.contains(&id)))
        .collect()
}

/// A command as both sides are compared: the fields of the model's `toS`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sent {
    kind: String,
    channel: String,
    token: i64,
    recover: bool,
    epoch: i64,
    offset: i64,
    reject: bool,
    /// Server-side subscriptions recovered in a connect: channel, epoch,
    /// offset.
    positions: BTreeMap<String, (i64, i64)>,
}

impl Sent {
    fn of_model(command: &Value) -> Self {
        let mut positions = BTreeMap::new();
        if let Some(map) = command["pos"].as_object() {
            for (channel, position) in map {
                if position["on"] == true {
                    positions.insert(
                        channel.clone(),
                        (
                            position["ep"].as_i64().unwrap_or(-1),
                            position["off"].as_i64().unwrap_or(0),
                        ),
                    );
                }
            }
        }
        Self {
            kind: command["k"].as_str().unwrap_or_default().to_owned(),
            channel: command["ch"].as_str().unwrap_or_default().to_owned(),
            token: command["tok"].as_i64().unwrap_or(0),
            recover: command["rec"] == true,
            epoch: if command["rec"] == true {
                command["ep"].as_i64().unwrap_or(-1)
            } else {
                -1
            },
            offset: if command["rec"] == true {
                command["off"].as_i64().unwrap_or(0)
            } else {
                0
            },
            reject: command["rej"] == true,
            positions,
        }
    }

    /// A command of the core's JSON frame, and its id.
    fn of_core(command: &Value) -> (u64, Self) {
        let id = command["id"].as_u64().unwrap_or(0);
        let (kind, body) = if let Some(body) = command.get("connect") {
            ("connect", body)
        } else if let Some(body) = command.get("subscribe") {
            ("sub", body)
        } else if let Some(body) = command.get("unsubscribe") {
            ("unsub", body)
        } else {
            let kind = command
                .as_object()
                .and_then(|map| map.keys().find(|key| *key != "id").cloned())
                .unwrap_or_else(|| "pong".to_owned());
            return (
                id,
                Self {
                    kind,
                    channel: String::new(),
                    token: 0,
                    recover: false,
                    epoch: -1,
                    offset: 0,
                    reject: false,
                    positions: BTreeMap::new(),
                },
            );
        };
        let recover = body["recover"] == true;
        let mut positions = BTreeMap::new();
        if let Some(map) = body["subs"].as_object() {
            for (channel, position) in map {
                if position["recover"] == true {
                    positions.insert(
                        channel.clone(),
                        (
                            epoch_number(position["epoch"].as_str()),
                            position["offset"].as_i64().unwrap_or(0),
                        ),
                    );
                }
            }
        }
        (
            id,
            Self {
                kind: kind.to_owned(),
                channel: body["channel"].as_str().unwrap_or_default().to_owned(),
                token: token_number(body["token"].as_str()),
                recover,
                epoch: if recover {
                    epoch_number(body["epoch"].as_str())
                } else {
                    -1
                },
                offset: if recover {
                    body["offset"].as_i64().unwrap_or(0)
                } else {
                    0
                },
                reject: body["flag"].as_i64().unwrap_or(0) & 2 != 0,
                positions,
            },
        )
    }
}

/// Token version n is the token `t<n>`; 0 is none (D4).
fn token(version: i64) -> String {
    if version > 0 {
        format!("t{version}")
    } else {
        String::new()
    }
}

fn token_number(token: Option<&str>) -> i64 {
    token
        .and_then(|token| token.strip_prefix('t'))
        .and_then(|number| number.parse().ok())
        .unwrap_or(0)
}

/// Epoch -1 is none, 0 the sentinel `_`, n is `e<n>` (D4).
fn epoch(number: i64) -> Option<String> {
    match number {
        ..0 => None,
        0 => Some("_".to_owned()),
        n => Some(format!("e{n}")),
    }
}

fn epoch_number(epoch: Option<&str>) -> i64 {
    match epoch {
        None | Some("") => -1,
        Some("_") => 0,
        Some(epoch) => epoch
            .strip_prefix('e')
            .and_then(|number| number.parse().ok())
            .unwrap_or(-1),
    }
}

/// Abstract codes of the model (D3).
fn error_code(code: &str) -> (u32, bool) {
    match code {
        "temp" => (100, true),
        "perm" => (403, false),
        "expired" => (109, false),
        "already" => (105, false),
        "unrec" => (112, false),
        other => panic!("an error code without a mapping: {other}"),
    }
}

fn unsubscribe_code(code: &str) -> u32 {
    match code {
        "terminal" => 2000,
        "resub" => 2500,
        "invalidate" => 2502,
        other => panic!("an unsubscribe code without a mapping: {other}"),
    }
}

fn disconnect_code(code: &str) -> u32 {
    match code {
        "reconnect" => 3001,
        "terminal" => 3500,
        "invalidate" => 3014,
        other => panic!("a disconnect code without a mapping: {other}"),
    }
}

/// A subscribe result of the model as Centrifugo sends it.
fn result_of(result: &Value) -> Value {
    let mut reply = json!({
        "recoverable": result["rec"] == true,
        "was_recovering": result["was"] == true,
        "recovered": result["rcv"] == true,
    });
    if let Some(epoch) = epoch(result["ep"].as_i64().unwrap_or(-1)) {
        reply["positioned"] = json!(true);
        reply["epoch"] = json!(epoch);
        reply["offset"] = json!(result["off"].as_i64().unwrap_or(0));
    }
    let (low, high) = (
        result["lo"].as_i64().unwrap_or(1),
        result["hi"].as_i64().unwrap_or(0),
    );
    if high >= low {
        let publications: Vec<Value> = (low..=high)
            .map(|offset| json!({"data": {}, "offset": offset}))
            .collect();
        reply["publications"] = json!(publications);
    }
    if result["delta"] == true {
        reply["delta"] = json!(true);
    }
    reply
}

/// A step's input could not be built, or a value differs.
#[derive(Debug)]
struct Mismatch {
    step: usize,
    kind: Option<Kind>,
    text: String,
}

/// Counts of a run (D8).
#[derive(Default)]
struct Counts {
    acts: BTreeMap<String, usize>,
    values: BTreeMap<Kind, usize>,
}

/// One trace replayed into one machine.
struct Replay<'a> {
    trace: &'a Trace,
    machine: Machine,
    /// The client-side subscription of each channel.
    ids: BTreeMap<String, SubscriptionId>,
    transport: Option<TransportId>,
    /// The connect of an emulated transport, sent when it opens.
    initial: Vec<Value>,
    /// The core's command id for each of the model's.
    command_ids: BTreeMap<i64, u64>,
    reconnect_armed: bool,
    resubscribe_armed: BTreeSet<SubscriptionId>,
    connection_fetch: Option<FetchId>,
    token_fetches: BTreeMap<SubscriptionId, FetchId>,
    state_fetches: BTreeMap<SubscriptionId, FetchId>,
    calls: Vec<ferrifuge::machine::CallId>,
    /// The server-side subscriptions' states, from their events.
    server: BTreeMap<String, &'static str>,
    /// The application's position per channel: epoch, offset.
    positions: BTreeMap<String, (i64, i64)>,
    /// Recovered server-side publications the model delivers one per
    /// `Sdk_SSDeliver`.
    held: Vec<(String, i64)>,
    /// What happened during the step being replayed.
    sent: Vec<(u64, Sent)>,
    started: Vec<(String, String)>,
    counts: Counts,
}

impl<'a> Replay<'a> {
    fn new(trace: &'a Trace) -> Result<Self, Mismatch> {
        let config = Config::default();
        let machine = if trace.constant("ConnToken") {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        let kind = if trace.ordered() {
            TransportKind::WebSocket
        } else {
            TransportKind::HttpStream
        };
        let mut machine = machine
            .with_automatic_batching()
            .with_transports(vec![kind])
            .expect("one transport");
        for name in ["MapChans", "SPChans"] {
            if !trace.names(name).is_empty() {
                return Err(Mismatch {
                    step: 0,
                    kind: None,
                    text: format!("{name} is not replayed"),
                });
            }
        }
        let mut ids = BTreeMap::new();
        for channel in trace.names("Chans") {
            let id = machine
                .add_subscription(channel.clone(), Self::options(trace))
                .expect("a new channel");
            ids.insert(channel, id);
        }
        Ok(Self {
            trace,
            machine,
            ids,
            transport: None,
            initial: Vec::new(),
            command_ids: BTreeMap::new(),
            reconnect_armed: false,
            resubscribe_armed: BTreeSet::new(),
            connection_fetch: None,
            token_fetches: BTreeMap::new(),
            state_fetches: BTreeMap::new(),
            calls: Vec::new(),
            server: BTreeMap::new(),
            positions: BTreeMap::new(),
            held: Vec::new(),
            sent: Vec::new(),
            started: Vec::new(),
            counts: Counts::default(),
        })
    }

    fn options(trace: &Trace) -> SubscriptionOptions {
        let mut options = SubscriptionOptions::default();
        if trace.constant("Recoverable") {
            options = options.recoverable(true);
        }
        if trace.constant("SubToken") {
            options = options.token_source();
        }
        if trace.constant("GetState") {
            options = options.state_source();
        }
        if trace.constant("Delta") {
            options = options.delta(DeltaFormat::Fossil);
        }
        options
    }

    /// Replays the trace; on a divergence step, the mismatch found there.
    fn run(&mut self, divergence: Option<(usize, Divergence)>) -> Result<(), Mismatch> {
        let last = self.trace.acts.len();
        let mut step = 1;
        while step < last {
            let act = self.trace.acts[step].clone();
            if matches!(
                act.as_str(),
                "Srv_Publish"
                    | "Srv_Connect"
                    | "Srv_Subscribe"
                    | "Srv_Trim"
                    | "Srv_UnsubPush"
                    | "Srv_Disconnect"
                    | "Srv_NewEpoch"
                    | "Srv_Lost"
                    | "Srv_Unsubscribe"
                    | "Env_TokenExpires"
            ) {
                *self.counts.acts.entry(act).or_default() += 1;
                step += 1;
                continue;
            }
            if matches!(act.as_str(), "Sdk_StartFlow" | "Sdk_SSSubscribed") {
                return Err(self.mismatch(
                    step,
                    None,
                    &format!("{act} after no step with an input"),
                ));
            }
            // The steps without an input that follow are checked with it.
            let mut end = step;
            while end + 1 < last
                && matches!(
                    self.trace.acts[end + 1].as_str(),
                    "Sdk_StartFlow" | "Sdk_SSSubscribed"
                )
            {
                end += 1;
            }
            self.sent.clear();
            self.started.clear();
            self.input(step)?;
            for index in step..=end {
                *self
                    .counts
                    .acts
                    .entry(self.trace.acts[index].clone())
                    .or_default() += 1;
            }
            let compared = self.compare(step, end);
            let diverges = divergence.filter(|(at, _)| (step..=end).contains(at));
            match (compared, diverges) {
                (Ok(()), None) => {}
                (Err(mismatch), None) => return Err(mismatch),
                (Err(mismatch), Some((_, divergence))) => {
                    if mismatch.kind == Some(divergence.kind()) {
                        return Ok(());
                    }
                    return Err(mismatch);
                }
                (Ok(()), Some((at, divergence))) => {
                    return Err(self.mismatch(
                        at,
                        None,
                        &format!("{divergence:?} not found at that step"),
                    ));
                }
            }
            step = end + 1;
        }
        Ok(())
    }

    fn mismatch(&self, step: usize, kind: Option<Kind>, text: &str) -> Mismatch {
        Mismatch {
            step,
            kind,
            text: format!(
                "{} line {} step {} {}: {text}",
                self.trace.config,
                self.trace.line,
                step,
                self.trace.acts.get(step).map_or("", String::as_str)
            ),
        }
    }

    fn feed(&mut self, input: Input) {
        let actions = self.machine.handle(input);
        self.perform(actions);
        let flushed = self.machine.handle(Input::Flush);
        self.perform(flushed);
    }

    fn perform(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::OpenTransport {
                    transport,
                    initial_frame,
                    ..
                } => {
                    self.transport = Some(transport);
                    self.initial = initial_frame
                        .map(|frame| commands(&frame))
                        .unwrap_or_default();
                }
                Action::CloseTransport(transport) => {
                    if self.transport == Some(transport) {
                        self.transport = None;
                    }
                }
                Action::Send { frame, .. } => {
                    for command in commands(&frame) {
                        self.sent.push(Sent::of_core(&command));
                    }
                }
                Action::SetTimer(Timer::Reconnect, _) => self.reconnect_armed = true,
                Action::CancelTimer(Timer::Reconnect) => self.reconnect_armed = false,
                Action::SetSubscriptionTimer(id, SubscriptionTimer::Resubscribe, _) => {
                    self.resubscribe_armed.insert(id);
                }
                Action::CancelSubscriptionTimer(id, SubscriptionTimer::Resubscribe) => {
                    self.resubscribe_armed.remove(&id);
                }
                Action::FetchToken(fetch) => {
                    self.connection_fetch = Some(fetch);
                    self.started.push(("ctoken".to_owned(), String::new()));
                }
                Action::FetchSubscriptionToken {
                    subscription,
                    channel,
                    fetch,
                } => {
                    self.token_fetches.insert(subscription, fetch);
                    self.started.push(("stoken".to_owned(), channel));
                }
                Action::CancelSubscriptionToken(fetch) => {
                    self.token_fetches.retain(|_, pending| *pending != fetch);
                }
                Action::FetchSubscriptionState {
                    subscription,
                    channel,
                    fetch,
                } => {
                    self.state_fetches.insert(subscription, fetch);
                    self.started.push(("state".to_owned(), channel));
                }
                Action::CancelSubscriptionState(fetch) => {
                    self.state_fetches.retain(|_, pending| *pending != fetch);
                }
                Action::FetchSubscriptionData { channel, .. } => {
                    self.started.push(("data".to_owned(), channel));
                }
                Action::Emit(_, ClientEvent::ServerSubscription { channel, event }) => {
                    self.server_event(&channel, &event);
                }
                Action::EmitSubscription(_, id, event) => {
                    let channel = self.channel_of(id);
                    match event {
                        SubscriptionEvent::Subscribed(subscribed) => {
                            if let Some(position) = subscribed.stream_position {
                                self.position(&channel, &position);
                            }
                        }
                        SubscriptionEvent::Publication(publication) => {
                            self.positions.entry(channel).or_insert((-1, 0)).1 =
                                offset_of(publication.offset);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    fn server_event(&mut self, channel: &str, event: &ServerSubscriptionEvent) {
        match event {
            ServerSubscriptionEvent::Subscribing => {
                self.server.insert(channel.to_owned(), "subscribing");
            }
            ServerSubscriptionEvent::Subscribed(subscribed) => {
                self.server.insert(channel.to_owned(), "subscribed");
                if let Some(position) = &subscribed.stream_position {
                    self.position(channel, position);
                }
            }
            ServerSubscriptionEvent::Unsubscribed { .. } => {
                self.server.insert(channel.to_owned(), "unsubscribed");
            }
            ServerSubscriptionEvent::Publication(publication) => {
                self.held
                    .push((channel.to_owned(), offset_of(publication.offset)));
            }
            _ => {}
        }
    }

    fn position(&mut self, channel: &str, position: &StreamPosition) {
        self.positions.insert(
            channel.to_owned(),
            (
                epoch_number(Some(&position.epoch)),
                i64::try_from(position.offset).unwrap_or(i64::MAX),
            ),
        );
    }

    fn channel_of(&self, id: SubscriptionId) -> String {
        self.ids
            .iter()
            .find(|(_, known)| **known == id)
            .map(|(channel, _)| channel.clone())
            .unwrap_or_default()
    }

    /// The channel whose `variable` entry the step changed.
    fn changed_channel(&self, step: usize, variable: &str) -> Result<String, Mismatch> {
        let before = &self.trace.states[step - 1];
        let after = &self.trace.states[step];
        let changed: Vec<String> = channels(after, variable)
            .into_iter()
            .filter(|channel| before[variable][channel] != after[variable][channel])
            .collect();
        match changed.as_slice() {
            [channel] => Ok(channel.clone()),
            _ => Err(self.mismatch(
                step,
                None,
                &format!("not one channel of {variable} changed: {changed:?}"),
            )),
        }
    }

    fn id(&self, step: usize, channel: &str) -> Result<SubscriptionId, Mismatch> {
        self.ids
            .get(channel)
            .copied()
            .ok_or_else(|| self.mismatch(step, None, &format!("no subscription to {channel}")))
    }

    /// Feeds the input of step `step`.
    fn input(&mut self, step: usize) -> Result<(), Mismatch> {
        let act = self.trace.acts[step].clone();
        match act.split('_').next() {
            Some("App") => self.application(step, &act),
            Some("Env") => self.environment(step, &act),
            Some("Cb") => self.callback(step, &act),
            Some("Sdk") => self.sdk(step, &act),
            _ => Err(self.unmapped(step, &act)),
        }
    }

    fn unmapped(&self, step: usize, act: &str) -> Mismatch {
        self.mismatch(step, None, &format!("the act without a mapping {act}"))
    }

    /// The application's steps.
    fn application(&mut self, step: usize, act: &str) -> Result<(), Mismatch> {
        match act {
            "App_Connect" => self.feed(Input::Connect),
            "App_Disconnect" => self.feed(Input::Disconnect),
            "App_Close" => self.feed(Input::Close),
            "App_Subscribe" => {
                let id = self.id(step, &self.changed_channel(step, "sub")?)?;
                self.feed(Input::Subscribe(id));
            }
            "App_Unsubscribe" => {
                let id = self.id(step, &self.changed_channel(step, "sub")?)?;
                self.feed(Input::Unsubscribe(id));
            }
            "App_Replace" => {
                let channel = self.changed_channel(step, "sub")?;
                let id = self.id(step, &channel)?;
                self.feed(Input::RemoveSubscription(id));
                let new = self
                    .machine
                    .add_subscription(channel.clone(), Self::options(self.trace))
                    .map_err(|error| self.mismatch(step, None, &format!("{error:?}")))?;
                self.ids.insert(channel, new);
            }
            "App_Call" => {
                let call = self.machine.add_call(CallRequest::Rpc {
                    method: String::new(),
                    data: b"{}".to_vec(),
                });
                self.calls.push(call);
                self.feed(Input::Call(call));
            }
            other => return Err(self.unmapped(step, other)),
        }
        Ok(())
    }

    /// The network's steps, and the call timeout.
    fn environment(&mut self, step: usize, act: &str) -> Result<(), Mismatch> {
        let before = self.trace.states[step - 1].clone();
        match act {
            "Env_CallTimeout" => {
                let call = self.calls.remove(0);
                self.feed(Input::CallTimerFired(
                    call,
                    ferrifuge::machine::CallTimer::Connection,
                ));
            }
            "Env_Open" => {
                let transport = self.transport_of(step)?;
                self.feed(Input::TransportOpened(transport));
                let initial = std::mem::take(&mut self.initial);
                for command in initial {
                    self.sent.insert(0, Sent::of_core(&command));
                }
            }
            "Env_Drop" | "Env_WriteError" => {
                let transport = self.transport_of(step)?;
                if before["cl"]["topen"] == true {
                    self.feed(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                } else {
                    self.feed(Input::TransportFailed {
                        transport,
                        message: "dropped".to_owned(),
                    });
                }
            }
            "Env_Timeout" => {
                let head = list(&before, "pend")
                    .first()
                    .cloned()
                    .unwrap_or(Value::Null);
                if head["k"] == "connect" {
                    self.feed(Input::TimerFired(Timer::ConnectTimeout));
                } else if head["k"] == "sub" {
                    let id = self.id(step, head["ch"].as_str().unwrap_or_default())?;
                    self.feed(Input::SubscriptionTimerFired(
                        id,
                        SubscriptionTimer::SubscribeReply,
                    ));
                } else if head["k"] == "unsub" {
                    let id = self.id(step, head["ch"].as_str().unwrap_or_default())?;
                    self.feed(Input::SubscriptionTimerFired(
                        id,
                        SubscriptionTimer::UnsubscribeReply,
                    ));
                } else {
                    return Err(self.mismatch(step, None, &format!("a timeout of {head}")));
                }
            }
            other => return Err(self.unmapped(step, other)),
        }
        Ok(())
    }

    /// A callback settles; its outcome is inferred from the step (D3).
    fn callback(&mut self, step: usize, act: &str) -> Result<(), Mismatch> {
        let before = self.trace.states[step - 1].clone();
        let after = self.trace.states[step].clone();
        match act {
            "Cb_ConnToken" => {
                let result = match after["cl"]["ph"].as_str() {
                    Some("open") if after["cl"]["tok"].as_i64() > Some(0) => {
                        Ok(token(after["cl"]["tok"].as_i64().unwrap_or(0)))
                    }
                    Some("wait") => Err(TokenError::Failed("model".to_owned())),
                    Some("idle") => Err(TokenError::Unauthorized),
                    _ if !changed(&before, &after, "cl") => {
                        // A stale callback: its outcome is dropped.
                        return Ok(());
                    }
                    other => {
                        return Err(self.mismatch(
                            step,
                            None,
                            &format!("no token outcome fits phase {other:?}"),
                        ));
                    }
                };
                if let Some(fetch) = self.connection_fetch.take() {
                    self.feed(Input::TokenFetched { fetch, result });
                }
            }
            "Cb_SubToken" | "Cb_GetState" => {
                let callback = settled_callback(&before, &after)
                    .ok_or_else(|| self.mismatch(step, None, "no callback settled"))?;
                let channel = callback["ch"].as_str().unwrap_or_default().to_owned();
                let id = self.id(step, &channel)?;
                let sub_before = &before["sub"][&channel];
                let sub_after = &after["sub"][&channel];
                if act == "Cb_SubToken" {
                    let Some(fetch) = self.token_fetches.remove(&id) else {
                        return Ok(());
                    };
                    let result = if sub_after["st"] == "unsubscribed"
                        && sub_before["st"] != "unsubscribed"
                    {
                        Err(TokenError::Unauthorized)
                    } else if sub_after["tmr"] == true && sub_before["tmr"] == false {
                        Err(TokenError::Failed("model".to_owned()))
                    } else {
                        Ok(token(sub_after["tok"].as_i64().unwrap_or(0)))
                    };
                    self.feed(Input::SubscriptionTokenFetched { fetch, result });
                } else {
                    let Some(fetch) = self.state_fetches.remove(&id) else {
                        return Ok(());
                    };
                    let result = if sub_after["tmr"] == true && sub_before["tmr"] == false {
                        Err(StateError("model".to_owned()))
                    } else {
                        let position = StreamPosition {
                            offset: sub_after["pos"].as_u64().unwrap_or(0),
                            epoch: epoch(sub_after["ep"].as_i64().unwrap_or(-1))
                                .unwrap_or_default(),
                        };
                        // The application loaded its state at this position.
                        self.position(&channel, &position);
                        Ok(position)
                    };
                    self.feed(Input::SubscriptionStateFetched { fetch, result });
                }
            }
            other => return Err(self.unmapped(step, other)),
        }
        Ok(())
    }

    /// The client's own steps: its timers and the messages it reads.
    fn sdk(&mut self, step: usize, act: &str) -> Result<(), Mismatch> {
        let before = self.trace.states[step - 1].clone();
        match act {
            "Sdk_ReconnectTimer" => {
                // A timer that fires is no longer armed.
                self.reconnect_armed = false;
                self.feed(Input::TimerFired(Timer::Reconnect));
            }
            "Sdk_ResubscribeTimer" => {
                let id = self.id(step, &self.changed_channel(step, "sub")?)?;
                self.resubscribe_armed.remove(&id);
                self.feed(Input::SubscriptionTimerFired(
                    id,
                    SubscriptionTimer::Resubscribe,
                ));
            }
            "Sdk_ConnectReply"
            | "Sdk_SubscribeReply"
            | "Sdk_UnsubscribeReply"
            | "Sdk_UnsubscribePush"
            | "Sdk_DisconnectPush"
            | "Sdk_Publication" => {
                let message = list(&before, "toC")
                    .first()
                    .cloned()
                    .ok_or_else(|| self.mismatch(step, None, "nothing to read"))?;
                let frame = self.frame(step, &message)?;
                let transport = self.transport_of(step)?;
                self.feed(Input::Frame {
                    transport,
                    data: frame.to_string().into_bytes(),
                });
            }
            "Sdk_SSDeliver" => {
                if !self.held.is_empty() {
                    let (channel, offset) = self.held.remove(0);
                    self.positions.entry(channel).or_insert((-1, 0)).1 = offset;
                }
            }
            other => return Err(self.unmapped(step, other)),
        }
        Ok(())
    }

    fn transport_of(&self, step: usize) -> Result<TransportId, Mismatch> {
        self.transport
            .ok_or_else(|| self.mismatch(step, None, "no transport"))
    }

    /// The frame carrying the model's message `message`.
    fn frame(&self, step: usize, message: &Value) -> Result<Value, Mismatch> {
        let id = |model: &Value| -> Result<u64, Mismatch> {
            let model = model.as_i64().unwrap_or(0);
            self.command_ids.get(&model).copied().ok_or_else(|| {
                self.mismatch(
                    step,
                    None,
                    &format!("a reply to command {model}, never sent"),
                )
            })
        };
        let channel = message["ch"].as_str().unwrap_or_default();
        let code = message["code"].as_str().unwrap_or_default();
        let ok = message["ok"] == true && code.is_empty();
        let error = |id: u64| -> Value {
            let (code, temporary) = error_code(code);
            json!({"id": id, "error": {"code": code, "message": "model", "temporary": temporary}})
        };
        Ok(match message["k"].as_str().unwrap_or_default() {
            "conrep" => {
                let id = id(&message["id"])?;
                if ok {
                    let mut subs = serde_json::Map::new();
                    if let Some(map) = message["srs"].as_object() {
                        for (channel, result) in map {
                            subs.insert(channel.clone(), result_of(result));
                        }
                    }
                    json!({"id": id, "connect": {"client": "c", "version": "0", "subs": subs}})
                } else {
                    error(id)
                }
            }
            "subrep" => {
                let id = id(&message["id"])?;
                if ok {
                    json!({"id": id, "subscribe": result_of(&message["res"])})
                } else {
                    error(id)
                }
            }
            "unsubrep" => {
                let id = id(&message["id"])?;
                if ok {
                    json!({"id": id, "unsubscribe": {}})
                } else {
                    error(id)
                }
            }
            "unsubpush" => json!({"push": {"channel": channel, "unsubscribe": {
                "code": unsubscribe_code(code), "reason": "model",
            }}}),
            "disc" => json!({"push": {"disconnect": {
                "code": disconnect_code(code), "reason": "model",
            }}}),
            "pub" => json!({"push": {"channel": channel, "pub": {
                "data": {}, "offset": message["off"].as_i64().unwrap_or(0),
            }}}),
            other => return Err(self.mismatch(step, None, &format!("a message {other}"))),
        })
    }

    /// Compares the model's state after step `end` with the core, for the
    /// steps from `start`.
    fn compare(&mut self, start: usize, end: usize) -> Result<(), Mismatch> {
        let before = self.trace.states[start - 1].clone();
        let after = self.trace.states[end].clone();
        self.compare_commands(start, &before, &after)?;
        self.compare_states(start, &after)?;
        self.compare_others(start, &before, &after)
    }

    /// The commands sent, in order, field by field.
    fn compare_commands(
        &mut self,
        start: usize,
        before: &Value,
        after: &Value,
    ) -> Result<(), Mismatch> {
        // Commands, in order.
        let expected: Vec<Sent> = new_commands(before, after)
            .iter()
            .map(Sent::of_model)
            .collect();
        let sent: Vec<Sent> = self.sent.iter().map(|(_, sent)| sent.clone()).collect();
        self.count(Kind::Commands, expected.len().max(1));
        if expected != sent {
            return Err(self.mismatch(
                start,
                Some(Kind::Commands),
                &format!(
                    "{}: model {expected:?}, core {sent:?}",
                    Kind::Commands.name()
                ),
            ));
        }
        let model_ids: Vec<i64> = new_commands(before, after)
            .iter()
            .filter_map(|command| command["id"].as_i64())
            .collect();
        for (model, (core, _)) in model_ids.iter().zip(&self.sent) {
            self.command_ids.insert(*model, *core);
        }
        Ok(())
    }

    /// The client's, the subscriptions' and the server-side subscriptions'
    /// states.
    fn compare_states(&mut self, start: usize, after: &Value) -> Result<(), Mismatch> {
        // The client's state.
        let model = match after["cl"]["st"].as_str() {
            Some("connected") => State::Connected,
            Some("connecting") => State::Connecting,
            _ => State::Disconnected,
        };
        self.count(Kind::Client, 1);
        if self.machine.state() != model {
            return Err(self.mismatch(
                start,
                Some(Kind::Client),
                &format!(
                    "{}: model {model:?}, core {:?}",
                    Kind::Client.name(),
                    self.machine.state()
                ),
            ));
        }
        // Each subscription's state.
        for channel in channels(after, "sub") {
            let model = match after["sub"][&channel]["st"].as_str() {
                Some("subscribed") => SubscriptionState::Subscribed,
                Some("subscribing") => SubscriptionState::Subscribing,
                _ => SubscriptionState::Unsubscribed,
            };
            let core = self
                .ids
                .get(&channel)
                .and_then(|id| self.machine.subscription_state(*id));
            self.count(Kind::Subscriptions, 1);
            if core != Some(model) {
                return Err(self.mismatch(
                    start,
                    Some(Kind::Subscriptions),
                    &format!(
                        "{} of {channel}: model {model:?}, core {core:?}",
                        Kind::Subscriptions.name()
                    ),
                ));
            }
        }
        // Server-side subscriptions.
        let mut all: BTreeSet<String> = channels(after, "ss").into_iter().collect();
        all.extend(self.server.keys().cloned());
        for channel in all {
            let model = after["ss"][&channel]["st"]
                .as_str()
                .unwrap_or("unsubscribed");
            let core = self.server.get(&channel).copied().unwrap_or("unsubscribed");
            self.count(Kind::ServerSubscriptions, 1);
            if model != core {
                return Err(self.mismatch(
                    start,
                    Some(Kind::ServerSubscriptions),
                    &format!(
                        "{} of {channel}: model {model}, core {core}",
                        Kind::ServerSubscriptions.name()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The callbacks started, the timers and the application's positions.
    fn compare_others(
        &mut self,
        start: usize,
        before: &Value,
        after: &Value,
    ) -> Result<(), Mismatch> {
        // Callbacks started.
        let mut expected: Vec<(String, String)> = started_callbacks(before, after);
        let mut started = self.started.clone();
        expected.sort();
        started.sort();
        self.count(Kind::Callbacks, 1);
        if expected != started {
            return Err(self.mismatch(
                start,
                Some(Kind::Callbacks),
                &format!(
                    "{}: model {expected:?}, core {started:?}",
                    Kind::Callbacks.name()
                ),
            ));
        }
        // Timers.
        let model_reconnect = after["cl"]["ph"] == "wait";
        self.count(Kind::Timers, 1);
        if model_reconnect != self.reconnect_armed {
            return Err(self.mismatch(
                start,
                Some(Kind::Timers),
                &format!(
                    "{}: reconnect model {model_reconnect}, core {}",
                    Kind::Timers.name(),
                    self.reconnect_armed
                ),
            ));
        }
        for channel in channels(after, "sub") {
            let model = after["sub"][&channel]["tmr"] == true;
            let core = self
                .ids
                .get(&channel)
                .is_some_and(|id| self.resubscribe_armed.contains(id));
            self.count(Kind::Timers, 1);
            if model != core {
                return Err(self.mismatch(
                    start,
                    Some(Kind::Timers),
                    &format!(
                        "{}: resubscribe of {channel} model {model}, core {core}",
                        Kind::Timers.name()
                    ),
                ));
            }
        }
        // The application's position.
        for channel in channels(after, "app") {
            let app = &after["app"][&channel];
            let model = (
                app["ep"].as_i64().unwrap_or(-1),
                app["off"].as_i64().unwrap_or(0),
            );
            let core = self.positions.get(&channel).copied().unwrap_or((-1, 0));
            self.count(Kind::Position, 1);
            if model != core {
                return Err(self.mismatch(
                    start,
                    Some(Kind::Position),
                    &format!(
                        "{} of {channel}: model {model:?}, core {core:?}",
                        Kind::Position.name()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn count(&mut self, kind: Kind, values: usize) {
        *self.counts.values.entry(kind).or_default() += values;
    }
}

/// The commands of a JSON frame: one per line, those of an emulation
/// request in the frame it carries.
fn commands(frame: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(frame)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .flat_map(|line| {
            let value: Value = serde_json::from_str(line).expect("a JSON command");
            match value.get("session").and(value["data"].as_str()) {
                Some(carried) => commands(carried.as_bytes()),
                None => vec![value],
            }
        })
        .collect()
}

/// The callback of `cbs` that `after` no longer holds.
fn settled_callback(before: &Value, after: &Value) -> Option<Value> {
    let remaining = list(after, "cbs");
    list(before, "cbs")
        .into_iter()
        .find(|callback| !remaining.contains(callback))
}

/// The callbacks `after` holds that `before` did not, as kind and channel.
fn started_callbacks(before: &Value, after: &Value) -> Vec<(String, String)> {
    let mut known = list(before, "cbs");
    let mut started = Vec::new();
    for callback in list(after, "cbs") {
        if let Some(index) = known.iter().position(|old| *old == callback) {
            known.remove(index);
        } else {
            started.push((
                callback["k"].as_str().unwrap_or_default().to_owned(),
                callback["ch"].as_str().unwrap_or_default().to_owned(),
            ));
        }
    }
    started
}

/// Replays `trace`: `Ok` when it matches the model, up to its recorded
/// divergence where it must differ.
fn replay(trace: &Trace, counts: &mut Counts) -> Result<Option<Divergence>, Mismatch> {
    let divergence = trace.divergence();
    let mut replay = Replay::new(trace)?;
    let result = replay.run(divergence);
    for (act, count) in std::mem::take(&mut replay.counts.acts) {
        *counts.acts.entry(act).or_default() += count;
    }
    for (kind, count) in std::mem::take(&mut replay.counts.values) {
        *counts.values.entry(kind).or_default() += count;
    }
    result.map(|()| divergence.map(|(_, divergence)| divergence))
}

/// The outcome of replaying every trace.
struct Run {
    counts: Counts,
    clean: usize,
    diverged: BTreeMap<Divergence, usize>,
    /// The traces each recorded divergence's predicate matches.
    reached: BTreeMap<Divergence, usize>,
    failures: Vec<String>,
}

fn run_all() -> Run {
    let mut run = Run {
        counts: Counts::default(),
        clean: 0,
        diverged: BTreeMap::new(),
        reached: BTreeMap::new(),
        failures: Vec::new(),
    };
    for (_, traces) in traces() {
        for trace in &traces {
            for divergence in trace.divergences() {
                *run.reached.entry(divergence).or_default() += 1;
            }
            match replay(trace, &mut run.counts) {
                Ok(None) => run.clean += 1,
                Ok(Some(divergence)) => *run.diverged.entry(divergence).or_default() += 1,
                Err(mismatch) => run.failures.push(mismatch.text),
            }
        }
    }
    run
}

/// The failures, grouped by reason, most frequent first, with an example.
fn report(failures: &[String]) -> String {
    let mut reasons: BTreeMap<String, (usize, String)> = BTreeMap::new();
    for failure in failures {
        let reason = failure
            .split_once(": ")
            .map_or(failure.as_str(), |(_, reason)| reason);
        let key: String = reason.chars().take(60).collect();
        reasons.entry(key).or_insert((0, failure.clone())).0 += 1;
    }
    let mut reasons: Vec<(usize, String)> = reasons.into_values().collect();
    reasons.sort_by_key(|(count, _)| std::cmp::Reverse(*count));
    let mut report = String::new();
    for (count, example) in reasons.iter().take(25) {
        let _ = writeln!(report, "{count:4} × {example}");
    }
    report
}

#[test]
fn every_model_trace_replays_into_the_core_up_to_its_recorded_divergence() {
    let run = run_all();
    for (divergence, recorded) in RECORDED {
        let matched = run.reached.get(&divergence).copied().unwrap_or(0);
        assert_eq!(
            matched, recorded,
            "{divergence:?} matches {matched} traces, recorded {recorded}"
        );
    }
    let reaching: usize = run.reached.values().sum();
    println!(
        "{} clean, diverging as recorded: {:?}; predicates matched: {:?} ({reaching} with overlaps)",
        run.clean, run.diverged, run.reached
    );
    for act in ACTS {
        let count = run.counts.acts.get(act).copied().unwrap_or(0);
        println!("  {act}: {count} steps");
        if PAST_A_DIVERGENCE.contains(&act) {
            assert_eq!(count, 0, "{act} is replayed now: update PAST_A_DIVERGENCE");
        } else {
            assert!(count > 0, "no step of {act} replayed");
        }
    }
    for act in run.counts.acts.keys() {
        assert!(ACTS.contains(&act.as_str()), "{act} is not in ACTS");
    }
    for kind in Kind::ALL {
        let count = run.counts.values.get(&kind).copied().unwrap_or(0);
        println!("  {}: {count} values", kind.name());
        assert!(count > 0, "no value of {} compared", kind.name());
    }
    assert!(
        run.failures.is_empty(),
        "{} traces differ from the model:\n{}",
        run.failures.len(),
        report(&run.failures)
    );
    assert_eq!(run.clean, CLEAN, "traces reaching no recorded divergence");
}

/// A fault planted in a copy of a model trace, for the replayer to report
/// (the outline "The replayer reports a planted mismatch").
#[derive(Debug, Clone, Copy)]
enum Fault {
    SubscribeChannel,
    ClientState,
    SubscriptionState,
    TokenCallback,
    ResubscribeTimer,
    /// Not an example of the outline: the reconnect timer's comparison.
    ReconnectTimer,
    ApplicationOffset,
    UnsubscribeCommand,
    UnknownAct,
    MarkedDivergence,
}

impl Fault {
    /// Plants the fault at step `step` of `trace`, when it applies there.
    fn plant(self, trace: &mut Trace, step: usize) -> bool {
        let before = trace.states[step - 1].clone();
        let state = &mut trace.states[step];
        match self {
            Self::SubscribeChannel | Self::UnsubscribeCommand => {
                let kind = if matches!(self, Self::SubscribeChannel) {
                    "sub"
                } else {
                    "unsub"
                };
                let new = new_commands(&before, state);
                let Some(target) = new.iter().find(|command| command["k"] == kind) else {
                    return false;
                };
                let Some(queue) = state["toS"].as_array_mut() else {
                    return false;
                };
                let index = queue
                    .iter()
                    .position(|command| command == target)
                    .expect("a new command is in toS");
                if matches!(self, Self::SubscribeChannel) {
                    queue[index]["ch"] = json!("planted");
                } else {
                    queue.remove(index);
                }
                true
            }
            Self::ClientState => {
                let planted = if state["cl"]["st"] == "connected" {
                    "connecting"
                } else {
                    "connected"
                };
                state["cl"]["st"] = json!(planted);
                true
            }
            Self::SubscriptionState => {
                let Some(channel) = channels(state, "sub").into_iter().next() else {
                    return false;
                };
                let planted = if state["sub"][&channel]["st"] == "subscribed" {
                    "unsubscribed"
                } else {
                    "subscribed"
                };
                state["sub"][&channel]["st"] = json!(planted);
                true
            }
            Self::TokenCallback => {
                let started = started_callbacks(&before, state);
                if !started.iter().any(|(kind, _)| kind.ends_with("token")) {
                    return false;
                }
                let Some(callbacks) = state["cbs"].as_array_mut() else {
                    return false;
                };
                let index = callbacks
                    .iter()
                    .rposition(|callback| {
                        callback["k"]
                            .as_str()
                            .is_some_and(|kind| kind.ends_with("token"))
                    })
                    .expect("a started token callback");
                callbacks.remove(index);
                true
            }
            Self::ResubscribeTimer => {
                let Some(channel) = channels(state, "sub")
                    .into_iter()
                    .find(|channel| state["sub"][channel]["tmr"] == false)
                else {
                    return false;
                };
                state["sub"][&channel]["tmr"] = json!(true);
                true
            }
            Self::ReconnectTimer => {
                let planted = if state["cl"]["ph"] == "wait" {
                    "idle"
                } else {
                    "wait"
                };
                state["cl"]["ph"] = json!(planted);
                true
            }
            Self::ApplicationOffset => {
                let Some(channel) = channels(state, "app").into_iter().next() else {
                    return false;
                };
                let offset = state["app"][&channel]["off"].as_i64().unwrap_or(0);
                state["app"][&channel]["off"] = json!(offset + 1);
                true
            }
            Self::UnknownAct => {
                "App_Unmapped".clone_into(&mut trace.acts[step]);
                true
            }
            Self::MarkedDivergence => true,
        }
    }

    /// What the report must name.
    fn report(self) -> &'static str {
        match self {
            Self::SubscribeChannel | Self::UnsubscribeCommand => Kind::Commands.name(),
            Self::ClientState => Kind::Client.name(),
            Self::SubscriptionState => Kind::Subscriptions.name(),
            Self::TokenCallback => Kind::Callbacks.name(),
            Self::ResubscribeTimer | Self::ReconnectTimer => Kind::Timers.name(),
            Self::ApplicationOffset => Kind::Position.name(),
            Self::UnknownAct => "the act without a mapping",
            Self::MarkedDivergence => "Dv2 not found at that step",
        }
    }
}

/// Plants `fault` in a copy of the first clean trace it applies to, at a
/// step compared on its own, and returns the replay's report.
fn planted(fault: Fault) -> String {
    let inputs_alone = |trace: &Trace, step: usize| {
        !matches!(
            trace.acts[step].as_str(),
            "Sdk_StartFlow" | "Sdk_SSSubscribed" | "Env_TokenExpires"
        ) && !trace.acts[step].starts_with("Srv_")
            && trace
                .acts
                .get(step + 1)
                .is_none_or(|next| !matches!(next.as_str(), "Sdk_StartFlow" | "Sdk_SSSubscribed"))
    };
    for (_, traces) in traces() {
        for trace in &traces {
            if trace.divergence().is_some() || replay(trace, &mut Counts::default()).is_err() {
                continue;
            }
            for step in 1..trace.acts.len() {
                if !inputs_alone(trace, step) {
                    continue;
                }
                let mut copy = trace.clone();
                if !fault.plant(&mut copy, step) {
                    continue;
                }
                let mut replay = Replay::new(&copy).expect("a replayable trace");
                let marked =
                    matches!(fault, Fault::MarkedDivergence).then_some((step, Divergence::Dv2));
                let mismatch = replay
                    .run(marked)
                    .expect_err("the planted fault is reported");
                assert_eq!(mismatch.step, step, "{fault:?}: {}", mismatch.text);
                let named = format!(
                    "{} line {} step {step} {}: ",
                    copy.config, copy.line, copy.acts[step]
                );
                assert!(
                    mismatch.text.starts_with(&named),
                    "{fault:?}: {}",
                    mismatch.text
                );
                return mismatch.text;
            }
        }
    }
    panic!("no clean trace to plant {fault:?} in");
}

#[test]
fn the_replayer_reports_a_planted_mismatch() {
    for fault in [
        Fault::SubscribeChannel,
        Fault::ClientState,
        Fault::SubscriptionState,
        Fault::TokenCallback,
        Fault::ResubscribeTimer,
        Fault::ApplicationOffset,
        Fault::UnsubscribeCommand,
        Fault::UnknownAct,
        Fault::MarkedDivergence,
    ] {
        let report = planted(fault);
        assert!(report.contains(fault.report()), "{fault:?}: {report}");
        // Both values, where the fault changed one.
        if !matches!(fault, Fault::UnknownAct | Fault::MarkedDivergence) {
            assert!(
                report.contains("model") && report.contains("core"),
                "{fault:?}: {report}"
            );
        }
    }
}

#[test]
fn the_copied_traces_are_those_named_in_the_readme() {
    let readme = std::fs::read_to_string(fixtures().join("README.md")).expect("the README");
    let mut checked = 0;
    for line in readme.lines() {
        let Some((sum, path)) = line.split_once("  ") else {
            continue;
        };
        if sum.len() != 64 || !sum.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let bytes = std::fs::read(fixtures().join("upstream").join(path)).expect("a copied file");
        let digest = Sha256::digest(&bytes);
        let hex = digest.iter().fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
        assert_eq!(hex, sum, "{path} differs from the copy the README names");
        checked += 1;
    }
    assert_eq!(checked, 20, "files named in the README");
}

fn offset_of(offset: Option<u64>) -> i64 {
    offset
        .and_then(|offset| i64::try_from(offset).ok())
        .unwrap_or(0)
}

// Beyond the outline's examples: the reconnect timer is compared too.
#[test]
fn a_reconnect_timer_mismatch_is_reported() {
    let report = planted(Fault::ReconnectTimer);
    assert!(report.contains("reconnect model"), "{report}");
}
