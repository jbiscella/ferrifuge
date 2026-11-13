//! Client driven by the adapter of the target: tokio natively, the
//! browser's event loop on the browser target (Block 44, D2).

// Neither `Send` nor `Sync` on the browser target (Block 44, D3).
#![cfg_attr(adapter_browser, allow(clippy::arc_with_non_send_sync))]

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::{Duration, SystemTime};

use super::outbox::Outbox;
use futures_core::Stream;
use futures_util::future::{self, AbortHandle};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
#[cfg(adapter_tokio)]
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
#[cfg(adapter_tokio)]
use rustls::pki_types::CertificateDer;
use tokio::sync::{Notify, oneshot};
use tracing::Instrument;

use super::backend::Inbound;
use super::log::{self, Log};
use super::platform::{self, Handle, Sleep};
use super::ready::{self, Of, Waiters};
#[cfg(adapter_tokio)]
use super::tokio::connector::{self, Connector};
#[cfg(adapter_tokio)]
use super::tokio::http::HeaderSet;
#[cfg(adapter_tokio)]
use super::tokio::net::Network;
#[cfg(adapter_tokio)]
use super::tokio::proxy::Proxy;
#[cfg(adapter_tokio)]
use super::tokio::tls::{self, Tls, Trust, unparsable_root};
use crate::machine::{
    Action, CallId, CallTimer, DebounceTimer, FetchId, FrameType, Input, Machine, SubscriptionId,
    SubscriptionTimer, Timer, TransportId, TransportKind,
};
use crate::{
    CallError, CallReply, CallRequest, ClientError, ClientEvent, Config, DataError, ErrorKind,
    HistoryOptions, HistoryResult, JsonCallError, MaybeSend, MaybeSync, PresenceResult,
    PresenceStatsResult, State, StateError, StreamPosition, SubscriptionEvent, TokenError, codes,
};
use tracing::{debug, trace, warn};

/// Centrifugo client, running on tokio natively and on the page's event
/// loop in the browser (Block 44, D1).
///
/// Natively it is `Send` and `Sync`; on wasm32-unknown-unknown it is
/// neither, as the browser's objects it holds are not (Block 44, D3).
///
/// Clones share one client and its connection. Dropping the last clone
/// closes the client, as [`Client::close`] does without waiting (Block 33);
/// a [`WeakClient`] from [`Client::downgrade`] does not keep it alive
/// (Block 41, D5).
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    owner: Arc<Owner>,
}

/// What the clones of a client own together: the last one dropped shuts the
/// client down. The driver and the subscriptions hold `Shared`, not this.
struct Owner {
    shared: Arc<Shared>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.shut_down();
    }
}

/// A handle that does not keep its client alive, from
/// [`Client::downgrade`] (Block 41, D5), as tokio's `WeakSender`: for the
/// state of an application, or a handler, that must not hold the client.
#[derive(Clone)]
pub struct WeakClient {
    owner: std::sync::Weak<Owner>,
}

impl WeakClient {
    /// The client, while one of its clones is alive; `None` once the last
    /// one was dropped. A client closed with [`Client::close`] while clones
    /// live is still given, closed.
    #[must_use]
    pub fn upgrade(&self) -> Option<Client> {
        let owner = self.owner.upgrade()?;
        Some(Client {
            shared: Arc::clone(&owner.shared),
            owner,
        })
    }
}

impl std::fmt::Debug for WeakClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakClient").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Client {
    // Only the state: queued frames may carry the connection token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Websocket close code "message too big" (RFC 6455).
const MESSAGE_TOO_BIG: u16 = 1009;

/// What the client handle, its subscriptions and its driver task share.
#[derive(Debug)]
pub(super) struct Shared {
    pub(super) core: Mutex<Core>,
    /// Events waiting for the streams of the client and its subscriptions.
    pub(super) outboxes: Mutex<Outboxes>,
    /// Data and token callbacks of the subscriptions that have them.
    pub(super) callbacks: Mutex<SubscriptionCallbacks>,
    /// Where the outcome of each call goes, by call (Block 13).
    calls: Mutex<HashMap<CallId, oneshot::Sender<Result<CallReply, CallError>>>>,
    /// The waiting `ready()` futures (Block 30). Locked after `core`.
    pub(super) ready: Mutex<Waiters>,
    /// Set, and `driver_ended` notified, when the driver's task has ended
    /// (Block 33, D1).
    driver_gone: AtomicBool,
    driver_ended: Notify,
    /// Wakes the driver when actions are queued.
    actions_queued: Notify,
    /// The debounce timers the driver holds unarmed for a test, oldest
    /// first; `None` while it arms them (Block 46).
    #[cfg(feature = "__test-hooks")]
    held_debounce_timers: Mutex<Option<Vec<DebounceTimer>>>,
    /// Set when the client handle is dropped: the driver stops once the
    /// queued actions are performed.
    pub(super) closed: AtomicBool,
    /// Spans and records of this client (Block 21).
    pub(super) log: Log,
    /// Its TLS and proxy settings and HTTP clients (Blocks 22, 23).
    #[cfg(adapter_tokio)]
    pub(super) network: Arc<Network>,
    /// The real HTTP headers of every transport request, which
    /// `set_http_headers` replaces (Blocks 17, 32).
    #[cfg(adapter_tokio)]
    pub(super) http_headers: HeaderSet,
    /// The handlers of the client and its subscriptions (Block 41b).
    /// Locked after `outboxes`.
    pub(super) handlers: Mutex<super::handler::Handlers>,
    /// The runtime the client's tasks run on (Block 28).
    pub(super) runtime: Handle,
    /// The browser's network and page listeners (Block 44, D10, D11).
    /// Locked after `core`.
    #[cfg(adapter_browser)]
    events: Mutex<super::backend::events::Events>,
}

/// Events waiting for their streams (Block 15). Locked after
/// [`Shared::core`], never before.
#[derive(Debug, Default)]
pub(super) struct Outboxes {
    pub(super) client: Outbox<ClientEvent>,
    /// Those of the subscriptions whose `events()` was called.
    pub(super) subscriptions: HashMap<SubscriptionId, Outbox<SubscriptionEvent>>,
    /// Those of the map subscriptions whose `events()` was called (Block
    /// 45).
    #[cfg(feature = "unstable")]
    pub(super) maps: HashMap<SubscriptionId, Outbox<crate::MapSubscriptionEvent>>,
    /// Those of the shared poll subscriptions whose `events()` was called
    /// (Block 47).
    #[cfg(feature = "unstable")]
    pub(super) shared_polls: HashMap<SubscriptionId, Outbox<crate::SharedPollSubscriptionEvent>>,
}

impl Outboxes {
    fn withdraw(&mut self, ids: &[crate::machine::EventId]) {
        self.client.withdraw(ids);
        for outbox in self.subscriptions.values_mut() {
            outbox.withdraw(ids);
        }
        #[cfg(feature = "unstable")]
        for outbox in self.maps.values_mut() {
            outbox.withdraw(ids);
        }
        #[cfg(feature = "unstable")]
        for outbox in self.shared_polls.values_mut() {
            outbox.withdraw(ids);
        }
    }
}

impl Core {
    fn new(machine: Machine) -> Self {
        Self {
            machine,
            actions: VecDeque::new(),
            clock: Clock::default(),
            refreshes: Refreshes::default(),
            armed: Armed::default(),
            connected_since: None,
        }
    }
}

/// The machine and its pending actions, locked together: API calls change
/// the state before they return, and the driver performs actions in the
/// order the machine produced them, whoever fed the input. Events are not
/// queued: they go out while the lock is held, so a stream created after a
/// call never receives that call's events.
#[derive(Debug)]
pub(super) struct Core {
    pub(super) machine: Machine,
    pub(super) actions: VecDeque<Action>,
    /// The clocks at the last input, to notice a system sleep (Block 25).
    pub(super) clock: Clock,
    /// When each token refresh timer the driver runs comes due, so that a
    /// sleep's refreshes are given before the input revealing it (Block
    /// 25, D1).
    pub(super) refreshes: Refreshes,
    /// The timers set and not cancelled, to drop a stale firing.
    pub(super) armed: Armed,
    /// When the client was seen to become connected, while it is (Block
    /// 31, D2).
    pub(super) connected_since: Option<SystemTime>,
}

impl Core {
    /// `input`, unless it is the firing of a timer set again or cancelled
    /// since the setting `set`, by any task (see [`Armed`]); a timer whose
    /// firing is accepted no longer runs.
    fn accepted(&mut self, input: Input, set: Option<u64>) -> Option<Input> {
        if self.armed.superseded(&input, set) {
            return None;
        }
        self.armed.fired(&input);
        self.refreshes.fired(&input);
        Some(input)
    }
}

/// The wall-clock and monotonic times of the last input given the machine.
#[derive(Debug, Default)]
pub(super) struct Clock(Option<(SystemTime, platform::Monotonic)>);

impl Clock {
    /// How long the system slept since the last input, if it did.
    fn tick(&mut self) -> Option<Duration> {
        let now = platform::clocks();
        let gap = self.0.and_then(|last| sleep_gap(last, now));
        self.0 = Some(now);
        gap
    }
}

/// The sleep between `last` and `now`: how much further the wall clock
/// moved than the monotonic one, whose time stops in a suspend, when at
/// least 2 s (Block 25, D1). A wall clock set back gives none.
fn sleep_gap(
    last: (SystemTime, platform::Monotonic),
    now: (SystemTime, platform::Monotonic),
) -> Option<Duration> {
    let wall = now.0.duration_since(last.0).ok()?;
    let monotonic = now.1.saturating_duration_since(last.1);
    wall.checked_sub(monotonic)
        .filter(|gap| *gap >= SLEEP_THRESHOLD)
}

/// The smallest gap taken for a sleep, as the core ignores shorter ones.
const SLEEP_THRESHOLD: Duration = Duration::from_secs(2);

/// A timer of the machine, of any kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TimerKey {
    Client(Timer),
    Subscription(SubscriptionId, SubscriptionTimer),
    Call(CallId, CallTimer),
}

impl TimerKey {
    /// The timer whose firing `input` is.
    fn of(input: &Input) -> Option<Self> {
        match input {
            Input::TimerFired(timer) => Some(Self::Client(*timer)),
            Input::SubscriptionTimerFired(id, timer) => Some(Self::Subscription(*id, *timer)),
            Input::CallTimerFired(id, timer) => Some(Self::Call(*id, *timer)),
            _ => None,
        }
    }
}

/// The timers the machine has set and not cancelled, each with the number
/// of the setting: the driver numbers them the same way as it performs
/// the actions in order, so a firing whose timer was set again or
/// cancelled since is told apart, whichever task got the core first
/// (Block 25, D1).
#[derive(Debug, Default)]
pub(super) struct Armed {
    sets: u64,
    running: HashMap<TimerKey, u64>,
}

impl Armed {
    /// Follows `action`, which the driver will perform.
    fn note(&mut self, action: &Action) {
        let (key, set) = match action {
            Action::SetTimer(timer, _) => (TimerKey::Client(*timer), true),
            Action::CancelTimer(timer) => (TimerKey::Client(*timer), false),
            Action::SetSubscriptionTimer(id, timer, _) => {
                (TimerKey::Subscription(*id, *timer), true)
            }
            Action::CancelSubscriptionTimer(id, timer) => {
                (TimerKey::Subscription(*id, *timer), false)
            }
            Action::SetCallTimer(id, timer, _) => (TimerKey::Call(*id, *timer), true),
            Action::CancelCallTimer(id, timer) => (TimerKey::Call(*id, *timer), false),
            _ => return,
        };
        if set {
            self.sets += 1;
            self.running.insert(key, self.sets);
        } else {
            self.running.remove(&key);
        }
    }

    /// Whether `input`, the firing of the setting numbered `set`, belongs
    /// to a timer set again or cancelled since; an owner giving no number
    /// is trusted.
    fn superseded(&self, input: &Input, set: Option<u64>) -> bool {
        match (TimerKey::of(input), set) {
            (Some(key), Some(set)) => self.running.get(&key) != Some(&set),
            _ => false,
        }
    }

    /// `input` is the firing of a timer: it no longer runs.
    fn fired(&mut self, input: &Input) {
        if let Some(key) = TimerKey::of(input) {
            self.running.remove(&key);
        }
    }
}

/// The deadlines of the token refresh timers the driver runs, by
/// subscription (`None` for the connection's), kept beside the core: an
/// application task that reveals a sleep cannot reach the driver's
/// timers (Block 25, D1, D3).
#[derive(Debug, Default)]
pub(super) struct Refreshes(HashMap<Option<SubscriptionId>, platform::Instant>);

impl Refreshes {
    /// Follows `action`, which the driver will perform.
    fn note(&mut self, action: &Action) {
        match action {
            Action::SetTimer(Timer::Refresh, delay) => self.set(None, *delay),
            Action::SetSubscriptionTimer(id, SubscriptionTimer::Refresh, delay) => {
                self.set(Some(*id), *delay);
            }
            Action::CancelTimer(Timer::Refresh) => {
                self.0.remove(&None);
            }
            Action::CancelSubscriptionTimer(id, SubscriptionTimer::Refresh) => {
                self.0.remove(&Some(*id));
            }
            _ => {}
        }
    }

    fn set(&mut self, key: Option<SubscriptionId>, delay: Duration) {
        self.0.insert(key, platform::Instant::now() + delay);
    }

    /// Every refresh comes due `by` earlier, as the core's shortening of
    /// all of them on a sleep will make the driver's timers do.
    fn shorten_all(&mut self, by: Duration) {
        for deadline in self.0.values_mut() {
            *deadline = deadline
                .checked_sub(by)
                .unwrap_or_else(platform::Instant::now);
        }
    }

    /// `input` is the firing of a refresh timer: it no longer runs.
    fn fired(&mut self, input: &Input) {
        match input {
            Input::TimerFired(Timer::Refresh) => {
                self.0.remove(&None);
            }
            Input::SubscriptionTimerFired(id, SubscriptionTimer::Refresh) => {
                self.0.remove(&Some(*id));
            }
            _ => {}
        }
    }

    /// Removes the refresh timers due now, and names them.
    fn due(&mut self) -> Vec<Option<SubscriptionId>> {
        let now = platform::Instant::now();
        let mut due: Vec<_> = self
            .0
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(key, _)| *key)
            .collect();
        due.sort_unstable();
        for key in &due {
            self.0.remove(key);
        }
        due
    }
}

/// Async callback returning the data of a subscription's next subscribe
/// command, given its channel.
pub(super) type GetData =
    platform::ChannelCallback<platform::BoxFuture<'static, Result<Vec<u8>, DataError>>>;

/// Async callback returning a new subscription token, given the channel.
pub(super) type GetSubscriptionToken =
    platform::ChannelCallback<platform::BoxFuture<'static, Result<String, TokenError>>>;

/// Async callback loading the application's state for a channel, giving
/// the stream position it was loaded at.
pub(super) type GetState =
    platform::ChannelCallback<platform::BoxFuture<'static, Result<StreamPosition, StateError>>>;

/// Async callback signing keys of a shared poll subscription (Block 47,
/// D6).
#[cfg(feature = "unstable")]
pub(super) type GetSignature = platform::KeysCallback<
    platform::BoxFuture<'static, Result<crate::SharedPollSignature, TokenError>>,
>;

/// The callbacks of a subscription.
#[derive(Clone, Default)]
pub(super) struct Callbacks {
    pub(super) data: Option<GetData>,
    pub(super) token: Option<GetSubscriptionToken>,
    pub(super) state: Option<GetState>,
    #[cfg(feature = "unstable")]
    pub(super) signature: Option<GetSignature>,
}

/// Callbacks by subscription; opaque in `Debug` output.
#[derive(Default)]
pub(super) struct SubscriptionCallbacks(pub(super) HashMap<SubscriptionId, Callbacks>);

impl std::fmt::Debug for SubscriptionCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl Shared {
    pub(super) fn apply(&self, input: Input) {
        self.apply_fired(input, None);
    }

    /// As [`Shared::apply`], for the firing of the driver's timer of the
    /// setting `set` (see [`Armed`]).
    fn apply_fired(&self, input: Input, set: Option<u64>) {
        let mut core = lock(&self.core);
        // The driver has stopped (the client was dropped, after its final
        // disconnect, or its runtime shut down): actions would pile up with nothing to perform them,
        // e.g. from a subscription handle that outlived the client.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.handle_fired(&mut core, input, set);
        drop(core);
        self.actions_queued.notify_one();
    }

    /// Closes the client (Block 33, D1), in one locked section: an API
    /// call either runs before and is closed with the rest, or after and
    /// sees the client closed. The `ready()` waiters fail with "client
    /// closed" before the machine's new states would judge them.
    pub(super) fn shut_down(&self) {
        let mut core = lock(&self.core);
        if !self.closed.load(Ordering::Acquire) {
            lock(&self.ready).close();
            self.handle(&mut core, Input::Close);
            self.closed.store(true, Ordering::Release);
        }
        drop(core);
        self.actions_queued.notify_one();
        // A consumer left without a handler ends now (Codex, #169).
        super::handler::wake_consumer(self);
    }

    /// Resolves once the driver's task has ended, however it ended (Block
    /// 33, D1).
    pub(super) async fn ended(&self) {
        loop {
            // Registered before the check: a notification in between is
            // kept (tokio `Notify::notify_waiters`).
            let notified = self.driver_ended.notified();
            if self.driver_gone.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Wakes the driver for actions queued by another task.
    pub(super) fn notify_actions(&self) {
        self.actions_queued.notify_one();
    }

    /// The next action queued for the driver, for tests.
    #[cfg(all(test, adapter_tokio))]
    pub(super) fn next_action_for_tests(&self) -> Option<Action> {
        self.next_action()
    }

    /// Whether the driver was woken since this was last asked, for tests.
    #[cfg(all(test, adapter_tokio))]
    pub(super) fn woken(&self) -> bool {
        use futures_util::FutureExt as _;
        self.actions_queued.notified().now_or_never().is_some()
    }

    /// A network or page event a listener heard (Block 44, D10, D11).
    #[cfg(adapter_browser)]
    pub(super) fn heard(&self, heard: super::backend::events::Heard) {
        let mut core = lock(&self.core);
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let input = lock(&self.events).input(heard, core.machine.state());
        if let Some(input) = input {
            self.handle(&mut core, input);
        }
        drop(core);
        self.actions_queued.notify_one();
    }

    /// Feeds `input` to the machine: events go to their outboxes, taken at
    /// once where no stream takes them, other actions to the driver.
    pub(super) fn handle(&self, core: &mut Core, input: Input) {
        self.handle_fired(core, input, None);
    }

    /// As [`Shared::handle`], `input` being, when `set` is given, the
    /// firing of the driver's timer of that setting (see [`Armed`]).
    pub(super) fn handle_fired(&self, core: &mut Core, input: Input, set: Option<u64>) {
        // Application calls and stream reads feed the machine too, on the
        // application's tasks.
        let _entered = self.log.span.enter();
        // A sleep since the last input is the machine's first news of the
        // wake (Block 25, D1).
        let Some(gap) = core.clock.tick() else {
            self.give(core, input, set);
            return;
        };
        debug!(gap = ?gap, "system sleep detected");
        // As the core shortens every running refresh timer (D3).
        core.refreshes.shorten_all(gap);
        // While connected, the sleep may drop the connection: what the
        // server did before the wake stands, a terminal close or disconnect
        // push included, and an event taken is already the application's,
        // its position moving before the sleep can withdraw it. The
        // refreshes now due go first, also before the held messages taking
        // an event releases, so that a connect or subscribe the input
        // starts fetches a new token, and one it tears down makes the next
        // attempt fetch one (D3). While connecting, the sleep changes only
        // the refresh timers and goes first, so that a connect reply is not
        // judged by a sleep it did not live through. A `reconnect()` while
        // connected also goes first, so that it keeps its code and jitter
        // window rather than finding the client already reconnecting for
        // the sleep (D5; Codex, #122).
        let connected = core.machine.state() == State::Connected;
        match input {
            // Closing ends the client for good: the sleep it would have
            // revealed changes nothing after it, and must not reconnect or
            // fail calls with another code first (Block 33, D2; Codex,
            // #141).
            Input::Close | Input::DriverDropped { .. } => self.feed(core, input),
            Input::EventTaken(_)
            | Input::EventHanded(_)
            | Input::HandlerPanicked { .. }
            | Input::Frame { .. }
            | Input::TransportClosed { .. }
            | Input::Reconnect
                if connected =>
            {
                self.fire_due_refreshes(core);
                self.feed_before_sleep(core, input, gap);
            }
            input => {
                self.feed(core, Input::Slept { gap });
                // A firing whose timer the answer to the sleep set again is
                // stale; a refresh firing is given once.
                let input = core.accepted(input, set);
                self.fire_due_refreshes(core);
                if let Some(input) = input {
                    self.feed(core, input);
                }
            }
        }
        // A stream created or polled may be what revealed the sleep, with
        // no notification of its own.
        self.actions_queued.notify_one();
    }

    /// Gives the machine `input`, then the sleep of `gap` it revealed. The
    /// timers `input` set lost no time to the sleep: the sleep does not
    /// shorten them (Codex, #120).
    fn feed_before_sleep(&self, core: &mut Core, input: Input, gap: Duration) {
        let sets = core.armed.sets;
        self.feed(core, input);
        let fresh: HashSet<TimerKey> = core
            .armed
            .running
            .iter()
            .filter(|(_, set)| **set > sets)
            .map(|(key, _)| *key)
            .collect();
        let from = core.actions.len();
        self.feed(core, Input::Slept { gap });
        if fresh.is_empty() {
            return;
        }
        let answer: Vec<Action> = core.actions.drain(from..).collect();
        core.actions.extend(answer.into_iter().filter(|action| {
            let shortened = match action {
                Action::ShortenTimer(timer, _) => TimerKey::Client(*timer),
                Action::ShortenSubscriptionTimer(id, timer, _) => {
                    TimerKey::Subscription(*id, *timer)
                }
                _ => return true,
            };
            !fresh.contains(&shortened)
        }));
    }

    /// Gives the machine `input` unless it is a stale firing.
    fn give(&self, core: &mut Core, input: Input, set: Option<u64>) {
        if let Some(input) = core.accepted(input, set) {
            self.feed(core, input);
        }
    }

    /// Gives the machine the firings of the refreshes a sleep made due,
    /// before the input that revealed it, so that an attempt or a subscribe
    /// it starts fetches a new token first (Block 25, D3); the driver's
    /// timers for them are cancelled.
    fn fire_due_refreshes(&self, core: &mut Core) {
        for of in core.refreshes.due() {
            let (cancel, fired) = match of {
                None => (
                    Action::CancelTimer(Timer::Refresh),
                    Input::TimerFired(Timer::Refresh),
                ),
                Some(id) => (
                    Action::CancelSubscriptionTimer(id, SubscriptionTimer::Refresh),
                    Input::SubscriptionTimerFired(id, SubscriptionTimer::Refresh),
                ),
            };
            core.armed.note(&cancel);
            core.actions.push_back(cancel);
            self.feed(core, fired);
        }
    }

    /// Gives the machine `input`, and the inputs its events cause.
    fn feed(&self, core: &mut Core, input: Input) {
        let mut inputs = VecDeque::from([input]);
        while let Some(input) = inputs.pop_front() {
            self.log.input(&input);
            for action in core.machine.handle(input) {
                self.log.action(&action);
                core.armed.note(&action);
                core.refreshes.note(&action);
                let mut outboxes = lock(&self.outboxes);
                let taken = match action {
                    Action::Emit(id, event) => outboxes.client.offer(id, event),
                    Action::EmitSubscription(id, subscription, event) => {
                        match outboxes.subscriptions.get_mut(&subscription) {
                            Some(outbox) => outbox.offer(id, event),
                            None => Some(id),
                        }
                    }
                    #[cfg(feature = "unstable")]
                    Action::EmitMapSubscription(id, subscription, event) => {
                        match outboxes.maps.get_mut(&subscription) {
                            Some(outbox) => outbox.offer(id, event),
                            None => Some(id),
                        }
                    }
                    #[cfg(feature = "unstable")]
                    Action::EmitSharedPollSubscription(id, subscription, event) => {
                        match outboxes.shared_polls.get_mut(&subscription) {
                            Some(outbox) => outbox.offer(id, event),
                            None => Some(id),
                        }
                    }
                    Action::Withdraw(ids) => {
                        outboxes.withdraw(&ids);
                        None
                    }
                    action => {
                        core.actions.push_back(action);
                        None
                    }
                };
                inputs.extend(taken.map(Input::EventTaken));
            }
        }
        // On the states, not the events taken (Block 30, D4).
        ready::settle(core, &mut lock(&self.ready));
        #[cfg(adapter_browser)]
        lock(&self.events).follow(core.machine.state());
        let connected = core.machine.state() == State::Connected;
        if connected != core.connected_since.is_some() {
            core.connected_since = connected.then(platform::wall_clock);
        }
    }

    /// Gives the call `id` its outcome now, leaving the machine to be told;
    /// locks only the calls, after any other lock.
    pub(super) fn settle_call_now(&self, id: CallId, result: Result<CallReply, CallError>) {
        if let Some(sender) = lock(&self.calls).remove(&id) {
            // The future may be gone already.
            let _ = sender.send(result);
        }
    }

    /// Feeds [`Input::Flush`].
    fn flush(&self) {
        let mut core = lock(&self.core);
        self.handle(&mut core, Input::Flush);
    }

    fn next_action(&self) -> Option<Action> {
        let mut core = lock(&self.core);
        let action = core.actions.pop_front();
        if action.is_none() {
            core.actions.shrink_to(crate::RESTING_SLOTS);
        }
        action
    }

    /// Makes a call now, of `subscription` when given; the future gives its
    /// outcome. Dropping the future abandons the call (Block 13).
    pub(super) fn call(
        self: &Arc<Self>,
        subscription: Option<SubscriptionId>,
        request: CallRequest,
    ) -> impl Future<Output = Result<CallReply, CallError>> + MaybeSend + use<> {
        let (sender, receiver) = oneshot::channel();
        let pending = {
            let mut core = lock(&self.core);
            // The driver has stopped: nothing would ever settle the call.
            if self.closed.load(Ordering::Acquire) {
                None
            } else {
                let kind = request.name();
                let id = match subscription {
                    Some(subscription) => core.machine.add_subscription_call(subscription, request),
                    None => core.machine.add_call(request),
                };
                self.log.call(id, kind);
                lock(&self.calls).insert(id, sender);
                self.handle(&mut core, Input::Call(id));
                Some(PendingCall {
                    shared: Arc::clone(self),
                    id,
                    settled: false,
                })
            }
        };
        self.actions_queued.notify_one();
        let shared = Arc::clone(self);
        async move {
            let Some(mut pending) = pending else {
                return Err(client_closed());
            };
            let mut receiver = receiver;
            // Checked at every poll: a future first polled elsewhere may be
            // awaited in a handler later (Codex, #169). Abandoned, as a
            // dropped call is (Block 41, D6).
            let wait = super::handler::Wait::Call(pending.id);
            let result = std::future::poll_fn(|cx| {
                if super::handler::would_deadlock(&shared, wait) {
                    // Under `Thread`, settled meanwhile, it keeps its
                    // outcome (Codex, #171); under `Task` and `Inline` a
                    // marked call fails at once, whatever came (D6).
                    if !super::handler::in_thread_handler() {
                        return Poll::Ready(None);
                    }
                    return match Pin::new(&mut receiver).poll(cx) {
                        Poll::Ready(result) => Poll::Ready(Some(result)),
                        Poll::Pending => Poll::Ready(None),
                    };
                }
                Pin::new(&mut receiver).poll(cx).map(Some)
            })
            .await;
            let Some(result) = result else {
                return Err(super::handler::deadlock_error());
            };
            pending.settled = true;
            super::handler::unwatch(&shared, pending.id);
            // The sender was dropped unsettled only when the driver stopped.
            result.unwrap_or_else(|_| Err(client_closed()))
        }
    }
}

/// The error of a call on a closed client (Block 33, D4).
pub(super) fn client_closed() -> CallError {
    CallError::new(codes::error::CLIENT_CLOSED, "client closed")
}

/// The result `extract` takes from the reply of `call`.
pub(super) async fn reply_of<T>(
    call: impl Future<Output = Result<CallReply, CallError>>,
    extract: fn(CallReply) -> Option<T>,
) -> Result<T, CallError> {
    Ok(extract(call.await?).expect("the machine answers each call with its own method's result"))
}

/// A call awaiting its outcome: dropped before it, the call is abandoned.
struct PendingCall {
    shared: Arc<Shared>,
    id: CallId,
    settled: bool,
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        lock(&self.shared.calls).remove(&self.id);
        super::handler::unwatch(&self.shared, self.id);
        self.shared.apply(Input::CallAbandoned(self.id));
    }
}

/// A stream of the client's or a subscription's events, taking them from
/// its outbox: each event it yields is reported taken to the machine
/// under the core lock, so a withdrawal never races a take (Block 15).
pub(super) struct EventStream<E: Routed> {
    shared: Arc<Shared>,
    /// The subscription whose events it yields; the client's when `None`.
    of: Option<SubscriptionId>,
    number: u64,
    event: std::marker::PhantomData<fn() -> E>,
}

/// Events with an outbox: the client's and the subscriptions'.
pub(super) trait Routed: Sized {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>>;
    /// Forgets the finished outbox of subscription `of`.
    fn forget(outboxes: &mut Outboxes, of: SubscriptionId);
}

impl Routed for ClientEvent {
    fn outbox(outboxes: &mut Outboxes, _: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        Some(&mut outboxes.client)
    }

    fn forget(_: &mut Outboxes, _: SubscriptionId) {}
}

impl Routed for SubscriptionEvent {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        outboxes.subscriptions.get_mut(&of?)
    }

    fn forget(outboxes: &mut Outboxes, of: SubscriptionId) {
        outboxes.subscriptions.remove(&of);
    }
}

#[cfg(feature = "unstable")]
impl Routed for crate::MapSubscriptionEvent {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        outboxes.maps.get_mut(&of?)
    }

    fn forget(outboxes: &mut Outboxes, of: SubscriptionId) {
        outboxes.maps.remove(&of);
    }
}

#[cfg(feature = "unstable")]
impl Routed for crate::SharedPollSubscriptionEvent {
    fn outbox(outboxes: &mut Outboxes, of: Option<SubscriptionId>) -> Option<&mut Outbox<Self>> {
        outboxes.shared_polls.get_mut(&of?)
    }

    fn forget(outboxes: &mut Outboxes, of: SubscriptionId) {
        outboxes.shared_polls.remove(&of);
    }
}

impl<E: Routed> EventStream<E> {
    pub(super) fn new(shared: Arc<Shared>, of: Option<SubscriptionId>, number: u64) -> Self {
        Self {
            shared,
            of,
            number,
            event: std::marker::PhantomData,
        }
    }
}

impl<E: Routed> Stream for EventStream<E> {
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<E>> {
        let mut core = lock(&self.shared.core);
        let mut outboxes = lock(&self.shared.outboxes);
        let Some(outbox) = E::outbox(&mut outboxes, self.of) else {
            return Poll::Ready(None);
        };
        let polled = outbox.poll_take(self.number, cx);
        if outbox.is_finished()
            && let Some(of) = self.of
        {
            E::forget(&mut outboxes, of);
        }
        drop(outboxes);
        let Poll::Ready(Some((id, event))) = polled else {
            return polled.map(|_| None);
        };
        self.shared.handle(&mut core, Input::EventTaken(id));
        drop(core);
        self.shared.actions_queued.notify_one();
        Poll::Ready(Some(event))
    }
}

impl<E: Routed> Drop for EventStream<E> {
    // Dropping the active stream takes what it held, as with no stream.
    fn drop(&mut self) {
        let mut core = lock(&self.shared.core);
        let mut outboxes = lock(&self.shared.outboxes);
        let Some(outbox) = E::outbox(&mut outboxes, self.of) else {
            return;
        };
        let taken = outbox.drop_stream(self.number);
        if outbox.is_finished()
            && let Some(of) = self.of
        {
            E::forget(&mut outboxes, of);
        }
        drop(outboxes);
        // A superseded stream leaves the active one in place.
        let Some(taken) = taken else {
            return;
        };
        for id in taken {
            self.shared.handle(&mut core, Input::EventTaken(id));
        }
        self.shared.handle(&mut core, Input::StreamClosed(self.of));
        drop(core);
        // Held messages may now be handled and the reading resume.
        self.shared.actions_queued.notify_one();
    }
}

/// Async callback returning a fresh connection token.
type GetToken = platform::Callback<platform::BoxFuture<'static, Result<String, TokenError>>>;

/// Async callback returning the data of the next connect command.
type GetConnectData = platform::Callback<platform::BoxFuture<'static, Result<Vec<u8>, DataError>>>;

/// Builds a [`Client`] with options beyond [`Config`].
pub struct ClientBuilder {
    /// Transports to try in order, each with its endpoint.
    endpoints: Vec<(TransportKind, String)>,
    config: Config,
    get_token: Option<GetToken>,
    get_data: Option<GetConnectData>,
    emulation_endpoint: Option<String>,
    #[cfg(adapter_tokio)]
    /// Real HTTP headers, as given; checked by [`ClientBuilder::build`].
    http_headers: Vec<(String, String)>,
    /// Emulated headers, as given; checked by [`ClientBuilder::build`].
    emulated_headers: Vec<(String, String)>,
    #[cfg(adapter_tokio)]
    /// Extra root certificates, as given; checked by [`ClientBuilder::build`].
    root_certificates: Vec<CertificateDer<'static>>,
    #[cfg(adapter_tokio)]
    /// The application's TLS configuration, if any.
    tls_config: Option<Arc<rustls::ClientConfig>>,
    #[cfg(adapter_tokio)]
    proxy: Proxy,
    #[cfg(adapter_tokio)]
    /// Whether [`ClientBuilder::proxy`] was called: a connector replaces
    /// any proxy setting (Block 37, D4).
    proxy_set: bool,
    #[cfg(adapter_tokio)]
    /// The application's connector for the websocket, if any.
    connector: Option<connector::Shared>,
    #[cfg(adapter_tokio)]
    /// The runtime the client runs on; the current one otherwise.
    runtime: Option<Handle>,
    /// The client's handler, set once it is built (Block 41b); behind a
    /// mutex so that the builder stays `Sync` for a handler that is only
    /// `Send`.
    handler: Option<Mutex<Box<dyn super::ClientHandler>>>,
    /// Where its handlers run (Block 41, D2).
    event_delivery: super::EventDelivery,
    #[cfg(adapter_tokio)]
    /// The application's cookie jar (Block 43).
    cookie_jar: super::tokio::net::Jar,
    #[cfg(adapter_browser)]
    /// Where `offline` and `online` are listened to (Block 44, D10).
    network_event_target: Option<web_sys::EventTarget>,
    #[cfg(adapter_browser)]
    /// Where `pagehide` and `pageshow` are listened to (Block 44, D11).
    page_event_target: Option<web_sys::EventTarget>,
    #[cfg(adapter_browser)]
    /// The application's constructors in place of the page's (Block 44,
    /// D12).
    shims: super::backend::shims::Shims,
    #[cfg(adapter_browser)]
    /// Whether HTTP requests carry credentials to other origins (Block 44,
    /// D13).
    include_credentials: bool,
}

/// Endpoints as they may be logged (Block 21, D5).
struct Endpoints<'a>(&'a [(TransportKind, String)]);

impl std::fmt::Debug for Endpoints<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .map(|(kind, endpoint)| (kind, log::endpoint(endpoint))),
            )
            .finish()
    }
}

/// Header names, without their values, which may be secrets.
struct HeaderNames<'a>(&'a [(String, String)]);

impl std::fmt::Debug for HeaderNames<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| name))
            .finish()
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("ClientBuilder");
        f.field("endpoints", &Endpoints(&self.endpoints))
            .field("config", &self.config)
            .field("get_token", &self.get_token.is_some())
            .field("get_data", &self.get_data.is_some())
            .field(
                "emulation_endpoint",
                &self.emulation_endpoint.as_deref().map(log::endpoint),
            )
            .field("emulated_headers", &HeaderNames(&self.emulated_headers))
            .field("handler", &self.handler.is_some())
            .field("event_delivery", &self.event_delivery);
        #[cfg(adapter_tokio)]
        {
            f.field("http_headers", &HeaderNames(&self.http_headers))
                // Their number, and whether one is set: never their content.
                .field("root_certificates", &self.root_certificates.len())
                .field("tls_config", &self.tls_config.is_some())
                .field("proxy", &self.proxy)
                .field("proxy_set", &self.proxy_set)
                .field("connector", &self.connector.is_some())
                .field("runtime", &self.runtime.is_some());
            // Whether one is set: never its cookies (Block 43, D6).
            f.field("cookie_jar", &self.cookie_jar);
        }
        #[cfg(adapter_browser)]
        f.field("network_event_target", &self.network_event_target.is_some())
            .field("page_event_target", &self.page_event_target.is_some())
            .field("shims", &self.shims)
            .field("include_credentials", &self.include_credentials);
        f.finish()
    }
}

impl ClientBuilder {
    /// Sets the options; [`Config::default`] otherwise.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets the callback that provides a new connection token when the
    /// current one expires, or when there is none to connect with.
    /// Return [`TokenError::Unauthorized`] to disconnect the client for good.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, get_token: F) -> Self
    where
        F: Fn() -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.get_token = Some(Arc::new(move || Box::pin(get_token())));
        self
    }

    /// Sets the callback that provides the data of the next connect command,
    /// called before every connection attempt once a token is obtained; its
    /// data replaces [`Config::data`]. Return [`DataError::Unauthorized`] to
    /// disconnect the client for good; any other error is reported as an
    /// `error` event and the attempt retried after the reconnect backoff.
    #[must_use]
    pub fn get_data<F, Fut>(mut self, get_data: F) -> Self
    where
        F: Fn() -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Vec<u8>, DataError>> + MaybeSend + 'static,
    {
        self.get_data = Some(Arc::new(move || Box::pin(get_data())));
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Listens to `offline` and `online` on `target` instead of
    /// `globalThis` (Block 44, D10; centrifuge-js `networkEventTarget`):
    /// while the client is not disconnected, `offline` closes its
    /// connection, which then waits the reconnect delay, and `online`
    /// while connecting reconnects it at once, or after a draw from
    /// [`Config::reconnect_jitter_window`] when it lost its connection.
    /// A target that never fires, `EventTarget::new()`, turns this off.
    #[must_use]
    pub fn network_event_target(mut self, target: web_sys::EventTarget) -> Self {
        self.network_event_target = Some(target);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Listens to `pagehide` and `pageshow` on `target` instead of
    /// `globalThis` (Block 44, D11): a
    /// `pagehide` whose `persisted` is true, the page entering the
    /// back/forward cache, closes the connection with the reason "page
    /// entered back/forward cache", and the `pageshow` that restores the
    /// page reconnects as `online` does. Events whose `persisted` is false
    /// change nothing. A target that never fires turns this off.
    #[must_use]
    pub fn page_event_target(mut self, target: web_sys::EventTarget) -> Self {
        self.page_event_target = Some(target);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Opens websocket connections with `constructor`, called as one with
    /// the URL and the subprotocol, in place of the page's `WebSocket`
    /// (Block 44, D12; centrifuge-js `websocket`). The object it returns is
    /// used through its `WebSocket` methods and properties, by name.
    #[must_use]
    pub fn websocket(mut self, constructor: js_sys::Function) -> Self {
        self.shims.websocket = Some(constructor);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Makes the `http_stream` and sse transports' HTTP requests with
    /// `fetch`, called as the page's `fetch` is, `fetch(url, init)` with a
    /// plain `init`, in its place (Block 44, D12; centrifuge-js `fetch`).
    #[must_use]
    pub fn fetch(mut self, fetch: js_sys::Function) -> Self {
        self.shims.fetch = Some(fetch);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Reads the `http_stream` response through a stream built with
    /// `constructor`, called as `new ReadableStream(source, strategy)` is,
    /// in place of reading the body directly (Block 44, D12; centrifuge-js
    /// `readableStream`): its source's `pull` enqueues each chunk of the
    /// body unchanged, with `highWaterMark` 0, so the body is still read
    /// only when the client wants messages. With it the page needs no
    /// global `ReadableStream` for `http_stream`.
    #[must_use]
    pub fn readable_stream(mut self, constructor: js_sys::Function) -> Self {
        self.shims.readable_stream = Some(constructor);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Opens sse streams with `constructor`, called as one with the URL and
    /// `{ withCredentials }`, in place of the page's `EventSource` (Block
    /// 44, D12; centrifuge-js `eventsource`). The object it returns is used
    /// through its `EventSource` methods and properties, by name.
    ///
    /// Each attempt uses the shim given, else the page's global of that
    /// name; a transport whose constructors neither provides is skipped,
    /// and [`ClientBuilder::build`] fails when no configured transport can
    /// be used: websocket needs `WebSocket`, `http_stream` `fetch`,
    /// `ReadableStream` and `AbortController`, sse `EventSource` and
    /// `fetch`.
    #[must_use]
    pub fn event_source(mut self, constructor: js_sys::Function) -> Self {
        self.shims.event_source = Some(constructor);
        self
    }

    #[cfg(adapter_browser)]
    #[cfg_attr(docsrs, doc(cfg(all(target_arch = "wasm32", target_os = "unknown"))))]
    /// Whether the `http_stream` and sse transports' requests carry the
    /// page's cookies and HTTP authentication to a server on another origin
    /// (Block 44, D13), false by default: true sends fetch's `credentials:
    /// "include"` and `EventSource`'s `withCredentials: true`, so the
    /// server's `Set-Cookie` is stored too; false sends `credentials:
    /// "same-origin"` and `withCredentials: false`, as centrifuge-js does.
    /// The server must allow credentialed CORS requests from the page's
    /// origin, as Centrifugo's `allowed_origins` does.
    ///
    /// The websocket upgrade is unaffected: the browser sends its cookies by
    /// their `SameSite` rules whatever this says.
    #[must_use]
    pub fn include_credentials(mut self, include: bool) -> Self {
        self.include_credentials = include;
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Trusts `roots`, certificate authorities, in addition to the
    /// platform's, for `wss://` and every `https://` request (Block 22);
    /// they add to the roots of earlier calls. A certificate that is not a
    /// valid root makes [`ClientBuilder::build`] fail, naming its index.
    ///
    /// PEM files parse with [`rustls::pki_types::pem::PemObject`]:
    ///
    /// ```no_run
    /// use ferrifuge::Client;
    /// use ferrifuge::rustls::pki_types::CertificateDer;
    /// use ferrifuge::rustls::pki_types::pem::PemObject;
    ///
    /// # fn run(pem: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    /// let roots = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
    /// let client = Client::builder("wss://example.com/connection/websocket")
    ///     .root_certificates(roots)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Each client given extra roots holds its own TLS settings: many
    /// clients should rather share one [`ClientBuilder::tls_config`].
    #[must_use]
    pub fn root_certificates(
        mut self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.root_certificates.extend(roots);
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Uses `config` for `wss://` and every `https://` request, instead of
    /// the platform's trust (Block 22): its roots, verifier, provider and
    /// client certificates are the application's. Ferrifuge only replaces
    /// its ALPN list with `http/1.1`, the one protocol of its transports.
    /// [`ClientBuilder::build`] fails when extra root certificates are also
    /// given, and, in the `fips` build, when `config.fips()` is false.
    ///
    /// The configuration is shared as given, so many clients can use one.
    #[must_use]
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Sets how the client reaches the server's hosts (Block 23): through
    /// the system's proxy by default ([`Proxy::system`]), the environment's,
    /// none, one proxy, or one chosen per endpoint. Every transport takes
    /// the same decision for each URL it opens.
    #[must_use]
    pub fn proxy(mut self, proxy: Proxy) -> Self {
        self.proxy = proxy;
        self.proxy_set = true;
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Sets the connector that gives the websocket its stream, for
    /// networking the client does not have (Block 37): a VPN tunnel, a
    /// custom proxy, an in-process transport. Over its stream the client
    /// runs TLS, for `wss://`, and the websocket upgrade, as over a stream
    /// it dials. With a connector the client reads no proxy setting, the
    /// environment's and the system's included. A connector carries the
    /// websocket only: [`ClientBuilder::build`] fails with an `http_stream` or
    /// sse endpoint, or after [`ClientBuilder::proxy`].
    #[must_use]
    pub fn connector(mut self, connector: impl Connector + 'static) -> Self {
        self.connector = Some(connector::Shared(Arc::new(connector)));
        self
    }

    /// Sets the cookie jar that keeps the cookies servers set and sends
    /// them back on the later requests they match (Block 43): the
    /// websocket upgrade, a refused one's response included. A later call
    /// replaces an earlier one; the application keeps its own `Arc` to look
    /// into the jar or seed it. The cookies merge with a `Cookie` header of
    /// [`ClientBuilder::http_headers`] into one header, the application's
    /// first. No proxy is sent them. Without a jar, no cookie is kept.
    #[cfg(all(adapter_tokio, feature = "cookies"))]
    #[cfg_attr(docsrs, doc(cfg(feature = "cookies")))]
    #[must_use]
    pub fn cookie_jar(mut self, jar: Arc<dyn super::CookieJar>) -> Self {
        self.cookie_jar = super::tokio::net::Jar(Some(jar));
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Adds real HTTP headers to the websocket upgrade request and to every
    /// request of `http_stream` and sse, so that a gateway checking one
    /// still admits a client that fell back (Block 17). Names are
    /// case-insensitive, a later value replacing an earlier one. A name or
    /// value that is not valid HTTP, or a header the transport sets itself
    /// (`Host`, `Connection`, `Upgrade`, `Content-Length`,
    /// `Transfer-Encoding`, `Content-Type`, `Accept`, `Sec-WebSocket-*`,
    /// `Proxy-Authorization`), makes [`ClientBuilder::build`] fail.
    /// [`Client::set_http_headers`] replaces them on a built client.
    #[must_use]
    pub fn http_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.http_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    /// Adds emulated headers, sent in every connect command
    /// (`ConnectRequest.headers`), which the server forwards to its proxies
    /// as HTTP headers when its `emulated_headers` lists them (centrifuge-js
    /// `headers`). They are client-controlled input to those proxies. Names
    /// and values must be valid HTTP, or [`ClientBuilder::build`] fails.
    #[must_use]
    pub fn emulated_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.emulated_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    /// Sets the absolute URL of the emulation endpoint, where commands over
    /// `http_stream` and sse go, e.g. when the server is behind a path
    /// prefix; by default `/emulation` on the server of the stream endpoint
    /// (centrifuge-js `emulationEndpoint`).
    #[must_use]
    pub fn emulation_endpoint(mut self, url: impl Into<String>) -> Self {
        self.emulation_endpoint = Some(url.into());
        self
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Runs the client on `runtime` (Block 28), e.g. when building it where
    /// no tokio runtime is current, as in a Tauri setup hook:
    ///
    /// ```no_run
    /// # fn run(handle: tokio::runtime::Handle) -> Result<(), ferrifuge::ClientError> {
    /// // In Tauri: `tauri::async_runtime::handle().inner().clone()`.
    /// let client = ferrifuge::Client::builder("wss://example.com/connection/websocket")
    ///     .runtime(handle)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default the client runs on the runtime current when it is built.
    /// The runtime needs time and I/O enabled (`enable_all`). A
    /// current-thread runtime runs the client only while a thread is inside
    /// its `Runtime::block_on`: meanwhile calls and streams wait, and no
    /// timeout runs. When the runtime shuts down, the client emits
    /// `disconnected` with code 4 "runtime shut down"
    /// ([`crate::codes::disconnected::RUNTIME_SHUT_DOWN`]), its calls fail
    /// and its streams end; the client's futures and streams themselves need
    /// no runtime, so any executor can await them.
    #[must_use]
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Creates the disconnected client.
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error when the options cannot
    /// work together, e.g. the Protobuf protocol with the sse transport.
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also when there is no runtime to run on: none was given with",
        doc = "[`ClientBuilder::runtime`] and none is current, or it has shut down."
    )]
    #[cfg_attr(
        adapter_browser,
        doc = "In the browser the client runs on the page's event loop, which is",
        doc = "always there (Block 44, D4)."
    )]
    pub fn build(mut self) -> Result<Client, ClientError> {
        let handler = self.handler.take();
        let client = Client::start(self)?;
        if let Some(handler) = handler {
            let handler = handler.into_inner().unwrap_or_else(PoisonError::into_inner);
            super::handler::set_client_handler(&client.shared, handler);
        }
        Ok(client)
    }

    /// The client's handler, which takes its events from the start instead
    /// of a stream (Block 41, D1); see [`Client::set_handler`].
    #[must_use]
    pub fn handler(mut self, handler: impl super::ClientHandler) -> Self {
        self.handler = Some(Mutex::new(Box::new(handler)));
        self
    }

    /// Where the handlers of the client and of its subscriptions run
    /// (Block 41, D2): [`EventDelivery::Task`](super::EventDelivery::Task)
    /// by default, or on the client's driver with
    /// [`EventDelivery::Inline`](super::EventDelivery::Inline).
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also on a thread of the client's own, with",
        doc = "[`EventDelivery::Thread`](super::EventDelivery::Thread)."
    )]
    #[must_use]
    pub fn event_delivery(mut self, delivery: super::EventDelivery) -> Self {
        self.event_delivery = delivery;
        self
    }
}

impl Client {
    /// Creates a disconnected client for the websocket `endpoint`, e.g.
    /// `ws://localhost:8000/connection/websocket`.
    ///
    /// # Errors
    ///
    /// As [`ClientBuilder::build`], e.g. for an empty endpoint or an
    /// `http://` one.
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also when called outside a tokio runtime: the client runs as",
        doc = "a task on the current one ([`ClientBuilder::runtime`] takes another)."
    )]
    pub fn new(endpoint: impl Into<String>, config: Config) -> Result<Self, ClientError> {
        Self::builder(endpoint).config(config).build()
    }

    /// Starts building a client for the websocket `endpoint`.
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        Self::builder_with_transports([(TransportKind::WebSocket, endpoint.into())])
    }

    /// Starts building a client that tries the `endpoints` in order on its
    /// first connection, e.g. a websocket endpoint, then `http_stream`
    /// and `sse` ones as fallbacks; its reconnects keep the transport that
    /// worked.
    pub fn builder_with_transports(
        endpoints: impl IntoIterator<Item = (TransportKind, String)>,
    ) -> ClientBuilder {
        ClientBuilder {
            endpoints: endpoints.into_iter().collect(),
            config: Config::default(),
            get_token: None,
            get_data: None,
            emulation_endpoint: None,
            #[cfg(adapter_tokio)]
            http_headers: Vec::new(),
            emulated_headers: Vec::new(),
            #[cfg(adapter_tokio)]
            root_certificates: Vec::new(),
            #[cfg(adapter_tokio)]
            tls_config: None,
            #[cfg(adapter_tokio)]
            proxy: Proxy::default(),
            #[cfg(adapter_tokio)]
            proxy_set: false,
            #[cfg(adapter_tokio)]
            connector: None,
            #[cfg(adapter_tokio)]
            runtime: None,
            handler: None,
            event_delivery: super::EventDelivery::default(),
            #[cfg(adapter_tokio)]
            cookie_jar: super::tokio::net::Jar::default(),
            #[cfg(adapter_browser)]
            network_event_target: None,
            #[cfg(adapter_browser)]
            page_event_target: None,
            #[cfg(adapter_browser)]
            shims: super::backend::shims::Shims::default(),
            #[cfg(adapter_browser)]
            include_credentials: false,
        }
    }

    #[allow(clippy::too_many_lines)] // The settings of both adapters.
    fn start(builder: ClientBuilder) -> Result<Self, ClientError> {
        let ClientBuilder {
            endpoints,
            config,
            get_token,
            get_data,
            emulation_endpoint,
            #[cfg(adapter_tokio)]
            http_headers,
            emulated_headers,
            #[cfg(adapter_tokio)]
            root_certificates,
            #[cfg(adapter_tokio)]
            tls_config,
            #[cfg(adapter_tokio)]
            proxy,
            #[cfg(adapter_tokio)]
            proxy_set,
            #[cfg(adapter_tokio)]
            connector,
            #[cfg(adapter_tokio)]
            runtime,
            handler: _,
            event_delivery,
            #[cfg(adapter_tokio)]
            cookie_jar,
            #[cfg(adapter_browser)]
            network_event_target,
            #[cfg(adapter_browser)]
            page_event_target,
            #[cfg(adapter_browser)]
            shims,
            #[cfg(adapter_browser)]
            include_credentials,
        } = builder;
        #[cfg(adapter_tokio)]
        check_endpoints(&endpoints, connector.is_some(), proxy_set)?;
        #[cfg(adapter_browser)]
        for (kind, endpoint) in &endpoints {
            check_endpoint(*kind, endpoint)?;
        }
        #[cfg(adapter_browser)]
        check_usable(&endpoints, &shims)?;
        #[cfg(adapter_tokio)]
        let android = cfg!(target_os = "android");
        #[cfg(adapter_tokio)]
        let trust = trust(root_certificates, tls_config, android)?;
        #[cfg(all(adapter_tokio, feature = "unstable"))]
        check_webtransport(&endpoints, &trust)?;
        #[cfg(adapter_tokio)]
        android_gate(
            android && !tls::android_ready(),
            &trust,
            &endpoints,
            emulation_endpoint.as_deref(),
            &proxy,
        )?;
        #[cfg(adapter_tokio)]
        let http_headers = HeaderSet::new(http_header_map(&http_headers)?);
        let emulated_headers = emulated_header_map(&emulated_headers)?;
        check_limits(&config)?;
        let subprotocol = config.protocol.websocket_subprotocol();
        let timeout = config.timeout;
        let max_message = config.max_message_bytes;
        #[cfg(adapter_browser)]
        let read_ahead = super::backend::websocket::Bound {
            messages: config.read_ahead_messages,
            bytes: config.read_ahead_bytes,
        };
        let machine = machine_for(
            config,
            get_token.is_some(),
            get_data.is_some(),
            emulated_headers,
            endpoints.iter().map(|(kind, _)| *kind).collect(),
        )?;
        #[cfg(adapter_tokio)]
        let runtime = runtime_or_current(runtime)?;
        // The page's event loop needs no handle (Block 44, D4).
        #[cfg(adapter_browser)]
        let runtime = Handle;
        let shared = Arc::new_cyclic(|weak| {
            #[cfg(not(adapter_browser))]
            let _ = weak;
            Shared {
                core: Mutex::new(Core::new(machine)),
                outboxes: Mutex::new(Outboxes::default()),
                callbacks: Mutex::new(SubscriptionCallbacks::default()),
                calls: Mutex::new(HashMap::new()),
                ready: Mutex::new(Waiters::default()),
                driver_gone: AtomicBool::new(false),
                driver_ended: Notify::new(),
                actions_queued: Notify::new(),
                #[cfg(feature = "__test-hooks")]
                held_debounce_timers: Mutex::new(None),
                closed: AtomicBool::new(false),
                log: Log::new(endpoints.iter().map(|(_, endpoint)| endpoint.as_str())),
                #[cfg(adapter_tokio)]
                network: Arc::new(
                    Network::new(Tls::new(trust), proxy)
                        .with_connector(connector)
                        .with_cookie_jar(cookie_jar),
                ),
                #[cfg(adapter_tokio)]
                http_headers,
                handlers: Mutex::new(super::handler::Handlers::new(event_delivery)),
                runtime: runtime.clone(),
                #[cfg(adapter_browser)]
                events: Mutex::new(super::backend::events::Events::new(
                    network_event_target,
                    page_event_target,
                    std::sync::Weak::clone(weak),
                )),
            }
        });
        let driver = Driver {
            endpoints: endpoints
                .into_iter()
                .map(|(_, endpoint)| endpoint)
                .collect(),
            subprotocol,
            emulation_endpoint,
            timeout,
            max_message,
            #[cfg(adapter_browser)]
            read_ahead,
            #[cfg(adapter_browser)]
            shims,
            #[cfg(adapter_browser)]
            include_credentials,
            shared: Arc::clone(&shared),
            runtime: runtime.clone(),
            _ending: Ending(Arc::clone(&shared)),
            delivery: event_delivery,
            closing: Vec::new(),
            opening: None,
            transport: None,
            kind: TransportKind::WebSocket,
            reading: true,
            get_token,
            get_data,
            fetching: None,
            stale_fetching: None,
            timers: Timers::default(),
            subscription_timers: KeyedTimers::default(),
            call_timers: KeyedTimers::default(),
            debounce_timers: KeyedTimers::default(),
            shared_poll_timers: SharedPollTimers::default(),
            shared_poll_refreshes: SharedPollRefreshes::default(),
            timer_sets: 0,
            callback_fetches: FuturesUnordered::new(),
            callback_aborts: HashMap::new(),
        };
        let span = shared.log.span.clone();
        platform::spawn(&runtime, driver.run().instrument(span));
        // A runtime that has shut down drops the task at once, and its
        // guard closes the client (Block 28, D5).
        if shared.closed.load(Ordering::Acquire) {
            return Err(runtime_error("the tokio runtime has shut down"));
        }
        Ok(Self::owning(shared))
    }

    /// The first clone of the client `shared` drives.
    fn owning(shared: Arc<Shared>) -> Self {
        Self {
            owner: Arc::new(Owner {
                shared: Arc::clone(&shared),
            }),
            shared,
        }
    }

    /// A handle that does not keep this client alive (Block 41, D5).
    #[must_use]
    pub fn downgrade(&self) -> WeakClient {
        WeakClient {
            owner: Arc::downgrade(&self.owner),
        }
    }

    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// The number of this client in its log records' `client` field
    /// (Block 21), for tests.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn log_number(&self) -> u64 {
        self.shared.log.number
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        lock(&self.shared.core).machine.state()
    }

    /// Closes the client for good (Block 33): every pending call and
    /// `ready()` fails with code 4 "client closed", every subscription emits
    /// `unsubscribed` with code 2 "client closed" (server-side ones too),
    /// then the client emits `disconnected` with code 0 unless it was
    /// disconnected; no unsubscribe command is sent. Later calls fail at
    /// once with code 4, `connect()` and the setters do nothing, and
    /// `new_subscription` returns [`crate::SubscriptionError::ClientClosed`].
    ///
    /// Closing starts when this returns; the future resolves once the
    /// driver has stopped: the transport closed (within `Config::timeout`)
    /// and every stream ended after the events already queued. Dropping the
    /// future, or never awaiting it, still closes the client. A second call
    /// changes nothing. Dropping the client closes it the same way, without
    /// waiting.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) {
    /// client.close().await;
    /// assert!(client.is_closed());
    /// # }
    /// ```
    pub fn close(&self) -> impl Future<Output = ()> + MaybeSend + use<> {
        self.shared.shut_down();
        let shared = Arc::clone(&self.shared);
        async move {
            // From one of its own handlers, closing has started: waiting for
            // the driver could wait on the worker the handler holds (Block
            // 41, D6); under `Thread` the driver stops whatever it does.
            // Checked at every poll, as a call is (Codex, #169).
            let ended = shared.ended();
            let mut ended = std::pin::pin!(ended);
            std::future::poll_fn(|cx| {
                if super::handler::would_deadlock(&shared, super::handler::Wait::Close) {
                    return Poll::Ready(());
                }
                ended.as_mut().poll(cx)
            })
            .await;
        }
    }

    /// Whether the client is closed: by [`Client::close`], its runtime shut
    /// down or its driver panicked (Block 33). A closed client reports
    /// `disconnected` from [`Client::state`].
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// A snapshot of the client's connection and traffic counters since it
    /// was built (Block 31), never a round trip. A closed client keeps its
    /// last counters.
    ///
    /// ```no_run
    /// # fn run(client: &ferrifuge::Client) {
    /// let statistics = client.statistics();
    /// println!("{:?} over {:?}", statistics.state, statistics.transport);
    /// # }
    /// ```
    #[must_use]
    pub fn statistics(&self) -> crate::Statistics {
        let core = lock(&self.shared.core);
        let mut statistics = core.machine.statistics();
        statistics.connected_since = core.connected_since;
        statistics
    }

    /// Waits until the client is connected, without taking an event from
    /// its stream (Block 30). The wait starts when this method returns, and
    /// the future borrows nothing from the client, so it can be spawned.
    ///
    /// It follows the state [`Client::state`] reports, not the events
    /// taken: it can resolve before the `connected` event is taken, and a
    /// state that lasts only within one message is not seen. The state moves
    /// only as the client handles messages, each after the events of earlier
    /// ones were taken (Block 15), so a task holding the client's stream with
    /// events not taken, and awaiting this without reading it, waits until
    /// the stream is read or dropped. No timeout applies; wrap the future in
    /// one:
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) {
    /// use std::time::Duration;
    ///
    /// client.connect();
    /// match tokio::time::timeout(Duration::from_secs(10), client.ready()).await {
    ///     Ok(Ok(())) => println!("connected"),
    ///     Ok(Err(error)) => println!("will not connect: {error}"),
    ///     Err(_) => println!("still connecting"),
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Code 3 "disconnected" (`codes::error::DISCONNECTED`) at once when the
    /// client is disconnected, `connect()` never called included; and when
    /// the client enters `disconnected` while connecting: `disconnect()`, a
    /// terminal code, a denied token. Code 4 "client closed"
    /// (`codes::error::CLIENT_CLOSED`) when the client is closed, its last
    /// clone dropped or its runtime shut down included (Block 33).
    pub fn ready(&self) -> impl Future<Output = Result<(), CallError>> + MaybeSend + use<> {
        ready::ready(&self.shared, Of::Client)
    }

    /// Sets the client's handler (Block 41, D1): from now on it takes the
    /// client's events, server-side subscriptions' included, one at a time,
    /// in order with those of its subscriptions' handlers, as a later
    /// [`Client::events`] call would; the earlier stream or handler gets no
    /// more. Its events count as delivered when its method returns.
    pub fn set_handler(&self, handler: impl super::ClientHandler) {
        super::handler::set_client_handler(&self.shared, Box::new(handler));
    }

    /// Events from now on. An event counts as delivered once this stream
    /// yields it; until a stream is taken, events count as delivered at
    /// once, so take it before calling [`Client::connect`] to get every
    /// one. A later call takes over: the earlier stream ends and the events
    /// it had not yielded are the new one's (Block 15). The stream borrows
    /// nothing from the client, so it can be kept beside it, and it ends
    /// once the client is dropped or its runtime shuts down (Block 28).
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + MaybeSend + Unpin + use<> {
        let mut core = lock(&self.shared.core);
        let number = lock(&self.shared.outboxes).client.open_stream();
        self.shared.handle(&mut core, Input::StreamOpened(None));
        drop(core);
        EventStream::new(Arc::clone(&self.shared), None, number)
    }

    /// Messages read from the server and not yet handled, and their bytes
    /// (Block 15), so that tests can wait for the read-ahead to fill. Only
    /// with the `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn read_ahead(&self) -> (usize, usize) {
        lock(&self.shared.core).machine.read_ahead()
    }

    /// Events of the client queued for its streams and not yet taken, so
    /// that tests can wait for them. Only with the `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_events(&self) -> usize {
        lock(&self.shared.outboxes).client.held()
    }

    /// Leaves the debounce timers set from now on unarmed, until
    /// [`Client::fire_held_debounce_timers`], so that a test decides when a
    /// window ends (Block 46). Only with the `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn hold_debounce_timers(&self) {
        *lock(&self.shared.held_debounce_timers) = Some(Vec::new());
    }

    /// How many debounce timers are held unarmed. Only with the
    /// `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_debounce_timers(&self) -> usize {
        lock(&self.shared.held_debounce_timers)
            .as_ref()
            .map_or(0, Vec::len)
    }

    /// Fires the debounce timers held so far, oldest first, as if each had
    /// run its interval; timers set later stay held. Only with the
    /// `__test-hooks` feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    pub fn fire_held_debounce_timers(&self) {
        let held = lock(&self.shared.held_debounce_timers)
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for timer in held {
            self.shared.apply(Input::DebounceTimerFired(timer));
        }
    }

    /// Starts connecting; reconnects until connected or disconnected. The
    /// state is [`State::Connecting`] when this returns.
    pub fn connect(&self) {
        let mut core = lock(&self.shared.core);
        if self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        self.shared.handle(&mut core, Input::Connect);
        super::handler::start_again(&self.shared, &mut core);
        drop(core);
        self.shared.actions_queued.notify_one();
    }

    /// Connects again at once, as an app does on its platform's wake or
    /// network-change notification (macOS `NSWorkspace.didWakeNotification`,
    /// Windows `WM_POWERBROADCAST`, systemd-logind `PrepareForSleep`,
    /// Android `ConnectivityManager.NetworkCallback`, iOS `NWPathMonitor`).
    ///
    /// A connected client closes its transport and emits
    /// [`ClientEvent::Connecting`] with code
    /// [`codes::connecting::RECONNECT_CALLED`], then connects after the
    /// reconnect jitter window only, without the backoff, so a fleet told
    /// at once does not reconnect at once; a connecting client starts a new
    /// attempt at once, with no event. Either way the attempt counter
    /// restarts, the transports are tried again from the first, and every
    /// client's next attempt reads the environment's and the operating
    /// system's proxy settings again. A disconnected client is left alone:
    /// call [`Client::connect`] to start it.
    ///
    /// Calls waiting for the connection keep waiting; calls sent on the
    /// closed transport fail with code [`codes::error::CONNECTION_CLOSED`].
    /// Publications not yet taken are withdrawn and, on recoverable
    /// channels, recovered by the next connection.
    pub fn reconnect(&self) {
        #[cfg(adapter_tokio)]
        super::tokio::proxy::read_settings_again();
        self.shared.apply(Input::Reconnect);
    }

    /// Disconnects and stops reconnecting. The state is
    /// [`State::Disconnected`] when this returns. Every pending call fails
    /// with code [`codes::error::DISCONNECTED`].
    pub fn disconnect(&self) {
        self.shared.apply(Input::Disconnect);
    }

    /// Starts a batch: until it stops, the application's calls (but
    /// `send`) and the subscribe and unsubscribe commands its
    /// `subscribe()`, `unsubscribe()` and subscription removal cause are
    /// held, then sent in order in frames of several commands, each within
    /// [`Config::batch_frame_bytes`] (centrifuge-js `startBatching`). The
    /// client's own commands are never held. A held call still fails with
    /// the timeout error when not sent within `Config::timeout`; a held
    /// subscribe's reply deadline runs too. Several batches may be open:
    /// stopping one sends what is held so far.
    #[must_use = "the batch stops when dropped"]
    pub fn start_batching(&self) -> Batch {
        self.shared.apply(Input::StartBatching);
        Batch {
            shared: Arc::clone(&self.shared),
            stopped: false,
        }
    }

    /// Sets the connection token used from the next connection attempt,
    /// sending nothing now; empty for none (centrifuge-js `setToken`).
    /// Without a token callback, a token the server rejected as expired
    /// (error 109) is then replaced by this one, instead of the client
    /// stopping on a configuration error; with one, the next attempt still
    /// calls it. A connected client's refresh needs the callback.
    pub fn set_token(&self, token: impl Into<String>) {
        self.shared.apply(Input::SetToken(token.into()));
    }

    /// Sets the connect data used from the next connection attempt, sending
    /// nothing now; a [`ClientBuilder::get_data`] callback still replaces it
    /// on every attempt (centrifuge-js `setData`).
    pub fn set_data(&self, data: impl Into<Vec<u8>>) {
        self.shared.apply(Input::SetData(data.into()));
    }

    /// Replaces the emulated headers sent from the next connection attempt,
    /// sending nothing now (centrifuge-js `setHeaders`).
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error naming a header whose
    /// name or value is not valid HTTP, as [`ClientBuilder::emulated_headers`];
    /// the headers are then left as they were.
    pub fn set_emulated_headers<I, K, V>(&self, headers: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let headers: Vec<(String, String)> = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        let headers = emulated_header_map(&headers)?;
        self.shared.apply(Input::SetEmulatedHeaders(headers));
        Ok(())
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Replaces the real HTTP headers (Block 32): every HTTP request started
    /// from now on carries the new set, the upgrade or stream request of
    /// the next attempt and each emulation request of an open `http_stream`
    /// or sse transport. The open connection is kept, and a request already
    /// started keeps the headers it started with. A header missing from the
    /// new set is no longer sent; a name given twice keeps its later value.
    ///
    /// ```no_run
    /// # fn run(client: &ferrifuge::Client, token: &str) -> Result<(), ferrifuge::ClientError> {
    /// client.set_http_headers([("Authorization", format!("Bearer {token}"))])?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error naming a header, never its
    /// value, as [`ClientBuilder::http_headers`] refuses it: a name or value
    /// that is not valid HTTP, or a header the transport sets,
    /// `Proxy-Authorization` included. The headers are then left as they
    /// were.
    pub fn set_http_headers<I, K, V>(&self, headers: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let headers: Vec<(String, String)> = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        let headers = http_header_map(&headers)?;
        self.shared.http_headers.replace(headers);
        Ok(())
    }

    #[cfg(adapter_tokio)]
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Replaces the proxy setting from the next connection attempt
    /// (Block 23), as the other setters do; the current connection is
    /// kept. A client given a connector reads no proxy setting: this
    /// changes nothing (Block 37).
    pub fn set_proxy(&self, proxy: Proxy) {
        self.shared.network.set_proxy(proxy);
    }

    /// Calls the server's RPC `method` with `data`; the future gives the
    /// result data: raw JSON with the JSON protocol, any bytes with
    /// Protobuf.
    ///
    /// The call is made when this returns: a call made while not connected
    /// waits for the connection, then for the reply, each for at most
    /// `Config::timeout`. The future borrows nothing from the client, so it
    /// can be spawned. Results are not ordered with events: a reply settles
    /// its call as soon as it is read. Dropping the future abandons the
    /// call: if it is still waiting for the connection, it is never sent.
    ///
    /// # Errors
    ///
    /// The server's error, with its code, message and `temporary` flag, or
    /// a client-side one: [`codes::error::TIMEOUT`],
    /// [`codes::error::DISCONNECTED`] when the client enters
    /// `disconnected`, including when the `Client` is dropped,
    /// [`codes::error::CONNECTION_CLOSED`] when the transport the call was
    /// sent on closes, and [`codes::error::TRANSPORT_WRITE`] when the
    /// server refuses the request carrying it (HTTP 413).
    pub fn rpc(
        &self,
        method: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<u8>, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Rpc {
                method: method.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| match reply {
            CallReply::Rpc(data) => Some(data),
            _ => None,
        })
    }

    /// Calls the server's RPC `method` with `data` encoded as JSON, and
    /// decodes the result data as `Resp` (Block 38). Made, waiting and
    /// failing as [`Client::rpc`]; `data` is encoded when this returns, so
    /// the future borrows nothing. Under the Protobuf protocol the data are
    /// the JSON bytes. For an RPC that returns no data, use
    /// [`Client::rpc`]: empty data is not JSON.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) -> Result<(), ferrifuge::JsonCallError> {
    /// #[derive(serde::Serialize)]
    /// struct Terms {
    ///     a: u32,
    ///     b: u32,
    /// }
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Sum {
    ///     sum: u32,
    /// }
    ///
    /// let sum: Sum = client.rpc_json("sum", &Terms { a: 1, b: 2 }).await?;
    /// println!("{}", sum.sum);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of [`Client::rpc`];
    /// [`JsonCallError::Decode`] when the server answered with data that
    /// does not decode as `Resp`: the RPC ran.
    pub fn rpc_json<Req, Resp>(
        &self,
        method: impl Into<String>,
        data: &Req,
    ) -> impl Future<Output = Result<Resp, JsonCallError>> + MaybeSend + 'static
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let call = serde_json::to_vec(data).map(|data| self.rpc(method, data));
        async move {
            let data = call.map_err(JsonCallError::Encode)?.await?;
            match serde_json::from_slice(&data) {
                Ok(result) => Ok(result),
                Err(error) => Err(JsonCallError::Decode { error, data }),
            }
        }
    }

    /// Publishes `data` to `channel`: raw JSON with the JSON protocol, any
    /// bytes with Protobuf. Made, waiting and failing as [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn publish(
        &self,
        channel: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Publish {
                channel: channel.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// Publishes `data` encoded as JSON to `channel` (Block 38), as
    /// [`Client::publish`] does; `data` is encoded when this returns, so
    /// the future borrows nothing. Under the Protobuf protocol the data are
    /// the JSON bytes, which a JSON client receives unchanged.
    ///
    /// ```no_run
    /// # async fn run(client: ferrifuge::Client) -> Result<(), ferrifuge::JsonCallError> {
    /// #[derive(serde::Serialize)]
    /// struct Note<'a> {
    ///     text: &'a str,
    /// }
    ///
    /// client.publish_json("news", &Note { text: "hi" }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of [`Client::rpc`].
    pub fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        channel: impl Into<String>,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.publish(channel, data));
        async move { Ok(call.map_err(JsonCallError::Encode)?.await?) }
    }

    /// Sets `key` of the map channel `channel` to `data`: raw JSON with the
    /// JSON protocol, any bytes with Protobuf (Block 45, D8). Made, waiting
    /// and failing as [`Client::rpc`]: it waits for the connection only,
    /// not for a map subscription. Every subscriber of the channel, this
    /// client's included, receives the change. Never debounced, unlike
    /// [`MapSubscription::publish`](crate::MapSubscription::publish) on a
    /// namespace where the server advises an interval (Block 46).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// The key is not checked here: the server may replace it (its
    /// namespace's `map.client_key`) and refuses an empty one.
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`]; the server's refusal keeps its code, e.g. 103
    /// "permission denied" on a namespace that does not allow a client's
    /// writes or is not a map namespace, 107 "bad request" without data or
    /// with an empty key.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_publish(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::MapPublish {
                channel: channel.into(),
                key: key.into(),
                data: data.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// Sets `key` of the map channel `channel` to `data` encoded as JSON
    /// (Block 38), as [`Client::map_publish`] does; `data` is encoded when
    /// this returns, so the future borrows nothing.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of
    /// [`Client::map_publish`].
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_publish_json<T: serde::Serialize + ?Sized>(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.map_publish(channel, key, data));
        async move { Ok(call.map_err(JsonCallError::Encode)?.await?) }
    }

    /// Removes `key` from the map channel `channel` (Block 45, D8). Made,
    /// waiting and failing as [`Client::map_publish`]; removing a key the
    /// map does not hold succeeds. Never debounced (Block 46).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// As [`Client::map_publish`].
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_remove(
        &self,
        channel: impl Into<String>,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::MapRemove {
                channel: channel.into(),
                key: key.into(),
            },
        );
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// The history of `channel`: the publications `options` select, and the
    /// stream's current position. Made, waiting and failing as
    /// [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn history(
        &self,
        channel: impl Into<String>,
        options: HistoryOptions,
    ) -> impl Future<Output = Result<HistoryResult, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::History {
                channel: channel.into(),
                options,
            },
        );
        reply_of(call, history_of)
    }

    /// The clients subscribed to `channel`. Made, waiting and failing as
    /// [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn presence(
        &self,
        channel: impl Into<String>,
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::Presence {
                channel: channel.into(),
            },
        );
        reply_of(call, presence_of)
    }

    /// Sends `data` to the server as an asynchronous message, which gets no
    /// reply (`client.proto`, `SendRequest`): raw JSON with the JSON
    /// protocol, any bytes with Protobuf.
    ///
    /// The call is made when this returns. While the client is not
    /// connected it waits for the connection, for at most
    /// `Config::timeout`; the future resolves once the message is handed to
    /// the transport, which writes it even if the client disconnects right
    /// after (within `Config::timeout`), unless the connection fails first.
    /// Dropping the future before then abandons the message: a send whose
    /// future is never awaited may be lost.
    ///
    /// # Errors
    ///
    /// [`codes::error::TIMEOUT`] when no connection comes in time, and
    /// [`codes::error::DISCONNECTED`] when the client enters
    /// `disconnected`, including when the `Client` is dropped.
    pub fn send(
        &self,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self
            .shared
            .call(None, CallRequest::Send { data: data.into() });
        reply_of(call, |reply| matches!(reply, CallReply::Send).then_some(()))
    }

    /// How many clients and users are subscribed to `channel`. Made,
    /// waiting and failing as [`Client::rpc`].
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`].
    pub fn presence_stats(
        &self,
        channel: impl Into<String>,
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + MaybeSend + 'static {
        let call = self.shared.call(
            None,
            CallRequest::PresenceStats {
                channel: channel.into(),
            },
        );
        reply_of(call, presence_stats_of)
    }
}

/// An open batch of [`Client::start_batching`]: [`Batch::stop`], or dropping
/// it, stops it and sends what is held (centrifuge-js `stopBatching`). It
/// borrows nothing from the client.
#[derive(Debug)]
pub struct Batch {
    shared: Arc<Shared>,
    stopped: bool,
}

impl Batch {
    /// Stops the batch: what the open batches hold is sent now.
    pub fn stop(mut self) {
        self.end();
    }

    fn end(&mut self) {
        if !std::mem::replace(&mut self.stopped, true) {
            self.shared.apply(Input::StopBatching);
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        self.end();
    }
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // No code panics while holding these locks; recover rather than cascade.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

use super::backend::{Opening, Transport};
type Fetching = platform::BoxFuture<'static, Result<String, TokenError>>;

/// Performs the machine's actions, and turns transport activity and timers
/// into inputs for it.
struct Driver {
    /// Endpoints of the transports, in the machine's order.
    endpoints: Vec<String>,
    /// Websocket subprotocol of the configured encoding, if it needs one.
    subprotocol: Option<&'static str>,
    /// Where commands over emulated transports go, when not the default.
    emulation_endpoint: Option<String>,
    /// Time allowed for an emulation request, and for writing a websocket
    /// frame or closing a websocket (`Config::timeout`).
    timeout: Duration,
    /// Largest message accepted from the server (`Config::max_message_bytes`).
    max_message: usize,
    /// The bound of a browser transport's inbox (Block 44, D7).
    #[cfg(adapter_browser)]
    read_ahead: super::backend::websocket::Bound,
    /// The application's constructors, looked up with the page's at each
    /// attempt (Block 44, D12).
    #[cfg(adapter_browser)]
    shims: super::backend::shims::Shims,
    /// Whether HTTP requests carry credentials to other origins (Block 44,
    /// D13).
    #[cfg(adapter_browser)]
    include_credentials: bool,
    shared: Arc<Shared>,
    /// The runtime the driver runs on, for every spawn (Block 28, D1).
    runtime: Handle,
    /// Ends the client however the driver's task ends (Block 28, D3).
    _ending: Ending,
    /// Where the handlers run (Block 41, D2): `Inline` on this driver.
    delivery: super::EventDelivery,
    /// The closings of the transports closed that may still run, each
    /// awaited before the driver stops (Block 33, D1; Codex, #141).
    closing: Vec<platform::JoinHandle<()>>,
    opening: Option<(TransportId, Opening)>,
    transport: Option<(TransportId, Transport)>,
    /// The kind of the transport opening or open, for the records.
    kind: TransportKind,
    /// Whether the transport was read on the last turn, for the records of
    /// the read-ahead filling and draining (Block 21).
    reading: bool,
    get_token: Option<GetToken>,
    get_data: Option<GetConnectData>,
    fetching: Option<(FetchId, Fetching)>,
    /// The callback a newer one replaced, still polled: when it was a
    /// refresh of a connection that is gone, its denial must still reach
    /// the machine, which ignores any other answer of it.
    stale_fetching: Option<(FetchId, Fetching)>,
    timers: Timers,
    subscription_timers: KeyedTimers<(SubscriptionId, SubscriptionTimer)>,
    call_timers: KeyedTimers<(CallId, CallTimer)>,
    /// One timer per open debounce window (Block 46, D5).
    debounce_timers: KeyedTimers<DebounceTimer>,
    /// The reply deadlines of shared poll requests (Block 47, D12).
    shared_poll_timers: SharedPollTimers,
    /// The signature refresh of each shared poll subscription, at the
    /// earliest deadline reported (Block 47, D7).
    shared_poll_refreshes: SharedPollRefreshes,
    /// How many timers were set, numbering each setting as the core's
    /// [`Armed`] does.
    timer_sets: u64,
    /// Subscription callbacks and the connect data callback running, each yielding the
    /// input answering it; an aborted one yields `None`.
    callback_fetches: FuturesUnordered<CallbackFetching>,
    /// Handles to abort the callbacks running, by fetch.
    callback_aborts: HashMap<FetchId, AbortHandle>,
}

type CallbackFetching = platform::BoxFuture<'static, Option<(FetchId, Input)>>;

/// The machine's subscription timers, by subscription and kind, or its
/// call timers, by call.
struct KeyedTimers<K>(HashMap<K, (Pin<Box<Sleep>>, u64)>);

impl<K> Default for KeyedTimers<K> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Copy + Eq + std::hash::Hash> KeyedTimers<K> {
    /// Sets `key`'s timer, of the setting numbered `set` (see [`Armed`]).
    fn set(&mut self, key: K, delay: Duration, set: u64) {
        self.0.insert(key, (Box::pin(platform::sleep(delay)), set));
    }

    fn cancel(&mut self, key: K) {
        self.0.remove(&key);
    }

    fn shorten(&mut self, key: K, by: Duration) {
        if let Some((sleep, _)) = self.0.get_mut(&key) {
            shortened(sleep, by);
        }
    }

    /// Arms `key`'s timer to fire `delay` from now, unless it runs and
    /// fires sooner: one timer at the earliest deadline asked for.
    #[cfg_attr(not(feature = "unstable"), allow(dead_code))]
    fn set_earliest(&mut self, key: K, delay: Duration) {
        match self.0.get_mut(&key) {
            Some((sleep, _)) => {
                let deadline = platform::Instant::now() + delay;
                if deadline < sleep.deadline() {
                    sleep.as_mut().reset(deadline);
                }
            }
            None => self.set(key, delay, 0),
        }
    }

    /// The number of timers running.
    #[cfg(all(test, feature = "unstable"))]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Resolves with every timer ready at this poll, which are removed;
    /// never while none is ready or none is set. Cancel-safe. A wakeup polls
    /// every timer, as [`KeyedTimers::fired`]; several windows ending
    /// together cost one poll and, the driver feeding them all before its
    /// next flush, one flush (Block 46, D5).
    async fn fired_all(&mut self) -> Vec<K> {
        std::future::poll_fn(|cx| {
            let mut fired = Vec::new();
            self.0.retain(|key, (sleep, _)| {
                if sleep.as_mut().poll(cx).is_ready() {
                    fired.push(*key);
                    false
                } else {
                    true
                }
            });
            if fired.is_empty() {
                Poll::Pending
            } else {
                Poll::Ready(fired)
            }
        })
        .await
    }

    /// Resolves with the first timer to fire, which is removed, and the
    /// number of its setting; never while none is set. Cancel-safe.
    async fn fired(&mut self) -> (K, u64) {
        std::future::poll_fn(|cx| {
            let fired = self.0.iter_mut().find_map(|(key, (sleep, set))| {
                sleep.as_mut().poll(cx).is_ready().then_some((*key, *set))
            });
            match fired {
                Some((key, set)) => {
                    self.0.remove(&key);
                    Poll::Ready((key, set))
                }
                None => Poll::Pending,
            }
        })
        .await
    }
}

/// Makes `sleep` end `by` earlier; a deadline already past fires at once
/// (Block 25, D3).
fn shortened(sleep: &mut Pin<Box<Sleep>>, by: Duration) {
    let deadline = sleep
        .deadline()
        .checked_sub(by)
        .unwrap_or_else(platform::Instant::now);
    sleep.as_mut().reset(deadline);
}

/// The machine's timers: one slot per [`Timer`] kind, so a new kind needs
/// no change here.
#[derive(Default)]
struct Timers([Option<(Pin<Box<Sleep>>, u64)>; Timer::COUNT]);

impl Timers {
    /// Sets `timer`, of the setting numbered `set` (see [`Armed`]).
    fn set(&mut self, timer: Timer, delay: Duration, set: u64) {
        // sleep() clamps delays too large for an Instant instead of panicking.
        self.0[timer.index()] = Some((Box::pin(platform::sleep(delay)), set));
    }

    fn cancel(&mut self, timer: Timer) {
        self.0[timer.index()] = None;
    }

    fn shorten(&mut self, timer: Timer, by: Duration) {
        if let Some((sleep, _)) = &mut self.0[timer.index()] {
            shortened(sleep, by);
        }
    }

    /// Resolves with the first timer to fire, which is cleared, and the
    /// number of its setting; never while none is set. Cancel-safe:
    /// unfired timers keep running.
    async fn fired(&mut self) -> (Timer, u64) {
        std::future::poll_fn(|cx| {
            for timer in Timer::ALL {
                let slot = &mut self.0[timer.index()];
                if let Some((sleep, set)) = slot
                    && sleep.as_mut().poll(cx).is_ready()
                {
                    let set = *set;
                    *slot = None;
                    return Poll::Ready((timer, set));
                }
            }
            Poll::Pending
        })
        .await
    }
}

/// The machine of a client: its callbacks, headers and transports, and
/// automatic batching, the driver flushing at each iteration (Block 36,
/// D3).
/// Checks the endpoints, and the connector's limits when one is given
/// (Block 37, D4).
#[cfg(adapter_tokio)]
fn check_endpoints(
    endpoints: &[(TransportKind, String)],
    connector: bool,
    proxy_set: bool,
) -> Result<(), ClientError> {
    for (kind, endpoint) in endpoints {
        check_endpoint(*kind, endpoint)?;
    }
    if connector {
        check_connector(endpoints, proxy_set)?;
    }
    Ok(())
}

fn machine_for(
    config: Config,
    token_source: bool,
    data_source: bool,
    emulated_headers: std::collections::BTreeMap<String, String>,
    kinds: Vec<TransportKind>,
) -> Result<Machine, ClientError> {
    let machine = if token_source {
        Machine::with_token_source(config)
    } else {
        Machine::new(config)
    };
    let machine = if data_source {
        machine.with_data_source()
    } else {
        machine
    };
    machine
        .with_emulated_headers(emulated_headers)
        .with_automatic_batching()
        .with_publish_debounce()
        .with_transports(kinds)
}

impl Driver {
    /// The client is closed. The close queued its actions before setting
    /// the flag, under the core lock: performed now, its transport's close
    /// among them, even when it came after the queue was last emptied
    /// (Codex, #141). Every transport's close is awaited, so that `close()`
    /// resolves after it (Block 33, D1); the guard then ends the streams as
    /// the driver goes.
    async fn finish(&mut self) {
        while let Some(action) = self.shared.next_action() {
            self.perform(action);
        }
        if self.delivery == super::EventDelivery::Inline {
            super::handler::deliver_remaining(&self.shared);
            while let Some(action) = self.shared.next_action() {
                self.perform(action);
            }
        }
        for closing in std::mem::take(&mut self.closing) {
            let _ = closing.await;
        }
    }

    #[allow(clippy::too_many_lines)] // One arm per source of input.
    async fn run(mut self) {
        loop {
            // Everything the core buffered since the last flush, from this
            // task's inputs and the application's calls, goes out in as few
            // frames as the cap allows (Block 36, D3).
            self.shared.flush();
            while let Some(action) = self.shared.next_action() {
                self.perform(action);
            }
            if self.shared.closed.load(Ordering::Acquire) {
                return self.finish().await;
            }
            let reading = self.reading();
            if self.delivery.is_thread() {
                super::handler::check_thread_waits(&self.shared);
            }
            // The number of the setting of a timer that fired.
            let mut fired = None;
            let input = tokio::select! {
                // Queued by an API call; performed at the top of the loop.
                () = self.shared.actions_queued.notified() => continue,
                // Under `Inline` the handler runs here: nothing else of the
                // driver runs until it returns (Block 41, D3).
                Some(handed) = std::future::poll_fn(|cx| super::handler::next(&self.shared, cx)),
                    if self.delivery == super::EventDelivery::Inline =>
                {
                    super::handler::deliver(&self.shared, handed);
                    continue;
                }
                (id, result) = opened(&mut self.opening) => {
                    self.opening = None;
                    match result {
                        Ok(transport) => {
                            debug!(transport = self.kind.name(), "transport opened");
                            self.transport = Some((id, transport));
                            Input::TransportOpened(id)
                        }
                        Err(message) => {
                            debug!(transport = self.kind.name(), reason = %message, "transport failed");
                            Input::TransportFailed { transport: id, message }
                        }
                    }
                }
                (id, inbound) = received(&mut self.transport, reading) => match inbound {
                    Inbound::Frame(data) => {
                        trace!(
                            transport = self.kind.name(),
                            frame_type = ?self.frame_type(),
                            bytes = data.len(),
                            "frame received"
                        );
                        Input::Frame { transport: id, data }
                    }
                    Inbound::Rejected(frame) => Input::SendRejected { transport: id, frame },
                    Inbound::Written(frame) => Input::FrameWritten { transport: id, frame },
                    Inbound::Closed { code, reason } => {
                        self.transport = None;
                        self.closed(code, &reason);
                        Input::TransportClosed { transport: id, code, reason }
                    }
                    Inbound::TooLarge => {
                        self.transport = None;
                        warn!(
                            client = self.shared.log.number,
                            transport = self.kind.name(),
                            "a message from the server is larger than the size limit"
                        );
                        Input::MessageTooLarge(id)
                    }
                    #[cfg(adapter_browser)]
                    Inbound::Overflow => {
                        self.transport = None;
                        warn!(
                            client = self.shared.log.number,
                            transport = self.kind.name(),
                            "the inbox overflowed while the read-ahead was full"
                        );
                        Input::InboxOverflow(id)
                    }
                },
                (timer, set) = self.timers.fired() => {
                    fired = Some(set);
                    Input::TimerFired(timer)
                }
                ((id, timer), set) = self.subscription_timers.fired() => {
                    fired = Some(set);
                    Input::SubscriptionTimerFired(id, timer)
                }
                ((id, timer), set) = self.call_timers.fired() => {
                    fired = Some(set);
                    Input::CallTimerFired(id, timer)
                }
                // Not numbered, as debounce timers: the core ignores one it
                // no longer knows (Block 47, D3).
                input = self.shared_poll_timers.next_input() => input,
                input = self.shared_poll_refreshes.next_input() => input,
                // Every window due now, fed before the next flush: windows
                // ending together share a frame (Block 46, D5, D9).
                timers = self.debounce_timers.fired_all() => {
                    for timer in timers {
                        self.shared.apply(Input::DebounceTimerFired(timer));
                    }
                    continue;
                }
                Some(fetched) = self.callback_fetches.next(),
                    if !self.callback_fetches.is_empty() =>
                {
                    // Aborted: the machine no longer wants it.
                    let Some((fetch, input)) = fetched else {
                        continue;
                    };
                    self.callback_aborts.remove(&fetch);
                    input
                }
                (fetch, result) = fetched(&mut self.fetching) => {
                    self.fetching = None;
                    Input::TokenFetched { fetch, result }
                }
                (fetch, result) = fetched(&mut self.stale_fetching) => {
                    self.stale_fetching = None;
                    Input::TokenFetched { fetch, result }
                }
            };
            self.shared.apply_fired(input, fired);
        }
    }

    /// Whether to read the transport: a full read-ahead stops the reading,
    /// so that the server's handling of slow clients acts instead
    /// (Block 15). Taking an event wakes the loop.
    fn reading(&mut self) -> bool {
        let reading = lock(&self.shared.core).machine.wants_frames();
        if reading != self.reading {
            self.reading = reading;
            if reading {
                debug!("read-ahead drained: reading resumes");
            } else {
                debug!("read-ahead full: reading stops");
            }
        }
        reading
    }

    /// Numbers the next setting of a timer (see [`Armed`]).
    fn next_set(&mut self) -> u64 {
        self.timer_sets += 1;
        self.timer_sets
    }

    #[allow(clippy::too_many_lines)] // One arm per action.
    fn perform(&mut self, action: Action) {
        match action {
            Action::OpenTransport {
                transport,
                kind,
                endpoint,
                initial_frame,
            } => self.open_transport(transport, kind, endpoint, initial_frame),
            Action::CloseTransport(id) => {
                if self
                    .opening
                    .as_ref()
                    .is_some_and(|(opening, _)| *opening == id)
                {
                    self.opening = None;
                }
                if let Some((_, transport)) = self.transport.take_if(|(open, _)| *open == id) {
                    self.closing.retain(|closing| !closing.is_finished());
                    self.closing.push(transport.close(&self.runtime));
                }
            }
            Action::Send {
                transport,
                id: frame_id,
                frame,
                frame_type,
            } => {
                trace!(
                    transport = self.kind.name(),
                    frame_type = ?frame_type,
                    bytes = frame.len(),
                    "frame sent"
                );
                if let Some((id, open)) = &mut self.transport
                    && *id == transport
                    && open.send(frame_id, frame, frame_type).is_err()
                {
                    self.transport = None;
                    self.shared.apply(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                }
            }
            Action::SetTimer(timer, delay) => {
                let set = self.next_set();
                self.timers.set(timer, delay, set);
            }
            Action::CancelTimer(timer) => self.timers.cancel(timer),
            Action::ShortenTimer(timer, by) => self.timers.shorten(timer, by),
            Action::ShortenSubscriptionTimer(id, timer, by) => {
                self.subscription_timers.shorten((id, timer), by);
            }
            // Logged by `Shared::handle`, as every action.
            Action::Anomaly(_) => {}
            // Shared::handle routes them to the outboxes.
            Action::Emit(..) | Action::EmitSubscription(..) | Action::Withdraw(_) => {
                unreachable!("events never reach the driver")
            }
            #[cfg(feature = "unstable")]
            Action::EmitMapSubscription(..) | Action::EmitSharedPollSubscription(..) => {
                unreachable!("events never reach the driver")
            }
            #[cfg(feature = "unstable")]
            Action::SetSharedPollTimer(timer, delay) => {
                self.shared_poll_timers.set(timer, delay, 0);
            }
            #[cfg(feature = "unstable")]
            Action::CancelSharedPollTimer(timer) => self.shared_poll_timers.cancel(timer),
            #[cfg(feature = "unstable")]
            Action::SetSharedPollRefresh(id, delay) => {
                self.shared_poll_refreshes.set_earliest(id, delay);
            }
            #[cfg(feature = "unstable")]
            Action::CancelSharedPollRefresh(id) => self.shared_poll_refreshes.cancel(id),
            #[cfg(feature = "unstable")]
            Action::FetchSignature {
                subscription,
                request,
                keys,
            } => self.fetch_signature(subscription, request, keys),
            // In order with the other actions: a send settles once its
            // frame went to the transport, which writes it even when closed
            // right after.
            Action::CallSettled { call, result } => {
                if let Some(sender) = lock(&self.shared.calls).remove(&call) {
                    // The future may be gone already.
                    let _ = sender.send(result);
                }
            }
            Action::SetSubscriptionTimer(id, timer, delay) => {
                let set = self.next_set();
                self.subscription_timers.set((id, timer), delay, set);
            }
            Action::CancelSubscriptionTimer(id, timer) => {
                self.subscription_timers.cancel((id, timer));
            }
            // Not numbered: a window's timer is never set twice and the
            // core ignores one it no longer knows (Block 46, D5). Numbering
            // it here would skew the settings the core's `Armed` counts.
            Action::SetDebounceTimer(timer, delay) => self.set_debounce_timer(timer, delay),
            Action::CancelDebounceTimer(timer) => self.debounce_timers.cancel(timer),
            Action::SetCallTimer(id, timer, delay) => {
                let set = self.next_set();
                self.call_timers.set((id, timer), delay, set);
            }
            Action::CancelCallTimer(id, timer) => self.call_timers.cancel((id, timer)),
            action @ (Action::FetchSubscriptionData { .. }
            | Action::FetchSubscriptionToken { .. }
            | Action::FetchSubscriptionState { .. }
            | Action::FetchConnectData(_)
            | Action::CancelSubscriptionData(_)
            | Action::CancelSubscriptionToken(_)
            | Action::CancelSubscriptionState(_)
            | Action::CancelConnectData(_)) => self.callback(action),
            Action::FetchToken(id) => {
                // The machine asks only when it was built with a token source.
                if let Some(get_token) = &self.get_token {
                    // A stale callback stays until a running one replaces it.
                    if let Some(running) = self.fetching.take()
                        && let Some((dropped, _)) = self.stale_fetching.replace(running)
                    {
                        // Dropped without an answer: nothing else tells the log.
                        self.shared.log.fetch_dropped(dropped);
                    }
                    self.fetching = Some((id, guarded(|| get_token(), callback_panicked)));
                }
            }
        }
    }
}

/// The reply deadlines of shared poll requests (Block 47, D12).
#[cfg(feature = "unstable")]
type SharedPollTimers = KeyedTimers<crate::machine::SharedPollTimer>;

#[cfg(feature = "unstable")]
impl SharedPollTimers {
    /// The input of the next timer due.
    async fn next_input(&mut self) -> Input {
        Input::SharedPollTimerFired(self.fired().await.0)
    }
}

/// No shared poll timer without `unstable`.
#[cfg(not(feature = "unstable"))]
#[derive(Debug, Default)]
struct SharedPollTimers(());

#[cfg(not(feature = "unstable"))]
impl SharedPollTimers {
    /// Never ready.
    async fn next_input(&mut self) -> Input {
        std::future::pending().await
    }
}

/// The signature refresh timer of each shared poll subscription (Block
/// 47, D7).
#[cfg(feature = "unstable")]
type SharedPollRefreshes = KeyedTimers<SubscriptionId>;

#[cfg(feature = "unstable")]
impl SharedPollRefreshes {
    /// The input of the next refresh due.
    async fn next_input(&mut self) -> Input {
        Input::SharedPollRefreshFired(self.fired().await.0)
    }
}

/// No shared poll refresh without `unstable`.
#[cfg(not(feature = "unstable"))]
#[derive(Debug, Default)]
struct SharedPollRefreshes(());

#[cfg(not(feature = "unstable"))]
impl SharedPollRefreshes {
    /// Never ready.
    async fn next_input(&mut self) -> Input {
        std::future::pending().await
    }
}

impl Driver {
    /// Arms a debounce window's timer, unless a test holds them
    /// (`__test-hooks`).
    fn set_debounce_timer(&mut self, timer: DebounceTimer, delay: Duration) {
        #[cfg(feature = "__test-hooks")]
        if let Some(held) = lock(&self.shared.held_debounce_timers).as_mut() {
            held.push(timer);
            return;
        }
        self.debounce_timers.set(timer, delay, 0);
    }

    /// How frames travel with the configured encoding.
    fn frame_type(&self) -> FrameType {
        if self.subprotocol.is_some() {
            FrameType::Binary
        } else {
            FrameType::Text
        }
    }

    /// Records the close of the open transport: a message too large for
    /// the server is an anomaly (one too large for the client, see
    /// `Inbound::TooLarge`, too).
    fn closed(&self, code: Option<u16>, reason: &str) {
        let transport = self.kind.name();
        if code == Some(MESSAGE_TOO_BIG) {
            warn!(
                client = self.shared.log.number,
                transport, code, "the server closed the websocket: message too big"
            );
        } else {
            debug!(transport, code, reason, "transport closed");
        }
    }

    /// Starts opening transport `id`, of `kind`, at endpoint `endpoint`;
    /// an emulated one sends `initial_frame`, its connect command.
    fn open_transport(
        &mut self,
        id: TransportId,
        kind: TransportKind,
        endpoint: usize,
        initial_frame: Option<Vec<u8>>,
    ) {
        self.kind = kind;
        #[cfg(adapter_tokio)]
        let open = super::tokio::Open {
            kind,
            endpoint,
            url: self.endpoints.get(endpoint).cloned().unwrap_or_default(),
            subprotocol: self.subprotocol,
            timeout: self.timeout,
            max_message: self.max_message,
            headers: self.shared.http_headers.clone(),
            network: Arc::clone(&self.shared.network),
            emulation: self.emulation_endpoint.clone(),
            initial_frame,
        };
        #[cfg(adapter_browser)]
        let found = self.shims.find();
        // Skipped in the round, as centrifuge-js `_initializeTransport`
        // skips a transport not supported (Block 44, D12).
        #[cfg(adapter_browser)]
        if !found.usable(kind) {
            debug!(transport = kind.name(), "transport not available");
            self.shared.apply(Input::TransportUnsupported(id));
            return;
        }
        #[cfg(adapter_browser)]
        let open = super::browser::Open {
            kind,
            endpoint,
            url: self.endpoints.get(endpoint).cloned().unwrap_or_default(),
            subprotocol: self.subprotocol,
            timeout: self.timeout,
            max_message: self.max_message,
            read_ahead: self.read_ahead,
            emulation: self.emulation_endpoint.clone(),
            initial_frame,
            found,
            include_credentials: self.include_credentials,
        };
        let opening = super::backend::open(open, &self.shared.log);
        self.opening = Some((id, opening));
    }

    fn fetch_connect_data(&mut self, fetch: FetchId) {
        // The machine asks only when it was built with a data source.
        let Some(get_data) = &self.get_data else {
            return;
        };
        let fetching = guarded(|| get_data(), connect_data_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::ConnectDataFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    /// Runs or cancels a subscription callback, or the connect data one.
    fn callback(&mut self, action: Action) {
        match action {
            Action::FetchConnectData(fetch) => self.fetch_connect_data(fetch),
            Action::FetchSubscriptionData {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_data(subscription, channel, fetch),
            Action::FetchSubscriptionToken {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_token(subscription, channel, fetch),
            Action::FetchSubscriptionState {
                subscription,
                channel,
                fetch,
            } => self.fetch_subscription_state(subscription, channel, fetch),
            Action::CancelSubscriptionData(fetch)
            | Action::CancelSubscriptionToken(fetch)
            | Action::CancelSubscriptionState(fetch)
            | Action::CancelConnectData(fetch) => {
                if let Some(abort) = self.callback_aborts.remove(&fetch) {
                    abort.abort();
                }
            }
            _ => unreachable!("not a callback action: {action:?}"),
        }
    }

    fn fetch_subscription_data(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.data.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_data) = callback else {
            return;
        };
        let fetching = guarded(|| get_data(channel), data_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionDataFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    fn fetch_subscription_token(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.token.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_token) = callback else {
            return;
        };
        let fetching = guarded(|| get_token(channel), token_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionTokenFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    fn fetch_subscription_state(&mut self, id: SubscriptionId, channel: String, fetch: FetchId) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.state.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_state) = callback else {
            return;
        };
        let fetching = guarded(|| get_state(channel), state_callback_panicked);
        self.start_fetch(fetch, async move {
            Input::SubscriptionStateFetched {
                fetch,
                result: fetching.await,
            }
        });
    }

    /// Runs a shared poll subscription's `get_signature` callback (Block 47,
    /// D6); its answer to an abandoned request changes nothing in the core.
    #[cfg(feature = "unstable")]
    fn fetch_signature(&mut self, id: SubscriptionId, request: FetchId, keys: Vec<String>) {
        let callback = lock(&self.shared.callbacks)
            .0
            .get(&id)
            .and_then(|callbacks| callbacks.signature.clone());
        // The machine asks only for subscriptions with a callback.
        let Some(get_signature) = callback else {
            return;
        };
        let fetching = guarded(|| get_signature(keys), signature_callback_panicked);
        self.start_fetch(request, async move {
            Input::SignatureFetched {
                subscription: id,
                request,
                result: fetching.await,
            }
        });
    }

    /// Runs a subscription or connect data callback, abortable by its
    /// fetch.
    fn start_fetch(
        &mut self,
        fetch: FetchId,
        answer: impl Future<Output = Input> + MaybeSend + 'static,
    ) {
        let (fetching, abort) = future::abortable(answer);
        self.callback_aborts.insert(fetch, abort);
        self.callback_fetches.push(Box::pin(
            fetching.map(move |input| input.ok().map(|input| (fetch, input))),
        ));
    }
}

/// The future of a callback, which fails with `panicked()` rather than
/// panicking the driver: whether the callback panics creating its future
/// or polling it.
fn guarded<T: MaybeSend + 'static, E: MaybeSend + 'static>(
    callback: impl FnOnce() -> platform::BoxFuture<'static, Result<T, E>>,
    panicked: fn() -> E,
) -> platform::BoxFuture<'static, Result<T, E>> {
    let Ok(future) = std::panic::catch_unwind(AssertUnwindSafe(callback)) else {
        debug!("callback panicked");
        return Box::pin(std::future::ready(Err(panicked())));
    };
    Box::pin(async move {
        AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                debug!("callback panicked");
                Err(panicked())
            })
    })
}

#[cfg(feature = "unstable")]
fn signature_callback_panicked() -> TokenError {
    TokenError::Failed("get_signature callback panicked".to_owned())
}

fn token_callback_panicked() -> TokenError {
    TokenError::Failed("subscription token callback panicked".to_owned())
}

fn connect_data_callback_panicked() -> DataError {
    DataError::Failed("connect data callback panicked".to_owned())
}

fn data_callback_panicked() -> DataError {
    DataError::Failed("subscription data callback panicked".to_owned())
}

fn state_callback_panicked() -> StateError {
    StateError("subscription getState callback panicked".to_owned())
}

fn callback_panicked() -> TokenError {
    TokenError::Failed("token callback panicked".to_owned())
}

/// Resolves when the transport being opened is ready; never without one.
async fn opened(
    opening: &mut Option<(TransportId, Opening)>,
) -> (TransportId, Result<Transport, String>) {
    match opening {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

#[cfg(adapter_tokio)]
/// Refuses an endpoint the transport cannot use: an empty one, or an
/// `http(s)://` one for the websocket transport, which needs `ws(s)://`;
/// `http(s)://` is right for `http_stream` and sse (owner decision,
/// 2026-11-04). The message leaves the endpoint out: it may carry a token.
/// An error of building a client without a runtime to run on (Block 28).
/// The runtime given, else the current one (Block 28, D1).
fn runtime_or_current(runtime: Option<Handle>) -> Result<Handle, ClientError> {
    match runtime {
        Some(runtime) => Ok(runtime),
        None => platform::current_runtime().ok_or_else(|| {
            runtime_error(
                "no tokio runtime is current: build the client inside one, or give one \
                 with ClientBuilder::runtime",
            )
        }),
    }
}

fn runtime_error(message: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    }
}

/// Ends the client when the driver's task ends, however it ends: after
/// the final disconnect, or dropped by a runtime shutting down or by a
/// panic (Block 28, D3, D4), which would otherwise leave calls and
/// streams pending forever, even after the client is dropped (as hyper's
/// dispatch callback fails its request when its task is dropped).
pub(super) struct Ending(pub(super) Arc<Shared>);

impl Drop for Ending {
    fn drop(&mut self) {
        let shared = &self.0;
        match shared.core.lock() {
            Ok(mut core) => {
                // Checked and set under the lock, as `Shared::shut_down`
                // does: a client dropped as its runtime shuts down ends
                // once.
                if !shared.closed.load(Ordering::Acquire) {
                    let panicked = std::thread::panicking();
                    // "Client closed" before the new states would judge them
                    // (Block 33, D3).
                    lock(&shared.ready).close();
                    shared.handle(&mut core, Input::DriverDropped { panicked });
                    shared.closed.store(true, Ordering::Release);
                }
                // Nothing performs them anymore; the transport is dropped
                // with the driver.
                core.actions.clear();
            }
            // A panic with the lock held may have left the machine
            // inconsistent, and a second panic would abort: it is not fed,
            // and no event is emitted (D4).
            Err(_) => shared.closed.store(true, Ordering::Release),
        }
        // Handles of subscriptions may outlive the client: end every event
        // stream rather than leave it pending forever, and release the data
        // callbacks nothing can call anymore.
        let mut outboxes = lock(&shared.outboxes);
        outboxes.client.close();
        outboxes.subscriptions.values_mut().for_each(Outbox::close);
        #[cfg(feature = "unstable")]
        outboxes.maps.values_mut().for_each(Outbox::close);
        #[cfg(feature = "unstable")]
        outboxes.shared_polls.values_mut().for_each(Outbox::close);
        drop(outboxes);
        lock(&shared.callbacks).0.clear();
        super::handler::wake_consumer(shared);
        super::handler::release_unconsumed(shared);
        // The final disconnect settled the calls it could; any other future
        // gets "client closed" rather than waiting forever (Block 33).
        lock(&shared.calls).clear();
        lock(&shared.ready).clear();
        // Releases the `close()` futures (Block 33, D1).
        shared.driver_gone.store(true, Ordering::Release);
        shared.driver_ended.notify_waiters();
    }
}

#[cfg(adapter_tokio)]
/// A connector carries the websocket only, and replaces every proxy
/// setting (Block 37, D4).
fn check_connector(
    endpoints: &[(TransportKind, String)],
    proxy_set: bool,
) -> Result<(), ClientError> {
    if let Some((kind, _)) = endpoints
        .iter()
        .find(|(kind, _)| *kind != TransportKind::WebSocket)
    {
        return Err(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: format!(
                "a connector carries the websocket only, not transport {}",
                kind.name()
            ),
            transport: Some(kind.name().to_owned()),
        });
    }
    if proxy_set {
        return Err(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: "a connector replaces the proxy setting: give one or the other".to_owned(),
            transport: None,
        });
    }
    Ok(())
}

#[cfg(adapter_browser)]
/// A client none of whose transports the page or the application's shims
/// can open is refused (Block 44, D12; centrifuge-js 63ed73e
/// `_validateTransportConfig`, "no supported transport found").
fn check_usable(
    endpoints: &[(TransportKind, String)],
    shims: &super::backend::shims::Shims,
) -> Result<(), ClientError> {
    let found = shims.find();
    if endpoints.iter().any(|(kind, _)| found.usable(*kind)) {
        return Ok(());
    }
    let kinds: Vec<TransportKind> = endpoints.iter().map(|(kind, _)| *kind).collect();
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: super::backend::shims::none_usable(&kinds),
        transport: None,
    })
}

/// Whether `kind` is WebTransport (Block 48).
#[cfg_attr(not(feature = "unstable"), allow(clippy::missing_const_for_fn))]
fn is_webtransport(kind: TransportKind) -> bool {
    #[cfg(feature = "unstable")]
    {
        kind == TransportKind::WebTransport
    }
    #[cfg(not(feature = "unstable"))]
    {
        let _ = kind;
        false
    }
}

#[cfg(all(adapter_tokio, feature = "unstable"))]
/// Refuses WebTransport endpoints the native build cannot open: without
/// the `unstable-webtransport` feature (Block 48, D1), or with a raw TLS
/// configuration QUIC cannot use (D4).
fn check_webtransport(
    endpoints: &[(TransportKind, String)],
    trust: &Trust,
) -> Result<(), ClientError> {
    let Some((kind, endpoint)) = endpoints.iter().find(|(kind, _)| is_webtransport(*kind)) else {
        return Ok(());
    };
    let error = |message: String| ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: Some(kind.name().to_owned()),
    };
    #[cfg(not(feature = "unstable-webtransport"))]
    {
        let _ = (endpoint, trust);
        Err(error(super::tokio::NO_WEBTRANSPORT.to_owned()))
    }
    #[cfg(feature = "unstable-webtransport")]
    {
        use super::tokio::webtransport::{Alpn, check_quic, quic_tls};
        let Trust::Raw(raw) = trust else {
            // Ferrifuge's own configurations always pass (D4).
            return Ok(());
        };
        let host = reqwest::Url::parse(endpoint.trim())
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_else(|| "localhost".to_owned());
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .map_or_else(|| host.clone(), str::to_owned);
        check_quic(&quic_tls(raw, Alpn::H3), &host).map_err(error)
    }
}

fn check_endpoint(kind: TransportKind, endpoint: &str) -> Result<(), ClientError> {
    let endpoint = endpoint.trim();
    let http = endpoint.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    });
    let message = if is_webtransport(kind) {
        // WebTransport is defined over https only (Block 48, D2).
        let https = endpoint
            .split_once("://")
            .is_some_and(|(scheme, rest)| scheme.eq_ignore_ascii_case("https") && !rest.is_empty());
        if https {
            return Ok(());
        }
        "the webtransport transport needs an https:// endpoint".to_owned()
    } else if endpoint.is_empty() {
        format!("no endpoint given for the {} transport", kind.name())
    } else if kind == TransportKind::WebSocket && http {
        "the websocket transport needs a ws:// or wss:// endpoint, not http:// or https://"
            .to_owned()
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: Some(kind.name().to_owned()),
    })
}

#[cfg(adapter_tokio)]
/// A read-ahead limit of 0 could never let a message in (Block 15).
/// Headers a transport sets itself, which an application may not set:
/// proxy credentials go through `ProxyTarget` only, never through a tunnel
/// to the server (Block 23, D7).
fn transport_header(name: &HeaderName) -> bool {
    use reqwest::header::{
        ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, PROXY_AUTHORIZATION,
        TRANSFER_ENCODING, UPGRADE,
    };
    [
        HOST,
        CONNECTION,
        UPGRADE,
        CONTENT_LENGTH,
        TRANSFER_ENCODING,
        CONTENT_TYPE,
        ACCEPT,
        PROXY_AUTHORIZATION,
    ]
    .contains(name)
        || name.as_str().starts_with("sec-websocket-")
}

/// A configuration error about header `name`, never naming its value.
fn header_error(name: &str, problem: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: format!("HTTP header {name:?} {problem}"),
        transport: None,
    }
}

#[cfg(adapter_tokio)]
/// The checked HTTP headers (Block 17).
fn http_header_map(headers: &[(String, String)]) -> Result<HeaderMap, ClientError> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let header = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| header_error(name, "is not a valid header name"))?;
        if transport_header(&header) {
            return Err(header_error(name, "is set by the transport"));
        }
        let value = HeaderValue::from_str(value)
            .map_err(|_| header_error(name, "has a value that is not valid HTTP"))?;
        map.insert(header, value);
    }
    Ok(map)
}

/// The checked emulated headers, by name (Block 17).
fn emulated_header_map(
    headers: &[(String, String)],
) -> Result<std::collections::BTreeMap<String, String>, ClientError> {
    let mut map = std::collections::BTreeMap::new();
    for (name, value) in headers {
        #[cfg(adapter_tokio)]
        let valid = (
            HeaderName::from_bytes(name.as_bytes()).is_ok(),
            HeaderValue::from_str(value).is_ok(),
        );
        #[cfg(adapter_browser)]
        let valid = (header_name_valid(name), header_value_valid(value));
        if !valid.0 {
            return Err(header_error(name, "is not a valid header name"));
        }
        if !valid.1 {
            return Err(header_error(name, "has a value that is not valid HTTP"));
        }
        map.insert(name.clone(), value.clone());
    }
    Ok(map)
}

/// Whether `name` is an HTTP token (RFC 9110, 5.6.2), as the `http`
/// crate's `HeaderName` takes natively.
#[cfg(adapter_browser)]
fn header_name_valid(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// Whether `value` has no control character but tab, as the `http`
/// crate's `HeaderValue::from_str` takes natively.
#[cfg(adapter_browser)]
fn header_value_valid(value: &str) -> bool {
    value
        .bytes()
        .all(|b| (b >= 0x20 && b != 0x7f) || b == b'\t')
}

#[cfg(adapter_tokio)]
/// Whom the client trusts, from its extra roots and raw configuration,
/// checked (Block 22, D2 and D3).
fn trust(
    roots: Vec<CertificateDer<'static>>,
    raw: Option<Arc<rustls::ClientConfig>>,
    android: bool,
) -> Result<Trust, ClientError> {
    let message = match (roots.is_empty(), raw) {
        // Android's verifier takes no extra roots (Block 24, D3).
        (false, None) if android => {
            "extra root certificates are not supported on Android: give a tls_config trusting them"
                .to_owned()
        }
        (true, None) => return Ok(Trust::Platform),
        (true, Some(raw)) => {
            if cfg!(feature = "fips") && !raw.fips() {
                "the fips build needs a TLS configuration whose fips() is true".to_owned()
            } else {
                return Ok(Trust::Raw(raw));
            }
        }
        (false, Some(_)) => {
            "extra root certificates and a TLS configuration cannot be given together: \
             put the roots in the TLS configuration"
                .to_owned()
        }
        (false, None) => match unparsable_root(&roots) {
            Some(index) => format!("extra root certificate {index} is not a valid root"),
            None => return Ok(Trust::ExtraRoots(roots.into())),
        },
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: None,
    })
}

#[cfg(adapter_tokio)]
/// On Android before the verifier's setup (`unready`), refuses a client
/// whose connections known now need the platform verifier (Block 24, D5):
/// a `wss://` or `https://` endpoint, an `https://` emulation endpoint with
/// an HTTP transport, an `https://` proxy for every endpoint. Any other TLS
/// use fails its handshake instead; a raw configuration is the
/// application's.
fn android_gate(
    unready: bool,
    trust: &Trust,
    endpoints: &[(TransportKind, String)],
    emulation_endpoint: Option<&str>,
    proxy: &Proxy,
) -> Result<(), ClientError> {
    if !unready || !matches!(trust, Trust::Platform) {
        return Ok(());
    }
    let secure = |url: &str| {
        url.split_once("://").is_some_and(|(scheme, _)| {
            scheme.eq_ignore_ascii_case("wss") || scheme.eq_ignore_ascii_case("https")
        })
    };
    let emulated = endpoints.iter().any(|(kind, _)| kind.is_emulated());
    let emulation = emulation_endpoint.filter(|_| emulated);
    let needed = endpoints
        .iter()
        .map(|(_, url)| url.as_str())
        .chain(emulation)
        .any(|url| secure(url) || proxy.is_https_proxy_for(url));
    if !needed {
        return Ok(());
    }
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: tls::ANDROID_SETUP_MISSING.to_owned(),
        transport: None,
    })
}

fn check_limits(config: &Config) -> Result<(), ClientError> {
    let message = if config.read_ahead_messages == 0 || config.read_ahead_bytes == 0 {
        "the read-ahead limits must be at least 1"
    } else if config.write_queue_bytes == 0 {
        "the write queue limit must be at least 1"
    } else if config.batch_frame_bytes == 0 {
        "the batch frame limit must be at least 1"
    } else if config.max_message_bytes == 0 {
        "the maximum message size (max_message_bytes) must be at least 1"
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    })
}

/// Resolves with the token callback's answer; never without a fetch.
async fn fetched(
    fetching: &mut Option<(FetchId, Fetching)>,
) -> (FetchId, Result<String, TokenError>) {
    match fetching {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

/// Resolves with the next inbound message; never without a transport.
async fn received(
    transport: &mut Option<(TransportId, Transport)>,
    reading: bool,
) -> (TransportId, Inbound) {
    match transport {
        Some((id, transport)) => (*id, transport.recv(reading).await),
        None => std::future::pending().await,
    }
}

pub(super) fn history_of(reply: CallReply) -> Option<HistoryResult> {
    match reply {
        CallReply::History(history) => Some(history),
        _ => None,
    }
}

pub(super) fn presence_of(reply: CallReply) -> Option<PresenceResult> {
    match reply {
        CallReply::Presence(presence) => Some(presence),
        _ => None,
    }
}

#[allow(clippy::needless_pass_by_value)] // The signature `reply_of` takes.
pub(super) fn presence_stats_of(reply: CallReply) -> Option<PresenceStatsResult> {
    match reply {
        CallReply::PresenceStats(stats) => Some(stats),
        _ => None,
    }
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(all(test, adapter_tokio))]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::*;
    use crate::adapter::SubscriptionBuilder;

    // The typed calls borrow nothing: their futures can be spawned whatever
    // the application's types (Block 38, D3).
    #[test]
    fn typed_call_futures_are_send_and_static() {
        use std::marker::PhantomData;

        /// Neither `Send` nor `Sync`.
        struct Local(PhantomData<*const ()>);
        impl serde::Serialize for Local {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_unit()
            }
        }
        /// Neither `Send` nor `'static`.
        struct Borrowing<'a>(PhantomData<&'a *const ()>);
        impl<'de> serde::Deserialize<'de> for Borrowing<'_> {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                serde::de::IgnoredAny::deserialize(deserializer)?;
                Ok(Self(PhantomData))
            }
        }
        fn spawnable<F: Future + Send + 'static>(_: &F) {}
        // `'a` is no longer than the call: `Borrowing<'a>` is not `'static`.
        fn rpc_with<'a>(client: &Client, local: &Local, _: PhantomData<&'a ()>) {
            spawnable(&client.rpc_json::<_, Borrowing<'a>>("m", local));
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _entered = runtime.enter();
        let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default())
            .expect("a valid endpoint");
        let local = Local(PhantomData);
        rpc_with(&client, &local, PhantomData);
        spawnable(&client.publish_json("news", &local));
        let subscription = client.new_subscription("news").expect("a new channel");
        spawnable(&subscription.publish_json(&local));
    }

    // Clones and weak handles cross tasks and threads (Block 41, D5).
    #[test]
    fn clients_and_weak_handles_are_send_and_sync() {
        fn shared<T: Send + Sync + Clone>() {}
        shared::<Client>();
        shared::<WeakClient>();
    }

    // A batch can be stopped from another task (Block 18, D1).
    #[test]
    fn a_batch_is_send() {
        fn send<T: Send>() {}
        send::<Batch>();
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn http_header_names_are_case_insensitive_and_a_later_value_wins() {
        let map = http_header_map(&headers(&[("X-Device", "a"), ("x-device", "b")])).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map["x-device"], "b");
    }

    #[test]
    fn headers_the_transport_sets_are_refused_without_their_value() {
        for name in ["Host", "upgrade", "Accept", "Sec-WebSocket-Protocol"] {
            let error = http_header_map(&headers(&[(name, "v-secret")])).unwrap_err();

            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(error.message.contains(name), "{error:?}");
            assert!(!error.message.contains("v-secret"), "{error:?}");
        }
    }

    #[test]
    fn invalid_headers_are_refused_without_their_value() {
        let bad_value = headers(&[("X-Device", "line\nbreak-secret")]);
        let bad_name = headers(&[("X Device", "v")]);

        for checked in [
            http_header_map(&bad_value).err(),
            emulated_header_map(&bad_value).err(),
            http_header_map(&bad_name).err(),
            emulated_header_map(&bad_name).err(),
        ] {
            let error = checked.expect("refused");
            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(!error.message.contains("secret"), "{error:?}");
        }
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn state_changes_as_soon_as_connect_and_disconnect_return() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();

        client.connect();
        let after_connect = client.state();
        client.disconnect();
        let after_disconnect = client.state();

        assert_eq!(after_connect, State::Connecting);
        assert_eq!(after_disconnect, State::Disconnected);
    }

    /// The stream borrows nothing: an application can keep it next to its
    /// client, returned together or in one struct.
    fn client_with_events() -> (Client, impl Stream<Item = ClientEvent> + Unpin) {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let events = client.events();
        (client, events)
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn an_event_stream_does_not_borrow_its_client() {
        let (client, mut events) = client_with_events();

        client.connect();
        let first = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event");

        assert!(
            matches!(first, Some(ClientEvent::Connecting { .. })),
            "{first:?}"
        );
    }

    // Owner decision, 2026-11-04: no construction path accepts these.
    #[tokio::test]
    async fn an_empty_or_http_websocket_endpoint_is_refused() {
        for endpoint in [
            "",
            "http://127.0.0.1:1/connection/websocket",
            "HTTPS://example.com/ws",
        ] {
            let built = Client::builder(endpoint).build();

            let Err(error) = built else {
                panic!("{endpoint:?} was accepted");
            };
            assert_eq!(error.kind, crate::ErrorKind::Configuration, "{endpoint:?}");
            assert_eq!(
                error.transport.as_deref(),
                Some("websocket"),
                "{endpoint:?}"
            );
        }
    }

    #[tokio::test]
    async fn client_new_returns_an_endpoint_error_instead_of_panicking() {
        let created = Client::new("", Config::default());

        assert!(
            matches!(&created, Err(error) if error.kind == crate::ErrorKind::Configuration),
            "{created:?}"
        );
    }

    /// A current-thread runtime nothing drives, so that its client's
    /// driver never runs until the runtime is dropped.
    fn idle_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// The events queued for `stream`, until it is pending or ends, and
    /// whether it ended.
    fn queued(stream: &mut (impl Stream<Item = ClientEvent> + Unpin)) -> (Vec<ClientEvent>, bool) {
        let mut events = Vec::new();
        loop {
            match stream.next().now_or_never() {
                Some(Some(event)) => events.push(event),
                Some(None) => return (events, true),
                None => return (events, false),
            }
        }
    }

    fn disconnected_codes(events: &[ClientEvent]) -> Vec<u32> {
        events
            .iter()
            .filter_map(|event| match event {
                ClientEvent::Disconnected { code, .. } => Some(*code),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn client_new_outside_a_runtime_returns_an_error() {
        let created = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default());

        let error = created.expect_err("an error, without a runtime");
        assert_eq!(error.kind, crate::ErrorKind::Configuration);
        assert!(
            error.message.contains("ClientBuilder::runtime"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn the_driver_runs_on_the_given_runtime() {
        let runtime = idle_runtime();
        let current = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);

        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();

        assert_eq!(runtime.metrics().num_alive_tasks(), 1);
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            current
        );
        drop(client);
        // Dropping a runtime inside an asynchronous context panics.
        runtime.shutdown_background();
    }

    #[test]
    fn a_runtime_shut_down_while_connecting_ends_the_client() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let mut events = client.events();
        client.connect();
        let mut call = Box::pin(client.rpc("echo", b"{}".to_vec()));
        assert!(
            call.as_mut().now_or_never().is_none(),
            "waits for the connection"
        );

        drop(runtime);

        let (taken, ended) = queued(&mut events);
        assert_eq!(
            disconnected_codes(&taken),
            [codes::disconnected::RUNTIME_SHUT_DOWN]
        );
        assert!(
            matches!(taken.last(), Some(ClientEvent::Disconnected { reason, .. }) if reason == "runtime shut down"),
            "{taken:?}"
        );
        assert!(ended, "the stream ends");
        let failed = call.now_or_never().expect("settled").unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
    }

    #[test]
    fn a_runtime_shut_down_while_disconnected_emits_nothing() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let mut events = client.events();

        drop(runtime);

        let (taken, ended) = queued(&mut events);
        assert!(taken.is_empty(), "{taken:?}");
        assert!(ended, "the stream ends");
    }

    #[test]
    fn a_client_dropped_before_its_runtime_ends_once_with_its_own_code() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let subscription = client.new_subscription("news").unwrap();
        let mut subscription_events = subscription.events();
        let mut events = client.events();
        client.connect();

        drop(client);
        drop(runtime);

        let (taken, ended) = queued(&mut events);
        assert_eq!(
            disconnected_codes(&taken),
            [codes::disconnected::DISCONNECT_CALLED]
        );
        assert!(ended, "the client's stream ends");
        let mut ended = false;
        while let Some(next) = subscription_events.next().now_or_never() {
            if next.is_none() {
                ended = true;
                break;
            }
        }
        assert!(ended, "the subscription's stream ends");
        let failed = subscription
            .history(crate::HistoryOptions::default())
            .now_or_never()
            .expect("fails at once")
            .unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
    }

    #[test]
    fn after_a_runtime_shut_down_the_client_stays_disconnected() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let mut events = client.events();
        client.connect();
        drop(runtime);
        queued(&mut events);
        let mut events = client.events();

        client.connect();

        assert_eq!(client.state(), State::Disconnected);
        let (taken, _) = queued(&mut events);
        assert!(taken.is_empty(), "{taken:?}");
        let failed = client
            .rpc("echo", b"{}".to_vec())
            .now_or_never()
            .expect("fails at once")
            .unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        assert_eq!(
            client.new_subscription("news").err(),
            Some(crate::SubscriptionError::ClientClosed)
        );
    }

    #[test]
    fn a_runtime_without_time_or_io_ends_the_client_with_code_2() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let mut events = client.events();
        client.connect();

        // The driver panics on its first timer or socket; the stream ends.
        let taken: Vec<ClientEvent> = runtime.block_on(async {
            let mut taken = Vec::new();
            while let Some(event) = events.next().await {
                taken.push(event);
            }
            taken
        });

        assert_eq!(
            disconnected_codes(&taken),
            [codes::disconnected::BAD_PROTOCOL]
        );
    }

    #[test]
    fn a_guard_dropped_with_the_core_poisoned_ends_calls_and_streams_without_an_event() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let mut events = client.events();
        client.connect();
        queued(&mut events);
        let mut call = Box::pin(client.rpc("echo", b"{}".to_vec()));
        assert!(call.as_mut().now_or_never().is_none());
        let shared = Arc::clone(client.shared());

        let panicked = std::thread::spawn(move || {
            // The lock guard drops first as the panic unwinds, poisoning
            // the lock; then the guard runs, still panicking.
            let _ending = Ending(Arc::clone(&shared));
            let _core = shared.core.lock().unwrap();
            panic!("a bug in the driver");
        })
        .join();

        assert!(panicked.is_err());
        let (taken, ended) = queued(&mut events);
        assert!(taken.is_empty(), "{taken:?}");
        assert!(ended, "the stream ends");
        let failed = call.now_or_never().expect("settled").unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        drop(client);
        drop(runtime);
    }

    fn header_names(client: &Client) -> Vec<(String, String)> {
        client
            .shared()
            .http_headers
            .current()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect()
    }

    // Block 32, D1: a refused header leaves the set as it was, named
    // without its value.
    #[tokio::test]
    async fn a_refused_header_leaves_the_set_and_names_the_header_only() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .http_headers([("X-Kept", "k")])
            .build()
            .unwrap();
        for (name, value) in [
            ("Sec-WebSocket-Key", "v-secret"),
            ("Proxy-Authorization", "v-secret"),
            ("bad name", "v-secret"),
            ("X-Bad-Value", "v-secret\n"),
        ] {
            let error = client
                .set_http_headers([("X-New", "n"), (name, value)])
                .expect_err(name);

            assert_eq!(error.kind, crate::ErrorKind::Configuration, "{name}");
            assert!(error.message.contains(name), "{error:?}");
            assert!(!error.message.contains("v-secret"), "{error:?}");
            assert_eq!(
                header_names(&client),
                [("x-kept".to_owned(), "k".to_owned())]
            );
        }
    }

    // Block 32, D1: the whole set is replaced; a name given twice keeps the
    // later value.
    #[tokio::test]
    async fn set_http_headers_replaces_the_whole_set() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .http_headers([("X-Old", "o")])
            .build()
            .unwrap();

        client
            .set_http_headers([("X-New", "1"), ("x-new", "2")])
            .unwrap();

        assert_eq!(
            header_names(&client),
            [("x-new".to_owned(), "2".to_owned())]
        );
    }

    // Block 45, D8: the map calls carry their channel and key to the wire,
    // and the empty publish result resolves them.
    #[cfg(feature = "unstable")]
    #[tokio::test]
    async fn map_calls_send_their_channel_and_key() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (publishes, mut received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(Message::Text(frame))) = ws.next().await {
                for line in frame.as_str().lines() {
                    let command: serde_json::Value = serde_json::from_str(line).unwrap();
                    let id = &command["id"];
                    if command.get("connect").is_some() {
                        let reply = format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                    } else if let Some(publish) = command.get("publish") {
                        publishes.send(publish.clone()).unwrap();
                        let reply = format!(r#"{{"id":{id},"publish":{{}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                    }
                }
            }
        });
        let client = Client::new(endpoint, Config::default()).unwrap();
        client.connect();

        client
            .map_publish("map:a", "k1", br#"{"x":1}"#.to_vec())
            .await
            .unwrap();
        client.map_remove("map:a", "k2").await.unwrap();
        client
            .map_publish_json("map:b", "k3", &serde_json::json!({"y": 2}))
            .await
            .unwrap();

        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(received.recv().await.unwrap());
        }
        assert_eq!(
            seen,
            [
                serde_json::json!({"channel": "map:a", "data": {"x": 1}, "type": 1, "key": "k1"}),
                serde_json::json!({"channel": "map:a", "type": 1, "key": "k2", "removed": true}),
                serde_json::json!({"channel": "map:b", "data": {"y": 2}, "type": 1, "key": "k3"}),
            ]
        );
    }

    // Block 32, D2: the next attempt's upgrade request carries the new set.
    #[tokio::test]
    async fn the_next_upgrade_request_carries_the_new_set() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://{}/connection/websocket",
            listener.local_addr().unwrap()
        );
        let client = Client::builder(url)
            .http_headers([("X-Device-Real", "r1")])
            .build()
            .unwrap();
        client.set_http_headers([("X-Device-Real", "r2")]).unwrap();

        client.connect();
        let (mut socket, _) = listener.accept().await.unwrap();
        // The whole head: TCP may deliver it in several reads.
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0, "the connection ended before the request's head");
            request.extend_from_slice(&buffer[..read]);
        }
        let request = String::from_utf8_lossy(&request).to_lowercase();

        assert!(request.contains("x-device-real: r2\r\n"), "{request}");
        // The header, not "r1" anywhere: the random sec-websocket-key may
        // hold it (seen on macOS, #159).
        assert!(!request.contains("x-device-real: r1"), "{request}");
    }

    // Block 32, D4: the set's values never reach a Debug output.
    #[tokio::test]
    async fn debug_output_shows_header_names_only() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .build()
            .unwrap();
        client
            .set_http_headers([("Authorization", "h-secret")])
            .unwrap();
        let batch = client.start_batching();

        let debug = format!("{client:?} {batch:?} {:?}", client.shared());

        assert!(!debug.contains("h-secret"), "{debug}");
        assert!(debug.contains("authorization"), "{debug}");
        drop(batch);
    }

    /// An `http_stream` server on `listener`: the stream request gets a
    /// connect reply and stays open; an emulation request is read and never
    /// answered, so closing the transport waits for it.
    async fn stalling_http_stream(listener: tokio::net::TcpListener) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut held = Vec::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            // The whole request: its headers, then a body of Content-Length.
            let mut request = Vec::new();
            let mut buffer = [0; 8192];
            loop {
                let read = socket.read(&mut buffer).await.unwrap_or(0);
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).to_lowercase();
                let complete = text.split_once("\r\n\r\n").is_some_and(|(head, body)| {
                    let length = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .and_then(|length| length.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    body.len() >= length
                });
                if read == 0 || complete {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&request).into_owned();
            if text.starts_with("POST /connection/") {
                // The connect command's id, from the request body.
                let id: String = text.split("\"id\":").nth(1).map_or_else(
                    || "1".to_owned(),
                    |rest| rest.chars().take_while(char::is_ascii_digit).collect(),
                );
                let reply = format!(
                    "{{\"id\":{id},\"connect\":{{\"client\":\"c\",\"session\":\"s\",\"node\":\"n\"}}}}\n"
                );
                let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n";
                let chunk = format!("{response}{:x}\r\n{reply}\r\n", reply.len());
                let _ = socket.write_all(chunk.as_bytes()).await;
            }
            held.push(socket);
        }
    }

    // Block 33, D1: close() resolves once the driver has ended, the
    // transport's close awaited (here an emulation request in flight, up
    // to the timeout): no task of the client is left.
    #[test]
    fn close_resolves_once_no_task_of_the_client_is_left() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let server = tokio::runtime::Runtime::new().unwrap();
        let listener = server
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!(
            "http://{}/connection/http_stream",
            listener.local_addr().unwrap()
        );
        server.spawn(stalling_http_stream(listener));
        let timeout = Duration::from_millis(400);
        let client = Client::builder_with_transports([(TransportKind::HttpStream, url)])
            .config(Config::default().timeout(timeout))
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        client.connect();
        let ready = client.ready();
        let connected =
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), ready).await });
        assert_eq!(connected.ok(), Some(Ok(())));
        // An emulation request the server never answers, its deadline
        // counting from when it is made.
        let made = std::time::Instant::now();
        let call = client.rpc("slow", b"{}".to_vec());
        std::thread::sleep(Duration::from_millis(100));

        runtime.block_on(client.close());
        // From the call, not from the close: a sleep that oversleeps, as on
        // macOS runners, shortens the close but not this.
        let took = made.elapsed();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        while runtime.metrics().num_alive_tasks() > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert!(
            took >= timeout.saturating_sub(Duration::from_millis(50)),
            "the close was not awaited: {took:?}"
        );
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
        assert!(client.is_closed());
        let failed = runtime.block_on(call).unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        runtime.shutdown_background();
        server.shutdown_background();
    }

    // Codex, #141: every transport's close is awaited, not only the last.
    #[test]
    fn close_awaits_the_close_of_an_earlier_transport_too() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let server = tokio::runtime::Runtime::new().unwrap();
        let listener = server
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!(
            "http://{}/connection/http_stream",
            listener.local_addr().unwrap()
        );
        server.spawn(stalling_http_stream(listener));
        let timeout = Duration::from_millis(400);
        let client = Client::builder_with_transports([(TransportKind::HttpStream, url)])
            .config(Config::default().timeout(timeout))
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        let wait_ready = |client: &Client| {
            let ready = client.ready();
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), ready).await })
        };
        client.connect();
        assert_eq!(wait_ready(&client).ok(), Some(Ok(())));
        // An emulation request the server never answers keeps the first
        // transport's close running for the timeout.
        let call = client.rpc("slow", b"{}".to_vec());
        std::thread::sleep(Duration::from_millis(100));
        client.reconnect();
        assert_eq!(wait_ready(&client).ok(), Some(Ok(())));

        let started = std::time::Instant::now();
        runtime.block_on(client.close());
        let took = started.elapsed();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        while runtime.metrics().num_alive_tasks() > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert!(
            took >= Duration::from_millis(100),
            "the first close was not awaited: {took:?}"
        );
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
        drop(call);
        runtime.shutdown_background();
        server.shutdown_background();
    }

    // Block 33, D1: dropping the future still closes; a second close()
    // resolves; the stream yields what it held, then ends.
    #[tokio::test]
    async fn a_dropped_close_future_still_closes_and_a_second_one_resolves() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut events = client.events();
        client.connect();
        let call = client.rpc("echo", b"{}".to_vec());

        drop(client.close());

        assert!(client.is_closed());
        assert_eq!(client.state(), State::Disconnected);
        tokio::time::timeout(Duration::from_secs(5), client.close())
            .await
            .expect("a second close resolves");
        let failed = call.await.unwrap_err();
        assert_eq!(failed.code, codes::error::CLIENT_CLOSED);
        let mut names = Vec::new();
        while let Some(event) = events.next().await {
            names.push(event.name());
        }
        assert_eq!(names, ["connecting", "disconnected"]);
    }

    // Block 33, D3: waiting ready() futures fail with code 4.
    #[tokio::test]
    async fn close_fails_waiting_ready_futures_with_code_4() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        client.connect();
        let of_client = client.ready();
        let of_subscription = subscription.ready();

        client.close().await;

        assert_eq!(
            of_client.await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
        assert_eq!(
            of_subscription.await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
        assert_eq!(subscription.state(), crate::SubscriptionState::Unsubscribed);
        assert_eq!(
            subscription.ready().await.unwrap_err().code,
            codes::error::CLIENT_CLOSED
        );
    }

    // Block 33, D1: a runtime shut down closes the client too.
    #[test]
    fn a_runtime_shut_down_closes_the_client() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        assert!(!client.is_closed());

        drop(runtime);

        assert!(client.is_closed());
        assert_eq!(client.close().now_or_never(), Some(()));
    }

    // Block 31, D2: set when connected is seen, cleared otherwise.
    #[test]
    fn connected_since_follows_the_state() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        assert_eq!(client.statistics().connected_since, None);

        let before = SystemTime::now();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c"}}"#);
        drop(core);
        let after = SystemTime::now();
        let first = client.statistics().connected_since.expect("connected");
        assert!(before <= first && first <= after);

        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(
            &mut core,
            Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            },
        );
        drop(core);
        assert_eq!(client.statistics().connected_since, None);

        client.disconnect();
        assert_eq!(client.statistics().connected_since, None);
        assert_eq!(client.statistics().connects, 1);
    }

    // Block 31, D2: a closed client, its core lock poisoned too, keeps its
    // counters.
    #[test]
    fn a_closed_client_keeps_its_counters() {
        let runtime = idle_runtime();
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();
        drop(connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c"}}"#,
        ));
        let shared = Arc::clone(client.shared());

        drop(runtime);
        let poisoned = std::thread::spawn(move || {
            let _core = shared.core.lock().unwrap();
            panic!("poisons the core lock");
        })
        .join();

        assert!(poisoned.is_err());
        let statistics = client.statistics();
        assert_eq!(statistics.connects, 1);
        assert_eq!(statistics.state, State::Disconnected);
    }

    #[test]
    fn building_on_a_runtime_already_shut_down_returns_an_error() {
        let runtime = idle_runtime();
        let handle = runtime.handle().clone();
        drop(runtime);

        let built = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(handle)
            .build();

        let error = built.expect_err("an error, the runtime being gone");
        assert_eq!(error.kind, crate::ErrorKind::Configuration);
        assert!(error.message.contains("shut down"), "{}", error.message);
    }

    /// The code lines of every source file under `src/adapter`, before its
    /// test module, comments left out, with the file's path relative to
    /// `src/adapter` (`/`-separated) and the line's number.
    fn adapter_code() -> Vec<(String, usize, String)> {
        fn visit(
            dir: &std::path::Path,
            root: &std::path::Path,
            out: &mut Vec<(String, usize, String)>,
        ) {
            for entry in std::fs::read_dir(dir).expect("a source directory") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    visit(&path, root, out);
                    continue;
                }
                if path.extension().is_none_or(|extension| extension != "rs") {
                    continue;
                }
                // Line endings normalised: a Windows checkout has CRLF.
                let text = std::fs::read_to_string(&path)
                    .expect("a source file")
                    .replace("\r\n", "\n");
                // Code before the test module only, the tokio adapter's
                // tests included (Block 44, D18).
                let code = [
                    "#[cfg(test)]\nmod tests",
                    "#[cfg(all(test, adapter_tokio))]\nmod tests",
                ]
                .iter()
                .filter_map(|marker| text.find(marker))
                .min()
                .map_or(text.as_str(), |end| &text[..end]);
                let relative = path
                    .strip_prefix(root)
                    .expect("under src/adapter")
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                for (number, line) in code.lines().enumerate() {
                    if !line.trim_start().starts_with("//") {
                        out.push((relative.clone(), number + 1, line.to_owned()));
                    }
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/adapter");
        let mut out = Vec::new();
        visit(&root, &root, &mut out);
        out
    }

    #[test]
    fn the_adapter_spawns_only_through_its_runtime_handle() {
        let found: Vec<String> = adapter_code()
            .into_iter()
            .filter(|(_, _, line)| {
                line.contains("tokio::spawn") || line.contains("tokio::task::spawn")
            })
            .map(|(path, number, line)| format!("{path}:{number}: {line}"))
            .collect();

        assert!(found.is_empty(), "{found:#?}");
    }

    /// Whether `line` names the tokio crate's `path`: `super::tokio::net`
    /// is the adapter's own module, `::tokio::net` and `tokio::net` the
    /// crate's.
    fn names_the_crate(line: &str, path: &str) -> bool {
        line.match_indices(path).any(|(at, _)| {
            let before = &line[..at];
            !before.ends_with("::")
                || before.ends_with(" ::")
                || before.ends_with("(::")
                || before == "::"
        })
    }

    // Block 44, D2: the shared layer names no runtime; tokio's runtime,
    // timers and sockets appear only in the tokio module and the seam.
    #[test]
    fn only_the_tokio_module_and_the_seam_name_tokio_s_runtime_timers_and_sockets() {
        let found: Vec<String> = adapter_code()
            .into_iter()
            .filter(|(path, _, line)| {
                !path.starts_with("tokio/")
                    && path != "platform.rs"
                    && ["tokio::runtime", "tokio::time", "tokio::net"]
                        .iter()
                        .any(|name| names_the_crate(line, name))
            })
            .map(|(path, number, line)| format!("{path}:{number}: {line}"))
            .collect();

        assert!(found.is_empty(), "{found:#?}");
    }

    #[tokio::test]
    async fn http_endpoints_stay_valid_for_http_stream_and_sse() {
        let built = Client::builder_with_transports([
            (
                TransportKind::HttpStream,
                "http://127.0.0.1:1/connection/http_stream".to_owned(),
            ),
            (
                TransportKind::Sse,
                "https://127.0.0.1:1/connection/sse".to_owned(),
            ),
        ])
        .build();

        assert!(built.is_ok(), "{built:?}");
    }

    #[tokio::test]
    async fn a_build_error_is_a_standard_error() {
        fn build() -> Result<Client, Box<dyn std::error::Error>> {
            let client = Client::builder_with_transports([(
                TransportKind::Sse,
                "http://127.0.0.1:1/connection/sse".to_owned(),
            )])
            .config(Config {
                protocol: crate::Protocol::Protobuf,
                ..Config::default()
            })
            .build()?;
            Ok(client)
        }

        let error = build().expect_err("Protobuf over sse is refused");

        let client_error = error.downcast_ref::<ClientError>().expect("a ClientError");
        assert_eq!(
            error.to_string(),
            format!("{} (code 12) over sse", client_error.message)
        );
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn events_stream_starts_after_events_already_emitted() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();

        client.connect();
        let mut events = client.events();
        client.disconnect();
        let first = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event");

        assert!(
            matches!(first, Some(ClientEvent::Disconnected { .. })),
            "{first:?}"
        );
    }

    #[test]
    fn a_sleep_is_the_wall_clock_moving_2_s_further_than_the_monotonic_one() {
        let wall = SystemTime::now();
        let monotonic = std::time::Instant::now();
        let last = (wall, monotonic);
        let later = monotonic + Duration::from_secs(10);

        let gap = |wall_moved: Duration| sleep_gap(last, (wall + wall_moved, later));
        let behind = sleep_gap(last, (wall - Duration::from_secs(5), later));

        assert_eq!(gap(Duration::from_secs(12)), Some(Duration::from_secs(2)));
        assert_eq!(gap(Duration::from_millis(11_999)), None);
        assert_eq!(gap(Duration::from_secs(10)), None);
        assert_eq!(behind, None);
    }

    /// Takes `client`'s core, connected by `reply` to its connect command,
    /// with the actions so far performed. The caller holds the core, so the
    /// driver task does not run until it is dropped.
    fn connected_core<'a>(client: &'a Client, reply: &str) -> std::sync::MutexGuard<'a, Core> {
        connected_core_over(client, reply).0
    }

    /// As [`connected_core`], with the transport.
    fn connected_core_over<'a>(
        client: &'a Client,
        reply: &str,
    ) -> (std::sync::MutexGuard<'a, Core>, TransportId) {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(&mut core, Input::Connect);
        let transport = core
            .actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("an attempt");
        shared.handle(&mut core, Input::TransportOpened(transport));
        // The connect command leaves as the driver would send it.
        shared.handle(&mut core, Input::Flush);
        shared.handle(
            &mut core,
            Input::Frame {
                transport,
                data: reply.as_bytes().to_vec(),
            },
        );
        assert_eq!(core.machine.state(), State::Connected);
        core.actions.clear();
        (core, transport)
    }

    /// The number of the setting of `timer` the driver would fire.
    fn setting(core: &Core, timer: Timer) -> u64 {
        *core
            .armed
            .running
            .get(&TimerKey::Client(timer))
            .expect("a running timer")
    }

    /// The frames among the core's queued actions, as text.
    fn queued_frames(core: &Core) -> Vec<String> {
        core.actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect()
    }

    // Block 36, D3. Single-threaded runtime: the driver runs only when the
    // test yields.
    #[tokio::test(flavor = "current_thread")]
    async fn calls_made_without_yielding_share_a_frame() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        drop(connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c"}}"#,
        ));
        let _a = client.rpc("a", b"{}".to_vec());
        let _b = client.rpc("b", b"{}".to_vec());
        let shared = client.shared();
        let mut core = lock(&shared.core);
        let before = queued_frames(&core);

        shared.handle(&mut core, Input::Flush);

        assert_eq!(before, Vec::<String>::new());
        assert_eq!(
            queued_frames(&core),
            [concat!(
                r#"{"id":2,"rpc":{"method":"a","data":{}}}"#,
                "\n",
                r#"{"id":3,"rpc":{"method":"b","data":{}}}"#
            )]
        );
    }

    // Block 36, D3: a call's notification starts a driver iteration, which
    // flushes before it performs the actions.
    #[tokio::test(flavor = "current_thread")]
    async fn the_driver_flushes_on_the_iteration_a_call_starts() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        drop(connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c"}}"#,
        ));
        let _a = client.rpc("a", b"{}".to_vec());

        tokio::task::yield_now().await;

        let shared = client.shared();
        let mut core = lock(&shared.core);
        assert!(core.actions.is_empty(), "{:?}", core.actions);
        shared.handle(&mut core, Input::Flush);
        assert_eq!(queued_frames(&core), Vec::<String>::new());
    }

    // Codex, #120: the driver took the no-ping firing before another task
    // got the core and handled a short sleep, which set that timer again.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_firing_superseded_by_another_tasks_sleep_handling_is_dropped() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        let set = setting(&core, Timer::ServerPing);

        slept(&mut core, Duration::from_secs(30));
        client
            .shared()
            .handle(&mut core, Input::SetData(b"{}".to_vec()));
        client
            .shared()
            .handle_fired(&mut core, Input::TimerFired(Timer::ServerPing), Some(set));

        assert_eq!(core.machine.state(), State::Connected);
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_firing_of_the_running_setting_is_delivered() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        let set = setting(&core, Timer::ServerPing);

        client
            .shared()
            .handle_fired(&mut core, Input::TimerFired(Timer::ServerPing), Some(set));

        assert_eq!(core.machine.state(), State::Connecting);
    }

    /// Makes the core's next input reveal a sleep of `gap`.
    fn slept(core: &mut Core, gap: Duration) {
        core.clock = Clock(Some((SystemTime::now() - gap, std::time::Instant::now())));
    }

    // Codex, #141: a close that reveals a sleep goes first: calls fail with
    // "client closed", and the client does not reconnect for the sleep.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_close_revealing_a_sleep_goes_before_it() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut events = client.events();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        let shared = Arc::clone(client.shared());
        let call = core.machine.add_call(CallRequest::Rpc {
            method: "slow".into(),
            data: Vec::new(),
        });
        shared.handle(&mut core, Input::Call(call));
        slept(&mut core, Duration::from_secs(120));

        shared.handle(&mut core, Input::Close);

        let settled: Vec<u32> = core
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::CallSettled {
                    result: Err(error), ..
                } => Some(error.code),
                _ => None,
            })
            .collect();
        assert_eq!(settled, [codes::error::CLIENT_CLOSED]);
        drop(core);
        let (taken, _) = queued(&mut events);
        let connecting = taken
            .iter()
            .filter(|event| matches!(event, ClientEvent::Connecting { .. }))
            .count();
        assert_eq!(connecting, 1, "only the connect's: {taken:?}");
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_short_sleep_seen_at_the_no_ping_deadline_keeps_the_connection() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);

        let set = setting(&core, Timer::ServerPing);

        slept(&mut core, Duration::from_secs(30));
        client
            .shared()
            .handle_fired(&mut core, Input::TimerFired(Timer::ServerPing), Some(set));

        assert_eq!(core.machine.state(), State::Connected);
        assert!(
            core.actions
                .iter()
                .any(|action| matches!(action, Action::SetTimer(Timer::ServerPing, _))),
            "{:?}",
            core.actions
        );
        assert!(
            !core
                .actions
                .iter()
                .any(|action| matches!(action, Action::CloseTransport(_))),
            "{:?}",
            core.actions
        );
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_sleep_seen_at_the_refresh_timer_still_refreshes_the_token() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .config(Config {
                token: "t".to_owned(),
                ..Config::default()
            })
            .get_token(|| async { Ok("t2".to_owned()) })
            .build()
            .unwrap();
        let mut core = connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c","ping":25,"expires":true,"ttl":60}}"#,
        );

        slept(&mut core, Duration::from_secs(30));
        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::Refresh));

        assert_eq!(core.machine.state(), State::Connected);
        assert!(
            core.actions
                .iter()
                .any(|action| matches!(action, Action::FetchToken(_))),
            "{:?}",
            core.actions
        );
    }

    /// Keeps the message and fields of each DEBUG record.
    #[derive(Clone, Default)]
    struct Records(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Records {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Text(String);
            impl tracing::field::Visit for Text {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }
            }
            if *event.metadata().level() == tracing::Level::DEBUG {
                let mut text = Text(String::new());
                event.record(&mut text);
                lock(&self.0).push(text.0);
            }
        }
    }

    // Block 25, D10.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_detected_sleep_is_logged_with_its_gap() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        let records = Records::default();
        let subscriber = tracing_subscriber::registry().with(records.clone());

        tracing::subscriber::with_default(subscriber, || {
            slept(&mut core, Duration::from_secs(30));
            client
                .shared()
                .handle(&mut core, Input::TimerFired(Timer::ServerPing));
        });

        let records = lock(&records.0);
        let logged = records
            .iter()
            .find(|record| record.contains("system sleep detected"))
            .unwrap_or_else(|| panic!("{records:?}"));
        // The monotonic clock may move a little meanwhile: 30s, 30.0…s or
        // 29.9…s.
        assert!(
            logged.contains(" gap=29.9") || logged.contains(" gap=30"),
            "{logged}"
        );
    }

    // Codex, #120: the reconnect timer revealing the sleep must not start
    // an attempt with the token that expired meanwhile.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_sleep_seen_at_the_reconnect_timer_fetches_a_token_before_the_attempt() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .config(Config {
                token: "t".to_owned(),
                ..Config::default()
            })
            .get_token(|| async { Ok("t2".to_owned()) })
            .build()
            .unwrap();
        let (mut core, transport) = connected_core_over(
            &client,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":60}}"#,
        );
        client.shared().handle(
            &mut core,
            Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            },
        );
        core.actions.clear();

        slept(&mut core, Duration::from_secs(70));
        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::Reconnect));

        let fetch = core
            .actions
            .iter()
            .position(|action| matches!(action, Action::FetchToken(_)));
        let open = core
            .actions
            .iter()
            .position(|action| matches!(action, Action::OpenTransport { .. }));
        assert!(fetch.is_some(), "{:?}", core.actions);
        // The driver's timer for that refresh would fire it a second time.
        assert!(
            core.actions
                .iter()
                .any(|action| matches!(action, Action::CancelTimer(Timer::Refresh))),
            "{:?}",
            core.actions
        );
        assert!(
            open.is_none_or(|open| fetch.is_some_and(|fetch| fetch < open)),
            "{:?}",
            core.actions
        );
    }

    // Codex, #120: an event the stream returns as a sleep is noticed is
    // already the application's, so its position moves although the sleep
    // drops the connection.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_publication_taken_as_a_sleep_is_noticed_moves_its_position() {
        use futures_util::FutureExt as _;
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        drop(core);
        let subscription = client
            .subscription_builder("news")
            .recoverable(true)
            .build()
            .unwrap();
        let mut events = subscription.events();
        subscription.subscribe();
        let frame = |data: &str| Input::Frame {
            transport,
            data: data.as_bytes().to_vec(),
        };
        {
            let mut core = lock(&client.shared().core);
            let shared = client.shared();
            shared.handle(
                &mut core,
                frame(r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":0}}"#),
            );
            shared.handle(
                &mut core,
                frame(r#"{"push":{"channel":"news","pub":{"data":{"n":1},"offset":1}}}"#),
            );
        }
        for _ in ["subscribing", "subscribed"] {
            events
                .next()
                .now_or_never()
                .expect("an event")
                .expect("a stream");
        }

        slept(&mut lock(&client.shared().core), Duration::from_secs(70));
        let taken = events.next().now_or_never().expect("an event");

        assert!(
            matches!(taken, Some(SubscriptionEvent::Publication(_))),
            "{taken:?}"
        );
        let mut core = lock(&client.shared().core);
        assert_eq!(core.machine.state(), State::Connecting);
        let shared = client.shared();
        shared.handle(&mut core, Input::TimerFired(Timer::Reconnect));
        let again = core
            .actions
            .iter()
            .rev()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a reconnect attempt");
        core.actions.clear();
        shared.handle(&mut core, Input::TransportOpened(again));
        shared.handle(
            &mut core,
            Input::Frame {
                transport: again,
                data: br#"{"id":3,"connect":{"client":"c"}}"#.to_vec(),
            },
        );
        // As the driver does before performing the actions (Block 36, D3).
        shared.handle(&mut core, Input::Flush);
        let sent: Vec<String> = core
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            sent.iter()
                .any(|frame| frame.contains(r#""recover":true"#) && frame.contains(r#""offset":1"#)),
            "{sent:?}"
        );
    }

    // Codex, #169: an event handed to a handler as a sleep is noticed moves
    // its position as one taken does.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_publication_handed_as_a_sleep_is_noticed_moves_its_position() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        drop(core);
        let subscription = client
            .subscription_builder("news")
            .recoverable(true)
            .build()
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);
        subscription.set_handler(move |event: SubscriptionEvent| {
            record.lock().unwrap().push(event.name());
        });
        subscription.subscribe();
        // The consumer's steps, by hand: no task runs in between.
        let hand = |shared: &Shared| {
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let Poll::Ready(Some(handed)) = super::super::handler::next(shared, &mut cx) else {
                panic!("an event for the handler");
            };
            super::super::handler::deliver(shared, handed);
        };
        let frame = |data: &str| Input::Frame {
            transport,
            data: data.as_bytes().to_vec(),
        };
        {
            let mut core = lock(&client.shared().core);
            let shared = client.shared();
            shared.handle(
                &mut core,
                frame(r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":0}}"#),
            );
            shared.handle(
                &mut core,
                frame(r#"{"push":{"channel":"news","pub":{"data":{"n":1},"offset":1}}}"#),
            );
        }
        for _ in ["subscribing", "subscribed"] {
            hand(client.shared());
        }

        slept(&mut lock(&client.shared().core), Duration::from_secs(70));
        hand(client.shared());

        assert_eq!(
            *seen.lock().unwrap(),
            ["subscribing", "subscribed", "publication"]
        );
        let mut core = lock(&client.shared().core);
        assert_eq!(core.machine.state(), State::Connecting);
        let shared = client.shared();
        shared.handle(&mut core, Input::TimerFired(Timer::Reconnect));
        let again = core
            .actions
            .iter()
            .rev()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a reconnect attempt");
        core.actions.clear();
        shared.handle(&mut core, Input::TransportOpened(again));
        shared.handle(
            &mut core,
            Input::Frame {
                transport: again,
                data: br#"{"id":3,"connect":{"client":"c"}}"#.to_vec(),
            },
        );
        // As the driver does before performing the actions (Block 36, D3).
        shared.handle(&mut core, Input::Flush);
        let sent: Vec<String> = core
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            sent.iter()
                .any(|frame| frame.contains(r#""recover":true"#) && frame.contains(r#""offset":1"#)),
            "{sent:?}"
        );
    }

    // Codex, #120: a resubscribe-advised push read after a sleep longer
    // than the subscription token's refresh, shorter than the no-ping
    // window, resubscribes with a new token.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_resubscribe_push_read_after_a_sleep_fetches_a_new_token() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":120}}"#);
        drop(core);
        let subscription = client
            .subscription_builder("news")
            .token("s0")
            .get_token(|_| async { Ok("s1".to_owned()) })
            .build()
            .unwrap();
        let _events = subscription.events();
        subscription.subscribe();
        let frame = |data: &str| Input::Frame {
            transport,
            data: data.as_bytes().to_vec(),
        };
        let mut core = lock(&client.shared().core);
        let shared = client.shared();
        shared.handle(
            &mut core,
            frame(r#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#),
        );
        core.actions.clear();

        slept(&mut core, Duration::from_secs(70));
        shared.handle(
            &mut core,
            frame(
                r#"{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"resubscribe"}}}"#,
            ),
        );

        assert_eq!(core.machine.state(), State::Connected);
        let sent: Vec<String> = core
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            !sent.iter().any(|frame| frame.contains(r#""token":"s0""#)),
            "{sent:?}"
        );
        assert!(
            core.actions
                .iter()
                .rev()
                .take_while(|action| !matches!(action, Action::CancelSubscriptionToken(_)))
                .any(|action| matches!(action, Action::FetchSubscriptionToken { .. })),
            "{:?}",
            core.actions
        );
    }

    // Codex, #120: the refresh timer a subscribe reply sets, read first
    // after a sleep, lost no time to it and is not shortened.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_timer_set_by_the_frame_revealing_a_sleep_is_not_shortened() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":120}}"#);
        drop(core);
        let subscription = client
            .subscription_builder("news")
            .token("s0")
            .get_token(|_| async { Ok("s1".to_owned()) })
            .build()
            .unwrap();
        let _events = subscription.events();
        subscription.subscribe();
        let mut core = lock(&client.shared().core);
        core.actions.clear();

        slept(&mut core, Duration::from_secs(30));
        client.shared().handle(
            &mut core,
            Input::Frame {
                transport,
                data: br#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#.to_vec(),
            },
        );

        assert!(
            core.actions.iter().any(|action| matches!(
                action,
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Refresh, _)
            )),
            "{:?}",
            core.actions
        );
        assert!(
            !core.actions.iter().any(|action| matches!(
                action,
                Action::ShortenSubscriptionTimer(_, SubscriptionTimer::Refresh, _)
            )),
            "{:?}",
            core.actions
        );
    }

    // Codex, #120: the connect reply read first after a sleep the attempt
    // lived through keeps its connection.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_connect_reply_read_after_a_sleep_while_connecting_keeps_the_connection() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(&mut core, Input::Connect);
        let transport = core
            .actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("an attempt");
        shared.handle(&mut core, Input::TransportOpened(transport));
        core.actions.clear();

        slept(&mut core, Duration::from_secs(70));
        shared.handle(
            &mut core,
            Input::Frame {
                transport,
                data: br#"{"id":1,"connect":{"client":"c","ping":25}}"#.to_vec(),
            },
        );

        assert_eq!(core.machine.state(), State::Connected);
        assert!(
            !core
                .actions
                .iter()
                .any(|action| matches!(action, Action::CloseTransport(_))),
            "{:?}",
            core.actions
        );
    }

    // Codex, #120: taking an event releases the messages held behind it; a
    // resubscribe push among them, after a sleep past the subscription
    // token's refresh, resubscribes with a new token.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_resubscribe_push_released_by_taking_an_event_after_a_sleep_fetches_a_new_token() {
        use futures_util::FutureExt as _;
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":120}}"#);
        drop(core);
        let subscription = client
            .subscription_builder("news")
            .token("s0")
            .get_token(|_| async { Ok("s1".to_owned()) })
            .build()
            .unwrap();
        let mut events = subscription.events();
        subscription.subscribe();
        let frame = |data: &str| Input::Frame {
            transport,
            data: data.as_bytes().to_vec(),
        };
        {
            let mut core = lock(&client.shared().core);
            let shared = client.shared();
            shared.handle(
                &mut core,
                frame(r#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#),
            );
            // Held until the subscribe reply's events are taken.
            shared.handle(
                &mut core,
                frame(
                    r#"{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"resubscribe"}}}"#,
                ),
            );
        }
        events
            .next()
            .now_or_never()
            .expect("an event")
            .expect("a stream");
        let mut core = lock(&client.shared().core);
        core.actions.clear();
        slept(&mut core, Duration::from_secs(70));
        drop(core);

        events
            .next()
            .now_or_never()
            .expect("an event")
            .expect("a stream");

        let core = lock(&client.shared().core);
        let sent: Vec<String> = core
            .actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8_lossy(frame).into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            !sent.iter().any(|frame| frame.contains(r#""token":"s0""#)),
            "{sent:?}"
        );
        assert!(
            core.actions
                .iter()
                .rev()
                .take_while(|action| !matches!(action, Action::CancelSubscriptionToken(_)))
                .any(|action| matches!(action, Action::FetchSubscriptionToken { .. })),
            "{:?}",
            core.actions
        );
    }

    // Codex, #120: the server's terminal close, read first after a sleep
    // longer than the no-ping window, still ends the client.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_terminal_close_read_after_a_long_sleep_disconnects() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let (mut core, transport) =
            connected_core_over(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);

        slept(&mut core, Duration::from_secs(70));
        client.shared().handle(
            &mut core,
            Input::TransportClosed {
                transport,
                code: Some(3500),
                reason: "banned".into(),
            },
        );

        assert_eq!(core.machine.state(), State::Disconnected);
    }

    // Codex, #120: creating a stream may be what reveals the sleep, and it
    // wakes the driver for the actions the sleep queued.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_sleep_revealed_by_creating_a_stream_wakes_the_driver() {
        use futures_util::FutureExt as _;
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);
        slept(&mut core, Duration::from_secs(30));
        drop(core);
        let shared = client.shared();
        let _ = shared.actions_queued.notified().now_or_never();

        let _events = client.events();

        assert!(
            shared.actions_queued.notified().now_or_never().is_some(),
            "the driver is not woken"
        );
    }

    // Codex, #122: reconnect() called on the wake notification, first after
    // a sleep longer than the no-ping window, keeps its code and jitter.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_reconnect_revealing_a_long_sleep_keeps_its_code_and_jitter() {
        use futures_util::FutureExt as _;
        let client = Client::new(
            "ws://127.0.0.1:1/connection/websocket",
            Config {
                reconnect_jitter_window: Duration::from_secs(10),
                ..Config::default()
            },
        )
        .unwrap();
        let mut events = client.events();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);

        slept(&mut core, Duration::from_secs(70));
        client.shared().handle(&mut core, Input::Reconnect);

        assert_eq!(core.machine.state(), State::Connecting);
        assert!(
            !core
                .actions
                .iter()
                .any(|action| matches!(action, Action::OpenTransport { .. })),
            "no attempt at once: {:?}",
            core.actions
        );
        assert_eq!(
            core.actions
                .iter()
                .filter(|action| matches!(action, Action::SetTimer(Timer::Reconnect, _)))
                .count(),
            1,
            "{:?}",
            core.actions
        );
        drop(core);
        let mut codes = Vec::new();
        while let Some(Some(event)) = events.next().now_or_never() {
            if let ClientEvent::Connecting { code, .. } = event {
                codes.push(code);
            }
        }
        // `connect()`'s, then `reconnect()`'s.
        assert_eq!(codes, [0, codes::connecting::RECONNECT_CALLED]);
    }

    // Block 25, D7.
    #[tokio::test]
    async fn reconnect_asks_for_the_proxy_settings_to_be_read_again() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let before = super::super::tokio::proxy::settings_generation();

        client.reconnect();

        assert!(super::super::tokio::proxy::settings_generation() > before);
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn without_a_sleep_the_no_ping_deadline_reconnects() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);

        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::ServerPing));

        assert_eq!(core.machine.state(), State::Connecting);
    }

    fn raw_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(tls::crypto_provider()))
                .with_safe_default_protocol_versions()
                .expect("protocol versions")
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    /// Before Android's setup, a connection known now to need the platform
    /// verifier is refused, naming the call (Block 24, D5).
    #[test]
    fn android_refuses_tls_known_at_build_before_the_setup() {
        // Endpoints, the emulation endpoint, the proxy.
        type Case<'a> = (&'a [(TransportKind, &'a str)], Option<&'a str>, Proxy);
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"));
        let cases: [Case<'_>; 4] = [
            (
                &[(
                    TransportKind::WebSocket,
                    "wss://example.com/connection/websocket",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "https://example.com/connection/http_stream",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "http://example.com/connection/http_stream",
                )],
                Some("https://example.com/emulation"),
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::WebSocket,
                    "ws://example.com/connection/websocket",
                )],
                None,
                proxy,
            ),
        ];
        for (endpoints, emulation, proxy) in cases {
            let endpoints: Vec<_> = endpoints
                .iter()
                .map(|(kind, url)| (*kind, (*url).to_owned()))
                .collect();

            let gated = android_gate(true, &Trust::Platform, &endpoints, emulation, &proxy);

            assert!(
                matches!(&gated, Err(error) if error.message.contains("ferrifuge::android::init_with_env")),
                "{endpoints:?} {emulation:?}: {gated:?}"
            );
        }
    }

    /// A no-proxy list excluding every endpoint leaves them direct, with no
    /// verifier needed (Codex, #118).
    #[test]
    fn android_accepts_endpoints_an_https_proxy_excludes_before_the_setup() {
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"))
                .no_proxy("*");
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &proxy).is_ok());
    }

    #[test]
    fn android_accepts_plain_endpoints_and_raw_configurations_before_the_setup() {
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];
        let secure = [(
            TransportKind::WebSocket,
            "wss://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &Proxy::none()).is_ok());
        assert!(
            android_gate(
                true,
                &Trust::Raw(raw_config()),
                &secure,
                None,
                &Proxy::none()
            )
            .is_ok()
        );
        assert!(android_gate(false, &Trust::Platform, &secure, None, &Proxy::none()).is_ok());
    }

    /// Android's verifier takes no extra roots (Block 24, D3).
    #[test]
    fn android_refuses_extra_roots_naming_tls_config() {
        let root = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a certificate")
            .cert
            .der()
            .clone();

        let refused = trust(vec![root.clone()], None, true);
        let elsewhere = trust(vec![root], None, false);

        assert!(
            matches!(&refused, Err(error) if error.message.contains("tls_config")),
            "{:?}",
            refused.err()
        );
        assert!(elsewhere.is_ok());
    }

    /// The fips build never connects with non-FIPS cryptography: a raw
    /// configuration whose `fips()` is false fails `build()` (Block 22, D3).
    #[cfg(feature = "fips")]
    #[test]
    fn the_fips_build_refuses_a_raw_configuration_that_is_not_fips() {
        let mut raw = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::default_fips_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        assert!(raw.fips());
        raw.require_ems = false;

        let built = Client::builder("wss://localhost/connection/websocket")
            .tls_config(Arc::new(raw))
            .build();

        assert!(
            matches!(&built, Err(error) if error.message.contains("fips")),
            "{built:?}"
        );
    }

    /// Each connect of a client without a token asks the callback, which
    /// never answers here: the driver keeps the running callback and one
    /// stale one, and the log no more (Codex, #109).
    #[tokio::test]
    async fn callbacks_the_driver_drops_leave_the_log() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(std::future::pending::<Result<String, TokenError>>)
            .build()
            .unwrap();
        for _ in 0..10 {
            client.connect();
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.disconnect();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let tracked = client.shared.log.tracked_fetches();
        assert!(tracked <= 2, "{tracked} callbacks tracked");
    }

    #[tokio::test]
    async fn panicking_token_callback_is_reported_as_a_failed_fetch() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(|| async { panic!("callback bug") })
            .build()
            .unwrap();
        let mut events = client.events();

        client.connect();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
        };
        let connecting = next().await;
        let error = next().await;

        assert!(
            matches!(connecting, Some(ClientEvent::Connecting { .. })),
            "{connecting:?}"
        );
        assert!(
            matches!(&error, Some(ClientEvent::Error(e)) if e.message == "token callback panicked"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn token_callback_panicking_before_its_future_is_a_failed_fetch() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(|| -> std::future::Ready<Result<String, TokenError>> {
                panic!("callback bug")
            })
            .build()
            .unwrap();
        let mut events = client.events();

        client.connect();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
        };
        next().await;
        let error = next().await;

        assert!(
            matches!(&error, Some(ClientEvent::Error(e)) if e.message == "token callback panicked"),
            "{error:?}"
        );
    }

    // Codex on PR #65: dropping the stream that held a full read-ahead
    // lets the held messages go, and the driver must then read again.
    #[tokio::test]
    async fn dropping_the_stream_that_held_the_read_ahead_resumes_reading() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        // Answers the connect and the subscribe, then pushes two
        // publications and a disconnect.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(Message::Text(frame))) = ws.next().await {
                for line in frame.as_str().lines() {
                    let command: serde_json::Value = serde_json::from_str(line).unwrap();
                    let id = &command["id"];
                    if command.get("connect").is_some() {
                        let reply = format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                    } else if command.get("subscribe").is_some() {
                        let reply = format!(r#"{{"id":{id},"subscribe":{{}}}}"#);
                        ws.send(Message::text(reply)).await.unwrap();
                        for n in 1..=2 {
                            let push = format!(
                                r#"{{"push":{{"channel":"news","pub":{{"data":{{"n":{n}}}}}}}}}"#
                            );
                            ws.send(Message::text(push)).await.unwrap();
                        }
                        let bye = r#"{"push":{"disconnect":{"code":3500,"reason":"bye"}}}"#;
                        ws.send(Message::text(bye)).await.unwrap();
                    }
                }
            }
        });
        let config = Config {
            read_ahead_messages: 1,
            ..Config::default()
        };
        let client = Client::new(endpoint, config).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        let mut events = subscription.events();
        client.connect();
        subscription.subscribe();
        while !matches!(events.next().await, Some(SubscriptionEvent::Subscribed(_))) {}
        let filled = tokio::time::timeout(Duration::from_secs(5), async {
            while client.read_ahead().0 < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        drop(events);
        let read_on = tokio::time::timeout(Duration::from_secs(2), async {
            while client.state() == State::Connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(filled.is_ok(), "the read-ahead never filled");
        assert!(read_on.is_ok(), "the disconnect push was never read");
    }

    /// A websocket server answering every connect and subscribe command:
    /// its endpoint, and the number of connect commands it got.
    async fn answering_server() -> (String, Arc<AtomicUsize>) {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let connects = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&connects);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    while let Some(Ok(Message::Text(frame))) = ws.next().await {
                        for line in frame.as_str().lines() {
                            let command: serde_json::Value = serde_json::from_str(line).unwrap();
                            let id = &command["id"];
                            let reply = if command.get("connect").is_some() {
                                counted.fetch_add(1, Ordering::SeqCst);
                                format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#)
                            } else if command.get("subscribe").is_some() {
                                format!(r#"{{"id":{id},"subscribe":{{}}}}"#)
                            } else {
                                continue;
                            };
                            ws.send(Message::text(reply)).await.unwrap();
                        }
                    }
                });
            }
        });
        (endpoint, connects)
    }

    /// Subscribes a subscription to "news", set up by `with_callback` and
    /// retried at once, on a client connecting to an answering server: its
    /// events up to "subscribed", by name, an error with its message; then
    /// the client state and the connect commands the server got.
    async fn subscribed_with(
        with_callback: impl FnOnce(SubscriptionBuilder) -> SubscriptionBuilder,
    ) -> (Vec<String>, State, usize) {
        let (endpoint, connects) = answering_server().await;
        let client = Client::new(endpoint, Config::default()).unwrap();
        let subscription = with_callback(
            client
                .subscription_builder("news")
                .min_resubscribe_delay(Duration::from_millis(10))
                .max_resubscribe_delay(Duration::from_millis(50)),
        )
        .build()
        .unwrap();
        let mut events = subscription.events();

        subscription.subscribe();
        client.connect();
        let mut seen = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("an event")
                .expect("an open stream");
            seen.push(match &event {
                SubscriptionEvent::Error(error) => format!("error: {}", error.message),
                event => event.name().to_owned(),
            });
            if matches!(event, SubscriptionEvent::Subscribed(_)) {
                break;
            }
        }
        (seen, client.state(), connects.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_panicking_subscription_token_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_token(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok("t".to_owned())
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription token callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
    }

    #[tokio::test]
    async fn a_panicking_subscription_data_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_data(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok(b"{}".to_vec())
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription data callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
    }

    #[tokio::test]
    async fn a_panicking_get_state_callback_is_retried() {
        let calls = AtomicUsize::new(0);

        let (events, state, connects) = subscribed_with(|builder| {
            builder.get_state(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    assert!(!first, "callback bug");
                    Ok(StreamPosition {
                        offset: 0,
                        epoch: "e".into(),
                    })
                }
            })
        })
        .await;

        assert_eq!(
            events,
            [
                "subscribing",
                "error: subscription getState callback panicked",
                "subscribed"
            ]
        );
        assert_eq!((state, connects), (State::Connected, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn debug_output_of_builder_and_client_never_shows_tokens() {
        let config = Config {
            token: "secret-token".into(),
            ..Config::default()
        };
        let builder = Client::builder("ws://127.0.0.1:1/connection/websocket").config(config);
        let printed_builder = format!("{builder:?}");
        let client = builder.build().unwrap();
        // Queues the connect command, which carries the token, unperformed.
        client.connect();

        let printed = format!("{printed_builder} {client:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    #[test]
    fn the_builder_shows_whether_it_has_a_connector() {
        let without = Client::builder("ws://127.0.0.1:1/connection/websocket");
        let with = Client::builder("ws://127.0.0.1:1/connection/websocket").connector(
            |_: http::Uri| async {
                Err::<tokio::net::TcpStream, _>(std::io::Error::other("never called"))
            },
        );

        assert!(format!("{without:?}").contains("connector: false"));
        assert!(format!("{with:?}").contains("connector: true"));
    }

    // Block 47, D7: replies with ever shorter deadlines keep one timer per
    // subscription, at the earliest of them, and a later deadline moves
    // nothing.
    #[cfg(feature = "unstable")]
    #[tokio::test(start_paused = true)]
    async fn one_refresh_timer_per_subscription_at_the_earliest_deadline() {
        let mut refreshes = SharedPollRefreshes::default();
        let id = SubscriptionId::for_tests(1);
        let start = tokio::time::Instant::now();
        for ttl in (50..=60).rev() {
            refreshes.set_earliest(id, Duration::from_secs(ttl));
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        refreshes.set_earliest(id, Duration::from_secs(120));
        assert_eq!(refreshes.len(), 1);

        let Input::SharedPollRefreshFired(fired) = refreshes.next_input().await else {
            unreachable!("only refreshes");
        };

        assert_eq!(fired, id);
        // The 50 s deadline, set after ten advances of 100 ms.
        assert_eq!(start.elapsed(), Duration::from_secs(51));
        assert_eq!(refreshes.len(), 0);
    }

    #[tokio::test]
    async fn timers_fire_in_deadline_order_and_cancelled_ones_never() {
        let mut timers = Timers::default();
        timers.set(Timer::ServerPing, Duration::from_millis(60), 1);
        timers.set(Timer::Refresh, Duration::from_millis(20), 2);
        timers.set(Timer::Reconnect, Duration::from_millis(40), 3);
        timers.cancel(Timer::Reconnect);

        let first = timers.fired().await;
        let second = timers.fired().await;
        let none_left = tokio::time::timeout(Duration::from_millis(100), timers.fired()).await;

        // Each with the number of its setting.
        assert_eq!(
            (first, second),
            ((Timer::Refresh, 2), (Timer::ServerPing, 1))
        );
        assert!(none_left.is_err(), "{none_left:?}");
    }

    // Block 46, D5: every timer ready at the poll, none not due, whatever
    // was cancelled, on a paused clock that only advances past timers.
    #[tokio::test(start_paused = true)]
    async fn fired_all_returns_every_timer_due_and_none_not_due() {
        let mut timers = KeyedTimers::<u32>::default();
        timers.set(1, Duration::from_millis(20), 1);
        timers.set(2, Duration::from_millis(20), 2);
        timers.set(3, Duration::from_millis(50), 3);
        timers.set(4, Duration::from_millis(50), 4);
        timers.cancel(4);

        let mut due = timers.fired_all().await;
        due.sort_unstable();
        let later = timers.fired_all().await;
        let none_left = tokio::time::timeout(Duration::from_secs(1), timers.fired_all()).await;

        assert_eq!(due, [1, 2]);
        assert_eq!(later, [3]);
        assert!(none_left.is_err(), "{none_left:?}");
    }

    #[tokio::test]
    async fn huge_connect_timeout_does_not_panic_the_driver() {
        let client = Client::new(
            "ws://127.0.0.1:1/connection/websocket",
            Config {
                timeout: Duration::MAX,
                ..Config::default()
            },
        )
        .unwrap();
        let mut events = client.events();
        let mut next = async || {
            tokio::time::timeout(Duration::from_secs(5), events.next())
                .await
                .expect("driver stopped emitting events")
        };

        client.connect();
        let connecting = next().await;
        let error = next().await;
        client.disconnect();
        let disconnected = next().await;

        assert!(
            matches!(connecting, Some(ClientEvent::Connecting { .. })),
            "{connecting:?}"
        );
        assert!(matches!(error, Some(ClientEvent::Error(_))), "{error:?}");
        assert!(
            matches!(disconnected, Some(ClientEvent::Disconnected { .. })),
            "{disconnected:?}"
        );
    }
}
