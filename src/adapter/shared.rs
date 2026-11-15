use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::{Duration, SystemTime};

use super::outbox::Outbox;
use futures_core::Stream;
use tokio::sync::{Notify, oneshot};

use super::client::PendingCall;
use super::client::client_closed;
use super::log::Log;
use super::platform::{self, Handle};
use super::ready::{self, Waiters};
#[cfg(adapter_tokio)]
use super::tokio::http::HeaderSet;
#[cfg(adapter_tokio)]
use super::tokio::net::Network;
#[cfg(feature = "__test-hooks")]
use crate::sans_io::DebounceTimer;
use crate::sans_io::{
    Action, CallId, Input, Machine, SubscriptionId, SubscriptionTimer, Timer, TimerKey,
    TimerSetting,
};
use crate::{
    CallError, CallReply, CallRequest, ClientEvent, DataError, MaybeSend, State, StateError,
    StreamPosition, SubscriptionEvent, TokenError,
};
use tracing::debug;

/// What the client handle, its subscriptions and its driver task share.
#[derive(Debug)]
pub(super) struct Shared {
    pub(super) core: Mutex<Core>,
    /// Events waiting for the streams of the client and its subscriptions.
    pub(super) outboxes: Mutex<Outboxes>,
    /// Data and token callbacks of the subscriptions that have them.
    pub(super) callbacks: Mutex<SubscriptionCallbacks>,
    /// Where the outcome of each call goes, by call.
    // Block 13.
    pub(super) calls: Mutex<HashMap<CallId, oneshot::Sender<Result<CallReply, CallError>>>>,
    /// The waiting `ready()` futures. Locked after `core`.
    // Block 30.
    pub(super) ready: Mutex<Waiters>,
    /// Set, and `driver_ended` notified, when the driver's task has ended.
    // Block 33, D1.
    pub(super) driver_gone: AtomicBool,
    pub(super) driver_ended: Notify,
    /// Wakes the driver when actions are queued.
    pub(super) actions_queued: Notify,
    /// The debounce timers the driver holds unarmed for a test, oldest
    /// first; `None` while it arms them.
    // Block 46.
    #[cfg(feature = "__test-hooks")]
    pub(super) held_debounce_timers: Mutex<Option<Vec<DebounceTimer>>>,
    /// The driver panics before handling its next input, for a test.
    // Block 49.
    #[cfg(feature = "__test-hooks")]
    pub(super) driver_panic_planted: AtomicBool,
    /// Notified when the driver's end is about to await the close of an
    /// earlier transport than the last one closed, for a test.
    // Codex, #141.
    #[cfg(feature = "__test-hooks")]
    pub(super) awaiting_earlier_close: Notify,
    /// The driver's timers and the calls' ages, for the soak.
    // Block 52.
    #[cfg(feature = "__test-hooks")]
    pub(super) records: super::hooks::Records,
    /// Set when the client handle is dropped: the driver stops once the
    /// queued actions are performed.
    pub(super) closed: AtomicBool,
    /// Spans and records of this client.
    // Block 21.
    pub(super) log: Log,
    /// Its TLS and proxy settings and HTTP clients.
    // Blocks 22, 23.
    #[cfg(adapter_tokio)]
    pub(super) network: Arc<Network>,
    /// The real HTTP headers of every transport request, which
    /// `set_http_headers` replaces.
    // Blocks 17, 32.
    #[cfg(adapter_tokio)]
    pub(super) http_headers: HeaderSet,
    /// The handlers of the client and its subscriptions. Locked after
    /// `outboxes`.
    // Block 41b.
    pub(super) handlers: Mutex<super::handler::Handlers>,
    /// The runtime the client's tasks run on.
    // Block 28.
    pub(super) runtime: Handle,
    /// The browser's network and page listeners. Locked after `core`.
    // Block 44, D10, D11.
    #[cfg(adapter_browser)]
    pub(super) events: Mutex<super::backend::events::Events>,
}

/// Events waiting for their streams. Locked after [`Shared::core`], never
/// before.
// Block 15.
#[derive(Debug, Default)]
pub(super) struct Outboxes {
    pub(super) client: Outbox<ClientEvent>,
    /// Those of the subscriptions whose `events()` was called.
    pub(super) subscriptions: HashMap<SubscriptionId, Outbox<SubscriptionEvent>>,
    /// Those of the map subscriptions whose `events()` was called.
    // Block 45.
    #[cfg(feature = "unstable")]
    pub(super) maps: HashMap<SubscriptionId, Outbox<crate::MapSubscriptionEvent>>,
    /// Those of the shared poll subscriptions whose `events()` was called.
    // Block 47.
    #[cfg(feature = "unstable")]
    pub(super) shared_polls: HashMap<SubscriptionId, Outbox<crate::SharedPollSubscriptionEvent>>,
}

impl Outboxes {
    fn withdraw(&mut self, ids: &[crate::sans_io::EventId]) {
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
    pub(super) fn new(machine: Machine) -> Self {
        Self {
            machine,
            actions: VecDeque::new(),
            clock: Clock::default(),
            refreshes: Refreshes::default(),
            connected_since: None,
            #[cfg(feature = "__test-hooks")]
            timer_record: super::hooks::TimerRecord::default(),
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
    /// The clocks at the last input, to notice a system sleep.
    // Block 25.
    pub(super) clock: Clock,
    /// When each token refresh timer the driver runs comes due, so that a
    /// sleep's refreshes are given before the input revealing it.
    // Block 25, D1.
    pub(super) refreshes: Refreshes,
    /// When the client was seen to become connected, while it is.
    // Block 31, D2.
    pub(super) connected_since: Option<SystemTime>,
    /// The timers the core set and did not cancel, for the soak, kept once
    /// the records are started.
    // Block 52, D8.
    #[cfg(feature = "__test-hooks")]
    pub(super) timer_record: super::hooks::TimerRecord,
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
/// least 2 s. A wall clock set back gives none.
// Block 25, D1.
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

/// The deadlines of the token refresh timers the driver runs, by
/// subscription (`None` for the connection's), each with its setting, kept
/// beside the core: an application task that reveals a sleep cannot reach
/// the driver's timers.
// Block 25, D1, D3; Block 53 D4.
#[derive(Debug, Default)]
pub(super) struct Refreshes(HashMap<Option<SubscriptionId>, (platform::Instant, TimerSetting)>);

impl Refreshes {
    /// Follows `action`, which the driver will perform.
    fn note(&mut self, action: &Action) {
        match action {
            Action::SetTimer(Timer::Refresh, delay, setting) => self.set(None, *delay, *setting),
            Action::SetSubscriptionTimer(id, SubscriptionTimer::Refresh, delay, setting) => {
                self.set(Some(*id), *delay, *setting);
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

    fn set(&mut self, key: Option<SubscriptionId>, delay: Duration, setting: TimerSetting) {
        self.0
            .insert(key, (platform::Instant::now() + delay, setting));
    }

    /// Every refresh comes due `by` earlier, as the core's shortening of
    /// all of them on a sleep will make the driver's timers do.
    fn shorten_all(&mut self, by: Duration) {
        for (deadline, _) in self.0.values_mut() {
            *deadline = deadline
                .checked_sub(by)
                .unwrap_or_else(platform::Instant::now);
        }
    }

    /// `input` is the firing of a refresh timer: the entry of its setting
    /// no longer runs, so that each refresh is given once.
    // Block 53 D4.
    fn fired(&mut self, input: &Input) {
        let (key, setting) = match input {
            Input::TimerFired(Timer::Refresh, setting) => (None, *setting),
            Input::SubscriptionTimerFired(id, SubscriptionTimer::Refresh, setting) => {
                (Some(*id), *setting)
            }
            _ => return,
        };
        if self
            .0
            .get(&key)
            .is_some_and(|(_, running)| *running == setting)
        {
            self.0.remove(&key);
        }
    }

    /// Removes the refresh timers due now, and names them with their
    /// settings.
    fn due(&mut self) -> Vec<(Option<SubscriptionId>, TimerSetting)> {
        let now = platform::Instant::now();
        let mut due: Vec<_> = self
            .0
            .iter()
            .filter(|(_, (deadline, _))| *deadline <= now)
            .map(|(key, (_, setting))| (*key, *setting))
            .collect();
        due.sort_unstable_by_key(|(key, _)| *key);
        for (key, _) in &due {
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

/// Async callback signing keys of a shared poll subscription.
// Block 47, D6.
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
        let mut core = lock(&self.core);
        // The driver has stopped (the client was dropped, after its final
        // disconnect, or its runtime shut down): actions would pile up with nothing to perform them,
        // e.g. from a subscription handle that outlived the client.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.handle(&mut core, input);
        drop(core);
        self.actions_queued.notify_one();
    }

    /// Closes the client, in one locked section: an API call either runs
    /// before and is closed with the rest, or after and sees the client
    /// closed. The `ready()` waiters fail with "client closed" before the
    /// machine's new states would judge them.
    // Block 33, D1.
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

    /// Resolves once the driver's task has ended, however it ended.
    // Block 33, D1.
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
    #[cfg(test)]
    #[cfg(adapter_tokio)]
    pub(super) fn next_action_for_tests(&self) -> Option<Action> {
        self.next_action()
    }

    /// Whether the driver was woken since this was last asked, for tests.
    #[cfg(test)]
    #[cfg(adapter_tokio)]
    pub(super) fn woken(&self) -> bool {
        use futures_util::FutureExt as _;
        self.actions_queued.notified().now_or_never().is_some()
    }

    /// A network or page event a listener heard.
    // Block 44, D10, D11.
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
        // Application calls and stream reads feed the machine too, on the
        // application's tasks.
        let _entered = self.log.span.enter();
        // A sleep since the last input is the machine's first news of the
        // wake (Block 25, D1).
        let Some(gap) = core.clock.tick() else {
            self.give(core, input);
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
                // ignored by the core (Block 53 D2); a refresh firing is
                // given once.
                core.refreshes.fired(&input);
                self.fire_due_refreshes(core);
                self.feed(core, input);
            }
        }
        // A stream created or polled may be what revealed the sleep, with
        // no notification of its own.
        self.actions_queued.notify_one();
    }

    /// Gives the machine `input`, then the sleep of `gap` it revealed. The
    /// timers `input` set lost no time to the sleep: the sleep does not
    /// shorten them.
    // Codex, #120.
    fn feed_before_sleep(&self, core: &mut Core, input: Input, gap: Duration) {
        // The timers `input` set are those of the set actions it returned
        // (Block 53 D4).
        let before = core.actions.len();
        self.feed(core, input);
        let fresh: HashSet<TimerKey> = core
            .actions
            .range(before..)
            .filter_map(|action| match action {
                Action::SetTimer(timer, ..) => Some(TimerKey::Client(*timer)),
                Action::SetSubscriptionTimer(id, timer, ..) => {
                    Some(TimerKey::Subscription(*id, *timer))
                }
                Action::SetCallTimer(id, timer, ..) => Some(TimerKey::Call(*id, *timer)),
                _ => None,
            })
            .collect();
        let from = core.actions.len();
        // The soak's record of the fresh timers, whose shortening is
        // filtered out below, is kept as it was (Codex, #264).
        #[cfg(feature = "__test-hooks")]
        let kept: Vec<_> = fresh
            .iter()
            .map(|key| {
                let name = super::hooks::name(*key);
                let entry = core.timer_record.get(&name);
                (name, entry)
            })
            .collect();
        self.feed(core, Input::Slept { gap });
        #[cfg(feature = "__test-hooks")]
        if self.records.on() {
            for (name, entry) in kept {
                core.timer_record.put(name, entry);
            }
        }
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

    /// Gives the machine `input`; a refresh firing is given once.
    fn give(&self, core: &mut Core, input: Input) {
        core.refreshes.fired(&input);
        self.feed(core, input);
    }

    /// Gives the machine the firings of the refreshes a sleep made due,
    /// before the input that revealed it, so that an attempt or a subscribe
    /// it starts fetches a new token first; the driver's timers for them
    /// are cancelled.
    // Block 25, D3.
    fn fire_due_refreshes(&self, core: &mut Core) {
        for (of, setting) in core.refreshes.due() {
            let (cancel, fired) = match of {
                None => (
                    Action::CancelTimer(Timer::Refresh),
                    Input::TimerFired(Timer::Refresh, setting),
                ),
                Some(id) => (
                    Action::CancelSubscriptionTimer(id, SubscriptionTimer::Refresh),
                    Input::SubscriptionTimerFired(id, SubscriptionTimer::Refresh, setting),
                ),
            };
            #[cfg(feature = "__test-hooks")]
            if self.records.on() {
                core.timer_record.action(&cancel);
            }
            core.actions.push_back(cancel);
            self.feed(core, fired);
        }
    }

    /// Gives the machine `input`, and the inputs its events cause.
    fn feed(&self, core: &mut Core, input: Input) {
        let mut inputs = VecDeque::from([input]);
        while let Some(input) = inputs.pop_front() {
            self.log.input(&input);
            #[cfg(feature = "__test-hooks")]
            if self.records.on() {
                core.timer_record.input(&input);
            }
            for action in core.machine.handle(input) {
                self.log.action(&action);
                #[cfg(feature = "__test-hooks")]
                if self.records.on() {
                    core.timer_record.action(&action);
                }
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

    /// Moves every queued action into `into`, which is empty, under one
    /// lock, after feeding [`Input::Flush`] when `flush`: API calls that
    /// keep queueing no longer win the lock once per action against the
    /// driver. A take that finds none releases what the queue grew; the
    /// driver's buffer, which takes turns with the queue holding the
    /// actions, is released after each turn's batches.
    // Block 14.
    pub(super) fn take_actions(&self, into: &mut VecDeque<Action>, flush: bool) {
        let mut core = lock(&self.core);
        if flush {
            self.handle(&mut core, Input::Flush);
        }
        if core.actions.is_empty() {
            core.actions.shrink_to(crate::RESTING_SLOTS);
        } else {
            std::mem::swap(&mut core.actions, into);
        }
    }

    #[cfg(test)]
    #[cfg(adapter_tokio)]
    fn next_action(&self) -> Option<Action> {
        lock(&self.core).actions.pop_front()
    }

    /// Makes a call now, of `subscription` when given; the future gives its
    /// outcome. Dropping the future abandons the call.
    // Block 13.
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
                #[cfg(feature = "__test-hooks")]
                if self.records.on() {
                    lock(&self.records.calls).made(id);
                }
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

/// A stream of the client's or a subscription's events, taking them from
/// its outbox: each event it yields is reported taken to the machine under
/// the core lock, so a withdrawal never races a take.
// Block 15.
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
pub(super) type GetToken =
    platform::Callback<platform::BoxFuture<'static, Result<String, TokenError>>>;

/// Async callback returning the data of the next connect command.
pub(super) type GetConnectData =
    platform::Callback<platform::BoxFuture<'static, Result<Vec<u8>, DataError>>>;

pub(super) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // No code panics while holding these locks; recover rather than cascade.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use super::*;
    use crate::Client;
    use crate::Config;
    use crate::adapter::test_support::*;
    use crate::codes;
    use futures_util::StreamExt;
    use std::time::Duration;

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn an_event_stream_does_not_borrow_its_client() {
        let (client, mut events) = client_with_events();

        client.connect();
        let first = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("an event");

        assert!(
            matches!(first, Some(ClientEvent::Connecting(_))),
            "{first:?}"
        );
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
            matches!(first, Some(ClientEvent::Disconnected(_))),
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

    // Codex, #120: the driver took the no-ping firing before another task
    // got the core and handled a short sleep, which set that timer again.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn a_firing_superseded_by_another_tasks_sleep_handling_is_ignored() {
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
            .handle(&mut core, Input::TimerFired(Timer::ServerPing, set));

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
            .handle(&mut core, Input::TimerFired(Timer::ServerPing, set));

        assert_eq!(core.machine.state(), State::Connecting);
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
            .filter(|event| matches!(event, ClientEvent::Connecting(_)))
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
            .handle(&mut core, Input::TimerFired(Timer::ServerPing, set));

        assert_eq!(core.machine.state(), State::Connected);
        assert!(
            core.actions
                .iter()
                .any(|action| matches!(action, Action::SetTimer(Timer::ServerPing, ..))),
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
            .get_token(|_| async { Ok("t2".to_owned()) })
            .build()
            .unwrap();
        let mut core = connected_core(
            &client,
            r#"{"id":1,"connect":{"client":"c","ping":25,"expires":true,"ttl":60}}"#,
        );

        slept(&mut core, Duration::from_secs(30));
        let fired = setting(&core, Timer::Refresh);
        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::Refresh, fired));

        assert_eq!(core.machine.state(), State::Connected);
        assert!(
            core.actions
                .iter()
                .any(|action| matches!(action, Action::FetchToken(_))),
            "{:?}",
            core.actions
        );
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
        // While a single dispatcher is registered, tracing-core caches the
        // interest of a callsite first reached on another thread from that
        // thread's default subscriber: a test thread with none caches
        // "never", and this test's subscriber then misses the record
        // (macOS, run 43392682353). With a second dispatcher alive, every
        // registered one is asked, this test's included.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());

        tracing::subscriber::with_default(subscriber, || {
            slept(&mut core, Duration::from_secs(30));
            let fired = setting(&core, Timer::ServerPing);
            client
                .shared()
                .handle(&mut core, Input::TimerFired(Timer::ServerPing, fired));
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
            .get_token(|_| async { Ok("t2".to_owned()) })
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
        let fired = setting(&core, Timer::Reconnect);
        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::Reconnect, fired));

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
        let fired = setting(&core, Timer::Reconnect);
        shared.handle(&mut core, Input::TimerFired(Timer::Reconnect, fired));
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
        let fired = setting(&core, Timer::Reconnect);
        shared.handle(&mut core, Input::TimerFired(Timer::Reconnect, fired));
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
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Refresh, ..)
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

    // Codex, #264: the soak's record keeps the deadline of a timer whose
    // shortening the sleep that revealed it filtered out.
    #[cfg(feature = "__test-hooks")]
    #[tokio::test]
    async fn the_record_of_a_timer_set_by_the_frame_revealing_a_sleep_is_not_shortened() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        client.record_timers();
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
        drop(core);

        // Set at 80 to 90% of 60 s, and not 30 s earlier.
        let timers = client.armed_timers();
        let refresh = timers
            .core
            .iter()
            .find(|timer| {
                timer.name.starts_with("subscription ") && timer.name.ends_with(" Refresh")
            })
            .expect("the subscription's refresh in the core's list");
        assert!(refresh.due_in > Duration::from_secs(40), "{refresh:?}");
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
                .filter(|action| matches!(action, Action::SetTimer(Timer::Reconnect, ..)))
                .count(),
            1,
            "{:?}",
            core.actions
        );
        drop(core);
        let mut codes = Vec::new();
        while let Some(Some(event)) = events.next().now_or_never() {
            if let ClientEvent::Connecting(crate::ConnectingContext { code, .. }) = event {
                codes.push(code);
            }
        }
        // `connect()`'s, then `reconnect()`'s.
        assert_eq!(codes, [0, codes::connecting::RECONNECT_CALLED]);
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn without_a_sleep_the_no_ping_deadline_reconnects() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let mut core = connected_core(&client, r#"{"id":1,"connect":{"client":"c","ping":25}}"#);

        let fired = setting(&core, Timer::ServerPing);
        client
            .shared()
            .handle(&mut core, Input::TimerFired(Timer::ServerPing, fired));

        assert_eq!(core.machine.state(), State::Connecting);
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

    /// The stream borrows nothing: an application can keep it next to its
    /// client, returned together or in one struct.
    fn client_with_events() -> (Client, impl Stream<Item = ClientEvent> + Unpin) {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let events = client.events();
        (client, events)
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
                    "#[cfg(test)]\n#[cfg(adapter_tokio)]\nmod tests",
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
                // Test code whole: `mod.rs` declares it `#[cfg(test)]`
                // (Block 61, D1).
                if relative == "test_support.rs" {
                    continue;
                }
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

    /// The setting of `timer` the driver would fire.
    fn setting(core: &Core, timer: Timer) -> TimerSetting {
        core.machine.timer_setting(timer).expect("a running timer")
    }

    /// Makes the core's next input reveal a sleep of `gap`.
    fn slept(core: &mut Core, gap: Duration) {
        core.clock = Clock(Some((SystemTime::now() - gap, std::time::Instant::now())));
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
}
