use crate::sans_io::TimerKey;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures_util::future::{self, AbortHandle};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};

use super::backend::Inbound;
use super::platform::{self, Handle, Sleep};
use crate::sans_io::{
    Action, DebounceTimer, FetchId, FrameType, Input, SubscriptionId, TimerSetting, TransportId,
    TransportKind,
};
use crate::{DataError, MaybeSend, StateError, TokenError};
use tracing::{debug, trace, warn};

/// Websocket close code "message too big" (RFC 6455).
const MESSAGE_TOO_BIG: u16 = 1009;

/// Inputs the driver takes in a row from the branches above the timers
/// before those branches sit out one turn: biased toward the transport, so
/// that what arrived is handled before a timer that expired with it, the
/// driver must not starve the timers under a server flood, also while the
/// application takes events, each take waking the actions branch.
// Block 49, row 28/55.
pub(super) const UPPER_STREAK: u32 = 32;

use super::backend::{Opening, Transport};
use super::client::perform_batches;
use super::shared::GetConnectData;
use super::shared::GetToken;
use super::shared::Shared;
use super::shared::lock;

type Fetching = platform::BoxFuture<'static, Result<String, TokenError>>;

/// Performs the machine's actions, and turns transport activity and timers
/// into inputs for it.
pub(super) struct Driver {
    /// Endpoints of the transports, in the machine's order.
    pub(super) endpoints: Vec<String>,
    /// Websocket subprotocol of the configured encoding, if it needs one.
    pub(super) subprotocol: Option<&'static str>,
    /// Where commands over emulated transports go, when not the default.
    pub(super) emulation_endpoint: Option<String>,
    /// Time allowed for an emulation request, and for writing a websocket
    /// frame or closing a websocket (`Config::timeout`).
    pub(super) timeout: Duration,
    /// Largest message accepted from the server (`Config::max_message_bytes`).
    pub(super) max_message: usize,
    /// What the driver keeps of the options only this target has: natively
    /// the network and the HTTP headers, in the browser the constructors,
    /// the credentials flag and the inbox bound.
    // Block 61, D2.
    pub(super) backend: super::backend::DriverPart,
    pub(super) shared: Arc<Shared>,
    /// The runtime the driver runs on, for every spawn.
    // Block 28, D1.
    pub(super) runtime: Handle,
    /// Ends the client however the driver's task ends.
    // Block 28, D3.
    pub(super) _ending: Ending,
    /// Where the handlers run: `Inline` on this driver.
    // Block 41, D2.
    pub(super) delivery: super::EventDelivery,
    /// The closings of the transports closed that may still run, each
    /// awaited before the driver stops.
    // Block 33, D1; Codex, #141.
    pub(super) closing: Vec<platform::JoinHandle<()>>,
    pub(super) opening: Option<(TransportId, Opening)>,
    pub(super) transport: Option<(TransportId, Transport)>,
    /// The kind of the transport opening or open, for the records.
    pub(super) kind: TransportKind,
    /// Whether the transport was read on the last turn, for the records of
    /// the read-ahead filling and draining.
    // Block 21.
    pub(super) reading: bool,
    /// Inputs taken in a row from the branches above the timers (the
    /// actions wake, the inline handler, the open, the transport), with no
    /// wait between them: after [`UPPER_STREAK`] of them, those branches
    /// sit out one turn.
    // Block 49, row 28/55.
    pub(super) upper_streak: u32,
    pub(super) get_token: Option<GetToken>,
    pub(super) get_data: Option<GetConnectData>,
    pub(super) fetching: Option<(FetchId, Fetching)>,
    /// The callback a newer one replaced, still polled: when it was a
    /// refresh of a connection that is gone, its denial must still reach
    /// the machine, which ignores any other answer of it.
    pub(super) stale_fetching: Option<(FetchId, Fetching)>,
    /// The machine's timers, of every family, each with its setting.
    // Block 53 D4; Block 61, D4.
    pub(super) timers: KeyedTimers,
    /// Subscription callbacks and the connect data callback running, each yielding the
    /// input answering it; an aborted one yields `None`.
    pub(super) callback_fetches: FuturesUnordered<CallbackFetching>,
    /// Handles to abort the callbacks running, by fetch.
    pub(super) callback_aborts: HashMap<FetchId, AbortHandle>,
    /// The actions taken from the core and not yet performed, between two
    /// takes; its slots go back to the core's queue at the next take.
    pub(super) taken: VecDeque<Action>,
    /// Set when performing an action fed the core an input (a failed
    /// write's close): the actions that input queued are taken in the same
    /// turn.
    pub(super) fed: bool,
}

type CallbackFetching = platform::BoxFuture<'static, Option<(FetchId, Input)>>;

/// The machine's timers, of every family, by key, each with the setting it
/// was set with.
// Block 53 D4; Block 61, D4.
#[derive(Default)]
pub(super) struct KeyedTimers(HashMap<TimerKey, (Pin<Box<Sleep>>, TimerSetting)>);

impl KeyedTimers {
    /// Sets `key`'s timer, of `setting`. A shared poll refresh running with
    /// an earlier deadline keeps that deadline and takes `setting`.
    fn set(&mut self, key: TimerKey, delay: Duration, setting: TimerSetting) {
        #[cfg(feature = "unstable")]
        if let TimerKey::SharedPollRefresh(_) = key
            && let Some((sleep, running)) = self.0.get_mut(&key)
        {
            let deadline = platform::Instant::now() + delay;
            if deadline < sleep.deadline() {
                sleep.as_mut().reset(deadline);
            }
            *running = setting;
            return;
        }
        // sleep() clamps delays too large for an Instant instead of panicking.
        self.0
            .insert(key, (Box::pin(platform::sleep(delay)), setting));
    }

    fn cancel(&mut self, key: TimerKey) {
        self.0.remove(&key);
    }

    fn shorten(&mut self, key: TimerKey, by: Duration) {
        if let Some((sleep, _)) = self.0.get_mut(&key) {
            shortened(sleep, by);
        }
    }

    /// The number of timers running.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Resolves with the timers to feed, which are removed, each with its
    /// setting: of those ready at this poll, the one with the earliest
    /// deadline, ties in [`rank`] order, or, when it is a debounce window,
    /// every debounce window ready. Never while none is ready or none is
    /// set. Cancel-safe: unfired timers keep running.
    // Block 46, D5; Block 61, D4.
    async fn fired(&mut self) -> Vec<(TimerKey, TimerSetting)> {
        std::future::poll_fn(|cx| {
            let mut ready: Vec<(platform::Instant, TimerKey)> = Vec::new();
            for (key, (sleep, _)) in &mut self.0 {
                if sleep.as_mut().poll(cx).is_ready() {
                    ready.push((sleep.deadline(), *key));
                }
            }
            let Some(&(_, first)) = ready
                .iter()
                .min_by_key(|(deadline, key)| (*deadline, rank(*key)))
            else {
                return Poll::Pending;
            };
            let keys: Vec<TimerKey> = if matches!(first, TimerKey::Debounce(_)) {
                // Fed together, before the next flush: windows ending
                // together share a frame (Block 46, D5, D9).
                ready
                    .into_iter()
                    .map(|(_, key)| key)
                    .filter(|key| matches!(key, TimerKey::Debounce(_)))
                    .collect()
            } else {
                vec![first]
            };
            Poll::Ready(
                keys.into_iter()
                    .filter_map(|key| self.0.remove(&key).map(|(_, setting)| (key, setting)))
                    .collect(),
            )
        })
        .await
    }
}

/// The order of timers due at one deadline: by family, in `TimerKey`'s
/// variant order, and a client's timers in [`Timer::ALL`] order.
// Block 61, D4.
fn rank(key: TimerKey) -> (u8, usize) {
    match key {
        TimerKey::Client(timer) => (0, timer.index()),
        TimerKey::Subscription(..) => (1, 0),
        TimerKey::Call(..) => (2, 0),
        TimerKey::Debounce(_) => (3, 0),
        #[cfg(feature = "unstable")]
        TimerKey::SharedPoll(_) => (4, 0),
        #[cfg(feature = "unstable")]
        TimerKey::SharedPollRefresh(_) => (5, 0),
    }
}

/// Makes `sleep` end `by` earlier; a deadline already past fires at once.
// Block 25, D3.
fn shortened(sleep: &mut Pin<Box<Sleep>>, by: Duration) {
    let deadline = sleep
        .deadline()
        .checked_sub(by)
        .unwrap_or_else(platform::Instant::now);
    sleep.as_mut().reset(deadline);
}

impl Driver {
    /// The client is closed. The close queued its actions before setting
    /// the flag, under the core lock: performed now, its transport's close
    /// among them, even when it came after the queue was last emptied.
    /// Every transport's close is awaited, so that `close()` resolves after
    /// it; the guard then ends the streams as the driver goes.
    // Codex, #141; Block 33, D1.
    async fn finish(&mut self) {
        self.perform_all_queued();
        if self.delivery == super::EventDelivery::Inline {
            super::handler::deliver_remaining(&self.shared);
            self.perform_all_queued();
        }
        let closing = std::mem::take(&mut self.closing);
        // Kept in the order the transports were closed: all but the last
        // are earlier transports'.
        #[cfg(feature = "__test-hooks")]
        let mut earlier = closing.len().saturating_sub(1);
        for closing in closing {
            #[cfg(feature = "__test-hooks")]
            if earlier > 0 {
                earlier -= 1;
                self.shared.awaiting_earlier_close.notify_one();
            }
            let _ = closing.await;
        }
    }

    /// Performs the turn's actions: everything one take moves, the flush
    /// included, then again only while the batch just performed fed the
    /// core an input, whose actions belong to that input. An API call
    /// queueing meanwhile waits for the next turn, its wake stored: callers
    /// that keep queueing cannot hold the driver in one drain.
    // Block 49c.
    fn perform_turn(&mut self) {
        let mut taken = std::mem::take(&mut self.taken);
        perform_batches(
            self,
            &mut taken,
            true,
            |driver, into, flush| driver.shared.take_actions(into, flush),
            |driver, action| {
                driver.fed = false;
                driver.perform(action);
                driver.fed
            },
        );
        self.taken = taken;
    }

    /// Performs every queued action, until a take finds none: the client is
    /// closed, so no call queues more, and the close's actions, queued with
    /// the flag under the core lock, are among them.
    // Codex, #141.
    fn perform_all_queued(&mut self) {
        let mut taken = std::mem::take(&mut self.taken);
        loop {
            self.shared.take_actions(&mut taken, false);
            if taken.is_empty() {
                break;
            }
            while let Some(action) = taken.pop_front() {
                self.perform(action);
            }
        }
        self.taken = taken;
    }

    /// Feeds the core `input` while performing an action: the actions it
    /// queues are taken in this turn.
    fn feed(&mut self, input: Input) {
        self.fed = true;
        self.shared.apply(input);
    }

    #[expect(clippy::too_many_lines, reason = "one arm per source of input")]
    pub(super) async fn run(mut self) {
        // Set when the select! waited: every branch was pending once. Kept
        // across turns, for the yield at the top of the next one; the first
        // turn starts as if after a wait.
        let waited = AtomicBool::new(true);
        loop {
            // Everything the core buffered since the last flush, from this
            // task's inputs and the application's calls, goes out in as few
            // frames as the cap allows (Block 36, D3).
            self.perform_turn();
            #[cfg(feature = "__test-hooks")]
            if self.shared.records.on() {
                self.publish_timers();
            }
            if self.shared.closed.load(Ordering::Acquire) {
                return self.finish().await;
            }
            // A turn whose select! did not wait gave the runtime no chance
            // to poll the sockets and timers: the transport branch would
            // see only what the runtime last polled, and a deadline the
            // runtime polls with a reply would win the timers' turn over
            // it. The actions wake spends no coop budget, so API calls made
            // without pause would hold the runtime for as long as they last
            // (Block 49, row 28/55; Codex, #248). Yielding once lets the
            // runtime poll them first. Only once the last input's actions
            // are performed and its timers published: no other task of the
            // runtime runs between an input and what it set, so a timer is
            // armed from the input that set it, not from a later turn.
            if !waited.swap(false, Ordering::Relaxed) {
                platform::yield_now().await;
            }
            let reading = self.reading();
            if self.delivery.is_thread() {
                super::handler::check_thread_waits(&self.shared);
            }
            // The branches above the timers sit out one turn after a streak
            // of inputs, so that they never starve a refresh, call or reply
            // deadline; the flag clears after that turn, whatever wins
            // (Block 49, row 28/55).
            let yielding = self.upper_streak >= UPPER_STREAK;
            if yielding {
                self.upper_streak = 0;
            }
            let mut from_upper = false;
            // Biased, in this order: the inline handler, the open, the
            // transport, the actions wake, then the timers and the rest.
            // What reached the transport is handled before a timer that
            // expired in the same turn, as after a suspension, where an
            // unbiased choice could let the deadline win over the reply or
            // the ping already readable (Block 49, row 28/55;
            // centrifuge-rust §9.7).
            let input = tokio::select! {
                biased;
                // Under `Inline` the handler runs here: nothing else of the
                // driver runs until it returns (Block 41, D3).
                Some(handed) = std::future::poll_fn(|cx| super::handler::next(&self.shared, cx)),
                    if !yielding && self.delivery == super::EventDelivery::Inline =>
                {
                    super::handler::deliver(&self.shared, handed);
                    self.upper_input(waited.load(Ordering::Relaxed));
                    continue;
                }
                (id, result) = opened(&mut self.opening), if !yielding => {
                    from_upper = true;
                    self.opening = None;
                    match result {
                        Ok(transport) => {
                            debug!(transport = self.kind.as_str(), "transport opened");
                            self.transport = Some((id, transport));
                            Input::TransportOpened(id)
                        }
                        Err(failure) => {
                            debug!(transport = self.kind.as_str(), reason = %failure, "transport failed");
                            Input::TransportFailed { transport: id, message: failure.message }
                        }
                    }
                }
                (id, inbound) = received(&mut self.transport, reading), if !yielding => {
                    from_upper = true;
                    match inbound {
                    Inbound::Frame(data) => {
                        trace!(
                            transport = self.kind.as_str(),
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
                            transport = self.kind.as_str(),
                            "a message from the server is larger than the size limit"
                        );
                        Input::MessageTooLarge(id)
                    }
                    #[cfg(adapter_browser)]
                    Inbound::Overflow => {
                        self.transport = None;
                        warn!(
                            client = self.shared.log.number,
                            transport = self.kind.as_str(),
                            "the inbox overflowed while the read-ahead was full"
                        );
                        Input::InboxOverflow(id)
                    }
                }},
                // Queued by an API call: performed at the top of every turn,
                // whichever branch woke it, so this branch only wakes the
                // loop, after the transport, which sustained API calls must
                // not starve (Codex, #248); a wake found stored, without a
                // wait, is followed by the yield at the top of the loop.
                () = self.shared.actions_queued.notified(), if !yielding => {
                    self.upper_input(waited.load(Ordering::Relaxed));
                    continue;
                }
                // Each with the setting its sleep was armed with: the core
                // ignores one no longer running (Block 53 D2, D4; Block 61,
                // D4). Debounce windows due together come in one take, fed
                // before the next flush.
                fired = self.timers.fired() => {
                    let mut fired = fired.into_iter();
                    let Some((key, setting)) = fired.next() else {
                        continue;
                    };
                    let rest: Vec<_> = fired.collect();
                    if !rest.is_empty() {
                        self.shared.apply(Input::TimerFired(key, setting));
                        for (key, setting) in rest {
                            self.shared.apply(Input::TimerFired(key, setting));
                        }
                        self.upper_streak = 0;
                        continue;
                    }
                    Input::TimerFired(key, setting)
                }
                Some(fetched) = self.callback_fetches.next(),
                    if !self.callback_fetches.is_empty() =>
                {
                    // Aborted: the machine no longer wants it.
                    let Some((fetch, input)) = fetched else {
                        self.upper_streak = 0;
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
                // The upper branches sat out and nothing else was due.
                () = std::future::ready(()), if yielding => continue,
                // Polled only when every branch above was pending: the
                // select! waits, which restarts the streak.
                () = std::future::poll_fn(|_| {
                    waited.store(true, Ordering::Relaxed);
                    Poll::<()>::Pending
                }) => unreachable!("never ready"),
            };
            if from_upper {
                self.upper_input(waited.load(Ordering::Relaxed));
            } else {
                self.upper_streak = 0;
            }
            #[cfg(feature = "__test-hooks")]
            assert!(
                !self
                    .shared
                    .driver_panic_planted
                    .swap(false, Ordering::SeqCst),
                "a panic planted in the driver (Block 49)"
            );
            self.shared.apply(input);
        }
    }

    /// An input from a branch above the timers: the streak goes on, or
    /// restarts at this input when the `select!` waited for it.
    fn upper_input(&mut self, waited: bool) {
        self.upper_streak = if waited { 1 } else { self.upper_streak + 1 };
    }

    /// Whether to read the transport: a full read-ahead stops the reading,
    /// so that the server's handling of slow clients acts instead. Taking
    /// an event wakes the loop.
    // Block 15.
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

    /// Publishes the timers the driver's own maps run, for the soak.
    // Block 52, D8.
    #[cfg(feature = "__test-hooks")]
    fn publish_timers(&self) {
        use super::hooks;
        let running = self
            .timers
            .0
            .iter()
            .map(|(key, (sleep, _))| (hooks::name(*key), sleep.deadline()))
            .collect();
        lock(&self.shared.records.driver).observe(running);
    }

    #[expect(clippy::too_many_lines, reason = "one arm per action")]
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
                    transport = self.kind.as_str(),
                    frame_type = ?frame_type,
                    bytes = frame.len(),
                    "frame sent"
                );
                if let Some((id, open)) = &mut self.transport
                    && *id == transport
                    && open.send(frame_id, frame, frame_type).is_err()
                {
                    self.transport = None;
                    self.feed(Input::TransportClosed {
                        transport,
                        code: None,
                        reason: String::new(),
                    });
                }
            }
            // A test may hold the debounce windows (`__test-hooks`).
            Action::SetTimer(TimerKey::Debounce(timer), delay, setting) => {
                self.set_debounce_timer(timer, delay, setting);
            }
            Action::SetTimer(key, delay, setting) => self.timers.set(key, delay, setting),
            Action::CancelTimer(key) => self.timers.cancel(key),
            Action::ShortenTimer(key, by) => self.timers.shorten(key, by),
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

impl Driver {
    /// Arms a debounce window's timer, unless a test holds them
    /// (`__test-hooks`).
    fn set_debounce_timer(&mut self, timer: DebounceTimer, delay: Duration, setting: TimerSetting) {
        #[cfg(feature = "__test-hooks")]
        if let Some(held) = lock(&self.shared.held_debounce_timers).as_mut() {
            held.push((timer, setting));
            return;
        }
        self.timers.set(TimerKey::Debounce(timer), delay, setting);
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
        let transport = self.kind.as_str();
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
            headers: self.backend.http_headers.clone(),
            network: Arc::clone(&self.backend.network),
            emulation: self.emulation_endpoint.clone(),
            initial_frame,
        };
        #[cfg(adapter_browser)]
        let found = self.backend.shims.find();
        // Skipped in the round, as centrifuge-js `_initializeTransport`
        // skips a transport not supported (Block 44, D12).
        #[cfg(adapter_browser)]
        if !found.usable(kind) {
            debug!(transport = kind.as_str(), "transport not available");
            self.feed(Input::TransportUnsupported(id));
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
            read_ahead: self.backend.read_ahead,
            emulation: self.emulation_endpoint.clone(),
            initial_frame,
            found,
            include_credentials: self.backend.include_credentials,
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

    /// Runs a shared poll subscription's `get_signature` callback; its
    /// answer to an abandoned request changes nothing in the core.
    // Block 47, D6.
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
    StateError::new("subscription getState callback panicked")
}

fn callback_panicked() -> TokenError {
    TokenError::Failed("token callback panicked".to_owned())
}

/// Resolves when the transport being opened is ready; never without one.
async fn opened(
    opening: &mut Option<(TransportId, Opening)>,
) -> (
    TransportId,
    Result<Transport, super::body::TransportFailure>,
) {
    match opening {
        Some((id, future)) => (*id, future.await),
        None => std::future::pending().await,
    }
}

/// Ends the client when the driver's task ends, however it ends: after the
/// final disconnect, or dropped by a runtime shutting down or by a panic,
/// which would otherwise leave calls and streams pending forever, even
/// after the client is dropped (as hyper's dispatch callback fails its
/// request when its task is dropped).
// Block 28, D3, D4.
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
        lock(&shared.outboxes).every(|outbox| outbox.close());
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

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use super::*;
    use crate::Client;
    use crate::ClientEvent;
    use crate::Config;
    use crate::State;
    use crate::StreamPosition;
    use crate::SubscriptionEvent;
    use crate::adapter::SubscriptionBuilder;
    use crate::adapter::shared::Core;
    use crate::adapter::test_support::*;
    use crate::codes;
    use crate::sans_io::{CallId, CallTimer, SubscriptionTimer, Timer};
    use futures_util::StreamExt;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

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
            matches!(taken.last(), Some(ClientEvent::Disconnected(crate::DisconnectedContext { reason, .. })) if reason == "runtime shut down"),
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

    // Codex, #141: a close made while the driver performs an action, after
    // its turn took the queue, from a callback it runs, is still performed:
    // the transport's close is awaited, not dropped.
    #[test]
    fn a_close_from_a_callback_the_driver_runs_is_performed() {
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
        let ready =
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), ready).await });
        assert_eq!(ready.ok(), Some(Ok(())));
        // An emulation request the server never answers keeps the
        // transport's close running for the timeout.
        let call = client.rpc("slow", b"{}".to_vec());
        std::thread::sleep(Duration::from_millis(100));
        let weak = client.downgrade();
        let subscription = client
            .subscription_builder("news")
            .get_data(move |_| {
                // Inside the driver's perform: the close's actions are
                // queued after the turn's take.
                if let Some(client) = weak.upgrade() {
                    drop(client.close());
                }
                std::future::ready(Ok(b"{}".to_vec()))
            })
            .build()
            .unwrap();

        let started = std::time::Instant::now();
        subscription.subscribe();
        // The callback closes the client, not this test: only then do the
        // close's actions come after the turn's take.
        let closing = std::time::Instant::now() + Duration::from_secs(5);
        while !client.is_closed() && std::time::Instant::now() < closing {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(client.is_closed(), "the callback did not close the client");
        runtime.block_on(client.close());
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(100),
            "the transport was dropped, not closed: {took:?}"
        );
        drop(call);
        runtime.shutdown_background();
        server.shutdown_background();
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

    // One take moves the whole queue, under one lock, in order. Single-
    // threaded runtime: the driver runs only when the test yields.
    #[tokio::test(flavor = "current_thread")]
    async fn one_take_moves_every_queued_action() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let shared = client.shared();
        let queued: Vec<Action> = (0..100).map(marker).collect();
        {
            let mut core = lock(&shared.core);
            core.actions.clear();
            core.actions.extend(queued.iter().cloned());
        }
        let mut taken = VecDeque::new();

        shared.take_actions(&mut taken, false);

        assert_eq!(Vec::from(taken), queued);
        assert!(lock(&shared.core).actions.is_empty());
    }

    // Block 14: a queue a burst grew, emptied without a take, is released
    // by the take that finds nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn a_take_that_finds_none_releases_a_burst() {
        let client =
            Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();
        let shared = client.shared();
        {
            let mut core = lock(&shared.core);
            core.actions.extend((0..1000).map(marker));
            core.actions.clear();
        }
        assert!(lock(&shared.core).actions.capacity() > crate::RESTING_SLOTS);
        let mut taken = VecDeque::new();

        shared.take_actions(&mut taken, false);

        assert!(lock(&shared.core).actions.capacity() <= crate::RESTING_SLOTS);
    }

    // Block 49c: a failed write's close, fed to the core while its send is
    // performed, has its actions performed in the same turn, before the
    // runtime's other tasks run.
    #[test]
    fn the_actions_of_an_input_fed_while_performing_come_in_the_same_turn() {
        let mut script = Script {
            queue: VecDeque::from([marker(0), marker(1)]),
            performed: Vec::new(),
            then: Some((marker(0), marker(2), true)),
        };
        let mut into = VecDeque::new();

        perform_batches(&mut script, &mut into, true, Script::take, Script::perform);

        assert_eq!(script.performed, [marker(0), marker(1), marker(2)]);
        assert!(script.queue.is_empty());
    }

    // An action an API call queues between two takes, with no input fed by
    // the driver, waits for the next turn: callers that keep queueing
    // cannot hold the driver in one drain.
    #[test]
    fn an_action_queued_between_two_takes_waits_for_the_next_turn() {
        let mut script = Script {
            queue: VecDeque::from([marker(0), marker(1)]),
            performed: Vec::new(),
            then: Some((marker(0), marker(2), false)),
        };
        let mut into = VecDeque::new();

        perform_batches(&mut script, &mut into, true, Script::take, Script::perform);

        assert_eq!(script.performed, [marker(0), marker(1)]);
        assert_eq!(Vec::from(script.queue), [marker(2)]);
    }

    // Codex, #271: a turn that performs a burst releases its buffer before
    // the driver waits, since no later turn may come (a disconnect settling
    // many calls leaves nothing to wake it).
    #[test]
    fn a_turn_that_performs_a_burst_releases_its_buffer() {
        let mut script = Script {
            queue: (0..1000).map(marker).collect(),
            performed: Vec::new(),
            then: None,
        };
        let mut into = VecDeque::new();

        perform_batches(&mut script, &mut into, true, Script::take, Script::perform);

        assert_eq!(script.performed.len(), 1000);
        assert!(into.is_empty());
        assert!(into.capacity() <= crate::RESTING_SLOTS);
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

    /// Each connect of a client without a token asks the callback, which
    /// never answers here: the driver keeps the running callback and one
    /// stale one, and the log no more (Codex, #109).
    #[tokio::test]
    async fn callbacks_the_driver_drops_leave_the_log() {
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(|_| std::future::pending::<Result<String, TokenError>>())
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
            .get_token(|_| async { panic!("callback bug") })
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
            matches!(connecting, Some(ClientEvent::Connecting(_))),
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
            .get_token(|_| -> std::future::Ready<Result<String, TokenError>> {
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

    /// The keys of `fired`, in order.
    fn keys(fired: Vec<(TimerKey, TimerSetting)>) -> Vec<TimerKey> {
        fired.into_iter().map(|(key, _)| key).collect()
    }

    // Block 47, D7; Block 61, D4: replies with ever shorter deadlines keep
    // one timer per subscription, at the earliest of them; a later deadline
    // moves nothing but gives its setting, which the firing carries.
    #[cfg(feature = "unstable")]
    #[tokio::test(start_paused = true)]
    async fn a_refresh_set_again_fires_at_the_earlier_deadline_with_the_new_setting() {
        let mut timers = KeyedTimers::default();
        let key = TimerKey::SharedPollRefresh(SubscriptionId::for_tests(1));
        let start = tokio::time::Instant::now();
        for ttl in (50..=60).rev() {
            timers.set(key, Duration::from_secs(ttl), TimerSetting::for_tests(ttl));
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        timers.set(key, Duration::from_secs(120), TimerSetting::for_tests(120));
        assert_eq!(timers.len(), 1);

        let fired = timers.fired().await;

        assert_eq!(fired, [(key, TimerSetting::for_tests(120))]);
        // The 50 s deadline, set after ten advances of 100 ms.
        assert_eq!(start.elapsed(), Duration::from_secs(51));
        assert_eq!(timers.len(), 0);
    }

    #[tokio::test]
    async fn timers_fire_in_deadline_order_and_cancelled_ones_never() {
        let mut timers = KeyedTimers::default();
        let n = TimerSetting::for_tests;
        let client = TimerKey::Client;
        timers.set(client(Timer::ServerPing), Duration::from_millis(60), n(1));
        timers.set(client(Timer::Refresh), Duration::from_millis(20), n(2));
        timers.set(client(Timer::Reconnect), Duration::from_millis(40), n(3));
        timers.cancel(client(Timer::Reconnect));

        let first = timers.fired().await;
        let second = timers.fired().await;
        let none_left = tokio::time::timeout(Duration::from_millis(100), timers.fired()).await;

        // Each with its setting.
        assert_eq!(
            (first, second),
            (
                vec![(client(Timer::Refresh), n(2))],
                vec![(client(Timer::ServerPing), n(1))]
            )
        );
        assert!(none_left.is_err(), "{none_left:?}");
    }

    // Block 61, D4: of the timers ready at one poll, the earliest deadline
    // first, whatever the family; ties in `TimerKey`'s variant order, the
    // client's timers in `Timer::ALL` order.
    #[tokio::test(start_paused = true)]
    async fn timers_ready_together_come_by_deadline_then_by_family() {
        let mut timers = KeyedTimers::default();
        let n = TimerSetting::for_tests;
        let call = TimerKey::Call(CallId::for_tests(1), CallTimer::Reply);
        let subscription =
            TimerKey::Subscription(SubscriptionId::for_tests(1), SubscriptionTimer::Resubscribe);
        let refresh = TimerKey::Client(Timer::Refresh);
        let reconnect = TimerKey::Client(Timer::Reconnect);
        timers.set(call, Duration::from_millis(30), n(1));
        timers.set(refresh, Duration::from_millis(30), n(2));
        timers.set(reconnect, Duration::from_millis(30), n(3));
        timers.set(subscription, Duration::from_millis(20), n(4));
        // All four are ready at the first poll.
        tokio::time::advance(Duration::from_millis(50)).await;

        let mut order = Vec::new();
        for _ in 0..4 {
            order.extend(keys(timers.fired().await));
        }

        assert_eq!(order, [subscription, reconnect, refresh, call]);
    }

    // Block 46, D5; Block 61, D4: every debounce window ready at the poll
    // comes in one take, none not due, whatever was cancelled, on a paused
    // clock that only advances past timers.
    #[tokio::test(start_paused = true)]
    async fn every_debounce_window_due_comes_in_one_take() {
        let mut timers = KeyedTimers::default();
        let window = |n| TimerKey::Debounce(crate::sans_io::DebounceTimer::for_tests(n));
        let n = TimerSetting::for_tests;
        timers.set(window(1), Duration::from_millis(20), n(1));
        timers.set(window(2), Duration::from_millis(20), n(2));
        timers.set(window(3), Duration::from_millis(50), n(3));
        timers.set(window(4), Duration::from_millis(50), n(4));
        timers.cancel(window(4));

        let mut due = timers.fired().await;
        due.sort_unstable_by_key(|(_, setting)| format!("{setting:?}"));
        let later = timers.fired().await;
        let none_left = tokio::time::timeout(Duration::from_secs(1), timers.fired()).await;

        assert_eq!(due, [(window(1), n(1)), (window(2), n(2))]);
        assert_eq!(later, [(window(3), n(3))]);
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
            matches!(connecting, Some(ClientEvent::Connecting(_))),
            "{connecting:?}"
        );
        assert!(matches!(error, Some(ClientEvent::Error(_))), "{error:?}");
        assert!(
            matches!(disconnected, Some(ClientEvent::Disconnected(_))),
            "{disconnected:?}"
        );
    }

    fn disconnected_codes(events: &[ClientEvent]) -> Vec<u32> {
        events
            .iter()
            .filter_map(|event| match event {
                ClientEvent::Disconnected(crate::DisconnectedContext { code, .. }) => Some(*code),
                _ => None,
            })
            .collect()
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

    /// Distinct actions for the drain tests.
    fn marker(n: usize) -> Action {
        Action::CancelTimer(TimerKey::Client(Timer::ALL[n % Timer::COUNT]))
    }

    /// A scripted turn for `perform_batches`: the core's queue, what was
    /// performed, and what performing one action queues.
    struct Script {
        queue: VecDeque<Action>,
        performed: Vec<Action>,
        /// Performing `.0` queues `.1`; `.2` says whether it fed the core an
        /// input (true) or stands for an API call made meanwhile (false).
        then: Option<(Action, Action, bool)>,
    }

    impl Script {
        fn take(&mut self, into: &mut VecDeque<Action>, _flush: bool) {
            std::mem::swap(&mut self.queue, into);
        }

        fn perform(&mut self, action: Action) -> bool {
            let fed = match self.then.take() {
                Some((trigger, queued, fed)) if trigger == action => {
                    self.queue.push_back(queued);
                    fed
                }
                other => {
                    self.then = other;
                    false
                }
            };
            self.performed.push(action);
            fed
        }
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
}
