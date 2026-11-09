//! Block 46: publish debouncing as advised by the server. A subscription's
//! publishes send the first value at once and the latest one per interval,
//! per key (`""` on a stream subscription); a publish merged in a window
//! resolves with the result of the send that carried later data, never
//! before the server answered it. Without an interval, nothing here runs.

use std::collections::HashMap;
use std::time::Duration;

use super::{Action, CallId, DebounceTimer, Machine, SubscriptionId};
use crate::{CallError, CallRequest, codes};

/// The windows of a machine (Block 46, D5): one timer each, as the core has
/// no clock to keep one per subscription.
#[derive(Debug, Default)]
pub(super) struct Debouncing {
    /// Whether [`Machine::with_publish_debounce`] turned it on.
    pub(super) enabled: bool,
    next_timer: u64,
    /// The windows open, by timer.
    windows: HashMap<DebounceTimer, Window>,
}

/// An open debounce window.
#[derive(Debug)]
struct Window {
    subscription: SubscriptionId,
    key: String,
    /// The value its timer will send: the latest publish made in it, or
    /// `None` when none waits (D11).
    latest: Option<CallRequest>,
    /// The publishes waiting for that value's result, in the order they
    /// were made.
    waiters: Vec<CallId>,
}

/// What a subscription that was advised an interval holds (Block 46, D15).
#[derive(Debug)]
pub(super) struct Windows {
    /// The effective interval (D2).
    interval: Duration,
    /// The timer of the window open for each key.
    by_key: HashMap<String, DebounceTimer>,
}

/// The error of a publish dropped from a window as its subscription left
/// `subscribed` while the client stayed up (Block 46, D10).
pub(super) fn publish_dropped() -> CallError {
    CallError::new(codes::error::PUBLISH_DROPPED, "publish dropped")
}

impl Machine {
    /// The windows a subscribe reply advising `advised` milliseconds leaves
    /// the subscription with (Block 46, D2): the interval is at most
    /// `Config::timeout`, as a wait of a call is (D6); 0, or a machine
    /// without [`Machine::with_publish_debounce`], means no debounce.
    pub(super) fn debounce_windows(&self, advised: u32) -> Option<Box<Windows>> {
        if !self.debouncing.enabled {
            return None;
        }
        let interval = Duration::from_millis(u64::from(advised)).min(self.config.timeout);
        (!interval.is_zero()).then(|| {
            Box::new(Windows {
                interval,
                by_key: HashMap::new(),
            })
        })
    }

    /// The gate of a call about to start (Block 46, D3): a debounced
    /// publish with no open window opens one and goes on, one with an open
    /// window waits there, its data the window's latest. Whether it waits.
    pub(super) fn debounce_gate(&mut self, id: CallId) -> bool {
        let Some((subscription, key)) = self.debounce_key(id) else {
            return false;
        };
        let Some(windows) = self.subscription_windows(subscription) else {
            return false;
        };
        let interval = windows.interval;
        let Some(timer) = windows.by_key.get(&key).copied() else {
            self.open_window(subscription, key, interval);
            return false;
        };
        let request = self.park_call(id, timer);
        if let Some(window) = self.debouncing.windows.get_mut(&timer) {
            window.latest = request;
            window.waiters.push(id);
        }
        true
    }

    /// Opens a window for `key`, with its timer.
    fn open_window(&mut self, subscription: SubscriptionId, key: String, interval: Duration) {
        let timer = DebounceTimer(self.debouncing.next_timer);
        self.debouncing.next_timer += 1;
        if let Some(windows) = self.subscription_windows(subscription) {
            windows.by_key.insert(key.clone(), timer);
        }
        self.debouncing.windows.insert(
            timer,
            Window {
                subscription,
                key,
                latest: None,
                waiters: Vec::new(),
            },
        );
        self.actions.push(Action::SetDebounceTimer(timer, interval));
    }

    /// A window's timer fired (Block 46, D3): the latest value goes out as
    /// a trailing send and a new window opens, or, with none, the window
    /// closes. A timer whose window is gone is ignored.
    pub(super) fn debounce_timer_fired(&mut self, timer: DebounceTimer) {
        let Some(window) = self.debouncing.windows.remove(&timer) else {
            return;
        };
        let interval = self
            .subscription_windows(window.subscription)
            .map(|windows| {
                windows.by_key.remove(&window.key);
                windows.interval
            });
        match (window.latest, interval) {
            (Some(request), Some(interval)) => {
                self.open_window(window.subscription, window.key, interval);
                self.start_trailing_send(window.subscription, request, window.waiters);
            }
            _ => self.close_windows_of(window.subscription),
        }
    }

    /// Every window of `subscription` ends, as it left `subscribed` (Block
    /// 46, D10): their timers are cancelled, the values waiting in them
    /// never sent, and the publishes waiting fail with `error`.
    pub(super) fn drop_debounce_windows(
        &mut self,
        subscription: SubscriptionId,
        error: &CallError,
    ) {
        let Some(windows) = self.subscription_windows(subscription) else {
            return;
        };
        let mut timers: Vec<DebounceTimer> =
            windows.by_key.drain().map(|(_, timer)| timer).collect();
        timers.sort_unstable_by_key(|timer| timer.0);
        for timer in timers {
            self.actions.push(Action::CancelDebounceTimer(timer));
            let Some(window) = self.debouncing.windows.remove(&timer) else {
                continue;
            };
            for waiter in window.waiters {
                self.settle_call(waiter, Err(error.clone()));
            }
        }
        self.close_windows_of(subscription);
    }

    /// Frees what closed windows held, once none is open: `subscription`'s
    /// table, a subscription usually has none open for long, and the
    /// machine's (Block 46, D15, "A closed window frees its memory").
    fn close_windows_of(&mut self, subscription: SubscriptionId) {
        if let Some(windows) = self.subscription_windows(subscription)
            && windows.by_key.is_empty()
        {
            windows.by_key = HashMap::new();
        }
        if self.debouncing.windows.is_empty() {
            self.debouncing.windows = HashMap::new();
        }
    }

    /// Waiter `id` of the window of `timer` was abandoned (Block 46, D11):
    /// with none left, the window goes on with nothing to send.
    pub(super) fn debounce_waiter_abandoned(&mut self, timer: DebounceTimer, id: CallId) {
        let Some(window) = self.debouncing.windows.get_mut(&timer) else {
            return;
        };
        window.waiters.retain(|waiter| *waiter != id);
        if window.waiters.is_empty() {
            window.latest = None;
        }
    }
}

#[cfg(test)]
impl Machine {
    /// How many windows are open.
    pub(super) fn debounce_window_count(&self) -> usize {
        self.debouncing.windows.len()
    }

    /// How many windows `subscription` has open.
    pub(super) fn subscription_debounce_windows(&mut self, subscription: SubscriptionId) -> usize {
        self.subscription_windows(subscription)
            .map_or(0, |windows| windows.by_key.len())
    }
}
