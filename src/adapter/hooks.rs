//! What the soak's hooks read (Block 52, D6, D8): the timers the core set
//! and did not cancel, the timers the driver runs, and the calls every
//! registry holds, each with its age. Kept only once a client asks for
//! them ([`super::Client::record_timers`]), so that no other test pays for
//! them: the allocation gates of Block 14 run with this feature too.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::platform;
use crate::machine::{
    Action, CallId, CallTimer, DebounceTimer, Input, SubscriptionId, SubscriptionTimer, Timer,
};

/// A timer as [`super::Client::armed_timers`] reports it.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedTimer {
    /// The timer: `client ServerPing`, `subscription SubscriptionId(3)
    /// Refresh`, `call CallId(7) Reply`, `debounce DebounceTimer(1)`, `shared
    /// poll …`.
    pub name: String,
    /// Since it was set; in the driver's list, since the driver was first
    /// seen running it with this deadline.
    pub age: Duration,
    /// Until its deadline; zero once past.
    pub due_in: Duration,
    /// How far past its deadline; zero before it.
    pub overdue: Duration,
}

/// The two lists of [`super::Client::armed_timers`] (Block 52, D8).
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArmedTimers {
    /// The timers in the driver's own maps, as it last published them, at
    /// the top of its loop.
    pub driver: Vec<ArmedTimer>,
    /// The timers the core set and did not cancel, nor saw fire.
    pub core: Vec<ArmedTimer>,
}

/// A call as [`super::Client::pending_calls`] reports it.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRecord {
    /// The call, `CallId(7)`.
    pub id: String,
    /// Since it was made; for a call the core made itself, since it was
    /// first seen.
    pub age: Duration,
    /// The registries holding it: `adapter` (the outcome's sender), and the
    /// core's `by_id`, `by_command` and `room`.
    pub registries: Vec<&'static str>,
}

/// The records of a client, off until [`Records::start`].
#[derive(Debug, Default)]
pub(super) struct Records {
    on: AtomicBool,
    /// The driver's timers, by name: deadline and since when.
    pub(super) driver: Mutex<TimerRecord>,
    /// When each call was made, pruned only when read.
    pub(super) calls: Mutex<CallAges>,
}

impl Records {
    pub(super) fn start(&self) {
        self.on.store(true, Ordering::SeqCst);
    }

    pub(super) fn on(&self) -> bool {
        self.on.load(Ordering::Relaxed)
    }
}

pub(super) fn client(timer: Timer) -> String {
    format!("client {timer:?}")
}

pub(super) fn subscription(id: SubscriptionId, timer: SubscriptionTimer) -> String {
    format!("subscription {id:?} {timer:?}")
}

pub(super) fn call(id: CallId, timer: CallTimer) -> String {
    format!("call {id:?} {timer:?}")
}

pub(super) fn debounce(timer: DebounceTimer) -> String {
    format!("debounce {timer:?}")
}

#[cfg(feature = "unstable")]
pub(super) fn shared_poll(timer: crate::machine::SharedPollTimer) -> String {
    format!("shared poll {timer:?}")
}

#[cfg(feature = "unstable")]
pub(super) fn shared_poll_refresh(id: SubscriptionId) -> String {
    format!("shared poll refresh {id:?}")
}

/// Timers by name, each with its deadline and since when it runs.
#[derive(Debug, Default)]
pub(super) struct TimerRecord(BTreeMap<String, (platform::Instant, platform::Instant)>);

impl TimerRecord {
    /// Follows a timer action of the core.
    pub(super) fn action(&mut self, action: &Action) {
        let now = platform::Instant::now();
        match action {
            Action::SetTimer(timer, delay) => self.set(client(*timer), now, *delay),
            Action::CancelTimer(timer) => self.cancel(&client(*timer)),
            Action::ShortenTimer(timer, by) => self.shorten(&client(*timer), *by, now),
            Action::SetSubscriptionTimer(id, timer, delay) => {
                self.set(subscription(*id, *timer), now, *delay);
            }
            Action::CancelSubscriptionTimer(id, timer) => self.cancel(&subscription(*id, *timer)),
            Action::ShortenSubscriptionTimer(id, timer, by) => {
                self.shorten(&subscription(*id, *timer), *by, now);
            }
            Action::SetCallTimer(id, timer, delay) => self.set(call(*id, *timer), now, *delay),
            Action::CancelCallTimer(id, timer) => self.cancel(&call(*id, *timer)),
            Action::SetDebounceTimer(timer, delay) => self.set(debounce(*timer), now, *delay),
            Action::CancelDebounceTimer(timer) => self.cancel(&debounce(*timer)),
            #[cfg(feature = "unstable")]
            Action::SetSharedPollTimer(timer, delay) => {
                self.set(shared_poll(*timer), now, *delay);
            }
            #[cfg(feature = "unstable")]
            Action::CancelSharedPollTimer(timer) => self.cancel(&shared_poll(*timer)),
            // The driver keeps one at the earliest deadline asked for.
            #[cfg(feature = "unstable")]
            Action::SetSharedPollRefresh(id, delay) => {
                let deadline = now + *delay;
                match self.0.get_mut(&shared_poll_refresh(*id)) {
                    Some((running, _)) if *running <= deadline => {}
                    Some((running, _)) => *running = deadline,
                    None => {
                        self.0.insert(shared_poll_refresh(*id), (deadline, now));
                    }
                }
            }
            #[cfg(feature = "unstable")]
            Action::CancelSharedPollRefresh(id) => self.cancel(&shared_poll_refresh(*id)),
            _ => {}
        }
    }

    /// Follows an input the core takes: a firing ends its timer.
    pub(super) fn input(&mut self, input: &Input) {
        let name = match input {
            Input::TimerFired(timer) => client(*timer),
            Input::SubscriptionTimerFired(id, timer) => subscription(*id, *timer),
            Input::CallTimerFired(id, timer) => call(*id, *timer),
            Input::DebounceTimerFired(timer) => debounce(*timer),
            #[cfg(feature = "unstable")]
            Input::SharedPollTimerFired(timer) => shared_poll(*timer),
            #[cfg(feature = "unstable")]
            Input::SharedPollRefreshFired(id) => shared_poll_refresh(*id),
            _ => return,
        };
        self.0.remove(&name);
    }

    fn set(&mut self, name: String, now: platform::Instant, delay: Duration) {
        self.0.insert(name, (now + delay, now));
    }

    fn cancel(&mut self, name: &str) {
        self.0.remove(name);
    }

    fn shorten(&mut self, name: &str, by: Duration, now: platform::Instant) {
        if let Some((deadline, _)) = self.0.get_mut(name) {
            *deadline = deadline.checked_sub(by).unwrap_or(now);
        }
    }

    /// The driver's timers now, by name and deadline: one already seen with
    /// the same deadline keeps its age, any other starts now.
    pub(super) fn observe(&mut self, running: Vec<(String, platform::Instant)>) {
        let now = platform::Instant::now();
        let mut next = BTreeMap::new();
        for (name, deadline) in running {
            let since = match self.0.get(&name) {
                Some((seen, since)) if *seen == deadline => *since,
                _ => now,
            };
            next.insert(name, (deadline, since));
        }
        self.0 = next;
    }

    pub(super) fn report(&self) -> Vec<ArmedTimer> {
        let now = platform::Instant::now();
        self.0
            .iter()
            .map(|(name, (deadline, since))| ArmedTimer {
                name: name.clone(),
                age: now.saturating_duration_since(*since),
                due_in: deadline.saturating_duration_since(now),
                overdue: now.saturating_duration_since(*deadline),
            })
            .collect()
    }
}

/// When each call was made, or first seen.
#[derive(Debug, Default)]
pub(super) struct CallAges(HashMap<CallId, platform::Instant>);

impl CallAges {
    pub(super) fn made(&mut self, id: CallId) {
        self.0.entry(id).or_insert_with(platform::Instant::now);
    }

    /// The calls `held` by the registries, with their ages. Only here are
    /// the calls no registry holds forgotten, so that a leak cannot hide.
    pub(super) fn report(&mut self, held: BTreeMap<CallId, Vec<&'static str>>) -> Vec<CallRecord> {
        let now = platform::Instant::now();
        self.0.retain(|id, _| held.contains_key(id));
        held.into_iter()
            .map(|(id, registries)| {
                let since = *self.0.entry(id).or_insert(now);
                CallRecord {
                    id: format!("{id:?}"),
                    age: now.saturating_duration_since(since),
                    registries,
                }
            })
            .collect()
    }
}
