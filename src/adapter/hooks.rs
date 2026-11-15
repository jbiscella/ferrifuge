//! What the soak's hooks read: the timers the core set and did not cancel,
//! the timers the driver runs, and the calls every registry holds, each
//! with its age. Kept only once a client asks for them
//! ([`super::Client::record_timers`]), so that no other test pays for them:
//! the allocation gates run with this feature too.
//!
//! A `__test-hooks` module: outside semver, so its types need not be
//! `#[non_exhaustive]`.
// Block 52, D6, D8; Block 14; Block 54, D2.
#![allow(clippy::exhaustive_enums, reason = "outside semver")]
#![allow(clippy::exhaustive_structs, reason = "outside semver")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::platform;
use crate::sans_io::{
    Action, CallId, CallTimer, DebounceTimer, Input, SubscriptionId, SubscriptionTimer, Timer,
    TimerKey, TimerSetting,
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

/// The two lists of [`super::Client::armed_timers`].
// Block 52, D8.
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
    /// The registries holding it: `adapter` (the outcome's sender), `log`
    /// (the log's kinds of calls), `watched` (the calls a `Thread` handler
    /// waits on), and the core's `by_id`, `by_command` and `room`.
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

/// The name of one of the core's timers, by its key.
pub(super) fn name(key: TimerKey) -> String {
    match key {
        TimerKey::Client(timer) => client(timer),
        TimerKey::Subscription(id, timer) => subscription(id, timer),
        TimerKey::Call(id, timer) => call(id, timer),
        TimerKey::Debounce(timer) => debounce(timer),
        #[cfg(feature = "unstable")]
        TimerKey::SharedPoll(timer) => shared_poll(timer),
        #[cfg(feature = "unstable")]
        TimerKey::SharedPollRefresh(id) => shared_poll_refresh(id),
    }
}

pub(super) fn debounce(timer: DebounceTimer) -> String {
    format!("debounce {timer:?}")
}

#[cfg(feature = "unstable")]
pub(super) fn shared_poll(timer: crate::sans_io::SharedPollTimer) -> String {
    format!("shared poll {timer:?}")
}

#[cfg(feature = "unstable")]
pub(super) fn shared_poll_refresh(id: SubscriptionId) -> String {
    format!("shared poll refresh {id:?}")
}

/// Timers by name, each with its deadline, when known, and since when it
/// runs, and the setting of each client, subscription and call timer. For
/// those it gives only the ages and deadlines: their names come from the
/// core's running settings; it alone tracks the debounce and shared-poll
/// timers.
// Block 53 D4.
#[derive(Debug, Default)]
pub(super) struct TimerRecord(
    BTreeMap<String, (Option<platform::Instant>, platform::Instant)>,
    BTreeMap<String, TimerSetting>,
);

impl TimerRecord {
    /// Follows a timer action of the core.
    pub(super) fn action(&mut self, action: &Action) {
        let now = platform::Instant::now();
        match action {
            // The driver keeps one at the earliest deadline asked for, with
            // the latest setting (Block 61, D4).
            #[cfg(feature = "unstable")]
            Action::SetTimer(key @ TimerKey::SharedPollRefresh(_), delay, setting) => {
                let name = name(*key);
                let deadline = now + *delay;
                self.1.insert(name.clone(), *setting);
                match self.0.get_mut(&name) {
                    Some((Some(running), _)) if *running <= deadline => {}
                    Some((running, _)) => *running = Some(deadline),
                    None => {
                        self.0.insert(name, (Some(deadline), now));
                    }
                }
            }
            Action::SetTimer(key, delay, setting) => {
                self.set_numbered(name(*key), now, *delay, *setting);
            }
            Action::CancelTimer(key) => self.cancel(&name(*key)),
            Action::ShortenTimer(key, by) => self.shorten(&name(*key), *by, now),
            _ => {}
        }
    }

    // Block 53 D4; Block 61, D4.
    pub(super) fn input(&mut self, input: &Input) {
        if let Input::TimerFired(key, setting) = input {
            let name = name(*key);
            if self.1.get(&name) == Some(setting) {
                self.0.remove(&name);
                self.1.remove(&name);
            }
        }
    }

    fn set(&mut self, name: String, now: platform::Instant, delay: Duration) {
        self.0.insert(name, (Some(now + delay), now));
    }

    fn set_numbered(
        &mut self,
        name: String,
        now: platform::Instant,
        delay: Duration,
        setting: TimerSetting,
    ) {
        self.1.insert(name.clone(), setting);
        self.set(name, now, delay);
    }

    fn cancel(&mut self, name: &str) {
        self.0.remove(name);
        self.1.remove(name);
    }

    fn shorten(&mut self, name: &str, by: Duration, now: platform::Instant) {
        if let Some((Some(deadline), _)) = self.0.get_mut(name) {
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
                Some((Some(seen), since)) if *seen == deadline => *since,
                _ => now,
            };
            next.insert(name, (Some(deadline), since));
        }
        self.0 = next;
    }

    /// The record of `name`, to be [`TimerRecord::put`] back.
    pub(super) fn get(&self, name: &str) -> Option<(Option<platform::Instant>, platform::Instant)> {
        self.0.get(name).copied()
    }

    /// Puts back what [`TimerRecord::get`] gave.
    pub(super) fn put(
        &mut self,
        name: String,
        entry: Option<(Option<platform::Instant>, platform::Instant)>,
    ) {
        match entry {
            Some(entry) => {
                self.0.insert(name, entry);
            }
            None => {
                self.0.remove(&name);
            }
        }
    }

    /// Ages from now the timers `running` names that are not recorded yet:
    /// those set before the records started.
    // Reviewer, 52a.
    pub(super) fn seed(&mut self, running: impl IntoIterator<Item = (String, TimerSetting)>) {
        let now = platform::Instant::now();
        for (name, setting) in running {
            self.1.insert(name.clone(), setting);
            self.0.entry(name).or_insert((None, now));
        }
    }

    /// The driver's list.
    pub(super) fn report(&self) -> Vec<ArmedTimer> {
        let now = platform::Instant::now();
        self.0
            .iter()
            .map(|(name, entry)| timer(name, *entry, now))
            .collect()
    }

    /// The core's list: the client, subscription and call timers `armed`
    /// names, whose settings the core keeps running, and the debounce and
    /// shared-poll timers, which only this record keeps; each aged from
    /// this record, or from now when first seen here.
    // Block 53, D2.
    pub(super) fn report_armed(
        &mut self,
        armed: impl IntoIterator<Item = String>,
    ) -> Vec<ArmedTimer> {
        let now = platform::Instant::now();
        let names: std::collections::BTreeSet<String> = armed.into_iter().collect();
        names
            .into_iter()
            .map(|name| {
                let entry = *self.0.entry(name.clone()).or_insert((None, now));
                timer(&name, entry, now)
            })
            .collect()
    }
}

/// A timer with its deadline, when known: one aged from when first seen
/// has none, and reports no time to or past it.
fn timer(
    name: &str,
    (deadline, since): (Option<platform::Instant>, platform::Instant),
    now: platform::Instant,
) -> ArmedTimer {
    ArmedTimer {
        name: name.to_owned(),
        age: now.saturating_duration_since(since),
        due_in: deadline.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(now)
        }),
        overdue: deadline.map_or(Duration::ZERO, |deadline| {
            now.saturating_duration_since(deadline)
        }),
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
