//! Signatures the client asks the `get_signature` callback for, and
//! refreshes (Block 47, D6, D7): requests in chunks within the frame
//! budget, revocations, the refresh at the earliest deadline, and the
//! retries with their backoffs. centrifuge-js 63ed73e `track`,
//! `_sharedPollRefreshSignature` and `_sharedPollReplayTrack`
//! (`src/subscription.ts:1973-2245`, `:2934-3047`), except where Block 47
//! decides otherwise.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use super::{
    BATCH_COST, BATCH_TOO_LARGE, Purpose, SharedPollFlow, Signed, distinct, escaped_twice,
};
use crate::backoff;
use crate::machine::{Action, FetchId, Machine, SharedPollTimer, SubscriptionId};
use crate::token::Secret;
use crate::wire::{KeyedItem, TrackBatch};
use crate::{ErrorKind, SharedPollSignature, SharedPollUpdate, TokenError, codes};

/// The message of error 14 when no callback can sign keys (D6).
const CALLBACK_MISSING: &str = "get_signature callback required for track(keys)";

/// The length a signature not known yet is counted at when sizing a chunk
/// (D6).
const UNKNOWN_SIGNATURE_LENGTH: usize = 128;

/// The worst-case cost of a signature not known yet (D6, D7).
pub(super) fn unknown_signature_cost() -> usize {
    escaped_twice(&"0".repeat(UNKNOWN_SIGNATURE_LENGTH))
}

/// The bounds of the retries of a refresh and of signing the keys without
/// a signature (D7, `src/subscription.ts:2091`, `:2240-2245`).
const RETRY_MIN: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// The bounds of the replay after a track request failed with a temporary
/// error, one below 100 or no reply in time (D7, `src/subscription.ts:1926-1929`).
const REPLAY_MIN: Duration = Duration::from_secs(1);
const REPLAY_MAX: Duration = Duration::from_secs(15);

/// The bounds of the refresh after replies with `expires` and no `ttl`:
/// a signature accepted within the server's grace period (D7).
const GRACE_MIN: Duration = Duration::from_secs(1);
const GRACE_MAX: Duration = Duration::from_secs(15);

/// Why the callback is asked to sign keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::machine) enum Origin {
    /// Keys given to `track(keys)` while subscribed.
    Track,
    /// Keys tracked without a signature: after a subscribe, or retried
    /// after a failure (D8, phase 2).
    Replay,
    /// Every key tracked, as a signature expires or was refused with 109
    /// (D7).
    Refresh,
}

/// A request to the callback, asked for in chunks, one at a time (D6).
#[derive(Debug)]
pub(in crate::machine) struct Signing {
    origin: Origin,
    /// Every key asked for, in order.
    keys: Vec<String>,
    /// The chunks not asked for yet.
    pending: VecDeque<Vec<String>>,
    /// The chunk asked for now.
    asking: FetchId,
    /// What the chunks answered so far returned.
    signed: Vec<Signed>,
}

/// What runs again after a failure (D7): a retry timer of a
/// subscription's `subscribed` period, one of each at most; the refresh at
/// an expiry is the owner's timer of the subscription
/// ([`Action::SetSharedPollRefresh`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::machine) enum Retry {
    /// The refresh of every signature failed.
    Refresh,
    /// Signing the keys tracked without a signature failed.
    Sign,
    /// A track request failed for a while: the replay of the signatures
    /// held runs again.
    Replay,
}

impl SharedPollFlow {
    /// `keys` in chunks, in order, each a batch within the frame budget by
    /// D7's worst-case cost whatever versions it later holds, its signature
    /// counted as the longest received, at least [`UNKNOWN_SIGNATURE_LENGTH`]
    /// bytes (D6); a key too large for any goes alone.
    fn chunks(&self, channel: &str, keys: Vec<String>) -> VecDeque<Vec<String>> {
        let budget = self.max_track_frame_bytes;
        let base = Self::track_cost(channel, []) + BATCH_COST + self.signature_cost;
        let mut chunks = VecDeque::new();
        let mut chunk = Vec::new();
        let mut cost = base;
        for key in keys {
            let key_cost = escaped_twice(&key) + super::ITEM_COST;
            if !chunk.is_empty() && cost + key_cost > budget {
                chunks.push_back(std::mem::take(&mut chunk));
                cost = base;
            }
            cost += key_cost;
            chunk.push(key);
        }
        if !chunk.is_empty() {
            chunks.push_back(chunk);
        }
        chunks
    }

    /// The keys tracked that no signature held covers, nor a request to the
    /// callback in flight.
    fn unsigned(&self) -> Vec<String> {
        let covered: HashSet<&String> = self
            .signing
            .iter()
            .flat_map(|signing| &signing.keys)
            .chain(self.library.iter().flat_map(|signed| &signed.keys))
            .collect();
        self.keys()
            .into_iter()
            .filter(|key| !covered.contains(key))
            .collect()
    }

    /// Removes the timers of `kinds`, giving them to cancel.
    fn take_timers(&mut self, kinds: impl Fn(Retry) -> bool) -> Vec<SharedPollTimer> {
        let mut taken = Vec::new();
        self.timers.retain(|(timer, kind)| {
            let take = kinds(*kind);
            if take {
                taken.push(*timer);
            }
            !take
        });
        taken
    }
}

impl Machine {
    /// Error 14: keys need a signature, and no callback can sign them (D6).
    pub(super) fn signature_callback_missing(&mut self, id: SubscriptionId) {
        self.emit_shared_poll_error(
            id,
            ErrorKind::Track,
            codes::error::SHARED_POLL_GET_SIGNATURE,
            CALLBACK_MISSING.to_owned(),
        );
    }

    /// Asks the callback to sign `keys`, in chunks when one batch of them
    /// would pass the frame budget (D6).
    pub(super) fn request_signature(
        &mut self,
        id: SubscriptionId,
        keys: Vec<String>,
        origin: Origin,
    ) {
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_deref() else {
            return;
        };
        if !flow.signature_source {
            return self.signature_callback_missing(id);
        }
        let mut pending = flow.chunks(&sub.channel, keys.clone());
        let Some(first) = pending.pop_front() else {
            return;
        };
        let asking = self.subscriptions.new_fetch();
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        flow.signing.push(Signing {
            origin,
            keys,
            pending,
            asking,
            signed: Vec::new(),
        });
        self.actions.push(Action::FetchSignature {
            subscription: id,
            request: asking,
            keys: first,
        });
    }

    /// The callback answered `request` (D6). An answer to a request of an
    /// earlier `subscribed` period, or while not subscribed, changes
    /// nothing (I13).
    pub(super) fn signature_fetched(
        &mut self,
        id: SubscriptionId,
        request: FetchId,
        result: Result<SharedPollSignature, TokenError>,
    ) {
        let subscribed = self.subscribed_now(id);
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_deref_mut() else {
            return;
        };
        let Some(index) = flow
            .signing
            .iter()
            .position(|signing| signing.asking == request)
        else {
            return;
        };
        if !subscribed {
            flow.signing.remove(index);
            return;
        }
        let signature = match result {
            // centrifuge-js `_failUnauthorized`: the user may not use the
            // subscription any more.
            Err(TokenError::Unauthorized) => {
                flow.signing.remove(index);
                return self.set_unsubscribed(
                    id,
                    codes::unsubscribed::UNAUTHORIZED,
                    "unauthorized",
                    true,
                );
            }
            Err(error) => {
                let origin = flow.signing.remove(index).origin;
                return self.signing_failed(
                    id,
                    origin,
                    codes::error::SHARED_POLL_GET_SIGNATURE,
                    error.to_string(),
                );
            }
            Ok(signature) => signature,
        };
        // Later chunks are sized for it (D6).
        flow.signature_cost = flow.signature_cost.max(escaped_twice(&signature.signature));
        let signed = Signed {
            keys: signature.keys,
            signature: Secret(signature.signature),
        };
        // More keys than asked for can pass the budget, and a single batch
        // cannot be split: refused before the library or the wire, and
        // retried as a failed fetch (D6).
        let batch = TrackBatch {
            signature: signed.signature.clone(),
            items: signed
                .keys
                .iter()
                .map(|key| KeyedItem {
                    key: key.clone(),
                    version: 0,
                })
                .collect(),
        };
        if SharedPollFlow::track_cost(&sub.channel, [&batch]) > flow.max_track_frame_bytes {
            let origin = flow.signing.remove(index).origin;
            return self.signing_failed(
                id,
                origin,
                codes::error::BAD_CONFIGURATION,
                BATCH_TOO_LARGE.to_owned(),
            );
        }
        let signing = &mut flow.signing[index];
        signing.signed.push(signed);
        if let Some(next) = signing.pending.pop_front() {
            let asking = self.subscriptions.new_fetch();
            if let Some(flow) = self.shared_poll_flow(id) {
                flow.signing[index].asking = asking;
            }
            return self.actions.push(Action::FetchSignature {
                subscription: id,
                request: asking,
                keys: next,
            });
        }
        let signing = flow.signing.remove(index);
        self.signing_done(id, signing);
    }

    /// A request to the callback failed, or returned a signature too large
    /// (D6): an `Error`, then a retry after the backoff (D7).
    fn signing_failed(&mut self, id: SubscriptionId, origin: Origin, code: u32, message: String) {
        let kind = if origin == Origin::Track {
            ErrorKind::Track
        } else {
            ErrorKind::SignatureRefresh
        };
        self.emit_shared_poll_error(id, kind, code, message);
        if origin == Origin::Refresh {
            if let Some(flow) = self.shared_poll_flow(id) {
                // The retry covers every key tracked then.
                flow.refresh_again = false;
            }
            self.refresh_failed(id);
        } else {
            self.schedule_retry(id, Retry::Sign);
        }
    }

    /// Every chunk of `signing` answered (D6): the keys asked for and not
    /// returned are revoked; those returned are tracked under their
    /// signatures, a key untracked meanwhile in the request's `untrack`;
    /// for a refresh the signatures replace those they cover (D7).
    fn signing_done(&mut self, id: SubscriptionId, signing: Signing) {
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let Signing {
            origin,
            keys,
            signed,
            ..
        } = signing;
        if origin != Origin::Refresh {
            flow.sign_attempts = 0;
        }
        let returned: HashSet<&String> = signed.iter().flat_map(|entry| &entry.keys).collect();
        let revoked: Vec<String> = distinct(keys)
            .into_iter()
            .filter(|key| !returned.contains(key))
            .filter(|key| flow.untrack(key))
            .collect();
        let covered: HashSet<String> = returned.into_iter().cloned().collect();
        let mut batches = Vec::new();
        let mut untracked = HashSet::new();
        let mut entries = Vec::new();
        for entry in signed {
            if !entry.keys.iter().any(|key| flow.tracked.contains_key(key)) {
                continue;
            }
            batches.push(flow.batch(&entry));
            untracked.extend(
                entry
                    .keys
                    .iter()
                    .filter(|key| !flow.tracked.contains_key(*key))
                    .cloned(),
            );
            entries.push(entry);
        }
        if origin == Origin::Refresh {
            // Kept: the signatures of keys tracked that the refresh did not
            // cover, as one tracked while it ran.
            let held = &flow.tracked;
            flow.library.retain(|entry| {
                entry
                    .keys
                    .iter()
                    .any(|key| held.contains_key(key) && !covered.contains(key))
            });
            entries.append(&mut flow.library);
            flow.library = entries;
        } else {
            for entry in entries {
                flow.add_signature(entry.keys, entry.signature.0);
            }
        }
        let again = origin == Origin::Refresh && std::mem::take(&mut flow.refresh_again);
        if origin == Origin::Refresh && batches.is_empty() {
            flow.refresh_attempts = 0;
        }
        for key in &revoked {
            self.emit_shared_poll_update(id, SharedPollUpdate::removal(key.clone()), None, false);
        }
        if !revoked.is_empty() {
            self.send_untrack(id, revoked);
        }
        if !batches.is_empty() {
            let purpose = if origin == Origin::Refresh {
                Purpose::Refresh
            } else {
                Purpose::Track
            };
            self.send_track(id, batches, &untracked, purpose);
        }
        if again {
            self.start_refresh(id);
        }
    }

    /// Signs the keys tracked without a signature held (D8, phase 2; the
    /// retry of a failure).
    pub(super) fn sign_unsigned(&mut self, id: SubscriptionId) {
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let unsigned = flow.unsigned();
        if !unsigned.is_empty() {
            self.request_signature(id, unsigned, Origin::Replay);
        }
    }

    /// Refreshes the signatures of every key tracked, one refresh at a
    /// time (D7): a key the refresh in flight does not cover runs another
    /// once it ends. The timers pointing at now end: the refresh's reply
    /// sets the next deadline.
    pub(super) fn start_refresh(&mut self, id: SubscriptionId) {
        if !self.subscribed_now(id) {
            return;
        }
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let mut ended: Vec<Action> = flow
            .take_timers(|kind| kind == Retry::Refresh)
            .into_iter()
            .map(Action::CancelSharedPollTimer)
            .collect();
        if std::mem::take(&mut flow.refresh_armed) {
            ended.push(Action::CancelSharedPollRefresh(id));
        }
        let keys = flow.keys();
        let running = flow
            .signing
            .iter()
            .find(|signing| signing.origin == Origin::Refresh);
        let start = match running {
            Some(running) => {
                let asked: HashSet<&String> = running.keys.iter().collect();
                if keys.iter().any(|key| !asked.contains(key)) {
                    flow.refresh_again = true;
                }
                false
            }
            None => flow.signature_source && !keys.is_empty(),
        };
        self.actions.extend(ended);
        if start {
            self.request_signature(id, keys, Origin::Refresh);
        }
    }

    /// The refresh failed: it runs again after the backoff (D7).
    pub(super) fn refresh_failed(&mut self, id: SubscriptionId) {
        self.schedule_retry(id, Retry::Refresh);
    }

    /// A track request failed with a temporary error, one below 100 or no
    /// reply in time: the replay runs again after the backoff (D7).
    pub(super) fn replay_failed(&mut self, id: SubscriptionId) {
        self.schedule_retry(id, Retry::Replay);
    }

    /// A track reply with `expires`: the refresh runs at the earliest
    /// deadline seen (D7), `ttl` seconds from now, or, for a signature
    /// accepted within the server's grace period (`ttl` 0), 1 s from now,
    /// backing off between 1 s and 15 s while such replies come in a row.
    /// Without a callback, refreshing is the application's (D6).
    pub(super) fn refresh_deadline(&mut self, id: SubscriptionId, ttl: u32) {
        let Some(flow) = self
            .subscriptions
            .by_id
            .get_mut(&id)
            .and_then(|sub| sub.shared_poll.as_deref_mut())
        else {
            return;
        };
        if !flow.signature_source {
            return;
        }
        let delay = if ttl > 0 {
            flow.grace_attempts = 0;
            Duration::from_secs(u64::from(ttl))
        } else {
            let step = flow.grace_attempts;
            flow.grace_attempts = step.saturating_add(1);
            if step == 0 {
                GRACE_MIN
            } else {
                backoff::delay(step, GRACE_MIN, GRACE_MAX, &mut self.rng)
            }
        };
        // The owner keeps the earliest deadline, one timer per
        // subscription, as it has the clock.
        flow.refresh_armed = true;
        self.actions.push(Action::SetSharedPollRefresh(id, delay));
    }

    /// The refresh timer of `id` fired (D7); one not armed, cancelled or of
    /// a subscription no longer subscribed does nothing.
    pub(super) fn refresh_fired(&mut self, id: SubscriptionId) {
        let armed = self
            .shared_poll_flow(id)
            .is_some_and(|flow| std::mem::take(&mut flow.refresh_armed));
        if armed && self.subscribed_now(id) {
            self.start_refresh(id);
        }
    }

    /// Schedules `retry` after Block 2's backoff, between 5 s and 30 s, or
    /// 1 s and 15 s for the replay, its step counting the failures in a row
    /// (D7), replacing one scheduled.
    fn schedule_retry(&mut self, id: SubscriptionId, retry: Retry) {
        let Some(flow) = self
            .subscriptions
            .by_id
            .get_mut(&id)
            .and_then(|sub| sub.shared_poll.as_deref_mut())
        else {
            return;
        };
        let (attempts, (min, max)) = match retry {
            Retry::Refresh => (&mut flow.refresh_attempts, (RETRY_MIN, RETRY_MAX)),
            Retry::Sign => (&mut flow.sign_attempts, (RETRY_MIN, RETRY_MAX)),
            Retry::Replay => (&mut flow.replay_attempts, (REPLAY_MIN, REPLAY_MAX)),
        };
        let delay = backoff::delay(*attempts, min, max, &mut self.rng);
        *attempts = attempts.saturating_add(1);
        let replaced = flow.take_timers(|scheduled| scheduled == retry);
        let timer = self.new_shared_poll_timer();
        if let Some(flow) = self.shared_poll_flow(id) {
            flow.timers.push((timer, retry));
        }
        for old in replaced {
            self.actions.push(Action::CancelSharedPollTimer(old));
        }
        self.actions.push(Action::SetSharedPollTimer(timer, delay));
    }

    /// A refresh or retry timer of a subscription fired (D7); one of a
    /// subscription not subscribed now does nothing.
    pub(super) fn flow_timer_fired(&mut self, timer: SharedPollTimer) {
        let found = self.subscriptions.by_id.iter_mut().find_map(|(id, sub)| {
            let flow = sub.shared_poll.as_deref_mut()?;
            let index = flow.timers.iter().position(|(set, _)| *set == timer)?;
            Some((*id, flow.timers.remove(index).1))
        });
        let Some((id, kind)) = found else {
            return;
        };
        if !self.subscribed_now(id) {
            return;
        }
        match kind {
            Retry::Refresh => self.start_refresh(id),
            Retry::Sign => self.sign_unsigned(id),
            Retry::Replay => self.shared_poll_replay(id),
        }
    }
}
