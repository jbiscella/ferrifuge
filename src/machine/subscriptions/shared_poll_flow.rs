//! Shared poll subscriptions (Block 47): a subscription on which the
//! application names keys, each tracked under a signature its backend
//! issued; the server polls the backend once per key for every client and
//! sends each new version of a key's value. Behaviour follows centrifuge-js
//! 63ed73e `SharedPollSubscription` and its track handlers
//! (`src/subscription.ts:1109-2245`, `:2915-3084`), except where Block 47
//! decides otherwise.

use std::collections::{HashMap, HashSet, VecDeque};

mod signing;

use super::super::delivery::Owner;
use super::super::{
    Action, Machine, SharedPollSubscriptionOptions, SharedPollTimer, SubscriptionId,
};
use crate::fossil::DeltaError;
use crate::shared_poll::SharedPollUpdate;
use crate::token::Secret;
use crate::wire::{
    Command, KeyedItem, Method, Reply, ReplyResult, SubRefreshRequest, SubscribeRequest, TRACK,
    TrackBatch, UNTRACK,
};
use crate::{
    ClientError, ErrorKind, Publication, SharedPollSubscriptionEvent, SharedPollTrackItem, State,
    Subscribed, SubscriptionError, SubscriptionEvent, SubscriptionState, codes,
};

/// `type` of a shared poll subscription's subscribe command and reply
/// (`client.proto`, `SubscribeRequest.type`).
const SHARED_POLL_TYPE: i32 = 4;

/// Reason of [`codes::unsubscribed::SHARED_POLL_NOT_SUPPORTED`].
const NOT_SUPPORTED_REASON: &str = "shared poll subscriptions not supported";

/// Error code by which the server refuses an expired signature
/// (`client.proto`; centrifuge `ErrorTokenExpired`).
const SIGNATURE_EXPIRED: u32 = 109;

/// The worst-case cost of a request, beyond its keys (Block 47, D7): the
/// command's fields, the emulation envelope and its session and node.
const FRAME_COST: usize = 256;
/// The worst-case cost of a key in an untrack list, beyond its doubly
/// escaped length: its quotes and separator (Block 47, D7).
const UNTRACK_KEY_COST: usize = 6;
/// The worst-case cost of a batch, beyond its signature and items (D7).
const BATCH_COST: usize = 40;
/// The worst-case cost of an item, beyond its key's doubly escaped length:
/// its fields and a version at `u64::MAX` (D7).
const ITEM_COST: usize = 48;

/// Reason of `connecting` 3 when a shared poll request's reply deadline
/// passes over an emulated transport (D12).
const REQUEST_TIMEOUT_REASON: &str = "shared poll request timeout";

/// The message of error 12 for a batch over `max_track_frame_bytes` (D6).
const BATCH_TOO_LARGE: &str = "track batch over max_track_frame_bytes";

/// What makes a shared poll subscription one: the keys it tracks, the
/// signatures it holds for them, and the requests in flight (Block 47, D5).
/// Boxed in the subscription, so a stream subscription pays a pointer (Block
/// 14, "Marginal cost of a subscription").
#[derive(Debug)]
pub(in crate::machine) struct SharedPollFlow {
    signature_source: bool,
    max_track_frame_bytes: usize,
    /// Each tracked key, with the version of its value the application took
    /// last (0 for none) and its place in the order keys were tracked.
    tracked: HashMap<String, Tracked>,
    next_place: u64,
    /// The signatures held: each covers exactly its keys, in order (D5).
    library: Vec<Signed>,
    /// Counts the times the subscription became `subscribed`: a request of
    /// an earlier count was abandoned, and its answer changes nothing (I13).
    generation: u64,
    /// Requests not sent yet: over an emulated transport each waits for
    /// the reply of the one before (D12), and a request's frames each wait
    /// for the reply of the frame before (D7).
    queue: VecDeque<Queued>,
    /// Numbers the requests, each of one or more frames (D7).
    next_group: u64,
    /// The signature requests of this `subscribed` period awaiting the
    /// callback (D6); those of an earlier one were abandoned.
    signing: Vec<signing::Signing>,
    /// A refresh was asked for while one ran that did not cover every key
    /// tracked: it runs again once that one ends (D7).
    refresh_again: bool,
    /// The refresh deadlines and the retries scheduled in this `subscribed`
    /// period (D7).
    timers: Vec<(SharedPollTimer, signing::Retry)>,
    /// Failures in a row of the refresh, of signing the keys without a
    /// signature, and replies with `expires` and no `ttl` in a row: the
    /// steps of their backoffs (D7).
    refresh_attempts: u32,
    sign_attempts: u32,
    grace_attempts: u32,
    /// The owner runs the subscription's refresh timer (D7).
    refresh_armed: bool,
    /// The worst-case cost of the longest signature received, at least
    /// that of one of 128 bytes: chunks are sized for it (D6).
    signature_cost: usize,
    /// The epoch of the last subscribe reply, an absent one as `""`; `None`
    /// until the first reply after `unsubscribed` (D5, D8).
    epoch: Option<String>,
    /// Track requests failed in a row with a temporary error, one below
    /// 100 or no reply in time: the step of the replay's backoff (D7).
    replay_attempts: u32,
}

/// What a track or untrack request is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::machine) enum Purpose {
    /// Tracks keys: an application's call, a signature fetched for it, or
    /// the replay of a subscribe.
    Track,
    /// Tracks the keys a signature refresh signed (D7).
    Refresh,
    /// Stops tracking keys.
    Untrack,
}

/// A track or untrack request waiting to be sent.
#[derive(Debug)]
struct Queued {
    generation: u64,
    request: SubRefreshRequest,
    purpose: Purpose,
    keys: Vec<String>,
    /// The request it is a frame of (D7).
    group: u64,
    /// Waits for the reply of the frame before it (D7).
    after_reply: bool,
}

#[derive(Debug, Clone, Copy)]
struct Tracked {
    version: u64,
    place: u64,
}

/// A signature the client holds, and the keys it covers.
#[derive(Debug, Clone)]
struct Signed {
    keys: Vec<String>,
    signature: Secret,
}

/// A track or untrack request awaiting its reply.
#[derive(Debug)]
pub(in crate::machine) struct TrackCommand {
    subscription: SubscriptionId,
    /// Its channel: a subscription recreated there after a removal waits
    /// for it too (D12).
    channel: String,
    generation: u64,
    purpose: Purpose,
    keys: Vec<String>,
    /// The keys of its `untrack` field.
    untracked: Vec<String>,
    /// The request it is a frame of (D7).
    group: u64,
    /// Its reply deadline, until its reply is read (D7, D12).
    timer: Option<SharedPollTimer>,
}

impl TrackCommand {
    /// Its reply deadline, if it has one.
    pub(in crate::machine) fn timer(&self) -> Option<SharedPollTimer> {
        self.timer
    }
}

impl SharedPollFlow {
    fn new(options: &SharedPollSubscriptionOptions) -> Self {
        Self {
            signature_source: options.signature_source,
            max_track_frame_bytes: options.max_track_frame_bytes.get(),
            tracked: HashMap::new(),
            next_place: 0,
            library: Vec::new(),
            generation: 0,
            queue: VecDeque::new(),
            next_group: 0,
            signing: Vec::new(),
            refresh_again: false,
            timers: Vec::new(),
            refresh_attempts: 0,
            sign_attempts: 0,
            grace_attempts: 0,
            refresh_armed: false,
            signature_cost: signing::unknown_signature_cost(),
            epoch: None,
            replay_attempts: 0,
        }
    }

    /// Tracks `key` at `version`, or keeps the higher of it and the version
    /// held (D6); true when it was not tracked.
    fn track(&mut self, key: &str, version: u64) -> bool {
        if let Some(tracked) = self.tracked.get_mut(key) {
            tracked.version = tracked.version.max(version);
            return false;
        }
        let place = self.next_place;
        self.next_place += 1;
        self.tracked
            .insert(key.to_owned(), Tracked { version, place });
        true
    }

    /// Stops tracking `key`; true when it was tracked. A signature none of
    /// whose keys is tracked is dropped (D6).
    fn untrack(&mut self, key: &str) -> bool {
        let tracked = self.tracked.remove(key).is_some();
        if tracked {
            let held = &self.tracked;
            self.library
                .retain(|signed| signed.keys.iter().any(|key| held.contains_key(key)));
        }
        tracked
    }

    /// Adds a signature over `keys`, dropping those it covers entirely
    /// (centrifuge-js `_addSharedPollSignature`).
    fn add_signature(&mut self, keys: Vec<String>, signature: String) {
        self.library.retain(|signed| {
            !signed
                .keys
                .iter()
                .all(|key| keys.iter().any(|new| new == key))
        });
        self.library.push(Signed {
            keys,
            signature: Secret(signature),
        });
    }

    /// The tracked keys, in the order they were tracked.
    pub(in crate::machine) fn keys(&self) -> Vec<String> {
        let mut keys: Vec<(&String, u64)> = self
            .tracked
            .iter()
            .map(|(key, tracked)| (key, tracked.place))
            .collect();
        keys.sort_unstable_by_key(|(_, place)| *place);
        keys.into_iter().map(|(key, _)| key.clone()).collect()
    }

    fn version(&self, key: &str) -> u64 {
        self.tracked.get(key).map_or(0, |tracked| tracked.version)
    }

    /// The batch of `signed` at the versions held, 0 for a key no longer
    /// tracked (D8).
    fn batch(&self, signed: &Signed) -> TrackBatch {
        TrackBatch {
            signature: signed.signature.clone(),
            items: signed
                .keys
                .iter()
                .map(|key| KeyedItem {
                    key: key.clone(),
                    version: self.version(key),
                })
                .collect(),
        }
    }

    /// `unsubscribe()` of subscription `id`: the keys, signatures and
    /// epoch go with it, so the next subscribe starts cold (D5,
    /// `set_unsubscribed`); what it asked for and scheduled ends. The
    /// actions cancelling its timers.
    #[must_use]
    pub(in crate::machine) fn forget(&mut self, id: SubscriptionId) -> Vec<Action> {
        self.tracked.clear();
        self.library.clear();
        self.queue.clear();
        self.epoch = None;
        self.end_period(id)
    }

    /// The epoch of a subscribe reply, an absent one `""` (D8): when it
    /// differs from the one kept, every version held goes to 0, as the
    /// server's versions restart lower; with none kept, the first subscribe
    /// after `unsubscribed`, the versions given stay.
    fn new_epoch(&mut self, epoch: String) {
        if self.epoch.as_ref().is_some_and(|kept| *kept != epoch) {
            for tracked in self.tracked.values_mut() {
                tracked.version = 0;
            }
        }
        self.epoch = Some(epoch);
    }

    /// Subscription `id` became `subscribed` again: what an earlier period
    /// asked for or scheduled is abandoned, and the retries start over (D7,
    /// I13). The actions cancelling its timers.
    #[must_use]
    fn restart(&mut self, id: SubscriptionId) -> Vec<Action> {
        self.generation += 1;
        self.end_period(id)
    }

    fn end_period(&mut self, id: SubscriptionId) -> Vec<Action> {
        self.signing.clear();
        self.left_subscribed(id)
    }

    /// Subscription `id` left `subscribed`: its refresh and retry timers
    /// stop, and the retries and the refresh guard start over (D7). A
    /// request to the callback in flight stays until the next subscribe,
    /// its answer then changing nothing (I13). The actions cancelling the
    /// timers.
    #[must_use]
    pub(in crate::machine) fn left_subscribed(&mut self, id: SubscriptionId) -> Vec<Action> {
        self.refresh_again = false;
        self.refresh_attempts = 0;
        self.sign_attempts = 0;
        self.grace_attempts = 0;
        self.replay_attempts = 0;
        let mut cancel: Vec<Action> = self
            .timers
            .drain(..)
            .map(|(timer, _)| Action::CancelSharedPollTimer(timer))
            .collect();
        if std::mem::take(&mut self.refresh_armed) {
            cancel.push(Action::CancelSharedPollRefresh(id));
        }
        cancel
    }

    /// The connection is gone: nothing queued for it is sent.
    pub(in crate::machine) fn connection_lost(&mut self) {
        self.queue.clear();
    }

    /// The worst-case cost of a track request of `batches` on `channel`
    /// (D7): JSON inside an emulation request, each version at `u64::MAX`.
    fn track_cost<'a>(channel: &str, batches: impl IntoIterator<Item = &'a TrackBatch>) -> usize {
        FRAME_COST
            + escaped_twice(channel)
            + batches.into_iter().map(Self::batch_cost).sum::<usize>()
    }

    /// The worst-case cost of `batch` in a track request (D7).
    fn batch_cost(batch: &TrackBatch) -> usize {
        BATCH_COST
            + escaped_twice(&batch.signature.0)
            + batch
                .items
                .iter()
                .map(|item| escaped_twice(&item.key) + ITEM_COST)
                .sum::<usize>()
    }

    /// `batches` grouped in frames, in order, each within `budget` by the
    /// worst-case cost, with the keys of `untracked` its batches carry in
    /// its `untrack` field (D7, D8): a batch goes with its untracks to the
    /// frame being filled when both fit, else it starts the next frame,
    /// taking as many of its untracks as the budget lets; the others are
    /// untracked once that frame is answered, before the next frame (the
    /// stale rule of [`Machine::track_command_answered`]). A batch too
    /// large for any
    /// frame goes alone.
    fn track_frames(
        channel: &str,
        batches: Vec<TrackBatch>,
        untracked: &HashSet<String>,
        budget: usize,
    ) -> Vec<(Vec<TrackBatch>, Vec<String>)> {
        let base = Self::track_cost(channel, []);
        let mut frames = Vec::new();
        let mut frame: (Vec<TrackBatch>, Vec<String>) = (Vec::new(), Vec::new());
        let mut inline: HashSet<String> = HashSet::new();
        let mut cost = base;
        for batch in batches {
            let batch_cost = Self::batch_cost(&batch);
            let untracks: Vec<String> = distinct(
                batch
                    .items
                    .iter()
                    .filter(|item| untracked.contains(&item.key))
                    .map(|item| item.key.clone())
                    .collect(),
            );
            let with_untracks = batch_cost
                + untracks
                    .iter()
                    .filter(|key| !inline.contains(*key))
                    .map(|key| untrack_key_cost(key))
                    .sum::<usize>();
            if !frame.0.is_empty() && cost + with_untracks > budget {
                frames.push(std::mem::take(&mut frame));
                inline.clear();
                cost = base;
            }
            cost += batch_cost;
            frame.0.push(batch);
            for key in untracks {
                let key_cost = untrack_key_cost(&key);
                if !inline.contains(&key) && cost + key_cost <= budget {
                    cost += key_cost;
                    inline.insert(key.clone());
                    frame.1.push(key);
                }
            }
        }
        if !frame.0.is_empty() {
            frames.push(frame);
        }
        frames
    }

    /// The untrack requests of `keys` on `channel`, each within `budget`
    /// by the worst-case cost (D7); a key too large for any goes alone.
    fn untrack_requests(channel: &str, keys: Vec<String>, budget: usize) -> Vec<Vec<String>> {
        let base = FRAME_COST + escaped_twice(channel);
        let mut requests = Vec::new();
        let mut request = Vec::new();
        let mut cost = base;
        for key in keys {
            let key_cost = untrack_key_cost(&key);
            if !request.is_empty() && cost + key_cost > budget {
                requests.push(std::mem::take(&mut request));
                cost = base;
            }
            cost += key_cost;
            request.push(key);
        }
        if !request.is_empty() {
            requests.push(request);
        }
        requests
    }
}

/// The length of `text` escaped as a JSON string, quotes included, then
/// escaped again: inside the JSON string of an emulation request (Block 26).
fn escaped_twice(text: &str) -> usize {
    let once = serde_json::to_string(text).unwrap_or_default();
    serde_json::to_string(&once).map_or(0, |twice| twice.len())
}

/// The worst-case cost of `key` in an untrack list (D7).
fn untrack_key_cost(key: &str) -> usize {
    escaped_twice(key) + UNTRACK_KEY_COST
}

/// Keys without repeats, in the order given.
fn distinct(keys: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

impl Machine {
    /// Adds a shared poll subscription to `channel`, unsubscribed, to the
    /// registry (Block 47, D3), which it shares with the other kinds.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already, of any
    /// kind.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn add_shared_poll_subscription(
        &mut self,
        channel: String,
        options: SharedPollSubscriptionOptions,
    ) -> Result<SubscriptionId, SubscriptionError> {
        let flow = SharedPollFlow::new(&options);
        let mut common = options.common;
        // What does not apply to a shared poll subscription (Block 47, out
        // of scope).
        common.data_source = false;
        common.since = None;
        common.state_source = false;
        common.positioned = false;
        common.recoverable = false;
        common.join_leave = false;
        common.tag_filter = None;
        common.channel_compaction = false;
        let id = self.add_subscription(channel, common)?;
        if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
            sub.shared_poll = Some(Box::new(flow));
        }
        Ok(id)
    }

    /// Whether `id` is a shared poll subscription.
    pub(crate) fn is_shared_poll(&self, id: SubscriptionId) -> bool {
        self.subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.shared_poll.is_some())
    }

    /// The keys shared poll subscription `id` tracks, in the order they
    /// were tracked; `None` for another kind or an id not in the registry.
    #[cfg_attr(not(any(adapter_tokio, adapter_browser)), allow(dead_code))] // Read by the adapters.
    pub(crate) fn shared_poll_tracked_keys(&self, id: SubscriptionId) -> Option<Vec<String>> {
        self.subscriptions
            .by_id
            .get(&id)
            .and_then(|sub| sub.shared_poll.as_ref())
            .map(|flow| flow.keys())
    }

    fn shared_poll_flow(&mut self, id: SubscriptionId) -> Option<&mut SharedPollFlow> {
        self.subscriptions
            .by_id
            .get_mut(&id)
            .and_then(|sub| sub.shared_poll.as_deref_mut())
    }

    /// Whether `id` is subscribed now.
    fn subscribed_now(&self, id: SubscriptionId) -> bool {
        self.state == State::Connected
            && self
                .subscriptions
                .by_id
                .get(&id)
                .is_some_and(|sub| sub.state == SubscriptionState::Subscribed)
    }

    /// An application call or a callback answer of a shared poll
    /// subscription (Block 47, D6).
    pub(in crate::machine) fn shared_poll_input(&mut self, input: super::super::Input) {
        use super::super::Input;
        match input {
            Input::TrackKeys(id, keys) => self.track_keys(id, keys),
            Input::TrackKeysWithSignature(id, items, signature) => {
                self.track_keys_with_signature(id, items, signature);
            }
            Input::UntrackKeys(id, keys) => self.untrack_keys(id, keys),
            Input::SharedPollTimerFired(timer) => self.shared_poll_timer_fired(timer),
            Input::SharedPollRefreshFired(id) => self.refresh_fired(id),
            Input::SignatureFetched {
                subscription,
                request,
                result,
            } => self.signature_fetched(subscription, request, result),
            _ => {}
        }
    }

    /// `track(keys)` (D6): each key kept at the version held, 0 for a new
    /// one; signed now when subscribed, else by the replay of the next
    /// subscribe.
    fn track_keys(&mut self, id: SubscriptionId, keys: Vec<String>) {
        let keys = distinct(keys);
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        if keys.is_empty() {
            return;
        }
        for key in &keys {
            flow.track(key, 0);
        }
        // The keys stay tracked, and the next replay reports it again
        // (D6).
        if !flow.signature_source {
            return self.signature_callback_missing(id);
        }
        if self.subscribed_now(id) {
            self.request_signature(id, keys, signing::Origin::Track);
        }
    }

    /// `track_with_signature(items, signature)` (D6): each key at the higher
    /// of its version and the one held, the signature kept for the replay,
    /// and the batch sent as given when subscribed.
    fn track_keys_with_signature(
        &mut self,
        id: SubscriptionId,
        items: Vec<SharedPollTrackItem>,
        signature: String,
    ) {
        if items.is_empty() {
            return;
        }
        let batch = TrackBatch {
            signature: Secret(signature),
            items: items
                .into_iter()
                .map(|item| KeyedItem {
                    key: item.key,
                    version: item.version,
                })
                .collect(),
        };
        let keys: Vec<String> = batch.items.iter().map(|item| item.key.clone()).collect();
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_deref_mut() else {
            return;
        };
        // A single batch cannot be split, and a frame over the server's
        // limit would end the client: refused before the library or the
        // wire (D6).
        if SharedPollFlow::track_cost(&sub.channel, [&batch]) > flow.max_track_frame_bytes {
            return self.batch_refused(id, keys);
        }
        for item in &batch.items {
            flow.track(&item.key, item.version);
        }
        flow.add_signature(keys, batch.signature.0.clone());
        if self.subscribed_now(id) {
            self.send_track(id, vec![batch], &HashSet::new(), Purpose::Track);
        }
    }

    /// A batch over the frame budget (D6): error 12 of kind `Track`, its
    /// keys untracked, on the server too when they were tracked, each with
    /// a removal.
    fn batch_refused(&mut self, id: SubscriptionId, keys: Vec<String>) {
        self.emit_shared_poll_error(
            id,
            ErrorKind::Track,
            codes::error::BAD_CONFIGURATION,
            BATCH_TOO_LARGE.to_owned(),
        );
        let keys = distinct(keys);
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let held: Vec<String> = keys
            .iter()
            .filter(|key| flow.untrack(key))
            .cloned()
            .collect();
        if self.subscribed_now(id) {
            self.send_untrack(id, held);
        }
        for key in keys {
            self.emit_shared_poll_update(id, SharedPollUpdate::removal(key), None, false);
        }
    }

    /// `untrack(keys)` (D6): untracked at once, and on the server when
    /// subscribed; an empty key set makes no request (`untrack_requests`).
    fn untrack_keys(&mut self, id: SubscriptionId, keys: Vec<String>) {
        let keys = distinct(keys);
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        for key in &keys {
            flow.untrack(key);
        }
        if self.subscribed_now(id) {
            self.send_untrack(id, keys);
        }
    }

    /// Queues a track request of `batches` (D7), in frames within the frame
    /// budget, each after the reply to the one before. A key of `untracked`
    /// goes in the `untrack` field of the frame carrying its batch, as the
    /// budget lets; the others are untracked once a reply shows them
    /// tracked (the stale rule of [`Self::track_command_answered`]).
    fn send_track(
        &mut self,
        id: SubscriptionId,
        batches: Vec<TrackBatch>,
        untracked: &HashSet<String>,
        purpose: Purpose,
    ) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_deref_mut() else {
            return;
        };
        let budget = flow.max_track_frame_bytes;
        let group = flow.next_group;
        flow.next_group += 1;
        let frames = SharedPollFlow::track_frames(&sub.channel, batches, untracked, budget);
        for (number, (batches, untrack)) in frames.into_iter().enumerate() {
            let keys: Vec<String> = batches
                .iter()
                .flat_map(|batch| batch.items.iter().map(|item| item.key.clone()))
                .collect();
            let request = SubRefreshRequest {
                kind: TRACK,
                track: batches,
                untrack,
                ..SubRefreshRequest::default()
            };
            self.queue_request(id, request, purpose, keys, (group, number > 0));
        }
        self.pump_requests(id);
    }

    /// Queues untrack requests of `keys`, as many as the frame budget needs
    /// (D7).
    fn send_untrack(&mut self, id: SubscriptionId, keys: Vec<String>) {
        self.send_untrack_ahead_of(id, keys, None);
    }

    /// Queues untrack requests of `keys` as [`Self::send_untrack`] does,
    /// ahead of the queued frames of request `group` when given, so that
    /// they go before its next frame (D7).
    fn send_untrack_ahead_of(&mut self, id: SubscriptionId, keys: Vec<String>, group: Option<u64>) {
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_ref() else {
            return;
        };
        let requests =
            SharedPollFlow::untrack_requests(&sub.channel, keys, flow.max_track_frame_bytes);
        // Where they go: before the first queued frame of `group`, in order.
        let mut at = self.shared_poll_flow(id).and_then(|flow| {
            group.and_then(|group| flow.queue.iter().position(|queued| queued.group == group))
        });
        for keys in requests {
            let own = self.shared_poll_flow(id).map_or(0, |flow| {
                flow.next_group += 1;
                flow.next_group - 1
            });
            self.queue_request(
                id,
                SubRefreshRequest {
                    kind: UNTRACK,
                    untrack: keys.clone(),
                    ..SubRefreshRequest::default()
                },
                Purpose::Untrack,
                keys,
                (own, false),
            );
            if let (Some(index), Some(flow)) = (at, self.shared_poll_flow(id))
                && let Some(queued) = flow.queue.pop_back()
            {
                flow.queue.insert(index, queued);
                at = Some(index + 1);
            }
        }
        self.pump_requests(id);
    }

    /// Queues `request` of subscription `id`; `(group, after_reply)` says
    /// which request it is a frame of, and whether it waits for the frame
    /// before.
    fn queue_request(
        &mut self,
        id: SubscriptionId,
        request: SubRefreshRequest,
        purpose: Purpose,
        keys: Vec<String>,
        (group, after_reply): (u64, bool),
    ) {
        if let Some(flow) = self.shared_poll_flow(id) {
            let generation = flow.generation;
            flow.queue.push_back(Queued {
                generation,
                request,
                purpose,
                keys,
                group,
                after_reply,
            });
        }
    }

    /// Whether a request of `id` is in flight, and whether a frame of its
    /// request `group` is.
    fn requests_in_flight(&self, id: SubscriptionId, group: u64) -> (bool, bool) {
        let mine = || {
            self.subscriptions
                .track_commands
                .values()
                .filter(move |command| command.subscription == id)
        };
        (
            mine().next().is_some(),
            mine().any(|command| command.group == group),
        )
    }

    /// Sends what the queue of `id` lets go: over an emulated transport one
    /// request at a time (D12); a frame after the reply to the one before
    /// (D7); a request of an earlier subscription is dropped.
    fn pump_requests(&mut self, id: SubscriptionId) {
        loop {
            if !self.subscribed_now(id) {
                return;
            }
            let emulated = self.transport_kind().is_emulated();
            let Some(group) = self
                .subscriptions
                .by_id
                .get(&id)
                .and_then(|sub| sub.shared_poll.as_ref())
                .and_then(|flow| flow.queue.front())
                .map(|front| front.group)
            else {
                return;
            };
            let (busy, sending) = self.requests_in_flight(id, group);
            let Some(flow) = self.shared_poll_flow(id) else {
                return;
            };
            let Some(front) = flow.queue.front() else {
                return;
            };
            if front.generation != flow.generation {
                flow.queue.pop_front();
                continue;
            }
            if (emulated && busy) || (front.after_reply && sending) {
                return;
            }
            let queued = flow.queue.pop_front().expect("checked above");
            self.send_track_command(id, queued);
        }
    }

    /// Sends `queued` for subscription `id`, one of the client's own
    /// commands, with a reply deadline of `Config::timeout` (D7, D12).
    fn send_track_command(&mut self, id: SubscriptionId, queued: Queued) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        let Queued {
            generation,
            mut request,
            purpose,
            keys,
            group,
            ..
        } = queued;
        request.channel.clone_from(&sub.channel);
        let command_id = self.command_id();
        let timer = self.new_shared_poll_timer();
        self.subscriptions.track_commands.insert(
            command_id,
            TrackCommand {
                subscription: id,
                channel: request.channel.clone(),
                generation,
                purpose,
                untracked: if purpose == Purpose::Untrack {
                    Vec::new()
                } else {
                    request.untrack.clone()
                },
                keys,
                group,
                timer: Some(timer),
            },
        );
        self.send(
            transport,
            Command {
                id: command_id,
                method: Method::SubRefresh(request),
            },
        );
        self.actions
            .push(Action::SetSharedPollTimer(timer, self.config.timeout));
    }

    /// A number for a new timer, never reused (D3).
    fn new_shared_poll_timer(&mut self) -> SharedPollTimer {
        let timer = SharedPollTimer(self.subscriptions.next_track_timer);
        self.subscriptions.next_track_timer += 1;
        timer
    }

    /// A timer fired: a track or untrack request's reply deadline, or a
    /// refresh or a retry of a subscription (D7). Over an emulated
    /// transport, where whether the keys are tracked is unknown, a deadline
    /// passing makes the client reconnect with `connecting` 3 (D12); over
    /// websocket it fails the request with error 1, as a reply would.
    fn shared_poll_timer_fired(&mut self, timer: SharedPollTimer) {
        let command = self
            .subscriptions
            .track_commands
            .iter()
            .find(|(_, command)| command.timer == Some(timer))
            .map(|(command, _)| *command);
        let Some(command) = command else {
            return self.flow_timer_fired(timer);
        };
        if self.transport_kind().is_emulated() {
            return self.disconnect(
                codes::connecting::SUBSCRIBE_TIMEOUT,
                REQUEST_TIMEOUT_REASON,
                true,
            );
        }
        // It fired: nothing to cancel.
        if let Some(pending) = self.subscriptions.track_commands.get_mut(&command) {
            pending.timer = None;
        }
        self.track_command_reply(Reply {
            id: command,
            error: Some(crate::wire::Error {
                code: codes::error::TIMEOUT,
                message: "timeout".to_owned(),
                temporary: false,
            }),
            result: ReplyResult::Empty,
        });
    }

    /// The reply to track or untrack request `command` was read: its
    /// deadline stops, also when the reply waits behind events not taken
    /// (Block 15), and a firing already on its way is ignored.
    pub(in crate::machine) fn track_command_read(&mut self, command: u32) {
        if let Some(timer) = self
            .subscriptions
            .track_commands
            .get_mut(&command)
            .and_then(|command| command.timer.take())
        {
            self.actions.push(Action::CancelSharedPollTimer(timer));
        }
    }

    /// Whether the next subscribe of `id` waits for the requests in flight
    /// on its channel, of `id` or of a subscription removed from it: over
    /// an emulated transport they could be handled after it (D12).
    pub(in crate::machine) fn shared_poll_requests_pending(&self, id: SubscriptionId) -> bool {
        let Some(channel) = self.subscription_channel(id) else {
            return false;
        };
        self.transport_kind().is_emulated() && self.requests_on(channel)
    }

    /// Whether a track or untrack request on `channel` awaits its reply.
    fn requests_on(&self, channel: &str) -> bool {
        self.subscriptions
            .track_commands
            .values()
            .any(|command| command.channel == channel)
    }

    /// The subscribe command `command_id` of shared poll subscription `id`,
    /// carrying `data` (D4): `type` 4, the token and the data, and nothing
    /// else; no delta before 47d's per-key bases (D10).
    pub(super) fn shared_poll_subscribe_request(
        &mut self,
        id: SubscriptionId,
        command_id: u32,
        data: Option<Vec<u8>>,
    ) -> Option<Command> {
        let sub = self.subscriptions.by_id.get_mut(&id)?;
        Some(Command {
            id: command_id,
            method: Method::Subscribe(SubscribeRequest {
                channel: sub.channel.clone(),
                token: Secret(sub.tokens.for_connect()),
                data,
                kind: SHARED_POLL_TYPE,
                ..SubscribeRequest::default()
            }),
        })
    }

    /// The reply to the current subscribe command of shared poll
    /// subscription `id` (D4).
    pub(super) fn shared_poll_subscribe_reply(&mut self, id: SubscriptionId, reply: Reply) {
        let result = match (reply.error, reply.result) {
            (Some(error), _) => return self.subscribe_error(id, &error),
            (None, ReplyResult::Subscribe(result)) => result,
            (None, _) => return self.reply_without_result("subscribe"),
        };
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if result.kind != SHARED_POLL_TYPE {
            // A server without shared poll subscriptions subscribed a stream
            // subscription instead (D4).
            let channel = sub.channel.clone();
            self.send_unsubscribe(id, channel);
            return self.set_unsubscribed(
                id,
                codes::unsubscribed::SHARED_POLL_NOT_SUPPORTED,
                NOT_SUPPORTED_REASON,
                false,
            );
        }
        sub.state = SubscriptionState::Subscribed;
        sub.backoff.keep_reset();
        sub.backoff.reset();
        sub.tokens.connected();
        if let Some(flow) = sub.shared_poll.as_deref_mut() {
            self.actions.extend(flow.restart(id));
        }
        // What was not taken is withdrawn here, so no update of an earlier
        // epoch can move a version the epoch rule below resets; one handed
        // to a handler moved it already (Block 41, D3).
        self.withdraw(&Owner::Client(id));
        if let Some(flow) = self.shared_poll_flow(id) {
            flow.new_epoch(result.epoch);
        }
        self.rearm_subscription_refresh(id, result.expires, result.ttl);
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribed(Subscribed {
                data: result.data,
                positioned: false,
                recoverable: false,
                was_recovering: false,
                recovered: false,
                stream_position: None,
                delta: result.delta,
                has_recovered_publications: false,
            }),
        );
        self.shared_poll_replay(id);
        self.subscription_calls_subscribed(id);
    }

    /// Tracks again after a subscribe (D8): one track request with every
    /// signature held, at the versions held, 0 for a key since untracked,
    /// which goes in the `untrack` field of the frame carrying it; then the
    /// keys no signature covers, signed by the callback. Also the retry of
    /// a track request that failed for a while (D7).
    fn shared_poll_replay(&mut self, id: SubscriptionId) {
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let batches: Vec<TrackBatch> = flow
            .library
            .iter()
            .map(|signed| flow.batch(signed))
            .collect();
        let untracked: HashSet<String> = flow
            .library
            .iter()
            .flat_map(|signed| &signed.keys)
            .filter(|key| !flow.tracked.contains_key(*key))
            .cloned()
            .collect();
        if !batches.is_empty() {
            self.send_track(id, batches, &untracked, Purpose::Track);
        }
        self.sign_unsigned(id);
    }

    /// Whether `command` is a track or untrack request awaiting its reply.
    pub(in crate::machine) fn owns_track_command(&self, command: u32) -> bool {
        self.subscriptions.track_commands.contains_key(&command)
    }

    /// The reply to a track or untrack request (D7). One made before the
    /// subscription last became `subscribed`, or while it is not, changes
    /// nothing (I13).
    pub(super) fn track_command_reply(&mut self, reply: Reply) {
        let Some(command) = self.subscriptions.track_commands.remove(&reply.id) else {
            return;
        };
        if let Some(timer) = command.timer {
            self.actions.push(Action::CancelSharedPollTimer(timer));
        }
        let id = command.subscription;
        let current = self.subscribed_now(id)
            && self
                .subscriptions
                .by_id
                .get(&id)
                .and_then(|sub| sub.shared_poll.as_ref())
                .is_some_and(|flow| flow.generation == command.generation);
        let channel = command.channel.clone();
        if current {
            self.track_command_answered(command, reply);
        }
        self.requests_settled(id, &channel);
    }

    /// The reply to a current track or untrack request (D7).
    fn track_command_answered(&mut self, command: TrackCommand, reply: Reply) {
        let id = command.subscription;
        if let Some(error) = reply.error {
            if command.purpose == Purpose::Untrack {
                return self.emit_shared_poll_error(
                    id,
                    ErrorKind::Untrack,
                    error.code,
                    error.message,
                );
            }
            let code = error.code;
            let refused = !(code == SIGNATURE_EXPIRED || code < 100 || error.temporary);
            self.emit_shared_poll_error(id, ErrorKind::Track, code, error.message);
            let mut keys = command.keys;
            // A failing frame stops the rest of its request (D7).
            if let Some(flow) = self.shared_poll_flow(id) {
                flow.queue.retain_mut(|queued| {
                    let rest = queued.group == command.group;
                    if rest {
                        keys.append(&mut queued.keys);
                    }
                    !rest
                });
            }
            if refused {
                self.track_refused(id, keys);
            } else if code == SIGNATURE_EXPIRED {
                // A refresh's own track request refused so fails that
                // refresh, which waits before the next (D7).
                if command.purpose == Purpose::Refresh {
                    self.refresh_failed(id);
                } else {
                    self.start_refresh(id);
                }
            } else {
                // Temporary, below 100 or no reply in time: the replay runs
                // again after the backoff (D7).
                self.replay_failed(id);
            }
            return;
        }
        let ReplyResult::SubRefresh(result) = reply.result else {
            return self.reply_without_result("sub_refresh");
        };
        if command.purpose == Purpose::Untrack {
            return;
        }
        self.shared_poll_publications(id, result.items);
        if !self.subscribed_now(id) {
            return;
        }
        self.track_accepted(&command, result.expires, result.ttl);
    }

    /// A track request was accepted (D7): its keys no longer tracked, as
    /// one removed meanwhile, are untracked on the server too (the stale
    /// rule); `expires` schedules the refresh; an accepted refresh starts
    /// its backoff over.
    fn track_accepted(&mut self, command: &TrackCommand, expires: bool, ttl: u32) {
        let id = command.subscription;
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let stale: Vec<String> = distinct(
            command
                .keys
                .iter()
                .filter(|key| !flow.tracked.contains_key(*key) && !command.untracked.contains(key))
                .cloned()
                .collect(),
        );
        // The whole request accepted: the backoffs of what it was for
        // start over (D7).
        if !flow
            .queue
            .iter()
            .any(|queued| queued.group == command.group)
        {
            flow.replay_attempts = 0;
            if command.purpose == Purpose::Refresh {
                flow.refresh_attempts = 0;
            }
        }
        // Before the request's next frame: until then the server counts
        // these keys against `max_keys_per_connection` (D7, D8).
        if !stale.is_empty() {
            self.send_untrack_ahead_of(id, stale, Some(command.group));
        }
        if expires {
            self.refresh_deadline(id, ttl);
        }
    }

    /// A request of `id` on `channel` was settled: the queue goes on, and a
    /// subscribe waiting for the requests in flight on the channel follows
    /// once none is left (D12).
    fn requests_settled(&mut self, id: SubscriptionId, channel: &str) {
        self.pump_requests(id);
        if !self.requests_on(channel) {
            self.wake_waiting(channel);
        }
    }

    /// The server refused track or untrack request `command` as too large
    /// (an emulation request answered with HTTP 413); whether it was one.
    /// As any `sub_refresh` (Block 8): the subscription ends with
    /// `unsubscribed` 3, after an unsubscribe command, since the same
    /// request would be refused again.
    pub(in crate::machine) fn track_command_too_large(&mut self, command: u32) -> bool {
        let Some(command) = self.subscriptions.track_commands.remove(&command) else {
            return false;
        };
        if let Some(timer) = command.timer {
            self.actions.push(Action::CancelSharedPollTimer(timer));
        }
        let id = command.subscription;
        let current = self.subscribed_now(id)
            && self
                .subscriptions
                .by_id
                .get(&id)
                .and_then(|sub| sub.shared_poll.as_ref())
                .is_some_and(|flow| flow.generation == command.generation);
        if current {
            self.set_unsubscribed(
                id,
                codes::unsubscribed::MESSAGE_SIZE_LIMIT,
                super::MESSAGE_SIZE_LIMIT_REASON,
                true,
            );
        }
        self.requests_settled(id, &command.channel);
        true
    }

    /// The server refused a track request for good (D7): its keys are
    /// untracked, on the server too, each still tracked giving a removal.
    fn track_refused(&mut self, id: SubscriptionId, keys: Vec<String>) {
        let keys = distinct(keys);
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let removed: Vec<String> = keys
            .iter()
            .filter(|key| flow.untrack(key))
            .cloned()
            .collect();
        self.send_untrack(id, keys);
        for key in removed {
            self.emit_shared_poll_update(id, SharedPollUpdate::removal(key), None, false);
        }
    }

    /// Publications of shared poll subscription `id` in one message, in
    /// order (D9): a version is compared with what the earlier ones would
    /// set once taken.
    fn shared_poll_publications(&mut self, id: SubscriptionId, publications: Vec<Publication>) {
        // A delta for a tracked key fails the subscription before any item
        // is emitted: an update emitted then withdrawn in the same message
        // would hold the next messages (Block 15).
        let delta = self.shared_poll_flow(id).is_some_and(|flow| {
            publications.iter().any(|publication| {
                publication.delta
                    && publication
                        .map
                        .as_deref()
                        .is_some_and(|part| !part.removed && flow.tracked.contains_key(&part.key))
            })
        });
        if delta {
            return self.delta_failed(id, DeltaError::NotOn);
        }
        let mut handled = HashMap::new();
        for publication in publications {
            if !self.subscribed_now(id) {
                return;
            }
            self.shared_poll_item(id, publication, &mut handled);
        }
    }

    /// A publication pushed to shared poll subscription `id` (D9).
    pub(super) fn shared_poll_publication(&mut self, id: SubscriptionId, publication: Publication) {
        self.shared_poll_item(id, publication, &mut HashMap::new());
    }

    /// A value of a tracked key, a removal, or something dropped (D9):
    /// the key not tracked, or a version not newer than the one held or
    /// handled in the same message.
    fn shared_poll_item(
        &mut self,
        id: SubscriptionId,
        publication: Publication,
        handled: &mut HashMap<String, u64>,
    ) {
        let (key, removed, version) = publication
            .map
            .as_deref()
            .map(|part| (part.key.clone(), part.removed, part.version))
            .unwrap_or_default();
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        // A key not tracked: before decoding, so it leaves no trace.
        let Some(tracked) = flow.tracked.get(&key) else {
            return;
        };
        let held = tracked.version.max(handled.get(&key).copied().unwrap_or(0));
        if !removed && version > 0 && held > 0 && version <= held {
            return;
        }
        if removed {
            flow.untrack(&key);
            return self.emit_shared_poll_update(id, SharedPollUpdate::removal(key), None, true);
        }
        // Delta is never asked for before Block 47 D10's per-key bases
        // (47d): one is a delta on a subscription without delta (Block 19).
        if publication.delta {
            return self.delta_failed(id, DeltaError::NotOn);
        }
        if version > 0 {
            handled.insert(key.clone(), version);
        }
        let update = SharedPollUpdate::from(publication);
        let taken = (version > 0).then_some((key, version));
        self.emit_shared_poll_update(id, update, taken, true);
    }

    /// Emits `update` of `id`. Taking it moves its key's version to `taken`
    /// (D5); a removal is never withdrawn (D9); `publication` counts it as
    /// a publication delivered once taken, which a removal the client made
    /// itself is not (D13).
    fn emit_shared_poll_update(
        &mut self,
        id: SubscriptionId,
        update: SharedPollUpdate,
        taken: Option<(String, u64)>,
        publication: bool,
    ) {
        let Some(event_id) = self.admit_event(Some(id), false) else {
            return;
        };
        let keep = update.removed;
        self.deliver_shared_poll_update(event_id, Owner::Client(id), taken, keep, publication);
        self.actions.push(Action::EmitSharedPollSubscription(
            event_id,
            id,
            SharedPollSubscriptionEvent::Update(update),
        ));
    }

    /// The application took an update of `key` at `version` (D5): the
    /// version held moves to it, unless the key is no longer tracked.
    pub(in crate::machine) fn shared_poll_version_taken(
        &mut self,
        id: SubscriptionId,
        key: &str,
        version: u64,
    ) {
        if let Some(tracked) = self
            .shared_poll_flow(id)
            .and_then(|flow| flow.tracked.get_mut(key))
        {
            tracked.version = tracked.version.max(version);
        }
    }

    fn emit_shared_poll_error(
        &mut self,
        id: SubscriptionId,
        kind: ErrorKind,
        code: u32,
        message: String,
    ) {
        self.emit_subscription(
            id,
            SubscriptionEvent::Error(ClientError {
                kind,
                code,
                message,
                transport: None,
            }),
        );
    }

    /// Emits `event` of shared poll subscription `id` as a shared poll
    /// subscription's event; one with no counterpart is dropped.
    pub(super) fn emit_shared_poll_lifecycle(
        &mut self,
        id: SubscriptionId,
        event: SubscriptionEvent,
    ) {
        let event = match event {
            SubscriptionEvent::Subscribing { code, reason } => {
                SharedPollSubscriptionEvent::Subscribing { code, reason }
            }
            SubscriptionEvent::Subscribed(subscribed) => {
                SharedPollSubscriptionEvent::Subscribed(subscribed)
            }
            SubscriptionEvent::Unsubscribed { code, reason } => {
                SharedPollSubscriptionEvent::Unsubscribed { code, reason }
            }
            SubscriptionEvent::Error(error) => SharedPollSubscriptionEvent::Error(error),
            _ => return,
        };
        let error = matches!(event, SharedPollSubscriptionEvent::Error(_));
        if let Some(event_id) = self.admit_event(Some(id), error) {
            self.actions
                .push(Action::EmitSharedPollSubscription(event_id, id, event));
        }
    }
}

#[cfg(test)]
mod tests;
