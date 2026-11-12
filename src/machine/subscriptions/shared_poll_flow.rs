//! Shared poll subscriptions (Block 47): a subscription on which the
//! application names keys, each tracked under a signature its backend
//! issued; the server polls the backend once per key for every client and
//! sends each new version of a key's value. Behaviour follows centrifuge-js
//! 63ed73e `SharedPollSubscription` and its track handlers
//! (`src/subscription.ts:1109-2245`, `:2915-3084`), except where Block 47
//! decides otherwise.

use std::collections::HashMap;

use super::super::delivery::Owner;
use super::super::{Action, FetchId, Machine, SharedPollSubscriptionOptions, SubscriptionId};
use crate::fossil::DeltaError;
use crate::shared_poll::SharedPollUpdate;
use crate::token::Secret;
use crate::wire::{
    Command, KeyedItem, Method, Reply, ReplyResult, SubRefreshRequest, SubscribeRequest, TRACK,
    TrackBatch, UNTRACK,
};
use crate::{
    ClientError, ErrorKind, Publication, SharedPollSubscriptionEvent, SharedPollTrackItem, State,
    Subscribed, SubscriptionError, SubscriptionEvent, SubscriptionState, TokenError, codes,
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
    /// Signature requests awaiting their answer, with the generation that
    /// made them.
    fetches: HashMap<FetchId, u64>,
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
    generation: u64,
    track: bool,
    keys: Vec<String>,
}

impl TrackCommand {
    /// Whether it is a request of subscription `id`.
    pub(in crate::machine) fn of(&self, id: SubscriptionId) -> bool {
        self.subscription == id
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
            fetches: HashMap::new(),
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

    /// `unsubscribe()`: the keys and signatures go with it, so the next
    /// subscribe starts cold (D5, `set_unsubscribed`).
    pub(in crate::machine) fn forget(&mut self) {
        self.tracked.clear();
        self.library.clear();
        self.fetches.clear();
    }

    /// The untrack requests of `keys` on `channel`, each within `budget`
    /// by the worst-case cost (D7); a key too large for any goes alone.
    fn untrack_requests(channel: &str, keys: Vec<String>, budget: usize) -> Vec<Vec<String>> {
        let base = FRAME_COST + escaped_twice(channel);
        let mut requests = Vec::new();
        let mut request = Vec::new();
        let mut cost = base;
        for key in keys {
            let key_cost = escaped_twice(&key) + UNTRACK_KEY_COST;
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
        // Not negotiated until per-key delta bases decode the values (D10,
        // 47d): a value must never reach the application as a patch.
        common.delta = None;
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
        if self.subscribed_now(id) {
            self.fetch_signature(id, keys);
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
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        if items.is_empty() {
            return;
        }
        for item in &items {
            flow.track(&item.key, item.version);
        }
        let keys: Vec<String> = items.iter().map(|item| item.key.clone()).collect();
        flow.add_signature(keys, signature.clone());
        if self.subscribed_now(id) {
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
            self.send_track(id, vec![batch]);
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

    /// A track or untrack request's reply deadline passed (D12).
    #[allow(clippy::unused_self)] // Until D12's green step.
    fn shared_poll_timer_fired(&mut self, _timer: super::super::SharedPollTimer) {}

    /// Asks the `get_signature` callback to sign `keys`, when there is one
    /// (D6).
    fn fetch_signature(&mut self, id: SubscriptionId, keys: Vec<String>) {
        let request = self.subscriptions.new_fetch();
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        if !flow.signature_source {
            return;
        }
        flow.fetches.insert(request, flow.generation);
        self.actions.push(Action::FetchSignature {
            subscription: id,
            request,
            keys,
        });
    }

    /// The `get_signature` callback answered `request` (D6): the keys it
    /// signed are tracked under the signature; a failure is reported.
    fn signature_fetched(
        &mut self,
        id: SubscriptionId,
        request: FetchId,
        result: Result<crate::SharedPollSignature, TokenError>,
    ) {
        let subscribed = self.subscribed_now(id);
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        // Abandoned: the subscription left `subscribed` meanwhile (I13).
        let Some(generation) = flow.fetches.remove(&request) else {
            return;
        };
        if !subscribed || generation != flow.generation {
            return;
        }
        match result {
            Ok(signed) => {
                // Keys untracked meanwhile need no signature.
                if !signed.keys.iter().any(|key| flow.tracked.contains_key(key)) {
                    return;
                }
                let signed = Signed {
                    keys: signed.keys,
                    signature: Secret(signed.signature),
                };
                let batch = flow.batch(&signed);
                flow.add_signature(signed.keys, signed.signature.0);
                self.send_track(id, vec![batch]);
            }
            Err(error) => self.emit_shared_poll_error(
                id,
                ErrorKind::Track,
                codes::error::SHARED_POLL_GET_SIGNATURE,
                error.to_string(),
            ),
        }
    }

    /// Sends a track request of `batches` (D7).
    fn send_track(&mut self, id: SubscriptionId, batches: Vec<TrackBatch>) {
        let keys = batches
            .iter()
            .flat_map(|batch| batch.items.iter().map(|item| item.key.clone()))
            .collect();
        self.send_track_command(
            id,
            SubRefreshRequest {
                kind: TRACK,
                track: batches,
                ..SubRefreshRequest::default()
            },
            true,
            keys,
        );
    }

    /// Sends untrack requests of `keys`, as many as the frame budget needs
    /// (D7).
    fn send_untrack(&mut self, id: SubscriptionId, keys: Vec<String>) {
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_ref() else {
            return;
        };
        let requests =
            SharedPollFlow::untrack_requests(&sub.channel, keys, flow.max_track_frame_bytes);
        for keys in requests {
            self.send_track_command(
                id,
                SubRefreshRequest {
                    kind: UNTRACK,
                    untrack: keys.clone(),
                    ..SubRefreshRequest::default()
                },
                false,
                keys,
            );
        }
    }

    /// Sends `request` for subscription `id`, one of the client's own
    /// commands (D7).
    fn send_track_command(
        &mut self,
        id: SubscriptionId,
        mut request: SubRefreshRequest,
        track: bool,
        keys: Vec<String>,
    ) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        let Some(flow) = sub.shared_poll.as_ref() else {
            return;
        };
        request.channel.clone_from(&sub.channel);
        let generation = flow.generation;
        let command_id = self.command_id();
        self.subscriptions.track_commands.insert(
            command_id,
            TrackCommand {
                subscription: id,
                generation,
                track,
                keys,
            },
        );
        self.send(
            transport,
            Command {
                id: command_id,
                method: Method::SubRefresh(request),
            },
        );
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
            flow.generation += 1;
        }
        self.withdraw(&Owner::Client(id));
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
    /// signature held, at the versions held; then the keys no signature
    /// covers, signed by the callback.
    fn shared_poll_replay(&mut self, id: SubscriptionId) {
        let Some(flow) = self.shared_poll_flow(id) else {
            return;
        };
        let batches: Vec<TrackBatch> = flow
            .library
            .iter()
            .map(|signed| flow.batch(signed))
            .collect();
        let unsigned: Vec<String> = flow
            .keys()
            .into_iter()
            .filter(|key| !flow.library.iter().any(|signed| signed.keys.contains(key)))
            .collect();
        if !batches.is_empty() {
            self.send_track(id, batches);
        }
        if !unsigned.is_empty() {
            self.fetch_signature(id, unsigned);
        }
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
        let id = command.subscription;
        let current = self.subscribed_now(id)
            && self
                .subscriptions
                .by_id
                .get(&id)
                .and_then(|sub| sub.shared_poll.as_ref())
                .is_some_and(|flow| flow.generation == command.generation);
        if !current {
            return;
        }
        if let Some(error) = reply.error {
            if !command.track {
                return self.emit_shared_poll_error(
                    id,
                    ErrorKind::Untrack,
                    error.code,
                    error.message,
                );
            }
            let refused = !(error.code == SIGNATURE_EXPIRED || error.code < 100 || error.temporary);
            self.emit_shared_poll_error(id, ErrorKind::Track, error.code, error.message);
            if refused {
                self.track_refused(id, command.keys);
            }
            return;
        }
        let ReplyResult::SubRefresh(result) = reply.result else {
            return self.reply_without_result("sub_refresh");
        };
        if command.track {
            self.shared_poll_publications(id, result.items);
        }
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
