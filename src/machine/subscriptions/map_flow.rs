//! Map subscriptions (Block 45): a subscription whose subscribe loads a
//! keyed state page by page, then catches up through stream pages, then
//! goes live and delivers each change per key. Behaviour follows
//! centrifuge-js 63ed73e `_mapSubscribe` and its page handlers
//! (`src/subscription.ts:2253-2832`).

use std::collections::HashMap;

use super::super::delivery::Owner;
use super::super::{Action, Anomaly, Machine, MapSubscriptionOptions, SubscriptionId};
use crate::codec::Codec;
use crate::fossil::{self, DeltaError};
use crate::token::Secret;
use crate::wire::{CHANNEL_COMPACTION, Command, Method, Reply, ReplyResult, SubscribeRequest};
use crate::{
    MapEntry, MapKind, MapSubscriptionEvent, MapUpdate, Publication, State, StreamPosition,
    Subscribed, SubscriptionError, SubscriptionEvent, SubscriptionState, UnrecoverableStrategy,
    codes,
};

/// Phases of a map subscription's reply (`client.proto`,
/// `SubscribeResult.phase`).
const LIVE: i32 = 0;
const STREAM: i32 = 1;
const STATE: i32 = 2;

/// `type` of every map subscription's reply (`client.proto:618-620`).
const MAP_TYPE: i32 = 1;

/// Reason of [`codes::unsubscribed::MAP_STATE_TOO_LARGE`].
const STATE_TOO_LARGE_REASON: &str = "map state too large";
/// Reason of [`codes::unsubscribed::MAP_NOT_SUPPORTED`].
const NOT_SUPPORTED_REASON: &str = "map subscriptions not supported";

/// Error code by which the server refuses a position it cannot recover
/// from (`client.proto`; centrifuge `ErrorUnrecoverablePosition`), and its
/// reason, for an epoch change the client sees itself.
const UNRECOVERABLE_POSITION: u32 = 112;
const UNRECOVERABLE_POSITION_REASON: &str = "unrecoverable position";

/// What makes a map subscription one: its kind and options, and the load
/// in progress. Boxed in the subscription, so a stream subscription pays a
/// pointer (Block 14, "Marginal cost of a subscription").
#[derive(Debug)]
pub(in crate::machine) struct MapFlow {
    pub(super) kind: MapKind,
    page_size: u32,
    max_state_bytes: usize,
    strategy: UnrecoverableStrategy,
    load: Option<Load>,
    /// What each key's next delta applies to, while the last live reply
    /// has delta on; `None` while it is off (D9).
    bases: Option<HashMap<String, Vec<u8>>>,
}

/// A load of the state: the request in flight and what the pages read so
/// far gave.
#[derive(Debug)]
struct Load {
    /// The phase the request in flight asks for: [`STATE`] or [`STREAM`].
    asked_phase: i32,
    /// The cursor the state request in flight asks with; empty for the
    /// first page.
    asked_cursor: String,
    /// The stream's epoch and offset when the first page was read, kept
    /// for the whole load; stream pages move the offset (D4).
    epoch: Option<String>,
    offset: u64,
    /// The state entries and catch-up publications so far, in order and
    /// as received: only the live reply says whether delta is on, and so
    /// how to read them (D9).
    items: Vec<Publication>,
    /// Bytes of the page replies read (D5).
    bytes: usize,
    /// A reply of this load was a page: the server holds a reservation, so
    /// an error ending the load is followed by an unsubscribe (D6).
    paged: bool,
    /// The load recovers from a position: it starts in the stream phase,
    /// and its catch-up publications become updates, in order (D6).
    recovering: bool,
    catch_up: Vec<Publication>,
}

impl Default for Load {
    fn default() -> Self {
        Self {
            asked_phase: STATE,
            asked_cursor: String::new(),
            epoch: None,
            offset: 0,
            items: Vec::new(),
            bytes: 0,
            paged: false,
            recovering: false,
            catch_up: Vec::new(),
        }
    }
}

impl Load {
    /// A load recovering from `position`: the stream from there (D6).
    fn recovering(position: StreamPosition) -> Self {
        Self {
            asked_phase: STREAM,
            epoch: Some(position.epoch),
            offset: position.offset,
            recovering: true,
            ..Self::default()
        }
    }

    /// Takes a catch-up publication: kept apart when recovering, with the
    /// state otherwise.
    fn catch_up(&mut self, publication: Publication) {
        if self.recovering {
            self.catch_up.push(publication);
        } else {
            self.items.push(publication);
        }
    }
}

/// The state `items` make, by key: the last value wins, a removal deletes
/// the key, a key keeps the place of its first appearance, and a key set
/// again after a removal goes last (D4).
fn state_of(items: Vec<Publication>) -> Vec<MapEntry> {
    let mut entries: Vec<Option<Publication>> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    for publication in items {
        let (key, removed) = publication
            .map
            .as_ref()
            .map_or(("", false), |map| (map.key.as_str(), map.removed));
        let place = index.get(key).copied();
        if removed {
            if let Some(place) = place {
                entries[place] = None;
                index.remove(key);
            }
        } else if let Some(place) = place {
            entries[place] = Some(publication);
        } else {
            index.insert(key.to_owned(), entries.len());
            entries.push(Some(publication));
        }
    }
    entries.into_iter().flatten().map(MapEntry::from).collect()
}

/// How payloads are rebuilt while delta is on.
#[derive(Clone, Copy)]
struct Rebuild {
    codec: Codec,
    /// What one rebuilt value may reach (Block 19, D6).
    limit: usize,
}

impl Rebuild {
    /// Makes `publication`'s data its key's whole value and that value the
    /// key's base; a removal deletes the base (D9). On failure the key's
    /// base is gone too: its next value comes whole in the catch-up.
    fn apply(
        self,
        bases: &mut HashMap<String, Vec<u8>>,
        publication: &mut Publication,
        statistics: &mut Option<Box<crate::DeltaStatistics>>,
    ) -> Result<(), DeltaError> {
        let Some(map) = publication.map.as_ref() else {
            return Ok(());
        };
        if map.removed {
            bases.remove(&map.key);
            return Ok(());
        }
        let key = map.key.clone();
        let rebuilt = self.rebuild(bases, &key, publication, statistics);
        match rebuilt {
            Ok(data) => {
                bases.insert(key, data.clone());
                publication.data = data;
                publication.delta = false;
                Ok(())
            }
            Err(error) => {
                bases.remove(&key);
                Err(error)
            }
        }
    }

    fn rebuild(
        self,
        bases: &HashMap<String, Vec<u8>>,
        key: &str,
        publication: &mut Publication,
        statistics: &mut Option<Box<crate::DeltaStatistics>>,
    ) -> Result<Vec<u8>, DeltaError> {
        // An entry with no value (a `map_users` entry) has nothing to
        // unwrap.
        if publication.data.is_empty() && !publication.delta {
            return Ok(Vec::new());
        }
        let payload = self
            .codec
            .delta_payload(std::mem::take(&mut publication.data))
            .ok_or(DeltaError::NotAString)?;
        let received = payload.len();
        let data = if publication.delta {
            let base = bases.get(key).ok_or(DeltaError::NoBase)?;
            fossil::apply(base, &payload, self.limit)?
        } else {
            payload
        };
        statistics
            .get_or_insert_default()
            .count(publication.delta, received, data.len());
        Ok(data)
    }
}

/// What a map subscription does with a reply to its load.
enum Next {
    /// Not a map reply: the server has no map subscriptions (D15).
    NotSupported,
    /// A reply the protocol does not allow (D15).
    Fault(&'static str),
    /// The stream's epoch changed during the load (D4).
    EpochChanged,
    /// The pages passed `max_state_bytes` (D5).
    TooLarge,
    /// A page: the next request of the load follows.
    Request,
    /// The live reply, with the state built from every page.
    Live(Vec<MapEntry>),
    /// The live reply of a load that recovered, with its catch-up
    /// publications in order (D4, D6).
    Recovered(Vec<Publication>),
    /// The live reply's payloads could not be rebuilt (D9).
    DeltaFailed(DeltaError),
}

impl MapFlow {
    /// Applies `result`, `size` bytes as read, to the load in progress.
    fn next(
        &mut self,
        result: &mut crate::wire::SubscribeResult,
        size: usize,
        rebuild: Rebuild,
        statistics: &mut Option<Box<crate::DeltaStatistics>>,
    ) -> Next {
        let max_state_bytes = self.max_state_bytes;
        let load = self.load.get_or_insert_with(Load::default);
        if result.kind != MAP_TYPE {
            self.load = None;
            return Next::NotSupported;
        }
        if let Some(fault) = fault(load, result) {
            return Next::Fault(fault);
        }
        if load
            .epoch
            .as_ref()
            .is_some_and(|kept| *kept != result.epoch)
        {
            self.load = None;
            // The values of another stream's epoch (D6, D9).
            self.bases = None;
            return Next::EpochChanged;
        }
        // The live reply's state and catch-up go into the same `Sync`, so
        // they count too (D5); only a page leaves a reservation.
        if result.phase != LIVE {
            load.paged = true;
        }
        load.bytes = load.bytes.saturating_add(size);
        if load.bytes > max_state_bytes {
            self.load = None;
            return Next::TooLarge;
        }
        if load.epoch.is_none() {
            load.epoch = Some(result.epoch.clone());
            load.offset = result.offset;
        }
        match result.phase {
            STATE => {
                load.items.append(&mut result.state);
                // The next state page, or the stream from the first page's
                // offset once the state is read.
                if result.cursor.is_empty() {
                    load.asked_phase = STREAM;
                    load.asked_cursor.clear();
                } else {
                    load.asked_cursor.clone_from(&result.cursor);
                }
                Next::Request
            }
            STREAM => {
                for publication in std::mem::take(&mut result.publications) {
                    load.catch_up(publication);
                }
                load.offset = result.offset;
                Next::Request
            }
            _ => {
                let mut load = self.load.take().unwrap_or_default();
                load.items.append(&mut result.state);
                for publication in std::mem::take(&mut result.publications) {
                    load.catch_up(publication);
                }
                if let Err(error) = self.rebuild_load(&mut load, result.delta, rebuild, statistics)
                {
                    return Next::DeltaFailed(error);
                }
                if load.recovering && result.recovered {
                    return Next::Recovered(load.catch_up);
                }
                // A recovery the server did not complete: the catch-up is all
                // the state there is, so the application gets it whole.
                load.items.append(&mut load.catch_up);
                Next::Live(state_of(load.items))
            }
        }
    }

    /// Rebuilds what `load` read, now that the live reply says whether
    /// `delta` is on (D9): with delta on, each key's chain starts from the
    /// bases this load kept, none for a load from scratch; with it off the
    /// data stays as received and no base is kept.
    fn rebuild_load(
        &mut self,
        load: &mut Load,
        delta: bool,
        rebuild: Rebuild,
        statistics: &mut Option<Box<crate::DeltaStatistics>>,
    ) -> Result<(), DeltaError> {
        if !delta {
            self.bases = None;
            return if load
                .items
                .iter()
                .chain(&load.catch_up)
                .any(|publication| publication.delta)
            {
                Err(DeltaError::NotOn)
            } else {
                Ok(())
            };
        }
        let bases = self.bases.get_or_insert_default();
        for publication in load.items.iter_mut().chain(&mut load.catch_up) {
            rebuild.apply(bases, publication, statistics)?;
        }
        Ok(())
    }

    /// Rebuilds a publication pushed live: against its key's base while
    /// delta is on (D9).
    fn rebuild_live(
        &mut self,
        publication: &mut Publication,
        rebuild: Rebuild,
        statistics: &mut Option<Box<crate::DeltaStatistics>>,
    ) -> Result<(), DeltaError> {
        match &mut self.bases {
            Some(bases) => rebuild.apply(bases, publication, statistics),
            None if publication.delta => Err(DeltaError::NotOn),
            None => Ok(()),
        }
    }

    /// Ends the load in progress, if any: what it read is dropped.
    pub(super) fn end_load(&mut self) -> Option<bool> {
        self.load.take().map(|load| load.paged)
    }
}

/// What is wrong with a map subscription's reply (D15), if anything.
fn fault(load: &Load, result: &crate::wire::SubscribeResult) -> Option<&'static str> {
    let allowed = match load.asked_phase {
        STATE => matches!(result.phase, STATE | LIVE),
        _ => matches!(result.phase, STREAM | LIVE),
    };
    if !allowed {
        return Some("a phase the request does not allow");
    }
    if result.phase == STATE && !result.cursor.is_empty() && result.cursor == load.asked_cursor {
        return Some("a state page with the cursor it was asked with");
    }
    if result
        .state
        .iter()
        .chain(&result.publications)
        .any(|publication| {
            publication
                .map
                .as_ref()
                .is_none_or(|map| map.key.is_empty())
        })
    {
        return Some("an entry without a key");
    }
    None
}

impl Machine {
    /// Adds a map subscription to `channel`, unsubscribed, to the registry
    /// (Block 45, D3), which it shares with stream subscriptions.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already, of
    /// either kind; for a tag filter the server would refuse, or one with
    /// delta (Block 35, D1).
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn add_map_subscription(
        &mut self,
        channel: String,
        options: MapSubscriptionOptions,
    ) -> Result<SubscriptionId, SubscriptionError> {
        let MapSubscriptionOptions {
            kind,
            page_size,
            max_state_bytes,
            unrecoverable_strategy,
            mut common,
        } = options;
        // What does not apply to a map subscription (Block 45, out of scope).
        common.data_source = false;
        common.since = None;
        common.state_source = false;
        common.positioned = false;
        common.recoverable = false;
        common.join_leave = false;
        let id = self.add_subscription(channel, common)?;
        if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
            sub.map = Some(Box::new(MapFlow {
                kind,
                page_size,
                max_state_bytes: max_state_bytes.get(),
                strategy: unrecoverable_strategy,
                load: None,
                bases: None,
            }));
        }
        Ok(id)
    }

    /// The kind of map subscription `id` is, `None` for a stream
    /// subscription or an id not in the registry.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_subscription_kind(&self, id: SubscriptionId) -> Option<MapKind> {
        self.subscriptions
            .by_id
            .get(&id)
            .and_then(|sub| sub.map.as_ref())
            .map(|map| map.kind)
    }

    /// Whether `id` is a map subscription.
    pub(in crate::machine) fn is_map(&self, id: SubscriptionId) -> bool {
        self.subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.map.is_some())
    }

    /// The next subscribe command of map subscription `id`, carrying `data`
    /// (D4): of the load in progress, or the first of a new one.
    pub(super) fn map_subscribe_request(
        &mut self,
        id: SubscriptionId,
        command_id: u32,
        data: Option<Vec<u8>>,
    ) -> Option<Command> {
        let sub = self.subscriptions.by_id.get_mut(&id)?;
        let channel = sub.channel.clone();
        // On every request of a flow (D7).
        let token = Secret(sub.tokens.for_connect());
        let delta = sub.options.delta.is_some();
        let tag_filter = sub.options.tag_filter.clone();
        let flag = if sub.options.channel_compaction {
            CHANNEL_COMPACTION
        } else {
            0
        };
        // A new load recovers from the position a taken event set, when
        // the server made the map recoverable (D6).
        let recover_from = sub.position.clone().filter(|_| sub.recover);
        let map = sub.map.as_mut()?;
        let kind = map.kind.wire();
        let limit = i32::try_from(map.page_size).unwrap_or(i32::MAX);
        // A load from scratch starts no chain from what came before (D9).
        if map.load.is_none() && recover_from.is_none() {
            map.bases = None;
        }
        let load = map
            .load
            .get_or_insert_with(|| recover_from.map_or_else(Load::default, Load::recovering));
        Some(Command {
            id: command_id,
            method: Method::Subscribe(SubscribeRequest {
                channel,
                token,
                data,
                epoch: load.epoch.clone().unwrap_or_default(),
                offset: if load.epoch.is_some() { load.offset } else { 0 },
                // On every stream request of a recovering load, as
                // centrifuge-js sends it (`src/subscription.ts:2816-2828`).
                recover: load.recovering && load.asked_phase == STREAM,
                flag,
                delta,
                tag_filter,
                kind,
                phase: load.asked_phase,
                cursor: load.asked_cursor.clone(),
                limit,
                ..SubscribeRequest::default()
            }),
        })
    }

    /// The reply to the current subscribe command of map subscription
    /// `id`, its error or its result, `size` bytes as read (D4, D5, D15).
    pub(super) fn map_subscribe_reply(&mut self, id: SubscriptionId, reply: Reply, size: usize) {
        let rebuild = self.rebuild();
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let channel = sub.channel.clone();
        let mut result = match (reply.error, reply.result) {
            (Some(error), _) => {
                // The server may still hold the load's reservation, which
                // would refuse a new first request (D6).
                let paged = sub.map.as_mut().and_then(|map| map.end_load()) == Some(true);
                if paged {
                    self.send_unsubscribe(id, channel);
                }
                if error.code == UNRECOVERABLE_POSITION {
                    return self.map_unrecoverable(id, &error.message);
                }
                return self.subscribe_error(id, &error);
            }
            (None, ReplyResult::Subscribe(result)) => result,
            (None, _) => return self.reply_without_result("subscribe"),
        };
        let Some(map) = sub.map.as_mut() else {
            return;
        };
        let next = map.next(&mut result, size, rebuild, &mut sub.delta_statistics);
        let fatal = map.strategy == UnrecoverableStrategy::Fatal;
        match next {
            Next::NotSupported => {
                // A server without map subscriptions subscribed a stream
                // subscription instead (D15).
                self.send_unsubscribe(id, channel);
                self.set_unsubscribed(
                    id,
                    codes::unsubscribed::MAP_NOT_SUPPORTED,
                    NOT_SUPPORTED_REASON,
                    false,
                );
            }
            Next::Fault(fault) => {
                if let Some(transport) = self.transport {
                    self.actions
                        .push(Action::Anomaly(Anomaly::UnexpectedMapReply {
                            transport,
                            fault,
                        }));
                }
                self.reconnect_after_transport_closed();
            }
            // Undone, then loaded again from scratch once the unsubscribe is
            // settled (D4), unless the strategy is fatal (D6).
            Next::EpochChanged => {
                self.send_unsubscribe(id, channel);
                if fatal {
                    self.set_unsubscribed(
                        id,
                        UNRECOVERABLE_POSITION,
                        UNRECOVERABLE_POSITION_REASON,
                        false,
                    );
                } else {
                    // As for 112: the new epoch makes the position useless,
                    // so the next load starts from the first state page
                    // (D6; Codex, #200).
                    if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
                        sub.forget_position();
                    }
                    self.withdraw(&Owner::Client(id));
                    self.start_subscribe(id);
                }
            }
            Next::TooLarge => {
                self.send_unsubscribe(id, channel);
                self.set_unsubscribed(
                    id,
                    codes::unsubscribed::MAP_STATE_TOO_LARGE,
                    STATE_TOO_LARGE_REASON,
                    false,
                );
            }
            Next::Request => self.send_subscribe(id, None),
            Next::Live(entries) => self.map_live(id, result, entries),
            Next::Recovered(publications) => self.map_recovered(id, result, publications),
            Next::DeltaFailed(error) => self.delta_failed(id, error),
        }
    }

    /// Error 112 to a request of map subscription `id`'s load: its
    /// position cannot be recovered (D6). From scratch, the position is
    /// dropped and the state loaded again after the resubscribe backoff,
    /// with no `error` event; fatal, the subscription ends with 112.
    fn map_unrecoverable(&mut self, id: SubscriptionId, reason: &str) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let fatal = sub.map.as_mut().is_some_and(|map| {
            // Whatever comes next, these values have no stream (D9).
            map.bases = None;
            map.strategy == UnrecoverableStrategy::Fatal
        });
        if fatal {
            return self.set_unsubscribed(id, UNRECOVERABLE_POSITION, reason, false);
        }
        sub.forget_position();
        self.withdraw(&Owner::Client(id));
        self.schedule_resubscribe(id);
    }

    fn rebuild(&self) -> Rebuild {
        Rebuild {
            codec: self.codec,
            limit: self.config.max_message_bytes,
        }
    }

    /// The live reply of map subscription `id` (D4): subscribed, then the
    /// state with the changes made while it loaded.
    fn map_live(
        &mut self,
        id: SubscriptionId,
        result: crate::wire::SubscribeResult,
        entries: Vec<MapEntry>,
    ) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        sub.state = SubscriptionState::Subscribed;
        sub.backoff.keep_reset();
        sub.backoff.reset();
        sub.tokens.connected();
        let position = (result.positioned || result.recoverable).then(|| StreamPosition {
            offset: result.offset,
            epoch: result.epoch.clone(),
        });
        // Until the sync is taken the application lacks the state: the
        // next subscribe loads it again (D5; centrifuge-js
        // `src/subscription.ts:2657-2661`).
        sub.forget_position();
        sub.recover = result.recoverable;
        self.subscriptions.map_compact_id(id, result.id);
        self.withdraw(&Owner::Client(id));
        self.rearm_subscription_refresh(id, result.expires, result.ttl);
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribed(Subscribed {
                data: result.data,
                positioned: result.positioned,
                recoverable: result.recoverable,
                was_recovering: result.was_recovering,
                recovered: result.recovered,
                stream_position: position.clone(),
                delta: result.delta,
                has_recovered_publications: false,
            }),
        );
        // Taking the sync sets the position; it counts as no publication
        // delivered (D5, D11).
        if let Some(event_id) = self.admit_event(Some(id), false) {
            self.deliver_position(
                event_id,
                Owner::Client(id),
                position.filter(|_| result.recoverable),
            );
            self.actions.push(Action::EmitMapSubscription(
                event_id,
                id,
                MapSubscriptionEvent::Sync(entries),
            ));
        }
        self.subscription_calls_subscribed(id);
    }

    /// The live reply of map subscription `id`'s recovering load (D4, D6):
    /// subscribed, then one update per catch-up publication, in order, with
    /// no sync. Each update moves the position to its offset once taken,
    /// the last to the reply's, the top of the stream, which a tag filter
    /// may have put past it (D5).
    fn map_recovered(
        &mut self,
        id: SubscriptionId,
        result: crate::wire::SubscribeResult,
        publications: Vec<Publication>,
    ) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        sub.state = SubscriptionState::Subscribed;
        sub.backoff.keep_reset();
        sub.backoff.reset();
        sub.tokens.connected();
        sub.recover = result.recoverable;
        let reply = StreamPosition {
            offset: result.offset,
            epoch: result.epoch.clone(),
        };
        // Before the catch-up, so that updates not taken come back with the
        // next recovery (centrifuge-js `src/subscription.ts:2662-2666`).
        sub.position = result.recoverable.then(|| StreamPosition {
            offset: publications
                .first()
                .and_then(|first| first.offset)
                .map_or(reply.offset, |first| first.saturating_sub(1)),
            epoch: reply.epoch.clone(),
        });
        let recoverable = result.recoverable;
        self.subscriptions.map_compact_id(id, result.id);
        self.withdraw(&Owner::Client(id));
        self.rearm_subscription_refresh(id, result.expires, result.ttl);
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribed(Subscribed {
                data: result.data,
                positioned: result.positioned,
                recoverable: result.recoverable,
                was_recovering: result.was_recovering,
                recovered: result.recovered,
                stream_position: (result.positioned || result.recoverable).then(|| reply.clone()),
                delta: result.delta,
                has_recovered_publications: !publications.is_empty(),
            }),
        );
        let last = publications.len().checked_sub(1);
        for (index, publication) in publications.into_iter().enumerate() {
            let Some(event_id) = self.admit_event(Some(id), false) else {
                continue;
            };
            let (offset, epoch) = if !recoverable {
                (None, None)
            } else if Some(index) == last {
                (Some(reply.offset), Some(reply.epoch.clone()))
            } else {
                (
                    publication.offset,
                    Some(
                        publication
                            .epoch
                            .clone()
                            .unwrap_or_else(|| reply.epoch.clone()),
                    ),
                )
            };
            self.deliver_map_update(event_id, Owner::Client(id), offset, epoch);
            self.actions.push(Action::EmitMapSubscription(
                event_id,
                id,
                MapSubscriptionEvent::Update(MapUpdate::from(publication)),
            ));
        }
        self.subscription_calls_subscribed(id);
    }

    /// A publication pushed to map subscription `id`: an update of its key,
    /// which moves the position once taken (D5).
    pub(super) fn map_publication(&mut self, id: SubscriptionId, mut publication: Publication) {
        if publication
            .map
            .as_ref()
            .is_none_or(|map| map.key.is_empty())
        {
            if let Some(transport) = self.transport {
                self.actions
                    .push(Action::Anomaly(Anomaly::UnexpectedMapReply {
                        transport,
                        fault: "an entry without a key",
                    }));
            }
            return self.reconnect_after_transport_closed();
        }
        let rebuild = self.rebuild();
        if let Some(sub) = self.subscriptions.by_id.get_mut(&id)
            && let Some(map) = sub.map.as_mut()
            && let Err(error) =
                map.rebuild_live(&mut publication, rebuild, &mut sub.delta_statistics)
        {
            return self.delta_failed(id, error);
        }
        // No position is kept for a map the server does not recover
        // (ephemeral mode, D5).
        let recover = self
            .subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.recover);
        let Some(event_id) = self.admit_event(Some(id), false) else {
            return;
        };
        let (offset, epoch) = if recover {
            (publication.offset, publication.epoch.clone())
        } else {
            (None, None)
        };
        self.deliver_map_update(event_id, Owner::Client(id), offset, epoch);
        self.actions.push(Action::EmitMapSubscription(
            event_id,
            id,
            MapSubscriptionEvent::Update(MapUpdate::from(publication)),
        ));
    }

    /// Emits `event` of map subscription `id` as a map subscription's
    /// event; one with no counterpart is dropped.
    pub(super) fn emit_map_lifecycle(&mut self, id: SubscriptionId, event: SubscriptionEvent) {
        let event = match event {
            SubscriptionEvent::Subscribing { code, reason } => {
                MapSubscriptionEvent::Subscribing { code, reason }
            }
            SubscriptionEvent::Subscribed(subscribed) => {
                MapSubscriptionEvent::Subscribed(subscribed)
            }
            SubscriptionEvent::Unsubscribed { code, reason } => {
                MapSubscriptionEvent::Unsubscribed { code, reason }
            }
            SubscriptionEvent::Error(error) => MapSubscriptionEvent::Error(error),
            _ => return,
        };
        let error = matches!(event, MapSubscriptionEvent::Error(_));
        if let Some(event_id) = self.admit_event(Some(id), error) {
            self.actions
                .push(Action::EmitMapSubscription(event_id, id, event));
        }
    }

    /// Whether `reply`, `size` bytes as read, is a page of a map
    /// subscription's load that makes no event and can be handled at once,
    /// ahead of messages held behind events not taken (D5).
    pub(in crate::machine) fn map_page_early(&self, reply: &Reply, size: usize) -> bool {
        if self.state != State::Connected || reply.id == 0 || reply.error.is_some() {
            return false;
        }
        let ReplyResult::Subscribe(result) = &reply.result else {
            return false;
        };
        if result.kind != MAP_TYPE || !matches!(result.phase, STATE | STREAM) {
            return false;
        }
        let Some((id, _)) = self.subscriptions.subscribes.get(&reply.id) else {
            return false;
        };
        let Some(sub) = self.subscriptions.by_id.get(id) else {
            return false;
        };
        if sub.subscribe_command != Some(reply.id) {
            return false;
        }
        let Some(map) = sub.map.as_ref() else {
            return false;
        };
        let Some(load) = map.load.as_ref() else {
            return false;
        };
        fault(load, result).is_none()
            && load.epoch.as_ref().is_none_or(|kept| *kept == result.epoch)
            && load.bytes.saturating_add(size) <= map.max_state_bytes
            // The last state page has no cursor; `fault` rejects a repeated
            // one (Codex, #198).
            && (result.phase == STREAM
                || result.cursor.is_empty()
                || result.cursor != load.asked_cursor)
    }
}

#[cfg(test)]
mod tests;
