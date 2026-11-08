//! Block 15: an event counts as delivered once the application takes it,
//! and only then does a publication move its subscription's stream
//! position. A publication not yet taken when its subscription leaves
//! `subscribed` is withdrawn: never delivered, its position unmoved, so
//! recovery brings it back.

use std::collections::{HashMap, HashSet};

use super::{Action, EventId, Input, Machine, SubscriptionId};
use crate::{Publication, StreamPosition};

/// Whose stream position a publication moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Owner {
    /// A client-side subscription.
    Client(SubscriptionId),
    /// A server-side subscription, by channel.
    Server(String),
}

/// What taking an emitted publication changes.
#[derive(Debug)]
pub(super) struct Commit {
    owner: Owner,
    offset: Option<u64>,
    epoch: Option<String>,
    /// From a subscribe reply: it applies even where no position is kept.
    pub(super) replaces: bool,
    /// A publication, counted delivered once taken (Block 31); not a join
    /// or a leave.
    publication: bool,
}

/// Publications emitted and not yet taken, and the streams taking them.
#[derive(Debug, Default)]
pub(super) struct Deliveries {
    pending: HashMap<EventId, Commit>,
    /// Objects with a stream open: a subscription, or the client (`None`),
    /// whose stream also takes the server-side subscriptions' events.
    streams: HashSet<Option<SubscriptionId>>,
    /// Events emitted to an open stream and not yet taken, with the
    /// stream, and how many each stream holds.
    untaken: HashMap<EventId, Option<SubscriptionId>>,
    held: HashMap<Option<SubscriptionId>, usize>,
    /// Publications handed to a handler that has not returned (Block 41,
    /// D3): their position already moved.
    in_flight: HashMap<EventId, InFlight>,
}

/// A publication handed to a handler: the position it moved from and to,
/// for a panic to move it back.
#[derive(Debug)]
struct InFlight {
    owner: Owner,
    before: Option<StreamPosition>,
    after: Option<StreamPosition>,
    publication: bool,
}

/// Untaken events a stream holds past which further `error` events for it
/// are dropped: they change no state, and they are the only events a timer
/// can add without bound, one per failed attempt (Block 15).
const ERRORS_DROPPED_PAST: usize = 256;

impl Owner {
    /// The stream taking this owner's publications.
    fn stream(&self) -> Option<SubscriptionId> {
        match self {
            Self::Client(id) => Some(*id),
            Self::Server(_) => None,
        }
    }
}

impl Commit {
    /// Moves `position` as a publication at this offset and epoch does
    /// (centrifuge-js `_handlePublication`).
    pub(super) fn apply(self, position: &mut Option<StreamPosition>) {
        if self.offset.is_none() && self.epoch.is_none() {
            return;
        }
        let position = position.get_or_insert_default();
        if let Some(offset) = self.offset {
            position.offset = offset;
        }
        if let Some(epoch) = self.epoch {
            position.epoch = epoch;
        }
    }
}

impl Machine {
    /// Records that taking the event `id`, the publication `publication`
    /// of `owner`, moves the owner's position to it; with no stream open
    /// for it, the event counts as taken at once, and the position moves
    /// now. One that came with a
    /// subscribe reply takes the reply's epoch when it carries none, and
    /// the reply's offset too when it is the reply's `last` and carries no
    /// offset, so the position ends where centrifuge-js's does
    /// (`_setSubscribed`, then `_handlePublication` for each).
    pub(super) fn deliver_publication(
        &mut self,
        id: EventId,
        owner: Owner,
        publication: &Publication,
        reply: Option<(&StreamPosition, bool)>,
    ) {
        let replaces = reply.is_some();
        let (offset, epoch) = match reply {
            // A publication of another channel, as a wildcard subscription
            // gets, is of another stream: it moves nothing, not even to the
            // reply's position (Block 35c, D3; Codex on PR #147).
            _ if publication.channel.is_some() => (None, None),
            Some((position, last)) => (
                publication.offset.or(last.then_some(position.offset)),
                Some(
                    publication
                        .epoch
                        .clone()
                        .unwrap_or_else(|| position.epoch.clone()),
                ),
            ),
            None => (publication.offset, publication.epoch.clone()),
        };
        let commit = Commit {
            owner,
            offset,
            epoch,
            replaces,
            publication: true,
        };
        if self.deliveries.streams.contains(&commit.owner.stream()) {
            self.deliveries.pending.insert(id, commit);
        } else {
            self.commit(commit);
        }
    }

    /// Records the event `id`, a join or leave of `owner`, which moves no
    /// position: it waits to be taken, and is withdrawn, as a publication
    /// is (Block 13), but taking it changes nothing.
    pub(super) fn deliver_unpositioned(&mut self, id: EventId, owner: Owner) {
        let commit = Commit {
            owner,
            offset: None,
            epoch: None,
            replaces: false,
            publication: false,
        };
        if self.deliveries.streams.contains(&commit.owner.stream()) {
            self.deliveries.pending.insert(id, commit);
        }
    }

    /// Id of an event for the stream of `stream`, or `None` to drop it: an
    /// `error` event once that stream holds 256 untaken events.
    pub(super) fn admit_event(
        &mut self,
        stream: Option<SubscriptionId>,
        error: bool,
    ) -> Option<EventId> {
        let open = self.deliveries.streams.contains(&stream);
        let held = self.deliveries.held.get(&stream).copied().unwrap_or(0);
        if error && open && held >= ERRORS_DROPPED_PAST {
            return None;
        }
        let id = self.event_id();
        if open {
            self.deliveries.untaken.insert(id, stream);
            *self.deliveries.held.entry(stream).or_default() += 1;
        }
        Some(id)
    }

    /// The event `id` left its stream: taken or withdrawn.
    fn untake(&mut self, id: EventId) {
        if let Some(stream) = self.deliveries.untaken.remove(&id)
            && let Some(held) = self.deliveries.held.get_mut(&stream)
        {
            *held = held.saturating_sub(1);
        }
    }

    /// A stream of the subscription `of`, or of the client when `None`,
    /// takes its events from now on.
    pub(super) fn stream_opened(&mut self, of: Option<SubscriptionId>) {
        self.deliveries.streams.insert(of);
    }

    /// The subscription `id` left the registry: its stream and its
    /// publications not yet taken no longer matter.
    pub(super) fn forget_deliveries(&mut self, id: SubscriptionId) {
        self.deliveries.streams.remove(&Some(id));
        self.forget_held(Some(id));
        self.deliveries
            .pending
            .retain(|_, commit| commit.owner != Owner::Client(id));
        self.deliveries
            .in_flight
            .retain(|_, handed| handed.owner != Owner::Client(id));
    }

    /// Whether a stream takes the events of `of`.
    pub(super) fn has_stream(&self, of: Option<SubscriptionId>) -> bool {
        self.deliveries.streams.contains(&of)
    }

    /// No stream takes the events of `of` anymore: they count as taken at
    /// once from now on.
    pub(super) fn stream_closed(&mut self, of: Option<SubscriptionId>) {
        self.deliveries.streams.remove(&of);
        self.forget_held(of);
    }

    /// No event of the stream of `of` waits anymore.
    fn forget_held(&mut self, of: Option<SubscriptionId>) {
        self.deliveries.held.remove(&of);
        self.deliveries.untaken.retain(|_, stream| *stream != of);
    }

    /// An input about the delivery of events (Blocks 15, 41).
    pub(super) fn delivery_input(&mut self, input: Input) {
        match input {
            Input::EventTaken(id) => {
                self.event_taken(id);
                self.settle_event(id);
            }
            Input::EventHanded(id) => self.event_handed(id),
            Input::HandlerPanicked { event, message } => self.handler_panicked(event, &message),
            Input::StreamOpened(of) => self.stream_opened(of),
            Input::StreamClosed(of) => self.stream_closed(of),
            _ => {}
        }
    }

    /// The application took the event `id`, or its handler returned.
    pub(super) fn event_taken(&mut self, id: EventId) {
        self.untake(id);
        if let Some(handed) = self.deliveries.in_flight.remove(&id) {
            if handed.publication {
                self.counters.publications_delivered += 1;
            }
        } else if let Some(commit) = self.deliveries.pending.remove(&id) {
            self.commit(commit);
        }
    }

    /// The event `id` was handed to a handler (Block 41, D3): its position
    /// moves now, and it is no longer withdrawn; it is taken once the
    /// handler returns.
    pub(super) fn event_handed(&mut self, id: EventId) {
        let Some(commit) = self.deliveries.pending.remove(&id) else {
            return;
        };
        let owner = commit.owner.clone();
        let publication = commit.publication;
        let before = self.position_of(&owner);
        self.move_position(commit);
        let after = self.position_of(&owner);
        self.deliveries.in_flight.insert(
            id,
            InFlight {
                owner,
                before,
                after,
                publication,
            },
        );
    }

    /// The handler the event `id` was handed to panicked (Block 41, D4).
    pub(super) fn handler_panicked(&mut self, id: EventId, message: &str) {
        self.untake(id);
        self.deliveries.pending.remove(&id);
        if let Some(handed) = self.deliveries.in_flight.remove(&id)
            && self.position_of(&handed.owner) == handed.after
        {
            self.set_position_of(&handed.owner, handed.before);
        }
        self.settle_event(id);
        if self.state() != crate::State::Disconnected {
            self.disconnect(
                crate::codes::disconnected::BAD_PROTOCOL,
                &format!("exception in event handler: {message}"),
                false,
            );
        }
    }

    fn commit(&mut self, commit: Commit) {
        if commit.publication {
            self.counters.publications_delivered += 1;
        }
        self.move_position(commit);
    }

    fn position_of(&self, owner: &Owner) -> Option<StreamPosition> {
        match owner {
            Owner::Client(of) => self.subscription_position(*of),
            Owner::Server(channel) => self.server_subscription_position(channel),
        }
    }

    fn set_position_of(&mut self, owner: &Owner, position: Option<StreamPosition>) {
        match owner {
            Owner::Client(of) => self.set_subscription_position(*of, position),
            Owner::Server(channel) => self.set_server_subscription_position(channel, position),
        }
    }

    fn move_position(&mut self, commit: Commit) {
        match commit.owner.clone() {
            Owner::Client(of) => self.commit_subscription_position(of, commit),
            Owner::Server(channel) => self.commit_server_subscription_position(&channel, commit),
        }
    }

    /// Withdraws the publications of `owner` not yet taken: it left
    /// `subscribed`, or its position was just set otherwise (invalidated,
    /// dropped or loaded), which an older publication taken later must
    /// not overwrite.
    pub(super) fn withdraw(&mut self, owner: &Owner) {
        let mut ids: Vec<EventId> = self
            .deliveries
            .pending
            .iter()
            .filter(|(_, commit)| commit.owner == *owner)
            .map(|(id, _)| *id)
            .collect();
        if ids.is_empty() {
            return;
        }
        ids.sort_unstable();
        for id in &ids {
            self.deliveries.pending.remove(id);
            self.untake(*id);
            self.settle_event(*id);
        }
        self.actions.push(Action::Withdraw(ids));
    }

    /// The index of the last of a subscribe reply's `publications` that is
    /// of the subscription's own stream, which reaches the reply's position
    /// once taken (Block 35c, D3).
    pub(super) fn last_of_stream(publications: &[Publication]) -> Option<usize> {
        publications
            .iter()
            .rposition(|publication| publication.channel.is_none())
    }

    /// The position a subscribe reply at `replied` sets before any of its
    /// `publications` is taken: just before the first, in the reply's
    /// epoch, so that recovery brings them back if none is taken; the
    /// reply's own when it has none, or when the first carries no offset.
    pub(super) fn reply_baseline(
        replied: &StreamPosition,
        publications: &[Publication],
    ) -> StreamPosition {
        // Publications of another channel are of another stream (Block
        // 35c, D3).
        let offset = publications
            .iter()
            .find(|publication| publication.channel.is_none())
            .and_then(|first| first.offset)
            .map_or(replied.offset, |first| first.saturating_sub(1));
        StreamPosition {
            offset,
            epoch: replied.epoch.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Input, Timer};
    use super::*;
    use crate::{ClientEvent, Config};

    /// Fails `attempts` connection attempts, taking each event at once when
    /// `read`; the error events emitted.
    fn errors_of_failed_attempts(attempts: usize, read: bool) -> usize {
        let mut machine = Machine::new(Config::default());
        machine.handle(Input::StreamOpened(None));
        let mut errors = 0;
        let mut actions = machine.handle(Input::Connect);
        for attempt in 0..attempts {
            if attempt > 0 {
                actions = machine.handle(Input::TimerFired(Timer::Reconnect));
            }
            let transport = actions
                .iter()
                .find_map(|action| match action {
                    Action::OpenTransport { transport, .. } => Some(*transport),
                    _ => None,
                })
                .expect("a transport to open");
            actions.extend(machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            }));
            for action in &actions {
                if let Action::Emit(id, event) = action {
                    errors += usize::from(matches!(event, ClientEvent::Error(_)));
                    if read {
                        machine.handle(Input::EventTaken(*id));
                    }
                }
            }
        }
        errors
    }

    #[test]
    fn a_stream_that_is_read_gets_every_error() {
        assert_eq!(errors_of_failed_attempts(300, true), 300);
    }

    #[test]
    fn a_stream_not_read_gets_errors_only_until_it_holds_256_events() {
        // The connecting event comes first.
        assert_eq!(errors_of_failed_attempts(300, false), 255);
    }
}
