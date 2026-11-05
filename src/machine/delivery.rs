//! Block 15: an event counts as delivered once the application takes it,
//! and only then does a publication move its subscription's stream
//! position. A publication not yet taken when its subscription leaves
//! `subscribed` is withdrawn: never delivered, its position unmoved, so
//! recovery brings it back.

use std::collections::{HashMap, HashSet};

use super::{Action, EventId, Machine, SubscriptionId};
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
}

/// Publications emitted and not yet taken, and the streams taking them.
#[derive(Debug, Default)]
pub(super) struct Deliveries {
    pending: HashMap<EventId, Commit>,
    /// Objects with a stream open: a subscription, or the client (`None`),
    /// whose stream also takes the server-side subscriptions' events.
    streams: HashSet<Option<SubscriptionId>>,
}

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
        };
        if self.deliveries.streams.contains(&commit.owner.stream()) {
            self.deliveries.pending.insert(id, commit);
        } else {
            self.commit(commit);
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
        self.deliveries
            .pending
            .retain(|_, commit| commit.owner != Owner::Client(id));
    }

    /// Whether a stream takes the events of `of`.
    pub(super) fn has_stream(&self, of: Option<SubscriptionId>) -> bool {
        self.deliveries.streams.contains(&of)
    }

    /// No stream takes the events of `of` anymore: they count as taken at
    /// once from now on.
    pub(super) fn stream_closed(&mut self, of: Option<SubscriptionId>) {
        self.deliveries.streams.remove(&of);
    }

    /// The application took the event `id`.
    pub(super) fn event_taken(&mut self, id: EventId) {
        if let Some(commit) = self.deliveries.pending.remove(&id) {
            self.commit(commit);
        }
    }

    fn commit(&mut self, commit: Commit) {
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
            self.settle_event(*id);
        }
        self.actions.push(Action::Withdraw(ids));
    }

    /// The position a subscribe reply at `replied` sets before any of its
    /// `publications` is taken: just before the first, in the reply's
    /// epoch, so that recovery brings them back if none is taken; the
    /// reply's own when it has none, or when the first carries no offset.
    pub(super) fn reply_baseline(
        replied: &StreamPosition,
        publications: &[Publication],
    ) -> StreamPosition {
        let offset = publications
            .first()
            .and_then(|first| first.offset)
            .map_or(replied.offset, |first| first.saturating_sub(1));
        StreamPosition {
            offset,
            epoch: replied.epoch.clone(),
        }
    }
}
