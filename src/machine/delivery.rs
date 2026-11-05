//! Block 15: an event counts as delivered once the application takes it,
//! and only then does a publication move its subscription's stream
//! position. A publication not yet taken when its subscription leaves
//! `subscribed` is withdrawn: never delivered, its position unmoved, so
//! recovery brings it back.

use std::collections::HashMap;

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

/// Publications emitted and not yet taken.
#[derive(Debug, Default)]
pub(super) struct Deliveries {
    pending: HashMap<EventId, Commit>,
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
    /// of `owner`, moves the owner's position to it. One that came with a
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
        self.deliveries.pending.insert(
            id,
            Commit {
                owner,
                offset,
                epoch,
                replaces,
            },
        );
    }

    /// The application took the event `id`.
    pub(super) fn event_taken(&mut self, id: EventId) {
        let Some(commit) = self.deliveries.pending.remove(&id) else {
            return;
        };
        match commit.owner.clone() {
            Owner::Client(of) => self.commit_subscription_position(of, commit),
            Owner::Server(channel) => self.commit_server_subscription_position(&channel, commit),
        }
    }

    /// Withdraws the publications of `owner` not yet taken: it left
    /// `subscribed`. Each keeps its commit until the owner of the machine
    /// answers: [`Input::EventTaken`](super::Input::EventTaken) when it was
    /// delivered already, such as one taken at once for want of a stream,
    /// [`Input::EventWithdrawn`](super::Input::EventWithdrawn) when it was
    /// dropped.
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
        self.actions.push(Action::Withdraw(ids));
    }

    /// The owner of the machine dropped the withdrawn event `id` without
    /// delivering it: the position stays.
    pub(super) fn event_withdrawn(&mut self, id: EventId) {
        self.deliveries.pending.remove(&id);
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
