//! Subscription getState (Block 11b): loading the application's state when a
//! subscription has no stream position, and again when a recovery fails.
//! Behaviour follows centrifuge-js `subscription.ts` (`_subscribe`,
//! `_loadStreamState`, `_subscribeError` for code 112).

use super::super::{Action, FetchId, Machine, SubscriptionId};
use crate::wire::Error;
use crate::{StateError, StreamPosition, SubscriptionState, codes};

/// Server error answering a subscribe with the "reject unrecovered" flag
/// whose recovery is not possible (`client.proto`, `SubscribeRequest.flag`).
const UNRECOVERABLE_POSITION: u32 = 112;

impl Machine {
    /// Whether subscription `id` must load its state before subscribing:
    /// it has a getState callback and no position, as on a first subscribe
    /// without `since` or after a failed recovery.
    pub(super) fn needs_state(&self, id: SubscriptionId) -> bool {
        self.subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.options.state_source && sub.position.is_none())
    }

    /// Asks the owner for the application's state and its stream position.
    pub(super) fn fetch_subscription_state(&mut self, id: SubscriptionId) {
        let fetch = FetchId(self.subscriptions.next_fetch);
        self.subscriptions.next_fetch += 1;
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        sub.state_fetch = Some(fetch);
        let channel = sub.channel.clone();
        self.actions.push(Action::FetchSubscriptionState {
            subscription: id,
            channel,
            fetch,
        });
    }

    /// The getState callback answered. An answer no longer wanted, e.g.
    /// after `unsubscribe()` or a disconnect, changes nothing.
    pub(in crate::machine) fn subscription_state_fetched(
        &mut self,
        fetch: FetchId,
        result: Result<StreamPosition, StateError>,
    ) {
        let Some((id, sub)) = self
            .subscriptions
            .by_id
            .iter_mut()
            .find(|(_, sub)| sub.state_fetch == Some(fetch))
        else {
            return;
        };
        let id = *id;
        sub.state_fetch = None;
        if sub.state != SubscriptionState::Subscribing {
            return;
        }
        match result {
            // The subscribe recovers from the position of the state loaded,
            // after the token and the data, as any other.
            Ok(position) => {
                sub.position = Some(position);
                sub.recover = true;
                self.start_subscribe(id);
            }
            // A temporary subscribe error: reported, then retried after the
            // resubscribe backoff.
            Err(StateError(message)) => self.subscribe_error(
                id,
                &Error {
                    code: codes::error::SUBSCRIPTION_GET_STATE,
                    message,
                    temporary: true,
                },
            ),
        }
    }

    /// A subscribe answered with error 112: its recovery was rejected, as
    /// the "reject unrecovered" flag asks when getState is set. The position
    /// is dropped, and the subscription resubscribes after the backoff,
    /// loading the state again first; it stays subscribing meanwhile, with
    /// no error event. Whether `error` was that error.
    pub(super) fn recovery_rejected(&mut self, id: SubscriptionId, error: &Error) -> bool {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return false;
        };
        if error.code != UNRECOVERABLE_POSITION || !sub.options.state_source {
            return false;
        }
        sub.position = None;
        sub.recover = false;
        self.schedule_resubscribe(id);
        true
    }
}
