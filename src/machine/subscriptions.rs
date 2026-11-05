//! Client-side subscriptions of a [`Machine`] (Block 9): the registry, each
//! subscription's state and commands in flight, and how the machine reacts
//! to them. Behaviour follows centrifuge-js `subscription.ts`.

mod state_flow;
mod token_flow;

use std::collections::{BTreeMap, HashMap};

use super::delivery::{Commit, Owner};
use super::tokens::{BeforeConnect, FetchPurpose, Tokens};
use super::{Action, FetchId, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer};
use crate::backoff::Backoff;
use crate::token::Secret;
use crate::wire::{
    Command, Error, Method, REJECT_UNRECOVERED, Reply, ReplyResult, SubscribeRequest,
    UnsubscribeRequest,
};
use crate::{
    ClientError, DataError, ErrorKind, Publication, State, StreamPosition, Subscribed,
    SubscriptionError, SubscriptionEvent, SubscriptionState, codes,
};

#[derive(Debug, Default)]
pub(super) struct Subscriptions {
    by_id: BTreeMap<SubscriptionId, Subscription>,
    /// The subscription to each channel in `by_id`: lookups by channel,
    /// publications among them, cost the same however many there are
    /// (Block 14, "O(1) dispatch").
    by_channel: HashMap<String, SubscriptionId>,
    next_id: u64,
    next_fetch: u64,
    /// Subscribe commands awaiting their reply: whose, and for which
    /// channel, as the subscription may be gone when the reply arrives.
    subscribes: HashMap<u32, (SubscriptionId, String)>,
    /// Unsubscribe commands awaiting their reply: whose, and for which
    /// channel.
    unsubscribes: HashMap<u32, (SubscriptionId, String)>,
    /// `sub_refresh` commands awaiting their reply, and whose.
    sub_refreshes: HashMap<u32, SubscriptionId>,
}

#[derive(Debug)]
struct Subscription {
    channel: String,
    options: SubscriptionOptions,
    state: SubscriptionState,
    /// The subscribe command of this subscription awaiting its reply.
    subscribe_command: Option<u32>,
    /// The data fetch awaiting its answer.
    fetch: Option<FetchId>,
    /// The getState fetch awaiting its answer.
    state_fetch: Option<FetchId>,
    /// Waiting for a command for its channel to be settled before
    /// subscribing, so the server cannot apply them out of order.
    waiting: bool,
    backoff: Backoff,
    /// A resubscribe is scheduled after a failed subscribe: a new
    /// connection leaves the subscription to it (centrifuge-js
    /// `_resubscribeTimeout`).
    resubscribe_pending: bool,
    /// Its subscription token, the fetch of a new one and the refresh
    /// command in flight (Block 5's token component).
    tokens: Tokens,
    /// Position in the channel's stream: of the last publication received,
    /// or given by the subscribe reply or the `since` option. Kept across
    /// `unsubscribe()` and disconnects (spec, "Subscription recovery
    /// state").
    position: Option<StreamPosition>,
    /// The next subscribe asks to recover from `position`: the channel was
    /// recoverable, or `since` gave a position (centrifuge-js `_recover`).
    recover: bool,
}

/// Unsubscribe code by which the server invalidates a subscription's state
/// (`client.proto`, `Unsubscribe`).
const STATE_INVALIDATED: u32 = 2502;

/// Reason of the code 3 of an unsubscribe or disconnect: the server refused
/// a command as too large.
pub(super) const MESSAGE_SIZE_LIMIT_REASON: &str = "message size limit exceeded";

/// Epoch no stream has: not empty, which means "any epoch" to the server
/// (centrifuge-js `_invalidateState`).
pub(super) const UNMATCHABLE_EPOCH: &str = "_";

impl Subscription {
    /// Drops the stream position: the next subscribe recovers nothing, or
    /// loads the state through getState first.
    fn forget_position(&mut self) {
        self.position = None;
        self.recover = false;
    }

    /// Replaces a stream position with one the server can never match, so
    /// that the next recovery fails and the app is told (centrifuge-js
    /// `_invalidateState`). Without a position there is nothing to replace:
    /// getState, when set, still runs first.
    fn invalidate_position(&mut self) {
        if let Some(position) = &mut self.position {
            position.offset = 0;
            UNMATCHABLE_EPOCH.clone_into(&mut position.epoch);
        }
    }

    /// Whether it has subscription token timers or fetches to clean up.
    fn uses_token(&self) -> bool {
        self.tokens.has_source() || self.tokens.has_token()
    }
}

impl Subscriptions {
    /// Whether `id` is the id of a subscribe or unsubscribe command.
    /// The subscription and the reply deadline of command `id`, if it is a
    /// subscription's command awaiting its reply.
    pub(super) fn reply_timer(&self, id: u32) -> Option<(SubscriptionId, SubscriptionTimer)> {
        if let Some((sub, _)) = self.subscribes.get(&id) {
            return Some((*sub, SubscriptionTimer::SubscribeReply));
        }
        if let Some((sub, _)) = self.unsubscribes.get(&id) {
            return Some((*sub, SubscriptionTimer::UnsubscribeReply));
        }
        self.sub_refreshes
            .get(&id)
            .map(|sub| (*sub, SubscriptionTimer::RefreshReply))
    }

    pub(super) fn owns_command(&self, id: u32) -> bool {
        self.subscribes.contains_key(&id)
            || self.unsubscribes.contains_key(&id)
            || self.sub_refreshes.contains_key(&id)
    }

    /// Whether a subscribe or unsubscribe command for `channel` awaits its
    /// reply: on emulated transports, whose commands are separate requests,
    /// a new command for the channel could overtake it.
    fn channel_busy(&self, channel: &str) -> bool {
        self.subscribes
            .values()
            .chain(self.unsubscribes.values())
            .any(|(_, pending)| pending == channel)
    }
}

impl Machine {
    /// Adds a subscription to `channel`, unsubscribed, to the registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already.
    pub fn add_subscription(
        &mut self,
        channel: String,
        options: SubscriptionOptions,
    ) -> Result<SubscriptionId, SubscriptionError> {
        if self.find_subscription(&channel).is_some() {
            return Err(SubscriptionError::AlreadyExists { channel });
        }
        let subscriptions = &mut self.subscriptions;
        let id = SubscriptionId(subscriptions.next_id);
        subscriptions.next_id += 1;
        subscriptions.by_channel.insert(channel.clone(), id);
        let backoff = Backoff::new(options.min_resubscribe_delay, options.max_resubscribe_delay);
        let tokens = Tokens::new(options.token.clone(), options.token_source);
        // `since` starts recovery on the first subscribe (spec).
        let position = options.since.clone();
        let recover = position.is_some();
        subscriptions.by_id.insert(
            id,
            Subscription {
                channel,
                options,
                state: SubscriptionState::Unsubscribed,
                subscribe_command: None,
                fetch: None,
                state_fetch: None,
                waiting: false,
                backoff,
                resubscribe_pending: false,
                tokens,
                position,
                recover,
            },
        );
        Ok(id)
    }

    /// State of a subscription in the registry.
    #[must_use]
    pub fn subscription_state(&self, id: SubscriptionId) -> Option<SubscriptionState> {
        self.subscriptions.by_id.get(&id).map(|sub| sub.state)
    }

    /// The subscription to `channel` in the registry, if any.
    #[must_use]
    pub fn find_subscription(&self, channel: &str) -> Option<SubscriptionId> {
        self.subscriptions.by_channel.get(channel).copied()
    }

    /// Every subscription in the registry, with its channel.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<(SubscriptionId, String)> {
        self.subscriptions
            .by_id
            .iter()
            .map(|(id, sub)| (*id, sub.channel.clone()))
            .collect()
    }

    /// `subscribe()` was called.
    pub(super) fn subscribe(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if sub.state != SubscriptionState::Unsubscribed {
            return;
        }
        sub.state = SubscriptionState::Subscribing;
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribing {
                code: codes::subscribing::SUBSCRIBE_CALLED,
                reason: "subscribe called".to_owned(),
            },
        );
        if self.state == State::Connected {
            self.start_subscribe(id);
        }
    }

    /// Subscribes a subscription in the subscribing state over the current
    /// connection: after its data, and after commands for its channel in
    /// flight, e.g. the unsubscribe of a removed subscription or a subscribe
    /// abandoned by `unsubscribe()`, are settled.
    fn start_subscribe(&mut self, id: SubscriptionId) {
        let subscriptions = &mut self.subscriptions;
        let Some(sub) = subscriptions.by_id.get(&id) else {
            return;
        };
        if sub.state != SubscriptionState::Subscribing
            || sub.subscribe_command.is_some()
            || sub.fetch.is_some()
            || sub.state_fetch.is_some()
            || sub.tokens.pending_fetch().is_some()
        {
            return;
        }
        if subscriptions.channel_busy(&sub.channel) {
            self.subscriptions
                .by_id
                .get_mut(&id)
                .expect("checked above")
                .waiting = true;
            return;
        }
        // Without a position, getState loads the state first (centrifuge-js
        // `_subscribe`).
        if self.needs_state(id) {
            self.fetch_subscription_state(id);
            return;
        }
        let sub = self.subscriptions.by_id.get(&id).expect("checked above");
        // Without a token but with a callback, the token comes first, then
        // the data callback (centrifuge-js `_canSubscribeWithoutGettingToken`).
        if sub.tokens.before_connect() == BeforeConnect::Fetch {
            self.fetch_subscription_token(id, FetchPurpose::Connect);
            return;
        }
        self.continue_subscribe(id);
    }

    /// Subscribes once a token, if one is needed, is at hand: after the
    /// data callback, if any.
    fn continue_subscribe(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        if sub.options.data_source {
            let fetch = FetchId(self.subscriptions.next_fetch);
            self.subscriptions.next_fetch += 1;
            let sub = self
                .subscriptions
                .by_id
                .get_mut(&id)
                .expect("checked above");
            sub.fetch = Some(fetch);
            let channel = sub.channel.clone();
            self.actions.push(Action::FetchSubscriptionData {
                subscription: id,
                channel,
                fetch,
            });
        } else {
            let data = sub.options.data.clone();
            self.send_subscribe(id, data);
        }
    }

    fn send_subscribe(&mut self, id: SubscriptionId, data: Option<Vec<u8>>) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let command_id = self.command_id();
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        sub.subscribe_command = Some(command_id);
        let channel = sub.channel.clone();
        let token = Secret(sub.tokens.for_connect());
        let recover_from = sub.position.clone().filter(|_| sub.recover);
        let (positioned, recoverable) = (sub.options.positioned, sub.options.recoverable);
        // With getState, a failed recovery reloads the state rather than
        // succeeding with publications missing.
        let flag = if sub.options.state_source {
            REJECT_UNRECOVERED
        } else {
            0
        };
        self.subscriptions
            .subscribes
            .insert(command_id, (id, channel.clone()));
        self.send(
            transport,
            Command {
                id: command_id,
                method: Method::Subscribe(SubscribeRequest {
                    channel,
                    token,
                    data,
                    recover: recover_from.is_some(),
                    offset: recover_from.as_ref().map_or(0, |at| at.offset),
                    epoch: recover_from.map(|at| at.epoch).unwrap_or_default(),
                    positioned,
                    recoverable,
                    flag,
                }),
            },
        );
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
            self.config.timeout,
        ));
    }

    fn send_unsubscribe(&mut self, id: SubscriptionId, channel: String) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let command_id = self.command_id();
        self.subscriptions
            .unsubscribes
            .insert(command_id, (id, channel.clone()));
        self.send(
            transport,
            Command {
                id: command_id,
                method: Method::Unsubscribe(UnsubscribeRequest { channel }),
            },
        );
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::UnsubscribeReply,
            self.config.timeout,
        ));
    }

    /// Lets the subscriptions waiting on `channel` subscribe once no command
    /// for it is in flight.
    fn wake_waiting(&mut self, channel: &str) {
        if self.subscriptions.channel_busy(channel) {
            return;
        }
        let waiting: Vec<_> = self
            .subscriptions
            .by_id
            .iter_mut()
            .filter(|(_, sub)| sub.waiting && sub.channel == channel)
            .map(|(id, sub)| {
                sub.waiting = false;
                *id
            })
            .collect();
        for id in waiting {
            self.start_subscribe(id);
        }
    }

    /// The data callback answered.
    pub(super) fn subscription_data_fetched(
        &mut self,
        fetch: FetchId,
        result: Result<Vec<u8>, DataError>,
    ) {
        let Some((id, sub)) = self
            .subscriptions
            .by_id
            .iter_mut()
            .find(|(_, sub)| sub.fetch == Some(fetch))
        else {
            // Abandoned: unsubscribed, or the connection went away.
            return;
        };
        let id = *id;
        sub.fetch = None;
        match result {
            Ok(data) => self.send_subscribe(id, Some(data)),
            // centrifuge-js `_handleGetDataError`.
            Err(DataError::Unauthorized) => {
                self.set_unsubscribed(id, codes::unsubscribed::UNAUTHORIZED, "unauthorized", false);
            }
            Err(DataError::Failed(message)) => {
                self.emit_subscription(
                    id,
                    SubscriptionEvent::Error(ClientError {
                        kind: ErrorKind::SubscriptionData,
                        code: codes::error::BAD_CONFIGURATION,
                        message,
                        transport: None,
                    }),
                );
                self.schedule_resubscribe(id);
            }
        }
    }

    fn schedule_resubscribe(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let delay = sub.backoff.next_delay(&mut self.rng);
        sub.resubscribe_pending = true;
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::Resubscribe,
            delay,
        ));
    }

    pub(super) fn subscription_timer_fired(
        &mut self,
        id: SubscriptionId,
        timer: SubscriptionTimer,
    ) {
        match timer {
            SubscriptionTimer::Resubscribe => {
                if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
                    sub.resubscribe_pending = false;
                }
                // Not connected: the next connection subscribes it.
                if self.state == State::Connected {
                    self.start_subscribe(id);
                }
            }
            // The server-side state is unknown: start over (centrifuge-js
            // `connectingCodes.subscribeTimeout`, `unsubscribeError`).
            SubscriptionTimer::SubscribeReply => {
                let pending = self
                    .subscriptions
                    .subscribes
                    .values()
                    .any(|(of, _)| *of == id);
                if pending {
                    self.disconnect(
                        codes::connecting::SUBSCRIBE_TIMEOUT,
                        "subscribe timeout",
                        true,
                    );
                }
            }
            SubscriptionTimer::Refresh => self.subscription_refresh_due(id),
            SubscriptionTimer::RefreshReply => self.subscription_refresh_timed_out(id),
            SubscriptionTimer::UnsubscribeReply => {
                let pending = self
                    .subscriptions
                    .unsubscribes
                    .values()
                    .any(|(of, _)| *of == id);
                if pending {
                    self.unsubscribe_error();
                }
            }
        }
    }

    /// Handles the reply to a subscribe or unsubscribe command
    /// ([`Subscriptions::owns_command`]).
    /// The server refused command `command` as too large (an emulation
    /// request answered with HTTP 413). A subscribe or `sub_refresh` would be
    /// refused again, so it is not retried: its subscription goes to
    /// unsubscribed with [`codes::unsubscribed::MESSAGE_SIZE_LIMIT`],
    /// after an unsubscribe command when the server holds the subscription
    /// (owner decision, 2026-11-04). Whether `command` was one of them.
    pub(in crate::machine) fn subscription_command_too_large(&mut self, command: u32) -> bool {
        if let Some(id) = self.subscriptions.sub_refreshes.remove(&command) {
            self.actions.push(Action::CancelSubscriptionTimer(
                id,
                SubscriptionTimer::RefreshReply,
            ));
            let current = self.subscriptions.by_id.get_mut(&id).is_some_and(|sub| {
                sub.tokens.take_refresh_reply(command) && sub.state == SubscriptionState::Subscribed
            });
            if current {
                self.set_unsubscribed(
                    id,
                    codes::unsubscribed::MESSAGE_SIZE_LIMIT,
                    MESSAGE_SIZE_LIMIT_REASON,
                    true,
                );
            }
            return true;
        }
        let Some((id, channel)) = self.subscriptions.subscribes.remove(&command) else {
            return false;
        };
        self.actions.push(Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
        ));
        let current = self
            .subscriptions
            .by_id
            .get_mut(&id)
            .filter(|sub| sub.subscribe_command == Some(command));
        match current {
            // Never subscribed on the server: nothing to unsubscribe.
            Some(sub) => {
                sub.subscribe_command = None;
                self.set_unsubscribed(
                    id,
                    codes::unsubscribed::MESSAGE_SIZE_LIMIT,
                    MESSAGE_SIZE_LIMIT_REASON,
                    false,
                );
            }
            // Unsubscribed or removed while in flight: the channel is free.
            None => self.wake_waiting(&channel),
        }
        true
    }

    pub(super) fn subscription_command_reply(&mut self, reply: Reply) {
        if let Some(id) = self.subscriptions.sub_refreshes.remove(&reply.id) {
            self.sub_refresh_reply(id, reply);
            return;
        }
        if let Some((id, channel)) = self.subscriptions.unsubscribes.remove(&reply.id) {
            self.actions.push(Action::CancelSubscriptionTimer(
                id,
                SubscriptionTimer::UnsubscribeReply,
            ));
            // Without its result the server-side state is unknown too.
            if reply.error.is_some() || reply.result != ReplyResult::Unsubscribe {
                self.unsubscribe_error();
                return;
            }
            // Subscriptions to the channel that waited for it can go on.
            self.wake_waiting(&channel);
            return;
        }
        let Some((id, channel)) = self.subscriptions.subscribes.remove(&reply.id) else {
            return;
        };
        self.actions.push(Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
        ));
        let current = self
            .subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.subscribe_command == Some(reply.id));
        if !current {
            // Unsubscribed or removed while in flight: undo a subscription
            // the server made, so none outlives the handle; subscriptions
            // to the channel wait for that too.
            if reply.error.is_none() {
                self.send_unsubscribe(id, channel);
            } else {
                self.wake_waiting(&channel);
            }
            return;
        }
        let sub = self
            .subscriptions
            .by_id
            .get_mut(&id)
            .expect("checked above");
        sub.subscribe_command = None;
        match (reply.error, reply.result) {
            (Some(error), _) => self.subscribe_error(id, &error),
            (None, ReplyResult::Subscribe(result)) => {
                sub.state = SubscriptionState::Subscribed;
                sub.backoff.reset();
                sub.tokens.connected();
                // The server decides: a recoverable reply gives the position
                // to recover from next; any other reply ends recovery, so a
                // channel that does not recover is never asked to
                // (centrifuge-js `_setSubscribed`).
                let reported = (result.positioned || result.recoverable).then(|| StreamPosition {
                    offset: result.offset,
                    epoch: result.epoch.clone(),
                });
                sub.recover = result.recoverable;
                // With publications, the reply's position is reached once
                // the last of them is taken (Block 15).
                let replied = result.recoverable.then_some(StreamPosition {
                    offset: result.offset,
                    epoch: result.epoch,
                });
                if let Some(replied) = &replied {
                    sub.position = Some(Machine::reply_baseline(replied, &result.publications));
                }
                self.withdraw(&Owner::Client(id));
                if result.expires {
                    self.schedule_subscription_refresh(id, result.ttl);
                }
                self.emit_subscription(
                    id,
                    SubscriptionEvent::Subscribed(Subscribed {
                        data: result.data,
                        positioned: result.positioned,
                        recoverable: result.recoverable,
                        was_recovering: result.was_recovering,
                        recovered: result.recovered,
                        stream_position: reported,
                    }),
                );
                // Recovered publications, or initial ones, follow it and
                // move the position once taken.
                let last = result.publications.len().saturating_sub(1);
                for (index, publication) in result.publications.into_iter().enumerate() {
                    let reply = replied.as_ref().map(|position| (position, index == last));
                    self.emit_publication(id, publication, reply);
                }
            }
            // A reply to subscribe without its result breaks the protocol.
            (None, _) => self.reconnect_after_transport_closed(),
        }
    }

    /// An unsubscribe failed or got no reply: the server-side state is
    /// unknown, so the client reconnects (centrifuge-js `_unsubscribe`,
    /// `connectingCodes.unsubscribeError`).
    fn unsubscribe_error(&mut self) {
        self.disconnect(
            codes::connecting::UNSUBSCRIBE_ERROR,
            "unsubscribe error",
            true,
        );
    }

    /// centrifuge-js `_subscribeError`: temporary errors are retried.
    fn subscribe_error(&mut self, id: SubscriptionId, error: &Error) {
        if self.subscribe_token_expired(id, error) || self.recovery_rejected(id, error) {
            return;
        }
        if error.code < 100 || error.temporary {
            self.emit_subscription(
                id,
                SubscriptionEvent::Error(ClientError {
                    kind: ErrorKind::Subscribe,
                    code: error.code,
                    message: error.message.clone(),
                    transport: None,
                }),
            );
            self.schedule_resubscribe(id);
        } else {
            self.set_unsubscribed(id, error.code, &error.message, false);
        }
    }

    /// `unsubscribe()` was called.
    pub(super) fn unsubscribe(&mut self, id: SubscriptionId) {
        self.set_unsubscribed(
            id,
            codes::unsubscribed::UNSUBSCRIBE_CALLED,
            "unsubscribe called",
            true,
        );
    }

    /// The subscription was removed from the registry: unsubscribed first.
    pub(super) fn remove_subscription(&mut self, id: SubscriptionId) {
        self.unsubscribe(id);
        if let Some(sub) = self.subscriptions.by_id.remove(&id) {
            self.subscriptions.by_channel.remove(&sub.channel);
        }
        self.forget_deliveries(id);
    }

    /// Moves a subscription to unsubscribed; with `send`, a server-side
    /// subscription is undone with an unsubscribe command.
    fn set_unsubscribed(&mut self, id: SubscriptionId, code: u32, reason: &str, send: bool) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if sub.state == SubscriptionState::Unsubscribed {
            return;
        }
        let was_subscribed = sub.state == SubscriptionState::Subscribed;
        // A subscribe in flight stays in `subscribes`, with its reply
        // deadline: its reply undoes it, its timeout starts over.
        sub.subscribe_command = None;
        if let Some(fetch) = sub.fetch.take() {
            self.actions.push(Action::CancelSubscriptionData(fetch));
        }
        if let Some(fetch) = sub.state_fetch.take() {
            self.actions.push(Action::CancelSubscriptionState(fetch));
        }
        sub.waiting = false;
        sub.state = SubscriptionState::Unsubscribed;
        // A later subscribe() starts its retries over (centrifuge-js
        // `_clearSubscribingState`).
        sub.backoff.reset();
        sub.resubscribe_pending = false;
        let channel = sub.channel.clone();
        self.actions.push(Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::Resubscribe,
        ));
        // A token being fetched, a refresh scheduled or in flight end too
        // (centrifuge-js `_setUnsubscribed`).
        self.end_token_refresh(id);
        if send && was_subscribed {
            self.send_unsubscribe(id, channel);
        }
        self.emit_subscription(
            id,
            SubscriptionEvent::Unsubscribed {
                code,
                reason: reason.to_owned(),
            },
        );
    }

    /// The server unsubscribed the client from `channel` (spec, "Unsubscribe
    /// codes"; centrifuge-js `_handleUnsubscribe`): from code 2500 the
    /// subscription subscribes again at once, below it unsubscribes, with
    /// the server's code and reason; the server already dropped it, so no
    /// unsubscribe command is sent. A push for a subscription not subscribed
    /// ends an earlier one, e.g. one that `unsubscribe()` ended, and changes
    /// nothing.
    pub(super) fn server_unsubscribe(&mut self, channel: &str, code: u32, reason: &str) {
        let Some(id) = self.find_subscription(channel).filter(|id| {
            self.subscriptions
                .by_id
                .get(id)
                .is_some_and(|sub| sub.state == SubscriptionState::Subscribed)
        }) else {
            // No subscribed client-side subscription: the server-side one,
            // if any, as for publications.
            self.server_subscription_unsubscribed(channel, code, reason);
            return;
        };
        if code < 2500 {
            self.set_unsubscribed(id, code, reason, false);
            return;
        }
        let sub = self.subscriptions.by_id.get_mut(&id).expect("found above");
        sub.state = SubscriptionState::Subscribing;
        // A refresh of the dropped subscription's token would hold the
        // subscribe back, or report a timeout while subscribing; the new
        // subscribe reply schedules the next one.
        self.end_token_refresh(id);
        if code == STATE_INVALIDATED {
            self.invalidate_subscription_state(id);
        }
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribing {
                code,
                reason: reason.to_owned(),
            },
        );
        self.start_subscribe(id);
    }

    /// "State invalidated" (unsubscribe code 2502): the subscription drops
    /// its token and its stream position before subscribing again, so the
    /// token callback, if any, gives a new token, and getState, if any,
    /// reloads the state (`client.proto`, `Unsubscribe`: "client must drop
    /// its subscription token and any state accumulated"). centrifuge-js
    /// keeps a token without a callback, and a sentinel position.
    fn invalidate_subscription_state(&mut self, id: SubscriptionId) {
        if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
            sub.tokens.invalidate();
            sub.forget_position();
        }
        self.withdraw(&Owner::Client(id));
    }

    /// "State invalidated" (disconnect code 3014): every subscription keeps
    /// a stream position the server can never match, so that its next
    /// recovery fails, and drops its token, a fixed one too, after ending
    /// the token's refresh, as for code 2502: once dropped, a fixed token
    /// no longer tells the teardown that a refresh may be scheduled. A
    /// token callback gives the resubscribe a new token, without one it
    /// carries none (Block 11, owner decisions; centrifuge-js
    /// `_invalidateState` keeps a fixed token).
    pub(super) fn invalidate_subscriptions(&mut self) {
        let ids: Vec<SubscriptionId> = self.subscriptions.by_id.keys().copied().collect();
        for id in ids {
            self.end_token_refresh(id);
            if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
                sub.invalidate_position();
                sub.tokens.invalidate();
            }
            self.withdraw(&Owner::Client(id));
        }
    }

    /// A publication pushed for `channel`.
    pub(super) fn publication(&mut self, channel: &str, publication: Publication) {
        // Only a subscribed handle receives publications (Block 9).
        let subscribed = self.find_subscription(channel).filter(|id| {
            self.subscriptions
                .by_id
                .get(id)
                .is_some_and(|sub| sub.state == SubscriptionState::Subscribed)
        });
        if let Some(id) = subscribed {
            self.emit_publication(id, publication, None);
        } else {
            self.server_publication(channel, publication);
        }
    }

    /// The connection is established: subscribing subscriptions subscribe.
    pub(super) fn subscriptions_connected(&mut self) {
        let subscribing: Vec<_> = self
            .subscriptions
            .by_id
            .iter()
            // A subscribe that failed is retried after its backoff, also
            // across a reconnect (owner decision, 2026-11-04;
            // centrifuge-js `_sendSubscribeCommands`).
            .filter(|(_, sub)| {
                sub.state == SubscriptionState::Subscribing && !sub.resubscribe_pending
            })
            .map(|(id, _)| *id)
            .collect();
        for id in subscribing {
            self.start_subscribe(id);
        }
    }

    /// The connection is torn down: commands in flight will get no reply,
    /// and subscribed subscriptions subscribe again on the next connection
    /// (spec, "Subscription states").
    pub(super) fn subscriptions_disconnected(&mut self) {
        for (id, _) in self
            .subscriptions
            .subscribes
            .drain()
            .map(|(_, pending)| pending)
        {
            self.actions.push(Action::CancelSubscriptionTimer(
                id,
                SubscriptionTimer::SubscribeReply,
            ));
        }
        for (id, _) in self
            .subscriptions
            .unsubscribes
            .drain()
            .map(|(_, pending)| pending)
        {
            self.actions.push(Action::CancelSubscriptionTimer(
                id,
                SubscriptionTimer::UnsubscribeReply,
            ));
        }
        self.subscriptions.sub_refreshes.clear();
        let mut resubscribing = Vec::new();
        for (id, sub) in &mut self.subscriptions.by_id {
            sub.subscribe_command = None;
            if let Some(fetch) = sub.fetch.take() {
                self.actions.push(Action::CancelSubscriptionData(fetch));
            }
            if let Some(fetch) = sub.state_fetch.take() {
                self.actions.push(Action::CancelSubscriptionState(fetch));
            }
            // The token is kept for the resubscribe; its refresh starts
            // again from the new subscribe reply.
            if sub.uses_token() {
                if let Some(fetch) = sub.tokens.abandon() {
                    self.actions.push(Action::CancelSubscriptionToken(fetch));
                }
                for timer in [SubscriptionTimer::Refresh, SubscriptionTimer::RefreshReply] {
                    self.actions
                        .push(Action::CancelSubscriptionTimer(*id, timer));
                }
            }
            sub.waiting = false;
            if sub.state == SubscriptionState::Subscribed {
                sub.state = SubscriptionState::Subscribing;
                resubscribing.push(*id);
            }
        }
        for id in resubscribing {
            self.emit_subscription(
                id,
                SubscriptionEvent::Subscribing {
                    code: codes::subscribing::TRANSPORT_CLOSED,
                    reason: "transport closed".to_owned(),
                },
            );
        }
    }

    fn emit_subscription(&mut self, id: SubscriptionId, event: SubscriptionEvent) {
        // Leaving subscribed withdraws the publications not yet taken.
        if matches!(
            event,
            SubscriptionEvent::Subscribing { .. } | SubscriptionEvent::Unsubscribed { .. }
        ) {
            self.withdraw(&Owner::Client(id));
        }
        let event_id = self.event_id();
        self.actions
            .push(Action::EmitSubscription(event_id, id, event));
    }

    /// Emits a publication of `id`, which moves its position once taken;
    /// `reply` is the position of the subscribe reply it came with, and
    /// whether it is that reply's last publication.
    fn emit_publication(
        &mut self,
        id: SubscriptionId,
        publication: Publication,
        reply: Option<(&StreamPosition, bool)>,
    ) {
        let event_id = self.event_id();
        self.deliver_publication(event_id, Owner::Client(id), &publication, reply);
        self.actions.push(Action::EmitSubscription(
            event_id,
            id,
            SubscriptionEvent::Publication(publication),
        ));
    }

    /// Applies a taken publication's `commit` to the position of `id`.
    pub(super) fn commit_subscription_position(&mut self, id: SubscriptionId, commit: Commit) {
        if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
            commit.apply(&mut sub.position);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::machine::{EventId, Input, Timer, TransportId, TransportKind};
    use crate::{Config, StateError, TokenError};

    /// `actions` with every event id 0, to compare emitted events by value.
    fn without_ids(actions: &[Action]) -> Vec<Action> {
        actions
            .iter()
            .cloned()
            .map(|action| match action {
                Action::Emit(_, event) => Action::Emit(EventId(0), event),
                Action::EmitSubscription(_, id, event) => {
                    Action::EmitSubscription(EventId(0), id, event)
                }
                action => action,
            })
            .collect()
    }

    /// A machine connected over a transport, with a subscription to "news".
    fn connected() -> (Machine, TransportId, SubscriptionId) {
        connected_with(SubscriptionOptions::default())
    }

    /// As [`connected`], the subscription having `options`.
    fn connected_with(options: SubscriptionOptions) -> (Machine, TransportId, SubscriptionId) {
        let mut machine = Machine::new(Config::default());
        let transport = connect(&mut machine, Input::Connect, 1);
        let id = machine.add_subscription("news".into(), options).unwrap();
        (machine, transport, id)
    }

    /// Feeds `input`, which opens a transport, and answers connect command
    /// `command`: the transport.
    fn connect(machine: &mut Machine, input: Input, command: u32) -> TransportId {
        let transport = opened(&machine.handle(input)).expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));
        machine.handle(Input::Frame {
            transport,
            data: format!(r#"{{"id":{command},"connect":{{"client":"c"}}}}"#).into_bytes(),
        });
        transport
    }

    fn opened(actions: &[Action]) -> Option<TransportId> {
        actions.iter().find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
    }

    /// The server closes `transport` with `code`: the actions of the
    /// reconnect attempt that follows.
    fn closed_then_reconnecting(
        machine: &mut Machine,
        transport: TransportId,
        code: u16,
    ) -> Vec<Action> {
        machine.handle(Input::TransportClosed {
            transport,
            code: Some(code),
            reason: "closed by the test".into(),
        });
        machine.handle(Input::TimerFired(Timer::Reconnect))
    }

    /// Subscribes `id` over `transport`, the server answering command 2
    /// with a recoverable stream at offset 0 of epoch "e".
    /// The events with a stream to take them wait to be taken before the
    /// next message is handled (Block 15): the subscribe's are taken.
    fn subscribed_recoverable(machine: &mut Machine, transport: TransportId, id: SubscriptionId) {
        machine.handle(Input::Subscribe(id));
        let replied = machine.handle(Input::Frame {
            transport,
            data: br#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":0}}"#.to_vec(),
        });
        for action in replied {
            if let Action::EmitSubscription(event, ..) | Action::Emit(event, _) = action {
                machine.handle(Input::EventTaken(event));
            }
        }
    }

    /// The ids of the publications emitted among `actions`, in order.
    fn publication_ids(actions: &[Action]) -> Vec<EventId> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::EmitSubscription(event, _, SubscriptionEvent::Publication(_)) => {
                    Some(*event)
                }
                _ => None,
            })
            .collect()
    }

    fn offset_of(machine: &Machine, id: SubscriptionId) -> Option<u64> {
        machine.subscriptions.by_id[&id]
            .position
            .as_ref()
            .map(|position| position.offset)
    }

    /// A frame of `transport` pushing a publication at `offset` to "news".
    fn pushed(transport: TransportId, offset: u64) -> Input {
        Input::Frame {
            transport,
            data: format!(
                r#"{{"push":{{"channel":"news","pub":{{"data":{{}},"offset":{offset}}}}}}}"#
            )
            .into_bytes(),
        }
    }

    // Codex on PR #64: a long-lived client creating and removing
    // subscriptions keeps no trace of the removed ones' streams.
    #[test]
    fn removing_a_subscription_forgets_its_stream() {
        let (mut machine, _, id) = connected();
        machine.handle(Input::StreamOpened(Some(id)));

        machine.handle(Input::RemoveSubscription(id));

        assert!(!machine.has_stream(Some(id)));
    }

    #[test]
    fn a_publication_moves_the_position_only_once_taken() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::StreamOpened(Some(id)));
        subscribed_recoverable(&mut machine, transport, id);
        let pushed = machine.handle(pushed(transport, 1));
        let received = offset_of(&machine, id);

        machine.handle(Input::EventTaken(publication_ids(&pushed)[0]));

        assert_eq!(received, Some(0));
        assert_eq!(offset_of(&machine, id), Some(1));
    }

    #[test]
    fn without_a_stream_a_publication_moves_the_position_at_once() {
        let (mut machine, transport, id) = connected();
        subscribed_recoverable(&mut machine, transport, id);

        machine.handle(pushed(transport, 1));

        assert_eq!(offset_of(&machine, id), Some(1));
    }

    // Codex on PR #64: without a stream, a resubscribe made by a later
    // message of the same frame recovers from the publication before it.
    #[test]
    fn without_a_stream_a_resubscribe_in_the_same_frame_recovers_after_the_publication() {
        let (mut machine, transport, id) = connected();
        subscribed_recoverable(&mut machine, transport, id);

        let actions = machine.handle(Input::Frame {
            transport,
            data: br#"{"push":{"channel":"news","pub":{"data":{},"offset":1}}}
{"push":{"channel":"news","unsubscribe":{"code":2500,"reason":"resubscribe"}}}"#
                .to_vec(),
        });

        let resubscribe = String::from_utf8(frame_sent(&actions)).unwrap();
        assert!(resubscribe.contains(r#""offset":1"#), "{resubscribe}");
    }

    #[test]
    fn a_publication_not_taken_when_its_subscription_leaves_subscribed_is_withdrawn() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::StreamOpened(Some(id)));
        subscribed_recoverable(&mut machine, transport, id);
        let publication = publication_ids(&machine.handle(pushed(transport, 1)))[0];

        let disconnected = machine.handle(Input::Disconnect);
        machine.handle(Input::EventTaken(publication));

        assert!(
            disconnected.contains(&Action::Withdraw(vec![publication])),
            "{disconnected:?}"
        );
        assert_eq!(offset_of(&machine, id), Some(0));
    }

    // Block 15, rule X: a state invalidation read after a publication not
    // yet taken waits for it; taken, it moves the position, which the
    // invalidation then makes unmatchable.
    #[test]
    fn a_state_invalidation_waits_for_the_publication_before_it() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::StreamOpened(Some(id)));
        subscribed_recoverable(&mut machine, transport, id);
        let publication = publication_ids(&machine.handle(pushed(transport, 1)))[0];

        let held = machine.handle(Input::Frame {
            transport,
            data: br#"{"push":{"disconnect":{"code":3014,"reason":"state invalidated"}}}"#.to_vec(),
        });
        let read_ahead = machine.read_ahead();
        let taken = machine.handle(Input::EventTaken(publication));

        assert!(
            !held.iter().any(|action| matches!(action, Action::Emit(..))),
            "{held:?}"
        );
        assert_eq!(read_ahead.0, 1);
        assert!(
            taken.iter().any(|action| matches!(
                action,
                Action::Emit(_, crate::ClientEvent::Connecting { .. })
            )),
            "{taken:?}"
        );
        let position = machine.subscriptions.by_id[&id].position.clone();
        assert_eq!(
            position.map(|position| (position.offset, position.epoch)),
            Some((0, UNMATCHABLE_EPOCH.to_owned()))
        );
    }

    #[test]
    fn publications_of_a_subscribe_reply_move_the_position_one_by_one_as_taken() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::StreamOpened(Some(id)));
        machine.handle(Input::Subscribe(id));
        let replied = machine.handle(Input::Frame {
            transport,
            data: br#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":2,"publications":[{"data":{},"offset":1},{"data":{},"offset":2}]}}"#.to_vec(),
        });
        let publications = publication_ids(&replied);
        let before = offset_of(&machine, id);

        machine.handle(Input::EventTaken(publications[0]));
        let after_first = offset_of(&machine, id);
        machine.handle(Input::EventTaken(publications[1]));

        // Just before the first: recovery brings them back if none is
        // taken (Codex on PR #64).
        assert_eq!(before, Some(0));
        assert_eq!(after_first, Some(1));
        assert_eq!(offset_of(&machine, id), Some(2));
        assert_eq!(
            machine.subscriptions.by_id[&id]
                .position
                .as_ref()
                .map(|position| position.epoch.as_str()),
            Some("e")
        );
    }

    // Block 11c: the client-side half of "Connection state invalidation
    // clears the token and invalidates every position", whose scenario
    // passes with Block 12 (owner decision).
    #[test]
    fn a_state_invalidated_disconnect_fetches_a_new_connection_token_first() {
        let mut machine = Machine::with_token_source(Config {
            token: "t".into(),
            ..Config::default()
        });
        let transport = connect(&mut machine, Input::Connect, 1);

        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);

        assert!(
            reconnecting
                .iter()
                .any(|action| matches!(action, Action::FetchToken(_))),
            "{reconnecting:?}"
        );
        assert_eq!(opened(&reconnecting), None, "{reconnecting:?}");
    }

    #[test]
    fn a_state_invalidated_disconnect_fetches_new_subscription_tokens() {
        let (mut machine, transport, id) = connected_with(
            SubscriptionOptions::default()
                .token("t1".into())
                .token_source(),
        );
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);

        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        let resubscribing =
            machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        // The callback gives a new token before the subscribe command.
        assert!(
            resubscribing.iter().any(|action| matches!(
                action,
                Action::FetchSubscriptionToken { subscription, .. } if *subscription == id
            )),
            "{resubscribing:?}"
        );
        assert_eq!(sent(&resubscribing), Vec::<String>::new());
    }

    #[test]
    fn a_state_invalidated_disconnect_drops_a_fixed_subscription_token() {
        let (mut machine, transport, id) =
            connected_with(SubscriptionOptions::default().token("t1".into()));
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);

        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        // No callback gives a new token: the resubscribe carries none.
        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn a_state_invalidated_disconnect_cancels_the_refresh_of_a_fixed_token() {
        let (mut machine, transport, id) =
            connected_with(SubscriptionOptions::default().token("t1".into()));
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#,
        ));

        let closed = machine.handle(Input::TransportClosed {
            transport,
            code: Some(3014),
            reason: "closed by the test".into(),
        });

        // A refresh left scheduled would unsubscribe the subscription,
        // which resubscribes without a token, once it fires.
        for timer in [SubscriptionTimer::Refresh, SubscriptionTimer::RefreshReply] {
            assert!(
                closed.iter().any(|action| matches!(
                    action,
                    Action::CancelSubscriptionTimer(of, t) if *of == id && *t == timer
                )),
                "{timer:?} not cancelled: {closed:?}"
            );
        }
    }

    #[test]
    fn a_state_invalidated_disconnect_keeps_a_sentinel_position() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":5}}"#,
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);

        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        // The server can never match epoch "_": the recovery fails and the
        // subscribed event tells the app (offset 0 is not encoded).
        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"_"}}"#]
        );
    }

    #[test]
    fn a_state_invalidated_disconnect_gives_no_position_to_a_subscription_without_one() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);

        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn a_state_invalidated_disconnect_reloads_the_state_after_the_rejected_recovery() {
        let (mut machine, transport, id) =
            connected_with(SubscriptionOptions::default().state_source());
        let fetch = machine
            .handle(Input::Subscribe(id))
            .iter()
            .find_map(|action| match action {
                Action::FetchSubscriptionState { fetch, .. } => Some(*fetch),
                _ => None,
            })
            .unwrap();
        machine.handle(Input::SubscriptionStateFetched {
            fetch,
            result: Ok(StreamPosition {
                offset: 1,
                epoch: "e".into(),
            }),
        });
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":1}}"#,
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3014);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));
        machine.handle(reply(
            transport,
            r#"{"id":4,"error":{"code":112,"message":"unrecoverable position"}}"#,
        ));
        let retried = machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::Resubscribe,
        ));

        // The resubscribe tries the sentinel first; its rejection (error
        // 112) leads to getState, as any failed recovery.
        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"_","flag":2}}"#]
        );
        assert!(
            retried
                .iter()
                .any(|action| matches!(action, Action::FetchSubscriptionState { .. })),
            "{retried:?}"
        );
    }

    /// The one frame sent among `actions`, as the action carries it.
    fn frame_sent(actions: &[Action]) -> Vec<u8> {
        let mut frames = actions.iter().filter_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        });
        let frame = frames.next().expect("a frame sent");
        assert!(frames.next().is_none(), "{actions:?}");
        frame
    }

    /// Frames sent among `actions`, as text.
    fn sent(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8(frame.clone()).unwrap()),
                _ => None,
            })
            .collect()
    }

    fn reply(transport: TransportId, json: &str) -> Input {
        Input::Frame {
            transport,
            data: json.as_bytes().to_vec(),
        }
    }

    #[test]
    fn unsubscribe_during_a_subscribe_undoes_it_once_answered() {
        let (mut machine, transport, id) = connected();
        let subscribe = sent(&machine.handle(Input::Subscribe(id)));

        let unsubscribed = machine.handle(Input::Unsubscribe(id));
        let after_reply = machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));

        assert_eq!(subscribe, [r#"{"id":2,"subscribe":{"channel":"news"}}"#]);
        // Nothing sent while the subscribe is in flight.
        assert_eq!(sent(&unsubscribed), Vec::<String>::new());
        assert_eq!(
            sent(&after_reply),
            [r#"{"id":3,"unsubscribe":{"channel":"news"}}"#]
        );
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Unsubscribed)
        );
    }

    #[test]
    fn a_refused_subscribe_needs_no_unsubscribe() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(Input::Unsubscribe(id));

        let after_reply = machine.handle(reply(
            transport,
            r#"{"id":2,"error":{"code":403,"message":"no"}}"#,
        ));

        assert_eq!(sent(&after_reply), Vec::<String>::new());
    }

    fn unsubscribe_push(code: u32) -> String {
        format!(r#"{{"push":{{"channel":"news","unsubscribe":{{"code":{code},"reason":"r"}}}}}}"#)
    }

    #[test]
    fn a_server_unsubscribe_below_2500_sends_no_unsubscribe_command() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));

        let actions = machine.handle(reply(transport, &unsubscribe_push(2000)));

        // The server dropped the subscription already.
        assert_eq!(sent(&actions), Vec::<String>::new());
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Unsubscribed)
        );
    }

    /// A subscription to "news" with token "t" and a token callback,
    /// subscribed with a token expiring in 60 s, whose refresh is due: the
    /// token callback is asked, with the returned fetch.
    fn refresh_fetching() -> (Machine, TransportId, SubscriptionId, FetchId) {
        let (mut machine, transport, id) = connected_with(
            SubscriptionOptions::default()
                .token("t".into())
                .token_source(),
        );
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"expires":true,"ttl":60}}"#,
        ));
        let fetch = machine
            .handle(Input::SubscriptionTimerFired(
                id,
                SubscriptionTimer::Refresh,
            ))
            .iter()
            .find_map(|action| match action {
                Action::FetchSubscriptionToken { fetch, .. } => Some(*fetch),
                _ => None,
            })
            .unwrap();
        (machine, transport, id, fetch)
    }

    #[test]
    fn a_resubscribe_advised_during_a_token_refresh_fetch_subscribes_at_once() {
        let (mut machine, transport, _, fetch) = refresh_fetching();

        let actions = machine.handle(reply(transport, &unsubscribe_push(2500)));
        let late = machine.handle(Input::SubscriptionTokenFetched {
            fetch,
            result: Ok("late".into()),
        });

        assert!(actions.contains(&Action::CancelSubscriptionToken(fetch)));
        assert_eq!(
            sent(&actions),
            [r#"{"id":3,"subscribe":{"channel":"news","token":"t"}}"#]
        );
        // The abandoned refresh sends no sub_refresh.
        assert_eq!(sent(&late), Vec::<String>::new());
    }

    #[test]
    fn a_resubscribe_advised_during_a_sub_refresh_ends_it() {
        let (mut machine, transport, id, fetch) = refresh_fetching();
        let refresh = machine.handle(Input::SubscriptionTokenFetched {
            fetch,
            result: Ok("u".into()),
        });

        let actions = machine.handle(reply(transport, &unsubscribe_push(2500)));
        let timed_out = machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::RefreshReply,
        ));

        assert_eq!(
            sent(&refresh),
            [r#"{"id":3,"sub_refresh":{"channel":"news","token":"u"}}"#]
        );
        assert!(actions.contains(&Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::RefreshReply
        )));
        assert_eq!(
            sent(&actions),
            [r#"{"id":4,"subscribe":{"channel":"news","token":"u"}}"#]
        );
        // No timeout error, no retry for the refresh the push ended.
        assert_eq!(timed_out, Vec::new());
    }

    #[test]
    fn a_server_unsubscribe_for_a_subscription_not_subscribed_changes_nothing() {
        for code in [2000, 2500] {
            let (mut machine, transport, id) = connected();
            machine.handle(Input::Subscribe(id));
            machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
            machine.handle(Input::Unsubscribe(id));
            machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

            // E.g. the push of the subscription unsubscribe() just ended.
            let actions = machine.handle(reply(transport, &unsubscribe_push(code)));

            assert_eq!(actions, [], "code {code}");
            assert_eq!(
                machine.subscription_state(id),
                Some(SubscriptionState::Unsubscribed)
            );
        }
    }

    fn reconnect_code(actions: &[Action]) -> Option<u32> {
        actions.iter().find_map(|action| match action {
            Action::Emit(_, crate::ClientEvent::Connecting { code, .. }) => Some(*code),
            _ => None,
        })
    }

    #[test]
    fn an_abandoned_subscribe_keeps_its_reply_deadline() {
        let (mut machine, _, id) = connected();
        machine.handle(Input::Subscribe(id));

        let unsubscribed = machine.handle(Input::Unsubscribe(id));
        let timed_out = machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::SubscribeReply,
        ));

        assert!(
            !unsubscribed.iter().any(|action| matches!(
                action,
                Action::CancelSubscriptionTimer(_, SubscriptionTimer::SubscribeReply)
            )),
            "{unsubscribed:?}"
        );
        assert_eq!(
            reconnect_code(&timed_out),
            Some(codes::connecting::SUBSCRIBE_TIMEOUT)
        );
    }

    #[test]
    fn an_unanswered_or_failed_unsubscribe_reconnects() {
        for answer in [
            None,
            Some(r#"{"id":3,"error":{"code":100,"message":"internal"}}"#),
        ] {
            let (mut machine, transport, id) = connected();
            machine.handle(Input::Subscribe(id));
            machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
            let unsubscribe = machine.handle(Input::Unsubscribe(id));

            let outcome = match answer {
                None => machine.handle(Input::SubscriptionTimerFired(
                    id,
                    SubscriptionTimer::UnsubscribeReply,
                )),
                Some(error) => machine.handle(reply(transport, error)),
            };

            assert!(
                unsubscribe.iter().any(|action| matches!(
                    action,
                    Action::SetSubscriptionTimer(_, SubscriptionTimer::UnsubscribeReply, _)
                )),
                "{unsubscribe:?}"
            );
            assert_eq!(
                reconnect_code(&outcome),
                Some(codes::connecting::UNSUBSCRIBE_ERROR),
                "{answer:?}"
            );
        }
    }

    #[test]
    fn a_resubscribe_waits_for_an_abandoned_subscribe_and_its_undoing() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(Input::Unsubscribe(id));

        let again = machine.handle(Input::Subscribe(id));
        let abandoned_reply = machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let undone = machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

        assert_eq!(sent(&again), Vec::<String>::new());
        assert_eq!(
            sent(&abandoned_reply),
            [r#"{"id":3,"unsubscribe":{"channel":"news"}}"#]
        );
        assert_eq!(
            sent(&undone),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn an_unsubscribe_reply_without_its_result_reconnects() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        machine.handle(Input::Unsubscribe(id));

        let outcome = machine.handle(reply(transport, r#"{"id":3}"#));

        assert_eq!(
            reconnect_code(&outcome),
            Some(codes::connecting::UNSUBSCRIBE_ERROR)
        );
    }

    #[test]
    fn unsubscribing_cancels_a_pending_data_fetch() {
        let mut machine = connected().0;
        let id = machine
            .add_subscription("data".into(), SubscriptionOptions::default().data_source())
            .unwrap();
        let fetch = machine
            .handle(Input::Subscribe(id))
            .iter()
            .find_map(|action| match action {
                Action::FetchSubscriptionData { fetch, .. } => Some(*fetch),
                _ => None,
            })
            .unwrap();

        let unsubscribed = machine.handle(Input::Unsubscribe(id));

        assert!(
            unsubscribed.contains(&Action::CancelSubscriptionData(fetch)),
            "{unsubscribed:?}"
        );
    }

    #[test]
    fn unsubscribing_cancels_a_pending_get_state_and_ignores_its_answer() {
        let mut machine = connected().0;
        let id = machine
            .add_subscription(
                "state".into(),
                SubscriptionOptions::default().state_source(),
            )
            .unwrap();
        let fetch = machine
            .handle(Input::Subscribe(id))
            .iter()
            .find_map(|action| match action {
                Action::FetchSubscriptionState { fetch, .. } => Some(*fetch),
                _ => None,
            })
            .unwrap();

        let unsubscribed = machine.handle(Input::Unsubscribe(id));
        let late = machine.handle(Input::SubscriptionStateFetched {
            fetch,
            result: Ok(StreamPosition {
                offset: 1,
                epoch: "e".into(),
            }),
        });

        assert!(
            unsubscribed.contains(&Action::CancelSubscriptionState(fetch)),
            "{unsubscribed:?}"
        );
        // No subscribe follows an answer no longer wanted.
        assert_eq!(sent(&late), Vec::<String>::new());
    }

    // Owner decision, 2026-11-04: an emulation request answered with HTTP
    // 413 ends its commands at once, keeping the transport; a subscribe
    // would be refused again, so it is not retried.
    #[test]
    fn a_subscribe_rejected_as_too_large_unsubscribes_without_retry() {
        let (mut machine, transport, id) = connected();
        let subscribing = machine.handle(Input::Subscribe(id));

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame_sent(&subscribing),
        });

        assert!(
            without_ids(&rejected).contains(&Action::EmitSubscription(
                EventId(0),
                id,
                SubscriptionEvent::Unsubscribed {
                    code: codes::unsubscribed::MESSAGE_SIZE_LIMIT,
                    reason: "message size limit exceeded".into(),
                }
            )),
            "{rejected:?}"
        );
        assert!(
            !rejected.iter().any(|action| matches!(
                action,
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, _)
                    | Action::CloseTransport(_)
            )),
            "{rejected:?}"
        );
        // The server never got the subscribe: nothing to unsubscribe.
        assert_eq!(sent(&rejected), Vec::<String>::new());
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn an_unsubscribe_rejected_as_too_large_reconnects_as_any_unsubscribe_error() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let unsubscribing = machine.handle(Input::Unsubscribe(id));

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame_sent(&unsubscribing),
        });

        assert!(
            rejected.iter().any(|action| matches!(
                action,
                Action::Emit(_, crate::ClientEvent::Connecting { code, .. })
                    if *code == codes::connecting::UNSUBSCRIBE_ERROR
            )),
            "{rejected:?}"
        );
    }

    #[test]
    fn a_sub_refresh_rejected_as_too_large_unsubscribes() {
        let (mut machine, transport, id, fetch) = refresh_fetching();
        let refreshing = machine.handle(Input::SubscriptionTokenFetched {
            fetch,
            result: Ok("u".into()),
        });

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame_sent(&refreshing),
        });

        assert!(
            without_ids(&rejected).contains(&Action::EmitSubscription(
                EventId(0),
                id,
                SubscriptionEvent::Unsubscribed {
                    code: codes::unsubscribed::MESSAGE_SIZE_LIMIT,
                    reason: "message size limit exceeded".into(),
                }
            )),
            "{rejected:?}"
        );
        // The server holds the subscription: it is ended there too.
        assert_eq!(
            sent(&rejected),
            [r#"{"id":4,"unsubscribe":{"channel":"news"}}"#]
        );
    }

    // A request the server refuses as too large (HTTP 413) concerns its
    // one command only, so ending that command is right (owner condition on
    // the 413 rule, 2026-11-04).
    #[test]
    fn every_emulation_request_carries_one_command() {
        let mut machine = Machine::new(Config::default())
            .with_transports(vec![TransportKind::HttpStream])
            .unwrap();
        for channel in ["a", "b", "c"] {
            let id = machine
                .add_subscription(channel.into(), SubscriptionOptions::default())
                .unwrap();
            machine.handle(Input::Subscribe(id));
        }
        let transport = opened(&machine.handle(Input::Connect)).expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));

        let connected = machine.handle(reply(
            transport,
            r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
        ));

        let requests: Vec<_> = connected
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(frame),
                _ => None,
            })
            .collect();
        assert_eq!(requests.len(), 3, "{connected:?}");
        for request in requests {
            let frame = crate::json::emulation_frame(request).expect("an emulation request");
            assert_eq!(
                crate::codec::Codec::Json.command_ids(&frame).len(),
                1,
                "{}",
                String::from_utf8_lossy(&frame)
            );
        }
    }

    // Over http_stream and sse the commands travel inside an emulation
    // request, as the frame the transport reports rejected.
    #[test]
    fn a_subscribe_rejected_over_an_emulated_transport_fails_at_once() {
        let mut machine = Machine::new(Config::default())
            .with_transports(vec![TransportKind::HttpStream])
            .unwrap();
        let transport = opened(&machine.handle(Input::Connect)).expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));
        machine.handle(reply(
            transport,
            r#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#,
        ));
        let id = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        let subscribing = machine.handle(Input::Subscribe(id));
        let Some(Action::Send { frame, .. }) = subscribing
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{subscribing:?}");
        };
        assert!(frame.starts_with(br#"{"session":"s""#), "{frame:?}");

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame.clone(),
        });

        assert!(
            rejected.iter().any(|action| matches!(
                action,
                Action::EmitSubscription(_, of, SubscriptionEvent::Unsubscribed { code, .. })
                    if *of == id && *code == codes::unsubscribed::MESSAGE_SIZE_LIMIT
            )),
            "{rejected:?}"
        );
    }

    // Owner decision, 2026-11-04 (centrifuge-js `_sendSubscribeCommands`).
    #[test]
    fn a_reconnect_keeps_a_pending_resubscribe_backoff() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(
            transport,
            r#"{"id":2,"error":{"code":100,"message":"busy","temporary":true}}"#,
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3000);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let reconnected = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));
        let retried = machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::Resubscribe,
        ));

        assert_eq!(sent(&reconnected), Vec::<String>::new());
        assert_eq!(
            sent(&retried),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn a_temporary_subscribe_error_is_reported_then_retried() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));

        let actions = machine.handle(reply(
            transport,
            r#"{"id":2,"error":{"code":100,"message":"busy","temporary":true}}"#,
        ));

        assert!(
            without_ids(&actions).contains(&Action::EmitSubscription(
                EventId(0),
                id,
                SubscriptionEvent::Error(ClientError {
                    kind: ErrorKind::Subscribe,
                    code: 100,
                    message: "busy".into(),
                    transport: None,
                })
            )),
            "{actions:?}"
        );
        assert!(
            actions.iter().any(|action| matches!(
                action,
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, _)
            )),
            "{actions:?}"
        );
    }

    #[test]
    fn the_last_data_option_decides() {
        let fixed = SubscriptionOptions::default()
            .data_source()
            .data(b"{}".to_vec());
        let fetched = SubscriptionOptions::default()
            .data(b"{}".to_vec())
            .data_source();

        assert_eq!(
            (fixed.data.as_deref(), fixed.data_source),
            (Some(&b"{}"[..]), false)
        );
        assert_eq!((fetched.data, fetched.data_source), (None, true));
    }

    #[test]
    fn a_fresh_subscribe_retries_from_the_minimum_delay() {
        let (mut machine, transport, id) = connected();
        let busy = r#""error":{"code":100,"message":"busy","temporary":true}"#;
        machine.handle(Input::Subscribe(id));
        // Several failures grow the backoff.
        let mut command = 2;
        for _ in 0..6 {
            machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));
            machine.handle(Input::SubscriptionTimerFired(
                id,
                SubscriptionTimer::Resubscribe,
            ));
            command += 1;
        }
        // The subscribe in flight is abandoned; the fresh one waits for it
        // to settle, then goes out as the next command.
        machine.handle(Input::Unsubscribe(id));
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));
        command += 1;

        let retry = machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));

        let delay = retry
            .iter()
            .find_map(|action| match action {
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, delay) => {
                    Some(*delay)
                }
                _ => None,
            })
            .expect("a retry");
        assert!(
            delay <= SubscriptionOptions::default().min_resubscribe_delay * 2,
            "{delay:?}"
        );
    }

    #[test]
    fn a_subscribe_waits_for_the_pending_unsubscribe_of_its_channel() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        machine.handle(Input::RemoveSubscription(id));
        let again = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();

        let waiting = machine.handle(Input::Subscribe(again));
        let settled = machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

        assert_eq!(sent(&waiting), Vec::<String>::new());
        assert_eq!(
            sent(&settled),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn unsubscribe_right_after_connect_then_subscribe_subscribes() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(Input::Disconnect);
        let transport_again = opened(&machine.handle(Input::Connect)).unwrap();

        let unsubscribed = machine.handle(Input::Unsubscribe(id));
        machine.handle(Input::TransportOpened(transport_again));
        let reconnected = machine.handle(reply(
            transport_again,
            r#"{"id":3,"connect":{"client":"c"}}"#,
        ));
        let again = machine.handle(Input::Subscribe(id));

        assert_ne!(transport_again, transport);
        assert!(
            without_ids(&unsubscribed).contains(&Action::EmitSubscription(
                EventId(0),
                id,
                SubscriptionEvent::Unsubscribed {
                    code: codes::unsubscribed::UNSUBSCRIBE_CALLED,
                    reason: "unsubscribe called".into(),
                }
            )),
            "{unsubscribed:?}"
        );
        assert_eq!(sent(&reconnected), Vec::<String>::new());
        // The subscribe the disconnect abandoned holds nothing back.
        assert_eq!(sent(&again), [r#"{"id":4,"subscribe":{"channel":"news"}}"#]);
    }

    #[test]
    fn unsubscribe_between_the_connect_command_and_its_reply_then_subscribe_subscribes() {
        let mut machine = Machine::new(Config::default());
        let id = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(id));
        let transport = opened(&machine.handle(Input::Connect)).unwrap();
        let connect = machine.handle(Input::TransportOpened(transport));

        machine.handle(Input::Unsubscribe(id));
        let connected = machine.handle(reply(transport, r#"{"id":1,"connect":{"client":"c"}}"#));
        let again = machine.handle(Input::Subscribe(id));

        assert_eq!(sent(&connect).len(), 1, "{connect:?}");
        assert_eq!(sent(&connected), Vec::<String>::new());
        assert_eq!(sent(&again), [r#"{"id":2,"subscribe":{"channel":"news"}}"#]);
    }

    #[test]
    fn a_second_unsubscribe_changes_nothing() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        let first = machine.handle(Input::Unsubscribe(id));

        let second = machine.handle(Input::Unsubscribe(id));
        let again = machine.handle(Input::Subscribe(id));
        let settled = machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

        assert_eq!(
            sent(&first),
            [r#"{"id":3,"unsubscribe":{"channel":"news"}}"#]
        );
        assert_eq!(second, []);
        // The subscribe still waits for the first unsubscribe.
        assert_eq!(sent(&again), Vec::<String>::new());
        assert_eq!(
            sent(&settled),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn removing_a_removed_subscription_keeps_the_newer_one_of_its_channel() {
        let (mut machine, transport, old) = connected();
        machine.handle(Input::RemoveSubscription(old));
        let next = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(next));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));

        let removed_again = machine.handle(Input::RemoveSubscription(old));

        assert_eq!(removed_again, []);
        assert_eq!(machine.find_subscription("news"), Some(next));
        assert_eq!(
            machine.subscription_state(next),
            Some(SubscriptionState::Subscribed)
        );
    }

    #[test]
    fn subscribe_on_a_removed_subscription_does_nothing() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        machine.handle(Input::RemoveSubscription(id));
        machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

        let subscribed = machine.handle(Input::Subscribe(id));

        assert_eq!(subscribed, []);
        assert_eq!(machine.subscription_state(id), None);
    }

    #[test]
    fn subscriptions_removed_in_a_row_wait_for_one_unsubscribe() {
        let (mut machine, transport, first) = connected();
        machine.handle(Input::Subscribe(first));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));
        machine.handle(Input::RemoveSubscription(first));
        // E.g. a component mounted and unmounted again while the
        // unsubscribe is pending.
        let second = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(second));
        let second_removed = machine.handle(Input::RemoveSubscription(second));
        let third = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();

        let waiting = machine.handle(Input::Subscribe(third));
        let settled = machine.handle(reply(transport, r#"{"id":3,"unsubscribe":{}}"#));

        // The second never reached the server: nothing to undo.
        assert_eq!(sent(&second_removed), Vec::<String>::new());
        assert_eq!(sent(&waiting), Vec::<String>::new());
        assert_eq!(
            sent(&settled),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn an_unsubscribe_push_without_a_channel_is_ignored() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));
        machine.handle(reply(transport, r#"{"id":2,"subscribe":{}}"#));

        let actions = machine.handle(reply(
            transport,
            r#"{"push":{"unsubscribe":{"code":2000,"reason":"r"}}}"#,
        ));

        assert_eq!(actions, []);
        assert_eq!(machine.state(), State::Connected);
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Subscribed)
        );
    }

    fn resubscribe_delay(actions: &[Action]) -> Option<Duration> {
        actions.iter().find_map(|action| match action {
            Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, delay) => Some(*delay),
            _ => None,
        })
    }

    #[test]
    fn a_subscribe_in_flight_when_the_transport_closes_is_sent_again_without_backoff() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));

        let closed = machine.handle(Input::TransportClosed {
            transport,
            code: Some(3000),
            reason: "closed by the test".into(),
        });
        let transport = opened(&machine.handle(Input::TimerFired(Timer::Reconnect)))
            .expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        let reconnected = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        assert_eq!(resubscribe_delay(&closed), None, "{closed:?}");
        assert_eq!(
            sent(&reconnected),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    #[test]
    fn subscribe_while_subscribing_keeps_the_resubscribe_backoff() {
        let mut machine = Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(7));
        let transport = connect(&mut machine, Input::Connect, 1);
        let id = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        let busy = r#""error":{"code":100,"message":"busy","temporary":true}"#;
        machine.handle(Input::Subscribe(id));
        // Several failures grow the backoff.
        let mut command = 2;
        for _ in 0..6 {
            machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));
            machine.handle(Input::SubscriptionTimerFired(
                id,
                SubscriptionTimer::Resubscribe,
            ));
            command += 1;
        }
        machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));
        command += 1;

        let again = machine.handle(Input::Subscribe(id));
        machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::Resubscribe,
        ));
        let retry = machine.handle(reply(transport, &format!(r#"{{"id":{command},{busy}}}"#)));

        assert_eq!(again, []);
        let delay = resubscribe_delay(&retry).expect("a retry");
        // A fresh backoff would be at most twice the minimum.
        assert!(
            delay > SubscriptionOptions::default().min_resubscribe_delay * 2,
            "{delay:?}"
        );
    }

    fn subscription_token_fetch(actions: &[Action]) -> FetchId {
        actions
            .iter()
            .find_map(|action| match action {
                Action::FetchSubscriptionToken { fetch, .. } => Some(*fetch),
                _ => None,
            })
            .expect("a subscription token fetch")
    }

    #[test]
    fn the_token_of_an_outdated_subscribe_sends_nothing() {
        let (mut machine, _, id) = connected_with(SubscriptionOptions::default().token_source());
        let outdated = subscription_token_fetch(&machine.handle(Input::Subscribe(id)));
        // A new subscribe while the token loads: a new fetch.
        machine.handle(Input::Unsubscribe(id));
        let current = subscription_token_fetch(&machine.handle(Input::Subscribe(id)));

        let late = machine.handle(Input::SubscriptionTokenFetched {
            fetch: outdated,
            result: Ok("old".into()),
        });
        let answered = machine.handle(Input::SubscriptionTokenFetched {
            fetch: current,
            result: Ok("new".into()),
        });

        assert_eq!(sent(&late), Vec::<String>::new());
        assert_eq!(
            sent(&answered),
            [r#"{"id":2,"subscribe":{"channel":"news","token":"new"}}"#]
        );
    }

    #[test]
    fn a_refresh_failing_after_a_reconnect_leaves_the_new_refresh_alone() {
        let (mut machine, transport, id, outdated) = refresh_fetching();
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3000);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));
        machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));
        machine.handle(reply(
            transport,
            r#"{"id":4,"subscribe":{"expires":true,"ttl":60}}"#,
        ));
        let current = subscription_token_fetch(&machine.handle(Input::SubscriptionTimerFired(
            id,
            SubscriptionTimer::Refresh,
        )));

        let failed = machine.handle(Input::SubscriptionTokenFetched {
            fetch: outdated,
            result: Err(TokenError::Failed("token unavailable".into())),
        });
        let refreshed = machine.handle(Input::SubscriptionTokenFetched {
            fetch: current,
            result: Ok("u".into()),
        });

        // No error, no retry: the refresh in progress is the new one.
        assert_eq!(failed, []);
        assert_eq!(
            sent(&refreshed),
            [r#"{"id":5,"sub_refresh":{"channel":"news","token":"u"}}"#]
        );
    }

    fn state_fetch(actions: &[Action]) -> Option<FetchId> {
        actions.iter().find_map(|action| match action {
            Action::FetchSubscriptionState { fetch, .. } => Some(*fetch),
            _ => None,
        })
    }

    #[test]
    fn get_state_is_not_called_after_a_successful_recovery() {
        let (mut machine, transport, id) =
            connected_with(SubscriptionOptions::default().state_source());
        let fetch = state_fetch(&machine.handle(Input::Subscribe(id))).unwrap();
        machine.handle(Input::SubscriptionStateFetched {
            fetch,
            result: Ok(StreamPosition {
                offset: 1,
                epoch: "e".into(),
            }),
        });
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"recoverable":true,"epoch":"e","offset":1}}"#,
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3000);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));
        let recovered = machine.handle(reply(
            transport,
            r#"{"id":4,"subscribe":{"recoverable":true,"was_recovering":true,"recovered":true,"epoch":"e","offset":1}}"#,
        ));

        assert_eq!(
            sent(&resubscribed),
            [
                r#"{"id":4,"subscribe":{"channel":"news","recover":true,"epoch":"e","offset":1,"flag":2}}"#
            ]
        );
        assert_eq!(state_fetch(&recovered), None, "{recovered:?}");
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Subscribed)
        );
    }

    #[test]
    fn a_get_state_that_always_fails_keeps_retrying_without_unsubscribing() {
        let (mut machine, _, id) = connected_with(SubscriptionOptions::default().state_source());
        let mut fetch = state_fetch(&machine.handle(Input::Subscribe(id))).unwrap();

        for attempt in 0..3 {
            let failed = machine.handle(Input::SubscriptionStateFetched {
                fetch,
                result: Err(StateError("always fails".into())),
            });
            let retried = machine.handle(Input::SubscriptionTimerFired(
                id,
                SubscriptionTimer::Resubscribe,
            ));

            assert!(
                resubscribe_delay(&failed).is_some(),
                "{attempt}: {failed:?}"
            );
            fetch = state_fetch(&retried).expect("getState called again");
        }
        assert_eq!(
            machine.subscription_state(id),
            Some(SubscriptionState::Subscribing)
        );
    }

    #[test]
    fn a_subscribe_reply_without_recoverable_stops_recovery() {
        let since = StreamPosition {
            offset: 1,
            epoch: "e".into(),
        };
        let (mut machine, transport, id) =
            connected_with(SubscriptionOptions::default().since(since));
        let first = machine.handle(Input::Subscribe(id));
        // Positioned, not recoverable: nothing to recover from next time.
        machine.handle(reply(
            transport,
            r#"{"id":2,"subscribe":{"positioned":true,"epoch":"e","offset":1}}"#,
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3000);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let resubscribed = machine.handle(reply(transport, r#"{"id":3,"connect":{"client":"c"}}"#));

        assert_eq!(
            sent(&first),
            [r#"{"id":2,"subscribe":{"channel":"news","recover":true,"epoch":"e","offset":1}}"#]
        );
        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
    }

    /// A Protobuf frame holding `reply`, received over `transport`.
    fn protobuf_reply(transport: TransportId, reply: &crate::protocol::Reply) -> Input {
        use prost::Message as _;

        Input::Frame {
            transport,
            data: reply.encode_length_delimited_to_vec(),
        }
    }

    #[test]
    fn a_protobuf_publication_without_an_offset_keeps_the_stored_position() {
        use crate::protocol as pb;
        use prost::Message as _;

        let mut machine = Machine::new(Config {
            protocol: crate::Protocol::Protobuf,
            ..Config::default()
        });
        let connected = pb::Reply {
            connect: Some(pb::ConnectResult {
                client: "c".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let transport = opened(&machine.handle(Input::Connect)).unwrap();
        machine.handle(Input::TransportOpened(transport));
        machine.handle(protobuf_reply(
            transport,
            &pb::Reply {
                id: 1,
                ..connected.clone()
            },
        ));
        let id = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        machine.handle(Input::Subscribe(id));
        machine.handle(protobuf_reply(
            transport,
            &pb::Reply {
                id: 2,
                subscribe: Some(pb::SubscribeResult {
                    recoverable: true,
                    epoch: "e".into(),
                    offset: 5,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ));
        // Published without history: Protobuf decodes the missing offset
        // as 0.
        machine.handle(protobuf_reply(
            transport,
            &pb::Reply {
                push: Some(pb::Push {
                    channel: "news".into(),
                    r#pub: Some(pb::Publication {
                        data: vec![1],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ));
        let reconnecting = closed_then_reconnecting(&mut machine, transport, 3000);
        let transport = opened(&reconnecting).expect("a reconnect attempt");
        machine.handle(Input::TransportOpened(transport));

        let resubscribed =
            machine.handle(protobuf_reply(transport, &pb::Reply { id: 3, ..connected }));

        let subscribe = resubscribed
            .iter()
            .find_map(|action| match action {
                Action::Send { frame, .. } => {
                    pb::Command::decode_length_delimited(frame.as_slice())
                        .unwrap()
                        .subscribe
                }
                _ => None,
            })
            .expect("a subscribe command");
        assert_eq!(
            (
                subscribe.recover,
                subscribe.epoch.as_str(),
                subscribe.offset
            ),
            (true, "e", 5)
        );
    }
}
