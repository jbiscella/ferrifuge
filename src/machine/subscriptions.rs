//! Client-side subscriptions of a [`Machine`] (Block 9): the registry, each
//! subscription's state and commands in flight, and how the machine reacts
//! to them. Behaviour follows centrifuge-js `subscription.ts`.

mod state_flow;
mod token_flow;

use std::collections::{BTreeMap, HashMap};

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

impl Subscription {
    /// Drops the stream position: the next subscribe recovers nothing, or
    /// loads the state through getState first.
    fn forget_position(&mut self) {
        self.position = None;
        self.recover = false;
    }

    /// Moves the stream position to a publication's, as received live or
    /// recovered (centrifuge-js `_handlePublication`); a changed epoch
    /// comes with it (`client.proto`, `Publication.epoch`).
    fn advance(&mut self, publication: &Publication) {
        if publication.offset.is_none() && publication.epoch.is_none() {
            return;
        }
        let position = self.position.get_or_insert_default();
        if let Some(offset) = publication.offset {
            position.offset = offset;
        }
        if let Some(epoch) = &publication.epoch {
            epoch.clone_into(&mut position.epoch);
        }
    }

    /// Whether it has subscription token timers or fetches to clean up.
    fn uses_token(&self) -> bool {
        self.tokens.has_source() || self.tokens.has_token()
    }
}

impl Subscriptions {
    /// Whether `id` is the id of a subscribe or unsubscribe command.
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
            &[Command {
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
            }],
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
            &[Command {
                id: command_id,
                method: Method::Unsubscribe(UnsubscribeRequest { channel }),
            }],
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
                if result.recoverable {
                    sub.position = Some(StreamPosition {
                        offset: result.offset,
                        epoch: result.epoch,
                    });
                }
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
                // move the position.
                for publication in result.publications {
                    if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
                        sub.advance(&publication);
                    }
                    self.emit_subscription(id, SubscriptionEvent::Publication(publication));
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
    }

    /// "State invalidated" (disconnect code 3014): every subscription drops
    /// its stream position and its token, a fixed one too, after ending a
    /// fetch of it in flight; a token callback gives the resubscribe a new
    /// token, without one it carries none (Block 11, owner decision;
    /// centrifuge-js `_invalidateState` keeps a fixed token).
    pub(super) fn invalidate_subscriptions(&mut self) {
        for sub in self.subscriptions.by_id.values_mut() {
            sub.forget_position();
            if let Some(fetch) = sub.tokens.abandon() {
                self.actions.push(Action::CancelSubscriptionToken(fetch));
            }
            sub.tokens.invalidate();
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
            if let Some(sub) = self.subscriptions.by_id.get_mut(&id) {
                sub.advance(&publication);
            }
            self.emit_subscription(id, SubscriptionEvent::Publication(publication));
        }
    }

    /// The connection is established: subscribing subscriptions subscribe.
    pub(super) fn subscriptions_connected(&mut self) {
        let subscribing: Vec<_> = self
            .subscriptions
            .by_id
            .iter()
            .filter(|(_, sub)| sub.state == SubscriptionState::Subscribing)
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
        self.actions.push(Action::EmitSubscription(id, event));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::machine::{Input, Timer, TransportId};

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

    // Block 11c: the client-side half of "Connection state invalidation
    // clears the token and every position", whose scenario passes with
    // Block 12 (owner decision).
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
    fn a_state_invalidated_disconnect_drops_the_stream_positions() {
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

        assert_eq!(
            sent(&resubscribed),
            [r#"{"id":4,"subscribe":{"channel":"news"}}"#]
        );
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
            Action::Emit(crate::ClientEvent::Connecting { code, .. }) => Some(*code),
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

    #[test]
    fn a_temporary_subscribe_error_is_reported_then_retried() {
        let (mut machine, transport, id) = connected();
        machine.handle(Input::Subscribe(id));

        let actions = machine.handle(reply(
            transport,
            r#"{"id":2,"error":{"code":100,"message":"busy","temporary":true}}"#,
        ));

        assert!(
            actions.contains(&Action::EmitSubscription(
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
}
