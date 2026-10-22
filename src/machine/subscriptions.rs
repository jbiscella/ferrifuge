//! Client-side subscriptions of a [`Machine`] (Block 9): the registry, each
//! subscription's state and commands in flight, and how the machine reacts
//! to them. Behaviour follows centrifuge-js `subscription.ts`.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use super::{Action, FetchId, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer};
use crate::backoff::Backoff;
use crate::wire::{
    Command, Error, Method, Reply, ReplyResult, SubscribeRequest, UnsubscribeRequest,
};
use crate::{
    ClientError, DataError, DuplicateSubscription, ErrorKind, Publication, State, Subscribed,
    SubscriptionEvent, SubscriptionState, codes,
};

/// Resubscribe backoff bounds (centrifuge-js `minResubscribeDelay`,
/// `maxResubscribeDelay` defaults).
const MIN_RESUBSCRIBE_DELAY: Duration = Duration::from_millis(500);
const MAX_RESUBSCRIBE_DELAY: Duration = Duration::from_secs(20);

#[derive(Debug, Default)]
pub(super) struct Subscriptions {
    by_id: BTreeMap<SubscriptionId, Subscription>,
    next_id: u64,
    next_fetch: u64,
    /// Subscribe commands awaiting their reply: whose, and for which
    /// channel, as the subscription may be gone when the reply arrives.
    subscribes: HashMap<u32, (SubscriptionId, String)>,
    /// Unsubscribe commands awaiting their reply, with their channel.
    unsubscribes: HashMap<u32, String>,
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
    /// Waiting for an unsubscribe of its channel to be settled before
    /// subscribing, so the server cannot apply them out of order.
    waiting: bool,
    backoff: Backoff,
}

impl Subscriptions {
    /// Whether `id` is the id of a subscribe or unsubscribe command.
    pub(super) fn owns_command(&self, id: u32) -> bool {
        self.subscribes.contains_key(&id) || self.unsubscribes.contains_key(&id)
    }

    fn unsubscribing(&self, channel: &str) -> bool {
        self.unsubscribes.values().any(|pending| pending == channel)
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
    ) -> Result<SubscriptionId, DuplicateSubscription> {
        if self.find_subscription(&channel).is_some() {
            return Err(DuplicateSubscription { channel });
        }
        let subscriptions = &mut self.subscriptions;
        let id = SubscriptionId(subscriptions.next_id);
        subscriptions.next_id += 1;
        subscriptions.by_id.insert(
            id,
            Subscription {
                channel,
                options,
                state: SubscriptionState::Unsubscribed,
                subscribe_command: None,
                fetch: None,
                waiting: false,
                backoff: Backoff::new(MIN_RESUBSCRIBE_DELAY, MAX_RESUBSCRIBE_DELAY),
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
        self.subscriptions
            .by_id
            .iter()
            .find_map(|(id, sub)| (sub.channel == channel).then_some(*id))
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
    /// connection: after its data, and after a pending unsubscribe of its
    /// channel is settled.
    fn start_subscribe(&mut self, id: SubscriptionId) {
        let subscriptions = &mut self.subscriptions;
        let unsubscribing = |channel: &str| subscriptions.unsubscribing(channel);
        let Some(sub) = subscriptions.by_id.get(&id) else {
            return;
        };
        if sub.state != SubscriptionState::Subscribing
            || sub.subscribe_command.is_some()
            || sub.fetch.is_some()
        {
            return;
        }
        if unsubscribing(&sub.channel) {
            self.subscriptions
                .by_id
                .get_mut(&id)
                .expect("checked above")
                .waiting = true;
            return;
        }
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
        self.subscriptions
            .subscribes
            .insert(command_id, (id, channel.clone()));
        self.send(
            transport,
            &[Command {
                id: command_id,
                method: Method::Subscribe(SubscribeRequest { channel, data }),
            }],
        );
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
            self.config.timeout,
        ));
    }

    fn send_unsubscribe(&mut self, channel: String) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let command_id = self.command_id();
        self.subscriptions
            .unsubscribes
            .insert(command_id, channel.clone());
        self.send(
            transport,
            &[Command {
                id: command_id,
                method: Method::Unsubscribe(UnsubscribeRequest { channel }),
            }],
        );
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
            SubscriptionTimer::SubscribeReply => {
                let pending = self
                    .subscriptions
                    .by_id
                    .get(&id)
                    .is_some_and(|sub| sub.subscribe_command.is_some());
                if pending {
                    // The server-side state is unknown: start over
                    // (centrifuge-js `connectingCodes.subscribeTimeout`).
                    self.disconnect(
                        codes::connecting::SUBSCRIBE_TIMEOUT,
                        "subscribe timeout",
                        true,
                    );
                }
            }
        }
    }

    /// Handles the reply to a subscribe or unsubscribe command
    /// ([`Subscriptions::owns_command`]).
    pub(super) fn subscription_command_reply(&mut self, reply: Reply) {
        if let Some(channel) = self.subscriptions.unsubscribes.remove(&reply.id) {
            // Subscriptions to the channel that waited for it can go on.
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
            return;
        }
        let Some((id, channel)) = self.subscriptions.subscribes.remove(&reply.id) else {
            return;
        };
        let current = self
            .subscriptions
            .by_id
            .get(&id)
            .is_some_and(|sub| sub.subscribe_command == Some(reply.id));
        if !current {
            // Unsubscribed or removed while in flight: undo a subscription
            // the server made, so none outlives the handle.
            if reply.error.is_none() {
                self.send_unsubscribe(channel);
            }
            return;
        }
        self.actions.push(Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::SubscribeReply,
        ));
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
                self.emit_subscription(
                    id,
                    SubscriptionEvent::Subscribed(Subscribed { data: result.data }),
                );
                for publication in result.publications {
                    self.emit_subscription(id, SubscriptionEvent::Publication(publication));
                }
            }
            // A reply to subscribe without its result breaks the protocol.
            (None, _) => self.reconnect_after_transport_closed(),
        }
    }

    /// centrifuge-js `_subscribeError`: temporary errors are retried.
    fn subscribe_error(&mut self, id: SubscriptionId, error: &Error) {
        if error.code < 100 || error.temporary {
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
        self.subscriptions.by_id.remove(&id);
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
        // A subscribe in flight stays in `subscribes`: its reply undoes it.
        sub.subscribe_command = None;
        sub.fetch = None;
        sub.waiting = false;
        sub.state = SubscriptionState::Unsubscribed;
        let channel = sub.channel.clone();
        for timer in [
            SubscriptionTimer::Resubscribe,
            SubscriptionTimer::SubscribeReply,
        ] {
            self.actions
                .push(Action::CancelSubscriptionTimer(id, timer));
        }
        if send && was_subscribed {
            self.send_unsubscribe(channel);
        }
        self.emit_subscription(
            id,
            SubscriptionEvent::Unsubscribed {
                code,
                reason: reason.to_owned(),
            },
        );
    }

    /// A publication pushed for `channel`.
    pub(super) fn publication(&mut self, channel: &str, publication: Publication) {
        let subscribed = self.subscriptions.by_id.iter().find_map(|(id, sub)| {
            (sub.channel == channel && sub.state == SubscriptionState::Subscribed).then_some(*id)
        });
        if let Some(id) = subscribed {
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
        self.subscriptions.subscribes.clear();
        self.subscriptions.unsubscribes.clear();
        let mut resubscribing = Vec::new();
        for (id, sub) in &mut self.subscriptions.by_id {
            sub.subscribe_command = None;
            sub.fetch = None;
            sub.waiting = false;
            self.actions.push(Action::CancelSubscriptionTimer(
                *id,
                SubscriptionTimer::SubscribeReply,
            ));
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
    use crate::machine::{Input, TransportId};

    /// A machine connected over a transport, with a subscription to "news".
    fn connected() -> (Machine, TransportId, SubscriptionId) {
        let mut machine = Machine::new(Config::default());
        let transport = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .unwrap();
        machine.handle(Input::TransportOpened(transport));
        machine.handle(Input::Frame {
            transport,
            data: br#"{"id":1,"connect":{"client":"c"}}"#.to_vec(),
        });
        let id = machine
            .add_subscription("news".into(), SubscriptionOptions::default())
            .unwrap();
        (machine, transport, id)
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
