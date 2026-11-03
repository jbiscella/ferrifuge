//! Client-side subscriptions of a [`Machine`] (Block 9): the registry, each
//! subscription's state and commands in flight, and how the machine reacts
//! to them. Behaviour follows centrifuge-js `subscription.ts`.

use std::collections::{BTreeMap, HashMap};

use super::{Action, FetchId, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer};
use crate::backoff::Backoff;
use crate::wire::{
    Command, Error, Method, Reply, ReplyResult, SubscribeRequest, UnsubscribeRequest,
};
use crate::{
    ClientError, DataError, ErrorKind, Publication, State, Subscribed, SubscriptionError,
    SubscriptionEvent, SubscriptionState, codes,
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
    /// Waiting for a command for its channel to be settled before
    /// subscribing, so the server cannot apply them out of order.
    waiting: bool,
    backoff: Backoff,
}

impl Subscriptions {
    /// Whether `id` is the id of a subscribe or unsubscribe command.
    pub(super) fn owns_command(&self, id: u32) -> bool {
        self.subscribes.contains_key(&id) || self.unsubscribes.contains_key(&id)
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
        subscriptions.by_id.insert(
            id,
            Subscription {
                channel,
                options,
                state: SubscriptionState::Unsubscribed,
                subscribe_command: None,
                fetch: None,
                waiting: false,
                backoff,
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
                method: Method::Subscribe(SubscribeRequest {
                    channel,
                    data,
                    ..SubscribeRequest::default()
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
        self.emit_subscription(
            id,
            SubscriptionEvent::Subscribing {
                code,
                reason: reason.to_owned(),
            },
        );
        self.start_subscribe(id);
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
        let mut resubscribing = Vec::new();
        for (id, sub) in &mut self.subscriptions.by_id {
            sub.subscribe_command = None;
            if let Some(fetch) = sub.fetch.take() {
                self.actions.push(Action::CancelSubscriptionData(fetch));
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
