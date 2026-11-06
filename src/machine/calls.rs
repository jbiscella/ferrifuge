//! Block 13: calls the application awaits. A call made while not
//! connected waits for the connection, then for its reply, each phase
//! bounded by `Config::timeout`. A reply settles its call when it is read,
//! ahead of the messages Block 15 holds. Losing the transport fails the
//! calls sent on it with "connection closed"; entering `disconnected` fails
//! every call with "disconnected". A subscription-level call first waits
//! for its subscription to be subscribed, and fails with "subscription
//! unsubscribed" when it is not or stops being (centrifuge-js
//! `subscription.ts` `_methodCall`).

use std::collections::{BTreeMap, HashMap};

use super::{Action, CallId, CallTimer, Machine, SubscriptionId};
use crate::wire::{
    ChannelRequest, Command, Error, HistoryRequest, Method, PublishRequest, Reply, ReplyResult,
    RpcRequest,
};
use crate::{
    CallError, CallReply, CallRequest, HistoryResult, PresenceResult, PresenceStatsResult, State,
    SubscriptionState, codes,
};

#[derive(Debug)]
enum Phase {
    /// Added, not yet started by [`super::Input::Call`].
    Added,
    /// Waiting for its subscription to be subscribed.
    Subscribing,
    /// Waiting for the connection.
    Queued,
    /// Sent as the command with this id, waiting for its reply.
    Sent(u32),
}

#[derive(Debug)]
struct Call {
    request: CallRequest,
    /// The subscription of a subscription-level call.
    subscription: Option<SubscriptionId>,
    phase: Phase,
}

/// The calls not yet settled, in the order they were added.
#[derive(Debug, Default)]
pub(super) struct Calls {
    next_id: u64,
    by_id: BTreeMap<CallId, Call>,
    /// The call each command sent and not yet answered belongs to.
    by_command: HashMap<u32, CallId>,
}

impl Phase {
    /// The timer of this phase.
    fn timer(&self) -> Option<CallTimer> {
        match self {
            Self::Added => None,
            Self::Subscribing => Some(CallTimer::Subscription),
            Self::Queued => Some(CallTimer::Connection),
            Self::Sent(_) => Some(CallTimer::Reply),
        }
    }
}

impl Calls {
    /// Whether the command `id` is a call's.
    pub(super) fn owns_command(&self, id: u32) -> bool {
        self.by_command.contains_key(&id)
    }
}

impl Machine {
    /// Adds a call, which [`super::Input::Call`] then starts; the machine
    /// settles it with [`Action::CallSettled`], unless the owner abandons
    /// it with [`super::Input::CallAbandoned`].
    pub fn add_call(&mut self, request: CallRequest) -> CallId {
        self.insert_call(request, None)
    }

    /// Adds a call of `subscription`, on its channel, which replaces the
    /// request's: [`super::Input::Call`] starts it once the subscription is
    /// subscribed, and it fails with code
    /// [`codes::error::SUBSCRIPTION_UNSUBSCRIBED`] when the subscription is
    /// or becomes unsubscribed before it is sent. The machine settles it
    /// with [`Action::CallSettled`], unless the owner abandons it with
    /// [`super::Input::CallAbandoned`].
    pub fn add_subscription_call(
        &mut self,
        subscription: SubscriptionId,
        mut request: CallRequest,
    ) -> CallId {
        if let (Some(channel), Some(subscribed)) = (
            request.channel_mut(),
            self.subscription_channel(subscription),
        ) {
            channel.clone_from(subscribed);
        }
        self.insert_call(request, Some(subscription))
    }

    fn insert_call(
        &mut self,
        request: CallRequest,
        subscription: Option<SubscriptionId>,
    ) -> CallId {
        let calls = &mut self.calls;
        let id = CallId(calls.next_id);
        calls.next_id += 1;
        calls.by_id.insert(
            id,
            Call {
                request,
                subscription,
                phase: Phase::Added,
            },
        );
        id
    }

    /// Starts call `id`. A subscription-level call waits for its
    /// subscription to be subscribed, or fails at once when it is
    /// unsubscribed (centrifuge-js `subscription.ts` `_methodCall`).
    pub(super) fn call(&mut self, id: CallId) {
        let Some(call) = self.calls.by_id.get(&id) else {
            return;
        };
        if !matches!(call.phase, Phase::Added) {
            return;
        }
        match call
            .subscription
            .map(|subscription| self.subscription_state(subscription))
        {
            None | Some(Some(SubscriptionState::Subscribed)) => self.start_call(id),
            Some(Some(SubscriptionState::Subscribing)) => {
                if let Some(call) = self.calls.by_id.get_mut(&id) {
                    call.phase = Phase::Subscribing;
                }
                self.actions.push(Action::SetCallTimer(
                    id,
                    CallTimer::Subscription,
                    self.config.timeout,
                ));
            }
            // Unsubscribed, or removed from the registry.
            Some(_) => self.settle_call(id, Err(subscription_unsubscribed())),
        }
    }

    /// Starts call `id` as a client-level call: sent at once when
    /// connected, otherwise queued until the connection is there
    /// (centrifuge-js `_methodCall`). A transport whose close is read but
    /// not yet handled counts as gone: the call waits for the next one.
    fn start_call(&mut self, id: CallId) {
        let connected = self.state == State::Connected && !self.transport_closing();
        let Some(call) = self.calls.by_id.get_mut(&id) else {
            return;
        };
        if matches!(call.phase, Phase::Subscribing) {
            self.actions
                .push(Action::CancelCallTimer(id, CallTimer::Subscription));
        }
        if connected {
            self.send_call(id);
        } else {
            call.phase = Phase::Queued;
            self.actions.push(Action::SetCallTimer(
                id,
                CallTimer::Connection,
                self.config.timeout,
            ));
        }
    }

    /// Sends call `id`, whose reply is then awaited for `Config::timeout`.
    /// Its command is built only now (centrifuge-js `_methodCall`).
    fn send_call(&mut self, id: CallId) {
        let Some(transport) = self.transport else {
            return;
        };
        let command_id = self.command_id();
        let Some(call) = self.calls.by_id.get_mut(&id) else {
            return;
        };
        if matches!(call.phase, Phase::Queued) {
            self.actions
                .push(Action::CancelCallTimer(id, CallTimer::Connection));
        }
        call.phase = Phase::Sent(command_id);
        let method = match &call.request {
            CallRequest::Rpc { method, data } => Method::Rpc(RpcRequest {
                method: method.clone(),
                data: data.clone(),
            }),
            CallRequest::Publish { channel, data } => Method::Publish(PublishRequest {
                channel: channel.clone(),
                data: data.clone(),
            }),
            CallRequest::History { channel, options } => Method::History(HistoryRequest {
                channel: channel.clone(),
                limit: options.limit,
                since: options
                    .since
                    .as_ref()
                    .map(|position| (position.offset, position.epoch.clone())),
                reverse: options.reverse,
            }),
            CallRequest::Presence { channel } => Method::Presence(ChannelRequest {
                channel: channel.clone(),
            }),
            CallRequest::PresenceStats { channel } => Method::PresenceStats(ChannelRequest {
                channel: channel.clone(),
            }),
        };
        self.calls.by_command.insert(command_id, id);
        self.send(
            transport,
            Command {
                id: command_id,
                method,
            },
        );
        self.actions.push(Action::SetCallTimer(
            id,
            CallTimer::Reply,
            self.config.timeout,
        ));
    }

    /// The client is connected: the queued calls are sent, in order,
    /// unless the transport's close is already read, held behind earlier
    /// events: they wait for the next connection then.
    pub(super) fn calls_connected(&mut self) {
        if self.transport_closing() {
            return;
        }
        let queued: Vec<CallId> = self
            .calls
            .by_id
            .iter()
            .filter(|(_, call)| matches!(call.phase, Phase::Queued))
            .map(|(id, _)| *id)
            .collect();
        for id in queued {
            self.send_call(id);
        }
    }

    /// Subscription `id` is subscribed: its calls waiting for it start as
    /// client-level calls, each phase with a fresh timeout (centrifuge-js
    /// resolves `_methodCall`, then the client's call starts its own).
    pub(super) fn subscription_calls_subscribed(&mut self, subscription: SubscriptionId) {
        for id in self.waiting_calls(subscription) {
            self.start_call(id);
        }
    }

    /// Subscription `id` is unsubscribed: its calls waiting for it fail
    /// with "subscription unsubscribed"; those already sent go on
    /// (centrifuge-js `_setUnsubscribed`, `_rejectPromises`).
    pub(super) fn subscription_calls_unsubscribed(&mut self, subscription: SubscriptionId) {
        for id in self.waiting_calls(subscription) {
            self.settle_call(id, Err(subscription_unsubscribed()));
        }
    }

    /// The calls waiting for `subscription` to be subscribed, in order.
    fn waiting_calls(&self, subscription: SubscriptionId) -> Vec<CallId> {
        self.calls
            .by_id
            .iter()
            .filter(|(_, call)| {
                call.subscription == Some(subscription) && matches!(call.phase, Phase::Subscribing)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// A call's deadline passed: waiting for its subscription, for the
    /// connection or for the reply, it fails with the timeout error, and a queued one is never
    /// sent. The timer of a phase the call has left is stale: it fired
    /// before the call moved on, and its input came after.
    pub(super) fn call_timer_fired(&mut self, id: CallId, timer: CallTimer) {
        if self
            .calls
            .by_id
            .get(&id)
            .is_some_and(|call| call.phase.timer() == Some(timer))
        {
            self.settle_call(id, Err(CallError::new(codes::error::TIMEOUT, "timeout")));
        }
    }

    /// The owner gave up on call `id`, e.g. its future was dropped: it is
    /// forgotten, and never sent if it was queued.
    pub(super) fn call_abandoned(&mut self, id: CallId) {
        if let Some(call) = self.calls.by_id.remove(&id) {
            if let Phase::Sent(command) = call.phase {
                self.calls.by_command.remove(&command);
            }
            if let Some(timer) = call.phase.timer() {
                self.actions.push(Action::CancelCallTimer(id, timer));
            }
        }
    }

    /// A reply to a call's command, read or made up for a rejected request:
    /// it settles the call. A reply without the result of the call's
    /// method, error aside, breaks the protocol; RPC and publish results
    /// may be left out, as their result is empty.
    pub(super) fn call_reply(&mut self, reply: Reply) {
        let Some(&id) = self.calls.by_command.get(&reply.id) else {
            return;
        };
        let Some(call) = self.calls.by_id.get(&id) else {
            return;
        };
        let result = match (reply.error, &call.request, reply.result) {
            (
                Some(Error {
                    code,
                    message,
                    temporary,
                }),
                _,
                _,
            ) => Err(CallError::new(code, message).temporary(temporary)),
            (None, CallRequest::Rpc { .. }, ReplyResult::Rpc(data)) => Ok(CallReply::Rpc(data)),
            (None, CallRequest::Rpc { .. }, ReplyResult::Empty) => Ok(CallReply::Rpc(Vec::new())),
            (None, CallRequest::Publish { .. }, ReplyResult::Publish | ReplyResult::Empty) => {
                Ok(CallReply::Publish)
            }
            (None, CallRequest::History { .. }, ReplyResult::History(history)) => {
                Ok(CallReply::History(HistoryResult {
                    publications: history.publications,
                    epoch: history.epoch,
                    offset: history.offset,
                }))
            }
            (None, CallRequest::Presence { .. }, ReplyResult::Presence(clients)) => {
                Ok(CallReply::Presence(PresenceResult { clients }))
            }
            (None, CallRequest::PresenceStats { .. }, ReplyResult::PresenceStats(stats)) => {
                Ok(CallReply::PresenceStats(PresenceStatsResult {
                    num_clients: stats.num_clients,
                    num_users: stats.num_users,
                }))
            }
            (None, _, _) => {
                self.settle_call(id, Err(connection_closed()));
                self.reconnect_after_transport_closed();
                return;
            }
        };
        self.settle_call(id, result);
    }

    /// The transport is lost: the calls sent on it can no longer get their
    /// reply and fail with "connection closed"; queued ones keep waiting.
    pub(super) fn fail_sent_calls(&mut self) {
        let sent: Vec<CallId> = self.calls.by_command.values().copied().collect();
        for id in sent {
            self.settle_call(id, Err(connection_closed()));
        }
    }

    /// The client is entering, or stays in, `disconnected`: every call
    /// started, waiting or sent, fails with "disconnected" (Block 13,
    /// exceptions for sent calls and calls waiting for their
    /// subscription).
    pub(super) fn fail_calls_disconnected(&mut self) {
        let started: Vec<CallId> = self
            .calls
            .by_id
            .iter()
            .filter(|(_, call)| !matches!(call.phase, Phase::Added))
            .map(|(id, _)| *id)
            .collect();
        for id in started {
            self.settle_call(
                id,
                Err(CallError::new(codes::error::DISCONNECTED, "disconnected")),
            );
        }
    }

    fn settle_call(&mut self, id: CallId, result: Result<CallReply, CallError>) {
        let Some(call) = self.calls.by_id.remove(&id) else {
            return;
        };
        if let Phase::Sent(command) = call.phase {
            self.calls.by_command.remove(&command);
        }
        if let Some(timer) = call.phase.timer() {
            self.actions.push(Action::CancelCallTimer(id, timer));
        }
        self.actions.push(Action::CallSettled { call: id, result });
    }
}

fn connection_closed() -> CallError {
    CallError::new(codes::error::CONNECTION_CLOSED, "connection closed")
}

fn subscription_unsubscribed() -> CallError {
    CallError::new(
        codes::error::SUBSCRIPTION_UNSUBSCRIBED,
        "subscription unsubscribed",
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::machine::{Input, TransportId};
    use crate::{ClientEvent, Config};

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn machine() -> Machine {
        Machine::new(Config {
            timeout: TIMEOUT,
            ..Config::default()
        })
    }

    /// Starts connecting; the websocket opened and its connect command is 1.
    fn opened(machine: &mut Machine) -> TransportId {
        let transport = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a transport to open");
        machine.handle(Input::TransportOpened(transport));
        transport
    }

    fn frame(transport: TransportId, json: &str) -> Input {
        Input::Frame {
            transport,
            data: json.as_bytes().to_vec(),
        }
    }

    fn connect_reply(machine: &mut Machine, transport: TransportId) -> Vec<Action> {
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#))
    }

    fn connected() -> (Machine, TransportId) {
        let mut machine = machine();
        let transport = opened(&mut machine);
        connect_reply(&mut machine, transport);
        (machine, transport)
    }

    fn rpc(machine: &mut Machine, method: &str) -> (CallId, Vec<Action>) {
        let id = machine.add_call(CallRequest::Rpc {
            method: method.into(),
            data: br#"{"n":1}"#.to_vec(),
        });
        let actions = machine.handle(Input::Call(id));
        (id, actions)
    }

    fn sent_text(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(String::from_utf8(frame.clone()).unwrap()),
                _ => None,
            })
            .collect()
    }

    fn settled(actions: &[Action]) -> Vec<(CallId, Result<CallReply, CallError>)> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::CallSettled { call, result } => Some((*call, result.clone())),
                _ => None,
            })
            .collect()
    }

    fn error_code(actions: &[Action]) -> Option<u32> {
        match settled(actions).as_slice() {
            [(_, Err(error))] => Some(error.code),
            _ => None,
        }
    }

    #[test]
    fn a_call_while_connected_is_sent_with_its_reply_deadline() {
        let (mut machine, _) = connected();

        let (id, actions) = rpc(&mut machine, "echo");

        assert_eq!(
            sent_text(&actions),
            [r#"{"id":2,"rpc":{"method":"echo","data":{"n":1}}}"#]
        );
        assert!(actions.contains(&Action::SetCallTimer(id, CallTimer::Reply, TIMEOUT)));
    }

    #[test]
    fn a_queued_call_waits_for_the_connection_then_for_its_reply() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, queued) = rpc(&mut machine, "echo");

        let connected = connect_reply(&mut machine, transport);

        assert_eq!(sent_text(&queued), Vec::<String>::new());
        assert!(queued.contains(&Action::SetCallTimer(id, CallTimer::Connection, TIMEOUT)));
        // Each phase gets the whole timeout (centrifuge-js `_methodCall`,
        // then `_registerCall`).
        assert_eq!(sent_text(&connected).len(), 1, "{connected:?}");
        assert!(connected.contains(&Action::CancelCallTimer(id, CallTimer::Connection)));
        assert!(connected.contains(&Action::SetCallTimer(id, CallTimer::Reply, TIMEOUT)));
    }

    #[test]
    fn a_connection_timer_firing_after_the_call_was_sent_is_ignored() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");
        connect_reply(&mut machine, transport);

        // It fired before the connect reply was handled; its input comes
        // after (review of PR #74).
        let stale = machine.handle(Input::CallTimerFired(id, CallTimer::Connection));
        let replied = machine.handle(frame(transport, r#"{"id":2,"rpc":{}}"#));

        assert_eq!(stale, []);
        assert_eq!(settled(&replied), [(id, Ok(CallReply::Rpc(Vec::new())))]);
    }

    #[test]
    fn a_call_made_while_a_close_waits_behind_events_is_queued() {
        let mut machine = machine();
        // A stream that takes nothing holds the messages after `connected`.
        machine.handle(Input::StreamOpened(None));
        let transport = opened(&mut machine);
        connect_reply(&mut machine, transport);
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let (id, actions) = rpc(&mut machine, "echo");

        assert_eq!(machine.state(), State::Connected);
        assert_eq!(sent_text(&actions), Vec::<String>::new());
        assert!(actions.contains(&Action::SetCallTimer(id, CallTimer::Connection, TIMEOUT)));
    }

    #[test]
    fn queued_calls_stay_queued_when_a_held_connect_reply_comes_after_the_close() {
        let mut machine = machine();
        // A stream that takes nothing: the first `connected` event, not
        // taken, holds the messages of the next connection (Block 15).
        machine.handle(Input::StreamOpened(None));
        let first = opened(&mut machine);
        let connected = connect_reply(&mut machine, first)
            .into_iter()
            .find_map(|action| match action {
                Action::Emit(event, ClientEvent::Connected { .. }) => Some(event),
                _ => None,
            })
            .expect("a connected event");
        machine.handle(Input::Disconnect);
        let second = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");
        machine.handle(frame(second, r#"{"id":2,"connect":{"client":"c2"}}"#));
        machine.handle(Input::TransportClosed {
            transport: second,
            code: None,
            reason: String::new(),
        });

        let released = machine.handle(Input::EventTaken(connected));

        assert!(
            released
                .iter()
                .any(|action| matches!(action, Action::Emit(_, ClientEvent::Connected { .. }))),
            "the held connect reply was not handled: {released:?}"
        );
        assert_eq!(sent_text(&released), Vec::<String>::new());
        assert_eq!(settled(&released), []);
        assert!(machine.calls.by_id.contains_key(&id));
    }

    #[test]
    fn a_call_timed_out_while_queued_is_never_sent() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");

        let timed_out = machine.handle(Input::CallTimerFired(id, CallTimer::Connection));
        let connected = connect_reply(&mut machine, transport);

        assert_eq!(error_code(&timed_out), Some(codes::error::TIMEOUT));
        assert_eq!(sent_text(&connected), Vec::<String>::new());
    }

    #[test]
    fn an_abandoned_queued_call_is_never_sent_nor_settled() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");

        let abandoned = machine.handle(Input::CallAbandoned(id));
        let connected = connect_reply(&mut machine, transport);

        assert_eq!(
            abandoned,
            [Action::CancelCallTimer(id, CallTimer::Connection)]
        );
        assert_eq!(sent_text(&connected), Vec::<String>::new());
        assert_eq!(settled(&connected), []);
    }

    #[test]
    fn replies_settle_their_own_calls_in_any_order() {
        let (mut machine, transport) = connected();
        let (first, _) = rpc(&mut machine, "first");
        let (second, _) = rpc(&mut machine, "second");

        let actions = machine.handle(frame(
            transport,
            "{\"id\":3,\"rpc\":{\"data\":2}}\n{\"id\":2,\"rpc\":{\"data\":1}}",
        ));

        assert_eq!(
            settled(&actions),
            [
                (second, Ok(CallReply::Rpc(b"2".to_vec()))),
                (first, Ok(CallReply::Rpc(b"1".to_vec())))
            ]
        );
    }

    #[test]
    fn a_server_error_keeps_its_code_message_and_temporary_flag() {
        let (mut machine, transport) = connected();
        let (id, _) = rpc(&mut machine, "busy");

        let actions = machine.handle(frame(
            transport,
            r#"{"id":2,"error":{"code":1000,"message":"try later","temporary":true}}"#,
        ));

        assert_eq!(
            settled(&actions),
            [(id, Err(CallError::new(1000, "try later").temporary(true)))]
        );
    }

    #[test]
    fn a_reply_settles_its_call_ahead_of_messages_held_for_events() {
        let mut machine = machine();
        // A stream that takes nothing holds the messages after `connected`.
        machine.handle(Input::StreamOpened(None));
        let transport = opened(&mut machine);
        connect_reply(&mut machine, transport);
        let (id, _) = rpc(&mut machine, "echo");
        machine.handle(frame(
            transport,
            r#"{"push":{"disconnect":{"code":3001,"reason":"later"}}}"#,
        ));

        let actions = machine.handle(frame(transport, r#"{"id":2,"rpc":{"data":1}}"#));

        assert_eq!(settled(&actions), [(id, Ok(CallReply::Rpc(b"1".to_vec())))]);
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn a_reply_read_wins_over_a_deadline_firing_after_it() {
        let (mut machine, transport) = connected();
        let (id, _) = rpc(&mut machine, "echo");

        let replied = machine.handle(frame(transport, r#"{"id":2,"rpc":{}}"#));
        let fired = machine.handle(Input::CallTimerFired(id, CallTimer::Reply));

        assert!(replied.contains(&Action::CancelCallTimer(id, CallTimer::Reply)));
        assert_eq!(settled(&replied), [(id, Ok(CallReply::Rpc(Vec::new())))]);
        assert_eq!(fired, []);
    }

    #[test]
    fn replies_to_unknown_commands_are_ignored() {
        let (mut machine, transport) = connected();
        let (_, _) = rpc(&mut machine, "echo");

        let actions = machine.handle(frame(transport, r#"{"id":9,"rpc":{}}"#));

        assert_eq!(settled(&actions), []);
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn losing_the_transport_fails_sent_calls_and_keeps_queued_ones() {
        let (mut machine, transport) = connected();
        let (sent, _) = rpc(&mut machine, "slow");

        let closed = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let (queued, _) = rpc(&mut machine, "later");

        assert_eq!(
            settled(&closed),
            [(
                sent,
                Err(CallError::new(
                    codes::error::CONNECTION_CLOSED,
                    "connection closed"
                ))
            )]
        );
        assert_eq!(machine.state(), State::Connecting);
        assert!(machine.calls.by_id.contains_key(&queued));
    }

    #[test]
    fn disconnect_fails_a_sent_call_with_disconnected() {
        let (mut machine, _) = connected();
        let (id, _) = rpc(&mut machine, "slow");

        let actions = machine.handle(Input::Disconnect);

        // Block 13's exception: centrifuge-js fails it with code 11.
        assert_eq!(
            settled(&actions),
            [(
                id,
                Err(CallError::new(codes::error::DISCONNECTED, "disconnected"))
            )]
        );
    }

    #[test]
    fn disconnect_fails_a_call_queued_while_reconnecting() {
        let (mut machine, transport) = connected();
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let (id, _) = rpc(&mut machine, "later");

        let actions = machine.handle(Input::Disconnect);

        assert_eq!(
            settled(&actions),
            [(
                id,
                Err(CallError::new(codes::error::DISCONNECTED, "disconnected"))
            )]
        );
        assert!(actions.contains(&Action::CancelCallTimer(id, CallTimer::Connection)));
    }

    #[test]
    fn a_terminal_disconnect_fails_queued_calls() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");

        let actions = machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":4500,"message":"go away"}}"#,
        ));

        assert_eq!(machine.state(), State::Disconnected);
        assert_eq!(settled(&actions)[0].0, id);
        assert_eq!(error_code(&actions), Some(codes::error::DISCONNECTED));
    }

    #[test]
    fn disconnect_while_disconnected_fails_queued_calls() {
        let mut machine = machine();
        let (id, _) = rpc(&mut machine, "echo");

        let actions = machine.handle(Input::Disconnect);

        assert_eq!(
            settled(&actions),
            [(
                id,
                Err(CallError::new(codes::error::DISCONNECTED, "disconnected"))
            )]
        );
    }

    #[test]
    fn a_request_rejected_as_too_large_fails_its_call_and_keeps_the_transport() {
        let (mut machine, transport) = connected();
        let (id, actions) = rpc(&mut machine, "echo");
        let Some(Action::Send { frame, .. }) = actions
            .iter()
            .find(|action| matches!(action, Action::Send { .. }))
        else {
            panic!("{actions:?}");
        };

        let rejected = machine.handle(Input::SendRejected {
            transport,
            frame: frame.clone(),
        });

        assert_eq!(
            settled(&rejected),
            [(
                id,
                Err(CallError::new(
                    codes::error::TRANSPORT_WRITE,
                    "transport write error"
                ))
            )]
        );
        assert_eq!(machine.state(), State::Connected);
    }

    /// A subscription to "news" on a connected client, its subscribe
    /// command sent (id 2) and not answered.
    fn subscribing() -> (Machine, TransportId, SubscriptionId) {
        let (mut machine, transport) = connected();
        let subscription = machine
            .add_subscription(
                "news".into(),
                crate::machine::SubscriptionOptions::default(),
            )
            .unwrap();
        machine.handle(Input::Subscribe(subscription));
        (machine, transport, subscription)
    }

    fn history(machine: &mut Machine, subscription: SubscriptionId) -> (CallId, Vec<Action>) {
        let id = machine.add_subscription_call(
            subscription,
            CallRequest::History {
                channel: "elsewhere".into(),
                options: crate::HistoryOptions::default().limit(2),
            },
        );
        let actions = machine.handle(Input::Call(id));
        (id, actions)
    }

    #[test]
    fn a_subscription_call_waits_until_subscribed_then_uses_its_channel() {
        let (mut machine, transport, subscription) = subscribing();
        let (id, waiting) = history(&mut machine, subscription);

        let subscribed = machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));

        assert_eq!(sent_text(&waiting), Vec::<String>::new());
        assert!(waiting.contains(&Action::SetCallTimer(id, CallTimer::Subscription, TIMEOUT)));
        assert_eq!(
            sent_text(&subscribed),
            [r#"{"id":3,"history":{"channel":"news","limit":2}}"#]
        );
        assert!(subscribed.contains(&Action::CancelCallTimer(id, CallTimer::Subscription)));
        assert!(subscribed.contains(&Action::SetCallTimer(id, CallTimer::Reply, TIMEOUT)));
    }

    #[test]
    fn a_subscription_call_on_an_unsubscribed_subscription_fails_at_once() {
        let (mut machine, _) = connected();
        let subscription = machine
            .add_subscription(
                "news".into(),
                crate::machine::SubscriptionOptions::default(),
            )
            .unwrap();

        let (id, actions) = history(&mut machine, subscription);

        assert_eq!(sent_text(&actions), Vec::<String>::new());
        assert_eq!(
            settled(&actions),
            [(
                id,
                Err(CallError::new(
                    codes::error::SUBSCRIPTION_UNSUBSCRIBED,
                    "subscription unsubscribed"
                ))
            )]
        );
    }

    #[test]
    fn a_waiting_subscription_call_fails_when_the_subscribe_fails_for_good() {
        let (mut machine, transport, subscription) = subscribing();
        let (id, _) = history(&mut machine, subscription);

        let failed = machine.handle(frame(
            transport,
            r#"{"id":2,"error":{"code":403,"message":"closed"}}"#,
        ));

        assert_eq!(sent_text(&failed), Vec::<String>::new());
        assert_eq!(
            error_code(&failed),
            Some(codes::error::SUBSCRIPTION_UNSUBSCRIBED)
        );
        assert!(failed.contains(&Action::CancelCallTimer(id, CallTimer::Subscription)));
    }

    #[test]
    fn a_subscription_call_sent_goes_on_when_the_subscription_ends() {
        let (mut machine, transport, subscription) = subscribing();
        machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));
        let (id, _) = history(&mut machine, subscription);

        let unsubscribed = machine.handle(Input::Unsubscribe(subscription));
        let replied = machine.handle(frame(
            transport,
            r#"{"id":3,"history":{"epoch":"e","offset":7}}"#,
        ));

        assert_eq!(settled(&unsubscribed), []);
        assert_eq!(
            settled(&replied),
            [(
                id,
                Ok(CallReply::History(HistoryResult {
                    publications: Vec::new(),
                    epoch: "e".into(),
                    offset: 7
                }))
            )]
        );
    }

    #[test]
    fn a_subscription_call_waiting_too_long_times_out() {
        let (mut machine, _, subscription) = subscribing();
        let (id, _) = history(&mut machine, subscription);

        let stale = machine.handle(Input::CallTimerFired(id, CallTimer::Connection));
        let fired = machine.handle(Input::CallTimerFired(id, CallTimer::Subscription));

        assert_eq!(stale, []);
        assert_eq!(error_code(&fired), Some(codes::error::TIMEOUT));
    }

    #[test]
    fn disconnect_fails_a_call_waiting_for_its_subscription() {
        let (mut machine, _, subscription) = subscribing();
        let (_, _) = history(&mut machine, subscription);

        let actions = machine.handle(Input::Disconnect);

        assert_eq!(error_code(&actions), Some(codes::error::DISCONNECTED));
    }

    #[test]
    fn a_reply_with_another_method_s_result_breaks_the_protocol() {
        let (mut machine, transport, subscription) = subscribing();
        machine.handle(frame(transport, r#"{"id":2,"subscribe":{}}"#));
        let (_, _) = history(&mut machine, subscription);

        let actions = machine.handle(frame(transport, r#"{"id":3,"rpc":{"data":1}}"#));

        assert_eq!(error_code(&actions), Some(codes::error::CONNECTION_CLOSED));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn each_method_gets_its_own_result() {
        let (mut machine, transport) = connected();
        let calls: Vec<CallId> = [
            CallRequest::Publish {
                channel: "news".into(),
                data: b"{}".to_vec(),
            },
            CallRequest::Presence {
                channel: "news".into(),
            },
            CallRequest::PresenceStats {
                channel: "news".into(),
            },
        ]
        .into_iter()
        .map(|request| {
            let id = machine.add_call(request);
            machine.handle(Input::Call(id));
            id
        })
        .collect();

        let actions = machine.handle(frame(
            transport,
            concat!(
                r#"{"id":2,"publish":{}}"#,
                "\n",
                r#"{"id":3,"presence":{"presence":{"c":{"client":"c","user":"u"}}}}"#,
                "\n",
                r#"{"id":4,"presence_stats":{"num_clients":1,"num_users":1}}"#,
            ),
        ));

        let settled = settled(&actions);
        assert_eq!(settled[0], (calls[0], Ok(CallReply::Publish)));
        let (_, Ok(CallReply::Presence(presence))) = &settled[1] else {
            panic!("{settled:?}");
        };
        assert_eq!(presence.clients["c"].user, "u");
        assert_eq!(
            settled[2],
            (
                calls[2],
                Ok(CallReply::PresenceStats(PresenceStatsResult {
                    num_clients: 1,
                    num_users: 1
                }))
            )
        );
    }
}
