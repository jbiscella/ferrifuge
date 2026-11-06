//! Block 13: calls the application awaits. A call made while not
//! connected waits for the connection, then for its reply, each phase
//! bounded by `Config::timeout`. A reply settles its call when it is read,
//! ahead of the messages Block 15 holds. Losing the transport fails the
//! calls sent on it with "connection closed"; entering `disconnected` fails
//! every call with "disconnected".

use std::collections::{BTreeMap, HashMap};

use super::{Action, CallId, Machine};
use crate::wire::{Command, Error, Method, Reply, ReplyResult, RpcRequest};
use crate::{CallError, CallReply, CallRequest, State, codes};

#[derive(Debug)]
enum Phase {
    /// Added, not yet started by [`super::Input::Call`].
    Added,
    /// Waiting for the connection.
    Queued,
    /// Sent as the command with this id, waiting for its reply.
    Sent(u32),
}

#[derive(Debug)]
struct Call {
    request: CallRequest,
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
        let calls = &mut self.calls;
        let id = CallId(calls.next_id);
        calls.next_id += 1;
        calls.by_id.insert(
            id,
            Call {
                request,
                phase: Phase::Added,
            },
        );
        id
    }

    /// Starts call `id`: sent at once when connected, otherwise queued
    /// until the connection is there (centrifuge-js `_methodCall`).
    pub(super) fn call(&mut self, id: CallId) {
        let Some(call) = self.calls.by_id.get_mut(&id) else {
            return;
        };
        if !matches!(call.phase, Phase::Added) {
            return;
        }
        if self.state == State::Connected {
            self.send_call(id);
        } else {
            call.phase = Phase::Queued;
            self.actions
                .push(Action::SetCallTimer(id, self.config.timeout));
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
        call.phase = Phase::Sent(command_id);
        let method = match &call.request {
            CallRequest::Rpc { method, data } => Method::Rpc(RpcRequest {
                method: method.clone(),
                data: data.clone(),
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
        self.actions
            .push(Action::SetCallTimer(id, self.config.timeout));
    }

    /// The client is connected: the queued calls are sent, in order.
    pub(super) fn calls_connected(&mut self) {
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

    /// A call's deadline passed: waiting for the connection or for the
    /// reply, it fails with the timeout error, and a queued one is never
    /// sent.
    pub(super) fn call_timer_fired(&mut self, id: CallId) {
        if self
            .calls
            .by_id
            .get(&id)
            .is_some_and(|call| !matches!(call.phase, Phase::Added))
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
            self.actions.push(Action::CancelCallTimer(id));
        }
    }

    /// A reply to a call's command, read or made up for a rejected request:
    /// it settles the call. A reply without the call's result, error aside,
    /// breaks the protocol.
    pub(super) fn call_reply(&mut self, reply: Reply) {
        let Some(&id) = self.calls.by_command.get(&reply.id) else {
            return;
        };
        let result = match (reply.error, reply.result) {
            (
                Some(Error {
                    code,
                    message,
                    temporary,
                }),
                _,
            ) => Err(CallError::new(code, message).temporary(temporary)),
            (None, ReplyResult::Rpc(data)) => Ok(CallReply::Rpc(data)),
            // An RPC without result data.
            (None, ReplyResult::Empty) => Ok(CallReply::Rpc(Vec::new())),
            (None, _) => {
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
    /// started, queued or sent, fails with "disconnected" (Block 13,
    /// exception for sent calls).
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
        self.actions.push(Action::CancelCallTimer(id));
        self.actions.push(Action::CallSettled { call: id, result });
    }
}

fn connection_closed() -> CallError {
    CallError::new(codes::error::CONNECTION_CLOSED, "connection closed")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Config;
    use crate::machine::{Input, TransportId};

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
        assert!(actions.contains(&Action::SetCallTimer(id, TIMEOUT)));
    }

    #[test]
    fn a_queued_call_waits_for_the_connection_then_for_its_reply() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, queued) = rpc(&mut machine, "echo");

        let connected = connect_reply(&mut machine, transport);

        assert_eq!(sent_text(&queued), Vec::<String>::new());
        assert!(queued.contains(&Action::SetCallTimer(id, TIMEOUT)));
        // Each phase gets the whole timeout (centrifuge-js `_methodCall`,
        // then `_registerCall`).
        assert_eq!(sent_text(&connected).len(), 1, "{connected:?}");
        assert!(connected.contains(&Action::SetCallTimer(id, TIMEOUT)));
    }

    #[test]
    fn a_call_timed_out_while_queued_is_never_sent() {
        let mut machine = machine();
        let transport = opened(&mut machine);
        let (id, _) = rpc(&mut machine, "echo");

        let timed_out = machine.handle(Input::CallTimerFired(id));
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

        assert_eq!(abandoned, [Action::CancelCallTimer(id)]);
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
        let fired = machine.handle(Input::CallTimerFired(id));

        assert!(replied.contains(&Action::CancelCallTimer(id)));
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
        assert!(actions.contains(&Action::CancelCallTimer(id)));
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
}
