//! Sans-IO core: the client connection state machine.
//!
//! A [`Machine`] owns no sockets, timers or tasks. Its owner feeds it
//! [`Input`]s and performs the returned [`Action`]s in order. The tokio
//! adapter is one such owner; any other runtime can drive it the same way.
//!
//! Behaviour follows the Client SDK spec, with centrifuge-js as the
//! implementation reference (`src/centrifuge.ts`).

use std::time::Duration;

use crate::backoff::Backoff;
use crate::wire::{Command, ConnectRequest, ConnectResult, Error, Method, Reply, ReplyResult};
use crate::{ClientError, ClientEvent, Config, ErrorKind, State, codes, json};

/// Name of the only transport implemented so far.
const TRANSPORT: &str = "websocket";

/// Identifies one transport the machine asked to open. Inputs about a
/// transport the machine has since abandoned are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportId(u64);

/// Timers the machine sets. At most one of each kind runs at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timer {
    /// Delay before the next connection attempt.
    Reconnect,
    /// Deadline for a connection attempt to get its connect reply.
    ConnectTimeout,
}

/// Something that happened, for the machine to react to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// The application called `connect()`.
    Connect,
    /// The application called `disconnect()`.
    Disconnect,
    /// The transport opened.
    TransportOpened(TransportId),
    /// The transport could not be opened.
    TransportFailed {
        /// The transport that failed.
        transport: TransportId,
        /// Why, for the error event.
        message: String,
    },
    /// The transport closed after opening.
    TransportClosed {
        /// The transport that closed.
        transport: TransportId,
        /// Close code, when the transport has one.
        code: Option<u16>,
        /// Close reason, possibly empty.
        reason: String,
    },
    /// The transport received a frame.
    Frame {
        /// The transport that received it.
        transport: TransportId,
        /// Encoded replies.
        data: Vec<u8>,
    },
    /// A timer set by [`Action::SetTimer`] fired.
    TimerFired(Timer),
}

/// Something the owner of the machine must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Open a transport to the endpoint; report the outcome with
    /// [`Input::TransportOpened`] or [`Input::TransportFailed`].
    OpenTransport(TransportId),
    /// Close the transport and report nothing more about it.
    CloseTransport(TransportId),
    /// Send a frame over the transport.
    Send {
        /// The transport to send on.
        transport: TransportId,
        /// Encoded commands.
        frame: Vec<u8>,
    },
    /// Start a timer, replacing a running one of the same kind.
    SetTimer(Timer, Duration),
    /// Stop a timer if it runs.
    CancelTimer(Timer),
    /// Deliver an event to the application.
    Emit(ClientEvent),
}

/// The client connection state machine.
#[derive(Debug)]
pub struct Machine {
    config: Config,
    state: State,
    backoff: Backoff,
    rng: fastrand::Rng,
    /// The transport opening or open, if any.
    transport: Option<TransportId>,
    transport_open: bool,
    next_transport: u64,
    next_command: u32,
    /// Id of the connect command awaiting its reply.
    connect_command: Option<u32>,
    actions: Vec<Action>,
}

impl Machine {
    /// Creates a disconnected machine.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_rng(config, fastrand::Rng::new())
    }

    fn with_rng(config: Config, rng: fastrand::Rng) -> Self {
        let backoff = Backoff::new(config.min_reconnect_delay, config.max_reconnect_delay);
        Self {
            config,
            state: State::Disconnected,
            backoff,
            rng,
            transport: None,
            transport_open: false,
            next_transport: 0,
            next_command: 0,
            connect_command: None,
            actions: Vec::new(),
        }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// Reacts to `input`; the owner performs the returned actions in order.
    pub fn handle(&mut self, input: Input) -> Vec<Action> {
        match input {
            Input::Connect => self.connect(),
            Input::Disconnect => self.disconnect(
                codes::disconnected::DISCONNECT_CALLED,
                "disconnect called",
                false,
            ),
            Input::TransportOpened(id) if self.is_current(id) => self.transport_opened(),
            Input::TransportFailed { transport, message } if self.is_current(transport) => {
                self.transport_failed(message);
            }
            Input::TransportClosed {
                transport,
                code,
                reason,
            } if self.is_current(transport) => self.transport_closed(code, &reason),
            Input::Frame { transport, data } if self.is_current(transport) => self.frame(&data),
            Input::TimerFired(Timer::Reconnect) => {
                if self.state == State::Connecting && self.transport.is_none() {
                    self.start_attempt();
                }
            }
            Input::TimerFired(Timer::ConnectTimeout) => {
                if self.state == State::Connecting && self.transport.is_some() {
                    self.transport_failed("connect timeout".to_owned());
                }
            }
            // About a transport already abandoned.
            Input::TransportOpened(_)
            | Input::TransportFailed { .. }
            | Input::TransportClosed { .. }
            | Input::Frame { .. } => {}
        }
        std::mem::take(&mut self.actions)
    }

    fn is_current(&self, id: TransportId) -> bool {
        self.transport == Some(id)
    }

    fn connect(&mut self) {
        if self.state != State::Disconnected {
            return;
        }
        self.state = State::Connecting;
        self.emit(ClientEvent::Connecting {
            code: codes::connecting::CONNECT_CALLED,
            reason: "connect called".to_owned(),
        });
        // The first attempt is immediate; delays apply to reconnects.
        self.start_attempt();
    }

    fn start_attempt(&mut self) {
        let id = TransportId(self.next_transport);
        self.next_transport += 1;
        self.transport = Some(id);
        self.transport_open = false;
        self.actions.push(Action::OpenTransport(id));
        self.actions
            .push(Action::SetTimer(Timer::ConnectTimeout, self.config.timeout));
    }

    fn transport_opened(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        self.transport_open = true;
        let id = self.command_id();
        self.connect_command = Some(id);
        let command = Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: String::new(),
                name: self.config.name.clone(),
                version: self.config.version.clone(),
            }),
        };
        self.actions.push(Action::Send {
            transport,
            frame: json::encode(&[command]),
        });
    }

    /// The attempt failed before a connection was established.
    fn transport_failed(&mut self, message: String) {
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Transport,
            code: codes::error::TRANSPORT_CLOSED,
            message,
            transport: Some(TRANSPORT.to_owned()),
        }));
        self.reconnect_after_transport_closed();
    }

    fn transport_closed(&mut self, code: Option<u16>, reason: &str) {
        match code.map(u32::from) {
            // Codes from 3000 are disconnect codes set by the server.
            Some(code) if code >= 3000 => self.disconnect(code, reason, reconnect_advised(code)),
            _ if self.state == State::Connecting && !self.transport_open => {
                self.transport_failed("transport closed".to_owned());
            }
            _ => self.reconnect_after_transport_closed(),
        }
    }

    fn reconnect_after_transport_closed(&mut self) {
        self.disconnect(
            codes::connecting::TRANSPORT_CLOSED,
            "transport closed",
            true,
        );
    }

    fn frame(&mut self, data: &[u8]) {
        let Ok(replies) = json::decode(data) else {
            // Not client protocol data, e.g. from a misconfigured proxy.
            self.reconnect_after_transport_closed();
            return;
        };
        for reply in replies {
            self.reply(reply);
            if self.transport.is_none() {
                // The reply ended this transport: the rest belongs to it.
                return;
            }
        }
    }

    fn reply(&mut self, reply: Reply) {
        if self.state != State::Connecting
            || reply.id == 0
            || self.connect_command != Some(reply.id)
        {
            return;
        }
        self.connect_command = None;
        match (reply.error, reply.result) {
            (Some(error), _) => self.connect_error(&error),
            (None, ReplyResult::Connect(result)) => self.connected(&result),
            // A reply to connect without its result breaks the protocol.
            (None, _) => self.reconnect_after_transport_closed(),
        }
    }

    fn connected(&mut self, result: &ConnectResult) {
        self.actions
            .push(Action::CancelTimer(Timer::ConnectTimeout));
        self.backoff.reset();
        self.state = State::Connected;
        self.emit(ClientEvent::Connected {
            client_id: result.client.clone(),
            transport: TRANSPORT.to_owned(),
        });
    }

    /// centrifuge-js `_connectError`: temporary errors are retried.
    fn connect_error(&mut self, error: &Error) {
        const TOKEN_EXPIRED: u32 = 109;
        if error.code < 100 || error.temporary || error.code == TOKEN_EXPIRED {
            self.emit(ClientEvent::Error(ClientError {
                kind: ErrorKind::Connect,
                code: error.code,
                message: error.message.clone(),
                transport: None,
            }));
            self.disconnect(error.code, &error.message, true);
        } else {
            self.disconnect(error.code, &error.message, false);
        }
    }

    /// centrifuge-js `_disconnect`: tears the connection down, then moves to
    /// connecting (`reconnect`) or disconnected. An event is emitted only on
    /// an actual state change.
    fn disconnect(&mut self, code: u32, reason: &str, reconnect: bool) {
        if self.state == State::Disconnected {
            return;
        }
        if let Some(transport) = self.transport.take() {
            self.actions.push(Action::CloseTransport(transport));
        }
        self.transport_open = false;
        self.connect_command = None;
        self.actions
            .push(Action::CancelTimer(Timer::ConnectTimeout));
        self.actions.push(Action::CancelTimer(Timer::Reconnect));

        let state = if reconnect {
            State::Connecting
        } else {
            State::Disconnected
        };
        if self.state != state {
            self.state = state;
            let reason = reason.to_owned();
            self.emit(if reconnect {
                ClientEvent::Connecting { code, reason }
            } else {
                ClientEvent::Disconnected { code, reason }
            });
        }
        if reconnect {
            let delay = self.backoff.next_delay(&mut self.rng);
            self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
        }
    }

    fn command_id(&mut self) -> u32 {
        self.next_command = self.next_command.wrapping_add(1).max(1);
        self.next_command
    }

    fn emit(&mut self, event: ClientEvent) {
        self.actions.push(Action::Emit(event));
    }
}

/// Spec, "Disconnect codes": 3000-3499 and 4000-4499 advise a reconnect,
/// 3500-3999 and 4500-4999 are terminal. Codes from 5000 are reserved by
/// Centrifuge and reconnect (`client.proto`, `Disconnect`; owner decision).
fn reconnect_advised(code: u32) -> bool {
    !matches!(code, 3500..=3999 | 4500..=4999)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine() -> Machine {
        Machine::with_rng(Config::default(), fastrand::Rng::with_seed(7))
    }

    fn opened_transport(actions: &[Action]) -> TransportId {
        actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport(id) => Some(*id),
                _ => None,
            })
            .expect("an OpenTransport action")
    }

    fn events(actions: &[Action]) -> Vec<&ClientEvent> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Emit(event) => Some(event),
                _ => None,
            })
            .collect()
    }

    /// A machine whose connect command was sent over `transport`.
    fn awaiting_connect_reply() -> (Machine, TransportId) {
        let mut machine = machine();
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        (machine, transport)
    }

    fn frame(transport: TransportId, json: &str) -> Input {
        Input::Frame {
            transport,
            data: json.as_bytes().to_vec(),
        }
    }

    #[test]
    fn each_failed_attempt_emits_exactly_one_error_and_schedules_a_retry() {
        let mut machine = machine();
        let mut transport = opened_transport(&machine.handle(Input::Connect));

        for _ in 0..3 {
            let actions = machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            });

            let events = events(&actions);
            assert_eq!(events.len(), 1, "{actions:?}");
            assert!(matches!(events[0], ClientEvent::Error(e) if e.kind == ErrorKind::Transport));
            assert!(matches!(
                actions.last(),
                Some(Action::SetTimer(Timer::Reconnect, _))
            ));
            transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        }
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn connect_timeout_fails_the_attempt() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(Input::TimerFired(Timer::ConnectTimeout));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError { message, .. })] if message == "connect timeout"
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn inputs_about_an_abandoned_transport_are_ignored() {
        let (mut machine, stale) = awaiting_connect_reply();
        machine.handle(Input::TimerFired(Timer::ConnectTimeout));
        opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));

        let actions = machine.handle(frame(stale, r#"{"id":1,"connect":{"client":"c"}}"#));

        assert_eq!(actions, []);
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn temporary_connect_error_is_reported_and_retried() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":400,"message":"busy","temporary":true}}"#,
        ));

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::Connect,
                code: 400,
                ..
            })]
        ));
        assert_eq!(machine.state(), State::Connecting);
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
    }

    #[test]
    fn permanent_connect_error_disconnects_with_its_code() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":403,"message":"forbidden"}}"#,
        ));

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: 403,
                reason: "forbidden".into()
            }]
        );
        assert_eq!(machine.state(), State::Disconnected);
        assert!(!actions.iter().any(|a| matches!(a, Action::SetTimer(..))));
    }

    #[test]
    fn undecodable_frame_closes_the_transport_and_reconnects() {
        let (mut machine, transport) = awaiting_connect_reply();

        let actions = machine.handle(frame(transport, "<html>captive portal</html>"));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn transport_closed_while_connected_reconnects_with_transport_closed_code() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::TRANSPORT_CLOSED,
                reason: "transport closed".into()
            }]
        );
    }

    #[test]
    fn reconnect_advice_follows_the_spec_ranges() {
        let advised: Vec<_> = [3000, 3499, 3500, 3999, 4000, 4499, 4500, 4999, 5000, 6000]
            .into_iter()
            .map(|code| (code, reconnect_advised(code)))
            .collect();

        assert_eq!(
            advised,
            [
                (3000, true),
                (3499, true),
                (3500, false),
                (3999, false),
                (4000, true),
                (4499, true),
                (4500, false),
                (4999, false),
                (5000, true),
                (6000, true),
            ]
        );
    }

    #[test]
    fn terminal_server_code_while_connected_disconnects_without_retry() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: Some(3500),
            reason: "bye".into(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Disconnected {
                code: 3500,
                reason: "bye".into()
            }]
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::SetTimer(..))));
    }

    #[test]
    fn close_codes_below_3000_are_transport_closures() {
        let (mut machine, transport) = awaiting_connect_reply();
        machine.handle(frame(transport, r#"{"id":1,"connect":{"client":"c"}}"#));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: Some(1006),
            reason: "abnormal".into(),
        });

        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::TRANSPORT_CLOSED,
                reason: "transport closed".into()
            }]
        );
    }

    #[test]
    fn connect_while_connecting_does_nothing() {
        let mut machine = machine();
        machine.handle(Input::Connect);

        assert_eq!(machine.handle(Input::Connect), []);
    }

    #[test]
    fn disconnect_while_disconnected_does_nothing() {
        assert_eq!(machine().handle(Input::Disconnect), []);
    }

    #[test]
    fn successful_connect_resets_the_backoff() {
        let mut machine = Machine::with_rng(
            Config {
                min_reconnect_delay: Duration::from_millis(100),
                max_reconnect_delay: Duration::from_secs(60),
                ..Config::default()
            },
            fastrand::Rng::with_seed(7),
        );
        let mut transport = opened_transport(&machine.handle(Input::Connect));
        for _ in 0..10 {
            machine.handle(Input::TransportFailed {
                transport,
                message: "refused".into(),
            });
            transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        }
        machine.handle(Input::TransportOpened(transport));
        let id = machine.connect_command.expect("connect sent");
        machine.handle(frame(
            transport,
            &format!(r#"{{"id":{id},"connect":{{"client":"c"}}}}"#),
        ));

        let actions = machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let Some(Action::SetTimer(Timer::Reconnect, delay)) = actions.last() else {
            panic!("no reconnect timer in {actions:?}");
        };
        assert!(*delay <= Duration::from_millis(200), "{delay:?}");
    }
}
