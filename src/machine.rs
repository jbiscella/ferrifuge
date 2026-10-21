//! Sans-IO core: the client connection state machine.
//!
//! A [`Machine`] owns no sockets, timers or tasks. Its owner feeds it
//! [`Input`]s and performs the returned [`Action`]s in order. The tokio
//! adapter is one such owner; any other runtime can drive it the same way.
//!
//! Behaviour follows the Client SDK spec, with centrifuge-js as the
//! implementation reference (`src/centrifuge.ts`).

use std::time::Duration;

use crate::backoff::{self, Backoff};
use crate::token::Secret;
use crate::wire::{
    Command, ConnectRequest, ConnectResult, Error, Method, RefreshRequest, RefreshResult, Reply,
    ReplyResult,
};
use crate::{ClientError, ClientEvent, Config, ErrorKind, State, TokenError, codes, json};

/// Name of the only transport implemented so far.
const TRANSPORT: &str = "websocket";

/// Identifies one transport the machine asked to open. Inputs about a
/// transport the machine has since abandoned are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportId(u64);

/// Identifies one token fetch the machine asked for. Results of fetches the
/// machine has since abandoned are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchId(u64);

/// Timers the machine sets. At most one of each kind runs at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timer {
    /// Delay before the next connection attempt.
    Reconnect,
    /// Deadline for a connection attempt to get its connect reply.
    ConnectTimeout,
    /// When the connection token must be refreshed.
    Refresh,
    /// Deadline for the refresh command to get its reply.
    RefreshReply,
    /// Deadline for data from a server that announced pings.
    ServerPing,
}

impl Timer {
    /// Number of timer kinds.
    pub const COUNT: usize = 5;

    /// Every timer kind, in [`Timer::index`] order.
    pub const ALL: [Timer; Self::COUNT] = [
        Self::Reconnect,
        Self::ConnectTimeout,
        Self::Refresh,
        Self::RefreshReply,
        Self::ServerPing,
    ];

    /// Position in [`Timer::ALL`], e.g. for an owner keeping one slot per
    /// kind, as the tokio adapter does.
    #[must_use]
    pub fn index(self) -> usize {
        // Exhaustive: a new kind fails to compile until it gets an index
        // (and a place in `ALL`, checked by a test).
        match self {
            Self::Reconnect => 0,
            Self::ConnectTimeout => 1,
            Self::Refresh => 2,
            Self::RefreshReply => 3,
            Self::ServerPing => 4,
        }
    }
}

/// Why the machine needs a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchPurpose {
    Connect,
    Refresh,
}

/// centrifuge-js `_getRefreshRetryDelay`: 5 to 10 s.
const REFRESH_RETRY_MIN: Duration = Duration::from_secs(5);
const REFRESH_RETRY_MAX: Duration = Duration::from_secs(10);
/// Server error code of an expired connection token.
const TOKEN_EXPIRED: u32 = 109;

/// Something that happened, for the machine to react to.
///
/// Its `Debug` output redacts a fetched token.
#[derive(Clone, PartialEq, Eq)]
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
    /// The token callback answered a [`Action::FetchToken`].
    TokenFetched {
        /// The fetch answered.
        fetch: FetchId,
        /// The new token, or why there is none.
        result: Result<String, TokenError>,
    },
}

/// Something the owner of the machine must do.
///
/// Its `Debug` output shows only the length of a [`Action::Send`] frame:
/// connect and refresh frames carry the connection token.
#[derive(Clone, PartialEq, Eq)]
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
    /// Ask the application's token callback for a new connection token;
    /// report the answer with [`Input::TokenFetched`]. Only issued by a
    /// machine created with [`Machine::with_token_source`].
    FetchToken(FetchId),
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect => f.write_str("Connect"),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::TransportOpened(id) => f.debug_tuple("TransportOpened").field(id).finish(),
            Self::TransportFailed { transport, message } => f
                .debug_struct("TransportFailed")
                .field("transport", transport)
                .field("message", message)
                .finish(),
            Self::TransportClosed {
                transport,
                code,
                reason,
            } => f
                .debug_struct("TransportClosed")
                .field("transport", transport)
                .field("code", code)
                .field("reason", reason)
                .finish(),
            Self::Frame { transport, data } => f
                .debug_struct("Frame")
                .field("transport", transport)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::TimerFired(timer) => f.debug_tuple("TimerFired").field(timer).finish(),
            Self::TokenFetched { fetch, result } => f
                .debug_struct("TokenFetched")
                .field("fetch", fetch)
                .field(
                    "result",
                    &result.as_ref().map(|token| Secret(token.clone())),
                )
                .finish(),
        }
    }
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenTransport(id) => f.debug_tuple("OpenTransport").field(id).finish(),
            Self::CloseTransport(id) => f.debug_tuple("CloseTransport").field(id).finish(),
            Self::Send { transport, frame } => f
                .debug_struct("Send")
                .field("transport", transport)
                .field("frame", &format_args!("<{} bytes>", frame.len()))
                .finish(),
            Self::SetTimer(timer, delay) => {
                f.debug_tuple("SetTimer").field(timer).field(delay).finish()
            }
            Self::CancelTimer(timer) => f.debug_tuple("CancelTimer").field(timer).finish(),
            Self::Emit(event) => f.debug_tuple("Emit").field(event).finish(),
            Self::FetchToken(id) => f.debug_tuple("FetchToken").field(id).finish(),
        }
    }
}

/// Server pings announced in the connect reply.
#[derive(Debug, Clone, Copy)]
struct Pings {
    interval: Duration,
    /// Whether the server expects a pong for each ping.
    pong: bool,
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
    /// Current connection token; empty for none.
    token: Secret,
    /// Token sent with the pending connect command.
    connect_token: Secret,
    /// Whether the owner answers [`Action::FetchToken`].
    token_source: bool,
    /// The server rejected the token as expired: get a new one first.
    refresh_required: bool,
    fetch: Option<(FetchId, FetchPurpose)>,
    next_fetch: u64,
    /// Id of the refresh command awaiting its reply.
    refresh_command: Option<u32>,
    /// Pings announced by the server for the current connection.
    pings: Option<Pings>,
    actions: Vec<Action>,
}

impl Machine {
    /// Creates a disconnected machine whose owner has no token callback: an
    /// expiring token cannot be refreshed and ends the connection.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_rng(config, false, fastrand::Rng::new())
    }

    /// Creates a disconnected machine whose owner answers
    /// [`Action::FetchToken`] from the application's token callback.
    #[must_use]
    pub fn with_token_source(config: Config) -> Self {
        Self::with_rng(config, true, fastrand::Rng::new())
    }

    fn with_rng(config: Config, token_source: bool, rng: fastrand::Rng) -> Self {
        let backoff = Backoff::new(config.min_reconnect_delay, config.max_reconnect_delay);
        let token = Secret(config.token.clone());
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
            token,
            connect_token: Secret::default(),
            token_source,
            refresh_required: false,
            fetch: None,
            next_fetch: 0,
            refresh_command: None,
            pings: None,
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
            Input::TimerFired(Timer::Refresh) => {
                let in_flight = self.fetch.is_some() || self.refresh_command.is_some();
                if self.state == State::Connected && !in_flight {
                    self.refresh();
                }
            }
            Input::TimerFired(Timer::ServerPing) => {
                if self.state == State::Connected {
                    self.disconnect(codes::connecting::NO_PING, "no ping", true);
                }
            }
            Input::TimerFired(Timer::RefreshReply) => {
                if self.state == State::Connected && self.refresh_command.take().is_some() {
                    // centrifuge-js: a command timeout is error code 1.
                    self.refresh_error(&Error {
                        code: codes::error::TIMEOUT,
                        message: "timeout".to_owned(),
                        temporary: false,
                    });
                }
            }
            Input::TokenFetched { fetch, result } => {
                if let Some((id, purpose)) = self.fetch
                    && id == fetch
                {
                    self.fetch = None;
                    self.token_fetched(purpose, result);
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
        // centrifuge-js `_startReconnecting`: get a token first when the
        // last one expired, or when there is none but a callback.
        let token_needed = self.refresh_required || (self.token.0.is_empty() && self.token_source);
        if !token_needed {
            self.open_transport();
        } else if self.token_source {
            self.fetch_token(FetchPurpose::Connect);
        } else {
            self.no_token_source();
        }
    }

    fn open_transport(&mut self) {
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
        self.connect_token.clone_from(&self.token);
        let command = Command {
            id,
            method: Method::Connect(ConnectRequest {
                token: self.token.0.clone(),
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
        // Any data from the server proves the connection alive, as in
        // centrifuge-js `_dataReceived`.
        if !replies.is_empty() {
            self.wait_server_ping();
        }
        for reply in replies {
            self.reply(reply);
            if self.transport.is_none() {
                // The reply ended this transport: the rest belongs to it.
                return;
            }
        }
    }

    fn reply(&mut self, reply: Reply) {
        if reply.id == 0 {
            // An empty reply is a server ping (client.proto, Reply).
            if self.state == State::Connected
                && self.pings.is_some_and(|pings| pings.pong)
                && reply.error.is_none()
                && reply.result == ReplyResult::Empty
            {
                self.pong();
            }
            return;
        }
        if self.state == State::Connected && self.refresh_command == Some(reply.id) {
            self.refresh_command = None;
            self.actions.push(Action::CancelTimer(Timer::RefreshReply));
            match (reply.error, reply.result) {
                (Some(error), _) => self.refresh_error(&error),
                (None, ReplyResult::Refresh(result)) => self.refreshed(&result),
                // A reply to refresh without its result breaks the protocol.
                (None, _) => self.reconnect_after_transport_closed(),
            }
            return;
        }
        if self.state != State::Connecting || self.connect_command != Some(reply.id) {
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
        self.refresh_required = false;
        if result.expires {
            self.set_refresh_timer(result.ttl);
        }
        self.state = State::Connected;
        self.pings = (result.ping > 0).then(|| Pings {
            interval: Duration::from_secs(result.ping.into()),
            pong: result.pong,
        });
        self.wait_server_ping();
        self.emit(ClientEvent::Connected {
            client_id: result.client.clone(),
            transport: TRANSPORT.to_owned(),
        });
    }

    /// centrifuge-js `_connectError`: temporary errors are retried.
    fn connect_error(&mut self, error: &Error) {
        // Unless the application set another token meanwhile.
        if error.code == TOKEN_EXPIRED && self.connect_token == self.token {
            self.refresh_required = true;
        }
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
        self.refresh_command = None;
        // A refresh fetch outlives a reconnect: the application's answer,
        // in particular a denial, must still apply.
        if !(reconnect && matches!(self.fetch, Some((_, FetchPurpose::Refresh)))) {
            self.fetch = None;
        }
        // Every timer belongs to the connection being torn down; the
        // reconnect timer is set again below when reconnecting.
        for timer in Timer::ALL {
            self.actions.push(Action::CancelTimer(timer));
        }
        self.pings = None;

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

    fn fetch_token(&mut self, purpose: FetchPurpose) {
        let id = FetchId(self.next_fetch);
        self.next_fetch += 1;
        self.fetch = Some((id, purpose));
        self.actions.push(Action::FetchToken(id));
    }

    /// The refresh timer fired while connected.
    fn refresh(&mut self) {
        if self.token_source {
            self.fetch_token(FetchPurpose::Refresh);
        } else {
            self.no_token_source();
        }
    }

    /// centrifuge-js `_getToken` without `getToken`: a configuration error,
    /// then the client stops as unauthorized.
    fn no_token_source(&mut self) {
        self.emit(ClientEvent::Error(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: "token expired but no token callback is set".to_owned(),
            transport: None,
        }));
        self.unauthorized();
    }

    fn token_fetched(&mut self, purpose: FetchPurpose, result: Result<String, TokenError>) {
        match result {
            // An empty token denies access too, as in centrifuge-js.
            Ok(token) if token.is_empty() => self.unauthorized(),
            Err(TokenError::Unauthorized) => self.unauthorized(),
            Ok(token) => {
                self.token = Secret(token);
                match purpose {
                    FetchPurpose::Connect => {
                        if self.state == State::Connecting && self.transport.is_none() {
                            self.refresh_required = false;
                            self.open_transport();
                        }
                    }
                    // While reconnecting, the new token is kept for the next
                    // attempt; once connected again, it is sent.
                    FetchPurpose::Refresh if self.state == State::Connected => self.send_refresh(),
                    FetchPurpose::Refresh => {}
                }
            }
            Err(TokenError::Failed(message)) => match purpose {
                FetchPurpose::Connect => {
                    self.emit(ClientEvent::Error(ClientError {
                        kind: ErrorKind::ConnectToken,
                        code: codes::error::CONNECT_TOKEN,
                        message,
                        transport: None,
                    }));
                    let delay = self.backoff.next_delay(&mut self.rng);
                    self.actions.push(Action::SetTimer(Timer::Reconnect, delay));
                }
                // Reconnecting: the next connection gets its own refresh timer.
                FetchPurpose::Refresh if self.state != State::Connected => {}
                FetchPurpose::Refresh => {
                    self.emit(ClientEvent::Error(ClientError {
                        kind: ErrorKind::RefreshToken,
                        code: codes::error::REFRESH_TOKEN,
                        message,
                        transport: None,
                    }));
                    self.retry_refresh();
                }
            },
        }
    }

    fn send_refresh(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        // The reply re-arms it; a timer armed meanwhile, e.g. by a reconnect
        // while the token was being fetched, must not start another refresh.
        self.actions.push(Action::CancelTimer(Timer::Refresh));
        let id = self.command_id();
        self.refresh_command = Some(id);
        let command = Command {
            id,
            method: Method::Refresh(RefreshRequest {
                token: self.token.0.clone(),
            }),
        };
        self.actions.push(Action::Send {
            transport,
            frame: json::encode(&[command]),
        });
        self.actions
            .push(Action::SetTimer(Timer::RefreshReply, self.config.timeout));
    }

    fn refreshed(&mut self, result: &RefreshResult) {
        if result.expires {
            self.set_refresh_timer(result.ttl);
        }
    }

    /// centrifuge-js `_refreshError`: temporary errors are retried.
    fn refresh_error(&mut self, error: &Error) {
        if error.code < 100 || error.temporary {
            self.emit(ClientEvent::Error(ClientError {
                kind: ErrorKind::Refresh,
                code: error.code,
                message: error.message.clone(),
                transport: None,
            }));
            self.retry_refresh();
        } else {
            self.disconnect(error.code, &error.message, false);
        }
    }

    fn retry_refresh(&mut self) {
        let delay = backoff::delay(0, REFRESH_RETRY_MIN, REFRESH_RETRY_MAX, &mut self.rng);
        self.actions.push(Action::SetTimer(Timer::Refresh, delay));
    }

    fn set_refresh_timer(&mut self, ttl_seconds: u32) {
        self.actions
            .push(Action::SetTimer(Timer::Refresh, refresh_delay(ttl_seconds)));
    }

    /// centrifuge-js `_waitServerPing`: (re)arms the deadline for data from a
    /// server that announced pings; disabled by a zero `max_server_ping_delay`.
    fn wait_server_ping(&mut self) {
        let Some(pings) = self.pings else {
            return;
        };
        if self.state != State::Connected || self.config.max_server_ping_delay.is_zero() {
            return;
        }
        let deadline = pings
            .interval
            .saturating_add(self.config.max_server_ping_delay);
        self.actions
            .push(Action::SetTimer(Timer::ServerPing, deadline));
    }

    fn pong(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        self.actions.push(Action::Send {
            transport,
            frame: json::encode(&[Command {
                id: 0,
                method: Method::Pong,
            }]),
        });
    }

    fn unauthorized(&mut self) {
        self.disconnect(codes::disconnected::UNAUTHORIZED, "unauthorized", false);
    }

    fn command_id(&mut self) -> u32 {
        self.next_command = self.next_command.wrapping_add(1).max(1);
        self.next_command
    }

    fn emit(&mut self, event: ClientEvent) {
        self.actions.push(Action::Emit(event));
    }
}

/// When to start refreshing a connection that expires in `ttl_seconds`.
///
/// `client.proto` (`ConnectResult.expires`) requires the refresh to be sent
/// before the ttl elapses, so it starts early enough for the token callback
/// and the round trip: a margin of 10% of the ttl, at most 30 s. centrifuge-js
/// waits for the full ttl; the proto takes precedence (see CLAUDE.md).
fn refresh_delay(ttl_seconds: u32) -> Duration {
    const MAX_MARGIN: Duration = Duration::from_secs(30);
    let ttl = Duration::from_secs(ttl_seconds.into());
    // Cannot underflow: the margin is at most a tenth of the ttl.
    ttl.saturating_sub((ttl / 10).min(MAX_MARGIN))
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
        Machine::with_rng(Config::default(), false, fastrand::Rng::with_seed(7))
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

    fn token_machine(token: &str) -> Machine {
        Machine::with_rng(
            Config {
                token: token.to_owned(),
                ..Config::default()
            },
            true,
            fastrand::Rng::with_seed(7),
        )
    }

    fn fetch_requested(actions: &[Action]) -> FetchId {
        actions
            .iter()
            .find_map(|action| match action {
                Action::FetchToken(id) => Some(*id),
                _ => None,
            })
            .expect("a FetchToken action")
    }

    /// A token machine connected over the returned transport; the connect
    /// reply carried `expires`/`ttl`.
    fn connected_with_token(ttl: u32) -> (Machine, TransportId, Vec<Action>) {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        let actions = machine.handle(frame(
            transport,
            &format!(r#"{{"id":1,"connect":{{"client":"c","expires":true,"ttl":{ttl}}}}}"#),
        ));
        (machine, transport, actions)
    }

    fn refresh_timer(actions: &[Action]) -> Option<Duration> {
        actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::Refresh, delay) => Some(*delay),
            _ => None,
        })
    }

    #[test]
    fn refresh_starts_before_the_ttl_with_a_capped_margin() {
        let delays: Vec<_> = [0, 1, 3, 30, 300, 3600]
            .into_iter()
            .map(|ttl| (ttl, refresh_delay(ttl)))
            .collect();

        assert_eq!(
            delays,
            [
                (0, Duration::ZERO),
                (1, Duration::from_millis(900)),
                (3, Duration::from_millis(2700)),
                (30, Duration::from_secs(27)),
                (300, Duration::from_secs(270)),
                (3600, Duration::from_secs(3570)),
            ]
        );
    }

    #[test]
    fn expiring_connection_arms_the_refresh_timer_before_its_ttl() {
        let (_, _, actions) = connected_with_token(30);

        assert_eq!(refresh_timer(&actions), Some(Duration::from_secs(27)));
    }

    #[test]
    fn refresh_sends_the_fetched_token_and_rearms_on_reply() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

        let sent = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        let rearmed = machine.handle(frame(
            transport,
            r#"{"id":2,"refresh":{"expires":true,"ttl":60}}"#,
        ));

        assert_eq!(
            sent,
            [
                Action::CancelTimer(Timer::Refresh),
                Action::Send {
                    transport,
                    frame: br#"{"id":2,"refresh":{"token":"t1"}}"#.to_vec()
                },
                Action::SetTimer(Timer::RefreshReply, Config::default().timeout)
            ]
        );
        assert_eq!(refresh_timer(&rearmed), Some(Duration::from_secs(54)));
    }

    #[test]
    fn unanswered_refresh_times_out_and_is_retried() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        let sent = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        assert!(sent.contains(&Action::SetTimer(
            Timer::RefreshReply,
            Config::default().timeout
        )));

        let actions = machine.handle(Input::TimerFired(Timer::RefreshReply));
        let late = machine.handle(frame(
            transport,
            r#"{"id":2,"refresh":{"expires":true,"ttl":60}}"#,
        ));

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::Refresh,
                code: codes::error::TIMEOUT,
                ..
            })]
        ));
        assert!(refresh_timer(&actions).is_some(), "{actions:?}");
        assert_eq!(late, [], "a reply after the timeout is ignored");
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn denial_arriving_after_the_transport_dropped_still_disconnects() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Unauthorized),
        });

        assert_eq!(machine.state(), State::Disconnected, "{actions:?}");
        assert!(events(&actions).contains(&&ClientEvent::Disconnected {
            code: codes::disconnected::UNAUTHORIZED,
            reason: "unauthorized".into()
        }));
    }

    #[test]
    fn debug_output_of_send_actions_shows_only_the_frame_length() {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TransportOpened(transport));

        let Action::Send { frame, .. } = &actions[0] else {
            panic!("{actions:?}");
        };
        assert_eq!(
            format!("{:?}", actions[0]),
            format!(
                "Send {{ transport: {transport:?}, frame: <{} bytes> }}",
                frame.len()
            )
        );
    }

    #[test]
    fn debug_output_of_inputs_never_shows_fetched_tokens() {
        let input = Input::TokenFetched {
            fetch: FetchId(0),
            result: Ok("secret-token".into()),
        };

        let printed = format!("{input:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    #[test]
    fn late_refresh_token_after_reconnect_replaces_the_new_refresh_timer() {
        let (mut machine, transport, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
        machine.handle(Input::TransportClosed {
            transport,
            code: None,
            reason: String::new(),
        });
        let transport = opened_transport(&machine.handle(Input::TimerFired(Timer::Reconnect)));
        machine.handle(Input::TransportOpened(transport));
        let reconnected = machine.handle(frame(
            transport,
            r#"{"id":2,"connect":{"client":"c2","expires":true,"ttl":30}}"#,
        ));
        assert!(refresh_timer(&reconnected).is_some());

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });

        assert!(
            matches!(
                &actions[..],
                [
                    Action::CancelTimer(Timer::Refresh),
                    Action::Send { .. },
                    Action::SetTimer(Timer::RefreshReply, _)
                ]
            ),
            "{actions:?}"
        );
        assert_eq!(
            machine.handle(Input::TimerFired(Timer::Refresh)),
            [],
            "no second refresh while one is in flight"
        );
    }

    #[test]
    fn debug_output_never_shows_tokens() {
        let (machine, _, _) = connected_with_token(30);
        let config = Config {
            token: "secret-token".into(),
            ..Config::default()
        };

        let printed = format!("{config:?} {machine:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
        assert!(!printed.contains("\"t0\""), "{printed}");
    }

    #[test]
    fn denied_or_empty_token_disconnects_as_unauthorized() {
        for result in [Err(TokenError::Unauthorized), Ok(String::new())] {
            let (mut machine, _, _) = connected_with_token(30);
            let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

            let actions = machine.handle(Input::TokenFetched { fetch, result });

            assert_eq!(
                events(&actions),
                [&ClientEvent::Disconnected {
                    code: codes::disconnected::UNAUTHORIZED,
                    reason: "unauthorized".into()
                }]
            );
        }
    }

    #[test]
    fn failed_refresh_fetch_is_reported_and_retried_in_5_to_10_s() {
        let (mut machine, _, _) = connected_with_token(30);
        let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Failed("backend down".into())),
        });

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::RefreshToken,
                code: 6,
                ..
            })]
        ));
        let retry = refresh_timer(&actions).expect("retry timer");
        assert!(
            (REFRESH_RETRY_MIN..=REFRESH_RETRY_MAX).contains(&retry),
            "{retry:?}"
        );
        assert_eq!(machine.state(), State::Connected);
    }

    #[test]
    fn refresh_reply_errors_retry_when_temporary_and_disconnect_otherwise() {
        for (error, expect_disconnect) in [
            (r#"{"code":400,"message":"busy","temporary":true}"#, false),
            (r#"{"code":403,"message":"forbidden"}"#, true),
        ] {
            let (mut machine, transport, _) = connected_with_token(30);
            let fetch = fetch_requested(&machine.handle(Input::TimerFired(Timer::Refresh)));
            machine.handle(Input::TokenFetched {
                fetch,
                result: Ok("t1".into()),
            });

            let actions =
                machine.handle(frame(transport, &format!(r#"{{"id":2,"error":{error}}}"#)));

            if expect_disconnect {
                assert_eq!(machine.state(), State::Disconnected, "{actions:?}");
            } else {
                assert!(refresh_timer(&actions).is_some(), "{actions:?}");
                assert_eq!(machine.state(), State::Connected);
            }
        }
    }

    #[test]
    fn expiring_token_without_callback_is_a_configuration_error_then_unauthorized() {
        let mut machine = awaiting_connect_reply().0;
        let transport = TransportId(0);
        machine.handle(frame(
            transport,
            r#"{"id":1,"connect":{"client":"c","expires":true,"ttl":30}}"#,
        ));

        let actions = machine.handle(Input::TimerFired(Timer::Refresh));

        assert!(matches!(
            events(&actions)[..],
            [
                ClientEvent::Error(ClientError {
                    kind: ErrorKind::Configuration,
                    code: 12,
                    ..
                }),
                ClientEvent::Disconnected { code: 1, .. }
            ]
        ));
    }

    #[test]
    fn without_a_token_the_first_attempt_fetches_one() {
        let mut machine = token_machine("");

        let fetch = fetch_requested(&machine.handle(Input::Connect));
        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });
        let transport = opened_transport(&actions);
        let sent = machine.handle(Input::TransportOpened(transport));

        assert!(
            matches!(&sent[..], [Action::Send { frame, .. }] if frame.windows(4).any(|w| w == b"\"t1\"")),
            "{sent:?}"
        );
    }

    #[test]
    fn expired_token_error_makes_the_next_attempt_fetch_a_new_token() {
        let mut machine = token_machine("t0");
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        machine.handle(frame(
            transport,
            r#"{"id":1,"error":{"code":109,"message":"token expired"}}"#,
        ));

        let actions = machine.handle(Input::TimerFired(Timer::Reconnect));

        fetch_requested(&actions);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::OpenTransport(_)))
        );
    }

    #[test]
    fn failed_connect_fetch_is_reported_and_retried_with_backoff() {
        let mut machine = token_machine("");
        let fetch = fetch_requested(&machine.handle(Input::Connect));

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Err(TokenError::Failed("backend down".into())),
        });

        assert!(matches!(
            events(&actions)[..],
            [ClientEvent::Error(ClientError {
                kind: ErrorKind::ConnectToken,
                code: 5,
                ..
            })]
        ));
        assert!(matches!(
            actions.last(),
            Some(Action::SetTimer(Timer::Reconnect, _))
        ));
        assert_eq!(machine.state(), State::Connecting);
    }

    #[test]
    fn token_fetched_after_disconnect_is_ignored() {
        let mut machine = token_machine("");
        let fetch = fetch_requested(&machine.handle(Input::Connect));
        machine.handle(Input::Disconnect);

        let actions = machine.handle(Input::TokenFetched {
            fetch,
            result: Ok("t1".into()),
        });

        assert_eq!(actions, []);
    }

    /// A machine connected to a server announcing `ping` seconds; returns
    /// the actions of the connect reply.
    fn connected_with_ping(ping: u32, max_delay: Duration) -> (Machine, TransportId, Vec<Action>) {
        let mut machine = Machine::with_rng(
            Config {
                max_server_ping_delay: max_delay,
                ..Config::default()
            },
            false,
            fastrand::Rng::with_seed(7),
        );
        let transport = opened_transport(&machine.handle(Input::Connect));
        machine.handle(Input::TransportOpened(transport));
        let actions = machine.handle(frame(
            transport,
            &format!(r#"{{"id":1,"connect":{{"client":"c","ping":{ping},"pong":true}}}}"#),
        ));
        (machine, transport, actions)
    }

    fn server_ping_timer(actions: &[Action]) -> Option<Duration> {
        actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::ServerPing, delay) => Some(*delay),
            _ => None,
        })
    }

    #[test]
    fn ping_deadline_is_interval_plus_max_delay_and_off_without_pings_or_delay() {
        let (_, _, with_pings) = connected_with_ping(25, Duration::from_secs(10));
        let (_, _, no_pings) = connected_with_ping(0, Duration::from_secs(10));
        let (_, _, check_off) = connected_with_ping(25, Duration::ZERO);

        assert_eq!(
            server_ping_timer(&with_pings),
            Some(Duration::from_secs(35))
        );
        assert_eq!(server_ping_timer(&no_pings), None);
        assert_eq!(server_ping_timer(&check_off), None);
    }

    #[test]
    fn any_frame_from_the_server_rearms_the_ping_deadline() {
        let (mut machine, transport, _) = connected_with_ping(25, Duration::from_secs(10));

        let actions = machine.handle(frame(transport, r#"{"push":{"channel":"c","pub":{}}}"#));

        assert_eq!(server_ping_timer(&actions), Some(Duration::from_secs(35)));
    }

    #[test]
    fn missing_ping_reconnects_with_no_ping_code() {
        let (mut machine, transport, _) = connected_with_ping(25, Duration::from_secs(10));

        let actions = machine.handle(Input::TimerFired(Timer::ServerPing));

        assert!(actions.contains(&Action::CloseTransport(transport)));
        assert_eq!(
            events(&actions),
            [&ClientEvent::Connecting {
                code: codes::connecting::NO_PING,
                reason: "no ping".into()
            }]
        );
    }

    #[test]
    fn timer_all_lists_every_kind_at_its_index() {
        let indices: Vec<_> = Timer::ALL.iter().map(|timer| timer.index()).collect();

        assert_eq!(indices, (0..Timer::COUNT).collect::<Vec<_>>());
    }

    #[test]
    fn disconnect_cancels_every_timer() {
        let (mut machine, _) = awaiting_connect_reply();

        let actions = machine.handle(Input::Disconnect);

        for timer in Timer::ALL {
            assert!(
                actions.contains(&Action::CancelTimer(timer)),
                "{timer:?}: {actions:?}"
            );
        }
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
            false,
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
