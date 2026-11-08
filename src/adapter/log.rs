//! Logging of the tokio adapter (Block 21): a span per client and per
//! subscription, and records of what the machine is given and does. Every
//! value goes through a redacting `Debug`, or is a code, a name or a length.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{Span, debug, debug_span, trace, warn};

use super::client::lock;
use super::proxy::ProxyTarget;
use crate::machine::{
    Action, Anomaly, CallId, FetchId, Input, SubscriptionId, SubscriptionTimer, Timer,
    TransportKind,
};
use crate::{
    ClientError, ClientEvent, DataError, ErrorKind, ServerSubscriptionEvent, SubscriptionEvent,
    TokenError,
};

/// The number of the next client built in this process.
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

/// What a client logs with.
#[derive(Debug)]
pub(super) struct Log {
    /// Unique within the process: tells the records of several clients
    /// apart, also through `log`, which drops span fields.
    pub(super) number: u64,
    pub(super) span: Span,
    /// The endpoints, as they may be logged.
    endpoints: Vec<String>,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// The span of each subscription, and its channel for WARN records.
    subscriptions: HashMap<SubscriptionId, (Span, String)>,
    /// The kind of each call not yet settled.
    calls: HashMap<CallId, &'static str>,
    /// What each running callback is, and for which subscription.
    fetches: HashMap<FetchId, (&'static str, Option<SubscriptionId>)>,
    /// The kind of the last transport attempt.
    attempt: Option<TransportKind>,
}

impl Log {
    pub(super) fn new<'a>(endpoints: impl IntoIterator<Item = &'a str>) -> Self {
        let number = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
        Self {
            number,
            span: debug_span!("client", client = number),
            endpoints: endpoints.into_iter().map(endpoint).collect(),
            state: Mutex::new(State::default()),
        }
    }

    /// Opens the span of subscription `id`, to `channel`.
    pub(super) fn subscription(&self, id: SubscriptionId, channel: &str) {
        let span = debug_span!(parent: &self.span, "subscription", channel);
        lock(&self.state)
            .subscriptions
            .insert(id, (span, channel.to_owned()));
    }

    pub(super) fn subscription_removed(&self, id: SubscriptionId) {
        lock(&self.state).subscriptions.remove(&id);
    }

    /// Call `id` was made, of `kind` ([`CallRequest::name`]).
    pub(super) fn call(&self, id: CallId, kind: &'static str) {
        lock(&self.state).calls.insert(id, kind);
    }

    fn span_of(&self, id: SubscriptionId) -> Span {
        self.subscription_of(id).0
    }

    /// The span of `subscription`, or the client's.
    fn parent(&self, subscription: Option<SubscriptionId>) -> Span {
        subscription.map_or_else(|| self.span.clone(), |id| self.span_of(id))
    }

    /// The span of subscription `id` and its channel; the client's span
    /// for a subscription removed.
    fn subscription_of(&self, id: SubscriptionId) -> (Span, Option<String>) {
        match lock(&self.state).subscriptions.get(&id) {
            Some((span, channel)) => (span.clone(), Some(channel.clone())),
            None => (self.span.clone(), None),
        }
    }

    /// What the machine is given.
    pub(super) fn input(&self, input: &Input) {
        trace!(input = ?input, "input");
        let (fetch, outcome) = match input {
            // Forgotten by the machine without a `CallSettled`.
            Input::CallAbandoned(call) => {
                lock(&self.state).calls.remove(call);
                return;
            }
            Input::TokenFetched { fetch, result }
            | Input::SubscriptionTokenFetched { fetch, result } => (fetch, token_outcome(result)),
            Input::ConnectDataFetched { fetch, result }
            | Input::SubscriptionDataFetched { fetch, result } => (fetch, data_outcome(result)),
            Input::SubscriptionStateFetched { fetch, result } => {
                (fetch, if result.is_ok() { "value" } else { "failed" })
            }
            _ => return,
        };
        let Some((kind, subscription)) = lock(&self.state).fetches.remove(fetch) else {
            return;
        };
        debug!(parent: &self.parent(subscription), kind, outcome, "callback ended");
    }

    /// What the machine does.
    pub(super) fn action(&self, action: &Action) {
        trace!(action = ?action, "action");
        match action {
            Action::Emit(_, event) => self.client_event(event),
            Action::EmitSubscription(_, id, event) => self.subscription_event(*id, event),
            Action::OpenTransport { kind, .. } => {
                let previous = lock(&self.state).attempt.replace(*kind);
                if previous.is_some_and(|previous| previous != *kind) {
                    debug!(transport = kind.name(), "trying the next transport");
                }
            }
            Action::SetTimer(Timer::Reconnect, delay) => {
                debug!(delay = ?delay, "reconnect scheduled");
            }
            Action::SetSubscriptionTimer(id, SubscriptionTimer::Resubscribe, delay) => {
                debug!(parent: &self.span_of(*id), delay = ?delay, "resubscribe scheduled");
            }
            Action::FetchToken(fetch) => self.callback_started(*fetch, "token", None),
            Action::FetchConnectData(fetch) => self.callback_started(*fetch, "connect_data", None),
            Action::FetchSubscriptionData {
                subscription,
                fetch,
                ..
            } => self.callback_started(*fetch, "data", Some(*subscription)),
            Action::FetchSubscriptionToken {
                subscription,
                fetch,
                ..
            } => self.callback_started(*fetch, "token", Some(*subscription)),
            Action::FetchSubscriptionState {
                subscription,
                fetch,
                ..
            } => self.callback_started(*fetch, "get_state", Some(*subscription)),
            Action::CancelSubscriptionData(fetch)
            | Action::CancelSubscriptionToken(fetch)
            | Action::CancelSubscriptionState(fetch)
            | Action::CancelConnectData(fetch) => {
                lock(&self.state).fetches.remove(fetch);
            }
            Action::CallSettled { call, result } => {
                let kind = lock(&self.state).calls.remove(call).unwrap_or("call");
                if let Err(error) = result {
                    debug!(kind, code = error.code, reason = %error.message, "call failed");
                }
            }
            Action::Anomaly(anomaly) => self.anomaly(anomaly),
            _ => {}
        }
    }

    /// How many callbacks the log tracks, for tests.
    #[cfg(test)]
    pub(super) fn tracked_fetches(&self) -> usize {
        lock(&self.state).fetches.len()
    }

    /// The driver dropped fetch `fetch` unanswered, as a stale token
    /// callback replaced by a newer stale one: no input will end it.
    pub(super) fn fetch_dropped(&self, fetch: FetchId) {
        lock(&self.state).fetches.remove(&fetch);
    }

    /// The record of a transport attempt (Block 21), made where it opens:
    /// its endpoint and its proxy, as scheme, host and port, `direct`, or
    /// `connector` for a client given one (Block 23, D7; Block 37, D4).
    pub(super) fn attempt(
        &self,
        kind: TransportKind,
        endpoint: usize,
        proxy: &Result<Option<ProxyTarget>, String>,
        connector: bool,
    ) {
        let endpoint = self.endpoints.get(endpoint).map_or("", String::as_str);
        let proxy = match proxy {
            _ if connector => "connector".to_owned(),
            Ok(Some(target)) => target.shown(),
            Ok(None) => "direct".to_owned(),
            Err(_) => "unusable".to_owned(),
        };
        debug!(
            transport = kind.name(),
            endpoint, proxy, "opening transport"
        );
    }

    fn callback_started(
        &self,
        fetch: FetchId,
        kind: &'static str,
        subscription: Option<SubscriptionId>,
    ) {
        lock(&self.state)
            .fetches
            .insert(fetch, (kind, subscription));
        debug!(parent: &self.parent(subscription), kind, "callback started");
    }

    fn anomaly(&self, anomaly: &Anomaly) {
        let transport = lock(&self.state).attempt.map_or("", TransportKind::name);
        match anomaly {
            Anomaly::UndecodableFrame { bytes, .. } => warn!(
                client = self.number,
                transport, bytes, "a frame from the server could not be decoded"
            ),
            Anomaly::ReplyWithoutResult { command, .. } => warn!(
                client = self.number,
                transport,
                kind = command,
                "a reply from the server lacks the result of its command"
            ),
            Anomaly::ReplyBeforeConnect { command, .. } => warn!(
                client = self.number,
                transport,
                kind = command,
                "a reply from the server came before the connect reply"
            ),
        }
    }

    fn client_event(&self, event: &ClientEvent) {
        match event {
            ClientEvent::Connecting { code, reason } => {
                debug!(state = "connecting", code, reason, "client state changed");
            }
            ClientEvent::Connected { transport, .. } => {
                debug!(state = "connected", transport, "client state changed");
            }
            ClientEvent::Disconnected { code, reason } => {
                debug!(state = "disconnected", code, reason, "client state changed");
            }
            ClientEvent::Error(error) => self.error(error, None),
            ClientEvent::ServerSubscription { channel, event } => match event {
                ServerSubscriptionEvent::Subscribing => debug!(
                    channel,
                    state = "subscribing",
                    "server-side subscription state changed"
                ),
                ServerSubscriptionEvent::Subscribed(subscribed) => debug!(
                    channel,
                    state = "subscribed",
                    recovered = subscribed.recovered,
                    "server-side subscription state changed"
                ),
                ServerSubscriptionEvent::Unsubscribed { code, reason } => debug!(
                    channel,
                    state = "unsubscribed",
                    code,
                    reason,
                    "server-side subscription state changed"
                ),
                _ => {}
            },
            _ => {}
        }
    }

    fn subscription_event(&self, id: SubscriptionId, event: &SubscriptionEvent) {
        // Publications, joins and leaves show at TRACE only, as actions: no
        // work for them here, logging off or on (D8).
        if !matches!(
            event,
            SubscriptionEvent::Subscribing { .. }
                | SubscriptionEvent::Subscribed(_)
                | SubscriptionEvent::Unsubscribed { .. }
                | SubscriptionEvent::Error(_)
        ) {
            return;
        }
        let span = self.span_of(id);
        match event {
            SubscriptionEvent::Subscribing { code, reason } => debug!(
                parent: &span,
                state = "subscribing",
                code,
                reason,
                "subscription state changed"
            ),
            SubscriptionEvent::Subscribed(subscribed) => debug!(
                parent: &span,
                state = "subscribed",
                recovered = subscribed.recovered,
                "subscription state changed"
            ),
            SubscriptionEvent::Unsubscribed { code, reason } => debug!(
                parent: &span,
                state = "unsubscribed",
                code,
                reason,
                "subscription state changed"
            ),
            SubscriptionEvent::Error(error) => self.error(error, Some(id)),
            _ => {}
        }
    }

    /// An error event, of the client or of `subscription`. A delta that
    /// cannot be applied is a server anomaly.
    fn error(&self, error: &ClientError, subscription: Option<SubscriptionId>) {
        let span = &self.parent(subscription);
        if error.kind == ErrorKind::Delta {
            let channel = subscription.and_then(|id| self.subscription_of(id).1);
            warn!(
                parent: span,
                client = self.number,
                channel,
                kind = ?error.kind,
                code = error.code,
                reason = %error.message,
                "a delta from the server could not be applied"
            );
        } else {
            debug!(
                parent: span,
                kind = ?error.kind,
                code = error.code,
                reason = %error.message,
                "error"
            );
        }
    }
}

/// How a token callback ended, never its value.
fn token_outcome<T>(result: &Result<T, TokenError>) -> &'static str {
    match result {
        Ok(_) => "value",
        Err(TokenError::Unauthorized) => "denied",
        Err(TokenError::Failed(_)) => "failed",
    }
}

/// How a data callback ended, never its value.
fn data_outcome<T>(result: &Result<T, DataError>) -> &'static str {
    match result {
        Ok(_) => "value",
        Err(DataError::Unauthorized) => "denied",
        Err(DataError::Failed(_)) => "failed",
    }
}

/// An endpoint as it may be logged: scheme, host, port and path, without
/// the userinfo, query and fragment, which may hold a credential (D5).
pub(super) fn endpoint(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut url) => {
            // Fails only for URLs without a host, which then have no
            // userinfo either.
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => "<invalid>".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Log, endpoint};
    use crate::machine::{Input, Machine};
    use crate::{CallRequest, Config};

    /// A call the application drops is forgotten by the machine without a
    /// `CallSettled`: its kind must not stay behind (Codex, #109).
    #[test]
    fn an_abandoned_call_leaves_nothing_behind() {
        let log = Log::new(["ws://localhost:8000/connection/websocket"]);
        let mut machine = Machine::new(Config::default());
        let call = machine.add_call(CallRequest::Rpc {
            method: "m".to_owned(),
            data: Vec::new(),
        });
        log.call(call, "rpc");
        log.input(&Input::CallAbandoned(call));
        assert!(super::lock(&log.state).calls.is_empty());
    }

    #[test]
    fn an_endpoint_is_logged_without_credentials_query_or_fragment() {
        assert_eq!(
            endpoint("ws://user:pw@127.0.0.1:9/connection/websocket?token=t#f"),
            "ws://127.0.0.1:9/connection/websocket"
        );
        assert_eq!(
            endpoint("https://user@example.com/connection/sse?cf_connect=x"),
            "https://example.com/connection/sse"
        );
        assert_eq!(endpoint("not a url"), "<invalid>");
    }
}
