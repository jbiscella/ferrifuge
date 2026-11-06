//! Subscription tokens (Block 10b): fetching one before subscribing,
//! refreshing it with `sub_refresh`, and the failures of both. Behaviour
//! follows centrifuge-js `subscription.ts` (`_subscribe`,
//! `_handleTokenResponse`, `_handleTokenError`, `_refresh`,
//! `_refreshResponse`, `_refreshError`, `_subscribeError`), except where
//! CLAUDE.md decides otherwise: the refresh starts at 80–90% of the ttl
//! (Block 14).

use std::time::Duration;

use super::super::tokens::{FetchPurpose, TOKEN_EXPIRED, refresh_delay};
use super::super::{Action, FetchId, Machine, SubscriptionId, SubscriptionTimer};
use crate::backoff;
use crate::token::Secret;
use crate::wire::{Command, Error, Method, RefreshResult, Reply, ReplyResult, SubRefreshRequest};
use crate::{
    ClientError, ErrorKind, State, SubscriptionEvent, SubscriptionState, TokenError, codes,
};

/// Retry delay of a failed subscription token refresh: centrifuge-js
/// `subscription.ts` `_getRefreshRetryDelay`, 10 to 20 s, twice the delay
/// for connection tokens.
const REFRESH_RETRY_MIN: Duration = Duration::from_secs(10);
const REFRESH_RETRY_MAX: Duration = Duration::from_secs(20);

impl Machine {
    /// Asks the owner for a subscription token.
    pub(super) fn fetch_subscription_token(&mut self, id: SubscriptionId, purpose: FetchPurpose) {
        let fetch = self.subscriptions.new_fetch();
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        sub.tokens.begin_fetch(fetch, purpose);
        let channel = sub.channel.clone();
        self.actions.push(Action::FetchSubscriptionToken {
            subscription: id,
            channel,
            fetch,
        });
    }

    /// The token callback answered. An answer no longer wanted, e.g. after
    /// `unsubscribe()` or a reconnect, changes nothing.
    pub(in crate::machine) fn subscription_token_fetched(
        &mut self,
        fetch: FetchId,
        result: Result<String, TokenError>,
    ) {
        let Some((id, purpose)) = self
            .subscriptions
            .by_id
            .iter_mut()
            .find_map(|(id, sub)| sub.tokens.take_fetch(fetch).map(|purpose| (*id, purpose)))
        else {
            return;
        };
        let sub = self.subscriptions.by_id.get_mut(&id).expect("found above");
        let wanted = match purpose {
            FetchPurpose::Connect => SubscriptionState::Subscribing,
            FetchPurpose::Refresh => SubscriptionState::Subscribed,
        };
        if sub.state != wanted {
            return;
        }
        match result {
            Ok(token) if !token.is_empty() => {
                sub.tokens.store(token, purpose);
                match purpose {
                    // A token obtained: then the data callback, if any.
                    FetchPurpose::Connect => self.continue_subscribe(id),
                    FetchPurpose::Refresh => self.send_sub_refresh(id),
                }
            }
            // An empty token too (centrifuge-js; as Block 5 for connection
            // tokens): no subscribe command, no retry.
            Ok(_) | Err(TokenError::Unauthorized) => self.fail_unauthorized(id),
            Err(TokenError::Failed(message)) => match purpose {
                FetchPurpose::Connect => {
                    self.emit_subscription_error(
                        id,
                        ErrorKind::SubscribeToken,
                        codes::error::SUBSCRIPTION_SUBSCRIBE_TOKEN,
                        message,
                    );
                    self.schedule_resubscribe(id);
                }
                FetchPurpose::Refresh => {
                    self.emit_subscription_error(
                        id,
                        ErrorKind::SubscriptionRefreshToken,
                        codes::error::SUBSCRIPTION_REFRESH_TOKEN,
                        message,
                    );
                    self.retry_subscription_refresh(id);
                }
            },
        }
    }

    /// centrifuge-js `_failUnauthorized`: unsubscribed for good, with an
    /// unsubscribe command if the server has the subscription.
    fn fail_unauthorized(&mut self, id: SubscriptionId) {
        self.set_unsubscribed(id, codes::unsubscribed::UNAUTHORIZED, "unauthorized", true);
    }

    fn emit_subscription_error(
        &mut self,
        id: SubscriptionId,
        kind: ErrorKind,
        code: u32,
        message: String,
    ) {
        self.emit_subscription(
            id,
            SubscriptionEvent::Error(ClientError {
                kind,
                code,
                message,
                transport: None,
            }),
        );
    }

    /// Ends the token work of subscription `id`: a token being fetched, a
    /// refresh scheduled or a `sub_refresh` in flight. The token is kept.
    pub(super) fn end_token_refresh(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if !sub.uses_token() {
            return;
        }
        if let Some(fetch) = sub.tokens.abandon() {
            self.actions.push(Action::CancelSubscriptionToken(fetch));
        }
        for timer in [SubscriptionTimer::Refresh, SubscriptionTimer::RefreshReply] {
            self.actions
                .push(Action::CancelSubscriptionTimer(id, timer));
        }
        self.subscriptions.sub_refreshes.retain(|_, of| *of != id);
    }

    /// Schedules the refresh of a token expiring in `ttl` seconds.
    pub(super) fn schedule_subscription_refresh(&mut self, id: SubscriptionId, ttl: u32) {
        let delay = refresh_delay(ttl, &mut self.rng);
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::Refresh,
            delay,
        ));
    }

    fn retry_subscription_refresh(&mut self, id: SubscriptionId) {
        let delay = backoff::delay(0, REFRESH_RETRY_MIN, REFRESH_RETRY_MAX, &mut self.rng);
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::Refresh,
            delay,
        ));
    }

    /// The refresh timer fired: a new token from the callback, or, without
    /// one, unauthorized (centrifuge-js `_getSubscriptionToken`).
    pub(super) fn subscription_refresh_due(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get(&id) else {
            return;
        };
        if sub.state != SubscriptionState::Subscribed || sub.tokens.refresh_in_flight() {
            return;
        }
        if sub.tokens.has_source() {
            self.fetch_subscription_token(id, FetchPurpose::Refresh);
        } else {
            self.emit_subscription_error(
                id,
                ErrorKind::Configuration,
                codes::error::BAD_CONFIGURATION,
                "provide a function to get channel subscription token".to_owned(),
            );
            self.fail_unauthorized(id);
        }
    }

    fn send_sub_refresh(&mut self, id: SubscriptionId) {
        let Some(transport) = self.transport.filter(|_| self.state == State::Connected) else {
            return;
        };
        let command_id = self.command_id();
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        let token = Secret(sub.tokens.for_refresh(command_id));
        let channel = sub.channel.clone();
        self.subscriptions.sub_refreshes.insert(command_id, id);
        self.send(
            transport,
            Command {
                id: command_id,
                method: Method::SubRefresh(SubRefreshRequest { channel, token }),
            },
        );
        self.actions.push(Action::SetSubscriptionTimer(
            id,
            SubscriptionTimer::RefreshReply,
            self.config.timeout,
        ));
    }

    /// The `sub_refresh` got no reply in time: a timeout error (code 1),
    /// retried like a temporary one (centrifuge-js `_refreshError`).
    pub(super) fn subscription_refresh_timed_out(&mut self, id: SubscriptionId) {
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if !sub.tokens.refresh_timed_out() {
            return;
        }
        self.subscriptions.sub_refreshes.retain(|_, of| *of != id);
        self.emit_subscription_error(
            id,
            ErrorKind::SubscriptionRefresh,
            codes::error::TIMEOUT,
            "timeout".to_owned(),
        );
        self.retry_subscription_refresh(id);
    }

    /// The reply to a `sub_refresh` command of subscription `id`.
    pub(super) fn sub_refresh_reply(&mut self, id: SubscriptionId, reply: Reply) {
        self.actions.push(Action::CancelSubscriptionTimer(
            id,
            SubscriptionTimer::RefreshReply,
        ));
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return;
        };
        if !sub.tokens.take_refresh_reply(reply.id) || sub.state != SubscriptionState::Subscribed {
            return;
        }
        match (reply.error, reply.result) {
            (Some(error), _) if error.code < 100 || error.temporary => {
                self.emit_subscription_error(
                    id,
                    ErrorKind::SubscriptionRefresh,
                    error.code,
                    error.message,
                );
                self.retry_subscription_refresh(id);
            }
            (Some(error), _) => self.set_unsubscribed(id, error.code, &error.message, true),
            (None, ReplyResult::SubRefresh(RefreshResult { expires, ttl })) => {
                if expires {
                    self.schedule_subscription_refresh(id, ttl);
                }
            }
            // A reply to sub_refresh without its result breaks the protocol.
            (None, _) => self.reconnect_after_transport_closed(),
        }
    }

    /// A subscribe answered with error 109, token expired: without a token
    /// callback a configuration error and unauthorized, with no
    /// unsubscribe command and no retry; with one, the token is dropped and
    /// the subscription retries after the resubscribe backoff, the callback
    /// then supplying a new token. Whether `error` was that error.
    pub(super) fn subscribe_token_expired(&mut self, id: SubscriptionId, error: &Error) -> bool {
        if error.code != TOKEN_EXPIRED {
            return false;
        }
        let Some(sub) = self.subscriptions.by_id.get_mut(&id) else {
            return true;
        };
        if !sub.tokens.has_source() {
            self.emit_subscription_error(
                id,
                ErrorKind::Configuration,
                codes::error::BAD_CONFIGURATION,
                "subscription token expired but no getToken function set".to_owned(),
            );
            self.set_unsubscribed(id, codes::unsubscribed::UNAUTHORIZED, "unauthorized", false);
            return true;
        }
        sub.tokens.connect_token_expired();
        self.emit_subscription_error(id, ErrorKind::Subscribe, error.code, error.message.clone());
        self.schedule_resubscribe(id);
        true
    }
}
