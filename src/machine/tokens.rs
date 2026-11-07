//! Connection token state and policy, owned by the [`super::Machine`].
//!
//! It decides when a token is needed and remembers which fetch and which
//! refresh command are pending; the machine turns its answers into events
//! and actions. Kept apart so that subscription tokens, which follow the
//! same fetch / refresh / deny cycle, can reuse it.

use std::time::Duration;

use super::FetchId;
use crate::token::Secret;

/// Retry delay of a failed connection token refresh: centrifuge-js
/// `centrifuge.ts` `_getRefreshRetryDelay`, 5 to 10 s.
pub(super) const REFRESH_RETRY_MIN: Duration = Duration::from_secs(5);
pub(super) const REFRESH_RETRY_MAX: Duration = Duration::from_secs(10);
/// Server error code of an expired token.
pub(super) const TOKEN_EXPIRED: u32 = 109;

/// Why a token is being fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchPurpose {
    Connect,
    Refresh,
}

/// What a connection attempt needs before opening its transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BeforeConnect {
    /// The current token, possibly none, will do.
    Ready,
    /// A token must be fetched first.
    Fetch,
    /// A token is needed but nothing can provide one.
    NoSource,
}

#[derive(Debug)]
pub(super) struct Tokens {
    /// Current token; empty for none.
    current: Secret,
    /// Token sent with the pending connect command.
    sent_with_connect: Secret,
    /// Whether the owner answers token fetches.
    source: bool,
    /// The server rejected the token as expired: get a new one first.
    refresh_required: bool,
    fetch: Option<(FetchId, FetchPurpose)>,
    /// A refresh fetch of a connection that is gone, still awaited for a
    /// denial (the last one, if several reconnects followed each other).
    stale_fetch: Option<FetchId>,
    next_fetch: u64,
    /// Id of the refresh command awaiting its reply.
    refresh_command: Option<u32>,
    /// A refresh came due and has no new token yet: its fetch runs, or its
    /// retry waits (Block 25, D3).
    refreshing: bool,
}

impl Tokens {
    pub(super) fn new(initial: String, source: bool) -> Self {
        Self {
            current: Secret(initial),
            sent_with_connect: Secret::default(),
            source,
            refresh_required: false,
            fetch: None,
            stale_fetch: None,
            next_fetch: 0,
            refresh_command: None,
            refreshing: false,
        }
    }

    /// centrifuge-js `_startReconnecting`: a token is needed when the last
    /// one expired, or when there is none but a source.
    pub(super) fn before_connect(&self) -> BeforeConnect {
        let needed = self.refresh_required || (self.current.0.is_empty() && self.source);
        match (needed, self.source) {
            (false, _) => BeforeConnect::Ready,
            (true, true) => BeforeConnect::Fetch,
            (true, false) => BeforeConnect::NoSource,
        }
    }

    pub(super) fn has_source(&self) -> bool {
        self.source
    }

    /// Whether a token is held.
    pub(super) fn has_token(&self) -> bool {
        !self.current.0.is_empty()
    }

    pub(super) fn start_fetch(&mut self, purpose: FetchPurpose) -> FetchId {
        let id = FetchId(self.next_fetch);
        self.next_fetch += 1;
        self.begin_fetch(id, purpose);
        id
    }

    /// Records fetch `id`, numbered by the owner: subscriptions number
    /// their fetches from one counter, as the adapter routes answers by id.
    pub(super) fn begin_fetch(&mut self, id: FetchId, purpose: FetchPurpose) {
        self.fetch = Some((id, purpose));
        if purpose == FetchPurpose::Refresh {
            self.refreshing = true;
        }
    }

    /// The pending fetch, if any.
    pub(super) fn pending_fetch(&self) -> Option<(FetchId, FetchPurpose)> {
        self.fetch
    }

    /// Drops the pending fetch and refresh command, keeping the token.
    pub(super) fn abandon(&mut self) -> Option<FetchId> {
        self.refresh_command = None;
        self.fetch.take().map(|(id, _)| id)
    }

    /// Whether a fetch or a refresh command is pending.
    pub(super) fn refresh_in_flight(&self) -> bool {
        self.fetch.is_some() || self.refresh_command.is_some()
    }

    /// Whether `fetch` is the refresh of a connection that is gone, which
    /// it no longer is afterwards.
    pub(super) fn take_stale_fetch(&mut self, fetch: FetchId) -> bool {
        self.stale_fetch.take_if(|stale| *stale == fetch).is_some()
    }

    /// The purpose of `fetch` if it is the pending one, which it no longer
    /// is afterwards; `None` for a fetch abandoned meanwhile.
    pub(super) fn take_fetch(&mut self, fetch: FetchId) -> Option<FetchPurpose> {
        match self.fetch {
            Some((id, purpose)) if id == fetch => {
                self.fetch = None;
                Some(purpose)
            }
            _ => None,
        }
    }

    /// Stores a fetched token. A token fetched to connect also clears the
    /// expired flag.
    pub(super) fn store(&mut self, token: String, purpose: FetchPurpose) {
        self.current = Secret(token);
        self.refreshing = false;
        if purpose == FetchPurpose::Connect {
            self.refresh_required = false;
        }
    }

    /// The application set `token`, used from the next connection attempt
    /// (centrifuge-js `setToken`). Without a source it also clears a
    /// pending "token expired": nothing else could replace the expired
    /// token. With one, the next attempt still asks it, as the token set
    /// may be the expired one.
    pub(super) fn set(&mut self, token: String) {
        self.current = Secret(token);
        if !self.source {
            self.refresh_required = false;
        }
    }

    /// The token for a connect command, remembered as the one sent.
    pub(super) fn for_connect(&mut self) -> String {
        self.sent_with_connect.clone_from(&self.current);
        self.current.0.clone()
    }

    /// The server rejected the connect token as expired: unless the
    /// application set another token meanwhile, get a new one first.
    pub(super) fn connect_token_expired(&mut self) {
        if self.sent_with_connect == self.current {
            self.refresh_required = true;
        }
    }

    /// The refresh came due while no connection could carry it (Block 25,
    /// D3): with a source, the next connect or subscribe fetches a new
    /// token first; a fixed token is sent as it is.
    pub(super) fn refresh_due(&mut self) {
        if self.source {
            self.refresh_required = true;
        }
    }

    /// The connection or subscription is torn down to connect or subscribe
    /// again: a refresh left without a new token makes that attempt fetch
    /// one first; a token already fetched, its command pending, is kept
    /// for it (Block 25, D3).
    pub(super) fn abandon_refresh(&mut self) {
        if std::mem::take(&mut self.refreshing) {
            self.refresh_due();
        }
    }

    /// The refresh ends without another attempt: `disconnect()`,
    /// `unsubscribe()` or a terminal condition.
    pub(super) fn forget_refresh(&mut self) {
        self.refreshing = false;
    }

    pub(super) fn connected(&mut self) {
        self.refresh_required = false;
    }

    /// The server invalidated the state the token belongs to (`client.proto`
    /// codes 2502 and 3014): the token is dropped, so the next connect or
    /// subscribe fetches a new one when there is a source, and carries none
    /// otherwise. A fetch in flight was asked for the invalidated state: its
    /// answer, even a refresh's that would outlive a reconnect, is ignored.
    pub(super) fn invalidate(&mut self) {
        self.current = Secret::default();
        self.refresh_required = false;
        self.refreshing = false;
        self.fetch = None;
        self.stale_fetch = None;
    }

    /// The token for refresh command `id`, which is now the pending one.
    pub(super) fn for_refresh(&mut self, id: u32) -> String {
        self.refresh_command = Some(id);
        self.current.0.clone()
    }

    /// Whether `id` answers the pending refresh command, which it no
    /// longer is afterwards.
    /// Whether the refresh command `id` awaits its reply.
    pub(super) fn awaits_refresh_reply(&self, id: u32) -> bool {
        self.refresh_command == Some(id)
    }

    pub(super) fn take_refresh_reply(&mut self, id: u32) -> bool {
        self.refresh_command
            .take_if(|pending| *pending == id)
            .is_some()
    }

    /// Drops the pending refresh command after its reply deadline; whether
    /// there was one.
    pub(super) fn refresh_timed_out(&mut self) -> bool {
        self.refresh_command.take().is_some()
    }

    /// The connection is torn down. A refresh fetch outlives a reconnect,
    /// as a stale one: its denial still applies, its token is not used
    /// (centrifuge-js `_refresh`), and the next connection refreshes on its
    /// own schedule.
    pub(super) fn teardown(&mut self, reconnect: bool) {
        if reconnect {
            self.abandon_refresh();
        } else {
            self.forget_refresh();
        }
        self.refresh_command = None;
        match self.fetch.take() {
            Some((id, FetchPurpose::Refresh)) if reconnect => self.stale_fetch = Some(id),
            _ if !reconnect => self.stale_fetch = None,
            _ => {}
        }
    }
}

/// When to start refreshing a connection that expires in `ttl_seconds`: at
/// a uniformly random millisecond between 80% and 90% of the ttl, both
/// included (Block 14, "Token refresh jitter").
///
/// `client.proto` (`ConnectResult.expires`) requires the refresh to be sent
/// before the ttl elapses, so it starts early enough for the token callback
/// and the round trip. The randomness keeps a fleet whose tokens were issued
/// at once from refreshing at once; centrifuge-js waits for the full ttl.
pub(super) fn refresh_delay(ttl_seconds: u32, rng: &mut fastrand::Rng) -> Duration {
    let ttl_ms = u64::from(ttl_seconds) * 1000;
    Duration::from_millis(rng.u64(ttl_ms * 8 / 10..=ttl_ms * 9 / 10))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalidated_token_is_fetched_again_or_dropped() {
        let mut with_source = Tokens::new("t".into(), true);
        let mut fixed = Tokens::new("t".into(), false);

        with_source.invalidate();
        fixed.invalidate();

        assert_eq!(with_source.before_connect(), BeforeConnect::Fetch);
        assert_eq!(fixed.before_connect(), BeforeConnect::Ready);
        assert_eq!(fixed.for_connect(), "");
    }

    #[test]
    fn before_connect_needs_a_fetch_only_without_token_or_after_expiry() {
        let mut expired = Tokens::new("t".into(), true);
        expired.for_connect();
        expired.connect_token_expired();

        let cases = [
            (
                Tokens::new("t".into(), true).before_connect(),
                BeforeConnect::Ready,
            ),
            (
                Tokens::new(String::new(), false).before_connect(),
                BeforeConnect::Ready,
            ),
            (
                Tokens::new(String::new(), true).before_connect(),
                BeforeConnect::Fetch,
            ),
            (expired.before_connect(), BeforeConnect::Fetch),
        ];

        for (got, expected) in cases {
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn expiry_of_a_token_replaced_meanwhile_needs_no_fetch() {
        let mut tokens = Tokens::new("t0".into(), false);
        tokens.for_connect();
        tokens.store("t1".into(), FetchPurpose::Refresh);

        tokens.connect_token_expired();

        assert_eq!(tokens.before_connect(), BeforeConnect::Ready);
    }

    #[test]
    fn a_token_set_replaces_an_expired_one_only_without_a_source() {
        let mut fixed = Tokens::new("old".into(), false);
        let mut sourced = Tokens::new("old".into(), true);
        for tokens in [&mut fixed, &mut sourced] {
            tokens.for_connect();
            tokens.connect_token_expired();
            tokens.set("new".into());
        }

        assert_eq!(fixed.before_connect(), BeforeConnect::Ready);
        assert_eq!(fixed.for_connect(), "new");
        assert_eq!(sourced.before_connect(), BeforeConnect::Fetch);
    }

    #[test]
    fn only_the_pending_refresh_command_is_answered_once() {
        let mut tokens = Tokens::new("t".into(), true);
        tokens.for_refresh(7);

        let other = tokens.take_refresh_reply(8);
        let pending = tokens.take_refresh_reply(7);
        let again = tokens.take_refresh_reply(7);

        assert_eq!((other, pending, again), (false, true, false));
    }
}
