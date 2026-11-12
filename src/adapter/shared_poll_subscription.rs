//! Shared poll subscriptions of the client (Block 47): a subscription on
//! which the application names keys, tracked under signatures its backend
//! issued; Centrifugo polls the backend once per key for every client and
//! sends each new version. They share the client's registry with the other
//! kinds.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_core::Stream;

use super::client::{
    Callbacks, Client, EventStream, GetSignature, GetSubscriptionToken, Shared, lock, presence_of,
    presence_stats_of, reply_of,
};
use crate::machine::{Input, SharedPollSubscriptionOptions, SubscriptionId};
use crate::{
    CallError, CallReply, CallRequest, MaybeSend, MaybeSync, PresenceResult, PresenceStatsResult,
    SharedPollSignature, SharedPollSubscriptionEvent, SharedPollTrackItem, SubscriptionError,
    SubscriptionState, TokenError,
};

/// A shared poll subscription to a channel, in its client's registry
/// (Block 47): the application names the keys it follows, each tracked
/// under a signature its backend issued, and its stream yields its
/// lifecycle events, then a [`SharedPollSubscriptionEvent::Update`] per new
/// version of a tracked key's value. Clones are handles to the same
/// subscription.
///
/// Centrifugo (6.8.0 and later, a namespace with `subscription_type:
/// shared_poll`) polls the application's backend once per key on behalf of
/// every subscriber and sends each new version once. A key is tracked from
/// [`Self::track_with_signature`], with a signature the backend issued for
/// exactly those keys, or from [`Self::track`], whose keys the
/// [`SharedPollSubscriptionBuilder::get_signature`] callback signs. The
/// client keeps the keys, their versions and the signatures across
/// reconnects, and tracks them again on each subscribe without calling the
/// backend for keys it holds a signature for; [`Self::unsubscribe`] forgets
/// them. With the callback, the client also renews signatures as they
/// expire. Keys and signatures are not bounded by the client: the server's
/// `max_keys_per_connection` (5000 by default) is the usual limit.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
#[derive(Clone)]
pub struct SharedPollSubscription {
    shared: Arc<Shared>,
    id: SubscriptionId,
    channel: Arc<str>,
}

impl std::fmt::Debug for SharedPollSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPollSubscription")
            .field("channel", &self.channel)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Handles to the same shared poll subscription are equal.
impl PartialEq for SharedPollSubscription {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared) && self.id == other.id
    }
}

impl Eq for SharedPollSubscription {}

impl SharedPollSubscription {
    /// The channel subscribed to.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> SubscriptionState {
        lock(&self.shared.core)
            .machine
            .subscription_state(self.id)
            .unwrap_or(SubscriptionState::Unsubscribed)
    }

    /// Tracks `keys`, keeping the version held for a key already tracked.
    /// Subscribed, the [`SharedPollSubscriptionBuilder::get_signature`]
    /// callback signs them, in chunks when one batch of them would pass
    /// [`SharedPollSubscriptionBuilder::max_track_frame_bytes`], and they
    /// are tracked under its signatures; not subscribed, they are signed
    /// once the subscription is. A key the callback does not return is
    /// revoked, with a removal [`SharedPollSubscriptionEvent::Update`]. An
    /// empty set does nothing. Returns at once: failures come as
    /// [`SharedPollSubscriptionEvent::Error`] events with code 14, the
    /// callback being asked again after 5 to 30 s, also when there is no
    /// callback, the keys staying tracked (Block 47, D6).
    pub fn track(&self, keys: impl IntoIterator<Item = impl Into<String>>) {
        let keys = keys.into_iter().map(Into::into).collect();
        self.shared.apply(Input::TrackKeys(self.id, keys));
    }

    /// Tracks `items` under `signature`, which the backend issued for
    /// exactly their keys in this order, each at the higher of the version
    /// given and the one held. The client keeps the signature to track them
    /// again after a reconnect. An empty set does nothing. Returns at once:
    /// a refusal comes as a [`SharedPollSubscriptionEvent::Error`] event,
    /// with a removal [`SharedPollSubscriptionEvent::Update`] for each key
    /// (Block 47, D6, D7).
    ///
    /// With a [`SharedPollSubscriptionBuilder::get_signature`] callback,
    /// the client renews a signature with an expiry through it. Without
    /// one, renewing is the application's: call this again with a new
    /// signature before it expires, as the server stops tracking its keys
    /// some time after it, with no event.
    pub fn track_with_signature(
        &self,
        items: impl IntoIterator<Item = SharedPollTrackItem>,
        signature: impl Into<String>,
    ) {
        let items = items.into_iter().collect();
        self.shared.apply(Input::TrackKeysWithSignature(
            self.id,
            items,
            signature.into(),
        ));
    }

    /// Stops tracking `keys`, at once, and on the server when subscribed. An
    /// empty set sends nothing.
    pub fn untrack(&self, keys: impl IntoIterator<Item = impl Into<String>>) {
        let keys = keys.into_iter().map(Into::into).collect();
        self.shared.apply(Input::UntrackKeys(self.id, keys));
    }

    /// The keys tracked, in the order they were first tracked.
    #[must_use]
    pub fn tracked_keys(&self) -> Vec<String> {
        lock(&self.shared.core)
            .machine
            .shared_poll_tracked_keys(self.id)
            .unwrap_or_default()
    }

    /// Sets the data sent with every subscribe command from now on (Block
    /// 32), sending nothing now.
    pub fn set_data(&self, data: impl Into<Vec<u8>>) {
        self.shared
            .apply(Input::SetSubscriptionData(self.id, data.into()));
    }

    /// Waits until the subscription is subscribed, without taking an event
    /// from its stream, as [`crate::Subscription::ready`] does.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::ready`].
    pub fn ready(&self) -> impl Future<Output = Result<(), CallError>> + MaybeSend + use<> {
        super::ready::ready(&self.shared, super::ready::Of::Subscription(self.id))
    }

    /// Events of the subscription queued for its streams and not yet
    /// taken, so that tests can wait for them. Only with the `__test-hooks`
    /// feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_events(&self) -> usize {
        lock(&self.shared.outboxes)
            .shared_polls
            .get(&self.id)
            .map_or(0, super::outbox::Outbox::held)
    }

    /// Events from now on, as [`crate::Subscription::events`] gives a
    /// subscription's: an event counts as delivered once this stream yields
    /// it, so take it before calling [`Self::subscribe`] to get every one; a
    /// later call takes over (Block 15).
    pub fn events(
        &self,
    ) -> impl Stream<Item = SharedPollSubscriptionEvent> + MaybeSend + Unpin + use<> {
        let mut core = lock(&self.shared.core);
        let number = if core.machine.subscription_state(self.id).is_some()
            && !self.shared.closed.load(Ordering::Acquire)
        {
            let number = lock(&self.shared.outboxes)
                .shared_polls
                .entry(self.id)
                .or_default()
                .open_stream();
            self.shared
                .handle(&mut core, Input::StreamOpened(Some(self.id)));
            number
        } else {
            0
        };
        drop(core);
        EventStream::new(Arc::clone(&self.shared), Some(self.id), number)
    }

    /// Starts subscribing: at once when the client is connected, otherwise
    /// once it is.
    pub fn subscribe(&self) {
        self.shared.apply(Input::Subscribe(self.id));
    }

    /// Unsubscribes, forgetting the keys tracked and the signatures held.
    pub fn unsubscribe(&self) {
        self.shared.apply(Input::Unsubscribe(self.id));
    }

    /// The clients subscribed to the channel, once the subscription is
    /// subscribed, on a namespace with presence.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::presence`].
    pub fn presence(
        &self,
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::Presence {
            channel: self.channel.to_string(),
        });
        reply_of(call, presence_of)
    }

    /// How many clients and users are subscribed to the channel, once the
    /// subscription is subscribed, on a namespace with presence.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::presence_stats`].
    pub fn presence_stats(
        &self,
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::PresenceStats {
            channel: self.channel.to_string(),
        });
        reply_of(call, presence_stats_of)
    }

    fn call(
        &self,
        request: CallRequest,
    ) -> impl Future<Output = Result<CallReply, CallError>> + MaybeSend + 'static {
        self.shared.call(Some(self.id), request)
    }
}

/// Builds a [`SharedPollSubscription`] with options.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
pub struct SharedPollSubscriptionBuilder {
    shared: Arc<Shared>,
    channel: String,
    options: SharedPollSubscriptionOptions,
    get_token: Option<GetSubscriptionToken>,
    get_signature: Option<GetSignature>,
}

/// The options' `Debug` output redacts the token.
impl std::fmt::Debug for SharedPollSubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPollSubscriptionBuilder")
            .field("channel", &self.channel)
            .field("options", &self.options)
            .field("get_token", &self.get_token.is_some())
            .field("get_signature", &self.get_signature.is_some())
            .finish_non_exhaustive()
    }
}

impl SharedPollSubscriptionBuilder {
    /// Sends `data` with every subscribe command: JSON text with the JSON
    /// protocol, any bytes with Protobuf.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.options.common = self.options.common.data(data.into());
        self
    }

    /// Sends `token`, a subscription token (JWT) for the channel, with
    /// every subscribe command. With [`Self::get_token`] too, the callback
    /// is asked once it expires.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.options.common = self.options.common.token(token.into());
        self
    }

    /// Sets a callback giving a new subscription token, as
    /// [`crate::SubscriptionBuilder::get_token`] does.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, get_token: F) -> Self
    where
        F: Fn(String) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.options.common = self.options.common.token_source();
        self.get_token = Some(Arc::new(move |channel| Box::pin(get_token(channel))));
        self
    }

    /// Sets the callback signing the keys of [`SharedPollSubscription::track`]
    /// and the keys tracked without a signature held, given them in order;
    /// it returns a [`SharedPollSignature`] from the application's backend
    /// (Block 47, D6). It runs beside the client, so a slow backend holds
    /// back no ping or call.
    ///
    /// The client asks it for the keys in chunks when one batch of them
    /// would pass `max_track_frame_bytes`, one at a time, each sized for
    /// the longest signature it has returned, at least 128 bytes. A key
    /// asked for and not returned is revoked. It is asked again for every key tracked when a signature
    /// expires, at the earliest expiry the server reports, or when the
    /// server refuses one with 109; the signatures it returns then replace
    /// those they cover. A failure is reported as an `Error` event with
    /// code 14 and retried after 5 to 30 s; [`TokenError::Unauthorized`]
    /// ends the subscription with `unsubscribed` 1 (Block 47, D6, D7).
    #[must_use]
    pub fn get_signature<F, Fut>(mut self, get_signature: F) -> Self
    where
        F: Fn(Vec<String>) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<SharedPollSignature, TokenError>> + MaybeSend + 'static,
    {
        self.options.signature_source = true;
        self.get_signature = Some(Arc::new(move |keys| Box::pin(get_signature(keys))));
        self
    }

    /// Sets the lower bound of the resubscribe delay. Default 500 ms.
    #[must_use]
    pub fn min_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options.common = self.options.common.min_resubscribe_delay(delay);
        self
    }

    /// Sets the upper bound of the resubscribe delay. Default 20 s.
    #[must_use]
    pub fn max_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options.common = self.options.common.max_resubscribe_delay(delay);
        self
    }

    /// Bytes a track or untrack request may take, 60 000 by default: a
    /// request over it is split (Block 47, D7). Keep it below the server's
    /// `websocket.message_size_limit` and the emulation endpoint's
    /// `max_request_body_size`, 65 536 each by default: an operator who
    /// lowers either lowers this.
    #[must_use]
    pub fn max_track_frame_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.options.max_track_frame_bytes = bytes;
        self
    }

    /// Adds the shared poll subscription, unsubscribed, to the client's
    /// registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to the channel already, of
    /// any kind.
    pub fn build(self) -> Result<SharedPollSubscription, SubscriptionError> {
        let mut core = lock(&self.shared.core);
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(SubscriptionError::ClientClosed);
        }
        let id = core
            .machine
            .add_shared_poll_subscription(self.channel.clone(), self.options)?;
        self.shared.log.subscription(id, &self.channel);
        if self.get_token.is_some() || self.get_signature.is_some() {
            lock(&self.shared.callbacks).0.insert(
                id,
                Callbacks {
                    token: self.get_token,
                    signature: self.get_signature,
                    ..Callbacks::default()
                },
            );
        }
        drop(core);
        Ok(SharedPollSubscription {
            shared: self.shared,
            id,
            channel: self.channel.into(),
        })
    }
}

impl Client {
    /// Starts building a shared poll subscription to `channel` (Block 47).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn shared_poll_subscription_builder(
        &self,
        channel: impl Into<String>,
    ) -> SharedPollSubscriptionBuilder {
        SharedPollSubscriptionBuilder {
            shared: Arc::clone(self.shared()),
            channel: channel.into(),
            options: SharedPollSubscriptionOptions::default(),
            get_token: None,
            get_signature: None,
        }
    }

    /// Adds a shared poll subscription to `channel`, unsubscribed, to the
    /// registry.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already, of any
    /// kind.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn new_shared_poll_subscription(
        &self,
        channel: impl Into<String>,
    ) -> Result<SharedPollSubscription, SubscriptionError> {
        self.shared_poll_subscription_builder(channel).build()
    }

    /// The shared poll subscription to `channel` in the registry, if any;
    /// `None` for a subscription of another kind.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn get_shared_poll_subscription(&self, channel: &str) -> Option<SharedPollSubscription> {
        let core = lock(&self.shared().core);
        let id = core
            .machine
            .find_subscription(channel)
            .filter(|id| core.machine.is_shared_poll(*id))?;
        drop(core);
        Some(SharedPollSubscription {
            shared: Arc::clone(self.shared()),
            id,
            channel: channel.into(),
        })
    }

    /// Every shared poll subscription in the registry.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn shared_poll_subscriptions(&self) -> Vec<SharedPollSubscription> {
        let core = lock(&self.shared().core);
        let subscriptions: Vec<_> = core
            .machine
            .subscriptions()
            .into_iter()
            .filter(|(id, _)| core.machine.is_shared_poll(*id))
            .collect();
        drop(core);
        subscriptions
            .into_iter()
            .map(|(id, channel)| SharedPollSubscription {
                shared: Arc::clone(self.shared()),
                id,
                channel: channel.into(),
            })
            .collect()
    }

    /// Unsubscribes `subscription` and removes it from the registry; a new
    /// subscription to its channel, of any kind, can then be created. Its
    /// event streams end after the final "unsubscribed". A shared poll
    /// subscription of another client is left alone.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn remove_shared_poll_subscription(&self, subscription: &SharedPollSubscription) {
        if !Arc::ptr_eq(&subscription.shared, self.shared()) {
            return;
        }
        self.shared()
            .apply(Input::RemoveSubscription(subscription.id));
        lock(&self.shared().callbacks).0.remove(&subscription.id);
        self.shared().log.subscription_removed(subscription.id);
        let mut outboxes = lock(&self.shared().outboxes);
        if let Some(outbox) = outboxes.shared_polls.get_mut(&subscription.id) {
            outbox.close();
            if outbox.is_finished() {
                outboxes.shared_polls.remove(&subscription.id);
            }
        }
    }
}
