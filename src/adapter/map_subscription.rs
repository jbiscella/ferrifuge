//! Map subscriptions of the client (Block 45): a subscription to a keyed
//! collection that Centrifugo owns, loaded page by page, then kept current
//! per key. They share the client's registry with stream subscriptions.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_core::Stream;

use super::client::{
    Callbacks, Client, EventStream, GetSubscriptionToken, Shared, lock, presence_of,
    presence_stats_of, reply_of,
};
use crate::machine::{Input, MapSubscriptionOptions, SubscriptionId};
use crate::{
    CallError, CallReply, CallRequest, JsonCallError, MapKind, MapSubscriptionEvent, MaybeSend,
    MaybeSync, PresenceResult, PresenceStatsResult, SubscriptionError, SubscriptionState,
    TokenError, UnrecoverableStrategy,
};

/// A map subscription to a channel, in its client's registry (Block 45):
/// its stream yields its lifecycle events, one
/// [`MapSubscriptionEvent::Sync`] with the whole state once loaded, then a
/// [`MapSubscriptionEvent::Update`] per change. Clones are handles to the
/// same subscription.
///
/// The client loads the state page by page, `page_size` entries at a time,
/// then the changes made meanwhile, as Centrifugo's map subscriptions
/// require (Centrifugo 6.8.0 and later; the namespace declares
/// `subscription_type`). Pages that make no event are handled as soon as
/// they are read, ahead of the events the application has not taken, as
/// Centrifugo closes a load that takes longer than the namespace's
/// `map.subscribe_catch_up_timeout` (5 s by default) with 3008 "slow". A
/// full read-ahead still stops the reading, pages included (Block 15), so
/// an application that does not read its streams can make loads time out.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Clone)]
pub struct MapSubscription {
    shared: Arc<Shared>,
    id: SubscriptionId,
    channel: Arc<str>,
    kind: MapKind,
}

impl std::fmt::Debug for MapSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapSubscription")
            .field("channel", &self.channel)
            .field("kind", &self.kind)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Handles to the same map subscription are equal.
impl PartialEq for MapSubscription {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared) && self.id == other.id
    }
}

impl Eq for MapSubscription {}

impl MapSubscription {
    /// The channel subscribed to.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The map's kind.
    #[must_use]
    pub fn kind(&self) -> MapKind {
        self.kind
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> SubscriptionState {
        lock(&self.shared.core)
            .machine
            .subscription_state(self.id)
            .unwrap_or(SubscriptionState::Unsubscribed)
    }

    /// Sets the data sent with the first request of every load from now on
    /// (Block 32), sending nothing now.
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
            .maps
            .get(&self.id)
            .map_or(0, super::outbox::Outbox::held)
    }

    /// Events from now on, as [`crate::Subscription::events`] gives a
    /// subscription's: an event counts as delivered once this stream yields
    /// it, so take it before calling [`MapSubscription::subscribe`] to get
    /// every one; a later call takes over (Block 15).
    pub fn events(&self) -> impl Stream<Item = MapSubscriptionEvent> + MaybeSend + Unpin + use<> {
        let mut core = lock(&self.shared.core);
        let number = if core.machine.subscription_state(self.id).is_some()
            && !self.shared.closed.load(Ordering::Acquire)
        {
            let number = lock(&self.shared.outboxes)
                .maps
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

    /// Starts subscribing: loading the state, at once when the client is
    /// connected, otherwise once it is.
    pub fn subscribe(&self) {
        self.shared.apply(Input::Subscribe(self.id));
    }

    /// Unsubscribes. A load in progress ends once the page in flight is
    /// read, with an unsubscribe command.
    pub fn unsubscribe(&self) {
        self.shared.apply(Input::Unsubscribe(self.id));
    }

    /// Sets `key` of the map to `data`, once the subscription is
    /// subscribed, as [`Client::map_publish`] does (Block 45, D8). The
    /// subscription gets its own change as an update, as every subscriber.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::publish`]; the server's refusal keeps its
    /// code (103 without permission, 107 for an empty key).
    pub fn publish(
        &self,
        key: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::MapPublish {
            channel: self.channel.to_string(),
            key: key.into(),
            data: data.into(),
        });
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// Sets `key` of the map to `data` encoded as JSON (Block 38), as
    /// [`MapSubscription::publish`] does.
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of
    /// [`MapSubscription::publish`].
    pub fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        key: impl Into<String>,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.publish(key, data));
        async move { Ok(call.map_err(JsonCallError::Encode)?.await?) }
    }

    /// Removes `key` from the map, once the subscription is subscribed;
    /// removing a key the map does not hold succeeds.
    ///
    /// # Errors
    ///
    /// As [`MapSubscription::publish`].
    pub fn remove(
        &self,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::MapRemove {
            channel: self.channel.to_string(),
            key: key.into(),
        });
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// The clients subscribed to the channel, once the subscription is
    /// subscribed, on a namespace with presence.
    ///
    /// # Errors
    ///
    /// As [`MapSubscription::publish`].
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
    /// As [`MapSubscription::publish`].
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

/// Builds a [`MapSubscription`] with options.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
pub struct MapSubscriptionBuilder {
    shared: Arc<Shared>,
    channel: String,
    options: MapSubscriptionOptions,
    get_token: Option<GetSubscriptionToken>,
}

/// The options' `Debug` output redacts the token.
impl std::fmt::Debug for MapSubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapSubscriptionBuilder")
            .field("channel", &self.channel)
            .field("options", &self.options)
            .field("get_token", &self.get_token.is_some())
            .finish_non_exhaustive()
    }
}

impl MapSubscriptionBuilder {
    /// Sends `data` with the first request of every load: JSON text with
    /// the JSON protocol, any bytes with Protobuf.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.options.common = self.options.common.data(data.into());
        self
    }

    /// Sends `token`, a subscription token (JWT) for the channel, with
    /// every request of a load. With [`Self::get_token`] too, the callback
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

    /// Entries asked for per page; 0, the default, leaves it to the
    /// server. Centrifugo clamps it to the namespace's `map.min_page_size`
    /// and `map.max_page_size`, 100 and 1000 by default, so a size under
    /// 100 has no effect unless the namespace lowers `min_page_size`.
    #[must_use]
    pub fn page_size(mut self, page_size: u32) -> Self {
        self.options.page_size = page_size;
        self
    }

    /// Bytes of page replies one load may read, 64 MiB by default: past
    /// it the subscription sends an unsubscribe command and ends with
    /// `unsubscribed` 5 "map state too large", without subscribing again by
    /// itself (Block 45, D5). The application holds the state it gets, and
    /// its own copy.
    #[must_use]
    pub fn max_state_bytes(mut self, max_state_bytes: NonZeroUsize) -> Self {
        self.options.max_state_bytes = max_state_bytes;
        self
    }

    /// What an unrecoverable position does; [`UnrecoverableStrategy::FromScratch`]
    /// by default.
    #[must_use]
    pub fn unrecoverable_strategy(mut self, strategy: UnrecoverableStrategy) -> Self {
        self.options.unrecoverable_strategy = strategy;
        self
    }

    /// Asks the server to send changes as deltas in `format` against each
    /// key's previous value, on namespaces that allow it.
    #[must_use]
    pub fn delta(mut self, format: crate::DeltaFormat) -> Self {
        self.options.common = self.options.common.delta(format);
        self
    }

    /// Asks the server to send only the entries and changes `filter`
    /// matches, on namespaces with `allow_tags_filter`; building fails when
    /// the filter is one the server would refuse, or goes with
    /// [`Self::delta`].
    #[must_use]
    pub fn tag_filter(mut self, filter: crate::TagFilter) -> Self {
        self.options.common = self.options.common.tag_filter(filter);
        self
    }

    /// Asks a server with channel compaction (Centrifugo PRO) to name this
    /// subscription by a numeric id in its pushes. Off by default.
    #[must_use]
    pub fn channel_compaction(mut self, channel_compaction: bool) -> Self {
        self.options.common = self.options.common.channel_compaction(channel_compaction);
        self
    }

    /// Adds the map subscription, unsubscribed, to the client's registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to the channel already, of
    /// either kind; for a tag filter the server would refuse, or one with
    /// delta.
    pub fn build(self) -> Result<MapSubscription, SubscriptionError> {
        let mut core = lock(&self.shared.core);
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(SubscriptionError::ClientClosed);
        }
        let kind = self.options.kind;
        let id = core
            .machine
            .add_map_subscription(self.channel.clone(), self.options)?;
        self.shared.log.subscription(id, &self.channel);
        if self.get_token.is_some() {
            lock(&self.shared.callbacks).0.insert(
                id,
                Callbacks {
                    data: None,
                    token: self.get_token,
                    state: None,
                },
            );
        }
        drop(core);
        Ok(MapSubscription {
            shared: self.shared,
            id,
            channel: self.channel.into(),
            kind,
        })
    }
}

impl Client {
    /// Starts building a map subscription of `kind` to `channel` (Block
    /// 45).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_subscription_builder(
        &self,
        channel: impl Into<String>,
        kind: MapKind,
    ) -> MapSubscriptionBuilder {
        MapSubscriptionBuilder {
            shared: Arc::clone(self.shared()),
            channel: channel.into(),
            options: MapSubscriptionOptions::new(kind),
            get_token: None,
        }
    }

    /// Adds a map subscription of `kind` to `channel`, unsubscribed, to the
    /// registry.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already, of
    /// either kind.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn new_map_subscription(
        &self,
        channel: impl Into<String>,
        kind: MapKind,
    ) -> Result<MapSubscription, SubscriptionError> {
        self.map_subscription_builder(channel, kind).build()
    }

    /// The map subscription to `channel` in the registry, if any; `None`
    /// for a stream subscription.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn get_map_subscription(&self, channel: &str) -> Option<MapSubscription> {
        let core = lock(&self.shared().core);
        let id = core.machine.find_subscription(channel)?;
        let kind = core.machine.map_subscription_kind(id)?;
        drop(core);
        Some(MapSubscription {
            shared: Arc::clone(self.shared()),
            id,
            channel: channel.into(),
            kind,
        })
    }

    /// Every map subscription in the registry.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_subscriptions(&self) -> Vec<MapSubscription> {
        let core = lock(&self.shared().core);
        let subscriptions: Vec<_> = core
            .machine
            .subscriptions()
            .into_iter()
            .filter_map(|(id, channel)| {
                Some((id, channel, core.machine.map_subscription_kind(id)?))
            })
            .collect();
        drop(core);
        subscriptions
            .into_iter()
            .map(|(id, channel, kind)| MapSubscription {
                shared: Arc::clone(self.shared()),
                id,
                channel: channel.into(),
                kind,
            })
            .collect()
    }

    /// Unsubscribes `subscription` and removes it from the registry; a new
    /// subscription to its channel, of either kind, can then be created. Its
    /// event streams end after the final "unsubscribed". A map subscription
    /// of another client is left alone.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn remove_map_subscription(&self, subscription: &MapSubscription) {
        if !Arc::ptr_eq(&subscription.shared, self.shared()) {
            return;
        }
        self.shared()
            .apply(Input::RemoveSubscription(subscription.id));
        lock(&self.shared().callbacks).0.remove(&subscription.id);
        self.shared().log.subscription_removed(subscription.id);
        let mut outboxes = lock(&self.shared().outboxes);
        if let Some(outbox) = outboxes.maps.get_mut(&subscription.id) {
            outbox.close();
            if outbox.is_finished() {
                outboxes.maps.remove(&subscription.id);
            }
        }
    }
}
