//! Map subscriptions of the client: a subscription to a keyed collection
//! that Centrifugo owns, loaded page by page, then kept current per key.
//! They share the client's registry with stream subscriptions.
// Block 45.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;

use super::client::{Client, reply_of};
use super::kind::SubscriptionHandle;
use super::shared::{Callbacks, GetSubscriptionToken, Shared};
use crate::sans_io::{MapSubscriptionOptions, SubscriptionKind};
use crate::{
    CallError, CallReply, CallRequest, JsonCallError, MapKind, MapSubscriptionEvent, MaybeSend,
    MaybeSync, PresenceResult, PresenceStatsResult, SubscriptionError, SubscriptionState,
    TokenError, UnrecoverableStrategy,
};

/// A map subscription to a channel, in its client's registry: its stream
/// yields its lifecycle events, one [`MapSubscriptionEvent::Sync`] with the
/// whole state once loaded, then a [`MapSubscriptionEvent::Update`] per
/// change. Clones are handles to the same subscription.
///
/// The client loads the state page by page, `page_size` entries at a time,
/// then the changes made meanwhile, as Centrifugo's map subscriptions
/// require (Centrifugo 6.8.0 and later; the namespace declares
/// `subscription_type`). Pages that make no event are handled as soon as
/// they are read, ahead of the events the application has not taken, as
/// Centrifugo closes a load that takes longer than the namespace's
/// `map.subscribe_catch_up_timeout` (5 s by default) with 3008 "slow". A
/// full read-ahead still stops the reading, pages included, so an
/// application that does not read its streams can make loads time out.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
// Block 45; Block 15.
#[derive(Clone)]
pub struct MapSubscription {
    pub(super) handle: SubscriptionHandle,
    kind: MapKind,
}

impl std::fmt::Debug for MapSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.handle
            .fmt(f, "MapSubscription", &[("kind", &self.kind)])
    }
}

/// Handles to the same map subscription are equal.
impl PartialEq for MapSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.handle.same(&other.handle)
    }
}

impl Eq for MapSubscription {}

impl MapSubscription {
    /// Sets the tag filter sent in every subscribe command built from now
    /// on, `None` to remove it, sending nothing now. The position is
    /// dropped, so the next subscribe loads the state through the new
    /// filter.
    ///
    /// # Errors
    ///
    /// [`SubscriptionError::InvalidTagFilter`] for a filter the server
    /// would refuse, [`SubscriptionError::TagFilterWithDelta`] on a map
    /// subscription with delta; the filter is then left as it was.
    // Block 45, D6.
    pub fn set_tag_filter(
        &self,
        filter: Option<crate::TagFilter>,
    ) -> Result<(), SubscriptionError> {
        self.handle.set_tag_filter(filter)
    }

    /// What delta compression did on the map subscription since it was
    /// created: state entries and catch-up publications received with delta
    /// on count as full payloads, each live or recovered delta as a delta.
    /// Zeros without negotiated delta.
    // Block 45, D11.
    #[must_use]
    pub fn delta_statistics(&self) -> crate::DeltaStatistics {
        self.handle.delta_statistics()
    }

    /// The channel subscribed to.
    #[must_use]
    pub fn channel(&self) -> &str {
        self.handle.channel()
    }

    /// The map's kind.
    #[must_use]
    pub fn kind(&self) -> MapKind {
        self.kind
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> SubscriptionState {
        self.handle.state()
    }

    /// Sets the data sent with the first request of every load from now on,
    /// sending nothing now.
    // Block 32.
    pub fn set_data(&self, data: impl Into<Vec<u8>>) {
        self.handle.set_data(data.into());
    }

    /// Waits until the subscription is subscribed, without taking an event
    /// from its stream, as [`crate::Subscription::ready`] does.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::ready`].
    pub fn ready(&self) -> impl Future<Output = Result<(), CallError>> + MaybeSend + use<> {
        self.handle.ready()
    }

    /// Events of the subscription queued for its streams and not yet
    /// taken, so that tests can wait for them. Only with the `__test-hooks`
    /// feature.
    #[cfg(feature = "__test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn held_events(&self) -> usize {
        self.handle.held_events::<crate::MapSubscriptionEvent>()
    }

    /// Sets the map subscription's handler, as
    /// [`crate::Subscription::set_handler`] does a subscription's: from now
    /// on it takes the events, one at a time, in order with those of the
    /// client's other handlers; its events count as delivered when its
    /// method returns.
    // Block 45, D13.
    pub fn set_handler(&self, handler: impl super::MapSubscriptionHandler) {
        self.handle
            .set_handler::<crate::MapSubscriptionEvent>(Box::new(handler));
    }

    /// Events from now on, as [`crate::Subscription::events`] gives a
    /// subscription's: an event counts as delivered once this stream yields
    /// it, so take it before calling [`MapSubscription::subscribe`] to get
    /// every one; a later call takes over.
    // Block 15.
    pub fn events(&self) -> impl Stream<Item = MapSubscriptionEvent> + MaybeSend + Unpin + use<> {
        self.handle.events::<MapSubscriptionEvent>()
    }

    /// Starts subscribing: loading the state, at once when the client is
    /// connected, otherwise once it is.
    pub fn subscribe(&self) {
        self.handle.subscribe();
    }

    /// Unsubscribes. A load in progress ends once the page in flight is
    /// read, with an unsubscribe command.
    pub fn unsubscribe(&self) {
        self.handle.unsubscribe();
    }

    /// Sets `key` of the map to `data`, once the subscription is
    /// subscribed, as [`Client::map_publish`] does. The subscription gets
    /// its own change as an update, as every subscriber.
    ///
    /// On a namespace where the server advises a debounce interval
    /// (Centrifugo PRO, `publish_debounce`) the first value of a key is
    /// sent at once and the latest one per interval, each key with its own
    /// window, so that a chatty key cannot delay a quiet one, and this
    /// future resolves with the result of the send that carried later data
    /// for its key, as [`crate::Subscription::publish`] does. Use
    /// [`Client::map_publish`] for data that must arrive: it is never
    /// debounced.
    ///
    /// # Errors
    ///
    /// As [`crate::Subscription::publish`]; the server's refusal keeps its
    /// code (103 without permission, 107 for an empty key). A publish made
    /// while a window of its key is open fails with the error of the send
    /// or the [`MapSubscription::remove`] that replaced it.
    // Block 45, D8; Block 46.
    pub fn publish(
        &self,
        key: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.handle.call(CallRequest::MapPublish {
            channel: self.handle.channel.to_string(),
            key: key.into(),
            data: data.into(),
        });
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(())
        })
    }

    /// Sets `key` of the map to `data` encoded as JSON, as
    /// [`MapSubscription::publish`] does.
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of
    /// [`MapSubscription::publish`].
    // Block 38.
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
    /// The remove is sent at once and opens no debounce window: if the key
    /// has one open, it closes and its pending value is never sent, so a
    /// publish made after it is sent at once, and the publishes waiting in
    /// the window settle with the result of this remove.
    ///
    /// # Errors
    ///
    /// As [`MapSubscription::publish`].
    // Block 46.
    pub fn remove(
        &self,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<(), CallError>> + MaybeSend + 'static {
        let call = self.handle.call(CallRequest::MapRemove {
            channel: self.handle.channel.to_string(),
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
        self.handle.presence()
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
        self.handle.presence_stats()
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
    handler: Option<std::sync::Mutex<Box<dyn super::MapSubscriptionHandler>>>,
}

/// The options' `Debug` output redacts the token.
impl std::fmt::Debug for MapSubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapSubscriptionBuilder")
            .field("channel", &self.channel)
            .field("options", &self.options)
            .field("get_token", &self.get_token.is_some())
            .field("handler", &self.handler.is_some())
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
        F: Fn(crate::SubscriptionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.options.common = self.options.common.token_source();
        self.get_token = Some(Arc::new(move |channel| {
            Box::pin(get_token(crate::SubscriptionTokenContext { channel }))
        }));
        self
    }

    /// Sets the lower bound of the resubscribe delay. Default 500 ms.
    /// More than 0, or `build()` fails with
    /// [`SubscriptionError::ZeroDelay`](crate::SubscriptionError::ZeroDelay).
    #[must_use]
    pub fn min_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options.common = self.options.common.min_resubscribe_delay(delay);
        self
    }

    /// Sets the upper bound of the resubscribe delay. Default 20 s.
    /// More than 0, or `build()` fails with
    /// [`SubscriptionError::ZeroDelay`](crate::SubscriptionError::ZeroDelay).
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

    /// Bytes of page replies one load may read, 64 MiB by default: past it
    /// the subscription sends an unsubscribe command and ends with
    /// `unsubscribed` 5 "map state too large", without subscribing again by
    /// itself. The application holds the state it gets, and its own copy.
    // Block 45, D5.
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
    /// key's previous value, on namespaces that allow it. The client then
    /// holds one copy of every key's value, each within
    /// [`crate::Config::max_message_bytes`], for as long as delta is on.
    // Block 45, D9.
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

    /// The map subscription's handler, which takes its events from the
    /// start instead of a stream; see [`MapSubscription::set_handler`].
    // Block 45, D13.
    #[must_use]
    pub fn handler(mut self, handler: impl super::MapSubscriptionHandler) -> Self {
        self.handler = Some(std::sync::Mutex::new(Box::new(handler)));
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
        self.register()
    }
}

impl Client {
    /// Starts building a map subscription of `kind` to `channel`.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    // Block 45.
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
            handler: None,
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
        super::kind::find(self, channel, map_only)
            .map(|(handle, kind)| MapSubscription { handle, kind })
    }

    /// Every map subscription in the registry.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[must_use]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn map_subscriptions(&self) -> Vec<MapSubscription> {
        super::kind::list(self, map_only)
            .into_iter()
            .map(|(handle, kind)| MapSubscription { handle, kind })
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
        super::kind::remove::<crate::MapSubscriptionEvent>(self, &subscription.handle);
    }
}

impl MapSubscriptionBuilder {
    /// Registers the map subscription, its callback and its handler.
    // Block 61, D3.
    fn register(self) -> Result<MapSubscription, SubscriptionError> {
        crate::subscription::check_resubscribe_delays(&self.options.common)?;
        let kind = self.options.kind;
        let callbacks = self.get_token.is_some().then(|| Callbacks {
            data: None,
            token: self.get_token,
            state: None,
            #[cfg(feature = "unstable")]
            signature: None,
        });
        let options = self.options;
        super::kind::register::<MapSubscriptionEvent>(
            self.shared,
            self.channel,
            |machine, channel| machine.add_map_subscription(channel, options),
            callbacks,
            self.handler,
        )
        .map(|handle| MapSubscription { handle, kind })
    }
}

/// What a map subscription's lookup takes: its map's kind.
fn map_only(kind: SubscriptionKind) -> Option<MapKind> {
    match kind {
        SubscriptionKind::Map(kind) => Some(kind),
        _ => None,
    }
}
