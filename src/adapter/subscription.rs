//! Client-side subscriptions of the tokio client.
// Block 9.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;

use super::client::{Client, history_of, reply_of};
use super::kind::SubscriptionHandle;
use super::shared::{Callbacks, GetData, GetState, GetSubscriptionToken, Shared};
use crate::sans_io::SubscriptionKind;
use crate::sans_io::SubscriptionOptions;
use crate::{
    CallError, CallRequest, DataError, HistoryOptions, HistoryResult, JsonCallError, MaybeSend,
    MaybeSync, PresenceResult, PresenceStatsResult, Publication, StateError, StreamPosition,
    SubscriptionError, SubscriptionEvent, SubscriptionState, TokenError,
};

/// A subscription to a channel, in its client's registry. Clones are
/// handles to the same subscription.
#[derive(Clone)]
pub struct Subscription {
    pub(super) handle: SubscriptionHandle,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.handle.fmt(f, "Subscription", &[])
    }
}

/// Handles to the same subscription are equal.
impl PartialEq for Subscription {
    fn eq(&self, other: &Self) -> bool {
        self.handle.same(&other.handle)
    }
}

impl Eq for Subscription {}

impl Subscription {
    /// The channel subscribed to.
    #[must_use]
    pub fn channel(&self) -> &str {
        self.handle.channel()
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> SubscriptionState {
        self.handle.state()
    }

    /// Sets the tag filter sent in every subscribe command built from now
    /// on, `None` to remove it, sending nothing now; a command already sent
    /// keeps its filter (centrifuge-js `setTagsFilter`).
    ///
    /// # Errors
    ///
    /// [`SubscriptionError::InvalidTagFilter`] for a filter the server
    /// would refuse, [`SubscriptionError::TagFilterWithDelta`] on a
    /// subscription with delta; the filter is then left as it was.
    // Block 35a.
    pub fn set_tag_filter(
        &self,
        filter: Option<crate::TagFilter>,
    ) -> Result<(), SubscriptionError> {
        self.handle.set_tag_filter(filter)
    }

    /// Sets the data sent in every subscribe command built from now on: the
    /// next `subscribe()` and every resubscribe, sending nothing now; a
    /// command already sent keeps its data. Raw JSON with the JSON
    /// protocol, any bytes with Protobuf. A
    /// [`SubscriptionBuilder::get_data`] callback still replaces it on
    /// every attempt (centrifuge-js `setData`).
    // Block 32.
    pub fn set_data(&self, data: impl Into<Vec<u8>>) {
        self.handle.set_data(data.into());
    }

    /// What delta compression did on the subscription since it was created:
    /// zeros without negotiated delta, and once the subscription is
    /// removed.
    ///
    /// ```no_run
    /// # fn run(subscription: &ferrifuge::Subscription) {
    /// let delta = subscription.delta_statistics();
    /// println!("saved {:.0}%", delta.compression_ratio() * 100.0);
    /// # }
    /// ```
    // Block 31.
    #[must_use]
    pub fn delta_statistics(&self) -> crate::DeltaStatistics {
        self.handle.delta_statistics()
    }

    /// Waits until the subscription is subscribed, without taking an event
    /// from its stream. The wait starts when this method returns, and the
    /// future borrows nothing, so it can be spawned. It waits across the
    /// client's reconnects, and while the client is not connected yet: a
    /// subscription subscribed before `connect()` included.
    ///
    /// As [`Client::ready`], it follows the state [`Subscription::state`]
    /// reports, not the events taken, so it can resolve before the
    /// `subscribed` event is taken; and a task holding a stream of the
    /// client with events not taken waits until it is read or dropped. No
    /// timeout applies; wrap the future in one.
    ///
    /// # Errors
    ///
    /// Code 7 "subscription unsubscribed"
    /// (`codes::error::SUBSCRIPTION_UNSUBSCRIBED`) at once when the
    /// subscription is unsubscribed or removed, and when it becomes
    /// unsubscribed while waiting. Code 3 "disconnected" at once when the
    /// client is closed (dropped, or its runtime shut down), and when the
    /// client enters `disconnected` while it waits, as its calls waiting for
    /// it do.
    // Block 30; Block 15.
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
        self.handle.held_events::<SubscriptionEvent>()
    }

    /// This subscription's publications from now on, and nothing else: a
    /// view of its one stream, which takes over from
    /// [`Subscription::events`] as a later `events()` call takes over from
    /// it. A publication counts as delivered once this stream yields it;
    /// the other events count as delivered as the stream passes them, so an
    /// unread stream holds the connection as an unread `events()` does. It
    /// ends as `events()` does, not on `unsubscribed`.
    ///
    /// It hides what only the other events tell:
    /// - a `SubscribedContext` reporting `was_recovering` true and
    ///   `recovered` false, the one sign of a gap on a recoverable channel
    ///   without `get_state` (with `get_state`, a failed recovery reloads
    ///   the state instead);
    /// - `Unsubscribed`: after a permanent unsubscribe the stream goes
    ///   silent, it does not end;
    /// - `Error` events.
    ///
    /// An `events()` call elsewhere ends this stream, and dropping it counts
    /// the events it had not yielded as delivered.
    ///
    /// ```no_run
    /// # async fn run(subscription: ferrifuge::Subscription) {
    /// use futures_util::StreamExt;
    ///
    /// let mut publications = subscription.publications();
    /// subscription.subscribe();
    /// while let Some(publication) = publications.next().await {
    ///     println!("{} bytes", publication.data.len());
    /// }
    /// # }
    /// ```
    // Block 39; Block 15; Block 11.
    pub fn publications(&self) -> impl Stream<Item = Publication> + MaybeSend + Unpin + use<> {
        use futures_util::StreamExt as _;
        self.events().filter_map(|event| {
            std::future::ready(match event {
                SubscriptionEvent::Publication(publication) => Some(publication),
                _ => None,
            })
        })
    }

    /// Sets the subscription's handler: from now on it takes the
    /// subscription's events, one at a time, in order with those of the
    /// client's other handlers, as a later [`Subscription::events`] call
    /// would; the earlier stream or handler gets no more. Its events count
    /// as delivered when its method returns.
    // Block 41, D1.
    pub fn set_handler(&self, handler: impl super::SubscriptionHandler) {
        self.handle
            .set_handler::<SubscriptionEvent>(Box::new(handler));
    }

    /// Events from now on. An event counts as delivered once this stream
    /// yields it; until a stream is taken, events count as delivered at
    /// once, so take it before calling [`Subscription::subscribe`] to get
    /// every one. A later call takes over: the earlier stream ends and the
    /// events it had not yielded are the new one's. The stream borrows
    /// nothing from the subscription, so it can be kept beside it, and it
    /// ends once the subscription is removed or its client dropped.
    ///
    /// A loop over the stream gives its runtime a turn: once the reading
    /// task has spent its tokio budget, `next()` returns `Pending` and takes
    /// nothing, as tokio's own channels do, so a large recovery does not hold
    /// up the task's other work. In the browser, with no tokio runtime, the
    /// budget never runs out, and `next()` returns events as long as some
    /// are queued.
    // Block 15; Block 63, D1, D3.
    pub fn events(&self) -> impl Stream<Item = SubscriptionEvent> + MaybeSend + Unpin + use<> {
        self.handle.events::<SubscriptionEvent>()
    }

    /// Starts subscribing: at once when the client is connected, otherwise
    /// once it is. The state is [`SubscriptionState::Subscribing`] when this
    /// returns, unless already subscribed.
    pub fn subscribe(&self) {
        self.handle.subscribe();
    }

    /// Unsubscribes. The state is [`SubscriptionState::Unsubscribed`] when
    /// this returns.
    pub fn unsubscribe(&self) {
        self.handle.unsubscribe();
    }

    /// Publishes `data` to the subscription's channel, as
    /// [`Client::publish`] does, once the subscription is subscribed.
    ///
    /// The call is made when this returns. While the subscription is
    /// subscribing, it waits for at most `Config::timeout`; it then waits
    /// for the connection and the reply as a client's call does.
    ///
    /// On a namespace where the server advises a debounce interval
    /// (Centrifugo PRO, `publish_debounce`) the first value is sent at once
    /// and the latest one per interval: a publish merged in an open window
    /// resolves with the result of the send that carried later data, and is
    /// failed, never sent, if its subscription leaves `subscribed` first.
    /// Use [`Client::publish`] for data that must arrive: it is never
    /// debounced.
    ///
    /// # Errors
    ///
    /// As [`Client::rpc`], and [`crate::codes::error::SUBSCRIPTION_UNSUBSCRIBED`]
    /// when the subscription is unsubscribed, or becomes so before the call
    /// is sent; [`crate::codes::error::PUBLISH_DROPPED`] when it waited in a
    /// debounce window that its subscription's re-subscribing dropped.
    /// Entering `disconnected` fails a call waiting for the subscription
    /// with [`crate::codes::error::DISCONNECTED`].
    // Block 46.
    pub fn publish(
        &self,
        data: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<crate::PublishResult, CallError>> + MaybeSend + 'static {
        self.handle.publish(data.into())
    }

    /// Publishes `data` encoded as JSON to the subscription's channel, as
    /// [`Subscription::publish`] does; `data` is encoded when this returns,
    /// so the future borrows nothing.
    ///
    /// ```no_run
    /// # async fn run(subscription: ferrifuge::Subscription) -> Result<(), ferrifuge::JsonCallError> {
    /// subscription.publish_json(&serde_json::json!({"text": "hi"})).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`JsonCallError::Encode`] when `data` does not encode, and nothing is
    /// sent; [`JsonCallError::Call`] with the errors of
    /// [`Subscription::publish`].
    // Block 38.
    pub fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        data: &T,
    ) -> impl Future<Output = Result<(), JsonCallError>> + MaybeSend + 'static {
        let call = serde_json::to_vec(data).map(|data| self.publish(data));
        async move {
            call.map_err(JsonCallError::Encode)?.await?;
            Ok(())
        }
    }

    /// The history of the subscription's channel, as [`Client::history`]
    /// gives it, once the subscription is subscribed.
    ///
    /// # Errors
    ///
    /// As [`Subscription::publish`].
    pub fn history(
        &self,
        options: HistoryOptions,
    ) -> impl Future<Output = Result<HistoryResult, CallError>> + MaybeSend + 'static {
        let call = self.handle.call(CallRequest::History {
            channel: self.handle.channel.to_string(),
            options,
        });
        reply_of(call, history_of)
    }

    /// The clients subscribed to the subscription's channel, once the
    /// subscription is subscribed.
    ///
    /// # Errors
    ///
    /// As [`Subscription::publish`].
    pub fn presence(
        &self,
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + MaybeSend + 'static {
        self.handle.presence()
    }

    /// How many clients and users are subscribed to the subscription's
    /// channel, once the subscription is subscribed.
    ///
    /// # Errors
    ///
    /// As [`Subscription::publish`].
    pub fn presence_stats(
        &self,
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + MaybeSend + 'static {
        self.handle.presence_stats()
    }
}

/// Builds a [`Subscription`] with options.
pub struct SubscriptionBuilder {
    shared: Arc<Shared>,
    channel: String,
    options: SubscriptionOptions,
    get_data: Option<GetData>,
    get_token: Option<GetSubscriptionToken>,
    get_state: Option<GetState>,
    /// Its handler, set once it is built; behind a mutex so that the
    /// builder stays `Sync` for a handler that is only `Send`.
    // Block 41b.
    handler: Option<std::sync::Mutex<Box<dyn super::SubscriptionHandler>>>,
}

/// The options' `Debug` output redacts the token.
impl std::fmt::Debug for SubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionBuilder")
            .field("channel", &self.channel)
            .field("options", &self.options)
            .field("get_data", &self.get_data.is_some())
            .field("get_token", &self.get_token.is_some())
            .field("get_state", &self.get_state.is_some())
            .field("handler", &self.handler.is_some())
            .finish_non_exhaustive()
    }
}

impl SubscriptionBuilder {
    /// Sends `data` with every subscribe command: JSON text with the JSON
    /// protocol, any bytes with Protobuf.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        // The last of `data` and `get_data` set decides.
        self.get_data = None;
        self.options = self.options.data(data.into());
        self
    }

    /// Sets a callback giving the data of every subscribe command, called
    /// with the channel before each one; it replaces [`Self::data`]. Return
    /// [`DataError::Unauthorized`] to unsubscribe for good, or
    /// [`DataError::Failed`] to retry after the resubscribe backoff.
    #[must_use]
    pub fn get_data<F, Fut>(mut self, get_data: F) -> Self
    where
        F: Fn(crate::SubscriptionDataContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Vec<u8>, DataError>> + MaybeSend + 'static,
    {
        self.options = self.options.data_source();
        self.get_data = Some(Arc::new(move |channel| {
            Box::pin(get_data(crate::SubscriptionDataContext { channel }))
        }));
        self
    }

    /// Sets the lower bound of the resubscribe delay. Default 500 ms.
    /// More than 0, or `build()` fails with
    /// [`SubscriptionError::ZeroDelay`](crate::SubscriptionError::ZeroDelay).
    #[must_use]
    pub fn min_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options = self.options.min_resubscribe_delay(delay);
        self
    }

    /// Sets the upper bound of the resubscribe delay. Default 20 s.
    /// More than 0, or `build()` fails with
    /// [`SubscriptionError::ZeroDelay`](crate::SubscriptionError::ZeroDelay).
    #[must_use]
    pub fn max_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options = self.options.max_resubscribe_delay(delay);
        self
    }

    /// Recovers the publications missed since `position` on the first
    /// subscribe, e.g. a position saved by an earlier run of the app.
    #[must_use]
    pub fn since(mut self, position: StreamPosition) -> Self {
        self.options = self.options.since(position);
        self
    }

    /// Asks the server to keep a stream position for the subscription, on a
    /// channel that allows it but does not force it.
    #[must_use]
    pub fn positioned(mut self, positioned: bool) -> Self {
        self.options = self.options.positioned(positioned);
        self
    }

    /// Asks the server to make missed publications recoverable, on a
    /// channel that allows it but does not force it.
    #[must_use]
    pub fn recoverable(mut self, recoverable: bool) -> Self {
        self.options = self.options.recoverable(recoverable);
        self
    }

    /// Asks the server for join and leave pushes, delivered as
    /// [`SubscriptionEvent::Join`] and [`SubscriptionEvent::Leave`]: the
    /// channel must have them, and the client the right to see its
    /// presence (Centrifugo `join_leave`).
    #[must_use]
    pub fn join_leave(mut self, join_leave: bool) -> Self {
        self.options = self.options.join_leave(join_leave);
        self
    }

    /// Asks the server to send publications as deltas in `format` against
    /// the previous one, on channels that allow it (Centrifugo
    /// `allowed_delta_types`). The client rebuilds each one, so the
    /// application always gets whole payloads; whether the server agreed is
    /// [`crate::SubscribedContext::delta`]. A delta that cannot be applied
    /// makes the subscription subscribe again, recovering on channels with
    /// recovery.
    // Block 19.
    #[must_use]
    pub fn delta(mut self, format: crate::DeltaFormat) -> Self {
        self.options = self.options.delta(format);
        self
    }

    /// Asks the server to send only the publications `filter` matches, on
    /// channels with `allow_tags_filter` (Centrifugo from v6.4.0), with
    /// every subscribe command. Building fails when the filter is one the
    /// server would refuse, or goes with [`Self::delta`].
    // Block 35a.
    #[must_use]
    pub fn tag_filter(mut self, filter: crate::TagFilter) -> Self {
        self.options = self.options.tag_filter(filter);
        self
    }

    /// Asks a server with channel compaction (Centrifugo PRO) to name this
    /// subscription by a numeric id instead of its channel in publication,
    /// join and leave pushes, with every subscribe command. A server
    /// without it ignores the request. Off by default.
    // Block 35b.
    #[must_use]
    pub fn channel_compaction(mut self, channel_compaction: bool) -> Self {
        self.options = self.options.channel_compaction(channel_compaction);
        self
    }

    /// Sends `token`, a subscription token (JWT) for the channel, with the
    /// first subscribe command. With [`Self::get_token`] too, the callback
    /// is asked once it expires.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.options = self.options.token(token.into());
        self
    }

    /// Sets a callback giving a new subscription token, called with the
    /// channel when one is needed: before the first subscribe without a
    /// token, after the server rejects one as expired, and before it
    /// expires. Return [`TokenError::Unauthorized`] (or an empty token) to
    /// unsubscribe for good, or [`TokenError::Failed`] to retry later.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, get_token: F) -> Self
    where
        F: Fn(crate::SubscriptionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.options = self.options.token_source();
        self.get_token = Some(Arc::new(move |channel| {
            Box::pin(get_token(crate::SubscriptionTokenContext { channel }))
        }));
        self
    }

    /// Sets a callback loading the application's state for the channel and
    /// returning the stream position it was loaded at (spec, "Subscription
    /// getState"). It is called with the channel before the first subscribe
    /// without [`Self::since`], and again when a recovery fails: the
    /// subscription then stays subscribing, with no publication, until the
    /// state is reloaded and the subscribe from its position succeeds.
    /// Return [`StateError`] to retry after the resubscribe backoff.
    ///
    /// Read the stream position before the state, so that publications made
    /// meanwhile are recovered rather than missed.
    #[must_use]
    pub fn get_state<F, Fut>(mut self, get_state: F) -> Self
    where
        F: Fn(crate::GetStateContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<StreamPosition, StateError>> + MaybeSend + 'static,
    {
        self.options = self.options.state_source();
        self.get_state = Some(Arc::new(move |channel| {
            Box::pin(get_state(crate::GetStateContext { channel }))
        }));
        self
    }

    /// Adds the subscription, unsubscribed, to the client's registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to the channel already.
    pub fn build(self) -> Result<Subscription, SubscriptionError> {
        self.register()
    }

    /// The subscription's handler, which takes its events from the start
    /// instead of a stream; see [`Subscription::set_handler`].
    // Block 41, D1.
    #[must_use]
    pub fn handler(mut self, handler: impl super::SubscriptionHandler) -> Self {
        self.handler = Some(std::sync::Mutex::new(Box::new(handler)));
        self
    }
}

impl Client {
    /// Starts building a subscription to `channel`.
    pub fn subscription_builder(&self, channel: impl Into<String>) -> SubscriptionBuilder {
        SubscriptionBuilder {
            shared: Arc::clone(self.shared()),
            channel: channel.into(),
            options: SubscriptionOptions::default(),
            get_data: None,
            get_token: None,
            get_state: None,
            handler: None,
        }
    }

    /// Adds a subscription to `channel`, unsubscribed, to the registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already.
    pub fn new_subscription(
        &self,
        channel: impl Into<String>,
    ) -> Result<Subscription, SubscriptionError> {
        self.subscription_builder(channel).build()
    }

    /// The subscription to `channel` in the registry, if any.
    #[must_use]
    pub fn get_subscription(&self, channel: &str) -> Option<Subscription> {
        super::kind::find(self, channel, stream_only).map(|(handle, ())| Subscription { handle })
    }

    /// Every subscription in the registry.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<Subscription> {
        super::kind::list(self, stream_only)
            .into_iter()
            .map(|(handle, ())| Subscription { handle })
            .collect()
    }

    /// The channels of the server-side subscriptions the server made for
    /// this client, in channel order (spec, "Server-side subscriptions").
    ///
    /// A snapshot: events already emitted, or about to be, may have
    /// overtaken it, so the [`crate::ClientEvent::ServerSubscription`]
    /// events are the authority on changes. A Ferrifuge addition: neither
    /// centrifuge-js nor centrifugal/centrifuge-rust (`docs/api.md`)
    /// exposes the server-side registry.
    #[must_use]
    pub fn server_subscriptions(&self) -> Vec<String> {
        self.shared()
            .core()
            .map(|core| core.machine.server_subscriptions())
            .unwrap_or_default()
    }

    /// Unsubscribes `subscription` and removes it from the registry; a new
    /// subscription to its channel can then be created. Its event streams
    /// end after the final "unsubscribed". A subscription of another client
    /// is left alone.
    pub fn remove_subscription(&self, subscription: &Subscription) {
        super::kind::remove::<SubscriptionEvent>(self, &subscription.handle);
    }
}

impl SubscriptionBuilder {
    /// Registers the subscription, its callbacks and its handler.
    // Block 61, D3.
    fn register(self) -> Result<Subscription, SubscriptionError> {
        crate::subscription::check_resubscribe_delays(&self.options)?;
        let callbacks = (self.get_data.is_some()
            || self.get_token.is_some()
            || self.get_state.is_some())
        .then(|| Callbacks {
            data: self.get_data,
            token: self.get_token,
            state: self.get_state,
            #[cfg(feature = "unstable")]
            signature: None,
        });
        let options = self.options;
        super::kind::register::<SubscriptionEvent>(
            self.shared,
            self.channel,
            |machine, channel| machine.add_subscription(channel, options),
            callbacks,
            self.handler,
        )
        .map(|handle| Subscription { handle })
    }
}

/// What a stream subscription's lookup takes: map and shared poll
/// subscriptions are listed apart.
// Blocks 45, 47, D2.
fn stream_only(kind: SubscriptionKind) -> Option<()> {
    (kind == SubscriptionKind::Stream).then_some(())
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use crate::adapter::shared::lock;
    use futures_util::StreamExt;

    use super::*;
    use crate::Config;

    const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

    /// The stream borrows nothing: an application can keep it next to its
    /// subscription, returned together or in one struct.
    fn subscription_with_events(
        client: &Client,
    ) -> (Subscription, impl Stream<Item = SubscriptionEvent> + Unpin) {
        let subscription = client.new_subscription("news").unwrap();
        let events = subscription.events();
        (subscription, events)
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn an_event_stream_does_not_borrow_its_subscription() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let (subscription, mut events) = subscription_with_events(&client);

        subscription.subscribe();

        assert_eq!(
            events.next().await.map(|event| event.name()),
            Some("subscribing")
        );
    }

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn removing_ends_the_event_streams_after_the_last_event() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        let mut events = subscription.events();
        subscription.subscribe();

        client.remove_subscription(&subscription);

        assert_eq!(
            events.next().await.map(|event| event.name()),
            Some("subscribing")
        );
        assert_eq!(
            events.next().await.map(|event| event.name()),
            Some("unsubscribed")
        );
        assert_eq!(events.next().await, None);
        assert_eq!(client.get_subscription("news"), None);
        // A stream asked for afterwards ends at once.
        assert_eq!(subscription.events().next().await, None);
    }

    #[tokio::test]
    async fn streams_end_and_callbacks_go_when_the_client_is_dropped() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let subscription = client
            .subscription_builder("news")
            .get_data(|_| async { Ok(Vec::new()) })
            .build()
            .unwrap();
        let mut events = subscription.events();
        let mut client_events = client.events();

        drop(client);

        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while events.next().await.is_some() {}
            while client_events.next().await.is_some() {}
        })
        .await;
        assert!(
            ended.is_ok(),
            "streams still open after the client was dropped"
        );
        assert_eq!(subscription.events().next().await, None);
        assert!(lock(&subscription.handle.shared.callbacks).0.is_empty());
    }

    #[tokio::test]
    async fn calls_after_the_client_is_dropped_change_nothing() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        drop(client);
        let before = {
            let core = subscription.handle.shared.core().expect("a core");
            (
                core.machine.subscription_state(subscription.handle.id),
                core.actions.len(),
            )
        };

        for _ in 0..10 {
            subscription.subscribe();
            subscription.unsubscribe();
        }

        let core = subscription.handle.shared.core().expect("a core");
        assert_eq!(
            (
                core.machine.subscription_state(subscription.handle.id),
                core.actions.len()
            ),
            before
        );
    }

    #[tokio::test]
    async fn a_builder_outliving_its_client_builds_nothing() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let builder = client
            .subscription_builder("news")
            .get_data(|_| async { Ok(Vec::new()) });
        let shared = Arc::clone(client.shared());
        drop(client);

        let built = builder.build();

        assert_eq!(built, Err(SubscriptionError::ClientClosed));
        assert!(lock(&shared.callbacks).0.is_empty());
        assert_eq!(
            shared
                .core()
                .expect("a core")
                .machine
                .find_subscription("news"),
            None
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn debug_output_never_shows_subscription_tokens() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let builder = client
            .subscription_builder("news")
            .token("sub-secret")
            .get_token(|_| async { Ok("sub-fetched".to_owned()) });
        let printed_builder = format!("{builder:?}");
        let subscription = builder.build().unwrap();

        let printed = format!("{printed_builder} {subscription:?} {client:?}");

        assert!(!printed.contains("sub-secret"), "{printed}");
    }

    #[test]
    fn the_last_data_setting_decides() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _guard = runtime.enter();
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();

        let bytes_last = client
            .subscription_builder("a")
            .get_data(|_| async { Ok(Vec::new()) })
            .data(b"{}".to_vec());
        let callback_last = client
            .subscription_builder("b")
            .data(b"{}".to_vec())
            .get_data(|_| async { Ok(Vec::new()) });

        assert!(!bytes_last.options.data_source && bytes_last.get_data.is_none());
        assert_eq!(bytes_last.options.data.as_deref(), Some(&b"{}"[..]));
        assert!(callback_last.options.data_source && callback_last.get_data.is_some());
        assert_eq!(callback_last.options.data, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_subscription_of_another_client_is_not_removed() {
        let first = Client::new(UNREACHABLE, Config::default()).unwrap();
        let second = Client::new(UNREACHABLE, Config::default()).unwrap();
        let mine = first.new_subscription("a").unwrap();
        let theirs = second.new_subscription("b").unwrap();

        first.remove_subscription(&theirs);

        assert_eq!(first.get_subscription("a"), Some(mine));
        assert_eq!(second.get_subscription("b"), Some(theirs));
    }
}
