//! Client-side subscriptions of the tokio client (Block 9).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_channel::mpsc as events;
use futures_core::Stream;

use super::client::{Callbacks, Client, GetData, GetState, GetSubscriptionToken, Shared, lock};
use crate::machine::{Input, SubscriptionId, SubscriptionOptions};
use crate::{
    DataError, StateError, StreamPosition, SubscriptionError, SubscriptionEvent, SubscriptionState,
    TokenError,
};

/// A subscription to a channel, in its client's registry. Clones are
/// handles to the same subscription.
#[derive(Clone)]
pub struct Subscription {
    shared: Arc<Shared>,
    id: SubscriptionId,
    channel: Arc<str>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("channel", &self.channel)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Handles to the same subscription are equal.
impl PartialEq for Subscription {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared) && self.id == other.id
    }
}

impl Eq for Subscription {}

impl Subscription {
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

    /// Events from now on. Each call returns an independent stream; it
    /// borrows nothing from the subscription, so it can be kept beside it,
    /// and it ends once the subscription is removed or its client dropped.
    pub fn events(&self) -> impl Stream<Item = SubscriptionEvent> + Send + Unpin + use<> {
        let (sender, receiver) = events::unbounded();
        // Checked against the registry under the core lock: removal leaves
        // the registry first, then ends the streams, so a removed
        // subscription's stream ends at once instead of leaking a sender.
        let core = lock(&self.shared.core);
        if core.machine.subscription_state(self.id).is_some()
            && !self.shared.closed.load(Ordering::Acquire)
        {
            lock(&self.shared.subscription_subscribers)
                .entry(self.id)
                .or_default()
                .push(sender);
        }
        drop(core);
        receiver
    }

    /// Starts subscribing: at once when the client is connected, otherwise
    /// once it is. The state is [`SubscriptionState::Subscribing`] when this
    /// returns, unless already subscribed.
    pub fn subscribe(&self) {
        self.shared.apply(Input::Subscribe(self.id));
    }

    /// Unsubscribes. The state is [`SubscriptionState::Unsubscribed`] when
    /// this returns.
    pub fn unsubscribe(&self) {
        self.shared.apply(Input::Unsubscribe(self.id));
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
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, DataError>> + Send + 'static,
    {
        self.options = self.options.data_source();
        self.get_data = Some(Arc::new(move |channel| Box::pin(get_data(channel))));
        self
    }

    /// Sets the lower bound of the resubscribe delay. Default 500 ms.
    #[must_use]
    pub fn min_resubscribe_delay(mut self, delay: Duration) -> Self {
        self.options = self.options.min_resubscribe_delay(delay);
        self
    }

    /// Sets the upper bound of the resubscribe delay. Default 20 s.
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
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + Send + 'static,
    {
        self.options = self.options.token_source();
        self.get_token = Some(Arc::new(move |channel| Box::pin(get_token(channel))));
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
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StreamPosition, StateError>> + Send + 'static,
    {
        self.options = self.options.state_source();
        self.get_state = Some(Arc::new(move |channel| Box::pin(get_state(channel))));
        self
    }

    /// Adds the subscription, unsubscribed, to the client's registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to the channel already.
    pub fn build(self) -> Result<Subscription, SubscriptionError> {
        // Registered with its callback under the core lock: no other thread
        // can find and subscribe it before the callback is there.
        let mut core = lock(&self.shared.core);
        // After shutdown nothing could ever subscribe it, and a callback
        // registered now would outlive the driver's cleanup.
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(SubscriptionError::ClientClosed);
        }
        let id = core
            .machine
            .add_subscription(self.channel.clone(), self.options)?;
        if self.get_data.is_some() || self.get_token.is_some() || self.get_state.is_some() {
            lock(&self.shared.callbacks).0.insert(
                id,
                Callbacks {
                    data: self.get_data,
                    token: self.get_token,
                    state: self.get_state,
                },
            );
        }
        drop(core);
        Ok(Subscription {
            shared: self.shared,
            id,
            channel: self.channel.into(),
        })
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
        let id = lock(&self.shared().core)
            .machine
            .find_subscription(channel)?;
        Some(Subscription {
            shared: Arc::clone(self.shared()),
            id,
            channel: channel.into(),
        })
    }

    /// Every subscription in the registry.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<Subscription> {
        let subscriptions = lock(&self.shared().core).machine.subscriptions();
        subscriptions
            .into_iter()
            .map(|(id, channel)| Subscription {
                shared: Arc::clone(self.shared()),
                id,
                channel: channel.into(),
            })
            .collect()
    }

    /// Unsubscribes `subscription` and removes it from the registry; a new
    /// subscription to its channel can then be created. Its event streams
    /// end after the final "unsubscribed". A subscription of another client
    /// is left alone.
    pub fn remove_subscription(&self, subscription: &Subscription) {
        if !Arc::ptr_eq(&subscription.shared, self.shared()) {
            return;
        }
        self.shared()
            .apply(Input::RemoveSubscription(subscription.id));
        lock(&self.shared().callbacks).0.remove(&subscription.id);
        // The final events were delivered by the call above.
        lock(&self.shared().subscription_subscribers).remove(&subscription.id);
    }
}

#[cfg(test)]
mod tests {
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
        assert!(lock(&subscription.shared.callbacks).0.is_empty());
    }

    #[tokio::test]
    async fn calls_after_the_client_is_dropped_change_nothing() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let subscription = client.new_subscription("news").unwrap();
        drop(client);
        let before = {
            let core = lock(&subscription.shared.core);
            (
                core.machine.subscription_state(subscription.id),
                core.actions.len(),
            )
        };

        for _ in 0..10 {
            subscription.subscribe();
            subscription.unsubscribe();
        }

        let core = lock(&subscription.shared.core);
        assert_eq!(
            (
                core.machine.subscription_state(subscription.id),
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
        assert_eq!(lock(&shared.core).machine.find_subscription("news"), None);
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
