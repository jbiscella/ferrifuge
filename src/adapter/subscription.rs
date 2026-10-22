//! Client-side subscriptions of the tokio client (Block 9).

use std::future::Future;
use std::sync::Arc;

use futures_channel::mpsc as events;
use futures_core::Stream;

use super::client::{Client, GetData, Shared, lock};
use crate::machine::{Input, SubscriptionId, SubscriptionOptions};
use crate::{DataError, DuplicateSubscription, SubscriptionEvent, SubscriptionState};

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

    /// Events from now on. Each call returns an independent stream.
    pub fn events(&self) -> impl Stream<Item = SubscriptionEvent> + Send + Unpin + 'static {
        let (sender, receiver) = events::unbounded();
        // Checked against the registry under the core lock: removal leaves
        // the registry first, then ends the streams, so a removed
        // subscription's stream ends at once instead of leaking a sender.
        let core = lock(&self.shared.core);
        if core.machine.subscription_state(self.id).is_some() {
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
}

impl std::fmt::Debug for SubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionBuilder")
            .field("channel", &self.channel)
            .field("options", &self.options)
            .field("get_data", &self.get_data.is_some())
            .finish_non_exhaustive()
    }
}

impl SubscriptionBuilder {
    /// Sends `data` with every subscribe command: JSON text with the JSON
    /// protocol, any bytes with Protobuf.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
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

    /// Adds the subscription, unsubscribed, to the client's registry.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to the channel already.
    pub fn build(self) -> Result<Subscription, DuplicateSubscription> {
        // Registered with its callback under the core lock: no other thread
        // can find and subscribe it before the callback is there.
        let mut core = lock(&self.shared.core);
        let id = core
            .machine
            .add_subscription(self.channel.clone(), self.options)?;
        if let Some(get_data) = self.get_data {
            lock(&self.shared.data_callbacks).0.insert(id, get_data);
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
    ) -> Result<Subscription, DuplicateSubscription> {
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
        lock(&self.shared().data_callbacks)
            .0
            .remove(&subscription.id);
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

    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn removing_ends_the_event_streams_after_the_last_event() {
        let client = Client::new(UNREACHABLE, Config::default());
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

    #[tokio::test(flavor = "current_thread")]
    async fn a_subscription_of_another_client_is_not_removed() {
        let first = Client::new(UNREACHABLE, Config::default());
        let second = Client::new(UNREACHABLE, Config::default());
        let mine = first.new_subscription("a").unwrap();
        let theirs = second.new_subscription("b").unwrap();

        first.remove_subscription(&theirs);

        assert_eq!(first.get_subscription("a"), Some(mine));
        assert_eq!(second.get_subscription("b"), Some(theirs));
    }
}
