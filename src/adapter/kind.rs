//! What the subscription kinds share: the handle of a subscription in the
//! registry, and the code each kind's public methods call, written once
//! for every kind.
// Block 61, D3.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use super::client::{Client, presence_of, presence_stats_of, reply_of};
use super::shared::{Callbacks, EventStream, Kind, Shared, lock};
use crate::sans_io::{Input, Machine, SubscriptionId, SubscriptionKind};
use crate::{
    CallError, CallReply, CallRequest, MaybeSend, PresenceResult, PresenceStatsResult,
    SubscriptionError, SubscriptionState,
};

/// A subscription of any kind in a client's registry: what its handle
/// holds, and the methods the kinds share.
#[derive(Clone)]
pub(super) struct SubscriptionHandle {
    pub(super) shared: Arc<Shared>,
    pub(super) id: SubscriptionId,
    pub(super) channel: Arc<str>,
}

impl SubscriptionHandle {
    /// The `Debug` output of a subscription of type `name`: its channel,
    /// the fields `extra`, and its state.
    pub(super) fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        name: &str,
        extra: &[(&str, &dyn std::fmt::Debug)],
    ) -> std::fmt::Result {
        let mut f = f.debug_struct(name);
        f.field("channel", &self.channel);
        for (field, value) in extra {
            f.field(field, value);
        }
        f.field("state", &self.state()).finish_non_exhaustive()
    }

    /// Whether `other` is the same subscription of the same client.
    pub(super) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared) && self.id == other.id
    }

    pub(super) fn channel(&self) -> &str {
        &self.channel
    }

    pub(super) fn state(&self) -> SubscriptionState {
        lock(&self.shared.core)
            .machine
            .subscription_state(self.id)
            .unwrap_or(SubscriptionState::Unsubscribed)
    }

    pub(super) fn set_tag_filter(
        &self,
        filter: Option<crate::TagFilter>,
    ) -> Result<(), SubscriptionError> {
        lock(&self.shared.core)
            .machine
            .check_subscription_tag_filter(self.id, filter.as_ref())?;
        self.shared
            .apply(Input::SetSubscriptionTagFilter(self.id, filter));
        Ok(())
    }

    pub(super) fn set_data(&self, data: Vec<u8>) {
        self.shared.apply(Input::SetSubscriptionData(self.id, data));
    }

    pub(super) fn delta_statistics(&self) -> crate::DeltaStatistics {
        lock(&self.shared.core)
            .machine
            .delta_statistics(self.id)
            .unwrap_or_default()
    }

    pub(super) fn ready(&self) -> impl Future<Output = Result<(), CallError>> + MaybeSend + use<> {
        super::ready::ready(&self.shared, super::ready::Of::Subscription(self.id))
    }

    pub(super) fn subscribe(&self) {
        self.shared.apply(Input::Subscribe(self.id));
    }

    pub(super) fn unsubscribe(&self) {
        self.shared.apply(Input::Unsubscribe(self.id));
    }

    /// Publishes `data` to the channel, as a stream subscription does.
    pub(super) fn publish(
        &self,
        data: Vec<u8>,
    ) -> impl Future<Output = Result<crate::PublishResult, CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::Publish {
            channel: self.channel.to_string(),
            data,
        });
        reply_of(call, |reply| {
            matches!(reply, CallReply::Publish).then_some(crate::PublishResult {})
        })
    }

    pub(super) fn presence(
        &self,
    ) -> impl Future<Output = Result<PresenceResult, CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::Presence {
            channel: self.channel.to_string(),
        });
        reply_of(call, presence_of)
    }

    pub(super) fn presence_stats(
        &self,
    ) -> impl Future<Output = Result<PresenceStatsResult, CallError>> + MaybeSend + 'static {
        let call = self.call(CallRequest::PresenceStats {
            channel: self.channel.to_string(),
        });
        reply_of(call, presence_stats_of)
    }

    /// Makes `request`, a call of this subscription.
    pub(super) fn call(
        &self,
        request: CallRequest,
    ) -> impl Future<Output = Result<CallReply, CallError>> + MaybeSend + 'static {
        self.shared.call(Some(self.id), request)
    }

    /// The subscription's events of kind `E` from now on.
    // Block 15.
    pub(super) fn events<E: Kind>(&self) -> EventStream<E> {
        // Checked against the registry under the core lock: removal leaves
        // the registry first, then closes the outbox, so a removed
        // subscription's stream ends at once.
        let mut core = lock(&self.shared.core);
        let number = if core.machine.subscription_state(self.id).is_some()
            && !self.shared.closed.load(Ordering::Acquire)
        {
            let number = E::outboxes(&mut lock(&self.shared.outboxes))
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

    /// The events of kind `E` queued for the subscription's streams and not
    /// yet taken.
    #[cfg(feature = "__test-hooks")]
    pub(super) fn held_events<E: Kind>(&self) -> usize {
        E::outboxes(&mut lock(&self.shared.outboxes))
            .get(&self.id)
            .map_or(0, super::outbox::Outbox::held)
    }

    /// Sets the subscription's handler, of kind `E`.
    // Block 41, D1.
    pub(super) fn set_handler<E: Kind>(&self, handler: Box<E::Handler>) {
        super::handler::set_handler::<E>(&self.shared, self.id, handler);
    }
}

/// Registers a subscription of kind `E` to `channel` with `add`, its
/// callbacks and its handler: under the core lock, so that no other thread
/// can find and subscribe it before its callbacks and handler are there.
pub(super) fn register<E: Kind>(
    shared: Arc<Shared>,
    channel: String,
    add: impl FnOnce(&mut Machine, String) -> Result<SubscriptionId, SubscriptionError>,
    callbacks: Option<Callbacks>,
    handler: Option<Mutex<Box<E::Handler>>>,
) -> Result<SubscriptionHandle, SubscriptionError> {
    let mut core = lock(&shared.core);
    // After shutdown nothing could ever subscribe it, and a callback
    // registered now would outlive the driver's cleanup.
    if shared.closed.load(Ordering::Acquire) {
        return Err(SubscriptionError::ClientClosed);
    }
    let id = add(&mut core.machine, channel.clone())?;
    shared.log.subscription(id, &channel);
    if let Some(callbacks) = callbacks {
        lock(&shared.callbacks).0.insert(id, callbacks);
    }
    let replaced = handler.and_then(|handler| {
        let handler = handler.into_inner().unwrap_or_else(PoisonError::into_inner);
        super::handler::install_handler::<E>(&shared, &mut core, id, handler)
    });
    drop(core);
    drop(replaced);
    Ok(SubscriptionHandle {
        shared,
        id,
        channel: channel.into(),
    })
}

/// Removes `subscription`, of kind `E`, if it is one of `client`'s: the
/// machine queues its final events, then its streams end once they have
/// yielded them.
pub(super) fn remove<E: Kind>(client: &Client, subscription: &SubscriptionHandle) {
    let shared = client.shared();
    if !Arc::ptr_eq(&subscription.shared, shared) {
        return;
    }
    shared.apply(Input::RemoveSubscription(subscription.id));
    lock(&shared.callbacks).0.remove(&subscription.id);
    shared.log.subscription_removed(subscription.id);
    let mut outboxes = lock(&shared.outboxes);
    let outboxes = E::outboxes(&mut outboxes);
    if let Some(outbox) = outboxes.get_mut(&subscription.id) {
        outbox.close();
        if outbox.is_finished() {
            outboxes.remove(&subscription.id);
        }
    }
}

/// The subscription to `channel` in `client`'s registry, with what `pick`
/// takes of its kind; `None` when `pick` takes nothing.
pub(super) fn find<T>(
    client: &Client,
    channel: &str,
    pick: impl Fn(SubscriptionKind) -> Option<T>,
) -> Option<(SubscriptionHandle, T)> {
    let core = lock(&client.shared().core);
    let id = core.machine.find_subscription(channel)?;
    let picked = pick(core.machine.subscription_kind(id)?)?;
    drop(core);
    Some((
        SubscriptionHandle {
            shared: Arc::clone(client.shared()),
            id,
            channel: channel.into(),
        },
        picked,
    ))
}

/// Every subscription in `client`'s registry, with what `pick` takes of its
/// kind; those it takes nothing of are left out.
pub(super) fn list<T>(
    client: &Client,
    pick: impl Fn(SubscriptionKind) -> Option<T>,
) -> Vec<(SubscriptionHandle, T)> {
    let core = lock(&client.shared().core);
    let picked: Vec<_> = core
        .machine
        .subscriptions()
        .into_iter()
        .filter_map(|(id, channel)| {
            let picked = pick(core.machine.subscription_kind(id)?)?;
            Some((id, channel, picked))
        })
        .collect();
    drop(core);
    picked
        .into_iter()
        .map(|(id, channel, picked)| {
            (
                SubscriptionHandle {
                    shared: Arc::clone(client.shared()),
                    id,
                    channel: channel.into(),
                },
                picked,
            )
        })
        .collect()
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use futures_util::StreamExt;

    use crate::{Client, Config};

    const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

    /// The names of the events `events` yields until it ends.
    async fn names_until_the_end<E>(
        events: impl futures_core::Stream<Item = E> + Unpin,
        name: impl Fn(&E) -> &'static str,
    ) -> Vec<&'static str> {
        let all = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            events.collect::<Vec<_>>(),
        )
        .await
        .expect("the stream ends");
        all.iter().map(name).collect()
    }

    // Block 61, D3: the generic removal, for each kind, ends the stream
    // after the final events the removal queued.
    // Single-threaded runtime: the driver task cannot run in between.
    #[tokio::test(flavor = "current_thread")]
    async fn removing_a_subscription_of_each_kind_ends_its_stream_after_its_final_events() {
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let stream = client.new_subscription("news").unwrap();
        let events = stream.events();
        stream.subscribe();
        client.remove_subscription(&stream);
        assert_eq!(
            names_until_the_end(events, crate::SubscriptionEvent::name).await,
            ["subscribing", "unsubscribed"]
        );

        #[cfg(feature = "unstable")]
        {
            let map = client
                .new_map_subscription("map", crate::MapKind::Map)
                .unwrap();
            let events = map.events();
            map.subscribe();
            client.remove_map_subscription(&map);
            assert_eq!(
                names_until_the_end(events, crate::MapSubscriptionEvent::name).await,
                ["subscribing", "unsubscribed"]
            );

            let poll = client.new_shared_poll_subscription("poll").unwrap();
            let events = poll.events();
            poll.subscribe();
            client.remove_shared_poll_subscription(&poll);
            assert_eq!(
                names_until_the_end(events, crate::SharedPollSubscriptionEvent::name).await,
                ["subscribing", "unsubscribed"]
            );
        }
    }

    // Block 61, D3: the registry's stream subscriptions are those of kind
    // `Stream`.
    #[cfg(feature = "unstable")]
    #[test]
    fn get_subscription_and_subscriptions_leave_out_map_and_shared_poll_subscriptions() {
        let runtime = crate::adapter::test_support::idle_runtime();
        let _entered = runtime.enter();
        let client = Client::new(UNREACHABLE, Config::default()).unwrap();
        let stream = client.new_subscription("news").unwrap();
        let _map = client
            .new_map_subscription("map", crate::MapKind::Map)
            .unwrap();
        let _poll = client.new_shared_poll_subscription("poll").unwrap();

        assert_eq!(client.get_subscription("news"), Some(stream.clone()));
        assert_eq!(client.get_subscription("map"), None);
        assert_eq!(client.get_subscription("poll"), None);
        assert_eq!(client.subscriptions(), [stream]);
    }
}
