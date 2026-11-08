//! Event handlers (Block 41b): one more consumer of the client's and its
//! subscriptions' events, beside the streams, taking over as a stream does
//! (Block 15). One consumer per client runs every handler of the client and
//! of its subscriptions, one at a time, in the order the events were
//! emitted. An event counts as taken when its handler returns (D3).

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::client::{Shared, lock};
use super::outbox::Head;
use crate::machine::{EventId, Input, SubscriptionId};
use crate::{
    ClientError, ClientEvent, ClientInfo, Publication, ServerSubscriptionEvent, Subscribed,
    SubscriptionEvent,
};

/// Receives a client's events, one at a time, in order (Block 41, D1): the
/// client's own and those of its server-side subscriptions. Set with
/// [`ClientBuilder::handler`](crate::ClientBuilder::handler) or
/// [`Client::set_handler`](crate::Client::set_handler); it takes over the
/// client's stream, as a second `events()` call does.
///
/// [`on_event`](Self::on_event) is called for each event, and by default
/// calls the method of its kind, which does nothing unless overridden. A
/// closure `FnMut(ClientEvent)` is a handler too. An event counts as
/// delivered when the method returns (Block 15); a method that panics
/// disconnects the client with code 2 and its event is not taken, so
/// recovery brings it back (D4).
///
/// Methods are called on a task of the client's runtime (D2): one that
/// blocks holds a worker of the runtime, and on a current-thread runtime
/// nothing else of it runs, the client's timers and pongs included, until
/// it returns.
///
/// ```no_run
/// # fn run(client: ferrifuge::Client) {
/// struct Log;
///
/// impl ferrifuge::ClientHandler for Log {
///     fn on_connected(&mut self, client_id: String, _: String, _: Option<Vec<u8>>) {
///         println!("connected as {client_id}");
///     }
/// }
///
/// client.set_handler(Log);
/// # }
/// ```
#[allow(unused_variables)] // The defaults ignore their event.
pub trait ClientHandler: Send + 'static {
    /// Called for each event, in order; by default, the method of its kind.
    fn on_event(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::Connecting { code, reason } => self.on_connecting(code, reason),
            ClientEvent::Connected {
                client_id,
                transport,
                data,
                ..
            } => self.on_connected(client_id, transport, data),
            ClientEvent::Disconnected { code, reason } => self.on_disconnected(code, reason),
            ClientEvent::Error(error) => self.on_error(error),
            ClientEvent::ServerSubscription { channel, event } => {
                self.on_server_subscription(channel, event);
            }
            ClientEvent::Message(data) => self.on_message(data),
        }
    }

    /// [`ClientEvent::Connecting`].
    fn on_connecting(&mut self, code: u32, reason: String) {}

    /// [`ClientEvent::Connected`].
    fn on_connected(&mut self, client_id: String, transport: String, data: Option<Vec<u8>>) {}

    /// [`ClientEvent::Disconnected`].
    fn on_disconnected(&mut self, code: u32, reason: String) {}

    /// [`ClientEvent::Error`].
    fn on_error(&mut self, error: ClientError) {}

    /// [`ClientEvent::ServerSubscription`].
    fn on_server_subscription(&mut self, channel: String, event: ServerSubscriptionEvent) {}

    /// [`ClientEvent::Message`].
    fn on_message(&mut self, data: Vec<u8>) {}
}

impl<F: FnMut(ClientEvent) + Send + 'static> ClientHandler for F {
    fn on_event(&mut self, event: ClientEvent) {
        self(event);
    }
}

/// Receives a subscription's events, one at a time, in order, as
/// [`ClientHandler`] does the client's (Block 41, D1). Set with
/// [`SubscriptionBuilder::handler`](crate::SubscriptionBuilder::handler) or
/// [`Subscription::set_handler`](crate::Subscription::set_handler); it
/// takes over the subscription's stream.
///
/// ```no_run
/// # fn run(subscription: ferrifuge::Subscription) {
/// subscription.set_handler(|event: ferrifuge::SubscriptionEvent| {
///     if let ferrifuge::SubscriptionEvent::Publication(publication) = event {
///         println!("{} bytes", publication.data.len());
///     }
/// });
/// subscription.subscribe();
/// # }
/// ```
#[allow(unused_variables)] // The defaults ignore their event.
pub trait SubscriptionHandler: Send + 'static {
    /// Called for each event, in order; by default, the method of its kind.
    fn on_event(&mut self, event: SubscriptionEvent) {
        match event {
            SubscriptionEvent::Subscribing { code, reason } => self.on_subscribing(code, reason),
            SubscriptionEvent::Subscribed(subscribed) => self.on_subscribed(subscribed),
            SubscriptionEvent::Unsubscribed { code, reason } => {
                self.on_unsubscribed(code, reason);
            }
            SubscriptionEvent::Publication(publication) => self.on_publication(publication),
            SubscriptionEvent::Error(error) => self.on_error(error),
            SubscriptionEvent::Join(info) => self.on_join(info),
            SubscriptionEvent::Leave(info) => self.on_leave(info),
        }
    }

    /// [`SubscriptionEvent::Subscribing`].
    fn on_subscribing(&mut self, code: u32, reason: String) {}

    /// [`SubscriptionEvent::Subscribed`].
    fn on_subscribed(&mut self, subscribed: Subscribed) {}

    /// [`SubscriptionEvent::Unsubscribed`].
    fn on_unsubscribed(&mut self, code: u32, reason: String) {}

    /// [`SubscriptionEvent::Publication`].
    fn on_publication(&mut self, publication: Publication) {}

    /// [`SubscriptionEvent::Error`].
    fn on_error(&mut self, error: ClientError) {}

    /// [`SubscriptionEvent::Join`].
    fn on_join(&mut self, info: ClientInfo) {}

    /// [`SubscriptionEvent::Leave`].
    fn on_leave(&mut self, info: ClientInfo) {}
}

impl<F: FnMut(SubscriptionEvent) + Send + 'static> SubscriptionHandler for F {
    fn on_event(&mut self, event: SubscriptionEvent) {
        self(event);
    }
}

/// Where a client's handlers run (Block 41, D2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventDelivery {
    /// On a task of the client's runtime, one handler call at a time.
    #[default]
    Task,
}

thread_local! {
    /// The client whose handler this thread runs, by the address of its
    /// `Shared`; 0 when none (Block 41, D6).
    static RUNNING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Marks this thread as running a handler of `shared` until dropped.
struct Marked(usize);

impl Marked {
    fn new(shared: &Shared) -> Self {
        Self(RUNNING.replace(std::ptr::from_ref(shared) as usize))
    }
}

impl Drop for Marked {
    fn drop(&mut self) {
        RUNNING.set(self.0);
    }
}

/// Whether a call, `ready()` or `close()` of `shared` polled now would wait
/// on the loop the running handler holds (Block 41, D6): under `Task`, the
/// handler holds a worker its client's driver may need, so any such wait
/// fails at once.
pub(super) fn would_deadlock(shared: &Shared) -> bool {
    RUNNING.get() == std::ptr::from_ref(shared) as usize
}

/// Error 16, for a wait [`would_deadlock`].
pub(super) fn deadlock_error() -> crate::CallError {
    crate::CallError::new(
        crate::codes::error::WOULD_DEADLOCK,
        "would deadlock: called from an event handler of this client",
    )
}

/// A handler with the number of the outbox stream it took over.
struct Slot<H: ?Sized> {
    stream: u64,
    /// `None` while one of its calls runs.
    handler: Option<Box<H>>,
}

/// The handlers of a client and of its subscriptions, and its consumer.
#[derive(Default)]
pub(super) struct Handlers {
    delivery: EventDelivery,
    client: Option<Slot<dyn ClientHandler>>,
    subscriptions: HashMap<SubscriptionId, Slot<dyn SubscriptionHandler>>,
    /// The consumer runs, from the first handler set.
    started: bool,
    waker: Option<Waker>,
}

impl std::fmt::Debug for Handlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handlers")
            .field("client", &self.client.is_some())
            .field("subscriptions", &self.subscriptions.len())
            .finish_non_exhaustive()
    }
}

impl Handlers {
    pub(super) fn new(delivery: EventDelivery) -> Self {
        Self {
            delivery,
            ..Self::default()
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// A handler the client no longer calls, dropped once no lock is held.
#[allow(dead_code)] // Held only to be dropped.
enum Dropped {
    Client(Box<dyn ClientHandler>),
    Subscription(Box<dyn SubscriptionHandler>),
}

/// Sets the client's handler, which takes over its stream (D1, D3).
pub(super) fn set_client_handler(shared: &Arc<Shared>, handler: Box<dyn ClientHandler>) {
    let mut core = lock(&shared.core);
    let stream = lock(&shared.outboxes).client.open_stream();
    shared.handle(&mut core, Input::StreamOpened(None));
    let old = install(shared, |handlers| {
        handlers
            .client
            .replace(Slot {
                stream,
                handler: Some(handler),
            })
            .and_then(|slot| slot.handler)
            .map(Dropped::Client)
    });
    drop(core);
    drop(old);
}

/// Sets the handler of the subscription `of`, which takes over its stream;
/// none for a subscription no longer in the registry.
pub(super) fn set_subscription_handler(
    shared: &Arc<Shared>,
    of: SubscriptionId,
    handler: Box<dyn SubscriptionHandler>,
) {
    let mut core = lock(&shared.core);
    if core.machine.subscription_state(of).is_none() {
        return;
    }
    let stream = lock(&shared.outboxes)
        .subscriptions
        .entry(of)
        .or_default()
        .open_stream();
    shared.handle(&mut core, Input::StreamOpened(Some(of)));
    let old = install(shared, |handlers| {
        handlers
            .subscriptions
            .insert(
                of,
                Slot {
                    stream,
                    handler: Some(handler),
                },
            )
            .and_then(|slot| slot.handler)
            .map(Dropped::Subscription)
    });
    drop(core);
    drop(old);
}

/// Installs a handler with `set`, starting the consumer at the first one.
fn install(
    shared: &Arc<Shared>,
    set: impl FnOnce(&mut Handlers) -> Option<Dropped>,
) -> Option<Dropped> {
    let mut handlers = lock(&shared.handlers);
    let old = set(&mut handlers);
    handlers.wake();
    if !handlers.started {
        handlers.started = true;
        match handlers.delivery {
            EventDelivery::Task => {
                shared.runtime.spawn(consume(Arc::clone(shared)));
            }
        }
    }
    old
}

/// An event handed to its handler, which runs with no lock held.
enum Handed {
    Client {
        id: EventId,
        event: ClientEvent,
        handler: Box<dyn ClientHandler>,
        stream: u64,
    },
    Subscription {
        of: SubscriptionId,
        id: EventId,
        event: SubscriptionEvent,
        handler: Box<dyn SubscriptionHandler>,
        stream: u64,
    },
}

/// The consumer of a client's handlers (D2: `Task`).
async fn consume(shared: Arc<Shared>) {
    while let Some(handed) = std::future::poll_fn(|cx| next(&shared, cx)).await {
        deliver(&shared, handed);
        // A task's turn: the driver and the other tasks run between events.
        tokio::task::yield_now().await;
    }
    // Dropped after close, once their events are delivered (D5).
    let handlers = std::mem::take(&mut *lock(&shared.handlers));
    drop(handlers);
}

/// The next event for a handler, the earliest emitted; `None` once the
/// client is closed and no handler has an event left.
fn next(shared: &Shared, cx: &mut Context<'_>) -> Poll<Option<Handed>> {
    let mut core = lock(&shared.core);
    let mut outboxes = lock(&shared.outboxes);
    let mut handlers = lock(&shared.handlers);
    handlers.waker = Some(cx.waker().clone());
    let mut dropped = Vec::new();
    let mut earliest: Option<(EventId, Option<SubscriptionId>)> = None;
    if let Some(slot) = &handlers.client {
        match outboxes.client.head(slot.stream) {
            Head::Event(id) if slot.handler.is_some() => earliest = Some((id, None)),
            Head::Event(_) | Head::Empty => {}
            Head::Gone => dropped.extend(
                handlers
                    .client
                    .take()
                    .and_then(|slot| slot.handler)
                    .map(Dropped::Client),
            ),
        }
    }
    let ids: Vec<SubscriptionId> = handlers.subscriptions.keys().copied().collect();
    for of in ids {
        let slot = &handlers.subscriptions[&of];
        let head = outboxes
            .subscriptions
            .get(&of)
            .map_or(Head::Gone, |outbox| outbox.head(slot.stream));
        match head {
            Head::Event(id) if slot.handler.is_some() => {
                if earliest.is_none_or(|(first, _)| id < first) {
                    earliest = Some((id, Some(of)));
                }
            }
            Head::Event(_) | Head::Empty => {}
            Head::Gone => dropped.extend(
                handlers
                    .subscriptions
                    .remove(&of)
                    .and_then(|slot| slot.handler)
                    .map(Dropped::Subscription),
            ),
        }
    }
    let handed = earliest.and_then(|(_, of)| match of {
        None => {
            let slot = handlers.client.as_mut()?;
            let Poll::Ready(Some((id, event))) = outboxes.client.poll_take(slot.stream, cx) else {
                return None;
            };
            Some(Handed::Client {
                id,
                event,
                handler: slot.handler.take()?,
                stream: slot.stream,
            })
        }
        Some(of) => {
            let slot = handlers.subscriptions.get_mut(&of)?;
            let outbox = outboxes.subscriptions.get_mut(&of)?;
            let Poll::Ready(Some((id, event))) = outbox.poll_take(slot.stream, cx) else {
                return None;
            };
            Some(Handed::Subscription {
                of,
                id,
                event,
                handler: slot.handler.take()?,
                stream: slot.stream,
            })
        }
    });
    let idle = handlers.client.is_none() && handlers.subscriptions.is_empty();
    if handed.is_none() {
        // Each outbox a handler waits on wakes the consumer.
        if let Some(slot) = &handlers.client {
            outboxes.client.watch(slot.stream, cx);
        }
        for (of, slot) in &handlers.subscriptions {
            if let Some(outbox) = outboxes.subscriptions.get_mut(of) {
                outbox.watch(slot.stream, cx);
            }
        }
    }
    drop(handlers);
    drop(outboxes);
    let id = match &handed {
        Some(Handed::Client { id, .. } | Handed::Subscription { id, .. }) => Some(*id),
        None => None,
    };
    if let Some(id) = id {
        shared.handle(&mut core, Input::EventHanded(id));
    }
    drop(core);
    drop(dropped);
    match handed {
        Some(handed) => Poll::Ready(Some(handed)),
        None if idle && shared.closed.load(std::sync::atomic::Ordering::Acquire) => {
            Poll::Ready(None)
        }
        None => Poll::Pending,
    }
}

/// Calls the handler with no lock held, then reports its outcome (D3, D4).
fn deliver(shared: &Shared, handed: Handed) {
    let (id, outcome) = match handed {
        Handed::Client {
            id,
            event,
            mut handler,
            stream,
        } => {
            let outcome = {
                let _marked = Marked::new(shared);
                run(|| handler.on_event(event))
            };
            let mut handlers = lock(&shared.handlers);
            let old = match handlers.client.as_mut() {
                Some(slot) if slot.stream == stream && slot.handler.is_none() => {
                    slot.handler = Some(handler);
                    None
                }
                _ => Some(Dropped::Client(handler)),
            };
            drop(handlers);
            drop(old);
            (id, outcome)
        }
        Handed::Subscription {
            of,
            id,
            event,
            mut handler,
            stream,
        } => {
            let outcome = {
                let _marked = Marked::new(shared);
                run(|| handler.on_event(event))
            };
            let mut handlers = lock(&shared.handlers);
            let old = match handlers.subscriptions.get_mut(&of) {
                Some(slot) if slot.stream == stream && slot.handler.is_none() => {
                    slot.handler = Some(handler);
                    None
                }
                _ => Some(Dropped::Subscription(handler)),
            };
            drop(handlers);
            drop(old);
            (id, outcome)
        }
    };
    let input = match outcome {
        Ok(()) => Input::EventTaken(id),
        Err(panic) => {
            let message = panic_message(panic.as_ref());
            tracing::warn!(message = %message, "an event handler panicked");
            Input::HandlerPanicked { event: id, message }
        }
    };
    let mut core = lock(&shared.core);
    shared.handle(&mut core, input);
    drop(core);
    shared.notify_actions();
}

/// Runs a handler call, catching its panic (D4). On a multi-thread runtime
/// the worker hands its other tasks over first (`block_in_place`), so that
/// a handler that blocks holds no task woken before it behind it (D6: the
/// driver among them); on a current-thread runtime nothing else runs until
/// it returns.
fn run(call: impl FnOnce()) -> std::thread::Result<()> {
    let call = AssertUnwindSafe(call);
    let multi_thread = tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    });
    if multi_thread {
        tokio::task::block_in_place(|| std::panic::catch_unwind(call))
    } else {
        std::panic::catch_unwind(call)
    }
}

/// The message of a panic's payload, as `std` prints it.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "Box<dyn Any>".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::Client;

    const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

    // D2: the earliest event goes first, whichever handler it is for.
    #[test]
    fn handlers_get_events_in_the_order_they_were_emitted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            let of_client = Arc::clone(&seen);
            client.set_handler(move |event: ClientEvent| {
                of_client.lock().unwrap().push(event.name().to_owned());
            });
            let of_subscription = Arc::clone(&seen);
            let subscription = client
                .subscription_builder("news")
                .handler(move |event: SubscriptionEvent| {
                    of_subscription
                        .lock()
                        .unwrap()
                        .push(event.name().to_owned());
                })
                .build()
                .unwrap();
            // Both queued before the consumer, on this thread, runs.
            subscription.subscribe();
            client.connect();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let seen = seen.lock().unwrap();
        assert_eq!(seen[..2], ["subscribing", "connecting"], "{seen:?}");
    }

    /// Sets `dropped` when dropped.
    struct Flagged(Arc<AtomicBool>);

    impl Drop for Flagged {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl ClientHandler for Flagged {}

    async fn dropped_within_a_second(dropped: &AtomicBool) -> bool {
        for _ in 0..100 {
            if dropped.load(Ordering::Acquire) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    // D3, D5: a handler a stream took over is dropped, as are the handlers
    // once the client is closed.
    #[tokio::test]
    async fn handlers_are_dropped_when_taken_over_and_after_close() {
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        let taken_over = Arc::new(AtomicBool::new(false));
        client.set_handler(Flagged(Arc::clone(&taken_over)));
        client.connect();
        let _events = client.events();
        assert!(dropped_within_a_second(&taken_over).await, "kept");

        let closed = Arc::new(AtomicBool::new(false));
        client.set_handler(Flagged(Arc::clone(&closed)));
        client.close().await;
        assert!(dropped_within_a_second(&closed).await, "kept after close");
    }
}
