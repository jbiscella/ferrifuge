//! Event handlers (Block 41b): one more consumer of the client's and its
//! subscriptions' events, beside the streams, taking over as a stream does
//! (Block 15). One consumer per client runs every handler of the client and
//! of its subscriptions, one at a time, in the order the events were
//! emitted. An event counts as taken when its handler returns (D3).

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use super::client::{Core, Outboxes, Shared, lock};
use super::outbox::{Head, Outbox};
use crate::machine::{CallId, CallWait, EventId, Input, SubscriptionId};
use crate::{
    ClientError, ClientEvent, ClientInfo, MaybeSend, Publication, ServerSubscriptionEvent,
    Subscribed, SubscriptionEvent,
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
pub trait ClientHandler: MaybeSend + 'static {
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

impl<F: FnMut(ClientEvent) + MaybeSend + 'static> ClientHandler for F {
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
pub trait SubscriptionHandler: MaybeSend + 'static {
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

impl<F: FnMut(SubscriptionEvent) + MaybeSend + 'static> SubscriptionHandler for F {
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
    /// On the client's driver, adding no task or thread per client. Gives
    /// up Block 15's "timers keep running": while a handler runs, no timer
    /// fires, nothing is read and no ping is answered.
    Inline,
    /// On a thread of the client's own, named `ferrifuge-events` and
    /// started at the first handler, for handlers that block: the runtime
    /// and the driver go on while one runs.
    Thread,
}

thread_local! {
    /// The client whose handler this thread runs, by the address of its
    /// `Shared`, 0 when none, and whether under `Thread` (Block 41, D6).
    static RUNNING: std::cell::Cell<(usize, bool)> = const { std::cell::Cell::new((0, false)) };
}

/// Marks this thread as running a handler of `shared` until dropped.
struct Marked((usize, bool));

impl Marked {
    fn new(shared: &Shared, thread: bool) -> Self {
        Self(RUNNING.replace((std::ptr::from_ref(shared) as usize, thread)))
    }
}

impl Drop for Marked {
    fn drop(&mut self) {
        RUNNING.set(self.0);
    }
}

/// What a handler of its own client waits on, for [`would_deadlock`].
#[derive(Debug, Clone, Copy)]
pub(super) enum Wait {
    /// A call not yet settled.
    Call(CallId),
    /// `ready()`: `true` while it waits for a state, `false` before its
    /// outcome is looked at, which only `Task` and `Inline` refuse.
    Ready(bool),
    /// `close()`, waiting for the driver to stop.
    Close,
}

/// Whether `wait`, of `shared` and polled now, would never end (Block 41,
/// D6). Under `Task` the handler holds a worker its client's driver may
/// need, and under `Inline` the driver itself, so any such wait fails at
/// once. Under `Thread` it fails when only a message that the running
/// handler's event holds back could end it.
pub(super) fn would_deadlock(shared: &Shared, wait: Wait) -> bool {
    let (running, thread) = RUNNING.get();
    if running != std::ptr::from_ref(shared) as usize {
        return false;
    }
    if !thread {
        return true;
    }
    match wait {
        // The driver stops whatever the handler does.
        Wait::Close | Wait::Ready(false) => false,
        Wait::Ready(true) => held_back(shared, None),
        Wait::Call(id) => held_back(shared, Some(id)),
    }
}

/// Under `Thread`: whether the event the running handler was given holds
/// the message that `call`, or `ready()` when `None`, waits for. A call is
/// watched from then on: its reply also becomes unreadable once a full
/// read-ahead stops the reading ([`check_thread_waits`]).
fn held_back(shared: &Shared, call: Option<CallId>) -> bool {
    let core = lock(&shared.core);
    let outboxes = lock(&shared.outboxes);
    let mut handlers = lock(&shared.handlers);
    if let Some(id) = call {
        handlers.watched.insert(id);
    }
    if !consumer_holds_messages(&core, &outboxes, &handlers) {
        return false;
    }
    match call.map(|id| core.machine.call_wait(id)) {
        None | Some(Some(CallWait::Message)) => true,
        Some(Some(CallWait::Reply)) => !core.machine.wants_frames(),
        Some(Some(CallWait::Other) | None) => false,
    }
}

/// Whether the messages read next wait for an event only this client's
/// handler consumer can take: the one its running handler was given, or
/// one queued for a handler, which the consumer reaches only after the
/// running one returns (Codex, #171).
fn consumer_holds_messages(core: &Core, outboxes: &Outboxes, handlers: &Handlers) -> bool {
    if handlers
        .running
        .is_some_and(|event| core.machine.holds_messages(event))
    {
        return true;
    }
    core.machine.events_holding_messages().any(|event| {
        handlers
            .client
            .as_ref()
            .is_some_and(|slot| outboxes.client.holds(slot.stream, event))
            || handlers.subscriptions.iter().any(|(of, slot)| {
                outboxes
                    .subscriptions
                    .get(of)
                    .is_some_and(|outbox| outbox.holds(slot.stream, event))
            })
    })
}

/// No handler call waits on the call `id` anymore: it settled or was
/// dropped (Codex, #171).
pub(super) fn unwatch(shared: &Shared, id: CallId) {
    lock(&shared.handlers).watched.remove(&id);
}

/// Under `Thread`, at each turn of the driver: once the consumer holds the
/// messages back, fails with error 16 the calls a handler waits on that
/// only a held message could end, waiting for the connection or a
/// subscription, or for a reply a full read-ahead keeps unread, and wakes
/// the handler's pending `ready()` waits to check again (D6; Codex, #171).
pub(super) fn check_thread_waits(shared: &Shared) {
    {
        let handlers = lock(&shared.handlers);
        if handlers.watched.is_empty() && handlers.watched_ready.is_empty() {
            return;
        }
    }
    let (stuck, ready): (Vec<CallId>, Vec<Waker>) = {
        let core = lock(&shared.core);
        let outboxes = lock(&shared.outboxes);
        let mut handlers = lock(&shared.handlers);
        handlers
            .watched
            .retain(|id| core.machine.call_wait(*id).is_some());
        if !consumer_holds_messages(&core, &outboxes, &handlers) {
            return;
        }
        let reading = core.machine.wants_frames();
        let stuck = handlers
            .watched
            .iter()
            .copied()
            .filter(|id| match core.machine.call_wait(*id) {
                Some(CallWait::Message) => true,
                Some(CallWait::Reply) => !reading,
                Some(CallWait::Other) | None => false,
            })
            .collect::<Vec<CallId>>();
        // Failed while the handler's watches are held, so a handler that
        // returns meanwhile cannot leave a call failed that it no longer
        // waits on (Codex, #174).
        for id in &stuck {
            handlers.watched.remove(id);
            shared.settle_call_now(*id, Err(deadlock_error()));
        }
        (stuck, std::mem::take(&mut handlers.watched_ready))
    };
    ready.into_iter().for_each(Waker::wake);
    for id in stuck {
        shared.apply(Input::CallAbandoned(id));
    }
}

/// Whether this thread runs a handler under `Thread`.
pub(super) fn in_thread_handler() -> bool {
    RUNNING.get().1
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
    /// The event a handler runs for.
    running: Option<EventId>,
    /// Calls a `Thread` handler waits on, for [`check_thread_waits`].
    watched: HashSet<CallId>,
    /// The wakers of the `ready()` waits pending in a `Thread` handler, for
    /// [`check_thread_waits`].
    watched_ready: Vec<Waker>,
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

/// Wakes the consumer, to see the client closed (Codex, #169).
pub(super) fn wake_consumer(shared: &Shared) {
    lock(&shared.handlers).wake();
}

/// A handler the client no longer calls, dropped once no lock is held.
#[allow(dead_code)] // Held only to be dropped.
pub(super) enum Dropped {
    Client(Box<dyn ClientHandler>),
    Subscription(Box<dyn SubscriptionHandler>),
}

/// Sets the client's handler, which takes over its stream (D1, D3); none
/// once the client is closed, when nothing would call or drop it (Codex,
/// #169).
pub(super) fn set_client_handler(shared: &Arc<Shared>, handler: Box<dyn ClientHandler>) {
    let mut core = lock(&shared.core);
    if shared.closed.load(Ordering::Acquire) {
        drop(core);
        drop(handler);
        return;
    }
    let stream = lock(&shared.outboxes).client.open_stream();
    shared.handle(&mut core, Input::StreamOpened(None));
    let old = install(shared, &mut core, |handlers| {
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

/// Sets the handler of the subscription `of`, which takes over its stream.
pub(super) fn set_subscription_handler(
    shared: &Arc<Shared>,
    of: SubscriptionId,
    handler: Box<dyn SubscriptionHandler>,
) {
    let mut core = lock(&shared.core);
    let old = install_subscription_handler(shared, &mut core, of, handler);
    drop(core);
    drop(old);
}

/// Sets the handler of the subscription `of` under the core lock, so that
/// a subscription being built gets it before another thread can subscribe
/// it (Codex, #169). Gives the handler to drop with no lock held: the one
/// replaced, or this one for a subscription no longer in the registry or a
/// closed client.
pub(super) fn install_subscription_handler(
    shared: &Arc<Shared>,
    core: &mut Core,
    of: SubscriptionId,
    handler: Box<dyn SubscriptionHandler>,
) -> Option<Dropped> {
    if shared.closed.load(Ordering::Acquire) || core.machine.subscription_state(of).is_none() {
        return Some(Dropped::Subscription(handler));
    }
    let stream = lock(&shared.outboxes)
        .subscriptions
        .entry(of)
        .or_default()
        .open_stream();
    shared.handle(core, Input::StreamOpened(Some(of)));
    install(shared, core, |handlers| {
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
    })
}

/// Installs a handler with `set`, starting the consumer at the first one.
fn install(
    shared: &Arc<Shared>,
    core: &mut Core,
    set: impl FnOnce(&mut Handlers) -> Option<Dropped>,
) -> Option<Dropped> {
    let mut handlers = lock(&shared.handlers);
    let old = set(&mut handlers);
    handlers.wake();
    let failed = start(shared, &mut handlers);
    drop(handlers);
    if let Some(error) = failed {
        thread_failed(shared, core, &error);
    }
    old
}

/// Starts the consumer unless it runs. An events thread that could not
/// start gives its error and is tried again at the next start (D2).
fn start(shared: &Arc<Shared>, handlers: &mut Handlers) -> Option<std::io::Error> {
    if handlers.started {
        return None;
    }
    handlers.started = true;
    match handlers.delivery {
        EventDelivery::Task => {
            super::platform::spawn(&shared.runtime, consume(Arc::clone(shared)));
        }
        // The driver polls `next` itself, woken by the installation.
        EventDelivery::Inline => {}
        EventDelivery::Thread => {
            let consumer = Arc::clone(shared);
            let runtime = shared.runtime.clone();
            // Detached: it ends after close, once its events are delivered.
            let spawned = spawn_thread(move || {
                let _context = runtime.enter();
                consume_on_thread(&consumer);
            });
            if let Err(error) = spawned {
                handlers.started = false;
                return Some(error);
            }
        }
    }
    None
}

/// Starts the events thread of `Thread`.
fn spawn_thread(run: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
    #[cfg(test)]
    if tests::FAIL_SPAWN.get() {
        return Err(std::io::Error::other("no thread for a test"));
    }
    std::thread::Builder::new()
        .name("ferrifuge-events".to_owned())
        .spawn(run)
        .map(drop)
}

/// At `connect()`: under `Thread`, an events thread that could not start
/// is tried again, so that the connection's events reach the handlers;
/// failing again, the connection fails as for a panic (D2, D4).
pub(super) fn start_again(shared: &Arc<Shared>, core: &mut Core) {
    let mut handlers = lock(&shared.handlers);
    let unstarted = handlers.delivery == EventDelivery::Thread
        && !handlers.started
        && (handlers.client.is_some() || !handlers.subscriptions.is_empty());
    let failed = if unstarted {
        start(shared, &mut handlers)
    } else {
        None
    };
    drop(handlers);
    if let Some(error) = failed {
        thread_failed(shared, core, &error);
    }
}

/// The events thread could not start: the client fails as for a panic,
/// with no event of its own (D2, D4).
fn thread_failed(shared: &Shared, core: &mut Core, error: &std::io::Error) {
    let message = format!("the events thread could not start: {error}");
    shared.log.span.in_scope(|| {
        tracing::warn!(message = %message, "an event handler panicked");
    });
    shared.handle(
        core,
        Input::HandlerPanicked {
            event: EventId::NONE,
            message,
        },
    );
    // Its transport close and settled calls are the driver's (Codex,
    // #171).
    shared.notify_actions();
}

/// An event handed to its handler, which runs with no lock held.
pub(super) enum Handed {
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
    let _release = Release(&shared);
    while let Some(handed) = std::future::poll_fn(|cx| next(&shared, cx)).await {
        deliver(&shared, handed);
        // A task's turn: the driver and the other tasks run between events.
        super::platform::yield_now().await;
    }
}

/// The consumer of a client's handlers on its own thread (D2: `Thread`),
/// ending after close once its events are delivered.
fn consume_on_thread(shared: &Arc<Shared>) {
    /// Wakes the thread parked for the next event.
    struct Unpark(std::thread::Thread);

    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let _release = Release(shared);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match next(shared, &mut cx) {
            Poll::Ready(Some(handed)) => deliver(shared, handed),
            Poll::Ready(None) => return,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// Drops the handlers after close, once their events are delivered (D5),
/// or when a runtime shutting down drops the consumer, which a handler
/// holding a clone of its client would otherwise keep alive (Codex, #169).
struct Release<'a>(&'a Shared);

impl Drop for Release<'_> {
    fn drop(&mut self) {
        drop_handlers(self.0);
    }
}

/// Under `Inline`, delivers the events left at close; the driver then
/// stops (D2).
pub(super) fn deliver_remaining(shared: &Shared) {
    let mut cx = Context::from_waker(Waker::noop());
    while let Poll::Ready(Some(handed)) = next(shared, &mut cx) {
        deliver(shared, handed);
    }
    drop_handlers(shared);
}

/// When the driver ends, drops the handlers no consumer will drop: under
/// `Inline`, even when a runtime shutting down drops the driver before
/// [`deliver_remaining`], and under `Thread` when the events thread never
/// started (Codex, #171).
pub(super) fn release_unconsumed(shared: &Shared) {
    let unconsumed = {
        let handlers = lock(&shared.handlers);
        handlers.delivery == EventDelivery::Inline
            || (handlers.delivery == EventDelivery::Thread && !handlers.started)
    };
    if unconsumed {
        drop_handlers(shared);
    }
}

/// Under `Thread`, a pending `ready()` of a handler is woken when the
/// consumer comes to hold messages back ([`check_thread_waits`]).
pub(super) fn watch_ready(shared: &Shared, waker: &Waker) {
    let (running, thread) = RUNNING.get();
    if !thread || running != std::ptr::from_ref(shared) as usize {
        return;
    }
    let mut handlers = lock(&shared.handlers);
    if !handlers
        .watched_ready
        .iter()
        .any(|known| known.will_wake(waker))
    {
        handlers.watched_ready.push(waker.clone());
    }
}

/// Drops the handlers, with no lock held as they go.
fn drop_handlers(shared: &Shared) {
    let handlers = std::mem::take(&mut *lock(&shared.handlers));
    drop(handlers);
}

/// The next event for a handler, the earliest emitted; `None` once the
/// client is closed and no handler has an event left.
pub(super) fn next(shared: &Shared, cx: &mut Context<'_>) -> Poll<Option<Handed>> {
    let mut core = lock(&shared.core);
    let mut outboxes = lock(&shared.outboxes);
    let mut handlers = lock(&shared.handlers);
    handlers.waker = Some(cx.waker().clone());
    let mut dropped = Vec::new();
    let earliest = earliest(&mut handlers, &mut outboxes, &mut dropped);
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
            let polled = outbox.poll_take(slot.stream, cx);
            remove_finished(&mut outboxes, of);
            let Poll::Ready(Some((id, event))) = polled else {
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
    let id = match &handed {
        Some(Handed::Client { id, .. } | Handed::Subscription { id, .. }) => Some(*id),
        None => None,
    };
    if id.is_some() {
        handlers.running = id;
    }
    drop(handlers);
    drop(outboxes);
    if let Some(id) = id {
        shared.handle(&mut core, Input::EventHanded(id));
    }
    drop(core);
    drop(dropped);
    match handed {
        Some(handed) => Poll::Ready(Some(handed)),
        None if idle && shared.closed.load(Ordering::Acquire) => Poll::Ready(None),
        None => Poll::Pending,
    }
}

/// The earliest event a handler can take, and whose it is; handlers whose
/// stream is gone go to `dropped`.
fn earliest(
    handlers: &mut Handlers,
    outboxes: &mut Outboxes,
    dropped: &mut Vec<Dropped>,
) -> Option<(EventId, Option<SubscriptionId>)> {
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
        if head == Head::Gone {
            remove_finished(outboxes, of);
        }
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
    earliest
}

/// Removes the outbox of a removed subscription once its last event is
/// taken, as its stream does (Codex, #169).
fn remove_finished(outboxes: &mut Outboxes, of: SubscriptionId) {
    if outboxes
        .subscriptions
        .get(&of)
        .is_some_and(Outbox::is_finished)
    {
        outboxes.subscriptions.remove(&of);
    }
}

/// Calls the handler with no lock held, then reports its outcome (D3, D4).
pub(super) fn deliver(shared: &Shared, handed: Handed) {
    let thread = lock(&shared.handlers).delivery == EventDelivery::Thread;
    let (id, outcome) = match handed {
        Handed::Client {
            id,
            event,
            mut handler,
            stream,
        } => {
            let outcome = {
                let _marked = Marked::new(shared, thread);
                run(|| handler.on_event(event), thread)
            };
            let mut handlers = lock(&shared.handlers);
            handlers.running = None;
            // Waits are watched only while the handler that made them runs
            // (Codex, #171).
            handlers.watched.clear();
            handlers.watched_ready.clear();
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
                let _marked = Marked::new(shared, thread);
                run(|| handler.on_event(event), thread)
            };
            let mut handlers = lock(&shared.handlers);
            handlers.running = None;
            // Waits are watched only while the handler that made them runs
            // (Codex, #171).
            handlers.watched.clear();
            handlers.watched_ready.clear();
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
            // In the client's span, as every record (Block 21).
            shared.log.span.in_scope(|| {
                tracing::warn!(message = %message, "an event handler panicked");
            });
            Input::HandlerPanicked { event: id, message }
        }
    };
    let mut core = lock(&shared.core);
    shared.handle(&mut core, input);
    drop(core);
    shared.notify_actions();
}

/// Runs a handler call, catching its panic (D4). On a worker of a
/// multi-thread runtime the worker hands its other tasks over first
/// (`block_in_place`), so that a handler that blocks holds no task woken
/// before it behind it (D6: the driver among them); on a current-thread
/// runtime nothing else runs until it returns. The events thread of
/// `Thread` is no worker.
fn run(call: impl FnOnce(), thread: bool) -> std::thread::Result<()> {
    let call = AssertUnwindSafe(call);
    if thread {
        std::panic::catch_unwind(call)
    } else {
        super::platform::hand_over_worker(|| std::panic::catch_unwind(call))
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
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::Client;

    const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

    thread_local! {
        /// Makes this thread's next events-thread starts fail.
        pub(super) static FAIL_SPAWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

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

    /// Sets its flag when dropped, as [`Flagged`].
    struct FlaggedSubscription(Arc<AtomicBool>);

    impl Drop for FlaggedSubscription {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl SubscriptionHandler for FlaggedSubscription {}

    /// Whether `flag` is set within a second.
    async fn set_within_a_second(flag: &AtomicBool) -> bool {
        for _ in 0..100 {
            if flag.load(Ordering::Acquire) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    // Codex, #169: the outbox of a subscription removed with a handler goes
    // once the handler took its last event, as with a stream.
    #[test]
    fn a_removed_subscription_s_outbox_goes_after_its_handler_took_the_last_event() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let record = Arc::clone(&seen);
            let subscription = client
                .subscription_builder("news")
                .handler(move |event: SubscriptionEvent| {
                    record.lock().unwrap().push(event.name().to_owned());
                })
                .build()
                .unwrap();
            subscription.subscribe();
            client.remove_subscription(&subscription);
            tokio::time::sleep(Duration::from_millis(50)).await;

            assert_eq!(*seen.lock().unwrap(), ["subscribing", "unsubscribed"]);
            assert!(lock(&client.shared().outboxes).subscriptions.is_empty());
        });
    }

    // Codex, #169: a runtime that shuts down drops the consumer, and its
    // handlers with it.
    #[test]
    fn handlers_are_dropped_with_the_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let client = runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            client.set_handler(Flagged(Arc::clone(&dropped)));
            tokio::task::yield_now().await;
            client
        });

        drop(runtime);

        assert!(dropped.load(Ordering::Acquire));
        drop(client);
    }

    // Codex, #169: a handler set after close is dropped at once, and no
    // consumer starts for it.
    #[test]
    fn a_handler_set_after_close_is_dropped() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (of_client, of_subscription) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            let subscription = client.new_subscription("news").unwrap();
            client.close().await;

            client.set_handler(Flagged(Arc::clone(&of_client)));
            subscription.set_handler(FlaggedSubscription(Arc::clone(&of_subscription)));
            tokio::time::sleep(Duration::from_millis(20)).await;
        });

        assert!(of_client.load(Ordering::Acquire));
        assert!(of_subscription.load(Ordering::Acquire));
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
    }

    // Codex, #169: a call first polled outside a handler fails when a
    // handler polls it later.
    #[test]
    fn a_call_polled_again_in_a_handler_would_deadlock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = Arc::new(Mutex::new(None));
        runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            let mut call = Box::pin(client.rpc("echo", b"{}".to_vec()));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(call.as_mut().poll(&mut cx).is_pending());
            let call = Mutex::new(Some(call));
            let record = Arc::clone(&outcome);
            client.set_handler(move |_: ClientEvent| {
                if let Some(mut call) = call.lock().unwrap().take() {
                    let mut cx = Context::from_waker(Waker::noop());
                    *record.lock().unwrap() = Some(
                        call.as_mut()
                            .poll(&mut cx)
                            .map(|result| result.map(|_| ()).map_err(|error| error.code)),
                    );
                }
            });
            client.connect();
            tokio::time::sleep(Duration::from_millis(20)).await;
        });

        assert_eq!(
            *outcome.lock().unwrap(),
            Some(Poll::Ready(Err(crate::codes::error::WOULD_DEADLOCK)))
        );
    }

    /// A `close()` future handed to a handler.
    type Closing = Pin<Box<dyn Future<Output = ()> + Send>>;

    // Codex, #169: a close() first polled outside a handler returns when a
    // handler polls it later. The runtime never runs: the test hands the
    // event to the handler itself, so the driver cannot end in between.
    #[test]
    fn a_close_polled_again_in_a_handler_returns() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        let closing: Arc<Mutex<Option<Closing>>> = Arc::default();
        let in_handler = Arc::clone(&closing);
        let returned = Arc::new(Mutex::new(None));
        let record = Arc::clone(&returned);
        client.set_handler(move |_: ClientEvent| {
            if let Some(mut close) = in_handler.lock().unwrap().take() {
                let mut cx = Context::from_waker(Waker::noop());
                *record.lock().unwrap() = Some(close.as_mut().poll(&mut cx).is_ready());
            }
        });
        client.connect();
        let mut close: Closing = Box::pin(client.close());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(close.as_mut().poll(&mut cx).is_pending());
        *closing.lock().unwrap() = Some(close);

        let Poll::Ready(Some(handed)) = next(client.shared(), &mut cx) else {
            panic!("an event for the handler");
        };
        deliver(client.shared(), handed);

        assert_eq!(*returned.lock().unwrap(), Some(true));
    }

    // Codex, #169, under Inline: a runtime that shuts down drops the
    // driver, and the handlers with it.
    #[test]
    fn inline_handlers_are_dropped_with_the_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let client = runtime.block_on(async {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(EventDelivery::Inline)
                .build()
                .unwrap();
            client.set_handler(Flagged(Arc::clone(&dropped)));
            tokio::task::yield_now().await;
            client
        });

        drop(runtime);

        assert!(dropped.load(Ordering::Acquire));
        drop(client);
    }

    // Structural acceptance (D2): under `Thread` every handler of a client
    // runs on one thread named `ferrifuge-events`.
    #[test]
    fn thread_handlers_run_on_one_thread_named_ferrifuge_events() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let threads = Arc::new(Mutex::new(Vec::new()));
        runtime.block_on(async {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(EventDelivery::Thread)
                .build()
                .unwrap();
            let of_client = Arc::clone(&threads);
            client.set_handler(move |_: ClientEvent| {
                let thread = std::thread::current();
                of_client
                    .lock()
                    .unwrap()
                    .push((thread.name().map(str::to_owned), thread.id()));
            });
            let of_subscription = Arc::clone(&threads);
            let subscription = client
                .subscription_builder("news")
                .handler(move |_: SubscriptionEvent| {
                    let thread = std::thread::current();
                    of_subscription
                        .lock()
                        .unwrap()
                        .push((thread.name().map(str::to_owned), thread.id()));
                })
                .build()
                .unwrap();
            subscription.subscribe();
            client.connect();
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.close().await;
        });

        let threads = threads.lock().unwrap();
        assert!(threads.len() >= 3, "{threads:?}");
        assert!(
            threads.iter().all(
                |(name, id)| name.as_deref() == Some("ferrifuge-events") && *id == threads[0].1
            ),
            "{threads:?}"
        );
    }

    // D2, D4: an events thread that cannot start fails the connection as a
    // panic does, and is tried again at the next connect().
    #[test]
    fn an_events_thread_that_cannot_start_fails_the_connection_and_starts_later() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        runtime.block_on(async {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(EventDelivery::Thread)
                .build()
                .unwrap();
            FAIL_SPAWN.set(true);
            let record = Arc::clone(&seen);
            client.set_handler(move |event: ClientEvent| {
                if let ClientEvent::Disconnected { code, reason } = &event {
                    record.lock().unwrap().push(format!("{code} {reason}"));
                }
            });
            client.connect();
            let failed = client.state();
            FAIL_SPAWN.set(false);
            client.connect();
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.close().await;

            assert_eq!(failed, crate::State::Disconnected);
        });

        let seen = seen.lock().unwrap();
        assert!(
            seen[0]
                .starts_with("2 exception in event handler: the events thread could not start: "),
            "{seen:?}"
        );
    }

    // Structural acceptance: the marker is restored after a handler panics.
    #[test]
    fn the_marker_is_restored_after_a_panic() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        client.set_handler(|_: ClientEvent| panic!("boom"));
        client.connect();

        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(handed)) = next(client.shared(), &mut cx) else {
            panic!("an event for the handler");
        };
        deliver(client.shared(), handed);

        assert!(!would_deadlock(client.shared(), Wait::Close));
    }

    /// Connects `client` by hand, its runtime not running: the connect
    /// reply `reply` is read.
    fn connected_by_hand(client: &Client, reply: &str) {
        let transport = opened_by_hand(client);
        replied_by_hand(client, transport, reply);
    }

    /// Opens `client`'s transport by hand: its connect command is sent.
    fn opened_by_hand(client: &Client) -> crate::machine::TransportId {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(&mut core, Input::Connect);
        let transport = core
            .actions
            .iter()
            .find_map(|action| match action {
                crate::machine::Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("an attempt");
        shared.handle(&mut core, Input::TransportOpened(transport));
        shared.handle(&mut core, Input::Flush);
        transport
    }

    /// Reads `reply` from `transport` by hand.
    fn replied_by_hand(client: &Client, transport: crate::machine::TransportId, reply: &str) {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(
            &mut core,
            Input::Frame {
                transport,
                data: reply.as_bytes().to_vec(),
            },
        );
    }

    /// A client whose handler holds an event that holds the messages after
    /// it, its runtime entered and not running.
    fn connected_with_a_held_event(runtime: &tokio::runtime::Runtime) -> Client {
        let _context = runtime.enter();
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        client.set_handler(|_: ClientEvent| {});
        client
    }

    // Codex, #171: under `Thread`, a `ready()` settled before the handler
    // awaits it gives its result, though the handler's event holds the
    // messages.
    #[test]
    fn a_settled_ready_gives_its_result_in_a_thread_handler() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = connected_with_a_held_event(&runtime);
        let _context = runtime.enter();
        let transport = opened_by_hand(&client);
        let mut ready = Box::pin(client.ready());
        replied_by_hand(&client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
        let shared = client.shared();
        let held = lock(&shared.core).machine.events_holding_messages().next();
        lock(&shared.handlers).running = held;
        let _marked = Marked::new(shared, true);

        let mut cx = Context::from_waker(Waker::noop());
        let outcome = ready.as_mut().poll(&mut cx);

        assert!(held.is_some());
        assert!(matches!(outcome, Poll::Ready(Ok(()))), "{outcome:?}");
    }

    // Codex, #171: under `Thread`, a wait that only a message held behind an
    // event queued for this consumer could end fails, though the running
    // event holds nothing.
    #[test]
    fn a_thread_wait_held_by_a_queued_handler_event_would_deadlock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = connected_with_a_held_event(&runtime);
        let _context = runtime.enter();
        connected_by_hand(&client, r#"{"id":1,"connect":{"client":"c"}}"#);
        let shared = client.shared();
        // A running event that holds nothing, as `connecting` does.
        lock(&shared.handlers).running = Some(EventId::for_tests(u64::MAX - 1));
        let _marked = Marked::new(shared, true);

        assert!(would_deadlock(shared, Wait::Ready(true)));
    }

    // Codex, #171: a call a Thread handler waited on is no longer watched
    // once it ends.
    #[test]
    fn a_call_ended_is_no_longer_watched() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        let shared = client.shared();
        let mut call = Box::pin(client.rpc("echo", b"{}".to_vec()));
        {
            let _marked = Marked::new(shared, true);
            let mut cx = Context::from_waker(Waker::noop());
            assert!(call.as_mut().poll(&mut cx).is_pending());
        }
        let watched = lock(&shared.handlers).watched.len();

        drop(call);

        assert_eq!(watched, 1);
        assert!(lock(&shared.handlers).watched.is_empty());
    }

    // Codex, #171: an events thread that cannot start wakes the driver, to
    // close the transport and settle the calls of the disconnect.
    #[test]
    fn an_events_thread_that_cannot_start_wakes_the_driver() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::builder(UNREACHABLE)
            .event_delivery(EventDelivery::Thread)
            .build()
            .unwrap();
        connected_by_hand(&client, r#"{"id":1,"connect":{"client":"c"}}"#);
        let shared = client.shared();
        shared.woken();

        FAIL_SPAWN.set(true);
        client.set_handler(|_: ClientEvent| {});
        FAIL_SPAWN.set(false);

        assert_eq!(client.state(), crate::State::Disconnected);
        assert!(shared.woken());
    }

    /// Counts its wakes.
    #[derive(Default)]
    struct Counted(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for Counted {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A client with a subscription subscribing and its transport open by
    /// hand, its runtime entered and not running.
    fn subscribing_by_hand(
        runtime: &tokio::runtime::Runtime,
    ) -> (Client, crate::Subscription, crate::machine::TransportId) {
        let client = connected_with_a_held_event(runtime);
        let _context = runtime.enter();
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        let transport = opened_by_hand(&client);
        (client, subscription, transport)
    }

    // Codex, #171: a Thread handler's pending ready() that the consumer
    // comes to hold back later is woken by the driver's check, to fail.
    #[test]
    fn a_thread_ready_held_back_later_is_woken() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (client, subscription, transport) = subscribing_by_hand(&runtime);
        let _context = runtime.enter();
        let shared = client.shared();
        let counted = Arc::new(Counted::default());
        let waker = Waker::from(Arc::clone(&counted));
        let mut cx = Context::from_waker(&waker);
        let mut ready = Box::pin(subscription.ready());
        {
            let _marked = Marked::new(shared, true);
            assert!(ready.as_mut().poll(&mut cx).is_pending());
        }

        replied_by_hand(&client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
        check_thread_waits(shared);

        assert_eq!(counted.0.load(Ordering::SeqCst), 1);
    }

    // Codex, #171: a call a Thread handler waits on, waiting for its
    // subscription, fails once the consumer comes to hold the subscribe
    // reply back.
    #[test]
    fn a_thread_call_held_back_later_fails() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (client, subscription, transport) = subscribing_by_hand(&runtime);
        let _context = runtime.enter();
        let shared = client.shared();
        let mut cx = Context::from_waker(Waker::noop());
        let mut history = Box::pin(subscription.history(crate::HistoryOptions::default()));
        {
            let _marked = Marked::new(shared, true);
            assert!(history.as_mut().poll(&mut cx).is_pending());
        }

        replied_by_hand(&client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
        check_thread_waits(shared);
        // Out of the handler: only the check can have failed it.
        let outcome = history.as_mut().poll(&mut cx);

        assert!(
            matches!(&outcome, Poll::Ready(Err(error)) if error.code == crate::codes::error::WOULD_DEADLOCK),
            "{outcome:?}"
        );
    }

    /// A history call handed between a handler and the test.
    type History =
        Pin<Box<dyn Future<Output = Result<crate::HistoryResult, crate::CallError>> + Send>>;

    // Codex, #171: a call a Thread handler polled and left pending when it
    // returned is no longer watched: nothing waits on it in the handler.
    #[test]
    fn a_call_left_pending_by_a_returned_handler_is_not_failed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::builder(UNREACHABLE)
            .event_delivery(EventDelivery::Thread)
            .build()
            .unwrap();
        let subscription = client.new_subscription("news").unwrap();
        let slot: Arc<Mutex<Option<History>>> = Arc::default();
        let in_handler = Arc::clone(&slot);
        let made = subscription.clone();
        // No events thread: the test hands the events to the handler.
        FAIL_SPAWN.set(true);
        client.set_handler(move |_: ClientEvent| {
            let mut slot = in_handler.lock().unwrap();
            if slot.is_none() {
                let mut history: History = Box::pin(made.history(crate::HistoryOptions::default()));
                let mut cx = Context::from_waker(Waker::noop());
                assert!(history.as_mut().poll(&mut cx).is_pending());
                *slot = Some(history);
            }
        });
        FAIL_SPAWN.set(false);
        subscription.subscribe();
        let transport = opened_by_hand(&client);
        let shared = client.shared();
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(handed)) = next(shared, &mut cx) else {
            panic!("an event for the handler");
        };
        deliver(shared, handed);

        replied_by_hand(&client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
        check_thread_waits(shared);
        let mut history = slot.lock().unwrap().take().expect("the handler's call");

        assert!(history.as_mut().poll(&mut cx).is_pending());
    }

    // Codex, #171: handlers whose events thread never started are dropped
    // when the client closes.
    #[test]
    fn unstarted_thread_handlers_are_dropped_at_close() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        runtime.block_on(async {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(EventDelivery::Thread)
                .build()
                .unwrap();
            FAIL_SPAWN.set(true);
            client.set_handler(Flagged(Arc::clone(&dropped)));
            FAIL_SPAWN.set(false);
            client.close().await;
            // The client still lives: only the close dropped them.
            assert!(set_within_a_second(&dropped).await);
            drop(client);
        });
    }

    // D6: under `Task` a call a handler makes fails at once, even when its
    // reply has already come (CI on #180: a fast reply made it succeed).
    #[test]
    fn a_task_handler_s_call_fails_at_once_though_answered() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _context = runtime.enter();
        let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
        let shared = client.shared();
        let transport = opened_by_hand(&client);
        replied_by_hand(&client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
        let mut call = Box::pin(client.rpc("echo", b"{}".to_vec()));
        // The reply, handed over as the driver would.
        replied_by_hand(&client, transport, r#"{"id":2,"rpc":{"data":{}}}"#);
        while let Some(action) = shared.next_action_for_tests() {
            if let crate::machine::Action::CallSettled { call, result } = action {
                shared.settle_call_now(call, result);
            }
        }

        let _marked = Marked::new(shared, false);
        let mut cx = Context::from_waker(Waker::noop());
        let outcome = call.as_mut().poll(&mut cx);

        assert!(
            matches!(&outcome, Poll::Ready(Err(error)) if error.code == crate::codes::error::WOULD_DEADLOCK),
            "{outcome:?}"
        );
    }

    // A handler only Send keeps the builders Sync (Codex, #169: semver).
    #[test]
    fn builders_stay_send_and_sync() {
        fn sync<T: Send + Sync>() {}
        sync::<crate::ClientBuilder>();
        sync::<crate::SubscriptionBuilder>();
    }

    // Codex, #169: a consumer left without a handler ends when the client
    // closes, rather than keep the client's state on a long-lived runtime.
    #[test]
    fn a_consumer_without_a_handler_ends_at_close() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let client = Client::new(UNREACHABLE, crate::Config::default()).unwrap();
            client.set_handler(|_: ClientEvent| {});
            tokio::task::yield_now().await;
            // The stream takes over: the consumer has no handler left.
            let events = client.events();
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.close().await;
            drop(events);
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        assert_eq!(runtime.metrics().num_alive_tasks(), 0);
    }

    // D3, D5: a handler a stream took over is dropped, as are the handlers
    // once the client is closed.
    #[tokio::test]
    async fn handlers_are_dropped_when_taken_over_and_after_close() {
        for delivery in [EventDelivery::Task, EventDelivery::Inline] {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(delivery)
                .build()
                .unwrap();
            let taken_over = Arc::new(AtomicBool::new(false));
            client.set_handler(Flagged(Arc::clone(&taken_over)));
            client.connect();
            let _events = client.events();
            assert!(set_within_a_second(&taken_over).await, "{delivery:?}: kept");

            let closed = Arc::new(AtomicBool::new(false));
            client.set_handler(Flagged(Arc::clone(&closed)));
            client.close().await;
            assert!(
                set_within_a_second(&closed).await,
                "{delivery:?}: kept after close"
            );
        }
    }

    // Structural acceptance: on the driver, a handler may call every
    // synchronous method of its client and of a subscription, since no
    // lock is held while it runs (D3).
    #[test]
    fn an_inline_handler_calls_every_synchronous_method() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let returned = Arc::new(AtomicBool::new(false));
        runtime.block_on(async {
            let client = Client::builder(UNREACHABLE)
                .event_delivery(EventDelivery::Inline)
                .build()
                .unwrap();
            let weak = client.downgrade();
            let done = Arc::clone(&returned);
            client.set_handler(move |event: ClientEvent| {
                if done.load(Ordering::Acquire) || !matches!(event, ClientEvent::Connecting { .. })
                {
                    return;
                }
                let client = weak.upgrade().expect("a clone lives");
                let _ = (client.log_number(), client.state(), client.is_closed());
                let _ = (
                    client.statistics(),
                    client.read_ahead(),
                    client.held_events(),
                );
                drop(client.clone().downgrade());
                client.set_token("t");
                client.set_data(b"{}".to_vec());
                let _ = client.set_emulated_headers([("a", "b")]);
                let _ = client.set_http_headers([("a", "b")]);
                client.set_proxy(crate::Proxy::none());
                drop(client.start_batching());
                let subscription = client.new_subscription("news").unwrap();
                let _ = (subscription.channel(), subscription.state());
                let _ = (subscription.held_events(), subscription.delta_statistics());
                let _ = subscription.set_tag_filter(None);
                subscription.set_data(b"{}".to_vec());
                subscription.subscribe();
                subscription.unsubscribe();
                subscription.set_handler(|_: SubscriptionEvent| {});
                drop(subscription.publications());
                drop(subscription.events());
                let _ = client.get_subscription("news");
                let _ = (client.subscriptions(), client.server_subscriptions());
                client.remove_subscription(&subscription);
                drop(client.subscription_builder("sport").build().unwrap());
                client.reconnect();
                client.disconnect();
                client.connect();
                done.store(true, Ordering::Release);
            });
            client.connect();
            assert!(set_within_a_second(&returned).await, "never returned");
            // The driver still runs.
            client.close().await;
        });
    }
}
