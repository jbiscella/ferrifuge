//! Waiting until the client is connected or a subscription subscribed
//! (Block 30): waiters registered under the core lock, settled on the
//! states the machine reports after each input, never on the events taken.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;

use super::client::{Core, Shared, client_closed, lock};
use crate::machine::SubscriptionId;
use crate::{CallError, State, SubscriptionState, codes};

type Waiter = oneshot::Sender<Result<(), CallError>>;

/// Whose readiness a waiter waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Of {
    Client,
    Subscription(SubscriptionId),
}

/// The waiting `ready()` futures. Locked after [`Shared::core`], never
/// before.
#[derive(Debug)]
pub(super) struct Waiters {
    next: u64,
    client: HashMap<u64, Waiter>,
    subscriptions: HashMap<SubscriptionId, HashMap<u64, Waiter>>,
    /// The client's state when the waiters were last settled, to see it
    /// enter `disconnected`.
    state: State,
}

impl Default for Waiters {
    fn default() -> Self {
        Self {
            next: 0,
            client: HashMap::new(),
            subscriptions: HashMap::new(),
            state: State::Disconnected,
        }
    }
}

impl Waiters {
    /// Fails every waiter with "client closed": the driver stopped (Block
    /// 28, D3, D4; Block 33, D4), the dropped senders mapping to it.
    pub(super) fn clear(&mut self) {
        self.client.clear();
        self.subscriptions.clear();
    }

    /// Fails every waiter with "client closed": the client is closing
    /// (Block 33, D3).
    pub(super) fn close(&mut self) {
        send_all(&mut self.client, &Err(client_closed()));
        for mut subscription in std::mem::take(&mut self.subscriptions).into_values() {
            send_all(&mut subscription, &Err(client_closed()));
        }
    }

    fn remove(&mut self, of: Of, id: u64) {
        match of {
            Of::Client => {
                self.client.remove(&id);
            }
            Of::Subscription(subscription) => {
                if let Some(waiters) = self.subscriptions.get_mut(&subscription) {
                    waiters.remove(&id);
                    if waiters.is_empty() {
                        self.subscriptions.remove(&subscription);
                    }
                }
            }
        }
    }

    /// Waiters left, for tests.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.client.len() + self.subscriptions.values().map(HashMap::len).sum::<usize>()
    }
}

/// Settles the waiters the states of `core` now decide (D1, D2, D4).
/// Subscription states are read only for subscriptions with a waiter.
pub(super) fn settle(core: &Core, waiters: &mut Waiters) {
    let state = core.machine.state();
    let entered_disconnected = state == State::Disconnected && waiters.state != State::Disconnected;
    waiters.state = state;
    match state {
        State::Connected => send_all(&mut waiters.client, &Ok(())),
        State::Disconnected => send_all(&mut waiters.client, &Err(disconnected())),
        State::Connecting => {}
    }
    if entered_disconnected {
        for mut subscription in std::mem::take(&mut waiters.subscriptions).into_values() {
            send_all(&mut subscription, &Err(disconnected()));
        }
        return;
    }
    waiters.subscriptions.retain(|id, subscription| {
        match core.machine.subscription_state(*id) {
            Some(SubscriptionState::Subscribed) => send_all(subscription, &Ok(())),
            Some(SubscriptionState::Subscribing) => return true,
            // Unsubscribed, or removed from the registry.
            _ => send_all(subscription, &Err(unsubscribed())),
        }
        false
    });
}

fn send_all(waiters: &mut HashMap<u64, Waiter>, result: &Result<(), CallError>) {
    for (_, waiter) in waiters.drain() {
        let _ = waiter.send(result.clone());
    }
}

fn disconnected() -> CallError {
    CallError::new(codes::error::DISCONNECTED, "disconnected")
}

fn unsubscribed() -> CallError {
    CallError::new(
        codes::error::SUBSCRIPTION_UNSUBSCRIBED,
        "subscription unsubscribed",
    )
}

/// What a `ready()` call decided when it was made.
enum Start {
    Settled(Result<(), CallError>),
    Waiting(oneshot::Receiver<Result<(), CallError>>, Registered),
}

/// A waiter of a `ready()` future still pending: dropped unsettled, it is
/// removed (D5).
struct Registered {
    shared: Arc<Shared>,
    of: Of,
    id: u64,
}

impl Drop for Registered {
    fn drop(&mut self) {
        lock(&self.shared.ready).remove(self.of, self.id);
    }
}

/// Starts waiting until `of` is ready: judged, and the waiter registered,
/// under the core lock (D1, D2). The future borrows nothing.
pub(super) fn ready(
    shared: &Arc<Shared>,
    of: Of,
) -> impl Future<Output = Result<(), CallError>> + Send + use<> {
    let start = {
        let core = lock(&shared.core);
        if shared.closed.load(Ordering::Acquire) {
            Start::Settled(Err(client_closed()))
        } else {
            let at_once = match of {
                Of::Client => match core.machine.state() {
                    State::Connected => Some(Ok(())),
                    State::Disconnected => Some(Err(disconnected())),
                    State::Connecting => None,
                },
                Of::Subscription(id) => match core.machine.subscription_state(id) {
                    Some(SubscriptionState::Subscribed) => Some(Ok(())),
                    Some(SubscriptionState::Subscribing) => None,
                    _ => Some(Err(unsubscribed())),
                },
            };
            if let Some(result) = at_once {
                Start::Settled(result)
            } else {
                let (sender, receiver) = oneshot::channel();
                let mut waiters = lock(&shared.ready);
                let id = waiters.next;
                waiters.next += 1;
                match of {
                    Of::Client => {
                        waiters.client.insert(id, sender);
                    }
                    Of::Subscription(subscription) => {
                        waiters
                            .subscriptions
                            .entry(subscription)
                            .or_default()
                            .insert(id, sender);
                    }
                }
                let registered = Registered {
                    shared: Arc::clone(shared),
                    of,
                    id,
                };
                Start::Waiting(receiver, registered)
            }
        }
    };
    let shared = Arc::clone(shared);
    async move {
        if super::handler::would_deadlock(&shared) {
            return Err(super::handler::deadlock_error());
        }
        match start {
            Start::Settled(result) => result,
            Start::Waiting(receiver, registered) => {
                let result = receiver.await;
                drop(registered);
                // The sender was dropped unsettled only when the driver
                // stopped (Block 28): the client is closed (Block 33).
                result.unwrap_or_else(|_| Err(client_closed()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use futures_util::FutureExt as _;

    use super::super::client::{Ending, lock};
    use crate::machine::{Action, Input, TransportId};
    use crate::{Client, SubscriptionState, codes};

    const UNREACHABLE: &str = "ws://127.0.0.1:1/connection/websocket";

    /// A runtime never driven: the driver task does not run, so the tests
    /// feed the machine themselves.
    fn idle_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn client_on(runtime: &tokio::runtime::Runtime) -> Client {
        Client::builder(UNREACHABLE)
            .runtime(runtime.handle().clone())
            .build()
            .unwrap()
    }

    /// Starts connecting `client`: the transport of its attempt, opened.
    fn connecting(client: &Client) -> TransportId {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(&mut core, Input::Connect);
        let transport = core
            .actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("an attempt");
        shared.handle(&mut core, Input::TransportOpened(transport));
        transport
    }

    /// Gives `client` the frame `json` on `transport`.
    fn frame(client: &Client, transport: TransportId, json: &str) {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(
            &mut core,
            Input::Frame {
                transport,
                data: json.as_bytes().to_vec(),
            },
        );
    }

    fn connected(client: &Client, transport: TransportId) {
        frame(client, transport, r#"{"id":1,"connect":{"client":"c"}}"#);
    }

    fn lost(client: &Client, transport: TransportId) {
        let shared = client.shared();
        let mut core = lock(&shared.core);
        shared.handle(
            &mut core,
            Input::TransportClosed {
                transport,
                code: None,
                reason: String::new(),
            },
        );
    }

    fn waiters(client: &Client) -> usize {
        lock(&client.shared().ready).len()
    }

    fn pending<F: Future + Unpin>(future: &mut F) -> bool {
        future.now_or_never().is_none()
    }

    fn code(result: Option<Result<(), crate::CallError>>) -> u32 {
        result.expect("settled").expect_err("an error").code
    }

    #[test]
    fn a_dropped_ready_future_leaves_no_waiter() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        connecting(&client);

        let mut ready = Box::pin(client.ready());
        assert!(pending(&mut ready));
        assert_eq!(waiters(&client), 1);
        drop(ready);

        assert_eq!(waiters(&client), 0);
    }

    #[test]
    fn several_waiting_futures_all_settle() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let transport = connecting(&client);
        let mut futures: Vec<_> = (0..3).map(|_| Box::pin(client.ready())).collect();

        connected(&client, transport);

        for future in &mut futures {
            assert_eq!(future.now_or_never(), Some(Ok(())));
        }
        assert_eq!(waiters(&client), 0);
    }

    // D4: settled under the lock when the state is seen, not when polled.
    #[test]
    fn a_waiter_released_by_connected_stays_resolved_after_a_reconnect() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let transport = connecting(&client);
        let ready = Box::pin(client.ready());
        connected(&client, transport);

        lost(&client, transport);

        assert_eq!(client.state(), crate::State::Connecting);
        assert_eq!(ready.now_or_never(), Some(Ok(())));
    }

    #[test]
    fn a_subscription_waiter_waits_across_a_reconnect_and_fails_on_removal() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let transport = connecting(&client);
        connected(&client, transport);
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        let mut ready = Box::pin(subscription.ready());

        lost(&client, transport);
        assert!(pending(&mut ready), "waits across the reconnect");
        client.remove_subscription(&subscription);

        assert_eq!(
            code(ready.now_or_never()),
            codes::error::SUBSCRIPTION_UNSUBSCRIBED
        );
    }

    #[test]
    fn after_the_client_is_dropped_subscription_ready_fails_with_code_4() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        connecting(&client);
        let mut waiting = Box::pin(subscription.ready());
        assert!(pending(&mut waiting));

        drop(client);

        // Unsubscribed with code 2 (Block 33, D4).
        assert_eq!(subscription.state(), SubscriptionState::Unsubscribed);
        assert_eq!(code(waiting.now_or_never()), codes::error::CLIENT_CLOSED);
        assert_eq!(
            code(subscription.ready().now_or_never()),
            codes::error::CLIENT_CLOSED
        );
    }

    #[test]
    fn a_runtime_shut_down_fails_the_waiters_with_code_4() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        connecting(&client);
        let mut of_client = Box::pin(client.ready());
        let mut of_subscription = Box::pin(subscription.ready());
        assert!(pending(&mut of_client) && pending(&mut of_subscription));

        drop(runtime);

        assert_eq!(code(of_client.now_or_never()), codes::error::CLIENT_CLOSED);
        assert_eq!(
            code(of_subscription.now_or_never()),
            codes::error::CLIENT_CLOSED
        );
    }

    // Block 28, D4: the machine is not fed, the waiters still end.
    #[test]
    fn the_guard_with_the_core_poisoned_fails_the_waiters_with_code_4() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        let transport = connecting(&client);
        connected(&client, transport);
        let subscription = client.new_subscription("news").unwrap();
        subscription.subscribe();
        lost(&client, transport);
        let mut of_client = Box::pin(client.ready());
        let mut of_subscription = Box::pin(subscription.ready());
        assert!(pending(&mut of_client) && pending(&mut of_subscription));
        let shared = Arc::clone(client.shared());

        let panicked = std::thread::spawn(move || {
            let _ending = Ending(Arc::clone(&shared));
            let _core = shared.core.lock().unwrap();
            panic!("a bug in the driver");
        })
        .join();

        assert!(panicked.is_err());
        assert_eq!(code(of_client.now_or_never()), codes::error::CLIENT_CLOSED);
        assert_eq!(
            code(of_subscription.now_or_never()),
            codes::error::CLIENT_CLOSED
        );
        drop(client);
        drop(runtime);
    }

    // D5: any executor, outside any runtime.
    #[test]
    fn a_ready_future_settles_on_another_executor_outside_any_runtime() {
        let runtime = idle_runtime();
        let client = client_on(&runtime);
        connecting(&client);
        let ready = client.ready();

        let waiting = std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            futures_executor::block_on(ready)
        });
        // Registered when `ready()` returned: settled whenever it is awaited.
        client.disconnect();

        let result = waiting.join().expect("the thread");
        assert_eq!(result.unwrap_err().code, codes::error::DISCONNECTED);
    }
}
