//! Steps of Block 41b and 41c: event handlers under `Task` and `Inline`
//! delivery.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::{
    CallError, Client, ClientEvent, EventDelivery, State, SubscriptionEvent, SubscriptionState,
    WeakClient,
};

use super::delivery::{offset, wait_until};
use super::{ClientWorld, recorded};
use crate::support::fake_server::Announce;

/// A handler call: what it was for, and when it ran.
#[derive(Debug, Clone)]
struct Call {
    what: String,
    started: Instant,
    ended: Instant,
}

/// A gate a handler blocks on until a step releases it.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.opened.wait(open).unwrap();
        }
    }

    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }
}

/// What the handlers of a Block 41 scenario record and share with the steps.
#[derive(Default)]
struct Shared {
    calls: Mutex<Vec<Call>>,
    /// What the handler running now is for.
    running: Mutex<Option<String>>,
    gate: Gate,
    /// The client, for a handler that calls it.
    client: Mutex<Option<WeakClient>>,
    /// The outcome of the handler's call, `ready()` or `close()`, and how
    /// long it took.
    outcome: Mutex<Option<(Result<(), CallError>, Duration)>>,
    panicked: Mutex<bool>,
}

#[derive(Default)]
pub(crate) struct Handlers {
    shared: Arc<Shared>,
    /// Pings sent and pongs received when the handler started.
    pings_before: Option<(usize, usize)>,
}

impl std::fmt::Debug for Handlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handlers").finish_non_exhaustive()
    }
}

fn delivery(mode: &str) -> EventDelivery {
    match mode {
        "Task" => EventDelivery::Task,
        "Inline" => EventDelivery::Inline,
        mode => panic!("{mode} delivery comes with a later sub-increment"),
    }
}

/// What a subscription event is for, as the steps name it.
fn name_of(event: &SubscriptionEvent) -> String {
    match event {
        SubscriptionEvent::Publication(publication) => {
            format!("publication {}", offset(publication))
        }
        event => event.name().to_owned(),
    }
}

/// Runs `call` as a handler call for `what`, recording it.
fn record(shared: &Shared, what: String, call: impl FnOnce()) {
    let started = Instant::now();
    *shared.running.lock().unwrap() = Some(what.clone());
    call();
    *shared.running.lock().unwrap() = None;
    shared.calls.lock().unwrap().push(Call {
        what,
        started,
        ended: Instant::now(),
    });
}

/// The world's client, built for the fake server with `mode` delivery and
/// `handler`, its stream recorded when it has none.
fn build(
    world: &mut ClientWorld,
    mode: &str,
    handler: Option<Box<dyn FnMut(ClientEvent) + Send>>,
) -> Client {
    let mut builder = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .event_delivery(delivery(mode));
    let recorded_stream = handler.is_none();
    if let Some(mut handler) = handler {
        builder = builder.handler(move |event: ClientEvent| handler(event));
    }
    let client = builder.build().expect("a valid client configuration");
    if recorded_stream {
        world.events = Some(recorded(client.events()));
    }
    *world.handlers.shared.client.lock().unwrap() = Some(client.downgrade());
    world.client = Some(client.clone());
    client
}

/// A subscription handler recording each event; `on` runs for each.
fn subscription_handler(
    shared: Arc<Shared>,
    mut on: impl FnMut(&Shared, &SubscriptionEvent) + Send + 'static,
) -> impl FnMut(SubscriptionEvent) + Send + 'static {
    move |event: SubscriptionEvent| {
        let what = name_of(&event);
        record(&shared, what, || on(&shared, &event));
    }
}

async fn connected(world: &ClientWorld) {
    let client = world.client().clone();
    wait_until("connected", || client.state() == State::Connected).await;
}

async fn subscribed(world: &ClientWorld) {
    let subscription = world.subscription().clone();
    wait_until("subscribed", || {
        subscription.state() == SubscriptionState::Subscribed
    })
    .await;
}

#[given(
    regex = r#"^a client with (\w+) delivery whose client handler takes 200 ms on "connected", with a subscription to channel "news" that has a subscription handler$"#
)]
fn a_client_whose_handler_takes_200_ms(world: &mut ClientWorld, mode: String) {
    let shared = Arc::clone(&world.handlers.shared);
    let client = build(
        world,
        &mode,
        Some(Box::new(move |event: ClientEvent| {
            record(&shared, event.name().to_owned(), || {
                if matches!(event, ClientEvent::Connected { .. }) {
                    std::thread::sleep(Duration::from_millis(200));
                }
            });
        })),
    );
    let subscription = client
        .subscription_builder("news")
        .handler(subscription_handler(
            Arc::clone(&world.handlers.shared),
            |_, _| {},
        ))
        .build()
        .expect("no subscription to the channel yet");
    world.subscription = Some(subscription);
}

#[when(
    regex = r#"^the fake server answers the connect and subscribe commands, then sends a publication at offset (\d+) to "(\w+)"$"#
)]
async fn answers_then_publishes(world: &mut ClientWorld, at: u64, channel: String) {
    subscribed(world).await;
    world.fake().publish(&channel, at);
}

#[then(
    regex = r#"^the handlers have received "subscribing", "connecting", "connected", "subscribed" and the publication at offset 1, in this order$"#
)]
async fn received_in_order(world: &mut ClientWorld) {
    let shared = Arc::clone(&world.handlers.shared);
    wait_until("five handler calls", || {
        shared.calls.lock().unwrap().len() >= 5
    })
    .await;
    let seen: Vec<String> = shared
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| call.what.clone())
        .collect();
    assert_eq!(
        seen,
        [
            "subscribing",
            "connecting",
            "connected",
            "subscribed",
            "publication 1"
        ]
    );
}

#[then("no two handler calls overlapped")]
fn no_overlap(world: &mut ClientWorld) {
    let mut calls = world.handlers.shared.calls.lock().unwrap().clone();
    calls.sort_by_key(|call| call.started);
    for pair in calls.windows(2) {
        assert!(pair[0].ended <= pair[1].started, "{pair:?}");
    }
}

/// A client with `mode` delivery subscribed to `channel` on the fake
/// server, its subscription handler running `on` for each event.
async fn subscribed_with_handler(
    world: &mut ClientWorld,
    mode: &str,
    channel: &str,
    recoverable: bool,
    on: impl FnMut(&Shared, &SubscriptionEvent) + Send + 'static,
) {
    if recoverable {
        world.fake().recover_from_history(channel);
    }
    let client = build(world, mode, None);
    client.connect();
    // The client's stream is read up to "connected", as a connected
    // client's is.
    while !matches!(world.next_event().await, ClientEvent::Connected { .. }) {}
    let subscription = client
        .subscription_builder(channel)
        .handler(subscription_handler(Arc::clone(&world.handlers.shared), on))
        .build()
        .expect("no subscription to the channel yet");
    subscription.subscribe();
    world.subscription = Some(subscription);
    subscribed(world).await;
}

#[given(
    regex = r#"^a client with (\w+) delivery, subscribed to (recoverable )?channel "(\w+)" on the fake server, whose subscription handler blocks on the publication at offset (\d+) until released$"#
)]
async fn a_handler_blocking_on(
    world: &mut ClientWorld,
    mode: String,
    recoverable: String,
    channel: String,
    at: u64,
) {
    let mut blocked = false;
    subscribed_with_handler(
        world,
        &mode,
        &channel,
        !recoverable.is_empty(),
        move |shared, event| {
            if let SubscriptionEvent::Publication(publication) = event
                && offset(publication) == at
                && !blocked
            {
                blocked = true;
                shared.gate.wait();
            }
        },
    )
    .await;
}

#[given(
    regex = r#"^a client with (\w+) delivery, subscribed to recoverable channel "(\w+)" on the fake server, whose subscription handler panics with "(\w+)" on the first publication at offset (\d+) it receives$"#
)]
async fn a_handler_panicking_on(
    world: &mut ClientWorld,
    mode: String,
    channel: String,
    message: String,
    at: u64,
) {
    subscribed_with_handler(world, &mode, &channel, true, move |shared, event| {
        if let SubscriptionEvent::Publication(publication) = event
            && offset(publication) == at
        {
            let mut panicked = shared.panicked.lock().unwrap();
            if !*panicked {
                *panicked = true;
                drop(panicked);
                std::panic::panic_any(message.clone());
            }
        }
    })
    .await;
}

#[when(regex = r#"^the fake server sends publications at offsets ([\d, and]+) to "(\w+)"$"#)]
fn sends_publications(world: &mut ClientWorld, list: String, channel: String) {
    for at in super::delivery::offsets(&list) {
        world.fake().publish(&channel, at);
    }
}

/// Waits until a handler runs for `what`.
async fn running_for(world: &ClientWorld, what: &str) {
    let shared = Arc::clone(&world.handlers.shared);
    let deadline = Instant::now() + super::EVENT_TIMEOUT;
    while shared.running.lock().unwrap().as_deref() != Some(what) {
        assert!(
            Instant::now() < deadline,
            "no handler running for {what}; calls: {:?}",
            shared.calls.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[given(
    regex = r"^the subscription handler has returned from the publication at offset (\d+) and is running for the one at offset (\d+)$"
)]
async fn returned_and_running(world: &mut ClientWorld, returned: u64, running: u64) {
    running_for(world, &format!("publication {running}")).await;
    let done = format!("publication {returned}");
    assert!(
        world
            .handlers
            .shared
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.what == done),
        "it has not returned from {done}"
    );
}

#[given(regex = r"^the subscription handler is running for the publication at offset (\d+)$")]
async fn running_for_publication(world: &mut ClientWorld, at: u64) {
    running_for(world, &format!("publication {at}")).await;
}

#[when("the subscription handler is released")]
fn released(world: &mut ClientWorld) {
    world.handlers.shared.gate.release();
}

/// The publications the subscription handler returned from, by offset.
fn publications(world: &ClientWorld) -> Vec<String> {
    world
        .handlers
        .shared
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| call.what.starts_with("publication"))
        .map(|call| call.what.clone())
        .collect()
}

#[then(
    regex = r"^when the fake server answers it with the publication at offset (\d+) recovered, the subscription handler receives it$"
)]
async fn receives_the_recovered(world: &mut ClientWorld, at: u64) {
    let shared = Arc::clone(&world.handlers.shared);
    let what = format!("publication {at}");
    wait_until(&format!("the handler returning from {what}"), || {
        shared
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.what == what)
    })
    .await;
}

#[then("the subscription handler has received each publication exactly once")]
fn each_exactly_once(world: &mut ClientWorld) {
    let mut seen = publications(world);
    let all = seen.len();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), all, "{:?}", publications(world));
}

#[then(regex = r#"^the subscription handler then receives "(\w+)"$"#)]
async fn then_receives(world: &mut ClientWorld, what: String) {
    let shared = Arc::clone(&world.handlers.shared);
    wait_until(&format!("the handler returning from {what}"), || {
        shared
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.what == what)
    })
    .await;
}

#[then("1 s later the client has opened no new connection")]
async fn no_new_connection_a_second_later(world: &mut ClientWorld) {
    let before = world.fake().connections();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(world.fake().connections(), before);
}

#[then(
    regex = r"^when connect is called and the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered, that subscribe command asked to recover from offset (\d+)$"
)]
async fn connect_recovers_from(world: &mut ClientWorld, expected: u64) {
    let subscribes = |world: &ClientWorld| {
        world
            .fake()
            .received()
            .into_iter()
            .filter(|command| command["subscribe"].is_object())
            .collect::<Vec<_>>()
    };
    let before = subscribes(world).len();
    world.client().connect();
    let fake = world.fake();
    wait_until("a new subscribe command", || {
        fake.received()
            .iter()
            .filter(|command| command["subscribe"].is_object())
            .count()
            > before
    })
    .await;
    let subscribe = subscribes(world).pop().expect("a subscribe command");
    assert_eq!(
        (
            &subscribe["subscribe"]["recover"],
            subscribe["subscribe"]["offset"].as_u64()
        ),
        (&serde_json::json!(true), Some(expected)),
        "{subscribe}"
    );
}

#[then(
    regex = r"^the subscription handler receives the publications at offsets ([\d, and]+), in order$"
)]
async fn receives_in_order(world: &mut ClientWorld, list: String) {
    let expected: Vec<String> = super::delivery::offsets(&list)
        .iter()
        .map(|at| format!("publication {at}"))
        .collect();
    let shared = Arc::clone(&world.handlers.shared);
    let tail = |calls: &[Call]| -> Vec<String> {
        let seen: Vec<String> = calls
            .iter()
            .filter(|call| call.what.starts_with("publication"))
            .map(|call| call.what.clone())
            .collect();
        seen[seen.len().saturating_sub(expected.len())..].to_vec()
    };
    wait_until("those publications", || {
        tail(&shared.calls.lock().unwrap()) == expected
    })
    .await;
}

#[then("the subscription handler receives nothing more")]
async fn receives_nothing_more(world: &mut ClientWorld) {
    let before = world.handlers.shared.calls.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        world.handlers.shared.calls.lock().unwrap().len(),
        before,
        "{:?}",
        world.handlers.shared.calls.lock().unwrap()
    );
}

#[when("the application sets a subscription handler")]
fn sets_a_subscription_handler(world: &mut ClientWorld) {
    let handler = subscription_handler(Arc::clone(&world.handlers.shared), |_, _| {});
    world.subscription().set_handler(handler);
    world.delivery.superseded = true;
}

#[then(regex = r"^the subscription handler receives the publications at offsets ([\d, and]+)$")]
async fn receives_publications(world: &mut ClientWorld, list: String) {
    receives_in_order(world, list).await;
}

#[given(
    regex = r#"^a client with (\w+) delivery on a multi-thread runtime, connected to the fake server, which pings every (\d+) s and expects pongs, subscribed to channel "(\w+)" whose subscription handler blocks for (\d+) s on each publication$"#
)]
async fn a_blocking_handler_while_pinged(
    world: &mut ClientWorld,
    mode: String,
    seconds: u32,
    channel: String,
    blocks: u64,
) {
    // The scenario runner's runtime is a multi-thread one.
    world
        .restart_fake(Announce {
            ping: seconds,
            pong: true,
            ..Announce::default()
        })
        .await;
    world.fake().ping_every(Duration::from_secs(seconds.into()));
    subscribed_with_handler(world, &mode, &channel, false, move |_, event| {
        if matches!(event, SubscriptionEvent::Publication(_)) {
            std::thread::sleep(Duration::from_secs(blocks));
        }
    })
    .await;
}

/// Pongs the fake server received: empty commands.
fn pongs(world: &ClientWorld) -> usize {
    world
        .fake()
        .received()
        .iter()
        .filter(|command| command.as_object().is_some_and(serde_json::Map::is_empty))
        .count()
}

#[given("the subscription handler is running for it")]
async fn running_for_it(world: &mut ClientWorld) {
    running_for(world, "publication 1").await;
    world.handlers.pings_before = Some((world.fake().pings(), pongs(world)));
}

#[then(regex = r"^the client has answered (every|none) of those pings$")]
fn answered_those_pings(world: &mut ClientWorld, which: String) {
    let (sent_before, answered_before) = world.handlers.pings_before.expect("a handler running");
    let sent = world.fake().pings() - sent_before;
    let answered = pongs(world) - answered_before;
    assert!(sent >= 3, "{sent} pings in the window");
    if which == "every" {
        // The last ping may still be on its way back.
        assert!(answered + 1 >= sent, "{answered} pongs to {sent} pings");
    } else {
        assert_eq!(answered, 0, "{answered} pongs to {sent} pings");
    }
}

/// Runs `wait` from the client handler at `connected`, recording its
/// outcome and how long it took.
fn a_handler_waiting_on<F>(world: &mut ClientWorld, mode: &str, wait: fn(Client) -> F)
where
    F: std::future::Future<Output = Result<(), CallError>> + 'static,
{
    let shared = Arc::clone(&world.handlers.shared);
    build(
        world,
        mode,
        Some(Box::new(move |event: ClientEvent| {
            record(&shared, client_name_of(&event), || {
                if !matches!(event, ClientEvent::Connected { .. }) {
                    return;
                }
                let client = shared
                    .client
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(WeakClient::upgrade)
                    .expect("the client");
                let started = Instant::now();
                let outcome = block_on(wait(client));
                *shared.outcome.lock().unwrap() = Some((outcome, started.elapsed()));
            });
        })),
    );
}

/// Blocks this thread on `future`, as a handler that waits on a call does.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(Unpark(std::thread::current())).into();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

/// What a client event is for, as the steps name it.
fn client_name_of(event: &ClientEvent) -> String {
    match event {
        ClientEvent::Disconnected { code, .. } => format!("disconnected {code}"),
        event => event.name().to_owned(),
    }
}

#[given(
    regex = r#"^a client with (\w+) delivery whose client handler blocks on an RPC call of the client when it receives "connected"$"#
)]
fn a_handler_blocking_on_a_call(world: &mut ClientWorld, mode: String) {
    a_handler_waiting_on(world, &mode, |client| async move {
        client.rpc("m", b"{}".to_vec()).await.map(drop)
    });
}

#[when("the fake server answers the connect command")]
async fn answers_the_connect(world: &mut ClientWorld) {
    connected(world).await;
}

/// The handler's outcome, once it has one.
async fn outcome(world: &ClientWorld) -> (Result<(), CallError>, Duration) {
    let shared = Arc::clone(&world.handlers.shared);
    wait_until("the handler's outcome", || {
        shared.outcome.lock().unwrap().is_some()
    })
    .await;
    shared.outcome.lock().unwrap().clone().expect("an outcome")
}

#[then(regex = r#"^the handler's call fails at once with a "would deadlock" error$"#)]
async fn fails_with_would_deadlock(world: &mut ClientWorld) {
    let (result, took) = outcome(world).await;
    let error = result.expect_err("the call succeeded");
    assert_eq!(
        error.code,
        ferrifuge::codes::error::WOULD_DEADLOCK,
        "{error:?}"
    );
    assert!(took < Duration::from_millis(100), "it took {took:?}");
}

#[given(
    regex = r#"^a client with (\w+) delivery connected to the fake server, whose client handler blocks on close\(\) when it receives "connected"$"#
)]
fn a_handler_closing(world: &mut ClientWorld, mode: String) {
    a_handler_waiting_on(world, &mode, |client| async move {
        client.close().await;
        Ok(())
    });
    world.client().connect();
}

#[then("the handler's close() returns within the configured timeout plus tolerance")]
async fn close_returns(world: &mut ClientWorld) {
    let (result, took) = outcome(world).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(
        took < super::test_config().timeout + Duration::from_secs(1),
        "close() took {took:?}"
    );
}

#[then(regex = r#"^the client handler then receives "disconnected" with code (\d+)$"#)]
async fn client_handler_receives_disconnected(world: &mut ClientWorld, code: u32) {
    let shared = Arc::clone(&world.handlers.shared);
    let what = format!("disconnected {code}");
    wait_until(&format!("the client handler returning from {what}"), || {
        shared
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.what == what)
    })
    .await;
}
