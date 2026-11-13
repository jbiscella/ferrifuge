//! Steps of Block 49b: the probes of the bug-pattern catalogue that pass
//! today, each seen failing on the mutation its row records.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportId, TransportKind};
use ferrifuge::{
    CallError, Client, ClientEvent, Config, EventDelivery, ServerSubscriptionEvent, State,
    Subscription,
};
use futures_util::FutureExt as _;
use serde_json::json;

use super::batching::{BatchRun, connected_run, run};
use super::connection::a_connected_client;
use super::delivery::wait_until;
use super::transports::{
    PING_INTERVAL, configure, connect_and_wait, pinging_server_behind_an_http_proxy,
};
use super::{ClientWorld, EVENT_TIMEOUT};
use crate::support::fake_server::Rpc;
use crate::support::relay::{CountingRelay, Mode};

// --- Command ids skip 0 when they wrap (S2) ------------------------------

#[given("a machine whose next command id is the largest value a 32-bit id can hold")]
fn a_machine_before_the_wrap(world: &mut ClientWorld) {
    let mut run = connected_run(Config::default());
    run.machine.set_last_command_id(u32::MAX - 1);
    run.mark = run.actions.len();
    world.batching.run = Some(run);
}

#[when("it sends two commands")]
fn it_sends_two_commands(world: &mut ClientWorld) {
    let run = run(world);
    run.rpc("first");
    run.rpc("second");
}

#[then("the ids are the largest value and 1")]
fn the_ids_wrap_past_0(world: &mut ClientWorld) {
    let ids: Vec<u64> = run(world)
        .frames()
        .into_iter()
        .flat_map(|(_, commands)| commands)
        .filter_map(|command| command["id"].as_u64())
        .collect();
    assert_eq!(ids, [u64::from(u32::MAX), 1]);
}

// --- Disconnect during a websocket handshake that never finishes (5, 34,
// 51, 52) ------------------------------------------------------------------

#[given("a client connecting through the relay, whose upgrade the relay holds")]
async fn a_client_whose_upgrade_is_held(world: &mut ClientWorld) {
    // Silent: the target is never dialled, and the upgrade never answered.
    let relay = CountingRelay::start_in("127.0.0.1:1".to_owned(), Mode::Silent).await;
    let endpoint = relay.websocket_url();
    world.relay = Some(relay);
    configure(world, vec![(TransportKind::WebSocket, endpoint)]);
    world.client().connect();
    let relay = world.relay.as_ref().expect("set above");
    wait_until("the relay to hold the upgrade", || relay.connections() > 0).await;
}

#[then("the client is disconnected before disconnect returns")]
fn disconnected_before_disconnect_returns(world: &mut ClientWorld) {
    // Nothing but disconnect() makes a connecting client disconnected here.
    assert_eq!(world.client().state(), State::Disconnected);
}

#[then(regex = r"^the relay's connection closes within (\d+) s$")]
async fn the_relays_connection_closes(world: &mut ClientWorld, seconds: u64) {
    let called = world.calls.settling_step_at.expect("disconnect() called");
    let relay = world.relay.as_ref().expect("a relay");
    // The relay's own record of the close, not this step's clock.
    let closed = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(closed) = relay.first_silent_closed_at() {
                return closed;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the held connection closed by the client");
    let after = closed.saturating_duration_since(called);
    assert!(
        after <= Duration::from_secs(seconds),
        "closed {after:?} after disconnect()"
    );
}

// --- A transport the client closed reports no error (14) -----------------

#[given("a machine that closed its transport on a connect timeout and is opening the next one")]
fn a_machine_opening_the_next_transport(world: &mut ClientWorld) {
    let machine = Machine::new(Config::default())
        .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
        .expect("two transports");
    let mut run = BatchRun::of(machine);
    run.handle(Input::Connect);
    let closed = run.transport.expect("the first transport opening");
    run.handle(Input::TransportOpened(closed));
    // No connect reply: the connect timeout closes it and opens the next,
    // at once within the first sequence (a timer of no delay).
    run.handle(Input::TimerFired(Timer::ConnectTimeout));
    run.advance(Duration::ZERO);
    assert!(
        run.actions.contains(&Action::CloseTransport(closed)),
        "{:?}",
        run.actions
    );
    let next = run.transport.expect("the next transport opening");
    assert_ne!(next, closed);
    run.mark = run.actions.len();
    world.probes.closed_transport = Some(closed);
    world.batching.run = Some(run);
}

#[when("the closed transport reports a read error as its close")]
fn the_closed_transport_reports_a_read_error(world: &mut ClientWorld) {
    let closed = world.probes.closed_transport.expect("a closed transport");
    run(world).handle(Input::TransportClosed {
        transport: closed,
        code: None,
        reason: "read error".to_owned(),
    });
}

// --- Dropping the last clone of a client inside its handler (1) ----------

/// What a Block 49b scenario holds.
#[derive(Default)]
pub(crate) struct Probes {
    /// The handler's clone of the client, its only one.
    held: Arc<Mutex<Option<Client>>>,
    /// The handler drops its clone on the next publication once set.
    drop_armed: Arc<AtomicBool>,
    /// When the handler dropped its clone.
    dropped_at: Arc<Mutex<Option<Instant>>>,
    /// The handler saw the client connected.
    connected: Arc<AtomicBool>,
    /// The handler's thread has returned: set by a thread-local's
    /// destructor, which runs when the thread exits.
    thread_returned: Arc<AtomicBool>,
    /// An endpoint that closes every connection at once, recording when.
    unreachable: Option<CountingRelay>,
    /// A call and a `ready()` held unspawned, and the subscription.
    call: Option<Held>,
    ready: Option<Held>,
    subscription: Option<Subscription>,
    /// The transport a machine closed (Block 49, row 14).
    closed_transport: Option<TransportId>,
}

impl std::fmt::Debug for Probes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Probes").finish_non_exhaustive()
    }
}

/// Sets its flag when dropped, at the exit of the thread holding it.
struct OnThreadExit(Arc<AtomicBool>);

impl Drop for OnThreadExit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

thread_local! {
    static THREAD_EXIT: RefCell<Option<OnThreadExit>> = const { RefCell::new(None) };
}

/// The channel the fake server subscribes the client to at connect, so
/// that the client's own handler gets publications.
const SERVER_CHANNEL: &str = "alerts";

#[given("a connected client whose event handler holds its only clone")]
async fn a_client_whose_handler_holds_it(world: &mut ClientWorld) {
    world
        .fake()
        .grant_server_subscriptions(json!({ SERVER_CHANNEL: {} }));
    let probes = &world.probes;
    let (held, armed, dropped_at, connected, returned) = (
        Arc::clone(&probes.held),
        Arc::clone(&probes.drop_armed),
        Arc::clone(&probes.dropped_at),
        Arc::clone(&probes.connected),
        Arc::clone(&probes.thread_returned),
    );
    let handler = move |event: ClientEvent| {
        THREAD_EXIT.with(|exit| {
            exit.borrow_mut()
                .get_or_insert_with(|| OnThreadExit(Arc::clone(&returned)));
        });
        match event {
            ClientEvent::Connected { .. } => connected.store(true, Ordering::SeqCst),
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(_),
                ..
            } if armed.load(Ordering::SeqCst) => {
                let clone = held.lock().unwrap().take();
                *dropped_at.lock().unwrap() = Some(Instant::now());
                drop(clone);
            }
            _ => {}
        }
    };
    let client = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .event_delivery(EventDelivery::Thread)
        .handler(handler)
        .build()
        .expect("a valid client configuration");
    client.connect();
    // The handler's clone is the only one: the world keeps none.
    *world.probes.held.lock().unwrap() = Some(client);
    let connected = Arc::clone(&world.probes.connected);
    wait_until("the client connected", || connected.load(Ordering::SeqCst)).await;
}

#[when("the handler drops that clone on the next publication")]
fn the_handler_drops_it(world: &mut ClientWorld) {
    world.probes.drop_armed.store(true, Ordering::SeqCst);
    world.fake().publish(SERVER_CHANNEL, 1);
}

#[then(regex = r"^the fake server sees the connection closed within (\d+) s$")]
async fn the_fake_server_sees_it_closed(world: &mut ClientWorld, seconds: u64) {
    let dropped_at = Arc::clone(&world.probes.dropped_at);
    wait_until("the handler to drop its clone", || {
        dropped_at.lock().unwrap().is_some()
    })
    .await;
    let fake = world.fake();
    wait_until("the connection closed", || fake.closed_connections() > 0).await;
    let dropped = world
        .probes
        .dropped_at
        .lock()
        .unwrap()
        .expect("checked above");
    // From the drop to the server seeing the end, by their own records.
    let after = fake
        .last_closed_at()
        .expect("a connection closed")
        .saturating_duration_since(dropped);
    assert!(
        after <= Duration::from_secs(seconds),
        "closed {after:?} after the drop"
    );
}

#[then("the handler's thread has returned")]
async fn the_handlers_thread_returned(world: &mut ClientWorld) {
    let returned = Arc::clone(&world.probes.thread_returned);
    wait_until("the handler's thread to return", || {
        returned.load(Ordering::SeqCst)
    })
    .await;
}

// --- A websocket answering what is not client protocol (72) --------------

/// A text frame that is not client protocol data.
const NOT_PROTOCOL: &str = "<html>not client protocol</html>";

#[given(
    "a client configured with the fake server's websocket endpoint and an unreachable http_stream endpoint"
)]
async fn a_client_with_an_unreachable_http_stream(world: &mut ClientWorld) {
    // Closes every connection at once, recording when it took each.
    let down = CountingRelay::start_in("127.0.0.1:1".to_owned(), Mode::Down).await;
    let endpoints = vec![
        (TransportKind::WebSocket, world.server_endpoint()),
        (TransportKind::HttpStream, down.http_stream_url()),
    ];
    world.probes.unreachable = Some(down);
    configure(world, endpoints);
}

#[given(
    "the fake server answering the first connection with one text frame that is not client protocol data, and no connect reply"
)]
fn the_fake_server_answers_with_non_protocol(world: &mut ClientWorld) {
    world.fake().answer_next_connect_with_frame(NOT_PROTOCOL);
}

#[then(regex = r"^the client attempts the http_stream endpoint within (\d+) s of that frame$")]
async fn attempts_http_stream_within(world: &mut ClientWorld, seconds: u64) {
    let down = world
        .probes
        .unreachable
        .as_ref()
        .expect("an unreachable endpoint");
    wait_until("an attempt on the http_stream endpoint", || {
        down.first_accepted_at().is_some()
    })
    .await;
    let attempted = down.first_accepted_at().expect("checked above");
    let sent = world
        .fake()
        .connect_frame_sent_at()
        .expect("the fake server sent its frame");
    // From the frame to the attempt, by the servers' own records.
    let after = attempted.saturating_duration_since(sent);
    assert!(
        after <= Duration::from_secs(seconds),
        "the http_stream attempt came {after:?} after the frame"
    );
}

// --- A panic in the client's driver ends the client visibly (19, 70) ----

/// A call or a `ready()` the steps hold without spawning, so that whether
/// it is settled is read at its first poll, not through a task's
/// scheduling.
type Held = Pin<Box<dyn Future<Output = Result<(), CallError>> + Send>>;

#[given("a client connected to the fake server, with a panic planted in the driver's next step")]
async fn a_client_whose_driver_will_panic(world: &mut ClientWorld) {
    // Planted by the When step together with the input that sets it off:
    // the writes of the call and the subscribe made below are inputs of
    // the driver too, and would set it off first.
    a_connected_client(world).await;
}

#[given("a call in flight and a waiting ready() on a subscription")]
async fn a_call_and_a_ready_waiting(world: &mut ClientWorld) {
    let fake = world.fake();
    fake.answer_rpc(Rpc::Ignore);
    // Held: the subscription stays subscribing.
    fake.answer_subscribes_after_release(Duration::ZERO, None);
    let subscription = world
        .client()
        .new_subscription("news")
        .expect("a new subscription");
    subscription.subscribe();
    let rpc = world.client().rpc("slow", b"{}".to_vec());
    world.probes.call = Some(Box::pin(async move { rpc.await.map(|_| ()) }));
    world.probes.ready = Some(Box::pin(subscription.ready()));
    world.probes.subscription = Some(subscription);
    let fake = world.fake();
    wait_until("the call and the subscribe sent", || {
        let received = fake.received();
        received.iter().any(|command| command["rpc"].is_object())
            && received
                .iter()
                .any(|command| command["subscribe"].is_object())
    })
    .await;
    let call = world.probes.call.as_mut().expect("set above");
    assert!(call.now_or_never().is_none(), "the call settled already");
    let ready = world.probes.ready.as_mut().expect("set above");
    assert!(ready.now_or_never().is_none(), "ready() settled already");
}

#[when("the driver takes the next input")]
fn the_driver_takes_the_next_input(world: &mut ClientWorld) {
    world.client().plant_driver_panic();
    // A server ping: an input of the driver.
    world.fake().send("{}");
}

#[then(regex = r#"^the client's state is "disconnected" with code (\d+)$"#)]
async fn disconnected_with_code(world: &mut ClientWorld, code: u32) {
    loop {
        match world.next_event().await {
            ClientEvent::Disconnected { code: got, .. } => {
                assert_eq!(got, code);
                break;
            }
            ClientEvent::Error(_) => {}
            event => panic!("expected disconnected, got {event:?}"),
        }
    }
    assert_eq!(world.client().state(), State::Disconnected);
}

#[then(regex = r"^the call fails, and ready\(\) fails, each within (\d+) s$")]
async fn the_call_and_ready_fail(world: &mut ClientWorld, seconds: u64) {
    // Settled by the step that disconnected the client: each within its
    // bound from here, which a hang does not meet.
    let bound = Duration::from_secs(seconds);
    for (what, held) in [
        ("the call", world.probes.call.take()),
        ("ready()", world.probes.ready.take()),
    ] {
        let held = held.unwrap_or_else(|| panic!("{what} held"));
        let outcome = tokio::time::timeout(bound, held)
            .await
            .unwrap_or_else(|_| panic!("{what} still pending after {bound:?}"));
        assert!(outcome.is_err(), "{what} succeeded");
    }
}

// --- A terminal disconnect push right before the end of an HTTP stream
// (18) --------------------------------------------------------------------

#[then(regex = r#"^the client goes to state "disconnected" with code (\d+) and the reason sent$"#)]
async fn disconnected_with_the_reason_sent(world: &mut ClientWorld, code: u32) {
    let (sent, reason) = world.sent_disconnect.clone().expect("a disconnect sent");
    assert_eq!(sent, code);
    loop {
        match world.next_event().await {
            ClientEvent::Disconnected {
                code: got,
                reason: got_reason,
            } => {
                assert_eq!((got, got_reason.as_str()), (code, reason.as_str()));
                return;
            }
            // The push lost with the end of the stream: a reconnect.
            event @ (ClientEvent::Connecting { .. } | ClientEvent::Connected { .. }) => {
                panic!("expected disconnected, got {event:?}");
            }
            _ => {}
        }
    }
}

// --- An emulation request that never answers (23, 83) --------------------

#[given(
    regex = r#"^a client with a timeout of (\d+) s, connected over transport "http_stream" through a proxy that never answers the next emulation request$"#
)]
async fn a_client_behind_a_proxy_that_never_answers(world: &mut ClientWorld, seconds: u64) {
    world.timeout = Some(Duration::from_secs(seconds));
    let http_proxy = pinging_server_behind_an_http_proxy(world).await;
    let url = format!("http://{}/connection/http_stream", http_proxy.address());
    configure(world, vec![(TransportKind::HttpStream, url)]);
    connect_and_wait(world).await;
    http_proxy.never_answer_next_emulation();
    world.http_proxy = Some(http_proxy);
}

#[then(regex = r#"^within (\d+) s the client emits "connecting" with code (\d+)$"#)]
async fn within_connecting_with_code(world: &mut ClientWorld, seconds: u64, code: u32) {
    match world.next_event().await {
        ClientEvent::Connecting { code: got, .. } => assert_eq!(got, code),
        event => panic!("expected connecting, got {event:?}"),
    }
    // From the request to its end, as the proxy saw them: the transport
    // ending aborts its requests.
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    wait_until("the held request abandoned", || {
        proxy.held_emulation().1.is_some()
    })
    .await;
    let (Some(held), Some(abandoned)) = proxy.held_emulation() else {
        unreachable!("checked above");
    };
    let after = abandoned.saturating_duration_since(held);
    assert!(
        after <= Duration::from_secs(seconds),
        "the request ended {after:?} after it was sent"
    );
}

#[then("it connects again and the next command is answered")]
async fn it_connects_again_and_is_answered(world: &mut ClientWorld) {
    loop {
        match world.next_event().await {
            ClientEvent::Connected { .. } => break,
            ClientEvent::Error(_) | ClientEvent::Connecting { .. } => {}
            event => panic!("expected connected, got {event:?}"),
        }
    }
    let proxy = world.http_proxy.as_ref().expect("an HTTP proxy");
    let answered = proxy.emulation_answered();
    // Its next pong, to a server pinging every few seconds.
    let next = tokio::time::timeout(PING_INTERVAL + EVENT_TIMEOUT, async {
        while proxy.emulation_answered() <= answered {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        next.is_ok(),
        "no emulation request answered after the reconnect"
    );
}
