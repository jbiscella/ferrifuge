//! Block 49, row 28/55 (`@P`): what reached the client's socket while its
//! process was suspended is handled before a deadline that expired in the
//! meantime. The client runs on a runtime of its own, on a thread of its
//! own; holding that thread is the suspension, which nothing else shares.
//! On resume the runtime finds the socket readable and the timer expired in
//! the same turn, so the driver's `select!` sees both ready at once.

// @P scenario: What arrived during a process suspension beats a deadline that fired meanwhile

mod support;

use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config};
use futures_util::{FutureExt as _, Stream, StreamExt};
use serde_json::json;
use support::fake_server::{Announce, FakeServer, Rpc};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

/// Repetitions of each row. An unbiased `select!` starts at a random one
/// of its 13 branches, so the timer wins when the start falls after the
/// transport branch and at or before the timer's: 1 in 13 for the no-ping
/// timer, 3 in 13 for the call timer; 32 repetitions pass on main with
/// probability about 0.08 (ping row) and 2·10⁻⁴ (rpc row).
const REPETITIONS: usize = 32;
/// How far past a deadline the process stays held, once the frame was
/// sent: the frame is written, and the timer has expired.
const MARGIN: Duration = Duration::from_millis(200);
/// A bound on anything the steps wait for.
const WAIT: Duration = Duration::from_secs(10);

/// The client's own runtime, run by a thread of its own until dropped.
struct Process {
    handle: Handle,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Process {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("client-process".to_owned())
            .spawn(move || {
                runtime.block_on(async {
                    let _ = stopped.await;
                });
            })
            .expect("the client's thread");
        Self {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Suspends the process until the returned sender is used or dropped:
    /// its only thread is held, so neither its timers nor its sockets are
    /// served. Returns once the suspension has begun.
    async fn suspend(&self) -> std::sync::mpsc::Sender<()> {
        let (begun, begins) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        self.handle.spawn(async move {
            let _ = begun.send(());
            // The suspension itself, on a runtime nothing else shares,
            // until the test releases it.
            let _ = released.recv();
        });
        begins.await.expect("the suspension begins");
        release
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A connected client of `server` on `process`, and its events.
async fn connected(
    server: &FakeServer,
    process: &Process,
    config: Config,
) -> (Client, impl Stream<Item = ClientEvent> + Unpin) {
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(config)
        .runtime(process.handle.clone())
        .build()
        .expect("a valid client");
    let mut events = client.events();
    client.connect();
    let connected = tokio::time::timeout(WAIT, async {
        while let Some(event) = events.next().await {
            if matches!(event, ClientEvent::Connected { .. }) {
                return;
            }
        }
    })
    .await;
    assert!(connected.is_ok(), "not connected within {WAIT:?}");
    (client, events)
}

/// Waits until `ready` holds, within [`WAIT`].
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(WAIT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{what}: not within {WAIT:?}");
}

/// Row 1: an rpc in flight and a timeout of 200 ms; the rpc's reply arrives
/// while the process is suspended; the rpc succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_arrived_during_a_suspension_beats_its_deadline() {
    let server = FakeServer::start(Announce::default()).await;
    server.answer_rpc(Rpc::Ignore);
    let process = Process::start();
    let config = Config::default().timeout(Duration::from_millis(200));
    let (client, _events) = connected(&server, &process, config).await;
    for repetition in 0..REPETITIONS {
        let sent = server.rpc_commands().len();
        let call = client.rpc("echo", b"{}".to_vec());
        let (settled, outcome) = oneshot::channel();
        process.handle.spawn(async move {
            let _ = settled.send(call.await);
        });
        until("the rpc command sent", || {
            server.rpc_commands().len() > sent
        })
        .await;
        let command = server.rpc_commands()[sent].clone();
        let sent_at = std::time::Instant::now();
        let release = process.suspend().await;
        // While suspended: the reply reaches the socket, and the deadline
        // passes by a margin.
        server.send(&json!({"id": command["id"], "rpc": {"data": {}}}).to_string());
        tokio::time::sleep_until((sent_at + Duration::from_millis(200) + MARGIN).into()).await;
        let _ = release.send(());
        let outcome = tokio::time::timeout(WAIT, outcome)
            .await
            .expect("the rpc settles")
            .expect("its task runs");
        assert!(
            outcome.is_ok(),
            "repetition {repetition}: the deadline won over the reply read: {outcome:?}"
        );
    }
}

/// Row 2: a server ping interval of 1 s and a `maxServerPingDelay` of
/// 200 ms; the next ping arrives while the process is suspended; the client
/// stays connected and no reconnect is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ping_that_arrived_during_a_suspension_beats_the_no_ping_deadline() {
    let server = FakeServer::start(Announce {
        ping: 1,
        ..Announce::default()
    })
    .await;
    let process = Process::start();
    let config = Config::default().max_server_ping_delay(Duration::from_millis(200));
    let (client, mut events) = connected(&server, &process, config).await;
    for repetition in 0..REPETITIONS {
        // A ping read re-arms the deadline: 1.2 s from now.
        server.send("{}");
        let armed = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let release = process.suspend().await;
        // While suspended, before the deadline passes: the next ping, sent
        // on demand, reaches the socket; then the deadline passes by a
        // margin.
        server.send("{}");
        tokio::time::sleep_until((armed + Duration::from_millis(1200) + MARGIN).into()).await;
        let _ = release.send(());
        // Time for the client to act on both.
        tokio::time::sleep(MARGIN).await;
        // Resumed by now; what it did with both is in its events.
        let mut reconnected = None;
        while let Some(Some(event)) = events.next().now_or_never() {
            if !matches!(event, ClientEvent::Connected { .. }) {
                reconnected = Some(event);
            }
        }
        assert_eq!(
            (reconnected, server.connections()),
            (None, 1),
            "repetition {repetition}: the no-ping deadline won over the ping read"
        );
        assert_eq!(client.state(), ferrifuge::State::Connected);
    }
}
