//! Allocations per publication, end to end (Block 14, `@P`): a client
//! connected over websocket to the fake server receives 10000 publications
//! of 1 KiB on one of its 1, 100 or 10000 subscribed channels. dhat counts
//! the allocations; the fake server runs in a child process, a copy of this
//! binary, so that only the client's are counted.
//!
//! Slow under dhat, so it runs only with `FERRIFUGE_PERF=1`, as the perf
//! CI job does: `FERRIFUGE_PERF=1 cargo test --release --test perf_alloc`.

// @P scenario: Allocations per publication do not depend on the number of subscriptions

mod support;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use std::{env, io, thread};

use ferrifuge::{Client, ClientEvent, Config, SubscriptionEvent, SubscriptionState};
use futures_util::StreamExt;
use support::fake_server::{Announce, FakeServer};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Enables the measurement.
const RUN: &str = "FERRIFUGE_PERF";
/// Makes this binary the fake server, for that many channels.
const SERVER_ROLE: &str = "FERRIFUGE_PERF_FAKE_SERVER";
const PUBLICATIONS: usize = 10_000;
const COUNTS: [usize; 3] = [1, 100, 10_000];
/// Payload of each publication, so that its JSON data is 1 KiB.
const TEXT_LEN: usize = 1024 - r#"{"t":""}"#.len();

fn main() {
    if let Ok(channels) = env::var(SERVER_ROLE) {
        serve(channels.parse().expect("a channel count"));
        return;
    }
    if env::var_os(RUN).is_none() {
        println!("perf_alloc skipped: set {RUN}=1 to measure");
        return;
    }
    let _profiler = dhat::Profiler::builder().testing().build();
    let measured = COUNTS.map(|count| (count, allocations_per_publication(count)));
    for (count, per_publication) in measured {
        println!(
            "@P report: {per_publication:.2} allocations per 1 KiB publication \
             with {count} subscribed channels"
        );
    }
    let (_, baseline) = measured[0];
    for (count, per_publication) in measured {
        assert!(
            (per_publication - baseline).abs() < 1.0,
            "{per_publication:.2} allocations per publication with {count} channels, \
             {baseline:.2} with 1"
        );
    }
}

/// The channel the publications go to: the one subscribed last.
fn target(channels: usize) -> String {
    format!("ch{}", channels - 1)
}

/// Fake server role: prints its address, then on "go" from stdin sends the
/// publications, and serves until stdin closes.
fn serve(channels: usize) {
    let (go, wait_for_go) = mpsc::channel();
    let (closed, wait_for_close) = mpsc::channel();
    thread::spawn(move || {
        let mut lines = io::stdin().lines();
        let first = lines.next().transpose().expect("stdin");
        assert_eq!(first.as_deref(), Some("go"));
        let _ = go.send(());
        lines.for_each(drop);
        let _ = closed.send(());
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let server = FakeServer::start(Announce::default()).await;
        println!("{}", server.address());
        io::stdout().flush().expect("stdout");
        tokio::task::spawn_blocking(move || wait_for_go.recv())
            .await
            .expect("the stdin thread")
            .expect("go");
        let text = "x".repeat(TEXT_LEN);
        let publication = format!(
            r#"{{"push":{{"channel":"{}","pub":{{"data":{{"t":"{text}"}}}}}}}}"#,
            target(channels)
        );
        for _ in 0..PUBLICATIONS {
            server.send(&publication);
        }
        let _ = tokio::task::spawn_blocking(move || wait_for_close.recv()).await;
    });
}

struct Server {
    child: Child,
    stdin: ChildStdin,
    address: String,
}

impl Server {
    fn start(channels: usize) -> Self {
        let mut child = Command::new(env::current_exe().expect("this binary"))
            .env(SERVER_ROLE, channels.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("start the fake server");
        let stdin = child.stdin.take().expect("piped stdin");
        let mut address = String::new();
        BufReader::new(child.stdout.take().expect("piped stdout"))
            .read_line(&mut address)
            .expect("the fake server's address");
        Self {
            child,
            stdin,
            address: address.trim().to_owned(),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn allocations_per_publication(channels: usize) -> f64 {
    let mut server = Server::start(channels);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client = Client::new(
            format!("ws://{}/connection/websocket", server.address),
            Config::default(),
        );
        let mut client_events = client.events();
        client.connect();
        while !matches!(
            client_events.next().await,
            Some(ClientEvent::Connected { .. })
        ) {}
        drop(client_events);

        let mut subscriptions = Vec::with_capacity(channels);
        for index in 0..channels {
            let subscription = client
                .new_subscription(format!("ch{index}"))
                .expect("a new channel");
            subscription.subscribe();
            subscriptions.push(subscription);
        }
        let target = subscriptions.last().expect("a subscription");
        assert_eq!(target.channel(), self::target(channels));
        let mut events = target.events();
        while subscriptions
            .iter()
            .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let before = dhat::HeapStats::get().total_blocks;
        writeln!(server.stdin, "go").expect("start the publications");
        let mut received = 0;
        while received < PUBLICATIONS {
            match events.next().await.expect("the subscription's events") {
                SubscriptionEvent::Publication(_) => received += 1,
                // Queued while subscribing, before the count started.
                SubscriptionEvent::Subscribing { .. } | SubscriptionEvent::Subscribed(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        let after = dhat::HeapStats::get().total_blocks;
        #[allow(clippy::cast_precision_loss)] // Counts far below 2^52.
        let per_publication = (after - before) as f64 / PUBLICATIONS as f64;
        per_publication
    })
}
