//! Runs the Gherkin scenarios in `tests/features/`.
//!
//! Only scenarios listed in [`IMPLEMENTED`] run: feature files are verbatim
//! copies of CLAUDE.md and also hold scenarios of later sub-increments.
//! Select a test layer with cucumber's tag filter, e.g.
//! `cargo test --test bdd -- --tags @A`, or scenarios by name with `--name`:
//! both narrow that list, never widen it. Scenarios with undefined steps
//! fail the run rather than being skipped.

mod steps;
mod support;

use std::collections::HashSet;
use std::process::{self, Command};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use cucumber::gherkin::{Feature, GherkinEnv, Rule, Scenario};
use cucumber::runner::ScenarioType;
use cucumber::tag::Ext as _;
use cucumber::{World, cli};

use steps::ClientWorld;
use support::centrifugo::Centrifugo;
use support::fake_server::{Announce, FakeServer};
use support::proxy::ProxyBackend;
use support::relay::CountingRelay;
use support::tls;
use support::token::HMAC_SECRET;

const FEATURES: &str = "tests/features";

/// Scenarios whose behaviour is implemented, by name.
const IMPLEMENTED: &[&str] = &[
    // Block 1, sub-increment 1a.
    "New client starts disconnected",
    "Successful connect",
    "Explicit disconnect stops reconnection",
    "Connect after explicit disconnect",
    "Connect failures not reflected in state are reported as errors",
    // Block 1, sub-increment 1b.
    "Temporary connection loss then successful reconnect",
    "Connection loss then terminal error on reconnect",
    // Block 1, sub-increment 1c.
    "Terminal condition while connected",
    // Block 2.
    "Delays never exceed the maximum",
    "Delay upper bound grows exponentially until capped",
    "Delay upper bound per attempt",
    "Delays are jittered",
    "Backoff options are configurable",
    // Block 3.
    "Application disconnect codes sent via server API",
    "Protocol-range disconnect codes",
    "Disconnect returned by the connect proxy",
    // Block 4.
    "Client answers ping with pong",
    "Client does not answer when pong is not expected",
    "Missing pings trigger reconnect",
    "Regular pings keep the connection alive",
    // Block 5, sub-increment 5b.
    "Token refreshed before expiry keeps the connection",
    // Block 5, sub-increment 5c.
    "The client speaks JSON by default",
    // Block 5, sub-increment 5d.
    "The connected event names the transport",
    // Block 6.
    "Secure websocket connection with the build's provider",
    // Block 7.
    "Connect with Protobuf",
    "Frames are binary Protobuf",
    "Ping answered in Protobuf",
    "Every message of a multi-message Protobuf frame is handled",
    "Malformed Protobuf frame forces a reconnect",
    // Block 8.
    "Fallback to http_stream when websocket is unreachable",
    "Fallback over HTTPS uses the build's TLS provider",
    "Fallback to sse when websocket and http_stream are unreachable",
    "A transport that worked is kept on reconnect",
    "Commands work over an emulated transport",
    "A failed emulation request makes the client reconnect",
    "An HTTP stream that never answers the handshake falls back",
    // Brief C3: an opened websocket is chosen only when the server answers.
    "A websocket that never answers the handshake falls back",
    "An opened websocket without a connect reply falls back",
    "A websocket write the server never reads closes the transport",
    "A stalled websocket write does not delay disconnect",
    "Every transport unreachable",
    "A failed fallback sequence is retried after the reconnect backoff",
    "The fallback sequence is retried until a transport succeeds",
    "Protobuf with an HTTP transport is rejected",
    "Server pings over an HTTP transport are answered",
    // Block 9.
    "New subscription starts unsubscribed",
    "Subscribe while connected",
    "Subscribe before connect",
    "Publication delivered",
    "Publication delivered over each transport",
    "Publication metadata is delivered",
    "Resubscribe after reconnect",
    "Unsubscribe stops publications",
    "Duplicate subscription is refused",
    "Permanent subscribe error",
    "Unsubscribe during an in-flight subscribe over an emulated transport",
    "Subscriptions survive an explicit disconnect and connect",
    "Initial publications in the subscribe reply are delivered",
    "JSON publication bytes are exposed exactly as received",
    "A subscribe without a reply makes the client reconnect",
    "Data returned with the subscribe reply is delivered",
    "Subscription data is sent on subscribe and on resubscribe",
    "Subscription data from a callback is renewed on resubscribe",
    "Each subscription data callback receives its own channel",
    "A failing subscription data callback is retried",
    "A data callback denying access unsubscribes",
    "Subscriptions are found by channel and listed",
    "A subscription recreated after removal stays subscribed on an emulated transport",
    "A subscription resubscribed at once stays subscribed on an emulated transport",
    "Messages after a disconnect in the same frame are discarded",
    "Removing a subscription unsubscribes it first",
    "Binary publication delivered over Protobuf",
    "Without a jitter window the first reconnect follows the backoff",
    "The jitter window does not delay the first connect",
    "The jitter window does not delay a connect after an explicit disconnect",
    "Server unsubscribe codes",
    "After a resubscribe-advised unsubscribe the subscription is subscribed again",
    "Resubscribe delays are bounded",
    "Resubscribe delay upper bound grows exponentially until capped",
    "Resubscribe delays are jittered",
    "A successful subscribe resets the resubscribe backoff",
    "Resubscribe backoff options are configurable",
    "Subscription token refreshed before expiry keeps the subscription",
    "A failing first subscription token fetch is retried before subscribing",
    "Subscribe error 109 fetches a new token and resubscribes",
    "A failing subscription token callback is retried",
    "A temporary sub_refresh error is retried",
    "A sub_refresh without a reply is retried",
    "A subscription token denied before the first subscribe unsubscribes",
    "Ending a subscription during the resubscribe backoff cancels the retry",
    "Ending a subscription during the refresh retry delay cancels the refresh",
    "A refresh reply without expiry stops refreshing",
    "A permanent sub_refresh error unsubscribes",
    "An expiring fixed subscription token without a callback unsubscribes",
    "A subscribe rejected for an expired token without a callback unsubscribes",
    "A subscribe rejected for an expired token fetches a new one after the backoff",
    "An empty subscription token before the first subscribe unsubscribes",
    "An empty subscription token on refresh unsubscribes",
    "The data callback runs only after a subscription token is obtained",
    "The subscription token is fetched before the data callback runs",
    "A subscription that has a token calls the data callback without the token callback",
    "Each subscription token callback receives its own channel",
    "Subscription token denied",
    // Block 11a: stream positions and recovery.
    "A first subscribe does not recover",
    "Positioned and recoverable options are requested",
    "The subscribed event reports the granted capabilities",
    "The first subscribe reply's position is used for recovery",
    "A changed epoch on a live publication is used for recovery",
    "Recovered publications advance the stored position",
    "Missed publications are recovered",
    "Publications between unsubscribe and subscribe are recovered",
    "Publications made while explicitly disconnected are recovered",
    "The since option recovers on the first subscribe",
    "Recovery fails when history is gone",
    "A failed recovery's reply replaces the stored position",
    // Block 11b: getState.
    "getState provides the position of the initial subscribe",
    "getState reloads state when recovery fails",
    "The since option takes precedence over getState on the first subscribe",
    "A failing getState is retried before subscribing",
    // Block 11c: state invalidation. "Connection state invalidation clears
    // the token and invalidates every position" is with Block 12c, which
    // brings its server-side positions (owner decision).
    "State invalidation fetches a new subscription token",
    "Connection state invalidation drops a fixed connection token",
    "State invalidation drops a fixed subscription token",
    // Block 12a: server-side subscriptions granted at connect.
    "Publications of a server-side subscription are delivered",
    "Server-side subscriptions survive a reconnect",
    "Client-side and server-side subscriptions to one channel stay separate",
    "Server-side subscriptions granted at connect over an emulated transport",
    "Initial publications of a server-side subscription granted at connect are delivered",
    "A server-side subscription missing after reconnect is unsubscribed",
    // Block 12b: subscriptions the server adds or removes later.
    "A subscription added by the server after connect is tracked",
    "A server-side subscription removed by the server is unsubscribed",
    "Server-side subscribed events carry the server's data",
    // Block 12c: positions and recovery, and Block 11's 3014 scenario.
    "Missed publications of a server-side subscription are recovered",
    "Server-side subscriptions survive an explicit disconnect and connect",
    "A subscription added by a push is recovered after reconnect",
    "A server-side subscription missing after reconnect is forgotten",
    "A pushed server-side subscription's position is used for recovery",
    "Recovered server-side publications advance the stored position",
    "A server-side unsubscribe carries its code and reason and forgets the subscription",
    "Connection state invalidation clears the token and invalidates every position",
    // Review item C4.
    "Close code 1009 stops the client",
    // Review item C2.
    "An emulated transport the server answered is kept",
    // Block 15, sub-increment 15a.
    "Publications not taken before disconnect are recovered on the next connect",
    "Server-side publications not taken before disconnect are recovered on the next connect",
    "Publications not taken before unsubscribe are withdrawn",
    "A second events() call takes over the subscription's stream",
    "A second events() call takes over the client's stream",
    "A cancelled read loses no publication",
    "Publications of a subscription without a stream do not hold the connection",
    // Block 15, sub-increment 15b.
    "The server closing the connection is handled only after the publications before it are taken",
    "A state change not taken holds the messages of the next connection",
    "A stream that is not read stops the client reading after 256 items",
    "The read-ahead also stops at 4 MiB",
    "An item larger than the read-ahead budget is still delivered",
    "A read-ahead limit of 0 is rejected",
    "Pings are answered while a stream is not read",
    "A full read-ahead does not make the client report missing pings",
    "A subscribe reply waiting behind a publication not taken does not time out",
    "The connection token is refreshed while a stream is not read",
    "A slow consumer on a channel with history and recovery loses no publication",
    // Block 15, sub-increment 15c.
    "A client stream not read keeps every state change but drops errors past 256 untaken events",
    "A subscription stream not read drops errors past 256 untaken events",
    // Block 13, sub-increment 13a: calls and RPC.
    "RPC returns the proxy's result",
    "Temporary RPC error is reported as such",
    "Concurrent calls get their own replies",
    "A call without a reply times out",
    "A call made before connect waits for the connection",
    "A call made while connecting waits for the connection",
    "A queued call fails after the timeout when no connection comes",
    "A call that timed out while queued is never sent",
    "A call on a client never connected fails after the timeout",
    "A terminal connect failure fails a queued call at once",
    "Disconnect fails a queued call at once",
    "Disconnect fails a call already sent at once",
    "A call whose emulation request fails fails at once",
    "Losing the transport fails calls already sent at once",
];

/// Server configuration of @A scenarios.
pub(crate) const SERVER_A: &[(&str, &str)] = &[
    (
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    ),
    ("CENTRIFUGO_CLIENT_TOKEN_HMAC_SECRET_KEY", HMAC_SECRET),
    // An expired, unrefreshed connection is closed within a scenario
    // (default 25 s).
    ("CENTRIFUGO_CLIENT_EXPIRED_CLOSE_DELAY", "1s"),
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
    // Anonymous clients subscribe to channels without namespace, whose
    // publications carry offsets (both options needed; verified, Block 9).
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_ANONYMOUS",
        "true",
    ),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "10"),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s"),
    // Clients publish to, and read the history and presence of, channels
    // without namespace (Block 13b; option names from Centrifugo v6.10.1's
    // configuration defaults).
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_PRESENCE", "true"),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_HISTORY_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_HISTORY_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PRESENCE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PRESENCE_FOR_ANONYMOUS",
        "true",
    ),
    // Channels of the `private` namespace need a subscription token
    // (Block 10); an expired, unrefreshed one is ended after 1 s past the
    // server's periodic check (default 25 s). Those of the `rec` namespace
    // keep a history and are recoverable (Block 11).
    (
        "CENTRIFUGO_CHANNEL_NAMESPACES",
        r#"[{"name":"private"},{"name":"rec","history_size":10,"history_ttl":"300s","force_recovery":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true}]"#,
    ),
    ("CENTRIFUGO_CLIENT_EXPIRED_SUB_CLOSE_DELAY", "1s"),
];

/// Server configuration added for @B scenarios, besides the proxy's.
pub(crate) const SERVER_B: &[(&str, &str)] = &[
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
    // A history for channels without namespace, so that server-side
    // subscriptions granted recoverable can recover (Block 12c). It gives
    // publications offsets; client-side subscriptions stay unrecoverable.
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "10"),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s"),
    // Channels of the `rec` namespace keep a history and are recoverable
    // (Block 11); the subscribe proxy sees them too.
    (
        "CENTRIFUGO_CHANNEL_NAMESPACES",
        r#"[{"name":"rec","history_size":10,"history_ttl":"300s","force_recovery":true,"subscribe_proxy_enabled":true}]"#,
    ),
];

/// A scenario still running after this long is reported as hung and fails
/// the run: none takes a minute, and a hang would otherwise last until the
/// CI job's timeout without naming the scenario (concurrent output is
/// printed in order, once scenarios finish).
const SCENARIO_LIMIT: Duration = Duration::from_secs(180);

/// Scenarios running now, with when each started, for the watchdog.
static RUNNING: Mutex<Vec<(String, Instant)>> = Mutex::new(Vec::new());

/// Marks the runner process started by [`trust_test_ca`].
const TEST_CA_MARKER: &str = "FERRIFUGE_TEST_CA";

/// Stack of the thread running the scenarios: Windows gives the main
/// thread 1 MiB, too little for the runner's futures in a debug build,
/// where Linux gives 8 MiB.
const RUNNER_STACK: usize = 8 << 20;

/// Scenarios run alone: each spends seconds of CPU on a 32 MiB frame in a
/// debug build, enough to delay the timers of scenarios running alongside
/// (seen: a missed ping deadline in "Regular pings keep the connection
/// alive").
const SERIAL: &[&str] = &[
    "A websocket write the server never reads closes the transport",
    "A stalled websocket write does not delay disconnect",
];

fn main() {
    // `cargo test <filter>` passes a libtest filter to every test target,
    // e.g. when cargo-bolero runs one fuzz target; cucumber takes options
    // only, so a leading positional argument means another target's tests.
    if env::args().nth(1).is_some_and(|arg| !arg.starts_with('-')) {
        println!("bdd skipped: a test filter is for libtest targets");
        return;
    }
    trust_test_ca();
    assert_implemented_scenarios_exist();
    start_watchdog();
    let runner = thread::Builder::new()
        .name("scenarios".to_owned())
        .stack_size(RUNNER_STACK)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(run_scenarios());
        })
        .expect("start the scenario runner");
    // The runner exits the process when done; it returns only by panicking.
    if runner.join().is_err() {
        process::exit(101);
    }
}

async fn run_scenarios() {
    // cucumber's `--tags` and `--name` replace the filter given to
    // `filter_run` instead of narrowing it, so they are taken out and
    // applied in it.
    let mut cli = cli::Opts::<_, _, _, cli::Empty>::parsed();
    let name_filter = cli.re_filter.take();
    let tags_filter = cli.tags_filter.take();
    let selected = move |feature: &Feature, rule: Option<&Rule>, scenario: &Scenario| {
        name_filter
            .as_ref()
            .is_none_or(|name| name.is_match(&scenario.name))
            && tags_filter.as_ref().is_none_or(|tags| {
                tags.eval(
                    feature
                        .tags
                        .iter()
                        .chain(rule.iter().flat_map(|rule| &rule.tags))
                        .chain(&scenario.tags),
                )
            })
    };
    ClientWorld::cucumber()
        .with_cli(cli)
        .which_scenario(|_, _, scenario| {
            if SERIAL.contains(&scenario.name.as_str()) {
                ScenarioType::Serial
            } else {
                ScenarioType::Concurrent
            }
        })
        .before(|_, _, scenario, world| {
            Box::pin(async move {
                running().push((scenario_key(scenario), Instant::now()));
                if scenario.tags.iter().any(|tag| tag == "A") {
                    world.server = Some(Centrifugo::start(SERVER_A).await);
                } else if scenario.tags.iter().any(|tag| tag == "B") {
                    let proxy = ProxyBackend::start().await;
                    let env = proxy.centrifugo_env();
                    let env: Vec<_> = env
                        .iter()
                        .map(|(name, value)| (*name, value.as_str()))
                        .chain(SERVER_B.iter().copied())
                        .collect();
                    world.server = Some(Centrifugo::start(&env).await);
                    world.proxy = Some(proxy);
                }
                if scenario.tags.iter().any(|tag| tag == "D") {
                    world.fake = Some(FakeServer::start(Announce::default()).await);
                }
                let target = match (&world.server, &world.fake) {
                    (Some(server), _) => Some(server.address().to_owned()),
                    (None, Some(fake)) => Some(fake.address()),
                    (None, None) => None,
                };
                if let Some(target) = target {
                    world.relay = Some(CountingRelay::start(target).await);
                }
            })
        })
        .after(|_, _, scenario, _, world| {
            Box::pin(async move {
                // Removed here, awaited, rather than by the world's drop.
                if let Some(server) = world.and_then(|world| world.server.take()) {
                    server.stop().await;
                }
                let key = scenario_key(scenario);
                let mut running = running();
                if let Some(index) = running.iter().position(|(name, _)| *name == key) {
                    running.remove(index);
                }
            })
        })
        .fail_on_skipped()
        .filter_run_and_exit(FEATURES, move |feature, rule, scenario| {
            // @P runs outside cucumber (Block 0, "Test layers"); a scenario
            // also tagged with a cucumber layer runs in that layer.
            let p_only = !scenario.tags.is_empty() && scenario.tags.iter().all(|tag| tag == "P");
            IMPLEMENTED.contains(&scenario.name.as_str())
                && !p_only
                && selected(feature, rule, scenario)
        })
        .await;
}

fn running() -> std::sync::MutexGuard<'static, Vec<(String, Instant)>> {
    RUNNING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Rows of an outline share its name and line: they are told apart only
/// by being removed one at a time.
fn scenario_key(scenario: &Scenario) -> String {
    format!("{} (line {})", scenario.name, scenario.position.line)
}

/// Fails the run, naming the scenarios still running, once one of them
/// runs past [`SCENARIO_LIMIT`].
fn start_watchdog() {
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(5));
            let running = running();
            if running
                .iter()
                .any(|(_, started)| started.elapsed() > SCENARIO_LIMIT)
            {
                eprintln!("Hung scenarios, running for more than {SCENARIO_LIMIT:?}:");
                for (name, started) in running.iter() {
                    eprintln!("  {name}: running for {:?}", started.elapsed());
                }
                process::exit(1);
            }
        }
    });
}

/// Makes the client trust the test CA of the encrypted scenarios (Block 6).
///
/// `rustls-native-certs` reads `SSL_CERT_FILE` when the first secure
/// connection is configured. Setting a variable in-process is `unsafe`, so
/// the runner creates the CA and runs itself again with the variable set.
fn trust_test_ca() {
    if env::var_os(TEST_CA_MARKER).is_some() {
        return;
    }
    tls::create();
    let status = Command::new(env::current_exe().expect("runner path"))
        .args(env::args_os().skip(1))
        .env("SSL_CERT_FILE", tls::ca_path())
        .env(TEST_CA_MARKER, "1")
        .status()
        .expect("run the scenarios");
    process::exit(status.code().unwrap_or(1));
}

/// A misspelt entry in [`IMPLEMENTED`] would silently run nothing.
fn assert_implemented_scenarios_exist() {
    let mut names = HashSet::new();
    for entry in fs::read_dir(FEATURES).expect("features directory") {
        let path = entry.expect("feature file").path();
        if path.extension().is_some_and(|ext| ext == "feature") {
            let feature = Feature::parse_path(&path, GherkinEnv::default())
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            names.extend(feature.scenarios.into_iter().map(|s| s.name));
        }
    }
    for name in IMPLEMENTED {
        assert!(
            names.contains(*name),
            "no scenario named {name:?} in {FEATURES}"
        );
    }
}
