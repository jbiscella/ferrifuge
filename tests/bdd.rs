//! Runs the Gherkin scenarios in `tests/features/`.
//!
//! Only scenarios listed in [`IMPLEMENTED`] run: feature files are verbatim
//! copies of CLAUDE.md and also hold scenarios of later sub-increments.
//! Select a test layer with cucumber's tag filter, e.g.
//! `cargo test --test bdd -- --tags @A`. Scenarios with undefined steps fail
//! the run rather than being skipped.

mod steps;
mod support;

use std::collections::HashSet;
use std::process::{self, Command};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use cucumber::World;
use cucumber::gherkin::{Feature, GherkinEnv, Scenario};

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
];

/// Server configuration added for @B scenarios, besides the proxy's.
pub(crate) const SERVER_B: &[(&str, &str)] = &[
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
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

#[tokio::main]
async fn main() {
    trust_test_ca();
    assert_implemented_scenarios_exist();
    start_watchdog();
    ClientWorld::cucumber()
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
        .filter_run_and_exit(FEATURES, |_, _, scenario| {
            // @P runs outside cucumber (Block 0, "Test layers"); a scenario
            // also tagged with a cucumber layer runs in that layer.
            let p_only = !scenario.tags.is_empty() && scenario.tags.iter().all(|tag| tag == "P");
            IMPLEMENTED.contains(&scenario.name.as_str()) && !p_only
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
