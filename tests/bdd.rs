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
use std::{env, fs};

use cucumber::World;
use cucumber::gherkin::{Feature, GherkinEnv};

use steps::ClientWorld;
use support::centrifugo::Centrifugo;
use support::fake_server::{Announce, FakeServer};
use support::proxy::ConnectProxy;
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
];

/// Marks the runner process started by [`trust_test_ca`].
const TEST_CA_MARKER: &str = "FERRIFUGE_TEST_CA";

#[tokio::main]
async fn main() {
    trust_test_ca();
    assert_implemented_scenarios_exist();
    ClientWorld::cucumber()
        .before(|_, _, scenario, world| {
            Box::pin(async move {
                if scenario.tags.iter().any(|tag| tag == "A") {
                    world.server = Some(Centrifugo::start(SERVER_A).await);
                } else if scenario.tags.iter().any(|tag| tag == "B") {
                    let proxy = ConnectProxy::start().await;
                    let env = proxy.centrifugo_env();
                    let env: Vec<_> = env
                        .iter()
                        .map(|(name, value)| (*name, value.as_str()))
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
        .fail_on_skipped()
        .filter_run_and_exit(FEATURES, |_, _, scenario| {
            IMPLEMENTED.contains(&scenario.name.as_str())
        })
        .await;
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
