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
use std::fs;

use cucumber::World;
use cucumber::gherkin::{Feature, GherkinEnv};

use steps::ClientWorld;
use support::centrifugo::Centrifugo;

const FEATURES: &str = "tests/features";

/// Scenarios whose behaviour is implemented, by name.
const IMPLEMENTED: &[&str] = &[
    // Block 1, sub-increment 1a.
    "New client starts disconnected",
    "Successful connect",
    "Explicit disconnect stops reconnection",
    "Connect after explicit disconnect",
    "Connect failures not reflected in state are reported as errors",
];

/// Server configuration of @A scenarios.
const SERVER_A: &[(&str, &str)] = &[(
    "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
    "true",
)];

#[tokio::main]
async fn main() {
    assert_implemented_scenarios_exist();
    ClientWorld::cucumber()
        .before(|_, _, scenario, world| {
            Box::pin(async move {
                if scenario.tags.iter().any(|tag| tag == "A") {
                    world.server = Some(Centrifugo::start(SERVER_A).await);
                }
            })
        })
        .fail_on_skipped()
        .filter_run_and_exit(FEATURES, |_, _, scenario| {
            IMPLEMENTED.contains(&scenario.name.as_str())
        })
        .await;
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
