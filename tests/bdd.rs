//! Runs the Gherkin scenarios in `tests/features/`.
//!
//! Select a test layer with cucumber's tag filter, e.g.
//! `cargo test --test bdd -- --tags @E`. Scenarios with undefined steps fail
//! the run rather than being skipped.

use cucumber::World;

#[derive(Debug, Default, World)]
struct ClientWorld {}

#[tokio::main]
async fn main() {
    ClientWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit("tests/features")
        .await;
}
