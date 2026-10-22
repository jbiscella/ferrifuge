//! Gates on the deterministic `@P` metrics of Block 14, read from the JSON
//! summaries the benchmarks save. Run them first:
//! `cargo bench --bench dispatch -- --save-summary=json`, then
//! `cargo test --test perf_gates -- --ignored`.

use std::path::PathBuf;
use std::{env, fs};

use serde_json::Value;

/// Instructions counted by gungraun for one benchmark of `benches/dispatch.rs`.
fn instructions(bench: &str) -> u64 {
    let target = env::var_os("CARGO_TARGET_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"),
        PathBuf::from,
    );
    let path = target
        .join("gungraun/ferrifuge/dispatch/publications")
        .join(format!("dispatch.{bench}"))
        .join("summary.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}; run the benchmarks first", path.display()));
    let summary: Value = serde_json::from_str(&text).expect("a gungraun summary");
    summary["profiles"][0]["data"]["total"]["metrics"]["Ir"]["values"]["new"]
        .as_u64()
        .unwrap_or_else(|| panic!("no instruction count in {}", path.display()))
}

// @P scenario: Dispatch cost does not depend on the number of subscriptions
#[test]
#[ignore = "needs the summaries of `cargo bench --bench dispatch -- --save-summary=json`"]
fn dispatch_cost_does_not_depend_on_the_number_of_subscriptions() {
    let one = instructions("one_channel");
    let many = instructions("ten_thousand_channels");

    // At most 1.1 times, in integers.
    assert!(
        many * 10 <= one * 11,
        "{many} instructions with 10000 channels, {one} with 1 ({}%)",
        many * 100 / one
    );
}
