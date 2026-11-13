//! Gates on the deterministic `@P` metrics of Blocks 14 and 50, read from
//! the JSON summaries the benchmarks save. Run them first:
//! `cargo bench --bench dispatch -- --save-summary=json` and
//! `cargo bench --bench decode -- --save-summary=json`, then
//! `cargo test --test perf_gates -- --ignored`.

use std::path::PathBuf;
use std::{env, fs};

use serde_json::Value;

/// Instructions counted by gungraun for one benchmark of `benches/dispatch.rs`.
fn instructions(bench: &str) -> u64 {
    counted(
        "dispatch/harness/publications",
        &format!("dispatch.{bench}"),
    )
}

/// Instructions counted by gungraun for the benchmark `id` of the group at
/// `group`, a path under the crate's gungraun directory.
fn counted(group: &str, id: &str) -> u64 {
    let target = env::var_os("CARGO_TARGET_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"),
        PathBuf::from,
    );
    let path = target
        .join("gungraun/ferrifuge")
        .join(group)
        .join(id)
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
    // No count means the benchmark measured nothing, e.g. a misnamed
    // function; it would pass the bound below.
    assert!(one > 0 && many > 0, "nothing measured: {one} and {many}");

    // At most 1.1 times, in integers.
    assert!(
        many * 10 <= one * 11,
        "{many} instructions with 10000 channels, {one} with 1 ({}%)",
        many * 100 / one
    );
}

// @P scenario: A large reply delivered one byte per chunk costs what its size predicts
#[test]
#[ignore = "needs the summaries of `cargo bench --bench decode -- --save-summary=json`"]
fn a_large_reply_one_byte_per_chunk_costs_what_its_size_predicts() {
    let mut ratios = Vec::new();
    for format in ["json", "sse", "protobuf"] {
        let counts = ["256_kib", "1_mib"].map(|size| {
            counted(
                "decode/harness/decoding",
                &format!("one_byte_per_chunk.{format}_{size}"),
            )
        });
        let [small, large] = counts;
        assert!(
            small > 0 && large > 0,
            "{format}: nothing measured: {counts:?}"
        );
        // At most 5 times for 4 times the bytes (Block 50, D5): a linear
        // decoder costs 4 times, one rescanning its buffer 16.
        println!(
            "@P report {format}: {large} instructions for 1 MiB, {small} for 256 KiB ({}%)",
            large * 100 / small
        );
        ratios.push((format, large <= small * 5, large * 100 / small));
    }
    let over: Vec<_> = ratios.iter().filter(|(_, within, _)| !within).collect();
    assert!(over.is_empty(), "over 5 times: {over:?}");
}
