//! Wall-clock time to decode and dispatch one publication (Block 14, `@P`,
//! reported only), with criterion. `tests/perf_wallclock.rs` measures the
//! delivery latency and the idle CPU of the same scenarios' clients.

// @P scenario: Wall-clock cost per publication is reported

// The criterion macros generate undocumented items.
#![allow(missing_docs)]

mod common;

use std::hint::black_box;

use common::subscribed;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

/// A connected sans-IO core with 10 subscribed channels handling one JSON
/// publication of 1 KiB: decoding the frame and dispatching it.
fn decode_and_dispatch(c: &mut Criterion) {
    let (mut machine, publication) = subscribed(10);
    c.bench_function(
        "decode and dispatch a 1 KiB publication, 10 channels",
        |b| {
            b.iter_batched(
                || publication.clone(),
                |publication| black_box(machine.handle(publication)),
                BatchSize::SmallInput,
            );
        },
    );
}

criterion_group!(benches, decode_and_dispatch);
criterion_main!(benches);
