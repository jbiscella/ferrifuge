//! Instruction counts of dispatching one publication (Block 14, `@P`), with
//! gungraun on Valgrind. `tests/perf_gates.rs` gates on them.

// The gungraun macros generate undocumented items.
#![allow(missing_docs)]

mod common;

use common::subscribed;
use ferrifuge::machine::{Action, Machine};

/// Drops the machine and the actions outside the measurement: dropping the
/// subscriptions would otherwise count, and grow with their number.
fn teardown((machine, actions): (Machine, Vec<Action>)) {
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::EmitSubscription(..))),
        "the publication was not dispatched"
    );
    drop((machine, actions));
}

/// The gungraun harness, run by [`main`] under `cargo bench`. The
/// benchmark, its group and the macro's `main` share this module: gungraun
/// finds the benchmark by its module path.
mod harness {
    use std::hint::black_box;

    use ferrifuge::machine::{Action, Input, Machine};
    use gungraun::{library_benchmark, library_benchmark_group};

    use super::{subscribed, teardown};

    #[library_benchmark]
    #[bench::one_channel(setup = subscribed, args = (1), teardown = teardown)]
    #[bench::ten_thousand_channels(setup = subscribed, args = (10_000), teardown = teardown)]
    fn dispatch((mut machine, publication): (Machine, Input)) -> (Machine, Vec<Action>) {
        let actions = machine.handle(black_box(publication));
        black_box((machine, actions))
    }

    library_benchmark_group!(name = publications, benchmarks = [dispatch]);

    gungraun::main!(library_benchmark_groups = publications);

    /// The macro's `main`, which it keeps private.
    pub(super) fn run() {
        main();
    }
}

/// Runs the harness under `cargo bench`, which passes `--bench`, and when
/// gungraun-runner runs this binary under Valgrind (`--gungraun-run`).
/// Under `cargo test --all-targets` it does nothing: the runner and
/// Valgrind are installed only in the perf job.
fn main() {
    if std::env::args().any(|arg| arg == "--bench" || arg.starts_with("--gungraun")) {
        harness::run();
    } else {
        println!("dispatch benchmark skipped: run it with cargo bench");
    }
}
