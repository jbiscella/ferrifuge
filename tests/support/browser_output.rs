//! Reading `wasm-bindgen-test-runner`'s output for the browser harness
//! (Block 44, D18): whether a browser's last run failed to start its
//! `WebDriver` session, and the one retry that failure gets.

/// The line the runner prints before it asks the driver for a session.
const RUNNING: &str = "Running headless tests in";
/// The line the runner prints when the driver refuses the session.
const SESSION_FAILED: &str = "webdriver POST /session request failed";
/// The line a test binary prints once its tests ran.
const RESULT: &str = "test result:";

/// Whether `output`'s last headless run failed `POST /session` before any
/// test result: the browser never started its tests, so running it again
/// masks no test failure. An earlier binary's result (the `--lib` run's)
/// does not count.
pub(crate) fn session_never_started(output: &str) -> bool {
    let Some(start) = output.rfind(RUNNING) else {
        return false;
    };
    let last = &output[start..];
    last.contains(SESSION_FAILED) && !last.contains(RESULT)
}

/// Runs `run`, which gives whether it passed and its output, and once
/// more, logged through `log`, when its session never started: a
/// stopgap, tracked with its removal in #325.
pub(crate) fn with_one_retry(
    mut run: impl FnMut() -> (bool, String),
    mut log: impl FnMut(&str),
) -> bool {
    let (passed, output) = run();
    if passed || !session_never_started(&output) {
        return passed;
    }
    log(
        "the WebDriver session never started (geckodriver gives Firefox 60 s to start Marionette): running once more",
    );
    run().0
}
