//! The browser harness's session-failure retry (Block 44, D18).

#[path = "support/browser_output.rs"]
mod browser_output;

use browser_output::{session_never_started, with_one_retry};

/// The output of main's run 43729006123, Firefox: the `--lib` binary's
/// result, then `browser_wasm`'s session refused.
const REFUSED: &str = "\
Running headless tests in Firefox on `http://127.0.0.1:41111/`
test result: ok. 51 passed; 0 failed; 0 ignored; 0 filtered out; finished in 2.57s
Running headless tests in Firefox on `http://127.0.0.1:36731/`
Try find `webdriver.json` for configure browser's capabilities:
Ok
driver status: signal: 9 (SIGKILL)
Error: webdriver POST /session request failed

Caused by:
    http status: 500
";

#[test]
fn a_session_refused_after_an_earlier_binarys_result_is_retried() {
    assert!(session_never_started(REFUSED));
}

#[test]
fn a_run_whose_last_binary_reported_a_result_is_not_retried() {
    let failed_tests = format!(
        "{REFUSED}Running headless tests in Firefox\ntest result: FAILED. 1 passed; 1 failed\n"
    );
    assert!(!session_never_started(&failed_tests));
    let after_result = "Running headless tests in Firefox\ntest result: FAILED.\nwebdriver POST /session request failed\n";
    assert!(!session_never_started(after_result));
    assert!(!session_never_started(
        "error: could not compile `ferrifuge`\n"
    ));
}

#[test]
fn the_retry_fires_once_and_only_for_a_session_never_started() {
    let mut runs = 0;
    let mut logged = Vec::new();
    let passed = with_one_retry(
        || {
            runs += 1;
            if runs == 1 {
                (false, REFUSED.to_owned())
            } else {
                (
                    true,
                    "Running headless tests in Firefox\ntest result: ok.\n".to_owned(),
                )
            }
        },
        |line| logged.push(line.to_owned()),
    );
    assert!(passed);
    assert_eq!(runs, 2);
    assert_eq!(logged.len(), 1, "{logged:?}");

    let mut runs = 0;
    let passed = with_one_retry(
        || {
            runs += 1;
            (false, REFUSED.to_owned())
        },
        |_| {},
    );
    assert!(!passed, "a second refusal fails the browser");
    assert_eq!(runs, 2, "never a third run");

    let mut runs = 0;
    let passed = with_one_retry(
        || {
            runs += 1;
            (
                false,
                "Running headless tests in Firefox\ntest result: FAILED.\n".to_owned(),
            )
        },
        |_| {},
    );
    assert!(!passed);
    assert_eq!(runs, 1, "a test failure is never retried");
}
