//! Steps of Block 51's `scripts/validation-run` scenarios (51b, D11 and
//! D12): the script reads fixtures of the GitHub API's answers, so the
//! scenarios need no token (Exceptions introduced).

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::path::PathBuf;
use std::process::Output;

use cucumber::{given, then, when};
use serde_json::{Value, json};
use tokio::process::Command;

use super::ClientWorld;
use super::release::{bash, repository};

/// The commit a scheduled run or a release is about.
const HEAD: &str = "1111111111111111111111111111111111111111";
/// Another commit.
const OTHER: &str = "2222222222222222222222222222222222222222";
const WORKFLOW: &str = "chaos-real.yml";

/// What a validation-run scenario holds.
#[derive(Debug, Default)]
pub(crate) struct ValidationRun {
    fixtures: Option<tempfile_dir::Dir>,
    output: Option<Output>,
}

/// A run in the fixtures: its head, its status and its `verdict` job's
/// conclusion, `None` for a run without one.
struct Run {
    head: &'static str,
    status: &'static str,
    verdict: Option<&'static str>,
}

/// A directory of fixtures, removed when dropped.
mod tempfile_dir {
    use std::path::{Path, PathBuf};

    #[derive(Debug)]
    pub(crate) struct Dir(PathBuf);

    impl Dir {
        pub(crate) fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let dir = std::env::temp_dir().join(format!(
                "ferrifuge-validation-run-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("a fixtures directory");
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Writes `runs`, newest first, as the API's answers: the workflow's runs
/// and each run's jobs. A run whose verdict is skipped concludes
/// `success`, as GitHub reports a run the guard skipped (D11), and its
/// jobs are skipped; a cancelled run has its `verdict` job cancelled.
fn fixtures(runs: &[Run]) -> tempfile_dir::Dir {
    let dir = tempfile_dir::Dir::new();
    let listed: Vec<Value> = runs
        .iter()
        .enumerate()
        .map(|(n, run)| {
            let id = 1000 - n;
            let conclusion = match (run.status, run.verdict) {
                (status, _) if status != "completed" => Value::Null,
                (_, Some("skipped")) => json!("success"),
                (_, Some(verdict)) => json!(verdict),
                (_, None) => json!("cancelled"),
            };
            let jobs = match run.verdict {
                Some(verdict) => json!({"jobs": [
                    {"name": "guard", "status": "completed", "conclusion": "success"},
                    {"name": "verdict", "status": run.status, "conclusion": verdict},
                ]}),
                None => json!({"jobs": [
                    {"name": "guard", "status": "completed", "conclusion": "success"},
                    {"name": "verdict", "status": run.status, "conclusion": Value::Null},
                ]}),
            };
            std::fs::write(
                dir.path().join(format!("jobs-{id}.json")),
                jobs.to_string(),
            )
            .expect("a jobs fixture");
            json!({"id": id, "head_sha": run.head, "status": run.status, "conclusion": conclusion, "event": "schedule"})
        })
        .collect();
    std::fs::write(
        dir.path().join("runs-1.json"),
        json!({"total_count": listed.len(), "workflow_runs": listed}).to_string(),
    )
    .expect("a runs fixture");
    dir
}

#[given(regex = r"^the validation-run script and a workflow history where (.+)$")]
fn a_workflow_history(world: &mut ClientWorld, history: String) {
    let runs: Vec<Run> = match history.as_str() {
        "no real run has concluded" => vec![
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("skipped"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: None,
            },
        ],
        "the last real run concluded on another commit" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: None,
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        "the last real run concluded on the head commit, passed" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("skipped"),
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("success"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("failure"),
            },
        ],
        "the last real run concluded on the head commit, failed" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("failure"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        "the head commit has only cancelled runs" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: None,
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("cancelled"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        other => panic!("no history {other:?}"),
    };
    world.validation_run.fixtures = Some(fixtures(&runs));
}

#[given(regex = r"^the validation-run script and the runs of the tagged commit, where (.+)$")]
fn the_runs_of_the_tagged_commit(world: &mut ClientWorld, history: String) {
    let runs: Vec<Run> = match history.as_str() {
        "the latest real run passed" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("skipped"),
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("success"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("failure"),
            },
        ],
        "the latest real run failed, a skipped run after" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("skipped"),
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("failure"),
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        "the latest real run is still in progress" => vec![
            Run {
                head: HEAD,
                status: "in_progress",
                verdict: None,
            },
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        "only a skipped run exists" => vec![
            Run {
                head: HEAD,
                status: "completed",
                verdict: Some("skipped"),
            },
            Run {
                head: OTHER,
                status: "completed",
                verdict: Some("success"),
            },
        ],
        "no run exists" => vec![Run {
            head: OTHER,
            status: "completed",
            verdict: Some("success"),
        }],
        other => panic!("no history {other:?}"),
    };
    world.validation_run.fixtures = Some(fixtures(&runs));
}

/// Runs the script with `args` on the scenario's fixtures. Awaited, not
/// waited for, as the release check is.
async fn validation_run(world: &ClientWorld, args: &[&str]) -> Output {
    let fixtures = world.validation_run.fixtures.as_ref().expect("fixtures");
    let script: PathBuf = repository().join("scripts").join("validation-run");
    Command::new(bash())
        .arg(script)
        .args(args)
        .env("FERRIFUGE_VALIDATION_FIXTURES", fixtures.path())
        .env_remove("GITHUB_OUTPUT")
        .output()
        .await
        .expect("bash runs validation-run")
}

#[when("it decides a scheduled run of the head commit")]
async fn it_decides(world: &mut ClientWorld) {
    let output = validation_run(world, &["should-run", WORKFLOW, HEAD]).await;
    world.validation_run.output = Some(output);
}

#[when("it checks the release gate for the commit")]
async fn it_gates(world: &mut ClientWorld) {
    let output = validation_run(world, &["gate", WORKFLOW, HEAD]).await;
    world.validation_run.output = Some(output);
}

/// Whether the scenario ran the script, for the step "it succeeds" that
/// the release check's scenarios share (`release.rs`).
pub(super) fn ran(world: &ClientWorld) -> bool {
    world.validation_run.output.is_some()
}

fn output(world: &ClientWorld) -> (&Output, String) {
    let output = world
        .validation_run
        .output
        .as_ref()
        .expect("the script ran");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output, text)
}

#[then(regex = r"^it (runs|skips)$")]
fn it_runs_or_skips(world: &mut ClientWorld, decision: String) {
    let (output, text) = output(world);
    assert!(output.status.success(), "{text}");
    let expected = if decision == "runs" {
        "run=true"
    } else {
        "run=false"
    };
    assert!(text.starts_with(expected), "{text}");
}

#[then(
    regex = r"^it (fails, naming the workflow and the commit|fails, naming the workflow and saying to dispatch it)$"
)]
pub(super) fn the_gate_outcome(world: &mut ClientWorld, outcome: String) {
    let (output, text) = output(world);
    if outcome == "succeeds" {
        assert!(output.status.success(), "{text}");
        return;
    }
    assert!(!output.status.success(), "{text}");
    assert!(text.contains(WORKFLOW) && text.contains(HEAD), "{text}");
    let dispatch = text.contains("dispatch it with 'gh workflow run");
    assert_eq!(
        dispatch,
        outcome.ends_with("saying to dispatch it"),
        "{text}"
    );
}
