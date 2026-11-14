//! `scripts/validation-run` beyond its scenarios (Block 51b, Codex on
//! #260): a real run past the first page of the history is found, and a
//! lookup that fails fails the script instead of reading as an empty
//! history. The script reads fixtures of the GitHub API's answers.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};

const HEAD: &str = "1111111111111111111111111111111111111111";
const WORKFLOW: &str = "chaos-real.yml";

/// bash: Git's on Windows, as the release check's steps use.
fn bash() -> PathBuf {
    let git_bash = Path::new(r"C:\Program Files\Git\bin\bash.exe");
    if cfg!(windows) && git_bash.exists() {
        git_bash.to_owned()
    } else {
        PathBuf::from("bash")
    }
}

/// A directory of fixtures, removed when dropped.
struct Fixtures(PathBuf);

impl Fixtures {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "ferrifuge-validation-run-test-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn write(&self, file: &str, contents: &str) {
        std::fs::write(self.0.join(file), contents).unwrap();
    }

    /// Writes page `page` of runs `(id, head, verdict)`, all completed, and
    /// each run's jobs.
    fn page(&self, page: usize, runs: &[(u64, &str, &str)]) {
        let listed: Vec<Value> = runs
            .iter()
            .map(|(id, head, verdict)| {
                self.write(
                    &format!("jobs-{id}.json"),
                    &json!({"jobs": [{"name": "verdict", "status": "completed", "conclusion": verdict}]})
                        .to_string(),
                );
                json!({"id": id, "head_sha": head, "status": "completed", "conclusion": "success"})
            })
            .collect();
        self.write(
            &format!("runs-{page}.json"),
            &json!({"workflow_runs": listed}).to_string(),
        );
    }

    fn run(&self, args: &[&str]) -> (Output, String) {
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("validation-run");
        let output = Command::new(bash())
            .arg(script)
            .args(args)
            .env("FERRIFUGE_VALIDATION_FIXTURES", &self.0)
            .env_remove("GITHUB_OUTPUT")
            .output()
            .expect("bash runs validation-run");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output, text)
    }
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 100 runs the guard skipped, on `head`, numbered down from `first`.
fn skipped(first: u64, head: &'static str) -> Vec<(u64, &'static str, &'static str)> {
    (0..100).map(|n| (first - n, head, "skipped")).collect()
}

// Codex on #260: a page of 100 skipped runs does not hide the real run
// on the next page.
#[test]
fn a_real_run_past_the_first_page_decides() {
    let fixtures = Fixtures::new("pages");
    fixtures.page(1, &skipped(1000, HEAD));
    fixtures.page(2, &[(800, HEAD, "failure"), (799, HEAD, "success")]);

    let (output, text) = fixtures.run(&["should-run", WORKFLOW, HEAD]);
    assert!(output.status.success(), "{text}");
    assert!(
        text.starts_with("run=false"),
        "a failed real run stands: {text}"
    );

    let (output, text) = fixtures.run(&["gate", WORKFLOW, HEAD]);
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("failed"),
        "the latest real run failed: {text}"
    );
}

// Codex on #260: an answer that cannot be read fails the script, rather
// than reading as an empty history (run=true, or "no real run").
#[test]
fn a_lookup_that_fails_fails_the_script() {
    let runs = Fixtures::new("runs");
    runs.write("runs-1.json", "<html>502 Bad Gateway</html>");
    for args in [["should-run", WORKFLOW, HEAD], ["gate", WORKFLOW, HEAD]] {
        let (output, text) = runs.run(&args);
        assert!(!output.status.success(), "{args:?}: {text}");
        assert!(
            text.contains("could not read page 1 of the runs"),
            "{args:?}: {text}"
        );
    }

    let jobs = Fixtures::new("jobs");
    jobs.page(1, &[(900, HEAD, "success")]);
    jobs.write("jobs-900.json", "not json");
    for args in [["should-run", WORKFLOW, HEAD], ["gate", WORKFLOW, HEAD]] {
        let (output, text) = jobs.run(&args);
        assert!(!output.status.success(), "{args:?}: {text}");
        assert!(
            text.contains("could not read the jobs of run 900"),
            "{args:?}: {text}"
        );
    }
}

// #260 on Windows: jq there writes CRLF, and a status read as
// "completed\r" matched nothing. A jq that ends its lines with CR stands in
// for it here, where bash and jq are Unix ones.
#[cfg(unix)]
#[test]
fn answers_ending_their_lines_with_cr_are_read_alike() {
    use std::os::unix::fs::PermissionsExt;
    let fixtures = Fixtures::new("crlf");
    fixtures.page(1, &[(900, HEAD, "success")]);
    let bin = fixtures.0.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let real_jq = String::from_utf8(
        Command::new("bash")
            .args(["-c", "command -v jq"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let jq = bin.join("jq");
    std::fs::write(
        &jq,
        format!("#!/usr/bin/env bash\n\"{real_jq}\" \"$@\" | sed 's/$/\\r/'\n"),
    )
    .unwrap();
    std::fs::set_permissions(&jq, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("validation-run");
    for (args, decided) in [
        (["should-run", WORKFLOW, HEAD], "run=false"),
        (["gate", WORKFLOW, HEAD], "passed"),
    ] {
        let output = Command::new("bash")
            .arg(&script)
            .args(args)
            .env("FERRIFUGE_VALIDATION_FIXTURES", &fixtures.0)
            .env("PATH", &path)
            .env_remove("GITHUB_OUTPUT")
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "{args:?}: {text}");
        assert!(text.contains(decided), "{args:?}: {text}");
    }
}
