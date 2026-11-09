//! Every `.feature` file under `tests/features/` must be a verbatim copy of
//! the `gherkin` block in `CLAUDE.md` with the same `Feature:` name, and
//! every feature in `CLAUDE.md` must have its file unless it is listed in
//! [`NOT_YET_IMPLEMENTED`].
//! CLAUDE.md is canonical: on mismatch, fix the feature file or raise drift.
//!
//! `@P` scenarios run outside cucumber (Block 0, "Test layers"): each must be
//! claimed by a benchmark or simulation unless it is listed in
//! [`P_NOT_YET_MEASURED`], and every claim must name a `@P` scenario. So do
//! `@W` scenarios, claimed by the wasm tests run in a browser (Block 44,
//! D18), unless listed in [`W_NOT_YET_WRITTEN`].

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Features of `CLAUDE.md` whose blocks are not implemented yet, so have no
/// feature file. A block's feature leaves the list when its file is added.
const NOT_YET_IMPLEMENTED: &[&str] = &[
    "Bug-pattern catalogue",
    "Byte-split matrix",
    "Real-server chaos",
    "Soak run",
    "Shared poll subscriptions (unstable)",
];

/// `@P` scenarios of `CLAUDE.md` that no benchmark or simulation claims
/// yet. A scenario leaves the list when a harness claims it.
const P_NOT_YET_MEASURED: &[&str] = &[
    "What arrived during a process suspension beats a deadline that fired meanwhile",
    "Seeded chaos on the core applies no stale answer",
    "The core runner reports a planted continuity fault",
    "Seeded chaos on the core leaks no secret",
    "Seeded chaos with handlers on the tokio client keeps the invariants",
    "A large reply delivered one byte per chunk costs what its size predicts",
    "Seeded real-server chaos keeps the invariants",
    "A server restart with the memory engine reports recovered false",
    "A server restart with the Redis engine loses nothing",
    "A failing real-server seed is a finding only when it fails twice",
    "A seed of the real-server runner fixes its operations",
    "The real-server runner reports a planted fault",
    "Hours of normal use keep resources flat and delivery exact",
];

/// How a benchmark or simulation claims a `@P` scenario: a line of its own
/// reading `// @P scenario: <scenario name>`.
const P_CLAIM: &str = "// @P scenario: ";

/// Directories searched for `@P` claims.
const P_HARNESS_DIRS: &[&str] = &["benches", "tests"];

/// `@W` scenarios of `CLAUDE.md` that no wasm test claims yet. A scenario
/// leaves the list when a test claims it.
const W_NOT_YET_WRITTEN: &[&str] = &["A browser client tracks a key and receives its updates"];

/// How a wasm test claims a `@W` scenario: a line of its own reading
/// `// @W scenario: <scenario name>`.
const W_CLAIM: &str = "// @W scenario: ";

/// Directories searched for `@W` claims.
const W_TEST_DIRS: &[&str] = &["tests"];

/// A text file, with Windows line endings (a checkout with
/// `core.autocrlf`) read as Unix ones.
fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .replace("\r\n", "\n")
}

fn normalize(text: &str) -> String {
    text.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn feature_name(text: &str) -> Option<&str> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix("Feature:"))
        .map(str::trim)
}

fn gherkin_blocks(claude_md: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut rest = claude_md;
    while let Some(start) = rest.find("```gherkin\n") {
        let body = &rest[start + "```gherkin\n".len()..];
        let end = body
            .find("\n```")
            .expect("unterminated gherkin block in CLAUDE.md");
        blocks.push(&body[..end]);
        rest = &body[end..];
    }
    blocks
}

fn spec_features(claude_md: &str) -> HashMap<String, String> {
    let mut features = HashMap::new();
    for block in gherkin_blocks(claude_md) {
        let name = feature_name(block).expect("gherkin block without a Feature: line");
        let previous = features.insert(name.to_owned(), normalize(block));
        assert!(
            previous.is_none(),
            "duplicate feature {name:?} in CLAUDE.md"
        );
    }
    features
}

/// Names of the scenarios tagged `tag` in the gherkin blocks of
/// `claude_md`.
fn tagged_scenarios(claude_md: &str, tag: &str) -> BTreeSet<String> {
    gherkin_blocks(claude_md)
        .into_iter()
        .flat_map(|gherkin| tagged_scenarios_of(gherkin, tag))
        .collect()
}

/// Names of the scenarios tagged `@P` in one gherkin text.
fn p_scenarios_of(gherkin: &str) -> BTreeSet<String> {
    tagged_scenarios_of(gherkin, "@P")
}

/// Names of the scenarios tagged `tag` in one gherkin text, by Gherkin's
/// rules: a scenario's tags may span several lines; a feature's or a rule's
/// tags apply to every scenario under it; an `Examples` table's tags apply
/// to the rows of its outline, so tag the outline.
fn tagged_scenarios_of(gherkin: &str, tag: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let (mut feature, mut rule, mut pending) = (false, false, false);
    let mut outline: Option<&str> = None;
    for line in gherkin.lines().map(str::trim) {
        if line.starts_with('@') {
            pending |= line.split_whitespace().any(|found| found == tag);
        } else if line.starts_with("Feature:") {
            (feature, rule, outline) = (pending, false, None);
            pending = false;
        } else if line.starts_with("Rule:") {
            (rule, outline) = (pending, None);
            pending = false;
        } else if line.starts_with("Background:") {
            (outline, pending) = (None, false);
        } else if line.starts_with("Examples:") || line.starts_with("Scenarios:") {
            if let Some(name) = outline.filter(|_| pending) {
                names.insert(name.to_owned());
            }
            pending = false;
        } else if let Some(name) = [
            "Scenario Outline:",
            "Scenario Template:",
            "Scenario:",
            "Example:",
        ]
        .iter()
        .find_map(|keyword| line.strip_prefix(keyword))
        {
            let name = name.trim();
            if feature || rule || pending {
                names.insert(name.to_owned());
            }
            outline = Some(name);
            pending = false;
        }
    }
    names
}

fn rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// Scenario names claimed with `marker` under `dirs`, with where.
fn claims(root: &Path, dirs: &[&str], marker: &str) -> Vec<(String, PathBuf)> {
    let mut files = Vec::new();
    for dir in dirs {
        rust_files(&root.join(dir), &mut files);
    }
    let mut claims = Vec::new();
    for path in files {
        // This file names the marker without claiming anything.
        if path.ends_with("spec_sync.rs") {
            continue;
        }
        for line in read(&path).lines() {
            if let Some(name) = line.trim().strip_prefix(marker) {
                claims.push((name.trim().to_owned(), path.clone()));
            }
        }
    }
    claims
}

#[test]
fn feature_files_match_claude_md() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = spec_features(&read(&root.join("CLAUDE.md")));
    let mut files = HashSet::new();

    for entry in fs::read_dir(root.join("tests/features")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "feature") {
            continue;
        }
        let text = read(&path);
        let name = feature_name(&text)
            .unwrap_or_else(|| panic!("{} has no Feature: line", path.display()));
        let expected = spec
            .get(name)
            .unwrap_or_else(|| panic!("{} declares {name:?}, not in CLAUDE.md", path.display()));
        assert_eq!(
            &normalize(&text),
            expected,
            "{} differs from CLAUDE.md",
            path.display()
        );
        assert!(
            files.insert(name.to_owned()),
            "{} declares {name:?} again",
            path.display()
        );
    }

    for name in NOT_YET_IMPLEMENTED {
        assert!(
            spec.contains_key(*name),
            "{name:?} is listed as not yet implemented but is not in CLAUDE.md"
        );
        assert!(
            !files.contains(*name),
            "{name:?} has a feature file: remove it from NOT_YET_IMPLEMENTED"
        );
    }
    let mut missing: Vec<_> = spec
        .keys()
        .filter(|name| !files.contains(*name) && !NOT_YET_IMPLEMENTED.contains(&name.as_str()))
        .collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "features of CLAUDE.md without a feature file: {missing:?}"
    );
}

#[test]
fn p_scenarios_match_their_harnesses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = tagged_scenarios(&read(&root.join("CLAUDE.md")), "@P");

    let mut claimed = HashSet::new();
    for (name, path) in claims(root, P_HARNESS_DIRS, P_CLAIM) {
        assert!(
            spec.contains(&name),
            "{} claims {name:?}, not a @P scenario of CLAUDE.md",
            path.display()
        );
        claimed.insert(name);
    }
    for name in P_NOT_YET_MEASURED {
        assert!(
            spec.contains(*name),
            "{name:?} is listed as not yet measured but is not a @P scenario of CLAUDE.md"
        );
        assert!(
            !claimed.contains(*name),
            "{name:?} is claimed: remove it from P_NOT_YET_MEASURED"
        );
    }
    let unclaimed: Vec<_> = spec
        .iter()
        .filter(|name| !claimed.contains(*name) && !P_NOT_YET_MEASURED.contains(&name.as_str()))
        .collect();
    assert!(
        unclaimed.is_empty(),
        "@P scenarios with no benchmark or simulation: {unclaimed:?}"
    );
}

#[test]
fn w_scenarios_match_their_wasm_tests() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = tagged_scenarios(&read(&root.join("CLAUDE.md")), "@W");

    let mut claimed = HashSet::new();
    for (name, path) in claims(root, W_TEST_DIRS, W_CLAIM) {
        assert!(
            spec.contains(&name),
            "{} claims {name:?}, not a @W scenario of CLAUDE.md",
            path.display()
        );
        claimed.insert(name);
    }
    for name in W_NOT_YET_WRITTEN {
        assert!(
            spec.contains(*name),
            "{name:?} is listed as not yet written but is not a @W scenario of CLAUDE.md"
        );
        assert!(
            !claimed.contains(*name),
            "{name:?} is claimed: remove it from W_NOT_YET_WRITTEN"
        );
    }
    let unclaimed: Vec<_> = spec
        .iter()
        .filter(|name| !claimed.contains(*name) && !W_NOT_YET_WRITTEN.contains(&name.as_str()))
        .collect();
    assert!(
        unclaimed.is_empty(),
        "@W scenarios with no wasm test: {unclaimed:?}"
    );
}

#[test]
fn p_tags_follow_gherkin_tag_inheritance() {
    let gherkin = "\
Feature: F

  @P
  @E
  Scenario: Tags on two lines
    Given x

  @E
  Scenario: Not P
    Given x

  @E
  Scenario Outline: P on its examples
    Given <x>

    @P
    Examples:
      | x |
      | 1 |

  @P
  Rule: R

    Scenario: Under a P rule
      Given x

  Rule: S

    Scenario: Under another rule
      Given x
";
    let inherited = "\
@P
Feature: G

  Scenario: Under a P feature
    Given x
";

    let names = p_scenarios_of(gherkin);
    let inherited = p_scenarios_of(inherited);

    assert_eq!(
        names,
        BTreeSet::from(
            ["Tags on two lines", "P on its examples", "Under a P rule"].map(String::from)
        )
    );
    assert_eq!(inherited, BTreeSet::from(["Under a P feature".to_owned()]));
}
