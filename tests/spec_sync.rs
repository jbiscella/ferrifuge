//! Every `.feature` file under `tests/features/` must be a verbatim copy of
//! the `gherkin` block in `CLAUDE.md` with the same `Feature:` name, and
//! every feature in `CLAUDE.md` must have its file unless it is listed in
//! [`NOT_YET_IMPLEMENTED`].
//! CLAUDE.md is canonical: on mismatch, fix the feature file or raise drift.
//!
//! `@P` scenarios run outside cucumber (Block 0, "Test layers"): each must be
//! claimed by a benchmark or simulation unless it is listed in
//! [`P_NOT_YET_MEASURED`], and every claim must name a `@P` scenario.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Features of `CLAUDE.md` whose blocks are not implemented yet, so have no
/// feature file. A block's feature leaves the list when its file is added.
const NOT_YET_IMPLEMENTED: &[&str] = &[
    "Subscription tokens, unsubscribe codes and resubscribe backoff",
    "Subscription recovery",
    "Server-side subscriptions",
    "Client commands",
];

/// `@P` scenarios of `CLAUDE.md` that no benchmark or simulation claims
/// yet. A scenario leaves the list when a harness claims it.
const P_NOT_YET_MEASURED: &[&str] = &[
    "Idle footprint with ten subscriptions",
    "Marginal cost of a subscription",
    "Allocations per publication do not depend on the number of subscriptions",
    "Dispatch cost does not depend on the number of subscriptions",
    "Memory stays flat across reconnects",
    "An idle client wakes only for pings and refreshes",
    "Reconnects of a fleet are spread over the jitter window",
    "Token refreshes of a fleet are spread",
    "Wall-clock cost per publication is reported",
    "Idle CPU is reported",
];

/// How a benchmark or simulation claims a `@P` scenario: a line of its own
/// reading `// @P scenario: <scenario name>`.
const P_CLAIM: &str = "// @P scenario: ";

/// Directories searched for `@P` claims.
const P_HARNESS_DIRS: &[&str] = &["benches", "tests"];

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

/// Names of the scenarios tagged `@P` in the gherkin blocks of `claude_md`.
fn p_scenarios(claude_md: &str) -> BTreeSet<String> {
    gherkin_blocks(claude_md)
        .into_iter()
        .flat_map(p_scenarios_of)
        .collect()
}

/// Names of the scenarios tagged `@P` in one gherkin text. A scenario's tags
/// may span several lines; tags of another element (a feature, a rule, an
/// `Examples` table) do not carry over to the next scenario.
fn p_scenarios_of(gherkin: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut tagged = false;
    for line in gherkin.lines().map(str::trim) {
        if line.starts_with('@') {
            tagged |= line.split_whitespace().any(|tag| tag == "@P");
        } else if let Some(name) = line
            .strip_prefix("Scenario:")
            .or_else(|| line.strip_prefix("Scenario Outline:"))
        {
            if tagged {
                names.push(name.trim().to_owned());
            }
            tagged = false;
        } else if [
            "Feature:",
            "Rule:",
            "Background:",
            "Examples:",
            "Scenarios:",
        ]
        .iter()
        .any(|keyword| line.starts_with(keyword))
        {
            tagged = false;
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

/// `@P` scenario names claimed under [`P_HARNESS_DIRS`], with where.
fn p_claims(root: &Path) -> Vec<(String, PathBuf)> {
    let mut files = Vec::new();
    for dir in P_HARNESS_DIRS {
        rust_files(&root.join(dir), &mut files);
    }
    let mut claims = Vec::new();
    for path in files {
        // This file names the marker without claiming anything.
        if path.ends_with("spec_sync.rs") {
            continue;
        }
        for line in fs::read_to_string(&path).unwrap().lines() {
            if let Some(name) = line.trim().strip_prefix(P_CLAIM) {
                claims.push((name.trim().to_owned(), path.clone()));
            }
        }
    }
    claims
}

#[test]
fn feature_files_match_claude_md() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = spec_features(&fs::read_to_string(root.join("CLAUDE.md")).unwrap());
    let mut files = HashSet::new();

    for entry in fs::read_dir(root.join("tests/features")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "feature") {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
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
    let spec = p_scenarios(&fs::read_to_string(root.join("CLAUDE.md")).unwrap());

    let mut claimed = HashSet::new();
    for (name, path) in p_claims(root) {
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
fn p_tags_are_read_across_tag_lines_of_their_scenario_only() {
    let gherkin = "\
@P
Feature: F

  @P
  @E
  Scenario: P first
    Given x

  @E
  @P
  Scenario Outline: P second
    Given <x>

    @P
    Examples:
      | x |
      | 1 |

  @E
  Scenario: Not P
    Given x
";

    let names = p_scenarios_of(gherkin);

    assert_eq!(names, ["P first", "P second"]);
}
