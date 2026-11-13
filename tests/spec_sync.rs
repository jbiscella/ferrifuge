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

#[allow(dead_code)]
#[path = "support/bug_catalogue.rs"]
mod bug_catalogue;

/// Features of `CLAUDE.md` whose blocks are not implemented yet, so have no
/// feature file. A block's feature leaves the list when its file is added.
const NOT_YET_IMPLEMENTED: &[&str] = &["Byte-split matrix", "Real-server chaos", "Soak run"];

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
const W_NOT_YET_WRITTEN: &[&str] = &[];

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

/// Block 48, D4: wtransport's `ClientConfig::default` reads the platform's
/// roots as `with_native_certs` does, but a trait method is no path the
/// root `clippy.toml` can refuse, so the sources are checked instead; and
/// wtransport's `dangerous-configuration` feature is never enabled.
#[test]
fn wtransport_configures_nothing_of_its_own() {
    let mut files = Vec::new();
    rust_files(Path::new("src"), &mut files);
    for file in files {
        let text = read(&file);
        for banned in [
            "ClientConfig::default",
            "wtransport::ClientConfig as Default",
        ] {
            assert!(
                !text.contains(banned),
                "{} names {banned} (Block 48, D4)",
                file.display()
            );
        }
    }
    let manifest = read(Path::new("Cargo.toml"));
    assert!(
        !manifest.contains("dangerous-configuration"),
        "Cargo.toml enables wtransport's dangerous-configuration (Block 48, D4)"
    );
}

/// Block 49a: the bug-pattern catalogue as `CLAUDE.md` has it.
fn catalogue_text() -> String {
    read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("CLAUDE.md"))
}

/// The catalogue of `CLAUDE.md` with its one occurrence of `from` replaced
/// by `to`: a planted fault.
fn planted(from: &str, to: &str) -> bug_catalogue::Catalogue {
    let text = catalogue_text();
    assert_eq!(text.matches(from).count(), 1, "{from:?} is not unique");
    bug_catalogue::Catalogue::parse(&text.replacen(from, to, 1))
}

/// Fails unless one of `problems` contains `expected`.
fn reports(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "{expected:?} not reported: {problems:?}"
    );
}

/// The probes table's row of the probe planted in.
const PROBE_ROW: &str = "| Command ids skip 0 when they wrap | `.max(1)` removed | pending |";

/// Each check of the catalogue finds nothing on `CLAUDE.md` as it is. The
/// cucumber scenarios run the same checks; the tests below plant a fault
/// for each and see it reported.
#[test]
fn the_catalogue_checks_pass_on_claude_md() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let catalogue = bug_catalogue::Catalogue::parse(&catalogue_text());
    let none = Vec::<String>::new();
    assert_eq!(
        catalogue.numbers_not_in_one_row(&bug_catalogue::numbers()),
        none
    );
    assert_eq!(catalogue.unknown_statuses(), none);
    assert_eq!(catalogue.unknown_scenarios(), none);
    assert_eq!(catalogue.missing_functions(root), none);
    assert_eq!(catalogue.rows_without_reason(), none);
    assert_eq!(catalogue.gaps_without_probe(), none);
    assert_eq!(catalogue.probes_without_mutation(), none);
    assert_eq!(catalogue.probes_without_record(), none);
    assert_eq!(catalogue.malformed_commits(), none);
}

#[test]
fn the_catalogue_reports_a_number_not_in_one_row() {
    let numbers = bug_catalogue::numbers();
    let moved = planted(
        "| 84 | R | A disconnect push during connecting",
        "| 85 | R | A disconnect push during connecting",
    )
    .numbers_not_in_one_row(&numbers);
    reports(&moved, "84 is in 0 rows");
    reports(&moved, "\"85\", not a catalogue number");
    let twice = planted(
        "| 8 | R | A call made while",
        "| 8, 9 | R | A call made while",
    )
    .numbers_not_in_one_row(&numbers);
    reports(&twice, "9 is in 2 rows");
}

#[test]
fn the_catalogue_reports_an_unknown_status() {
    let unknown = planted(
        "| §5.8 | covered | \"Every transport unreachable\"",
        "| §5.8 | done | \"Every transport unreachable\"",
    );
    reports(&unknown.unknown_statuses(), "row 4 has status \"done\"");
}

#[test]
fn the_catalogue_reports_a_scenario_that_does_not_exist() {
    let in_a_row = planted(
        "\"Every transport unreachable\"",
        "\"Every transport unreachable now\"",
    );
    reports(
        &in_a_row.unknown_scenarios(),
        "\"Every transport unreachable now\", not a scenario",
    );
    let in_the_probes = planted(
        PROBE_ROW,
        "| Command ids skip 1 when they wrap | `.max(1)` removed | pending |",
    );
    reports(
        &in_the_probes.unknown_scenarios(),
        "\"Command ids skip 1 when they wrap\", not a scenario",
    );
}

#[test]
fn the_catalogue_reports_a_function_that_does_not_exist() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cited = "`src/machine.rs::token_fetched_after_disconnect_is_ignored`";
    let renamed = planted(
        cited,
        "`src/machine.rs::token_fetched_after_disconnect_is_ignored_now`",
    );
    reports(
        &renamed.missing_functions(root),
        "src/machine.rs has no fn token_fetched_after_disconnect_is_ignored_now",
    );
    let moved = planted(
        cited,
        "`src/machine_now.rs::token_fetched_after_disconnect_is_ignored`",
    );
    reports(&moved.missing_functions(root), "src/machine_now.rs:");
    let outside = planted(
        cited,
        "`machine.rs::token_fetched_after_disconnect_is_ignored`",
    );
    reports(
        &outside.missing_functions(root),
        "is not a function of src or tests",
    );
}

#[test]
fn the_catalogue_reports_a_row_without_its_reason() {
    let reason = "| not applicable | callbacks return a typed result; there is no shared error type to confuse |";
    let without = planted(reason, "| not applicable |  |");
    reports(
        &without.rows_without_reason(),
        "row 54 is not applicable without a reason",
    );
}

#[test]
fn the_catalogue_reports_a_gap_without_a_probe_or_a_listed_finding() {
    let s2 = "probe \"Command ids skip 0 when they wrap\"";
    // An implemented scenario, even after the word "scenario", is no probe.
    let implemented = planted(
        s2,
        "scenario \"A message over the limit ends a connected client\"",
    );
    reports(&implemented.gaps_without_probe(), "row S2 is a gap");
    // A finding of another row does not close this one.
    let another_rows = planted(s2, "finding F1");
    reports(&another_rows.gaps_without_probe(), "row S2 is a gap");
    let unlisted = planted("| gap | finding F1:", "| gap | finding F2:");
    reports(&unlisted.gaps_without_probe(), "row 63 is a gap");
}

#[test]
fn the_catalogue_reports_a_malformed_or_single_citation() {
    let not_hex = planted("| d86ddc9, 6d0addd,", "| d86ddcg, 6d0addd,");
    reports(
        &not_hex.malformed_commits(),
        "row C1 cites \"d86ddcg\", not a commit id",
    );
    let too_short = planted("| d86ddc9, 6d0addd,", "| d86ddc, 6d0addd,");
    reports(
        &too_short.malformed_commits(),
        "row C1 cites \"d86ddc\", not a commit id",
    );
    let single = planted("| 9a6354e, b509a5f, d2742bf |", "| 9a6354e |");
    reports(&single.malformed_commits(), "row C12 cites 1 commits");
}

#[test]
fn the_catalogue_reports_a_commit_that_is_not_an_ancestor() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let unknown = planted("| d86ddc9, 6d0addd,", "| 0123abc, 6d0addd,");
    assert!(unknown.commits().contains("0123abc"));
    reports(&unknown.commits_not_ancestors(root), "0123abc");
}

#[test]
fn the_catalogue_reports_a_probe_without_its_record() {
    for blank in ["", "–", "-"] {
        let no_mutation = planted(
            PROBE_ROW,
            &format!("| Command ids skip 0 when they wrap | {blank} | pending |"),
        );
        reports(
            &no_mutation.probes_without_mutation(),
            "\"Command ids skip 0 when they wrap\" names no mutation",
        );
    }
    let nowhere = planted(
        PROBE_ROW,
        "| Command ids skip 0 when they wrap | `.max(1)` removed | soon |",
    );
    reports(&nowhere.probes_without_record(), "seen failing in \"soon\"");
    let recorded = planted(
        PROBE_ROW,
        "| Command ids skip 0 when they wrap | `.max(1)` removed | #250 |",
    );
    assert_eq!(recorded.probes_without_record(), Vec::<String>::new());
}
