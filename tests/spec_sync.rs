//! Every `.feature` file under `tests/features/` must be a verbatim copy of
//! the `gherkin` block in `CLAUDE.md` with the same `Feature:` name, and
//! every feature in `CLAUDE.md` must have its file unless it is listed in
//! [`NOT_YET_IMPLEMENTED`].
//! CLAUDE.md is canonical: on mismatch, fix the feature file or raise drift.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Features of `CLAUDE.md` whose blocks are not implemented yet, so have no
/// feature file. A block's feature leaves the list when its file is added.
const NOT_YET_IMPLEMENTED: &[&str] = &[
    "Subscription tokens, unsubscribe codes and resubscribe backoff",
    "Subscription recovery",
    "Server-side subscriptions",
    "Client commands",
];

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

fn spec_features(claude_md: &str) -> HashMap<String, String> {
    let mut features = HashMap::new();
    let mut rest = claude_md;
    while let Some(start) = rest.find("```gherkin\n") {
        let body = &rest[start + "```gherkin\n".len()..];
        let end = body
            .find("\n```")
            .expect("unterminated gherkin block in CLAUDE.md");
        let block = &body[..end];
        let name = feature_name(block).expect("gherkin block without a Feature: line");
        let previous = features.insert(name.to_owned(), normalize(block));
        assert!(
            previous.is_none(),
            "duplicate feature {name:?} in CLAUDE.md"
        );
        rest = &body[end..];
    }
    features
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
