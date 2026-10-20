//! Every `.feature` file under `tests/features/` must be a verbatim copy of
//! the `gherkin` block in `CLAUDE.md` with the same `Feature:` name.
//! CLAUDE.md is canonical: on mismatch, fix the feature file or raise drift.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

fn normalize(text: &str) -> String {
    text.lines().map(str::trim_end).collect::<Vec<_>>().join("\n").trim().to_owned()
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
        let end = body.find("\n```").expect("unterminated gherkin block in CLAUDE.md");
        let block = &body[..end];
        let name = feature_name(block).expect("gherkin block without a Feature: line");
        let previous = features.insert(name.to_owned(), normalize(block));
        assert!(previous.is_none(), "duplicate feature {name:?} in CLAUDE.md");
        rest = &body[end..];
    }
    features
}

#[test]
fn feature_files_match_claude_md() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = spec_features(&fs::read_to_string(root.join("CLAUDE.md")).unwrap());

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
    }
}
