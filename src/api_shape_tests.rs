//! Block 54, D2: the source keeps the 1.0 API able to grow. Clippy's
//! `exhaustive_enums` and `exhaustive_structs` check the types; this test
//! checks what they do not: the one `#[expect]` and the modules that
//! allow the lints.

use std::fs;
use std::path::{Path, PathBuf};

/// The modules outside semver, which allow the lints at module level: the
/// sans-IO core (`unstable-machine`, D1) and the `__test-hooks` module.
const ALLOWING: [&str; 2] = ["src/adapter/hooks.rs", "src/machine.rs"];

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("a source directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every `.rs` file under `src/`, as a path relative to the crate root,
/// with its text.
fn files() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut paths = Vec::new();
    sources(&root.join("src"), &mut paths);
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(root)
                .expect("under the crate")
                .to_string_lossy()
                .replace('\\', "/");
            let text = fs::read_to_string(&path).expect("a readable source");
            (relative, text)
        })
        .collect()
}

#[test]
fn the_exhaustive_lints_are_expected_only_on_subscription_state() {
    let mut found = Vec::new();
    for (path, text) in files() {
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("#[expect(") {
                let attribute: String = lines[index..]
                    .iter()
                    .take_while(|line| !line.trim_start().starts_with("pub "))
                    .copied()
                    .collect();
                if attribute.contains("clippy::exhaustive_") {
                    let item = lines[index..]
                        .iter()
                        .find(|line| line.trim_start().starts_with("pub "))
                        .map_or("", |line| line.trim());
                    found.push(format!("{path}: {item}"));
                }
            }
        }
    }
    assert_eq!(
        found,
        ["src/subscription.rs: pub enum SubscriptionState {"],
        "an #[expect] of an exhaustive_* lint outside SubscriptionState"
    );
}

#[test]
fn only_the_unstable_modules_allow_the_exhaustive_lints() {
    let mut found: Vec<String> = files()
        .into_iter()
        .filter(|(_, text)| {
            text.lines().any(|line| {
                line.trim_start().starts_with("#![allow(") && line.contains("clippy::exhaustive_")
            })
        })
        .map(|(path, _)| path)
        .collect();
    found.sort();
    assert_eq!(found, ALLOWING, "a module-level allow of exhaustive_*");
}
