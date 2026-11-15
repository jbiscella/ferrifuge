//! Block 54, D2: the source keeps the 1.0 API able to grow. Clippy's
//! `exhaustive_enums` and `exhaustive_structs` check the types; this test
//! checks what they do not: the one `#[expect]`, the modules that allow
//! the lints, and that every variant with named fields is
//! `#[non_exhaustive]`, so that it can gain a field (54c).

use std::fs;
use std::path::{Path, PathBuf};

/// The modules outside semver, which allow the lints at module level: the
/// sans-IO core (`unstable-machine`, D1) and the `__test-hooks` module.
const ALLOWING: [&str; 2] = ["src/adapter/hooks.rs", "src/sans_io.rs"];

/// Source files whose enums are not public API: the core's submodules
/// (under `ALLOWING`'s `sans_io`) and the generated Protobuf code, which is
/// crate-private.
fn exempt(path: &str) -> bool {
    ALLOWING.contains(&path) || path.starts_with("src/sans_io/") || path.starts_with("src/protocol")
}

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
    let root = crate::source_root();
    let mut paths = Vec::new();
    sources(&root.join("src"), &mut paths);
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&root)
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
            let lines: Vec<&str> = text.lines().collect();
            // Each allow read up to its `)]`, so that one over several
            // lines is seen too.
            lines.iter().enumerate().any(|(index, line)| {
                line.trim_start().starts_with("#![allow(") && {
                    let end = lines[index..]
                        .iter()
                        .position(|line| line.contains(")]"))
                        .map_or(lines.len(), |end| index + end + 1);
                    lines[index..end]
                        .iter()
                        .any(|line| line.contains("clippy::exhaustive_"))
                }
            })
        })
        .map(|(path, _)| path)
        .collect();
    found.sort();
    assert_eq!(found, ALLOWING, "a module-level allow of exhaustive_*");
}

/// The variants with named fields of `enum_body`'s lines, each with
/// whether `#[non_exhaustive]` precedes it among its attributes.
fn named_field_variants<'a>(lines: &[&'a str]) -> Vec<(&'a str, bool)> {
    let mut variants = Vec::new();
    let mut depth = 0usize;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        let opens_named = depth == 0
            && trimmed.chars().next().is_some_and(char::is_uppercase)
            && trimmed.split_once(' ').is_some_and(|(name, rest)| {
                name.chars().all(|c| c.is_alphanumeric() || c == '_') && rest.starts_with('{')
            });
        if opens_named {
            let marked = lines[..index]
                .iter()
                .rev()
                .map(|line| line.trim())
                .take_while(|line| {
                    line.starts_with("#[") || line.starts_with("///") || line.starts_with("//")
                })
                .any(|line| line == "#[non_exhaustive]");
            variants.push((trimmed, marked));
        }
        depth += trimmed.matches('{').count();
        depth = depth.saturating_sub(trimmed.matches('}').count());
    }
    variants
}

#[test]
fn every_variant_with_named_fields_is_non_exhaustive() {
    let mut unmarked = Vec::new();
    for (path, text) in files() {
        if exempt(&path) {
            continue;
        }
        let lines: Vec<&str> = text.lines().collect();
        let mut index = 0;
        while index < lines.len() {
            let trimmed = lines[index].trim_start();
            if trimmed.starts_with("pub enum ") && trimmed.ends_with('{') {
                let name = trimmed["pub enum ".len()..].trim_end_matches(['{', ' ']);
                let indent = lines[index].len() - trimmed.len();
                let end = lines[index + 1..]
                    .iter()
                    .position(|line| {
                        line.len() > indent
                            && line[..indent].trim().is_empty()
                            && line[indent..].starts_with('}')
                    })
                    .map_or(lines.len(), |offset| index + 1 + offset);
                for (variant, marked) in named_field_variants(&lines[index + 1..end]) {
                    if !marked {
                        unmarked.push(format!("{path}: {name}::{variant}"));
                    }
                }
                index = end;
            }
            index += 1;
        }
    }
    assert!(
        unmarked.is_empty(),
        "variants with named fields not #[non_exhaustive]: {unmarked:#?}"
    );
}

#[test]
fn the_scan_finds_a_variant_without_the_attribute() {
    let marked = [
        "    /// A.",
        "    #[non_exhaustive]",
        "    A {",
        "        a: u32,",
        "    },",
    ];
    let unmarked = [
        "    /// B.",
        "    B {",
        "        b: u32,",
        "    },",
        "    C(u32),",
    ];
    assert_eq!(named_field_variants(&marked), [("A {", true)]);
    assert_eq!(named_field_variants(&unmarked), [("B {", false)]);
}

/// The source checks read the checkout only through `crate::source_root()`:
/// no other run-time use of the crate's directory, which a device lacks.
/// `include_str!` and `include_bytes!` read at compile time, and are fine.
#[test]
fn only_source_root_reads_the_crate_directory() {
    let name = concat!("CARGO_", "MANIFEST_DIR");
    let found: Vec<String> = files()
        .into_iter()
        .flat_map(|(path, text)| {
            text.lines()
                .enumerate()
                .filter(|(_, line)| {
                    line.contains(name)
                        && !line.contains("include_str!")
                        && !line.contains("include_bytes!")
                })
                .map(|(number, _)| format!("{path}:{}", number + 1))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        found.len() == 1 && found[0].starts_with("src/lib.rs:"),
        "a run-time read of the crate's directory outside source_root: {found:?}"
    );
}
