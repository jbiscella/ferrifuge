//! Documentation in plain terms (Block 60, D3): the crate's doc text and
//! the README describe behaviour and cite no block, decision item, review
//! or `CLAUDE.md`. Those references stay in `//` comments beside the text.

use std::path::{Path, PathBuf};

/// What doc text must not cite: "Block N", "Blocks N", "Dn", "Codex" and
/// `CLAUDE.md`.
fn cites(text: &str) -> bool {
    let words: Vec<&str> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.'))
        .filter(|word| !word.is_empty())
        .collect();
    words.windows(2).any(|pair| {
        matches!(pair[0], "Block" | "Blocks") && pair[1].starts_with(|c: char| c.is_ascii_digit())
    }) || words.iter().any(|word| {
        let word = word.trim_end_matches('.');
        word == "Codex"
            || word == "CLAUDE.md"
            || (word.len() > 1
                && word.starts_with('D')
                && word[1..].chars().all(|c| c.is_ascii_digit()))
    })
}

/// The doc text of a Rust source, by line: `///`, `//!` and the string of
/// a `doc = "…"`, a `cfg_attr`'s included, outside `#[cfg(test)]` items.
fn doc_text(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_start();
        if line == "#[cfg(test)]" {
            // The item's own doc, above its attributes, is test code too.
            let mut top = i;
            while top > 0 && {
                let above = lines[top - 1].trim_start();
                above.starts_with("///") || above.starts_with("#[") || above.starts_with("//")
            } {
                top -= 1;
            }
            found.retain(|(number, _)| *number <= top);
            i = after_item(&lines, i + 1);
            continue;
        }
        if let Some(text) = line
            .strip_prefix("///")
            .or_else(|| line.strip_prefix("//!"))
        {
            found.push((i + 1, text.to_owned()));
        } else if let Some((_, rest)) = line.split_once("doc = \"") {
            found.push((i + 1, rest.trim_end_matches([',', ')', ']']).to_owned()));
        }
        i += 1;
    }
    found
}

/// The line after the item that starts at `start`, its attributes, doc
/// and body included: the item ends at its own line when that ends with
/// `;` or closes every brace it opens, else at the first later line of its
/// indentation starting with `}` (rustfmt's layout).
fn after_item(lines: &[&str], start: usize) -> usize {
    let indent = |line: &str| line.len() - line.trim_start().len();
    let Some(first) = (start..lines.len()).find(|&i| {
        let line = lines[i].trim_start();
        !(line.starts_with("#[") || line.starts_with("//") || line.is_empty())
    }) else {
        return lines.len();
    };
    let line = lines[first].trim_end();
    if line.ends_with(';')
        || (line.ends_with('}') && line.matches('{').count() == line.matches('}').count())
    {
        return first + 1;
    }
    let depth = indent(lines[first]);
    (first + 1..lines.len())
        .find(|&i| indent(lines[i]) == depth && lines[i].trim_start().starts_with('}'))
        .map_or(lines.len(), |i| i + 1)
}

/// Whether `path`, relative to the crate, is a test file: `*_tests.rs`,
/// `tests.rs`, or under a `tests` directory.
fn is_test_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.ends_with("_tests.rs")
        || name == "tests.rs"
        || path.components().any(|part| part.as_os_str() == "tests")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("a source directory") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// Every citation in `source`'s doc text, as `line: text`.
fn citations(source: &str) -> Vec<String> {
    doc_text(source)
        .into_iter()
        .filter(|(_, text)| cites(text))
        .map(|(number, text)| format!("{number}: {}", text.trim()))
        .collect()
}

/// Every README line citing a block, a decision item or a review. Its
/// links to `CLAUDE.md` stay.
fn readme_citations(readme: &str) -> Vec<String> {
    readme
        .lines()
        .enumerate()
        .filter(|(_, line)| cites(&line.replace("CLAUDE.md", "")))
        .map(|(number, line)| format!("{}: {}", number + 1, line.trim()))
        .collect()
}

#[test]
fn the_crate_documentation_cites_no_block() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_sources(&root.join("src"), &mut files);
    files.sort();
    let mut found = Vec::new();
    for path in files
        .iter()
        .filter(|path| !is_test_file(path.strip_prefix(root).expect("under the crate")))
    {
        let source = std::fs::read_to_string(path)
            .expect("a source file")
            .replace("\r\n", "\n");
        let relative = path.strip_prefix(root).expect("under the crate").display();
        found.extend(
            citations(&source)
                .into_iter()
                .map(|citation| format!("{relative}:{citation}")),
        );
    }
    assert!(found.is_empty(), "doc text citing a block: {found:#?}");
}

#[test]
fn the_readme_cites_no_block() {
    let readme = include_str!("../README.md").replace("\r\n", "\n");
    let found = readme_citations(&readme);
    assert!(found.is_empty(), "README lines citing a block: {found:#?}");
}

/// The checks report what they must, and only that.
#[test]
fn planted_citations_are_reported() {
    assert_eq!(
        citations("/// Retried later (Block 1).\nfn f() {}\n").len(),
        1
    );
    assert_eq!(citations("//! Delivery as D3 decides.\n").len(), 1);
    assert_eq!(
        citations("#[cfg_attr(docsrs, doc = \"Unstable (Block 2).\")]\npub fn f() {}\n").len(),
        1
    );
    assert_eq!(readme_citations("A note (Codex, #12).\n").len(), 1);

    assert_eq!(citations("// Block 4.\nfn f() {}\n"), Vec::<String>::new());
    assert_eq!(
        citations(
            "#[cfg(test)]\nmod tests {\n    //! Block 4's cases.\n    /// Block 4.\n    fn f() {}\n}\n/// Plain.\nfn g() {}\n"
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        readme_citations("See [CLAUDE.md](CLAUDE.md).\n"),
        Vec::<String>::new()
    );
    assert_eq!(
        citations("/// Plain text, 3D and 0xD1 included.\nfn f() {}\n"),
        Vec::<String>::new()
    );
    assert_eq!(
        citations("#[cfg(test)]\nfn f() {}\n/// Cited (Block 5).\nfn g() {\n}\n").len(),
        1
    );
    assert_eq!(
        citations("/// Tests of Block 4.\n#[cfg(test)]\nmod tests {\n    fn f() {}\n}\n"),
        Vec::<String>::new()
    );
}
