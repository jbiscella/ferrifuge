//! Steps of Block 34: the table of codes, checked against `src/codes.rs`
//! and the README.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cucumber::{given, then};

use super::ClientWorld;

/// The code constants, by `kind::NAME`, with their values, and the table
/// rows of the documentation and of the README.
#[derive(Debug, Default)]
pub(crate) struct CodesTable {
    constants: BTreeMap<String, u32>,
    /// Each table's rows, cell by cell, its Constant cells normalised to
    /// `kind::NAME`, in order.
    doc_tables: Vec<Vec<Vec<String>>>,
    readme_tables: Vec<Vec<Vec<String>>>,
    /// Every row's reason cell, from the documentation.
    reasons: Vec<String>,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The constants of `src/codes.rs`: `pub const NAME: u32 = N;` inside
/// `pub mod kind {`; any other `pub const` or `pub static` fails.
fn constants(source: &str) -> BTreeMap<String, u32> {
    let mut constants = BTreeMap::new();
    let mut kind: Option<String> = None;
    for line in source.lines() {
        let line = line.trim();
        if let Some(name) = line
            .strip_prefix("pub mod ")
            .and_then(|rest| rest.strip_suffix(" {"))
        {
            kind = Some(name.to_owned());
            continue;
        }
        if line == "}" {
            kind = None;
            continue;
        }
        if !(line.starts_with("pub const") || line.starts_with("pub static")) {
            continue;
        }
        let kind = kind
            .as_ref()
            .unwrap_or_else(|| panic!("a constant outside a kind: {line}"));
        let parsed = line
            .strip_prefix("pub const ")
            .and_then(|rest| rest.strip_suffix(';'))
            .and_then(|rest| rest.split_once(": u32 = "))
            .and_then(|(name, value)| Some((name, value.parse::<u32>().ok()?)));
        let Some((name, value)) = parsed else {
            panic!("not a code constant: {line}");
        };
        let previous = constants.insert(format!("{kind}::{name}"), value);
        assert!(previous.is_none(), "{kind}::{name} twice");
    }
    constants
}

/// The cells of a table row, or `None` for a line that is not one (or a
/// delimiter row).
fn cells(line: &str) -> Option<Vec<String>> {
    let line = line.trim();
    if !line.starts_with('|') || line.starts_with("|---") {
        return None;
    }
    Some(
        line.trim_matches('|')
            .split(" | ")
            .map(|cell| cell.trim().to_owned())
            .collect(),
    )
}

/// The tables of `lines`, a table being the rows between blank lines,
/// their Constant cells normalised by `normalise`.
fn tables<'a>(
    lines: impl Iterator<Item = &'a str>,
    normalise: impl Fn(&str) -> Option<String>,
) -> Vec<Vec<Vec<String>>> {
    let mut tables = Vec::new();
    let mut table: Vec<Vec<String>> = Vec::new();
    for line in lines {
        match cells(line) {
            Some(mut row) => {
                if row.len() == 5
                    && let Some(constant) = normalise(&row[1])
                {
                    row[1] = constant;
                }
                table.push(row);
            }
            None if !line.trim().starts_with("|---") && !table.is_empty() => {
                tables.push(std::mem::take(&mut table));
            }
            None => {}
        }
    }
    if !table.is_empty() {
        tables.push(table);
    }
    tables
}

/// ``[`kind::NAME`]``, the documentation's form.
fn doc_constant(cell: &str) -> Option<String> {
    cell.strip_prefix("[`")?
        .strip_suffix("`]")
        .map(str::to_owned)
}

/// `` `codes::kind::NAME` ``, the README's form.
fn readme_constant(cell: &str) -> Option<String> {
    cell.strip_prefix("`codes::")?
        .strip_suffix('`')
        .map(str::to_owned)
}

/// The code rows of `tables`: those whose Constant cell names a constant
/// (`kind::NAME`, upper case).
fn code_rows(tables: &[Vec<Vec<String>>]) -> Vec<(usize, &Vec<String>)> {
    tables
        .iter()
        .enumerate()
        .flat_map(|(index, table)| table.iter().map(move |row| (index, row)))
        .filter(|(_, row)| {
            row.len() == 5
                && row[1].split_once("::").is_some_and(|(kind, name)| {
                    kind.chars().all(|c| c.is_ascii_lowercase())
                        && !name.is_empty()
                        && name
                            .chars()
                            .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                })
        })
        .collect()
}

#[given(regex = r"^the crate's codes module(?: and its README)?$")]
fn the_codes_module(world: &mut ClientWorld) {
    let source = read(&root().join("src/codes.rs"));
    let doc_lines = source
        .lines()
        .filter_map(|line| line.strip_prefix("//!").map(str::trim_start));
    let readme = read(&root().join("README.md"));
    let section = readme
        .split_once("### Codes\n")
        .map(|(_, rest)| {
            rest.split_once("\n### ")
                .map_or(rest, |(section, _)| section)
        })
        .expect("a README Codes section");
    let doc_tables = tables(doc_lines, doc_constant);
    world.codes_table = CodesTable {
        constants: constants(&source),
        reasons: code_rows(&doc_tables)
            .iter()
            .map(|(_, row)| row[2].clone())
            .collect(),
        doc_tables,
        readme_tables: tables(section.lines(), readme_constant),
    };
}

#[then("the codes documentation has exactly one row for each code constant, giving its value")]
fn one_row_per_constant(world: &mut ClientWorld) {
    let table = &world.codes_table;
    for (constant, value) in &table.constants {
        let rows: Vec<_> = code_rows(&table.doc_tables)
            .into_iter()
            .filter(|(_, row)| &row[1] == constant)
            .collect();
        assert_eq!(rows.len(), 1, "rows for {constant}: {rows:?}");
        assert_eq!(rows[0].1[0], value.to_string(), "the value of {constant}");
    }
}

#[then(
    "every constant a row of the codes documentation or the README names is a code constant of the crate"
)]
fn no_unknown_constant(world: &mut ClientWorld) {
    let table = &world.codes_table;
    for tables in [&table.doc_tables, &table.readme_tables] {
        for (_, row) in code_rows(tables) {
            assert!(
                table.constants.contains_key(&row[1]),
                "unknown constant: {row:?}"
            );
        }
    }
    // A Constant cell naming nothing (e.g. a link with a typo) would not
    // count as a code row: every 5-cell row other than headers must be one.
    for tables in [&table.doc_tables, &table.readme_tables] {
        for row in tables.iter().flatten() {
            if row.len() == 5 && row[0] != "Code" {
                assert!(
                    code_rows(std::slice::from_ref(&vec![row.clone()])).len() == 1,
                    "a row naming no constant: {row:?}"
                );
            }
        }
    }
}

#[then("the rows of each table name constants of one kind")]
fn one_kind_per_table(world: &mut ClientWorld) {
    let table = &world.codes_table;
    for tables in [&table.doc_tables, &table.readme_tables] {
        let mut kinds: BTreeMap<usize, String> = BTreeMap::new();
        for (index, row) in code_rows(tables) {
            let kind = row[1].split_once("::").expect("kind::NAME").0.to_owned();
            let first = kinds.entry(index).or_insert_with(|| kind.clone());
            assert_eq!(*first, kind, "table {index} mixes kinds: {row:?}");
        }
    }
}

#[then(
    "the README's rows are the rows of the codes documentation, in the same order and cell for cell"
)]
fn readme_equals_doc(world: &mut ClientWorld) {
    let table = &world.codes_table;
    let doc: Vec<_> = table.doc_tables.iter().flatten().collect();
    let readme: Vec<_> = table.readme_tables.iter().flatten().collect();
    assert_eq!(readme.len(), doc.len(), "row counts differ");
    for (readme_row, doc_row) in readme.iter().zip(&doc) {
        assert_eq!(readme_row, doc_row);
    }
}

/// The `.rs` sources under `src/` but `codes.rs`, joined.
fn sources() -> String {
    fn walk(dir: &Path, out: &mut String) {
        for entry in std::fs::read_dir(dir).expect("src") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") && !path.ends_with("codes.rs")
            {
                out.push_str(&read(&path));
            }
        }
    }
    let mut out = String::new();
    walk(&root().join("src"), &mut out);
    out
}

#[then(
    "every reason a row quotes occurs as a string literal in the crate's source outside the codes module"
)]
fn reasons_exist(world: &mut ClientWorld) {
    let sources = sources();
    for reason in &world.codes_table.reasons {
        let Some(quoted) = reason
            .strip_prefix("`\"")
            .and_then(|rest| rest.strip_suffix("\"`"))
        else {
            continue;
        };
        assert!(
            sources.contains(&format!("\"{quoted}\"")),
            "no string literal {quoted:?} in src/"
        );
    }
}
