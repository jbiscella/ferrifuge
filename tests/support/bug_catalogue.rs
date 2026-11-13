//! Block 49: the bug-pattern catalogue of `CLAUDE.md`, read as its tables
//! (D1), and the checks of its structural acceptance. The table format is
//! part of the contract ("Exceptions introduced").

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;
use std::process::Command;

/// The numbers of centrifuge-rust `docs/spec.md` §12 at `cbeac19` that its
/// table lists (D9): numbers only, no text of the source.
pub(crate) const SECTION_12_TABLE: [u32; 77] = [
    1, 4, 5, 6, 7, 8, 9, 11, 13, 14, 15, 16, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
    80, 81, 82, 83,
];

/// The numbers §12 names only in its prose (D9).
pub(crate) const SECTION_12_PROSE: [u32; 6] = [2, 3, 10, 12, 17, 20];

/// The number §15 row 23 adds (D9).
pub(crate) const SECTION_15: [u32; 1] = [84];

/// Every number the numbered catalogue must hold, once each.
pub(crate) fn numbers() -> BTreeSet<u32> {
    SECTION_12_TABLE
        .into_iter()
        .chain(SECTION_12_PROSE)
        .chain(SECTION_15)
        .collect()
}

/// The statuses a row may have (D2).
pub(crate) const STATUSES: [&str; 4] = ["covered", "gap", "not applicable", "not reconstructible"];

/// What a probe's `Seen failing` says before its record is made (D7).
pub(crate) const PENDING: &str = "pending";

/// An own class's `Commits` cell when its class cell gives the commits.
const LISTED: &str = "listed";

const NUMBERED_HEADER: [&str; 6] = [
    "Id",
    "R",
    "Pattern (our wording)",
    "Src",
    "Status",
    "Where / reason",
];
const SDK_HEADER: [&str; 6] = [
    "Id",
    "R",
    "Pattern (our wording)",
    "Source",
    "Status",
    "Where / reason",
];
const OWN_HEADER: [&str; 5] = ["Id", "Class", "Commits", "Status", "Invariant / where"];
const PROBES_HEADER: [&str; 3] = ["Probe", "Mutation", "Seen failing"];
const FINDINGS_HEADER: [&str; 5] = [
    "Id",
    "Row",
    "Finding",
    "Proposed probe, not part of this block",
    "Mutation",
];

/// Which table a row is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Table {
    /// centrifuge-rust's numbered patterns.
    Numbered,
    /// The official SDKs and centrifuge-rust's CHANGELOG.
    Sdk,
    /// Our own fix commits.
    Own,
}

/// A row of one of the three catalogue tables.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) table: Table,
    pub(crate) id: String,
    pub(crate) status: String,
    /// "Where / reason", or "Invariant / where" for our own classes.
    pub(crate) place: String,
    /// The class and commit cells of our own classes, empty elsewhere.
    pub(crate) class: String,
    pub(crate) commits: String,
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "row {}", self.id)
    }
}

/// A row of "Probes and their mutations".
#[derive(Clone, Debug)]
pub(crate) struct Probe {
    pub(crate) name: String,
    pub(crate) mutation: String,
    pub(crate) seen_failing: String,
}

/// A row of "Findings for later pull requests".
#[derive(Clone, Debug)]
pub(crate) struct Finding {
    pub(crate) id: String,
    /// The `Row` cell: the id of the catalogue row the finding belongs to.
    pub(crate) row: String,
}

/// The catalogue's tables, and the names of every scenario of `CLAUDE.md`.
#[derive(Clone, Debug, Default)]
pub(crate) struct Catalogue {
    pub(crate) rows: Vec<Row>,
    pub(crate) probes: Vec<Probe>,
    pub(crate) findings: Vec<Finding>,
    pub(crate) scenarios: BTreeSet<String>,
    /// The scenarios of Blocks 50 and 51, which close some gaps (D2).
    pub(crate) closing: BTreeSet<String>,
}

/// The cells of a table row, or `None` for a line that is not one. Only
/// lines starting at column 0 count, so a Gherkin `Examples` table, which
/// is indented, is not taken for one.
fn cells(line: &str) -> Option<Vec<String>> {
    let line = line.trim_end();
    let inner = line.strip_prefix('|')?.strip_suffix('|')?;
    Some(
        inner
            .split('|')
            .map(|cell| cell.trim().to_owned())
            .collect(),
    )
}

fn is_delimiter(cells: &[String]) -> bool {
    cells
        .iter()
        .all(|cell| !cell.is_empty() && cell.chars().all(|c| c == '-' || c == ':'))
}

/// The tables of `text`, each as its header and its rows.
fn tables(text: &str) -> Vec<(Vec<String>, Vec<Vec<String>>)> {
    let mut tables = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(header) = cells(line) else {
            continue;
        };
        if !lines
            .peek()
            .and_then(|next| cells(next))
            .is_some_and(|next| is_delimiter(&next))
        {
            continue;
        }
        lines.next();
        let mut rows = Vec::new();
        while let Some(row) = lines.peek().and_then(|next| cells(next)) {
            lines.next();
            assert_eq!(
                row.len(),
                header.len(),
                "a row of the table headed {header:?} has {} cells: {row:?}",
                row.len()
            );
            rows.push(row);
        }
        tables.push((header, rows));
    }
    tables
}

/// Block `number`'s section of `CLAUDE.md`: from its heading to the next
/// level-2 heading.
fn block(claude_md: &str, number: u32) -> &str {
    let heading = format!("\n## Block {number} ");
    let start = claude_md
        .find(&heading)
        .unwrap_or_else(|| panic!("no Block {number} in CLAUDE.md"));
    let rest = &claude_md[start + 1..];
    let end = rest[1..].find("\n## ").map_or(rest.len(), |end| end + 1);
    &rest[..end]
}

/// The names of the scenarios in the `gherkin` blocks of `claude_md`.
pub(crate) fn scenario_names(claude_md: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut rest = claude_md;
    while let Some(start) = rest.find("```gherkin\n") {
        let body = &rest[start + "```gherkin\n".len()..];
        let end = body.find("\n```").expect("unterminated gherkin block");
        for line in body[..end].lines().map(str::trim) {
            if let Some(name) = [
                "Scenario Outline:",
                "Scenario Template:",
                "Scenario:",
                "Example:",
            ]
            .iter()
            .find_map(|keyword| line.strip_prefix(keyword))
            {
                names.insert(name.trim().to_owned());
            }
        }
        rest = &body[end..];
    }
    names
}

impl Catalogue {
    /// Reads the catalogue of `claude_md`. A table of Block 49 with an
    /// unknown header, or a row with the wrong number of cells, panics: the
    /// format is the contract.
    pub(crate) fn parse(claude_md: &str) -> Self {
        let mut catalogue = Catalogue {
            scenarios: scenario_names(claude_md),
            closing: [50, 51]
                .into_iter()
                .flat_map(|number| scenario_names(block(claude_md, number)))
                .collect(),
            ..Catalogue::default()
        };
        let (mut numbered, mut sdk, mut own, mut probes, mut findings) =
            (false, false, false, false, false);
        for (header, rows) in tables(block(claude_md, 49)) {
            let header: Vec<&str> = header.iter().map(String::as_str).collect();
            if header == NUMBERED_HEADER || header == SDK_HEADER {
                let table = if header == NUMBERED_HEADER {
                    numbered = true;
                    Table::Numbered
                } else {
                    sdk = true;
                    Table::Sdk
                };
                catalogue.rows.extend(rows.into_iter().map(|row| Row {
                    table,
                    id: row[0].clone(),
                    status: row[4].clone(),
                    place: row[5].clone(),
                    class: String::new(),
                    commits: String::new(),
                }));
            } else if header == OWN_HEADER {
                own = true;
                catalogue.rows.extend(rows.into_iter().map(|row| Row {
                    table: Table::Own,
                    id: row[0].clone(),
                    status: row[3].clone(),
                    place: row[4].clone(),
                    class: row[1].clone(),
                    commits: row[2].clone(),
                }));
            } else if header == PROBES_HEADER {
                probes = true;
                catalogue.probes.extend(rows.into_iter().map(|row| Probe {
                    name: row[0].clone(),
                    mutation: row[1].clone(),
                    seen_failing: row[2].clone(),
                }));
            } else if header == FINDINGS_HEADER {
                findings = true;
                catalogue
                    .findings
                    .extend(rows.into_iter().map(|row| Finding {
                        id: row[0].clone(),
                        row: row[1].clone(),
                    }));
            }
        }
        assert!(
            numbered && sdk && own && probes && findings,
            "Block 49 lacks a table: numbered {numbered}, SDKs {sdk}, own {own}, \
             probes {probes}, findings {findings}"
        );
        catalogue
    }

    /// Rows of the numbered table holding `number` in their `Id` cell.
    fn rows_of(&self, number: u32) -> Vec<&Row> {
        self.rows
            .iter()
            .filter(|row| row.table == Table::Numbered)
            .filter(|row| {
                row.id
                    .split(',')
                    .any(|id| id.trim().parse::<u32>() == Ok(number))
            })
            .collect()
    }

    /// Each number of `numbers` not in exactly one row, and each `Id` of the
    /// numbered table that is not a list of those numbers.
    pub(crate) fn numbers_not_in_one_row(&self, numbers: &BTreeSet<u32>) -> Vec<String> {
        let mut problems: Vec<String> = numbers
            .iter()
            .filter_map(|&number| {
                let rows = self.rows_of(number);
                (rows.len() != 1).then(|| {
                    let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
                    format!("{number} is in {} rows: {ids:?}", rows.len())
                })
            })
            .collect();
        for row in self.rows.iter().filter(|row| row.table == Table::Numbered) {
            for id in row.id.split(',').map(str::trim) {
                if !id
                    .parse::<u32>()
                    .is_ok_and(|number| numbers.contains(&number))
                {
                    problems.push(format!("{row} names {id:?}, not a catalogue number"));
                }
            }
        }
        problems
    }

    /// Rows whose status is not one of [`STATUSES`].
    pub(crate) fn unknown_statuses(&self) -> Vec<String> {
        self.rows
            .iter()
            .filter(|row| !STATUSES.contains(&row.status.as_str()))
            .map(|row| format!("{row} has status {:?}", row.status))
            .collect()
    }

    /// Every name in double quotes in a row's place cell, and every probe's
    /// name, that is not a scenario of `CLAUDE.md`. All rows, not only the
    /// covered and gap ones: the structural acceptance asks it of every
    /// scenario named.
    pub(crate) fn unknown_scenarios(&self) -> Vec<String> {
        let named = self.rows.iter().flat_map(|row| {
            quoted(&row.place)
                .into_iter()
                .map(move |name| (row.to_string(), name))
        });
        let probes = self
            .probes
            .iter()
            .map(|probe| ("the probes table".to_owned(), probe.name.clone()));
        named
            .chain(probes)
            .filter(|(_, name)| !self.scenarios.contains(name))
            .map(|(row, name)| format!("{row} names {name:?}, not a scenario of CLAUDE.md"))
            .collect()
    }

    /// Every `file::function` a row's place cell names, in backticks, whose
    /// file under `root` does not define that function.
    pub(crate) fn missing_functions(&self, root: &Path) -> Vec<String> {
        let mut problems = Vec::new();
        for row in &self.rows {
            for span in code_spans(&row.place).filter(|span| span.contains(".rs::")) {
                let Some((file, function)) = function_of(span) else {
                    problems.push(format!("{row}: `{span}` is not a function of src or tests"));
                    continue;
                };
                match std::fs::read_to_string(root.join(file)) {
                    Ok(source) if defines(&source, function) => {}
                    Ok(_) => problems.push(format!("{row}: {file} has no fn {function}")),
                    Err(e) => problems.push(format!("{row}: {file}: {e}")),
                }
            }
        }
        problems
    }

    /// `not applicable` and `not reconstructible` rows without a reason.
    pub(crate) fn rows_without_reason(&self) -> Vec<String> {
        self.rows
            .iter()
            .filter(|row| {
                matches!(
                    row.status.as_str(),
                    "not applicable" | "not reconstructible"
                )
            })
            .filter(|row| is_blank(&row.place))
            .map(|row| format!("{row} is {} without a reason", row.status))
            .collect()
    }

    /// `gap` rows that name none of: a probe of "Probes and their
    /// mutations", a scenario of Block 50 or 51 (both in double quotes), or
    /// a finding of "Findings for later pull requests" whose `Row` cell
    /// names that row (D2).
    pub(crate) fn gaps_without_probe(&self) -> Vec<String> {
        self.rows
            .iter()
            .filter(|row| row.status == "gap")
            .filter(|row| {
                let probe = quoted(&row.place).iter().any(|name| {
                    self.closing.contains(name)
                        || self.probes.iter().any(|probe| probe.name == *name)
                });
                let finding = words(&row.place).any(|word| {
                    self.findings
                        .iter()
                        .any(|finding| finding.id == word && finding.row == row.id)
                });
                !(probe || finding)
            })
            .map(|row| {
                format!(
                    "{row} is a gap naming no probe, no Block 50 or 51 scenario and no finding \
                     of its own"
                )
            })
            .collect()
    }

    /// The commits each of our own classes cites: the entries of its
    /// `Commits` cell, or, when that cell is `listed`, the commit ids its
    /// class cell gives.
    fn cited(row: &Row) -> Vec<&str> {
        if row.commits == LISTED {
            words(&row.class).filter(|word| is_commit(word)).collect()
        } else {
            row.commits.split(',').map(str::trim).collect()
        }
    }

    /// The commits our own classes cite.
    pub(crate) fn commits(&self) -> BTreeSet<String> {
        self.rows
            .iter()
            .filter(|row| row.table == Table::Own)
            .flat_map(Self::cited)
            .filter(|commit| is_commit_id(commit))
            .map(str::to_owned)
            .collect()
    }

    /// Each entry of an own class's `Commits` cell that is not a commit id,
    /// and each class citing fewer than two commits: a class is a row when
    /// fixed more than once (D6).
    pub(crate) fn malformed_commits(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for row in self.rows.iter().filter(|row| row.table == Table::Own) {
            let cited = Self::cited(row);
            for entry in cited.iter().filter(|entry| !is_commit_id(entry)) {
                problems.push(format!("{row} cites {entry:?}, not a commit id"));
            }
            if cited.len() < 2 {
                problems.push(format!(
                    "{row} cites {} commits: a class is fixed more than once (D6)",
                    cited.len()
                ));
            }
        }
        problems
    }

    /// Each commit of [`Catalogue::commits`] that is not an ancestor of
    /// `HEAD` in the repository at `root`.
    pub(crate) fn commits_not_ancestors(&self, root: &Path) -> Vec<String> {
        self.commits()
            .into_iter()
            .filter_map(|commit| {
                let status = Command::new("git")
                    .args(["merge-base", "--is-ancestor", &commit, "HEAD"])
                    .current_dir(root)
                    .output()
                    .expect("git runs");
                match status.status.code() {
                    Some(0) => None,
                    Some(1) => Some(format!("{commit} is not an ancestor of HEAD")),
                    _ => Some(format!(
                        "{commit} is not a commit of this clone (a shallow clone needs the \
                         full history): {}",
                        String::from_utf8_lossy(&status.stderr).trim()
                    )),
                }
            })
            .collect()
    }

    /// Probes with no mutation.
    pub(crate) fn probes_without_mutation(&self) -> Vec<String> {
        self.probes
            .iter()
            .filter(|probe| is_blank(&probe.mutation))
            .map(|probe| format!("probe {:?} names no mutation", probe.name))
            .collect()
    }

    /// Probes whose `Seen failing` is neither [`PENDING`] nor a pull request
    /// (`#` and its number).
    pub(crate) fn probes_without_record(&self) -> Vec<String> {
        self.probes
            .iter()
            .filter(|probe| probe.seen_failing != PENDING && !is_pull_request(&probe.seen_failing))
            .map(|probe| {
                format!(
                    "probe {:?} is seen failing in {:?}, neither a pull request nor {PENDING:?}",
                    probe.name, probe.seen_failing
                )
            })
            .collect()
    }
}

/// The texts between double quotes in `text`, in order.
pub(crate) fn quoted(text: &str) -> Vec<String> {
    text.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// The words of `text`: runs of letters, digits and underscores.
fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|word| !word.is_empty())
}

/// A commit id: 7 to 40 lowercase hexadecimal digits.
fn is_commit_id(entry: &str) -> bool {
    (7..=40).contains(&entry.len())
        && entry
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

/// A commit id in prose: one with a decimal digit, so an English word made
/// of the letters a to f is never taken for one.
fn is_commit(word: &str) -> bool {
    is_commit_id(word) && word.chars().any(|c| c.is_ascii_digit())
}

/// A cell holding nothing but spaces and dashes.
fn is_blank(cell: &str) -> bool {
    cell.chars()
        .all(|c| c.is_whitespace() || matches!(c, '-' | '–' | '—'))
}

fn is_pull_request(text: &str) -> bool {
    text.strip_prefix('#')
        .is_some_and(|number| !number.is_empty() && number.chars().all(|c| c.is_ascii_digit()))
}

/// The spans of `text` in backticks.
fn code_spans(text: &str) -> impl Iterator<Item = &str> {
    text.split('`').skip(1).step_by(2)
}

/// The file and function of a `path.rs::function` span, for a path under
/// `src` or `tests`.
fn function_of(span: &str) -> Option<(&str, &str)> {
    let (file, function) = span.rsplit_once("::")?;
    let path_like = (file.starts_with("src/") || file.starts_with("tests/"))
        && Path::new(file).extension().is_some_and(|ext| ext == "rs");
    let ident = !function.is_empty() && function.chars().all(|c| c.is_alphanumeric() || c == '_');
    (path_like && ident).then_some((file, function))
}

/// Whether `source` defines `fn function`.
fn defines(source: &str, function: &str) -> bool {
    let needle = format!("fn {function}");
    source.match_indices(&needle).any(|(at, _)| {
        let after = source[at + needle.len()..].chars().next();
        let before = source[..at].chars().next_back();
        matches!(after, Some('(' | '<'))
            && before.is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
    })
}
