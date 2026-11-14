//! Block 59 (D4, D6): the guide under `docs/` cites only what exists, states
//! the codes and defaults the crate has, keeps its diagrams balanced, and
//! links only to files and headings that exist; `README.md` links every file
//! of the guide.
//!
//! Every check runs on the texts as read; the planted tests below change one
//! citation or value in them and expect its report, as `spec_sync`'s
//! catalogue tests do.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use ferrifuge::Config;
use ferrifuge::machine::SubscriptionOptions;

#[allow(dead_code)]
#[path = "support/bug_catalogue.rs"]
mod bug_catalogue;

/// The lines starting a list of sources (D4).
const SOURCES: &str = "Sources:";

/// The kinds of client-side codes, as `src/codes.rs` names its tables.
const KINDS: [&str; 5] = [
    "connecting",
    "disconnected",
    "subscribing",
    "unsubscribed",
    "error",
];

/// The leads of the "Server codes" table, backticks removed, longest first
/// where one starts another's text.
const LEADS: [&str; 7] = [
    "sub_refresh error",
    "subscribe error",
    "connect error",
    "refresh error",
    "websocket close",
    "unsubscribe",
    "disconnect",
];

/// The Mermaid statements a matching `end` closes.
const OPENERS: [&str; 6] = ["alt", "opt", "loop", "par", "rect", "critical"];

/// The texts the checks read.
#[derive(Clone)]
struct Texts {
    root: PathBuf,
    claude_md: String,
    codes_rs: String,
    readme: String,
    /// Each file under `docs/`, by its path from the root.
    docs: BTreeMap<String, String>,
}

/// A text file, with Windows line endings read as Unix ones.
fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .replace("\r\n", "\n")
}

impl Texts {
    fn load() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut docs = BTreeMap::new();
        for entry in fs::read_dir(root.join("docs")).expect("docs/") {
            let path = entry.expect("docs/ entry").path();
            if path.is_file() {
                let name = path.file_name().expect("file name").to_string_lossy();
                docs.insert(format!("docs/{name}"), read(&path));
            }
        }
        Texts {
            claude_md: read(&root.join("CLAUDE.md")),
            codes_rs: read(&root.join("src/codes.rs")),
            readme: read(&root.join("README.md")),
            docs,
            root,
        }
    }

    /// The texts with the one occurrence of `from` in `file` replaced by
    /// `to`: a planted fault.
    fn planted(&self, file: &str, from: &str, to: &str) -> Self {
        let mut texts = self.clone();
        let text = if file == "README.md" {
            &mut texts.readme
        } else {
            texts.docs.get_mut(file).expect("a file under docs/")
        };
        assert_eq!(
            text.matches(from).count(),
            1,
            "{from:?} is not unique in {file}"
        );
        *text = text.replacen(from, to, 1);
        texts
    }

    /// The text of a repository file, the read ones as read.
    fn text_of(&self, path: &str) -> Option<String> {
        match path {
            "README.md" => Some(self.readme.clone()),
            "CLAUDE.md" => Some(self.claude_md.clone()),
            _ => self
                .docs
                .get(path)
                .cloned()
                .or_else(|| fs::read_to_string(self.root.join(path)).ok()),
        }
    }

    fn problems(&self) -> Vec<String> {
        let codes = CodeTables::parse(&self.codes_rs);
        let defaults = defaults();
        let mut problems = Vec::new();
        for (file, text) in &self.docs {
            let lines = scan(text);
            self.blocks(file, &lines, &mut problems);
            self.sources(file, &lines, &mut problems);
            sections(file, &lines, &mut problems);
            diagrams(file, &lines, &mut problems);
            codes.check(file, &lines, &mut problems);
            check_defaults(file, &lines, &defaults, &mut problems);
            self.links(file, text, &mut problems);
        }
        self.links("README.md", &self.readme, &mut problems);
        let linked: BTreeSet<String> = links(&scan(&self.readme))
            .into_iter()
            .filter_map(|(_, target)| resolve("README.md", &target).map(|(path, _)| path))
            .collect();
        for file in self.docs.keys() {
            if !linked.contains(file) {
                problems.push(format!("{file} is not linked from README.md"));
            }
        }
        problems
    }

    fn has_block(&self, number: u32) -> bool {
        self.claude_md.contains(&format!("\n## Block {number} — "))
    }

    /// Every `Block N`, `Blocks N, M and K`, `Blocks N–M` and `Block N D<k>`.
    fn blocks(&self, file: &str, lines: &[Line], problems: &mut Vec<String>) {
        for line in lines {
            for (number, item) in block_mentions(line.text) {
                if !self.has_block(number) {
                    problems.push(format!(
                        "{file}:{}: Block {number} has no heading in CLAUDE.md",
                        line.number
                    ));
                } else if let Some(item) = item {
                    let block = bug_catalogue::block(&self.claude_md, number);
                    if !block.contains(&format!("**D{item} ")) {
                        problems.push(format!(
                            "{file}:{}: Block {number} has no D{item}",
                            line.number
                        ));
                    }
                }
            }
        }
    }

    /// The items of every `Sources:` line.
    fn sources(&self, file: &str, lines: &[Line], problems: &mut Vec<String>) {
        for line in lines.iter().filter(|line| line.is_sources()) {
            for item in source_items(line.text) {
                let at = format!("{file}:{}", line.number);
                if item.starts_with("Block ") {
                    let Some(&(number, _)) = block_mentions(item).first() else {
                        problems.push(format!("{at}: {item:?} names no block"));
                        continue;
                    };
                    if !self.has_block(number) {
                        continue; // reported by `blocks`
                    }
                    let block = bug_catalogue::block(&self.claude_md, number);
                    let mut names = bug_catalogue::scenario_names(block);
                    names.extend(labels(block));
                    for name in bug_catalogue::quoted(item) {
                        if !names.contains(&name) {
                            problems.push(format!(
                                "{at}: {name:?} is not a scenario or a decision of Block {number}"
                            ));
                        }
                    }
                } else if item.starts_with('`') {
                    let span = bug_catalogue::code_spans(item).next().unwrap_or_default();
                    let Some((path, function)) = bug_catalogue::function_of(span) else {
                        problems.push(format!("{at}: `{span}` is not a function of src or tests"));
                        continue;
                    };
                    match fs::read_to_string(self.root.join(path)) {
                        Ok(source) if bug_catalogue::defines(&source, function) => {}
                        Ok(_) => problems.push(format!("{at}: {path} has no fn {function}")),
                        Err(e) => problems.push(format!("{at}: {path}: {e}")),
                    }
                } else if !(item.starts_with('[') && item.contains("](") && item.contains('#')) {
                    problems.push(format!("{at}: {item:?} is not a source"));
                }
            }
        }
    }

    /// Every relative link of `text` names a file and, with a fragment, one
    /// of its headings.
    fn links(&self, file: &str, text: &str, problems: &mut Vec<String>) {
        for (number, target) in links(&scan(text)) {
            let Some((path, fragment)) = resolve(file, &target) else {
                continue; // not relative
            };
            let at = format!("{file}:{number}");
            if !self.root.join(&path).exists() && self.text_of(&path).is_none() {
                problems.push(format!("{at}: link to {target}: no file {path}"));
                continue;
            }
            if let Some(fragment) = fragment {
                let headings = self.text_of(&path).map(|text| anchors(&text));
                if !headings.is_some_and(|headings| headings.contains(&fragment)) {
                    problems.push(format!(
                        "{at}: link to {target}: no heading #{fragment} in {path}"
                    ));
                }
            }
        }
    }
}

/// Where a line is: in prose, in a `mermaid` block, in another fenced block,
/// or a fence itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Place {
    Prose,
    Mermaid,
    Code,
    Fence,
}

struct Line<'a> {
    number: usize,
    text: &'a str,
    place: Place,
}

impl Line<'_> {
    fn is_sources(&self) -> bool {
        self.place == Place::Prose && self.text.trim_start().starts_with(SOURCES)
    }

    /// The level of a heading in prose, if it is one.
    fn heading(&self) -> Option<(usize, &str)> {
        if self.place != Place::Prose {
            return None;
        }
        let level = self.text.chars().take_while(|c| *c == '#').count();
        let rest = self.text.get(level..)?;
        ((1..=6).contains(&level) && rest.starts_with(' '))
            .then(|| (level, rest.trim().trim_end_matches('#').trim()))
    }
}

/// The lines of `text`, each with its place.
fn scan(text: &str) -> Vec<Line<'_>> {
    let mut place = Place::Prose;
    text.lines()
        .enumerate()
        .map(|(index, text)| {
            let fence = text.trim_start().starts_with("```");
            let this = if fence {
                place = match place {
                    Place::Prose if text.trim() == "```mermaid" => Place::Mermaid,
                    Place::Prose => Place::Code,
                    _ => Place::Prose,
                };
                Place::Fence
            } else {
                place
            };
            Line {
                number: index + 1,
                text,
                place: this,
            }
        })
        .collect()
}

/// The non-empty items of a `Sources:` line.
fn source_items(line: &str) -> Vec<&str> {
    line.trim_start()
        .strip_prefix(SOURCES)
        .unwrap_or_default()
        .split(';')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .collect()
}

/// The labels of the bold decisions of `block`: `**<label>.**` and
/// `**<label>:**`, and `**D<k> <title>.**` as a whole.
fn labels(block: &str) -> BTreeSet<String> {
    block
        .split("**")
        .skip(1)
        .step_by(2)
        .filter_map(|bold| bold.strip_suffix('.').or_else(|| bold.strip_suffix(':')))
        .map(str::to_owned)
        .collect()
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether `text[at..]` starts a word.
fn starts_word(text: &str, at: usize) -> bool {
    text[..at].chars().next_back().is_none_or(|c| !is_word(c))
}

/// Whether `text[at..]` ends a word.
fn ends_word(text: &str, at: usize) -> bool {
    text[at..].chars().next().is_none_or(|c| !is_word(c))
}

/// The number at the start of `text`, and the rest.
fn number(text: &str) -> Option<(u32, &str)> {
    let digits = text.chars().take_while(char::is_ascii_digit).count();
    let value = text.get(..digits)?.parse().ok()?;
    Some((value, &text[digits..]))
}

/// The block numbers `text` names, each with the D-item that follows it.
fn block_mentions(text: &str) -> Vec<(u32, Option<u32>)> {
    let mut found = Vec::new();
    for (at, _) in text.match_indices("Block") {
        if !starts_word(text, at) {
            continue;
        }
        let rest = &text[at + "Block".len()..];
        let (plural, rest) = match rest.strip_prefix("s ") {
            Some(rest) => (true, rest),
            None => (false, rest.strip_prefix(' ').unwrap_or("x")),
        };
        let mut rest = rest;
        while let Some((first, after)) = number(rest) {
            rest = after;
            if let Some((last, after)) = ["–", "-"]
                .iter()
                .find_map(|dash| rest.strip_prefix(dash))
                .and_then(number)
            {
                found.extend((first..=last).map(|n| (n, None)));
                rest = after;
            } else if let Some((item, after)) = rest
                .strip_prefix(" D")
                .and_then(number)
                .filter(|(_, after)| ends_word(after, 0))
            {
                found.push((first, Some(item)));
                rest = after;
            } else if ends_word(rest, 0) {
                found.push((first, None));
            } else {
                break;
            }
            if !plural {
                break;
            }
            match [", and ", ", ", " and "]
                .iter()
                .find_map(|separator| rest.strip_prefix(separator))
            {
                Some(after) if after.starts_with(|c: char| c.is_ascii_digit()) => rest = after,
                _ => break,
            }
        }
    }
    found
}

/// Every `##` and `###` section, up to the next heading, has a `Sources:`
/// line with an item.
fn sections(file: &str, lines: &[Line], problems: &mut Vec<String>) {
    let headings: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].heading().is_some())
        .collect();
    for (k, &start) in headings.iter().enumerate() {
        let Some((level, title)) = lines[start].heading() else {
            continue;
        };
        if !(2..=3).contains(&level) {
            continue;
        }
        let end = headings.get(k + 1).copied().unwrap_or(lines.len());
        if !lines[start..end]
            .iter()
            .any(|line| line.is_sources() && !source_items(line.text).is_empty())
        {
            problems.push(format!(
                "{file}:{}: section {title:?} has no Sources line",
                lines[start].number
            ));
        }
    }
}

/// Every `mermaid` block starts with `sequenceDiagram`, balances its blocks
/// with its `end` lines, and is followed by a `Sources:` line.
fn diagrams(file: &str, lines: &[Line], problems: &mut Vec<String>) {
    let mut i = 0;
    while i < lines.len() {
        if !(lines[i].place == Place::Fence && lines[i].text.trim() == "```mermaid") {
            i += 1;
            continue;
        }
        let start = lines[i].number;
        let mut body = Vec::new();
        i += 1;
        while i < lines.len() && lines[i].place == Place::Mermaid {
            body.push(lines[i].text.trim());
            i += 1;
        }
        if body.iter().find(|line| !line.is_empty()) != Some(&"sequenceDiagram") {
            problems.push(format!(
                "{file}:{start}: diagram does not start with sequenceDiagram"
            ));
        }
        let mut depth = 0i32;
        for line in &body {
            let word = line.split_whitespace().next().unwrap_or_default();
            if OPENERS.contains(&word) {
                depth += 1;
            } else if *line == "end" {
                depth -= 1;
                if depth < 0 {
                    break;
                }
            }
        }
        if depth != 0 {
            problems.push(format!(
                "{file}:{start}: diagram's alt, opt, loop, par, rect and critical lines do not balance with its end lines"
            ));
        }
        i += 1; // the closing fence
        let next = lines[i.min(lines.len())..]
            .iter()
            .find(|line| !line.text.trim().is_empty());
        if !next.is_some_and(|line| line.is_sources() && !source_items(line.text).is_empty()) {
            problems.push(format!(
                "{file}:{start}: diagram has no Sources line after it"
            ));
        }
    }
}

/// A bound of a server code: listed, a range, or below or from a number.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bound {
    Exact(u32),
    Range(u32, u32),
    Below(u32),
    From(u32),
}

impl Bound {
    /// Whether this bound of a row covers `named`, a bound the guide names.
    fn covers(self, named: Bound) -> bool {
        let (low, high) = match named {
            Bound::Exact(n) => (n, n),
            Bound::Range(low, high) => (low, high),
            Bound::Below(_) | Bound::From(_) => return self == named,
        };
        match self {
            Bound::Exact(n) => low == n && high == n,
            Bound::Range(a, b) => a <= low && high <= b,
            Bound::Below(b) => high < b,
            Bound::From(f) => low >= f,
        }
    }
}

/// The bounds a "Server codes" row's Codes cell names after its lead.
fn row_bounds(text: &str) -> Vec<Bound> {
    let words: Vec<&str> = text
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|word| !word.is_empty())
        .collect();
    let mut bounds = Vec::new();
    let mut i = 0;
    while i < words.len() {
        let word = words[i];
        let next = words.get(i + 1).and_then(|next| next.parse().ok());
        match (word, next) {
            ("below", Some(n)) => {
                bounds.push(Bound::Below(n));
                i += 1;
            }
            ("from", Some(n)) => {
                bounds.push(Bound::From(n));
                i += 1;
            }
            _ => {
                if let Some((low, high)) = word.split_once('–') {
                    if let (Ok(low), Ok(high)) = (low.parse(), high.parse()) {
                        bounds.push(Bound::Range(low, high));
                    }
                } else if let Ok(n) = word.parse() {
                    bounds.push(Bound::Exact(n));
                }
            }
        }
        i += 1;
    }
    bounds
}

/// The bound at the start of `text` (`N`, `N–M`, `below N`, `from N`), and
/// the rest.
fn named_bound(text: &str) -> Option<(Bound, &str)> {
    for (word, make) in [
        ("below ", Bound::Below as fn(u32) -> Bound),
        ("from ", Bound::From as fn(u32) -> Bound),
    ] {
        if let Some((n, rest)) = text.strip_prefix(word).and_then(number) {
            return Some((make(n), rest));
        }
    }
    let (low, rest) = number(text)?;
    if let Some((high, rest)) = ["–", "-"]
        .iter()
        .find_map(|dash| rest.strip_prefix(dash))
        .and_then(number)
    {
        return Some((Bound::Range(low, high), rest));
    }
    Some((Bound::Exact(low), rest))
}

/// The client-side and server code tables of `src/codes.rs`'s module
/// documentation.
struct CodeTables {
    /// Per kind, each code's string literals in its "Reason or message" cell.
    client: BTreeMap<String, BTreeMap<u32, Vec<String>>>,
    /// Each "Server codes" row with a lead, and its bounds.
    server: Vec<(String, Vec<Bound>)>,
}

impl CodeTables {
    fn parse(codes_rs: &str) -> Self {
        let mut tables = CodeTables {
            client: BTreeMap::new(),
            server: Vec::new(),
        };
        let mut kind: Option<String> = None;
        for line in codes_rs.lines().filter_map(|line| line.strip_prefix("//!")) {
            let line = line.trim();
            if let Some(heading) = line.strip_prefix("## ") {
                kind = Some(heading.trim_matches('`').to_owned());
                continue;
            }
            let Some(kind) = &kind else { continue };
            if !line.starts_with('|') {
                continue;
            }
            let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            if kind == "Server codes" {
                let cell = cells[0].replace('`', "");
                if let Some(lead) = LEADS.iter().find(|lead| cell.starts_with(*lead)) {
                    tables
                        .server
                        .push(((*lead).to_owned(), row_bounds(&cell[lead.len()..])));
                }
            } else if let (Ok(code), Some(reason)) = (cells[0].parse::<u32>(), cells.get(2)) {
                let literals = bug_catalogue::code_spans(reason)
                    .filter_map(|span| span.strip_prefix('"')?.strip_suffix('"'))
                    .map(str::to_owned)
                    .collect();
                tables
                    .client
                    .entry(kind.clone())
                    .or_default()
                    .insert(code, literals);
            }
        }
        assert!(
            KINDS.iter().all(|kind| tables.client.contains_key(*kind)) && !tables.server.is_empty(),
            "src/codes.rs has lost one of its tables"
        );
        tables
    }

    /// The codes of every line but the `Sources:` ones, outside code blocks
    /// other than diagrams.
    fn check(&self, file: &str, lines: &[Line], problems: &mut Vec<String>) {
        for line in lines {
            if !matches!(line.place, Place::Prose | Place::Mermaid) || line.is_sources() {
                continue;
            }
            let text = line.text.replace('`', "");
            let lower = text.to_ascii_lowercase();
            let at = format!("{file}:{}", line.number);
            let mut i = 0;
            while i < text.len() {
                if !text.is_char_boundary(i) || !starts_word(&text, i) {
                    i += 1;
                    continue;
                }
                if let Some(next) = self.server_code(&at, &text, &lower, i, problems) {
                    i = next;
                } else if let Some(next) = self.client_code(&at, &text, &lower, i, problems) {
                    i = next;
                } else {
                    if lower[i..].starts_with("code") && ends_word(&lower, i + 4) {
                        let after = lower[i + 4..].trim_start();
                        if after.starts_with(|c: char| c.is_ascii_digit()) {
                            problems.push(format!(
                                "{at}: a bare code and a number in {:?}: name its kind or its server lead",
                                line.text.trim()
                            ));
                        }
                    }
                    i += 1;
                }
            }
        }
    }

    /// A server code at `i`: its lead and the bounds after it, each covered
    /// by a row of that lead. The position after it, if there is one.
    fn server_code(
        &self,
        at: &str,
        text: &str,
        lower: &str,
        i: usize,
        problems: &mut Vec<String>,
    ) -> Option<usize> {
        let lead = LEADS
            .iter()
            .find(|lead| lower[i..].starts_with(*lead) && ends_word(lower, i + lead.len()))?;
        let mut rest = text[i + lead.len()..].strip_prefix(' ')?;
        let mut named = Vec::new();
        while let Some((bound, after)) = named_bound(rest) {
            named.push(bound);
            rest = after;
            match [", and ", ", or ", ", ", " and ", " or "]
                .iter()
                .find_map(|separator| rest.strip_prefix(separator))
            {
                Some(after) if named_bound(after).is_some() => rest = after,
                _ => break,
            }
        }
        if named.is_empty() {
            return None;
        }
        for bound in named {
            let covered = self
                .server
                .iter()
                .filter(|(row_lead, _)| row_lead == lead)
                .any(|(_, bounds)| bounds.iter().any(|row| row.covers(bound)));
            if !covered {
                problems.push(format!(
                    "{at}: {lead} {bound:?} is outside the rows of the Server codes table"
                ));
            }
        }
        Some(text.len() - rest.len())
    }

    /// A client-side code at `i`: its kind, its number and, when quoted
    /// right after it, its reason. The position after it, if there is one.
    fn client_code(
        &self,
        at: &str,
        text: &str,
        lower: &str,
        i: usize,
        problems: &mut Vec<String>,
    ) -> Option<usize> {
        let kind = KINDS
            .iter()
            .find(|kind| lower[i..].starts_with(*kind) && ends_word(lower, i + kind.len()))?;
        let (code, rest) = number(text[i + kind.len()..].strip_prefix(' ')?)?;
        if rest.starts_with(|c: char| is_word(c) || c == '–') {
            return None;
        }
        let Some(row) = self.client[*kind].get(&code) else {
            problems.push(format!("{at}: `{kind}` {code} is not a code of its table"));
            return Some(text.len() - rest.len());
        };
        let Some(quoted) = rest.strip_prefix(" \"") else {
            return Some(text.len() - rest.len());
        };
        let Some((reason, after)) = quoted.split_once('"') else {
            return Some(text.len() - rest.len());
        };
        if !row.iter().any(|literal| literal == reason) {
            problems.push(format!(
                "{at}: `{kind}` {code} {reason:?} is not a reason of its row"
            ));
        }
        Some(text.len() - after.len())
    }
}

/// A duration as the guide writes it.
fn duration(value: Duration) -> String {
    if value.is_zero() {
        "0".to_owned()
    } else if value.subsec_nanos() == 0 {
        format!("{} s", value.as_secs())
    } else {
        format!("{} ms", value.as_millis())
    }
}

/// A byte count as the guide writes it.
fn bytes(value: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;
    if value == 0 {
        "0".to_owned()
    } else if value.is_multiple_of(MIB) {
        format!("{} MiB", value / MIB)
    } else if value.is_multiple_of(KIB) {
        format!("{} KiB", value / KIB)
    } else {
        format!("{value} bytes")
    }
}

/// The defaults of `Config` and `SubscriptionOptions` the guide may state.
fn defaults() -> BTreeMap<&'static str, String> {
    let config = Config::default();
    let subscription = SubscriptionOptions::default();
    BTreeMap::from([
        ("min_reconnect_delay", duration(config.min_reconnect_delay)),
        ("max_reconnect_delay", duration(config.max_reconnect_delay)),
        ("timeout", duration(config.timeout)),
        ("handshake_timeout", duration(config.handshake_timeout)),
        (
            "max_server_ping_delay",
            duration(config.max_server_ping_delay),
        ),
        (
            "reconnect_jitter_window",
            duration(config.reconnect_jitter_window),
        ),
        (
            "read_ahead_messages",
            config.read_ahead_messages.to_string(),
        ),
        ("read_ahead_bytes", bytes(config.read_ahead_bytes)),
        ("write_queue_bytes", bytes(config.write_queue_bytes)),
        ("batch_frame_bytes", bytes(config.batch_frame_bytes)),
        ("max_message_bytes", bytes(config.max_message_bytes)),
        (
            "min_resubscribe_delay",
            duration(subscription.min_resubscribe_delay),
        ),
        (
            "max_resubscribe_delay",
            duration(subscription.max_resubscribe_delay),
        ),
    ])
}

/// Every `(default <value>)` follows an option, in backticks in prose and as
/// a word in a diagram, whose default is that value.
fn check_defaults(
    file: &str,
    lines: &[Line],
    defaults: &BTreeMap<&str, String>,
    problems: &mut Vec<String>,
) {
    for line in lines {
        if !matches!(line.place, Place::Prose | Place::Mermaid) {
            continue;
        }
        let at = format!("{file}:{}", line.number);
        for (i, _) in line.text.match_indices("(default ") {
            let value = line.text[i + "(default ".len()..]
                .split(')')
                .next()
                .unwrap_or_default();
            let before = line.text[..i].trim_end_matches(' ');
            let option = if line.place == Place::Prose {
                before
                    .strip_suffix('`')
                    .and_then(|before| before.rsplit_once('`'))
                    .map(|(_, option)| option)
            } else {
                before.rsplit(|c: char| !is_word(c)).next()
            };
            match option.filter(|option| !option.is_empty()) {
                None => problems.push(format!("{at}: (default {value}) follows no option")),
                Some(option) => match defaults.get(option) {
                    None => problems.push(format!(
                        "{at}: {option} is not a field of Config or SubscriptionOptions"
                    )),
                    Some(actual) if actual != value => {
                        problems.push(format!("{at}: {option} defaults to {actual}, not {value}"));
                    }
                    Some(_) => {}
                },
            }
        }
    }
}

/// The inline link targets of the prose lines, by line number.
fn links(lines: &[Line]) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    for line in lines.iter().filter(|line| line.place == Place::Prose) {
        let prose: String = line
            .text
            .split('`')
            .step_by(2)
            .collect::<Vec<_>>()
            .join(" ");
        for (i, _) in prose.match_indices("](") {
            let target = prose[i + 2..].split(')').next().unwrap_or_default();
            let target = target.split_whitespace().next().unwrap_or_default();
            found.push((line.number, target.to_owned()));
        }
    }
    found
}

/// A relative link of `file`: the repository path it names and its
/// fragment. `None` for an absolute or external one.
fn resolve(file: &str, target: &str) -> Option<(String, Option<String>)> {
    if target.contains("://") || target.starts_with("mailto:") || target.is_empty() {
        return None;
    }
    let (path, fragment) = match target.split_once('#') {
        Some((path, fragment)) => (path, Some(fragment.to_owned())),
        None => (target, None),
    };
    if path.is_empty() {
        return Some((file.to_owned(), fragment));
    }
    let mut parts: Vec<String> = Path::new(file)
        .parent()
        .into_iter()
        .flat_map(Path::components)
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    for part in Path::new(path).components() {
        match part {
            Component::ParentDir => {
                parts.pop();
            }
            Component::Normal(name) => parts.push(name.to_string_lossy().into_owned()),
            _ => {}
        }
    }
    Some((parts.join("/"), fragment))
}

/// The anchors GitHub gives the headings of `text`: lower case, punctuation
/// other than `-` and `_` removed, spaces as `-`, a repeated one suffixed
/// `-1`, `-2`.
fn anchors(text: &str) -> BTreeSet<String> {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut anchors = BTreeSet::new();
    for line in scan(text) {
        let Some((_, title)) = line.heading() else {
            continue;
        };
        let slug: String = title
            .chars()
            .flat_map(char::to_lowercase)
            .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
            .map(|c| if c == ' ' { '-' } else { c })
            .collect();
        let count = seen.entry(slug.clone()).or_insert(0);
        anchors.insert(if *count == 0 {
            slug
        } else {
            format!("{slug}-{count}")
        });
        *count += 1;
    }
    anchors
}

/// Fails unless one of `problems` contains `expected`.
fn reports(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "{expected:?} not reported: {problems:?}"
    );
}

const PROTOCOL: &str = "docs/protocol.md";
const RESILIENCE: &str = "docs/resilience.md";

#[test]
fn the_guide_passes_every_check() {
    let problems = Texts::load().problems();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn a_scenario_name_that_is_not_one_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "\"Missing pings trigger reconnect\"",
        "\"Missing pings trigger a reconnect\"",
    );
    reports(
        &texts.problems(),
        "\"Missing pings trigger a reconnect\" is not a scenario or a decision of Block 4",
    );
}

#[test]
fn a_block_without_a_heading_is_reported() {
    let texts = Texts::load().planted(RESILIENCE, "Block 31 D1", "Block 999 D1");
    reports(&texts.problems(), "Block 999 has no heading in CLAUDE.md");
}

#[test]
fn a_d_item_the_block_lacks_is_reported() {
    let texts = Texts::load().planted(PROTOCOL, "Block 29 D1", "Block 29 D9");
    reports(&texts.problems(), "Block 29 has no D9");
}

#[test]
fn a_renamed_function_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "`src/machine.rs::timer_fired`",
        "`src/machine.rs::timer_fired_now`",
    );
    reports(
        &texts.problems(),
        "src/machine.rs has no fn timer_fired_now",
    );
}

#[test]
fn a_diagram_without_sources_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "```\n\nSources: Block 9 \"Subscribe while connected\"",
        "```\n\nSee: Block 9 \"Subscribe while connected\"",
    );
    reports(&texts.problems(), "diagram has no Sources line after it");
}

#[test]
fn an_alt_without_its_end_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "        Note over App: the app reloads its state itself\n",
        "        Note over App: the app reloads its state itself\n    alt the app gives up\n",
    );
    reports(&texts.problems(), "do not balance with its end lines");
}

#[test]
fn a_code_whose_number_changed_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "Client-->>App: Connecting 2 \"no ping\"",
        "Client-->>App: Connecting 9 \"no ping\"",
    );
    reports(
        &texts.problems(),
        "`connecting` 9 is not a code of its table",
    );
}

#[test]
fn a_code_whose_reason_changed_is_reported() {
    let texts = Texts::load().planted(
        PROTOCOL,
        "Client-->>App: Connecting 2 \"no ping\"",
        "Client-->>App: Connecting 2 \"ping missing\"",
    );
    reports(
        &texts.problems(),
        "`connecting` 2 \"ping missing\" is not a reason of its row",
    );
}

#[test]
fn a_bare_code_and_a_number_is_reported() {
    let texts = Texts::load().planted(
        RESILIENCE,
        "Unsubscribe 2502 drops",
        "Unsubscribe code 2502 drops",
    );
    reports(&texts.problems(), "a bare code and a number");
}

#[test]
fn a_server_code_outside_its_rows_is_reported() {
    let texts = Texts::load().planted(RESILIENCE, "Disconnect 3014 makes", "Disconnect 2999 makes");
    reports(
        &texts.problems(),
        "disconnect Exact(2999) is outside the rows of the Server codes table",
    );
}

#[test]
fn a_changed_default_is_reported() {
    let texts = Texts::load().planted(
        RESILIENCE,
        "`read_ahead_messages` (default 256)",
        "`read_ahead_messages` (default 128)",
    );
    reports(
        &texts.problems(),
        "read_ahead_messages defaults to 256, not 128",
    );
}

#[test]
fn a_link_to_a_heading_that_does_not_exist_is_reported() {
    let texts = Texts::load().planted(
        RESILIENCE,
        "[Logging](../README.md#logging)",
        "[Logging](../README.md#logs)",
    );
    reports(&texts.problems(), "no heading #logs in README.md");
}

#[test]
fn a_guide_file_the_readme_does_not_link_is_reported() {
    let texts = Texts::load().planted(
        "README.md",
        "[docs/resilience.md](docs/resilience.md)",
        "docs/resilience.md",
    );
    reports(
        &texts.problems(),
        "docs/resilience.md is not linked from README.md",
    );
}
