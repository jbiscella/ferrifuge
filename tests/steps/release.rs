//! Steps of Block 20: release setup. The changelog, the release check
//! script and the package list are checked on the repository itself; the
//! quickstart example runs against a server started with the examples'
//! configuration.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use cucumber::{given, then, when};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout};

use super::ClientWorld;
use super::transports::server;
use crate::support::centrifugo::Centrifugo;

/// The crate's version, as its manifest says.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The repository: the crate's manifest directory.
fn repository() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// What a Block 20 scenario holds.
#[derive(Default)]
pub(crate) struct Release {
    /// The crate directory the release check runs in: the repository, or a
    /// copy a step made.
    crate_dir: Option<PathBuf>,
    /// The release check's outcome.
    check: Option<Output>,
    /// The files of the package.
    files: Vec<String>,
    /// The quickstart example running, and its output.
    example: Option<(Child, Lines<BufReader<ChildStdout>>)>,
    /// Keeps the crate copy alive.
    copy: Option<TempDir>,
}

/// A directory removed on drop.
struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[given("the crate's manifest")]
fn the_manifest(world: &mut ClientWorld) {
    world.release.crate_dir = Some(repository().to_owned());
}

/// The headings of the changelog's sections: `Unreleased`, or a version
/// with its date.
fn changelog_headings() -> Vec<String> {
    std::fs::read_to_string(repository().join("CHANGELOG.md"))
        .expect("CHANGELOG.md")
        .lines()
        .filter_map(|line| line.strip_prefix("## "))
        .map(str::to_owned)
        .collect()
}

/// `x.y.z` as numbers, for ordering.
fn version(text: &str) -> (u64, u64, u64) {
    let mut parts = text
        .split('.')
        .map(|part| part.parse().expect("a version number"));
    let mut next = || parts.next().expect("three version numbers");
    (next(), next(), next())
}

/// The versions of the changelog's sections, checking their headings.
fn changelog_versions() -> Vec<(u64, u64, u64)> {
    let headings = changelog_headings();
    assert_eq!(
        headings.first().map(String::as_str),
        Some("[Unreleased]"),
        "{headings:?}"
    );
    headings[1..]
        .iter()
        .map(|heading| {
            let (number, date) = heading
                .strip_prefix('[')
                .and_then(|rest| rest.split_once("] - "))
                .unwrap_or_else(|| panic!("not a version heading with a date: {heading}"));
            let date_ok = date.len() == 10
                && date.chars().enumerate().all(|(i, c)| {
                    if i == 4 || i == 7 {
                        c == '-'
                    } else {
                        c.is_ascii_digit()
                    }
                });
            assert!(date_ok, "not a YYYY-MM-DD date: {heading}");
            version(number)
        })
        .collect()
}

#[then(
    "CHANGELOG.md begins with an Unreleased section, then version sections from newest to oldest, each headed by its version and a date"
)]
fn changelog_in_order(_world: &mut ClientWorld) {
    let versions = changelog_versions();
    assert!(!versions.is_empty(), "no version section");
    assert!(
        versions.windows(2).all(|pair| pair[0] > pair[1]),
        "not newest first: {versions:?}"
    );
}

#[then("the manifest's version is the newest of those versions, or above it")]
fn manifest_is_newest(_world: &mut ClientWorld) {
    let newest = changelog_versions()[0];
    assert!(version(VERSION) >= newest, "{VERSION} below {newest:?}");
}

/// Runs the release check for `tag` in `dir`.
fn release_check(dir: &Path, tag: &str) -> Output {
    Command::new("bash")
        .arg(repository().join("scripts").join("check-release-tag"))
        .arg(tag)
        .arg(dir)
        .output()
        .expect("bash runs the release check")
}

#[when(regex = r#"^the release check runs for tag "([^"]+)"$"#)]
fn the_release_check_runs(world: &mut ClientWorld, tag: String) {
    let tag = match tag.as_str() {
        "v followed by its version" => format!("v{VERSION}"),
        "its version without the v" => VERSION.to_owned(),
        "v followed by its version and -rc.1" => format!("v{VERSION}-rc.1"),
        literal => literal.to_owned(),
    };
    let dir = world.release.crate_dir.clone().expect("a crate");
    world.release.check = Some(release_check(&dir, &tag));
}

#[when(r#"the release check runs for tag "v" followed by its version"#)]
fn the_release_check_runs_for_the_version(world: &mut ClientWorld) {
    the_release_check_runs(world, "v followed by its version".to_owned());
}

fn check_output(world: &ClientWorld) -> (&Output, String) {
    let output = world.release.check.as_ref().expect("the release check ran");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output, text)
}

#[then("it succeeds")]
fn it_succeeds(world: &mut ClientWorld) {
    let (output, text) = check_output(world);
    assert!(output.status.success(), "{text}");
}

#[then("it fails, naming the version the tag must be")]
fn it_fails_naming_the_tag(world: &mut ClientWorld) {
    let (output, text) = check_output(world);
    assert!(!output.status.success(), "{text}");
    assert!(text.contains(&format!("must be v{VERSION}")), "{text}");
}

#[given("a copy of the crate whose CHANGELOG.md has no section for its version")]
fn a_copy_without_the_section(world: &mut ClientWorld) {
    let dir = std::env::temp_dir().join(format!(
        "ferrifuge-release-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(dir.join("src")).expect("a crate directory");
    std::fs::write(
        dir.join("Cargo.toml"),
        format!("[package]\nname = \"ferrifuge\"\nversion = \"{VERSION}\"\nedition = \"2024\"\n\n[workspace]\n"),
    )
    .expect("a manifest");
    std::fs::write(dir.join("src").join("lib.rs"), "").expect("a library");
    std::fs::write(
        dir.join("CHANGELOG.md"),
        "# Changelog\n\n## [Unreleased]\n\n## [0.0.1] - 2026-01-01\n",
    )
    .expect("a changelog");
    world.release.crate_dir = Some(dir.clone());
    world.release.copy = Some(TempDir(dir));
}

#[then("it fails, naming the missing section")]
fn it_fails_naming_the_section(world: &mut ClientWorld) {
    let (output, text) = check_output(world);
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains(&format!("no section for {VERSION}")),
        "{text}"
    );
}

#[when("the files of its package are listed")]
fn package_files(world: &mut ClientWorld) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = Command::new(cargo)
        .args(["package", "--list", "--allow-dirty", "--offline"])
        .current_dir(repository())
        .output()
        .expect("cargo package --list");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    world.release.files = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| line.replace('\\', "/"))
        .collect();
}

#[then(
    "each is under src or proto, or is build.rs, Cargo.toml, Cargo.toml.orig, Cargo.lock, .cargo_vcs_info.json, README.md, LICENSE, CHANGELOG.md or SECURITY.md"
)]
fn only_allowed_files(world: &mut ClientWorld) {
    const FILES: [&str; 9] = [
        "build.rs",
        "Cargo.toml",
        "Cargo.toml.orig",
        "Cargo.lock",
        ".cargo_vcs_info.json",
        "README.md",
        "LICENSE",
        "CHANGELOG.md",
        "SECURITY.md",
    ];
    let unexpected: Vec<&String> = world
        .release
        .files
        .iter()
        .filter(|file| {
            !(file.starts_with("src/")
                || file.starts_with("proto/")
                || FILES.contains(&file.as_str()))
        })
        .collect();
    assert!(unexpected.is_empty(), "{unexpected:?}");
}

#[then(
    "build.rs, proto/client.proto, proto/LICENSE, LICENSE, README.md, CHANGELOG.md and SECURITY.md are among them"
)]
fn needed_files_present(world: &mut ClientWorld) {
    let missing: Vec<&str> = [
        "build.rs",
        "proto/client.proto",
        "proto/LICENSE",
        "LICENSE",
        "README.md",
        "CHANGELOG.md",
        "SECURITY.md",
    ]
    .into_iter()
    .filter(|needed| !world.release.files.iter().any(|file| file == needed))
    .collect();
    assert!(
        missing.is_empty(),
        "{missing:?} in {:?}",
        world.release.files
    );
}

/// How long the example has to print a line.
const EXAMPLE_LINE: Duration = Duration::from_secs(10);

#[given("a server")]
async fn a_server_with_the_examples_configuration(world: &mut ClientWorld) {
    if let Some(started) = world.server.take() {
        started.stop().await;
    }
    let config = std::fs::read_to_string(repository().join("examples").join("centrifugo.json"))
        .expect("examples/centrifugo.json");
    world.server = Some(Centrifugo::start_with_config(&config).await);
}

/// The quickstart binary Cargo builds next to this test's.
fn quickstart_binary() -> PathBuf {
    let test = std::env::current_exe().expect("the test binary");
    let profile = test
        .parent()
        .and_then(Path::parent)
        .expect("the target profile directory");
    let binary = profile
        .join("examples")
        .join(format!("quickstart{}", std::env::consts::EXE_SUFFIX));
    assert!(
        binary.exists(),
        "{} missing: build the examples first (cargo build --examples)",
        binary.display()
    );
    binary
}

#[when("the quickstart example runs against the server's websocket endpoint")]
fn the_quickstart_runs(world: &mut ClientWorld) {
    let endpoint = format!("ws://{}/connection/websocket", server(world).address());
    let mut child = tokio::process::Command::new(quickstart_binary())
        .env("FERRIFUGE_EXAMPLE_ENDPOINT", endpoint)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("the quickstart example starts");
    let stdout = child.stdout.take().expect("its output");
    world.release.example = Some((child, BufReader::new(stdout).lines()));
}

/// The example's next line of output.
async fn next_line(world: &mut ClientWorld) -> String {
    let (_, lines) = world.release.example.as_mut().expect("the example runs");
    tokio::time::timeout(EXAMPLE_LINE, lines.next_line())
        .await
        .expect("the example printed a line in time")
        .expect("its output")
        .expect("a line before its output ended")
}

#[when("it prints that it is subscribed to its channel")]
async fn prints_subscribed(world: &mut ClientWorld) {
    assert_eq!(next_line(world).await, "subscribed to news");
}

#[when(regex = r"^the server API publishes (\{.*\}) to that channel$")]
async fn publishes_to_the_examples_channel(world: &mut ClientWorld, data: String) {
    let data: serde_json::Value = serde_json::from_str(&data).expect("JSON data");
    server(world)
        .api()
        .call("publish", json!({"channel": "news", "data": data}))
        .await
        .expect("publish");
}

#[then(regex = r"^the example prints (\{.*\})$")]
async fn the_example_prints(world: &mut ClientWorld, data: String) {
    assert_eq!(next_line(world).await, data);
}
