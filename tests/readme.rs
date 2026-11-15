//! The README stays in step with the manifest (Block 20, D9; Block 55, D1,
//! D3): the version its dependency lines name, the minimum Rust version it
//! states, and the features and public dependencies its Stability section
//! names.

const README: &str = include_str!("../README.md");
const MANIFEST: &str = include_str!("../Cargo.toml");

/// `version` as a dependency requirement: `major.minor` for a release, the
/// whole version for a pre-release, which a `major.minor` requirement does
/// not match.
fn requirement_for(version: &str) -> String {
    if version.contains('-') {
        return version.to_owned();
    }
    let mut parts = version.split('.');
    format!(
        "{}.{}",
        parts.next().expect("a major version"),
        parts.next().expect("a minor version")
    )
}

/// The manifest's version as a dependency requirement.
fn requirement() -> String {
    requirement_for(env!("CARGO_PKG_VERSION"))
}

#[test]
fn a_release_is_required_by_major_and_minor_a_pre_release_whole() {
    assert_eq!(requirement_for("0.25.0"), "0.25");
    assert_eq!(requirement_for("1.0.0-alpha.1"), "1.0.0-alpha.1");
}

#[test]
fn every_dependency_line_names_the_manifests_version() {
    let expected = requirement();
    let lines: Vec<&str> = README
        .lines()
        .filter(|line| line.trim_start().starts_with("ferrifuge = "))
        .collect();
    assert!(!lines.is_empty(), "no dependency line in the README");
    for line in lines {
        // Git dependencies name no version.
        if line.contains("git = ") {
            continue;
        }
        assert!(
            line.contains(&format!("\"{expected}\"")),
            "{line} does not name {expected}"
        );
    }
}

#[test]
fn the_stated_msrv_is_the_manifests() {
    let msrv = env!("CARGO_PKG_RUST_VERSION");
    assert!(
        README.contains(&format!("MSRV-{msrv}-")),
        "the MSRV badge does not say {msrv}"
    );
    assert!(
        README.contains(&format!("Rust {msrv} or newer")),
        "the requirements do not say Rust {msrv}"
    );
}

/// The README's Stability section, up to the next section.
fn stability() -> &'static str {
    let start = README
        .find("\n## Stability\n")
        .expect("a Stability section");
    let rest = &README[start + 1..];
    rest.find("\n## ").map_or(rest, |end| &rest[..end])
}

/// The manifest's features, `default` aside.
fn features() -> Vec<&'static str> {
    let start = MANIFEST.find("\n[features]\n").expect("a [features] table");
    MANIFEST[start + "\n[features]\n".len()..]
        .lines()
        .take_while(|line| !line.starts_with('['))
        .filter_map(|line| line.split_once(" = ").map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty() && !name.starts_with('#') && *name != "default")
        .collect()
}

/// The version of `name`'s first requirement in the manifest, as the
/// Stability section names it: `0.y` for a 0.x version, the major
/// otherwise.
fn stated_version(name: &str) -> String {
    let start = MANIFEST
        .find("\n[dependencies]\n")
        .expect("a [dependencies] table");
    let line = MANIFEST[start..]
        .lines()
        .find(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.trim_start().starts_with('='))
        })
        .unwrap_or_else(|| panic!("no requirement for {name}"));
    // `name = "x.y.z"` or `name = { version = "x.y.z", ... }`.
    let version = line
        .split('"')
        .nth(1)
        .unwrap_or_else(|| panic!("no version in {line}"));
    let mut parts = version.split('.');
    let major = parts.next().expect("a major version");
    if major == "0" {
        format!("0.{}", parts.next().expect("a minor version"))
    } else {
        major.to_owned()
    }
}

#[test]
fn the_stability_section_names_every_feature_and_the_unstable_modules() {
    let section = stability();
    let features = features();
    assert!(features.contains(&"unstable"), "{features:?}");
    for feature in features {
        assert!(
            section.contains(&format!("`{feature}`")),
            "the Stability section does not name `{feature}`"
        );
    }
    for module in ["ferrifuge::android", "ferrifuge::machine"] {
        assert!(
            section.contains(&format!("`{module}`")),
            "the Stability section does not name `{module}`"
        );
    }
}

#[test]
fn the_stability_section_names_each_public_dependency_at_its_version() {
    let section = stability();
    for name in [
        "rustls",
        "futures-core",
        "http",
        "url",
        "tokio",
        "js-sys",
        "web-sys",
        "serde",
        "serde_json",
    ] {
        let stated = format!("`{name}` {}", stated_version(name));
        assert!(
            section.contains(&stated),
            "the Stability section does not name {stated}"
        );
    }
}
