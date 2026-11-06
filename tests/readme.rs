//! The README stays in step with the manifest (Block 20, D9): the version
//! its dependency lines name, and the minimum Rust version it states.

const README: &str = include_str!("../README.md");

/// The manifest's version as a dependency requirement: `major.minor`.
fn requirement() -> String {
    let mut parts = env!("CARGO_PKG_VERSION").split('.');
    format!(
        "{}.{}",
        parts.next().expect("a major version"),
        parts.next().expect("a minor version")
    )
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
