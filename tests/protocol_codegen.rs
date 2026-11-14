//! The committed Protobuf code is what the locked prost-build writes from
//! `proto/client.proto`, and it carries the MIT license of its source (Block
//! 56, D4, D5). `FERRIFUGE_REGENERATE_PROTOCOL=1 cargo test --test
//! protocol_codegen` writes the file instead of comparing.

// The generators are native dev-dependencies only (Block 56, D3).
#![cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]

use std::path::{Path, PathBuf};

/// The committed file, relative to the package root.
const GENERATED: &str = "src/protocol/centrifugal.centrifuge.protocol.rs";

const REGENERATE: &str = "FERRIFUGE_REGENERATE_PROTOCOL=1 cargo test --test protocol_codegen";

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &str) -> String {
    std::fs::read_to_string(root().join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// What the locked prost-build writes, with the locked vendored protoc,
/// into a fresh directory.
fn generated() -> Vec<u8> {
    let out: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protocol_codegen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    prost_build::Config::new()
        .protoc_executable(protoc_bin_vendored::protoc_bin_path().unwrap())
        .out_dir(&out)
        .compile_protos(
            &[root().join("proto/client.proto")],
            &[root().join("proto")],
        )
        .unwrap();
    let bytes = std::fs::read(out.join("centrifugal.centrifuge.protocol.rs")).unwrap();
    std::fs::remove_dir_all(&out).unwrap();
    bytes
}

#[test]
fn the_committed_protocol_code_is_what_prost_build_writes() {
    let generated = generated();
    let committed = root().join(GENERATED);
    if std::env::var_os("FERRIFUGE_REGENERATE_PROTOCOL").is_some() {
        std::fs::write(&committed, &generated).unwrap();
        return;
    }
    let committed = std::fs::read(&committed).unwrap();
    let first = committed
        .iter()
        .zip(&generated)
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| committed.len().min(generated.len()));
    assert!(
        committed == generated,
        "{GENERATED} differs from what prost-build writes (first at byte {first}; \
         {} bytes committed, {} generated): run `{REGENERATE}`",
        committed.len(),
        generated.len(),
    );
}

// The `**` 0BSD annotation would cover the file without its own: `reuse
// lint` cannot tell the override is missing.
#[test]
fn reuse_marks_the_generated_code_mit() {
    let reuse = read("REUSE.toml");
    let annotation = reuse.split("[[annotations]]").find(|table| {
        table
            .lines()
            .any(|line| line.trim() == format!("path = \"{GENERATED}\""))
    });
    let Some(annotation) = annotation else {
        panic!("REUSE.toml has no annotation for {GENERATED}");
    };
    let lines: Vec<&str> = annotation.lines().map(str::trim).collect();
    for wanted in [
        "precedence = \"override\"",
        "SPDX-FileCopyrightText = \"2023 Centrifugal Labs LTD\"",
        "SPDX-License-Identifier = \"MIT\"",
    ] {
        assert!(
            lines.contains(&wanted),
            "REUSE.toml, {GENERATED}: no `{wanted}`"
        );
    }
}

#[test]
fn license_lists_the_generated_code_as_shipped() {
    let license = read("LICENSE");
    let shipped = license
        .split_once("Shipped in the crates.io package:")
        .and_then(|(_, rest)| rest.split_once("In the repository only:"))
        .map(|(shipped, _)| shipped)
        .expect("LICENSE has its \"Shipped in the crates.io package\" list");
    assert!(
        shipped.lines().any(|line| line.trim() == GENERATED),
        "LICENSE does not list {GENERATED} as shipped"
    );
}
