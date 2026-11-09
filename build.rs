//! Generates the client protocol types from `proto/client.proto`, and says
//! which adapter the build has (Block 44, D1): `adapter_tokio` for the
//! `tokio` feature on a native target, `adapter_browser` for the `browser`
//! feature on wasm32-unknown-unknown.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/client.proto");
    println!("cargo::rustc-check-cfg=cfg(adapter_tokio)");
    println!("cargo::rustc-check-cfg=cfg(adapter_browser)");
    let browser_target = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32")
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("unknown");
    let feature = |name: &str| std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some();
    if browser_target {
        if feature("BROWSER") {
            println!("cargo::rustc-cfg=adapter_browser");
        }
    } else if feature("TOKIO") {
        println!("cargo::rustc-cfg=adapter_tokio");
    }
    prost_build::Config::new()
        .protoc_executable(protoc_bin_vendored::protoc_bin_path()?)
        .compile_protos(&["proto/client.proto"], &["proto"])?;
    Ok(())
}
