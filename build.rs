fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/client.proto");
    // SAFETY: build scripts are single-threaded; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    prost_build::compile_protos(&["proto/client.proto"], &["proto"])?;
    Ok(())
}
