//! Testing support for applications (Block 37), behind the `test-util`
//! feature: constructors of the events and results the client yields, and,
//! with the `tokio` feature, [`Server`], an in-memory server double the
//! real client runs against.

mod constructors;
#[cfg(feature = "tokio")]
mod server;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use server::{Command, Server};

#[cfg(test)]
mod tests {
    /// Every source file under `src/`, its path relative to `src/` and its
    /// code before its test module, line endings normalised (a Windows
    /// checkout has CRLF).
    fn sources() -> Vec<(String, String)> {
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, String)>) {
            for entry in std::fs::read_dir(dir).expect("a source directory") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    walk(&path, root, out);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    let relative = path
                        .strip_prefix(root)
                        .expect("under src")
                        .to_string_lossy()
                        .replace('\\', "/");
                    let text = std::fs::read_to_string(&path)
                        .expect("a source file")
                        .replace("\r\n", "\n");
                    // Code before the test module only.
                    let code = text
                        .split("#[cfg(test)]\nmod tests")
                        .next()
                        .unwrap_or(&text);
                    out.push((relative, code.to_owned()));
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut out = Vec::new();
        walk(&root, &root, &mut out);
        out
    }

    /// The constructors sit in this module, compiled only under
    /// `test-util`, and no hook of the crate's own tests is reachable
    /// through `test-util` (Block 37, D1).
    #[test]
    fn test_util_gates_this_module_only_and_no_hook() {
        let mut found = Vec::new();
        for (path, text) in sources() {
            let in_module = path.starts_with("test_util/");
            for (number, line) in text.lines().enumerate() {
                let gated = line.contains(r#"feature = "test-util""#);
                let allowed = (path == "lib.rs"
                    && (line.trim() == r#"#[cfg(feature = "test-util")]"#
                        || line.trim()
                            == r#"#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]"#))
                    || (in_module && line.contains("doc(cfg("));
                if gated && !allowed {
                    found.push(format!("src/{path}:{}: {line}", number + 1));
                }
                if in_module && line.contains(r#"feature = "__test-hooks""#) {
                    found.push(format!("src/{path}:{}: {line}", number + 1));
                }
            }
        }
        let lib = sources()
            .into_iter()
            .find(|(path, _)| path == "lib.rs")
            .expect("src/lib.rs")
            .1;

        assert!(found.is_empty(), "{found:#?}");
        assert!(
            lib.contains("#[cfg(feature = \"test-util\")]\n#[cfg_attr(docsrs, doc(cfg(feature = \"test-util\")))]\npub mod test_util;"),
            "src/lib.rs declares the module under test-util"
        );
    }
}
