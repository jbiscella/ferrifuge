//! The release step's version ordering (Block 55, D4).

#[path = "support/versions.rs"]
mod versions;

use versions::version;

#[test]
fn versions_sort_by_semver_precedence() {
    let chain = [
        "0.25.0",
        "1.0.0-alpha.1",
        "1.0.0-alpha.2",
        "1.0.0-alpha.10",
        "1.0.0",
    ];
    for pair in chain.windows(2) {
        assert!(
            version(pair[0]) < version(pair[1]),
            "{} is not below {}",
            pair[0],
            pair[1]
        );
    }
}
