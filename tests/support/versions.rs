//! Release versions in `SemVer` order (Block 55, D4): a pre-release sorts
//! below its release, and numeric identifiers sort as numbers.

/// `text` as a version, for ordering.
pub(crate) fn version(text: &str) -> semver::Version {
    semver::Version::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"))
}
