//! Connection tokens.

/// Why a token callback could not provide a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    /// The user may not connect anymore: the client disconnects for good.
    Unauthorized,
    /// Getting a token failed; the client retries later.
    Failed(String),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for TokenError {}

/// A token held by the client. Its `Debug` output never shows the value:
/// a token in a log can be replayed by anyone who reads the log.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct Secret(pub(crate) String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        redacted(&self.0, f)
    }
}

/// Application data in `Debug` output: its length only, as `<n bytes>`.
pub(crate) struct Bytes<'a>(pub(crate) &'a [u8]);

impl std::fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes>", self.0.len())
    }
}

/// Optional application data in `Debug` output: its length only.
pub(crate) fn optional_bytes(data: Option<&Vec<u8>>) -> Option<Bytes<'_>> {
    data.map(|data| Bytes(data))
}

/// Writes `"<redacted>"` for a non-empty token, `""` for none.
pub(crate) fn redacted(token: &str, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if token.is_empty() {
        f.write_str("\"\"")
    } else {
        f.write_str("<redacted>")
    }
}
