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
