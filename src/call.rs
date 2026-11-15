//! Calls the application makes over the client's connection and awaits
//! (Block 13): their requests, results and errors.

use std::collections::HashMap;

use crate::{ClientInfo, Publication, StreamPosition};

/// Which publications of a channel's history to return (`client.proto`,
/// `HistoryRequest`). The default returns none, only the stream's current
/// position, as centrifuge-js does without options.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryOptions {
    /// At most this many publications, `Some(0)` for none (the default),
    /// `None` for all. The server may cap it; a count above `i32::MAX` is
    /// sent as `i32::MAX` (Block 54, D6).
    pub limit: Option<u32>,
    /// Only the publications after this position, or before it with
    /// `reverse`; from the start of the stream (its end with `reverse`)
    /// when `None`.
    pub since: Option<StreamPosition>,
    /// Newest first.
    pub reverse: bool,
}

impl Default for HistoryOptions {
    /// No publication, only the stream position, as centrifuge-js does
    /// without options.
    fn default() -> Self {
        Self {
            limit: Some(0),
            since: None,
            reverse: false,
        }
    }
}

impl HistoryOptions {
    /// Returns at most `limit` publications: 0 for none, only the stream
    /// position.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns every publication the server keeps.
    #[must_use]
    pub fn all(mut self) -> Self {
        self.limit = None;
        self
    }

    /// The limit as `client.proto` carries it: -1 for all, a count above
    /// `i32::MAX` as `i32::MAX`, which the server caps anyway.
    pub(crate) fn wire_limit(&self) -> i32 {
        self.limit
            .map_or(-1, |limit| i32::try_from(limit).unwrap_or(i32::MAX))
    }

    /// Returns the publications after `position` (before it with
    /// [`HistoryOptions::reverse`]).
    #[must_use]
    pub fn since(mut self, position: StreamPosition) -> Self {
        self.since = Some(position);
        self
    }

    /// Returns the publications newest first.
    #[must_use]
    pub fn reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }
}

/// A channel's history (`client.proto`, `HistoryResult`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryResult {
    /// The publications asked for, with their exact data.
    pub publications: Vec<Publication>,
    /// The stream's current epoch.
    pub epoch: String,
    /// The stream's current top offset.
    pub offset: u64,
}

/// What a publish returned: nothing yet, standing in for `client.proto`'s
/// empty `PublishResult` so that a field can come in a minor release
/// (Block 54, D10).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PublishResult {}

/// What an RPC returned (`client.proto`, `RPCResult`; Block 54, D10). Its
/// `Debug` output shows the data as its length (Block 21).
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RpcResult {
    /// The result data, exactly as received: raw JSON with the JSON
    /// protocol, any bytes with Protobuf.
    pub data: Vec<u8>,
}

impl std::fmt::Debug for RpcResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcResult")
            .field("data", &format_args!("<{} bytes>", self.data.len()))
            .finish()
    }
}

/// The clients subscribed to a channel (`client.proto`, `PresenceResult`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PresenceResult {
    /// The clients, by client id.
    pub clients: HashMap<String, ClientInfo>,
}

/// How many clients and users are subscribed to a channel (`client.proto`,
/// `PresenceStatsResult`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PresenceStatsResult {
    /// Connections subscribed.
    pub num_clients: u32,
    /// Distinct users among them.
    pub num_users: u32,
}

/// Why a call failed: an error the server answered with, keeping its code,
/// message and `temporary` flag (`client.proto`, `Error`), or a client-side
/// one with a code of [`crate::codes::error`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallError {
    /// The server's error code (from 100), or a client-side code below 100.
    pub code: u32,
    /// What went wrong.
    pub message: String,
    /// The server marked the error temporary: the same call may succeed
    /// later.
    pub temporary: bool,
}

impl CallError {
    /// An error with this code and message, not temporary.
    #[must_use]
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            temporary: false,
        }
    }

    /// The same error, marked temporary or not.
    #[must_use]
    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {}", self.message, self.code)?;
        if self.temporary {
            f.write_str(", temporary")?;
        }
        f.write_str(")")
    }
}

impl std::error::Error for CallError {}

/// Why a typed JSON call failed (Block 38, D2): before it was sent, as an
/// untyped call fails, or after the server answered.
///
/// `serde_json`'s messages may quote fragments of the data: the client never
/// logs them, and an application that logs this error should know it.
#[non_exhaustive]
pub enum JsonCallError {
    /// The value did not encode as JSON: nothing was sent.
    Encode(serde_json::Error),
    /// The call failed as the untyped one does (Block 13).
    Call(CallError),
    /// The server answered, and its result data did not decode as the
    /// type asked for. The call ran on the server: retrying it runs it
    /// again.
    #[non_exhaustive]
    Decode {
        /// Why the data did not decode.
        error: serde_json::Error,
        /// The result data, exactly as received.
        data: Vec<u8>,
    },
}

impl std::fmt::Debug for JsonCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(error) => f.debug_tuple("Encode").field(error).finish(),
            Self::Call(error) => f.debug_tuple("Call").field(error).finish(),
            // The data only by its length (Block 21, D3).
            Self::Decode { error, data } => f
                .debug_struct("Decode")
                .field("error", error)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
        }
    }
}

impl std::fmt::Display for JsonCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "the value did not encode as JSON: {error}"),
            Self::Call(error) => error.fmt(f),
            Self::Decode { error, .. } => {
                write!(
                    f,
                    "the result did not decode as the type asked for: {error}"
                )
            }
        }
    }
}

impl std::error::Error for JsonCallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) | Self::Decode { error, .. } => Some(error),
            Self::Call(error) => Some(error),
        }
    }
}

impl From<CallError> for JsonCallError {
    fn from(error: CallError) -> Self {
        Self::Call(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_history_returns_only_the_stream_position() {
        assert_eq!(HistoryOptions::default().limit, Some(0));
        assert_eq!(HistoryOptions::default().wire_limit(), 0);
    }

    #[test]
    fn all_is_sent_as_minus_one() {
        assert_eq!(HistoryOptions::default().all().limit, None);
        assert_eq!(HistoryOptions::default().all().wire_limit(), -1);
    }

    #[test]
    fn a_count_above_i32_max_is_sent_as_i32_max() {
        assert_eq!(
            HistoryOptions::default().limit(u32::MAX).wire_limit(),
            i32::MAX
        );
        assert_eq!(HistoryOptions::default().limit(7).wire_limit(), 7);
    }

    #[test]
    fn a_decode_error_shows_its_data_only_by_length() {
        #[derive(Debug, serde::Deserialize)]
        #[expect(dead_code, reason = "its field is read only through `Debug`")]
        struct Sum {
            sum: u32,
        }
        let data = br#"{"token":"secret","sum":"x"}"#.to_vec();
        let error = serde_json::from_slice::<Sum>(&data).unwrap_err();
        let failed = JsonCallError::Decode { error, data };

        let debug = format!("{failed:?}");

        // serde_json's message quotes the value it refused, "x", which the
        // documentation warns about; the data itself does not show.
        assert!(debug.contains("<28 bytes>"), "{debug}");
        assert!(!debug.contains("secret"), "{debug}");
        assert!(std::error::Error::source(&failed).is_some());
    }

    #[test]
    fn a_call_error_converts_and_displays_as_itself() {
        let error = CallError::new(108, "not available");

        let failed = JsonCallError::from(error.clone());

        assert_eq!(failed.to_string(), error.to_string());
        assert!(matches!(failed, JsonCallError::Call(inner) if inner == error));
    }
}
