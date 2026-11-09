//! Calls the application makes over the client's connection and awaits
//! (Block 13): their requests, results and errors.

use std::collections::HashMap;

use crate::{ClientInfo, Publication, StreamPosition};

/// What a call asks the server. `#[non_exhaustive]`: later blocks add
/// methods.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallRequest {
    /// An RPC (`client.proto`, `RPCRequest`).
    Rpc {
        /// Routes the call; empty for none.
        method: String,
        /// Raw JSON with the JSON protocol, any bytes with Protobuf.
        data: Vec<u8>,
    },
    /// A publication to a channel (`client.proto`, `PublishRequest`).
    Publish {
        /// The channel.
        channel: String,
        /// Raw JSON with the JSON protocol, any bytes with Protobuf.
        data: Vec<u8>,
    },
    /// A channel's history (`client.proto`, `HistoryRequest`).
    History {
        /// The channel.
        channel: String,
        /// Which publications to return.
        options: HistoryOptions,
    },
    /// The clients subscribed to a channel (`client.proto`,
    /// `PresenceRequest`).
    Presence {
        /// The channel.
        channel: String,
    },
    /// How many clients and users are subscribed to a channel
    /// (`client.proto`, `PresenceStatsRequest`).
    PresenceStats {
        /// The channel.
        channel: String,
    },
    /// An asynchronous message to the server, which sends no reply
    /// (`client.proto`, `SendRequest`).
    Send {
        /// Raw JSON with the JSON protocol, any bytes with Protobuf.
        data: Vec<u8>,
    },
    /// Sets a key of a map channel (`client.proto`, `PublishRequest` with
    /// `type` 1).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    MapPublish {
        /// The map channel.
        channel: String,
        /// The key to set; the server may replace it.
        key: String,
        /// Raw JSON with the JSON protocol, any bytes with Protobuf.
        data: Vec<u8>,
    },
    /// Removes a key of a map channel (`client.proto`, `PublishRequest`
    /// with `type` 1 and `removed`).
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental map subscriptions (v6.8.0 and
    /// later), and may change in any minor release.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    MapRemove {
        /// The map channel.
        channel: String,
        /// The key to remove.
        key: String,
    },
}

impl CallRequest {
    /// The name `client.proto` gives its request.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Rpc { .. } => "rpc",
            Self::Publish { .. } => "publish",
            Self::History { .. } => "history",
            Self::Presence { .. } => "presence",
            Self::PresenceStats { .. } => "presence_stats",
            Self::Send { .. } => "send",
            #[cfg(feature = "unstable")]
            Self::MapPublish { .. } | Self::MapRemove { .. } => "publish",
        }
    }
}

impl std::fmt::Debug for CallRequest {
    // Application data is shown by its length only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc { method, data } => f
                .debug_struct("Rpc")
                .field("method", method)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::Publish { channel, data } => f
                .debug_struct("Publish")
                .field("channel", channel)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::History { channel, options } => f
                .debug_struct("History")
                .field("channel", channel)
                .field("options", options)
                .finish(),
            Self::Presence { channel } => f
                .debug_struct("Presence")
                .field("channel", channel)
                .finish(),
            Self::PresenceStats { channel } => f
                .debug_struct("PresenceStats")
                .field("channel", channel)
                .finish(),
            Self::Send { data } => f
                .debug_struct("Send")
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            #[cfg(feature = "unstable")]
            Self::MapPublish { channel, key, data } => f
                .debug_struct("MapPublish")
                .field("channel", channel)
                .field("key", key)
                .field("data", &format_args!("<{} bytes>", data.len()))
                .finish(),
            #[cfg(feature = "unstable")]
            Self::MapRemove { channel, key } => f
                .debug_struct("MapRemove")
                .field("channel", channel)
                .field("key", key)
                .finish(),
        }
    }
}

impl CallRequest {
    /// The channel the request is about, if any.
    pub(crate) fn channel_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Rpc { .. } | Self::Send { .. } => None,
            Self::Publish { channel, .. }
            | Self::History { channel, .. }
            | Self::Presence { channel }
            | Self::PresenceStats { channel } => Some(channel),
            #[cfg(feature = "unstable")]
            Self::MapPublish { channel, .. } | Self::MapRemove { channel, .. } => Some(channel),
        }
    }
}

/// Which publications of a channel's history to return (`client.proto`,
/// `HistoryRequest`). The default returns none, only the stream's current
/// position, as centrifuge-js does without options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryOptions {
    /// At most this many publications; 0 for none, -1 for all. The server
    /// may cap it.
    pub limit: i32,
    /// Only the publications after this position, or before it with
    /// `reverse`; from the start of the stream (its end with `reverse`)
    /// when `None`.
    pub since: Option<StreamPosition>,
    /// Newest first.
    pub reverse: bool,
}

impl HistoryOptions {
    /// Returns at most `limit` publications: 0 for none, -1 for all.
    #[must_use]
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
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

/// What a call returned. `#[non_exhaustive]`: later blocks add methods.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallReply {
    /// The RPC's result data (`client.proto`, `RPCResult.data`): raw JSON
    /// with the JSON protocol, any bytes with Protobuf; empty when the
    /// server sent none.
    Rpc(Vec<u8>),
    /// The publication was accepted.
    Publish,
    /// A channel's history.
    History(HistoryResult),
    /// The clients subscribed to a channel.
    Presence(PresenceResult),
    /// How many clients and users are subscribed to a channel.
    PresenceStats(PresenceStatsResult),
    /// The message was written: no reply follows.
    Send,
}

impl std::fmt::Debug for CallReply {
    // Application data is shown by its length only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(data) => f
                .debug_tuple("Rpc")
                .field(&format_args!("<{} bytes>", data.len()))
                .finish(),
            Self::Publish => f.write_str("Publish"),
            // Publications and client infos are application data: only
            // their number is shown.
            Self::History(history) => f
                .debug_struct("History")
                .field(
                    "publications",
                    &format_args!("<{}>", history.publications.len()),
                )
                .field("epoch", &history.epoch)
                .field("offset", &history.offset)
                .finish(),
            Self::Presence(presence) => f
                .debug_struct("Presence")
                .field("clients", &format_args!("<{}>", presence.clients.len()))
                .finish(),
            Self::PresenceStats(stats) => f.debug_tuple("PresenceStats").field(stats).finish(),
            Self::Send => f.write_str("Send"),
        }
    }
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
    fn a_decode_error_shows_its_data_only_by_length() {
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
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
