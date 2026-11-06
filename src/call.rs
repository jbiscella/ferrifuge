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
