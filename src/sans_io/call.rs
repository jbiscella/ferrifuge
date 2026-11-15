//! The calls an owner of the machine adds, and the replies they settle
//! with.
// Block 13; Block 54, D1.

use crate::{HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult};

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
