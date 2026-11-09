//! Map subscriptions (Block 45): the events and types of a subscription to
//! a keyed collection that Centrifugo owns, loaded page by page, then kept
//! current per key.
//!
//! Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
//! and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
//! may change in any minor release.

use std::collections::HashMap;

use crate::token::Bytes;
use crate::{ClientError, ClientInfo, Subscribed};

/// The kind of a map subscription (Block 45, D2), sent as the subscribe
/// command's `type` (`client.proto`, `SubscribeRequest.type`).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MapKind {
    /// A map the application writes (`subscription_type: map`).
    Map,
    /// The clients subscribed to a channel, keyed by client id, with their
    /// info (`map_clients`), on the namespace's
    /// `map_clients_presence_channel_prefix` channel.
    Clients,
    /// The users subscribed to a channel, keyed by user id, without data or
    /// info (`map_users`), on the namespace's
    /// `map_users_presence_channel_prefix` channel.
    Users,
}

impl MapKind {
    /// The subscribe command's `type` (`client.proto:557-564`).
    pub(crate) fn wire(self) -> i32 {
        match self {
            Self::Map => 1,
            Self::Clients => 2,
            Self::Users => 3,
        }
    }
}

/// What a map subscription does when its position cannot be recovered
/// (Block 45, D6; centrifuge-js `unrecoverableStrategy`).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnrecoverableStrategy {
    /// Loads the state again, after the resubscribe backoff: the
    /// application gets a new [`MapSubscriptionEvent::Sync`].
    #[default]
    FromScratch,
    /// Unsubscribes with the server's code, 112.
    Fatal,
}

/// An entry of a map's state, in a [`MapSubscriptionEvent::Sync`]
/// (centrifuge-js `MapEntry`).
///
/// Its `Debug` output shows the data as its length and the tags by name
/// (Block 21).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MapEntry {
    /// The key.
    pub key: String,
    /// Its value, exactly as received: JSON text with the JSON protocol,
    /// any bytes with Protobuf.
    pub data: Vec<u8>,
    /// Its position in the map's stream, when the map keeps one.
    pub offset: Option<u64>,
    /// Tags the publisher attached.
    pub tags: HashMap<String, String>,
    /// The client that set it, when a client did; a `Clients` map's entry
    /// is the subscribed client.
    pub info: Option<ClientInfo>,
}

/// A change of one key, in a [`MapSubscriptionEvent::Update`]
/// (centrifuge-js `MapUpdateContext`).
///
/// Its `Debug` output shows the data as its length and the tags by name
/// (Block 21).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MapUpdate {
    /// The key.
    pub key: String,
    /// Its new value, exactly as received; empty for a removal.
    pub data: Vec<u8>,
    /// The key is removed.
    pub removed: bool,
    /// Its position in the map's stream, when the map keeps one.
    pub offset: Option<u64>,
    /// Tags the publisher attached.
    pub tags: HashMap<String, String>,
    /// The client that made the change, when a client did.
    pub info: Option<ClientInfo>,
}

/// Something that happened to a map subscription (Block 45, D2): its
/// lifecycle as a stream subscription's, then one [`Self::Sync`] with the
/// whole state, then one [`Self::Update`] per change.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental map subscriptions (v6.8.0 and later), and
/// may change in any minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MapSubscriptionEvent {
    /// The subscription moved to
    /// [`crate::SubscriptionState::Subscribing`].
    Subscribing {
        /// Why: a code from [`crate::codes::subscribing`].
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The subscription moved to [`crate::SubscriptionState::Subscribed`]:
    /// the state is loaded, and a [`Self::Sync`] follows unless it
    /// recovered.
    Subscribed(Subscribed),
    /// The subscription moved to
    /// [`crate::SubscriptionState::Unsubscribed`].
    Unsubscribed {
        /// Why: a code from [`crate::codes::unsubscribed`], or the code of
        /// the server's error.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The whole state of the map, with the changes made while it loaded:
    /// keys in the order the server gave them, a key set again after a
    /// removal last. Never a removed key.
    Sync(Vec<MapEntry>),
    /// A change of one key.
    Update(MapUpdate),
    /// An error that does not change the state.
    Error(ClientError),
}

impl MapSubscriptionEvent {
    /// Name of the event, as centrifuge-js names it.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribing { .. } => "subscribing",
            Self::Subscribed(_) => "subscribed",
            Self::Unsubscribed { .. } => "unsubscribed",
            Self::Sync(_) => "sync",
            Self::Update(_) => "update",
            Self::Error(_) => "error",
        }
    }
}

impl From<crate::Publication> for MapEntry {
    fn from(publication: crate::Publication) -> Self {
        Self {
            key: publication.key,
            data: publication.data,
            offset: publication.offset,
            tags: publication.tags,
            info: publication.info,
        }
    }
}

impl From<crate::Publication> for MapUpdate {
    fn from(publication: crate::Publication) -> Self {
        let removed = publication.removed;
        Self {
            key: publication.key,
            // A removal carries no value (centrifuge-js `MapUpdateContext`).
            data: if removed {
                Vec::new()
            } else {
                publication.data
            },
            removed,
            offset: publication.offset,
            tags: publication.tags,
            info: publication.info,
        }
    }
}

impl std::fmt::Debug for MapEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tags: Vec<&String> = self.tags.keys().collect();
        tags.sort();
        f.debug_struct("MapEntry")
            .field("key", &self.key)
            .field("data", &Bytes(&self.data))
            .field("offset", &self.offset)
            .field("tags", &tags)
            .field("info", &self.info)
            .finish()
    }
}

impl std::fmt::Debug for MapUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tags: Vec<&String> = self.tags.keys().collect();
        tags.sort();
        f.debug_struct("MapUpdate")
            .field("key", &self.key)
            .field("data", &Bytes(&self.data))
            .field("removed", &self.removed)
            .field("offset", &self.offset)
            .field("tags", &tags)
            .field("info", &self.info)
            .finish()
    }
}
