//! Shared poll subscriptions (Block 47): the events and types of a
//! subscription on which the application names keys, each tracked under a
//! signature its backend issued, and gets every new version of each key's
//! value once.
//!
//! Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
//! and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
//! later), and may change in any minor release.

use crate::token::{Bytes, Secret};
use crate::{ClientError, Subscribed};

/// A key to track at the version of its value the application holds, in
/// [`crate::SharedPollSubscription::track_with_signature`] (`client.proto`,
/// `KeyedItem`).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SharedPollTrackItem {
    /// The key.
    pub key: String,
    /// The version of its value the application holds; 0 when it holds
    /// none, and the server then sends the current value.
    pub version: u64,
}

impl SharedPollTrackItem {
    /// `key` at `version`.
    #[must_use]
    pub fn new(key: impl Into<String>, version: u64) -> Self {
        Self {
            key: key.into(),
            version,
        }
    }
}

/// A signature the application's backend issued for exactly `keys`, in that
/// order, as a `get_signature` callback returns it (centrifuge-js
/// `SharedPollSignatureResult`). The signature is a credential: its `Debug`
/// output shows its length only (Block 47, D16).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SharedPollSignature {
    /// The keys the signature covers, in the order it covers them.
    pub keys: Vec<String>,
    /// The signature.
    pub signature: String,
}

impl SharedPollSignature {
    /// A signature over `keys`.
    #[must_use]
    pub fn new(keys: Vec<String>, signature: impl Into<String>) -> Self {
        Self {
            keys,
            signature: signature.into(),
        }
    }
}

impl std::fmt::Debug for SharedPollSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPollSignature")
            .field("keys", &self.keys)
            .field("signature", &Secret(self.signature.clone()))
            .finish()
    }
}

/// A new version of one tracked key's value, or the end of the key, in a
/// [`SharedPollSubscriptionEvent::Update`] (centrifuge-js
/// `SharedPollUpdateContext`).
///
/// Its `Debug` output shows the data as its length (Block 21).
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SharedPollUpdate {
    /// The key.
    pub key: String,
    /// Its value, exactly as received: JSON text with the JSON protocol, any
    /// bytes with Protobuf; empty for a removal.
    pub data: Vec<u8>,
    /// The key ended: the backend removed it, or the server refused to track
    /// it. It is no longer tracked.
    pub removed: bool,
    /// The value's version, `None` when the namespace has none (versionless
    /// mode) and for a removal.
    pub version: Option<u64>,
}

impl std::fmt::Debug for SharedPollUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPollUpdate")
            .field("key", &self.key)
            .field("data", &Bytes(&self.data))
            .field("removed", &self.removed)
            .field("version", &self.version)
            .finish()
    }
}

impl SharedPollUpdate {
    /// The removal of `key`: no data, no version.
    pub(crate) fn removal_of(key: String) -> Self {
        Self {
            key,
            data: Vec::new(),
            removed: true,
            version: None,
        }
    }
}

impl SharedPollUpdate {
    /// The value decoded as JSON into `T`, as [`crate::Publication::json`]
    /// does (Block 38, Block 47 D15).
    ///
    /// # Errors
    ///
    /// The data is not JSON, or not JSON that `T` accepts; empty data, as a
    /// removal's, is not JSON. `serde_json`'s message may quote fragments of
    /// the data.
    pub fn json<'de, T: serde::Deserialize<'de>>(&'de self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}

impl From<crate::Publication> for SharedPollUpdate {
    fn from(publication: crate::Publication) -> Self {
        let (key, removed, version) = publication
            .map
            .map(|part| (part.key, part.removed, part.version))
            .unwrap_or_default();
        if removed {
            return Self::removal_of(key);
        }
        Self {
            key,
            data: publication.data,
            removed,
            // 0 means no version is known (`client.proto`, `KeyedItem`).
            version: (version > 0).then_some(version),
        }
    }
}

/// Something that happened to a shared poll subscription (Block 47, D2):
/// its lifecycle as a stream subscription's, then one [`Self::Update`] per
/// new version of a tracked key's value, and per key that ends.
///
/// Unstable: requires the `unstable` feature, follows centrifuge-js 63ed73e
/// and Centrifugo's experimental shared poll subscriptions (v6.8.0 and
/// later), and may change in any minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SharedPollSubscriptionEvent {
    /// The subscription moved to
    /// [`crate::SubscriptionState::Subscribing`].
    Subscribing {
        /// Why: a code from [`crate::codes::subscribing`], or the server's
        /// unsubscribe code.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The subscription moved to [`crate::SubscriptionState::Subscribed`];
    /// a shared poll subscription has no stream position.
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
    /// A new version of a tracked key's value, or the end of a key.
    Update(SharedPollUpdate),
    /// An error that does not change the state, e.g. a track request the
    /// server refused.
    Error(ClientError),
}

impl SharedPollSubscriptionEvent {
    /// Name of the event, as centrifuge-js names it.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribing { .. } => "subscribing",
            Self::Subscribed(_) => "subscribed",
            Self::Unsubscribed { .. } => "unsubscribed",
            Self::Update(_) => "update",
            Self::Error(_) => "error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "s1gn4tur3-s3cr3t";

    // Block 47, D16: a signature is a credential.
    #[test]
    fn debug_output_shows_no_signature_and_no_value() {
        let signature = SharedPollSignature::new(vec!["a".into()], SECRET);
        let update = SharedPollUpdate {
            key: "a".into(),
            data: SECRET.as_bytes().to_vec(),
            removed: false,
            version: Some(2),
        };
        let printed = format!("{signature:?} {update:?}");
        assert!(!printed.contains(SECRET), "{printed}");
        assert!(printed.contains("\"a\""), "{printed}");
    }

    #[test]
    fn a_removal_has_no_data_and_no_version() {
        let publication = crate::Publication {
            data: b"{}".to_vec(),
            map: crate::subscription::MapPart::of("a".into(), true, 3),
            ..crate::Publication::default()
        };
        assert_eq!(
            SharedPollUpdate::from(publication),
            SharedPollUpdate::removal_of("a".into())
        );
    }

    #[test]
    fn version_0_is_no_version() {
        let publication = crate::Publication {
            data: b"{}".to_vec(),
            map: crate::subscription::MapPart::of("a".into(), false, 0),
            ..crate::Publication::default()
        };
        assert_eq!(SharedPollUpdate::from(publication).version, None);
    }
}
