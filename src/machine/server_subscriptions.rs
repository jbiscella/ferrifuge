//! Server-side subscriptions (Block 12): the ones the server made for the
//! connection, which the client tracks in a registry of their own and
//! reports through [`crate::ClientEvent::ServerSubscription`], but does not
//! control (spec, "Server-side subscriptions").

use std::collections::BTreeSet;

use super::Machine;

/// The registry of server-side subscriptions: their channels, until they
/// carry stream positions (12c).
#[derive(Debug, Default)]
pub(super) struct ServerSubscriptions {
    channels: BTreeSet<String>,
}

impl Machine {
    /// The channels of the server-side subscriptions in the registry, in
    /// channel order.
    #[must_use]
    pub fn server_subscriptions(&self) -> Vec<String> {
        self.server_subscriptions.channels.iter().cloned().collect()
    }
}
