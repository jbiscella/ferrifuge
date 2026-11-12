//! Shared poll subscriptions (Block 47): the core's entry points, before
//! their behaviour. A shared poll subscription is registered as a stream
//! subscription until 47a's green step.
#![allow(clippy::unused_self, clippy::needless_pass_by_value)] // Stubs.

use super::super::{Machine, SharedPollSubscriptionOptions, SubscriptionId};
use crate::wire::{Command, Reply};
use crate::{Publication, SubscriptionError, SubscriptionEvent};

/// What makes a shared poll subscription one.
#[derive(Debug)]
pub(in crate::machine) struct SharedPollFlow;

/// A track or untrack request awaiting its reply.
#[derive(Debug)]
pub(in crate::machine) struct TrackCommand(SubscriptionId);

impl TrackCommand {
    /// Whether it is a request of subscription `id`.
    pub(in crate::machine) fn of(&self, id: SubscriptionId) -> bool {
        self.0 == id
    }
}

impl SharedPollFlow {
    /// `unsubscribe()`: the keys and signatures go with it.
    pub(in crate::machine) fn forget(&mut self) {}
}

impl Machine {
    /// Adds a shared poll subscription to `channel`, unsubscribed, to the
    /// registry (Block 47, D3), which it shares with the other kinds.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    ///
    /// # Errors
    ///
    /// When the registry holds a subscription to `channel` already, of any
    /// kind.
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    pub fn add_shared_poll_subscription(
        &mut self,
        channel: String,
        options: SharedPollSubscriptionOptions,
    ) -> Result<SubscriptionId, SubscriptionError> {
        self.add_subscription(channel, options.common)
    }

    /// Whether `id` is a shared poll subscription.
    pub(crate) fn is_shared_poll(&self, _id: SubscriptionId) -> bool {
        false
    }

    /// The keys shared poll subscription `id` tracks.
    #[cfg_attr(not(any(adapter_tokio, adapter_browser)), allow(dead_code))] // Read by the adapters.
    pub(crate) fn shared_poll_tracked_keys(&self, _id: SubscriptionId) -> Option<Vec<String>> {
        None
    }

    /// An application call or a callback answer of a shared poll
    /// subscription.
    pub(in crate::machine) fn shared_poll_input(&mut self, _input: super::super::Input) {}

    pub(super) fn shared_poll_subscribe_request(
        &mut self,
        _id: SubscriptionId,
        _command_id: u32,
        _data: Option<Vec<u8>>,
    ) -> Option<Command> {
        None
    }

    pub(super) fn shared_poll_subscribe_reply(&mut self, _id: SubscriptionId, _reply: Reply) {}

    pub(in crate::machine) fn owns_track_command(&self, _command: u32) -> bool {
        false
    }

    pub(super) fn track_command_reply(&mut self, _reply: Reply) {}

    pub(super) fn shared_poll_publication(
        &mut self,
        _id: SubscriptionId,
        _publication: Publication,
    ) {
    }

    pub(in crate::machine) fn shared_poll_version_taken(
        &mut self,
        _id: SubscriptionId,
        _key: &str,
        _version: u64,
    ) {
    }

    pub(super) fn emit_shared_poll_lifecycle(
        &mut self,
        _id: SubscriptionId,
        _event: SubscriptionEvent,
    ) {
    }
}
