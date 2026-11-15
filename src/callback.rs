//! The contexts the application's callbacks are given (Block 54, D10): one
//! `#[non_exhaustive]` struct each, so that a later argument becomes a
//! field, as centrifuge-js's `ConnectionTokenContext` and centrifuge-go's
//! `ConnectionTokenEvent` are.

/// What [`ClientBuilder::get_token`](crate::ClientBuilder::get_token) is
/// given: nothing yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionTokenContext {}

/// What [`ClientBuilder::get_data`](crate::ClientBuilder::get_data) is
/// given: nothing yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionDataContext {}

/// What a subscription's `get_token` callback is given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionTokenContext {
    /// The subscription's channel.
    pub channel: String,
}

/// What a subscription's `get_data` callback is given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionDataContext {
    /// The subscription's channel.
    pub channel: String,
}

/// What a subscription's `get_state` callback is given. Not named
/// `SubscriptionStateContext`, centrifuge-js's state-change event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct GetStateContext {
    /// The subscription's channel.
    pub channel: String,
}

/// What a shared poll subscription's `get_signature` callback is given.
///
/// Unstable: requires the `unstable` feature.
#[cfg(feature = "unstable")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SharedPollSignatureContext {
    /// The keys to sign.
    pub keys: Vec<String>,
}
