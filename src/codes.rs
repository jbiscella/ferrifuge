//! Client-side codes carried by events.
//!
//! Values follow centrifuge-js `src/codes.ts`. Server codes use other
//! ranges: disconnect codes 3000-4999, error codes 100-1999.

/// Codes of [`crate::ClientEvent::Connecting`].
pub mod connecting {
    /// `connect()` was called.
    pub const CONNECT_CALLED: u32 = 0;
    /// The transport closed; the client reconnects.
    pub const TRANSPORT_CLOSED: u32 = 1;
    /// No data, not even a ping, arrived from the server in time.
    pub const NO_PING: u32 = 2;
}

/// Codes of [`crate::ClientEvent::Disconnected`].
pub mod disconnected {
    /// `disconnect()` was called.
    pub const DISCONNECT_CALLED: u32 = 0;
    /// The token callback denied access, or an expiring token could not be
    /// refreshed for lack of a callback.
    pub const UNAUTHORIZED: u32 = 1;
}

/// Codes of [`crate::ClientError`].
pub mod error {
    /// The server did not reply to a command in time.
    pub const TIMEOUT: u32 = 1;
    /// The transport failed to open or closed while connecting.
    pub const TRANSPORT_CLOSED: u32 = 2;
    /// The token callback failed while getting a token to connect with.
    pub const CONNECT_TOKEN: u32 = 5;
    /// The token callback failed while getting a token to refresh with.
    pub const REFRESH_TOKEN: u32 = 6;
    /// The client is misconfigured, e.g. a token expires without a callback.
    pub const BAD_CONFIGURATION: u32 = 12;
}
