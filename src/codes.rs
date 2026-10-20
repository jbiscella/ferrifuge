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
}

/// Codes of [`crate::ClientEvent::Disconnected`].
pub mod disconnected {
    /// `disconnect()` was called.
    pub const DISCONNECT_CALLED: u32 = 0;
}

/// Codes of [`crate::ClientError`].
pub mod error {
    /// The transport failed to open or closed while connecting.
    pub const TRANSPORT_CLOSED: u32 = 2;
}
