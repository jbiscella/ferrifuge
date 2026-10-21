//! Client options.

use std::time::Duration;

/// Options of a client. Defaults follow centrifuge-js.
///
/// Its `Debug` output redacts the token.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    /// Lower bound of the reconnect delay. Default 500 ms.
    pub min_reconnect_delay: Duration,
    /// Upper bound of the reconnect delay. Default 20 s.
    pub max_reconnect_delay: Duration,
    /// Time allowed for a connection attempt, from opening the transport to
    /// the connect reply, and for the reply to other commands. Default 5 s.
    pub timeout: Duration,
    /// Client name reported to the server, for analytics.
    pub name: String,
    /// Client version reported to the server, for analytics.
    pub version: String,
    /// Connection token (JWT) sent with the first connect. Empty for none.
    pub token: String,
    /// How long past the server's ping interval to wait for data before
    /// considering the connection broken and reconnecting. Zero disables
    /// the check. Default 10 s.
    pub max_server_ping_delay: Duration,
    /// Encoding of the client protocol, fixed for the client's lifetime.
    /// Default JSON.
    pub protocol: Protocol,
}

/// Encoding of the client protocol (`client.proto`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// JSON, in text frames.
    #[default]
    Json,
    /// Protobuf, in binary frames. Publication and other payloads can then
    /// be any bytes, not only JSON.
    Protobuf,
}

impl Protocol {
    /// The websocket subprotocol a transport requests for this encoding;
    /// `None` for JSON, which needs none.
    #[must_use]
    pub fn websocket_subprotocol(self) -> Option<&'static str> {
        match self {
            Self::Json => None,
            Self::Protobuf => Some("centrifuge-protobuf"),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_reconnect_delay: Duration::from_millis(500),
            max_reconnect_delay: Duration::from_secs(20),
            timeout: Duration::from_secs(5),
            name: "ferrifuge".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            token: String::new(),
            max_server_ping_delay: Duration::from_secs(10),
            protocol: Protocol::Json,
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct Token<'a>(&'a str);
        impl std::fmt::Debug for Token<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                crate::token::redacted(self.0, f)
            }
        }
        f.debug_struct("Config")
            .field("min_reconnect_delay", &self.min_reconnect_delay)
            .field("max_reconnect_delay", &self.max_reconnect_delay)
            .field("timeout", &self.timeout)
            .field("name", &self.name)
            .field("version", &self.version)
            .field("token", &Token(&self.token))
            .field("max_server_ping_delay", &self.max_server_ping_delay)
            .field("protocol", &self.protocol)
            .finish()
    }
}
