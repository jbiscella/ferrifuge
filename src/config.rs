//! Client options.

use std::time::Duration;

/// Options of a client. Defaults follow centrifuge-js.
///
/// Its `Debug` output redacts the token and shows the connect data as its
/// length.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    /// Lower bound of the reconnect delay. Default 500 ms.
    pub min_reconnect_delay: Duration,
    /// Upper bound of the reconnect delay. Default 20 s.
    pub max_reconnect_delay: Duration,
    /// Time allowed for a connection attempt, from opening the transport to
    /// the connect reply, and for the reply to other commands; also for
    /// writing a websocket frame, counted from when it is queued, and for
    /// closing a websocket. Default 5 s.
    pub timeout: Duration,
    /// Client name reported to the server, for analytics.
    pub name: String,
    /// Client version reported to the server, for analytics.
    pub version: String,
    /// Connection token (JWT) sent with the first connect. Empty for none.
    pub token: String,
    /// Data sent with every connect command (`ConnectRequest.data`): raw
    /// JSON with the JSON protocol, any bytes with Protobuf. A `get_data`
    /// callback, when set, replaces it on every attempt (Block 17).
    /// Default none.
    pub data: Option<Vec<u8>>,
    /// How long past the server's ping interval to wait for data before
    /// considering the connection broken and reconnecting. Zero disables
    /// the check. Default 10 s.
    pub max_server_ping_delay: Duration,
    /// Encoding of the client protocol, fixed for the client's lifetime.
    /// Default JSON.
    pub protocol: Protocol,
    /// Upper bound of a random delay added to the first reconnect after a
    /// connected client loses its connection, so that a fleet disconnected
    /// at once does not reconnect at once (Block 14). Not applied to
    /// `connect()`. Default 0: no added delay.
    pub reconnect_jitter_window: Duration,
    /// Most messages read from the server and not yet handled, waiting
    /// for the application to take earlier events from its streams; once
    /// reached, the client stops reading the connection, so the server's
    /// slow-client handling acts (Block 15). At least 1. Default 256.
    pub read_ahead_messages: usize,
    /// Most bytes of those messages; a single larger message is still
    /// read when none waits. At least 1. Default 4 MiB.
    pub read_ahead_bytes: usize,
    /// Most bytes of the application's calls (`send` included) handed to
    /// the transport and not yet written; a call that does not fit waits
    /// for room, for at most `timeout`, and a single larger call is sent
    /// when none waits. The client's own commands are not counted. At least
    /// 1. Default 4 MiB.
    pub write_queue_bytes: usize,
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
            data: None,
            max_server_ping_delay: Duration::from_secs(10),
            protocol: Protocol::Json,
            reconnect_jitter_window: Duration::ZERO,
            read_ahead_messages: 256,
            read_ahead_bytes: 4 << 20,
            write_queue_bytes: 4 << 20,
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
            .field(
                "data",
                &self
                    .data
                    .as_ref()
                    .map(|data| format!("<{} bytes>", data.len())),
            )
            .field("max_server_ping_delay", &self.max_server_ping_delay)
            .field("protocol", &self.protocol)
            .field("reconnect_jitter_window", &self.reconnect_jitter_window)
            .field("read_ahead_messages", &self.read_ahead_messages)
            .field("read_ahead_bytes", &self.read_ahead_bytes)
            .field("write_queue_bytes", &self.write_queue_bytes)
            .finish()
    }
}
