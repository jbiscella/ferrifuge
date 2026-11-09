//! Client options.

use std::time::Duration;

/// Options of a client. Defaults follow centrifuge-js.
///
/// Start from [`Config::default`], then set fields by assigning them or
/// through the chainable setters named after them (Block 29):
///
/// ```
/// use std::time::Duration;
/// use ferrifuge::Config;
///
/// let config = Config::default()
///     .timeout(Duration::from_secs(3))
///     .token("a JWT");
/// assert_eq!(config.timeout, Duration::from_secs(3));
/// ```
///
/// Its `Debug` output redacts the token and shows the connect data as its
/// length.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Lower bound of the reconnect delay. Default 500 ms.
    pub min_reconnect_delay: Duration,
    /// Upper bound of the reconnect delay. Default 20 s.
    pub max_reconnect_delay: Duration,
    /// Time allowed for the connect reply once the transport is open, and
    /// for the reply to other commands; also for a call waiting for the
    /// connection, for writing a websocket frame, counted from when it is
    /// queued, and for closing a websocket. Default 5 s.
    ///
    /// Opening the transport has its own [`handshake_timeout`], so one
    /// attempt can take both: 10 s by default, 30 s for a first fallback
    /// sequence over three transports; setting both to 2.5 s keeps the
    /// bound of 5 s per attempt. A call made while the client connects
    /// waits for the connection for at most `timeout`, so on a slow open it
    /// can fail before the attempt does.
    ///
    /// [`handshake_timeout`]: Config::handshake_timeout
    pub timeout: Duration,
    /// Time allowed for opening a transport: DNS, TCP, the proxy
    /// handshake, TLS and the websocket upgrade, or, for `http_stream` and
    /// sse, the stream request until its response headers. An open that
    /// takes longer fails the attempt with "handshake timeout", and the
    /// next transport is tried. Default 5 s.
    pub handshake_timeout: Duration,
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
    /// Most bytes of a frame of several commands, as sent: a batch, or the
    /// subscribe commands of a connect reply, is split in order to stay
    /// within it, and a single larger command goes alone (Block 18). Keep
    /// it below the server's `websocket.message_size_limit` and its HTTP
    /// streaming `max_request_body_size` (both 64 KiB by default in
    /// Centrifugo). At least 1. Default 16 KiB.
    pub batch_frame_bytes: usize,
    /// Largest message accepted from the server: a websocket message
    /// whole (with several replies in it, all of them) and each of its
    /// frames, an `http_stream` line or Protobuf reply, an sse event's data;
    /// also the largest payload a delta may build and the decoded
    /// publications of one reply. The read-ahead still takes one larger
    /// message alone (`read_ahead_bytes`), so this is the client's memory
    /// bound per message: lower it on devices with little memory.
    ///
    /// A larger message before the connect reply fails the attempt, as an
    /// unreachable transport does. After it the client ends: `disconnected`
    /// with code 3 "message size limit exceeded", without reconnecting,
    /// since the server would send the same data again; the application
    /// decides when to connect again. Keep it above the largest reply the
    /// application expects, history and recovery included. An owner of the
    /// [`Machine`](crate::machine::Machine) enforces it where the bytes are read and
    /// reports a larger message with
    /// [`Input::MessageTooLarge`](crate::machine::Input::MessageTooLarge). At least
    /// 1. Default 64 MiB.
    ///
    /// In the browser a websocket message is refused only once the browser
    /// has received it whole: the browser reads the connection, not the
    /// client (Block 44, D6).
    pub max_message_bytes: usize,
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
            handshake_timeout: Duration::from_secs(5),
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
            batch_frame_bytes: 16 << 10,
            max_message_bytes: 64 << 20,
        }
    }
}

/// Chainable setters, one per field and named after it, as tungstenite's
/// `WebSocketConfig` offers: each sets its field, as assigning it does
/// (Block 29, D4). `build()` still checks the values.
impl Config {
    /// Sets [`min_reconnect_delay`](Config::min_reconnect_delay).
    #[must_use]
    pub fn min_reconnect_delay(mut self, min_reconnect_delay: Duration) -> Self {
        self.min_reconnect_delay = min_reconnect_delay;
        self
    }

    /// Sets [`max_reconnect_delay`](Config::max_reconnect_delay).
    #[must_use]
    pub fn max_reconnect_delay(mut self, max_reconnect_delay: Duration) -> Self {
        self.max_reconnect_delay = max_reconnect_delay;
        self
    }

    /// Sets [`timeout`](Config::timeout).
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets [`handshake_timeout`](Config::handshake_timeout).
    #[must_use]
    pub fn handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Sets [`name`](Config::name).
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Sets [`version`](Config::version).
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Sets [`token`](Config::token).
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }

    /// Sets [`data`](Config::data) to `Some(data)`.
    #[must_use]
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Sets [`max_server_ping_delay`](Config::max_server_ping_delay).
    #[must_use]
    pub fn max_server_ping_delay(mut self, max_server_ping_delay: Duration) -> Self {
        self.max_server_ping_delay = max_server_ping_delay;
        self
    }

    /// Sets [`protocol`](Config::protocol).
    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Sets [`reconnect_jitter_window`](Config::reconnect_jitter_window).
    #[must_use]
    pub fn reconnect_jitter_window(mut self, reconnect_jitter_window: Duration) -> Self {
        self.reconnect_jitter_window = reconnect_jitter_window;
        self
    }

    /// Sets [`read_ahead_messages`](Config::read_ahead_messages).
    #[must_use]
    pub fn read_ahead_messages(mut self, read_ahead_messages: usize) -> Self {
        self.read_ahead_messages = read_ahead_messages;
        self
    }

    /// Sets [`read_ahead_bytes`](Config::read_ahead_bytes).
    #[must_use]
    pub fn read_ahead_bytes(mut self, read_ahead_bytes: usize) -> Self {
        self.read_ahead_bytes = read_ahead_bytes;
        self
    }

    /// Sets [`write_queue_bytes`](Config::write_queue_bytes).
    #[must_use]
    pub fn write_queue_bytes(mut self, write_queue_bytes: usize) -> Self {
        self.write_queue_bytes = write_queue_bytes;
        self
    }

    /// Sets [`batch_frame_bytes`](Config::batch_frame_bytes).
    #[must_use]
    pub fn batch_frame_bytes(mut self, batch_frame_bytes: usize) -> Self {
        self.batch_frame_bytes = batch_frame_bytes;
        self
    }

    /// Sets [`max_message_bytes`](Config::max_message_bytes).
    #[must_use]
    pub fn max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
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
            .field("handshake_timeout", &self.handshake_timeout)
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
            .field("batch_frame_bytes", &self.batch_frame_bytes)
            .field("max_message_bytes", &self.max_message_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Block 29, D4: each setter sets its field.
    #[test]
    fn each_setter_sets_its_field() {
        let second = Duration::from_secs(1);
        let set = Config::default()
            .min_reconnect_delay(second)
            .max_reconnect_delay(second * 2)
            .timeout(second * 3)
            .handshake_timeout(second * 4)
            .name("n")
            .version("v")
            .token("t")
            .data(b"d".to_vec())
            .max_server_ping_delay(second * 5)
            .protocol(Protocol::Protobuf)
            .reconnect_jitter_window(second * 6)
            .read_ahead_messages(7)
            .read_ahead_bytes(8)
            .write_queue_bytes(9)
            .batch_frame_bytes(10)
            .max_message_bytes(11);

        // A literal names every field: a new one fails to compile here
        // until it gets a setter above.
        let assigned = Config {
            min_reconnect_delay: second,
            max_reconnect_delay: second * 2,
            timeout: second * 3,
            handshake_timeout: second * 4,
            name: "n".to_owned(),
            version: "v".to_owned(),
            token: "t".to_owned(),
            data: Some(b"d".to_vec()),
            max_server_ping_delay: second * 5,
            protocol: Protocol::Protobuf,
            reconnect_jitter_window: second * 6,
            read_ahead_messages: 7,
            read_ahead_bytes: 8,
            write_queue_bytes: 9,
            batch_frame_bytes: 10,
            max_message_bytes: 11,
        };
        assert_eq!(set, assigned);
    }
}
