//! Client options.

use std::time::Duration;

/// Options of a client. Defaults follow centrifuge-js.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        }
    }
}
