//! Client driven by tokio.

use futures_core::Stream;

use crate::{ClientEvent, Config, State};

/// Centrifugo client running on tokio.
#[derive(Debug)]
pub struct Client {
    _endpoint: String,
    _config: Config,
}

impl Client {
    /// Creates a disconnected client for the websocket `endpoint`, e.g.
    /// `ws://localhost:8000/connection/websocket`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, config: Config) -> Self {
        Self {
            _endpoint: endpoint.into(),
            _config: config,
        }
    }

    /// Current connection state.
    #[must_use]
    pub fn state(&self) -> State {
        State::Disconnected
    }

    /// Events from now on. Each call returns an independent stream.
    pub fn events(&self) -> impl Stream<Item = ClientEvent> + Send + Unpin + 'static {
        futures_util::stream::pending()
    }

    /// Starts connecting; reconnects until connected or disconnected.
    pub fn connect(&self) {}

    /// Disconnects and stops reconnecting.
    pub fn disconnect(&self) {}
}
