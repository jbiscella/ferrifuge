//! Step definitions and the world they share.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use cucumber::World;
use ferrifuge::{Client, ClientBuilder, ClientEvent, Config};
use futures_util::{Stream, StreamExt};

use crate::support::centrifugo::Centrifugo;
use crate::support::proxy::ConnectProxy;
use crate::support::relay::CountingRelay;

mod connection;
mod token;

/// How long a step waits for an event it expects.
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a step watches for events that must not happen. Well above the
/// test clients' maximum reconnect delay.
const QUIET_PERIOD: Duration = Duration::from_secs(1);
/// An endpoint nothing listens on: connections are refused at once.
const UNREACHABLE_ENDPOINT: &str = "ws://127.0.0.1:1/connection/websocket";

type Events = Pin<Box<dyn Stream<Item = ClientEvent> + Send>>;

#[derive(Default, World)]
pub(crate) struct ClientWorld {
    /// Started before @A and @B scenarios.
    pub(crate) server: Option<Centrifugo>,
    /// Started before @B scenarios; the server calls it on every connect.
    pub(crate) proxy: Option<ConnectProxy>,
    /// Between the client and the server: counts transport connections.
    pub(crate) relay: Option<CountingRelay>,
    client: Option<Client>,
    events: Option<Events>,
    /// Connection id from the last "connected" event.
    pub(crate) client_id: Option<String>,
    /// Code and reason of the last disconnect the server was asked to send.
    server_disconnect: Option<(u32, String)>,
    /// Code and reason the proxy was programmed to answer with.
    proxy_disconnect: Option<(u32, String)>,
    /// When set, the client's token callback answers "unauthorized".
    pub(crate) token_denied: Arc<AtomicBool>,
}

impl fmt::Debug for ClientWorld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientWorld")
            .field("server", &self.server.is_some())
            .field("proxy", &self.proxy.is_some())
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl ClientWorld {
    /// Creates the scenario's client and starts recording its events.
    fn new_client(&mut self, endpoint: String) {
        self.build_client(Client::builder(endpoint).config(test_config()));
    }

    pub(crate) fn build_client(&mut self, builder: ClientBuilder) {
        let client = builder.build();
        self.events = Some(Box::pin(client.events()));
        self.client = Some(client);
    }

    /// Endpoint of the scenario's server.
    /// Endpoint of the scenario's server, through the counting relay.
    pub(crate) fn server_endpoint(&self) -> String {
        self.relay
            .as_ref()
            .expect("this step needs a server: tag the scenario @A or @B")
            .websocket_url()
    }

    pub(crate) fn client(&self) -> &Client {
        self.client.as_ref().expect("no client created yet")
    }

    async fn next_event(&mut self) -> ClientEvent {
        let events = self.events.as_mut().expect("no client created yet");
        tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .unwrap_or_else(|_| panic!("no event within {EVENT_TIMEOUT:?}"))
            .expect("event stream ended")
    }

    /// Every event emitted during `period`.
    async fn events_during(&mut self, period: Duration) -> Vec<ClientEvent> {
        let events = self.events.as_mut().expect("no client created yet");
        let mut seen = Vec::new();
        let deadline = tokio::time::sleep(period);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                () = &mut deadline => return seen,
                event = events.next() => match event {
                    Some(event) => seen.push(event),
                    None => return seen,
                },
            }
        }
    }
}

/// Waits for the next event and checks its name.
async fn expect_event(world: &mut ClientWorld, expected: &str) -> ClientEvent {
    let event = world.next_event().await;
    assert_eq!(event.name(), expected, "{event:?}");
    event
}

/// Short reconnect delays keep scenarios fast.
pub(crate) fn test_config() -> Config {
    Config {
        min_reconnect_delay: Duration::from_millis(50),
        max_reconnect_delay: Duration::from_millis(200),
        ..Config::default()
    }
}
