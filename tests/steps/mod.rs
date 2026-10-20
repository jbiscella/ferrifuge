//! Step definitions and the world they share.

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use cucumber::World;
use ferrifuge::{Client, ClientEvent, Config};
use futures_util::{Stream, StreamExt};

use crate::support::centrifugo::Centrifugo;
use crate::support::proxy::ConnectProxy;

mod connection;

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
    client: Option<Client>,
    events: Option<Events>,
    /// Connection id from the last "connected" event.
    client_id: Option<String>,
    /// Code and reason of the last disconnect the server was asked to send.
    server_disconnect: Option<(u32, String)>,
    /// Code and reason the proxy was programmed to answer with.
    proxy_disconnect: Option<(u32, String)>,
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
        let client = Client::new(endpoint, test_config());
        self.events = Some(Box::pin(client.events()));
        self.client = Some(client);
    }

    /// Endpoint of the scenario's server.
    fn server_endpoint(&self) -> String {
        self.server
            .as_ref()
            .expect("this step needs a server: tag the scenario @A")
            .websocket_url()
    }

    fn client(&self) -> &Client {
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

/// Short reconnect delays keep scenarios fast.
fn test_config() -> Config {
    Config {
        min_reconnect_delay: Duration::from_millis(50),
        max_reconnect_delay: Duration::from_millis(200),
        ..Config::default()
    }
}
