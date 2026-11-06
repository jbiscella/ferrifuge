//! Step definitions and the world they share.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use cucumber::World;
use ferrifuge::machine::TransportKind;
use ferrifuge::{
    Client, ClientBuilder, ClientError, ClientEvent, Config, ErrorKind, Protocol, Subscription,
    SubscriptionError, SubscriptionEvent,
};
use futures_util::{FutureExt, Stream, StreamExt};

use crate::support::centrifugo::Centrifugo;
use crate::support::fake_server::{Announce, FakeServer};
use crate::support::http_proxy::HttpProxy;
use crate::support::proxy::ProxyBackend;
use crate::support::relay::CountingRelay;

mod backoff;
mod batching;
mod calls;
mod codes;
mod connect_data;
mod connection;
mod delivery;
mod jitter;
mod ping;
mod protobuf;
mod recovery;
mod server_subs;
mod slow;
mod sub_tokens;
mod subscriptions;
mod tls;
mod token;
mod transports;

/// How long a step waits for an event it expects.
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a step watches for events that must not happen. Well above the
/// test clients' maximum reconnect delay.
const QUIET_PERIOD: Duration = Duration::from_secs(1);
/// An endpoint nothing listens on: connections are refused at once.
pub(crate) const UNREACHABLE_ENDPOINT: &str = "ws://127.0.0.1:1/connection/websocket";

type Events = Pin<Box<dyn Stream<Item = ClientEvent> + Send>>;

#[derive(Default, World)]
pub(crate) struct ClientWorld {
    /// Started before @A and @B scenarios.
    pub(crate) server: Option<Centrifugo>,
    /// Started before @B scenarios; the server calls it on every connect.
    pub(crate) proxy: Option<ProxyBackend>,
    /// Started before @D scenarios.
    pub(crate) fake: Option<FakeServer>,
    /// Between the client and the server: counts transport connections.
    pub(crate) relay: Option<CountingRelay>,
    pub(crate) client: Option<Client>,
    events: Option<Events>,
    /// Events of the client a step looked at and put back, for the next
    /// step to see first.
    pub(crate) put_back_events: std::collections::VecDeque<ClientEvent>,
    /// Connection id from the last "connected" event.
    pub(crate) client_id: Option<String>,
    /// Code and reason of the last disconnect the server was asked to send.
    server_disconnect: Option<(u32, String)>,
    /// Code and reason the proxy was programmed to answer with.
    proxy_disconnect: Option<(u32, String)>,
    /// Code and reason of the disconnect a Block 3 step made the server send.
    pub(crate) sent_disconnect: Option<(u32, String)>,
    /// Code and reason of the unsubscribe push the fake server sent.
    pub(crate) sent_unsubscribe: Option<(u32, String)>,
    /// The machine of an @E resubscribe scenario that spans several steps.
    pub(crate) resubscribe_run: Option<backoff::ResubscribeRun>,
    /// The resubscribe delay a step read last.
    pub(crate) last_delay: Option<Duration>,
    /// The state event that answered it.
    pub(crate) state_event: Option<ClientEvent>,
    /// Backoff options of an @E scenario.
    pub(crate) backoff: Option<backoff::UnderTest>,
    /// Delays computed by an @E scenario.
    pub(crate) computed_delays: Vec<Duration>,
    /// Whether `computed_delays` holds one delay per consecutive attempt.
    pub(crate) delays_by_attempt: bool,
    /// Delays sampled by the jitter scenario, per attempt.
    pub(crate) jitter_samples: Vec<(usize, Vec<Duration>)>,
    /// Estimated upper bound of the delay, per attempt.
    pub(crate) upper_bounds: Vec<Duration>,
    /// Client option set by a step before the client is created.
    pub(crate) max_server_ping_delay: Option<Duration>,
    /// Encoding of the clients the scenario creates.
    pub(crate) protocol: Protocol,
    /// Events seen while a step let time elapse.
    pub(crate) elapsed_events: Vec<ClientEvent>,
    /// Transport connections opened while a step let time elapse.
    pub(crate) elapsed_new_connections: usize,
    /// When set, the client's token callback answers "unauthorized".
    pub(crate) token_denied: Arc<AtomicBool>,
    /// Transports the scenario's client may report errors for; websocket
    /// when empty.
    pub(crate) transports: Vec<TransportKind>,
    /// Between the client and the server's HTTP endpoints, for @B scenarios
    /// that make an emulation request fail.
    pub(crate) http_proxy: Option<HttpProxy>,
    /// A client being configured by a step, built by a later one.
    pub(crate) pending_builder: Option<ClientBuilder>,
    /// Why building the pending client failed.
    pub(crate) build_error: Option<ClientError>,
    /// Transports opened by an @E scenario's machine, each with the reconnect
    /// delay it waited for (zero when opened at once).
    pub(crate) opened: Vec<(TransportKind, Duration)>,
    /// The scenario's subscription, and its events.
    pub(crate) subscription: Option<Subscription>,
    subscription_events: Option<SubscriptionEvents>,
    /// Events of the scenario's subscription a step looked at and put
    /// back, for the next step to see first.
    pub(crate) put_back: std::collections::VecDeque<SubscriptionEvent>,
    /// A second subscription to the same channel, created after the first
    /// was removed, and its events.
    pub(crate) new_subscription: Option<Subscription>,
    new_subscription_events: Option<SubscriptionEvents>,
    /// Further subscriptions, by channel.
    pub(crate) named_subscriptions: Vec<Subscription>,
    /// Why creating a subscription failed.
    pub(crate) duplicate: Option<SubscriptionError>,
    /// Channels the data callback was called with, in order.
    pub(crate) data_calls: Arc<std::sync::Mutex<Vec<String>>>,
    /// Channels the subscription token callback was called with, in order.
    pub(crate) token_calls: Arc<std::sync::Mutex<Vec<String>>>,
    /// Stalled websocket writes counted when the fake server stopped
    /// reading, before the scenario's own.
    pub(crate) stalled_writes: usize,
    /// The publication a step looked at last.
    pub(crate) last_publication: Option<ferrifuge::Publication>,
    /// Raw data a fake server pushed in a publication.
    pub(crate) last_raw_data: Option<Vec<u8>>,
    /// Offset the server API reported for the last publication.
    pub(crate) published_offset: Option<u64>,
    /// When the unsubscribe the HTTP proxy delays was requested.
    pub(crate) unsubscribed_at: Option<std::time::Instant>,
    /// The sans-IO core of an @E subscription scenario.
    pub(crate) subscription_machine: Option<subscriptions::MachineRun>,
    /// The sans-IO core of an @E reconnect jitter scenario.
    pub(crate) jitter: Option<jitter::JitterRun>,
    /// Publications and positions of a Block 11 scenario.
    pub(crate) recovery: recovery::Recovery,
    /// Expected publications of a Block 12 scenario.
    pub(crate) server_side: server_subs::ServerSide,
    /// Streams a Block 15 scenario reads itself.
    pub(crate) delivery: delivery::Delivery,
    /// The plan of a Block 15d scenario.
    pub(crate) slow: Option<slow::SlowPlan>,
    /// Calls of a Block 13 scenario.
    pub(crate) calls: calls::Calls,
    /// Connect data of a Block 17 scenario.
    pub(crate) connect_data: connect_data::ConnectData,
    /// Batches of a Block 18 scenario.
    pub(crate) batching: batching::Batching,
    /// The relay standing in for an unreachable websocket endpoint, and
    /// its connection attempts once the client connected (review item C5).
    pub(crate) websocket_relay: Option<(CountingRelay, usize)>,
}

type SubscriptionEvents = Pin<Box<dyn Stream<Item = SubscriptionEvent> + Send>>;

/// Takes every event of `stream` as soon as it comes, as an application
/// reading at once would, and keeps them for the steps: a stream that a
/// step leaves unread must not hold back delivery (Block 15).
fn recorded<E: Send + 'static>(
    mut stream: impl Stream<Item = E> + Send + Unpin + 'static,
) -> Pin<Box<dyn Stream<Item = E> + Send>> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            if sender.send(event).is_err() {
                return;
            }
        }
    });
    Box::pin(futures_util::stream::poll_fn(move |cx| {
        receiver.poll_recv(cx)
    }))
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
        let config = self.client_config();
        let builder = self
            .connect_data
            .build(Client::builder(endpoint).config(config));
        self.build_client(builder);
    }

    /// Options of the clients the scenario creates.
    fn client_config(&self) -> Config {
        let mut config = test_config();
        if let Some(delay) = self.max_server_ping_delay {
            config.max_server_ping_delay = delay;
        }
        self.delivery.configure(&mut config);
        if let Some(bytes) = self.calls.write_queue_bytes {
            config.write_queue_bytes = bytes;
        }
        config.protocol = self.protocol;
        self.connect_data.configure(&mut config);
        config
    }

    /// Replaces the scenario's fake server with one announcing `announce`.
    pub(crate) async fn restart_fake(&mut self, announce: Announce) {
        let fake = FakeServer::start(announce).await;
        self.relay = Some(CountingRelay::start(fake.address()).await);
        self.fake = Some(fake);
    }

    pub(crate) fn fake(&self) -> &FakeServer {
        self.fake
            .as_ref()
            .expect("this step needs the fake server: tag the scenario @D")
    }

    pub(crate) fn build_client(&mut self, builder: ClientBuilder) {
        let client = builder.build().expect("a valid client configuration");
        self.events = Some(recorded(client.events()));
        self.client = Some(client);
    }

    /// Endpoint of the scenario's server.
    /// Endpoint of the scenario's server, through the counting relay.
    pub(crate) fn server_endpoint(&self) -> String {
        self.relay
            .as_ref()
            .expect("this step needs a server: tag the scenario @A, @B or @D")
            .websocket_url()
    }

    /// Makes `subscription` the scenario's, recording its events.
    pub(crate) fn track_subscription(&mut self, subscription: Subscription) {
        self.subscription_events = Some(recorded(subscription.events()));
        self.subscription = Some(subscription);
    }

    /// Makes `subscription` the scenario's new one, recording its events.
    pub(crate) fn track_new_subscription(&mut self, subscription: Subscription) {
        self.new_subscription_events = Some(recorded(subscription.events()));
        self.new_subscription = Some(subscription);
    }

    pub(crate) fn subscription(&self) -> &Subscription {
        self.subscription
            .as_ref()
            .expect("no subscription created yet")
    }

    /// The next event of the scenario's subscription (or, with `new`, of the
    /// new one), within [`EVENT_TIMEOUT`].
    pub(crate) async fn next_subscription_event(&mut self, new: bool) -> SubscriptionEvent {
        if !new && let Some(event) = self.put_back.pop_front() {
            return event;
        }
        let events = if new {
            self.new_subscription_events.as_mut()
        } else {
            self.subscription_events.as_mut()
        }
        .expect("no subscription created yet");
        tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .unwrap_or_else(|_| panic!("no subscription event within {EVENT_TIMEOUT:?}"))
            .expect("subscription event stream ended")
    }

    /// Subscription events arriving within `period`.
    pub(crate) async fn subscription_events_during(
        &mut self,
        period: Duration,
    ) -> Vec<SubscriptionEvent> {
        let events = self
            .subscription_events
            .as_mut()
            .expect("no subscription created yet");
        let mut seen = Vec::new();
        let _ = tokio::time::timeout(period, async {
            while let Some(event) = events.next().await {
                seen.push(event);
            }
        })
        .await;
        seen
    }

    pub(crate) fn client(&self) -> &Client {
        self.client.as_ref().expect("no client created yet")
    }

    pub(crate) async fn next_event(&mut self) -> ClientEvent {
        if let Some(event) = self.put_back_events.pop_front() {
            return event;
        }
        let events = self.events.as_mut().expect("no client created yet");
        tokio::time::timeout(EVENT_TIMEOUT, events.next())
            .await
            .unwrap_or_else(|_| panic!("no event within {EVENT_TIMEOUT:?}"))
            .expect("event stream ended")
    }

    /// Every event already emitted and not yet consumed.
    fn ready_events(&mut self) -> Vec<ClientEvent> {
        let events = self.events.as_mut().expect("no client created yet");
        let mut seen = Vec::new();
        while let Some(Some(event)) = events.next().now_or_never() {
            seen.push(event);
        }
        seen
    }

    /// Every event emitted during `period`.
    pub(crate) async fn events_during(&mut self, period: Duration) -> Vec<ClientEvent> {
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

/// The next event that is not a transport error, within [`EVENT_TIMEOUT`]:
/// failed attempts, e.g. of a fallback, may come first, and keep coming
/// while the client retries. Before the client's first connection, those
/// attempts must follow its configured transports in order, starting over
/// while it retries, and a `connected` event must name the transport that
/// comes next (Block 8; review item D5).
pub(crate) async fn next_event_after_transport_errors(world: &mut ClientWorld) -> ClientEvent {
    let mut failed = Vec::new();
    let event = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match world.next_event().await {
                ClientEvent::Error(error) if error.kind == ErrorKind::Transport => {
                    failed.push(error.transport.unwrap_or_default());
                }
                event => return event,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("only transport errors for {EVENT_TIMEOUT:?}"));
    if world.client_id.is_none() {
        let configured: Vec<&str> = if world.transports.is_empty() {
            vec![TransportKind::WebSocket.name()]
        } else {
            world.transports.iter().map(|kind| kind.name()).collect()
        };
        let expected: Vec<&str> = (0..failed.len())
            .map(|attempt| configured[attempt % configured.len()])
            .collect();
        assert_eq!(
            failed, expected,
            "failed attempts out of the configured order"
        );
        if let ClientEvent::Connected { transport, .. } = &event {
            assert_eq!(
                transport,
                configured[failed.len() % configured.len()],
                "connected over a transport out of order after {failed:?}"
            );
        }
    }
    event
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
