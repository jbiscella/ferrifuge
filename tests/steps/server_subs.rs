//! Steps of Block 12, server-side subscriptions.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{ClientEvent, ServerSubscriptionEvent, SubscriptionEvent, SubscriptionState};
use serde_json::{Value, json};

use super::subscriptions::{create, has_json_data};
use super::transports::{configure, connect_and_wait, kind, server, server_url};
use super::{ClientWorld, EVENT_TIMEOUT, expect_event};

/// How long a step waits to be sure an event does not come.
const QUIET: Duration = Duration::from_millis(500);

/// State of a Block 12 scenario.
#[derive(Debug, Default)]
pub(crate) struct ServerSide {
    /// Data of the publication the client must receive next.
    expected: Option<Value>,
}

#[given(regex = r#"^the connect proxy subscribes the connection to channel "(\w+)"$"#)]
fn proxy_grants(world: &mut ClientWorld, channel: String) {
    proxy(world).grant(&[&channel]);
}

#[given("the connect proxy will subscribe the next connection to no channel")]
fn proxy_grants_nothing(world: &mut ClientWorld) {
    proxy(world).grant(&[]);
}

#[given(
    regex = r#"^the fake server grants a server-side subscription to channel "(\w+)" at connect$"#
)]
fn fake_grants(world: &mut ClientWorld, channel: String) {
    world
        .fake()
        .grant_server_subscriptions(json!({ channel: {} }));
}

#[given(
    regex = r#"^the fake server grants a server-side subscription to channel "(\w+)" with 1 initial publication and recovered false at connect$"#
)]
fn fake_grants_with_a_publication(world: &mut ClientWorld, channel: String) {
    let data = json!({"initial": 1});
    // `recovered` false is left out, as the server does.
    world.fake().grant_server_subscriptions(json!({
        channel: {"publications": [{"data": data, "offset": 1}]},
    }));
    world.server_side.expected = Some(data);
}

#[given(regex = r#"^a client with an unsubscribed client-side subscription to channel "(\w+)"$"#)]
fn client_with_unsubscribed_subscription(world: &mut ClientWorld, channel: String) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    create(world, &channel);
}

#[given("the client is connected to the fake server")]
async fn client_is_connected(world: &mut ClientWorld) {
    connect(world).await;
}

#[when("a client connects to the fake server")]
async fn a_client_connects(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    connect(world).await;
}

#[when(regex = r#"^a client connects over transport "(\w+)"$"#)]
async fn a_client_connects_over(world: &mut ClientWorld, transport: String) {
    let kind = kind(&transport);
    let url = server_url(server(world), kind);
    configure(world, vec![(kind, url)]);
    connect_and_wait(world).await;
}

#[when(regex = r#"^the fake server pushes a publication on channel "(\w+)"$"#)]
fn fake_pushes_publication(world: &mut ClientWorld, channel: String) {
    let data = json!({"pushed": 1});
    push_publication(world, &channel, &data);
    world.server_side.expected = Some(data);
}

#[when("the client-side subscription is subscribed")]
fn client_side_subscribes(world: &mut ClientWorld) {
    world.subscription().subscribe();
}

#[then(regex = r#"^(?:then )?the client emits a server-side "(\w+)" event for channel "(\w+)"$"#)]
async fn emits_server_side_event(world: &mut ClientWorld, name: String, channel: String) {
    let event = next_server_side_event(world, &channel).await;
    assert_eq!(event.name(), name, "{event:?}");
}

#[then(
    regex = r#"^the client receives a server-side publication on channel "(\w+)" with data (\{.*\})$"#
)]
async fn receives_server_side_publication(world: &mut ClientWorld, channel: String, data: String) {
    expect_server_side_publication(world, &channel, &data).await;
}

#[then("the client receives it as a server-side publication")]
async fn receives_it_as_server_side(world: &mut ClientWorld) {
    let data = expected(world);
    expect_server_side_publication(world, "alerts", &data).await;
}

#[then(regex = r"^(?:then )?the client receives that publication as a server-side publication$")]
async fn receives_that_publication(world: &mut ClientWorld) {
    let data = expected(world);
    expect_server_side_publication(world, "alerts", &data).await;
}

#[then(r#"the client-side subscription receives nothing and stays "unsubscribed""#)]
async fn client_side_receives_nothing(world: &mut ClientWorld) {
    let events = world.subscription_events_during(QUIET).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{events:?}"
    );
    assert_eq!(
        world.subscription().state(),
        SubscriptionState::Unsubscribed
    );
}

#[then(r#"it reaches state "subscribed""#)]
async fn it_reaches_subscribed(world: &mut ClientWorld) {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        while !matches!(
            world.next_subscription_event(false).await,
            SubscriptionEvent::Subscribed(_)
        ) {}
    })
    .await
    .expect("subscribed in time");
}

#[then(regex = r#"^the next publication on channel "(\w+)" goes to the client-side subscription$"#)]
async fn next_publication_goes_client_side(world: &mut ClientWorld, channel: String) {
    let data = json!({"pushed": 2});
    push_publication(world, &channel, &data);
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let SubscriptionEvent::Publication(publication) =
                world.next_subscription_event(false).await
            {
                return publication;
            }
        }
    })
    .await
    .expect("a client-side publication in time");
    assert!(has_json_data(&received, &data.to_string()), "{received:?}");
    let events = world.events_during(QUIET).await;
    assert!(
        !events.iter().any(|event| matches!(
            event,
            ClientEvent::ServerSubscription {
                event: ServerSubscriptionEvent::Publication(_),
                ..
            }
        )),
        "{events:?}"
    );
}

#[then(regex = r#"^the server-side registry still lists channel "(\w+)"$"#)]
fn registry_lists(world: &mut ClientWorld, channel: String) {
    let channels = world.client().server_subscriptions();
    assert!(channels.contains(&channel), "{channels:?}");
}

fn proxy(world: &ClientWorld) -> &crate::support::proxy::ProxyBackend {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
}

fn expected(world: &ClientWorld) -> String {
    world
        .server_side
        .expected
        .as_ref()
        .expect("a publication the scenario made")
        .to_string()
}

/// Connects the scenario's client to the fake server and waits until it is
/// connected.
async fn connect(world: &mut ClientWorld) {
    world.client().connect();
    expect_event(world, "connecting").await;
    let ClientEvent::Connected { client_id, .. } = expect_event(world, "connected").await else {
        unreachable!("expect_event checked the name");
    };
    world.client_id = Some(client_id);
}

fn push_publication(world: &ClientWorld, channel: &str, data: &Value) {
    world
        .fake()
        .send(&json!({"push": {"channel": channel, "pub": {"data": data}}}).to_string());
}

/// The next event of the server-side subscription to `channel`, within
/// [`EVENT_TIMEOUT`]; other events are skipped.
async fn next_server_side_event(world: &mut ClientWorld, channel: &str) -> ServerSubscriptionEvent {
    tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let ClientEvent::ServerSubscription { channel: of, event } = world.next_event().await
                && of == channel
            {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no server-side event for {channel} within {EVENT_TIMEOUT:?}"))
}

async fn expect_server_side_publication(world: &mut ClientWorld, channel: &str, data: &str) {
    match next_server_side_event(world, channel).await {
        ServerSubscriptionEvent::Publication(publication) => {
            assert!(has_json_data(&publication, data), "{publication:?}");
        }
        other => panic!("expected a server-side publication, got {other:?}"),
    }
}
