//! Block 47e: handlers, the JSON helper and the test constructors of
//! shared poll subscriptions.

use std::sync::{Arc, Mutex};

use cucumber::{then, when};
use ferrifuge::machine::{Action, Input, Machine, SharedPollSubscriptionOptions};
use ferrifuge::{
    Client, Config, SharedPollSubscriptionEvent, SharedPollTrackItem, SharedPollUpdate,
};
use serde_json::{Value, json};

use super::super::delivery::wait_until;
use super::super::maps::{opened_transport, sent_commands};
use super::{ClientWorld, server_api, track_validly};

/// The events a shared poll subscription handler received.
pub(super) type Handled = Arc<Mutex<Vec<SharedPollSubscriptionEvent>>>;

#[cucumber::given(
    regex = r#"^a client with (\w+) delivery whose shared poll subscription to "([^"]+)" has a handler recording its events, subscribed, tracking "(\w+)"$"#
)]
async fn a_shared_poll_handler(
    world: &mut ClientWorld,
    mode: String,
    channel: String,
    key: String,
) {
    let client = Client::builder(world.server_endpoint())
        .config(world.client_config())
        .event_delivery(super::super::handlers::delivery(&mode))
        .build()
        .expect("a valid client configuration");
    let handled = Arc::clone(&world.shared_polls.handled);
    let subscription = client
        .shared_poll_subscription_builder(channel)
        .handler(move |event: SharedPollSubscriptionEvent| {
            handled.lock().unwrap().push(event);
        })
        .build()
        .unwrap();
    client.connect();
    subscription.subscribe();
    world.client = Some(client);
    world.shared_polls.subscription = Some(subscription);
    let handled = Arc::clone(&world.shared_polls.handled);
    wait_until("the handler getting subscribed", || {
        handled
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.name() == "subscribed")
    })
    .await;
    track_validly(world, &[key.as_str()]).await;
}

#[when(regex = r#"^the server API publishes key "(\w+)" at version (\d+)$"#)]
async fn the_server_api_publishes_one(world: &mut ClientWorld, key: String, version: u64) {
    let channel = world.shared_poll().channel().to_owned();
    server_api(world)
        .shared_poll_publish(&channel, &key, version, json!({"v": version}))
        .await
        .expect("server API shared_poll_publish");
}

#[then(
    regex = r#"^the handler has received "subscribing", "subscribed" and an update of "(\w+)" with version (\d+), in this order$"#
)]
async fn the_handler_received(world: &mut ClientWorld, key: String, version: u64) {
    let handled = Arc::clone(&world.shared_polls.handled);
    wait_until("the handler getting three events", || {
        handled.lock().unwrap().len() >= 3
    })
    .await;
    let events = handled.lock().unwrap().clone();
    let names: Vec<&str> = events
        .iter()
        .map(SharedPollSubscriptionEvent::name)
        .collect();
    assert_eq!(names, ["subscribing", "subscribed", "update"], "{events:?}");
    let SharedPollSubscriptionEvent::Update(update) = &events[2] else {
        panic!("{events:?}");
    };
    assert_eq!(update.key, key);
    assert_eq!(update.version, Some(version));
    assert!(!update.removed);
}

#[when(
    regex = r#"^a test builds an update of "(\w+)" with data (\{.*\}) at version (\d+) and a removal of "(\w+)"$"#
)]
fn builds_shared_poll_events(
    world: &mut ClientWorld,
    key: String,
    data: String,
    version: u64,
    removed: String,
) {
    world.shared_polls.built = vec![
        SharedPollSubscriptionEvent::Update(
            SharedPollUpdate::new(key, data.into_bytes()).version(version),
        ),
        SharedPollSubscriptionEvent::Update(SharedPollUpdate::removal(removed)),
    ];
}

#[then(
    regex = r"^the events equal those the client would emit, and the update's json\(\) gives (\{.*\})$"
)]
fn the_built_events_equal(world: &mut ClientWorld, expected: String) {
    let built = world.shared_polls.built.clone();
    let [
        SharedPollSubscriptionEvent::Update(update),
        SharedPollSubscriptionEvent::Update(removal),
    ] = built.as_slice()
    else {
        panic!("{built:?}");
    };
    let version = update.version.expect("a version");
    let emitted = emitted_by_a_client(update, &removal.key, version);

    assert_eq!(emitted, built);
    let value: Value = update.json().expect("the update's JSON");
    assert_eq!(value, serde_json::from_str::<Value>(&expected).unwrap());
}

/// The updates a client's core emits when the server sends `update`'s key
/// at `version` with its data, then the removal of `removed`, both keys
/// tracked.
fn emitted_by_a_client(
    update: &SharedPollUpdate,
    removed: &str,
    version: u64,
) -> Vec<SharedPollSubscriptionEvent> {
    let mut machine = Machine::new(Config::default());
    let id = machine
        .add_shared_poll_subscription("items".into(), SharedPollSubscriptionOptions::default())
        .unwrap();
    let connecting = machine.handle(Input::Connect);
    let transport = opened_transport(&connecting);
    machine.handle(Input::TransportOpened(transport));
    machine.handle(Input::Subscribe(id));
    let frame = |json: String| Input::Frame {
        transport,
        data: json.into_bytes(),
    };
    let subscribe = sent_commands(&machine.handle(frame(
        json!({"id": 1, "connect": {"client": "c"}}).to_string(),
    )))
    .into_iter()
    .find(|command| command.get("subscribe").is_some())
    .expect("a subscribe");
    machine.handle(frame(
        json!({"id": subscribe["id"], "subscribe": {"type": 4}}).to_string(),
    ));
    let track = sent_commands(&machine.handle(Input::TrackKeysWithSignature(
        id,
        vec![
            SharedPollTrackItem::new(update.key.clone(), 0),
            SharedPollTrackItem::new(removed, 0),
        ],
        "sig".into(),
    )))
    .into_iter()
    .find(|command| command.get("sub_refresh").is_some())
    .expect("a track request");
    machine.handle(frame(
        json!({"id": track["id"], "sub_refresh": {}}).to_string(),
    ));
    let data = std::str::from_utf8(&update.data).expect("JSON text");
    let pushed = machine.handle(frame(format!(
        r#"{{"push":{{"channel":"items","pub":{{"key":{},"data":{data},"version":{version}}}}}}}"#,
        json!(update.key)
    )));
    let ended = machine.handle(frame(
        json!({"push": {"channel": "items", "pub": {"key": removed, "removed": true}}}).to_string(),
    ));
    pushed
        .into_iter()
        .chain(ended)
        .filter_map(|action| match action {
            Action::EmitSharedPollSubscription(
                _,
                _,
                event @ SharedPollSubscriptionEvent::Update(_),
            ) => Some(event),
            _ => None,
        })
        .collect()
}
