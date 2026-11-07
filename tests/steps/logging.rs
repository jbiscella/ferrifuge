//! Steps of Block 21, logging.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, Anomaly, Input, TransportKind};
use ferrifuge::{Client, ClientEvent, Config, Subscription, SubscriptionEvent, codes};
use futures_util::StreamExt;
use serde_json::json;
use tracing::Level;

use super::batching::{connected_run, run};

use super::delivery::wait_until;
use super::{ClientWorld, expect_event, test_config};
use crate::support::fake_server::{Announce, FakeServer, Rpc};
use crate::support::logs::{self, Record};

/// What a Block 21 scenario holds.
#[derive(Default)]
pub(crate) struct Logging {
    /// Where the actions of the step under test start.
    mark: usize,
    /// The call of an @E scenario.
    call: Option<ferrifuge::machine::CallId>,
    /// Index of the anomaly a step found among the actions.
    anomaly: Option<usize>,
    /// The numbers of the clients whose records are captured, in order.
    clients: Vec<u64>,
    /// Index of the record the last step found, among its client's.
    last: Option<usize>,
    /// The secrets the scenario gave the client and the fake server.
    secrets: Vec<String>,
    /// The subscription a step made, with its stream.
    subscription: Option<(
        Subscription,
        Box<dyn futures_util::Stream<Item = SubscriptionEvent> + Send + Unpin>,
    )>,
    /// The second client and its fake server.
    second: Option<(Client, FakeServer)>,
}

#[given("a machine connected over a websocket transport")]
fn a_connected_machine(world: &mut ClientWorld) {
    world.batching.run = Some(connected_run(Config::default()));
    world.logging.mark = run(world).actions.len();
}

#[given("a machine connected over a websocket transport, with an RPC call sent")]
fn a_connected_machine_with_a_call(world: &mut ClientWorld) {
    a_connected_machine(world);
    world.logging.call = Some(run(world).rpc("rpc"));
    world.logging.mark = run(world).actions.len();
}

#[when(regex = r"^it is given a frame of (\d+) bytes that is not client protocol data$")]
fn given_a_frame_that_is_no_data(world: &mut ClientWorld, bytes: usize) {
    let data = b"not-json-7f3a".to_vec();
    assert_eq!(data.len(), bytes, "the scenario's frame length");
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.handle(Input::Frame { transport, data });
}

#[when("it is given the reply to that call carrying a history result")]
fn given_a_reply_of_another_kind(world: &mut ClientWorld) {
    let run = run(world);
    let id = run
        .frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .find(|command| command["rpc"].is_object())
        .and_then(|command| command["id"].as_u64())
        .expect("the RPC command");
    let transport = run.transport.expect("a transport");
    let reply = json!({"id": id, "history": {"publications": [], "epoch": "e", "offset": 0}});
    run.handle(Input::Frame {
        transport,
        data: reply.to_string().into_bytes(),
    });
}

/// The index, among all actions, of the one anomaly reported since the
/// step that set the mark, checked by `expected`.
fn anomaly(world: &mut ClientWorld, expected: impl Fn(&Anomaly) -> bool) -> usize {
    let mark = world.logging.mark;
    let run = run(world);
    let anomalies: Vec<(usize, &Anomaly)> = run
        .actions
        .iter()
        .enumerate()
        .skip(mark)
        .filter_map(|(index, action)| match action {
            Action::Anomaly(anomaly) => Some((index, anomaly)),
            _ => None,
        })
        .collect();
    assert!(
        matches!(anomalies.as_slice(), [(_, anomaly)] if expected(anomaly)),
        "{anomalies:?}"
    );
    anomalies[0].0
}

#[then(
    regex = r"^its actions report an anomaly naming that transport and a length of (\d+) bytes$"
)]
fn reports_an_undecodable_frame(world: &mut ClientWorld, length: usize) {
    let transport = run(world).transport.expect("a transport");
    world.logging.anomaly = Some(anomaly(world, |anomaly| {
        *anomaly
            == Anomaly::UndecodableFrame {
                transport,
                bytes: length,
            }
    }));
}

#[then(
    regex = r#"^its actions report an anomaly naming that transport and the command kind "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn reports_a_reply_without_result(world: &mut ClientWorld, kind: String) {
    let transport = run(world).transport.expect("a transport");
    world.logging.anomaly = Some(anomaly(world, |anomaly| {
        *anomaly
            == Anomaly::ReplyWithoutResult {
                transport,
                command: match kind.as_str() {
                    "rpc" => "rpc",
                    other => panic!("a command kind of the scenarios, not {other}"),
                },
            }
    }));
}

/// The actions after the anomaly the previous step found.
fn after_the_anomaly(world: &mut ClientWorld) -> Vec<Action> {
    let from = world.logging.anomaly.expect("an anomaly reported") + 1;
    run(world).actions[from..].to_vec()
}

fn emits_connecting(actions: &[Action], code: u32, reason: &str) -> bool {
    actions.iter().any(|action| {
        matches!(action, Action::Emit(_, ClientEvent::Connecting { code: of, reason: why })
            if *of == code && why == reason)
    })
}

#[then(
    regex = r#"^then they close the transport and emit "connecting" with code (\d+) and reason "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn then_they_reconnect(world: &mut ClientWorld, code: u32, reason: String) {
    let transport = run(world).transport;
    let actions = after_the_anomaly(world);
    assert!(
        actions
            .iter()
            .any(|action| Some(action) == transport.map(Action::CloseTransport).as_ref()),
        "{actions:?}"
    );
    assert!(emits_connecting(&actions, code, &reason), "{actions:?}");
}

#[then(
    regex = r#"^then the call fails with a "connection closed" error and the machine emits "connecting" with code (\d+)$"#
)]
fn then_the_call_fails(world: &mut ClientWorld, code: u32) {
    let call = world.logging.call.expect("a call sent");
    let result = run(world).settled.get(&call).cloned();
    assert!(
        matches!(&result, Some(Err(error)) if error.code == codes::error::CONNECTION_CLOSED),
        "{result:?}"
    );
    let actions = after_the_anomaly(world);
    assert!(
        actions.iter().any(|action| matches!(action,
            Action::Emit(_, ClientEvent::Connecting { code: of, .. }) if *of == code)),
        "{actions:?}"
    );
}

/// A string no scenario's text holds, distinct per call: a match in a
/// record cannot be a coincidence.
fn secret(what: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |now| now.subsec_nanos());
    format!(
        "{what}-{:x}{nanos:x}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// `text` as a JSON string, as data must be with the JSON protocol.
fn json_string(text: &str) -> Vec<u8> {
    serde_json::to_vec(text).expect("a string")
}

/// Registers the scenario's client for the capture.
fn captured(world: &mut ClientWorld) -> u64 {
    let number = world.client().log_number();
    logs::register(number);
    world.logging.clients.push(number);
    number
}

fn the_client(world: &ClientWorld) -> u64 {
    *world.logging.clients.first().expect("a captured client")
}

/// Waits for a record of client `number` after the one at index `after`
/// that `matches`, giving its index.
async fn record_of(
    number: u64,
    after: Option<usize>,
    what: &str,
    matches: impl Fn(&Record) -> bool,
) -> usize {
    let mut found = None;
    wait_until(what, || {
        found = logs::of(number)
            .iter()
            .enumerate()
            .skip(after.map_or(0, |index| index + 1))
            .find(|(_, record)| matches(record))
            .map(|(index, _)| index);
        found.is_some()
    })
    .await;
    found.expect("found")
}

/// Fails if any record kept shows one of `secrets`, as text or as the
/// `Debug` list of its bytes.
fn no_record_shows(secrets: &[String]) {
    for record in logs::all() {
        let text = record.text();
        for secret in secrets {
            let bytes = format!("{:?}", secret.as_bytes());
            let bytes = bytes.trim_matches(['[', ']']);
            assert!(
                !text.contains(secret.as_str()) && !text.contains(bytes),
                "{secret} in {text}"
            );
        }
    }
}

fn level(name: &str) -> Level {
    name.parse().expect("a level")
}

#[given("a client connected to the fake server, its log records captured")]
async fn a_captured_connected_client(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    captured(world);
    world.client().connect();
    expect_event(world, "connecting").await;
    expect_event(world, "connected").await;
}

#[when(regex = r#"^the fake server disconnects the client with code (\d+) and reason "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_fake_server_disconnects_with(world: &mut ClientWorld, code: u16, reason: String) {
    world.fake().close(code, &reason);
}

#[then(
    regex = r#"^(then )?a (DEBUG|WARN) record in the client's span says it is connecting with code (\d+) and reason "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn says_connecting(
    world: &mut ClientWorld,
    then: String,
    at: String,
    code: String,
    reason: String,
) {
    let after = if then.is_empty() {
        None
    } else {
        world.logging.last
    };
    let index = record_of(the_client(world), after, "a connecting record", |record| {
        record.level == level(&at)
            && record.field("state") == Some("connecting")
            && record.field("code") == Some(code.as_str())
            && record.field("reason") == Some(reason.as_str())
    })
    .await;
    world.logging.last = Some(index);
}

#[then(
    regex = r#"^(then )?a DEBUG record in the client's span says it is connected over transport "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn says_connected(world: &mut ClientWorld, then: String, transport: String) {
    let after = if then.is_empty() {
        None
    } else {
        world.logging.last
    };
    let index = record_of(the_client(world), after, "a connected record", |record| {
        record.level == Level::DEBUG
            && record.field("state") == Some("connected")
            && record.field("transport") == Some(transport.as_str())
    })
    .await;
    world.logging.last = Some(index);
}

#[then(
    regex = r#"^a DEBUG record in the span of the subscription to "(\w+)" says it is subscribed$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn says_subscribed(world: &mut ClientWorld, channel: String) {
    record_of(the_client(world), None, "a subscribed record", |record| {
        record.level == Level::DEBUG
            && record.in_subscription(&channel)
            && record.field("state") == Some("subscribed")
    })
    .await;
}

#[when(regex = r#"^the fake server sends the text frame "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_fake_server_sends_text(world: &mut ClientWorld, frame: String) {
    world.fake().send(&frame);
}

#[then(
    regex = r"^a WARN record in the client's span says a frame from the server could not be decoded, with a length of (\d+) bytes and the client's number$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn warns_of_an_undecodable_frame(world: &mut ClientWorld, bytes: String) {
    let number = the_client(world);
    record_of(number, None, "a WARN record of the frame", |record| {
        record.level == Level::WARN
            && record.field("bytes") == Some(bytes.as_str())
            && record.field("client") == Some(number.to_string().as_str())
    })
    .await;
}

#[then(regex = r#"^no record contains "([^"]+)"(?: or "([^"]+)")?$"#)]
fn no_record_contains(world: &mut ClientWorld, first: String, second: String) {
    let _ = world;
    let secrets: Vec<String> = [first, second]
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect();
    no_record_shows(&secrets);
}

#[given(
    regex = r#"^a client configured with the unreachable websocket endpoint "([^"]+)" and the unreachable http_stream endpoint "([^"]+)", its log records captured$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_captured_unreachable_client(world: &mut ClientWorld, websocket: String, http_stream: String) {
    let builder = Client::builder_with_transports([
        (TransportKind::WebSocket, websocket),
        (TransportKind::HttpStream, http_stream),
    ])
    .config(test_config());
    world.build_client(builder);
    captured(world);
}

#[then(
    regex = r#"^(then )?a DEBUG record in the client's span names an? (websocket|http_stream) attempt to "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn names_an_attempt(
    world: &mut ClientWorld,
    then: String,
    transport: String,
    endpoint: String,
) {
    let after = if then.is_empty() {
        None
    } else {
        world.logging.last
    };
    let index = record_of(the_client(world), after, "an attempt record", |record| {
        record.level == Level::DEBUG
            && record.field("transport") == Some(transport.as_str())
            && record.field("endpoint") == Some(endpoint.as_str())
    })
    .await;
    world.logging.last = Some(index);
}

#[given(
    "a client with a connection token, connect data, an HTTP header and an emulated header, each holding a distinct secret, its log records captured"
)]
fn a_client_with_secrets(world: &mut ClientWorld) {
    let token = secret("token");
    let data = secret("connect-data");
    let header = secret("header");
    let emulated = secret("emulated");
    let mut config = test_config();
    config.token.clone_from(&token);
    config.data = Some(json_string(&data));
    let builder = Client::builder(world.server_endpoint())
        .config(config)
        .http_headers([("X-Secret", header.as_str())])
        .emulated_headers([("X-Emulated", emulated.as_str())]);
    world.build_client(builder);
    captured(world);
    world
        .logging
        .secrets
        .extend([token, data, header, emulated]);
}

#[given(
    "the fake server answers connects with data holding a distinct secret and answers RPC with the data it received"
)]
fn the_fake_server_answers_with_secrets(world: &mut ClientWorld) {
    let data = secret("server-data");
    world
        .fake()
        .answer_connects_with_data(&serde_json::to_string(&data).expect("a string"));
    world.fake().answer_rpc(Rpc::Echo);
    world.logging.secrets.push(data);
}

#[when(
    regex = r#"^a subscription to channel "(\w+)" with a subscription token and subscription data, each holding a distinct secret, is subscribed$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn a_subscription_with_secrets(world: &mut ClientWorld, channel: String) {
    let token = secret("sub-token");
    let data = secret("sub-data");
    let subscription = world
        .client()
        .subscription_builder(channel.as_str())
        .token(token.as_str())
        .data(json_string(&data))
        .build()
        .expect("a new subscription");
    let mut events = subscription.events();
    subscription.subscribe();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("subscribed in time")
            .expect("an event");
        if matches!(event, SubscriptionEvent::Subscribed(_)) {
            break;
        }
    }
    world.logging.subscription = Some((subscription, Box::new(events)));
    world.logging.secrets.extend([token, data]);
}

#[when(
    regex = r#"^the fake server pushes a publication on "(\w+)" whose data holds a distinct secret$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn the_fake_server_pushes_a_secret(world: &mut ClientWorld, channel: String) {
    let data = secret("publication");
    let push = json!({"push": {"channel": channel, "pub": {"data": data}}});
    world.fake().send(&push.to_string());
    let (_, events) = world.logging.subscription.as_mut().expect("a subscription");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("the publication in time")
            .expect("an event");
        if matches!(event, SubscriptionEvent::Publication(_)) {
            break;
        }
    }
    world.logging.secrets.push(data);
}

#[when(
    regex = r#"^the client publishes data holding a distinct secret to "(\w+)" and calls RPC "(\w+)" with data holding a distinct secret$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn the_client_publishes_and_calls(world: &mut ClientWorld, channel: String, method: String) {
    let published = secret("publish");
    let sent = secret("rpc");
    world
        .client()
        .publish(channel, json_string(&published))
        .await
        .expect("published");
    let reply = world
        .client()
        .rpc(method, json_string(&sent))
        .await
        .expect("an RPC reply");
    assert_eq!(reply, json_string(&sent), "the fake server echoes RPC data");
    world.logging.secrets.extend([published, sent]);
}

#[then("the client's span holds TRACE records of inputs, actions, and frames sent and received")]
fn holds_trace_records(world: &mut ClientWorld) {
    let records = logs::of(the_client(world));
    for message in ["input", "action", "frame sent", "frame received"] {
        assert!(
            records
                .iter()
                .any(|record| record.level == Level::TRACE && record.message == message),
            "no TRACE {message} record"
        );
    }
}

#[then("no record contains any of those secrets, as text or as the Debug list of its bytes")]
fn no_record_contains_the_secrets(world: &mut ClientWorld) {
    assert_eq!(world.logging.secrets.len(), 10, "the secrets of the scenario");
    no_record_shows(&world.logging.secrets);
}

#[given("two clients, each connected to its own fake server, their log records captured")]
async fn two_captured_clients(world: &mut ClientWorld) {
    a_captured_connected_client(world).await;
    let second = FakeServer::start(Announce::default()).await;
    let client = Client::builder(format!("ws://{}/connection/websocket", second.address()))
        .config(test_config())
        .build()
        .expect("a valid client");
    let number = client.log_number();
    logs::register(number);
    world.logging.clients.push(number);
    let mut events = client.events();
    client.connect();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("connected in time")
            .expect("an event");
        if matches!(event, ClientEvent::Connected { .. }) {
            break;
        }
    }
    world.logging.second = Some((client, second));
}

#[when(
    regex = r#"^the first client's fake server disconnects it with code (\d+) and reason "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_first_fake_server_disconnects(world: &mut ClientWorld, code: u16, reason: String) {
    world.fake().close(code, &reason);
}

#[then(
    regex = r"^the record saying a client is connecting with code (\d+) is in the first client's span only$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn in_the_first_clients_span_only(world: &mut ClientWorld, code: String) {
    let connecting = |record: &Record| {
        record.field("state") == Some("connecting") && record.field("code") == Some(code.as_str())
    };
    let [first, second] = world.logging.clients[..] else {
        panic!("two captured clients");
    };
    record_of(first, None, "the first client's record", connecting).await;
    assert!(
        !logs::of(second).iter().any(connecting),
        "a connecting record with code {code} in the second client's span"
    );
}
