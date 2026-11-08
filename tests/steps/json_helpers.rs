//! Steps of Block 38: typed JSON helpers.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{JsonCallError, Protocol, Publication};
use futures_util::FutureExt as _;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use serde_json::value::RawValue;

use super::calls::{second_publication, two_clients_of};
use super::connection::a_connected_client;
use super::subscriptions::{a_client_subscribed, next_publication};
use super::{ClientWorld, EVENT_TIMEOUT};

/// The application's type of the scenarios.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Note {
    text: String,
    n: u32,
}

#[derive(Serialize)]
struct Terms {
    a: u32,
    b: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Sum {
    sum: u32,
}

/// A value whose serialization fails.
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this value never encodes"))
    }
}

type Held = Pin<Box<dyn Future<Output = Result<(), JsonCallError>> + Send>>;

/// A typed call's future at its first poll.
#[derive(Debug)]
enum Encoded {
    Ready(Result<(), JsonCallError>),
    /// It was not ready: it did not fail at once.
    Pending,
}

/// What a Block 38 scenario holds.
#[derive(Default)]
pub(crate) struct JsonHelpers {
    /// The outcome of the last typed RPC.
    rpc: Option<Result<Sum, JsonCallError>>,
    /// The outcome of the last helper given a value that does not encode.
    encoded: Option<Encoded>,
    /// Messages the fake server had received before that helper was called.
    received_before: usize,
    /// A typed publish the scenario holds without polling it.
    held: Option<Held>,
    /// The publication of a unit-level scenario.
    publication: Option<Publication>,
    /// What decoding it gave.
    decoded: Option<Result<Note, serde_json::Error>>,
}

impl std::fmt::Debug for JsonHelpers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonHelpers")
            .field("rpc", &self.rpc)
            .field("encoded", &self.encoded)
            .field("held", &self.held.is_some())
            .finish_non_exhaustive()
    }
}

fn note(text: String, n: u32) -> Note {
    Note { text, n }
}

/// Awaits a typed call that must succeed.
async fn succeeds<T>(call: impl Future<Output = Result<T, JsonCallError>>) -> T {
    tokio::time::timeout(EVENT_TIMEOUT, call)
        .await
        .expect("the call settled in time")
        .unwrap_or_else(|error| panic!("the call failed: {error:?}"))
}

#[when(
    regex = r#"^the first client publishes the note \{text: "(\w+)", n: (\d+)\} as JSON through its (client|subscription)$"#
)]
async fn the_first_client_publishes_a_note(
    world: &mut ClientWorld,
    text: String,
    n: u32,
    through: String,
) {
    let note = note(text, n);
    if through == "client" {
        succeeds(world.client().publish_json("news", &note)).await;
    } else {
        succeeds(world.subscription().publish_json(&note)).await;
    }
}

#[then(
    regex = r"^the (?:second|JSON) client's subscription receives a publication with data exactly (\{.*\})$"
)]
async fn the_second_receives_exactly(world: &mut ClientWorld, data: String) {
    let publication = second_publication(world).await;
    assert_eq!(
        String::from_utf8_lossy(&publication.data),
        data,
        "{publication:?}"
    );
}

#[then(
    regex = r#"^the subscription receives a publication that decodes as the note \{text: "(\w+)", n: (\d+)\}$"#
)]
async fn the_publication_decodes(world: &mut ClientWorld, text: String, n: u32) {
    let publication = next_publication(world, false).await;
    let decoded: Note = publication.json().expect("a note");
    assert_eq!(decoded, note(text, n));
}

#[given(r#"two clients subscribed to channel "news""#)]
async fn two_clients_subscribed(world: &mut ClientWorld) {
    let protocol = world.protocol;
    two_clients_of(world, false, protocol).await;
}

#[given(
    r#"a client configured with the Protobuf protocol and a client configured with the JSON protocol, both subscribed to channel "news""#
)]
async fn a_protobuf_and_a_json_client(world: &mut ClientWorld) {
    world.protocol = Protocol::Protobuf;
    two_clients_of(world, false, Protocol::Json).await;
}

#[when(regex = r#"^the Protobuf client publishes the note \{text: "(\w+)", n: (\d+)\} as JSON$"#)]
async fn the_protobuf_client_publishes(world: &mut ClientWorld, text: String, n: u32) {
    succeeds(world.client().publish_json("news", &note(text, n))).await;
}

#[given(r#"a connected client and an RPC proxy answering method "sum" with {"sum":3}"#)]
async fn a_connected_client_and_a_sum_proxy(world: &mut ClientWorld) {
    assert!(
        world.proxy.is_some(),
        "this step needs the RPC proxy: tag the scenario @B"
    );
    a_connected_client(world).await;
}

#[when(
    regex = r#"^the client calls RPC "sum" as JSON with the terms \{a: (\d+), b: (\d+)\}, expecting a sum$"#
)]
async fn the_client_calls_sum(world: &mut ClientWorld, a: u32, b: u32) {
    let call = world.client().rpc_json::<_, Sum>("sum", &Terms { a, b });
    let result = tokio::time::timeout(EVENT_TIMEOUT, call)
        .await
        .expect("the call settled in time");
    world.json_helpers.rpc = Some(result);
}

#[then(regex = r"^the RPC proxy received data equal as JSON to (\{.*\})$")]
fn the_proxy_received(world: &mut ClientWorld, data: String) {
    let requests = world.proxy.as_ref().expect("the RPC proxy").rpc_requests();
    let request = requests
        .iter()
        .find(|request| request["method"] == "sum")
        .unwrap_or_else(|| panic!("no RPC sum among {requests:?}"));
    let expected: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(request["data"], expected, "{request}");
}

#[then(regex = r"^the call returns the sum \{sum: (\d+)\}$")]
fn the_call_returns_the_sum(world: &mut ClientWorld, sum: u32) {
    match world.json_helpers.rpc.take().expect("a typed RPC") {
        Ok(result) => assert_eq!(result, Sum { sum }),
        Err(error) => panic!("the call failed: {error:?}"),
    }
}

#[given(regex = r#"^a client connected to the fake server that answers RPC "sum" with (.+)$"#)]
async fn a_fake_server_answering_sum(world: &mut ClientWorld, answer: String) {
    let reply = if answer == "no data" {
        serde_json::json!({"rpc": {}})
    } else if let Some(error) = answer.strip_prefix("a temporary error with code 1000 and message ")
    {
        serde_json::json!({"error": {
            "code": 1000,
            "message": error.trim_matches('"'),
            "temporary": true,
        }})
    } else {
        let data: Value = serde_json::from_str(&answer).expect("JSON result data");
        serde_json::json!({"rpc": {"data": data}})
    };
    world.fake().answer_rpcs_with(reply);
    a_connected_client(world).await;
}

/// The result data a `Decode` error holds.
fn decoding_error_data(world: &mut ClientWorld) -> Vec<u8> {
    match world.json_helpers.rpc.take().expect("a typed RPC") {
        Err(JsonCallError::Decode { data, .. }) => data,
        other => panic!("expected a decoding error, got {other:?}"),
    }
}

#[then(regex = r"^the call fails with a decoding error holding the result data (\{.*\})$")]
fn the_call_fails_decoding(world: &mut ClientWorld, data: String) {
    assert_eq!(String::from_utf8_lossy(&decoding_error_data(world)), data);
}

#[then("the call fails with a decoding error holding empty result data")]
fn the_call_fails_decoding_empty(world: &mut ClientWorld) {
    assert_eq!(decoding_error_data(world), b"");
}

#[then(regex = r#"^the call fails with a call error with code (\d+), message "(\w+)", temporary$"#)]
fn the_call_fails_with_a_call_error(world: &mut ClientWorld, code: u32, message: String) {
    match world.json_helpers.rpc.take().expect("a typed RPC") {
        Err(JsonCallError::Call(error)) => {
            assert_eq!(
                (error.code, error.message.as_str(), error.temporary),
                (code, message.as_str(), true)
            );
        }
        other => panic!("expected a call error, got {other:?}"),
    }
}

#[given(r#"a client connected to the fake server with a subscription to channel "news""#)]
async fn a_fake_server_and_a_subscription(world: &mut ClientWorld) {
    a_client_subscribed(world).await;
}

#[when(
    regex = r#"^the client calls (publish_json to channel "news"|publish_json on its subscription|rpc_json with method "sum") with a value whose serialization fails$"#
)]
fn the_client_calls_with_an_unencodable_value(world: &mut ClientWorld, helper: String) {
    world.json_helpers.received_before = world.fake().received().len();
    let client = world.client();
    // Ready at its first poll, or it did not fail at once.
    let encoded = match helper.as_str() {
        "publish_json on its subscription" => world
            .subscription()
            .publish_json(&Unencodable)
            .now_or_never(),
        "rpc_json with method \"sum\"" => client
            .rpc_json::<_, Value>("sum", &Unencodable)
            .now_or_never()
            .map(|result| result.map(drop)),
        _ => client.publish_json("news", &Unencodable).now_or_never(),
    };
    world.json_helpers.encoded = Some(encoded.map_or(Encoded::Pending, Encoded::Ready));
}

#[then("the call fails at once with an encoding error")]
fn the_call_fails_encoding(world: &mut ClientWorld) {
    match world.json_helpers.encoded.take().expect("a helper called") {
        Encoded::Ready(Err(JsonCallError::Encode(_))) => {}
        other => panic!("expected an encoding error at once, got {other:?}"),
    }
}

/// The raw `data` of the JSON publish command `text`.
fn raw_publish_data(text: &str) -> Option<String> {
    let command: HashMap<&str, &RawValue> = serde_json::from_str(text).ok()?;
    let publish: HashMap<&str, &RawValue> =
        serde_json::from_str(command.get("publish")?.get()).ok()?;
    Some(publish.get("data")?.get().to_owned())
}

#[then(regex = r"^the fake server has received one command since, a publish with data (\{.*\})$")]
fn one_command_since(world: &mut ClientWorld, data: String) {
    let since = world.json_helpers.received_before;
    let texts = world.fake().received_texts();
    let after = &texts[since..];
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(
        raw_publish_data(&after[0]).as_deref(),
        Some(data.as_str()),
        "{after:?}"
    );
}

#[when(
    regex = r#"^the client publishes the note \{text: "(\w+)", n: (\d+)\} as JSON to channel "(\w+)" and holds the future without polling it$"#
)]
fn the_client_publishes_and_holds(world: &mut ClientWorld, text: String, n: u32, channel: String) {
    let held = world.client().publish_json(channel, &note(text, n));
    world.json_helpers.held = Some(Box::pin(held));
}

#[then(regex = r"^the fake server receives a publish command with data exactly (\{.*\})$")]
async fn the_fake_server_receives_the_publish(world: &mut ClientWorld, data: String) {
    let fake = world.fake();
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let Some(raw) = fake
                .received_texts()
                .iter()
                .find_map(|text| raw_publish_data(text))
            {
                return raw;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the publish command, though its future was never polled");
    assert_eq!(received, data);
    assert!(
        world.json_helpers.held.is_some(),
        "the future is still held"
    );
}

#[given(regex = r"^a publication whose data is (.+)$")]
fn a_publication_whose_data_is(world: &mut ClientWorld, data: String) {
    let bytes = match data.as_str() {
        "nothing" => Vec::new(),
        "the bytes ff 00 fe" => vec![0xff, 0x00, 0xfe],
        json => json.as_bytes().to_vec(),
    };
    world.json_helpers.publication = Some(Publication::new(bytes));
}

#[when("it is decoded as a note")]
fn it_is_decoded(world: &mut ClientWorld) {
    let publication = world
        .json_helpers
        .publication
        .as_ref()
        .expect("a publication");
    world.json_helpers.decoded = Some(publication.json());
}

#[then(regex = r#"^it gives the note \{text: "(\w+)", n: (\d+)\}$"#)]
fn it_gives_the_note(world: &mut ClientWorld, text: String, n: u32) {
    let decoded = world.json_helpers.decoded.take().expect("decoded");
    assert_eq!(decoded.expect("a note"), note(text, n));
}

#[then(regex = r"^it fails with a serde_json (data|syntax|end-of-input) error$")]
fn it_fails(world: &mut ClientWorld, kind: String) {
    let decoded = world.json_helpers.decoded.take().expect("decoded");
    let error = decoded.expect_err("not a note");
    let expected = match kind.as_str() {
        "data" => serde_json::error::Category::Data,
        "syntax" => serde_json::error::Category::Syntax,
        _ => serde_json::error::Category::Eof,
    };
    assert_eq!(error.classify(), expected, "{error}");
}
