//! Block 67: custom websocket subprotocols.

use cucumber::{given, then, when};
use ferrifuge::{Client, ClientError, ErrorKind, Protocol, codes};

use super::{ClientWorld, expect_event};
use crate::support::fake_server::SubprotocolAnswer;

fn protocol(name: &str) -> Protocol {
    match name {
        "JSON" => Protocol::Json,
        "Protobuf" => Protocol::Protobuf,
        _ => panic!("unknown protocol {name}"),
    }
}

/// Builds the scenario's client against the server, with `calls` of
/// `websocket_subprotocols`, each with its values; the builder's `Debug`
/// output kept.
fn build_with(world: &mut ClientWorld, calls: &[&[&str]]) {
    let mut builder = Client::builder(world.server_endpoint()).config(world.client_config());
    for values in calls {
        builder = builder.websocket_subprotocols(values.iter().copied());
    }
    world.subprotocols.builder_debug = format!("{builder:?}");
    world.build_client(builder);
}

#[given("a fake server that records the websocket subprotocols offered")]
fn a_recording_fake_server(world: &mut ClientWorld) {
    // Every upgrade's lines are recorded (Block 67, D8).
    let _ = world.fake();
}

#[given(regex = r"^a client with the (JSON|Protobuf) protocol and no websocket subprotocols$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_without_subprotocols(world: &mut ClientWorld, name: String) {
    world.protocol = protocol(&name);
    build_with(world, &[]);
}

#[given(
    regex = r#"^a client with the (JSON|Protobuf) protocol, given websocket subprotocol "([^"]+)" in one call and "([^"]+)" in a later one$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_two_calls(world: &mut ClientWorld, name: String, first: String, second: String) {
    world.protocol = protocol(&name);
    build_with(world, &[&[first.as_str()], &[second.as_str()]]);
}

#[given(regex = r#"^a client with websocket subprotocol "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_one(world: &mut ClientWorld, value: String) {
    build_with(world, &[&[value.as_str()]]);
}

#[then(
    regex = r#"^the upgrade request carried (no Sec-WebSocket-Protocol header|one Sec-WebSocket-Protocol header "[^"]+")$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_upgrade_carried(world: &mut ClientWorld, carried: String) {
    let lines = world.fake().subprotocol_lines();
    let first = lines.first().expect("an upgrade");
    match carried.split('"').nth(1) {
        None => assert!(first.is_empty(), "{lines:?}"),
        Some(offer) => assert_eq!(first, &[offer.to_owned()], "{lines:?}"),
    }
}

#[then(regex = r#"^the fake server selected "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_fake_server_selected(world: &mut ClientWorld, selected: String) {
    let all = world.fake().subprotocols_selected();
    assert_eq!(all.first(), Some(&Some(selected)), "{all:?}");
}

#[given(
    regex = r"^a fake server that answers the websocket upgrade with (the subprotocol \S+|no subprotocol)$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_fake_server_answering(world: &mut ClientWorld, answer: String) {
    world
        .fake()
        .answer_subprotocols_with(match answer.strip_prefix("the subprotocol ") {
            Some(value) => SubprotocolAnswer::Value(value.to_owned()),
            None => SubprotocolAnswer::None,
        });
}

#[then(regex = r#"^the client emits an "error" event of kind transport with message "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn an_error_of_kind_transport(world: &mut ClientWorld, message: String) {
    let event = loop {
        match world.next_event().await {
            ferrifuge::ClientEvent::Connecting(_) => {}
            event => break event,
        }
    };
    let ferrifuge::ClientEvent::Error(error) = &event else {
        panic!("expected an error event, got {event:?}");
    };
    assert_eq!(
        (error.kind, error.message.as_str()),
        (ErrorKind::Transport, message.as_str()),
        "{error:?}"
    );
}

/// What the steps of Block 67 keep between steps.
#[derive(Debug, Default)]
pub(crate) struct Subprotocols {
    /// The builder's `Debug` output, taken before it was built.
    pub(crate) builder_debug: String,
    /// The values of "client options with websocket subprotocols".
    pub(crate) values: Vec<String>,
}

#[then("the fake server received no connect command")]
fn no_connect_command(world: &mut ClientWorld) {
    let connects: Vec<_> = world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command.get("connect").is_some())
        .collect();
    assert!(connects.is_empty(), "{connects:?}");
}

#[given(regex = r"^client options with websocket subprotocols (.*)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn client_options_with(world: &mut ClientWorld, values: String) {
    // Each value between quotes, the empty one included.
    let values: Vec<String> = values
        .split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect();
    world.pending_builder =
        Some(Client::builder(super::UNREACHABLE_ENDPOINT).websocket_subprotocols(values.clone()));
    world.subprotocols.values = values;
}

#[then(
    regex = r"^building fails with a configuration error naming websocket subprotocol (\d+) and containing none of the values$"
)]
fn building_fails_naming_the_index(world: &mut ClientWorld, index: usize) {
    let error: &ClientError = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(
        (error.kind, error.code, error.transport),
        (
            ErrorKind::Configuration,
            codes::error::BAD_CONFIGURATION,
            None
        ),
        "{error:?}"
    );
    // The whole message is one of the three: a value, even "a" or "",
    // cannot hide in it.
    let named = format!("websocket subprotocol {index} ");
    assert!(
        [
            "is not a valid token",
            "repeats an earlier one",
            "is reserved for the encoding"
        ]
        .iter()
        .any(|problem| error.message == format!("{named}{problem}")),
        "{error:?}"
    );
    for value in world
        .subprotocols
        .values
        .iter()
        .filter(|value| value.len() > 1)
    {
        assert!(!format!("{error:?}").contains(value.as_str()), "{error:?}");
    }
}

#[given(regex = r#"^a client with websocket subprotocol "([^"]+)", its log records captured$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_captured_client_with(world: &mut ClientWorld, value: String) {
    build_with(world, &[&[value.as_str()]]);
    super::logging::captured(world);
}

#[when("connect is called and the client connects to the fake server")]
async fn connects_to_the_fake_server(world: &mut ClientWorld) {
    world.client().connect();
    expect_event(world, "connecting").await;
    expect_event(world, "connected").await;
}

#[then(
    regex = r#"^the builder's Debug output shows (\d+) websocket subprotocols? and does not contain "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_builders_debug(world: &mut ClientWorld, count: usize, value: String) {
    let shown = &world.subprotocols.builder_debug;
    assert!(
        shown.contains(&format!("websocket_subprotocols: {count}")),
        "{shown}"
    );
    assert!(!shown.contains(&value), "{shown}");
}

#[then(regex = r#"^the client's Debug output does not contain "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_clients_debug(world: &mut ClientWorld, value: String) {
    let shown = format!("{:?}", world.client());
    assert!(!shown.contains(&value), "{shown}");
}

#[given(
    regex = r#"^a client configured with the (JSON|Protobuf) protocol and websocket subprotocol "([^"]+)", with a subscription to channel "(\w+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_configured_client_with(world: &mut ClientWorld, name: String, value: String, channel: String) {
    world.protocol = protocol(&name);
    build_with(world, &[&[value.as_str()]]);
    super::subscriptions::create(world, &channel);
}
