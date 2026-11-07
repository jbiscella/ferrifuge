//! Steps of Block 32: replacing the HTTP headers of a running client.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::{ClientError, ErrorKind};

use super::connect_data::{a_client_with_an_http_header, header};
use super::{ClientWorld, EVENT_TIMEOUT, UNREACHABLE_ENDPOINT};

/// What a Block 32 scenario records.
#[derive(Debug, Default)]
pub(crate) struct RuntimeHeaders {
    /// The outcome of the last `set_http_headers` call.
    result: Option<Result<(), ClientError>>,
    /// How many requests the HTTP proxy had relayed when it was called.
    relayed_before_call: usize,
}

#[given(
    regex = r#"^a client with HTTP header "([^"]+)", configured with only the server's (\w+) endpoint, connected$"#
)]
async fn a_connected_client_with_a_header(
    world: &mut ClientWorld,
    given: String,
    transport: String,
) {
    a_client_with_an_http_header(world, given, transport);
    super::transports::connect_and_wait(world).await;
}

#[given(regex = r#"^a new client with HTTP header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_new_client_with_a_header(world: &mut ClientWorld, given: String) {
    world.connect_data.http_headers.push(header(&given));
    world.new_client(UNREACHABLE_ENDPOINT.to_owned());
}

#[when(regex = r#"^set_http_headers is called with "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn set_http_headers_is_called(world: &mut ClientWorld, given: String) {
    let result = world.client().set_http_headers([header(&given)]);
    world.runtime_headers.result = Some(result);
}

/// The paths of the requests the HTTP proxy relayed, in order, with their
/// `X-Device-Real` header.
fn relayed(world: &ClientWorld) -> Vec<(String, Option<String>)> {
    world
        .http_proxy
        .as_ref()
        .expect("an HTTP proxy")
        .requests_with_header("x-device-real")
}

fn emulation_requests(relayed: &[(String, Option<String>)]) -> usize {
    relayed
        .iter()
        .filter(|(path, _)| path == "/emulation")
        .count()
}

/// Waits until the HTTP proxy has relayed more emulation requests than
/// `before`.
async fn another_emulation_request(world: &ClientWorld, before: usize) {
    let waited = tokio::time::timeout(EVENT_TIMEOUT, async {
        while emulation_requests(&relayed(world)) <= before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "no emulation request relayed");
}

/// Called just after a pong is relayed: the server pings every 2 s, so no
/// request with the old set is in flight.
#[when(
    regex = r#"^set_http_headers is called with "([^"]+)" right after the HTTP proxy relayed an emulation request$"#
)]
async fn set_right_after_an_emulation_request(world: &mut ClientWorld, given: String) {
    let before = emulation_requests(&relayed(world));
    another_emulation_request(world, before).await;
    world.runtime_headers.relayed_before_call = relayed(world).len();
    set_http_headers_is_called(world, given);
}

#[when("the HTTP proxy relays another emulation request")]
async fn the_proxy_relays_another(world: &mut ClientWorld) {
    let relayed_before = world.runtime_headers.relayed_before_call;
    let before = emulation_requests(&relayed(world)[..relayed_before]);
    another_emulation_request(world, before).await;
}

#[then(regex = r#"^the HTTP proxy relayed one stream request, carrying header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn one_stream_request(world: &mut ClientWorld, given: String) {
    let (_, value) = header(&given);
    let streams: Vec<_> = relayed(world)
        .into_iter()
        .filter(|(path, _)| path.starts_with("/connection/"))
        .collect();
    assert_eq!(streams.len(), 1, "{streams:?}");
    assert_eq!(streams[0].1.as_deref(), Some(value.as_str()), "{streams:?}");
}

#[then(regex = r#"^every emulation request it relayed after the call carried header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn every_later_emulation_request(world: &mut ClientWorld, given: String) {
    assert!(
        matches!(world.runtime_headers.result, Some(Ok(()))),
        "{:?}",
        world.runtime_headers.result
    );
    let (_, value) = header(&given);
    let after: Vec<_> = relayed(world)
        .into_iter()
        .skip(world.runtime_headers.relayed_before_call)
        .filter(|(path, _)| path == "/emulation")
        .collect();
    assert!(!after.is_empty(), "no emulation request after the call");
    assert!(
        after
            .iter()
            .all(|(_, got)| got.as_deref() == Some(value.as_str())),
        "{after:?}"
    );
}

#[then(regex = r#"^it fails with a configuration error naming "([^"]+)" and not "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_fails_naming(world: &mut ClientWorld, name: String, value: String) {
    let error = world
        .runtime_headers
        .result
        .clone()
        .expect("set_http_headers called")
        .expect_err("set_http_headers succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert!(error.message.contains(&name), "{error:?}");
    assert!(!error.message.contains(&value), "{error:?}");
}
