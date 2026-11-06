//! Steps of Block 13: client commands.

use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::{CallError, ClientEvent, ErrorKind, State, codes};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::connection::a_connected_client;
use super::{
    ClientWorld, EVENT_TIMEOUT, QUIET_PERIOD, next_event_after_transport_errors, test_config,
};
use crate::support::fake_server::Rpc;
use crate::support::relay::Mode;

/// Margin for a call's timeout to fire and its failure to reach the step.
const TOLERANCE: Duration = Duration::from_secs(1);
/// How long the connect proxy takes to accept a connection in the
/// scenarios calling while the client connects.
const CONNECTING_FOR: Duration = Duration::from_millis(500);

/// Calls of the scenario, in the order they were made.
#[derive(Default)]
pub(crate) struct Calls {
    made: Vec<Call>,
}

struct Call {
    method: String,
    made_at: Instant,
    outcome: Outcome,
}

enum Outcome {
    Pending(JoinHandle<Result<Vec<u8>, CallError>>),
    Settled(Result<Vec<u8>, CallError>, Instant),
}

impl Drop for Calls {
    fn drop(&mut self) {
        for call in &self.made {
            if let Outcome::Pending(task) = &call.outcome {
                task.abort();
            }
        }
    }
}

/// Makes an RPC call now, awaited by a task of its own.
fn call(world: &mut ClientWorld, method: &str, data: Vec<u8>) {
    let future = world.client().rpc(method, data);
    world.calls.made.push(Call {
        method: method.to_owned(),
        made_at: Instant::now(),
        outcome: Outcome::Pending(tokio::spawn(future)),
    });
}

/// The outcome of the call to `method` (the last call when `None`), once it
/// settles within `limit`, and how long after it was made.
async fn outcome(
    world: &mut ClientWorld,
    method: Option<&str>,
    limit: Duration,
) -> (Result<Vec<u8>, CallError>, Duration) {
    let call = match method {
        Some(method) => world
            .calls
            .made
            .iter_mut()
            .find(|call| call.method == method)
            .unwrap_or_else(|| panic!("no call to {method}")),
        None => world.calls.made.last_mut().expect("no call made"),
    };
    if let Outcome::Pending(task) = &mut call.outcome {
        let result = tokio::time::timeout(limit, task)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the call to {} did not settle within {limit:?}",
                    call.method
                )
            })
            .expect("the call's task panicked");
        call.outcome = Outcome::Settled(result, Instant::now());
    }
    let Outcome::Settled(result, at) = &call.outcome else {
        unreachable!("settled above");
    };
    (result.clone(), at.duration_since(call.made_at))
}

/// The call's error, failing on a result.
fn error(result: Result<Vec<u8>, CallError>) -> CallError {
    match result {
        Ok(data) => panic!("the call returned {:?}", String::from_utf8_lossy(&data)),
        Err(error) => error,
    }
}

/// `data` as JSON.
fn json_of(data: &[u8]) -> Value {
    serde_json::from_slice(data)
        .unwrap_or_else(|_| panic!("not JSON: {:?}", String::from_utf8_lossy(data)))
}

fn assert_rpc_proxy(world: &ClientWorld) {
    assert!(
        world.proxy.is_some(),
        "this step needs the RPC proxy: tag the scenario @B"
    );
}

/// The proxy's "echo" returns its input and "busy" fails with a temporary
/// error 1000 "try later" (`ProxyBackend`'s `/rpc` route).
#[given(
    regex = r#"^a connected client and an RPC proxy answering method "(?:echo" with its input|busy" with a temporary error with code 1000 and message "try later")$"#
)]
async fn a_connected_client_and_an_rpc_proxy(world: &mut ClientWorld) {
    assert_rpc_proxy(world);
    a_connected_client(world).await;
}

/// The connect proxy accepts connections only after a delay, so that a
/// call made right after `connect()` is made while the client connects.
#[given(r#"an RPC proxy answering method "echo" with its input"#)]
fn an_rpc_proxy(world: &mut ClientWorld) {
    assert_rpc_proxy(world);
    world
        .proxy
        .as_ref()
        .expect("the RPC proxy")
        .accept_after(CONNECTING_FOR);
}

#[given(
    regex = r#"^the client calls RPC "(\w+)"(?: with (\{.*\}))?(?: (before connect is called|before it is connected))?$"#
)]
#[when(
    regex = r#"^the client calls RPC "(\w+)"(?: with (\{.*\}))?(?: (before connect is called|before it is connected))?$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_calls_rpc(world: &mut ClientWorld, method: String, data: String, when: String) {
    match when.as_str() {
        "before connect is called" => assert_eq!(world.client().state(), State::Disconnected),
        "before it is connected" => assert_eq!(world.client().state(), State::Connecting),
        _ => {}
    }
    let data = if data.is_empty() {
        "{}".to_owned()
    } else {
        data
    };
    call(world, &method, data.into_bytes());
}

#[when(regex = r#"^the client calls RPC "(\w+)" and RPC "(\w+)" concurrently$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_client_calls_two_rpcs(world: &mut ClientWorld, first: String, second: String) {
    call(world, &first, b"{}".to_vec());
    call(world, &second, b"{}".to_vec());
}

#[when("connect is never called")]
fn connect_is_never_called(world: &mut ClientWorld) {
    assert_eq!(world.client().state(), State::Disconnected);
}

#[then(regex = r"^the call returns (\{.*\})$")]
async fn the_call_returns(world: &mut ClientWorld, expected: String) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let data = result.unwrap_or_else(|error| panic!("the call failed: {error}"));
    assert_eq!(
        json_of(&data),
        serde_json::from_str::<Value>(&expected).unwrap()
    );
}

#[then(regex = r#"^the "(\w+)" call returns the reply to "(\w+)"$"#)]
async fn the_call_returns_the_reply_to(world: &mut ClientWorld, method: String, to: String) {
    let (result, _) = outcome(world, Some(&method), EVENT_TIMEOUT).await;
    let data = result.unwrap_or_else(|error| panic!("the call failed: {error}"));
    assert_eq!(json_of(&data), json!({"reply_to": to}));
}

#[then("the call completes once the client is connected")]
async fn the_call_completes_once_connected(world: &mut ClientWorld) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let data = result.unwrap_or_else(|error| panic!("the call failed: {error}"));
    assert_eq!(json_of(&data), json!({}));
    assert_eq!(world.client().state(), State::Connected);
    let connected = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            if let ClientEvent::Connected { .. } = world.next_event().await {
                return;
            }
        }
    })
    .await;
    assert!(connected.is_ok(), "no connected event");
}

#[then(regex = r#"^the call fails with a temporary error with code (\d+) and message "(.*)"$"#)]
async fn the_call_fails_with_a_temporary_error(
    world: &mut ClientWorld,
    code: u32,
    message: String,
) {
    let (result, _) = outcome(world, None, EVENT_TIMEOUT).await;
    let error = error(result);
    assert_eq!(
        (error.code, error.message.as_str(), error.temporary),
        (code, message.as_str(), true),
        "{error:?}"
    );
}

/// The call fails with the timeout error once the configured timeout has
/// passed, not before.
#[given("the call fails with a timeout error")]
#[when("the call fails with a timeout error")]
#[then("the call fails with a timeout error within the configured timeout plus tolerance")]
async fn the_call_times_out(world: &mut ClientWorld) {
    let timeout = test_config().timeout;
    let (result, after) = outcome(world, None, timeout + TOLERANCE).await;
    let error = error(result);
    assert_eq!(error.code, codes::error::TIMEOUT, "{error:?}");
    assert!(
        after >= timeout,
        "timed out after {after:?}, before {timeout:?}"
    );
}

#[then(
    regex = r#"^the call fails with a "(disconnected|connection closed)" error( before the configured timeout)?$"#
)]
async fn the_call_fails_with(world: &mut ClientWorld, message: String, early: String) {
    let timeout = test_config().timeout;
    let (result, after) = outcome(world, None, EVENT_TIMEOUT).await;
    let error = error(result);
    let code = match message.as_str() {
        "disconnected" => codes::error::DISCONNECTED,
        _ => codes::error::CONNECTION_CLOSED,
    };
    assert_eq!(
        (error.code, error.message.as_str()),
        (code, message.as_str())
    );
    if !early.is_empty() {
        assert!(after < timeout, "failed after {after:?}");
    }
}

#[given(
    regex = r"^a client connected to the fake server that (answers commands in reverse order|never replies to commands)$"
)]
async fn a_client_connected_to_a_fake_server_answering(world: &mut ClientWorld, how: String) {
    world.fake().answer_rpc(match how.as_str() {
        "answers commands in reverse order" => Rpc::Reverse,
        _ => Rpc::Ignore,
    });
    a_connected_client(world).await;
}

#[given("a client configured with the fake server's endpoint behind a relay that is down")]
fn a_client_behind_a_relay_that_is_down(world: &mut ClientWorld) {
    world.relay.as_ref().expect("a relay").set_mode(Mode::Down);
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
}

/// The client is retrying: its next event, after the failed attempts, is
/// kept for the next step.
#[when("the relay comes up")]
async fn the_relay_comes_up(world: &mut ClientWorld) {
    world
        .relay
        .as_ref()
        .expect("a relay")
        .set_mode(Mode::Forward);
    loop {
        match next_event_after_transport_errors(world).await {
            ClientEvent::Connecting { code, .. } if code == codes::connecting::CONNECT_CALLED => {}
            event => {
                world.put_back_events.push_back(event);
                return;
            }
        }
    }
}

#[then("the fake server receives no RPC command")]
async fn the_fake_server_receives_no_rpc(world: &mut ClientWorld) {
    tokio::time::sleep(QUIET_PERIOD).await;
    assert_eq!(world.fake().rpc_commands(), Vec::<Value>::new());
}

#[given("the fake server has received the RPC command")]
async fn the_fake_server_has_received_the_rpc(world: &mut ClientWorld) {
    let received = tokio::time::timeout(EVENT_TIMEOUT, async {
        while world.fake().rpc_commands().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(received.is_ok(), "no RPC command reached the fake server");
}

#[given(regex = r"^the connect proxy answers after (\d+) s with disconnect code (\d+)$")]
fn the_connect_proxy_answers_after(world: &mut ClientWorld, seconds: u64, code: u32) {
    world
        .proxy
        .as_ref()
        .expect("this step needs a connect proxy: tag the scenario @B")
        .disconnect_next_after(Duration::from_secs(seconds), code, "go away");
}

#[then(regex = r#"^the client emits "disconnected" with code (\d+)$"#)]
async fn the_client_emits_disconnected_with(world: &mut ClientWorld, code: u32) {
    loop {
        match next_event_after_transport_errors(world).await {
            ClientEvent::Connecting { .. } => {}
            ClientEvent::Disconnected { code: got, .. } => {
                assert_eq!(got, code);
                return;
            }
            event => panic!("expected disconnected, got {event:?}"),
        }
    }
}

#[then(regex = r#"^the client reconnects over transport "(\w+)"$"#)]
async fn the_client_reconnects_over(world: &mut ClientWorld, transport: String) {
    let connecting = world.next_event().await;
    assert!(
        matches!(connecting, ClientEvent::Connecting { .. }),
        "{connecting:?}"
    );
    loop {
        match world.next_event().await {
            ClientEvent::Error(error) if error.kind == ErrorKind::Transport => {}
            ClientEvent::Connected { transport: got, .. } => {
                assert_eq!(got, transport);
                return;
            }
            event => panic!("expected connected, got {event:?}"),
        }
    }
}
