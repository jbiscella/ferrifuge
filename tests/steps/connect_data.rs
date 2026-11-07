//! Steps of Block 17: connect data, its callback, the server's data on the
//! `connected` event (17a), and HTTP and emulated headers (17b).
//!
//! @E scenarios drive the sans-IO core: a step answers the token and data
//! fetches the machine asks for, and the transports it opens. @B and @D
//! scenarios configure the scenario's client.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{Action, FetchId, Input, Machine, TransportId};
use ferrifuge::{ClientEvent, Config, DataError, ErrorKind, State, TokenError, codes};
use serde_json::Value;

use super::{ClientWorld, expect_event};

/// The data the @E data callback returns, unless a scenario says otherwise.
const DATA: &str = r#"{"n":1}"#;

/// Connect options a @B or @D scenario gives its client.
#[derive(Default)]
pub(crate) struct ConnectData {
    /// `Config::data`.
    pub(crate) data: Option<Vec<u8>>,
    /// What the data callback returns, call after call.
    pub(crate) callback: Option<Arc<Mutex<VecDeque<Vec<u8>>>>>,
    /// The raw data the fake server's connect replies carry.
    raw: Option<String>,
    /// HTTP headers of the client (17b).
    pub(super) http_headers: Vec<(String, String)>,
    /// Emulated headers of the client (17b).
    emulated_headers: Vec<(String, String)>,
    /// The debug output of client options a step formatted.
    debug: Option<String>,
    /// `Config::token` and `Config::min_reconnect_delay` of the client (17c).
    token: Option<String>,
    min_reconnect_delay: Option<Duration>,
    /// Events a 17c step read before the scenario's next steps.
    seen: Vec<ClientEvent>,
    /// How many commands the fake server had received when `set_data` was
    /// called.
    received_at_set: Option<usize>,
    /// The sans-IO core of an @E scenario.
    pub(crate) run: Option<DataRun>,
    /// The sans-IO core of the 17c token scenario.
    token_run: Option<TokenRun>,
}

impl ConnectData {
    /// Applies the scenario's options to `config`.
    pub(crate) fn configure(&self, config: &mut Config) {
        config.data.clone_from(&self.data);
        if let Some(token) = &self.token {
            config.token.clone_from(token);
        }
        if let Some(delay) = self.min_reconnect_delay {
            config.min_reconnect_delay = delay;
            config.max_reconnect_delay = config.max_reconnect_delay.max(delay * 2);
        }
    }

    /// Applies the scenario's data callback to `builder`.
    pub(crate) fn build(&self, builder: ferrifuge::ClientBuilder) -> ferrifuge::ClientBuilder {
        let builder = builder
            .http_headers(self.http_headers.clone())
            .emulated_headers(self.emulated_headers.clone());
        let Some(answers) = self.callback.clone() else {
            return builder;
        };
        builder.get_data(move || {
            let data = answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("an answer per call");
            async move { Ok(data) }
        })
    }
}

/// How the @E token callback answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenAnswer {
    Token,
    Unauthorized,
    Failed,
    Empty,
}

/// A machine with a data source, whose fetches a step answers.
pub(crate) struct DataRun {
    machine: Machine,
    /// Every action, in order.
    pub(crate) actions: Vec<Action>,
    /// The callbacks called, in order: "token" and "data".
    calls: Vec<&'static str>,
    token: TokenAnswer,
    /// What the data callback answers, call after call; once empty, it
    /// does not answer.
    answers: VecDeque<Result<Vec<u8>, DataError>>,
    /// A data fetch not answered yet.
    pending: Option<FetchId>,
}

impl DataRun {
    fn new(initial_token: Option<&str>, token: Option<TokenAnswer>) -> Self {
        let config = Config::default().token(initial_token.unwrap_or_default().to_owned());
        let machine = if token.is_some() {
            Machine::with_token_source(config)
        } else {
            Machine::new(config)
        };
        Self {
            machine: machine.with_data_source(),
            actions: Vec::new(),
            calls: Vec::new(),
            token: token.unwrap_or(TokenAnswer::Token),
            answers: VecDeque::new(),
            pending: None,
        }
    }

    fn handle(&mut self, input: Input) {
        let mut inputs = VecDeque::from([input]);
        while let Some(input) = inputs.pop_front() {
            for action in self.machine.handle(input) {
                match &action {
                    Action::FetchToken(fetch) => {
                        self.calls.push("token");
                        let result = match self.token {
                            TokenAnswer::Token => Ok("t1".to_owned()),
                            TokenAnswer::Empty => Ok(String::new()),
                            TokenAnswer::Unauthorized => Err(TokenError::Unauthorized),
                            TokenAnswer::Failed => Err(TokenError::Failed("down".to_owned())),
                        };
                        inputs.push_back(Input::TokenFetched {
                            fetch: *fetch,
                            result,
                        });
                    }
                    Action::FetchConnectData(fetch) => {
                        self.calls.push("data");
                        match self.answers.pop_front() {
                            Some(result) => inputs.push_back(Input::ConnectDataFetched {
                                fetch: *fetch,
                                result,
                            }),
                            None => self.pending = Some(*fetch),
                        }
                    }
                    Action::OpenTransport { transport, .. } => {
                        inputs.push_back(Input::TransportOpened(*transport));
                    }
                    _ => {}
                }
                self.actions.push(action);
            }
        }
    }

    fn opened(&self) -> Vec<TransportId> {
        self.actions
            .iter()
            .filter_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .collect()
    }

    /// The last connect command sent.
    fn connect_command(&self) -> Value {
        self.actions
            .iter()
            .rev()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => serde_json::from_slice::<Value>(frame).ok(),
                _ => None,
            })
            .find(|command| command.get("connect").is_some())
            .expect("a connect command")
    }

    fn data_calls(&self) -> usize {
        self.calls.iter().filter(|call| **call == "data").count()
    }
}

/// Whether an @E connect data scenario drives the machine.
pub(crate) fn drives_a_machine(world: &ClientWorld) -> bool {
    world.connect_data.run.is_some()
}

pub(crate) fn run(world: &mut ClientWorld) -> &mut DataRun {
    world
        .connect_data
        .run
        .as_mut()
        .expect("an @E connect data scenario")
}

pub(crate) fn connect(world: &mut ClientWorld) {
    run(world).handle(Input::Connect);
}

pub(crate) fn disconnect(world: &mut ClientWorld) {
    run(world).handle(Input::Disconnect);
}

pub(crate) fn state(world: &mut ClientWorld) -> State {
    run(world).machine.state()
}

fn json_bytes(data: &str) -> Vec<u8> {
    let value: Value = serde_json::from_str(data).expect("JSON data");
    serde_json::to_vec(&value).expect("serializable")
}

// @E: the sans-IO core.

#[given("a client whose data callback fails once")]
fn a_data_callback_failing_once(world: &mut ClientWorld) {
    let mut run = DataRun::new(None, None);
    run.answers = VecDeque::from([
        Err(DataError::Failed("down".to_owned())),
        Ok(json_bytes(r#"{"n":2}"#)),
    ]);
    world.connect_data.run = Some(run);
}

#[given(r#"a client whose data callback signals "unauthorized""#)]
fn a_data_callback_denying(world: &mut ClientWorld) {
    let mut run = DataRun::new(None, None);
    run.answers = VecDeque::from([Err(DataError::Unauthorized)]);
    world.connect_data.run = Some(run);
}

#[given("a client whose data callback has not answered since connect was called")]
fn a_data_callback_not_answering(world: &mut ClientWorld) {
    let mut run = DataRun::new(None, None);
    run.handle(Input::Connect);
    assert!(run.pending.is_some(), "{:?}", run.actions);
    world.connect_data.run = Some(run);
}

#[when("the data callback answers")]
fn the_data_callback_answers(world: &mut ClientWorld) {
    let run = run(world);
    let fetch = run.pending.take().expect("a data fetch not answered");
    run.handle(Input::ConnectDataFetched {
        fetch,
        result: Ok(json_bytes(DATA)),
    });
}

#[given(
    regex = r#"^a client without an initial token, with a data callback whose token callback (signals "unauthorized"|fails with a temporary error|returns an empty token)$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_token_callback_answering(world: &mut ClientWorld, outcome: String) {
    let token = match outcome.as_str() {
        r#"signals "unauthorized""# => TokenAnswer::Unauthorized,
        "fails with a temporary error" => TokenAnswer::Failed,
        _ => TokenAnswer::Empty,
    };
    let mut run = DataRun::new(None, Some(token));
    run.answers = VecDeque::from([Ok(json_bytes(DATA))]);
    world.connect_data.run = Some(run);
}

#[given("a client without an initial token, with a token callback and a data callback")]
fn a_token_and_a_data_callback(world: &mut ClientWorld) {
    let mut run = DataRun::new(None, Some(TokenAnswer::Token));
    run.answers = VecDeque::from([Ok(json_bytes(DATA))]);
    world.connect_data.run = Some(run);
}

#[given("a client with an initial token, a token callback and a data callback")]
fn an_initial_token_and_callbacks(world: &mut ClientWorld) {
    let mut run = DataRun::new(Some("t0"), Some(TokenAnswer::Token));
    run.answers = VecDeque::from([Ok(json_bytes(DATA))]);
    world.connect_data.run = Some(run);
}

#[then(regex = r#"^the client emits an "error" event of kind connect data with code (\d+)$"#)]
fn an_error_of_kind_connect_data(world: &mut ClientWorld, code: u32) {
    let run = run(world);
    let error = run
        .actions
        .iter()
        .find_map(|action| match action {
            Action::Emit(_, ClientEvent::Error(error)) => Some(error),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no error event: {:?}", run.actions));
    assert_eq!(
        (error.kind, error.code),
        (ErrorKind::ConnectData, code),
        "{error:?}"
    );
    assert_eq!(code, codes::error::BAD_CONFIGURATION);
}

/// The retry follows the reconnect backoff of attempt 0 (Block 2): within
/// the minimum and twice the minimum.
#[then("the data callback is called again after the reconnect backoff delay")]
fn the_data_callback_is_called_again(world: &mut ClientWorld) {
    let run = run(world);
    let delay = run
        .actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(ferrifuge::machine::Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no reconnect scheduled: {:?}", run.actions));
    let min = Config::default().min_reconnect_delay;
    assert!((min..=min * 2).contains(&delay), "{delay:?}");
    assert_eq!(run.data_calls(), 1, "called again before the delay");
    run.handle(Input::TimerFired(ferrifuge::machine::Timer::Reconnect));
    assert_eq!(run.data_calls(), 2, "{:?}", run.actions);
}

#[then("the connect command carries the data the second call returned")]
fn the_connect_command_carries_the_second_data(world: &mut ClientWorld) {
    let command = run(world).connect_command();
    assert_eq!(
        command["connect"]["data"],
        serde_json::json!({"n": 2}),
        "{command}"
    );
}

#[then("no transport is opened")]
fn no_transport_is_opened(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.opened(), Vec::new(), "{:?}", run.actions);
}

#[then("no reconnect is scheduled")]
fn no_reconnect_is_scheduled(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        !run.actions.iter().any(|action| matches!(
            action,
            Action::SetTimer(ferrifuge::machine::Timer::Reconnect, _)
        )),
        "{:?}",
        run.actions
    );
}

pub(crate) fn emitted_disconnected(world: &mut ClientWorld, code: u32, reason: &str) {
    let run = run(world);
    let disconnected = run
        .actions
        .iter()
        .find_map(|action| match action {
            Action::Emit(_, ClientEvent::Disconnected { code, reason }) => Some((*code, reason)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no disconnected event: {:?}", run.actions));
    assert_eq!((disconnected.0, disconnected.1.as_str()), (code, reason));
    assert!(
        !run.actions
            .iter()
            .any(|action| matches!(action, Action::Emit(_, ClientEvent::Error(_)))),
        "an error event: {:?}",
        run.actions
    );
}

pub(crate) fn no_data_callback(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.data_calls(), 0, "{:?}", run.calls);
}

pub(crate) fn token_before_data(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.calls, ["token", "data"]);
}

pub(crate) fn token_callback_not_called(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.calls, ["data"]);
}

#[then(regex = r"^the connect command carries (both the token|the initial token) and the data$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_connect_command_carries_token_and_data(world: &mut ClientWorld, which: String) {
    let command = run(world).connect_command();
    let token = if which == "both the token" {
        "t1"
    } else {
        "t0"
    };
    assert_eq!(command["connect"]["token"], token, "{command}");
    assert_eq!(
        command["connect"]["data"],
        serde_json::json!({"n": 1}),
        "{command}"
    );
}

// @B: a real Centrifugo and the connect proxy.

#[given("a connect proxy recording requests")]
fn a_recording_connect_proxy(world: &mut ClientWorld) {
    proxy(world);
}

fn proxy(world: &ClientWorld) -> &crate::support::proxy::ProxyBackend {
    world
        .proxy
        .as_ref()
        .expect("this step needs the proxy backend: tag the scenario @B")
}

#[given(regex = r"^a client with connect data (\{[^}]*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_connect_data(world: &mut ClientWorld, data: String) {
    world.connect_data.data = Some(json_bytes(&data));
}

#[given(
    regex = r"^a client with connect data (\{.*\}) and a data callback returning (\{.*\}) then (\{.*\})$"
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_a_data_callback(
    world: &mut ClientWorld,
    data: String,
    first: String,
    then: String,
) {
    world.connect_data.data = Some(json_bytes(&data));
    world.connect_data.callback = Some(Arc::new(Mutex::new(VecDeque::from([
        json_bytes(&first),
        json_bytes(&then),
    ]))));
}

#[when(r#"the client reaches state "connected" again"#)]
async fn the_client_reaches_connected_again(world: &mut ClientWorld) {
    let reached = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let ClientEvent::Connected { .. } = world.next_event().await {
                return;
            }
        }
    })
    .await;
    assert!(reached.is_ok(), "not connected again");
}

/// The data of the connect requests the proxy received.
fn recorded_data(world: &ClientWorld) -> Vec<Value> {
    let proxy = proxy(world);
    (0..proxy.requests())
        .map(|index| proxy.request(index)["data"].clone())
        .collect()
}

#[then(regex = r"^the connect proxy recorded data (\{.*\}) on both connect requests$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_proxy_recorded_data_twice(world: &mut ClientWorld, data: String) {
    let expected: Value = serde_json::from_str(&data).expect("JSON data");
    assert_eq!(recorded_data(world), [expected.clone(), expected]);
}

#[then(regex = r"^the connect proxy recorded data (\{.*\}) then (\{.*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_proxy_recorded_data_in_turn(world: &mut ClientWorld, first: String, then: String) {
    let first: Value = serde_json::from_str(&first).expect("JSON data");
    let then: Value = serde_json::from_str(&then).expect("JSON data");
    assert_eq!(recorded_data(world), [first, then]);
}

#[given(regex = r"^the connect proxy answers with data (\{.*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_proxy_answers_with_data(world: &mut ClientWorld, data: String) {
    proxy(world).connect_data(serde_json::from_str(&data).expect("JSON data"));
}

#[then(regex = r#"^the client emits "connected" with data (\{.*\})$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn connected_with_data(world: &mut ClientWorld, data: String) {
    expect_event(world, "connecting").await;
    let event = expect_event(world, "connected").await;
    let ClientEvent::Connected { data: got, .. } = &event else {
        unreachable!("expect_event checked the name");
    };
    let got: Option<Value> = got
        .as_deref()
        .map(|bytes| serde_json::from_slice(bytes).expect("JSON data"));
    assert_eq!(
        got,
        Some(serde_json::from_str::<Value>(&data).expect("JSON data")),
        "{event:?}"
    );
}

// @D: the fake server.

#[given(regex = r"^a fake server answering connects with the non-canonical JSON data (.+)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_fake_server_with_connect_data(world: &mut ClientWorld, raw: String) {
    world.fake().answer_connects_with_data(&raw);
    world.connect_data.raw = Some(raw);
}

#[when("a client connects to it")]
fn a_client_connects_to_it(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    world.client().connect();
}

#[then(r#"the client emits "connected" with data exactly those bytes"#)]
async fn connected_with_exact_data(world: &mut ClientWorld) {
    expect_event(world, "connecting").await;
    let event = expect_event(world, "connected").await;
    let ClientEvent::Connected { data, .. } = &event else {
        unreachable!("expect_event checked the name");
    };
    let raw = world.connect_data.raw.as_deref().expect("raw data");
    assert_eq!(data.as_deref(), Some(raw.as_bytes()), "{event:?}");
}

// 17b: HTTP and emulated headers.

/// A header given as `Name: value`.
pub(super) fn header(header: &str) -> (String, String) {
    let (name, value) = header.split_once(": ").expect("Name: value");
    (name.to_owned(), value.to_owned())
}

#[given(
    regex = r#"^a client with HTTP header "([^"]+)" configured with only the server's (\w+) endpoint$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
pub(super) fn a_client_with_an_http_header(
    world: &mut ClientWorld,
    given: String,
    transport: String,
) {
    world.connect_data.http_headers.push(header(&given));
    let kind = super::transports::kind(&transport);
    let url = super::transports::server_url(super::transports::server(world), kind);
    super::transports::configure(world, vec![(kind, url)]);
}

#[given(regex = r#"^a client with emulated header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_client_with_an_emulated_header(world: &mut ClientWorld, given: String) {
    world.connect_data.emulated_headers.push(header(&given));
}

#[then(regex = r#"^the connect proxy received header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn the_proxy_received_header(world: &mut ClientWorld, given: String) {
    let (name, value) = header(&given);
    let arrived = tokio::time::timeout(Duration::from_secs(10), async {
        while proxy(world).requests() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(arrived.is_ok(), "no connect request");
    let received = proxy(world).request_headers(&name);
    assert!(!received.is_empty(), "no connect request");
    assert!(
        received
            .iter()
            .all(|got| got.as_deref() == Some(value.as_str())),
        "{name}: {received:?}"
    );
}

#[given(
    regex = r#"^a client with HTTP header "([^"]+)" connected over transport "(\w+)" through the HTTP proxy$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn a_client_with_a_header_behind_the_http_proxy(
    world: &mut ClientWorld,
    given: String,
    transport: String,
) {
    world.connect_data.http_headers.push(header(&given));
    let kind = super::transports::kind(&transport);
    let http_proxy = super::transports::pinging_server_behind_an_http_proxy(world).await;
    let url = format!("http://{}/connection/{}", http_proxy.address(), kind.name());
    super::transports::configure(world, vec![(kind, url)]);
    super::transports::connect_and_wait(world).await;
    world.http_proxy = Some(http_proxy);
}

#[then("the HTTP proxy relayed the stream request and at least one emulation request")]
fn the_proxy_relayed_both(world: &mut ClientWorld) {
    let relayed = relayed(world);
    assert!(
        relayed
            .iter()
            .any(|(path, _)| path.starts_with("/connection/")),
        "{relayed:?}"
    );
    assert!(
        relayed.iter().any(|(path, _)| path == "/emulation"),
        "{relayed:?}"
    );
}

fn relayed(world: &ClientWorld) -> Vec<(String, Option<String>)> {
    world
        .http_proxy
        .as_ref()
        .expect("an HTTP proxy")
        .requests_with_header("x-device-real")
}

#[then(regex = r#"^every request it relayed carried header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn every_request_carried(world: &mut ClientWorld, given: String) {
    let (name, value) = header(&given);
    let relayed: Vec<_> = world
        .http_proxy
        .as_ref()
        .expect("an HTTP proxy")
        .requests_with_header(&name);
    assert!(
        relayed
            .iter()
            .all(|(_, got)| got.as_deref() == Some(value.as_str())),
        "{relayed:?}"
    );
}

#[given(regex = r#"^client options with HTTP header "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn client_options_with_a_header(world: &mut ClientWorld, given: String) {
    world.pending_builder = Some(
        ferrifuge::Client::builder(super::UNREACHABLE_ENDPOINT).http_headers([header(&given)]),
    );
}

#[then(regex = r#"^building fails with a configuration error naming "([^"]+)" and not "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn building_fails_naming(world: &mut ClientWorld, name: String, value: String) {
    let error = world.build_error.as_ref().expect("building succeeded");
    assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
    assert!(error.message.contains(&name), "{error:?}");
    assert!(!format!("{error:?}").contains(&value), "{error:?}");
}

#[given(
    regex = r#"^client options with token "([^"]+)", connect data (\{.*\}), HTTP header "([^"]+)" and emulated header "([^"]+)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn client_options_with_secrets(
    world: &mut ClientWorld,
    token: String,
    data: String,
    http: String,
    emulated: String,
) {
    world.pending_builder = Some(
        ferrifuge::Client::builder(super::UNREACHABLE_ENDPOINT)
            .config(Config::default().token(token).data(json_bytes(&data)))
            .http_headers([header(&http)])
            .emulated_headers([header(&emulated)]),
    );
}

#[when("their debug output is formatted")]
fn their_debug_output(world: &mut ClientWorld) {
    let builder = world.pending_builder.as_ref().expect("client options");
    world.connect_data.debug = Some(format!("{builder:?}"));
}

#[then(regex = r#"^it names the header "([^"]+)" and shows the data as (\d+) bytes$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_names_the_header(world: &mut ClientWorld, name: String, bytes: usize) {
    let debug = world.connect_data.debug.as_deref().expect("debug output");
    assert!(debug.contains(&name), "{debug}");
    assert!(debug.contains(&format!("<{bytes} bytes>")), "{debug}");
}

#[then(regex = r"^it contains none of (.+)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_contains_none_of(world: &mut ClientWorld, secrets: String) {
    let debug = world.connect_data.debug.as_deref().expect("debug output");
    for secret in secrets.split('"').skip(1).step_by(2) {
        assert!(!debug.contains(secret), "{secret} in {debug}");
    }
}

// 17c: set_token, set_data, set_emulated_headers.

#[given(
    regex = r#"^a client with token "(\w+)", no token callback and a minimum reconnect delay of (\d+) s, connected to the fake server$"#
)]
async fn a_client_with_a_fixed_token(world: &mut ClientWorld, token: String, delay: u64) {
    world.connect_data.token = Some(token);
    world.connect_data.min_reconnect_delay = Some(Duration::from_secs(delay));
    super::connection::a_connected_client(world).await;
}

#[given(regex = r#"^the fake server will answer the next connect with error (\d+) "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_fake_server_fails_the_next_connect(world: &mut ClientWorld, code: u32, message: String) {
    world.fake().fail_next_connect_with(code, &message);
}

#[when(
    regex = r#"^set_token is called with "(\w+)" when the client emits the "error" event with code (\d+)$"#
)]
/// `set_token` must land before the reconnect timer, at least the minimum
/// reconnect delay after the 109: the core then finds the token expired
/// with no callback and stops, before any transport opens. The scenario's
/// 2 s keeps both attempts (2–4 s, then 2–6 s) under the 10 s step
/// timeouts while doubling the time a loaded runner has to get here.
async fn set_token_on_the_error(world: &mut ClientWorld, token: String, code: u32) {
    loop {
        let event = world.next_event().await;
        let matched = matches!(&event, ClientEvent::Error(error) if error.code == code);
        world.connect_data.seen.push(event);
        if matched {
            break;
        }
    }
    world.client().set_token(token);
}

/// The connect commands the fake server received, oldest first.
fn connects(world: &ClientWorld) -> Vec<Value> {
    world
        .fake()
        .received()
        .into_iter()
        .filter(|command| command["connect"].is_object())
        .collect()
}

/// Waits for a connect command the fake server received after `after`
/// commands of any method; the commands received from there to it.
async fn next_connect_after(world: &ClientWorld, after: usize) -> Vec<Value> {
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let received = world.fake().received();
            let since: Vec<Value> = received.into_iter().skip(after).collect();
            if let Some(index) = since
                .iter()
                .position(|command| command["connect"].is_object())
            {
                return since[..=index].to_vec();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    found.expect("a connect command")
}

#[then(regex = r#"^the next connect command carries token "(\w+)"$"#)]
async fn the_next_connect_carries_token(world: &mut ClientWorld, token: String) {
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(last) = connects(world).last()
                && last["connect"]["token"] == token
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(found.is_ok(), "{:?}", connects(world));
}

#[then(r#"the client emits no "error" event of kind configuration"#)]
async fn no_configuration_error(world: &mut ClientWorld) {
    // Events after "connected" too, within a moment.
    let _ = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            let event = world.next_event().await;
            world.connect_data.seen.push(event);
        }
    })
    .await;
    assert!(
        !world.connect_data.seen.iter().any(|event| matches!(
            event,
            ClientEvent::Error(error) if error.kind == ErrorKind::Configuration
        )),
        "{:?}",
        world.connect_data.seen
    );
}

#[given("a client using a token callback whose connect was answered with error 109")]
fn a_connect_answered_with_109(world: &mut ClientWorld) {
    let mut run = TokenRun {
        machine: Machine::with_token_source(Config::default().token("old".to_owned())),
        actions: Vec::new(),
    };
    let transport = run.handle(Input::Connect);
    run.handle(Input::TransportOpened(transport.expect("a transport")));
    let id = run.connect_id().expect("a connect command");
    run.handle(Input::Frame {
        transport: transport.expect("a transport"),
        data: format!(r#"{{"id":{id},"error":{{"code":109,"message":"token expired"}}}}"#)
            .into_bytes(),
    });
    world.connect_data.token_run = Some(run);
}

/// The sans-IO core of the 17c token scenario.
pub(crate) struct TokenRun {
    machine: Machine,
    actions: Vec<Action>,
}

impl TokenRun {
    /// Handles `input`; the transport it opened, if any.
    fn handle(&mut self, input: Input) -> Option<TransportId> {
        let actions = self.machine.handle(input);
        let opened = actions.iter().find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        });
        self.actions.extend(actions);
        opened
    }

    fn connect_id(&self) -> Option<u64> {
        self.actions.iter().rev().find_map(|action| match action {
            Action::Send { frame, .. } => serde_json::from_slice::<Value>(frame)
                .ok()
                .filter(|command| command["connect"].is_object())
                .and_then(|command| command["id"].as_u64()),
            _ => None,
        })
    }
}

#[when(regex = r#"^set_token is called with "(\w+)" before the next attempt$"#)]
fn set_token_before_the_next_attempt(world: &mut ClientWorld, token: String) {
    let run = world
        .connect_data
        .token_run
        .as_mut()
        .expect("a 17c token run");
    run.handle(Input::SetToken(token));
}

#[then("the token callback is called before the next connect command")]
fn the_token_callback_is_called_first(world: &mut ClientWorld) {
    let run = world
        .connect_data
        .token_run
        .as_mut()
        .expect("a 17c token run");
    let from = run.actions.len();
    let transport = run.handle(Input::TimerFired(ferrifuge::machine::Timer::Reconnect));
    let fetch = run.actions[from..]
        .iter()
        .find_map(|action| match action {
            Action::FetchToken(fetch) => Some(*fetch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no token fetch: {:?}", &run.actions[from..]));
    assert_eq!(transport, None, "a transport opened before the token");
    let transport = run.handle(Input::TokenFetched {
        fetch,
        result: Ok("fresh".to_owned()),
    });
    if let Some(transport) = transport {
        run.handle(Input::TransportOpened(transport));
    }
    let connect = run.actions[from..]
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::Send { frame, .. } => serde_json::from_slice::<Value>(frame)
                .ok()
                .filter(|command| command["connect"].is_object()),
            _ => None,
        })
        .expect("a connect command after the token");
    assert_eq!(connect["connect"]["token"], "fresh", "{connect}");
}

#[given(regex = r"^a client with connect data (\{.*\}), connected to the fake server$")]
async fn a_client_with_data_connected(world: &mut ClientWorld, data: String) {
    world.connect_data.data = Some(json_bytes(&data));
    super::connection::a_connected_client(world).await;
}

#[when(regex = r"^set_data is called with (\{.*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn set_data_is_called(world: &mut ClientWorld, data: String) {
    world.connect_data.received_at_set = Some(world.fake().received().len());
    world.client().set_data(json_bytes(&data));
}

#[then("the fake server received no command between set_data and the disconnect")]
async fn no_command_after_set_data(world: &mut ClientWorld) {
    let after = world
        .connect_data
        .received_at_set
        .expect("set_data was called");
    let since = next_connect_after(world, after).await;
    assert_eq!(since.len(), 1, "commands before the reconnect: {since:?}");
}

#[then(regex = r"^the next connect command carries data (\{.*\})$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
async fn the_next_connect_carries_data(world: &mut ClientWorld, data: String) {
    let after = world
        .connect_data
        .received_at_set
        .expect("set_data was called");
    let since = next_connect_after(world, after).await;
    let connect = since.last().expect("a connect command");
    let expected: Value = serde_json::from_str(&data).expect("JSON data");
    assert_eq!(connect["connect"]["data"], expected, "{connect}");
}

#[given(regex = r#"^a connected client with emulated header "([^"]+)"$"#)]
async fn a_connected_client_with_a_header(world: &mut ClientWorld, given: String) {
    world.connect_data.emulated_headers.push(header(&given));
    super::connection::a_connected_client(world).await;
}

#[when(regex = r#"^set_emulated_headers is called with "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn set_emulated_headers_is_called(world: &mut ClientWorld, given: String) {
    world
        .client()
        .set_emulated_headers([header(&given)])
        .expect("valid headers");
}

#[then(regex = r#"^the connect proxy received header "([^"]+)" then "([^"]+)"$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn the_proxy_received_headers_in_turn(world: &mut ClientWorld, first: String, then: String) {
    let (name, first) = header(&first);
    let (_, then) = header(&then);
    let proxy = proxy(world);
    // The connect requests, to tell where an unexpected attempt came from.
    let requests: Vec<_> = (0..proxy.requests())
        .map(|index| proxy.request(index))
        .collect();
    assert_eq!(
        proxy.request_headers(&name),
        [Some(first), Some(then)],
        "connect requests: {requests:?}"
    );
}
