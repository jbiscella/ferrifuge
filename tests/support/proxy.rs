//! Proxy backend for @B scenarios: an HTTP server that Centrifugo calls on
//! every connection attempt, every subscription to a channel without
//! namespace and every RPC to a method without namespace, answering what the
//! scenario programmed.
//! Reference: <https://centrifugal.dev/docs/server/proxy>.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::centrifugo::HOST_GATEWAY;

/// User the proxy assigns to accepted connections.
pub(crate) const USER: &str = "proxy-user";

pub(crate) struct ProxyBackend {
    port: u16,
    state: Arc<Mutex<Answers>>,
    server: JoinHandle<()>,
}

#[derive(Default)]
struct Answers {
    /// Answers for the next connect requests, in order; then connections
    /// are accepted.
    programmed: VecDeque<Answer>,
    /// Bodies of the connect requests received.
    requests: Vec<Value>,
    /// Headers of the connect requests received (Block 17).
    request_headers: Vec<axum::http::HeaderMap>,
    /// How subscribe requests are answered, by channel; others succeed.
    subscribe: HashMap<String, SubscribeAnswer>,
    /// Bodies of the subscribe requests received.
    subscribe_requests: Vec<Value>,
    /// Bodies of the RPC requests received (Block 38).
    rpc_requests: Vec<Value>,
    /// When each of them arrived.
    subscribe_times: Vec<Instant>,
    /// Channels an accepted connection is subscribed to on the server side,
    /// each with whether it is recoverable.
    granted: Vec<(String, bool)>,
    /// Delay before accepting a connection, when no answer is programmed.
    accept_delay: Duration,
    /// The `data` of an accepted connection's result, if any (Block 17).
    connect_data: Option<Value>,
    /// The items a shared poll refresh answers, by channel and key, as the
    /// backend holds them now (Block 47, D18).
    shared_poll_items: HashMap<(String, String), Value>,
    /// Bodies of the shared poll refresh requests received.
    shared_poll_requests: Vec<Value>,
}

/// A programmed answer to the subscribe requests for a channel.
#[derive(Clone, Default)]
struct SubscribeAnswer {
    /// The `result` or `error` object, when not an empty result.
    body: Option<Value>,
    /// Delay before answering.
    delay: Duration,
}

enum Answer {
    Respond(Value),
    /// Answer after a delay.
    RespondAfter(Duration, Value),
    /// Leave the request unanswered for as long as the scenario runs.
    Hold,
}

impl ProxyBackend {
    /// Starts the proxy on a free port of every interface: the Centrifugo
    /// container reaches the host through the Docker host gateway.
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("0.0.0.0:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let state = Arc::new(Mutex::new(Answers::default()));
        let app = axum::Router::new()
            .route("/connect", post(connect))
            .route("/subscribe", post(subscribe))
            .route("/rpc", post(rpc))
            .route("/shared_poll_refresh", post(shared_poll_refresh))
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("proxy server");
        });
        Self {
            port,
            state,
            server,
        }
    }

    /// Centrifugo settings enabling this proxy for connections, and for
    /// subscriptions and RPCs without namespace.
    pub(crate) fn centrifugo_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("CENTRIFUGO_CLIENT_PROXY_CONNECT_ENABLED", "true".to_owned()),
            (
                "CENTRIFUGO_CLIENT_PROXY_CONNECT_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/connect", self.port),
            ),
            // Above the client's connect timeout (5 s): a connect the proxy
            // holds stays unanswered until the client gives up, rather than
            // the server answering it with an error first (brief C3).
            ("CENTRIFUGO_CLIENT_PROXY_CONNECT_TIMEOUT", "15s".to_owned()),
            // Each header travels one way only, so a scenario fails when
            // its header was sent the other way (Block 17).
            (
                "CENTRIFUGO_CLIENT_PROXY_CONNECT_HTTP_HEADERS",
                "x-device-real".to_owned(),
            ),
            (
                "CENTRIFUGO_CLIENT_PROXY_CONNECT_EMULATED_HEADERS",
                "x-device-emulated".to_owned(),
            ),
            (
                "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_SUBSCRIBE_PROXY_ENABLED",
                "true".to_owned(),
            ),
            (
                "CENTRIFUGO_CHANNEL_PROXY_SUBSCRIBE_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/subscribe", self.port),
            ),
            // Well above the slowest programmed answer (`subscribe_delay`,
            // 1 s): at the 1 s default the server gives up first, and the
            // subscribe fails instead of succeeding late.
            (
                "CENTRIFUGO_CHANNEL_PROXY_SUBSCRIBE_TIMEOUT",
                "5s".to_owned(),
            ),
            (
                "CENTRIFUGO_RPC_WITHOUT_NAMESPACE_PROXY_ENABLED",
                "true".to_owned(),
            ),
            (
                "CENTRIFUGO_RPC_PROXY_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/rpc", self.port),
            ),
            // The backend of shared poll namespaces (Block 47, D18).
            (
                "CENTRIFUGO_CHANNEL_PROXY_SHARED_POLL_REFRESH_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/shared_poll_refresh", self.port),
            ),
        ]
    }

    /// Answers subscribe requests for `channel` with this error.
    /// Makes the shared poll refresh of `channel` answer `key` with `item`
    /// (its `data`, `version`, `removed`), the key added, from now on
    /// (Block 47).
    pub(crate) fn shared_poll_item(&self, channel: &str, key: &str, mut item: Value) {
        item["key"] = json!(key);
        self.state
            .lock()
            .unwrap()
            .shared_poll_items
            .insert((channel.to_owned(), key.to_owned()), item);
    }

    /// Bodies of the shared poll refresh requests received.
    pub(crate) fn shared_poll_requests(&self) -> Vec<Value> {
        self.state.lock().unwrap().shared_poll_requests.clone()
    }

    pub(crate) fn subscribe_error(&self, channel: &str, code: u32, message: &str) {
        self.program_subscribe(channel, |answer| {
            answer.body = Some(json!({"error": {"code": code, "message": message}}));
        });
    }

    /// Answers subscribe requests for `channel` with this data.
    pub(crate) fn subscribe_data(&self, channel: &str, data: &Value) {
        self.program_subscribe(channel, |answer| {
            answer.body = Some(json!({"result": {"data": data}}));
        });
    }

    /// Answers subscribe requests for `channel` after `delay`.
    pub(crate) fn subscribe_delay(&self, channel: &str, delay: Duration) {
        self.program_subscribe(channel, |answer| answer.delay = delay);
    }

    /// Bodies of the subscribe requests received so far.
    pub(crate) fn subscribe_requests(&self) -> Vec<Value> {
        self.state.lock().unwrap().subscribe_requests.clone()
    }

    /// Bodies of the RPC requests received so far.
    pub(crate) fn rpc_requests(&self) -> Vec<Value> {
        self.state.lock().unwrap().rpc_requests.clone()
    }

    /// When each subscribe request received so far arrived.
    pub(crate) fn subscribe_times(&self) -> Vec<Instant> {
        self.state.lock().unwrap().subscribe_times.clone()
    }

    fn program_subscribe(&self, channel: &str, program: impl FnOnce(&mut SubscribeAnswer)) {
        let mut state = self.state.lock().unwrap();
        program(state.subscribe.entry(channel.to_owned()).or_default());
    }

    /// Answers the next connection attempt with a disconnect.
    pub(crate) fn disconnect_next(&self, code: u32, reason: &str) {
        self.state
            .lock()
            .unwrap()
            .programmed
            .push_back(Answer::Respond(
                json!({"disconnect": {"code": code, "reason": reason}}),
            ));
    }

    /// Accepts connections with this result `data` from now on.
    pub(crate) fn connect_data(&self, data: Value) {
        self.state.lock().unwrap().connect_data = Some(data);
    }

    /// Accepts connections only after `delay` from now on, when no answer is
    /// programmed, so that a scenario can act while the client connects.
    pub(crate) fn accept_after(&self, delay: Duration) {
        self.state.lock().unwrap().accept_delay = delay;
    }

    /// Answers the next connection attempt with a disconnect after `delay`.
    pub(crate) fn disconnect_next_after(&self, delay: Duration, code: u32, reason: &str) {
        self.state
            .lock()
            .unwrap()
            .programmed
            .push_back(Answer::RespondAfter(
                delay,
                json!({"disconnect": {"code": code, "reason": reason}}),
            ));
    }

    /// Subscribes the connections accepted from now on to `channels` on
    /// the server side (connect result `channels`); none for an empty list.
    pub(crate) fn grant(&self, channels: &[&str]) {
        self.state.lock().unwrap().granted =
            channels.iter().map(|&c| (c.to_owned(), false)).collect();
    }

    /// As [`ProxyBackend::grant`], the subscriptions recoverable: the
    /// connect result's `subs` force recovery on each channel, which needs
    /// the channel's history (`SERVER_B` keeps one for channels without a
    /// namespace).
    pub(crate) fn grant_recoverable(&self, channels: &[&str]) {
        self.state.lock().unwrap().granted =
            channels.iter().map(|&c| (c.to_owned(), true)).collect();
    }

    /// Leaves the next connection attempt unanswered, so the client stays
    /// in its connecting state while a step looks at it.
    pub(crate) fn hold_next(&self) {
        self.state
            .lock()
            .unwrap()
            .programmed
            .push_back(Answer::Hold);
    }

    /// Number of connection attempts received so far.
    pub(crate) fn requests(&self) -> usize {
        self.state.lock().unwrap().requests.len()
    }

    /// The values of header `name` of the connection attempts so far, in
    /// order, `None` where one did not carry it.
    pub(crate) fn request_headers(&self, name: &str) -> Vec<Option<String>> {
        self.state
            .lock()
            .unwrap()
            .request_headers
            .iter()
            .map(|headers| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            })
            .collect()
    }

    /// Body of the `index`-th connection attempt.
    pub(crate) fn request(&self, index: usize) -> Value {
        self.state.lock().unwrap().requests[index].clone()
    }
}

impl Drop for ProxyBackend {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn connect(
    State(state): State<Arc<Mutex<Answers>>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    let (answer, granted, accept_delay, data) = {
        let mut answers = state.lock().unwrap();
        answers.requests.push(request);
        answers.request_headers.push(headers);
        (
            answers.programmed.pop_front(),
            answers.granted.clone(),
            answers.accept_delay,
            answers.connect_data.clone(),
        )
    };
    match answer {
        Some(Answer::Respond(answer)) => Json(answer),
        Some(Answer::RespondAfter(delay, answer)) => {
            tokio::time::sleep(delay).await;
            Json(answer)
        }
        // Ends when the proxy is dropped with the scenario.
        Some(Answer::Hold) => std::future::pending().await,
        None => {
            tokio::time::sleep(accept_delay).await;
            let channels: Vec<_> = granted
                .iter()
                .filter(|(_, recoverable)| !recoverable)
                .map(|(channel, _)| channel.clone())
                .collect();
            let subs: serde_json::Map<_, _> = granted
                .iter()
                .filter(|(_, recoverable)| *recoverable)
                .map(|(channel, _)| {
                    (
                        channel.clone(),
                        json!({"override": {"force_recovery": {"value": true}}}),
                    )
                })
                .collect();
            let mut result = json!({"user": USER, "channels": channels, "subs": subs});
            if let Some(data) = data {
                result["data"] = data;
            }
            Json(json!({"result": result}))
        }
    }
}

async fn subscribe(
    State(state): State<Arc<Mutex<Answers>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let answer = {
        let mut answers = state.lock().unwrap();
        let channel = request["channel"].as_str().unwrap_or_default().to_owned();
        answers.subscribe_requests.push(request);
        answers.subscribe_times.push(Instant::now());
        answers.subscribe.get(&channel).cloned().unwrap_or_default()
    };
    tokio::time::sleep(answer.delay).await;
    Json(answer.body.unwrap_or_else(|| json!({"result": {}})))
}

/// Answers a shared poll refresh with the items programmed for the keys it
/// names (Centrifugo's shared poll documentation: the request lists
/// `items` by `key`, the result answers changed ones; an omitted item is
/// unchanged).
async fn shared_poll_refresh(
    State(state): State<Arc<Mutex<Answers>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut answers = state.lock().unwrap();
    let channel = request["channel"].as_str().unwrap_or_default().to_owned();
    let items: Vec<Value> = request["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let key = item["key"].as_str()?.to_owned();
            answers
                .shared_poll_items
                .get(&(channel.clone(), key))
                .cloned()
        })
        .collect();
    answers.shared_poll_requests.push(request);
    Json(json!({"result": {"items": items}}))
}

/// Answers RPCs: "echo" returns its input, "sum" returns `{"sum":3}`,
/// "busy" fails with a temporary error 1000 "try later", and any other
/// method returns no data.
async fn rpc(State(state): State<Arc<Mutex<Answers>>>, Json(request): Json<Value>) -> Json<Value> {
    state.lock().unwrap().rpc_requests.push(request.clone());
    Json(match request["method"].as_str() {
        Some("echo") => json!({"result": {"data": request["data"]}}),
        Some("sum") => json!({"result": {"data": {"sum": 3}}}),
        Some("busy") => {
            json!({"error": {"code": 1000, "message": "try later", "temporary": true}})
        }
        _ => json!({"result": {}}),
    })
}
