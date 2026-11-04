//! Proxy backend for @B scenarios: an HTTP server that Centrifugo calls on
//! every connection attempt and every subscription to a channel without
//! namespace, answering what the scenario programmed.
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
    /// How subscribe requests are answered, by channel; others succeed.
    subscribe: HashMap<String, SubscribeAnswer>,
    /// Bodies of the subscribe requests received.
    subscribe_requests: Vec<Value>,
    /// When each of them arrived.
    subscribe_times: Vec<Instant>,
    /// Channels an accepted connection is subscribed to on the server side.
    granted: Vec<String>,
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

    /// Centrifugo settings enabling this proxy for connections and for
    /// subscriptions to channels without namespace.
    pub(crate) fn centrifugo_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("CENTRIFUGO_CLIENT_PROXY_CONNECT_ENABLED", "true".to_owned()),
            (
                "CENTRIFUGO_CLIENT_PROXY_CONNECT_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/connect", self.port),
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
        ]
    }

    /// Answers subscribe requests for `channel` with this error.
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

    /// Subscribes the connections accepted from now on to `channels` on
    /// the server side (connect result `channels`); none for an empty list.
    pub(crate) fn grant(&self, channels: &[&str]) {
        self.state.lock().unwrap().granted = channels.iter().map(|&c| c.to_owned()).collect();
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
    Json(request): Json<Value>,
) -> Json<Value> {
    let (answer, granted) = {
        let mut answers = state.lock().unwrap();
        answers.requests.push(request);
        (answers.programmed.pop_front(), answers.granted.clone())
    };
    match answer {
        Some(Answer::Respond(answer)) => Json(answer),
        // Ends when the proxy is dropped with the scenario.
        Some(Answer::Hold) => std::future::pending().await,
        None => Json(json!({"result": {"user": USER, "channels": granted}})),
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
