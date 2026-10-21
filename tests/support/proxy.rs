//! Connect proxy backend for @B scenarios: an HTTP server that Centrifugo
//! calls on every connection attempt, answering what the scenario programmed.
//! Reference: <https://centrifugal.dev/docs/server/proxy>.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::State;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::centrifugo::HOST_GATEWAY;

/// User the proxy assigns to accepted connections.
pub(crate) const USER: &str = "proxy-user";

pub(crate) struct ConnectProxy {
    port: u16,
    state: Arc<Mutex<Answers>>,
    server: JoinHandle<()>,
}

#[derive(Default)]
struct Answers {
    /// Answers for the next requests, in order; then connections are accepted.
    programmed: VecDeque<Answer>,
    /// Bodies of the requests received.
    requests: Vec<Value>,
}

enum Answer {
    Respond(Value),
    /// Leave the request unanswered for as long as the scenario runs.
    Hold,
}

impl ConnectProxy {
    /// Starts the proxy on a free port of every interface: the Centrifugo
    /// container reaches the host through the Docker host gateway.
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("0.0.0.0:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let state = Arc::new(Mutex::new(Answers::default()));
        let app = axum::Router::new()
            .route("/connect", post(connect))
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

    /// Centrifugo settings enabling this proxy for connections.
    pub(crate) fn centrifugo_env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("CENTRIFUGO_CLIENT_PROXY_CONNECT_ENABLED", "true".to_owned()),
            (
                "CENTRIFUGO_CLIENT_PROXY_CONNECT_ENDPOINT",
                format!("http://{HOST_GATEWAY}:{}/connect", self.port),
            ),
        ]
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

impl Drop for ConnectProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn connect(
    State(state): State<Arc<Mutex<Answers>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let answer = {
        let mut answers = state.lock().unwrap();
        answers.requests.push(request);
        answers.programmed.pop_front()
    };
    match answer {
        Some(Answer::Respond(answer)) => Json(answer),
        // Ends when the proxy is dropped with the scenario.
        Some(Answer::Hold) => std::future::pending().await,
        None => Json(json!({"result": {"user": USER}})),
    }
}
