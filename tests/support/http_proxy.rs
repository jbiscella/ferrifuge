//! Reverse proxy in front of the test server's HTTP endpoints, for @B
//! scenarios that need an emulation request to fail or to be late. It
//! relays requests and streams the responses, so `http_stream` and sse work
//! through it.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Request bodies relayed: connect and emulation commands, all small.
const MAX_BODY: usize = 1 << 20;

pub(crate) struct HttpProxy {
    port: u16,
    state: Arc<ProxyState>,
    server: JoinHandle<()>,
}

struct ProxyState {
    /// `host:port` of the server.
    target: String,
    http: reqwest::Client,
    reject_next_emulation: AtomicBool,
    /// Set once a programmed rejection was answered.
    rejected: AtomicBool,
    /// Delay for the next emulation request carrying an unsubscribe command.
    delay_next_unsubscribe: Mutex<Option<Duration>>,
    /// Emulation requests carrying an unsubscribe command that the server
    /// answered with success.
    unsubscribes_relayed: AtomicUsize,
    /// The path and headers of every request received, in order.
    requests: Mutex<Vec<(String, axum::http::HeaderMap)>>,
}

impl HttpProxy {
    pub(crate) async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HTTP proxy");
        let port = listener.local_addr().expect("HTTP proxy address").port();
        let state = Arc::new(ProxyState {
            target,
            http: reqwest::Client::new(),
            reject_next_emulation: AtomicBool::new(false),
            rejected: AtomicBool::new(false),
            delay_next_unsubscribe: Mutex::new(None),
            unsubscribes_relayed: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let app = axum::Router::new()
            .fallback(forward)
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("HTTP proxy server");
        });
        Self {
            port,
            state,
            server,
        }
    }

    /// Answers the next emulation request with 503 instead of relaying it.
    pub(crate) fn reject_next_emulation(&self) {
        self.state.rejected.store(false, Ordering::SeqCst);
        self.state
            .reject_next_emulation
            .store(true, Ordering::SeqCst);
    }

    /// Relays the next emulation request carrying an unsubscribe command
    /// only after `delay`, so a later request can overtake it.
    pub(crate) fn delay_next_unsubscribe(&self, delay: Duration) {
        *self.state.delay_next_unsubscribe.lock().unwrap() = Some(delay);
    }

    /// How many emulation requests carrying an unsubscribe command the
    /// server answered with success.
    pub(crate) fn unsubscribes_relayed(&self) -> usize {
        self.state.unsubscribes_relayed.load(Ordering::SeqCst)
    }

    /// The path of every request received, in order, with the value of its
    /// header `name`, if any.
    pub(crate) fn requests_with_header(&self, name: &str) -> Vec<(String, Option<String>)> {
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|(path, headers)| {
                let value = headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                (path.clone(), value)
            })
            .collect()
    }

    /// Whether a rejection is programmed and not yet answered.
    pub(crate) fn rejection_programmed(&self) -> bool {
        self.state.reject_next_emulation.load(Ordering::SeqCst)
    }

    /// Whether the programmed rejection happened.
    pub(crate) fn rejected(&self) -> bool {
        self.state.rejected.load(Ordering::SeqCst)
    }

    /// `host:port` of the proxy.
    pub(crate) fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for HttpProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn forward(State(state): State<Arc<ProxyState>>, request: Request) -> Response<Body> {
    let (parts, body) = request.into_parts();
    state
        .requests
        .lock()
        .unwrap()
        .push((parts.uri.path().to_owned(), parts.headers.clone()));
    if parts.uri.path() == "/emulation" && state.reject_next_emulation.swap(false, Ordering::SeqCst)
    {
        state.rejected.store(true, Ordering::SeqCst);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(body) = axum::body::to_bytes(body, MAX_BODY).await else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let unsubscribe = parts.uri.path() == "/emulation"
        && body
            .windows(b"unsubscribe".len())
            .any(|window| window == b"unsubscribe");
    if unsubscribe {
        let delay = state.delay_next_unsubscribe.lock().unwrap().take();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }
    let path = parts.uri.path_and_query().map_or("/", |path| path.as_str());
    let mut upstream = state
        .http
        .request(parts.method, format!("http://{}{path}", state.target))
        .body(body);
    for name in [CONTENT_TYPE, ACCEPT] {
        if let Some(value) = parts.headers.get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    let Ok(response) = upstream.send().await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    if unsubscribe && response.status().is_success() {
        state.unsubscribes_relayed.fetch_add(1, Ordering::SeqCst);
    }
    let mut relayed = Response::builder().status(response.status());
    if let Some(value) = response.headers().get(CONTENT_TYPE) {
        relayed = relayed.header(CONTENT_TYPE, value);
    }
    relayed
        .body(Body::from_stream(response.bytes_stream()))
        .expect("valid relayed response")
}
