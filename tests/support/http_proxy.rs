//! Reverse proxy in front of the test server's HTTP endpoints, for @B
//! scenarios that need an emulation request to fail or to be late. It
//! relays requests and streams the responses, so `http_stream` and sse work
//! through it. For the browser (Block 44, D18) it relays `Origin` and a
//! preflight's `Access-Control-Request-*` headers to the server and its
//! `Access-Control-*` headers back, and can end its open stream responses.
//! A browser test, which cannot call the proxy's methods, drives it with
//! GET requests under [`CONTROL`].

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{
    ACCEPT, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
    CONTENT_TYPE, COOKIE, ORIGIN, SET_COOKIE, VARY,
};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Request bodies relayed: connect and emulation commands, all small.
const MAX_BODY: usize = 1 << 20;

/// Where a browser test drives the proxy, answered by the proxy itself and
/// not recorded: `end-streams` does [`HttpProxy::end_streams`], and
/// `requests/<path>` gives how many requests to `/<path>` it received.
const CONTROL: &str = "/_relay/";

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
    /// Rejects the next emulation request whose body holds these bytes.
    reject_carrying: Mutex<Option<&'static [u8]>>,
    /// Set once a programmed rejection was answered.
    rejected: AtomicBool,
    /// Delay for the next emulation request carrying an unsubscribe command.
    delay_next_unsubscribe: Mutex<Option<Duration>>,
    /// Emulation requests carrying an unsubscribe command that the server
    /// answered with success.
    unsubscribes_relayed: AtomicUsize,
    /// The path and headers of every request received, in order.
    requests: Mutex<Vec<(String, axum::http::HeaderMap)>>,
    /// Emulation requests with a larger body are answered with HTTP 413,
    /// without relaying them (Block 18).
    refuse_over: Mutex<Option<usize>>,
    /// The bodies of the requests refused that way.
    refused: Mutex<Vec<Vec<u8>>>,
    /// A `Set-Cookie` value added to the stream response (Block 43b).
    stream_cookie: Mutex<Option<String>>,
    /// A `Set-Cookie` value added to every emulation response (Block 43b).
    emulation_cookie: Mutex<Option<String>>,
    /// Emulation requests the server answered and the proxy relayed back.
    emulation_answered: AtomicUsize,
    /// Bumped to end every stream response open (Block 44, D18).
    end_streams: watch::Sender<u64>,
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
            reject_carrying: Mutex::new(None),
            rejected: AtomicBool::new(false),
            delay_next_unsubscribe: Mutex::new(None),
            unsubscribes_relayed: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            refuse_over: Mutex::new(None),
            refused: Mutex::new(Vec::new()),
            stream_cookie: Mutex::new(None),
            emulation_cookie: Mutex::new(None),
            emulation_answered: AtomicUsize::new(0),
            end_streams: watch::Sender::new(0),
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

    /// Answers the next emulation request whose body holds `bytes` with 503
    /// instead of relaying it, in place of a [`HttpProxy::reject_next_emulation`]
    /// still programmed: requests without them, such as pongs, pass.
    pub(crate) fn reject_next_emulation_carrying(&self, bytes: &'static [u8]) {
        self.state.rejected.store(false, Ordering::SeqCst);
        self.state
            .reject_next_emulation
            .store(false, Ordering::SeqCst);
        *self.state.reject_carrying.lock().unwrap() = Some(bytes);
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

    /// The path of every request received, in order, with every value of
    /// its header `name` (Block 43b: one Cookie header, not two).
    pub(crate) fn requests_with_header_values(&self, name: &str) -> Vec<(String, Vec<String>)> {
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|(path, headers)| {
                let values = headers
                    .get_all(name)
                    .iter()
                    .filter_map(|value| value.to_str().ok())
                    .map(str::to_owned)
                    .collect();
                (path.clone(), values)
            })
            .collect()
    }

    /// Adds the `Set-Cookie` value `cookie` to the stream response.
    pub(crate) fn set_cookie_on_stream(&self, cookie: &str) {
        *self.state.stream_cookie.lock().unwrap() = Some(cookie.to_owned());
    }

    /// Adds the `Set-Cookie` value `cookie` to every emulation response.
    pub(crate) fn set_cookie_on_emulation(&self, cookie: &str) {
        *self.state.emulation_cookie.lock().unwrap() = Some(cookie.to_owned());
    }

    /// How many emulation requests were answered back to the client.
    pub(crate) fn emulation_answered(&self) -> usize {
        self.state.emulation_answered.load(Ordering::SeqCst)
    }

    /// Answers every emulation request whose body exceeds `size` bytes with
    /// HTTP 413, without relaying it.
    pub(crate) fn refuse_emulation_over(&self, size: usize) {
        *self.state.refuse_over.lock().unwrap() = Some(size);
    }

    /// The bodies of the emulation requests refused with HTTP 413.
    pub(crate) fn refused(&self) -> Vec<Vec<u8>> {
        self.state.refused.lock().unwrap().clone()
    }

    /// Whether a rejection is programmed and not yet answered.
    pub(crate) fn rejection_programmed(&self) -> bool {
        self.state.reject_next_emulation.load(Ordering::SeqCst)
    }

    /// Whether the programmed rejection happened.
    pub(crate) fn rejected(&self) -> bool {
        self.state.rejected.load(Ordering::SeqCst)
    }

    /// Ends every stream response open now, as a server closing the
    /// connection would.
    pub(crate) fn end_streams(&self) {
        self.state.end_streams.send_modify(|ended| *ended += 1);
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

/// A request under [`CONTROL`], answered to any origin.
fn control(state: &ProxyState, command: &str) -> Response<Body> {
    let answer = if command == "end-streams" {
        state.end_streams.send_modify(|ended| *ended += 1);
        String::new()
    } else if let Some(path) = command.strip_prefix("requests") {
        let requests = state.requests.lock().unwrap();
        requests
            .iter()
            .filter(|(received, _)| received == path)
            .count()
            .to_string()
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Response::builder()
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(answer))
        .expect("valid control response")
}

async fn forward(State(state): State<Arc<ProxyState>>, request: Request) -> Response<Body> {
    let (parts, body) = request.into_parts();
    if let Some(command) = parts.uri.path().strip_prefix(CONTROL) {
        return control(&state, command);
    }
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
    if parts.uri.path() == "/emulation" {
        let mut carrying = state.reject_carrying.lock().unwrap();
        if carrying.is_some_and(|bytes| body.windows(bytes.len()).any(|window| window == bytes)) {
            *carrying = None;
            state.rejected.store(true, Ordering::SeqCst);
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    let limit = *state.refuse_over.lock().unwrap();
    if parts.uri.path() == "/emulation" && limit.is_some_and(|limit| body.len() > limit) {
        state.refused.lock().unwrap().push(body.to_vec());
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
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
    // The browser's own, and what CORS needs (Block 44, D18); the
    // cookies a browser sends (Block 44, D13).
    for name in [
        CONTENT_TYPE,
        ACCEPT,
        ORIGIN,
        ACCESS_CONTROL_REQUEST_METHOD,
        ACCESS_CONTROL_REQUEST_HEADERS,
        COOKIE,
    ] {
        for value in parts.headers.get_all(&name) {
            upstream = upstream.header(&name, value);
        }
    }
    let Ok(response) = upstream.send().await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    if unsubscribe && response.status().is_success() {
        state.unsubscribes_relayed.fetch_add(1, Ordering::SeqCst);
    }
    let mut relayed = Response::builder().status(response.status());
    for name in [
        CONTENT_TYPE,
        ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_ALLOW_CREDENTIALS,
        ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_EXPOSE_HEADERS,
        ACCESS_CONTROL_MAX_AGE,
        VARY,
    ] {
        for value in response.headers().get_all(&name) {
            relayed = relayed.header(&name, value);
        }
    }
    let cookie = if parts.uri.path() == "/emulation" {
        state.emulation_answered.fetch_add(1, Ordering::SeqCst);
        state.emulation_cookie.lock().unwrap().clone()
    } else if parts.uri.path().starts_with("/connection/") {
        state.stream_cookie.lock().unwrap().clone()
    } else {
        None
    };
    if let Some(cookie) = cookie {
        relayed = relayed.header(SET_COOKIE, cookie);
    }
    let mut ended = state.end_streams.subscribe();
    ended.mark_unchanged();
    let body = response.bytes_stream().take_until(async move {
        let _ = ended.changed().await;
    });
    relayed
        .body(Body::from_stream(body))
        .expect("valid relayed response")
}
