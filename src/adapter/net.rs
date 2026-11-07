//! How one client reaches the server (Blocks 22 and 23): its TLS settings,
//! its proxy setting, and the HTTP clients of `http_stream` and sse built
//! from both.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use reqwest::Url;
use rustls::ClientConfig;

use super::client::lock;
use super::proxy::{Proxy, ProxyTarget};
use super::tls::{BuildOnce, Tls, plain_tls_config};

/// The network settings of one client.
#[derive(Debug, Default)]
pub(super) struct Network {
    pub(super) tls: Tls,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    proxy: Proxy,
    /// The client's own HTTP clients, for the current proxy setting.
    clients: Arc<Clients>,
    /// The decisions of the current attempt, by endpoint: a proxy function
    /// is asked once per endpoint per attempt (Block 23, D1), though the
    /// HTTP client asks for each request.
    decided: HashMap<String, Option<ProxyTarget>>,
}

#[derive(Debug, Default)]
struct Clients {
    secure: BuildOnce<reqwest::Client>,
    plain: BuildOnce<reqwest::Client>,
}

impl Network {
    pub(super) fn new(tls: Tls, proxy: Proxy) -> Self {
        Self {
            tls,
            state: Arc::new(Mutex::new(State {
                proxy,
                ..State::default()
            })),
        }
    }

    /// Replaces the proxy setting from the next connection attempt
    /// (Block 23, D1); the HTTP clients are built again.
    pub(super) fn set_proxy(&self, proxy: Proxy) {
        *lock(&self.state) = State {
            proxy,
            ..State::default()
        };
    }

    /// A new connection attempt starts: its endpoints are decided again.
    pub(super) fn new_attempt(&self) {
        lock(&self.state).decided.clear();
    }

    /// The proxy for `url` under the current setting (Block 23, D2),
    /// decided once per endpoint per attempt.
    pub(super) fn decide(&self, url: &Url) -> Result<Option<ProxyTarget>, String> {
        decide_once(&self.state, url)
    }

    /// The HTTP client of `https://` or plain requests: built once, per
    /// process for a client with the default proxy setting and the
    /// platform's trust, per client and per proxy setting otherwise; not
    /// kept when its TLS settings are not (Block 6, C7).
    pub(super) fn http_client(&self, secure: bool) -> Result<reqwest::Client, String> {
        static SECURE: BuildOnce<reqwest::Client> = BuildOnce::new();
        static PLAIN: BuildOnce<reqwest::Client> = BuildOnce::new();
        let (system, own) = {
            let state = lock(&self.state);
            (state.proxy.is_system(), Arc::clone(&state.clients))
        };
        let shared = system && self.tls.is_platform();
        let once = match (shared, secure) {
            (true, true) => &SECURE,
            (true, false) => &PLAIN,
            (false, true) => &own.secure,
            (false, false) => &own.plain,
        };
        let (client, _) = once.get(|| {
            let (config, keep) = if secure {
                let (config, keep) = self.tls.setup()?;
                ((*config).clone(), keep)
            } else {
                // A plain request may still go through an https:// proxy,
                // whose certificate the client's settings verify; without
                // them, plain requests still work, and the next attempt
                // tries again.
                match self.tls.setup() {
                    Ok((config, keep)) => ((*config).clone(), keep),
                    Err(_) => (plain_tls_config(), false),
                }
            };
            // The process-wide clients take the system's decision; a
            // client's own, its own decisions.
            let decide: Decide = if shared {
                Arc::new(|url: &Url| Proxy::system().decide(url))
            } else {
                let state = Arc::clone(&self.state);
                Arc::new(move |url: &Url| decide_once(&state, url))
            };
            build(config, decide).map(|client| (client, keep))
        })?;
        Ok(client)
    }
}

type Decide = Arc<dyn Fn(&Url) -> Result<Option<ProxyTarget>, String> + Send + Sync>;

/// The proxy for `url` under the setting of `state`, decided once per
/// endpoint per attempt.
fn decide_once(state: &Mutex<State>, url: &Url) -> Result<Option<ProxyTarget>, String> {
    let key = format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or_default(),
        url.port_or_known_default().unwrap_or_default()
    );
    let proxy = {
        let state = lock(state);
        if let Some(decided) = state.decided.get(&key) {
            return Ok(decided.clone());
        }
        state.proxy.clone()
    };
    // Outside the lock: a proxy function is the application's code.
    let decided = proxy.decide(url)?;
    lock(state).decided.insert(key, decided.clone());
    Ok(decided)
}

/// An HTTP client with `tls`, sending each request through the proxy
/// `decide` gives, and only through it: reqwest reads no proxy of its own
/// (Block 23, D4).
fn build(tls: ClientConfig, decide: Decide) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .no_proxy()
        .proxy(reqwest::Proxy::custom(move |url| {
            decide(url).ok().flatten().and_then(|target| target.url())
        }))
        // The TLS setup and the emulation endpoint follow from the
        // configured URL: a redirect elsewhere fails the attempt, like an
        // unreachable endpoint, rather than splitting the stream and the
        // commands between two servers.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())
}
