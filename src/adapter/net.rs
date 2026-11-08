//! How one client reaches the server (Blocks 22 and 23): its TLS settings,
//! its proxy setting, and the HTTP clients of `http_stream` and sse built
//! from both.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use hyper_util::client::proxy::matcher::Matcher;

use reqwest::Url;
use rustls::ClientConfig;

use super::client::lock;
use super::connector::Shared;
use super::proxy::{self, Proxy, ProxyTarget};
use super::tls::{BuildOnce, Tls, plain_tls_config};

/// The network settings of one client.
#[derive(Debug, Default)]
pub(super) struct Network {
    pub(super) tls: Tls,
    /// The application's connector, which replaces every proxy setting
    /// (Block 37, D4).
    connector: Option<Shared>,
    /// A setting given by `set_proxy`, applied at the next attempt.
    pending: Mutex<Option<Proxy>>,
    current: Mutex<Current>,
}

/// The proxy setting in use, its decisions and the HTTP clients built on
/// them. A transport keeps the decisions it was opened with, so a setting
/// changed while it runs applies from the next attempt (Block 23, D1).
#[derive(Debug, Default)]
struct Current {
    decisions: Arc<Mutex<Decisions>>,
    /// The client's own HTTP clients. Kept apart from the decisions their
    /// proxy function holds, which would otherwise own them in a cycle.
    clients: Arc<Clients>,
    /// The process-wide HTTP clients this attempt took first, kept for
    /// all its requests, whoever asks for the proxy settings to be read
    /// again meanwhile (Block 25, D7).
    shared: Arc<OnceLock<Arc<Clients>>>,
}

#[derive(Debug, Default)]
struct Decisions {
    proxy: Proxy,
    /// The decisions of the current attempt, by endpoint: a proxy function
    /// is asked once per endpoint per attempt (Block 23, D1), though the
    /// HTTP client asks for each request.
    decided: HashMap<String, Option<ProxyTarget>>,
    /// The environment's or the system's settings, read once per attempt.
    pinned: Arc<OnceLock<Arc<Matcher>>>,
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
            connector: None,
            pending: Mutex::new(None),
            current: Mutex::new(Current {
                decisions: Arc::new(Mutex::new(Decisions {
                    proxy,
                    decided: HashMap::new(),
                    pinned: Arc::default(),
                })),
                clients: Arc::default(),
                shared: Arc::default(),
            }),
        }
    }

    /// The network of a client given `connector`.
    pub(super) fn with_connector(mut self, connector: Option<Shared>) -> Self {
        self.connector = connector;
        self
    }

    /// The application's connector, if it gave one.
    pub(super) fn connector(&self) -> Option<&Shared> {
        self.connector.as_ref()
    }

    /// Replaces the proxy setting from the next connection attempt
    /// (Block 23, D1); with a connector, which is the route, nothing
    /// (Block 37, D4).
    pub(super) fn set_proxy(&self, proxy: Proxy) {
        if self.connector.is_none() {
            *lock(&self.pending) = Some(proxy);
        }
    }

    /// A new connection attempt starts: a setting given since the last one
    /// takes effect, endpoints are decided again, and the client's own HTTP
    /// clients are built again. Their pools are keyed by destination, not by
    /// proxy, so a connection through the last attempt's proxy would be
    /// reused when a proxy function now chooses another (Codex, #115).
    pub(super) fn new_attempt(&self) {
        let pending = lock(&self.pending).take();
        let mut current = lock(&self.current);
        let proxy = pending.unwrap_or_else(|| lock(&current.decisions).proxy.clone());
        *current = Current {
            decisions: Arc::new(Mutex::new(Decisions {
                proxy,
                decided: HashMap::new(),
                pinned: Arc::default(),
            })),
            clients: Arc::default(),
            shared: Arc::default(),
        };
    }

    /// The proxy for `url` under the setting in use (Block 23, D2),
    /// decided once per endpoint per attempt.
    pub(super) fn decide(&self, url: &Url) -> Result<Option<ProxyTarget>, String> {
        // A connector reads no proxy setting (Block 37, D4).
        if self.connector.is_some() {
            return Ok(None);
        }
        let decisions = Arc::clone(&lock(&self.current).decisions);
        decide_once(&decisions, url)
    }

    /// The HTTP client of `https://` or plain requests: built once, per
    /// process for a client with the default proxy setting and the
    /// platform's trust, per client and per proxy setting otherwise; not
    /// kept when its TLS settings are not (Block 6, C7).
    pub(super) fn http_client(&self, secure: bool) -> Result<reqwest::Client, String> {
        // Built again once the system's proxy settings are read again
        // (Block 25, D7).
        static SHARED: Mutex<Option<(u64, Arc<Clients>)>> = Mutex::new(None);
        let (decisions, own, pinned) = {
            let current = lock(&self.current);
            (
                Arc::clone(&current.decisions),
                Arc::clone(&current.clients),
                Arc::clone(&current.shared),
            )
        };
        let system = lock(&decisions).proxy.is_system();
        let shared = system && self.tls.is_platform();
        let process = shared.then(|| {
            Arc::clone(pinned.get_or_init(|| {
                let generation = proxy::settings_generation();
                let mut cached = lock(&SHARED);
                match &*cached {
                    Some((built_in, clients)) if *built_in == generation => Arc::clone(clients),
                    _ => {
                        let clients = Arc::<Clients>::default();
                        *cached = Some((generation, Arc::clone(&clients)));
                        clients
                    }
                }
            }))
        });
        let clients = process.as_ref().unwrap_or(&own);
        let once = if secure {
            &clients.secure
        } else {
            &clients.plain
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
            // client's own, the decisions of its setting, never the clients.
            let decide: Decide = if shared {
                Arc::new(Proxy::system_decider())
            } else {
                Arc::new(move |url: &Url| decide_once(&decisions, url))
            };
            build(config, decide).map(|client| (client, keep))
        })?;
        Ok(client)
    }
}

type Decide = Arc<dyn Fn(&Url) -> Result<Option<ProxyTarget>, String> + Send + Sync>;

/// The proxy for `url` under the setting of `decisions`, decided once per
/// endpoint per attempt.
fn decide_once(decisions: &Mutex<Decisions>, url: &Url) -> Result<Option<ProxyTarget>, String> {
    let key = format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or_default(),
        url.port_or_known_default().unwrap_or_default()
    );
    let (proxy, pinned) = {
        let decisions = lock(decisions);
        if let Some(decided) = decisions.decided.get(&key) {
            return Ok(decided.clone());
        }
        (decisions.proxy.clone(), Arc::clone(&decisions.pinned))
    };
    // Outside the lock: a proxy function is the application's code.
    let decided = proxy.decide_pinned(url, &pinned)?;
    lock(decisions).decided.insert(key, decided.clone());
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

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use super::*;

    /// A client's own HTTP client holds only the decisions, which hold no
    /// HTTP client: dropping the network frees both (Codex, #115).
    #[test]
    fn dropping_a_network_frees_its_http_clients() {
        let network = Network::new(Tls::default(), Proxy::none());
        network.http_client(false).expect("an HTTP client");
        let decisions: Weak<Mutex<Decisions>> = Arc::downgrade(&lock(&network.current).decisions);

        drop(network);

        assert!(
            decisions.upgrade().is_none(),
            "the decisions outlive the network"
        );
    }

    /// An attempt keeps the process-wide HTTP clients it took first, though
    /// another client's `reconnect()` asks for the proxy settings to be
    /// read again; the next attempt takes those of the new settings
    /// (Block 25, D7; Codex, #122).
    #[test]
    fn an_attempt_keeps_its_shared_http_clients() {
        let network = Network::new(Tls::default(), Proxy::system());
        let shared = |network: &Network| {
            Arc::clone(
                lock(&network.current)
                    .shared
                    .get()
                    .expect("the process-wide clients"),
            )
        };
        network.http_client(false).expect("an HTTP client");
        let first = shared(&network);

        proxy::read_settings_again();
        network.http_client(true).expect("an HTTP client");
        let same_attempt = shared(&network);
        network.new_attempt();
        network.http_client(false).expect("an HTTP client");
        let next_attempt = shared(&network);

        assert!(Arc::ptr_eq(&first, &same_attempt));
        assert!(!Arc::ptr_eq(&first, &next_attempt));
    }

    /// A setting given while a transport runs keeps its decisions until the
    /// next attempt (Codex, #115).
    #[test]
    fn set_proxy_applies_from_the_next_attempt() {
        let network = Network::new(Tls::default(), Proxy::none());
        let url = Url::parse("ws://centrifugo.example:8000/").expect("a URL");
        let target = ProxyTarget::new("http://proxy.example:3128").expect("a proxy");

        network.set_proxy(Proxy::all(target.clone()));
        let before = network.decide(&url);
        network.new_attempt();
        let after = network.decide(&url);

        assert_eq!(before, Ok(None));
        assert_eq!(after, Ok(Some(target)));
    }

    /// Each attempt gets new HTTP clients, whose pools hold no connection
    /// through the last attempt's proxy (Codex, #115).
    #[test]
    fn each_attempt_gets_new_http_clients() {
        let network = Network::new(Tls::default(), Proxy::none());
        network.http_client(false).expect("an HTTP client");
        let first = Arc::downgrade(&lock(&network.current).clients);

        network.new_attempt();

        assert!(
            first.upgrade().is_none(),
            "the last attempt's clients are kept"
        );
    }
}
