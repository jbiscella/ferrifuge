use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::stream::FuturesUnordered;
#[cfg(adapter_tokio)]
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
#[cfg(adapter_tokio)]
use rustls::pki_types::CertificateDer;
use tokio::sync::Notify;
use tracing::Instrument;

use super::client::Client;
use super::driver::Driver;
use super::driver::Ending;
use super::driver::KeyedTimers;
use super::log::{self, Log};
use super::platform;
#[cfg(adapter_tokio)]
use super::platform::Handle;
use super::ready::Waiters;
use super::shared::Core;
use super::shared::GetConnectData;
use super::shared::GetToken;
use super::shared::Outboxes;
use super::shared::Shared;
use super::shared::SubscriptionCallbacks;
#[cfg(adapter_tokio)]
use super::tokio::proxy::Proxy;
#[cfg(adapter_tokio)]
use super::tokio::tls::{self, Trust, unparsable_root};
use crate::sans_io::{Machine, TransportKind};
use crate::{ClientError, Config, DataError, ErrorKind, MaybeSend, MaybeSync, TokenError, codes};

/// Builds a [`Client`] with options beyond [`Config`].
pub struct ClientBuilder {
    /// Transports to try in order, each with its endpoint.
    pub(super) endpoints: Vec<(TransportKind, String)>,
    pub(super) config: Config,
    pub(super) get_token: Option<GetToken>,
    pub(super) get_data: Option<GetConnectData>,
    pub(super) emulation_endpoint: Option<String>,
    /// Emulated headers, as given; checked by [`ClientBuilder::build`].
    pub(super) emulated_headers: Vec<(String, String)>,
    /// The application's websocket subprotocols, as given; checked by
    /// [`ClientBuilder::build`].
    // Block 67.
    pub(super) websocket_subprotocols: Vec<String>,
    /// The client's handler, set once it is built; behind a mutex so that
    /// the builder stays `Sync` for a handler that is only `Send`.
    // Block 41b.
    pub(super) handler: Option<Mutex<Box<dyn super::ClientHandler>>>,
    /// Where its handlers run.
    // Block 41, D2.
    pub(super) event_delivery: super::EventDelivery,
    /// The options only this target has.
    // Block 61, D2.
    pub(super) backend: super::backend::Options,
}

/// Endpoints as they may be logged.
// Block 21, D5.
struct Endpoints<'a>(&'a [(TransportKind, String)]);

impl std::fmt::Debug for Endpoints<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .map(|(kind, endpoint)| (kind, log::endpoint(endpoint))),
            )
            .finish()
    }
}

/// Header names, without their values, which may be secrets.
pub(super) struct HeaderNames<'a>(pub(super) &'a [(String, String)]);

impl std::fmt::Debug for HeaderNames<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| name))
            .finish()
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("ClientBuilder");
        f.field("endpoints", &Endpoints(&self.endpoints))
            .field("config", &self.config)
            .field("get_token", &self.get_token.is_some())
            .field("get_data", &self.get_data.is_some())
            .field(
                "emulation_endpoint",
                &self.emulation_endpoint.as_deref().map(log::endpoint),
            )
            .field("emulated_headers", &HeaderNames(&self.emulated_headers))
            // How many, never the values (Block 67, D5).
            .field("websocket_subprotocols", &self.websocket_subprotocols.len())
            .field("handler", &self.handler.is_some())
            .field("event_delivery", &self.event_delivery);
        self.backend.fields(&mut f);
        f.finish()
    }
}

impl ClientBuilder {
    /// Sets the options; [`Config::default`] otherwise.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets the callback that provides a new connection token when the
    /// current one expires, or when there is none to connect with.
    /// Return [`TokenError::Unauthorized`] to disconnect the client for good.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, get_token: F) -> Self
    where
        F: Fn(crate::ConnectionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.get_token = Some(Arc::new(move || {
            Box::pin(get_token(crate::ConnectionTokenContext {}))
        }));
        self
    }

    /// Sets the callback that provides the data of the next connect command,
    /// called before every connection attempt once a token is obtained; its
    /// data replaces [`Config::data`]. Return [`DataError::Unauthorized`] to
    /// disconnect the client for good; any other error is reported as an
    /// `error` event and the attempt retried after the reconnect backoff.
    #[must_use]
    pub fn get_data<F, Fut>(mut self, get_data: F) -> Self
    where
        F: Fn(crate::ConnectionDataContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Vec<u8>, DataError>> + MaybeSend + 'static,
    {
        self.get_data = Some(Arc::new(move || {
            Box::pin(get_data(crate::ConnectionDataContext {}))
        }));
        self
    }

    /// Adds emulated headers, sent in every connect command
    /// (`ConnectRequest.headers`), which the server forwards to its proxies
    /// as HTTP headers when its `emulated_headers` lists them (centrifuge-js
    /// `headers`). They are client-controlled input to those proxies. Names
    /// and values must be valid HTTP, or [`ClientBuilder::build`] fails.
    #[must_use]
    pub fn emulated_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.emulated_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    /// Adds websocket subprotocols, offered in the websocket upgrade after
    /// the encoding's own value, for example for a gateway that routes on
    /// `Sec-WebSocket-Protocol` before any message is read. Each call adds
    /// its values after those of earlier calls.
    ///
    /// With values set, the upgrade carries one `Sec-WebSocket-Protocol`
    /// header: `centrifuge-protobuf`, or `centrifuge-json` for JSON (which
    /// Centrifugo selects), then these values in order. The server must
    /// select the encoding's own value: a server that selects none, or one
    /// of these, fails the attempt as a transport failure, "websocket
    /// subprotocol not accepted", and the next transport is tried. The
    /// HTTP transports and WebTransport carry no subprotocol: after a
    /// fallback to them the values are not sent; `http_headers` and
    /// `emulated_headers` reach them.
    ///
    /// Each value must be an RFC 6455 token, not repeat an earlier one, and
    /// not be `centrifuge-json` or `centrifuge-protobuf`, or
    /// [`ClientBuilder::build`] fails, naming the value's index (from 0,
    /// across calls), never the value. No log record, error message or
    /// `Debug` output of the client carries a value; at the TRACE level,
    /// the websocket library logs the whole upgrade request, these values
    /// included.
    #[must_use]
    pub fn websocket_subprotocols<I, V>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.websocket_subprotocols
            .extend(values.into_iter().map(Into::into));
        self
    }

    /// Sets the absolute URL of the emulation endpoint, where commands over
    /// `http_stream` and sse go, e.g. when the server is behind a path
    /// prefix; by default `/emulation` on the server of the stream endpoint
    /// (centrifuge-js `emulationEndpoint`).
    #[must_use]
    pub fn emulation_endpoint(mut self, url: impl Into<String>) -> Self {
        self.emulation_endpoint = Some(url.into());
        self
    }

    /// Creates the disconnected client.
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error when the options cannot
    /// work together, e.g. the Protobuf protocol with the sse transport.
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also when there is no runtime to run on: none was given with",
        doc = "[`ClientBuilder::runtime`] and none is current, or it has shut down."
    )]
    #[cfg_attr(
        adapter_browser,
        doc = "In the browser the client runs on the page's event loop, which is",
        doc = "always there."
    )]
    // Block 44, D4.
    pub fn build(mut self) -> Result<Client, ClientError> {
        let handler = self.handler.take();
        let client = Client::start(self)?;
        if let Some(handler) = handler {
            let handler = handler.into_inner().unwrap_or_else(PoisonError::into_inner);
            super::handler::set_client_handler(&client.shared, handler);
        }
        Ok(client)
    }

    /// The client's handler, which takes its events from the start instead
    /// of a stream; see [`Client::set_handler`].
    // Block 41, D1.
    #[must_use]
    pub fn handler(mut self, handler: impl super::ClientHandler) -> Self {
        self.handler = Some(Mutex::new(Box::new(handler)));
        self
    }

    /// Where the handlers of the client and of its subscriptions run:
    /// [`EventDelivery::Task`](super::EventDelivery::Task) by default, or
    /// on the client's driver with
    /// [`EventDelivery::Inline`](super::EventDelivery::Inline).
    // Block 41, D2.
    #[cfg_attr(
        adapter_tokio,
        doc = "Natively also on a thread of the client's own, with",
        doc = "[`EventDelivery::Thread`](super::EventDelivery::Thread)."
    )]
    #[must_use]
    pub fn event_delivery(mut self, delivery: super::EventDelivery) -> Self {
        self.event_delivery = delivery;
        self
    }
}

impl Client {
    fn start(builder: ClientBuilder) -> Result<Self, ClientError> {
        let ClientBuilder {
            endpoints,
            config,
            get_token,
            get_data,
            emulation_endpoint,
            emulated_headers,
            websocket_subprotocols,
            handler: _,
            event_delivery,
            backend,
        } = builder;
        super::subprotocols::check(&websocket_subprotocols)?;
        let super::backend::Started {
            shared: backend_shared,
            driver: backend_driver,
            runtime,
        } = backend.start(&endpoints, emulation_endpoint.as_deref(), &config)?;
        let emulated_headers = emulated_header_map(&emulated_headers)?;
        check_limits(&config)?;
        let offer = super::subprotocols::Offer::new(config.protocol, &websocket_subprotocols);
        let timeout = config.timeout;
        let max_message = config.max_message_bytes;
        let machine = machine_for(
            config,
            get_token.is_some(),
            get_data.is_some(),
            emulated_headers,
            endpoints.iter().map(|(kind, _)| *kind).collect(),
        )?;
        let runtime = runtime.resolve()?;
        let shared = Arc::new_cyclic(|weak| Shared {
            core_mutex: Mutex::new(Core::new(machine)),
            finished_disconnected: AtomicBool::new(true),
            outboxes: Mutex::new(Outboxes::default()),
            callbacks: Mutex::new(SubscriptionCallbacks::default()),
            calls: Mutex::new(HashMap::new()),
            ready: Mutex::new(Waiters::default()),
            driver_gone: AtomicBool::new(false),
            driver_ended: Notify::new(),
            actions_queued: Notify::new(),
            #[cfg(feature = "__test-hooks")]
            held_debounce_timers: Mutex::new(None),
            #[cfg(feature = "__test-hooks")]
            driver_panic_planted: AtomicBool::new(false),
            #[cfg(feature = "__test-hooks")]
            awaiting_earlier_close: Notify::new(),
            #[cfg(feature = "__test-hooks")]
            records: super::hooks::Records::default(),
            closed: AtomicBool::new(false),
            log: Log::new(endpoints.iter().map(|(_, endpoint)| endpoint.as_str())),
            backend: backend_shared.into_part(weak),
            handlers: Mutex::new(super::handler::Handlers::new(event_delivery)),
            runtime: runtime.clone(),
        });
        let driver = Driver {
            endpoints: endpoints
                .into_iter()
                .map(|(_, endpoint)| endpoint)
                .collect(),
            offer,
            emulation_endpoint,
            timeout,
            max_message,
            backend: backend_driver,
            shared: Arc::clone(&shared),
            runtime: runtime.clone(),
            _ending: Ending(Arc::clone(&shared)),
            delivery: event_delivery,
            closing: Vec::new(),
            opening: None,
            transport: None,
            kind: TransportKind::WebSocket,
            reading: true,
            upper_streak: 0,
            get_token,
            get_data,
            fetching: None,
            stale_fetching: None,
            timers: KeyedTimers::default(),
            callback_fetches: FuturesUnordered::new(),
            callback_aborts: HashMap::new(),
            taken: VecDeque::new(),
            fed: false,
        };
        let span = shared.log.span.clone();
        platform::spawn(&runtime, driver.run().instrument(span));
        // A runtime that has shut down drops the task at once, and its
        // guard closes the client (Block 28, D5).
        if shared.closed.load(Ordering::Acquire) {
            return Err(runtime_error("the tokio runtime has shut down"));
        }
        Ok(Self::owning(shared))
    }
}

/// The machine of a client: its callbacks, headers and transports, and
/// automatic batching, the driver flushing at each iteration. Checks the
/// endpoints, and the connector's limits when one is given.
// Block 36, D3; Block 37, D4.
#[cfg(adapter_tokio)]
pub(super) fn check_endpoints(
    endpoints: &[(TransportKind, String)],
    connector: bool,
    proxy_set: bool,
) -> Result<(), ClientError> {
    for (kind, endpoint) in endpoints {
        check_endpoint(*kind, endpoint)?;
    }
    if connector {
        check_connector(endpoints, proxy_set)?;
    }
    Ok(())
}

fn machine_for(
    config: Config,
    token_source: bool,
    data_source: bool,
    emulated_headers: std::collections::BTreeMap<String, String>,
    kinds: Vec<TransportKind>,
) -> Result<Machine, ClientError> {
    let machine = if token_source {
        Machine::with_token_source(config)
    } else {
        Machine::new(config)
    };
    let machine = if data_source {
        machine.with_data_source()
    } else {
        machine
    };
    machine
        .with_emulated_headers(emulated_headers)
        .with_automatic_batching()
        .with_publish_debounce()
        .with_transports(kinds)
}

#[cfg(adapter_tokio)]
/// An error of building a client without a runtime to run on. The runtime
/// given, else the current one.
// Block 28; Block 28, D1.
pub(super) fn runtime_or_current(runtime: Option<Handle>) -> Result<Handle, ClientError> {
    match runtime {
        Some(runtime) => Ok(runtime),
        None => platform::current_runtime().ok_or_else(|| {
            runtime_error(
                "no tokio runtime is current: build the client inside one, or give one \
                 with ClientBuilder::runtime",
            )
        }),
    }
}

fn runtime_error(message: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    }
}

#[cfg(adapter_tokio)]
/// A connector carries the websocket only, and replaces every proxy
/// setting.
// Block 37, D4.
fn check_connector(
    endpoints: &[(TransportKind, String)],
    proxy_set: bool,
) -> Result<(), ClientError> {
    if let Some((kind, _)) = endpoints
        .iter()
        .find(|(kind, _)| *kind != TransportKind::WebSocket)
    {
        return Err(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: format!(
                "a connector carries the websocket only, not transport {}",
                kind.as_str()
            ),
            transport: Some(*kind),
        });
    }
    if proxy_set {
        return Err(ClientError {
            kind: ErrorKind::Configuration,
            code: codes::error::BAD_CONFIGURATION,
            message: "a connector replaces the proxy setting: give one or the other".to_owned(),
            transport: None,
        });
    }
    Ok(())
}

#[cfg(adapter_browser)]
/// A client none of whose transports the page or the application's shims
/// can open is refused (centrifuge-js 63ed73e `_validateTransportConfig`,
/// "no supported transport found").
// Block 44, D12.
pub(super) fn check_usable(
    endpoints: &[(TransportKind, String)],
    shims: &super::backend::shims::Shims,
) -> Result<(), ClientError> {
    let found = shims.find();
    if endpoints.iter().any(|(kind, _)| found.usable(*kind)) {
        return Ok(());
    }
    let kinds: Vec<TransportKind> = endpoints.iter().map(|(kind, _)| *kind).collect();
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: super::backend::shims::none_usable(&kinds),
        transport: None,
    })
}

/// Whether `kind` is WebTransport.
// Block 48.
#[cfg_attr(
    not(feature = "unstable"),
    allow(
        clippy::missing_const_for_fn,
        reason = "a constant `false` without WebTransport"
    )
)]
fn is_webtransport(kind: TransportKind) -> bool {
    #[cfg(feature = "unstable")]
    {
        kind == TransportKind::WebTransport
    }
    #[cfg(not(feature = "unstable"))]
    {
        let _ = kind;
        false
    }
}

#[cfg(all(adapter_tokio, feature = "unstable"))]
/// Refuses WebTransport endpoints the native build cannot open: without the
/// `unstable-webtransport` feature, or with a raw TLS configuration QUIC
/// cannot use.
// Block 48, D1; D4.
pub(super) fn check_webtransport(
    endpoints: &[(TransportKind, String)],
    trust: &Trust,
) -> Result<(), ClientError> {
    let Some((kind, endpoint)) = endpoints.iter().find(|(kind, _)| is_webtransport(*kind)) else {
        return Ok(());
    };
    let error = |message: String| ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: Some(*kind),
    };
    #[cfg(not(feature = "unstable-webtransport"))]
    {
        let _ = (endpoint, trust);
        Err(error(super::tokio::NO_WEBTRANSPORT.to_owned()))
    }
    #[cfg(feature = "unstable-webtransport")]
    {
        use super::tokio::webtransport::{Alpn, check_quic, quic_tls};
        let Trust::Raw(raw) = trust else {
            // Ferrifuge's own configurations always pass (D4).
            return Ok(());
        };
        let host = reqwest::Url::parse(endpoint.trim())
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_else(|| "localhost".to_owned());
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .map_or_else(|| host.clone(), str::to_owned);
        check_quic(&quic_tls(raw, Alpn::H3, None), &host).map_err(error)
    }
}

/// Refuses an endpoint the transport cannot use: an empty one, or an
/// `http(s)://` one for the websocket transport, which needs `ws(s)://`;
/// `http(s)://` is right for `http_stream` and sse. The message leaves the
/// endpoint out: it may carry a token.
// Owner decision, 2026-11-04.
pub(super) fn check_endpoint(kind: TransportKind, endpoint: &str) -> Result<(), ClientError> {
    let endpoint = endpoint.trim();
    let http = endpoint.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    });
    let message = if is_webtransport(kind) {
        // WebTransport is defined over https only (Block 48, D2).
        let https = endpoint
            .split_once("://")
            .is_some_and(|(scheme, rest)| scheme.eq_ignore_ascii_case("https") && !rest.is_empty());
        if https {
            return Ok(());
        }
        "the webtransport transport needs an https:// endpoint".to_owned()
    } else if endpoint.is_empty() {
        format!("no endpoint given for the {} transport", kind.as_str())
    } else if kind == TransportKind::WebSocket && http {
        "the websocket transport needs a ws:// or wss:// endpoint, not http:// or https://"
            .to_owned()
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: Some(kind),
    })
}

#[cfg(adapter_tokio)]
/// A read-ahead limit of 0 could never let a message in. Headers a
/// transport sets itself, which an application may not set: proxy
/// credentials go through `ProxyTarget` only, never through a tunnel to the
/// server.
// Block 15; Block 23, D7.
fn transport_header(name: &HeaderName) -> bool {
    use reqwest::header::{
        ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, PROXY_AUTHORIZATION,
        TRANSFER_ENCODING, UPGRADE,
    };
    [
        HOST,
        CONNECTION,
        UPGRADE,
        CONTENT_LENGTH,
        TRANSFER_ENCODING,
        CONTENT_TYPE,
        ACCEPT,
        PROXY_AUTHORIZATION,
    ]
    .contains(name)
        || name.as_str().starts_with("sec-websocket-")
}

/// A configuration error about header `name`, never naming its value.
fn header_error(name: &str, problem: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: format!("HTTP header {name:?} {problem}"),
        transport: None,
    }
}

#[cfg(adapter_tokio)]
/// The checked HTTP headers.
// Block 17.
pub(super) fn http_header_map(headers: &[(String, String)]) -> Result<HeaderMap, ClientError> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let header = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| header_error(name, "is not a valid header name"))?;
        if transport_header(&header) {
            return Err(header_error(name, "is set by the transport"));
        }
        let value = HeaderValue::from_str(value)
            .map_err(|_| header_error(name, "has a value that is not valid HTTP"))?;
        map.insert(header, value);
    }
    Ok(map)
}

/// The checked emulated headers, by name.
// Block 17.
pub(super) fn emulated_header_map(
    headers: &[(String, String)],
) -> Result<std::collections::BTreeMap<String, String>, ClientError> {
    let mut map = std::collections::BTreeMap::new();
    for (name, value) in headers {
        #[cfg(adapter_tokio)]
        let valid = (
            HeaderName::from_bytes(name.as_bytes()).is_ok(),
            HeaderValue::from_str(value).is_ok(),
        );
        #[cfg(adapter_browser)]
        let valid = (header_name_valid(name), header_value_valid(value));
        if !valid.0 {
            return Err(header_error(name, "is not a valid header name"));
        }
        if !valid.1 {
            return Err(header_error(name, "has a value that is not valid HTTP"));
        }
        map.insert(name.clone(), value.clone());
    }
    Ok(map)
}

/// Whether `name` is an HTTP token (RFC 9110, 5.6.2), as the `http`
/// crate's `HeaderName` takes natively.
#[cfg(adapter_browser)]
fn header_name_valid(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// Whether `value` has no control character but tab, as the `http`
/// crate's `HeaderValue::from_str` takes natively.
#[cfg(adapter_browser)]
fn header_value_valid(value: &str) -> bool {
    value
        .bytes()
        .all(|b| (b >= 0x20 && b != 0x7f) || b == b'\t')
}

#[cfg(adapter_tokio)]
/// Whom the client trusts, from its extra roots and raw configuration,
/// checked.
// Block 22, D2 and D3.
pub(super) fn trust(
    roots: Vec<CertificateDer<'static>>,
    raw: Option<Arc<rustls::ClientConfig>>,
    android: bool,
) -> Result<Trust, ClientError> {
    let message = match (roots.is_empty(), raw) {
        // Android's verifier takes no extra roots (Block 24, D3).
        (false, None) if android => {
            "extra root certificates are not supported on Android: give a tls_config trusting them"
                .to_owned()
        }
        (true, None) => return Ok(Trust::Platform),
        (true, Some(raw)) => {
            if cfg!(feature = "fips") && !raw.fips() {
                "the fips build needs a TLS configuration whose fips() is true".to_owned()
            } else {
                return Ok(Trust::Raw(raw));
            }
        }
        (false, Some(_)) => {
            "extra root certificates and a TLS configuration cannot be given together: \
             put the roots in the TLS configuration"
                .to_owned()
        }
        (false, None) => match unparsable_root(&roots) {
            Some(index) => format!("extra root certificate {index} is not a valid root"),
            None => return Ok(Trust::ExtraRoots(roots.into())),
        },
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message,
        transport: None,
    })
}

#[cfg(adapter_tokio)]
/// On Android before the verifier's setup (`unready`), refuses a client
/// whose connections known now need the platform verifier: a `wss://` or
/// `https://` endpoint, an `https://` emulation endpoint with an HTTP
/// transport, an `https://` proxy for every endpoint. Any other TLS use
/// fails its handshake instead; a raw configuration is the application's.
// Block 24, D5.
pub(super) fn android_gate(
    unready: bool,
    trust: &Trust,
    endpoints: &[(TransportKind, String)],
    emulation_endpoint: Option<&str>,
    proxy: &Proxy,
) -> Result<(), ClientError> {
    if !unready || !matches!(trust, Trust::Platform) {
        return Ok(());
    }
    let secure = |url: &str| {
        url.split_once("://").is_some_and(|(scheme, _)| {
            scheme.eq_ignore_ascii_case("wss") || scheme.eq_ignore_ascii_case("https")
        })
    };
    let emulated = endpoints.iter().any(|(kind, _)| kind.is_emulated());
    let emulation = emulation_endpoint.filter(|_| emulated);
    let needed = endpoints
        .iter()
        .map(|(_, url)| url.as_str())
        .chain(emulation)
        .any(|url| secure(url) || proxy.is_https_proxy_for(url));
    if !needed {
        return Ok(());
    }
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: tls::ANDROID_SETUP_MISSING.to_owned(),
        transport: None,
    })
}

fn check_limits(config: &Config) -> Result<(), ClientError> {
    let message = if config.read_ahead_messages == 0 || config.read_ahead_bytes == 0 {
        "the read-ahead limits must be at least 1"
    } else if config.write_queue_bytes == 0 {
        "the write queue limit must be at least 1"
    } else if config.batch_frame_bytes == 0 {
        "the batch frame limit must be at least 1"
    } else if config.max_message_bytes == 0 {
        "the maximum message size (max_message_bytes) must be at least 1"
    } else if config.timeout.is_zero() {
        // A zero duration has no working meaning: every attempt would fail
        // at once, or retry with no wait (Block 53 D6).
        "Config::timeout must be more than 0"
    } else if config.handshake_timeout.is_zero() {
        "Config::handshake_timeout must be more than 0"
    } else if config.min_reconnect_delay.is_zero() {
        "Config::min_reconnect_delay must be more than 0"
    } else if config.max_reconnect_delay.is_zero() {
        "Config::max_reconnect_delay must be more than 0"
    } else {
        return Ok(());
    };
    Err(ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    })
}

// On tokio; the browser client is tested in a browser (Block 44, D18).
#[cfg(test)]
#[cfg(adapter_tokio)]
mod tests {
    use super::*;
    use crate::adapter::test_support::*;

    #[test]
    fn http_header_names_are_case_insensitive_and_a_later_value_wins() {
        let map = http_header_map(&headers(&[("X-Device", "a"), ("x-device", "b")])).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map["x-device"], "b");
    }

    #[test]
    fn headers_the_transport_sets_are_refused_without_their_value() {
        for name in ["Host", "upgrade", "Accept", "Sec-WebSocket-Protocol"] {
            let error = http_header_map(&headers(&[(name, "v-secret")])).unwrap_err();

            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(error.message.contains(name), "{error:?}");
            assert!(!error.message.contains("v-secret"), "{error:?}");
        }
    }

    #[test]
    fn invalid_headers_are_refused_without_their_value() {
        let bad_value = headers(&[("X-Device", "line\nbreak-secret")]);
        let bad_name = headers(&[("X Device", "v")]);

        for checked in [
            http_header_map(&bad_value).err(),
            emulated_header_map(&bad_value).err(),
            http_header_map(&bad_name).err(),
            emulated_header_map(&bad_name).err(),
        ] {
            let error = checked.expect("refused");
            assert_eq!(error.kind, ErrorKind::Configuration);
            assert!(!error.message.contains("secret"), "{error:?}");
        }
    }

    // Owner decision, 2026-11-04: no construction path accepts these.
    #[tokio::test]
    async fn an_empty_or_http_websocket_endpoint_is_refused() {
        for endpoint in [
            "",
            "http://127.0.0.1:1/connection/websocket",
            "HTTPS://example.com/ws",
        ] {
            let built = Client::builder(endpoint).build();

            let Err(error) = built else {
                panic!("{endpoint:?} was accepted");
            };
            assert_eq!(error.kind, crate::ErrorKind::Configuration, "{endpoint:?}");
            assert_eq!(
                error.transport,
                Some(crate::TransportKind::WebSocket),
                "{endpoint:?}"
            );
        }
    }

    #[tokio::test]
    async fn client_new_returns_an_endpoint_error_instead_of_panicking() {
        let created = Client::new("", Config::default());

        assert!(
            matches!(&created, Err(error) if error.kind == crate::ErrorKind::Configuration),
            "{created:?}"
        );
    }

    #[test]
    fn client_new_outside_a_runtime_returns_an_error() {
        let created = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default());

        let error = created.expect_err("an error, without a runtime");
        assert_eq!(error.kind, crate::ErrorKind::Configuration);
        assert!(
            error.message.contains("ClientBuilder::runtime"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn the_driver_runs_on_the_given_runtime() {
        let runtime = idle_runtime();
        let current = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        assert_eq!(runtime.metrics().num_alive_tasks(), 0);

        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(runtime.handle().clone())
            .build()
            .unwrap();

        assert_eq!(runtime.metrics().num_alive_tasks(), 1);
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            current
        );
        drop(client);
        // Dropping a runtime inside an asynchronous context panics.
        runtime.shutdown_background();
    }

    #[test]
    fn building_on_a_runtime_already_shut_down_returns_an_error() {
        let runtime = idle_runtime();
        let handle = runtime.handle().clone();
        drop(runtime);

        let built = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .runtime(handle)
            .build();

        let error = built.expect_err("an error, the runtime being gone");
        assert_eq!(error.kind, crate::ErrorKind::Configuration);
        assert!(error.message.contains("shut down"), "{}", error.message);
    }

    #[tokio::test]
    async fn http_endpoints_stay_valid_for_http_stream_and_sse() {
        let built = Client::builder_with_transports([
            (
                TransportKind::HttpStream,
                "http://127.0.0.1:1/connection/http_stream".to_owned(),
            ),
            (
                TransportKind::Sse,
                "https://127.0.0.1:1/connection/sse".to_owned(),
            ),
        ])
        .build();

        // On Android the https:// endpoint first needs the app's setup of
        // the platform verifier, which the unit tests run without (Block
        // 24, D5; Block 58, D4).
        if cfg!(all(target_os = "android", not(feature = "fips"))) {
            let error = built.expect_err("refused without the Android setup");
            assert_eq!(error.message, tls::ANDROID_SETUP_MISSING);
        } else {
            assert!(built.is_ok(), "{built:?}");
        }
    }

    #[tokio::test]
    async fn a_build_error_is_a_standard_error() {
        fn build() -> Result<Client, Box<dyn std::error::Error>> {
            let client = Client::builder_with_transports([(
                TransportKind::Sse,
                "http://127.0.0.1:1/connection/sse".to_owned(),
            )])
            .config(Config {
                protocol: crate::Protocol::Protobuf,
                ..Config::default()
            })
            .build()?;
            Ok(client)
        }

        let error = build().expect_err("Protobuf over sse is refused");

        let client_error = error.downcast_ref::<ClientError>().expect("a ClientError");
        assert_eq!(
            error.to_string(),
            format!("{} (code 12) over sse", client_error.message)
        );
    }

    /// Before Android's setup, a connection known now to need the platform
    /// verifier is refused, naming the call (Block 24, D5).
    #[test]
    fn android_refuses_tls_known_at_build_before_the_setup() {
        // Endpoints, the emulation endpoint, the proxy.
        type Case<'a> = (&'a [(TransportKind, &'a str)], Option<&'a str>, Proxy);
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"));
        let cases: [Case<'_>; 4] = [
            (
                &[(
                    TransportKind::WebSocket,
                    "wss://example.com/connection/websocket",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "https://example.com/connection/http_stream",
                )],
                None,
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::HttpStream,
                    "http://example.com/connection/http_stream",
                )],
                Some("https://example.com/emulation"),
                Proxy::none(),
            ),
            (
                &[(
                    TransportKind::WebSocket,
                    "ws://example.com/connection/websocket",
                )],
                None,
                proxy,
            ),
        ];
        for (endpoints, emulation, proxy) in cases {
            let endpoints: Vec<_> = endpoints
                .iter()
                .map(|(kind, url)| (*kind, (*url).to_owned()))
                .collect();

            let gated = android_gate(true, &Trust::Platform, &endpoints, emulation, &proxy);

            assert!(
                matches!(&gated, Err(error) if error.message.contains("ferrifuge::android::init_with_env")),
                "{endpoints:?} {emulation:?}: {gated:?}"
            );
        }
    }

    /// A no-proxy list excluding every endpoint leaves them direct, with no
    /// verifier needed (Codex, #118).
    #[test]
    fn android_accepts_endpoints_an_https_proxy_excludes_before_the_setup() {
        let proxy =
            Proxy::all(crate::ProxyTarget::new("https://proxy.example:3128").expect("a proxy"))
                .no_proxy("*");
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &proxy).is_ok());
    }

    #[test]
    fn android_accepts_plain_endpoints_and_raw_configurations_before_the_setup() {
        let plain = [(
            TransportKind::WebSocket,
            "ws://example.com/connection/websocket".to_owned(),
        )];
        let secure = [(
            TransportKind::WebSocket,
            "wss://example.com/connection/websocket".to_owned(),
        )];

        assert!(android_gate(true, &Trust::Platform, &plain, None, &Proxy::none()).is_ok());
        assert!(
            android_gate(
                true,
                &Trust::Raw(raw_config()),
                &secure,
                None,
                &Proxy::none()
            )
            .is_ok()
        );
        assert!(android_gate(false, &Trust::Platform, &secure, None, &Proxy::none()).is_ok());
    }

    /// Android's verifier takes no extra roots (Block 24, D3).
    #[test]
    fn android_refuses_extra_roots_naming_tls_config() {
        let root = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a certificate")
            .cert
            .der()
            .clone();

        let refused = trust(vec![root.clone()], None, true);
        let elsewhere = trust(vec![root], None, false);

        assert!(
            matches!(&refused, Err(error) if error.message.contains("tls_config")),
            "{:?}",
            refused.err()
        );
        assert!(elsewhere.is_ok());
    }

    /// The fips build never connects with non-FIPS cryptography: a raw
    /// configuration whose `fips()` is false fails `build()` (Block 22, D3).
    #[cfg(feature = "fips")]
    #[test]
    fn the_fips_build_refuses_a_raw_configuration_that_is_not_fips() {
        let mut raw = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::default_fips_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        assert!(raw.fips());
        raw.require_ems = false;

        let built = Client::builder("wss://localhost/connection/websocket")
            .tls_config(Arc::new(raw))
            .build();

        assert!(
            matches!(&built, Err(error) if error.message.contains("fips")),
            "{built:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn debug_output_of_builder_and_client_never_shows_tokens() {
        let config = Config {
            token: "secret-token".into(),
            ..Config::default()
        };
        let builder = Client::builder("ws://127.0.0.1:1/connection/websocket").config(config);
        let printed_builder = format!("{builder:?}");
        let client = builder.build().unwrap();
        // Queues the connect command, which carries the token, unperformed.
        client.connect();

        let printed = format!("{printed_builder} {client:?}");

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    #[test]
    fn the_builder_shows_whether_it_has_a_connector() {
        let without = Client::builder("ws://127.0.0.1:1/connection/websocket");
        let with = Client::builder("ws://127.0.0.1:1/connection/websocket").connector(
            |_: http::Uri| async {
                Err::<tokio::net::TcpStream, _>(std::io::Error::other("never called"))
            },
        );

        assert!(format!("{without:?}").contains("connector: false"));
        assert!(format!("{with:?}").contains("connector: true"));
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn raw_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(tls::crypto_provider()))
                .with_safe_default_protocol_versions()
                .expect("protocol versions")
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    // Block 61, D2 (reviewer, 2026-11-15): the runtime is checked last, as
    // before the backend held it: a builder with no runtime and another
    // fault reports the other fault.
    #[test]
    fn with_no_runtime_and_a_bad_limit_the_limit_is_reported() {
        let config = Config {
            write_queue_bytes: 0,
            ..Config::default()
        };

        let error = Client::new("ws://127.0.0.1:1/connection/websocket", config).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Configuration);
        assert_eq!(error.code, codes::error::BAD_CONFIGURATION);
        assert_eq!(error.message, "the write queue limit must be at least 1");
        assert_eq!(error.transport, None);
    }

    // Block 61, D2: each backend holds its options, so the builder, the
    // shared layer, the driver and the client's start name no target.
    #[test]
    fn the_builder_the_shared_layer_the_driver_and_start_name_no_target() {
        let read = |file: &str| {
            std::fs::read_to_string(crate::source_root().join("src/adapter").join(file))
                .expect("a source file")
                .replace("\r\n", "\n")
        };
        // From `start` to the first `end` after it.
        let item = |text: &str, start: &str, end: &str| -> String {
            let from = text.find(start).unwrap_or_else(|| panic!("{start}"));
            let length = text[from..]
                .find(end)
                .unwrap_or_else(|| panic!("the end of {start}"));
            text[from..from + length].to_owned()
        };
        let builder = read("builder.rs");
        let items = [
            (
                "ClientBuilder",
                item(&builder, "\npub struct ClientBuilder {\n", "\n}\n"),
            ),
            (
                "Client::start",
                item(
                    &builder,
                    "\n    fn start(builder: ClientBuilder)",
                    "\n    }\n",
                ),
            ),
            (
                "Shared",
                item(
                    &read("shared.rs"),
                    "\npub(super) struct Shared {\n",
                    "\n}\n",
                ),
            ),
            (
                "Driver",
                item(
                    &read("driver.rs"),
                    "\npub(super) struct Driver {\n",
                    "\n}\n",
                ),
            ),
        ];

        let mut found = Vec::new();
        for (name, text) in items {
            assert!(text.contains("backend"), "{name}: {text}");
            for target in ["adapter_tokio", "adapter_browser"] {
                if text.contains(target) {
                    found.push(format!("{name} names {target}"));
                }
            }
        }

        assert!(found.is_empty(), "{found:#?}");
    }
}
