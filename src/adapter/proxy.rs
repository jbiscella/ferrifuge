//! Proxies (Block 23): the setting, the one decision both transports take
//! for each URL, and the websocket's dialing through it. The handshakes are
//! hyper-util's (Block 0, "Components").

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use base64::Engine as _;
use hyper_rustls::MaybeHttpsStream;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::proxy::{SocksV5, Tunnel};
use hyper_util::client::proxy::matcher::{Intercept, Matcher};
use hyper_util::rt::TokioIo;
use reqwest::Url;
use reqwest::header::HeaderValue;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tower_service::Service;

use super::tls::Tls;
use crate::{ClientError, ErrorKind, codes};

/// Where a client's connections go (Block 23): the system's proxy by
/// default, the environment's, none, one proxy, or one chosen by a
/// function for each endpoint.
///
/// Its `Debug` output shows the kind of setting and a proxy by scheme,
/// host and port, never credentials.
#[derive(Clone, Default)]
pub struct Proxy(Setting);

#[derive(Clone, Default)]
enum Setting {
    #[default]
    System,
    Env,
    None,
    All(ProxyTarget, Option<Arc<Matcher>>),
    Custom(Custom, Option<Arc<Matcher>>),
}

type Custom = Arc<dyn Fn(&str) -> Option<ProxyTarget> + Send + Sync>;

impl Proxy {
    /// The environment, then the operating system's settings: `ALL_PROXY`,
    /// `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` (upper case before lower
    /// case, none of them in a CGI process, where `REQUEST_METHOD` is set),
    /// then macOS's HTTP and HTTPS proxies and Windows' Internet settings.
    /// Read once per process. The default.
    #[must_use]
    pub fn system() -> Self {
        Self(Setting::System)
    }

    /// The environment only, as [`Proxy::system`] reads it.
    #[must_use]
    pub fn from_env() -> Self {
        Self(Setting::Env)
    }

    /// Direct connections.
    #[must_use]
    pub fn none() -> Self {
        Self(Setting::None)
    }

    /// `target` for every endpoint.
    #[must_use]
    pub fn all(target: ProxyTarget) -> Self {
        Self(Setting::All(target, None))
    }

    /// The proxy `choose` returns for each endpoint, given as
    /// `scheme://host:port` (`ws`, `wss`, `http` or `https`, the port always
    /// written), or direct for `None`. It is called once per endpoint per
    /// connection attempt.
    #[must_use]
    pub fn custom(choose: impl Fn(&str) -> Option<ProxyTarget> + Send + Sync + 'static) -> Self {
        Self(Setting::Custom(Arc::new(choose), None))
    }

    /// Reaches the hosts of `list` directly, in `NO_PROXY` syntax: a domain
    /// and its subdomains, an IP address, a CIDR range, or `*`. For
    /// [`Proxy::all`] and [`Proxy::custom`]; [`Proxy::system`] and
    /// [`Proxy::from_env`] take `NO_PROXY` and the system's exceptions.
    #[must_use]
    pub fn no_proxy(self, list: &str) -> Self {
        // hyper-util's matcher parses the list: a proxy for everything but
        // the list's hosts.
        let excluded = Some(Arc::new(
            Matcher::builder().all("http://excluded").no(list).build(),
        ));
        Self(match self.0 {
            Setting::All(target, _) => Setting::All(target, excluded),
            Setting::Custom(choose, _) => Setting::Custom(choose, excluded),
            other => other,
        })
    }

    /// Whether this is the default, process-wide setting.
    pub(super) fn is_system(&self) -> bool {
        matches!(self.0, Setting::System)
    }

    /// The proxy for `url`, or `None` for a direct connection (Block 23,
    /// D2): the one decision of every transport.
    pub(super) fn decide(&self, url: &Url) -> Result<Option<ProxyTarget>, String> {
        let endpoint = endpoint(url)?;
        let matcher = match &self.0 {
            Setting::None => return Ok(None),
            Setting::System => system_matcher(),
            Setting::Env => env_matcher(),
            Setting::All(target, excluded) => {
                return Ok((!is_excluded(excluded.as_deref(), &endpoint)).then(|| target.clone()));
            }
            Setting::Custom(choose, excluded) => {
                if is_excluded(excluded.as_deref(), &endpoint) {
                    return Ok(None);
                }
                return Ok(choose(&endpoint.display()));
            }
        };
        match matcher.intercept(&endpoint.as_http()) {
            Some(intercept) => ProxyTarget::from_intercept(&intercept).map(Some),
            None => Ok(None),
        }
    }
}

impl fmt::Debug for Proxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Setting::System => f.write_str("System"),
            Setting::Env => f.write_str("FromEnv"),
            Setting::None => f.write_str("None"),
            Setting::All(target, excluded) => f
                .debug_struct("All")
                .field("target", target)
                .field("no_proxy", &excluded.is_some())
                .finish(),
            Setting::Custom(_, excluded) => f
                .debug_struct("Custom")
                .field("no_proxy", &excluded.is_some())
                .finish_non_exhaustive(),
        }
    }
}

fn system_matcher() -> &'static Matcher {
    static SYSTEM: OnceLock<Matcher> = OnceLock::new();
    SYSTEM.get_or_init(Matcher::from_system)
}

fn env_matcher() -> &'static Matcher {
    static ENV: OnceLock<Matcher> = OnceLock::new();
    ENV.get_or_init(Matcher::from_env)
}

fn is_excluded(excluded: Option<&Matcher>, endpoint: &Endpoint) -> bool {
    excluded.is_some_and(|matcher| matcher.intercept(&endpoint.as_http()).is_none())
}

/// An endpoint as the decision sees it: its scheme, host and port.
struct Endpoint {
    scheme: String,
    host: String,
    port: u16,
}

fn endpoint(url: &Url) -> Result<Endpoint, String> {
    let host = url
        .host_str()
        .ok_or_else(|| "an endpoint without a host".to_owned())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| format!("an endpoint without a port: {}", url.scheme()))?;
    Ok(Endpoint {
        scheme: url.scheme().to_owned(),
        host: host.to_owned(),
        port,
    })
}

impl Endpoint {
    /// `scheme://host:port`, as [`Proxy::custom`] gets it.
    fn display(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }

    /// The URI hyper-util's matcher asks about: `ws` and `wss` as `http`
    /// and `https`, the only schemes it knows.
    fn as_http(&self) -> http::Uri {
        let scheme = match self.scheme.as_str() {
            "ws" => "http",
            "wss" => "https",
            other => other,
        };
        format!("{scheme}://{}:{}/", self.host, self.port)
            .parse()
            .unwrap_or_else(|_| http::Uri::from_static("http://invalid/"))
    }
}

/// A proxy to go through: `http://`, `https://` (TLS to the proxy),
/// `socks5://` (DNS by the client) or `socks5h://` (DNS by the proxy), with
/// optional credentials.
///
/// Its `Debug` output shows the proxy by scheme, host and port, and whether
/// credentials are set, never them.
#[derive(Clone, PartialEq, Eq)]
pub struct ProxyTarget {
    scheme: Scheme,
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scheme {
    Http,
    Https,
    Socks5,
    Socks5h,
}

impl Scheme {
    fn name(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Socks5 => "socks5",
            Self::Socks5h => "socks5h",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "http" => Self::Http,
            "https" => Self::Https,
            "socks5" => Self::Socks5,
            "socks5h" => Self::Socks5h,
            _ => return None,
        })
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
            Self::Socks5 | Self::Socks5h => 1080,
        }
    }
}

impl ProxyTarget {
    /// The proxy at `url`, with credentials as `user:password@`,
    /// percent-encoded.
    ///
    /// # Errors
    ///
    /// A configuration error naming the scheme when `url` does not parse or
    /// is of another scheme; it never shows the credentials.
    pub fn new(url: &str) -> Result<Self, ClientError> {
        let parsed = Url::parse(url).map_err(|_| proxy_error("a proxy URL that does not parse"))?;
        let scheme = Scheme::parse(parsed.scheme()).ok_or_else(|| {
            proxy_error(&format!(
                "proxy scheme {} is not supported: http, https, socks5 or socks5h",
                parsed.scheme()
            ))
        })?;
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| proxy_error("a proxy URL without a host"))?;
        let credentials = (!parsed.username().is_empty()).then(|| {
            (
                decoded(parsed.username()),
                decoded(parsed.password().unwrap_or("")),
            )
        });
        Ok(Self {
            scheme,
            host: host.to_owned(),
            port: parsed.port().unwrap_or(scheme.default_port()),
            credentials,
        })
    }

    /// Sets the credentials, not percent-encoded: Basic for an HTTP or
    /// HTTPS proxy, username and password for SOCKS5.
    #[must_use]
    pub fn basic_auth(mut self, user: &str, password: &str) -> Self {
        self.credentials = Some((user.to_owned(), password.to_owned()));
        self
    }

    /// The proxy the environment or the system named for an endpoint.
    fn from_intercept(intercept: &Intercept) -> Result<Self, String> {
        let uri = intercept.uri();
        let name = uri.scheme_str().unwrap_or("http");
        let scheme = Scheme::parse(name).ok_or_else(|| {
            format!("proxy scheme {name} is not supported: http, https, socks5 or socks5h")
        })?;
        let host = uri
            .host()
            .ok_or_else(|| "a proxy without a host".to_owned())?;
        let credentials = match (intercept.raw_auth(), intercept.basic_auth()) {
            (Some((user, password)), _) => Some((user.to_owned(), password.to_owned())),
            (None, Some(basic)) => Some(from_basic(basic)?),
            (None, None) => None,
        };
        Ok(Self {
            scheme,
            host: host.trim_matches(['[', ']']).to_owned(),
            port: uri.port_u16().unwrap_or(scheme.default_port()),
            credentials,
        })
    }

    /// As scheme, host and port: what may be logged (Block 23, D7).
    pub(super) fn shown(&self) -> String {
        format!(
            "{}://{}:{}",
            self.scheme.name(),
            self.host_for_url(),
            self.port
        )
    }

    fn host_for_url(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        }
    }

    /// The proxy's URI, without credentials, as hyper-util's connectors
    /// take it.
    fn uri(&self) -> Result<http::Uri, String> {
        let scheme = match self.scheme {
            Scheme::Http | Scheme::Https => self.scheme.name(),
            // The connector only dials it.
            Scheme::Socks5 | Scheme::Socks5h => "http",
        };
        format!("{scheme}://{}:{}", self.host_for_url(), self.port)
            .parse()
            .map_err(|_| "a proxy URL that does not parse".to_owned())
    }

    /// The proxy's URL with its credentials, as reqwest takes it (D4).
    pub(super) fn url(&self) -> Option<Url> {
        let mut url = Url::parse(&self.shown()).ok()?;
        if let Some((user, password)) = &self.credentials {
            url.set_username(user).ok()?;
            url.set_password(Some(password)).ok()?;
        }
        Some(url)
    }

    fn basic(&self) -> Option<HeaderValue> {
        let (user, password) = self.credentials.as_ref()?;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        let mut value = HeaderValue::from_str(&format!("Basic {encoded}")).ok()?;
        value.set_sensitive(true);
        Some(value)
    }
}

#[allow(clippy::missing_fields_in_debug)] // The credentials, on purpose.
impl fmt::Debug for ProxyTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyTarget")
            .field("proxy", &self.shown())
            .field("credentials", &self.credentials.is_some())
            .finish()
    }
}

/// A percent-encoded username or password, decoded.
fn decoded(part: &str) -> String {
    percent_encoding::percent_decode_str(part)
        .decode_utf8_lossy()
        .into_owned()
}

/// The user and password of a `Basic` header value.
fn from_basic(value: &HeaderValue) -> Result<(String, String), String> {
    let invalid = || "proxy credentials that do not decode".to_owned();
    let encoded = value
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("Basic "))
        .ok_or_else(invalid)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| invalid())?;
    let decoded = String::from_utf8(decoded).map_err(|_| invalid())?;
    let (user, password) = decoded.split_once(':').ok_or_else(invalid)?;
    Ok((user.to_owned(), password.to_owned()))
}

fn proxy_error(message: &str) -> ClientError {
    ClientError {
        kind: ErrorKind::Configuration,
        code: codes::error::BAD_CONFIGURATION,
        message: message.to_owned(),
        transport: None,
    }
}

/// A connected stream to the server's host: TCP, directly or through an
/// HTTP or SOCKS5 proxy, or TLS to an `https://` proxy carrying the tunnel.
pub(super) enum Stream {
    Tcp(TcpStream),
    // Boxed: the TLS state is large, and one stream is held per connection.
    ThroughTls(Box<TokioIo<MaybeHttpsStream<TokioIo<TcpStream>>>>),
}

impl Stream {
    /// The TCP connection underneath, for a reset on abort.
    pub(super) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Tcp(tcp) => tcp,
            Self::ThroughTls(stream) => match stream.inner() {
                MaybeHttpsStream::Http(tcp) => tcp.inner(),
                MaybeHttpsStream::Https(tls) => tls.inner().get_ref().0.inner().inner(),
            },
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Self::ThroughTls(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Self::ThroughTls(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            Self::ThroughTls(stream) => Pin::new(&mut **stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
            Self::ThroughTls(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
        }
    }
}

/// The stream to `url`'s host and port, through `proxy` or directly
/// (Block 23, D3). TLS to Centrifugo, for `wss://`, comes after.
pub(super) async fn dial(
    url: &Url,
    proxy: Option<&ProxyTarget>,
    tls: &Tls,
) -> Result<Stream, String> {
    let endpoint = endpoint(url)?;
    let Some(proxy) = proxy else {
        let stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(|e| e.to_string())?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        return Ok(Stream::Tcp(stream));
    };
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    // The destination always carries its port: both connectors default
    // to 443.
    let destination: http::Uri = format!("http://{}:{}", endpoint.host, endpoint.port)
        .parse()
        .map_err(|_| "an endpoint that does not parse".to_owned())?;
    match proxy.scheme {
        Scheme::Http => {
            let mut tunnel = Tunnel::new(proxy.uri()?, connector);
            if let Some(basic) = proxy.basic() {
                tunnel = tunnel.with_auth(basic);
            }
            let stream = call(&mut tunnel, destination).await.map_err(tunnel_error)?;
            Ok(Stream::Tcp(stream.into_inner()))
        }
        Scheme::Https => {
            connector.enforce_http(false);
            // TLS to the proxy with the client's settings, without ALPN, as
            // reqwest does.
            let mut config = (*tls.config()?).clone();
            config.alpn_protocols.clear();
            let https = hyper_rustls::HttpsConnector::from((connector, Arc::new(config)));
            let mut tunnel = Tunnel::new(proxy.uri()?, https);
            if let Some(basic) = proxy.basic() {
                tunnel = tunnel.with_auth(basic);
            }
            let stream = call(&mut tunnel, destination).await.map_err(tunnel_error)?;
            Ok(Stream::ThroughTls(Box::new(TokioIo::new(stream))))
        }
        Scheme::Socks5 | Scheme::Socks5h => {
            // socks5: the client resolves, without blocking (the
            // connector's own local DNS blocks); socks5h: the proxy does.
            let destination = if proxy.scheme == Scheme::Socks5 {
                let address = tokio::net::lookup_host((endpoint.host.as_str(), endpoint.port))
                    .await
                    .map_err(|e| e.to_string())?
                    .next()
                    .ok_or_else(|| format!("no address for {}", endpoint.host))?;
                format!("http://{address}")
                    .parse()
                    .map_err(|_| "an address that does not parse".to_owned())?
            } else {
                destination
            };
            let mut socks = SocksV5::new(proxy.uri()?, connector).local_dns(false);
            if let Some((user, password)) = &proxy.credentials {
                socks = socks.with_auth(user.clone(), password.clone());
            }
            let stream = call(&mut socks, destination)
                .await
                .map_err(|e| format!("SOCKS5 proxy: {e}"))?;
            Ok(Stream::Tcp(stream.into_inner()))
        }
    }
}

/// Calls a connector once it is ready.
async fn call<S>(service: &mut S, destination: http::Uri) -> Result<S::Response, S::Error>
where
    S: Service<http::Uri>,
{
    std::future::poll_fn(|cx| service.poll_ready(cx)).await?;
    service.call(destination).await
}

/// The message of a failed CONNECT. hyper-util does not export its error
/// type, so a 407 is told by its message (hyper-util 0.1.21 `tunnel.rs:241`;
/// a unit test pins it).
fn tunnel_error(error: impl fmt::Display) -> String {
    let message = error.to_string();
    if message == "tunnel error: proxy authorization required" {
        PROXY_AUTHENTICATION_REQUIRED.to_owned()
    } else {
        message
    }
}

/// The message of a failed attempt the proxy refused credentials for
/// (Block 23, D5).
pub(super) const PROXY_AUTHENTICATION_REQUIRED: &str = "proxy authentication required";

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn url(text: &str) -> Url {
        Url::parse(text).expect("a URL")
    }

    /// Credentials holding reserved characters, from a proxy URL in the
    /// environment's syntax, reach both executors unchanged (Block 23,
    /// structural acceptance): the websocket's handshake and reqwest's URL.
    /// An HTTP proxy's user has no `:`, which Basic cannot carry (RFC 7617,
    /// section 2); a SOCKS5 one may.
    #[test]
    fn credentials_with_reserved_characters_survive_the_decision() {
        for (proxy, user) in [
            ("http://us%40er:p%40ss%2Fw%3Ard@proxy.example:3128", "us@er"),
            (
                "socks5h://us%3Aer:p%40ss%2Fw%3Ard@proxy.example:1080",
                "us:er",
            ),
        ] {
            let matcher = Matcher::builder().all(proxy).build();
            let intercept = matcher
                .intercept(&"https://centrifugo.example/".parse().expect("a URI"))
                .expect("intercepted");

            let target = ProxyTarget::from_intercept(&intercept).expect("a target");
            let reqwest_url = target.url().expect("a URL");

            assert_eq!(
                target.credentials,
                Some((user.to_owned(), "p@ss/w:rd".to_owned())),
                "{proxy}"
            );
            assert_eq!(decoded(reqwest_url.username()), user);
            assert_eq!(decoded(reqwest_url.password().unwrap_or("")), "p@ss/w:rd");
        }
    }

    #[test]
    fn websocket_endpoints_are_asked_as_http_and_https() {
        let proxy = Proxy(Setting::Custom(
            Arc::new(|endpoint| {
                (endpoint == "wss://centrifugo.example:443")
                    .then(|| ProxyTarget::new("http://proxy.example:3128").expect("a proxy"))
            }),
            None,
        ));
        let matcher = Proxy(Setting::All(
            ProxyTarget::new("http://proxy.example:3128").expect("a proxy"),
            None,
        ))
        .no_proxy("centrifugo.example");

        let chosen = proxy.decide(&url("wss://centrifugo.example/connection/websocket"));
        let excluded = matcher.decide(&url("ws://centrifugo.example:8000/connection/websocket"));

        assert_eq!(
            chosen.map(|target| target.map(|t| t.shown())),
            Ok(Some("http://proxy.example:3128".to_owned()))
        );
        assert_eq!(excluded, Ok(None));
    }

    /// Reading the operating system's settings works on every platform
    /// the portability jobs run (Block 23): none are set there.
    #[test]
    fn the_systems_settings_are_read() {
        let decided = Proxy::system().decide(&url("wss://centrifugo.example/connection/websocket"));

        println!("system proxy: {decided:?}");
        assert!(decided.is_ok(), "{decided:?}");
    }

    #[test]
    fn neither_debug_nor_errors_show_credentials() {
        let target = ProxyTarget::new("http://user:pw-9f1c@proxy.example:3128")
            .expect("a proxy")
            .basic_auth("user", "pw-9f1c");
        let refused = ProxyTarget::new("ftp://user:pw-9f1c@proxy.example:21");

        let shown = format!("{:?} {:?} {refused:?}", target, Proxy::all(target.clone()));

        assert!(!shown.contains("pw-9f1c"), "{shown}");
        assert!(shown.contains("http://proxy.example:3128"), "{shown}");
    }

    /// A CONNECT answered 407 says so: pins hyper-util's message the
    /// mapping relies on (`tunnel.rs:241`).
    #[tokio::test]
    async fn a_connect_answered_407_asks_for_proxy_authentication() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.expect("accept");
            let mut buffer = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut tcp, &mut buffer).await;
            let _ = tcp
                .write_all(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
        });
        let target = ProxyTarget::new(&format!("http://{address}")).expect("a proxy");

        let result = dial(
            &url("ws://centrifugo.invalid:8000/connection/websocket"),
            Some(&target),
            &Tls::default(),
        )
        .await;

        assert_eq!(result.err().as_deref(), Some(PROXY_AUTHENTICATION_REQUIRED));
    }
}
