//! The options only the native client has: their setters, their checks
//! and what the client keeps of them, and the native client's own methods.
// Block 61, D2.

use std::sync::{Arc, Weak};

use rustls::pki_types::CertificateDer;

#[cfg(feature = "cookies")]
use super::super::CookieJar;
use super::super::builder::{self, ClientBuilder, HeaderNames, http_header_map};
use super::super::client::Client;
use super::super::platform::Handle;
use super::super::shared::Shared;
use super::connector::{self, Connector};
use super::http::HeaderSet;
use super::net::{Jar, Network};
use super::proxy::Proxy;
use super::tls::{self, Tls};
use crate::sans_io::TransportKind;
use crate::{ClientError, Config};

/// The builder's native options, as given; checked by
/// [`ClientBuilder::build`].
#[derive(Default)]
pub(in crate::adapter) struct Options {
    /// Real HTTP headers, as given; checked by [`ClientBuilder::build`].
    http_headers: Vec<(String, String)>,
    /// Extra root certificates, as given; checked by [`ClientBuilder::build`].
    root_certificates: Vec<CertificateDer<'static>>,
    /// The application's TLS configuration, if any.
    tls_config: Option<Arc<rustls::ClientConfig>>,
    proxy: Proxy,
    /// Whether [`ClientBuilder::proxy`] was called: a connector replaces
    /// any proxy setting.
    // Block 37, D4.
    proxy_set: bool,
    /// The application's connector for the websocket, if any.
    connector: Option<connector::Shared>,
    /// The runtime the client runs on; the current one otherwise.
    runtime: Option<Handle>,
    /// The application's cookie jar.
    // Block 43.
    cookie_jar: Jar,
}

/// What the native client's [`Shared`] keeps of the options.
#[derive(Debug)]
pub(in crate::adapter) struct SharedPart {
    /// Its TLS and proxy settings and HTTP clients.
    // Blocks 22, 23.
    pub(in crate::adapter) network: Arc<Network>,
    /// The real HTTP headers of every transport request, which
    /// `set_http_headers` replaces.
    // Blocks 17, 32.
    pub(in crate::adapter) http_headers: HeaderSet,
}

/// What [`SharedPart`] is made from.
pub(in crate::adapter) struct SharedSeed(SharedPart);

impl SharedSeed {
    /// The part of `shared`, which needs nothing of it natively.
    pub(in crate::adapter) fn into_part(self, _shared: &Weak<Shared>) -> SharedPart {
        self.0
    }
}

/// What the native driver keeps of the options: the same network and HTTP
/// headers as [`SharedPart`].
pub(in crate::adapter) struct DriverPart {
    pub(in crate::adapter) network: Arc<Network>,
    pub(in crate::adapter) http_headers: HeaderSet,
}

/// The options checked, for the client being started.
pub(in crate::adapter) struct Started {
    pub(in crate::adapter) shared: SharedSeed,
    pub(in crate::adapter) driver: DriverPart,
    /// The runtime the client runs on.
    pub(in crate::adapter) runtime: Handle,
}

impl Options {
    /// Writes the options into the builder's `Debug`.
    pub(in crate::adapter) fn fields(&self, f: &mut std::fmt::DebugStruct<'_, '_>) {
        f.field("http_headers", &HeaderNames(&self.http_headers))
            // Their number, and whether one is set: never their content.
            .field("root_certificates", &self.root_certificates.len())
            .field("tls_config", &self.tls_config.is_some())
            .field("proxy", &self.proxy)
            .field("proxy_set", &self.proxy_set)
            .field("connector", &self.connector.is_some())
            .field("runtime", &self.runtime.is_some());
        // Whether one is set: never its cookies (Block 43, D6).
        f.field("cookie_jar", &self.cookie_jar);
    }

    /// Runs the native checks, for `endpoints` and `emulation_endpoint`:
    /// the endpoints and the connector, the trust, WebTransport's, the
    /// Android gate, the HTTP headers and the runtime.
    pub(in crate::adapter) fn start(
        self,
        endpoints: &[(TransportKind, String)],
        emulation_endpoint: Option<&str>,
        _config: &Config,
    ) -> Result<Started, ClientError> {
        let Self {
            http_headers,
            root_certificates,
            tls_config,
            proxy,
            proxy_set,
            connector,
            runtime,
            cookie_jar,
        } = self;
        builder::check_endpoints(endpoints, connector.is_some(), proxy_set)?;
        let android = cfg!(target_os = "android");
        let trust = builder::trust(root_certificates, tls_config, android)?;
        #[cfg(feature = "unstable")]
        builder::check_webtransport(endpoints, &trust)?;
        builder::android_gate(
            android && !tls::android_ready(),
            &trust,
            endpoints,
            emulation_endpoint,
            &proxy,
        )?;
        let http_headers = HeaderSet::new(http_header_map(&http_headers)?);
        let runtime = builder::runtime_or_current(runtime)?;
        let network = Arc::new(
            Network::new(Tls::new(trust), proxy)
                .with_connector(connector)
                .with_cookie_jar(cookie_jar),
        );
        Ok(Started {
            driver: DriverPart {
                network: Arc::clone(&network),
                http_headers: http_headers.clone(),
            },
            shared: SharedSeed(SharedPart {
                network,
                http_headers,
            }),
            runtime,
        })
    }
}

impl ClientBuilder {
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Trusts `roots`, certificate authorities, in addition to the
    /// platform's, for `wss://` and every `https://` request; they add to
    /// the roots of earlier calls. A certificate that is not a valid root
    /// makes [`ClientBuilder::build`] fail, naming its index.
    ///
    /// PEM files parse with [`rustls::pki_types::pem::PemObject`]:
    ///
    /// ```no_run
    /// use ferrifuge::Client;
    /// use ferrifuge::rustls::pki_types::CertificateDer;
    /// use ferrifuge::rustls::pki_types::pem::PemObject;
    ///
    /// # fn run(pem: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    /// let roots = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
    /// let client = Client::builder("wss://example.com/connection/websocket")
    ///     .root_certificates(roots)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Each client given extra roots holds its own TLS settings: many
    /// clients should rather share one [`ClientBuilder::tls_config`].
    // Block 22.
    #[must_use]
    pub fn root_certificates(
        mut self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.backend.root_certificates.extend(roots);
        self
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Uses `config` for `wss://` and every `https://` request, instead of
    /// the platform's trust: its roots, verifier, provider and client
    /// certificates are the application's. Ferrifuge only replaces its ALPN
    /// list with `http/1.1`, the one protocol of its transports.
    /// [`ClientBuilder::build`] fails when extra root certificates are also
    /// given, and, in the `fips` build, when `config.fips()` is false.
    ///
    /// The configuration is shared as given, so many clients can use one.
    // Block 22.
    #[must_use]
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.backend.tls_config = Some(config);
        self
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Sets how the client reaches the server's hosts: through the system's
    /// proxy by default ([`Proxy::system`]), the environment's, none, one
    /// proxy, or one chosen per endpoint. Every transport takes the same
    /// decision for each URL it opens.
    // Block 23.
    #[must_use]
    pub fn proxy(mut self, proxy: Proxy) -> Self {
        self.backend.proxy = proxy;
        self.backend.proxy_set = true;
        self
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Sets the connector that gives the websocket its stream, for
    /// networking the client does not have: a VPN tunnel, a custom proxy,
    /// an in-process transport. Over its stream the client runs TLS, for
    /// `wss://`, and the websocket upgrade, as over a stream it dials. With
    /// a connector the client reads no proxy setting, the environment's and
    /// the system's included. A connector carries the websocket only:
    /// [`ClientBuilder::build`] fails with an `http_stream` or sse
    /// endpoint, or after [`ClientBuilder::proxy`].
    // Block 37.
    #[must_use]
    pub fn connector(mut self, connector: impl Connector + 'static) -> Self {
        self.backend.connector = Some(connector::Shared(Arc::new(connector)));
        self
    }

    /// Sets the cookie jar that keeps the cookies servers set and sends
    /// them back on the later requests they match: the websocket upgrade, a
    /// refused one's response included. A later call replaces an earlier
    /// one; the application keeps its own `Arc` to look into the jar or
    /// seed it. The cookies merge with a `Cookie` header of
    /// [`ClientBuilder::http_headers`] into one header, the application's
    /// first. No proxy is sent them. Without a jar, no cookie is kept.
    // Block 43.
    #[cfg(feature = "cookies")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cookies")))]
    #[must_use]
    pub fn cookie_jar(mut self, jar: Arc<dyn CookieJar>) -> Self {
        self.backend.cookie_jar = Jar(Some(jar));
        self
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Adds real HTTP headers to the websocket upgrade request and to every
    /// request of `http_stream` and sse, so that a gateway checking one
    /// still admits a client that fell back. Names are case-insensitive, a
    /// later value replacing an earlier one. A name or value that is not
    /// valid HTTP, or a header the transport sets itself (`Host`,
    /// `Connection`, `Upgrade`, `Content-Length`, `Transfer-Encoding`,
    /// `Content-Type`, `Accept`, `Sec-WebSocket-*`, `Proxy-Authorization`),
    /// makes [`ClientBuilder::build`] fail. [`Client::set_http_headers`]
    /// replaces them on a built client.
    // Block 17.
    #[must_use]
    pub fn http_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.backend.http_headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Runs the client on `runtime`, e.g. when building it where no tokio
    /// runtime is current, as in a Tauri setup hook:
    ///
    /// ```no_run
    /// # fn run(handle: tokio::runtime::Handle) -> Result<(), ferrifuge::ClientError> {
    /// // In Tauri: `tauri::async_runtime::handle().inner().clone()`.
    /// let client = ferrifuge::Client::builder("wss://example.com/connection/websocket")
    ///     .runtime(handle)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default the client runs on the runtime current when it is built.
    /// The runtime needs time and I/O enabled (`enable_all`). A
    /// current-thread runtime runs the client only while a thread is inside
    /// its `Runtime::block_on`: meanwhile calls and streams wait, and no
    /// timeout runs. When the runtime shuts down, the client emits
    /// `disconnected` with code 4 "runtime shut down"
    /// ([`crate::codes::disconnected::RUNTIME_SHUT_DOWN`]), its calls fail
    /// and its streams end; the client's futures and streams themselves need
    /// no runtime, so any executor can await them.
    // Block 28.
    #[must_use]
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.backend.runtime = Some(runtime);
        self
    }
}

impl Client {
    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Replaces the real HTTP headers: every HTTP request started from now
    /// on carries the new set, the upgrade or stream request of the next
    /// attempt and each emulation request of an open `http_stream` or sse
    /// transport. The open connection is kept, and a request already
    /// started keeps the headers it started with. A header missing from the
    /// new set is no longer sent; a name given twice keeps its later value.
    ///
    /// ```no_run
    /// # fn run(client: &ferrifuge::Client, token: &str) -> Result<(), ferrifuge::ClientError> {
    /// client.set_http_headers([("Authorization", format!("Bearer {token}"))])?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A [`crate::ErrorKind::Configuration`] error naming a header, never its
    /// value, as [`ClientBuilder::http_headers`] refuses it: a name or value
    /// that is not valid HTTP, or a header the transport sets,
    /// `Proxy-Authorization` included. The headers are then left as they
    /// were.
    // Block 32.
    pub fn set_http_headers<I, K, V>(&self, headers: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let headers: Vec<(String, String)> = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        let headers = http_header_map(&headers)?;
        self.shared.backend.http_headers.replace(headers);
        Ok(())
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(not(all(target_arch = "wasm32", target_os = "unknown"))))
    )]
    /// Replaces the proxy setting from the next connection attempt, as the
    /// other setters do; the current connection is kept. A client given a
    /// connector reads no proxy setting: this changes nothing.
    // Block 23; Block 37.
    pub fn set_proxy(&self, proxy: Proxy) {
        self.shared.backend.network.set_proxy(proxy);
    }
}
