//! TLS settings of the tokio adapter: the build's crypto provider
//! (Block 6) and how servers are verified (Block 22), chosen in one place.

use std::sync::{Arc, OnceLock};

use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};

#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
compile_error!(
    "the tokio adapter needs a TLS crypto provider: enable the `ring` (default), `aws-lc-rs` or `fips` feature"
);

/// The crypto provider chosen by the build's features (Block 6). Features
/// are additive, so the strongest enabled one wins: fips > aws-lc-rs > ring.
#[cfg(feature = "fips")]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::default_fips_provider()
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "fips")))]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
fn crypto_provider() -> CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// Whom a client trusts (Block 22).
#[derive(Clone, Default)]
pub(crate) enum Trust {
    /// The platform's roots, or its verifier.
    #[default]
    Platform,
    /// The platform's, and these certificate authorities.
    ExtraRoots(Arc<[CertificateDer<'static>]>),
    /// The application's configuration, as given but for its ALPN.
    Raw(Arc<ClientConfig>),
}

impl std::fmt::Debug for Trust {
    // Neither certificates nor configurations: their number, or whether one
    // is set (Block 22, D5).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Platform => f.write_str("Platform"),
            Self::ExtraRoots(roots) => write!(f, "ExtraRoots(<{}>)", roots.len()),
            Self::Raw(_) => f.write_str("Raw(..)"),
        }
    }
}

/// The TLS settings of one client: the process-wide ones when it trusts the
/// platform alone, its own otherwise.
#[derive(Debug, Default)]
pub(super) struct Tls {
    trust: Trust,
    /// Its own TLS settings, built at its first secure attempt.
    config: BuildOnce<Arc<ClientConfig>>,
    /// Its own HTTPS client, built from them.
    https: BuildOnce<reqwest::Client>,
}

impl Tls {
    pub(super) fn new(trust: Trust) -> Self {
        Self {
            trust,
            ..Self::default()
        }
    }

    /// The settings of `wss://` connections.
    pub(super) fn config(&self) -> Result<Arc<ClientConfig>, String> {
        self.setup().map(|(config, _)| config)
    }

    /// [`Tls::config`], and whether it is kept: a read of the platform's
    /// root certificates that reported errors serves the attempt but is
    /// read again by the next one (Block 6, review item C7).
    fn setup(&self) -> Result<(Arc<ClientConfig>, bool), String> {
        match &self.trust {
            Trust::Platform => platform_setup(),
            Trust::ExtraRoots(roots) => self
                .config
                .get(|| verified_config(roots).map(|(config, keep)| (Arc::new(config), keep))),
            Trust::Raw(raw) => self.config.get(|| Ok((Arc::new(http1_only(raw)), true))),
        }
    }

    /// The HTTP client of `https://` requests: built once, per process
    /// when the client trusts the platform alone, per client otherwise;
    /// not kept when its TLS settings are not.
    pub(super) fn https_client(&self) -> Result<reqwest::Client, String> {
        static PLATFORM: BuildOnce<reqwest::Client> = BuildOnce::new();
        let client = match self.trust {
            Trust::Platform => &PLATFORM,
            _ => &self.https,
        };
        let (client, _) = client.get(|| {
            let (config, keep) = self.setup()?;
            http_client((*config).clone()).map(|client| (client, keep))
        })?;
        Ok(client)
    }
}

/// The HTTP client of plain `http://` requests, built once per process.
pub(super) fn plain_http_client() -> Result<reqwest::Client, String> {
    static PLAIN: BuildOnce<reqwest::Client> = BuildOnce::new();
    let (client, _) = PLAIN.get(|| http_client(plain_tls_config()).map(|client| (client, true)))?;
    Ok(client)
}

fn http_client(tls: ClientConfig) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        // The TLS setup and the emulation endpoint follow from the
        // configured URL: a redirect elsewhere fails the attempt, like an
        // unreachable endpoint, rather than splitting the stream and the
        // commands between two servers.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())
}

/// TLS settings of an HTTP client that only ever speaks plain `http://`:
/// no root certificates, so it works on hosts without platform ones, and
/// still the build's provider, as the HTTP client takes no default one.
fn plain_tls_config() -> ClientConfig {
    ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("the provider supports the default protocol versions")
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth()
}

/// A copy of the application's configuration offering HTTP/1.1 only, the
/// one protocol of the transports: one offering `h2` would fail them
/// (Block 22, D3).
fn http1_only(raw: &ClientConfig) -> ClientConfig {
    let mut config = raw.clone();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

/// The process-wide settings of the clients trusting the platform alone.
///
/// The crypto provider is set explicitly rather than taken from the process
/// default: when another crate also enables another rustls provider, rustls
/// cannot pick a default and panics.
fn platform_setup() -> Result<(Arc<ClientConfig>, bool), String> {
    static CONFIG: BuildOnce<Arc<ClientConfig>> = BuildOnce::new();
    CONFIG.get(|| verified_config(&[]).map(|(config, keep)| (Arc::new(config), keep)))
}

/// How servers are verified in this build and on this platform (Block 22,
/// D1), for tests.
#[cfg(test)]
pub(super) const VERIFIER: &str = if cfg!(all(
    not(feature = "fips"),
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows"
    )
)) {
    "platform"
} else {
    "webpki"
};

/// TLS settings verifying servers as this build does on this platform, also
/// trusting `extra`, and whether to keep them (Block 22, D1).
///
/// The operating system's verifier on macOS, iOS and Windows, but in the
/// fips build, whose chains must be checked inside the FIPS module.
#[cfg(all(
    not(feature = "fips"),
    any(target_os = "macos", target_os = "ios", target_os = "windows")
))]
fn verified_config(extra: &[CertificateDer<'static>]) -> Result<(ClientConfig, bool), String> {
    let provider = Arc::new(crypto_provider());
    let verifier = rustls_platform_verifier::Verifier::new_with_extra_roots(
        extra.iter().cloned(),
        Arc::clone(&provider),
    )
    .map_err(|e| e.to_string())?;
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    // The operating system reads its store at each verification.
    Ok((config, true))
}

/// TLS settings verifying servers as this build does on this platform, also
/// trusting `extra`, and whether to keep them (Block 22, D1).
///
/// Webpki over the platform's root certificates: on Linux this is what the
/// platform verifier does, but the read's errors must be seen to keep C7;
/// in the fips build, on every platform.
#[cfg(not(all(
    not(feature = "fips"),
    any(target_os = "macos", target_os = "ios", target_os = "windows")
)))]
fn verified_config(extra: &[CertificateDer<'static>]) -> Result<(ClientConfig, bool), String> {
    let found = rustls_native_certs::load_native_certs();
    roots_config(found.certs, extra, found.errors.is_empty())
}

/// The TLS settings trusting `platform` and `extra`, kept when `complete`:
/// read without errors. Unreadable platform certificates are skipped, as
/// browsers do; the extra ones were checked by `ClientBuilder::build`.
#[cfg_attr(
    all(
        not(feature = "fips"),
        any(target_os = "macos", target_os = "ios", target_os = "windows")
    ),
    allow(dead_code)
)]
fn roots_config(
    platform: Vec<CertificateDer<'static>>,
    extra: &[CertificateDer<'static>],
    complete: bool,
) -> Result<(ClientConfig, bool), String> {
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(platform);
    let (extra_added, _) = roots.add_parsable_certificates(extra.iter().cloned());
    if added + extra_added == 0 {
        return Err("no platform root certificates found".to_owned());
    }
    let config = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((config, complete))
}

/// The index of the first certificate of `roots` webpki cannot take as a
/// root (Block 22, D2).
pub(super) fn unparsable_root(roots: &[CertificateDer<'static>]) -> Option<usize> {
    roots
        .iter()
        .position(|root| RootCertStore::empty().add(root.clone()).is_err())
}

/// A value built at first use and shared; a build that fails, or that its
/// builder says not to keep, is not kept, so the next use builds again
/// (Block 6, review item C7).
#[derive(Debug)]
pub(super) struct BuildOnce<T>(OnceLock<T>);

impl<T> Default for BuildOnce<T> {
    fn default() -> Self {
        Self(OnceLock::new())
    }
}

impl<T: Clone> BuildOnce<T> {
    pub(super) const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// The value, and whether it is kept: built by `build`, which also says
    /// whether to keep it, unless an earlier build was kept. Two first uses
    /// racing may both build; one value is kept.
    pub(super) fn get(
        &self,
        build: impl FnOnce() -> Result<(T, bool), String>,
    ) -> Result<(T, bool), String> {
        if let Some(value) = self.0.get() {
            return Ok((value.clone(), true));
        }
        let (value, keep) = build()?;
        if !keep {
            return Ok((value, false));
        }
        Ok((self.0.get_or_init(|| value).clone(), true))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

    use super::*;

    /// A CA and a `localhost` server certificate it signed, valid for a
    /// short while around now, with the extensions Apple's and Windows'
    /// verifiers want of a server certificate: the CA, the server's chain
    /// and its key.
    fn private_chain() -> (
        CertificateDer<'static>,
        CertificateDer<'static>,
        PrivateKeyDer<'static>,
    ) {
        let now = time::OffsetDateTime::now_utc();
        let ca_key = KeyPair::generate().expect("CA key");
        let mut ca = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca.distinguished_name
            .push(DnType::CommonName, "ferrifuge private test CA");
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca.not_before = now - time::Duration::days(1);
        ca.not_after = now + time::Duration::days(30);
        let ca_cert = ca.self_signed(&ca_key).expect("CA certificate");
        let issuer = Issuer::new(ca, ca_key);
        let server_key = KeyPair::generate().expect("server key");
        let mut server =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("server parameters");
        server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        server.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server.not_before = now - time::Duration::days(1);
        server.not_after = now + time::Duration::days(30);
        let server_cert = server
            .signed_by(&server_key, &issuer)
            .expect("server certificate");
        (
            ca_cert.der().clone(),
            server_cert.der().clone(),
            PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        )
    }

    /// Runs a TLS handshake in memory between `client` and a server
    /// presenting `chain`: the client's error, if it refuses the server.
    fn handshake(
        client: Arc<ClientConfig>,
        chain: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    ) -> Result<(), rustls::Error> {
        let server = ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![chain], key)
            .expect("server configuration");
        let name = ServerName::try_from("localhost").expect("a name");
        let mut client = ClientConnection::new(client, name).expect("client connection");
        let mut server = ServerConnection::new(Arc::new(server)).expect("server connection");
        while client.is_handshaking() || server.is_handshaking() {
            let mut to_server = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut to_server).expect("client write");
            }
            server
                .read_tls(&mut to_server.as_slice())
                .expect("server read");
            // The client's alert ends the server too: the client's error
            // is the one asked for.
            let server_side = server.process_new_packets();
            let mut to_client = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut to_client).expect("server write");
            }
            client
                .read_tls(&mut to_client.as_slice())
                .expect("client read");
            client.process_new_packets()?;
            server_side.expect("the server side of the handshake");
            assert!(
                !to_server.is_empty() || !to_client.is_empty(),
                "the handshake stalled"
            );
        }
        Ok(())
    }

    /// The verifier this build uses on this platform (Block 22, D1).
    #[test]
    fn the_build_verifies_with_its_platforms_verifier() {
        let expected = if cfg!(feature = "fips") {
            "webpki"
        } else if cfg!(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "windows"
        )) {
            "platform"
        } else {
            "webpki"
        };
        println!("verifier: {VERIFIER}");
        assert_eq!(VERIFIER, expected);
    }

    /// The chosen verifier trusts an extra root and refuses a server whose
    /// issuer it does not know, on every platform (Block 22, D1 and D2).
    #[test]
    fn an_extra_root_is_trusted_and_an_unknown_issuer_refused() {
        let (ca, chain, key) = private_chain();
        let trusting = Tls::new(Trust::ExtraRoots(vec![ca].into()))
            .config()
            .expect("TLS settings");
        let platform = Tls::default().config().expect("TLS settings");

        let trusted = handshake(trusting, chain.clone(), key.clone_key());
        let refused = handshake(platform, chain, key);

        assert_eq!(trusted, Ok(()));
        assert!(
            matches!(&refused, Err(rustls::Error::InvalidCertificate(_))),
            "{refused:?}"
        );
    }

    /// A raw configuration is used as given, but its copies offer
    /// HTTP/1.1 only (Block 22, D3).
    #[test]
    fn a_raw_configuration_offers_http1_only() {
        let (ca, chain, key) = private_chain();
        let mut roots = RootCertStore::empty();
        roots.add(ca).expect("a root");
        let mut raw = ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        raw.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls = Tls::new(Trust::Raw(Arc::new(raw)));

        let config = tls.config().expect("TLS settings");

        assert_eq!(config.alpn_protocols, [b"http/1.1".to_vec()]);
        assert_eq!(handshake(config, chain, key), Ok(()));
        assert!(tls.https_client().is_ok());
    }

    // Review item C7: a transient failure (a CA bundle being replaced, no
    // file descriptor left) must not break every later secure connection.
    #[test]
    fn a_failed_tls_setup_is_retried() {
        let setup = BuildOnce::new();

        let failed = setup.get(|| Err("no platform root certificates found".to_owned()));
        let retried = setup.get(|| Ok((1, true)));

        assert!(failed.is_err());
        assert_eq!(retried, Ok((1, true)));
    }

    #[test]
    fn a_tls_setup_its_builder_does_not_keep_is_built_again() {
        let setup = BuildOnce::new();

        let first = setup.get(|| Ok((1, false)));
        let second = setup.get(|| Ok((2, true)));
        let third = setup.get(|| Ok((3, true)));

        assert_eq!(
            (first, second, third),
            (Ok((1, false)), Ok((2, true)), Ok((2, true)))
        );
    }

    #[test]
    fn root_certificates_read_with_errors_serve_the_attempt_but_are_not_kept() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .unwrap()
            .cert
            .der()
            .clone();

        let partial = roots_config(vec![certificate.clone()], &[], false);
        let complete = roots_config(vec![certificate], &[], true);

        assert!(matches!(partial, Ok((_, false))));
        assert!(matches!(complete, Ok((_, true))));
    }

    #[test]
    fn a_built_tls_setup_is_reused() {
        let setup = BuildOnce::new();
        let builds = std::cell::Cell::new(0);
        let build = || {
            builds.set(builds.get() + 1);
            Ok((builds.get(), true))
        };

        let first = setup.get(build);
        let second = setup.get(build);

        assert_eq!(
            (first, second, builds.get()),
            (Ok((1, true)), Ok((1, true)), 1)
        );
    }

    #[test]
    fn tls_config_builds_even_with_several_rustls_providers_enabled() {
        // Test builds enable aws-lc-rs too (through reqwest), which made the
        // process-default provider ambiguous and panic.
        let config = Tls::default().config();

        assert!(config.is_ok(), "{config:?}");
    }

    /// The build's strongest provider feature selects the provider
    /// (Block 6): fips > aws-lc-rs > ring.
    #[test]
    fn tls_config_uses_the_build_provider() {
        let config = Tls::default().config().expect("TLS configuration");

        let key_provider = format!("{:?}", config.crypto_provider().key_provider);

        let expected = if cfg!(feature = "aws-lc-rs") {
            "AwsLcRs"
        } else {
            "Ring"
        };
        assert_eq!(key_provider, expected);
        // rustls also checks TLS-level FIPS requirements here, beyond the
        // provider's primitives.
        assert_eq!(config.fips(), cfg!(feature = "fips"));
    }

    /// A dependency update must not switch the FIPS build to another
    /// module generation unnoticed (Block 6: AWS-LC-FIPS 4.x).
    #[cfg(feature = "fips")]
    #[test]
    fn fips_build_links_the_4x_fips_module() {
        // The FIPS module generation, not the AWS-LC release
        // (`awslc_version`), which aws-lc-rs documents as unfit for it.
        let generation = aws_lc_rs::fips_version();

        assert_eq!(generation, Some(4), "FIPS module generation");
        assert!(aws_lc_rs::try_fips_mode().is_ok(), "not in FIPS mode");
    }
}
