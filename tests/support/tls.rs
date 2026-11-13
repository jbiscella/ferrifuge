//! Test certificate authority for the encrypted scenarios (Block 6).
//!
//! On Linux the client verifies with webpki over the platform's root
//! certificates, which `rustls-native-certs` reads from the file named by
//! `SSL_CERT_FILE` when it is set, so the runner points it at this CA (see
//! `trust_test_ca` in `bdd.rs`). A second, private CA, never in that file,
//! tests the extra roots and raw configurations of Block 22.

use std::fs;
use std::path::PathBuf;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};

/// Names the server certificate is valid for: the host testcontainers
/// reports for a local Docker daemon, by name and by address.
/// `centrifugo.invalid`, which never resolves (RFC 6761), is an endpoint only
/// a test proxy can reach (Block 23).
const SERVER_NAMES: [&str; 3] = ["localhost", "127.0.0.1", "centrifugo.invalid"];

/// One directory per test binary, so that two binaries run at once (a
/// coverage run beside another) never share the files; a child process
/// of the same binary, as the @P harness's fake server, finds them too.
fn dir() -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary");
    let binary = exe
        .file_stem()
        .expect("a file name")
        .to_string_lossy()
        .into_owned();
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("test-ca")
        .join(binary)
}

/// The CA certificate, in PEM.
pub(crate) fn ca_path() -> PathBuf {
    dir().join("ca.pem")
}

fn server_cert_path() -> PathBuf {
    dir().join("server.pem")
}

fn server_key_path() -> PathBuf {
    dir().join("server-key.pem")
}

/// Creates a new CA and a server certificate signed by it, replacing those
/// of an earlier run, once per process: tests of one binary running at
/// once share them (CI health, finding 2: a second `create` replaced the
/// CA a running test had just read its certificate for).
pub(crate) fn create() {
    static CREATED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    CREATED.get_or_init(create_once);
}

fn create_once() {
    let ca_key = KeyPair::generate().expect("CA key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "ferrifuge test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("server key");
    let server_cert = CertificateParams::new(SERVER_NAMES.map(str::to_owned).to_vec())
        .expect("server certificate parameters")
        .signed_by(&server_key, &issuer)
        .expect("server certificate");

    fs::create_dir_all(dir()).expect("test CA directory");
    write_atomically(&ca_path(), &ca_cert.pem());
    write_atomically(&server_cert_path(), &server_cert.pem());
    write_atomically(&server_key_path(), &server_key.serialize_pem());
}

/// Writes `contents` to a file beside `path`, then renames it there: a
/// reader never sees part of it.
fn write_atomically(path: &std::path::Path, contents: &str) {
    let partial = path.with_extension(format!("partial-{}", std::process::id()));
    fs::write(&partial, contents).expect("write a test CA file");
    fs::rename(&partial, path).expect("move a test CA file into place");
}

/// The server certificate and its private key, in PEM.
pub(crate) fn server_pem() -> (String, String) {
    (
        fs::read_to_string(server_cert_path()).expect("server certificate"),
        fs::read_to_string(server_key_path()).expect("server key"),
    )
}

/// A second CA, never in `SSL_CERT_FILE`, so the platform does not trust it
/// (Block 22): its certificate and a server certificate it signed, with the
/// server's key, in DER and PEM. Created once per process.
pub(crate) struct Private {
    pub(crate) ca: rustls::pki_types::CertificateDer<'static>,
    pub(crate) server_pem: String,
    pub(crate) key_pem: String,
}

pub(crate) fn private() -> &'static Private {
    static PRIVATE: std::sync::OnceLock<Private> = std::sync::OnceLock::new();
    PRIVATE.get_or_init(|| {
        let now = time::OffsetDateTime::now_utc();
        let ca_key = KeyPair::generate().expect("CA key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "ferrifuge private test CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca_params.not_before = now - time::Duration::days(1);
        ca_params.not_after = now + time::Duration::days(30);
        let ca_cert = ca_params.self_signed(&ca_key).expect("CA certificate");
        let issuer = Issuer::new(ca_params, ca_key);
        let server_key = KeyPair::generate().expect("server key");
        let mut server = CertificateParams::new(SERVER_NAMES.map(str::to_owned).to_vec())
            .expect("server certificate parameters");
        server.not_before = now - time::Duration::days(1);
        server.not_after = now + time::Duration::days(30);
        let server_cert = server
            .signed_by(&server_key, &issuer)
            .expect("server certificate");
        Private {
            ca: ca_cert.der().clone(),
            server_pem: server_cert.pem(),
            key_pem: server_key.serialize_pem(),
        }
    })
}

/// The test CA's certificate, in DER, for a client trusting it explicitly.
pub(crate) fn ca_der() -> rustls::pki_types::CertificateDer<'static> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::from_pem_file(ca_path()).expect("the test CA")
}

/// The server certificate and key of the test CA, for a TLS listener of
/// the tests.
pub(crate) fn server_config() -> std::sync::Arc<rustls::ServerConfig> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let chain =
        vec![CertificateDer::from_pem_file(server_cert_path()).expect("server certificate")];
    let key = PrivateKeyDer::from_pem_file(server_key_path()).expect("server key");
    std::sync::Arc::new(
        rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server configuration"),
    )
}

/// The build's crypto provider, as the library chooses it (Block 6).
#[cfg(feature = "fips")]
pub(crate) fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::default_fips_provider()
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "fips")))]
pub(crate) fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

#[cfg(not(feature = "aws-lc-rs"))]
pub(crate) fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}
