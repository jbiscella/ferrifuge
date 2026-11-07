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
const SERVER_NAMES: [&str; 2] = ["localhost", "127.0.0.1"];

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("test-ca")
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
/// of an earlier run.
pub(crate) fn create() {
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
    fs::write(ca_path(), ca_cert.pem()).expect("write CA certificate");
    fs::write(server_cert_path(), server_cert.pem()).expect("write server certificate");
    fs::write(server_key_path(), server_key.serialize_pem()).expect("write server key");
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
