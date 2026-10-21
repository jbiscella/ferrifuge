//! Test certificate authority for the encrypted scenarios (Block 6).
//!
//! The client trusts the platform's root certificates only. `rustls-native-certs`
//! reads them from the file named by `SSL_CERT_FILE` when it is set, so the
//! runner points it at this CA (see `trust_test_ca` in `bdd.rs`): no
//! custom-root API is needed.

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
