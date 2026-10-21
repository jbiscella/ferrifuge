//! Connection tokens (HS256 JWT) accepted by the test server.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha2::Sha256;

/// HMAC secret the test server verifies connection tokens with.
pub(crate) const HMAC_SECRET: &str = "test-hmac-secret";

/// A token for `user` that expires `lifetime` from now.
pub(crate) fn connection_token(user: &str, lifetime: Duration) -> String {
    let expires = SystemTime::now()
        .checked_add(lifetime)
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .expect("expiry after the epoch")
        .as_secs();
    let header = URL_SAFE_NO_PAD.encode(json!({"alg": "HS256", "typ": "JWT"}).to_string());
    let claims = URL_SAFE_NO_PAD.encode(json!({"sub": user, "exp": expires}).to_string());
    let signed = format!("{header}.{claims}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(HMAC_SECRET.as_bytes()).expect("any key size is valid");
    mac.update(signed.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signed}.{signature}")
}
