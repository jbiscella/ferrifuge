//! Connection and subscription tokens (HS256 JWT) accepted by the test
//! server.

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
    signed(&json!({"sub": user, "exp": expires_in(lifetime)}))
}

/// A subscription token for `user` and `channel` that expires `lifetime`
/// from now (Centrifugo verifies it with the connection token key).
pub(crate) fn subscription_token(user: &str, channel: &str, lifetime: Duration) -> String {
    signed(&json!({"sub": user, "channel": channel, "exp": expires_in(lifetime)}))
}

/// A token for `user` carrying `info`, the connection info the server
/// attaches to the user's publications and presence (claim `info`).
pub(crate) fn connection_token_with_info(user: &str, info: &serde_json::Value) -> String {
    signed(&json!({"sub": user, "info": info, "exp": expires_in(Duration::from_secs(3600))}))
}

/// A subscription token for `user` and `channel` carrying `info`, the
/// channel info the server attaches (claim `info`).
pub(crate) fn subscription_token_with_info(
    user: &str,
    channel: &str,
    info: &serde_json::Value,
) -> String {
    signed(&json!({
        "sub": user,
        "channel": channel,
        "info": info,
        "exp": expires_in(Duration::from_secs(3600)),
    }))
}

/// HMAC secret the test server verifies shared poll signatures with
/// (Block 47, D18).
pub(crate) const SHARED_POLL_SECRET: &str = "test-shared-poll-secret";

/// A shared poll signature for `user` (empty for anonymous) over `keys` of
/// `channel`, in that order, expiring `expiry` from now, or never for
/// `None`: `iat:exp:hex(HMAC-SHA256(secret, iat \0 exp \0 user \0 channel
/// \0 hex(SHA-256(keys joined by \0)))))` (Centrifugo's shared poll
/// documentation, "Signature").
pub(crate) fn shared_poll_signature(
    channel: &str,
    user: &str,
    keys: &[&str],
    expiry: Option<Duration>,
) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs();
    let exp = expiry.map_or(0, expires_in);
    shared_poll_signature_at(channel, user, keys, now, exp)
}

/// As [`shared_poll_signature`], issued at `iat` and expiring at `exp`
/// (Unix seconds, 0 for never).
pub(crate) fn shared_poll_signature_at(
    channel: &str,
    user: &str,
    keys: &[&str],
    iat: u64,
    exp: u64,
) -> String {
    use sha2::Digest;
    let keys_hash = hex(&Sha256::digest(keys.join("\0").as_bytes()));
    let payload = format!("{iat}\0{exp}\0{user}\0{channel}\0{keys_hash}");
    let mut mac = Hmac::<Sha256>::new_from_slice(SHARED_POLL_SECRET.as_bytes())
        .expect("any key size is valid");
    mac.update(payload.as_bytes());
    format!("{iat}:{exp}:{}", hex(&mac.finalize().into_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn expires_in(lifetime: Duration) -> u64 {
    SystemTime::now()
        .checked_add(lifetime)
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .expect("expiry after the epoch")
        .as_secs()
}

fn signed(claims: &serde_json::Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(json!({"alg": "HS256", "typ": "JWT"}).to_string());
    let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signed = format!("{header}.{claims}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(HMAC_SECRET.as_bytes()).expect("any key size is valid");
    mac.update(signed.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signed}.{signature}")
}
