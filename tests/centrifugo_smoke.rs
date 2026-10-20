//! Checks the @A/@B test fixtures themselves, independent of the SDK.

mod support;

use std::time::Duration;

use serde_json::json;
use support::centrifugo::{Centrifugo, TAG};
use support::proxy::ConnectProxy;
use support::token::{HMAC_SECRET, connection_token};

#[tokio::test]
async fn server_api_reaches_the_pinned_centrifugo_version() {
    let server = Centrifugo::start(&[]).await;

    let info = server.api().call("info", json!({})).await.expect("info");

    let version = info["nodes"][0]["version"].as_str().expect("node version");
    assert!(
        version.starts_with(TAG.trim_start_matches('v')),
        "expected {TAG}, server reports {version}"
    );
}

#[tokio::test]
async fn server_api_reports_method_errors() {
    let server = Centrifugo::start(&[]).await;

    // Namespace "missing" is not configured.
    let error = server
        .api()
        .call("publish", json!({"channel": "missing:channel", "data": {}}))
        .await
        .unwrap_err();

    assert_eq!(error.code, 102, "{error:?}");
    assert_eq!(error.message, "unknown channel");
}

#[tokio::test]
async fn connect_proxy_accepts_then_rejects_as_programmed() {
    let proxy = ConnectProxy::start().await;
    let env = proxy.centrifugo_env();
    let env: Vec<_> = env
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    let server = Centrifugo::start(&env).await;

    let accepted = raw_connect(&server.websocket_url()).await;
    proxy.disconnect_next(4500, "rejected by proxy");
    let rejected = raw_connect(&server.websocket_url()).await;

    assert!(accepted.contains(r#""connect":{"client""#), "{accepted}");
    assert_eq!(rejected, "close 4500 rejected by proxy");
    assert_eq!(proxy.requests(), 2);
    assert!(
        proxy.request(0)["client"].is_string(),
        "{}",
        proxy.request(0)
    );
}

/// Sends a connect command over a raw websocket; returns the first text
/// frame, or the close code and reason.
async fn raw_connect(url: &str) -> String {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.expect("open");
    ws.send(Message::text(r#"{"id":1,"connect":{}}"#))
        .await
        .expect("send connect");
    match ws.next().await {
        Some(Ok(Message::Text(text))) => text.to_string(),
        Some(Ok(Message::Close(Some(frame)))) => {
            format!("close {} {}", u16::from(frame.code), frame.reason)
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn expiring_token_is_accepted_and_refreshed() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let server =
        Centrifugo::start(&[("CENTRIFUGO_CLIENT_TOKEN_HMAC_SECRET_KEY", HMAC_SECRET)]).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(server.websocket_url())
        .await
        .expect("open");
    let mut reply = async |command: String| {
        ws.send(Message::text(command)).await.expect("send");
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                serde_json::from_str::<serde_json::Value>(&text).unwrap()
            }
            other => panic!("unexpected {other:?}"),
        }
    };

    let token = connection_token("u1", Duration::from_secs(30));
    let connected = reply(format!(r#"{{"id":1,"connect":{{"token":"{token}"}}}}"#)).await;
    let token = connection_token("u1", Duration::from_secs(60));
    let refreshed = reply(format!(r#"{{"id":2,"refresh":{{"token":"{token}"}}}}"#)).await;

    assert_eq!(connected["connect"]["expires"], true, "{connected}");
    let ttl = connected["connect"]["ttl"].as_u64().expect("ttl");
    assert!((28..=30).contains(&ttl), "{connected}");
    assert_eq!(refreshed["refresh"]["expires"], true, "{refreshed}");
    let ttl = refreshed["refresh"]["ttl"].as_u64().expect("ttl");
    assert!((58..=60).contains(&ttl), "{refreshed}");
}
