//! Checks the @A/@B test fixtures themselves, independent of the SDK.

mod support;

use serde_json::json;
use support::centrifugo::{Centrifugo, TAG};
use support::proxy::ConnectProxy;

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
