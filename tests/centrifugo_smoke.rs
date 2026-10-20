//! Checks the @A/@B test server fixture itself, independent of the SDK.

mod support;

use serde_json::json;
use support::centrifugo::{Centrifugo, TAG};

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
