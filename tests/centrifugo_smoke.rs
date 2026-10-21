//! Checks the @A/@B test fixtures themselves, independent of the SDK.

mod support;

use std::time::Duration;

use serde_json::json;
use support::centrifugo::{Centrifugo, TAG};
use support::fake_server::{Announce, FakeServer};
use support::protobuf;
use support::http_proxy::HttpProxy;
use support::proxy::ConnectProxy;
use support::relay::{CountingRelay, Mode};
use support::tls;
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

/// The TLS variant serves the client endpoints with the test CA's server
/// certificate and keeps the API plain.
#[tokio::test]
async fn tls_server_serves_client_endpoints_with_the_test_ca() {
    tls::create();
    let (cert, key) = tls::server_pem();
    let server = Centrifugo::start_tls(&[], &cert, &key).await;
    let ca = std::fs::read(tls::ca_path()).expect("CA certificate");
    let https = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(&ca).expect("CA PEM")])
        .build()
        .expect("HTTPS client");

    let response = https
        .get(format!("https://{}/connection/websocket", server.address()))
        .send()
        .await;
    let info = server.api().call("info", json!({})).await;

    let response = response.expect("TLS handshake with the test CA");
    assert!(response.status().is_client_error(), "{response:?}");
    assert!(info.is_ok(), "{info:?}");
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

#[tokio::test]
async fn relay_counts_each_connection_and_relays_traffic() {
    let server = Centrifugo::start(&[(
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    )])
    .await;
    let relay = CountingRelay::start(server.address().to_owned()).await;

    let first = raw_connect(&relay.websocket_url()).await;
    let second = raw_connect(&relay.websocket_url()).await;

    assert!(first.contains(r#""connect":{"client""#), "{first}");
    assert!(second.contains(r#""connect":{"client""#), "{second}");
    assert_eq!(relay.connections(), 2);
}

#[tokio::test]
async fn fake_server_replies_to_connect_records_frames_and_closes_with_any_code() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let server = FakeServer::start(Announce {
        ping: 25,
        pong: true,
    })
    .await;
    let url = format!("ws://{}/connection/websocket", server.address());
    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .expect("open");

    ws.send(Message::text(r#"{"id":1,"connect":{}}"#))
        .await
        .expect("send");
    let reply = ws.next().await;
    server.send(r#"{"push":{"channel":"c"}}"#);
    let pushed = ws.next().await;
    server.close(3999, "fake close");
    let closed = ws.next().await;

    let Some(Ok(Message::Text(reply))) = reply else {
        panic!("{reply:?}");
    };
    let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["connect"]["ping"], 25);
    assert_eq!(reply["connect"]["pong"], true);
    assert!(matches!(pushed, Some(Ok(Message::Text(t))) if t.contains("push")));
    let Some(Ok(Message::Close(Some(frame)))) = closed else {
        panic!("{closed:?}");
    };
    assert_eq!(
        (u16::from(frame.code), frame.reason.as_str()),
        (3999, "fake close")
    );
    assert_eq!(server.connections(), 1);
    assert_eq!(
        server.received(),
        [serde_json::json!({"id": 1, "connect": {}})]
    );
}

#[tokio::test]
async fn fake_server_pings_periodically_until_stopped() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let server = FakeServer::start(Announce {
        ping: 1,
        pong: true,
    })
    .await;
    let url = format!("ws://{}/connection/websocket", server.address());
    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .expect("open");
    ws.send(Message::text(r#"{"id":1,"connect":{}}"#))
        .await
        .expect("send");
    ws.next().await;

    server.ping_every(Duration::from_millis(100));
    let mut pings = 0;
    while pings < 3 {
        let message = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        assert!(
            matches!(message, Ok(Some(Ok(Message::Text(ref t)))) if t.as_str() == "{}"),
            "{message:?}"
        );
        pings += 1;
    }
    server.stop_pinging();
    let silence = tokio::time::timeout(Duration::from_millis(400), ws.next()).await;

    assert!(silence.is_err(), "no ping after stop_pinging: {silence:?}");
}

/// Opens a websocket asking for the Protobuf subprotocol and sends a
/// connect command; the server's first reply, decoded.
async fn raw_protobuf_connect(
    url: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    protobuf::Reply,
) {
    use futures_util::{SinkExt, StreamExt};
    use prost::Message as _;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    let mut request = url.into_client_request().expect("request");
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static(protobuf::SUBPROTOCOL),
    );
    let (mut ws, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("open");
    assert_eq!(
        response.headers().get("sec-websocket-protocol"),
        Some(&HeaderValue::from_static(protobuf::SUBPROTOCOL))
    );
    let connect = protobuf::Command {
        id: 1,
        connect: Some(protobuf::ConnectRequest::default()),
        ..Default::default()
    };
    ws.send(Message::binary(connect.encode_length_delimited_to_vec()))
        .await
        .expect("send connect");
    let reply = match ws.next().await {
        Some(Ok(Message::Binary(frame))) => {
            protobuf::Reply::decode_length_delimited(frame.as_ref()).expect("Protobuf reply")
        }
        other => panic!("unexpected {other:?}"),
    };
    (ws, reply)
}

/// Block 7 VERIFY: Centrifugo v6.10.1 has no option disabling Protobuf; its
/// default configuration accepts the subprotocol and answers in Protobuf.
#[tokio::test]
async fn centrifugo_speaks_protobuf_by_default() {
    let server = Centrifugo::start(&[(
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    )])
    .await;

    let (_ws, reply) = raw_protobuf_connect(&server.websocket_url()).await;

    assert_eq!(reply.id, 1);
    assert!(
        reply
            .connect
            .as_ref()
            .is_some_and(|result| !result.client.is_empty()),
        "{reply:?}"
    );
}

#[tokio::test]
async fn fake_server_speaks_protobuf_when_asked() {
    use futures_util::StreamExt;
    use prost::Message as _;
    use tokio_tungstenite::tungstenite::Message;

    let fake = FakeServer::start(Announce {
        ping: 25,
        pong: true,
    })
    .await;

    let (mut ws, reply) =
        raw_protobuf_connect(&format!("ws://{}/connection/websocket", fake.address())).await;
    fake.ping_every(Duration::from_millis(50));
    let ping = ws.next().await;
    fake.send_protobuf(&[protobuf::Reply::default(), protobuf::Reply::default()]);
    let two_pings = ws.next().await;

    let connect = reply.connect.expect("connect result");
    assert_eq!((reply.id, connect.ping, connect.pong), (1, 25, true));
    assert!(
        fake.received_protobuf()
            .first()
            .is_some_and(|command| command.connect.is_some())
    );
    let Some(Ok(Message::Binary(ping))) = ping else {
        panic!("{ping:?}");
    };
    assert_eq!(
        protobuf::Reply::decode_length_delimited(ping.as_ref()).expect("ping"),
        protobuf::Reply::default()
    );
    let Some(Ok(Message::Binary(two_pings))) = two_pings else {
        panic!("{two_pings:?}");
    };
    assert_eq!(two_pings.as_ref(), [0, 0]);
}

#[tokio::test]
async fn relay_can_be_silent_down_or_forwarding() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let fake = FakeServer::start(Announce::default()).await;
    let relay = CountingRelay::start_in(fake.address(), Mode::Silent).await;
    let mut buffer = [0u8; 64];

    let mut silent = TcpStream::connect(relay.address()).await.expect("connect");
    silent
        .write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("write");
    let silent_read =
        tokio::time::timeout(Duration::from_millis(300), silent.read(&mut buffer)).await;
    relay.set_mode(Mode::Down);
    let mut down = TcpStream::connect(relay.address()).await.expect("connect");
    let down_read = tokio::time::timeout(Duration::from_secs(2), down.read(&mut buffer)).await;
    relay.set_mode(Mode::Forward);
    let forwarded = raw_connect(&relay.websocket_url()).await;

    assert!(
        silent_read.is_err(),
        "silent relay answered: {silent_read:?}"
    );
    assert!(
        matches!(down_read, Ok(Ok(0) | Err(_))),
        "down relay kept the connection: {down_read:?}"
    );
    assert!(forwarded.contains(r#""connect""#), "{forwarded}");
    assert_eq!(relay.connections(), 3);
}

#[tokio::test]
async fn http_proxy_streams_and_rejects_one_emulation_request() {
    let server = Centrifugo::start(&[
        (
            "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
            "true",
        ),
        ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ])
    .await;
    let proxy = HttpProxy::start(server.address().to_owned()).await;
    let http = reqwest::Client::new();
    let emulation = format!("http://{}/emulation", proxy.address());

    let mut stream = http
        .post(format!("http://{}/connection/http_stream", proxy.address()))
        .header("content-type", "application/json")
        .body(r#"{"id":1,"connect":{}}"#)
        .send()
        .await
        .expect("http_stream through the proxy");
    let first = stream.chunk().await.expect("stream chunk").expect("data");
    proxy.reject_next_emulation();
    let rejected = http
        .post(&emulation)
        .body("{}")
        .send()
        .await
        .expect("rejected");
    let relayed = http
        .post(&emulation)
        .body("{}")
        .send()
        .await
        .expect("relayed");

    assert!(
        String::from_utf8_lossy(&first).contains(r#""session""#),
        "{first:?}"
    );
    assert_eq!(rejected.status(), 503);
    assert!(proxy.rejected());
    assert_ne!(relayed.status(), 503);
}
