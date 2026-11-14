//! Interop with centrifuge-js (Block 14e): on a real Centrifugo, a
//! centrifuge-js client (JSON) publishes to a channel that two Ferrifuge
//! clients, one JSON and one Protobuf, are subscribed to; each must receive
//! every publication, in order, with the exact bytes centrifuge-js sent.
//! Ferrifuge cannot publish yet (Block 13): the other direction comes then.
//!
//! Needs Docker and Node, so it runs only with `FERRIFUGE_INTEROP=1`, as
//! the interop CI job does, after `npm ci --prefix tests/interop`.
//! `FERRIFUGE_INTEROP_NODE` names the Node command (default `node`).

mod support;

use std::env;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::time::Duration;

use ferrifuge::{
    Client, ClientEvent, Config, Protocol, Subscription, SubscriptionEvent, SubscriptionState,
};
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::json;
use support::centrifugo::Centrifugo;

/// Enables the test.
const RUN: &str = "FERRIFUGE_INTEROP";
/// The Node command.
const NODE: &str = "FERRIFUGE_INTEROP_NODE";
const CHANNEL: &str = "interop";
const PUBLICATIONS: u32 = 100;
/// The package `publish.mjs` imports, which `npm ci --prefix tests/interop`
/// installs.
const PACKAGE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/interop/node_modules/centrifuge/package.json"
);
/// Bound on the centrifuge-js run, which takes about a second.
const NODE_TIMEOUT: Duration = Duration::from_secs(60);

/// Anonymous clients may connect, subscribe and publish to channels
/// without namespace.
const SERVER: &[(&str, &str)] = &[
    (
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_ANONYMOUS",
        "true",
    ),
];

fn main() {
    if env::var_os(RUN).is_none() {
        println!("interop skipped: set {RUN}=1, with Docker and Node");
        return;
    }
    // Before Docker: without it, Node fails only once the server and both
    // clients are up.
    assert!(
        Path::new(PACKAGE).exists(),
        "interop: centrifuge-js is not installed ({PACKAGE} is missing): \
         run `npm ci --prefix tests/interop` first"
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(interop());
}

/// A Ferrifuge client subscribed to [`CHANNEL`], and its subscription's
/// events.
async fn subscriber(
    server: &Centrifugo,
    protocol: Protocol,
) -> (
    Client,
    Subscription,
    impl Stream<Item = SubscriptionEvent> + Unpin + use<>,
) {
    let config = Config::default().protocol(protocol);
    let client = Client::new(server.websocket_url(), config).expect("a valid endpoint");
    let mut events = client.events();
    client.connect();
    loop {
        match events.next().await.expect("the client's events") {
            ClientEvent::Connected { .. } => break,
            ClientEvent::Disconnected { code, reason } => panic!("disconnected: {code} {reason}"),
            _ => {}
        }
    }
    let subscription = client.new_subscription(CHANNEL).expect("a new channel");
    let publications = subscription.events();
    subscription.subscribe();
    while subscription.state() != SubscriptionState::Subscribed {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (client, subscription, publications)
}

/// What centrifuge-js publishes, as `JSON.stringify` writes it.
fn expected(n: u32) -> Vec<u8> {
    serde_json::to_vec(&json!({"n": n, "text": format!("héllo ✓ {n}")})).expect("JSON")
}

async fn received(
    protocol: Protocol,
    events: &mut (impl Stream<Item = SubscriptionEvent> + Unpin),
) {
    for n in 0..PUBLICATIONS {
        let publication = loop {
            let event = tokio::time::timeout(Duration::from_secs(10), events.next())
                .await
                .unwrap_or_else(|_| panic!("{protocol:?}: publication {n} within 10 s"))
                .expect("the subscription's events");
            if let SubscriptionEvent::Publication(publication) = event {
                break publication;
            }
        };
        assert_eq!(
            String::from_utf8_lossy(&publication.data),
            String::from_utf8_lossy(&expected(n)),
            "{protocol:?}: publication {n}"
        );
    }
}

async fn interop() {
    let server = Centrifugo::start(SERVER).await;
    // A failure still removes the server, before the panic goes on: left to
    // drop as the panic unwinds out of `block_on`, the container's removal
    // waits on the runtime shutting down, and the process never exits.
    let outcome = AssertUnwindSafe(exchange(&server)).catch_unwind().await;
    server.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// centrifuge-js publishes; both Ferrifuge clients receive every
/// publication.
async fn exchange(server: &Centrifugo) {
    let (json_client, _json_subscription, mut json_events) =
        subscriber(server, Protocol::Json).await;
    let (protobuf_client, _protobuf_subscription, mut protobuf_events) =
        subscriber(server, Protocol::Protobuf).await;

    let node = env::var(NODE).unwrap_or_else(|_| "node".to_owned());
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/interop/publish.mjs");
    let run = tokio::process::Command::new(node)
        .args([script, &server.websocket_url(), CHANNEL])
        .arg(PUBLICATIONS.to_string())
        .kill_on_drop(true)
        .status();
    let status = tokio::time::timeout(NODE_TIMEOUT, run)
        .await
        .unwrap_or_else(|_| panic!("centrifuge-js: not finished within {NODE_TIMEOUT:?}"))
        .expect("run centrifuge-js");
    assert!(status.success(), "centrifuge-js: {status}");

    received(Protocol::Json, &mut json_events).await;
    received(Protocol::Protobuf, &mut protobuf_events).await;
    println!(
        "interop: {PUBLICATIONS} publications from centrifuge-js 5.7.4 received byte for byte \
         over JSON and Protobuf"
    );
    drop((json_client, protobuf_client));
}
