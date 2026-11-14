//! Real-server chaos (Block 51): seeded faults against a real Centrifugo
//! through Toxiproxy. Needs Docker, so it runs only with
//! `FERRIFUGE_REAL_CHAOS=1`, as the chaos-real workflow does with
//! `cargo test --test chaos_real`; `cargo test` and `--all-targets` leave
//! it out (D1).
//!
//! This prerequisite only checks the environment: a client connects
//! through Toxiproxy, a toxic slows it down, the proxy turned down drops
//! it, and it connects again once the proxy is up.

mod support;

use std::env;
use std::time::{Duration, Instant};

use ferrifuge::{Client, ClientEvent, Config};
use futures_util::StreamExt;
use serde_json::json;
use support::real_server::{Engine, Environment, PROXY, Stream, configuration};

/// Enables the run.
const RUN: &str = "FERRIFUGE_REAL_CHAOS";
/// How long the client may take to reach a state.
const WITHIN: Duration = Duration::from_secs(20);

fn main() {
    if env::var_os(RUN).is_none() {
        println!("chaos_real skipped: set {RUN}=1, with Docker");
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(environment_works());
}

/// Waits for the next event matching `wanted`, failing after [`WITHIN`].
async fn until(
    events: &mut (impl futures_util::Stream<Item = ClientEvent> + Unpin),
    what: &str,
    wanted: impl Fn(&ClientEvent) -> bool,
) -> ClientEvent {
    tokio::time::timeout(WITHIN, async {
        loop {
            let event = events.next().await.expect("the client's events");
            if wanted(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {what} within {WITHIN:?}"))
}

async fn environment_works() {
    let run = format!("prereq-{}", std::process::id());
    let environment = Environment::start(&run, Engine::Memory, &configuration(Engine::Memory))
        .await
        .unwrap_or_else(|error| panic!("the environment: {error}"));
    let info = environment
        .api()
        .call("info", json!({}))
        .await
        .expect("the server's info");
    println!(
        "chaos_real: {} up, {}",
        environment.run(),
        info["nodes"][0]["version"]
    );

    let token = support::token::connection_token("chaos", Duration::from_secs(3600));
    let config = Config::default().token(token);
    let client = Client::new(environment.websocket_url(), config).expect("a client");
    let mut events = client.events();
    client.connect();
    until(&mut events, "connected", |event| {
        matches!(event, ClientEvent::Connected { .. })
    })
    .await;

    // A toxic slows the connection down, and its removal leaves it up.
    let toxics = environment.toxics();
    toxics
        .add(
            PROXY,
            "latency-down",
            "latency",
            Stream::Downstream,
            json!({"latency": 300, "jitter": 0}),
        )
        .await
        .expect("a latency toxic");
    assert_eq!(toxics.toxics(PROXY).await.unwrap(), ["latency-down"]);
    let start = Instant::now();
    client.rpc("missing", b"{}".to_vec()).await.ok();
    let slowed = start.elapsed();
    toxics
        .remove(PROXY, "latency-down")
        .await
        .expect("the toxic removed");
    assert!(
        slowed >= Duration::from_millis(300),
        "a call took {slowed:?} under 300 ms of latency"
    );

    // The proxy down ends the connection; up again, the client reconnects.
    toxics
        .set_enabled(PROXY, false)
        .await
        .expect("the proxy down");
    until(&mut events, "connecting", |event| {
        matches!(event, ClientEvent::Connecting { .. })
    })
    .await;
    toxics.set_enabled(PROXY, true).await.expect("the proxy up");
    until(&mut events, "connected again", |event| {
        matches!(event, ClientEvent::Connected { .. })
    })
    .await;

    client.close().await;
    println!("chaos_real: the environment works ({slowed:?} for a call under 300 ms of latency)");
}
