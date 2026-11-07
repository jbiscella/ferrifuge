//! Ferrifuge's records reach a `log` logger when the application sets no
//! `tracing` subscriber (Block 21, D1 and D7). In its own binary: once a
//! process sets a `tracing` dispatcher, `tracing` sends nothing more to
//! `log`, and the BDD runner sets one.

mod support;

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config};
use futures_util::StreamExt;
use log::{Level, LevelFilter, Log, Metadata, Record};
use support::fake_server::{Announce, FakeServer};

/// The records the logger got: level, target and message with its
/// key-values, as `tracing` formats them.
fn records() -> &'static Mutex<Vec<(Level, String, String)>> {
    static RECORDS: OnceLock<Mutex<Vec<(Level, String, String)>>> = OnceLock::new();
    RECORDS.get_or_init(Mutex::default)
}

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        records().lock().unwrap().push((
            record.level(),
            record.target().to_owned(),
            record.args().to_string(),
        ));
    }

    fn flush(&self) {}
}

#[tokio::test]
async fn records_reach_a_log_logger_without_the_token() {
    static CAPTURE: Capture = Capture;
    log::set_logger(&CAPTURE).expect("the only logger");
    log::set_max_level(LevelFilter::Trace);
    let server = FakeServer::start(Announce::default()).await;
    let token = "tok-1f9e-log-facade";
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(Config::default().token(token.to_owned()))
        .build()
        .expect("a valid client");
    let mut events = client.events();
    client.connect();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(events.next().await, Some(ClientEvent::Connected { .. })) {}
    })
    .await
    .expect("connected in time");
    let records = records().lock().unwrap().clone();
    let ours: Vec<_> = records
        .iter()
        .filter(|(_, target, _)| target.starts_with("ferrifuge"))
        .collect();
    for state in ["connecting", "connected"] {
        assert!(
            ours.iter().any(|(level, _, message)| *level == Level::Debug
                && message.contains(&format!("state=\"{state}\""))),
            "no DEBUG record of the {state} state in {ours:#?}"
        );
    }
    for (_, target, message) in &ours {
        assert!(!message.contains(token), "the token in {target}: {message}");
    }
}

/// The test above cannot see a missing `log` feature: a dev-dependency
/// (axum, through tower) enables it in every test build. The manifest must
/// declare it on the library's own `tracing` dependency.
#[test]
fn the_library_enables_the_log_feature_of_tracing() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata");
    assert!(output.status.success(), "cargo metadata failed");
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata's JSON");
    let tracing = metadata["packages"]
        .as_array()
        .and_then(|packages| {
            packages
                .iter()
                .find(|package| package["name"] == "ferrifuge")
        })
        .and_then(|package| package["dependencies"].as_array())
        .and_then(|dependencies| {
            dependencies
                .iter()
                .find(|dependency| dependency["name"] == "tracing" && dependency["kind"].is_null())
        })
        .expect("a normal tracing dependency");
    assert!(
        tracing["features"]
            .as_array()
            .is_some_and(|features| features.iter().any(|feature| feature == "log")),
        "{tracing}"
    );
}
