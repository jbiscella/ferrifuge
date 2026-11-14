//! The soak's hooks (Block 52, D6, D8): `Client::armed_timers` lists the
//! timers of the driver's maps and those the core set and did not cancel,
//! and `Client::pending_calls` every call a registry holds, each with its
//! age; nothing is kept until `Client::record_timers`.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config};
use futures_util::StreamExt;
use support::fake_server::{Announce, FakeServer, Rpc};

/// A bound on anything the tests wait for.
const WAIT: Duration = Duration::from_secs(10);

/// A client of `server`, its records started when `recorded`, connected.
async fn connected(server: &FakeServer, config: Config, recorded: bool) -> Client {
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(config)
        .build()
        .expect("a valid client");
    if recorded {
        client.record_timers();
    }
    let mut events = client.events();
    client.connect();
    let connected = tokio::time::timeout(WAIT, async {
        while let Some(event) = events.next().await {
            if matches!(event, ClientEvent::Connected { .. }) {
                return;
            }
        }
    })
    .await;
    assert!(connected.is_ok(), "not connected within {WAIT:?}");
    client
}

/// The names of the timers in each list: the driver's and the core's.
fn names(client: &Client) -> (BTreeSet<String>, BTreeSet<String>) {
    let timers = client.armed_timers();
    let names = |list: &[ferrifuge::ArmedTimer]| list.iter().map(|t| t.name.clone()).collect();
    (names(&timers.driver), names(&timers.core))
}

/// Waits until both lists name exactly `expected`, within [`WAIT`].
async fn until_both_list(client: &Client, expected: &[&str]) {
    let expected: BTreeSet<String> = expected.iter().map(|name| (*name).to_owned()).collect();
    let listed = tokio::time::timeout(WAIT, async {
        loop {
            let (driver, core) = names(client);
            if driver == expected && core == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        listed.is_ok(),
        "the lists never named {expected:?}: {:?}",
        client.armed_timers()
    );
}

const AT_REST: &[&str] = &["client Refresh", "client ServerPing"];

fn announce() -> Announce {
    Announce {
        ping: 25,
        pong: true,
        ttl: 600,
    }
}

#[tokio::test]
async fn nothing_is_recorded_before_record_timers() {
    let server = FakeServer::start(announce()).await;
    let client = connected(&server, Config::default().token("t"), false).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let timers = client.armed_timers();

    assert!(
        timers.driver.is_empty() && timers.core.is_empty(),
        "{timers:?}"
    );
}

#[tokio::test]
async fn at_rest_both_lists_name_the_ping_deadline_and_the_refresh() {
    let server = FakeServer::start(announce()).await;
    let client = connected(&server, Config::default().token("t"), true).await;

    until_both_list(&client, AT_REST).await;
    let timers = client.armed_timers();

    for timer in timers.driver.iter().chain(&timers.core) {
        assert!(
            timer.overdue.is_zero() && timer.due_in <= Duration::from_secs(600),
            "{timer:?}"
        );
    }
}

#[tokio::test]
async fn the_attempts_timers_leave_both_lists_after_a_reconnect() {
    let server = FakeServer::start(announce()).await;
    let client = connected(&server, Config::default().token("t"), true).await;
    until_both_list(&client, AT_REST).await;
    let connections = server.connections();

    server.close(4000, "reconnect");
    let reconnected = tokio::time::timeout(WAIT, async {
        while server.connections() == connections || client.state() != ferrifuge::State::Connected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reconnected.is_ok(), "no reconnect within {WAIT:?}");

    // The reconnect delay and the connect deadline ran, and are gone.
    until_both_list(&client, AT_REST).await;
}

#[tokio::test]
async fn a_call_in_flight_is_listed_with_its_timer_until_it_settles() {
    let server = FakeServer::start(announce()).await;
    server.answer_rpc(Rpc::Ignore);
    let config = Config::default()
        .token("t")
        .timeout(Duration::from_millis(500));
    let client = connected(&server, config, true).await;
    until_both_list(&client, AT_REST).await;

    let call = tokio::spawn(client.rpc("echo", b"{}".to_vec()));
    let listed = tokio::time::timeout(WAIT, async {
        loop {
            let calls = client.pending_calls();
            if let [call] = &calls[..]
                && call.registries.contains(&"adapter")
                && call.registries.contains(&"by_id")
                && call.registries.contains(&"by_command")
            {
                return call.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the call listed by the adapter and the core");
    let reply = format!("call {} Reply", listed.id);

    // The core notes it when set; the driver lists it once it has
    // performed the setting, at the top of its next turn.
    let both = tokio::time::timeout(WAIT, async {
        loop {
            let (driver, core) = names(&client);
            if driver.contains(&reply) && core.contains(&reply) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(both.is_ok(), "{reply}: {:?}", client.armed_timers());
    let outcome = call.await.expect("the call's task");
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(
        client.pending_calls().is_empty(),
        "{:?}",
        client.pending_calls()
    );
    until_both_list(&client, AT_REST).await;
}

#[tokio::test]
async fn a_call_is_aged_from_when_it_was_made() {
    let server = FakeServer::start(announce()).await;
    server.answer_rpc(Rpc::Ignore);
    let config = Config::default().token("t").timeout(Duration::from_secs(5));
    let client = connected(&server, config, true).await;

    let call = client.rpc("echo", b"{}".to_vec());
    let handle = tokio::spawn(call);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let calls = client.pending_calls();

    assert!(
        matches!(&calls[..], [call] if call.age >= Duration::from_millis(300)),
        "{calls:?}"
    );
    handle.abort();
}
