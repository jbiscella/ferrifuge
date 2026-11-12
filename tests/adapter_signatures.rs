//! A shared poll subscription's handler and its `get_signature` callback
//! calling the client, under each delivery mode, against the fake server
//! (Block 47, D6, D15): what the unit tests of the sans-IO core cannot
//! reach.

#![cfg(feature = "unstable")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferrifuge::{Client, Config, EventDelivery, SharedPollSignature, SharedPollSubscriptionEvent};
use serde_json::Value;
use support::fake_server::{Announce, FakeServer};

/// A handler calls `track` on "subscribed"; the callback, run beside the
/// client, reads the subscription's keys while it creates its future and
/// awaits `ready()` in it. None of them is a handler's wait, so none
/// would deadlock: the keys are signed and tracked, with no error.
async fn a_handler_s_track_is_signed(delivery: EventDelivery) {
    let server = FakeServer::start(Announce::default()).await;
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .config(Config::default())
        .event_delivery(delivery)
        .build()
        .unwrap();
    let seen_by_callback = Arc::new(Mutex::new(Vec::new()));
    let items = Arc::new(Mutex::new(None::<ferrifuge::SharedPollSubscription>));
    let subscription = client
        .shared_poll_subscription_builder("items")
        .get_signature({
            let items = Arc::clone(&items);
            let seen = Arc::clone(&seen_by_callback);
            move |keys: Vec<String>| {
                let subscription = items.lock().unwrap().clone().expect("set");
                seen.lock().unwrap().push(subscription.tracked_keys());
                async move {
                    subscription.ready().await.expect("subscribed");
                    Ok(SharedPollSignature::new(keys, "sig1"))
                }
            }
        })
        .build()
        .unwrap();
    *items.lock().unwrap() = Some(subscription.clone());
    let handled = Arc::new(Mutex::new(Vec::new()));
    subscription.set_handler({
        let handled = Arc::clone(&handled);
        let subscription = subscription.clone();
        move |event: SharedPollSubscriptionEvent| {
            if event.name() == "subscribed" {
                subscription.track(["a"]);
            }
            handled.lock().unwrap().push(event);
        }
    });

    client.connect();
    subscription.subscribe();
    wait_until(|| {
        server.received().iter().any(|command| {
            command["sub_refresh"]["type"] == 1
                && command["sub_refresh"]["track"][0]["signature"] == "sig1"
        })
    })
    .await;
    let track: Value = server
        .received()
        .into_iter()
        .find(|command| command["sub_refresh"]["type"] == 1)
        .expect("a track request");

    assert_eq!(track["sub_refresh"]["track"][0]["items"][0]["key"], "a");
    assert_eq!(*seen_by_callback.lock().unwrap(), [vec!["a".to_owned()]]);
    let handled = handled.lock().unwrap().clone();
    assert!(
        !handled.iter().any(|event| event.name() == "error"),
        "{handled:?}"
    );
    *items.lock().unwrap() = None;
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_handler_s_track_is_signed() {
    a_handler_s_track_is_signed(EventDelivery::Task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_handler_s_track_is_signed() {
    a_handler_s_track_is_signed(EventDelivery::Inline).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_handler_s_track_is_signed() {
    a_handler_s_track_is_signed(EventDelivery::Thread).await;
}

/// A shared poll subscription's handler gets the events emitted before
/// `close()`, then is dropped (Block 41, D5).
#[tokio::test]
async fn a_shared_poll_handler_gets_its_events_before_close_and_is_dropped() {
    /// Records the events' names, and when it is dropped.
    struct Recorder(Arc<Mutex<Vec<&'static str>>>);

    impl ferrifuge::SharedPollSubscriptionHandler for Recorder {
        fn on_event(&mut self, event: SharedPollSubscriptionEvent) {
            self.0.lock().unwrap().push(event.name());
        }
    }

    impl Drop for Recorder {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("dropped");
        }
    }

    let server = FakeServer::start(Announce::default()).await;
    let client = Client::builder(format!("ws://{}/connection/websocket", server.address()))
        .build()
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let subscription = client
        .shared_poll_subscription_builder("items")
        .handler(Recorder(Arc::clone(&seen)))
        .build()
        .unwrap();
    client.connect();
    subscription.subscribe();
    tokio::time::timeout(Duration::from_secs(5), subscription.ready())
        .await
        .expect("subscribed in time")
        .expect("subscribed");

    client.close().await;
    wait_until(|| seen.lock().unwrap().last() == Some(&"dropped")).await;

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[..2], ["subscribing", "subscribed"], "{seen:?}");
    assert_eq!(seen.iter().filter(|name| **name == "dropped").count(), 1);
}

/// Waits up to 5 s for `condition`.
async fn wait_until(condition: impl Fn() -> bool) {
    let met = tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(met.is_ok(), "condition not met within 5 s");
}
