//! The driver feeds every debounce timer due at one poll before its next
//! flush (Block 46, D5, D9): windows that end together share a frame.

mod support;

use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config, Subscription, SubscriptionState};
use futures_util::StreamExt;
use serde_json::json;
use support::fake_server::{Announce, FakeServer};

/// How long the trailing sends may take to arrive.
const WAIT: Duration = Duration::from_secs(5);
/// Attempts: the windows of an attempt open a few microseconds apart, so
/// their timers normally end at the same poll; on a loaded machine they
/// can straddle a tick of the timer wheel. A failing flush after each
/// timer never shares a frame in any attempt.
const ATTEMPTS: usize = 10;

fn subscribed(client: &Client, channel: String) -> Subscription {
    let subscription = client.new_subscription(channel).expect("a new channel");
    subscription.subscribe();
    subscription
}

async fn wait_subscribed(subscriptions: &[&Subscription]) {
    tokio::time::timeout(WAIT, async {
        while subscriptions
            .iter()
            .any(|subscription| subscription.state() != SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("subscribed");
}

#[tokio::test]
async fn windows_that_end_together_share_a_frame() {
    let fake = FakeServer::start(Announce::default()).await;
    fake.advise_publish_debounce(Some(50));
    let client = Client::new(
        format!("ws://{}/connection/websocket", fake.address()),
        Config::default(),
    )
    .expect("a valid endpoint");
    let mut events = client.events();
    client.connect();
    loop {
        match tokio::time::timeout(WAIT, events.next())
            .await
            .expect("connected")
        {
            Some(ClientEvent::Connected { .. }) => break,
            Some(_) => {}
            None => panic!("the client's events ended"),
        }
    }
    for attempt in 0..ATTEMPTS {
        let first = subscribed(&client, format!("a{attempt}"));
        let second = subscribed(&client, format!("b{attempt}"));
        wait_subscribed(&[&first, &second]).await;
        let trailing = json!({"attempt": attempt, "trailing": true});
        // The leading publishes open the windows; the later ones wait in
        // them.
        let mut tasks = Vec::new();
        for subscription in [&first, &second] {
            tasks.push(tokio::spawn(
                subscription.publish(json!({"attempt": attempt}).to_string()),
            ));
            tasks.push(tokio::spawn(subscription.publish(trailing.to_string())));
        }
        for task in tasks {
            tokio::time::timeout(WAIT, task)
                .await
                .expect("a settled publish")
                .expect("the publish task")
                .expect("the publish");
        }
        let received = fake.received_with_frames();
        let frames: Vec<usize> = received
            .iter()
            .filter(|(command, _)| command["publish"]["data"] == trailing)
            .map(|(_, frame)| *frame)
            .collect();
        assert_eq!(frames.len(), 2, "both trailing sends arrived: {frames:?}");
        if frames[0] == frames[1] {
            return;
        }
    }
    panic!("no attempt carried both trailing sends in one frame");
}
