//! Steps of Block 46b: publish debouncing per map key (`unstable`), @D
//! scenarios of a tokio client against the fake server, whose map subscribe
//! replies the steps write, with the interval they advise.

// Cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::SubscriptionState;
use serde_json::{Value, json};

use super::super::ClientWorld;
use super::super::connection::a_connected_client;
use super::super::debouncing::{is_finished, outcome_of, received_at, received_of, wait_until};
use super::{a_scripted_map_subscription, nth_subscribe};

const MAP: &str = "map:m";

/// Answers the `n`th subscribe command (from 1) for [`MAP`] with `result`.
async fn answer_subscribe(world: &mut ClientWorld, n: usize, result: Value) {
    let command = nth_subscribe(world, MAP, n).await;
    world
        .fake()
        .send(&json!({"id": command["id"], "subscribe": result}).to_string());
}

/// A client connected to the fake server with a map subscription to
/// [`MAP`] subscribed: its first subscribe command is answered with `pages`,
/// in order, then its load's live reply advises `interval` ms.
async fn a_map_subscribed(world: &mut ClientWorld, pages: &[Value], interval: u32) {
    a_connected_client(world).await;
    a_scripted_map_subscription(world, MAP, |builder| builder);
    world.map().subscribe();
    for (index, page) in pages.iter().enumerate() {
        answer_subscribe(world, index + 1, page.clone()).await;
    }
    answer_subscribe(
        world,
        pages.len() + 1,
        json!({"type": 1, "epoch": "e1", "offset": 5, "publish_debounce": interval}),
    )
    .await;
    let map = world.map().clone();
    wait_until("the map subscription to be subscribed", move || {
        map.state() == SubscriptionState::Subscribed
    })
    .await;
}

/// Makes a publish of `data` to `key` on the world's map subscription, kept
/// in a task and named by `data`.
fn map_publish(world: &mut ClientWorld, key: &str, data: Value) {
    let future = world
        .map()
        .publish(key.to_owned(), data.to_string().into_bytes());
    world
        .debouncing
        .publishes
        .push((data, tokio::spawn(future)));
}

/// The value `n` written to `key` in the scenario of two keys.
fn value_of(key: &str, n: u32) -> Value {
    json!({"key": key, "x": n})
}

/// The commands the fake server received for the map's keys, in order.
fn map_commands(world: &ClientWorld) -> Vec<(Value, std::time::Instant)> {
    world
        .fake()
        .commands_with_times("publish")
        .into_iter()
        .filter(|(command, _)| command["publish"]["channel"] == MAP)
        .collect()
}

fn data_of(command: &Value) -> &Value {
    &command["publish"]["data"]
}

fn json_value(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|error| panic!("{text}: {error}"))
}

#[given(
    regex = r#"^a client with a map subscription to "map:m" on the fake server, whose live reply advises an interval of (\d+) ms$"#
)]
async fn a_map_with_a_live_interval(world: &mut ClientWorld, interval: u32) {
    a_map_subscribed(world, &[], interval).await;
}

#[given(
    regex = r#"^a client with a map subscription to "map:m" on the fake server with an advised interval of (\d+) ms$"#
)]
async fn a_map_with_an_interval(world: &mut ClientWorld, interval: u32) {
    a_map_subscribed(world, &[], interval).await;
}

#[given(
    regex = r#"^a client with a map subscription to "map:m" on the fake server, whose pages carry no interval and whose live reply advises (\d+) ms$"#
)]
async fn a_map_whose_pages_carry_none(world: &mut ClientWorld, interval: u32) {
    // A last state page: the stream page is asked next, the live reply
    // ends the load.
    let page = json!({"type": 1, "phase": 2, "epoch": "e1", "offset": 5});
    a_map_subscribed(world, &[page], interval).await;
}

#[when(regex = r#"^the subscription publishes to keys "a" and "b" twice each without waiting$"#)]
fn publishes_to_two_keys(world: &mut ClientWorld) {
    for key in ["a", "b"] {
        map_publish(world, key, value_of(key, 1));
    }
    for key in ["a", "b"] {
        map_publish(world, key, value_of(key, 2));
    }
}

#[then("the fake server receives one publish per key at once")]
async fn one_publish_per_key(world: &mut ClientWorld) {
    let received = || map_commands(world);
    wait_until("one publish per key", || received().len() >= 2).await;
    // The windows' trailing sends come no sooner than the interval: the next
    // step checks when.
    let first: Vec<Value> = received()
        .iter()
        .take(2)
        .map(|(command, _)| data_of(command).clone())
        .collect();
    assert_eq!(first, [value_of("a", 1), value_of("b", 1)]);
}

#[then(
    regex = r"^no sooner than (\d+) ms and within (\d+) s of those it receives one more per key, each the second value, and no other$"
)]
async fn one_more_per_key(world: &mut ClientWorld, floor: u64, ceiling: u64) {
    let received = || map_commands(world);
    wait_until("one more per key", || received().len() >= 4).await;
    let commands = received();
    let key_of = |index: usize| commands[index].0["publish"]["key"].as_str().unwrap_or_default();
    let trailing: Vec<(Value, std::time::Instant)> = commands[2..].to_vec();
    assert_eq!(
        trailing
            .iter()
            .map(|(command, _)| data_of(command).clone())
            .collect::<Vec<_>>(),
        [value_of(key_of(2), 2), value_of(key_of(3), 2)],
        "each key's latest, once"
    );
    for (key, (command, at)) in [key_of(2), key_of(3)].into_iter().zip(&trailing) {
        assert_eq!(data_of(command), &value_of(key, 2));
        let leading = commands[..2]
            .iter()
            .find(|(command, _)| command["publish"]["key"] == key)
            .map(|(_, at)| *at)
            .expect("the key's leading publish");
        let gap = at.duration_since(leading);
        assert!(gap >= Duration::from_millis(floor), "{gap:?}");
        assert!(gap <= Duration::from_secs(ceiling), "{gap:?}");
    }
    // The windows' next ends have nothing to send: past them, no other.
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(received().len(), 4, "no other");
}

#[given(
    regex = r#"^the publish of (\{.*\}) to key "(\w+)" completed, and the publish of (\{.*\}) to key "(\w+)" waits in the window$"#
)]
async fn one_completed_one_waiting(
    world: &mut ClientWorld,
    first: String,
    key: String,
    second: String,
    second_key: String,
) {
    let first = json_value(&first);
    // A window's end is up to the steps: a stall of the runner cannot end
    // the window before the remove arrives.
    world.client().hold_debounce_timers();
    map_publish(world, &key, first.clone());
    received_at(world, &first).await;
    outcome_of(world, &first).await.expect("the first publish");
    let client = world.client().clone();
    wait_until("the window's timer", move || client.held_debounce_timers() == 1).await;
    let second = json_value(&second);
    map_publish(world, &second_key, second.clone());
    assert!(received_of(world, &second).is_empty(), "it was sent");
    assert!(!is_finished(world, &second), "it completed");
}

#[when(regex = r#"^the subscription removes key "(\w+)"$"#)]
fn removes_the_key(world: &mut ClientWorld, key: String) {
    world.fake().hold_publish_replies();
    let future = world.map().remove(key);
    world
        .debouncing
        .publishes
        .push((json!("remove"), tokio::spawn(future)));
}

#[then(regex = r#"^the fake server receives the remove of key "(\w+)" at once, and no publish of (\{.*\})$"#)]
async fn receives_the_remove(world: &mut ClientWorld, key: String, data: String) {
    let data = json_value(&data);
    let removes = || {
        map_commands(world)
            .iter()
            .filter(|(command, _)| {
                command["publish"]["key"] == key && command["publish"]["removed"] == true
            })
            .count()
    };
    wait_until("the remove", || removes() == 1).await;
    // The window's timer fires now: a window the remove had left open would
    // send the value it held.
    world.client().fire_held_debounce_timers();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(received_of(world, &data).is_empty(), "it was sent");
}

#[then(
    regex = r#"^when it answers the remove with success, the publish of (\{.*\}) and the remove complete with success$"#
)]
async fn answers_the_remove(world: &mut ClientWorld, data: String) {
    let data = json_value(&data);
    assert!(!is_finished(world, &data), "it completed before the reply");
    let remove = json!("remove");
    assert!(!is_finished(world, &remove), "it completed before the reply");
    let fake = world.fake();
    wait_until("the held remove", || fake.answer_held_publish()).await;
    outcome_of(world, &data).await.expect("the replaced publish");
    outcome_of(world, &remove).await.expect("the remove");
}

#[then(regex = r#"^a publish of (\{.*\}) to key "(\w+)" is sent at once$"#)]
async fn a_publish_is_sent_at_once(world: &mut ClientWorld, data: String, key: String) {
    let data = json_value(&data);
    map_publish(world, &key, data.clone());
    received_at(world, &data).await;
    let fake = world.fake();
    wait_until("the held publish", || fake.answer_held_publish()).await;
    outcome_of(world, &data).await.expect("the publish");
}

#[when(
    regex = r#"^the client calls map_publish on "map:m" with key "(\w+)" and (\{.*\}), then with (\{.*\}), without waiting$"#
)]
fn the_client_map_publishes(world: &mut ClientWorld, key: String, first: String, second: String) {
    for data in [first, second] {
        let data = json_value(&data);
        let future = world
            .client()
            .map_publish(MAP, key.clone(), data.to_string().into_bytes());
        world
            .debouncing
            .publishes
            .push((data, tokio::spawn(future)));
    }
}

#[then(regex = r"^the fake server has received (\d+) map publish commands within (\d+) s$")]
async fn received_map_publishes(world: &mut ClientWorld, count: usize, seconds: u64) {
    let started = std::time::Instant::now();
    let received = || map_commands(world);
    wait_until("the map publish commands", || received().len() >= count).await;
    assert!(started.elapsed() <= Duration::from_secs(seconds));
    let numbers: Vec<u64> = received()
        .iter()
        .map(|(command, _)| command["publish"]["data"]["x"].as_u64().unwrap_or(0))
        .collect();
    assert_eq!(numbers, (1..=count as u64).collect::<Vec<_>>());
}

#[when(regex = r#"^the subscription publishes twice to key "(\w+)" without waiting$"#)]
fn publishes_twice(world: &mut ClientWorld, key: String) {
    map_publish(world, &key, json!({"x": 1}));
    map_publish(world, &key, json!({"x": 2}));
}

#[then("the fake server receives one publish at once")]
async fn receives_one_publish(world: &mut ClientWorld) {
    let received = || map_commands(world);
    wait_until("one publish", || !received().is_empty()).await;
    assert_eq!(data_of(&received()[0].0), &json!({"x": 1}));
}

#[then(regex = r"^it receives the second no sooner than (\d+) ms and within (\d+) s of the first$")]
async fn receives_the_second(world: &mut ClientWorld, floor: u64, ceiling: u64) {
    let received = || map_commands(world);
    wait_until("the second publish", || received().len() >= 2).await;
    let commands = received();
    assert_eq!(data_of(&commands[1].0), &json!({"x": 2}));
    let gap = commands[1].1.duration_since(commands[0].1);
    assert!(gap >= Duration::from_millis(floor), "{gap:?}");
    assert!(gap <= Duration::from_secs(ceiling), "{gap:?}");
}
