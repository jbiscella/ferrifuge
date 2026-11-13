//! Steps of Block 36b: subscribe commands in the frame of the connect
//! command. @E scenarios drive the sans-IO core on a simulated clock; @D
//! scenarios run a tokio client against the fake server; the @A scenario
//! counts the frames Centrifugo reads.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, Anomaly, Input, Machine, SubscriptionOptions, Timer, TransportKind,
};
use ferrifuge::{Config, State, StreamPosition, SubscriptionEvent, SubscriptionState};
use serde_json::{Value, json};

use super::ClientWorld;
use super::auto_batching::received_from;
use super::batching::{BatchRun, run, subscription_of_size};

/// What a scenario's steps share.
#[derive(Debug, Default)]
pub(crate) struct Optimistic {
    /// When the callback of "private" was first called.
    called_at: Arc<Mutex<Option<Instant>>>,
    /// How many commands the fake server had before the trigger.
    start: usize,
    /// The channels of the core's subscriptions, in creation order.
    channels: Vec<String>,
    /// The endpoint of the scenario's own Centrifugo server.
    endpoint: Option<String>,
}

// @E: the core.

/// A core over `kind`, not connected, with subscriptions to `channels`
/// subscribed.
fn a_core(world: &mut ClientWorld, config: Config, kind: TransportKind, channels: &[&str]) {
    let mut run = BatchRun::of(
        Machine::new(config)
            .with_transports(vec![kind])
            .expect("one transport"),
    );
    for channel in channels {
        let id = run
            .machine
            .add_subscription((*channel).to_owned(), SubscriptionOptions::default())
            .expect("a new channel");
        run.handle(Input::Subscribe(id));
        run.subscriptions.push(id);
    }
    world.optimistic.channels = channels
        .iter()
        .map(|channel| (*channel).to_owned())
        .collect();
    world.batching.run = Some(run);
}

#[given(
    regex = r"^a client core over (websocket|http_stream) with a subscription subscribed before connect$"
)]
fn a_core_over(world: &mut ClientWorld, transport: String) {
    let kind = match transport.as_str() {
        "websocket" => TransportKind::WebSocket,
        _ => TransportKind::HttpStream,
    };
    a_core(world, Config::default(), kind, &["news"]);
}

#[given(
    "a client core configured with a websocket endpoint only, with a subscription subscribed before connect"
)]
fn a_websocket_only_core(world: &mut ClientWorld) {
    a_core(
        world,
        Config::default(),
        TransportKind::WebSocket,
        &["news"],
    );
}

#[given(
    regex = r"^a client core over websocket with a timeout of (\d+) s and a subscription subscribed before connect$"
)]
fn a_core_with_a_timeout(world: &mut ClientWorld, seconds: u64) {
    let config = Config::default().timeout(Duration::from_secs(seconds));
    a_core(world, config, TransportKind::WebSocket, &["news"]);
}

/// The length of the connect command a core built with `config` sends.
fn connect_length(config: &Config) -> usize {
    let mut run = BatchRun::of(Machine::new(config.clone()));
    run.handle(Input::Connect);
    run.handle(Input::TransportOpened(run.transport.expect("an attempt")));
    let frame = run
        .actions
        .iter()
        .find_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.clone()),
            _ => None,
        })
        .expect("a connect command");
    frame.len()
}

#[given(
    regex = r"^a client core over websocket with a batch frame cap of (\d+) bytes, whose connect command encodes to (\d+) bytes, and five subscriptions subscribed before connect whose subscribe commands encode to (\d+) bytes each$"
)]
fn a_core_with_sizes(world: &mut ClientWorld, cap: usize, connect: usize, size: usize) {
    // The connect data, a JSON string, pads the command to its size.
    let base = Config::default()
        .batch_frame_bytes(cap)
        .data(br#""""#.to_vec());
    let pad = connect
        .checked_sub(connect_length(&base))
        .expect("a larger connect command");
    let config = Config::default()
        .batch_frame_bytes(cap)
        .data(format!(r#""{}""#, "d".repeat(pad)).into_bytes());
    assert_eq!(connect_length(&config), connect);
    let mut run = BatchRun::of(Machine::new(config));
    // The connect command is 1, the subscribe commands 2 to 6.
    world.optimistic.channels = (2..7)
        .map(|id| subscription_of_size(&mut run, id, size))
        .collect();
    world.batching.run = Some(run);
}

#[when("connect is called and the transport opens")]
fn connect_and_open(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(Input::Connect);
    run.handle(Input::TransportOpened(run.transport.expect("an attempt")));
}

/// The id of the last connect command, sent or carried by the request
/// that opened an emulated transport.
fn connect_id(run: &BatchRun) -> u64 {
    run.actions
        .iter()
        .rev()
        .filter_map(|action| match action {
            Action::Send { frame, .. }
            | Action::OpenTransport {
                initial_frame: Some(frame),
                ..
            } => Some(super::batching::commands_of(frame)),
            _ => None,
        })
        .flatten()
        .find(|command| command["connect"].is_object())
        .and_then(|command| command["id"].as_u64())
        .expect("a connect command")
}

/// The id of the subscribe command sent for `channel`.
fn subscribe_id(run: &BatchRun, channel: &str) -> u64 {
    run.frames_since(0)
        .into_iter()
        .flat_map(|(_, commands)| commands)
        .filter(|command| command["subscribe"]["channel"] == channel)
        .filter_map(|command| command["id"].as_u64())
        .next_back()
        .expect("a subscribe command")
}

fn answer_connect(run: &mut BatchRun) {
    let id = connect_id(run);
    run.mark = run.actions.len();
    run.frame(&json!({
        "id": id,
        "connect": {"client": "c", "session": "s", "node": "n"},
    }));
}

#[when("the server answers the connect command")]
fn the_server_answers(world: &mut ClientWorld) {
    answer_connect(run(world));
}

#[when(regex = r"^(\d+) s elapse and the core reads the connect reply$")]
fn elapse_then_connect_reply(world: &mut ClientWorld, seconds: u64) {
    let run = run(world);
    run.advance(Duration::from_secs(seconds));
    answer_connect(run);
}

#[when("1 s elapses")]
fn one_second(world: &mut ClientWorld) {
    run(world).advance(Duration::from_secs(1));
}

/// Each frame sent since the mark, as the kinds of its commands:
/// `connect`, or `subscribe <channel>`.
fn frame_kinds(run: &BatchRun) -> Vec<Vec<String>> {
    run.frames()
        .into_iter()
        .map(|(_, commands)| {
            commands
                .iter()
                .map(|command| match command["subscribe"]["channel"].as_str() {
                    Some(channel) => format!("subscribe {channel}"),
                    None if command["connect"].is_object() => "connect".to_owned(),
                    None => command.to_string(),
                })
                .collect()
        })
        .collect()
}

#[then("the core sends one frame carrying the connect command, then the subscribe command")]
fn connect_then_subscribe(world: &mut ClientWorld) {
    assert_eq!(frame_kinds(run(world)), [["connect", "subscribe news"]]);
}

#[then("the core sends one frame carrying the connect command and the subscribe command")]
fn connect_and_subscribe(world: &mut ClientWorld) {
    connect_then_subscribe(world);
}

#[then(
    "the request opening the stream carries only the connect command, and the core sends nothing"
)]
fn only_connect_opens(world: &mut ClientWorld) {
    let run = run(world);
    let initial: Vec<Value> = run.actions[run.mark..]
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport {
                initial_frame: Some(frame),
                ..
            } => Some(super::batching::commands_of(frame)),
            _ => None,
        })
        .expect("a request opening the stream");
    assert_eq!(initial.len(), 1, "{initial:?}");
    assert!(initial[0]["connect"].is_object(), "{initial:?}");
    assert_eq!(frame_kinds(run), Vec::<Vec<String>>::new());
}

#[then("the core sends no subscribe command")]
fn no_subscribe(world: &mut ClientWorld) {
    let kinds = frame_kinds(run(world)).concat();
    assert!(
        !kinds.iter().any(|kind| kind.starts_with("subscribe")),
        "{kinds:?}"
    );
}

#[then("the core sends an emulation request carrying the subscribe command")]
fn an_emulation_request(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(frame_kinds(run), [["subscribe news"]]);
    let raw: Vec<Value> = run.actions[run.mark..]
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => serde_json::from_slice(frame).ok(),
            _ => None,
        })
        .collect();
    assert!(raw[0].get("session").is_some(), "{raw:?}");
}

#[then(
    regex = r"^the core sends one frame carrying the connect command and the subscribe commands of the first (\d+) subscriptions$"
)]
fn connect_and_the_first(world: &mut ClientWorld, count: usize) {
    let channels = world.optimistic.channels.clone();
    let cap = 1000;
    let run = run(world);
    let mut expected = vec!["connect".to_owned()];
    expected.extend(
        channels[..count]
            .iter()
            .map(|channel| format!("subscribe {channel}")),
    );
    assert_eq!(frame_kinds(run), [expected]);
    let sizes: Vec<usize> = run.actions[run.mark..]
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(frame.len()),
            _ => None,
        })
        .collect();
    assert!(count == 0 || sizes[0] <= cap, "{sizes:?}");
}

#[then(
    regex = r"^the core sends frames carrying (\d+(?:, \d+)*) subscribe commands, for the other subscriptions in the order they were created$"
)]
fn frames_of_the_others(world: &mut ClientWorld, list: String) {
    let counts: Vec<usize> = list
        .split(", ")
        .map(|count| count.parse().expect("a count"))
        .collect();
    let channels = world.optimistic.channels.clone();
    let kinds = frame_kinds(run(world));
    assert_eq!(
        kinds.iter().map(Vec::len).collect::<Vec<_>>(),
        counts,
        "{kinds:?}"
    );
    let total: usize = counts.iter().sum();
    let expected: Vec<String> = channels[channels.len() - total..]
        .iter()
        .map(|channel| format!("subscribe {channel}"))
        .collect();
    assert_eq!(kinds.concat(), expected);
}

#[then(regex = r#"^the client is "connected" and the subscription is "subscribing"$"#)]
fn connected_and_subscribing(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.machine.state(), State::Connected);
    let id = run.subscriptions[0];
    assert_eq!(
        run.machine.subscription_state(id),
        Some(SubscriptionState::Subscribing)
    );
}

#[when(
    "the core reads one frame holding the reply to the subscribe command, then the connect reply"
)]
fn reply_before_connect(world: &mut ClientWorld) {
    let run = run(world);
    let subscribe = subscribe_id(run, "news");
    let connect = connect_id(run);
    run.mark = run.actions.len();
    let transport = run.transport.expect("a transport");
    run.handle(Input::Frame {
        transport,
        data: format!(
            "{}\n{}",
            json!({"id": subscribe, "subscribe": {}}),
            json!({"id": connect, "connect": {"client": "c"}})
        )
        .into_bytes(),
    });
}

#[then("the core reports a reply-before-connect anomaly naming the subscribe command")]
fn the_anomaly(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        run.actions[run.mark..].iter().any(|action| matches!(
            action,
            Action::Anomaly(Anomaly::ReplyBeforeConnect {
                command: "subscribe",
                ..
            })
        )),
        "{:?}",
        &run.actions[run.mark..]
    );
}

#[then("the core closes the transport and, after the reconnect backoff delay, opens a new one")]
fn closed_then_reopened(world: &mut ClientWorld) {
    let run = run(world);
    let old = run.transport.expect("a transport");
    let since = &run.actions[run.mark..];
    assert!(since.contains(&Action::CloseTransport(old)), "{since:?}");
    let delay = since
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .expect("a reconnect after the backoff delay");
    let defaults = Config::default();
    assert!(
        delay >= defaults.min_reconnect_delay && delay <= 2 * defaults.min_reconnect_delay,
        "{delay:?}"
    );
    let opens = |run: &BatchRun, from: usize| {
        run.actions[from..]
            .iter()
            .filter(|action| matches!(action, Action::OpenTransport { .. }))
            .count()
    };
    let mark = run.mark;
    assert_eq!(opens(run, mark), 0);
    run.advance(
        delay
            .checked_sub(Duration::from_millis(1))
            .expect("a delay of at least 1 ms"),
    );
    assert_eq!(opens(run, mark), 0, "opened before the backoff delay");
    run.advance(Duration::from_millis(1));
    assert_eq!(opens(run, mark), 1, "a new attempt after the backoff delay");
    assert_ne!(run.transport, Some(old));
}

#[then(regex = r#"^the client stays "connecting" and emits no further event$"#)]
fn stays_connecting(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.machine.state(), State::Connecting);
    let emitted: Vec<&Action> = run.actions[run.mark..]
        .iter()
        .filter(|action| matches!(action, Action::Emit(..)))
        .collect();
    assert!(emitted.is_empty(), "{emitted:?}");
}

#[then(regex = r#"^the subscription emits no "subscribed" for that transport$"#)]
fn no_subscribed(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        !run.actions[run.mark..].iter().any(|action| matches!(
            action,
            Action::EmitSubscription(_, _, SubscriptionEvent::Subscribed(_))
        )),
        "{:?}",
        &run.actions[run.mark..]
    );
}

// @D: a tokio client against the fake server.

fn subscribe_named(world: &mut ClientWorld, channels: &[&str]) {
    for channel in channels {
        let subscription = world
            .client()
            .new_subscription(*channel)
            .expect("a new channel");
        subscription.subscribe();
        world.named_subscriptions.push(subscription);
    }
}

#[given(
    regex = r#"^a client configured with the fake server's endpoint, with subscriptions to "a", "b" and "c" subscribed before connect$"#
)]
fn three_before_connect(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    subscribe_named(world, &["a", "b", "c"]);
    world.optimistic.start = world.fake().received().len();
}

#[given(
    regex = r#"^a client connected to the fake server, with subscriptions to "a", "b" and "c" subscribed$"#
)]
async fn three_on_a_connection(world: &mut ClientWorld) {
    super::connection::a_connected_client(world).await;
    subscribe_named(world, &["a", "b", "c"]);
    all_subscribed(world).await;
    world.optimistic.start = world.fake().received().len();
}

#[then(
    regex = r#"^the next frame the fake server receives carries the connect command, then the subscribe commands for "a", "b" and "c", in that order$"#
)]
async fn the_connect_frame(world: &mut ClientWorld) {
    let received = received_from(world, world.optimistic.start, 4).await;
    let frame = received[0].1;
    let kinds: Vec<String> = received
        .iter()
        .filter(|(_, of)| *of == frame)
        .map(
            |(command, _)| match command["subscribe"]["channel"].as_str() {
                Some(channel) => format!("subscribe {channel}"),
                None if command["connect"].is_object() => "connect".to_owned(),
                None => command.to_string(),
            },
        )
        .collect();
    assert_eq!(
        kinds,
        ["connect", "subscribe a", "subscribe b", "subscribe c"],
        "{received:?}"
    );
}

#[then(
    "the fake server received exactly one subscribe command per subscription on that connection"
)]
async fn one_subscribe_each(world: &mut ClientWorld) {
    // A second subscribe would follow the connect reply at once.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut channels: Vec<String> = world.fake().received()[world.optimistic.start..]
        .iter()
        .filter_map(|command| command["subscribe"]["channel"].as_str().map(str::to_owned))
        .collect();
    channels.sort();
    assert_eq!(channels, ["a", "b", "c"]);
}

/// Waits until every named subscription is subscribed.
async fn all_subscribed(world: &ClientWorld) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        while !world
            .named_subscriptions
            .iter()
            .all(|subscription| subscription.state() == SubscriptionState::Subscribed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "not all subscribed");
}

#[then(regex = r#"^both subscriptions reach state "subscribed"$"#)]
async fn both_subscribed(world: &mut ClientWorld) {
    all_subscribed(world).await;
}

#[given(
    regex = r#"^a client configured with the fake server's endpoint, with subscriptions to "news" and "private" subscribed before connect, "private" with (a token callback and no token|a data callback|a getState callback)$"#
)]
fn private_with_a_callback(world: &mut ClientWorld, callback: String) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    subscribe_named(world, &["news"]);
    let called_at = Arc::clone(&world.optimistic.called_at);
    let note = move || {
        called_at.lock().unwrap().get_or_insert_with(Instant::now);
    };
    let builder = world.client().subscription_builder("private");
    let builder = match callback.as_str() {
        "a token callback and no token" => builder.get_token(move |_channel| {
            note();
            async { Ok("token".to_owned()) }
        }),
        "a data callback" => builder.get_data(move |_channel| {
            note();
            async { Ok(b"{}".to_vec()) }
        }),
        _ => builder.get_state(move |_channel| {
            note();
            async {
                Ok(StreamPosition {
                    offset: 0,
                    epoch: "e".to_owned(),
                })
            }
        }),
    };
    let private = builder.build().expect("a new channel");
    private.subscribe();
    world.named_subscriptions.push(private);
}

#[then(
    regex = r#"^the first frame the fake server receives carries the connect command and the subscribe command for "news", and no subscribe command for "private"$"#
)]
async fn first_frame_without_private(world: &mut ClientWorld) {
    let received = received_from(world, 0, 2).await;
    let frame = received[0].1;
    let in_frame: Vec<&Value> = received
        .iter()
        .filter(|(_, of)| *of == frame)
        .map(|(command, _)| command)
        .collect();
    assert_eq!(in_frame.len(), 2, "{received:?}");
    assert!(in_frame[0]["connect"].is_object(), "{received:?}");
    assert_eq!(in_frame[1]["subscribe"]["channel"], "news", "{received:?}");
}

#[then(
    regex = r#"^the (?:a token callback and no token|a data callback|a getState callback) of "private" is first called after the fake server sent its connect reply$"#
)]
async fn called_after_the_connect_reply(world: &mut ClientWorld) {
    let called_at = Arc::clone(&world.optimistic.called_at);
    let called = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(at) = *called_at.lock().unwrap() {
                return at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the callback is called");
    let answered = world.fake().connects_answered_at();
    assert!(
        answered.first().is_some_and(|reply| called > *reply),
        "called at {called:?}, connect replies at {answered:?}"
    );
}

#[then(regex = r#"^the subscribe command for "private" reaches the fake server in a later frame$"#)]
async fn private_later(world: &mut ClientWorld) {
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let received = world.fake().received_with_frames();
            if let Some((_, frame)) = received
                .iter()
                .find(|(command, _)| command["subscribe"]["channel"] == "private")
            {
                return (*frame, received[0].1);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a subscribe command for private");
    assert!(found.0 > found.1, "{found:?}");
}

#[given(
    "the fake server answers the first connect command with a temporary error and drops the rest of its frame, as Centrifugo does"
)]
fn the_first_connect_fails(world: &mut ClientWorld) {
    // A failed connect ends the frame on the fake server, as on Centrifugo.
    world.fake().fail_connects(1);
}

/// The delay counts from the step that must act before the reply
/// ("unsubscribe is called on the subscription before the connect reply"),
/// which releases it: counted from the connect command, a runner stalled
/// for longer than the delay let the reply out first (providers, #218).
/// Until then the client waits for the reply, under a connect timeout no
/// stall reaches.
#[given(regex = r"^the fake server delays its connect reply by (\d+) ms$")]
fn a_late_connect_reply(world: &mut ClientWorld, millis: u64) {
    world
        .fake()
        .delay_next_connect_after_release(Duration::from_millis(millis));
    world.timeout = Some(super::STALL_PROOF_TIMEOUT);
}

#[given(
    regex = r#"^a client configured with the fake server's endpoint, with a subscription to "news" subscribed before connect$"#
)]
fn news_before_connect(world: &mut ClientWorld) {
    let endpoint = world.server_endpoint();
    world.new_client(endpoint);
    let subscription = world
        .client()
        .new_subscription("news")
        .expect("a new channel");
    world.track_subscription(subscription);
    world.subscription().subscribe();
}

#[then(regex = r#"^the client emits "connecting", then an "error" event, then "connected"$"#)]
async fn connecting_error_connected(world: &mut ClientWorld) {
    for expected in ["connecting", "error", "connected"] {
        let event = world.next_event().await;
        assert_eq!(event.name(), expected, "{event:?}");
    }
}

#[then(
    regex = r#"^the fake server received exactly (\d+) subscribe commands for "news", each in the frame of a connect command, and no unsubscribe command$"#
)]
async fn subscribes_with_connects(world: &mut ClientWorld, count: usize) {
    let fake = world.fake();
    let subscribes = |received: &[(Value, usize)]| -> Vec<usize> {
        received
            .iter()
            .filter(|(command, _)| command["subscribe"]["channel"] == "news")
            .map(|(_, frame)| *frame)
            .collect()
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while subscribes(&fake.received_with_frames()).len() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let received = fake.received_with_frames();
    let frames = subscribes(&received);
    assert_eq!(frames.len(), count, "{received:?}");
    for frame in frames {
        assert!(
            received
                .iter()
                .any(|(command, of)| *of == frame && command["connect"].is_object()),
            "{received:?}"
        );
    }
    assert!(
        !received
            .iter()
            .any(|(command, _)| command["unsubscribe"].is_object()),
        "{received:?}"
    );
}

#[then(regex = r#"^the subscription emits "subscribed" exactly once and never "unsubscribed"$"#)]
async fn subscribed_once(world: &mut ClientWorld) {
    let events = world
        .subscription_events_during(Duration::from_secs(1))
        .await;
    let names: Vec<&str> = events.iter().map(SubscriptionEvent::name).collect();
    assert_eq!(
        names.iter().filter(|name| **name == "subscribed").count(),
        1,
        "{names:?}"
    );
    assert!(!names.contains(&"unsubscribed"), "{names:?}");
}

#[when(
    regex = r#"^the fake server has received the subscribe command for "news" in the frame of the connect command$"#
)]
async fn subscribe_in_the_connect_frame(world: &mut ClientWorld) {
    let received = received_from(world, 0, 2).await;
    assert!(received[0].0["connect"].is_object(), "{received:?}");
    assert_eq!(
        received[1].0["subscribe"]["channel"], "news",
        "{received:?}"
    );
    assert_eq!(received[0].1, received[1].1, "{received:?}");
}

#[when("unsubscribe is called on the subscription before the connect reply")]
fn unsubscribe_before_the_reply(world: &mut ClientWorld) {
    assert!(
        world.fake().connects_answered_at().is_empty(),
        "the connect reply went out already"
    );
    world.subscription().unsubscribe();
    world.fake().release_held_connect();
}

#[then(
    regex = r#"^after answering the connect and the subscribe commands, the fake server receives an unsubscribe command for "news"$"#
)]
async fn the_unsubscribe_follows(world: &mut ClientWorld) {
    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let received = world.fake().received();
            if received
                .iter()
                .any(|command| command["unsubscribe"]["channel"] == "news")
            {
                return received;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("an unsubscribe command for news");
    assert_eq!(world.fake().connects_answered_at().len(), 1);
    let unsubscribe = received
        .iter()
        .position(|command| command["unsubscribe"].is_object())
        .expect("checked above");
    let subscribe = received
        .iter()
        .position(|command| command["subscribe"].is_object())
        .expect("a subscribe command");
    assert!(subscribe < unsubscribe, "{received:?}");
}

#[then(regex = r#"^the subscription is "unsubscribed" and never emitted "subscribed"$"#)]
async fn unsubscribed_never_subscribed(world: &mut ClientWorld) {
    assert_eq!(
        world.subscription().state(),
        SubscriptionState::Unsubscribed
    );
    let events = world
        .subscription_events_during(Duration::from_millis(500))
        .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Subscribed(_))),
        "{events:?}"
    );
}

// @A: Centrifugo counting the frames it reads.

#[given("a Centrifugo server of its own with Prometheus metrics")]
async fn a_counting_server(world: &mut ClientWorld) {
    world.optimistic.endpoint = Some(super::auto_batching::a_counting_server(world).await);
}

#[given(
    regex = r#"^a client with subscriptions to channels "news" and "chat" subscribed before connect$"#
)]
fn news_and_chat_before_connect(world: &mut ClientWorld) {
    let endpoint = world
        .optimistic
        .endpoint
        .clone()
        .expect("the server's endpoint");
    let builder = ferrifuge::Client::builder(endpoint).config(world.client_config());
    world.build_client(builder);
    let news = world
        .client()
        .new_subscription("news")
        .expect("a new channel");
    news.subscribe();
    world.track_subscription(news.clone());
    world.named_subscriptions.push(news);
    subscribe_named(world, &["chat"]);
}

#[then(regex = r"^the server counted exactly (\d+) websocket frames? received$")]
async fn frames_counted(world: &mut ClientWorld, count: u64) {
    let server = world.server.as_ref().expect("a server");
    assert_eq!(server.websocket_frames_received().await, count);
}

#[then(regex = r#"^the subscription to "news" receives a publication with data (\{.*\})$"#)]
async fn news_receives(world: &mut ClientWorld, data: String) {
    let publication = super::subscriptions::next_publication(world, false).await;
    let got: Value = serde_json::from_slice(&publication.data).expect("JSON data");
    let expected: Value = serde_json::from_str(&data).expect("JSON");
    assert_eq!(got, expected);
}
