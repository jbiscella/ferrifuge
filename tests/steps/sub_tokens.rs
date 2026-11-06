//! Steps of Block 10b: subscription tokens. The @E steps drive the sans-IO
//! core through [`MachineRun`]; the @A steps use a real Centrifugo whose
//! `private` namespace requires subscription tokens.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{SubscriptionOptions, SubscriptionTimer};
use ferrifuge::{ErrorKind, SubscriptionEvent, SubscriptionState, TokenError, codes};
use serde_json::{Value, json};

use super::connection::a_connected_client;
use super::subscriptions::MachineRun;
use super::{ClientWorld, EVENT_TIMEOUT, test_config};
use crate::support::token::subscription_token;

/// Reply fields of a successful subscribe whose token expires.
const SUBSCRIBED_EXPIRING: &str = r#""subscribe":{"expires":true,"ttl":60}"#;
/// Reply fields of a temporary error.
const TEMPORARY_ERROR: &str = r#""error":{"code":100,"message":"busy","temporary":true}"#;
/// Lifetime of the subscription tokens of the @A scenarios: refreshed at
/// 80–90% of it, so several times within a scenario.
const TOKEN_LIFETIME: Duration = Duration::from_secs(3);

fn run(world: &mut ClientWorld) -> &mut MachineRun {
    world
        .subscription_machine
        .as_mut()
        .expect("an @E subscription")
}

fn failed() -> TokenError {
    TokenError::Failed("backend down".to_owned())
}

/// A token callback answer: answers are `Result`s.
#[allow(clippy::unnecessary_wraps)]
fn token(value: &str) -> Result<String, TokenError> {
    Ok(value.to_owned())
}

/// A subscription with a token callback answered from `answers`.
fn with_callback(
    world: &mut ClientWorld,
    options: SubscriptionOptions,
    answers: Vec<Result<String, TokenError>>,
) {
    world.subscription_machine = Some(MachineRun::with(
        options.token_source(),
        Vec::new(),
        answers,
    ));
}

/// Connects and subscribes; the subscribe is answered with a success
/// whose token expires.
fn subscribed_expiring(world: &mut ClientWorld) {
    let run = run(world);
    run.subscribe_answers
        .push_back(SUBSCRIBED_EXPIRING.to_owned());
    run.connect();
    let subscription = run.subscription;
    run.handle(ferrifuge::machine::Input::Subscribe(subscription));
    assert_eq!(run.state(), Some(SubscriptionState::Subscribed));
}

// Givens (@E)

#[given(
    "a subscription to a private channel without an initial token whose token callback fails once"
)]
fn first_fetch_fails_once(world: &mut ClientWorld) {
    with_callback(
        world,
        SubscriptionOptions::default(),
        vec![Err(failed()), token("t2")],
    );
}

#[given("a subscription to a private channel using a subscription token callback")]
fn using_a_token_callback(world: &mut ClientWorld) {
    with_callback(
        world,
        SubscriptionOptions::default(),
        vec![token("t1"), token("t2")],
    );
}

#[given("a subscription to a private channel whose token callback is set")]
fn whose_token_callback_is_set(world: &mut ClientWorld) {
    using_a_token_callback(world);
}

#[given("a subscribed subscription whose token expires and whose token callback fails once")]
fn expiring_with_a_failing_callback(world: &mut ClientWorld) {
    with_callback(
        world,
        SubscriptionOptions::default().token("t0".to_owned()),
        vec![Err(failed()), token("t2")],
    );
    subscribed_expiring(world);
}

#[given("a subscribed subscription whose token expires")]
fn subscribed_whose_token_expires(world: &mut ClientWorld) {
    with_callback(
        world,
        SubscriptionOptions::default().token("t0".to_owned()),
        vec![token("t1"), token("t2")],
    );
    subscribed_expiring(world);
}

#[given(
    r#"a subscription to a private channel without an initial token whose token callback signals "unauthorized""#
)]
fn first_fetch_denied(world: &mut ClientWorld) {
    with_callback(
        world,
        SubscriptionOptions::default(),
        vec![Err(TokenError::Unauthorized)],
    );
}

#[given(
    "a subscription to a private channel without an initial token whose token callback returns an empty token"
)]
fn first_fetch_empty(world: &mut ClientWorld) {
    with_callback(world, SubscriptionOptions::default(), vec![token("")]);
}

#[given("a subscription whose subscribe attempt failed with a temporary error")]
fn subscribe_failed_temporarily(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default(),
        Vec::new(),
        Vec::new(),
    ));
    let run = run(world);
    run.subscribe_answers.push_back(TEMPORARY_ERROR.to_owned());
    run.connect();
    let subscription = run.subscription;
    run.handle(ferrifuge::machine::Input::Subscribe(subscription));
    assert_eq!(
        run.timers(SubscriptionTimer::Resubscribe, 0).len(),
        1,
        "{:?}",
        run.actions
    );
}

#[given("a subscribed subscription whose token refresh failed with a temporary error")]
fn refresh_failed_temporarily(world: &mut ClientWorld) {
    subscribed_whose_token_expires(world);
    let run = run(world);
    run.sub_refresh_answers
        .push_back(TEMPORARY_ERROR.to_owned());
    run.fire(SubscriptionTimer::Refresh);
}

#[given("a subscribed subscription with a fixed token that expires and no token callback")]
fn fixed_expiring_token(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default().token("t0".to_owned()),
        Vec::new(),
        Vec::new(),
    ));
    subscribed_expiring(world);
}

#[given("a subscription with a fixed token and no token callback")]
fn fixed_token(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default().token("t0".to_owned()),
        Vec::new(),
        Vec::new(),
    ));
}

#[given(
    regex = r#"^a subscription to a private channel without an initial token, with a data callback whose token callback (signals "unauthorized"|fails with a temporary error|returns an empty token)$"#
)]
fn data_callback_after_token(world: &mut ClientWorld, outcome: String) {
    let answer = match outcome.as_str() {
        r#"signals "unauthorized""# => Err(TokenError::Unauthorized),
        "fails with a temporary error" => Err(failed()),
        _ => token(""),
    };
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default().data_source().token_source(),
        vec![Ok(br#"{"n":1}"#.to_vec())],
        vec![answer],
    ));
}

#[given(
    "a subscription to a private channel without an initial token, with a token callback and a data callback"
)]
fn token_and_data_callbacks(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default().data_source().token_source(),
        vec![Ok(br#"{"n":1}"#.to_vec())],
        vec![token("t1")],
    ));
}

#[given(
    "a subscription to a private channel with an initial token, a token callback and a data callback"
)]
fn initial_token_and_callbacks(world: &mut ClientWorld) {
    world.subscription_machine = Some(MachineRun::with(
        SubscriptionOptions::default()
            .token("t0".to_owned())
            .data_source()
            .token_source(),
        vec![Ok(br#"{"n":1}"#.to_vec())],
        vec![token("t1")],
    ));
}

// Whens (@E)

/// Subscribes first when nothing is subscribing yet.
#[when("the server answers its subscribe command with error 109")]
fn subscribe_error_109(world: &mut ClientWorld) {
    let run = run(world);
    if !run.subscribing() {
        run.connect();
        let subscription = run.subscription;
        run.handle(ferrifuge::machine::Input::Subscribe(subscription));
    }
    run.mark = run.actions.len();
    run.answer_subscribe(r#""error":{"code":109,"message":"token expired"}"#);
}

#[when("the subscription refresh is due")]
fn refresh_is_due(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
}

#[when("the server answers its sub_refresh with a temporary error")]
fn sub_refresh_temporary_error(world: &mut ClientWorld) {
    let run = run(world);
    run.sub_refresh_answers
        .push_back(TEMPORARY_ERROR.to_owned());
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
}

#[when("the server never answers its sub_refresh")]
fn sub_refresh_unanswered(world: &mut ClientWorld) {
    let run = run(world);
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
    assert_eq!(
        run.requests("sub_refresh", run.mark).len(),
        1,
        "{:?}",
        run.actions
    );
}

#[when("the server answers its sub_refresh with expires false")]
fn sub_refresh_without_expiry(world: &mut ClientWorld) {
    let run = run(world);
    run.sub_refresh_answers
        .push_back(r#""sub_refresh":{"expires":false}"#.to_owned());
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
}

#[when("the server answers its sub_refresh with a permanent error")]
fn sub_refresh_permanent_error(world: &mut ClientWorld) {
    let run = run(world);
    run.sub_refresh_answers
        .push_back(r#""error":{"code":403,"message":"forbidden"}"#.to_owned());
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
}

#[when("the subscription token callback returns an empty token during refresh")]
fn empty_token_on_refresh(world: &mut ClientWorld) {
    let run = run(world);
    run.token_answers = [token("")].into();
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::Refresh);
}

#[when(
    regex = r"^(unsubscribe is called on the subscription|the subscription is removed from the client) before the (?:resubscribe|refresh retry) delay elapses$"
)]
fn ended_before_the_delay(world: &mut ClientWorld, action: String) {
    let run = run(world);
    run.mark = run.actions.len();
    let subscription = run.subscription;
    run.handle(if action.starts_with("unsubscribe") {
        ferrifuge::machine::Input::Unsubscribe(subscription)
    } else {
        ferrifuge::machine::Input::RemoveSubscription(subscription)
    });
}

// Thens (@E)

fn token_of(request: &Value) -> &str {
    request["token"].as_str().unwrap_or_default()
}

fn last_subscribe(run: &MachineRun) -> Value {
    run.requests("subscribe", 0)
        .pop()
        .unwrap_or_else(|| panic!("no subscribe command: {:?}", run.actions))
}

fn error_events(run: &MachineRun) -> Vec<&ferrifuge::ClientError> {
    run.events()
        .into_iter()
        .filter_map(|event| match event {
            SubscriptionEvent::Error(error) => Some(error),
            _ => None,
        })
        .collect()
}

#[then(
    regex = r"^the (?:subscription )?token callback is called again after the resubscribe (?:backoff )?delay$"
)]
fn token_callback_called_again_after_backoff(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.token_calls.len(), 2, "{:?}", run.actions);
    let delay = run.delay_before_fetch.expect("a resubscribe delay");
    // The resubscribe backoff starts at 500 ms by default.
    assert!(delay >= Duration::from_millis(500), "{delay:?}");
}

#[then(
    regex = r"^the (?:new )?subscribe command carries the (?:token the second call returned|new token)$"
)]
fn subscribe_carries_the_second_token(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(token_of(&last_subscribe(run)), "t2", "{:?}", run.actions);
}

#[then("the subscription token callback is called again")]
async fn token_callback_called_again(world: &mut ClientWorld) {
    // A real client in @D scenarios (Block 11c).
    if world.subscription_machine.is_none() {
        let calls = std::sync::Arc::clone(&world.token_calls);
        let called_again = tokio::time::timeout(super::EVENT_TIMEOUT, async {
            while calls.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(called_again.is_ok(), "calls: {:?}", calls.lock().unwrap());
        return;
    }
    let run = run(world);
    assert_eq!(run.token_calls.len(), 2, "{:?}", run.actions);
}

#[then("the subscription resubscribes with the new token")]
fn resubscribes_with_the_new_token(world: &mut ClientWorld) {
    let run = run(world);
    let subscribes = run.requests("subscribe", 0);
    assert_eq!(subscribes.len(), 2, "{subscribes:?}");
    assert_eq!(token_of(&subscribes[1]), "t2", "{subscribes:?}");
}

#[then("the refresh is retried after 10 to 20 s")]
fn refresh_retried_after_10_to_20_s(world: &mut ClientWorld) {
    let run = run(world);
    let delay = *run
        .timers(SubscriptionTimer::Refresh, run.mark)
        .last()
        .unwrap_or_else(|| panic!("no refresh retry: {:?}", run.actions));
    assert!(
        (Duration::from_secs(10)..=Duration::from_secs(20)).contains(&delay),
        "{delay:?}"
    );
}

#[then(
    r#"after the configured timeout the subscription emits an "error" event with the timeout code"#
)]
fn refresh_times_out(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(
        run.timers(SubscriptionTimer::RefreshReply, run.mark),
        [test_config().timeout],
        "{:?}",
        run.actions
    );
    run.mark = run.actions.len();
    run.fire(SubscriptionTimer::RefreshReply);
    let errors: Vec<_> = error_events(run)
        .into_iter()
        .map(|error| error.code)
        .collect();
    assert_eq!(errors, [codes::error::TIMEOUT], "{:?}", run.actions);
}

#[then("the subscription token callback is called again after 10 to 20 s")]
fn token_callback_called_again_after_10_to_20_s(world: &mut ClientWorld) {
    refresh_retried_after_10_to_20_s(world);
    let run = run(world);
    let calls = run.token_calls.len();
    run.fire(SubscriptionTimer::Refresh);
    assert_eq!(run.token_calls.len(), calls + 1, "{:?}", run.actions);
}

#[then("a new sub_refresh carrying the new token is sent")]
fn new_sub_refresh_with_the_new_token(world: &mut ClientWorld) {
    let run = run(world);
    let refreshes = run.requests("sub_refresh", 0);
    assert_eq!(refreshes.len(), 2, "{refreshes:?}");
    assert_eq!(
        refreshes[1],
        json!({"channel": "news", "token": "t2"}),
        "{refreshes:?}"
    );
}

#[then("no subscribe command is sent after the resubscribe delay")]
fn no_subscribe_after_the_delay(world: &mut ClientWorld) {
    let run = run(world);
    let mark = run.mark;
    run.fire(SubscriptionTimer::Resubscribe);
    assert_eq!(run.requests("subscribe", mark), Vec::<Value>::new());
}

#[then("neither the token callback nor a sub_refresh runs after the refresh retry delay")]
fn no_refresh_after_the_delay(world: &mut ClientWorld) {
    let run = run(world);
    let (mark, calls) = (run.mark, run.token_calls.len());
    run.fire(SubscriptionTimer::Refresh);
    assert_eq!(run.token_calls.len(), calls, "{:?}", run.actions);
    assert_eq!(run.requests("sub_refresh", mark), Vec::<Value>::new());
}

/// No refresh timer set since the step that answered the refresh, or one
/// set and then cancelled.
#[then("no further refresh is scheduled")]
fn no_further_refresh(world: &mut ClientWorld) {
    let run = run(world);
    let set = run.timers(SubscriptionTimer::Refresh, run.mark);
    assert_eq!(set, Vec::<Duration>::new(), "{:?}", run.actions);
}

#[then(r#"the subscription emits "unsubscribed" with that error's code and message"#)]
fn unsubscribed_with_the_error(world: &mut ClientWorld) {
    let run = run(world);
    let expected = SubscriptionEvent::Unsubscribed {
        code: 403,
        reason: "forbidden".into(),
    };
    assert!(run.events().contains(&&expected), "{:?}", run.actions);
}

#[then(r#"the subscription emits an "error" event for the missing token callback"#)]
fn error_for_the_missing_callback(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        error_events(run)
            .iter()
            .any(|error| error.kind == ErrorKind::Configuration
                && error.code == codes::error::BAD_CONFIGURATION),
        "{:?}",
        run.actions
    );
}

#[then(
    regex = r#"^the subscription emits a configuration "error" event with code (\d+) and message "(.*)"$"#
)]
fn configuration_error_with(world: &mut ClientWorld, code: u32, message: String) {
    let run = run(world);
    assert!(
        error_events(run)
            .iter()
            .any(|error| error.kind == ErrorKind::Configuration
                && error.code == code
                && error.message == message),
        "{:?}",
        run.actions
    );
}

#[then(r#"the subscription emits an "error" event of kind "subscribe""#)]
fn subscribe_error_event(world: &mut ClientWorld) {
    let run = run(world);
    assert!(
        error_events(run)
            .iter()
            .any(|error| error.kind == ErrorKind::Subscribe && error.code == 109),
        "{:?}",
        run.actions
    );
}

#[then(regex = r"^(no|an) unsubscribe command is sent$")]
fn unsubscribe_command_sent(world: &mut ClientWorld, which: String) {
    let run = run(world);
    let sent = run.requests("unsubscribe", 0).len();
    assert_eq!(sent, usize::from(which == "an"), "{:?}", run.actions);
}

#[then("no sub_refresh command is sent")]
fn no_sub_refresh(world: &mut ClientWorld) {
    let run = run(world);
    assert_eq!(run.requests("sub_refresh", 0), Vec::<Value>::new());
}

#[then("the data callback is not called for that attempt")]
fn no_data_callback(world: &mut ClientWorld) {
    if super::connect_data::drives_a_machine(world) {
        super::connect_data::no_data_callback(world);
        return;
    }
    let run = run(world);
    assert!(!run.calls.contains(&"data"), "{:?}", run.calls);
}

#[then("the token callback is called before the data callback")]
fn token_before_data(world: &mut ClientWorld) {
    if super::connect_data::drives_a_machine(world) {
        super::connect_data::token_before_data(world);
        return;
    }
    let run = run(world);
    assert_eq!(run.calls, ["token", "data"]);
}

#[then(
    regex = r"^the subscribe command carries (?:both the token|the initial token) and the data$"
)]
fn subscribe_carries_token_and_data(world: &mut ClientWorld) {
    let run = run(world);
    let expected = if run.token_calls.is_empty() {
        "t0"
    } else {
        "t1"
    };
    let subscribe = last_subscribe(run);
    assert_eq!(
        (token_of(&subscribe), &subscribe["data"]),
        (expected, &json!({"n": 1})),
        "{subscribe}"
    );
}

#[then("the token callback is not called")]
fn token_callback_not_called(world: &mut ClientWorld) {
    if super::connect_data::drives_a_machine(world) {
        super::connect_data::token_callback_not_called(world);
        return;
    }
    let run = run(world);
    assert_eq!(run.token_calls, Vec::<String>::new());
}

// @A: a real Centrifugo, `private` channels requiring subscription tokens.

/// A token callback issuing tokens of [`TOKEN_LIFETIME`] for the channel
/// asked, recording each call, until the scenario denies access.
fn token_callback(
    world: &ClientWorld,
) -> impl Fn(String) -> std::future::Ready<Result<String, TokenError>> + Clone + Send + Sync + 'static
{
    let calls = Arc::clone(&world.token_calls);
    let denied = Arc::clone(&world.token_denied);
    move |channel: String| {
        calls.lock().unwrap().push(channel.clone());
        std::future::ready(if denied.load(Ordering::SeqCst) {
            Err(TokenError::Unauthorized)
        } else {
            // Anonymous connections have the empty user.
            Ok(subscription_token("", &channel, TOKEN_LIFETIME))
        })
    }
}

#[given("a client subscribed to a private channel using a subscription token with expiration")]
async fn subscribed_with_an_expiring_token(world: &mut ClientWorld) {
    a_connected_client(world).await;
    let subscription = world
        .client()
        .subscription_builder("private:news")
        .get_token(token_callback(world))
        .build()
        .expect("no subscription yet");
    world.track_subscription(subscription);
    world.subscription().subscribe();
    // Up to the "subscribed" event itself, not the state: a subscription
    // that is already subscribed when its state is first read would leave
    // its events to the next steps.
    let subscribed = tokio::time::timeout(EVENT_TIMEOUT, async {
        while !matches!(
            world.next_subscription_event(false).await,
            SubscriptionEvent::Subscribed(_)
        ) {}
    })
    .await;
    assert!(subscribed.is_ok(), "{:?}", world.subscription());
}

/// No subscription event other than publications while the time elapsed,
/// and still subscribed. The server only ends an expired subscription at
/// its periodic check (2501 "subscription expired" after about 16 s, on
/// v6.10.1), so the refresh count below is what shows the refreshes.
#[then(r#"the subscription stays "subscribed""#)]
async fn the_subscription_stays_subscribed(world: &mut ClientWorld) {
    let events = world.subscription_events_during(Duration::ZERO).await;
    assert!(
        events
            .iter()
            .all(|event| matches!(event, SubscriptionEvent::Publication(_))),
        "{events:?}"
    );
    assert_eq!(world.subscription().state(), SubscriptionState::Subscribed);
}

#[then("the subscription token was refreshed at least twice")]
fn refreshed_at_least_twice(world: &mut ClientWorld) {
    // The first call got the token to subscribe with.
    let calls = world.token_calls.lock().unwrap().len();
    assert!(calls >= 3, "{calls} token callback calls");
}

#[given(
    regex = r#"^a connected client with subscriptions to private channels "(\w+)" and "(\w+)" using one token callback$"#
)]
async fn subscriptions_with_one_token_callback(world: &mut ClientWorld, a: String, b: String) {
    a_connected_client(world).await;
    let callback = token_callback(world);
    for name in [a, b] {
        let subscription = world
            .client()
            .subscription_builder(format!("private:{name}"))
            .get_token(callback.clone())
            .build()
            .expect("no subscription yet");
        world.named_subscriptions.push(subscription);
    }
}

#[then(
    regex = r#"^the token callback was called once for channel "(\w+)" and once for channel "(\w+)"$"#
)]
fn token_callback_called_per_channel(world: &mut ClientWorld, a: String, b: String) {
    let mut calls = world.token_calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(calls, [format!("private:{a}"), format!("private:{b}")]);
}

#[when(r#"the subscription token callback signals "unauthorized" during refresh"#)]
fn token_denied_on_refresh(world: &mut ClientWorld) {
    world.token_denied.store(true, Ordering::SeqCst);
}
