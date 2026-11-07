//! Steps of Block 25: system sleep (@E). They drive the sans-IO core on the
//! simulated clock of Block 18's steps.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, CallId, CallTimer, Input, SubscriptionId, SubscriptionOptions, SubscriptionTimer, Timer,
};
use ferrifuge::{ClientEvent, Config, State};
use serde_json::{Value, json};

use super::ClientWorld;
use super::batching::{BatchRun, Key};

/// What a Block 25 scenario holds.
#[derive(Default)]
pub(crate) struct Sleep {
    run: Option<BatchRun>,
    /// Where the actions of the sleep, or of the attempt after it, start.
    mark: usize,
    /// How long the reconnect timer had left when the sleep was reported.
    reconnect_due: Option<Duration>,
    call: Option<CallId>,
    subscription: Option<SubscriptionId>,
}

fn sleep(world: &mut ClientWorld) -> &mut Sleep {
    &mut world.sleep
}

fn run(world: &mut ClientWorld) -> &mut BatchRun {
    world.sleep.run.as_mut().expect("a Block 25 machine")
}

/// The actions since the step under test started.
fn since(world: &ClientWorld) -> &[Action] {
    let run = world.sleep.run.as_ref().expect("a Block 25 machine");
    &run.actions[world.sleep.mark..]
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// A machine connected by the connect result `reply`; with a token, it
/// also has a token callback.
fn connected(config: Config, token: bool, reply: &Value) -> BatchRun {
    let config = if token {
        Config {
            token: "t0".to_owned(),
            ..config
        }
    } else {
        config
    };
    let mut run = BatchRun::new(config, token);
    run.handle(Input::Connect);
    run.connect(reply);
    run
}

fn pinging(max_server_ping_delay: u64) -> Config {
    Config {
        max_server_ping_delay: secs(max_server_ping_delay),
        ..Config::default()
    }
}

/// A token ttl whose refresh, at 80% to 90% of it (Block 14), comes after
/// `seconds`.
fn ttl_after(seconds: u64) -> u64 {
    seconds * 5 / 4 + 2
}

/// Lets `by` pass, the server pinging every 20 s when `pinged`.
fn pass(run: &mut BatchRun, by: Duration, pinged: bool) {
    let mut left = by;
    while !left.is_zero() {
        let step = left.min(secs(20));
        run.advance(step);
        if pinged {
            run.frame(&json!({}));
        }
        left -= step;
    }
}

/// Lets time pass until the refresh timer `key` is due in `seconds`.
fn refresh_due_in(run: &mut BatchRun, key: Key, seconds: u64, pinged: bool) {
    let due = run.due(key).expect("a refresh timer");
    let wanted = secs(seconds);
    assert!(due >= wanted, "{key:?} due in {due:?}");
    pass(run, due.saturating_sub(wanted), pinged);
    assert_eq!(run.due(key), Some(wanted));
}

#[given(
    regex = r"^a connected machine whose server pings every (\d+) s, with maxServerPingDelay (\d+) s$"
)]
fn a_pinged_machine(world: &mut ClientWorld, ping: u64, delay: u64) {
    sleep(world).run = Some(connected(
        pinging(delay),
        false,
        &json!({"client": "c", "ping": ping}),
    ));
}

#[given(
    regex = r"^a connected machine whose server pings every (\d+) s, with maxServerPingDelay (\d+) s and a reconnect jitter window of (\d+) s$"
)]
fn a_pinged_machine_with_a_jitter_window(
    world: &mut ClientWorld,
    ping: u64,
    delay: u64,
    window: u64,
) {
    let config = Config {
        reconnect_jitter_window: secs(window),
        ..pinging(delay)
    };
    sleep(world).run = Some(connected(
        config,
        false,
        &json!({"client": "c", "ping": ping}),
    ));
}

#[given(
    regex = r"^a connected machine whose server pings every (\d+) s, with maxServerPingDelay (\d+) s, and whose connection token refresh is due in (\d+) s$"
)]
fn a_pinged_machine_with_a_token(world: &mut ClientWorld, ping: u64, delay: u64, due: u64) {
    let mut run = connected(
        pinging(delay),
        true,
        &json!({"client": "c", "ping": ping, "expires": true, "ttl": ttl_after(due)}),
    );
    refresh_due_in(&mut run, Key::Client(Timer::Refresh), due, true);
    sleep(world).run = Some(run);
}

#[given(
    regex = r#"^a connected machine whose server pings every (\d+) s, with maxServerPingDelay (\d+) s, subscribed to "([^"]+)" with a token whose refresh is due in (\d+) s$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn a_pinged_machine_with_a_subscription_token(
    world: &mut ClientWorld,
    ping: u64,
    delay: u64,
    channel: String,
    due: u64,
) {
    let mut run = connected(pinging(delay), false, &json!({"client": "c", "ping": ping}));
    let mut options = SubscriptionOptions::default();
    "s0".clone_into(&mut options.token);
    options.token_source = true;
    let id = run
        .machine
        .add_subscription(channel, options)
        .expect("a new channel");
    let from = run.actions.len();
    run.handle(Input::Subscribe(id));
    let command = run
        .frames_since(from)
        .into_iter()
        .flat_map(|(_, commands)| commands)
        .find(|command| command["subscribe"].is_object())
        .and_then(|command| command["id"].as_u64())
        .expect("a subscribe command");
    run.frame(&json!({"id": command, "subscribe": {"expires": true, "ttl": ttl_after(due)}}));
    refresh_due_in(
        &mut run,
        Key::Subscription(id, SubscriptionTimer::Refresh),
        due,
        true,
    );
    let sleep = sleep(world);
    sleep.run = Some(run);
    sleep.subscription = Some(id);
}

#[given(
    regex = r"^a connected machine whose server pings every (\d+) s, with maxServerPingDelay (\d+) s and timeout (\d+) s, with an RPC call sent (\d+) s ago and not answered$"
)]
fn a_pinged_machine_with_a_call(
    world: &mut ClientWorld,
    ping: u64,
    delay: u64,
    timeout: u64,
    ago: u64,
) {
    let config = Config {
        timeout: secs(timeout),
        ..pinging(delay)
    };
    let mut run = connected(config, false, &json!({"client": "c", "ping": ping}));
    let call = run.rpc("slow");
    assert!(
        run.due(Key::Call(call, CallTimer::Reply)).is_some(),
        "the call was sent"
    );
    pass(&mut run, secs(ago), false);
    let sleep = sleep(world);
    sleep.run = Some(run);
    sleep.call = Some(call);
}

#[given("a connected machine whose server sends no pings")]
fn a_machine_without_pings(world: &mut ClientWorld) {
    sleep(world).run = Some(connected(Config::default(), false, &json!({"client": "c"})));
}

#[given(
    regex = r"^a connected machine with minReconnectDelay (\d+) ms whose connection token refresh is due in (\d+) s$"
)]
fn a_machine_with_a_token(world: &mut ClientWorld, min: u64, due: u64) {
    let config = Config {
        min_reconnect_delay: Duration::from_millis(min),
        ..Config::default()
    };
    let mut run = connected(
        config,
        true,
        &json!({"client": "c", "expires": true, "ttl": ttl_after(due)}),
    );
    refresh_due_in(&mut run, Key::Client(Timer::Refresh), due, false);
    sleep(world).run = Some(run);
}

#[given("its transport was lost a moment ago")]
fn its_transport_was_lost(world: &mut ClientWorld) {
    let run = run(world);
    let transport = run.transport.expect("a transport");
    run.handle(Input::TransportClosed {
        transport,
        code: None,
        reason: String::new(),
    });
    assert_eq!(run.machine.state(), State::Connecting);
}

#[given(
    regex = r"^a machine with minReconnectDelay (\d+) ms whose first connection attempt failed$"
)]
fn a_machine_whose_attempt_failed(world: &mut ClientWorld, min: u64) {
    let mut run = BatchRun::new(
        Config {
            min_reconnect_delay: Duration::from_millis(min),
            ..Config::default()
        },
        false,
    );
    run.handle(Input::Connect);
    let transport = run.transport.expect("an attempt");
    run.handle(Input::TransportFailed {
        transport,
        message: "refused".into(),
    });
    assert!(
        run.due(Key::Client(Timer::Reconnect)).is_some(),
        "a reconnect delay"
    );
    sleep(world).run = Some(run);
}

#[given(regex = r"^(\d+) s have elapsed(?: since its last ping)?$")]
fn time_has_elapsed(world: &mut ClientWorld, seconds: u64) {
    pass(run(world), secs(seconds), false);
}

#[when(regex = r"^it is told it slept for (\d+) (ms|s)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_slept(world: &mut ClientWorld, amount: u64, unit: String) {
    let gap = if unit == "ms" {
        Duration::from_millis(amount)
    } else {
        secs(amount)
    };
    let sleep = sleep(world);
    let run = sleep.run.as_mut().expect("a Block 25 machine");
    sleep.mark = run.actions.len();
    sleep.reconnect_due = run.due(Key::Client(Timer::Reconnect));
    run.handle(Input::Slept { gap });
}

#[when("its next attempt starts")]
fn its_next_attempt_starts(world: &mut ClientWorld) {
    let run = run(world);
    let delay = run
        .due(Key::Client(Timer::Reconnect))
        .expect("a reconnect delay");
    run.advance(delay);
}

#[then(
    regex = r#"^it closes the transport and emits "connecting" with code (\d+) and reason "([^"]*)"$"#
)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_reconnects(world: &mut ClientWorld, code: u32, reason: String) {
    let actions = since(world);
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{actions:?}"
    );
    assert!(
        actions.iter().any(|action| matches!(
            action,
            Action::Emit(_, ClientEvent::Connecting { code: c, reason: r }) if *c == code && *r == reason
        )),
        "{actions:?}"
    );
}

#[then(regex = r"^its next attempt is delayed by at least (\d+) ms and at most (\d+) ms$")]
fn its_next_attempt_is_delayed(world: &mut ClientWorld, least: u64, most: u64) {
    let actions = since(world);
    let delay = actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a reconnect delay in {actions:?}"));
    assert!(
        (Duration::from_millis(least)..=Duration::from_millis(most)).contains(&delay),
        "{delay:?}"
    );
}

#[then("it keeps the transport")]
fn it_keeps_the_transport(world: &mut ClientWorld) {
    let actions = since(world);
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{actions:?}"
    );
    assert_eq!(run(world).machine.state(), State::Connected);
}

#[then(regex = r"^its no-ping deadline is (\d+) s from now$")]
fn its_no_ping_deadline(world: &mut ClientWorld, seconds: u64) {
    assert_eq!(
        run(world).due(Key::Client(Timer::ServerPing)),
        Some(secs(seconds))
    );
}

#[then("it returns no action")]
fn it_returns_no_action(world: &mut ClientWorld) {
    let actions = since(world);
    assert!(actions.is_empty(), "{actions:?}");
}

#[then("it asks for a new connection token at once")]
fn it_fetches_a_token_at_once(world: &mut ClientWorld) {
    run(world).advance(Duration::ZERO);
    let actions = since(world);
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::FetchToken(_))),
        "{actions:?}"
    );
}

#[then(regex = r"^its connection token refresh is due in (\d+) s$")]
fn its_refresh_is_due(world: &mut ClientWorld, seconds: u64) {
    assert_eq!(
        run(world).due(Key::Client(Timer::Refresh)),
        Some(secs(seconds))
    );
}

#[then("it asks for a new connection token before opening a transport")]
fn it_fetches_a_token_first(world: &mut ClientWorld) {
    let actions = since(world);
    let position = |wanted: fn(&Action) -> bool| actions.iter().position(wanted);
    let fetch = position(|action| matches!(action, Action::FetchToken(_)))
        .unwrap_or_else(|| panic!("a token fetch in {actions:?}"));
    let open = position(|action| matches!(action, Action::OpenTransport { .. }));
    assert!(open.is_none_or(|open| fetch < open), "{actions:?}");
}

#[then(regex = r#"^it asks for a new token for "([^"]+)" at once$"#)]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn it_fetches_a_subscription_token_at_once(world: &mut ClientWorld, channel: String) {
    run(world).advance(Duration::ZERO);
    let id = world.sleep.subscription.expect("a subscription");
    let actions = since(world);
    assert!(
        actions.iter().any(|action| matches!(
            action,
            Action::FetchSubscriptionToken { subscription, channel: c, .. }
                if *subscription == id && *c == channel
        )),
        "{actions:?}"
    );
}

#[then(regex = r"^the call's reply deadline is (\d+) s from now$")]
fn the_reply_deadline(world: &mut ClientWorld, seconds: u64) {
    let call = world.sleep.call.expect("a call");
    assert_eq!(
        run(world).due(Key::Call(call, CallTimer::Reply)),
        Some(secs(seconds))
    );
}

#[then("its reconnect timer is unchanged")]
fn its_reconnect_timer_is_unchanged(world: &mut ClientWorld) {
    let before = world.sleep.reconnect_due;
    assert!(before.is_some(), "a reconnect delay before the sleep");
    let actions = since(world);
    assert!(
        !actions.iter().any(|action| matches!(
            action,
            Action::SetTimer(Timer::Reconnect, _)
                | Action::CancelTimer(Timer::Reconnect)
                | Action::ShortenTimer(Timer::Reconnect, _)
        )),
        "{actions:?}"
    );
    assert_eq!(run(world).due(Key::Client(Timer::Reconnect)), before);
}
