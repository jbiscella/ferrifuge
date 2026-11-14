//! Block 53: numbered timer settings and zero durations refused.
//!
//! The @E scenarios drive the sans-IO core on [`BatchRun`]'s clock and
//! take each setting's number from the set action that gave it, as an
//! owner of the machine does; the zero-duration rows build a client or a
//! subscription.

// Cucumber hands each capture over as an owned `String`.
#![allow(clippy::needless_pass_by_value)]

use std::collections::BTreeMap;
use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::machine::{
    Action, Input, SubscriptionId, SubscriptionOptions, SubscriptionTimer, Timer, TimerSetting,
};
use ferrifuge::{
    Client, ClientError, Config, ErrorKind, State, SubscriptionError, TokenError, codes,
};
use serde_json::{Value, json};

use super::batching::BatchRun;
use super::{ClientWorld, UNREACHABLE_ENDPOINT, test_config};

/// What a Block 53 scenario holds.
#[derive(Default)]
pub(crate) struct NumberedTimers {
    run: Option<BatchRun>,
    /// The firings of the first and the second setting of the scenario's
    /// timer, in that order.
    first: Option<Input>,
    second: Option<Input>,
    /// The actions of the step under test.
    returned: Vec<Action>,
    /// A zero-duration row: what is built, and the outcome.
    builder: Option<(String, String)>,
    built: Option<Result<(), BuildError>>,
}

#[derive(Debug)]
enum BuildError {
    Client(ClientError),
    Subscription(SubscriptionError),
}

fn state(world: &mut ClientWorld) -> &mut NumberedTimers {
    &mut world.numbered_timers
}

fn run(world: &mut ClientWorld) -> &mut BatchRun {
    world
        .numbered_timers
        .run
        .as_mut()
        .expect("a Block 53 machine")
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// A machine connected over websocket, its server pinging every 25 s.
fn pinged(config: Config, token: bool) -> BatchRun {
    let mut run = BatchRun::new(config, token);
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c", "ping": 25}));
    run
}

/// The settings of the client timer `timer` given in `actions`, in order.
fn client_settings(actions: &[Action], timer: Timer) -> Vec<TimerSetting> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::SetTimer(of, _, setting) if *of == timer => Some(*setting),
            _ => None,
        })
        .collect()
}

/// The settings of the subscription timer `timer` of `id` given in
/// `actions`, in order.
fn subscription_settings(
    actions: &[Action],
    id: SubscriptionId,
    timer: SubscriptionTimer,
) -> Vec<TimerSetting> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::SetSubscriptionTimer(of, kind, _, setting) if *of == id && *kind == timer => {
                Some(*setting)
            }
            _ => None,
        })
        .collect()
}

/// Feeds `input` and keeps the actions of that step alone.
fn feed(world: &mut ClientWorld, input: Input) {
    let run = run(world);
    let from = run.actions.len();
    run.handle(input);
    let returned = run.actions[from..].to_vec();
    state(world).returned = returned;
}

/// The id of the last command sent that has the field `field`.
fn last_command(run: &BatchRun, field: &str) -> u64 {
    run.frames_since(0)
        .iter()
        .flat_map(|(_, commands)| commands.clone())
        .rev()
        .find(|command| command.get(field).is_some())
        .and_then(|command| command["id"].as_u64())
        .unwrap_or_else(|| panic!("no {field} command in {:?}", run.actions))
}

/// Answers the last command with the field `field` with `reply`.
fn answer(run: &mut BatchRun, field: &str, reply: &Value) {
    let id = last_command(run, field);
    run.frame(&json!({"id": id, field: reply}));
}

#[given(
    regex = r"^a machine whose (no-ping deadline|connect deadline|subscribe reply deadline) was set and then set again$"
)]
fn a_timer_set_twice(world: &mut ClientWorld, timer: String) {
    let (run, settings) = match timer.as_str() {
        "no-ping deadline" => {
            let mut run = pinged(Config::default(), false);
            // A ping read sets the deadline again (Block 4).
            run.frame(&json!({}));
            let settings = client_settings(&run.actions, Timer::ServerPing)
                .into_iter()
                .map(|setting| Input::TimerFired(Timer::ServerPing, setting))
                .collect::<Vec<_>>();
            (run, settings)
        }
        "connect deadline" => {
            // Set at the open with `handshake_timeout`, then again at
            // `TransportOpened` with `timeout` (Block 29, D1).
            let mut run = BatchRun::new(Config::default(), false);
            run.handle(Input::Connect);
            let transport = run.transport.expect("a transport opening");
            run.handle(Input::TransportOpened(transport));
            let settings = client_settings(&run.actions, Timer::ConnectTimeout)
                .into_iter()
                .map(|setting| Input::TimerFired(Timer::ConnectTimeout, setting))
                .collect::<Vec<_>>();
            (run, settings)
        }
        "subscribe reply deadline" => {
            // Set when the subscribe is sent, then again when the
            // reconnected client sends it once more.
            let mut run = pinged(Config::default(), false);
            let id = run
                .machine
                .add_subscription("news".to_owned(), SubscriptionOptions::default())
                .expect("a new channel");
            run.handle(Input::Subscribe(id));
            run.handle(Input::Flush);
            let transport = run.transport.expect("a transport");
            run.handle(Input::TransportClosed {
                transport,
                code: Some(3001),
                reason: "closed by the test".to_owned(),
            });
            let setting = run
                .machine
                .timer_setting(Timer::Reconnect)
                .expect("a reconnect set");
            run.handle(Input::TimerFired(Timer::Reconnect, setting));
            run.connect(&json!({"client": "c", "ping": 25}));
            run.handle(Input::Flush);
            let settings =
                subscription_settings(&run.actions, id, SubscriptionTimer::SubscribeReply)
                    .into_iter()
                    .map(|setting| {
                        Input::SubscriptionTimerFired(
                            id,
                            SubscriptionTimer::SubscribeReply,
                            setting,
                        )
                    })
                    .collect::<Vec<_>>();
            (run, settings)
        }
        other => unreachable!("{other}"),
    };
    let [first, second] = <[Input; 2]>::try_from(settings)
        .unwrap_or_else(|settings| panic!("not two settings: {settings:?} in {:?}", run.actions));
    let scenario = state(world);
    scenario.run = Some(run);
    scenario.first = Some(first);
    scenario.second = Some(second);
}

#[when("the firing of the first setting is fed")]
fn the_first_firing(world: &mut ClientWorld) {
    let input = state(world).first.take().expect("a first setting");
    feed(world, input);
}

#[when("the firing of the second setting is fed")]
fn the_second_firing(world: &mut ClientWorld) {
    let input = state(world).second.take().expect("a second setting");
    feed(world, input);
}

#[then("the step returns no action")]
fn no_action(world: &mut ClientWorld) {
    let returned = &state(world).returned;
    assert!(returned.is_empty(), "{returned:?}");
}

#[then("the machine acts on it")]
fn the_machine_acts(world: &mut ClientWorld) {
    let returned = &state(world).returned;
    assert!(
        returned
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{returned:?}"
    );
}

#[given(
    "a connected machine whose server pings, with a message held unread while the application holds its events"
)]
fn a_held_message(world: &mut ClientWorld) {
    let mut run = BatchRun::new(Config::default(), false);
    // The client's event stream is taken and not read: the connected
    // event waits, and what is read after it is held (Block 15).
    run.handle(Input::StreamOpened(None));
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c", "ping": 25}));
    run.frame(&json!({"push": {"message": {"data": {"n": 1}}}}));
    state(world).run = Some(run);
}

#[when("the transport's close is read behind it, cancelling the no-ping deadline")]
fn the_close_is_read(world: &mut ClientWorld) {
    let run = run(world);
    let transport = run.transport.expect("a transport");
    let before = client_settings(&run.actions, Timer::ServerPing);
    let from = run.actions.len();
    run.handle(Input::TransportClosed {
        transport,
        code: Some(3500),
        reason: "bye".to_owned(),
    });
    let closed = &run.actions[from..];
    assert!(
        closed.contains(&Action::CancelTimer(Timer::ServerPing)),
        "{closed:?}"
    );
    // The close waits behind the held message: still connected.
    assert_eq!(run.machine.state(), State::Connected);
    let setting = *before.last().expect("a no-ping deadline set");
    state(world).first = Some(Input::TimerFired(Timer::ServerPing, setting));
}

#[when("the firing of the no-ping deadline set before the close is fed")]
fn the_firing_before_the_close(world: &mut ClientWorld) {
    let input = state(world).first.take().expect("the no-ping setting");
    feed(world, input);
}

#[given("a connected machine whose server pings")]
fn a_pinged_machine(world: &mut ClientWorld) {
    state(world).run = Some(pinged(Config::default(), false));
}

#[when("a sleep shorter than the no-ping window is fed, setting the no-ping deadline again")]
fn a_short_sleep(world: &mut ClientWorld) {
    let run = run(world);
    let before = client_settings(&run.actions, Timer::ServerPing);
    let from = run.actions.len();
    // The window is the ping interval and maxServerPingDelay: 25 s + 10 s.
    run.handle(Input::Slept { gap: secs(20) });
    let again = client_settings(&run.actions[from..], Timer::ServerPing);
    assert_eq!(again.len(), 1, "{:?}", &run.actions[from..]);
    let setting = *before.last().expect("a no-ping deadline set");
    assert_ne!(again[0], setting);
    state(world).first = Some(Input::TimerFired(Timer::ServerPing, setting));
}

#[when("the firing of the no-ping deadline set before the sleep is fed")]
fn the_firing_before_the_sleep(world: &mut ClientWorld) {
    let input = state(world).first.take().expect("the no-ping setting");
    feed(world, input);
}

#[then("the step returns no action and the client stays connected")]
fn no_action_and_connected(world: &mut ClientWorld) {
    no_action(world);
    assert_eq!(run(world).machine.state(), State::Connected);
}

#[given("a machine driven through connects, subscribes, calls, refreshes and reconnects")]
fn a_machine_driven_through_everything(world: &mut ClientWorld) {
    let mut run = BatchRun::new(Config::default().token("t0".to_owned()), true);
    run.handle(Input::Connect);
    run.connect(&json!({"client": "c", "ping": 25, "expires": true, "ttl": 20}));
    let id = run
        .machine
        .add_subscription("news".to_owned(), SubscriptionOptions::default())
        .expect("a new channel");
    for round in 0..2 {
        if round == 0 {
            run.handle(Input::Subscribe(id));
        }
        run.handle(Input::Flush);
        answer(&mut run, "subscribe", &json!({}));
        run.rpc(&format!("rpc{round}"));
        run.handle(Input::Flush);
        answer(&mut run, "rpc", &json!({"data": {}}));
        // The connection token's refresh, due within 20 s.
        run.advance(secs(18));
        let fetch = run
            .actions
            .iter()
            .rev()
            .find_map(|action| match action {
                Action::FetchToken(fetch) => Some(*fetch),
                _ => None,
            })
            .expect("a token fetch for the refresh");
        run.handle(Input::TokenFetched {
            fetch,
            result: Ok::<_, TokenError>(format!("t{}", round + 1)),
        });
        run.handle(Input::Flush);
        answer(
            &mut run,
            "refresh",
            &json!({"client": "c", "expires": true, "ttl": 20}),
        );
        // A reconnect-advised close, and the next connection.
        let transport = run.transport.expect("a transport");
        run.handle(Input::TransportClosed {
            transport,
            code: Some(3001),
            reason: "closed by the test".to_owned(),
        });
        run.advance(secs(1));
        run.connect(&json!({"client": "c", "ping": 25, "expires": true, "ttl": 20}));
    }
    state(world).run = Some(run);
}

#[then("every timer setting it gave carries a number no other setting carried")]
fn every_setting_is_new(world: &mut ClientWorld) {
    let run = run(world);
    let mut given: BTreeMap<String, &Action> = BTreeMap::new();
    let mut families = [0; 3];
    for action in &run.actions {
        let (setting, family) = match action {
            Action::SetTimer(_, _, setting) => (setting, 0),
            Action::SetSubscriptionTimer(_, _, _, setting) => (setting, 1),
            Action::SetCallTimer(_, _, _, setting) => (setting, 2),
            _ => continue,
        };
        families[family] += 1;
        if let Some(earlier) = given.insert(format!("{setting:?}"), action) {
            panic!("{earlier:?} and {action:?} share a number");
        }
    }
    assert!(
        families.iter().all(|count| *count > 0),
        "each family set: {families:?}"
    );
}

#[given(regex = r"^a (client|subscription builder) whose (\w+) is 0$")]
fn a_zero_duration(world: &mut ClientWorld, builder: String, duration: String) {
    state(world).builder = Some((builder, duration));
}

#[when("it is built")]
async fn it_is_built(world: &mut ClientWorld) {
    let (builder, duration) = state(world).builder.clone().expect("a builder");
    let built = if builder == "client" {
        let mut config = test_config();
        match duration.as_str() {
            "timeout" => config.timeout = Duration::ZERO,
            "handshake_timeout" => config.handshake_timeout = Duration::ZERO,
            "min_reconnect_delay" => config.min_reconnect_delay = Duration::ZERO,
            "max_reconnect_delay" => config.max_reconnect_delay = Duration::ZERO,
            other => panic!("no client duration {other}"),
        }
        Client::builder(UNREACHABLE_ENDPOINT)
            .config(config)
            .build()
            .map(drop)
            .map_err(BuildError::Client)
    } else {
        let client =
            Client::new(UNREACHABLE_ENDPOINT, test_config()).expect("a client of valid options");
        let subscription = client.subscription_builder("news");
        let subscription = match duration.as_str() {
            "min_resubscribe_delay" => subscription.min_resubscribe_delay(Duration::ZERO),
            "max_resubscribe_delay" => subscription.max_resubscribe_delay(Duration::ZERO),
            other => panic!("no subscription duration {other}"),
        };
        let built = subscription
            .build()
            .map(drop)
            .map_err(BuildError::Subscription);
        client.close().await;
        built
    };
    state(world).built = Some(built);
}

#[then(regex = r"^building fails with an error naming (\w+)$")]
fn building_fails_naming(world: &mut ClientWorld, duration: String) {
    let built = state(world).built.take().expect("a build");
    match built {
        Err(BuildError::Client(error)) => {
            assert_eq!(error.kind, ErrorKind::Configuration, "{error:?}");
            assert_eq!(error.code, codes::error::BAD_CONFIGURATION, "{error:?}");
            assert!(
                error.message.contains(&format!("Config::{duration} ")),
                "{error:?}"
            );
        }
        Err(BuildError::Subscription(error)) => {
            assert!(
                matches!(&error, SubscriptionError::ZeroDelay { option } if *option == duration),
                "{error:?}"
            );
            assert!(error.to_string().contains(&duration), "{error}");
        }
        Ok(()) => panic!("built with a zero {duration}"),
    }
}
