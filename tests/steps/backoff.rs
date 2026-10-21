//! Steps of Block 2: reconnect backoff (@E).
//!
//! The sans-IO core is the controlled clock: a step fails connection
//! attempts and reads the reconnect delays the machine schedules, without
//! network or real time.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::Config;
use ferrifuge::machine::{Action, Input, Machine, Timer};

use super::ClientWorld;

/// Samples taken to estimate the upper bound of an attempt's delay.
const SAMPLES: usize = 1000;
/// Attempts examined for the growth of the upper bound.
const ATTEMPTS: usize = 12;
/// An estimate within this fraction of the maximum counts as having reached it:
/// the largest of 1000 jittered samples falls just short of the true bound.
const REACHED: f64 = 0.95;

/// Reconnect delays scheduled for `attempts` consecutive failed attempts.
fn reconnect_delays(config: &Config, attempts: usize) -> Vec<Duration> {
    let mut machine = Machine::new(config.clone());
    let mut actions = machine.handle(Input::Connect);
    let mut delays = Vec::with_capacity(attempts);
    for _ in 0..attempts {
        let transport = actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport(id) => Some(*id),
                _ => None,
            })
            .expect("an attempt opens a transport");
        actions = machine.handle(Input::TransportFailed {
            transport,
            message: "refused".to_owned(),
        });
        delays.push(
            actions
                .iter()
                .find_map(|action| match action {
                    Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
                    _ => None,
                })
                .expect("a failed attempt schedules a reconnect"),
        );
        actions = machine.handle(Input::TimerFired(Timer::Reconnect));
    }
    delays
}

/// The delay of 0-based `attempt`, sampled `times` times.
fn attempt_delays(config: &Config, attempt: usize, times: usize) -> Vec<Duration> {
    (0..times)
        .map(|_| reconnect_delays(config, attempt + 1)[attempt])
        .collect()
}

fn backoff_config(world: &ClientWorld) -> &Config {
    world
        .backoff_config
        .as_ref()
        .expect("a client with backoff options")
}

#[given(regex = r"^a client with minReconnectDelay (\d+) ms and maxReconnectDelay (\d+) ms$")]
fn a_client_with_backoff_options(world: &mut ClientWorld, min: u64, max: u64) {
    world.backoff_config = Some(Config {
        min_reconnect_delay: Duration::from_millis(min),
        max_reconnect_delay: Duration::from_millis(max),
        ..Config::default()
    });
}

#[given("a client with fixed backoff options")]
fn a_client_with_fixed_backoff_options(world: &mut ClientWorld) {
    world.backoff_config = Some(Config::default());
}

#[when(regex = r"^(\d+) consecutive reconnect attempts fail$")]
fn consecutive_attempts_fail(world: &mut ClientWorld, attempts: usize) {
    world.computed_delays = reconnect_delays(backoff_config(world), attempts);
}

#[when("consecutive reconnect attempts fail")]
fn attempts_keep_failing(world: &mut ClientWorld) {
    let config = backoff_config(world).clone();
    world.upper_bounds = (0..ATTEMPTS)
        .map(|attempt| {
            attempt_delays(&config, attempt, SAMPLES)
                .into_iter()
                .max()
                .expect("samples")
        })
        .collect();
}

#[when(regex = r"^the delay for attempt (\d+) is computed (\d+) times$")]
fn delay_for_attempt_computed(world: &mut ClientWorld, attempt: usize, times: usize) {
    world.computed_delays = attempt_delays(backoff_config(world), attempt, times);
}

#[when(regex = r"^the delay for the same attempt number is computed (\d+) times$")]
fn delay_for_same_attempt_computed(world: &mut ClientWorld, times: usize) {
    world.computed_delays = attempt_delays(backoff_config(world), 3, times);
}

#[then(regex = r"^every computed delay is at most (\d+) ms$")]
fn every_delay_is_at_most(world: &mut ClientWorld, millis: u64) {
    let limit = Duration::from_millis(millis);
    assert!(!world.computed_delays.is_empty(), "no delays computed");
    let over: Vec<_> = world
        .computed_delays
        .iter()
        .filter(|delay| **delay > limit)
        .collect();
    assert!(over.is_empty(), "delays above {limit:?}: {over:?}");
}

#[then("the upper bound of the delay for attempt n+1 is greater than for attempt n")]
fn upper_bound_grows(world: &mut ClientWorld) {
    let max = backoff_config(world).max_reconnect_delay;
    let bounds = &world.upper_bounds;
    for n in 0..bounds.len() - 1 {
        if reached(bounds[n], max) {
            break;
        }
        assert!(
            bounds[n + 1] > bounds[n],
            "attempt {} bound {:?} not above attempt {n} bound {:?}: {bounds:?}",
            n + 1,
            bounds[n + 1],
            bounds[n]
        );
    }
}

#[then(regex = r"^the upper bound stops growing once it reaches (\d+) ms$")]
fn upper_bound_stops_growing(world: &mut ClientWorld, millis: u64) {
    let max = Duration::from_millis(millis);
    let bounds = &world.upper_bounds;
    let first = bounds
        .iter()
        .position(|bound| reached(*bound, max))
        .unwrap_or_else(|| panic!("the bound never reached {max:?}: {bounds:?}"));
    assert!(
        bounds[first..]
            .iter()
            .all(|bound| reached(*bound, max) && *bound <= max),
        "{bounds:?}"
    );
}

#[then("the computed delays are not all equal")]
fn delays_not_all_equal(world: &mut ClientWorld) {
    let first = world.computed_delays.first().expect("delays computed");
    assert!(
        world.computed_delays.iter().any(|delay| delay != first),
        "all {} delays equal {first:?}",
        world.computed_delays.len()
    );
}

fn reached(bound: Duration, max: Duration) -> bool {
    bound.as_secs_f64() >= max.as_secs_f64() * REACHED
}
