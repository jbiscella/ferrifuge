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

/// Samples taken to estimate the upper bound of an attempt's delay. Once an
/// attempt is capped, a delay lands exactly on the maximum with probability
/// about 1/40 (min / max of the jitter range), so the largest of 1000
/// samples is the maximum itself, short of a 1e-11 chance.
const SAMPLES: usize = 1000;
/// Attempts examined for the growth of the upper bound.
const ATTEMPTS: usize = 12;
/// Attempt whose delays the jitter scenario samples.
const JITTER_ATTEMPT: usize = 3;
/// The jitter range is split into this many equal parts, each of which must
/// receive samples: full jitter spreads delays over the whole range.
const JITTER_BUCKETS: usize = 10;

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
    world.computed_delays = attempt_delays(backoff_config(world), JITTER_ATTEMPT, times);
}

/// Also checks the lower bound: the Goal bounds delays by min and max.
#[then(regex = r"^every computed delay is at most (\d+) ms$")]
fn every_delay_is_at_most(world: &mut ClientWorld, millis: u64) {
    let limit = Duration::from_millis(millis);
    let min = backoff_config(world).min_reconnect_delay;
    assert!(!world.computed_delays.is_empty(), "no delays computed");
    let out: Vec<_> = world
        .computed_delays
        .iter()
        .filter(|delay| **delay > limit || **delay < min)
        .collect();
    assert!(
        out.is_empty(),
        "delays outside {min:?}..={limit:?}: {out:?}"
    );
}

#[then("the upper bound of the delay for attempt n+1 is greater than for attempt n")]
fn upper_bound_grows(world: &mut ClientWorld) {
    let max = backoff_config(world).max_reconnect_delay;
    let bounds = &world.upper_bounds;
    for n in 0..bounds.len() - 1 {
        if bounds[n] == max {
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
        .position(|bound| *bound == max)
        .unwrap_or_else(|| panic!("the bound never reached {max:?}: {bounds:?}"));
    assert!(
        bounds[first..].iter().all(|bound| *bound == max),
        "{bounds:?}"
    );
}

/// Also checks that the jitter is full: delays spread over the whole range
/// `[min, min + min * 2^attempt]` (Block 2 VERIFY), so a delay that only
/// takes a few values fails even though not all of them are equal.
#[then("the computed delays are not all equal")]
fn delays_not_all_equal(world: &mut ClientWorld) {
    let delays = &world.computed_delays;
    let first = delays.first().expect("delays computed");
    assert!(
        delays.iter().any(|delay| delay != first),
        "all {} delays equal {first:?}",
        delays.len()
    );

    let config = backoff_config(world);
    let low = millis(config.min_reconnect_delay);
    let high = millis(config.max_reconnect_delay).min(low + low * (1 << JITTER_ATTEMPT));
    let width = (high - low).div_ceil(JITTER_BUCKETS as u64);
    let mut buckets = [0usize; JITTER_BUCKETS];
    for delay in delays {
        let delay = millis(*delay);
        assert!(
            (low..=high).contains(&delay),
            "{delay} ms outside the jitter range {low}..={high} ms"
        );
        // The range is inclusive: its upper end belongs to the last bucket.
        let bucket = usize::try_from((delay - low) / width)
            .expect("small index")
            .min(JITTER_BUCKETS - 1);
        buckets[bucket] += 1;
    }
    assert!(
        buckets.iter().all(|count| *count > 0),
        "delays do not cover {low}..={high} ms: {buckets:?}"
    );
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("delays fit in u64 ms")
}
