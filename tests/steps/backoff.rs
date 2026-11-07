//! Steps of Block 2, reconnect backoff, and of Block 10, resubscribe backoff
//! (@E), which apply the same checks.
//!
//! The sans-IO core is the controlled clock: a step fails connection or
//! subscribe attempts and reads the delays the machine schedules, without
//! network or real time.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::Config;
use ferrifuge::machine::{
    Action, Input, Machine, SubscriptionId, SubscriptionOptions, SubscriptionTimer, Timer,
    TransportId,
};

use super::ClientWorld;

/// Samples taken to estimate the upper bound of an attempt's delay. Once an
/// attempt is capped, a delay lands exactly on the maximum with probability
/// about 1/40 (min / max of the jitter range), so the largest of 1000
/// samples is the maximum itself, short of a 1e-11 chance.
const SAMPLES: usize = 1000;
/// Attempts examined for the growth of the upper bound.
const ATTEMPTS: usize = 12;
/// Attempts whose delays the jitter scenario samples: one before the cap
/// and one well past it (attempt 6 reaches 20 s with the default options),
/// where jitter matters most for long outages.
const JITTER_ATTEMPTS: [usize; 2] = [3, 10];
/// The jitter range is split into this many equal parts, each of which must
/// receive samples: full jitter spreads delays over the whole range.
const JITTER_BUCKETS: usize = 10;
/// An estimated bound may fall short of the true one by 1/50 of the jitter
/// range: with 1000 uniform samples that happens with a chance near 1e-9.
const BOUND_SLACK_DIVISOR: u64 = 50;

/// Reconnect delays scheduled for `attempts` consecutive failed attempts.
fn reconnect_delays(config: &Config, attempts: usize) -> Vec<Duration> {
    let mut machine = Machine::new(config.clone());
    let mut actions = machine.handle(Input::Connect);
    let mut delays = Vec::with_capacity(attempts);
    for _ in 0..attempts {
        let transport = actions
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
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

/// The backoff a scenario examines.
#[derive(Clone)]
pub(crate) enum UnderTest {
    /// Reconnects of a client (Block 2).
    Reconnect(Config),
    /// Resubscribes of a subscription (Block 10).
    Resubscribe(SubscriptionOptions),
}

impl UnderTest {
    /// Lower and upper bound of the delays.
    fn bounds(&self) -> (Duration, Duration) {
        match self {
            Self::Reconnect(config) => (config.min_reconnect_delay, config.max_reconnect_delay),
            Self::Resubscribe(options) => {
                (options.min_resubscribe_delay, options.max_resubscribe_delay)
            }
        }
    }

    /// Delays scheduled for `attempts` consecutive failed attempts.
    fn delays(&self, attempts: usize) -> Vec<Duration> {
        match self {
            Self::Reconnect(config) => reconnect_delays(config, attempts),
            Self::Resubscribe(options) => {
                let mut run = ResubscribeRun::new(options.clone());
                (0..attempts).map(|_| run.fail()).collect()
            }
        }
    }

    /// The delay of 0-based `attempt`, sampled `times` times.
    fn attempt_delays(&self, attempt: usize, times: usize) -> Vec<Duration> {
        (0..times)
            .map(|_| self.delays(attempt + 1)[attempt])
            .collect()
    }
}

fn under_test(world: &ClientWorld) -> &UnderTest {
    world
        .backoff
        .as_ref()
        .expect("a client or subscription with backoff options")
}

/// A connected machine with a subscription to "news" whose subscribe
/// commands a step answers (Block 10).
pub(crate) struct ResubscribeRun {
    machine: Machine,
    transport: TransportId,
    id: SubscriptionId,
    /// Id of the subscribe command awaiting its answer.
    pending: Option<u64>,
}

impl ResubscribeRun {
    fn new(options: SubscriptionOptions) -> Self {
        let mut machine = Machine::new(Config::default());
        let actions = machine.handle(Input::Connect);
        let transport = opened(&actions);
        let id = machine
            .add_subscription("news".to_owned(), options)
            .expect("a new channel");
        let mut run = Self {
            machine,
            transport,
            id,
            pending: None,
        };
        run.connect();
        let actions = run.machine.handle(Input::Subscribe(run.id));
        run.note_subscribe(&actions);
        run
    }

    /// Opens the transport and answers the connect command.
    fn connect(&mut self) {
        let actions = self.machine.handle(Input::TransportOpened(self.transport));
        let connect = command_ids(&actions, "connect")
            .pop()
            .expect("a connect command");
        let actions = self.reply(&format!(r#"{{"id":{connect},"connect":{{"client":"c"}}}}"#));
        self.note_subscribe(&actions);
    }

    fn reply(&mut self, json: &str) -> Vec<Action> {
        self.machine.handle(Input::Frame {
            transport: self.transport,
            data: json.as_bytes().to_vec(),
        })
    }

    fn note_subscribe(&mut self, actions: &[Action]) {
        if let Some(id) = command_ids(actions, "subscribe").pop() {
            self.pending = Some(id);
        }
    }

    /// Answers the pending subscribe command with a temporary error, and
    /// returns the resubscribe delay scheduled; the next attempt follows.
    fn fail(&mut self) -> Duration {
        let id = self.pending.take().expect("a subscribe command in flight");
        let actions = self.reply(&format!(
            r#"{{"id":{id},"error":{{"code":100,"message":"busy","temporary":true}}}}"#
        ));
        let delay = actions
            .iter()
            .find_map(|action| match action {
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, delay) => {
                    Some(*delay)
                }
                _ => None,
            })
            .expect("a failed subscribe schedules a resubscribe");
        let actions = self.machine.handle(Input::SubscriptionTimerFired(
            self.id,
            SubscriptionTimer::Resubscribe,
        ));
        self.note_subscribe(&actions);
        delay
    }

    /// Answers the pending subscribe command with delta on, then pushes a
    /// whole publication and a delta whose checksum is wrong (Block 19);
    /// answers the unsubscribe command the failure sends and returns the
    /// resubscribe delay scheduled; the next attempt follows.
    fn fail_with_broken_delta(&mut self) -> Duration {
        let id = self.pending.take().expect("a subscribe command in flight");
        self.reply(&format!(r#"{{"id":{id},"subscribe":{{"delta":true}}}}"#));
        self.reply(r#"{"push":{"channel":"news","pub":{"data":"{}"}}}"#);
        let actions =
            self.reply(r#"{"push":{"channel":"news","pub":{"data":"2\n2:{}0;","delta":true}}}"#);
        let delay = actions
            .iter()
            .find_map(|action| match action {
                Action::SetSubscriptionTimer(_, SubscriptionTimer::Resubscribe, delay) => {
                    Some(*delay)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("a broken delta schedules a resubscribe: {actions:?}"));
        let unsubscribe = command_ids(&actions, "unsubscribe")
            .pop()
            .expect("an unsubscribe command");
        self.reply(&format!(r#"{{"id":{unsubscribe},"unsubscribe":{{}}}}"#));
        let actions = self.machine.handle(Input::SubscriptionTimerFired(
            self.id,
            SubscriptionTimer::Resubscribe,
        ));
        self.note_subscribe(&actions);
        delay
    }

    /// Answers the pending subscribe command with success.
    fn succeed(&mut self) {
        let id = self.pending.take().expect("a subscribe command in flight");
        self.reply(&format!(r#"{{"id":{id},"subscribe":{{}}}}"#));
    }

    /// The connection is lost and established again: the subscription
    /// subscribes again on the new one.
    fn reconnect(&mut self) {
        self.machine.handle(Input::TransportClosed {
            transport: self.transport,
            code: None,
            reason: "lost".to_owned(),
        });
        let actions = self.machine.handle(Input::TimerFired(Timer::Reconnect));
        self.transport = opened(&actions);
        self.connect();
    }
}

fn opened(actions: &[Action]) -> TransportId {
    actions
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        })
        .expect("a transport to open")
}

/// Ids of the `method` commands sent among `actions`.
pub(crate) fn command_ids(actions: &[Action], method: &str) -> Vec<u64> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Send { frame, .. } => Some(frame),
            _ => None,
        })
        .flat_map(|frame| frame_ids(frame, method))
        .collect()
}

/// Ids of the `method` commands in a JSON `frame`, unwrapping an emulation
/// request, which carries its commands as text in `data`.
pub(crate) fn frame_ids(frame: &[u8], method: &str) -> Vec<u64> {
    std::str::from_utf8(frame)
        .expect("JSON")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON"))
        .flat_map(
            |value| match value.get("data").and_then(|data| data.as_str()) {
                Some(data) if value.get("session").is_some() => frame_ids(data.as_bytes(), method),
                _ => value
                    .get(method)
                    .map(|_| value["id"].as_u64().expect("an id"))
                    .into_iter()
                    .collect(),
            },
        )
        .collect()
}

#[given(regex = r"^a client with minReconnectDelay (\d+) ms and maxReconnectDelay (\d+) ms$")]
fn a_client_with_backoff_options(world: &mut ClientWorld, min: u64, max: u64) {
    world.backoff = Some(UnderTest::Reconnect(
        Config::default()
            .min_reconnect_delay(Duration::from_millis(min))
            .max_reconnect_delay(Duration::from_millis(max)),
    ));
}

#[given("a client with fixed backoff options")]
fn a_client_with_fixed_backoff_options(world: &mut ClientWorld) {
    world.backoff = Some(UnderTest::Reconnect(Config::default()));
}

#[given(
    regex = r"^a subscription with minResubscribeDelay (\d+) ms and maxResubscribeDelay (\d+) ms$"
)]
fn a_subscription_with_backoff_options(world: &mut ClientWorld, min: u64, max: u64) {
    world.backoff = Some(UnderTest::Resubscribe(
        SubscriptionOptions::default()
            .min_resubscribe_delay(Duration::from_millis(min))
            .max_resubscribe_delay(Duration::from_millis(max)),
    ));
}

#[given("a subscription with fixed resubscribe backoff options")]
fn a_subscription_with_fixed_backoff_options(world: &mut ClientWorld) {
    world.backoff = Some(UnderTest::Resubscribe(SubscriptionOptions::default()));
}

#[when(regex = r"^(\d+) consecutive reconnect attempts fail$")]
fn consecutive_attempts_fail(world: &mut ClientWorld, attempts: usize) {
    world.computed_delays = under_test(world).delays(attempts);
    world.delays_by_attempt = true;
}

/// Keeps the machine: a later step of the scenario continues with it.
#[when(regex = r"^(\d+) consecutive subscribe attempts fail with a temporary error$")]
fn consecutive_subscribe_attempts_fail(world: &mut ClientWorld, attempts: usize) {
    let UnderTest::Resubscribe(options) = under_test(world).clone() else {
        panic!("this step needs a subscription with resubscribe options");
    };
    let mut run = ResubscribeRun::new(options);
    world.computed_delays = (0..attempts).map(|_| run.fail()).collect();
    world.delays_by_attempt = true;
    world.resubscribe_run = Some(run);
}

#[when(
    regex = r"^consecutive (?:reconnect attempts fail|subscribe attempts fail with a temporary error)$"
)]
fn attempts_keep_failing(world: &mut ClientWorld) {
    let under_test = under_test(world).clone();
    world.upper_bounds = (0..ATTEMPTS)
        .map(|attempt| {
            under_test
                .attempt_delays(attempt, SAMPLES)
                .into_iter()
                .max()
                .expect("samples")
        })
        .collect();
}

#[when(regex = r"^the delay for attempt (\d+) is computed (\d+) times$")]
fn delay_for_attempt_computed(world: &mut ClientWorld, attempt: usize, times: usize) {
    world.computed_delays = under_test(world).attempt_delays(attempt, times);
    world.delays_by_attempt = false;
}

#[when(regex = r"^the (?:resubscribe )?delay for the same attempt number is computed (\d+) times$")]
fn delay_for_same_attempt_computed(world: &mut ClientWorld, times: usize) {
    let under_test = under_test(world).clone();
    world.jitter_samples = JITTER_ATTEMPTS
        .iter()
        .map(|attempt| (*attempt, under_test.attempt_delays(*attempt, times)))
        .collect();
}

#[when("the next subscribe attempt succeeds")]
fn the_next_subscribe_attempt_succeeds(world: &mut ClientWorld) {
    resubscribe_run(world).succeed();
}

/// After a reconnect, as a subscribed subscription subscribes again then.
#[when("a later subscribe attempt fails with a temporary error")]
fn a_later_subscribe_attempt_fails(world: &mut ClientWorld) {
    let run = resubscribe_run(world);
    run.reconnect();
    world.last_delay = Some(run.fail());
}

fn resubscribe_run(world: &mut ClientWorld) -> &mut ResubscribeRun {
    world
        .resubscribe_run
        .as_mut()
        .expect("subscribe attempts made by an earlier step")
}

#[then(regex = r"^the delay after that failure is at most (\d+) ms$")]
fn the_delay_after_that_failure(world: &mut ClientWorld, millis: u64) {
    let delay = world.last_delay.expect("a failed attempt");
    assert!(delay <= Duration::from_millis(millis), "{delay:?}");
}

/// Also checks the lower bound, as the Goal bounds delays by min and max,
/// and, for delays of consecutive attempts, each attempt's own bound: with
/// a configured minimum this shows the option is applied (the first delay
/// cannot exceed twice the minimum).
#[then(regex = r"^every computed delay is at most (\d+) ms$")]
fn every_delay_is_at_most(world: &mut ClientWorld, millis: u64) {
    let limit = Duration::from_millis(millis);
    let (min, max) = under_test(world).bounds();
    if world.delays_by_attempt {
        for (attempt, delay) in world.computed_delays.iter().enumerate() {
            let bound = expected_bound(min, max, attempt);
            assert!(
                *delay <= bound,
                "attempt {attempt}: delay {delay:?} above its bound {bound:?}"
            );
        }
    }
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

/// The scenario promises exponential growth: besides growing, each attempt's
/// estimated bound must match the verified formula
/// `min(max, min + min * 2^n)` (Block 2 VERIFY), within the sampling slack.
#[then("the upper bound of the delay for attempt n+1 is greater than for attempt n")]
fn upper_bound_grows(world: &mut ClientWorld) {
    let (min, max) = under_test(world).bounds();
    let bounds = &world.upper_bounds;
    for (n, bound) in bounds.iter().enumerate() {
        let expected = expected_bound(min, max, n);
        let slack = (millis(expected) - millis(min)) / BOUND_SLACK_DIVISOR;
        assert!(
            millis(*bound) <= millis(expected) && millis(*bound) + slack >= millis(expected),
            "attempt {n}: bound {bound:?}, expected {expected:?}: {bounds:?}"
        );
    }
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
    let (min, max) = under_test(world).bounds();
    assert!(!world.jitter_samples.is_empty(), "no delays computed");
    for (attempt, delays) in &world.jitter_samples {
        assert_full_jitter(min, max, *attempt, delays);
    }
}

fn assert_full_jitter(min: Duration, max: Duration, attempt: usize, delays: &[Duration]) {
    let first = delays.first().expect("delays computed");
    assert!(
        delays.iter().any(|delay| delay != first),
        "attempt {attempt}: all {} delays equal {first:?}",
        delays.len()
    );

    let low = millis(min);
    let high = millis(expected_bound(min, max, attempt));
    let width = (high - low).div_ceil(JITTER_BUCKETS as u64);
    let mut buckets = [0usize; JITTER_BUCKETS];
    for delay in delays {
        let delay = millis(*delay);
        assert!(
            (low..=high).contains(&delay),
            "attempt {attempt}: {delay} ms outside the jitter range {low}..={high} ms"
        );
        // The range is inclusive: its upper end belongs to the last bucket.
        let bucket = usize::try_from((delay - low) / width)
            .expect("small index")
            .min(JITTER_BUCKETS - 1);
        buckets[bucket] += 1;
    }
    assert!(
        buckets.iter().all(|count| *count > 0),
        "attempt {attempt}: delays do not cover {low}..={high} ms: {buckets:?}"
    );
}

/// Upper bound of the delay for 0-based `attempt` (Block 2 VERIFY).
fn expected_bound(min: Duration, max: Duration, attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt).expect("small attempt").min(31);
    max.min(min + min * (1 << exponent))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("delays fit in u64 ms")
}

#[given(
    regex = r"^a subscription with the fossil delta option, min_resubscribe_delay (\d+) ms and max_resubscribe_delay (\d+) ms$"
)]
fn a_delta_subscription_with_resubscribe_options(world: &mut ClientWorld, min: u64, max: u64) {
    let options = SubscriptionOptions::default()
        .delta(ferrifuge::DeltaFormat::Fossil)
        .min_resubscribe_delay(Duration::from_millis(min))
        .max_resubscribe_delay(Duration::from_millis(max));
    world.backoff = Some(UnderTest::Resubscribe(options));
}

/// Block 19: each subscribe is followed by a broken delta, whose
/// resubscribe must not reset the backoff (D8). The bound of attempt `n` is
/// the largest delay of [`SAMPLES`] runs reaching it.
#[when(
    regex = r"^each of (\d+) subscribes is answered with delta on, a whole publication and then a broken delta$"
)]
async fn subscribes_followed_by_broken_deltas(world: &mut ClientWorld, subscribes: usize) {
    let UnderTest::Resubscribe(options) = under_test(world).clone() else {
        panic!("this step needs a subscription with resubscribe options");
    };
    // Each run gives one sample of every attempt's delay. Off the async
    // runtime, so that the scenarios running alongside keep their timing.
    world.upper_bounds = tokio::task::spawn_blocking(move || {
        let mut bounds = vec![Duration::ZERO; subscribes];
        for _ in 0..SAMPLES {
            let mut run = ResubscribeRun::new(options.clone());
            for bound in &mut bounds {
                *bound = (*bound).max(run.fail_with_broken_delta());
            }
        }
        bounds
    })
    .await
    .expect("the sampling task");
}

#[then(
    regex = r"^the upper bound of each resubscribe delay is greater than the previous one until it reaches (\d+) ms$"
)]
fn resubscribe_bounds_grow_until(world: &mut ClientWorld, millis_max: u64) {
    upper_bound_grows(world);
    upper_bound_stops_growing(world, millis_max);
}
