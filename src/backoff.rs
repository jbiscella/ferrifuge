//! Reconnect delays: exponential backoff with full jitter.
//!
//! Port of `backoff` in centrifuge-js `src/utils.ts`:
//! `min(max, min + rand(0, min(max, min * 2^step)))`, `rand` inclusive.

use std::time::Duration;

/// Exponent cap, as in centrifuge-js: beyond it the bound is `max` anyway.
const MAX_STEP: u32 = 31;

/// Attempt counter of a backoff sequence and its bounds.
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    min: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub(crate) fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            attempt: 0,
        }
    }

    /// Delay before the next attempt. Each call counts as one attempt.
    pub(crate) fn next_delay(&mut self, rng: &mut fastrand::Rng) -> Duration {
        let delay = delay(self.attempt, self.min, self.max, rng);
        self.attempt = self.attempt.saturating_add(1);
        delay
    }

    /// Starts the sequence over, after a connection proved usable.
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Delay for 0-based attempt `step`; never below `min` nor above `max`.
pub(crate) fn delay(step: u32, min: Duration, max: Duration, rng: &mut fastrand::Rng) -> Duration {
    let (min, max) = (millis(min), millis(max));
    let ceiling = max.min(min.saturating_mul(1 << step.min(MAX_STEP)));
    Duration::from_millis(max.min(min.saturating_add(rng.u64(0..=ceiling))))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: Duration = Duration::from_millis(500);
    const MAX: Duration = Duration::from_millis(20_000);

    #[test]
    fn delays_stay_within_min_and_max() {
        let mut rng = fastrand::Rng::with_seed(7);
        let mut backoff = Backoff::new(MIN, MAX);

        let delays: Vec<_> = (0..100).map(|_| backoff.next_delay(&mut rng)).collect();

        assert!(delays.iter().all(|d| (MIN..=MAX).contains(d)), "{delays:?}");
    }

    #[test]
    fn first_attempt_is_bounded_by_twice_min() {
        let mut rng = fastrand::Rng::with_seed(7);

        let delays: Vec<_> = (0..1000).map(|_| delay(0, MIN, MAX, &mut rng)).collect();

        assert!(delays.iter().all(|d| *d <= 2 * MIN), "{delays:?}");
    }

    #[test]
    fn same_attempt_is_jittered() {
        let mut rng = fastrand::Rng::with_seed(7);

        let first = delay(3, MIN, MAX, &mut rng);
        let varies = (0..1000).any(|_| delay(3, MIN, MAX, &mut rng) != first);

        assert!(varies);
    }

    #[test]
    fn huge_step_saturates_at_max_without_overflow() {
        let mut rng = fastrand::Rng::with_seed(7);

        let delays: Vec<_> = (0..1000)
            .map(|_| delay(u32::MAX, Duration::MAX, Duration::MAX, &mut rng))
            .collect();

        assert!(delays.iter().all(|d| *d == Duration::from_millis(u64::MAX)));
    }

    #[test]
    fn reset_starts_the_sequence_over() {
        let mut rng = fastrand::Rng::with_seed(7);
        let mut backoff = Backoff::new(MIN, MAX);
        for _ in 0..10 {
            backoff.next_delay(&mut rng);
        }

        backoff.reset();
        let after_reset: Vec<_> = (0..1000)
            .map(|_| {
                let mut fresh = backoff.clone();
                fresh.next_delay(&mut rng)
            })
            .collect();

        assert!(after_reset.iter().all(|d| *d <= 2 * MIN), "{after_reset:?}");
    }
}
