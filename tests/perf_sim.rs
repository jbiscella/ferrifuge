//! Simulations of the sans-IO core on a controlled clock (Block 14, `@P`):
//! no network, no real time, deterministic.

// @P scenario: An idle client wakes only for pings and refreshes
// @P scenario: Reconnects of a fleet are spread over the jitter window
// @P scenario: Token refreshes of a fleet are spread

use std::collections::VecDeque;
use std::time::Duration;

use ferrifuge::machine::{Action, Input, Machine, SubscriptionOptions, Timer, TransportId};
use ferrifuge::{Config, State, SubscriptionState};
use serde_json::Value;

/// The client side of a simulated server: answers what the machine sends
/// and keeps its timers on a simulated clock.
struct Simulation {
    machine: Machine,
    /// Simulated time since the start.
    now: Duration,
    transport: Option<TransportId>,
    /// When each running timer fires, by [`Timer::index`].
    timers: [Option<Duration>; Timer::COUNT],
    /// What the server and the token callback answer, in order.
    pending: VecDeque<Input>,
    /// Ttl the server gives connection tokens, in seconds.
    token_ttl: u32,
    /// Server ping interval, in seconds.
    ping: u32,
    /// Every timer set, with when.
    set: Vec<(Duration, Timer)>,
    /// Every timer fired, with when.
    fired: Vec<(Duration, Timer)>,
    /// Whether any subscription timer was set.
    subscription_timers_set: usize,
}

impl Simulation {
    /// A machine seeded with `seed`, whose server gives tokens `token_ttl`
    /// seconds and pings every `ping` seconds (0 for no pings).
    fn new(config: Config, seed: u64, token_ttl: u32, ping: u32) -> Self {
        Self {
            machine: Machine::with_token_source(config).with_seed(seed),
            now: Duration::ZERO,
            transport: None,
            timers: [None; Timer::COUNT],
            pending: VecDeque::new(),
            token_ttl,
            ping,
            set: Vec::new(),
            fired: Vec::new(),
            subscription_timers_set: 0,
        }
    }

    /// Feeds `input`, then everything the server and the token callback
    /// answer at once, until the machine waits for a timer.
    fn handle(&mut self, input: Input) {
        self.pending.push_back(input);
        while let Some(input) = self.pending.pop_front() {
            for action in self.machine.handle(input) {
                self.act(action);
            }
        }
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::OpenTransport { transport, .. } => {
                self.transport = Some(transport);
                self.pending.push_back(Input::TransportOpened(transport));
            }
            Action::Send { frame, .. } => {
                for command in String::from_utf8(frame).expect("JSON").lines() {
                    self.answer(command);
                }
            }
            Action::SetTimer(timer, delay) => {
                self.timers[timer.index()] = Some(self.now + delay);
                self.set.push((self.now, timer));
            }
            Action::CancelTimer(timer) => self.timers[timer.index()] = None,
            Action::SetSubscriptionTimer(..) => self.subscription_timers_set += 1,
            Action::FetchToken(fetch) => self.pending.push_back(Input::TokenFetched {
                fetch,
                result: Ok("token".to_owned()),
            }),
            _ => {}
        }
    }

    fn answer(&mut self, command: &str) {
        let command: Value = serde_json::from_str(command).expect("a JSON command");
        let id = &command["id"];
        let ttl = self.token_ttl;
        let reply = if command.get("connect").is_some() {
            format!(
                r#"{{"id":{id},"connect":{{"client":"c","expires":true,"ttl":{ttl},"ping":{},"pong":true}}}}"#,
                self.ping
            )
        } else if command.get("subscribe").is_some() {
            format!(r#"{{"id":{id},"subscribe":{{}}}}"#)
        } else if command.get("refresh").is_some() {
            format!(r#"{{"id":{id},"refresh":{{"expires":true,"ttl":{ttl}}}}}"#)
        } else {
            // A pong.
            return;
        };
        self.deliver(reply);
    }

    /// Queues `frame` from the server for the machine.
    fn deliver(&mut self, frame: String) {
        let transport = self.transport.expect("an open transport");
        self.pending.push_back(Input::Frame {
            transport,
            data: frame.into_bytes(),
        });
    }

    /// Runs until `until`, delivering a server ping every ping interval
    /// and firing timers when due.
    fn run_until(&mut self, until: Duration) {
        let interval = Duration::from_secs(self.ping.into());
        let mut next_ping = self.now + interval;
        loop {
            let next_timer = Timer::ALL
                .into_iter()
                .filter_map(|timer| self.timers[timer.index()].map(|at| (at, timer)))
                .min_by_key(|(at, _)| *at);
            let next = match next_timer {
                Some((at, _)) if at <= next_ping => at,
                _ => next_ping,
            };
            if next > until {
                self.now = until;
                return;
            }
            self.now = next;
            match next_timer {
                Some((at, timer)) if at == next => {
                    self.timers[timer.index()] = None;
                    self.fired.push((self.now, timer));
                    self.handle(Input::TimerFired(timer));
                }
                _ => {
                    next_ping += interval;
                    // A server ping: an empty reply.
                    self.deliver("{}".to_owned());
                    let ping = self.pending.pop_back().expect("the ping");
                    self.handle(ping);
                }
            }
        }
    }
}

#[test]
fn an_idle_client_wakes_only_for_pings_and_refreshes() {
    const PING: u32 = 25;
    let mut simulation = Simulation::new(Config::default(), 1, 60, PING);
    simulation.handle(Input::Connect);
    let mut subscriptions = Vec::new();
    for index in 0..10 {
        let id = simulation
            .machine
            .add_subscription(format!("ch{index}"), SubscriptionOptions::default())
            .expect("a new channel");
        simulation.handle(Input::Subscribe(id));
        subscriptions.push(id);
    }
    let idle_from = simulation.now;
    simulation.set.clear();
    simulation.fired.clear();
    simulation.subscription_timers_set = 0;

    simulation.run_until(idle_from + Duration::from_secs(10 * 60));

    // The simulation did what the scenario says: connected throughout,
    // subscribed, pinged, refreshing.
    assert_eq!(simulation.machine.state(), State::Connected);
    assert!(subscriptions.iter().all(|id| {
        simulation.machine.subscription_state(*id) == Some(SubscriptionState::Subscribed)
    }));
    let count = |kind: Timer, events: &[(Duration, Timer)]| {
        events.iter().filter(|(_, timer)| *timer == kind).count()
    };
    // Re-armed by any data from the server: each ping, each refresh reply.
    assert!(count(Timer::ServerPing, &simulation.set) >= 24);
    assert!(count(Timer::Refresh, &simulation.fired) >= 10);

    let kinds: Vec<_> = simulation.set.iter().map(|(_, timer)| *timer).collect();
    assert!(
        kinds.iter().all(|timer| matches!(
            timer,
            Timer::ServerPing | Timer::Refresh | Timer::RefreshReply
        )),
        "{kinds:?}"
    );
    assert_eq!(simulation.subscription_timers_set, 0);
    let fires: Vec<_> = simulation.fired.iter().map(|(at, _)| *at).collect();
    assert!(!fires.is_empty(), "the token was never refreshed");
    for pair in fires.windows(2) {
        assert!(
            pair[1].saturating_sub(pair[0]) >= Duration::from_secs(PING.into()),
            "timers fired at {:?}",
            simulation.fired
        );
    }
}

/// Clients in a simulated fleet, each seeded differently.
const FLEET: u64 = 100_000;

/// When `timer` of a connected client of the fleet fires, after `then`.
fn fleet_delays(
    config: &Config,
    token_ttl: u32,
    then: impl Fn(&mut Simulation),
    timer: Timer,
) -> Vec<Duration> {
    (0..FLEET)
        .map(|seed| {
            let mut simulation = Simulation::new(config.clone(), seed, token_ttl, 0);
            simulation.handle(Input::Connect);
            assert_eq!(simulation.machine.state(), State::Connected);
            then(&mut simulation);
            simulation.timers[timer.index()]
                .expect("the timer is set")
                .saturating_sub(simulation.now)
        })
        .collect()
}

#[test]
fn reconnects_of_a_fleet_are_spread_over_the_jitter_window() {
    const WINDOW: Duration = Duration::from_secs(100);
    let config = Config::default().reconnect_jitter_window(WINDOW);

    // A disconnect code from the server that advises reconnecting (Block 3).
    let lose_connection = |simulation: &mut Simulation| {
        let transport = simulation.transport.expect("an open transport");
        simulation.handle(Input::TransportClosed {
            transport,
            code: Some(3000),
            reason: "lost".to_owned(),
        });
    };
    let delays = fleet_delays(&config, 60, lose_connection, Timer::Reconnect);

    // The window plus the Block 2 delay of attempt 0, at most 1 s.
    let mut per_second = vec![0_u64; usize::try_from(WINDOW.as_secs()).unwrap() + 2];
    for delay in &delays {
        per_second[usize::try_from(delay.as_secs()).unwrap()] += 1;
    }
    let busiest = per_second.iter().copied().max().unwrap_or_default();
    println!("@P report: busiest second of the window: {busiest} of {FLEET} first reconnects");
    assert!(busiest * 50 <= FLEET, "{per_second:?}");
}

#[test]
fn token_refreshes_of_a_fleet_are_spread() {
    const TTL: u32 = 60;
    let ttl = Duration::from_secs(TTL.into());

    let delays = fleet_delays(&Config::default(), TTL, |_| {}, Timer::Refresh);

    let first = delays.iter().min().copied().unwrap_or_default();
    let last = delays.iter().max().copied().unwrap_or_default();
    let span = last.saturating_sub(first);
    println!(
        "@P report: refreshes from {first:?} to {last:?} of a {ttl:?} ttl ({:.2}% of it)",
        span.as_secs_f64() * 100.0 / ttl.as_secs_f64()
    );
    // At least 9.9%: a finite sample of [80%, 90%] reaches the full 10% only
    // when it draws both ends.
    assert!(span * 1000 >= ttl * 99, "a span of {span:?}");
    // Intervals of 1% of the ttl over the span, the last one closed.
    let interval = ttl / 100;
    let intervals = usize::try_from(span.as_nanos().div_ceil(interval.as_nanos()))
        .unwrap()
        .max(1);
    let mut counts = vec![0_u64; intervals];
    for delay in &delays {
        let index =
            usize::try_from(delay.saturating_sub(first).as_nanos() / interval.as_nanos()).unwrap();
        counts[index.min(intervals - 1)] += 1;
    }
    let busiest = counts.iter().copied().max().unwrap_or_default();
    // At most twice the average share: busiest / FLEET <= 2 / intervals.
    assert!(
        busiest * intervals as u64 <= 2 * FLEET,
        "{counts:?} over {intervals} intervals"
    );
}
