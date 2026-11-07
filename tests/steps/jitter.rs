//! Steps of Block 14: the reconnect jitter window (@E).
//!
//! The sans-IO core is the controlled clock: a step reads the delays the
//! machine schedules before its connection attempts.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::Config;
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportId};

use super::ClientWorld;

/// The machine of a jitter scenario, and what it did last.
pub(crate) struct JitterRun {
    machine: Machine,
    config: Config,
    transport: Option<TransportId>,
    /// Actions of the last input.
    actions: Vec<Action>,
}

impl JitterRun {
    fn new(window: Duration) -> Self {
        let config = Config::default().reconnect_jitter_window(window);
        Self {
            machine: Machine::new(config.clone()),
            config,
            transport: None,
            actions: Vec::new(),
        }
    }

    fn handle(&mut self, input: Input) {
        self.actions = self.machine.handle(input);
        if let Some(transport) = self.actions.iter().find_map(|action| match action {
            Action::OpenTransport { transport, .. } => Some(*transport),
            _ => None,
        }) {
            self.transport = Some(transport);
        }
    }

    /// Connects: opens the transport and answers the connect command.
    fn connect(&mut self) {
        self.handle(Input::Connect);
        let transport = self.transport.expect("a transport to open");
        self.handle(Input::TransportOpened(transport));
        let connect_id = self
            .actions
            .iter()
            .find_map(|action| match action {
                Action::Send { frame, .. } => {
                    let command: serde_json::Value =
                        serde_json::from_slice(frame).expect("a JSON command");
                    command["id"].as_u64()
                }
                _ => None,
            })
            .expect("a connect command");
        self.handle(Input::Frame {
            transport,
            data: format!(r#"{{"id":{connect_id},"connect":{{"client":"c"}}}}"#).into_bytes(),
        });
    }

    fn reconnect_delay(&self) -> Option<Duration> {
        self.actions.iter().find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
    }

    /// The last input opened a transport at once, without a delay first.
    fn opened_at_once(&self) -> bool {
        self.reconnect_delay().is_none()
            && self
                .actions
                .iter()
                .any(|action| matches!(action, Action::OpenTransport { .. }))
    }
}

fn run(world: &mut ClientWorld) -> &mut JitterRun {
    world
        .jitter
        .as_mut()
        .expect("a client with a reconnect jitter window")
}

/// Whether `connect is called` and `disconnect is called` drive the jitter
/// scenario's machine rather than a client.
pub(super) fn drives_a_machine(world: &ClientWorld) -> bool {
    world.jitter.is_some()
}

pub(super) fn connect(world: &mut ClientWorld) {
    run(world).handle(Input::Connect);
}

pub(super) fn disconnect(world: &mut ClientWorld) {
    run(world).handle(Input::Disconnect);
}

#[given(regex = r"^a client with a reconnect jitter window of (\d+)(?: s)?$")]
fn a_client_with_a_jitter_window(world: &mut ClientWorld, seconds: u64) {
    world.jitter = Some(JitterRun::new(Duration::from_secs(seconds)));
}

#[given(regex = r"^a connected client with a reconnect jitter window of (\d+) s$")]
fn a_connected_client_with_a_jitter_window(world: &mut ClientWorld, seconds: u64) {
    a_client_with_a_jitter_window(world, seconds);
    run(world).connect();
}

#[when("the connection is lost with a reconnect-advised code")]
fn the_connection_is_lost(world: &mut ClientWorld) {
    let run = run(world);
    run.connect();
    let transport = run.transport.expect("an open transport");
    // A disconnect code from the server that advises reconnecting (Block 3).
    run.handle(Input::TransportClosed {
        transport,
        code: Some(3000),
        reason: "lost".to_owned(),
    });
}

#[then("the first reconnect delay is within the Block 2 bound for attempt 0")]
fn the_first_reconnect_delay_is_within_the_backoff_bound(world: &mut ClientWorld) {
    let run = run(world);
    let delay = run.reconnect_delay().expect("a reconnect is scheduled");
    // Block 2: attempt 0 waits between the minimum and twice it.
    let min = run.config.min_reconnect_delay;
    assert!(delay >= min && delay <= min * 2, "{delay:?}");
}

#[then(regex = r"^the (?:first )?connect attempt is made without the jitter delay$")]
fn the_connect_attempt_is_made_at_once(world: &mut ClientWorld) {
    let run = run(world);
    assert!(run.opened_at_once(), "{:?}", run.actions);
}
