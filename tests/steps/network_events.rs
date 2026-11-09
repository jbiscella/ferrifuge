//! Steps of Block 44 D10 and D11: going offline, coming online and the
//! back/forward cache, on the sans-IO core (@E). The machine runs on the
//! simulated clock of Block 18's steps, so that the shared steps reading
//! its actions apply.

use std::time::Duration;

use cucumber::{given, then, when};
use ferrifuge::Config;
use ferrifuge::machine::{Action, Input, Machine, Timer, TransportKind};

use super::ClientWorld;
use super::batching::{BatchRun, Key, connected_run};

/// What a Block 44c @E scenario holds.
#[derive(Default)]
pub(crate) struct NetworkEvents {
    /// The machine's configuration.
    config: Config,
}

/// A machine built from `config`, kept for the steps that check delays.
fn drive(world: &mut ClientWorld, config: Config, run: BatchRun) {
    world.network_events.config = config;
    world.batching.run = Some(run);
}

fn run(world: &mut ClientWorld) -> &mut BatchRun {
    world.batching.run.as_mut().expect("a Block 44 machine")
}

/// The actions since the step under test started.
fn since(world: &mut ClientWorld) -> &[Action] {
    let run = run(world);
    &run.actions[run.mark..]
}

/// Hands `input` to the machine, its actions read from here on.
fn handle(world: &mut ClientWorld, input: Input) {
    let run = run(world);
    run.mark = run.actions.len();
    run.handle(input);
}

#[given(regex = r"^a client with minReconnectDelay (\d+) ms, connected over websocket$")]
fn a_connected_machine(world: &mut ClientWorld, min: u64) {
    let config = Config::default().min_reconnect_delay(Duration::from_millis(min));
    drive(world, config.clone(), connected_run(config));
}

#[given("a client connected over websocket")]
fn a_connected_machine_with_defaults(world: &mut ClientWorld) {
    drive(world, Config::default(), connected_run(Config::default()));
}

#[given(
    "a client configured with websocket and http_stream endpoints, whose websocket transport is opening on its first attempt"
)]
fn a_machine_opening_websocket(world: &mut ClientWorld) {
    let machine = Machine::new(Config::default())
        .with_transports(vec![TransportKind::WebSocket, TransportKind::HttpStream])
        .expect("two transports");
    let mut run = BatchRun::of(machine);
    run.handle(Input::Connect);
    assert!(
        run.actions.iter().any(|action| matches!(
            action,
            Action::OpenTransport {
                kind: TransportKind::WebSocket,
                ..
            }
        )),
        "{:?}",
        run.actions
    );
    drive(world, Config::default(), run);
}

#[given("a disconnected client")]
fn a_disconnected_machine(world: &mut ClientWorld) {
    drive(
        world,
        Config::default(),
        BatchRun::new(Config::default(), false),
    );
}

#[when("the device goes offline")]
fn the_device_goes_offline(world: &mut ClientWorld) {
    handle(world, Input::Offline);
}

#[when("the page enters the back/forward cache")]
fn the_page_enters_the_cache(world: &mut ClientWorld) {
    handle(world, Input::PageCached);
}

#[then("the client closes the transport")]
#[then("the client closes the opening transport")]
fn the_client_closes_the_transport(world: &mut ClientWorld) {
    let actions = since(world);
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::CloseTransport(_))),
        "{actions:?}"
    );
}

#[then("the client emits no event")]
fn the_client_emits_no_event(world: &mut ClientWorld) {
    let actions = since(world);
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::Emit(..))),
        "{actions:?}"
    );
}

#[then("the machine returns no action")]
fn the_machine_returns_no_action(world: &mut ClientWorld) {
    let actions = since(world);
    assert!(actions.is_empty(), "{actions:?}");
}

/// The reconnect delay set since the step under test started, with no
/// attempt started at once.
fn next_delay(world: &mut ClientWorld) -> Duration {
    let actions = since(world);
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::OpenTransport { .. })),
        "{actions:?}"
    );
    actions
        .iter()
        .find_map(|action| match action {
            Action::SetTimer(Timer::Reconnect, delay) => Some(*delay),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a reconnect delay: {actions:?}"))
}

#[then("the next attempt starts after the reconnect backoff delay and the jitter window")]
fn after_the_backoff_and_the_jitter(world: &mut ClientWorld) {
    let config = world.network_events.config.clone();
    let delay = next_delay(world);
    // Block 2's bound for attempt 0, plus Block 14's window.
    let bound = config
        .max_reconnect_delay
        .min(config.min_reconnect_delay * 2)
        + config.reconnect_jitter_window;
    assert!(
        delay >= config.min_reconnect_delay && delay <= bound,
        "{delay:?} outside {:?}..={bound:?}",
        config.min_reconnect_delay
    );
}

#[then("the next attempt opens the websocket transport again, after the reconnect backoff delay")]
fn websocket_again_after_the_backoff(world: &mut ClientWorld) {
    let min = world.network_events.config.min_reconnect_delay;
    let delay = next_delay(world);
    assert!(delay >= min, "{delay:?}");
    let run = run(world);
    assert_eq!(run.due(Key::Client(Timer::Reconnect)), Some(delay));
    run.mark = run.actions.len();
    run.advance(delay);
    let opened = run.actions[run.mark..]
        .iter()
        .find_map(|action| match action {
            Action::OpenTransport { kind, .. } => Some(*kind),
            _ => None,
        });
    assert_eq!(opened, Some(TransportKind::WebSocket), "{:?}", run.actions);
}
