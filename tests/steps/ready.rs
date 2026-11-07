//! Steps of Block 30: waiting until ready.

use std::pin::Pin;
use std::time::{Duration, Instant};

use cucumber::{given, then, when};
use ferrifuge::{CallError, SubscriptionEvent, codes};
use futures_util::{Stream, StreamExt};
use tokio::task::JoinHandle;

use super::{ClientWorld, EVENT_TIMEOUT, test_config};

/// How soon "at once" settles: the future is settled when it is made.
const AT_ONCE: Duration = Duration::from_millis(500);

type Outcome = (Result<(), CallError>, Instant);

/// A `ready()` future awaited on a task of its own, with when it was
/// made, as the call steps track calls.
#[derive(Debug)]
struct Waiting {
    made_at: Instant,
    task: Option<JoinHandle<Outcome>>,
    settled: Option<Outcome>,
}

/// The `ready()` futures of a Block 30 scenario, and the subscription
/// stream it took without reading.
#[derive(Default)]
pub(crate) struct Ready {
    client: Option<Waiting>,
    subscription: Option<Waiting>,
    subscription_events: Option<Pin<Box<dyn Stream<Item = SubscriptionEvent> + Send>>>,
}

impl std::fmt::Debug for Ready {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ready")
            .field("client", &self.client)
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

fn track(
    future: impl std::future::Future<Output = Result<(), CallError>> + Send + 'static,
    made_at: Instant,
) -> Waiting {
    Waiting {
        made_at,
        task: Some(tokio::spawn(async move {
            let result = future.await;
            (result, Instant::now())
        })),
        settled: None,
    }
}

fn waiting<'a>(world: &'a mut ClientWorld, of: &str) -> &'a mut Waiting {
    let waiting = if of == "client" {
        &mut world.ready.client
    } else {
        &mut world.ready.subscription
    };
    waiting
        .as_mut()
        .unwrap_or_else(|| panic!("no ready() awaited on the {of}"))
}

/// The outcome of `of`'s `ready()` once settled, and how long after it was
/// made.
async fn outcome(world: &mut ClientWorld, of: &str) -> (Result<(), CallError>, Duration) {
    let waiting = waiting(world, of);
    if let Some(task) = waiting.task.take() {
        let settled = tokio::time::timeout(EVENT_TIMEOUT, task)
            .await
            .unwrap_or_else(|_| {
                panic!("ready() on the {of} did not settle within {EVENT_TIMEOUT:?}")
            })
            .expect("the ready task panicked");
        waiting.settled = Some(settled);
    }
    let (result, at) = waiting.settled.clone().expect("settled above");
    (result, at.duration_since(waiting.made_at))
}

#[when(regex = r"^ready is awaited on the (client|subscription)$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn ready_is_awaited(world: &mut ClientWorld, of: String) {
    let made_at = Instant::now();
    if of == "client" {
        let future = world.client().ready();
        world.ready.client = Some(track(future, made_at));
    } else {
        let future = world.subscription().ready();
        world.ready.subscription = Some(track(future, made_at));
    }
}

#[then(regex = r"^ready on the (client|subscription) resolves$")]
async fn ready_resolves(world: &mut ClientWorld, of: String) {
    let (result, _) = outcome(world, &of).await;
    assert_eq!(result, Ok(()));
}

#[then(regex = r"^ready on the (client|subscription) is still waiting$")]
#[allow(clippy::needless_pass_by_value)] // cucumber passes captures by value
fn ready_still_waits(world: &mut ClientWorld, of: String) {
    let waiting = waiting(world, &of);
    let task = waiting.task.as_ref().expect("not settled yet");
    assert!(!task.is_finished(), "ready() on the {of} settled");
}

fn expected_code(message: &str) -> u32 {
    match message {
        "disconnected" => codes::error::DISCONNECTED,
        "subscription unsubscribed" => codes::error::SUBSCRIPTION_UNSUBSCRIBED,
        other => panic!("unknown error {other}"),
    }
}

#[then(
    regex = r#"^ready on the (client|subscription) fails( at once)? with a "(disconnected|subscription unsubscribed)" error( before the configured timeout)?$"#
)]
async fn ready_fails(
    world: &mut ClientWorld,
    of: String,
    at_once: String,
    message: String,
    early: String,
) {
    let (result, after) = outcome(world, &of).await;
    let error = result.expect_err("ready() resolved");
    assert_eq!(
        (error.code, error.message.as_str()),
        (expected_code(&message), message.as_str()),
        "{error:?}"
    );
    if !at_once.is_empty() {
        assert!(after < AT_ONCE, "failed after {after:?}");
    }
    if !early.is_empty() {
        let timeout = test_config().timeout;
        assert!(
            after < timeout,
            "failed after {after:?}, not before {timeout:?}"
        );
    }
}

#[given(
    "a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint"
)]
fn a_client_whose_stream_is_taken(world: &mut ClientWorld) {
    super::delivery::a_client_with_its_stream(world);
}

#[given(
    regex = r"^the fake server answers the first (\d+) connect commands with a temporary error$"
)]
fn the_fake_server_fails_connects(world: &mut ClientWorld, count: usize) {
    world.fake().fail_connects(count);
}

#[given(r#"a subscription to channel "news" whose stream was taken and is not read"#)]
fn a_subscription_whose_stream_is_taken(world: &mut ClientWorld) {
    let subscription = world
        .client()
        .new_subscription("news")
        .expect("a new subscription");
    world.ready.subscription_events = Some(Box::pin(subscription.events()));
    world.subscription = Some(subscription);
}

#[when("subscribe is called on the subscription")]
fn subscribe_is_called(world: &mut ClientWorld) {
    world.subscription().subscribe();
}

#[then(
    regex = r#"^when the application reads the subscription's stream, it yields ((?:"\w+"(?:, )?)+)$"#
)]
async fn subscription_stream_yields(world: &mut ClientWorld, list: String) {
    let expected: Vec<&str> = list
        .split(", ")
        .map(|name| name.trim_matches('"'))
        .collect();
    let stream = world
        .ready
        .subscription_events
        .as_mut()
        .expect("the subscription's stream");
    let mut names = Vec::new();
    for _ in 0..expected.len() {
        let event = tokio::time::timeout(EVENT_TIMEOUT, stream.next())
            .await
            .unwrap_or_else(|_| panic!("no event within {EVENT_TIMEOUT:?}"))
            .expect("the stream has not ended");
        names.push(event.name().to_owned());
    }
    assert_eq!(names, expected);
}
