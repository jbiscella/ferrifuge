//! The program a mobile device runs (Block 58, D1): one client with default
//! options apart from its endpoint connects, subscribes, receives the
//! publication the host harness (`tests/mobile.rs`) makes on `ready`, and
//! disconnects. Its first line, printed before the client is built, is
//! `start`, so that the job log's timestamps separate the simulator's
//! launch from the run. Each step is bounded and prints one line,
//! `step <name> ok at <t> s` or `step <name> failed at <t> s: <reason>`,
//! `<t>` being the seconds since `start`; the program exits 1 at the first
//! failure. After the last step it holds its client for 15 s, for the
//! harness's `channels` check, then prints `done` and exits 0.
//!
//! `FERRIFUGE_MOBILE_URL` is the endpoint, `FERRIFUGE_MOBILE_CHANNEL` the
//! channel and `FERRIFUGE_MOBILE_NONCE` what the publication's data
//! carries. With `FERRIFUGE_MOBILE_EXPECT=untrusted` only `connect` runs,
//! and it passes on an `error` event naming rustls's invalid peer
//! certificate while the client is still connecting; the error of another
//! failed attempt, such as a handshake timeout, does not end it, and is
//! named if the step runs out of time. The program uses no server API.

use std::process::ExitCode;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use ferrifuge::{Client, ClientEvent, Config, State, SubscriptionEvent};
use futures_util::{Stream, StreamExt};

/// The bound of each step.
const STEP: Duration = Duration::from_secs(30);

/// How long the client is held after disconnecting: longer than the
/// harness's 10 s `channels` check, so that a socket the client never
/// closed is still open when the server is asked (D1).
const HOLD: Duration = Duration::from_secs(15);

/// rustls's text for a certificate the verifier refused
/// (`rustls::Error::InvalidCertificate`).
const INVALID_CERTIFICATE: &str = "invalid peer certificate";

fn variable(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is not set"))
}

/// The next event of `events` that `decide` settles, within [`STEP`]:
/// `Some(Ok)` passes, `Some(Err)` fails, `None` waits for the next one.
async fn step<E, S>(
    events: &mut S,
    mut decide: impl FnMut(E) -> Option<Result<(), String>>,
    mut last: impl FnMut() -> String,
) -> Result<(), String>
where
    S: Stream<Item = E> + Unpin,
{
    let waited = tokio::time::timeout(STEP, async {
        while let Some(event) = events.next().await {
            if let Some(outcome) = decide(event) {
                return outcome;
            }
        }
        Err("the event stream ended".to_owned())
    })
    .await;
    waited.unwrap_or_else(|_| Err(format!("nothing within {STEP:?}{}", last())))
}

/// When `start` was printed.
static START: OnceLock<Instant> = OnceLock::new();

/// Prints `start`, from which the steps' times count.
fn start() {
    START.get_or_init(Instant::now);
    println!("start");
}

/// Prints the step's line, with the seconds since `start`; whether it
/// passed.
fn report(name: &str, outcome: Result<(), String>) -> bool {
    let at = START
        .get()
        .map_or(0.0, |start| start.elapsed().as_secs_f64());
    match outcome {
        Ok(()) => {
            println!("step {name} ok at {at:.1} s");
            true
        }
        Err(reason) => {
            println!("step {name} failed at {at:.1} s: {reason}");
            false
        }
    }
}

/// The last error a step saw, named when it runs out of time.
#[derive(Default)]
struct LastError(std::cell::RefCell<String>);

impl LastError {
    fn set(&self, message: String) {
        *self.0.borrow_mut() = message;
    }

    fn named(&self) -> String {
        let last = self.0.borrow();
        if last.is_empty() {
            String::new()
        } else {
            format!(", last error: {last}")
        }
    }
}

/// `untrusted`: refused by the verifier while still connecting. The error
/// of another failed attempt, such as a handshake timeout, does not end the
/// step: the client tries again.
async fn refused<S>(client: &Client, events: &mut S) -> Result<(), String>
where
    S: Stream<Item = ClientEvent> + Unpin,
{
    let last_error = LastError::default();
    step(
        events,
        |event| match event {
            ClientEvent::Error(error) if error.message.contains(INVALID_CERTIFICATE) => {
                Some(if client.state() == State::Connecting {
                    Ok(())
                } else {
                    Err(format!("refused, in state {:?}", client.state()))
                })
            }
            ClientEvent::Error(error) => {
                last_error.set(error.message);
                None
            }
            ClientEvent::Connected(_) => Some(Err("connected to an untrusted server".to_owned())),
            ClientEvent::Disconnected(ferrifuge::DisconnectedContext { code, reason, .. }) => {
                Some(Err(format!("disconnected: {code} {reason}")))
            }
            _ => None,
        },
        || last_error.named(),
    )
    .await
}

/// `connect`: the client emits `connected`.
async fn connected<S>(events: &mut S) -> Result<(), String>
where
    S: Stream<Item = ClientEvent> + Unpin,
{
    // The last transport error, named when the step runs out of time.
    let last_error = LastError::default();
    step(
        events,
        |event| match event {
            ClientEvent::Connected(_) => Some(Ok(())),
            ClientEvent::Disconnected(ferrifuge::DisconnectedContext { code, reason, .. }) => {
                Some(Err(format!("disconnected: {code} {reason}")))
            }
            ClientEvent::Error(error) => {
                last_error.set(error.message);
                None
            }
            _ => None,
        },
        || last_error.named(),
    )
    .await
}

/// `subscribe`: the subscription emits `subscribed`.
async fn subscribed<S>(events: &mut S) -> Result<(), String>
where
    S: Stream<Item = SubscriptionEvent> + Unpin,
{
    step(
        events,
        |event| match event {
            SubscriptionEvent::Subscribed(_) => Some(Ok(())),
            SubscriptionEvent::Unsubscribed(ferrifuge::UnsubscribedContext {
                code,
                reason,
                ..
            }) => Some(Err(format!("unsubscribed: {code} {reason}"))),
            SubscriptionEvent::Error(error) => Some(Err(format!("error: {}", error.message))),
            _ => None,
        },
        String::new,
    )
    .await
}

/// `receive`: a publication whose data carries `nonce`; any other fails.
async fn received<S>(events: &mut S, nonce: &str) -> Result<(), String>
where
    S: Stream<Item = SubscriptionEvent> + Unpin,
{
    step(
        events,
        |event| match event {
            SubscriptionEvent::Publication(publication) => {
                let data = String::from_utf8_lossy(&publication.data).into_owned();
                Some(if data.contains(nonce) {
                    Ok(())
                } else {
                    Err(format!("a publication without the nonce: {data}"))
                })
            }
            SubscriptionEvent::Unsubscribed(ferrifuge::UnsubscribedContext {
                code,
                reason,
                ..
            }) => Some(Err(format!("unsubscribed: {code} {reason}"))),
            _ => None,
        },
        String::new,
    )
    .await
}

/// `disconnect`: after `disconnect()`, the client emits `disconnected` and
/// its state is `disconnected`.
async fn disconnected<S>(client: &Client, events: &mut S) -> Result<(), String>
where
    S: Stream<Item = ClientEvent> + Unpin,
{
    client.disconnect();
    step(
        events,
        |event| match event {
            ClientEvent::Disconnected(_) => Some(if client.state() == State::Disconnected {
                Ok(())
            } else {
                Err(format!("disconnected, in state {:?}", client.state()))
            }),
            _ => None,
        },
        String::new,
    )
    .await
}

async fn run() -> bool {
    let setup = (|| {
        Ok::<_, String>((
            variable("FERRIFUGE_MOBILE_URL")?,
            variable("FERRIFUGE_MOBILE_CHANNEL")?,
            variable("FERRIFUGE_MOBILE_NONCE")?,
        ))
    })();
    let (url, channel, nonce) = match setup {
        Ok(setup) => setup,
        Err(reason) => return report("setup", Err(reason)),
    };
    let client = match Client::new(url, Config::default()) {
        Ok(client) => client,
        Err(error) => return report("setup", Err(error.message)),
    };
    let subscription = match client.new_subscription(channel) {
        Ok(subscription) => subscription,
        Err(error) => return report("setup", Err(format!("{error:?}"))),
    };
    // Taken before connect() and subscribe(), so that no event is missed.
    let mut client_events = client.events();
    let mut subscription_events = subscription.events();
    client.connect();

    if std::env::var("FERRIFUGE_MOBILE_EXPECT").as_deref() == Ok("untrusted") {
        return report("connect", refused(&client, &mut client_events).await);
    }
    if !report("connect", connected(&mut client_events).await) {
        return false;
    }
    subscription.subscribe();
    if !report("subscribe", subscribed(&mut subscription_events).await) {
        return false;
    }
    println!("ready");
    if !report("receive", received(&mut subscription_events, &nonce).await) {
        return false;
    }
    if !report(
        "disconnect",
        disconnected(&client, &mut client_events).await,
    ) {
        return false;
    }
    tokio::time::sleep(HOLD).await;
    drop(client);
    println!("done");
    true
}

fn main() -> ExitCode {
    start();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            println!("step setup failed: no runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    if runtime.block_on(run()) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
