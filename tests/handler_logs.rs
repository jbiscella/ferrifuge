//! The record of a handler's panic lies in its client's span, as every
//! record does (Block 21; Codex on #169, seen on Windows CI). In its own
//! binary, with a global subscriber: thread-local subscribers of parallel
//! tests race on `tracing`'s callsite interest, which can leave the client's
//! span disabled.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferrifuge::{Client, ClientEvent, Config};
use tracing_subscriber::layer::SubscriberExt as _;

/// Whether each WARN record lay in a client's span.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<bool>>>);

impl<S> tracing_subscriber::Layer<S> for Warnings
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() == tracing::Level::WARN {
            let in_client = context
                .event_span(event)
                .is_some_and(|span| span.name() == "client");
            self.0.lock().unwrap().push(in_client);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_handler_panic_is_logged_in_the_client_s_span() {
    let warnings = Warnings::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(warnings.clone()))
        .expect("the only subscriber of this binary");
    let client = Client::new("ws://127.0.0.1:1/connection/websocket", Config::default()).unwrap();

    client.set_handler(|_: ClientEvent| panic!("boom"));
    client.connect();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let warnings = warnings.0.lock().unwrap();
    assert!(!warnings.is_empty());
    assert!(warnings.iter().all(|in_client| *in_client), "{warnings:?}");
}
