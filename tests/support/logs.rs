//! A capture of Ferrifuge's log records (Block 21, D7): one process-wide
//! `tracing` subscriber keeps the records of the clients a scenario
//! registers, and every record made outside a client's span.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// A record, its values formatted.
#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub(crate) level: Level,
    pub(crate) target: String,
    pub(crate) message: String,
    pub(crate) fields: BTreeMap<String, String>,
    /// The spans in its scope, innermost first: their names and fields.
    pub(crate) spans: Vec<(String, BTreeMap<String, String>)>,
}

impl Record {
    /// The client whose span holds it, or its own `client` field.
    pub(crate) fn client(&self) -> Option<u64> {
        self.spans
            .iter()
            .find_map(|(_, fields)| fields.get("client"))
            .or_else(|| self.fields.get("client"))
            .and_then(|number| number.parse().ok())
    }

    /// Whether it is in the span of the subscription to `channel`.
    pub(crate) fn in_subscription(&self, channel: &str) -> bool {
        self.spans.iter().any(|(name, fields)| {
            name == "subscription" && fields.get("channel").map(String::as_str) == Some(channel)
        })
    }

    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }

    /// Everything it shows: message, fields and span fields.
    pub(crate) fn text(&self) -> String {
        let mut text = format!("{} {} {}", self.level, self.target, self.message);
        let spans = self.spans.iter().flat_map(|(_, fields)| fields);
        for (name, value) in self.fields.iter().chain(spans) {
            let _ = write!(text, " {name}={value}");
        }
        text
    }
}

#[derive(Default)]
struct Capture {
    clients: HashSet<u64>,
    records: Vec<Record>,
}

fn capture() -> &'static Mutex<Capture> {
    static CAPTURE: OnceLock<Mutex<Capture>> = OnceLock::new();
    CAPTURE.get_or_init(Mutex::default)
}

/// Installs the capturing subscriber, once per process.
pub(crate) fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::registry().with(Recorder);
        tracing::subscriber::set_global_default(subscriber).expect("no other subscriber");
    });
}

/// Keeps the records of client `number` from now on.
pub(crate) fn register(number: u64) {
    install();
    capture().lock().unwrap().clients.insert(number);
}

/// The records of client `number` so far, in order.
pub(crate) fn of(number: u64) -> Vec<Record> {
    capture()
        .lock()
        .unwrap()
        .records
        .iter()
        .filter(|record| record.client() == Some(number))
        .cloned()
        .collect()
}

/// Every record kept so far: those of the clients registered, and those
/// made outside any client's span.
pub(crate) fn all() -> Vec<Record> {
    capture().lock().unwrap().records.clone()
}

/// Fields as text: strings as they are, other values as their `Debug`.
#[derive(Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

struct Recorder;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Recorder {
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.target().starts_with("ferrifuge")
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let spans: Vec<(String, BTreeMap<String, String>)> = context
            .event_scope(event)
            .into_iter()
            .flatten()
            .map(|span| {
                let fields = span
                    .extensions()
                    .get::<Fields>()
                    .map(|fields| fields.0.clone())
                    .unwrap_or_default();
                (span.name().to_owned(), fields)
            })
            .collect();
        let client = spans
            .iter()
            .find_map(|(_, fields)| fields.get("client"))
            .and_then(|number| number.parse::<u64>().ok());
        // Records of clients no scenario watches are not formatted at all.
        if client.is_some_and(|number| !capture().lock().unwrap().clients.contains(&number)) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut fields = fields.0;
        let message = fields.remove("message").unwrap_or_default();
        capture().lock().unwrap().records.push(Record {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            message,
            fields,
            spans,
        });
    }
}
