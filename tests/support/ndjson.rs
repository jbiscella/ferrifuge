//! What the real-server runners' children share (Blocks 51, 52): records
//! written to stdout as NDJSON, each with its sequence number, and whether
//! a token fetch is a refresh. Each runner keeps its own record types.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// A record and its sequence number.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Line<R> {
    pub(crate) seq: u64,
    #[serde(flatten)]
    pub(crate) record: R,
}

/// A child's writer: one thread writing records to stdout in order.
pub(crate) struct Writer<R> {
    lines: std::sync::mpsc::Sender<R>,
}

impl<R> Clone for Writer<R> {
    fn clone(&self) -> Self {
        Self {
            lines: self.lines.clone(),
        }
    }
}

impl<R: Serialize + Send + 'static> Writer<R> {
    pub(crate) fn start() -> Self {
        let (lines, records) = std::sync::mpsc::channel::<R>();
        std::thread::spawn(move || {
            use std::io::Write as _;
            for (seq, record) in (1u64..).zip(records) {
                let line = serde_json::to_string(&Line { seq, record }).expect("a record");
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{line}");
                let _ = out.flush();
            }
        });
        Self { lines }
    }

    pub(crate) fn write(&self, record: R) {
        let _ = self.lines.send(record);
    }
}

/// Whether a connection token fetch is a refresh: the client connected,
/// read from its own state when the callback runs, which can only count
/// fewer (reviewer, 51c). Before the client is set, none is.
pub(crate) fn connection_established(client: &OnceLock<ferrifuge::Client>) -> bool {
    client
        .get()
        .is_some_and(|client| client.state() == ferrifuge::State::Connected)
}

/// Whether a subscription token fetch is a refresh: the subscription
/// subscribed, by its own state.
pub(crate) fn subscription_established(subscription: &OnceLock<ferrifuge::Subscription>) -> bool {
    subscription.get().is_some_and(|subscription| {
        subscription.state() == ferrifuge::SubscriptionState::Subscribed
    })
}
