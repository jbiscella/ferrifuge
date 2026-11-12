//! Connection, traffic and delta compression counters (Block 31).

use std::collections::HashMap;
use std::time::SystemTime;

use crate::State;
use crate::machine::{FrameId, TransportKind};

/// A snapshot of a client's counters since it was built (Block 31), read
/// without a round trip: for status displays, logs, support tickets and
/// fleet telemetry.
///
/// ```no_run
/// # fn run(client: &ferrifuge::Client) {
/// let statistics = client.statistics();
/// println!(
///     "{} connects, {} failed attempts, {} bytes in",
///     statistics.connects, statistics.failed_attempts, statistics.bytes_in
/// );
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Statistics {
    /// The connection state, as `state()` reports it.
    pub state: State,
    /// The transport while connected.
    pub transport: Option<TransportKind>,
    /// When the client became connected, while it is: set by the tokio
    /// adapter; the sans-IO core has no clock and reports `None`.
    pub connected_since: Option<SystemTime>,
    /// Times the client became connected, reconnects included.
    pub connects: u64,
    /// Attempts that ended without the client becoming connected: each
    /// transport opened to connect, each fallback transport included, that
    /// failed to open, closed, timed out or was refused before the connect
    /// reply. Attempts ended by `disconnect()`, `reconnect()` or the
    /// client's closing are not counted.
    pub failed_attempts: u64,
    /// Bytes of the frames read from the server, without transport framing.
    pub bytes_in: u64,
    /// Bytes of the frames written to the server, without transport
    /// framing: over `http_stream` and sse, the emulation request bodies.
    pub bytes_out: u64,
    /// Replies and pushes read, pings included.
    pub messages_in: u64,
    /// Commands written, pongs included.
    pub messages_out: u64,
    /// Publications of client-side and server-side subscriptions delivered:
    /// taken from a stream, or counted as taken where no stream holds them
    /// (Block 15). A withdrawn publication never counts. With the
    /// `unstable` feature, a map subscription's `Update` counts, its `Sync`
    /// not (Block 45, D11), and a shared poll subscription's `Update` of a
    /// publication counts, a removal the client made itself not (Block 47,
    /// D13).
    pub publications_delivered: u64,
    /// Client-side subscriptions in the registry, of every kind.
    pub subscriptions: usize,
    /// Server-side subscriptions the client tracks.
    pub server_subscriptions: usize,
}

/// What delta compression did on a subscription since it was created
/// (Block 31), as centrifuge-js `deltaStats()`: counted for each
/// publication whose payload was rebuilt while delta was negotiated, before
/// it is delivered. A subscription without delta reports zeros.
///
/// ```no_run
/// # fn run(subscription: &ferrifuge::Subscription) {
/// let delta = subscription.delta_statistics();
/// println!("{} deltas, ratio {:.2}", delta.delta_payloads, delta.compression_ratio());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeltaStatistics {
    /// Publications counted, full and delta.
    pub publications: u64,
    /// Publications that carried a full payload.
    pub full_payloads: u64,
    /// Publications that carried a delta.
    pub delta_payloads: u64,
    /// Payload bytes as carried, deltas and full payloads, after the JSON
    /// protocol's string unwrapping.
    pub bytes_received: u64,
    /// Payload bytes after the deltas were applied.
    pub bytes_decoded: u64,
}

impl DeltaStatistics {
    /// `1 - bytes_received / bytes_decoded`: the share of bytes delta
    /// saved; 0 when nothing was decoded, negative when deltas carried more
    /// than they built.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "a ratio: precision past 2^53 bytes does not matter"
    )]
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_decoded == 0 {
            return 0.0;
        }
        1.0 - self.bytes_received as f64 / self.bytes_decoded as f64
    }

    /// Counts a publication whose payload of `received` bytes rebuilt one
    /// of `decoded` bytes.
    pub(crate) fn count(&mut self, delta: bool, received: usize, decoded: usize) {
        self.publications += 1;
        if delta {
            self.delta_payloads += 1;
        } else {
            self.full_payloads += 1;
        }
        self.bytes_received += received as u64;
        self.bytes_decoded += decoded as u64;
    }
}

/// The core's counters (Block 31, D2).
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) connects: u64,
    pub(crate) failed_attempts: u64,
    pub(crate) bytes_in: u64,
    pub(crate) bytes_out: u64,
    pub(crate) messages_in: u64,
    pub(crate) messages_out: u64,
    pub(crate) publications_delivered: u64,
    /// Size and command count of each frame handed over and not yet
    /// reported written, on the current transport.
    unwritten: HashMap<FrameId, (u64, u64)>,
    /// Size and command count of the connect command an emulated
    /// transport's request carries, counted once it opens.
    opening: Option<(u64, u64)>,
    /// The attempt in flight ends by the application's or the client's
    /// doing, not as a failure.
    pub(crate) cancelling: bool,
}

impl Counters {
    /// A frame of `bytes` holding `commands` was handed to the transport.
    pub(crate) fn sent(&mut self, frame: FrameId, bytes: usize, commands: usize) {
        self.unwritten
            .insert(frame, (bytes as u64, commands as u64));
    }

    /// The transport wrote `frame`.
    pub(crate) fn written(&mut self, frame: FrameId) {
        if let Some((bytes, commands)) = self.unwritten.remove(&frame) {
            self.bytes_out += bytes;
            self.messages_out += commands;
        }
    }

    /// An emulated transport opens with a request carrying `bytes`.
    pub(crate) fn opening(&mut self, bytes: usize) {
        self.opening = Some((bytes as u64, 1));
    }

    /// The transport opened: its opening request was written.
    pub(crate) fn opened(&mut self) {
        if let Some((bytes, commands)) = self.opening.take() {
            self.bytes_out += bytes;
            self.messages_out += commands;
        }
    }

    /// The transport ended: what it had not written is not counted.
    pub(crate) fn transport_ended(&mut self) {
        self.unwritten.clear();
        self.opening = None;
    }

    /// Frames awaiting their written report, for tests.
    #[cfg(test)]
    pub(crate) fn unwritten(&self) -> usize {
        self.unwritten.len()
    }
}
