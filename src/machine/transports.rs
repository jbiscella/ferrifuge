//! The transports a [`super::Machine`] tries, in order, and the one it
//! keeps once a connection was established over it (Block 8; centrifuge-js
//! `_transportWasOpen`, `_triedAllTransports`, `_advanceTransportIndex`).

use super::TransportKind;

#[derive(Debug)]
pub(super) struct Transports {
    kinds: Vec<TransportKind>,
    /// Index in `kinds` of the transport in use, or to try next.
    index: usize,
    /// A connection was established over the transport at `index`: every
    /// reconnect uses it.
    chosen: bool,
    /// Every transport was tried once without one being chosen.
    tried_all: bool,
}

impl Transports {
    /// `kinds` must not be empty.
    pub(super) fn new(kinds: Vec<TransportKind>) -> Self {
        assert!(!kinds.is_empty(), "no transport");
        Self {
            kinds,
            index: 0,
            chosen: false,
            tried_all: false,
        }
    }

    pub(super) fn current(&self) -> TransportKind {
        self.kinds[self.index]
    }

    /// Index of the current transport in the configured list.
    pub(super) fn index(&self) -> usize {
        self.index
    }

    /// A connection was established over the current transport.
    pub(super) fn choose(&mut self) {
        self.chosen = true;
    }

    /// An attempt over the current transport failed: unless it was chosen,
    /// the next attempt tries the next one.
    pub(super) fn attempt_failed(&mut self) {
        if self.chosen {
            return;
        }
        self.index += 1;
        if self.index == self.kinds.len() {
            self.index = 0;
            self.tried_all = true;
        }
    }

    /// The current transport ended before the server answered on it
    /// (Block 48, D9): it is no longer chosen, and the next attempt starts
    /// a new round from the next transport, at once.
    pub(super) fn give_way(&mut self) {
        self.chosen = false;
        self.tried_all = false;
    }

    /// How many transports are configured.
    pub(super) fn len(&self) -> usize {
        self.kinds.len()
    }

    /// The current transport cannot be opened (Block 44, D12): the next
    /// one is tried, even after one was chosen, as centrifuge-js 63ed73e
    /// `_initializeTransport` advances past a transport not supported.
    pub(super) fn skip(&mut self) {
        self.index += 1;
        if self.index == self.kinds.len() {
            self.index = 0;
            self.tried_all = true;
        }
    }

    /// The round starts again from the first transport, as on the first
    /// connection: a new network may allow it again (Block 25, D6).
    pub(super) fn restart(&mut self) {
        self.index = 0;
        self.chosen = false;
        self.tried_all = false;
    }

    /// Whether the client is still in its first round over the transports,
    /// which tries each at once rather than after the reconnect backoff.
    pub(super) fn in_first_round(&self) -> bool {
        !self.chosen && !self.tried_all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_attempts_move_on_until_one_transport_is_chosen() {
        let mut transports = Transports::new(vec![
            TransportKind::WebSocket,
            TransportKind::HttpStream,
            TransportKind::Sse,
        ]);

        let mut seen = vec![(transports.current(), transports.in_first_round())];
        for _ in 0..3 {
            transports.attempt_failed();
            seen.push((transports.current(), transports.in_first_round()));
        }
        transports.choose();
        transports.attempt_failed();

        assert_eq!(
            seen,
            [
                (TransportKind::WebSocket, true),
                (TransportKind::HttpStream, true),
                (TransportKind::Sse, true),
                (TransportKind::WebSocket, false),
            ]
        );
        assert_eq!(transports.current(), TransportKind::WebSocket);
    }
}
