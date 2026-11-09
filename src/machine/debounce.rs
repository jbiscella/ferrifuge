//! Block 46: publish debouncing as advised by the server. A subscription's
//! publish sends the first value at once and the latest one per interval;
//! a publish merged in a window resolves with the result of the send that
//! carried later data.

use super::{DebounceTimer, Machine};

/// The debounce windows of a machine.
#[derive(Debug, Default)]
pub(super) struct Debouncing {
    /// Whether [`Machine::with_publish_debounce`] turned it on.
    pub(super) enabled: bool,
}

impl Machine {
    /// A debounce window's timer fired.
    pub(super) fn debounce_timer_fired(&mut self, timer: DebounceTimer) {
        // No window is open yet.
        let _ = (timer, self.debouncing.enabled);
    }
}
