//! Block 18: batches. While one is open, the application's calls (but
//! `send`) and the subscribe and unsubscribe commands its own calls cause
//! are held; stopping a batch sends them in order, in frames of several
//! commands bounded by `Config::batch_frame_bytes`, the calls admitted to
//! the write queue one by one (centrifuge-js `startBatching`,
//! `stopBatching`).

use super::{CallId, Machine, TransportId};
use crate::wire::Command;

/// A command an open batch holds.
#[derive(Debug)]
pub(super) enum HeldCommand {
    /// A call, whose command is built when the batch stops (Block 13).
    Call(CallId),
    /// A subscribe or unsubscribe command, handled as sent: its reply
    /// deadline runs (Block 18, D3).
    Command(Command),
}

#[derive(Debug, Default)]
pub(super) struct Batching {
    /// Batches open.
    open: usize,
    /// What they hold, in order.
    held: Vec<HeldCommand>,
    /// The input being handled is the application's `unsubscribe()` or
    /// removal of a subscription, whose unsubscribe command a batch holds.
    pub(super) application_unsubscribe: bool,
}

impl Machine {
    /// Whether a batch is open.
    pub(super) fn holding(&self) -> bool {
        self.batching.open > 0
    }

    pub(super) fn hold_in_batch(&mut self, command: HeldCommand) {
        self.batching.held.push(command);
    }

    /// Sends `command`, or holds it in the open batch when `held`: handled
    /// as sent, its reply deadline running (Block 18, D3).
    pub(super) fn send_or_hold(&mut self, transport: TransportId, held: bool, command: Command) {
        if held {
            self.hold_in_batch(HeldCommand::Command(command));
        } else {
            self.send(transport, command);
        }
    }

    pub(super) fn start_batching(&mut self) {
        self.batching.open += 1;
    }

    /// A batch stops: everything held so far goes out, the other batches
    /// staying open (Block 18, D1).
    pub(super) fn stop_batching(&mut self) {
        self.batching.open = self.batching.open.saturating_sub(1);
        self.send_held();
    }

    /// The transport is gone: the subscribe and unsubscribe commands held
    /// are lost as sent ones would be; the calls held keep waiting
    /// (Block 18, D3).
    pub(super) fn drop_held_commands(&mut self) {
        self.batching
            .held
            .retain(|held| matches!(held, HeldCommand::Call(_)));
    }

    /// Sends the commands held, in the order they were held, in frames of
    /// several commands within `Config::batch_frame_bytes`, a single larger
    /// command alone (Block 18, D4, D5).
    fn send_held(&mut self) {
        let held = std::mem::take(&mut self.batching.held);
        let mut frame: Vec<Command> = Vec::new();
        // Bytes of the calls in `frame`, and of the calls of the batch sent
        // so far, against the write queue.
        let mut frame_calls = 0;
        let mut batch_calls = 0;
        for command in held {
            let (command, calls) = match command {
                HeldCommand::Call(id) => {
                    if !self.is_held(id) {
                        // Settled or abandoned meanwhile.
                        continue;
                    }
                    match self.release_held(id, batch_calls) {
                        Some((command, size)) => {
                            batch_calls += size;
                            (command, size)
                        }
                        None => continue,
                    }
                }
                HeldCommand::Command(command) => (command, 0),
            };
            if !frame.is_empty() {
                frame.push(command.clone());
                let fits = self.frame_of(&frame).len() <= self.config.batch_frame_bytes.max(1);
                frame.pop();
                if !fits {
                    self.send_batch_frame(&std::mem::take(&mut frame), frame_calls);
                    frame_calls = 0;
                }
            }
            frame.push(command);
            frame_calls += calls;
        }
        if !frame.is_empty() {
            self.send_batch_frame(&frame, frame_calls);
        }
    }

    fn send_batch_frame(&mut self, commands: &[Command], calls: usize) {
        let Some(transport) = self.transport else {
            return;
        };
        let (frame, _) = self.send_commands(transport, commands);
        self.batch_frame_sent(frame, calls);
    }
}
