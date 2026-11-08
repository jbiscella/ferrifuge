//! Block 18: batches. While one is open, the application's calls (but
//! `send`) and the subscribe and unsubscribe commands its own calls cause
//! are held; stopping a batch sends them in order, in frames of several
//! commands bounded by `Config::batch_frame_bytes`, the calls admitted to
//! the write queue one by one (centrifuge-js `startBatching`,
//! `stopBatching`).
//!
//! Block 36: with automatic batching, every command goes into an output
//! buffer instead, which `Input::Flush` sends in frames of the same bound.

use super::{Action, CallId, FrameId, Machine, TransportId};
use crate::CallReply;
use crate::codec::Codec;
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

/// A frame of several commands being built.
#[derive(Default)]
struct Frame {
    commands: Vec<Command>,
    /// The sum of their sizes within a frame (see `Machine::piece_size`).
    size: usize,
    /// Bytes of the calls among them, not yet counted against the write
    /// queue: those of the frames sent before it are.
    calls: usize,
}

/// A command in the output buffer, encoded once (Block 36, D1).
#[derive(Debug)]
pub(super) struct Buffered {
    /// Its encoding as one command of a frame.
    bytes: Vec<u8>,
    /// Its size within a frame, as `Machine::piece_size` measures it.
    size: usize,
    /// The call it carries: not sent once settled or abandoned.
    call: Option<CallId>,
    /// The bytes counted against the write queue for it.
    queued: usize,
    /// Goes in a frame of its own: a resend after HTTP 413 (D4).
    alone: bool,
    /// A `send`, which settles once its frame is handed over.
    send: bool,
}

#[derive(Debug, Default)]
pub(super) struct Batching {
    /// Batches open.
    open: usize,
    /// What they hold, in order.
    held: Vec<HeldCommand>,
    /// The subscribe commands a connect reply starts, collected to go out as
    /// one batch.
    connect_reply: Option<Vec<Command>>,
    /// The input being handled is the application's `unsubscribe()` or
    /// removal of a subscription, whose unsubscribe command a batch holds.
    pub(super) application_unsubscribe: bool,
    /// Commands go into `buffer` until `Input::Flush` (Block 36, D2).
    pub(super) automatic: bool,
    /// The output buffer, in the order the commands were produced.
    buffer: Vec<Buffered>,
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
        } else if let Some(collected) = &mut self.batching.connect_reply {
            collected.push(command);
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
        let mut frame = Frame::default();
        for command in held {
            let (command, calls, call) = match command {
                HeldCommand::Call(id) => {
                    if !self.is_held(id) {
                        // Settled or abandoned meanwhile.
                        continue;
                    }
                    match self.release_held(id, frame.calls) {
                        Some((command, calls)) => (command, calls, Some(id)),
                        None => continue,
                    }
                }
                HeldCommand::Command(command) => (command, 0, None),
            };
            self.add_to_frame(&mut frame, command, calls, call);
        }
        self.send_frame(&frame);
    }

    /// While the subscriptions subscribe after a connect reply, their
    /// subscribe commands are collected, then sent as one batch, split at
    /// the frame cap (Block 18, D8; centrifuge-js `_connectResponse`).
    pub(super) fn collect_connect_subscribes(&mut self) {
        self.batching.connect_reply = Some(Vec::new());
    }

    pub(super) fn send_connect_subscribes(&mut self) {
        let mut frame = Frame::default();
        for command in self.batching.connect_reply.take().unwrap_or_default() {
            self.add_to_frame(&mut frame, command, 0, None);
        }
        self.send_frame(&frame);
    }

    /// Adds `command`, carrying `calls` bytes of call `call`, to `frame`,
    /// sending the frame first when the command would take it past the
    /// cap; with automatic batching it goes into the output buffer, whose
    /// flush splits frames at the cap, its call bytes counted now.
    fn add_to_frame(
        &mut self,
        frame: &mut Frame,
        command: Command,
        calls: usize,
        call: Option<CallId>,
    ) {
        if self.batching.automatic {
            self.queue_bytes(calls);
            self.buffer(&command, call, calls, false);
            return;
        }
        let size = self.piece_size(&self.codec.encode(std::slice::from_ref(&command)));
        if !frame.commands.is_empty()
            && self.frame_bytes(frame.commands.len() + 1, frame.size + size)
                > self.config.batch_frame_bytes.max(1)
        {
            self.send_frame(&std::mem::take(frame));
        }
        frame.commands.push(command);
        frame.size += size;
        frame.calls += calls;
    }

    /// Whether commands go into the output buffer (Block 36, D2).
    pub(super) fn buffering(&self) -> bool {
        self.batching.automatic
    }

    /// Puts `command` into the output buffer: of call `call` when given,
    /// `queued` bytes of which already count against the write queue.
    pub(super) fn buffer(
        &mut self,
        command: &Command,
        call: Option<CallId>,
        queued: usize,
        send: bool,
    ) {
        let bytes = self.codec.encode(std::slice::from_ref(command));
        self.buffer_bytes(bytes, call, queued, false, send);
    }

    /// Puts an encoded command into the output buffer; `alone` when it
    /// must go in a frame of its own.
    pub(super) fn buffer_bytes(
        &mut self,
        bytes: Vec<u8>,
        call: Option<CallId>,
        queued: usize,
        alone: bool,
        send: bool,
    ) {
        let size = self.piece_size(&bytes);
        self.batching.buffer.push(Buffered {
            bytes,
            size,
            call,
            queued,
            alone,
            send,
        });
    }

    /// The size of one encoded command within a frame as sent: inside a
    /// JSON emulation request it is escaped (Block 36, D1).
    pub(super) fn piece_size(&self, bytes: &[u8]) -> usize {
        match (&self.emulation, self.codec) {
            (Some(_), codec) => codec.escaped_len(bytes),
            _ => bytes.len(),
        }
    }

    /// The size of a frame of `count` commands whose sizes add up to `sum`,
    /// as sent: their separators and, over an emulated transport, the
    /// request around them; equal to the length of the frame encoded
    /// (Block 36, D1).
    pub(super) fn frame_bytes(&self, count: usize, sum: usize) -> usize {
        let separators = count.saturating_sub(1);
        let Some((session, node)) = &self.emulation else {
            return match self.codec {
                Codec::Json => sum + separators,
                Codec::Protobuf => sum,
            };
        };
        let around = self.codec.emulation_request(session, node, b"").len();
        match self.codec {
            // An escaped newline between commands.
            Codec::Json => around + sum + 2 * separators,
            // The data field, absent when empty: its tag and length.
            Codec::Protobuf => around + 1 + prost::length_delimiter_len(sum) + sum,
        }
    }

    /// Sends the output buffer in frames, in order, within
    /// `Config::batch_frame_bytes`; a call settled or abandoned meanwhile
    /// is left out (Block 36, D1).
    pub(super) fn flush(&mut self) {
        let buffer = std::mem::take(&mut self.batching.buffer);
        let Some(transport) = self.transport else {
            return;
        };
        let cap = self.config.batch_frame_bytes.max(1);
        let mut frame: Vec<Buffered> = Vec::new();
        let mut size = 0;
        for piece in buffer {
            if piece.call.is_some_and(|call| !self.awaits_call(call)) {
                self.unqueue_bytes(piece.queued);
                continue;
            }
            let apart = piece.alone || frame.last().is_some_and(|last| last.alone);
            if !frame.is_empty()
                && (apart || self.frame_bytes(frame.len() + 1, size + piece.size) > cap)
            {
                self.send_buffered(transport, std::mem::take(&mut frame));
                size = 0;
            }
            size += piece.size;
            frame.push(piece);
        }
        if !frame.is_empty() {
            self.send_buffered(transport, frame);
        }
    }

    /// Hands one frame of the output buffer to the owner: its calls count
    /// as unwritten until it is written, and its `send`s settle.
    fn send_buffered(&mut self, transport: TransportId, pieces: Vec<Buffered>) {
        let separator: &[u8] = match self.codec {
            Codec::Json => b"\n",
            Codec::Protobuf => b"",
        };
        let mut joined = Vec::new();
        for (index, piece) in pieces.iter().enumerate() {
            if index > 0 {
                joined.extend_from_slice(separator);
            }
            joined.extend_from_slice(&piece.bytes);
        }
        let frame = match &self.emulation {
            Some((session, node)) => self.codec.emulation_request(session, node, &joined),
            None => joined,
        };
        let id = FrameId(self.next_frame);
        self.next_frame += 1;
        self.counters.sent(id, frame.len(), pieces.len());
        self.actions.push(Action::Send {
            transport,
            id,
            frame,
            frame_type: self.codec.frame_type(),
        });
        let queued: usize = pieces.iter().map(|piece| piece.queued).sum();
        self.frame_queued(id, queued);
        for piece in pieces {
            if let (true, Some(call)) = (piece.send, piece.call) {
                self.settle_call(call, Ok(CallReply::Send));
            }
        }
    }

    /// The transport ends: its buffered commands are lost as sent ones
    /// are, a buffered `send` failing with "connection closed" (Block 36,
    /// D1).
    pub(super) fn drop_buffer(&mut self) {
        for piece in std::mem::take(&mut self.batching.buffer) {
            if let (true, Some(call)) = (piece.send, piece.call) {
                self.settle_call(call, Err(super::calls::connection_closed()));
            }
        }
    }

    fn send_frame(&mut self, frame: &Frame) {
        let Some(transport) = self.transport else {
            return;
        };
        if frame.commands.is_empty() {
            return;
        }
        let (sent, _) = self.send_commands(transport, &frame.commands);
        self.batch_frame_sent(sent, frame.calls);
    }
}

#[cfg(test)]
mod tests {
    use crate::machine::{
        Action, FrameId, Input, Machine, SubscriptionOptions, SubscriptionTimer, TransportId,
        TransportKind,
    };
    use crate::{CallRequest, Config};

    /// A connected machine with `config`; the connect command was id 1.
    fn connected(config: Config) -> Machine {
        let mut machine = Machine::new(config);
        let transport = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a transport");
        machine.handle(Input::TransportOpened(transport));
        machine.handle(Input::Frame {
            transport,
            data: br#"{"id":1,"connect":{"client":"c"}}"#.to_vec(),
        });
        machine
    }

    /// A machine connected over `http_stream`, with session "s", node "n".
    fn emulated(config: Config) -> (Machine, TransportId) {
        let mut machine = Machine::new(config)
            .with_transports(vec![TransportKind::HttpStream])
            .expect("an emulated transport");
        let transport = machine
            .handle(Input::Connect)
            .iter()
            .find_map(|action| match action {
                Action::OpenTransport { transport, .. } => Some(*transport),
                _ => None,
            })
            .expect("a transport");
        machine.handle(Input::TransportOpened(transport));
        machine.handle(Input::Frame {
            transport,
            data: br#"{"id":1,"connect":{"client":"c","session":"s","node":"n"}}"#.to_vec(),
        });
        (machine, transport)
    }

    fn rpc(method: &str) -> CallRequest {
        CallRequest::Rpc {
            method: method.to_owned(),
            data: b"{}".to_vec(),
        }
    }

    /// The frames of `actions`, with their ids.
    fn sent(actions: &[Action]) -> Vec<(FrameId, Vec<u8>)> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { id, frame, .. } => Some((*id, frame.clone())),
                _ => None,
            })
            .collect()
    }

    /// The server refuses `frame` with HTTP 413; the transport reports it
    /// written too, as the HTTP transport does.
    fn refuse(
        machine: &mut Machine,
        transport: TransportId,
        (id, frame): (FrameId, Vec<u8>),
    ) -> Vec<Action> {
        let mut actions = machine.handle(Input::SendRejected { transport, frame });
        actions.extend(machine.handle(Input::FrameWritten {
            transport,
            frame: id,
        }));
        actions
    }

    // Codex #94: a batched subscribe its subscription no longer waits for
    // is retired when its request is refused, not left blocking the channel
    // until its reply deadline reconnects the client.
    #[test]
    fn a_refused_stale_subscribe_is_retired_not_resent() {
        let (mut machine, transport) = emulated(Config::default());
        let sub = machine
            .add_subscription("news".to_owned(), SubscriptionOptions::default())
            .expect("a new channel");
        machine.handle(Input::StartBatching);
        let call = machine.add_call(rpc("a"));
        machine.handle(Input::Call(call));
        machine.handle(Input::Subscribe(sub));
        let batch = sent(&machine.handle(Input::StopBatching)).remove(0);
        machine.handle(Input::Unsubscribe(sub));

        let actions = refuse(&mut machine, transport, batch);

        assert!(
            actions.contains(&Action::CancelSubscriptionTimer(
                sub,
                SubscriptionTimer::SubscribeReply
            )),
            "{actions:?}"
        );
        let resent = sent(&actions);
        assert_eq!(resent.len(), 1, "{actions:?}");
        assert!(
            String::from_utf8_lossy(&resent[0].1).contains("rpc"),
            "{actions:?}"
        );
    }

    // Codex #94: a resent call counts its whole emulation request against
    // the write queue, as any call's frame does.
    #[test]
    fn a_resent_call_counts_its_whole_request() {
        // Request and inner sizes of a resent rpc, measured on a first run.
        let (mut machine, transport) = emulated(Config::default());
        machine.handle(Input::StartBatching);
        for method in ["a", "b"] {
            let call = machine.add_call(rpc(method));
            machine.handle(Input::Call(call));
        }
        let batch = sent(&machine.handle(Input::StopBatching)).remove(0);
        let resent = sent(&refuse(&mut machine, transport, batch));
        let full: usize = resent.iter().map(|(_, frame)| frame.len()).sum();
        let inner: usize = resent
            .iter()
            .map(|(_, frame)| {
                crate::codec::Codec::Json
                    .emulation_frame(frame)
                    .expect("a request")
                    .len()
            })
            .sum();
        let next = resent[0].1.len();
        assert!(full > inner);

        // A queue a third call fits only when the resent calls count their
        // inner commands alone: halfway between the two counts.
        let (mut machine, transport) = emulated(Config {
            write_queue_bytes: inner + next + (full - inner) / 2,
            ..Config::default()
        });
        machine.handle(Input::StartBatching);
        for method in ["a", "b"] {
            let call = machine.add_call(rpc(method));
            machine.handle(Input::Call(call));
        }
        let batch = sent(&machine.handle(Input::StopBatching)).remove(0);
        refuse(&mut machine, transport, batch);
        let third = machine.add_call(rpc("c"));

        let actions = machine.handle(Input::Call(third));

        assert_eq!(sent(&actions), Vec::new(), "{actions:?}");
    }

    /// An rpc whose command encodes to 300 bytes once numbered `id`.
    fn rpc_300(id: u32) -> CallRequest {
        let base = format!(r#"{{"id":{id},"rpc":{{"method":"m","data":""}}}}"#).len();
        CallRequest::Rpc {
            method: "m".to_owned(),
            data: format!("\"{}\"", "x".repeat(300 - base)).into_bytes(),
        }
    }

    // Codex #93: the calls of a frame already sent count once, as unwritten,
    // not again with the calls of the batch.
    #[test]
    fn calls_of_frames_already_sent_count_once_against_the_write_queue() {
        let mut machine = connected(Config {
            batch_frame_bytes: 1000,
            write_queue_bytes: 2000,
            ..Config::default()
        });
        machine.handle(Input::StartBatching);
        for id in 2..7 {
            let call = machine.add_call(rpc_300(id));
            machine.handle(Input::Call(call));
        }

        let actions = machine.handle(Input::StopBatching);

        let frames: Vec<usize> = actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { frame, .. } => Some(frame.split(|byte| *byte == b'\n').count()),
                _ => None,
            })
            .collect();
        assert_eq!(frames, [3, 2]);
    }
}
