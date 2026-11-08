//! Block 15: messages from the server wait in a bounded read-ahead and are
//! handled in order, each only after every event of earlier messages, of
//! any connection, has been taken or withdrawn. On read, before that, the
//! client answers pings, re-arms the no-ping deadline and pauses the
//! deadline of the command a reply answers. A full read-ahead stops the
//! reading, and pauses the no-ping deadline until reading resumes.

use std::collections::{HashSet, VecDeque};

use super::{Action, Anomaly, EventId, Machine, Timer, TransportId};
use crate::State;
use crate::wire::{Reply, ReplyResult};

/// A message from the server, read and not yet handled.
#[derive(Debug)]
enum Item {
    Reply(Box<Reply>),
    /// The server closed the transport after the messages before it.
    Closed {
        code: Option<u16>,
        reason: String,
    },
    /// The transport ended on a message over the size limit (Block 29).
    TooLarge,
}

#[derive(Debug)]
struct Held {
    transport: TransportId,
    item: Item,
    /// Its encoded size.
    size: usize,
}

/// The read-ahead and what holds it back.
#[derive(Debug, Default)]
pub(super) struct Inbound {
    held: VecDeque<Held>,
    bytes: usize,
    /// Events of handled messages not yet taken: the next message waits
    /// until there are none.
    waiting_for: HashSet<EventId>,
    /// Reading is stopped: the read-ahead is full.
    full: bool,
}

impl Machine {
    /// Reads a frame of `transport`: answers its pings at once and queues
    /// its other messages, which are then handled as rule X allows.
    pub(super) fn frame(&mut self, transport: TransportId, data: &[u8]) {
        let Some(replies) = self.codec.decode_sized(data) else {
            // Not client protocol data, e.g. from a misconfigured proxy.
            self.actions
                .push(Action::Anomaly(Anomaly::UndecodableFrame {
                    transport,
                    bytes: data.len(),
                }));
            self.reconnect_after_transport_closed();
            return;
        };
        self.counters.messages_in += replies.len() as u64;
        if replies.is_empty() {
            return;
        }
        // Any data from the server proves the connection alive, as in
        // centrifuge-js `_dataReceived`.
        self.wait_server_ping();
        for (reply, size) in replies {
            if reply.id == 0 && matches!(reply.result, ReplyResult::Empty) && reply.error.is_none()
            {
                self.ping_read();
                continue;
            }
            // A call's reply settles it when read, ahead of held messages
            // (Block 13; centrifuge-rust `docs/spec.md` §9.3).
            if reply.id > 0 && self.calls.owns_command(reply.id) {
                self.reset_backoff_if_pending();
                self.call_reply(reply);
                continue;
            }
            if reply.id > 0 {
                self.pause_reply_deadline(reply.id);
            }
            self.hold(transport, Item::Reply(Box::new(reply)), size.max(1));
        }
        self.handle_inbound();
    }

    /// The transport closed: handled after the messages read before it.
    pub(super) fn transport_closed_in_order(
        &mut self,
        transport: TransportId,
        code: Option<u16>,
        reason: String,
    ) {
        self.hold(transport, Item::Closed { code, reason }, 0);
        // No data can come after the close: no ping deadline either, or it
        // would end the connection before the close is handled.
        self.actions.push(Action::CancelTimer(Timer::ServerPing));
        self.handle_inbound();
    }

    /// The transport ended on a message over the size limit: handled after
    /// the messages read before it, as a close.
    pub(super) fn message_too_large_in_order(&mut self, transport: TransportId) {
        self.hold(transport, Item::TooLarge, 0);
        self.actions.push(Action::CancelTimer(Timer::ServerPing));
        self.handle_inbound();
    }

    fn hold(&mut self, transport: TransportId, item: Item, size: usize) {
        self.inbound.bytes += size;
        self.inbound.held.push_back(Held {
            transport,
            item,
            size,
        });
    }

    /// A server ping (an empty reply): answered on read, if the connect
    /// reply asked for pongs, even one still held (centrifuge-js
    /// `_handleServerPing`).
    fn ping_read(&mut self) {
        self.pinged = true;
        let pong = match self.state {
            State::Connected => {
                // A ping proves the connection usable.
                self.reset_backoff_if_pending();
                self.pings.is_some_and(|pings| pings.pong)
            }
            _ => self.inbound.held.iter().any(|held| {
                Some(held.transport) == self.transport
                    && matches!(&held.item, Item::Reply(reply) if matches!(&reply.result, ReplyResult::Connect(result) if result.pong))
            }),
        };
        if pong {
            self.pong();
        }
    }

    /// A reply to command `id` was read: its deadline stops counting, as
    /// the server has answered (Block 15). Handling the reply settles it.
    fn pause_reply_deadline(&mut self, id: u32) {
        if self.connect_command == Some(id) {
            self.actions
                .push(Action::CancelTimer(Timer::ConnectTimeout));
            self.start_optimistic_deadlines();
        } else if self.tokens.awaits_refresh_reply(id) {
            self.actions.push(Action::CancelTimer(Timer::RefreshReply));
        } else if let Some((subscription, timer)) = self.subscriptions.reply_timer(id) {
            self.optimistic_answered(id);
            self.actions
                .push(Action::CancelSubscriptionTimer(subscription, timer));
        }
    }

    /// Handles the held messages, in order, while no event of an earlier
    /// one waits to be taken; drops those of a transport that is gone.
    pub(super) fn handle_inbound(&mut self) {
        while self.inbound.waiting_for.is_empty() {
            let Some(held) = self.inbound.held.pop_front() else {
                self.inbound.held.shrink_to(crate::RESTING_SLOTS);
                break;
            };
            self.inbound.bytes -= held.size;
            if Some(held.transport) != self.transport {
                continue;
            }
            let start = self.actions.len();
            match held.item {
                Item::Reply(reply) => self.reply(*reply),
                Item::Closed { code, reason } => self.transport_closed(code, &reason),
                Item::TooLarge => self.message_too_large(),
            }
            let waiting: Vec<EventId> = self.actions[start..]
                .iter()
                .filter_map(|action| match action {
                    Action::Emit(id, _) if self.has_stream(None) => Some(*id),
                    Action::EmitSubscription(id, subscription, _)
                        if self.has_stream(Some(*subscription)) =>
                    {
                        Some(*id)
                    }
                    _ => None,
                })
                .collect();
            self.inbound.waiting_for.extend(waiting);
        }
        self.update_reading();
    }

    /// The event `id` was taken or withdrawn: once none waits, the next
    /// message follows, when the current input is handled.
    pub(super) fn settle_event(&mut self, id: EventId) {
        self.inbound.waiting_for.remove(&id);
    }

    /// Drops the messages of transports that are gone, then stops or
    /// resumes reading as the read-ahead is full or not.
    pub(super) fn update_reading(&mut self) {
        let current = self.transport;
        let mut dropped = 0;
        self.inbound.held.retain(|held| {
            let keep = Some(held.transport) == current;
            if !keep {
                dropped += held.size;
            }
            keep
        });
        self.inbound.bytes -= dropped;
        let (messages, bytes) = self.read_ahead();
        let full = messages < self.inbound.held.len()
            || messages >= self.config.read_ahead_messages.max(1)
            || bytes >= self.config.read_ahead_bytes.max(1);
        if full == self.inbound.full {
            return;
        }
        self.inbound.full = full;
        if full {
            // Not reading is the client's choice: the server's handling
            // of slow clients decides, not a missing ping (Block 15).
            self.actions.push(Action::CancelTimer(Timer::ServerPing));
        } else {
            self.wait_server_ping();
        }
    }

    /// Whether the owner of the machine should read the transport: false
    /// while the read-ahead is full (Block 15).
    #[must_use]
    pub fn wants_frames(&self) -> bool {
        !self.inbound.full
    }

    /// Messages read from the server and not yet handled that fit within
    /// the limits, and their bytes: the read-ahead of Block 15. A message
    /// read that does not fit waits outside it, with reading stopped, as
    /// a reader holding it until there is room (centrifuge-rust §9.2); one
    /// larger than the byte limit fits alone.
    #[must_use]
    pub fn read_ahead(&self) -> (usize, usize) {
        let (mut messages, mut bytes) = (0, 0);
        for held in &self.inbound.held {
            let fits = messages == 0
                || (messages < self.config.read_ahead_messages.max(1)
                    && bytes + held.size <= self.config.read_ahead_bytes.max(1));
            if !fits {
                break;
            }
            messages += 1;
            bytes += held.size;
        }
        (messages, bytes)
    }

    /// Whether no data is read now: the read-ahead is full, or the
    /// server's close of the transport is held.
    pub(super) fn reading_stopped(&self) -> bool {
        self.inbound.full
            || self
                .inbound
                .held
                .iter()
                .any(|held| matches!(held.item, Item::Closed { .. } | Item::TooLarge))
    }

    /// Whether the server's close of the current transport is read and
    /// waits behind earlier messages: nothing sent on it now would arrive.
    pub(super) fn transport_closing(&self) -> bool {
        self.inbound.held.iter().any(|held| {
            Some(held.transport) == self.transport
                && matches!(held.item, Item::Closed { .. } | Item::TooLarge)
        })
    }

    /// Whether a message of `transport` is held.
    pub(super) fn holds_messages_of(&self, transport: TransportId) -> bool {
        self.inbound
            .held
            .iter()
            .any(|held| held.transport == transport)
    }

    /// Whether the event `id` holds the messages read after the one that
    /// emitted it (Block 15, in order; Block 41, D6).
    pub(crate) fn holds_messages(&self, id: EventId) -> bool {
        self.inbound.waiting_for.contains(&id)
    }

    /// Whether an event of a handled message waits to be taken.
    pub(super) fn events_hold_inbound(&self) -> bool {
        !self.inbound.waiting_for.is_empty()
    }
}
