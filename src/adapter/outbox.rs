//! Event queues of the tokio adapter (Block 15): the client and each
//! subscription have one, and its one active stream takes the events from
//! it, hand-off style: an event counts as delivered only when taken.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

use crate::machine::EventId;

/// Events of one object waiting for its stream.
#[derive(Debug)]
pub(super) struct Outbox<E> {
    events: VecDeque<(EventId, E)>,
    /// Number of the active stream; none takes events while `None`.
    stream: Option<u64>,
    /// Streams opened so far.
    streams: u64,
    /// The active stream, waiting for an event.
    waker: Option<Waker>,
    /// No event will come: the active stream yields what is queued, then
    /// ends.
    closed: bool,
}

impl<E> Default for Outbox<E> {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            stream: None,
            streams: 0,
            waker: None,
            closed: false,
        }
    }
}

impl<E> Outbox<E> {
    /// Opens a stream, which takes over: the earlier stream ends, and the
    /// events it had not taken are now this one's. Returns its number.
    pub(super) fn open_stream(&mut self) -> u64 {
        self.streams += 1;
        self.stream = Some(self.streams);
        self.wake();
        self.streams
    }

    /// Queues `event` for the active stream; with none, the event counts as
    /// taken at once and its id is returned.
    pub(super) fn offer(&mut self, id: EventId, event: E) -> Option<EventId> {
        if self.stream.is_none() || self.closed {
            return Some(id);
        }
        self.events.push_back((id, event));
        self.wake();
        None
    }

    /// Drops the events among `ids`, sorted, never to be delivered.
    pub(super) fn withdraw(&mut self, ids: &[EventId]) {
        self.events.retain(|(id, _)| ids.binary_search(id).is_err());
    }

    /// The next event for stream `number`, which ends once superseded, or
    /// once the outbox is closed and empty.
    pub(super) fn poll_take(
        &mut self,
        number: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(EventId, E)>> {
        if self.stream != Some(number) {
            return Poll::Ready(None);
        }
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        if self.closed {
            return Poll::Ready(None);
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Stream `number` was dropped: if it was the active one, its queued
    /// events count as taken, as with no stream, and their ids are
    /// returned; `None` for a stream already superseded.
    pub(super) fn drop_stream(&mut self, number: u64) -> Option<Vec<EventId>> {
        if self.stream != Some(number) {
            return None;
        }
        self.stream = None;
        self.waker = None;
        Some(self.events.drain(..).map(|(id, _)| id).collect())
    }

    /// No event will come: the active stream ends once it took the queued
    /// ones.
    pub(super) fn close(&mut self) {
        self.closed = true;
        self.wake();
    }

    /// Whether nothing will be taken from it anymore.
    pub(super) fn is_finished(&self) -> bool {
        self.closed && (self.stream.is_none() || self.events.is_empty())
    }

    /// Events queued and not yet taken.
    #[cfg(any(test, feature = "test-util"))]
    pub(super) fn held(&self) -> usize {
        self.events.len()
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;

    fn id(n: u64) -> EventId {
        crate::machine::EventId::for_tests(n)
    }

    fn take(
        outbox: &mut Outbox<&'static str>,
        stream: u64,
    ) -> Poll<Option<(EventId, &'static str)>> {
        outbox.poll_take(stream, &mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn without_a_stream_an_event_counts_as_taken_at_once() {
        let mut outbox = Outbox::default();

        let taken = outbox.offer(id(1), "connected");

        assert_eq!(taken, Some(id(1)));
        assert_eq!(outbox.held(), 0);
    }

    #[test]
    fn a_stream_takes_its_events_in_order() {
        let mut outbox = Outbox::default();
        let stream = outbox.open_stream();
        outbox.offer(id(1), "a");
        outbox.offer(id(2), "b");

        let first = take(&mut outbox, stream);
        let second = take(&mut outbox, stream);
        let third = take(&mut outbox, stream);

        assert_eq!(first, Poll::Ready(Some((id(1), "a"))));
        assert_eq!(second, Poll::Ready(Some((id(2), "b"))));
        assert_eq!(third, Poll::Pending);
    }

    #[test]
    fn a_second_stream_takes_over_the_events_not_yet_taken() {
        let mut outbox = Outbox::default();
        let first = outbox.open_stream();
        outbox.offer(id(1), "a");

        let second = outbox.open_stream();

        assert_eq!(take(&mut outbox, first), Poll::Ready(None));
        assert_eq!(take(&mut outbox, second), Poll::Ready(Some((id(1), "a"))));
    }

    #[test]
    fn withdrawn_events_are_never_taken() {
        let mut outbox = Outbox::default();
        let stream = outbox.open_stream();
        for n in 1..=3 {
            outbox.offer(id(n), "publication");
        }

        outbox.withdraw(&[id(1), id(3)]);

        assert_eq!(
            take(&mut outbox, stream),
            Poll::Ready(Some((id(2), "publication")))
        );
        assert_eq!(take(&mut outbox, stream), Poll::Pending);
    }

    #[test]
    fn dropping_the_active_stream_takes_what_it_held() {
        let mut outbox = Outbox::default();
        let stream = outbox.open_stream();
        outbox.offer(id(1), "a");

        let taken = outbox.drop_stream(stream);

        assert_eq!(taken, Some(vec![id(1)]));
        assert_eq!(outbox.offer(id(2), "b"), Some(id(2)));
    }

    #[test]
    fn dropping_a_superseded_stream_takes_nothing() {
        let mut outbox = Outbox::default();
        let first = outbox.open_stream();
        outbox.offer(id(1), "a");
        let _second = outbox.open_stream();

        let taken = outbox.drop_stream(first);

        assert_eq!(taken, None);
        assert_eq!(outbox.held(), 1);
    }

    #[test]
    fn a_closed_outbox_ends_its_stream_after_the_queued_events() {
        let mut outbox = Outbox::default();
        let stream = outbox.open_stream();
        outbox.offer(id(1), "unsubscribed");

        outbox.close();

        assert_eq!(
            take(&mut outbox, stream),
            Poll::Ready(Some((id(1), "unsubscribed")))
        );
        assert_eq!(take(&mut outbox, stream), Poll::Ready(None));
        assert!(outbox.is_finished());
    }
}
