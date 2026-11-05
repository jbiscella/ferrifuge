//! Event streams of the tokio adapter: each stream takes its events from a
//! queue the client fills.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;

/// Events queued for one stream.
#[derive(Debug)]
struct Queue<E> {
    events: VecDeque<E>,
    /// The stream waiting for an event.
    waker: Option<Waker>,
    /// No event will come: the stream yields what it holds, then ends.
    closed: bool,
}

impl<E> Queue<E> {
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// Creates a stream and the sender that fills it.
pub(super) fn channel<E>() -> (Sender<E>, Receiver<E>) {
    let queue = Arc::new(Mutex::new(Queue {
        events: VecDeque::new(),
        waker: None,
        closed: false,
    }));
    (Sender(Arc::clone(&queue)), Receiver(queue))
}

/// Queues events for one stream; dropping it ends the stream once the
/// events it holds are taken.
#[derive(Debug)]
pub(super) struct Sender<E>(Arc<Mutex<Queue<E>>>);

impl<E> Sender<E> {
    /// Queues `event`; false once the stream is dropped.
    pub(super) fn send(&self, event: E) -> bool {
        if Arc::strong_count(&self.0) == 1 {
            return false;
        }
        let mut queue = lock(&self.0);
        queue.events.push_back(event);
        queue.wake();
        true
    }

    /// Events queued and not yet taken.
    #[cfg(any(test, feature = "test-util"))]
    pub(super) fn held(&self) -> usize {
        lock(&self.0).events.len()
    }
}

impl<E> Drop for Sender<E> {
    fn drop(&mut self) {
        let mut queue = lock(&self.0);
        queue.closed = true;
        queue.wake();
    }
}

/// The application's end: yields the queued events in order.
#[derive(Debug)]
pub(super) struct Receiver<E>(Arc<Mutex<Queue<E>>>);

impl<E> Stream for Receiver<E> {
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<E>> {
        let mut queue = lock(&self.0);
        if let Some(event) = queue.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        if queue.closed {
            return Poll::Ready(None);
        }
        queue.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;

    #[tokio::test]
    async fn yields_in_order_then_ends_when_the_sender_is_dropped() {
        let (sender, mut receiver) = channel();

        assert!(sender.send(1));
        assert!(sender.send(2));
        let held = sender.held();
        drop(sender);

        assert_eq!(held, 2);
        assert_eq!(receiver.next().await, Some(1));
        assert_eq!(receiver.next().await, Some(2));
        assert_eq!(receiver.next().await, None);
    }

    #[test]
    fn a_dropped_stream_takes_nothing_more() {
        let (sender, receiver) = channel();
        drop(receiver);

        assert!(!sender.send(1));
    }
}
