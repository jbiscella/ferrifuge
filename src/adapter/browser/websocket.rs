//! Websocket transport of the browser adapter, over the browser's own
//! `WebSocket` (Block 44, D6), with the bounded inbox of D7.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use futures_util::future::{Either, select};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::JsValue;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent};

use crate::adapter::platform;
use crate::machine::{FrameId, FrameType};

/// What the server sent, as far as the client protocol is concerned.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::adapter) enum Inbound {
    /// A frame of client protocol messages.
    Frame(Vec<u8>),
    /// The connection ended; `code` is absent when no close frame was received.
    Closed { code: Option<u16>, reason: String },
    /// The server sent a message over the size limit; the connection is
    /// closed (Block 29, D3).
    TooLarge,
    /// The frame was written: handed to the browser's `send`, or, over
    /// `http_stream`, its emulation request answered.
    Written(FrameId),
    /// The server refused the emulation request carrying this frame with
    /// HTTP 413; its commands fail (Block 8). `http_stream` only.
    Rejected(Vec<u8>),
    /// A message arrived while the inbox was full; the connection is closed
    /// (Block 44, D7).
    Overflow,
}

/// The bound of the inbox: `Config::read_ahead_messages` and
/// `read_ahead_bytes` (Block 15).
#[derive(Debug, Clone, Copy)]
pub(in crate::adapter) struct Bound {
    pub(in crate::adapter) messages: usize,
    pub(in crate::adapter) bytes: usize,
}

/// The messages the browser received that the client has not read yet,
/// where natively the socket's buffers hold them (Block 44, D7). A message
/// that would take it past its bound is refused; one larger than the byte
/// bound alone is admitted into an empty inbox.
#[derive(Debug, Default)]
struct Inbox {
    /// Frames, then at most one end of the connection.
    items: VecDeque<Inbound>,
    frames: usize,
    bytes: usize,
}

impl Inbox {
    /// Admits `frame`, or gives it back when it would overflow `bound`.
    fn admit(&mut self, frame: Vec<u8>, bound: Bound) -> Result<(), Vec<u8>> {
        let empty = self.frames == 0;
        if !empty && (self.frames >= bound.messages || self.bytes + frame.len() > bound.bytes) {
            return Err(frame);
        }
        self.frames += 1;
        self.bytes += frame.len();
        self.items.push_back(Inbound::Frame(frame));
        Ok(())
    }

    /// The end of the connection, read after the frames before it.
    fn end(&mut self, end: Inbound) {
        self.items.push_back(end);
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn pop(&mut self) -> Option<Inbound> {
        let item = self.items.pop_front()?;
        if let Inbound::Frame(frame) = &item {
            self.frames -= 1;
            self.bytes -= frame.len();
        }
        Some(item)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// How far the opening went.
enum Opening {
    Pending,
    Opened,
    Failed(String),
}

/// What the listeners and the transport share.
struct State {
    opening: Opening,
    inbox: Inbox,
    bound: Bound,
    max_message: usize,
    /// A close event, a message over the size limit or an overflow came:
    /// the browser's later events are not taken.
    ended: bool,
    /// The inbox overflowed, reported in place of anything else.
    overflowed: bool,
    /// The browser's `close` event came.
    closed: bool,
    waker: Option<Waker>,
}

impl State {
    fn wake(state: &RefCell<Self>) {
        let waker = state.borrow_mut().waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A listener of a browser event, by the event's name.
type Listener = (&'static str, Closure<dyn FnMut(Event)>);

/// A browser `WebSocket` and its listeners, which it removes before it
/// drops their closures, so that the browser never calls a dropped one
/// (Block 44, D6).
struct Socket {
    websocket: web_sys::WebSocket,
    listeners: Vec<Listener>,
    state: Rc<RefCell<State>>,
}

impl Socket {
    fn listen(websocket: web_sys::WebSocket, state: Rc<RefCell<State>>) -> Self {
        let mut socket = Self {
            websocket,
            listeners: Vec::new(),
            state,
        };
        let on_open = {
            let state = Rc::clone(&socket.state);
            move |_: Event| {
                state.borrow_mut().opening = Opening::Opened;
                State::wake(&state);
            }
        };
        let on_message = {
            let state = Rc::clone(&socket.state);
            let websocket = socket.websocket.clone();
            move |event: Event| {
                received(&state, &websocket, event.unchecked_ref::<MessageEvent>());
                State::wake(&state);
            }
        };
        let on_close = {
            let state = Rc::clone(&socket.state);
            move |event: Event| {
                closed(&state, event.unchecked_ref::<CloseEvent>());
                State::wake(&state);
            }
        };
        socket.add("open", Box::new(on_open));
        socket.add("message", Box::new(on_message));
        socket.add("close", Box::new(on_close));
        socket
    }

    fn add(&mut self, name: &'static str, listener: Box<dyn FnMut(Event)>) {
        let closure = Closure::wrap(listener);
        let _ = self
            .websocket
            .add_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        self.listeners.push((name, closure));
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        let listeners = std::mem::take(&mut self.listeners);
        for (name, closure) in &listeners {
            let _ = self
                .websocket
                .remove_event_listener_with_callback(name, closure.as_ref().unchecked_ref());
        }
        drop(listeners);
        // No code (Block 8); a no-op on a socket closed already.
        let _ = self.websocket.close();
    }
}

/// A `message` event: its data, copied out, into the inbox.
fn received(state: &RefCell<State>, websocket: &web_sys::WebSocket, event: &MessageEvent) {
    let mut state = state.borrow_mut();
    if state.ended {
        return;
    }
    let data = event.data();
    let frame = if let Some(buffer) = data.dyn_ref::<js_sys::ArrayBuffer>() {
        // Refused before it is copied.
        if buffer.byte_length() as usize > state.max_message {
            None
        } else {
            Some(js_sys::Uint8Array::new(buffer).to_vec())
        }
    } else if let Some(text) = data.as_string() {
        Some(text.into_bytes()).filter(|frame| frame.len() <= state.max_message)
    } else {
        // Neither text nor an `ArrayBuffer`, which `binaryType` rules out.
        state.inbox.end(Inbound::Closed {
            code: None,
            reason: "unexpected websocket message data".to_owned(),
        });
        state.ended = true;
        let _ = websocket.close();
        return;
    };
    let Some(frame) = frame else {
        state.inbox.end(Inbound::TooLarge);
        state.ended = true;
        let _ = websocket.close();
        return;
    };
    let bound = state.bound;
    if state.inbox.admit(frame, bound).is_err() {
        // What it held was not read: recovery brings it back (D7).
        state.inbox.clear();
        state.overflowed = true;
        state.ended = true;
        let _ = websocket.close();
    }
}

/// A `close` event: the opening fails, or the connection ends after the
/// frames received before it.
fn closed(state: &RefCell<State>, event: &CloseEvent) {
    let mut state = state.borrow_mut();
    state.closed = true;
    if matches!(state.opening, Opening::Pending) {
        // The browser hides the upgrade's response (D6).
        state.opening = Opening::Failed(format!(
            "websocket closed before open (code {})",
            event.code()
        ));
        return;
    }
    if state.ended {
        return;
    }
    state.ended = true;
    // 1005 and 1006 are the browser's for a close frame without a code and
    // for none at all.
    let code = Some(event.code()).filter(|code| !matches!(code, 1005 | 1006));
    state.inbox.end(Inbound::Closed {
        code,
        reason: event.reason(),
    });
}

/// The message of a JavaScript exception.
pub(super) fn js_message(error: &JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|error| String::from(error.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"))
}

/// An open websocket connection to the server.
///
/// The browser queues what `send` is given, so a frame counts as written at
/// once; the messages it receives wait in the inbox until the client reads
/// them (Block 44, D6, D7).
pub(in crate::adapter) struct WebSocket {
    socket: Socket,
    /// Frames sent and not yet reported by [`WebSocket::recv`].
    written: VecDeque<FrameId>,
    /// The last report was of a frame written: a read comes first next.
    read_next: bool,
    /// Bound on closing (`Config::timeout`).
    timeout: Duration,
}

impl WebSocket {
    /// Opens a connection to `url`, asking for `subprotocol` if any. A
    /// message over `max_message` bytes ends it, reported as
    /// [`Inbound::TooLarge`]; one that would take the inbox past `bound`
    /// ends it too, reported as [`Inbound::Overflow`].
    pub(in crate::adapter) async fn open(
        url: &str,
        subprotocol: Option<&str>,
        timeout: Duration,
        max_message: usize,
        bound: Bound,
    ) -> Result<Self, String> {
        // A constructor that throws is a failed attempt (D6).
        let websocket = match subprotocol {
            Some(subprotocol) => web_sys::WebSocket::new_with_str(url, subprotocol),
            None => web_sys::WebSocket::new(url),
        }
        .map_err(|error| js_message(&error))?;
        websocket.set_binary_type(BinaryType::Arraybuffer);
        let state = Rc::new(RefCell::new(State {
            opening: Opening::Pending,
            inbox: Inbox::default(),
            bound,
            max_message,
            ended: false,
            overflowed: false,
            closed: false,
            waker: None,
        }));
        // Dropped with the attempt, it closes the socket.
        let socket = Socket::listen(websocket, Rc::clone(&state));
        std::future::poll_fn(|cx| {
            let mut state = state.borrow_mut();
            match std::mem::replace(&mut state.opening, Opening::Opened) {
                Opening::Opened => Poll::Ready(Ok(())),
                Opening::Failed(message) => Poll::Ready(Err(message)),
                Opening::Pending => {
                    state.opening = Opening::Pending;
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await?;
        Ok(Self {
            socket,
            written: VecDeque::new(),
            read_next: false,
            timeout,
        })
    }

    /// Hands `frame` to the browser: JSON as text, Protobuf as binary.
    pub(in crate::adapter) fn send(
        &mut self,
        id: FrameId,
        frame: &[u8],
        frame_type: FrameType,
    ) -> Result<(), String> {
        let websocket = &self.socket.websocket;
        match frame_type {
            FrameType::Text => {
                let text = std::str::from_utf8(frame).map_err(|e| e.to_string())?;
                websocket.send_with_str(text)
            }
            FrameType::Binary => websocket.send_with_u8_array(frame),
        }
        .map_err(|error| js_message(&error))?;
        self.written.push_back(id);
        Ok(())
    }

    /// The next frame received, the end of the connection, or a frame
    /// written. Without `reading`, only frames written and an overflow are
    /// reported: received ones stay in the inbox. Cancel-safe.
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        let Self {
            socket,
            written,
            read_next,
            ..
        } = self;
        std::future::poll_fn(|cx| {
            let mut state = socket.state.borrow_mut();
            if state.overflowed {
                return Poll::Ready(Inbound::Overflow);
            }
            let frame_first = reading && !state.inbox.is_empty() && *read_next;
            if !frame_first && let Some(id) = written.pop_front() {
                *read_next = true;
                return Poll::Ready(Inbound::Written(id));
            }
            if reading && let Some(item) = state.inbox.pop() {
                *read_next = false;
                return Poll::Ready(item);
            }
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Closes the connection with no code (Block 8), awaiting the browser's
    /// `close` event for at most `Config::timeout` (Block 33, D1); the
    /// listeners are removed and their closures dropped after.
    pub(in crate::adapter) async fn close(self) {
        let Self {
            socket, timeout, ..
        } = self;
        let _ = socket.websocket.close();
        let state = Rc::clone(&socket.state);
        let closed = std::future::poll_fn(|cx| {
            let mut state = state.borrow_mut();
            if state.closed {
                Poll::Ready(())
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        });
        let timer = platform::sleep(timeout);
        if let Either::Right(_) = select(std::pin::pin!(closed), std::pin::pin!(timer)).await {
            tracing::debug!("websocket close event not received in time");
        }
        drop(socket);
    }
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{Bound, Inbound, Inbox};

    const BOUND: Bound = Bound {
        messages: 3,
        bytes: 10,
    };

    #[wasm_bindgen_test]
    fn the_inbox_holds_up_to_its_message_bound() {
        let mut inbox = Inbox::default();
        for _ in 0..3 {
            assert_eq!(inbox.admit(vec![1], BOUND), Ok(()));
        }
        assert_eq!(inbox.admit(vec![2], BOUND), Err(vec![2]));
        assert_eq!(inbox.pop(), Some(Inbound::Frame(vec![1])));
        assert_eq!(inbox.admit(vec![2], BOUND), Ok(()));
    }

    #[wasm_bindgen_test]
    fn the_inbox_holds_up_to_its_byte_bound() {
        let mut inbox = Inbox::default();
        assert_eq!(inbox.admit(vec![0; 6], BOUND), Ok(()));
        assert_eq!(inbox.admit(vec![0; 4], BOUND), Ok(()));
        assert_eq!(inbox.admit(vec![0; 1], BOUND), Err(vec![0; 1]));
        inbox.pop();
        assert_eq!(inbox.admit(vec![0; 6], BOUND), Ok(()));
    }

    #[wasm_bindgen_test]
    fn a_message_over_the_byte_bound_is_admitted_alone_into_an_empty_inbox() {
        let mut inbox = Inbox::default();
        assert_eq!(inbox.admit(vec![0; 11], BOUND), Ok(()));
        assert_eq!(inbox.admit(vec![0; 1], BOUND), Err(vec![0; 1]));
        inbox.pop();
        assert_eq!(inbox.admit(vec![0; 1], BOUND), Ok(()));
        assert_eq!(inbox.admit(vec![0; 11], BOUND), Err(vec![0; 11]));
    }

    #[wasm_bindgen_test]
    fn the_end_of_the_connection_is_read_after_the_frames_before_it() {
        let mut inbox = Inbox::default();
        inbox.admit(vec![1], BOUND).unwrap();
        inbox.end(Inbound::TooLarge);
        assert_eq!(inbox.pop(), Some(Inbound::Frame(vec![1])));
        assert_eq!(inbox.pop(), Some(Inbound::TooLarge));
        assert!(inbox.is_empty());
    }
}
