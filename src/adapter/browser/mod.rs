//! What the browser driver needs beyond the shared adapter layer: the
//! browser's WebSocket and the inbox that bounds what it holds (Block 44,
//! D2, D6, D7).

pub(super) mod websocket;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub(in crate::adapter) use self::websocket::Inbound;
use self::websocket::WebSocket;
use super::log::Log;
use super::platform::{Handle, JoinHandle};
use crate::machine::{FrameId, FrameType, TransportKind};

/// A transport being opened.
pub(in crate::adapter) type Opening = Pin<Box<dyn Future<Output = Result<Transport, String>>>>;

/// What the driver gives to open a transport.
pub(in crate::adapter) struct Open {
    pub(in crate::adapter) kind: TransportKind,
    /// The endpoint's index, for the records.
    pub(in crate::adapter) endpoint: usize,
    pub(in crate::adapter) url: String,
    /// Websocket subprotocol of the configured encoding, if it needs one.
    pub(in crate::adapter) subprotocol: Option<&'static str>,
    pub(in crate::adapter) timeout: Duration,
    pub(in crate::adapter) max_message: usize,
    /// The bound of the inbox: the read-ahead's own (Block 44, D7).
    pub(in crate::adapter) read_ahead: websocket::Bound,
}

/// Opens a transport of `open.kind`, its attempt recorded in `log`.
pub(in crate::adapter) fn open(open: Open, log: &Log) -> Opening {
    let Open {
        kind,
        endpoint,
        url,
        subprotocol,
        timeout,
        max_message,
        read_ahead,
    } = open;
    log.attempt(kind, endpoint);
    match kind {
        TransportKind::WebSocket => Box::pin(async move {
            WebSocket::open(&url, subprotocol, timeout, max_message, read_ahead)
                .await
                .map(|websocket| Transport::WebSocket(Box::new(websocket)))
        }),
        // Over fetch and EventSource in Blocks 44d and 44e.
        TransportKind::HttpStream | TransportKind::Sse => Box::pin(async move {
            Err(format!(
                "{} is not available in the browser yet",
                kind.name()
            ))
        }),
    }
}

/// An open transport of any kind.
pub(in crate::adapter) enum Transport {
    WebSocket(Box<WebSocket>),
}

impl Transport {
    /// Sends `frame`; it counts as written at once (Block 44, D6).
    #[allow(clippy::needless_pass_by_value)] // The native transport's signature.
    pub(in crate::adapter) fn send(
        &mut self,
        id: FrameId,
        frame: Vec<u8>,
        frame_type: FrameType,
    ) -> Result<(), String> {
        match self {
            Self::WebSocket(websocket) => websocket.send(id, &frame, frame_type),
        }
    }

    /// Cancel-safe. Without `reading`, only writes and an overflow of the
    /// inbox are reported (Block 44, D7).
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv(reading).await,
        }
    }

    /// Closes the transport on a task of its own, bounded by
    /// `Config::timeout` (Block 8); the task, which the driver awaits when it
    /// stops (Block 33, D1).
    pub(in crate::adapter) fn close(self, runtime: &Handle) -> JoinHandle<()> {
        match self {
            Self::WebSocket(websocket) => super::platform::spawn(runtime, websocket.close()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use wasm_bindgen::JsValue;
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::Client;

    // Block 44, D3: a token callback may hold what is not `Send` and await
    // a JavaScript promise, as one calling `fetch` does.
    #[wasm_bindgen_test]
    fn a_token_callback_holding_an_rc_may_await_a_js_future() {
        let token = Rc::new("t".to_owned());
        let client = Client::builder("ws://127.0.0.1:1/connection/websocket")
            .get_token(move || {
                let token = Rc::clone(&token);
                async move {
                    JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL))
                        .await
                        .map_err(|_| crate::TokenError::Failed("rejected".into()))?;
                    Ok(token.to_string())
                }
            })
            .build();

        assert!(client.is_ok());
    }
}
