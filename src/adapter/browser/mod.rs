//! What the browser driver needs beyond the shared adapter layer: the
//! browser's WebSocket and the inbox that bounds what it holds, and
//! `http_stream` over fetch (Block 44, D2, D6, D7, D8).

pub(super) mod events;
pub(super) mod http_stream;
pub(super) mod websocket;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use self::http_stream::HttpStream;
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
    /// Where commands over emulated transports go, when not the default.
    pub(in crate::adapter) emulation: Option<String>,
    /// The commands an emulated transport's stream request carries.
    pub(in crate::adapter) initial_frame: Option<Vec<u8>>,
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
        emulation,
        initial_frame,
    } = open;
    log.attempt(kind, endpoint);
    match kind {
        TransportKind::WebSocket => Box::pin(async move {
            WebSocket::open(&url, subprotocol, timeout, max_message, read_ahead)
                .await
                .map(|websocket| Transport::WebSocket(Box::new(websocket)))
        }),
        TransportKind::HttpStream => {
            let protobuf = subprotocol.is_some();
            Box::pin(async move {
                HttpStream::open(
                    http_stream::global_fetch()?,
                    &url,
                    initial_frame.unwrap_or_default(),
                    emulation.as_deref(),
                    timeout,
                    protobuf,
                    max_message,
                )
                .await
                .map(|http| Transport::HttpStream(Box::new(http)))
            })
        }
        // Over EventSource in Block 44e.
        TransportKind::Sse => Box::pin(async move {
            Err(format!(
                "{} is not available in the browser yet",
                kind.name()
            ))
        }),
    }
}

/// An open transport of any kind.
pub(in crate::adapter) enum Transport {
    // Boxed: the transports differ much in size, and one is held at a time.
    WebSocket(Box<WebSocket>),
    HttpStream(Box<HttpStream>),
}

impl Transport {
    /// Sends `frame`: over websocket it counts as written at once (Block
    /// 44, D6), over `http_stream` once its emulation request is answered.
    pub(in crate::adapter) fn send(
        &mut self,
        id: FrameId,
        frame: Vec<u8>,
        frame_type: FrameType,
    ) -> Result<(), String> {
        match self {
            Self::WebSocket(websocket) => websocket.send(id, &frame, frame_type),
            // Failures end the transport through `recv`.
            Self::HttpStream(http) => {
                http.send(id, frame);
                Ok(())
            }
        }
    }

    /// Cancel-safe. Without `reading`, only writes and an overflow of the
    /// inbox are reported (Block 44, D7).
    pub(in crate::adapter) async fn recv(&mut self, reading: bool) -> Inbound {
        match self {
            Self::WebSocket(websocket) => websocket.recv(reading).await,
            Self::HttpStream(http) => http.recv(reading).await,
        }
    }

    /// Closes the transport on a task of its own, bounded by
    /// `Config::timeout` (Block 8); the task, which the driver awaits when it
    /// stops (Block 33, D1).
    pub(in crate::adapter) fn close(self, runtime: &Handle) -> JoinHandle<()> {
        match self {
            Self::WebSocket(websocket) => super::platform::spawn(runtime, websocket.close()),
            Self::HttpStream(http) => super::platform::spawn(runtime, async move { http.close() }),
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
