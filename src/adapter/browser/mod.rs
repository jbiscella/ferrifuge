//! What the browser driver needs beyond the shared adapter layer: the
//! browser's WebSocket and the inbox that bounds what it holds,
//! `http_stream` over fetch, sse over `EventSource`, and the application's
//! shims of them (Block 44, D2, D6 to D9, D12).

pub(super) mod events;
pub(super) mod http_stream;
pub(super) mod shims;
pub(super) mod sse;
pub(super) mod websocket;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use self::http_stream::HttpStream;
use self::shims::Found;
use self::sse::Sse;
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
    /// The constructors this attempt found, which can open `kind` (D12).
    pub(in crate::adapter) found: Found,
    /// Whether HTTP requests carry the credentials of another origin (D13).
    pub(in crate::adapter) include_credentials: bool,
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
        found,
        include_credentials,
    } = open;
    log.attempt(kind, endpoint);
    // The driver opens only what `found` can (D12).
    let missing = move || {
        format!(
            "the page has no constructor for the {} transport",
            kind.name()
        )
    };
    match kind {
        // Never usable before Block 48c, so never opened (D12).
        #[cfg(feature = "unstable")]
        TransportKind::WebTransport => Box::pin(async move { Err(missing()) }),
        TransportKind::WebSocket => Box::pin(async move {
            let constructor = found.websocket.ok_or_else(missing)?;
            WebSocket::open(
                &constructor,
                &url,
                subprotocol,
                timeout,
                max_message,
                read_ahead,
            )
            .await
            .map(|websocket| Transport::WebSocket(Box::new(websocket)))
        }),
        TransportKind::HttpStream => {
            let protobuf = subprotocol.is_some();
            Box::pin(async move {
                HttpStream::open(
                    found.fetch.ok_or_else(missing)?,
                    found.readable_stream.as_ref(),
                    &url,
                    initial_frame.unwrap_or_default(),
                    emulation.as_deref(),
                    timeout,
                    protobuf,
                    max_message,
                    read_ahead,
                    include_credentials,
                )
                .await
                .map(|http| Transport::HttpStream(Box::new(http)))
            })
        }
        TransportKind::Sse => Box::pin(async move {
            Sse::open(
                &found.event_source.ok_or_else(missing)?,
                found.fetch.ok_or_else(missing)?,
                &url,
                &initial_frame.unwrap_or_default(),
                emulation.as_deref(),
                timeout,
                max_message,
                read_ahead,
                include_credentials,
            )
            .await
            .map(|sse| Transport::Sse(Box::new(sse)))
        }),
    }
}

/// An open transport of any kind.
pub(in crate::adapter) enum Transport {
    // Boxed: the transports differ much in size, and one is held at a time.
    WebSocket(Box<WebSocket>),
    HttpStream(Box<HttpStream>),
    Sse(Box<Sse>),
}

impl Transport {
    /// Sends `frame`: over websocket it counts as written at once (Block
    /// 44, D6), over `http_stream` and sse once its emulation request is
    /// answered.
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
            Self::Sse(sse) => {
                sse.send(id, frame);
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
            Self::Sse(sse) => sse.recv(reading).await,
        }
    }

    /// Closes the transport on a task of its own, bounded by
    /// `Config::timeout` (Block 8); the task, which the driver awaits when it
    /// stops (Block 33, D1).
    pub(in crate::adapter) fn close(self, runtime: &Handle) -> JoinHandle<()> {
        match self {
            Self::WebSocket(websocket) => super::platform::spawn(runtime, websocket.close()),
            Self::HttpStream(http) => super::platform::spawn(runtime, async move { http.close() }),
            Self::Sse(sse) => super::platform::spawn(runtime, async move { sse.close() }),
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
