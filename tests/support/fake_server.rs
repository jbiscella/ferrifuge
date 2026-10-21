//! Scripted fake server for @D scenarios: speaks the client protocol (JSON
//! over websocket) and lets a step drive the current connection in ways a
//! real Centrifugo cannot be made to, e.g. arbitrary close codes.
//! Prior art: `fakeserver_test.go` in centrifuge-go.

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// What the fake server announces in its connect replies.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Announce {
    /// Ping interval in seconds; 0 for none.
    pub(crate) ping: u32,
    /// Whether the server expects pongs.
    pub(crate) pong: bool,
}

/// An instruction for the current connection.
enum Control {
    Send(String),
    Close(u16, String),
}

#[derive(Default)]
struct State {
    /// Instructions to the latest connection, if one is open.
    current: Option<mpsc::UnboundedSender<Control>>,
    connections: usize,
    /// Every message received from clients, in order.
    received: Vec<Value>,
}

pub(crate) struct FakeServer {
    port: u16,
    state: Arc<Mutex<State>>,
    server: JoinHandle<()>,
    pinger: Mutex<Option<JoinHandle<()>>>,
}

impl FakeServer {
    pub(crate) async fn start(announce: Announce) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake server");
        let port = listener.local_addr().expect("fake server address").port();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        let server = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
                    continue;
                };
                let (control, commands) = mpsc::unbounded_channel();
                {
                    let mut state = shared.lock().unwrap();
                    state.connections += 1;
                    state.current = Some(control);
                }
                tokio::spawn(serve(ws, commands, announce, Arc::clone(&shared)));
            }
        });
        Self {
            port,
            state,
            server,
            pinger: Mutex::new(None),
        }
    }

    /// Sends a server ping (an empty reply) to the current connection every
    /// `period`, until [`FakeServer::stop_pinging`].
    pub(crate) fn ping_every(&self, period: std::time::Duration) {
        let state = Arc::clone(&self.state);
        let pinger = tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                ticks.tick().await;
                if let Some(current) = &state.lock().unwrap().current {
                    let _ = current.send(Control::Send("{}".to_owned()));
                }
            }
        });
        if let Some(previous) = self.pinger.lock().unwrap().replace(pinger) {
            previous.abort();
        }
    }

    pub(crate) fn stop_pinging(&self) {
        if let Some(pinger) = self.pinger.lock().unwrap().take() {
            pinger.abort();
        }
    }

    /// `host:port` to connect to.
    pub(crate) fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Closes the current connection with `code` and `reason`.
    pub(crate) fn close(&self, code: u16, reason: &str) {
        self.control(Control::Close(code, reason.to_owned()));
    }

    /// Sends a raw text frame over the current connection.
    pub(crate) fn send(&self, frame: &str) {
        self.control(Control::Send(frame.to_owned()));
    }

    /// Connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.state.lock().unwrap().connections
    }

    /// Every message received from clients so far.
    pub(crate) fn received(&self) -> Vec<Value> {
        self.state.lock().unwrap().received.clone()
    }

    fn control(&self, control: Control) {
        self.state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .expect("no connection to the fake server")
            .send(control)
            .expect("fake server connection gone");
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.stop_pinging();
        self.server.abort();
    }
}

async fn serve(
    mut ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    mut commands: mpsc::UnboundedReceiver<Control>,
    announce: Announce,
    state: Arc<Mutex<State>>,
) {
    let mut client_id = 0;
    loop {
        tokio::select! {
            message = ws.next() => {
                let Some(Ok(Message::Text(text))) = message else {
                    return;
                };
                for line in text.lines() {
                    let Ok(message) = serde_json::from_str::<Value>(line) else {
                        continue;
                    };
                    let reply = message["connect"].is_object().then(|| {
                        client_id += 1;
                        json!({
                            "id": message["id"],
                            "connect": {
                                "client": format!("fake-{client_id}"),
                                "version": "fake",
                                "ping": announce.ping,
                                "pong": announce.pong,
                            },
                        })
                    });
                    state.lock().unwrap().received.push(message);
                    if let Some(reply) = reply
                        && ws.send(Message::text(reply.to_string())).await.is_err()
                    {
                        return;
                    }
                }
            }
            command = commands.recv() => match command {
                Some(Control::Send(frame)) => {
                    if ws.send(Message::text(frame)).await.is_err() {
                        return;
                    }
                }
                Some(Control::Close(code, reason)) => {
                    let _ = ws
                        .close(Some(CloseFrame {
                            code: CloseCode::from(code),
                            reason: reason.into(),
                        }))
                        .await;
                    return;
                }
                None => return,
            },
        }
    }
}
