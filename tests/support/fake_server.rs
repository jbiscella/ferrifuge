//! Scripted fake server for @D scenarios: speaks the client protocol over
//! websocket, in JSON or, when the client asks for the `centrifuge-protobuf`
//! subprotocol, in Protobuf. It lets a step drive the current connection in
//! ways a real Centrifugo cannot be made to, e.g. arbitrary close codes.
//! Prior art: `fakeserver_test.go` in centrifuge-go.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::protobuf;

/// What the fake server announces in its connect replies.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Announce {
    /// Ping interval in seconds; 0 for none.
    pub(crate) ping: u32,
    /// Whether the server expects pongs.
    pub(crate) pong: bool,
    /// Lifetime of the connection in seconds, after which the client must
    /// refresh its token; 0 for a connection that does not expire.
    pub(crate) ttl: u32,
}

/// An instruction for the current connection.
enum Control {
    Send(Message),
    /// A server ping, in the connection's encoding.
    Ping,
    Close(u16, String),
}

#[derive(Default)]
struct State {
    /// Instructions to the latest connection, if one is open.
    current: Option<mpsc::UnboundedSender<Control>>,
    connections: usize,
    /// Every JSON message received from clients, in order.
    received: Vec<Value>,
    /// Every Protobuf command received from clients, in order.
    received_protobuf: Vec<protobuf::Command>,
    /// Subscribe commands still to leave unanswered.
    ignored_subscribes: usize,
    /// Results of the replies to the next subscribe commands, in order;
    /// an empty result once none is left.
    subscribe_results: VecDeque<Value>,
    /// Server-side subscriptions of the JSON connect replies (`subs`), by
    /// channel; none when empty.
    server_subscriptions: serde_json::Map<String, Value>,
    /// Server-side subscriptions of the next connect replies only, in
    /// order, before [`State::server_subscriptions`] applies again.
    next_server_subscriptions: VecDeque<serde_json::Map<String, Value>>,
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
                // Replies go out at once rather than waiting on the
                // client's delayed acknowledgement (Nagle's algorithm).
                let _ = tcp.set_nodelay(true);
                let mut protobuf = false;
                // The callback's error type is tungstenite's, not ours.
                #[allow(clippy::result_large_err)]
                let accepted = tokio_tungstenite::accept_hdr_async(
                    tcp,
                    |request: &Request, mut response: Response| {
                        protobuf = asks_for_protobuf(request);
                        if protobuf {
                            response.headers_mut().insert(
                                "sec-websocket-protocol",
                                HeaderValue::from_static(protobuf::SUBPROTOCOL),
                            );
                        }
                        Ok(response)
                    },
                )
                .await;
                let Ok(ws) = accepted else {
                    continue;
                };
                let (control, commands) = mpsc::unbounded_channel();
                {
                    let mut state = shared.lock().unwrap();
                    state.connections += 1;
                    state.current = Some(control);
                }
                let connection = Connection {
                    ws,
                    protobuf,
                    announce,
                    state: Arc::clone(&shared),
                    client_id: 0,
                };
                tokio::spawn(connection.serve(commands));
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
                    let _ = current.send(Control::Ping);
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
        self.control(Control::Send(Message::text(frame)));
    }

    /// Sends a raw binary frame over the current connection.
    pub(crate) fn send_binary(&self, frame: Vec<u8>) {
        self.control(Control::Send(Message::binary(frame)));
    }

    /// Sends `replies` length-delimited in one binary frame, as the Protobuf
    /// encoding joins several messages.
    pub(crate) fn send_protobuf(&self, replies: &[protobuf::Reply]) {
        let mut frame = Vec::new();
        for reply in replies {
            reply
                .encode_length_delimited(&mut frame)
                .expect("encoding into a Vec cannot fail");
        }
        self.send_binary(frame);
    }

    /// Connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.state.lock().unwrap().connections
    }

    /// Every JSON message received from clients so far.
    pub(crate) fn received(&self) -> Vec<Value> {
        self.state.lock().unwrap().received.clone()
    }

    /// Leaves the next subscribe command unanswered.
    pub(crate) fn ignore_next_subscribe(&self) {
        self.state.lock().unwrap().ignored_subscribes += 1;
    }

    /// Includes `publications` in the reply to the next subscribe command.
    pub(crate) fn answer_next_subscribe_with(&self, publications: &[Value]) {
        self.answer_a_subscribe_with(json!({"publications": publications}));
    }

    /// Answers a later subscribe command with `result`: the first one not
    /// given a result yet.
    pub(crate) fn answer_a_subscribe_with(&self, result: Value) {
        self.state
            .lock()
            .unwrap()
            .subscribe_results
            .push_back(result);
    }

    /// Every Protobuf command received from clients so far.
    /// Grants the server-side subscriptions `subs`, by channel, in the JSON
    /// connect replies from now on; none for an empty object.
    pub(crate) fn grant_server_subscriptions(&self, subs: Value) {
        let Value::Object(subs) = subs else {
            panic!("subs are an object by channel: {subs}");
        };
        self.state.lock().unwrap().server_subscriptions = subs;
    }

    /// Grants `subs` in the next JSON connect reply only; later ones grant
    /// what [`FakeServer::grant_server_subscriptions`] set.
    pub(crate) fn grant_server_subscriptions_next(&self, subs: Value) {
        let Value::Object(subs) = subs else {
            panic!("subs are an object by channel: {subs}");
        };
        self.state
            .lock()
            .unwrap()
            .next_server_subscriptions
            .push_back(subs);
    }

    pub(crate) fn received_protobuf(&self) -> Vec<protobuf::Command> {
        self.state.lock().unwrap().received_protobuf.clone()
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

fn asks_for_protobuf(request: &Request) -> bool {
    request
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|protocol| protocol.trim() == protobuf::SUBPROTOCOL)
}

struct Connection {
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    protobuf: bool,
    announce: Announce,
    state: Arc<Mutex<State>>,
    client_id: u32,
}

impl Connection {
    async fn serve(mut self, mut commands: mpsc::UnboundedReceiver<Control>) {
        loop {
            tokio::select! {
                message = self.ws.next() => {
                    let replies = match message {
                        Some(Ok(Message::Text(text))) => self.json_commands(&text),
                        Some(Ok(Message::Binary(frame))) => self.protobuf_commands(&frame),
                        _ => return,
                    };
                    for reply in replies {
                        if self.ws.send(reply).await.is_err() {
                            return;
                        }
                    }
                }
                command = commands.recv() => {
                    let message = match command {
                        Some(Control::Send(message)) => message,
                        Some(Control::Ping) => self.ping(),
                        Some(Control::Close(code, reason)) => {
                            let _ = self
                                .ws
                                .close(Some(CloseFrame {
                                    code: CloseCode::from(code),
                                    reason: reason.into(),
                                }))
                                .await;
                            return;
                        }
                        None => return,
                    };
                    if self.ws.send(message).await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Records the JSON commands of a text frame; the replies to send.
    fn json_commands(&mut self, text: &str) -> Vec<Message> {
        let mut replies = Vec::new();
        for line in text.lines() {
            let Ok(message) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if message["connect"].is_object() {
                let client = self.next_client();
                let subs = {
                    let mut state = self.state.lock().unwrap();
                    let default = state.server_subscriptions.clone();
                    state
                        .next_server_subscriptions
                        .pop_front()
                        .unwrap_or(default)
                };
                replies.push(Message::text(
                    json!({
                        "id": message["id"],
                        "connect": {
                            "client": client,
                            "version": "fake",
                            "ping": self.announce.ping,
                            "pong": self.announce.pong,
                            "expires": self.announce.ttl > 0,
                            "ttl": self.announce.ttl,
                            "subs": subs,
                        },
                    })
                    .to_string(),
                ));
            } else if message["subscribe"].is_object() {
                let mut state = self.state.lock().unwrap();
                if state.ignored_subscribes > 0 {
                    state.ignored_subscribes -= 1;
                } else {
                    let result = state.subscribe_results.pop_front().unwrap_or(json!({}));
                    replies.push(Message::text(
                        json!({"id": message["id"], "subscribe": result}).to_string(),
                    ));
                }
            } else if message["unsubscribe"].is_object() {
                replies.push(Message::text(
                    json!({"id": message["id"], "unsubscribe": {}}).to_string(),
                ));
            } else if message["refresh"].is_object() {
                // The connection lives another ttl, so the client refreshes again.
                replies.push(Message::text(
                    json!({
                        "id": message["id"],
                        "refresh": {"expires": self.announce.ttl > 0, "ttl": self.announce.ttl},
                    })
                    .to_string(),
                ));
            }
            self.state.lock().unwrap().received.push(message);
        }
        replies
    }

    /// Records the Protobuf commands of a binary frame; the replies to send.
    /// A frame that does not decode is dropped.
    fn protobuf_commands(&mut self, mut frame: &[u8]) -> Vec<Message> {
        let mut replies = Vec::new();
        while !frame.is_empty() {
            let Ok(command) = protobuf::Command::decode_length_delimited(&mut frame) else {
                return replies;
            };
            if command.connect.is_some() {
                let reply = protobuf::Reply {
                    id: command.id,
                    connect: Some(protobuf::ConnectResult {
                        client: self.next_client(),
                        version: "fake".to_owned(),
                        ping: self.announce.ping,
                        pong: self.announce.pong,
                        expires: self.announce.ttl > 0,
                        ttl: self.announce.ttl,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                replies.push(Message::binary(reply.encode_length_delimited_to_vec()));
            }
            self.state.lock().unwrap().received_protobuf.push(command);
        }
        replies
    }

    fn ping(&self) -> Message {
        if self.protobuf {
            Message::binary(protobuf::Reply::default().encode_length_delimited_to_vec())
        } else {
            Message::text("{}")
        }
    }

    fn next_client(&mut self) -> String {
        self.client_id += 1;
        format!("fake-{}", self.client_id)
    }
}
