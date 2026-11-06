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

/// How the fake server answers RPC commands (Block 13). An answer's data
/// names the method it replies to: `{"reply_to": method}`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Rpc {
    /// Each at once.
    #[default]
    Answer,
    /// In pairs, the second before the first.
    Reverse,
    /// Never.
    Ignore,
}

/// An instruction for the current connection.
enum Control {
    Send(Message),
    /// A server ping, in the connection's encoding.
    Ping,
    Close(u16, String),
    /// Stops reading from the connection, as a server that hangs while
    /// TCP stays open, until the client closes it.
    StopReading,
}

/// How often a connection that stopped reading probes the client with a
/// ping, to notice that the client closed it.
const PROBE_PERIOD: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Default)]
struct State {
    /// Instructions to the latest connection, if one is open.
    current: Option<mpsc::UnboundedSender<Control>>,
    connections: usize,
    /// Connections whose end the server has seen.
    closed: usize,
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
    /// Publications pushed with [`FakeServer::publish`], by channel.
    history: std::collections::HashMap<String, Vec<Value>>,
    /// Pings sent by [`FakeServer::ping_every`].
    pings: usize,
    /// Channels whose subscribes are answered from their history.
    recovering: std::collections::HashSet<String>,
    /// Results of the subscribe replies sent, with their channel, in order.
    subscribe_replies: Vec<(String, Value)>,
    rpc: Rpc,
    /// The RPC command whose answer waits for the next one's, in
    /// [`Rpc::Reverse`].
    held_rpc: Option<Value>,
}

/// Epoch of the channels answered from their history.
pub(crate) const HISTORY_EPOCH: &str = "history-epoch";

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
                    ws: Some(ws),
                    protobuf,
                    announce,
                    state: Arc::clone(&shared),
                    client_id: 0,
                };
                let closed = Arc::clone(&shared);
                tokio::spawn(async move {
                    connection.serve(commands).await;
                    closed.lock().unwrap().closed += 1;
                });
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
                let mut state = state.lock().unwrap();
                if let Some(current) = &state.current
                    && current.send(Control::Ping).is_ok()
                {
                    state.pings += 1;
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

    /// Connections whose end the server has seen so far.
    pub(crate) fn closed_connections(&self) -> usize {
        self.state.lock().unwrap().closed
    }

    /// Stops reading from the current connection; it sends a ping every
    /// [`PROBE_PERIOD`] until it sees the client close it.
    pub(crate) fn stop_reading(&self) {
        self.control(Control::StopReading);
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

    /// Pushes a publication at `offset` to `channel`, its data `{"n":
    /// offset}`, and keeps it for the subscribes recovering the channel.
    pub(crate) fn publish(&self, channel: &str, offset: u64) {
        self.publish_sized(channel, offset, 0);
    }

    /// As [`FakeServer::publish`], its data padded by `size` bytes.
    pub(crate) fn publish_sized(&self, channel: &str, offset: u64, size: usize) {
        let publication = json!({"data": {"n": offset, "pad": "x".repeat(size)}, "offset": offset});
        self.state
            .lock()
            .unwrap()
            .history
            .entry(channel.to_owned())
            .or_default()
            .push(publication.clone());
        self.send(&json!({"push": {"channel": channel, "pub": publication}}).to_string());
    }

    /// Answers the subscribes to `channel` as a server with history and
    /// recovery would, from the publications pushed with
    /// [`FakeServer::publish`]: positioned and recoverable at
    /// [`HISTORY_EPOCH`], and, to a subscribe recovering from an offset,
    /// with the publications after it.
    pub(crate) fn recover_from_history(&self, channel: &str) {
        self.state
            .lock()
            .unwrap()
            .recovering
            .insert(channel.to_owned());
    }

    /// Pings sent by [`FakeServer::ping_every`] so far.
    pub(crate) fn pings(&self) -> usize {
        self.state.lock().unwrap().pings
    }

    /// Answers RPC commands as `rpc` says from now on.
    pub(crate) fn answer_rpc(&self, rpc: Rpc) {
        self.state.lock().unwrap().rpc = rpc;
    }

    /// The JSON RPC commands received from clients so far.
    pub(crate) fn rpc_commands(&self) -> Vec<Value> {
        self.received()
            .into_iter()
            .filter(|message| message["rpc"].is_object())
            .collect()
    }

    /// Results of the subscribe replies sent so far, with their channel.
    pub(crate) fn subscribe_replies(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().subscribe_replies.clone()
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

/// What the writer task of a connection sends.
enum Outgoing {
    Message(Message),
    Close(CloseFrame),
}

struct Connection {
    /// Taken by [`Connection::serve`], which splits it.
    ws: Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>,
    protobuf: bool,
    announce: Announce,
    state: Arc<Mutex<State>>,
    client_id: u32,
}

impl Connection {
    /// Reads the client's commands and the test's controls, while a writer
    /// task sends the replies and pushes in order: a client that stops
    /// reading blocks the writer, never the reading of its commands.
    async fn serve(mut self, mut commands: mpsc::UnboundedReceiver<Control>) {
        let (mut sink, mut stream) = self.ws.take().expect("not served yet").split();
        let (out, mut queue) = mpsc::unbounded_channel::<Outgoing>();
        let writer = tokio::spawn(async move {
            while let Some(outgoing) = queue.recv().await {
                match outgoing {
                    Outgoing::Message(message) => {
                        if sink.send(message).await.is_err() {
                            return;
                        }
                    }
                    Outgoing::Close(frame) => {
                        let _ = sink.send(Message::Close(Some(frame))).await;
                        let _ = sink.close().await;
                        return;
                    }
                }
            }
        });
        let mut reading = true;
        let mut probes = tokio::time::interval(PROBE_PERIOD);
        loop {
            let outgoing = tokio::select! {
                message = stream.next(), if reading => {
                    let replies = match message {
                        Some(Ok(Message::Text(text))) => self.json_commands(&text),
                        Some(Ok(Message::Binary(frame))) => self.protobuf_commands(&frame),
                        _ => return,
                    };
                    if replies
                        .into_iter()
                        .any(|reply| out.send(Outgoing::Message(reply)).is_err())
                    {
                        return;
                    }
                    continue;
                }
                command = commands.recv() => match command {
                    Some(Control::Send(message)) => Outgoing::Message(message),
                    Some(Control::Ping) => Outgoing::Message(self.ping()),
                    Some(Control::StopReading) => {
                        reading = false;
                        continue;
                    }
                    Some(Control::Close(code, reason)) => {
                        let _ = out.send(Outgoing::Close(CloseFrame {
                            code: CloseCode::from(code),
                            reason: reason.into(),
                        }));
                        let _ = writer.await;
                        return;
                    }
                    None => return,
                },
                // Without reading, only a failing write tells that the
                // client has gone.
                _ = probes.tick(), if !reading => Outgoing::Message(self.ping()),
                // The writer failed: the client has gone.
                () = out.closed() => return,
            };
            if out.send(outgoing).is_err() {
                return;
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
                    let channel = message["subscribe"]["channel"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned();
                    let result = match state.subscribe_results.pop_front() {
                        Some(result) => result,
                        None if state.recovering.contains(&channel) => {
                            from_history(&state, &channel, &message["subscribe"])
                        }
                        None => json!({}),
                    };
                    state.subscribe_replies.push((channel, result.clone()));
                    replies.push(Message::text(
                        json!({"id": message["id"], "subscribe": result}).to_string(),
                    ));
                }
            } else if message["rpc"].is_object() {
                let mut state = self.state.lock().unwrap();
                match state.rpc {
                    Rpc::Answer => replies.push(rpc_answer(&message)),
                    Rpc::Reverse => match state.held_rpc.take() {
                        Some(first) => {
                            replies.push(rpc_answer(&message));
                            replies.push(rpc_answer(&first));
                        }
                        None => state.held_rpc = Some(message.clone()),
                    },
                    Rpc::Ignore => {}
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

/// The answer to the JSON RPC command `rpc`.
fn rpc_answer(rpc: &Value) -> Message {
    Message::text(
        json!({"id": rpc["id"], "rpc": {"data": {"reply_to": rpc["rpc"]["method"]}}}).to_string(),
    )
}

/// The result of a subscribe to `channel` answered from its history.
fn from_history(state: &State, channel: &str, subscribe: &Value) -> Value {
    let history = state.history.get(channel).map_or(&[][..], Vec::as_slice);
    let top = history
        .last()
        .map_or(0, |publication| publication["offset"].as_u64().unwrap_or(0));
    let mut result = json!({
        "recoverable": true,
        "positioned": true,
        "epoch": HISTORY_EPOCH,
        "offset": top,
    });
    if subscribe["recover"] == true {
        let from = subscribe["offset"].as_u64().unwrap_or(0);
        result["was_recovering"] = json!(true);
        result["recovered"] = json!(subscribe["epoch"] == HISTORY_EPOCH);
        result["publications"] = json!(
            history
                .iter()
                .filter(|publication| publication["offset"].as_u64().unwrap_or(0) > from)
                .collect::<Vec<_>>()
        );
    }
    result
}
