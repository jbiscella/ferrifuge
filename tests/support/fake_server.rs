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
    /// Each at once, with the data it carried (Block 21).
    Echo,
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
    /// What the latest connection writes, in order. Frames the server sends
    /// on its own (pushes, publications) and subscribe replies answered from
    /// history go here while the state is locked, so that the client
    /// receives them in the order they change the history.
    out: Option<mpsc::UnboundedSender<Outgoing>>,
    connections: usize,
    /// Connections whose end the server has seen.
    closed: usize,
    /// Answers every websocket upgrade with this HTTP status and a web
    /// page, as a captive portal does (Block 25, D8).
    portal_status: Option<u16>,
    /// Upgrades answered so.
    portal_answers: usize,
    /// Every JSON message received from clients, in order.
    received: Vec<Value>,
    /// The frame each message of `received` arrived in, numbered from 0
    /// over JSON and Protobuf frames alike (Block 18).
    received_frames: Vec<usize>,
    /// The frame each command of `received_protobuf` arrived in.
    received_protobuf_frames: Vec<usize>,
    /// Frames received so far.
    frames: usize,
    /// Every Protobuf command received from clients, in order.
    received_protobuf: Vec<protobuf::Command>,
    /// Subscribe commands still to leave unanswered.
    ignored_subscribes: usize,
    /// Every subscribe command is left unanswered.
    ignore_subscribes: bool,
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
    /// When each message of `received` arrived.
    received_at: Vec<std::time::Instant>,
    /// Subscribe commands are answered after this delay, with this error
    /// when one is given.
    subscribe_delay: Option<(std::time::Duration, Option<Value>)>,
    /// When each delayed subscribe answer was sent.
    subscribes_answered_at: Vec<std::time::Instant>,
    /// Per channel, the offset up to which the history has expired.
    expired: std::collections::HashMap<String, u64>,
    /// Publications a recovery may bring back at most; more fail it.
    recovery_limit: Option<u64>,
    /// A recovery of two publications or more skips the first: a planted
    /// fault for the chaos runner (Block 16).
    skip_recovered: bool,
    /// Connect commands still to answer with a temporary error.
    failing_connects: usize,
    /// Errors to answer the next connect commands with, in order (Block 17).
    connect_errors: VecDeque<Value>,
    /// Raw JSON the JSON connect replies carry as `data`, byte for byte
    /// (Block 17).
    connect_data: Option<String>,
    /// The next JSON connect command is answered with a text message of
    /// this many bytes (Block 29).
    sized_connect: Option<usize>,
    /// Bytes and JSON messages written to clients, and bytes and JSON
    /// commands read from them (Block 31).
    sent_traffic: (u64, u64),
    received_traffic: (u64, u64),
    /// When a test last asked the server to close the connection.
    closed_at: Option<std::time::SystemTime>,
    /// Channels whose subscribes asking for fossil delta get it (Block 19).
    delta_channels: std::collections::HashSet<String>,
    /// Per channel with delta on for the latest connection, the payload the
    /// next delta applies to; cleared at each subscribe and connection.
    delta_chains: std::collections::HashMap<String, Option<String>>,
    /// The next delta sent goes without the end of its trailer.
    corrupt_next_delta: bool,
    /// Deltas sent broken so far.
    corrupted_deltas: usize,
}

impl State {
    /// `publication` as the latest connection receives it: unchanged, or on
    /// a channel with delta on its data as a JSON string holding the
    /// payload or a delta against the previous one (Block 19).
    fn with_delta(&mut self, channel: &str, mut publication: Value) -> Value {
        let Some(base) = self.delta_chains.get(channel).cloned() else {
            return publication;
        };
        let payload = publication["data"].to_string();
        self.delta_chains
            .insert(channel.to_owned(), Some(payload.clone()));
        let Some(base) = base else {
            publication["data"] = Value::String(payload);
            return publication;
        };
        let mut delta = String::from_utf8(ferrifuge::write_fossil_delta(
            base.as_bytes(),
            payload.as_bytes(),
        ))
        .expect("an ASCII delta");
        if std::mem::take(&mut self.corrupt_next_delta) {
            delta.pop();
            self.corrupted_deltas += 1;
        }
        publication["data"] = Value::String(delta);
        publication["delta"] = json!(true);
        publication
    }
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
                // Written on the stream itself: tungstenite's server
                // refuses a 2xx error response.
                let portal = {
                    let mut state = shared.lock().unwrap();
                    state.portal_status.inspect(|_| state.portal_answers += 1)
                };
                if let Some(status) = portal {
                    tokio::spawn(answer_as_a_portal(tcp, status));
                    continue;
                }
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
                let (out, queue) = mpsc::unbounded_channel();
                {
                    let mut state = shared.lock().unwrap();
                    state.connections += 1;
                    state.current = Some(control);
                    state.out = Some(out.clone());
                    state.delta_chains.clear();
                }
                let connection = Connection {
                    ws: Some(ws),
                    out,
                    queue: Some(queue),
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
        self.state.lock().unwrap().closed_at = Some(std::time::SystemTime::now());
        self.control(Control::Close(code, reason.to_owned()));
    }

    /// When a test last asked the server to close the connection.
    pub(crate) fn closed_at(&self) -> Option<std::time::SystemTime> {
        self.state.lock().unwrap().closed_at
    }

    /// Sends a raw text frame over the current connection.
    pub(crate) fn send(&self, frame: &str) {
        self.enqueue(Message::text(frame));
    }

    /// Sends a raw binary frame over the current connection.
    pub(crate) fn send_binary(&self, frame: Vec<u8>) {
        self.enqueue(Message::binary(frame));
    }

    /// Queues `message` for the current connection's writer, in order with
    /// publications and history replies.
    fn enqueue(&self, message: Message) {
        self.state
            .lock()
            .unwrap()
            .out
            .as_ref()
            .expect("no connection to the fake server")
            .send(Outgoing::Message(message))
            .expect("fake server connection gone");
    }

    /// Closes the current connection with `code` and `reason`, if there is
    /// one; whether there was.
    pub(crate) fn close_current(&self, code: u16, reason: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .is_some_and(|current| {
                current
                    .send(Control::Close(code, reason.to_owned()))
                    .is_ok()
            })
    }

    /// The history of `channel` before its current last offset expires:
    /// a recovery from before it fails.
    pub(crate) fn expire_history(&self, channel: &str) {
        let mut state = self.state.lock().unwrap();
        let top = state
            .history
            .get(channel)
            .and_then(|history| history.last())
            .map_or(0, |publication| publication["offset"].as_u64().unwrap_or(0));
        state.expired.insert(channel.to_owned(), top);
    }

    /// A recovery of more than `limit` publications fails, as with
    /// Centrifugo's `client.recovery_max_publication_limit`.
    pub(crate) fn limit_recovery(&self, limit: u64) {
        self.state.lock().unwrap().recovery_limit = Some(limit);
    }

    /// Recoveries of two publications or more skip the first (a planted
    /// fault for the chaos runner).
    pub(crate) fn skip_recovered_offset(&self) {
        self.state.lock().unwrap().skip_recovered = true;
    }

    /// JSON connect replies carry `raw` as their `data`, byte for byte.
    pub(crate) fn answer_connects_with_data(&self, raw: &str) {
        self.state.lock().unwrap().connect_data = Some(raw.to_owned());
    }

    /// Answers the next JSON connect command with a connect reply padded to
    /// a websocket message of exactly `size` bytes (Block 29).
    pub(crate) fn answer_next_connect_sized(&self, size: usize) {
        self.state.lock().unwrap().sized_connect = Some(size);
    }

    /// Answers the next connect command with error `code` and `message`.
    pub(crate) fn fail_next_connect_with(&self, code: u32, message: &str) {
        self.state
            .lock()
            .unwrap()
            .connect_errors
            .push_back(json!({"code": code, "message": message}));
    }

    /// Answers the next `count` connect commands with a temporary error.
    pub(crate) fn fail_connects(&self, count: usize) {
        self.state.lock().unwrap().failing_connects = count;
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

    /// Bytes and JSON messages written to clients so far (Block 31).
    pub(crate) fn sent_traffic(&self) -> (u64, u64) {
        self.state.lock().unwrap().sent_traffic
    }

    /// Bytes and JSON commands read from clients so far (Block 31).
    pub(crate) fn received_traffic(&self) -> (u64, u64) {
        self.state.lock().unwrap().received_traffic
    }

    /// Connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.state.lock().unwrap().connections
    }

    /// Answers every websocket upgrade from now on with `status` and a web
    /// page, as a captive portal does.
    pub(crate) fn answer_upgrades_as_a_portal(&self, status: u16) {
        self.state.lock().unwrap().portal_status = Some(status);
    }

    /// Upgrades answered as a captive portal so far.
    pub(crate) fn portal_answers(&self) -> usize {
        self.state.lock().unwrap().portal_answers
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

    /// Every JSON message received so far, with the frame it arrived in.
    pub(crate) fn received_with_frames(&self) -> Vec<(Value, usize)> {
        let state = self.state.lock().unwrap();
        state
            .received
            .iter()
            .cloned()
            .zip(state.received_frames.iter().copied())
            .collect()
    }

    /// Every Protobuf command received so far, with the frame it arrived in.
    pub(crate) fn received_protobuf_with_frames(&self) -> Vec<(protobuf::Command, usize)> {
        let state = self.state.lock().unwrap();
        state
            .received_protobuf
            .iter()
            .cloned()
            .zip(state.received_protobuf_frames.iter().copied())
            .collect()
    }

    /// Leaves the next subscribe command unanswered.
    pub(crate) fn ignore_next_subscribe(&self) {
        self.state.lock().unwrap().ignored_subscribes += 1;
    }

    /// Leaves every subscribe command unanswered.
    pub(crate) fn ignore_subscribes(&self) {
        self.state.lock().unwrap().ignore_subscribes = true;
    }

    /// Answers subscribe commands after `delay`: with `error` (an `Error`
    /// object) when given, else with an empty result.
    pub(crate) fn answer_subscribes_after(&self, delay: std::time::Duration, error: Option<Value>) {
        self.state.lock().unwrap().subscribe_delay = Some((delay, error));
    }

    /// When each delayed subscribe answer was sent.
    pub(crate) fn subscribes_answered_at(&self) -> Vec<std::time::Instant> {
        self.state.lock().unwrap().subscribes_answered_at.clone()
    }

    /// The JSON commands received with `method` set, with when each arrived.
    pub(crate) fn commands_with_times(&self, method: &str) -> Vec<(Value, std::time::Instant)> {
        let state = self.state.lock().unwrap();
        state
            .received
            .iter()
            .zip(&state.received_at)
            .filter(|(message, _)| message[method].is_object())
            .map(|(message, at)| (message.clone(), *at))
            .collect()
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
        let mut state = self.state.lock().unwrap();
        state
            .history
            .entry(channel.to_owned())
            .or_default()
            .push(publication.clone());
        // With the state locked: a subscribe answered from history is
        // queued before this push, or includes this publication.
        if state.out.is_some() {
            let publication = state.with_delta(channel, publication);
            let out = state.out.as_ref().expect("checked above");
            let push = json!({"push": {"channel": channel, "pub": publication}});
            let _ = out.send(Outgoing::Message(Message::text(push.to_string())));
        }
    }

    /// Turns fossil delta on for the subscribes to `channel` asking for it
    /// (Block 19).
    pub(crate) fn delta_on(&self, channel: &str) {
        self.state
            .lock()
            .unwrap()
            .delta_channels
            .insert(channel.to_owned());
    }

    /// Sends the next delta without the end of its trailer, so that the
    /// client cannot apply it (Block 19).
    pub(crate) fn corrupt_next_delta(&self) {
        self.state.lock().unwrap().corrupt_next_delta = true;
    }

    /// Deltas sent broken so far.
    pub(crate) fn corrupted_deltas(&self) -> usize {
        self.state.lock().unwrap().corrupted_deltas
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

/// Aborts its task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What the writer task of a connection sends.
enum Outgoing {
    Message(Message),
    Close(CloseFrame),
}

struct Connection {
    /// Taken by [`Connection::serve`], which splits it.
    ws: Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>,
    /// What the writer task sends, in order.
    out: mpsc::UnboundedSender<Outgoing>,
    /// Taken by [`Connection::serve`]'s writer task.
    queue: Option<mpsc::UnboundedReceiver<Outgoing>>,
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
        let out = self.out.clone();
        let mut queue = self.queue.take().expect("not served yet");
        let counted = Arc::clone(&self.state);
        // Aborted when the connection ends: `State::out` keeps a sender, so
        // the writer would otherwise wait on its queue forever.
        let mut writer = AbortOnDrop(tokio::spawn(async move {
            while let Some(outgoing) = queue.recv().await {
                match outgoing {
                    Outgoing::Message(message) => {
                        let (bytes, messages) = json_size(&message);
                        if sink.send(message).await.is_err() {
                            return;
                        }
                        let mut state = counted.lock().unwrap();
                        state.sent_traffic.0 += bytes;
                        state.sent_traffic.1 += messages;
                    }
                    Outgoing::Close(frame) => {
                        let _ = sink.send(Message::Close(Some(frame))).await;
                        let _ = sink.close().await;
                        return;
                    }
                }
            }
        }));
        let mut reading = true;
        let mut probes = tokio::time::interval(PROBE_PERIOD);
        loop {
            let outgoing = tokio::select! {
                message = stream.next(), if reading => {
                    if let Some(Ok(message)) = &message {
                        let (bytes, commands) = json_size(message);
                        let mut state = self.state.lock().unwrap();
                        state.received_traffic.0 += bytes;
                        state.received_traffic.1 += commands;
                    }
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
                        let _ = (&mut writer.0).await;
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

    /// The reply to the JSON connect command `message`.
    fn connect_reply(&mut self, message: &Value) -> Message {
        if let Some(size) = self.state.lock().unwrap().sized_connect.take() {
            let prefix = format!(
                r#"{{"id":{},"connect":{{"client":"c","data":""#,
                message["id"]
            );
            return Message::text(sized_json(&prefix, "\"}}", size));
        }
        let client = self.next_client();
        let (subs, data) = {
            let mut state = self.state.lock().unwrap();
            let default = state.server_subscriptions.clone();
            let subs = state
                .next_server_subscriptions
                .pop_front()
                .unwrap_or(default);
            (subs, state.connect_data.clone())
        };
        let connect = json!({
            "client": client,
            "version": "fake",
            "ping": self.announce.ping,
            "pong": self.announce.pong,
            "expires": self.announce.ttl > 0,
            "ttl": self.announce.ttl,
            "subs": subs,
        })
        .to_string();
        // Raw data goes in as given, never re-serialized.
        let connect = match data {
            Some(raw) => format!("{},\"data\":{raw}}}", &connect[..connect.len() - 1]),
            None => connect,
        };
        Message::text(format!(
            "{{\"id\":{},\"connect\":{connect}}}",
            message["id"]
        ))
    }

    /// Records the JSON commands of a text frame; the replies to send.
    fn json_commands(&mut self, text: &str) -> Vec<Message> {
        let mut replies = Vec::new();
        let frame = {
            let mut state = self.state.lock().unwrap();
            state.frames += 1;
            state.frames - 1
        };
        for line in text.lines() {
            let Ok(message) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let failing = message["connect"].is_object().then(|| {
                let mut state = self.state.lock().unwrap();
                if state.failing_connects > 0 {
                    state.failing_connects -= 1;
                    return Some(json!({"code": 100, "message": "busy", "temporary": true}));
                }
                state.connect_errors.pop_front()
            });
            if let Some(Some(error)) = failing {
                replies.push(Message::text(
                    json!({"id": message["id"], "error": error}).to_string(),
                ));
            } else if message["connect"].is_object() {
                replies.push(self.connect_reply(&message));
            } else if message["subscribe"].is_object() {
                replies.extend(self.subscribe_answer(&message));
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
                    Rpc::Echo => replies.push(Message::text(
                        json!({"id": message["id"], "rpc": {"data": message["rpc"]["data"]}})
                            .to_string(),
                    )),
                }
            } else if message["history"].is_object() {
                replies.push(Message::text(
                    json!({"id": message["id"], "history": {"epoch": "fake", "offset": 0}})
                        .to_string(),
                ));
            } else if message["unsubscribe"].is_object() {
                replies.push(Message::text(
                    json!({"id": message["id"], "unsubscribe": {}}).to_string(),
                ));
            } else if message["publish"].is_object() {
                replies.push(Message::text(
                    json!({"id": message["id"], "publish": {}}).to_string(),
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
            let mut state = self.state.lock().unwrap();
            state.received.push(message);
            state.received_frames.push(frame);
            state.received_at.push(std::time::Instant::now());
        }
        replies
    }

    /// The answer to the JSON subscribe command `message`, unless it is
    /// left unanswered or answered later.
    fn subscribe_answer(&self, message: &Value) -> Option<Message> {
        let mut state = self.state.lock().unwrap();
        if state.ignore_subscribes {
            return None;
        }
        if state.ignored_subscribes > 0 {
            state.ignored_subscribes -= 1;
            return None;
        }
        if let Some((delay, error)) = state.subscribe_delay.clone() {
            let answer = match error {
                Some(error) => json!({"id": message["id"], "error": error}),
                None => json!({"id": message["id"], "subscribe": {}}),
            };
            let shared = Arc::clone(&self.state);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let mut state = shared.lock().unwrap();
                state.subscribes_answered_at.push(std::time::Instant::now());
                if let Some(out) = &state.out {
                    let _ = out.send(Outgoing::Message(Message::text(answer.to_string())));
                }
            });
            return None;
        }
        let channel = message["subscribe"]["channel"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let (mut result, from_history) = match state.subscribe_results.pop_front() {
            Some(result) => (result, false),
            None if state.recovering.contains(&channel) => {
                (from_history(&state, &channel, &message["subscribe"]), true)
            }
            None => (json!({}), false),
        };
        // Each subscribe starts a new delta chain (Block 19, D5).
        if message["subscribe"]["delta"] == "fossil" && state.delta_channels.contains(&channel) {
            state.delta_chains.insert(channel.clone(), None);
            result["delta"] = json!(true);
            if let Some(publications) = result["publications"].as_array().cloned() {
                let encoded: Vec<Value> = publications
                    .into_iter()
                    .map(|publication| state.with_delta(&channel, publication))
                    .collect();
                result["publications"] = json!(encoded);
            }
        } else {
            state.delta_chains.remove(&channel);
        }
        state.subscribe_replies.push((channel, result.clone()));
        let reply = Message::text(json!({"id": message["id"], "subscribe": result}).to_string());
        if from_history {
            // In order with the publications pushed (see `State::out`).
            let _ = self.out.send(Outgoing::Message(reply));
            return None;
        }
        Some(reply)
    }

    /// Records the Protobuf commands of a binary frame; the replies to send.
    /// A frame that does not decode is dropped.
    fn protobuf_commands(&mut self, mut frame: &[u8]) -> Vec<Message> {
        let mut replies = Vec::new();
        let number = {
            let mut state = self.state.lock().unwrap();
            state.frames += 1;
            state.frames - 1
        };
        while !frame.is_empty() {
            let Ok(command) = protobuf::Command::decode_length_delimited(&mut frame) else {
                return replies;
            };
            let reply = if command.connect.is_some() {
                Some(protobuf::Reply {
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
                })
            } else {
                protobuf_result(&command)
            };
            if let Some(reply) = reply {
                replies.push(Message::binary(reply.encode_length_delimited_to_vec()));
            }
            let mut state = self.state.lock().unwrap();
            state.received_protobuf.push(command);
            state.received_protobuf_frames.push(number);
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

/// The fixed Protobuf results the fake server answers calls with, by
/// command (Block 13): what [`protobuf_result`] sends.
pub(crate) mod protobuf_results {
    use super::protobuf;

    /// RPC result data.
    pub(crate) const RPC_DATA: [u8; 3] = [0xfe, 0x00, 0xff];

    pub(crate) fn history() -> protobuf::HistoryResult {
        protobuf::HistoryResult {
            publications: vec![protobuf::Publication {
                data: vec![0xfe, 0x01],
                offset: 4,
                ..Default::default()
            }],
            epoch: "pb-epoch".to_owned(),
            offset: 4,
        }
    }

    pub(crate) fn presence() -> protobuf::PresenceResult {
        protobuf::PresenceResult {
            presence: [(
                "pb-client".to_owned(),
                protobuf::ClientInfo {
                    user: "pb-user".to_owned(),
                    client: "pb-client".to_owned(),
                    conn_info: vec![0xff],
                    chan_info: vec![0x00],
                },
            )]
            .into(),
        }
    }

    pub(crate) fn presence_stats() -> protobuf::PresenceStatsResult {
        protobuf::PresenceStatsResult {
            num_clients: 3,
            num_users: 2,
        }
    }
}

/// The answer to a Protobuf call command, if it is one the fake server
/// answers: RPC, history, presence or presence stats.
fn protobuf_result(command: &protobuf::Command) -> Option<protobuf::Reply> {
    let mut reply = protobuf::Reply {
        id: command.id,
        ..Default::default()
    };
    if command.rpc.is_some() {
        reply.rpc = Some(protobuf::RpcResult {
            data: protobuf_results::RPC_DATA.to_vec(),
        });
    } else if command.history.is_some() {
        reply.history = Some(protobuf_results::history());
    } else if command.presence.is_some() {
        reply.presence = Some(protobuf_results::presence());
    } else if command.presence_stats.is_some() {
        reply.presence_stats = Some(protobuf_results::presence_stats());
    } else if command.subscribe.is_some() {
        // Granted, without position or publications (Block 29).
        reply.subscribe = Some(protobuf::SubscribeResult::default());
    } else {
        return None;
    }
    Some(reply)
}

/// The payload bytes of `message` and the JSON messages it holds, one per
/// non-empty line; none for a binary or control frame (Block 31).
fn json_size(message: &Message) -> (u64, u64) {
    match message {
        Message::Text(text) => (
            text.len() as u64,
            text.lines().filter(|line| !line.trim().is_empty()).count() as u64,
        ),
        Message::Binary(bytes) => (bytes.len() as u64, 0),
        _ => (0, 0),
    }
}

/// `prefix`, then `x` characters, then `suffix`: `size` bytes in all.
pub(crate) fn sized_json(prefix: &str, suffix: &str, size: usize) -> String {
    let padding = size
        .checked_sub(prefix.len() + suffix.len())
        .expect("a size that holds the prefix and suffix");
    format!("{prefix}{}{suffix}", "x".repeat(padding))
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
        let expired = state.expired.get(channel).is_some_and(|upto| from < *upto);
        let too_many = state
            .recovery_limit
            .is_some_and(|limit| top.saturating_sub(from) > limit);
        if expired || too_many {
            result["recovered"] = json!(false);
            return result;
        }
        // As Centrifugo: a failed recovery carries no publications
        // (centrifuge `client.go`, "recovered=false implies no publications").
        if subscribe["epoch"] != HISTORY_EPOCH {
            result["recovered"] = json!(false);
            return result;
        }
        result["recovered"] = json!(true);
        let mut publications: Vec<&Value> = history
            .iter()
            .filter(|publication| publication["offset"].as_u64().unwrap_or(0) > from)
            .collect();
        if state.skip_recovered && publications.len() >= 2 {
            publications.remove(0);
        }
        result["publications"] = json!(publications);
    }
    result
}

/// Reads the upgrade request on `tcp` and answers it with `status` and a
/// sign-in page, then closes.
async fn answer_as_a_portal(mut tcp: tokio::net::TcpStream, status: u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut request = Vec::new();
    let mut buffer = [0; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match tcp.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    let reason = match status {
        200 => "OK",
        302 => "Found",
        511 => "Network Authentication Required",
        _ => "Portal",
    };
    let page = "<html><body>Sign in to use this network</body></html>";
    let location = if (300..400).contains(&status) {
        "location: http://portal.example/login\r\n"
    } else {
        ""
    };
    let answer = format!(
        "HTTP/1.1 {status} {reason}\r\n{location}content-type: text/html; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{page}",
        page.len()
    );
    let _ = tcp.write_all(answer.as_bytes()).await;
    let _ = tcp.shutdown().await;
}
