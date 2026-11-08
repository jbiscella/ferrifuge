//! An in-memory server double for an application's tests (Block 37c, D5):
//! the real client, driver and core run against it over an in-memory
//! duplex and a real websocket handshake, with no socket opened.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use futures_util::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::protocol as pb;
use crate::{CallError, ConnectedStream, Connector, StreamPosition};

/// The endpoint a client of the double is built with: it never resolves
/// (RFC 6761), so a client that bypasses the double's connector fails.
const ENDPOINT: &str = "ws://ferrifuge-double.invalid/connection/websocket";
/// The websocket subprotocol of the Protobuf encoding.
const PROTOBUF: &str = "centrifuge-protobuf";
/// The epoch of every channel's stream.
const EPOCH: &str = "double";

type Rpc = Arc<dyn Fn(&str, &[u8]) -> Result<Vec<u8>, CallError> + Send + Sync>;

/// An in-memory Centrifugo double that an application's test drives, for
/// its real [`Client`](crate::Client): build the client with
/// [`Server::endpoint`] and [`Server::connector`], set the double's answers,
/// publish, disconnect, and read what the client sent.
///
/// Every channel has one stream and one epoch, with offsets from 1 and a
/// history of every publication, which a subscribe asking to recover from
/// a position of that stream gets back. Clones share one double, and
/// several clients may connect to it.
///
/// ```
/// use ferrifuge::test_util::Server;
/// use ferrifuge::{Client, SubscriptionEvent};
/// use futures_util::StreamExt as _;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let server = Server::new();
/// let client = Client::builder(server.endpoint())
///     .connector(server.connector())
///     .build()
///     .unwrap();
/// // The client's events are not read here, so none is held back.
/// let news = client.new_subscription("news").unwrap();
/// let mut publications = news.events();
/// client.connect();
/// news.subscribe();
/// client.ready().await.unwrap();
/// news.ready().await.unwrap();
///
/// server.publish("news", br#"{"n":1}"#.to_vec());
///
/// while let Some(event) = publications.next().await {
///     if let SubscriptionEvent::Publication(publication) = event {
///         assert_eq!(publication.data, br#"{"n":1}"#);
///         assert_eq!(publication.offset, Some(1));
///         break;
///     }
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct Server {
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.state);
        f.debug_struct("Server")
            .field("connections", &state.connections.len())
            .field("channels", &state.streams.len())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct State {
    next_connection: u64,
    connect_data: Option<Vec<u8>>,
    refuse_next_connect: Option<CallError>,
    granted: Vec<String>,
    refused: HashMap<String, CallError>,
    rpc: Option<Rpc>,
    streams: HashMap<String, Vec<Vec<u8>>>,
    connections: BTreeMap<u64, Connection>,
    received: Vec<Command>,
}

/// An open connection, as the double serves it.
struct Connection {
    client: String,
    protobuf: bool,
    /// Channels it gets the publications of, client-side and server-side.
    subscribed: HashSet<String>,
    out: mpsc::UnboundedSender<Message>,
}

fn lock(state: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A command the double read, decoded the same way from JSON and
/// Protobuf. Its `Debug` shows data as its length, and the token only as
/// set or not (Block 21).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// A connect command.
    #[non_exhaustive]
    Connect {
        /// Its token, empty when it carried none.
        token: String,
        /// Its data.
        data: Option<Vec<u8>>,
    },
    /// A subscribe command.
    #[non_exhaustive]
    Subscribe {
        /// Its channel.
        channel: String,
        /// The stream position it asked to recover from.
        recover: Option<StreamPosition>,
    },
    /// An unsubscribe command.
    #[non_exhaustive]
    Unsubscribe {
        /// Its channel.
        channel: String,
    },
    /// A publish command.
    #[non_exhaustive]
    Publish {
        /// Its channel.
        channel: String,
        /// Its data.
        data: Vec<u8>,
    },
    /// An RPC command.
    #[non_exhaustive]
    Rpc {
        /// Its method.
        method: String,
        /// Its data.
        data: Vec<u8>,
    },
    /// A send command.
    #[non_exhaustive]
    Send {
        /// Its data.
        data: Vec<u8>,
    },
    /// A history command.
    #[non_exhaustive]
    History {
        /// Its channel.
        channel: String,
    },
    /// A presence command.
    #[non_exhaustive]
    Presence {
        /// Its channel.
        channel: String,
    },
    /// A presence stats command.
    #[non_exhaustive]
    PresenceStats {
        /// Its channel.
        channel: String,
    },
}

/// Data as its length (Block 21).
struct Length<'a>(&'a [u8]);

impl fmt::Debug for Length<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} bytes>", self.0.len())
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { token, data } => f
                .debug_struct("Connect")
                .field("token", &!token.is_empty())
                .field("data", &data.as_deref().map(Length))
                .finish(),
            Self::Subscribe { channel, recover } => f
                .debug_struct("Subscribe")
                .field("channel", channel)
                .field("recover", recover)
                .finish(),
            Self::Unsubscribe { channel } => f
                .debug_struct("Unsubscribe")
                .field("channel", channel)
                .finish(),
            Self::Publish { channel, data } => f
                .debug_struct("Publish")
                .field("channel", channel)
                .field("data", &Length(data))
                .finish(),
            Self::Rpc { method, data } => f
                .debug_struct("Rpc")
                .field("method", method)
                .field("data", &Length(data))
                .finish(),
            Self::Send { data } => f.debug_struct("Send").field("data", &Length(data)).finish(),
            Self::History { channel } => {
                f.debug_struct("History").field("channel", channel).finish()
            }
            Self::Presence { channel } => f
                .debug_struct("Presence")
                .field("channel", channel)
                .finish(),
            Self::PresenceStats { channel } => f
                .debug_struct("PresenceStats")
                .field("channel", channel)
                .finish(),
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    /// A double with no channel, no rule and no connection. It needs no
    /// runtime: it serves each connection on the runtime of the client
    /// that opens it.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// The endpoint to build a client with, together with
    /// [`Server::connector`].
    #[must_use]
    pub fn endpoint(&self) -> String {
        ENDPOINT.to_owned()
    }

    /// The connector of a client of the double: each call opens an
    /// in-memory duplex and serves the websocket handshake and the
    /// connection over it, until the connection or the last handle of the
    /// double ends.
    #[must_use]
    pub fn connector(&self) -> impl Connector + Clone + 'static {
        DoubleConnector(Arc::downgrade(&self.state))
    }

    /// Later connect replies carry `data`.
    pub fn connect_data(&self, data: impl Into<Vec<u8>>) {
        lock(&self.state).connect_data = Some(data.into());
    }

    /// The next connect command is answered with `error`, its `temporary`
    /// flag included.
    pub fn refuse_next_connect(&self, error: CallError) {
        lock(&self.state).refuse_next_connect = Some(error);
    }

    /// Later connect replies subscribe the connection to `channel` on the
    /// server side.
    pub fn grant_server_subscription(&self, channel: impl Into<String>) {
        lock(&self.state).granted.push(channel.into());
    }

    /// Subscribes to `channel` are answered with `error`.
    pub fn refuse_subscriptions(&self, channel: impl Into<String>, error: CallError) {
        lock(&self.state).refused.insert(channel.into(), error);
    }

    /// RPCs are answered by `handler`, given the method and the data.
    /// Without a handler an RPC fails with 108 "not available", as on
    /// Centrifugo without an RPC proxy.
    pub fn on_rpc<F>(&self, handler: F)
    where
        F: Fn(&str, &[u8]) -> Result<Vec<u8>, CallError> + Send + Sync + 'static,
    {
        lock(&self.state).rpc = Some(Arc::new(handler));
    }

    /// Publishes `data` into `channel`'s stream, to every connection
    /// subscribed to it; its offset.
    pub fn publish(&self, channel: impl Into<String>, data: impl Into<Vec<u8>>) -> u64 {
        publish(&mut lock(&self.state), &channel.into(), &data.into())
    }

    /// Sends the async message `data` to every open connection.
    pub fn message(&self, data: impl Into<Vec<u8>>) {
        let data = data.into();
        let state = lock(&self.state);
        for connection in state.connections.values() {
            let message = if connection.protobuf {
                protobuf(&pb::Reply {
                    push: Some(pb::Push {
                        message: Some(pb::Message { data: data.clone() }),
                        ..pb::Push::default()
                    }),
                    ..pb::Reply::default()
                })
            } else {
                text(&json!({"push": {"message": {"data": json_data(&data)}}}))
            };
            let _ = connection.out.send(message);
        }
    }

    /// Closes every open connection with `code` and `reason`: from then on
    /// it gets nothing, and later publications go to the history only.
    pub fn disconnect(&self, code: u16, reason: &str) {
        let connections = std::mem::take(&mut lock(&self.state).connections);
        for connection in connections.into_values() {
            let _ = connection.out.send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.to_owned().into(),
            })));
        }
    }

    /// The commands of every connection, in the order read.
    #[must_use]
    pub fn received(&self) -> Vec<Command> {
        lock(&self.state).received.clone()
    }

    /// The connections open.
    #[must_use]
    pub fn connections(&self) -> usize {
        lock(&self.state).connections.len()
    }
}

/// The double's connector, which holds it weakly: the connections end
/// with its last handle.
#[derive(Clone)]
struct DoubleConnector(Weak<Mutex<State>>);

impl Connector for DoubleConnector {
    fn connect(&self, _endpoint: http::Uri) -> crate::Connecting {
        let state = self.0.clone();
        Box::pin(async move {
            if state.upgrade().is_none() {
                return Err("the server double is gone".into());
            }
            let (client, server) = tokio::io::duplex(1 << 16);
            tokio::spawn(serve(state, server));
            Ok(ConnectedStream::new(client))
        })
    }
}

/// Serves one connection: the handshake, then its commands in order.
async fn serve(state: Weak<Mutex<State>>, stream: tokio::io::DuplexStream) {
    let mut protobuf = false;
    // The callback's error type is tungstenite's.
    #[allow(clippy::result_large_err)]
    let accepted =
        tokio_tungstenite::accept_hdr_async(stream, |request: &Request, mut response: Response| {
            protobuf = request
                .headers()
                .get_all("sec-websocket-protocol")
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(','))
                .any(|protocol| protocol.trim() == PROTOBUF);
            if protobuf {
                response
                    .headers_mut()
                    .insert("sec-websocket-protocol", HeaderValue::from_static(PROTOBUF));
            }
            Ok(response)
        })
        .await;
    let Ok(websocket) = accepted else {
        return;
    };
    let (mut sink, mut reader) = websocket.split();
    let (out, mut queue) = mpsc::unbounded_channel();
    let id = {
        let Some(shared) = state.upgrade() else {
            return;
        };
        let mut state = lock(&shared);
        state.next_connection += 1;
        let id = state.next_connection;
        state.connections.insert(
            id,
            Connection {
                client: format!("double-{id}"),
                protobuf,
                subscribed: HashSet::new(),
                out,
            },
        );
        id
    };
    let writer = tokio::spawn(async move {
        while let Some(message) = queue.recv().await {
            let close = matches!(message, Message::Close(_));
            if sink.send(message).await.is_err() || close {
                break;
            }
        }
        let _ = sink.close().await;
    });
    while let Some(Ok(message)) = reader.next().await {
        let Some(shared) = state.upgrade() else {
            break;
        };
        let mut state = lock(&shared);
        if !state.connections.contains_key(&id) {
            break;
        }
        match message {
            // As Centrifugo, nothing after a refused connect in its frame is
            // handled (Block 36, D7).
            Message::Text(frame) => {
                for line in frame.as_str().lines() {
                    if let Ok(command) = serde_json::from_str::<Value>(line)
                        && handle_json(&mut state, id, &command)
                    {
                        break;
                    }
                }
            }
            Message::Binary(mut frame) => {
                while !frame.is_empty() {
                    let Ok(command) = pb::Command::decode_length_delimited(&mut frame) else {
                        break;
                    };
                    if handle_protobuf(&mut state, id, command) {
                        break;
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    if let Some(shared) = state.upgrade() {
        lock(&shared).connections.remove(&id);
    }
    writer.abort();
}

/// Data as the JSON protocol carries it: the bytes when they are JSON, a
/// JSON string of them otherwise.
fn json_data(data: &[u8]) -> Value {
    serde_json::from_slice(data)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(data).into_owned()))
}

/// The bytes of a JSON field the client sent as data.
fn bytes_of(value: &Value) -> Vec<u8> {
    if value.is_null() {
        Vec::new()
    } else {
        value.to_string().into_bytes()
    }
}

fn text(value: &Value) -> Message {
    Message::text(value.to_string())
}

fn protobuf(reply: &pb::Reply) -> Message {
    Message::binary(reply.encode_length_delimited_to_vec())
}

fn send(state: &State, id: u64, message: Message) {
    if let Some(connection) = state.connections.get(&id) {
        let _ = connection.out.send(message);
    }
}

/// Publishes into the stream of `channel`; the offset.
fn publish(state: &mut State, channel: &str, data: &[u8]) -> u64 {
    let stream = state.streams.entry(channel.to_owned()).or_default();
    stream.push(data.to_vec());
    let offset = stream.len() as u64;
    for connection in state.connections.values() {
        if !connection.subscribed.contains(channel) {
            continue;
        }
        let message = if connection.protobuf {
            protobuf(&pb::Reply {
                push: Some(pb::Push {
                    channel: channel.to_owned(),
                    r#pub: Some(pb::Publication {
                        data: data.to_vec(),
                        offset,
                        ..pb::Publication::default()
                    }),
                    ..pb::Push::default()
                }),
                ..pb::Reply::default()
            })
        } else {
            text(
                &json!({"push": {"channel": channel, "pub": {"data": json_data(data), "offset": offset}}}),
            )
        };
        let _ = connection.out.send(message);
    }
    offset
}

/// What a command asks, decoded from either encoding.
enum Asked {
    Connect,
    Subscribe {
        channel: String,
        recover: Option<StreamPosition>,
        positioned: bool,
        recoverable: bool,
    },
    Unsubscribe(String),
    Publish(String, Vec<u8>),
    Rpc(String, Vec<u8>),
    History {
        channel: String,
        limit: i32,
        since: Option<StreamPosition>,
        reverse: bool,
    },
    Presence(String),
    PresenceStats(String),
    /// Answered with an empty result: `refresh`, `sub_refresh`.
    Empty,
}

/// What the double answers.
enum Answer {
    Error(CallError),
    Connect {
        client: String,
        data: Option<Vec<u8>>,
        subs: Vec<(String, u64)>,
    },
    Subscribe {
        positioned: bool,
        recoverable: bool,
        offset: u64,
        was_recovering: bool,
        recovered: bool,
        publications: Vec<(u64, Vec<u8>)>,
    },
    Unsubscribe,
    Publish,
    Rpc(Vec<u8>),
    History {
        publications: Vec<(u64, Vec<u8>)>,
        offset: u64,
    },
    Presence(Vec<String>),
    PresenceStats(u32, u32),
    Empty,
}

fn answer(state: &mut State, id: u64, request: Asked) -> Answer {
    match request {
        Asked::Connect => {
            if let Some(error) = state.refuse_next_connect.take() {
                return Answer::Error(error);
            }
            let granted = state.granted.clone();
            let mut subs = Vec::new();
            for channel in granted {
                let top = state.streams.get(&channel).map_or(0, Vec::len) as u64;
                subs.push((channel, top));
            }
            let Some(connection) = state.connections.get_mut(&id) else {
                return Answer::Empty;
            };
            connection
                .subscribed
                .extend(subs.iter().map(|(channel, _)| channel.clone()));
            Answer::Connect {
                client: connection.client.clone(),
                data: state.connect_data.clone(),
                subs,
            }
        }
        Asked::Subscribe {
            channel,
            recover,
            positioned,
            recoverable,
        } => subscribe_answer(
            state,
            id,
            &channel,
            recover,
            positioned || recoverable,
            recoverable,
        ),
        Asked::Unsubscribe(channel) => {
            if let Some(connection) = state.connections.get_mut(&id) {
                connection.subscribed.remove(&channel);
            }
            Answer::Unsubscribe
        }
        Asked::Publish(channel, data) => {
            publish(state, &channel, &data);
            Answer::Publish
        }
        Asked::Rpc(method, data) => match &state.rpc {
            Some(handler) => match handler(&method, &data) {
                Ok(data) => Answer::Rpc(data),
                Err(error) => Answer::Error(error),
            },
            None => Answer::Error(CallError::new(108, "not available")),
        },
        Asked::History {
            channel,
            limit,
            since,
            reverse,
        } => history_answer(state, &channel, limit, since.as_ref(), reverse),
        Asked::Presence(channel) => Answer::Presence(
            state
                .connections
                .values()
                .filter(|connection| connection.subscribed.contains(&channel))
                .map(|connection| connection.client.clone())
                .collect(),
        ),
        Asked::PresenceStats(channel) => {
            let clients = state
                .connections
                .values()
                .filter(|connection| connection.subscribed.contains(&channel))
                .count();
            // Every connection of the double is of the same, anonymous user.
            let users = u32::from(clients > 0);
            Answer::PresenceStats(u32::try_from(clients).unwrap_or(u32::MAX), users)
        }
        Asked::Empty => Answer::Empty,
    }
}

/// The answer to a subscribe of connection `id` to `channel`.
fn subscribe_answer(
    state: &mut State,
    id: u64,
    channel: &str,
    recover: Option<StreamPosition>,
    positioned: bool,
    recoverable: bool,
) -> Answer {
    if let Some(error) = state.refused.get(channel) {
        return Answer::Error(error.clone());
    }
    if let Some(connection) = state.connections.get_mut(&id) {
        connection.subscribed.insert(channel.to_owned());
    }
    let stream = state.streams.get(channel).cloned().unwrap_or_default();
    let top = stream.len() as u64;
    let (was_recovering, recovered, publications, offset) = match recover {
        Some(from) if from.epoch == EPOCH && from.offset <= top => {
            (true, true, numbered(&stream, from.offset), from.offset)
        }
        Some(_) => (true, false, Vec::new(), top),
        None => (false, false, Vec::new(), top),
    };
    Answer::Subscribe {
        positioned,
        recoverable,
        offset,
        was_recovering,
        recovered,
        publications,
    }
}

/// The answer to a history call on `channel`, as Centrifugo's: the
/// publications after `since`, newest first when `reverse`, at most `limit`
/// of them unless it is negative, and the top of the stream.
fn history_answer(
    state: &State,
    channel: &str,
    limit: i32,
    since: Option<&StreamPosition>,
    reverse: bool,
) -> Answer {
    let stream = state.streams.get(channel).cloned().unwrap_or_default();
    let top = stream.len() as u64;
    let after = match since {
        Some(from) if from.epoch != EPOCH || from.offset > top => {
            return Answer::Error(CallError::new(112, "unrecoverable position"));
        }
        Some(from) => from.offset,
        None => 0,
    };
    let mut publications = numbered(&stream, after);
    if reverse {
        publications.reverse();
    }
    // A negative limit takes every publication.
    if let Ok(limit) = usize::try_from(limit) {
        publications.truncate(limit);
    }
    Answer::History {
        publications,
        offset: top,
    }
}

/// The publications of `stream` after offset `after`, with their offsets.
fn numbered(stream: &[Vec<u8>], after: u64) -> Vec<(u64, Vec<u8>)> {
    stream
        .iter()
        .zip(1u64..)
        .filter(|(_, offset)| *offset > after)
        .map(|(data, offset)| (offset, data.clone()))
        .collect()
}

fn position(offset: u64, epoch: &str, recover: bool) -> Option<StreamPosition> {
    recover.then(|| StreamPosition {
        offset,
        epoch: epoch.to_owned(),
    })
}

/// Handles a JSON command; whether it was a connect the double refused.
/// What a JSON history command asks.
fn json_history(history: &Value) -> (Option<Command>, Asked) {
    let string = |value: &Value| value.as_str().unwrap_or_default().to_owned();
    let channel = string(&history["channel"]);
    let since = history.get("since").map(|since| StreamPosition {
        offset: since["offset"].as_u64().unwrap_or(0),
        epoch: string(&since["epoch"]),
    });
    (
        Some(Command::History {
            channel: channel.clone(),
        }),
        Asked::History {
            channel,
            limit: history["limit"]
                .as_i64()
                .and_then(|limit| i32::try_from(limit).ok())
                .unwrap_or(0),
            since,
            reverse: history["reverse"].as_bool().unwrap_or(false),
        },
    )
}

/// What a JSON command asks, and the command to record; none for a
/// `send`, which is recorded only, or a pong.
fn json_request(state: &mut State, command: &Value) -> Option<(Option<Command>, Asked)> {
    let string = |value: &Value| value.as_str().unwrap_or_default().to_owned();
    let request = if let Some(connect) = command.get("connect") {
        (
            Some(Command::Connect {
                token: string(&connect["token"]),
                data: connect.get("data").map(bytes_of),
            }),
            Asked::Connect,
        )
    } else if let Some(subscribe) = command.get("subscribe") {
        let channel = string(&subscribe["channel"]);
        let recover = position(
            subscribe["offset"].as_u64().unwrap_or(0),
            subscribe["epoch"].as_str().unwrap_or_default(),
            subscribe["recover"].as_bool().unwrap_or(false),
        );
        (
            Some(Command::Subscribe {
                channel: channel.clone(),
                recover: recover.clone(),
            }),
            Asked::Subscribe {
                channel,
                recover,
                positioned: subscribe["positioned"].as_bool().unwrap_or(false),
                recoverable: subscribe["recoverable"].as_bool().unwrap_or(false),
            },
        )
    } else if let Some(unsubscribe) = command.get("unsubscribe") {
        let channel = string(&unsubscribe["channel"]);
        (
            Some(Command::Unsubscribe {
                channel: channel.clone(),
            }),
            Asked::Unsubscribe(channel),
        )
    } else if let Some(publish) = command.get("publish") {
        let (channel, data) = (string(&publish["channel"]), bytes_of(&publish["data"]));
        (
            Some(Command::Publish {
                channel: channel.clone(),
                data: data.clone(),
            }),
            Asked::Publish(channel, data),
        )
    } else if let Some(rpc) = command.get("rpc") {
        let (method, data) = (string(&rpc["method"]), bytes_of(&rpc["data"]));
        (
            Some(Command::Rpc {
                method: method.clone(),
                data: data.clone(),
            }),
            Asked::Rpc(method, data),
        )
    } else if let Some(sent) = command.get("send") {
        state.received.push(Command::Send {
            data: bytes_of(&sent["data"]),
        });
        return None;
    } else if let Some(history) = command.get("history") {
        json_history(history)
    } else if let Some(presence) = command.get("presence") {
        let channel = string(&presence["channel"]);
        (
            Some(Command::Presence {
                channel: channel.clone(),
            }),
            Asked::Presence(channel),
        )
    } else if let Some(stats) = command.get("presence_stats") {
        let channel = string(&stats["channel"]);
        (
            Some(Command::PresenceStats {
                channel: channel.clone(),
            }),
            Asked::PresenceStats(channel),
        )
    } else if command.get("refresh").is_some() || command.get("sub_refresh").is_some() {
        (None, Asked::Empty)
    } else {
        // A pong, or a command the double does not know.
        return None;
    };
    Some(request)
}

fn handle_json(state: &mut State, id: u64, command: &Value) -> bool {
    let reply_id = command["id"].as_u64().unwrap_or(0);
    let Some((recorded, request)) = json_request(state, command) else {
        return false;
    };
    let connect = matches!(request, Asked::Connect);
    state.received.extend(recorded);
    let answer = answer(state, id, request);
    let refused = connect && matches!(answer, Answer::Error(_));
    let mut reply = json_reply(answer, command);
    reply["id"] = json!(reply_id);
    send(state, id, text(&reply));
    refused
}

fn json_publications(publications: &[(u64, Vec<u8>)]) -> Value {
    Value::Array(
        publications
            .iter()
            .map(|(offset, data)| json!({"data": json_data(data), "offset": offset}))
            .collect(),
    )
}

fn json_reply(answer: Answer, command: &Value) -> Value {
    match answer {
        Answer::Error(error) => json!({"error": {
            "code": error.code, "message": error.message, "temporary": error.temporary,
        }}),
        Answer::Connect { client, data, subs } => {
            let mut result =
                json!({"client": client, "version": "double", "ping": 0, "pong": false});
            if let Some(data) = data {
                result["data"] = json_data(&data);
            }
            if !subs.is_empty() {
                let subs: serde_json::Map<String, Value> = subs
                    .into_iter()
                    .map(|(channel, offset)| {
                        (channel, json!({"recoverable": true, "positioned": true, "epoch": EPOCH, "offset": offset}))
                    })
                    .collect();
                result["subs"] = Value::Object(subs);
            }
            json!({"connect": result})
        }
        Answer::Subscribe {
            positioned,
            recoverable,
            offset,
            was_recovering,
            recovered,
            publications,
        } => json!({"subscribe": {
            "positioned": positioned,
            "recoverable": recoverable,
            "epoch": EPOCH,
            "offset": offset,
            "was_recovering": was_recovering,
            "recovered": recovered,
            "publications": json_publications(&publications),
        }}),
        Answer::Unsubscribe => json!({"unsubscribe": {}}),
        Answer::Publish => json!({"publish": {}}),
        Answer::Rpc(data) => json!({"rpc": {"data": json_data(&data)}}),
        Answer::History {
            publications,
            offset,
        } => json!({"history": {
            "publications": json_publications(&publications),
            "epoch": EPOCH,
            "offset": offset,
        }}),
        Answer::Presence(clients) => {
            let presence: serde_json::Map<String, Value> = clients
                .into_iter()
                .map(|client| (client.clone(), json!({"client": client, "user": ""})))
                .collect();
            json!({"presence": {"presence": presence}})
        }
        Answer::PresenceStats(clients, users) => {
            json!({"presence_stats": {"num_clients": clients, "num_users": users}})
        }
        Answer::Empty => {
            let kind = if command.get("sub_refresh").is_some() {
                "sub_refresh"
            } else {
                "refresh"
            };
            json!({kind: {}})
        }
    }
}

/// Handles a Protobuf command; whether it was a connect the double
/// refused.
fn handle_protobuf(state: &mut State, id: u64, command: pb::Command) -> bool {
    let reply_id = command.id;
    let (recorded, request) = if let Some(connect) = command.connect {
        (
            Some(Command::Connect {
                token: connect.token,
                data: (!connect.data.is_empty()).then_some(connect.data),
            }),
            Asked::Connect,
        )
    } else if let Some(subscribe) = command.subscribe {
        let recover = position(subscribe.offset, &subscribe.epoch, subscribe.recover);
        (
            Some(Command::Subscribe {
                channel: subscribe.channel.clone(),
                recover: recover.clone(),
            }),
            Asked::Subscribe {
                channel: subscribe.channel,
                recover,
                positioned: subscribe.positioned,
                recoverable: subscribe.recoverable,
            },
        )
    } else if let Some(unsubscribe) = command.unsubscribe {
        (
            Some(Command::Unsubscribe {
                channel: unsubscribe.channel.clone(),
            }),
            Asked::Unsubscribe(unsubscribe.channel),
        )
    } else if let Some(publish) = command.publish {
        (
            Some(Command::Publish {
                channel: publish.channel.clone(),
                data: publish.data.clone(),
            }),
            Asked::Publish(publish.channel, publish.data),
        )
    } else if let Some(rpc) = command.rpc {
        (
            Some(Command::Rpc {
                method: rpc.method.clone(),
                data: rpc.data.clone(),
            }),
            Asked::Rpc(rpc.method, rpc.data),
        )
    } else if let Some(sent) = command.send {
        state.received.push(Command::Send { data: sent.data });
        return false;
    } else if let Some(history) = command.history {
        let since = history.since.map(|since| StreamPosition {
            offset: since.offset,
            epoch: since.epoch,
        });
        (
            Some(Command::History {
                channel: history.channel.clone(),
            }),
            Asked::History {
                channel: history.channel,
                limit: history.limit,
                since,
                reverse: history.reverse,
            },
        )
    } else if let Some(presence) = command.presence {
        (
            Some(Command::Presence {
                channel: presence.channel.clone(),
            }),
            Asked::Presence(presence.channel),
        )
    } else if let Some(stats) = command.presence_stats {
        (
            Some(Command::PresenceStats {
                channel: stats.channel.clone(),
            }),
            Asked::PresenceStats(stats.channel),
        )
    } else if command.refresh.is_some() || command.sub_refresh.is_some() {
        (None, Asked::Empty)
    } else {
        return false;
    };
    let connect = matches!(request, Asked::Connect);
    state.received.extend(recorded);
    let refresh = command.sub_refresh.is_some();
    let answer = answer(state, id, request);
    let refused = connect && matches!(answer, Answer::Error(_));
    let mut reply = protobuf_reply(answer, refresh);
    reply.id = reply_id;
    send(state, id, protobuf(&reply));
    refused
}

fn protobuf_publications(publications: Vec<(u64, Vec<u8>)>) -> Vec<pb::Publication> {
    publications
        .into_iter()
        .map(|(offset, data)| pb::Publication {
            data,
            offset,
            ..pb::Publication::default()
        })
        .collect()
}

fn protobuf_reply(answer: Answer, sub_refresh: bool) -> pb::Reply {
    let mut reply = pb::Reply::default();
    match answer {
        Answer::Error(error) => {
            reply.error = Some(pb::Error {
                code: error.code,
                message: error.message,
                temporary: error.temporary,
            });
        }
        Answer::Connect { client, data, subs } => {
            reply.connect = Some(pb::ConnectResult {
                client,
                version: "double".to_owned(),
                data: data.unwrap_or_default(),
                subs: subs
                    .into_iter()
                    .map(|(channel, offset)| {
                        (
                            channel,
                            pb::SubscribeResult {
                                recoverable: true,
                                positioned: true,
                                epoch: EPOCH.to_owned(),
                                offset,
                                ..pb::SubscribeResult::default()
                            },
                        )
                    })
                    .collect(),
                ..pb::ConnectResult::default()
            });
        }
        Answer::Subscribe {
            positioned,
            recoverable,
            offset,
            was_recovering,
            recovered,
            publications,
        } => {
            reply.subscribe = Some(pb::SubscribeResult {
                positioned,
                recoverable,
                epoch: EPOCH.to_owned(),
                offset,
                was_recovering,
                recovered,
                publications: protobuf_publications(publications),
                ..pb::SubscribeResult::default()
            });
        }
        Answer::Unsubscribe => reply.unsubscribe = Some(pb::UnsubscribeResult::default()),
        Answer::Publish => reply.publish = Some(pb::PublishResult::default()),
        Answer::Rpc(data) => reply.rpc = Some(pb::RpcResult { data }),
        Answer::History {
            publications,
            offset,
        } => {
            reply.history = Some(pb::HistoryResult {
                publications: protobuf_publications(publications),
                epoch: EPOCH.to_owned(),
                offset,
            });
        }
        Answer::Presence(clients) => {
            reply.presence = Some(pb::PresenceResult {
                presence: clients
                    .into_iter()
                    .map(|client| {
                        (
                            client.clone(),
                            pb::ClientInfo {
                                client,
                                ..pb::ClientInfo::default()
                            },
                        )
                    })
                    .collect(),
            });
        }
        Answer::PresenceStats(num_clients, num_users) => {
            reply.presence_stats = Some(pb::PresenceStatsResult {
                num_clients,
                num_users,
            });
        }
        Answer::Empty => {
            if sub_refresh {
                reply.sub_refresh = Some(pb::SubRefreshResult::default());
            } else {
                reply.refresh = Some(pb::RefreshResult::default());
            }
        }
    }
    reply
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_debug_of_a_command_shows_no_token_and_no_data() {
        let connect = Command::Connect {
            token: "secret-token".to_owned(),
            data: Some(b"secret-data".to_vec()),
        };
        let publish = Command::Publish {
            channel: "news".to_owned(),
            data: b"secret-data".to_vec(),
        };

        let printed = format!("{connect:?} {publish:?}");

        assert!(!printed.contains("secret"), "{printed}");
        assert!(printed.contains("token: true"), "{printed}");
        assert!(printed.contains("<11 bytes>"), "{printed}");
    }

    #[test]
    fn history_follows_limit_since_and_reverse() {
        let mut state = State::default();
        for n in 1..=3 {
            publish(&mut state, "news", format!("{n}").as_bytes());
        }
        let history = |state: &mut State, limit, since: Option<u64>, reverse| match answer(
            state,
            0,
            Asked::History {
                channel: "news".to_owned(),
                limit,
                since: since.map(|offset| StreamPosition {
                    offset,
                    epoch: EPOCH.to_owned(),
                }),
                reverse,
            },
        ) {
            Answer::History {
                publications,
                offset,
            } => (
                publications
                    .into_iter()
                    .map(|(at, _)| at)
                    .collect::<Vec<_>>(),
                offset,
            ),
            _ => panic!("a history answer"),
        };

        assert_eq!(history(&mut state, -1, None, false), (vec![1, 2, 3], 3));
        assert_eq!(history(&mut state, 0, None, false), (vec![], 3));
        assert_eq!(history(&mut state, 2, None, false), (vec![1, 2], 3));
        assert_eq!(history(&mut state, 2, None, true), (vec![3, 2], 3));
        assert_eq!(history(&mut state, -1, Some(1), false), (vec![2, 3], 3));
    }
}
