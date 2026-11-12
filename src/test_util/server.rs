//! An in-memory server double for an application's tests (Block 37c, D5):
//! the real client, driver and core run against it over an in-memory
//! duplex and a real websocket handshake, with no socket opened.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use futures_util::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::protocol as pb;
use crate::wire::REJECT_UNRECOVERED;
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
/// several clients may connect to it. It does not model map or shared poll
/// subscriptions while they are unstable: it answers their subscribe as a
/// stream subscription's, as Centrifugo before 6.8.0 does, so a shared poll
/// subscription sends an unsubscribe command and ends with `unsubscribed`
/// 8, tracking no key.
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
    let mut writer = tokio::spawn(async move {
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
        if !lock(&shared).connections.contains_key(&id) {
            break;
        }
        if matches!(message, Message::Close(_)) {
            break;
        }
        handle_frame(&shared, id, message);
    }
    if let Some(shared) = state.upgrade() {
        lock(&shared).connections.remove(&id);
    }
    // With its sender gone, the writer ends once it has written what is
    // queued, a close the test asked for included (Codex on #161); a client
    // that stopped reading cannot hold it longer than this.
    if tokio::time::timeout(WRITER_DRAIN, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
}

/// How long a connection's writer may take to write what is queued once
/// the connection ends.
const WRITER_DRAIN: std::time::Duration = std::time::Duration::from_secs(1);

/// Handles the commands of a frame of connection `id`, in order.
fn handle_frame(shared: &Mutex<State>, id: u64, message: Message) {
    // The state is locked per command, so that an RPC handler runs with it
    // unlocked. As Centrifugo, nothing after a refused connect in its frame
    // is handled (Block 36, D7).
    match message {
        // Command by command, as the crate's codec reads replies: data may
        // hold newlines (Codex on #161).
        Message::Text(frame) => {
            for raw in serde_json::Deserializer::from_str(frame.as_str()).into_iter::<&RawValue>() {
                let Ok(raw) = raw else {
                    break;
                };
                let Ok(command) = serde_json::from_str::<Value>(raw.get()) else {
                    continue;
                };
                let handled = handle_json(&mut lock(shared), id, &command, raw.get());
                if finish(shared, id, handled) {
                    break;
                }
            }
        }
        Message::Binary(mut frame) => {
            while !frame.is_empty() {
                let Ok(command) = pb::Command::decode_length_delimited(&mut frame) else {
                    break;
                };
                let handled = handle_protobuf(&mut lock(shared), id, command);
                if finish(shared, id, handled) {
                    break;
                }
            }
        }
        _ => {}
    }
}

/// Marks, inside a reply's `Value`, data that [`text`] writes verbatim: as
/// a `Value` it would lose its key order and its whitespace, where
/// Centrifugo and the crate's codec keep its bytes (Codex on #161). The
/// mark holds a random nonce of the process, so that no string of the
/// application's, which `serde_json` writes escaped, can pass for one.
const RAW: char = '\u{1}';
/// The mark's ends as `serde_json` writes them, quotes included.
const RAW_OPEN: &str = "\"\\u0001";
const RAW_CLOSE: &str = "\\u0001\"";

/// The nonce of the marks: 32 hex digits, random per process.
fn nonce() -> &'static str {
    use std::hash::{BuildHasher, Hasher};
    static NONCE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    NONCE.get_or_init(|| {
        let random = || {
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish()
        };
        format!("{:016x}{:016x}", random(), random())
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn unhex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect()
}

/// Data as the JSON protocol carries it: the bytes themselves when they are
/// JSON, a JSON string of them otherwise.
fn json_data(data: &[u8]) -> Value {
    match std::str::from_utf8(data) {
        Ok(json) if serde_json::from_str::<serde::de::IgnoredAny>(json).is_ok() => {
            Value::String(format!("{RAW}{}{}{RAW}", nonce(), hex(data)))
        }
        _ => Value::String(String::from_utf8_lossy(data).into_owned()),
    }
}

/// The raw `data` of the `method` object of the JSON command `raw`, as the
/// client wrote it: none when it is missing, `null` kept as such (Codex on
/// #161).
fn data_of(raw: &str, method: &str) -> Option<Vec<u8>> {
    let command: HashMap<&str, &RawValue> = serde_json::from_str(raw).ok()?;
    let fields: HashMap<&str, &RawValue> = serde_json::from_str(command.get(method)?.get()).ok()?;
    Some(fields.get("data")?.get().as_bytes().to_vec())
}

/// `value` as a JSON frame, the data [`json_data`] marked written verbatim;
/// anything else is left as `serde_json` wrote it.
fn text(value: &Value) -> Message {
    let written = value.to_string();
    let open = format!("{RAW_OPEN}{}", nonce());
    let mut frame = String::with_capacity(written.len());
    let mut rest = written.as_str();
    while let Some(start) = rest.find(&open) {
        frame.push_str(&rest[..start]);
        let marked = &rest[start + open.len()..];
        let raw = marked.find(RAW_CLOSE).and_then(|end| {
            let bytes = unhex(&marked[..end])?;
            Some((String::from_utf8(bytes).ok()?, end + RAW_CLOSE.len()))
        });
        if let Some((data, past)) = raw {
            frame.push_str(&data);
            rest = &marked[past..];
        } else {
            frame.push_str(&open);
            rest = marked;
        }
    }
    frame.push_str(rest);
    Message::text(frame)
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
    /// With the positions it asks to recover server-side subscriptions from
    /// (`ConnectRequest.subs`).
    Connect(HashMap<String, StreamPosition>),
    Subscribe {
        channel: String,
        recover: Option<StreamPosition>,
        /// `SubscribeRequest.flag` asks for error 112 when recovery fails.
        reject_unrecovered: bool,
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
        subs: Vec<(String, Recovery)>,
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

/// A subscribe result's position and what it recovered.
struct Recovery {
    offset: u64,
    was_recovering: bool,
    recovered: bool,
    publications: Vec<(u64, Vec<u8>)>,
}

/// Recovers from `recover` in `stream`: the publications after a position
/// of that stream, or none with `recovered` false for another position.
fn recovery(stream: &[Vec<u8>], recover: Option<&StreamPosition>) -> Recovery {
    let top = stream.len() as u64;
    match recover {
        Some(from) if from.epoch == EPOCH && from.offset <= top => Recovery {
            offset: from.offset,
            was_recovering: true,
            recovered: true,
            publications: numbered(stream, from.offset),
        },
        Some(_) => Recovery {
            offset: top,
            was_recovering: true,
            recovered: false,
            publications: Vec::new(),
        },
        None => Recovery {
            offset: top,
            was_recovering: false,
            recovered: false,
            publications: Vec::new(),
        },
    }
}

fn answer(state: &mut State, id: u64, request: Asked) -> Answer {
    match request {
        Asked::Connect(recover) => {
            if let Some(error) = state.refuse_next_connect.take() {
                return Answer::Error(error);
            }
            let granted = state.granted.clone();
            let mut subs = Vec::new();
            for channel in granted {
                let stream = state.streams.get(&channel).map_or(&[][..], Vec::as_slice);
                let recovered = recovery(stream, recover.get(&channel));
                subs.push((channel, recovered));
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
            reject_unrecovered,
            positioned,
            recoverable,
        } => subscribe_answer(
            state,
            id,
            &channel,
            (recover.as_ref(), reject_unrecovered),
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
        Asked::Rpc(..) => unreachable!("an RPC is answered with the state unlocked"),
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
    (recover, reject_unrecovered): (Option<&StreamPosition>, bool),
    positioned: bool,
    recoverable: bool,
) -> Answer {
    if let Some(error) = state.refused.get(channel) {
        return Answer::Error(error.clone());
    }
    let stream = state.streams.get(channel).map_or(&[][..], Vec::as_slice);
    let Recovery {
        offset,
        was_recovering,
        recovered,
        publications,
    } = recovery(stream, recover);
    // With the flag, a failed recovery is refused, so that the client
    // reloads its state (`client.proto`, `SubscribeRequest.flag`; Codex on
    // #161).
    if reject_unrecovered && was_recovering && !recovered {
        return Answer::Error(CallError::new(112, "unrecoverable position"));
    }
    if let Some(connection) = state.connections.get_mut(&id) {
        connection.subscribed.insert(channel.to_owned());
    }
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
/// publications after `since`, or, when `reverse`, those before it newest
/// first (`HistoryOptions::since`), at most `limit` of them unless it is
/// negative, and the top of the stream.
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
        // `client.proto`, `HistoryRequest.since` (Codex on #161).
        Some(from) if reverse && from.offset == 0 => {
            return Answer::Error(CallError::new(107, "bad request"));
        }
        Some(from) => from.offset,
        None => 0,
    };
    let mut publications = if reverse {
        let before = since.map_or(u64::MAX, |from| from.offset);
        let mut publications: Vec<_> = numbered(&stream, 0)
            .into_iter()
            .filter(|(offset, _)| *offset < before)
            .collect();
        publications.reverse();
        publications
    } else {
        numbered(&stream, after)
    };
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

/// What a JSON subscribe command asks.
fn json_subscribe(subscribe: &Value) -> (Option<Command>, Asked) {
    let string = |value: &Value| value.as_str().unwrap_or_default().to_owned();
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
            reject_unrecovered: subscribe["flag"].as_i64().unwrap_or(0) & REJECT_UNRECOVERED != 0,
            positioned: subscribe["positioned"].as_bool().unwrap_or(false),
            recoverable: subscribe["recoverable"].as_bool().unwrap_or(false),
        },
    )
}

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
fn json_request(state: &mut State, command: &Value, raw: &str) -> Option<(Option<Command>, Asked)> {
    let string = |value: &Value| value.as_str().unwrap_or_default().to_owned();
    let request = if let Some(connect) = command.get("connect") {
        let recover = connect["subs"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(channel, sub)| {
                let from = position(
                    sub["offset"].as_u64().unwrap_or(0),
                    sub["epoch"].as_str().unwrap_or_default(),
                    sub["recover"].as_bool().unwrap_or(false),
                )?;
                Some((channel.clone(), from))
            })
            .collect();
        (
            Some(Command::Connect {
                token: string(&connect["token"]),
                data: data_of(raw, "connect"),
            }),
            Asked::Connect(recover),
        )
    } else if let Some(subscribe) = command.get("subscribe") {
        json_subscribe(subscribe)
    } else if let Some(unsubscribe) = command.get("unsubscribe") {
        let channel = string(&unsubscribe["channel"]);
        (
            Some(Command::Unsubscribe {
                channel: channel.clone(),
            }),
            Asked::Unsubscribe(channel),
        )
    } else if let Some(publish) = command.get("publish") {
        let (channel, data) = (
            string(&publish["channel"]),
            data_of(raw, "publish").unwrap_or_default(),
        );
        (
            Some(Command::Publish {
                channel: channel.clone(),
                data: data.clone(),
            }),
            Asked::Publish(channel, data),
        )
    } else if let Some(rpc) = command.get("rpc") {
        let (method, data) = (
            string(&rpc["method"]),
            data_of(raw, "rpc").unwrap_or_default(),
        );
        (
            Some(Command::Rpc {
                method: method.clone(),
                data: data.clone(),
            }),
            Asked::Rpc(method, data),
        )
    } else if command.get("send").is_some() {
        state.received.push(Command::Send {
            data: data_of(raw, "send").unwrap_or_default(),
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

/// Handles a JSON command, but for an RPC's answer.
fn handle_json(state: &mut State, id: u64, command: &Value, raw: &str) -> Handled {
    let reply_id = command["id"].as_u64().unwrap_or(0);
    let Some((recorded, request)) = json_request(state, command, raw) else {
        return Handled::Answered(false);
    };
    let connect = matches!(request, Asked::Connect(_));
    state.received.extend(recorded);
    if let Asked::Rpc(method, data) = request {
        return Handled::Rpc {
            handler: state.rpc.clone(),
            method,
            data,
            reply: ReplyTo::Json(reply_id),
        };
    }
    let answer = answer(state, id, request);
    let refused = connect && matches!(answer, Answer::Error(_));
    let mut reply = json_reply(answer, command);
    reply["id"] = json!(reply_id);
    send(state, id, text(&reply));
    Handled::Answered(refused)
}

/// What handling a command leaves to the frame loop.
enum Handled {
    /// Answered; `true` for a connect the double refused, which ends its
    /// frame.
    Answered(bool),
    /// An RPC, which the application's handler answers with the state
    /// unlocked: the handler may use the double (Codex on #161).
    Rpc {
        handler: Option<Rpc>,
        method: String,
        data: Vec<u8>,
        reply: ReplyTo,
    },
}

/// The id of an RPC's reply, in its connection's encoding.
enum ReplyTo {
    Json(u64),
    Protobuf(u32),
}

/// Completes `outcome` with the state unlocked; whether its frame ends
/// there.
fn finish(shared: &Mutex<State>, id: u64, outcome: Handled) -> bool {
    let Handled::Rpc {
        handler,
        method,
        data,
        reply,
    } = outcome
    else {
        return matches!(outcome, Handled::Answered(true));
    };
    let answer = match handler {
        Some(handler) => match handler(&method, &data) {
            Ok(data) => Answer::Rpc(data),
            Err(error) => Answer::Error(error),
        },
        None => Answer::Error(CallError::new(108, "not available")),
    };
    let message = match reply {
        ReplyTo::Json(reply_id) => {
            let mut reply = json_reply(answer, &Value::Null);
            reply["id"] = json!(reply_id);
            text(&reply)
        }
        ReplyTo::Protobuf(reply_id) => {
            let mut reply = protobuf_reply(answer, false);
            reply.id = reply_id;
            protobuf(&reply)
        }
    };
    send(&lock(shared), id, message);
    false
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
                    .map(|(channel, recovery)| {
                        let result = json!({
                            "recoverable": true,
                            "positioned": true,
                            "epoch": EPOCH,
                            "offset": recovery.offset,
                            "was_recovering": recovery.was_recovering,
                            "recovered": recovery.recovered,
                            "publications": json_publications(&recovery.publications),
                        });
                        (channel, result)
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

/// What a Protobuf command asks, and what `received()` records of it;
/// none for a command the double does not answer.
fn protobuf_request(state: &mut State, command: pb::Command) -> Option<(Option<Command>, Asked)> {
    let request = if let Some(connect) = command.connect {
        let recover = connect
            .subs
            .iter()
            .filter_map(|(channel, sub)| {
                Some((
                    channel.clone(),
                    position(sub.offset, &sub.epoch, sub.recover)?,
                ))
            })
            .collect();
        (
            Some(Command::Connect {
                token: connect.token,
                data: (!connect.data.is_empty()).then_some(connect.data),
            }),
            Asked::Connect(recover),
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
                reject_unrecovered: subscribe.flag & REJECT_UNRECOVERED != 0,
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
        return None;
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
        return None;
    };
    Some(request)
}

/// Handles a Protobuf command, but for an RPC's answer.
fn handle_protobuf(state: &mut State, id: u64, command: pb::Command) -> Handled {
    let reply_id = command.id;
    let refresh = command.sub_refresh.is_some();
    let Some((recorded, request)) = protobuf_request(state, command) else {
        return Handled::Answered(false);
    };
    let connect = matches!(request, Asked::Connect(_));
    state.received.extend(recorded);
    if let Asked::Rpc(method, data) = request {
        return Handled::Rpc {
            handler: state.rpc.clone(),
            method,
            data,
            reply: ReplyTo::Protobuf(reply_id),
        };
    }
    let answer = answer(state, id, request);
    let refused = connect && matches!(answer, Answer::Error(_));
    let mut reply = protobuf_reply(answer, refresh);
    reply.id = reply_id;
    send(state, id, protobuf(&reply));
    Handled::Answered(refused)
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
                    .map(|(channel, recovery)| {
                        (
                            channel,
                            pb::SubscribeResult {
                                recoverable: true,
                                positioned: true,
                                epoch: EPOCH.to_owned(),
                                offset: recovery.offset,
                                was_recovering: recovery.was_recovering,
                                recovered: recovery.recovered,
                                publications: protobuf_publications(recovery.publications),
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
        // In reverse, the publications before the position (Codex on #161).
        assert_eq!(history(&mut state, -1, Some(3), true), (vec![2, 1], 3));
        assert_eq!(history(&mut state, 1, Some(3), true), (vec![2], 3));
    }

    // Codex on #161: an application's string shaped like a mark stays a
    // string, beside data written verbatim.
    #[test]
    fn strings_like_a_mark_are_written_as_strings() {
        let value = json!({
            "message": "\u{1}0123\u{1}",
            "binary": json_data(&[1]),
            "data": json_data(br#"{"b":1, "a":2}"#),
        });

        let Message::Text(frame) = text(&value) else {
            panic!("a text frame");
        };
        let parsed: Value = serde_json::from_str(frame.as_str()).unwrap();

        assert_eq!(parsed["message"], "\u{1}0123\u{1}");
        assert_eq!(parsed["binary"], "\u{1}");
        assert!(frame.as_str().contains(r#"{"b":1, "a":2}"#), "{frame}");
    }

    // `client.proto`: a zero offset with reverse set is error 107 (Codex on
    // #161).
    #[test]
    fn reverse_history_from_offset_zero_is_a_bad_request() {
        let mut state = State::default();
        publish(&mut state, "news", b"1");

        let answered = history_answer(
            &state,
            "news",
            -1,
            Some(&StreamPosition {
                offset: 0,
                epoch: EPOCH.to_owned(),
            }),
            true,
        );

        assert!(
            matches!(&answered, Answer::Error(error) if error.code == 107),
            "not refused"
        );
    }

    // Codex on #161: a granted server-side subscription recovers from the
    // position its connect command gives.
    #[test]
    fn a_connect_recovers_granted_server_side_subscriptions() {
        let mut state = State::default();
        let (out, _queue) = mpsc::unbounded_channel();
        state.connections.insert(
            0,
            Connection {
                client: "double-0".to_owned(),
                protobuf: false,
                subscribed: HashSet::new(),
                out,
            },
        );
        state.granted.push("alerts".to_owned());
        for n in 1..=3 {
            publish(&mut state, "alerts", format!("{n}").as_bytes());
        }
        let connect = |state: &mut State, epoch: &str| {
            let recover = HashMap::from([(
                "alerts".to_owned(),
                StreamPosition {
                    offset: 1,
                    epoch: epoch.to_owned(),
                },
            )]);
            match answer(state, 0, Asked::Connect(recover)) {
                Answer::Connect { mut subs, .. } => subs.remove(0).1,
                _ => panic!("a connect answer"),
            }
        };

        let recovered = connect(&mut state, EPOCH);
        let lost = connect(&mut state, "another");

        assert!(recovered.was_recovering && recovered.recovered);
        assert_eq!(recovered.offset, 1);
        let offsets: Vec<u64> = recovered.publications.iter().map(|(at, _)| *at).collect();
        assert_eq!(offsets, [2, 3]);
        assert!(lost.was_recovering && !lost.recovered);
        assert_eq!(lost.publications, Vec::new());
    }

    // Codex on #161: explicit null data stays the JSON `null`; missing data
    // is none, as the crate's codec has them.
    #[test]
    fn json_null_data_is_kept() {
        let mut state = State::default();
        let data = |state: &mut State, command: Value| match json_request(
            state,
            &command,
            &command.to_string(),
        ) {
            Some((_, Asked::Rpc(_, data) | Asked::Publish(_, data))) => data,
            _ => panic!("an RPC or a publish"),
        };

        let null = data(&mut state, json!({"rpc": {"method": "m", "data": null}}));
        let missing = data(&mut state, json!({"publish": {"channel": "news"}}));

        assert_eq!(null, b"null");
        assert_eq!(missing, Vec::<u8>::new());
    }

    // Codex on #161: data keep their bytes both ways, newlines and key
    // order included.
    #[tokio::test]
    async fn json_data_keep_their_bytes() {
        let server = Server::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);
        server.on_rpc(move |_, data| {
            record.lock().unwrap().push(data.to_vec());
            Ok(br#"{"z":1, "a":2}"#.to_vec())
        });
        let client = crate::Client::builder(server.endpoint())
            .connector(server.connector())
            .build()
            .unwrap();
        client.connect();

        let sent = b"{\n  \"n\": 1,\n  \"b\": 2\n}".to_vec();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.rpc("m", sent.clone()),
        )
        .await;

        assert_eq!(
            result.ok().and_then(Result::ok),
            Some(br#"{"z":1, "a":2}"#.to_vec())
        );
        assert_eq!(*seen.lock().unwrap(), [sent]);
    }

    // Codex on #161: with the reject-unrecovered flag a failed recovery is
    // error 112, and the connection is not subscribed.
    #[test]
    fn the_reject_unrecovered_flag_refuses_a_failed_recovery() {
        let mut state = State::default();
        let (out, _queue) = mpsc::unbounded_channel();
        state.connections.insert(
            0,
            Connection {
                client: "double-0".to_owned(),
                protobuf: false,
                subscribed: HashSet::new(),
                out,
            },
        );
        let lost = StreamPosition {
            offset: 1,
            epoch: "another".to_owned(),
        };

        let answered = subscribe_answer(&mut state, 0, "news", (Some(&lost), true), true, true);

        assert!(
            matches!(&answered, Answer::Error(error) if error.code == 112),
            "not refused"
        );
        assert!(!state.connections[&0].subscribed.contains("news"));
    }

    // Codex on #161: a close the test asks for reaches the client, even when
    // a command is read before the writer runs.
    #[tokio::test]
    async fn a_close_asked_for_is_written_before_the_connection_ends() {
        use futures_util::StreamExt as _;

        let server = Server::new();
        let inside = server.clone();
        server.on_rpc(move |method, _| {
            if method == "bye" {
                inside.disconnect(3500, "bye");
            }
            Ok(b"{}".to_vec())
        });
        // One command per frame: "next" is already read when "bye" is
        // handled, before the writer runs.
        let client = crate::Client::builder(server.endpoint())
            .config(crate::Config::default().batch_frame_bytes(1))
            .connector(server.connector())
            .build()
            .unwrap();
        let mut events = client.events();
        client.connect();
        let connected = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !matches!(
                events.next().await,
                Some(crate::ClientEvent::Connected { .. })
            ) {}
        })
        .await;
        assert!(connected.is_ok(), "not connected");

        let bye = client.rpc("bye", b"{}".to_vec());
        let next = client.rpc("next", b"{}".to_vec());
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match events.next().await {
                    Some(crate::ClientEvent::Disconnected { code, .. }) => return Some(code),
                    Some(crate::ClientEvent::Connecting { code, .. }) if code != 0 => {
                        return None;
                    }
                    Some(_) => {}
                    None => return None,
                }
            }
        })
        .await;
        drop((bye, next));

        assert_eq!(ended.ok().flatten(), Some(3500));
    }

    // Block 47, D19: the double does not model shared poll subscriptions;
    // it answers one as Centrifugo v6.7.0 does (verified with a raw client:
    // `subscribe { type: 4 }` gets a plain `subscribe` result), and the
    // subscription ends with `unsubscribed` 8 without tracking a key.
    #[cfg(feature = "unstable")]
    #[tokio::test]
    async fn a_shared_poll_subscription_ends_with_unsubscribed_8() {
        use futures_util::StreamExt as _;

        let server = Server::new();
        let client = crate::Client::builder(server.endpoint())
            .connector(server.connector())
            .build()
            .unwrap();
        let items = client.new_shared_poll_subscription("items").unwrap();
        let mut events = items.events();
        items.track_with_signature([crate::SharedPollTrackItem::new("a", 0)], "sig");
        client.connect();
        items.subscribe();

        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match events.next().await {
                    Some(crate::SharedPollSubscriptionEvent::Unsubscribed { code, .. }) => {
                        return Some(code);
                    }
                    Some(_) => {}
                    None => return None,
                }
            }
        })
        .await;
        let unsubscribed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !server.received().contains(&Command::Unsubscribe {
                channel: "items".into(),
            }) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;

        assert_eq!(
            ended.ok().flatten(),
            Some(crate::codes::unsubscribed::SHARED_POLL_NOT_SUPPORTED)
        );
        assert!(unsubscribed.is_ok(), "{:?}", server.received());
    }

    // Codex on #161: an RPC handler may use the double, whose state is not
    // locked while it runs.
    #[tokio::test]
    async fn an_rpc_handler_can_use_the_double() {
        let server = Server::new();
        let inside = server.clone();
        server.on_rpc(move |_, data| {
            inside.publish("news", data.to_vec());
            Ok(b"{}".to_vec())
        });
        let client = crate::Client::builder(server.endpoint())
            .connector(server.connector())
            .build()
            .unwrap();
        client.connect();

        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.rpc("m", b"{}".to_vec()),
        )
        .await;

        assert!(matches!(answered, Ok(Ok(_))), "{answered:?}");
    }
}
