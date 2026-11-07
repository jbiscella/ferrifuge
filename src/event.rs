//! Client state and the events reporting its changes.

use crate::token::{Bytes, optional_bytes};
use crate::{ClientInfo, Publication, Subscribed};

/// Connection state of a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    /// Not connected and not trying to connect: initial state, after
    /// `disconnect()`, or after a terminal condition.
    Disconnected,
    /// Trying to establish or re-establish a connection.
    Connecting,
    /// Connected to the server.
    Connected,
}

impl State {
    /// Name of the state as used by the Client SDK spec.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
        }
    }
}

/// Something that happened to a client. `#[non_exhaustive]`: later blocks
/// add events.
///
/// Its `Debug` output shows data as its length, so that events can be
/// logged (Block 21).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientEvent {
    /// The client moved to [`State::Connecting`].
    Connecting {
        /// Why: a client-side code from [`crate::codes::connecting`] or a
        /// server disconnect code.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// The client moved to [`State::Connected`].
    #[non_exhaustive]
    Connected {
        /// Connection id assigned by the server.
        client_id: String,
        /// Transport in use, e.g. `"websocket"`.
        transport: String,
        /// The data the server attached to the connect reply
        /// (`ConnectResult.data`), exactly as received: raw JSON with the
        /// JSON protocol, any bytes with Protobuf.
        data: Option<Vec<u8>>,
    },
    /// The client moved to [`State::Disconnected`].
    Disconnected {
        /// Why: a client-side code from [`crate::codes::disconnected`] or a
        /// server disconnect code.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// An error that does not change the state, e.g. a failed connection
    /// attempt while the client keeps reconnecting.
    Error(ClientError),
    /// Something happened to the server-side subscription to `channel`: one
    /// the server made, which the client tracks but does not control (spec,
    /// "Server-side subscriptions").
    ServerSubscription {
        /// The channel subscribed by the server.
        channel: String,
        /// What happened.
        event: ServerSubscriptionEvent,
    },
    /// An asynchronous message from the server (`client.proto`,
    /// `Message.data`): raw JSON with the JSON protocol, any bytes with
    /// Protobuf, exactly as received.
    Message(Vec<u8>),
}

/// Something that happened to a server-side subscription, as for a
/// client-side one (spec, "Server-side subscriptions").
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerSubscriptionEvent {
    /// The connection was lost or `disconnect()` was called: the
    /// subscription stays in the registry and is restored on the next
    /// connection.
    Subscribing,
    /// The server subscribed the connection, at connect or later.
    Subscribed(Subscribed),
    /// The server ended the subscription, which leaves the registry: a code
    /// of the server's `Unsubscribe` push, or
    /// [`crate::codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`].
    Unsubscribed {
        /// Why: a server unsubscribe code or a client-side one.
        code: u32,
        /// Human-readable reason.
        reason: String,
    },
    /// A publication in the channel.
    Publication(Publication),
    /// A client subscribed to the channel (a join push).
    Join(ClientInfo),
    /// A client unsubscribed from the channel (a leave push).
    Leave(ClientInfo),
}

impl ServerSubscriptionEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Subscribing => "subscribing",
            Self::Subscribed(_) => "subscribed",
            Self::Unsubscribed { .. } => "unsubscribed",
            Self::Publication(_) => "publication",
            Self::Join(_) => "join",
            Self::Leave(_) => "leave",
        }
    }
}

impl std::fmt::Debug for ClientEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connecting { code, reason } => f
                .debug_struct("Connecting")
                .field("code", code)
                .field("reason", reason)
                .finish(),
            Self::Connected {
                client_id,
                transport,
                data,
            } => f
                .debug_struct("Connected")
                .field("client_id", client_id)
                .field("transport", transport)
                .field("data", &optional_bytes(data.as_ref()))
                .finish(),
            Self::Disconnected { code, reason } => f
                .debug_struct("Disconnected")
                .field("code", code)
                .field("reason", reason)
                .finish(),
            Self::Error(error) => f.debug_tuple("Error").field(error).finish(),
            Self::ServerSubscription { channel, event } => f
                .debug_struct("ServerSubscription")
                .field("channel", channel)
                .field("event", event)
                .finish(),
            Self::Message(data) => f.debug_tuple("Message").field(&Bytes(data)).finish(),
        }
    }
}

impl ClientEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connecting { .. } => "connecting",
            Self::Connected { .. } => "connected",
            Self::Disconnected { .. } => "disconnected",
            Self::Error(_) => "error",
            Self::ServerSubscription { event, .. } => event.name(),
            Self::Message(_) => "message",
        }
    }
}

/// Details of a [`ClientEvent::Error`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    /// Where the error happened.
    pub kind: ErrorKind,
    /// A client-side code from [`crate::codes::error`], or a server error code.
    pub code: u32,
    /// Human-readable description.
    pub message: String,
    /// Transport the error happened on, when it is transport-related.
    pub transport: Option<String>,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)?;
        if let Some(transport) = &self.transport {
            write!(f, " over {transport}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClientError {}

/// Origin of a [`ClientError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The transport failed to open, or closed before it opened. As in
    /// centrifuge-js, a transport that opened and then closed before the
    /// connect reply is not reported as an error: the client, still
    /// connecting, schedules the next attempt without an event.
    Transport,
    /// The server answered the connect command with a temporary error.
    Connect,
    /// The token callback failed while getting a token to connect with.
    ConnectToken,
    /// The connect data callback failed; the client retries after the
    /// reconnect backoff (Block 17).
    ConnectData,
    /// The token callback failed while getting a token to refresh with.
    RefreshToken,
    /// The server answered the refresh command with a temporary error.
    Refresh,
    /// The client options cannot work, e.g. a token expires without a
    /// token callback to refresh it.
    Configuration,
    /// A subscription data callback failed; the subscription retries.
    SubscriptionData,
    /// The server answered a subscribe command with a temporary error; the
    /// subscription retries.
    Subscribe,
    /// A subscription token callback failed while getting a token to
    /// subscribe with; the subscription retries.
    SubscribeToken,
    /// A subscription token callback failed while getting a token to
    /// refresh with; the refresh is retried.
    SubscriptionRefreshToken,
    /// The server answered a `sub_refresh` command with a temporary error,
    /// or not in time; the refresh is retried.
    SubscriptionRefresh,
    /// A delta publication could not be applied: the subscription
    /// subscribes again, or, for a server-side subscription, the client
    /// reconnects (Block 19, D8).
    Delta,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{ClientEvent, ServerSubscriptionEvent};
    use crate::{ClientInfo, Publication, Subscribed, SubscriptionEvent};

    const SECRET: &str = "s3cr3t-d4t4";

    fn secret() -> Vec<u8> {
        SECRET.as_bytes().to_vec()
    }

    fn info() -> ClientInfo {
        ClientInfo {
            client: "c".to_owned(),
            user: "u".to_owned(),
            conn_info: Some(secret()),
            chan_info: Some(secret()),
        }
    }

    fn publication() -> Publication {
        Publication {
            data: secret(),
            tags: HashMap::from([("kind".to_owned(), SECRET.to_owned())]),
            info: Some(info()),
            ..Publication::default()
        }
    }

    fn subscribed() -> Subscribed {
        Subscribed {
            data: Some(secret()),
            ..Subscribed::default()
        }
    }

    /// Events are logged at TRACE through their `Debug` output (Block 21,
    /// D5): no data, info or tag value may show, as text or as bytes.
    #[test]
    fn the_debug_output_of_events_shows_no_data() {
        let events: Vec<Box<dyn std::fmt::Debug>> = vec![
            Box::new(ClientEvent::Connected {
                client_id: "c".to_owned(),
                transport: "websocket".to_owned(),
                data: Some(secret()),
            }),
            Box::new(ClientEvent::Message(secret())),
            Box::new(ClientEvent::ServerSubscription {
                channel: "alerts".to_owned(),
                event: ServerSubscriptionEvent::Publication(publication()),
            }),
            Box::new(ServerSubscriptionEvent::Subscribed(subscribed())),
            Box::new(ServerSubscriptionEvent::Join(info())),
            Box::new(SubscriptionEvent::Subscribed(subscribed())),
            Box::new(SubscriptionEvent::Publication(publication())),
            Box::new(SubscriptionEvent::Leave(info())),
        ];
        let as_bytes = format!("{:?}", SECRET.as_bytes());
        let as_bytes = as_bytes.trim_matches(['[', ']']);
        for event in events {
            let shown = format!("{event:?}");
            assert!(!shown.contains(SECRET), "{shown}");
            assert!(!shown.contains(as_bytes), "{shown}");
            assert!(shown.contains("<11 bytes>"), "{shown}");
        }
    }
}
