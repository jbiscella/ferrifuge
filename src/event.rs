//! Client state and the events reporting its changes.

use crate::sans_io::TransportKind;
use crate::token::{Bytes, optional_bytes};
use crate::{ClientInfo, Publication, SubscribedContext, UnsubscribedContext};

/// Connection state of a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
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
/// logged.
// Block 21.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
// Not `expect`: on 32-bit targets the variants are closer in size.
#[allow(
    clippy::large_enum_variant,
    reason = "a server-side subscription's event is as large as a client-side one; the contexts made the others smaller (Block 54, D4)"
)]
pub enum ClientEvent {
    /// The client moved to [`State::Connecting`].
    Connecting(ConnectingContext),
    /// The client moved to [`State::Connected`].
    Connected(ConnectedContext),
    /// The client moved to [`State::Disconnected`].
    Disconnected(DisconnectedContext),
    /// An error that does not change the state, e.g. a failed connection
    /// attempt while the client keeps reconnecting.
    Error(ClientError),
    /// Something happened to the server-side subscription to `channel`: one
    /// the server made, which the client tracks but does not control (spec,
    /// "Server-side subscriptions").
    #[non_exhaustive]
    ServerSubscription {
        /// The channel subscribed by the server.
        channel: String,
        /// What happened.
        event: ServerSubscriptionEvent,
    },
    /// An asynchronous message from the server (`client.proto`,
    /// `Message.data`): raw JSON with the JSON protocol, any bytes with
    /// Protobuf, exactly as received.
    Message(MessageContext),
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
    Subscribed(SubscribedContext),
    /// The server ended the subscription, which leaves the registry: a code
    /// of the server's `Unsubscribe` push, or
    /// [`crate::codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`].
    Unsubscribed(UnsubscribedContext),
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
            Self::Unsubscribed(_) => "unsubscribed",
            Self::Publication(_) => "publication",
            Self::Join(_) => "join",
            Self::Leave(_) => "leave",
        }
    }
}

impl std::fmt::Debug for ClientEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connecting(context) => f.debug_tuple("Connecting").field(context).finish(),
            Self::Connected(context) => f.debug_tuple("Connected").field(context).finish(),
            Self::Disconnected(context) => f.debug_tuple("Disconnected").field(context).finish(),
            Self::Error(error) => f.debug_tuple("Error").field(error).finish(),
            Self::ServerSubscription { channel, event } => f
                .debug_struct("ServerSubscription")
                .field("channel", channel)
                .field("event", event)
                .finish(),
            Self::Message(context) => f.debug_tuple("Message").field(context).finish(),
        }
    }
}

impl ClientEvent {
    /// Name of the event as used by the Client SDK spec.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connecting(_) => "connecting",
            Self::Connected(_) => "connected",
            Self::Disconnected(_) => "disconnected",
            Self::Error(_) => "error",
            Self::ServerSubscription { event, .. } => event.name(),
            Self::Message(_) => "message",
        }
    }
}

/// Details of a [`ClientEvent::Connecting`].
// Block 54, D4.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectingContext {
    /// Why: a client-side code from [`crate::codes::connecting`] or a
    /// server disconnect code.
    pub code: u32,
    /// Human-readable reason.
    pub reason: String,
}

/// Details of a [`ClientEvent::Connected`]. Its `Debug` output shows the
/// data as its length.
// Block 54, D4; Block 21.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectedContext {
    /// Connection id assigned by the server.
    pub client_id: String,
    /// Transport in use.
    pub transport: TransportKind,
    /// The data the server attached to the connect reply
    /// (`ConnectResult.data`), exactly as received: raw JSON with the JSON
    /// protocol, any bytes with Protobuf.
    pub data: Option<Vec<u8>>,
}

impl std::fmt::Debug for ConnectedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectedContext")
            .field("client_id", &self.client_id)
            .field("transport", &self.transport)
            .field("data", &optional_bytes(self.data.as_ref()))
            .finish()
    }
}

/// Details of a [`ClientEvent::Message`]. Its `Debug` output shows the data
/// as its length.
// Block 54, D4; Block 21.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MessageContext {
    /// The message's data (`client.proto`, `Message.data`): raw JSON with
    /// the JSON protocol, any bytes with Protobuf, exactly as received.
    pub data: Vec<u8>,
}

impl std::fmt::Debug for MessageContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageContext")
            .field("data", &Bytes(&self.data))
            .finish()
    }
}

/// Details of a [`ClientEvent::Disconnected`].
// Block 54, D4.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DisconnectedContext {
    /// Why: a client-side code from [`crate::codes::disconnected`] or a
    /// server disconnect code.
    pub code: u32,
    /// Human-readable reason.
    pub reason: String,
}

/// Details of a [`ClientEvent::Error`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientError {
    /// Where the error happened.
    pub kind: ErrorKind,
    /// A client-side code from [`crate::codes::error`], or a server error code.
    pub code: u32,
    /// Human-readable description.
    pub message: String,
    /// Transport the error happened on, when it is transport-related.
    pub transport: Option<TransportKind>,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)?;
        if let Some(transport) = &self.transport {
            write!(f, " over {}", transport.as_str())?;
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
    /// connecting, schedules the next attempt without an event. A
    /// WebTransport stream that ends before the server answered is
    /// reported.
    // Block 48, D9.
    Transport,
    /// The server answered the connect command with a temporary error.
    Connect,
    /// The token callback failed while getting a token to connect with.
    ConnectToken,
    /// The connect data callback failed; the client retries after the
    /// reconnect backoff.
    // Block 17.
    ConnectData,
    /// The token callback failed while getting a token to refresh with.
    RefreshToken,
    /// The server answered the refresh command with a temporary error.
    Refresh,
    /// The client options cannot work, e.g. a token expires without a
    /// token callback to refresh it.
    Configuration,
    /// A subscription data callback failed; the subscription retries.
    SubscribeData,
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
    /// reconnects.
    // Block 19, D8.
    Delta,
    /// A shared poll subscription's track request failed: the server
    /// refused it, or the `get_signature` callback failed for the keys
    /// given to `track`, or there is none.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    // Block 47, D7, D6.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    Track,
    /// The server refused a shared poll subscription's untrack request; the
    /// keys stay untracked on the client.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    // Block 47, D7.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    Untrack,
    /// A shared poll subscription's `get_signature` callback failed while
    /// refreshing the signatures held, or while signing the keys tracked
    /// without one after a subscribe; it is retried after 5 s to 30 s.
    ///
    /// Unstable: requires the `unstable` feature, follows centrifuge-js
    /// 63ed73e and Centrifugo's experimental shared poll subscriptions
    /// (v6.8.0 and later), and may change in any minor release.
    // Block 47, D6, D7.
    #[cfg(feature = "unstable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable")))]
    SignatureRefresh,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{ClientEvent, ServerSubscriptionEvent, State};
    use crate::{ClientInfo, Publication, SubscribedContext, SubscriptionEvent};

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

    fn subscribed() -> SubscribedContext {
        SubscribedContext {
            data: Some(secret()),
            ..SubscribedContext::default()
        }
    }

    /// Events are logged at TRACE through their `Debug` output (Block 21,
    /// D5): no data, info or tag value may show, as text or as bytes.
    #[test]
    fn the_debug_output_of_events_shows_no_data() {
        let events: Vec<Box<dyn std::fmt::Debug>> = vec![
            Box::new(ClientEvent::Connected(crate::ConnectedContext {
                client_id: "c".to_owned(),
                transport: crate::TransportKind::WebSocket,
                data: Some(secret()),
            })),
            Box::new(ClientEvent::Message(crate::MessageContext {
                data: secret(),
            })),
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

    #[test]
    fn the_debug_output_of_a_publication_shows_its_channel() {
        let publication = Publication {
            channel: Some("news.sport".to_owned()),
            ..publication()
        };

        assert!(format!("{publication:?}").contains(r#"channel: Some("news.sport")"#));
    }

    #[test]
    fn each_state_is_named_as_the_spec_names_it() {
        assert_eq!(
            [State::Disconnected, State::Connecting, State::Connected].map(State::as_str),
            ["disconnected", "connecting", "connected"]
        );
    }
}
