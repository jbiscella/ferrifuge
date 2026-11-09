//! Client-side codes carried by events and errors (Block 34).
//!
//! How to read a code: a code means something only with the event or error
//! that carries it: 1 is "transport closed" in `Connecting` and "timeout" in
//! an error. The same fields also carry server codes, in their own ranges
//! (the last table). The client's logs show the state, code and reason
//! (Block 21). Values follow centrifuge-js 63ed73e `src/codes.ts`; Ferrifuge
//! adds `connecting` 5, 6 and 7, `subscribing` 2, `unsubscribed` 3 to 6, and
//! errors 15 and 16; error 14 is centrifuge-js's shared-poll code, unused until
//! Block 47. Blocks are those of the repository's `CLAUDE.md`.
//!
//! ## `connecting`
//!
//! [`ClientEvent::Connecting`](crate::ClientEvent::Connecting): the client lost or has not yet made its connection and is trying again.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`connecting::CONNECT_CALLED`] | `"connect called"` | `connect()` was called (Block 1) | at once |
//! | 1 | [`connecting::TRANSPORT_CLOSED`] | `"transport closed"` | the transport failed or closed, an undecodable frame included (Blocks 1, 8) | the next transport of a first fallback sequence at once, else after the reconnect backoff (and the jitter window when it was connected) |
//! | 2 | [`connecting::NO_PING`] | `"no ping"` | no data from the server within its ping interval plus `max_server_ping_delay`, or a system sleep longer than that (Blocks 4, 25) | after the reconnect backoff |
//! | 3 | [`connecting::SUBSCRIBE_TIMEOUT`] | `"subscribe timeout"` | a subscribe got no reply within `Config::timeout`, so the server's state is unknown (Block 9) | after the reconnect backoff |
//! | 4 | [`connecting::UNSUBSCRIBE_ERROR`] | `"unsubscribe error"` | an unsubscribe failed or got no reply in time (Block 9) | after the reconnect backoff |
//! | 5 | [`connecting::DELTA_ERROR`] | `"delta error"` | a server-side subscription received a delta publication (Block 19, D8) | after the reconnect backoff, which this connection did not reset |
//! | 6 | [`connecting::RECONNECT_CALLED`] | `"reconnect called"` | `reconnect()` was called (Block 25, D5) | at once, after the jitter window when it was connected |
//! | 7 | [`connecting::READ_AHEAD_OVERFLOW`] | `"read-ahead overflow"` | a message arrived while the inbox of a browser transport was full, the application not reading its streams (Block 44, D7) | after the reconnect backoff and the jitter window; before a server ping on that connection, the backoff keeps growing (Block 15) |
//!
//! ## `disconnected`
//!
//! [`ClientEvent::Disconnected`](crate::ClientEvent::Disconnected): the client stopped trying.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`disconnected::DISCONNECT_CALLED`] | `"disconnect called"` | `disconnect()` or `close()` was called, or the client was dropped (Blocks 1, 33) | never, until `connect()`; never after `close()` |
//! | 1 | [`disconnected::UNAUTHORIZED`] | `"unauthorized"` | a token or connect data callback denied access, returned an empty token, or a token expired without a callback (Blocks 1, 5, 17) | never, until `connect()` |
//! | 2 | [`disconnected::BAD_PROTOCOL`] | `"exception during message handling"`, or `"exception in event handler: "` and the panic's message | the task running the client panicked (Block 28, D4), or an event handler did (Block 41, D4) | never: the client is closed; after a handler, until `connect()` |
//! | 3 | [`disconnected::MESSAGE_SIZE_LIMIT`] | `"message size limit exceeded"` | the server closed with 1009, refused a refresh with HTTP 413, or sent a message over `max_message_bytes` once connected (Blocks 3, 8, 29) | never, until `connect()` |
//! | 4 | [`disconnected::RUNTIME_SHUT_DOWN`] | `"runtime shut down"` | the tokio runtime the client ran on shut down (Block 28, D3) | never: the client is closed |
//!
//! ## `subscribing`
//!
//! [`SubscriptionEvent::Subscribing`](crate::SubscriptionEvent::Subscribing): the subscription is not subscribed now and will be again.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`subscribing::SUBSCRIBE_CALLED`] | `"subscribe called"` | `subscribe()` was called (Block 9) | at once when connected, else when the client connects |
//! | 1 | [`subscribing::TRANSPORT_CLOSED`] | `"transport closed"` | the client lost its connection, or `disconnect()` was called (Block 9) | when the client connects |
//! | 2 | [`subscribing::DELTA_ERROR`] | `"delta error"` | a delta publication could not be applied (Block 19, D8) | after the resubscribe backoff, recovering from the last publication taken |
//!
//! ## `unsubscribed`
//!
//! [`SubscriptionEvent::Unsubscribed`](crate::SubscriptionEvent::Unsubscribed), and a server-side subscription's `Unsubscribed`: the subscription stopped.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`unsubscribed::UNSUBSCRIBE_CALLED`] | `"unsubscribe called"` | `unsubscribe()` was called, or the subscription was removed (Block 9) | never, until `subscribe()` |
//! | 1 | [`unsubscribed::UNAUTHORIZED`] | `"unauthorized"` | a subscription token or data callback denied access, or returned an empty token, or error 109 came without a token callback (Blocks 9, 10) | never, until `subscribe()` |
//! | 2 | [`unsubscribed::CLIENT_CLOSED`] | `"client closed"` | the client was closed: `close()`, dropped, its runtime shut down or its driver panicked (Block 33) | never: the client is closed |
//! | 3 | [`unsubscribed::MESSAGE_SIZE_LIMIT`] | `"message size limit exceeded"` | the server refused a subscribe or `sub_refresh` with HTTP 413 (Block 8) | never, until `subscribe()` |
//! | 4 | [`unsubscribed::NOT_GRANTED_ON_RECONNECT`] | `"not subscribed by the server after reconnect"` | a server-side subscription absent from a reconnect's connect reply (Block 12) | never: it left the registry |
//! | 5 | [`unsubscribed::MAP_STATE_TOO_LARGE`] | `"map state too large"` | a map subscription's load passed its `max_state_bytes` (Block 45, D5; `unstable`) | never, until `subscribe()` |
//! | 6 | [`unsubscribed::MAP_NOT_SUPPORTED`] | `"map subscriptions not supported"` | the server answered a map subscription as a stream one, as a Centrifugo before v6.8.0 does (Block 45, D15; `unstable`) | never, until `subscribe()` |
//!
//! ## `error`
//!
//! [`ClientError`](crate::ClientError) (in `Error` events and build errors) and [`CallError`](crate::CallError) (failed calls).
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 1 | [`error::TIMEOUT`] | `"timeout"` | a command got no reply within `Config::timeout`, or a call waited longer for the connection or its subscription (Block 13) | a call fails and may succeed when retried; a refresh is retried |
//! | 2 | [`error::TRANSPORT_CLOSED`] | the transport's own message | a transport failed to open or closed while connecting (Blocks 1, 8) | an `Error` event; the client keeps trying |
//! | 3 | [`error::DISCONNECTED`] | `"disconnected"` | the client entered `disconnected` while a call was pending, or a `ready()` waited (Blocks 13, 30) | the call fails; a retry can succeed after `connect()` |
//! | 4 | [`error::CLIENT_CLOSED`] | `"client closed"` | the client is closed: `close()`, dropped, its runtime shut down or its driver panicked (Block 33) | the call fails; a retry never succeeds |
//! | 5 | [`error::CONNECT_TOKEN`] | the token callback's message | the token callback failed while getting a token to connect with (Block 1) | an `Error` event; the attempt is retried after the reconnect backoff |
//! | 6 | [`error::REFRESH_TOKEN`] | the token callback's message | the token callback failed while getting a token to refresh with (Block 5) | an `Error` event; the refresh is retried after 5 to 10 s |
//! | 7 | [`error::SUBSCRIPTION_UNSUBSCRIBED`] | `"subscription unsubscribed"` | a subscription-level call or `ready()` met an unsubscribed subscription (Blocks 13, 30) | the call fails; a retry can succeed after `subscribe()` |
//! | 8 | [`error::SUBSCRIPTION_SUBSCRIBE_TOKEN`] | the token callback's message | a subscription token callback failed while getting a token to subscribe with (Block 10) | an `Error` event; the subscribe is retried after the resubscribe backoff |
//! | 9 | [`error::SUBSCRIPTION_REFRESH_TOKEN`] | the token callback's message | a subscription token callback failed while getting a token to refresh with (Block 10) | an `Error` event; the refresh is retried after 10 to 20 s |
//! | 10 | [`error::TRANSPORT_WRITE`] | `"transport write error"` | an emulation request carrying the call was answered with HTTP 413 (Block 8) | the call fails; a retry with the same data never succeeds |
//! | 11 | [`error::CONNECTION_CLOSED`] | `"connection closed"` | the transport a call was sent on closed before its reply (Block 13) | the call fails; a retry can succeed once reconnected |
//! | 12 | [`error::BAD_CONFIGURATION`] | what is misconfigured, or the data callback's message | a builder that cannot work, a token expiring without a callback, error 109 without a token callback, or a failing connect or subscription data callback (Blocks 8, 10, 17) | building fails; for a data callback, an `Error` event and a retry after the backoff |
//! | 13 | [`error::SUBSCRIPTION_GET_STATE`] | the getState callback's message | the subscription getState callback failed (Block 11) | an `Error` event; getState is retried after the resubscribe backoff |
//! | 15 | [`error::DELTA`] | what failed in the delta | a delta publication could not be applied (Block 19, D8) | an `Error` event; the subscription subscribes again (`subscribing` 2) or, server-side, the client reconnects (`connecting` 5) |
//! | 16 | [`error::WOULD_DEADLOCK`] | `"would deadlock: called from an event handler of this client"` | a handler of the client waited on a call, `ready()` or `close()` of that client (Block 41, D6) | the call fails at once and is abandoned |
//!
//! ## Server codes
//!
//! | Codes | Carried by | Then |
//! |---|---|---|
//! | disconnect 3500–3999, 4500–4999 | `Disconnected` | stays disconnected until `connect()` (Block 3) |
//! | disconnect 3000–3499, 4000–4499, from 5000 | `Connecting` | reconnects after the reconnect backoff (Block 3) |
//! | disconnect 3008 | `Connecting` | as above; before the first server ping on that connection, the backoff keeps growing (Block 15); during a map subscription's load it means the namespace's `map.subscribe_catch_up_timeout` passed, and the next connection loads again (Block 45) |
//! | disconnect 3014 | `Connecting` | drops the tokens, makes every stream position unmatchable (drops a map subscription's position and delta bases, Block 45), then reconnects (Block 11) |
//! | websocket close 1009 | `Disconnected` 3 | stays disconnected until `connect()` (Block 3); other close codes below 3000 give `Connecting` 1 |
//! | unsubscribe below 2500 | `Unsubscribed` | stays unsubscribed (Block 10) |
//! | unsubscribe from 2500 | `Subscribing` | subscribes again at once (Block 10); 2502 drops the token and the position first (Block 11); on a map subscription 2500 recovers, and after `map_clear` that recovery fails with 112 (Block 45); any code ends a server-side subscription (Block 12) |
//! | connect error, temporary or below 100, or 109 | `Error`, then `Connecting` | reconnects after the backoff; for 109 the next attempt gets a new token, and without a token callback the client reports error 12, then `Disconnected` 1 (Blocks 1, 5, 17) |
//! | connect error, any other | `Disconnected` | with the error's code, until `connect()` (Block 1) |
//! | refresh error, temporary or below 100 | `Error` | retried after 5 to 10 s (Block 5) |
//! | refresh error, any other | `Disconnected` | with the error's code (Block 5) |
//! | subscribe error 109 | `Error` or `Unsubscribed` 1 | a new token, then resubscribes; without a token callback, error 12, then `Unsubscribed` 1 (Block 10) |
//! | subscribe error 112 with getState | none | the state is reloaded and the subscription resubscribes after the backoff (Block 11); on a map subscription, with `FromScratch` no `Error` and the state is loaded again after the backoff, with `Fatal` `Unsubscribed` 112 (Block 45) |
//! | subscribe error 113 | `Error` | temporary: the map subscription's load starts again after the backoff (Block 45) |
//! | subscribe error, temporary or below 100 | `Error` | resubscribes after the resubscribe backoff (Block 9) |
//! | subscribe error, any other | `Unsubscribed` | with the error's code (Block 9) |
//! | `sub_refresh` error, temporary or below 100 | `Error` | retried after 10 to 20 s (Block 10) |
//! | `sub_refresh` error, any other | `Unsubscribed` | with the error's code (Block 10) |
//! | call error | `CallError` | the server's code, message and `temporary`; the client never retries a call (Block 13) |

/// Codes of [`crate::ClientEvent::Connecting`].
pub mod connecting {
    /// `connect()` was called.
    pub const CONNECT_CALLED: u32 = 0;
    /// The transport closed; the client reconnects.
    pub const TRANSPORT_CLOSED: u32 = 1;
    /// No data, not even a ping, arrived from the server in time.
    pub const NO_PING: u32 = 2;
    /// A subscribe command got no reply in time: the server-side
    /// subscription state is unknown, so the client reconnects.
    pub const SUBSCRIBE_TIMEOUT: u32 = 3;
    /// An unsubscribe command failed or got no reply in time: the
    /// server-side subscription state is unknown, so the client reconnects.
    pub const UNSUBSCRIBE_ERROR: u32 = 4;
    /// A server-side subscription received a delta publication, which the
    /// client never asks for there (Block 19, D8).
    pub const DELTA_ERROR: u32 = 5;
    /// `reconnect()` was called (Block 25, D5).
    pub const RECONNECT_CALLED: u32 = 6;
    /// A message arrived while the inbox of a browser transport was full:
    /// the application did not read its streams, and the transport does not
    /// stop reading (Block 44, D7); events not taken are recovered by the next
    /// connection on recoverable channels.
    pub const READ_AHEAD_OVERFLOW: u32 = 7;
}

/// Codes of [`crate::ClientEvent::Disconnected`].
pub mod disconnected {
    /// `disconnect()` was called.
    pub const DISCONNECT_CALLED: u32 = 0;
    /// The token callback denied access, or an expiring token could not be
    /// refreshed for lack of a callback.
    pub const UNAUTHORIZED: u32 = 1;
    /// The task running the client panicked (Block 28, D4; centrifuge-js
    /// `badProtocol`, with which it ends a client whose message handling
    /// threw).
    pub const BAD_PROTOCOL: u32 = 2;
    /// The server refused a refresh command as too large (an emulation
    /// request answered with HTTP 413); retrying it would fail the same way
    /// (centrifuge-js `messageSizeLimit`).
    pub const MESSAGE_SIZE_LIMIT: u32 = 3;
    /// The tokio runtime the client ran on shut down (Block 28, D3).
    pub const RUNTIME_SHUT_DOWN: u32 = 4;
}

/// Codes of [`crate::SubscriptionEvent::Subscribing`].
pub mod subscribing {
    /// `subscribe()` was called.
    pub const SUBSCRIBE_CALLED: u32 = 0;
    /// The connection closed; the subscription resubscribes on the next.
    pub const TRANSPORT_CLOSED: u32 = 1;
    /// A delta publication could not be applied: the subscription
    /// subscribes again, recovering from its last publication taken
    /// (Block 19, D8).
    pub const DELTA_ERROR: u32 = 2;
}

/// Codes of [`crate::SubscriptionEvent::Unsubscribed`], besides server error
/// codes.
pub mod unsubscribed {
    /// `unsubscribe()` was called, or the subscription was removed.
    pub const UNSUBSCRIBE_CALLED: u32 = 0;
    /// A callback denied access.
    pub const UNAUTHORIZED: u32 = 1;
    /// The client was closed: `close()`, dropped, its runtime shut down or
    /// its driver panicked (Block 33; centrifuge-js
    /// `unsubscribedCodes.clientClosed`).
    pub const CLIENT_CLOSED: u32 = 2;
    /// The server refused a subscribe or `sub_refresh` command as too large
    /// (an emulation request answered with HTTP 413); retrying it would fail
    /// the same way. 3, as the disconnect code of the same cause.
    pub const MESSAGE_SIZE_LIMIT: u32 = 3;
    /// A server-side subscription the server did not subscribe again after
    /// a reconnect (absent from the connect reply's `subs`, which carries no
    /// code for it).
    pub const NOT_GRANTED_ON_RECONNECT: u32 = 4;
    /// A map subscription's load passed its `max_state_bytes`: every load
    /// would end the same way (Block 45, D5). Only map subscriptions, which
    /// need the `unstable` feature, use it.
    pub const MAP_STATE_TOO_LARGE: u32 = 5;
    /// The server answered a map subscription as a stream subscription, as
    /// a Centrifugo before v6.8.0 does (Block 45, D15). Only map
    /// subscriptions, which need the `unstable` feature, use it.
    pub const MAP_NOT_SUPPORTED: u32 = 6;
}

/// Codes of [`crate::ClientError`] and [`crate::CallError`].
pub mod error {
    /// The server did not reply to a command in time, or a call waited for
    /// the connection longer than the timeout.
    pub const TIMEOUT: u32 = 1;
    /// The transport failed to open or closed while connecting.
    pub const TRANSPORT_CLOSED: u32 = 2;
    /// The client entered `disconnected` while a call was pending
    /// (centrifuge-js `clientDisconnected`).
    pub const DISCONNECTED: u32 = 3;
    /// The client is closed: `close()`, dropped, its runtime shut down or
    /// its driver panicked; retrying cannot succeed (Block 33; centrifuge-js
    /// `clientClosed`).
    pub const CLIENT_CLOSED: u32 = 4;
    /// The token callback failed while getting a token to connect with.
    pub const CONNECT_TOKEN: u32 = 5;
    /// The token callback failed while getting a token to refresh with.
    pub const REFRESH_TOKEN: u32 = 6;
    /// A subscription-level call was made, or was waiting, while its
    /// subscription is unsubscribed (centrifuge-js
    /// `subscriptionUnsubscribed`).
    pub const SUBSCRIPTION_UNSUBSCRIBED: u32 = 7;
    /// A subscription token callback failed while getting a token to
    /// subscribe with (centrifuge-js `subscriptionSubscribeToken`).
    pub const SUBSCRIPTION_SUBSCRIBE_TOKEN: u32 = 8;
    /// A subscription token callback failed while getting a token to
    /// refresh with (centrifuge-js `subscriptionRefreshToken`).
    pub const SUBSCRIPTION_REFRESH_TOKEN: u32 = 9;
    /// The server refused a request carrying the command without closing
    /// the transport: an emulation request answered with HTTP 413
    /// (centrifuge-js `transportWriteError`).
    pub const TRANSPORT_WRITE: u32 = 10;
    /// The transport a call was sent on closed before its reply arrived
    /// (centrifuge-js `connectionClosed`).
    pub const CONNECTION_CLOSED: u32 = 11;
    /// The client is misconfigured, e.g. a token expires without a callback.
    pub const BAD_CONFIGURATION: u32 = 12;
    /// The subscription getState callback failed (centrifuge-js
    /// `subscriptionGetState`).
    pub const SUBSCRIPTION_GET_STATE: u32 = 13;
    /// A delta publication could not be applied (Block 19, D8).
    pub const DELTA: u32 = 15;
    /// A handler of the client waited on that client (Block 41, D6).
    pub const WOULD_DEADLOCK: u32 = 16;
}
