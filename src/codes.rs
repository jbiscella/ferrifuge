//! Client-side codes carried by events and errors.
//!
//! How to read a code: a code means something only with the event or error
//! that carries it: 1 is "transport closed" in `Connecting` and "timeout"
//! in an error. The same fields also carry server codes, in their own
//! ranges (the last table). The client's logs show the state, code and
//! reason. Values follow centrifuge-js 63ed73e `src/codes.ts`; Ferrifuge
//! adds `connecting` 5, 6 and 7, `subscribing` 2, `unsubscribed` 3 to 8 (7
//! and 8 with `unstable`), and errors 15, 16 and 17; error 14 is
//! centrifuge-js's shared-poll code.
//!
//! ## `connecting`
//!
//! [`ClientEvent::Connecting`](crate::ClientEvent::Connecting): the client lost or has not yet made its connection and is trying again.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`connecting::CONNECT_CALLED`] | `"connect called"` | `connect()` was called | at once |
//! | 1 | [`connecting::TRANSPORT_CLOSED`] | `"transport closed"` | the transport failed or closed, an undecodable frame included | the next transport of a first fallback sequence at once, else after the reconnect backoff (and the jitter window when it was connected) |
//! | 2 | [`connecting::NO_PING`] | `"no ping"` | no data from the server within its ping interval plus `max_server_ping_delay`, or a system sleep longer than that | after the reconnect backoff |
//! | 3 | [`connecting::SUBSCRIBE_TIMEOUT`] | `"subscribe timeout"`; `"shared poll request timeout"` for a shared poll request | a subscribe got no reply within `Config::timeout`, so the server's state is unknown; over an emulated transport, a shared poll track or untrack request got none, so whether its keys are tracked is unknown (`unstable`) | after the reconnect backoff |
//! | 4 | [`connecting::UNSUBSCRIBE_ERROR`] | `"unsubscribe error"` | an unsubscribe failed or got no reply in time | after the reconnect backoff |
//! | 5 | [`connecting::DELTA_ERROR`] | `"delta error"` | a server-side subscription received a delta publication | after the reconnect backoff, which this connection did not reset |
//! | 6 | [`connecting::RECONNECT_CALLED`] | `"reconnect called"` | `reconnect()` was called | at once, after the jitter window when it was connected |
//! | 7 | [`connecting::READ_AHEAD_OVERFLOW`] | `"read-ahead overflow"` | a message arrived while the inbox of a browser transport was full, the application not reading its streams | after the reconnect backoff and the jitter window; before a server ping on that connection, the backoff keeps growing |
//!
//! ## `disconnected`
//!
//! [`ClientEvent::Disconnected`](crate::ClientEvent::Disconnected): the client stopped trying.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`disconnected::DISCONNECT_CALLED`] | `"disconnect called"` | `disconnect()` or `close()` was called, or the client was dropped | never, until `connect()`; never after `close()` |
//! | 1 | [`disconnected::UNAUTHORIZED`] | `"unauthorized"` | a token or connect data callback denied access, returned an empty token, or a token expired without a callback | never, until `connect()` |
//! | 2 | [`disconnected::BAD_PROTOCOL`] | `"exception during message handling"`, or `"exception in event handler: "` and the panic's message | the task running the client panicked, or an event handler did | never: the client is closed; after a handler, until `connect()` |
//! | 3 | [`disconnected::MESSAGE_SIZE_LIMIT`] | `"message size limit exceeded"` | the server closed with 1009, refused a refresh with HTTP 413, or sent a message over `max_message_bytes` once connected | never, until `connect()` |
//! | 4 | [`disconnected::RUNTIME_SHUT_DOWN`] | `"runtime shut down"` | the tokio runtime the client ran on shut down | never: the client is closed |
//!
//! ## `subscribing`
//!
//! [`SubscriptionEvent::Subscribing`](crate::SubscriptionEvent::Subscribing): the subscription is not subscribed now and will be again.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`subscribing::SUBSCRIBE_CALLED`] | `"subscribe called"` | `subscribe()` was called | at once when connected, else when the client connects |
//! | 1 | [`subscribing::TRANSPORT_CLOSED`] | `"transport closed"` | the client lost its connection, or `disconnect()` was called | when the client connects |
//! | 2 | [`subscribing::DELTA_ERROR`] | `"delta error"` | a delta publication could not be applied | after the resubscribe backoff, recovering from the last publication taken |
//!
//! ## `unsubscribed`
//!
//! [`SubscriptionEvent::Unsubscribed`](crate::SubscriptionEvent::Unsubscribed), and a server-side subscription's `Unsubscribed`: the subscription stopped.
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 0 | [`unsubscribed::UNSUBSCRIBE_CALLED`] | `"unsubscribe called"` | `unsubscribe()` was called, or the subscription was removed | never, until `subscribe()` |
//! | 1 | [`unsubscribed::UNAUTHORIZED`] | `"unauthorized"` | a subscription token or data callback denied access, or returned an empty token, or error 109 came without a token callback, or a shared poll subscription's `get_signature` callback denied access (`unstable`) | never, until `subscribe()` |
//! | 2 | [`unsubscribed::CLIENT_CLOSED`] | `"client closed"` | the client was closed: `close()`, dropped, its runtime shut down or its driver panicked | never: the client is closed |
//! | 3 | [`unsubscribed::MESSAGE_SIZE_LIMIT`] | `"message size limit exceeded"` | the server refused a subscribe or `sub_refresh` with HTTP 413 | never, until `subscribe()` |
//! | 4 | [`unsubscribed::NOT_GRANTED_ON_RECONNECT`] | `"not subscribed by the server after reconnect"` | a server-side subscription absent from a reconnect's connect reply | never: it left the registry |
//! | 5 | [`unsubscribed::MAP_STATE_TOO_LARGE`] | `"map state too large"` | a map subscription's load passed its `max_state_bytes` (`unstable`) | never, until `subscribe()` |
//! | 6 | [`unsubscribed::MAP_NOT_SUPPORTED`] | `"map subscriptions not supported"` | the server answered a map subscription as a stream one, as a Centrifugo before v6.8.0 does (`unstable`) | never, until `subscribe()` |
//! | 7 | [`unsubscribed::SHARED_POLL_DELTA_BASES_TOO_LARGE`] | `"shared poll delta bases too large"` | a shared poll subscription's delta bases would pass its `max_delta_bases_bytes` (`unstable`) | never, until `subscribe()` |
//! | 8 | [`unsubscribed::SHARED_POLL_NOT_SUPPORTED`] | `"shared poll subscriptions not supported"` | the server answered a shared poll subscription as a stream one, as a Centrifugo before v6.8.0 does (`unstable`) | never, until `subscribe()` |
//!
//! ## `error`
//!
//! [`ClientError`](crate::ClientError) (in `Error` events and build errors) and [`CallError`](crate::CallError) (failed calls).
//!
//! | Code | Constant | Reason or message | Cause | Then |
//! |---|---|---|---|---|
//! | 1 | [`error::TIMEOUT`] | `"timeout"` | a command got no reply within `Config::timeout`, or a call waited longer for the connection or its subscription; over websocket, a shared poll track or untrack request got none (`unstable`) | a call fails and may succeed when retried; a refresh is retried; for a shared poll request, an `Error` event of kind `Track` or `Untrack`, and the signatures held are tracked again after 1 to 15 s (track only) |
//! | 2 | [`error::TRANSPORT_CLOSED`] | the transport's own message | a transport failed to open or closed while connecting | an `Error` event; the client keeps trying |
//! | 3 | [`error::DISCONNECTED`] | `"disconnected"` | the client entered `disconnected` while a call was pending, or a `ready()` waited | the call fails; a retry can succeed after `connect()` |
//! | 4 | [`error::CLIENT_CLOSED`] | `"client closed"` | the client is closed: `close()`, dropped, its runtime shut down or its driver panicked | the call fails; a retry never succeeds |
//! | 5 | [`error::CONNECT_TOKEN`] | the token callback's message | the token callback failed while getting a token to connect with | an `Error` event; the attempt is retried after the reconnect backoff |
//! | 6 | [`error::REFRESH_TOKEN`] | the token callback's message | the token callback failed while getting a token to refresh with | an `Error` event; the refresh is retried after 5 to 10 s |
//! | 7 | [`error::SUBSCRIPTION_UNSUBSCRIBED`] | `"subscription unsubscribed"` | a subscription-level call or `ready()` met an unsubscribed subscription | the call fails; a retry can succeed after `subscribe()` |
//! | 8 | [`error::SUBSCRIPTION_SUBSCRIBE_TOKEN`] | the token callback's message | a subscription token callback failed while getting a token to subscribe with | an `Error` event; the subscribe is retried after the resubscribe backoff |
//! | 9 | [`error::SUBSCRIPTION_REFRESH_TOKEN`] | the token callback's message | a subscription token callback failed while getting a token to refresh with | an `Error` event; the refresh is retried after 10 to 20 s |
//! | 10 | [`error::TRANSPORT_WRITE`] | `"transport write error"` | an emulation request carrying the call was answered with HTTP 413 | the call fails; a retry with the same data never succeeds |
//! | 11 | [`error::CONNECTION_CLOSED`] | `"connection closed"` | the transport a call was sent on closed before its reply | the call fails; a retry can succeed once reconnected |
//! | 12 | [`error::BAD_CONFIGURATION`] | what is misconfigured, or the data callback's message | a builder that cannot work, a token expiring without a callback, error 109 without a token callback, or a failing connect or subscription data callback, or a shared poll track batch, given or signed by `get_signature`, over `max_track_frame_bytes` (`unstable`) | building fails; for a data callback, an `Error` event and a retry after the backoff; for a track batch given, an `Error` event of kind `Track`, and its keys end with a removal `Update` each; for a signature from `get_signature`, as error 14 |
//! | 13 | [`error::SUBSCRIPTION_GET_STATE`] | the getState callback's message | the subscription getState callback failed | an `Error` event; getState is retried after the resubscribe backoff |
//! | 14 | [`error::SHARED_POLL_GET_SIGNATURE`] | the `get_signature` callback's message; `"get_signature callback required for track(keys)"` without one | a shared poll subscription's `get_signature` callback failed, or keys need a signature and there is no callback (`unstable`) | an `Error` event of kind `Track`, or `SignatureRefresh` when refreshing or signing after a subscribe; the keys stay tracked, and the callback is asked again after 5 to 30 s, growing with each failure; without a callback, the next subscribe reports it again |
//! | 15 | [`error::DELTA`] | what failed in the delta | a delta publication could not be applied | an `Error` event; the subscription subscribes again (`subscribing` 2) or, server-side, the client reconnects (`connecting` 5) |
//! | 16 | [`error::WOULD_DEADLOCK`] | `"would deadlock: called from an event handler of this client"` | a handler of the client waited on a call, `ready()` or `close()` of that client | the call fails at once and is abandoned |
//! | 17 | [`error::PUBLISH_DROPPED`] | `"publish dropped"` | a publish waiting in a debounce window was dropped, never sent, as its subscription left `subscribed` while the client stayed up | the call fails; publish again when subscribed |
//!
//! ## Server codes
//!
//! | Codes | Carried by | Then |
//! |---|---|---|
//! | disconnect 3500–3999, 4500–4999 | `Disconnected` | stays disconnected until `connect()` |
//! | disconnect 3000–3499, 4000–4499, from 5000 | `Connecting` | reconnects after the reconnect backoff |
//! | disconnect 3008 | `Connecting` | as above; before the first server ping on that connection, the backoff keeps growing; during a map subscription's load it means the namespace's `map.subscribe_catch_up_timeout` passed, and the next connection loads again |
//! | disconnect 3014 | `Connecting` | drops the tokens a token callback can fetch again, keeps a fixed token without one, makes every stream position unmatchable (drops a map subscription's position and delta bases), then reconnects |
//! | websocket close 1009 | `Disconnected` 3 | stays disconnected until `connect()`; other close codes below 3000 give `Connecting` 1 |
//! | unsubscribe below 2500 | `Unsubscribed` | stays unsubscribed |
//! | unsubscribe from 2500 | `Subscribing` | subscribes again at once; 2502 drops the token and the position first; on a map subscription 2500 recovers, and after `map_clear` that recovery fails with 112; any code ends a server-side subscription |
//! | connect error, temporary or below 100, or 109 | `Error`, then `Connecting` | reconnects after the backoff; for 109 the next attempt gets a new token, and without a token callback the client reports error 12, then `Disconnected` 1 |
//! | connect error, any other | `Disconnected` | with the error's code, until `connect()` |
//! | refresh error, temporary or below 100 | `Error` | retried after 5 to 10 s |
//! | refresh error, any other | `Disconnected` | with the error's code |
//! | subscribe error 109 | `Error` or `Unsubscribed` 1 | a new token, then resubscribes; without a token callback, error 12, then `Unsubscribed` 1 |
//! | subscribe error 112 with getState | none | the state is reloaded and the subscription resubscribes after the backoff; on a map subscription, with `FromScratch` no `Error` and the state is loaded again after the backoff, with `Fatal` `Unsubscribed` 112 |
//! | subscribe error 113 | `Error` | temporary: the map subscription's load starts again after the backoff |
//! | subscribe error, temporary or below 100 | `Error` | resubscribes after the resubscribe backoff |
//! | subscribe error, any other | `Unsubscribed` | with the error's code |
//! | `sub_refresh` error, temporary or below 100 | `Error` | retried after 10 to 20 s |
//! | `sub_refresh` error, any other | `Unsubscribed` | with the error's code |
//! | shared poll `sub_refresh` error, `type` 1 (track) | `Error` of kind `Track` | 109: `get_signature` signs every key tracked again at once, and a 109 to that refresh's own track request retries it after 5 to 30 s; without the callback, nothing more until the next subscribe tracks the keys again; a temporary error, one below 100, or no reply within `Config::timeout` over websocket (error 1): the signatures held are tracked again after 1 to 15 s, longer with each failure in a row; any other: the keys of the request are untracked, with an untrack command and a removal `Update` each, as for 103 (signature refused) and 106 (key limit), and those of the request's frames not yet sent with them |
//! | shared poll `sub_refresh` error, `type` 2 (untrack) | `Error` of kind `Untrack` | nothing more |
//! | shared poll subscribe error (`type` 4) | as subscribe errors | as for a stream subscription; 108 on a namespace that is not shared poll |
//! | call error | `CallError` | the server's code, message and `temporary`; the client never retries a call |
// Block 34; Block 21; Block 47; Block 1; Blocks 1, 8; Blocks 4, 25; Block
// 9; Block 47, D12; Block 19, D8; Block 25, D5; Block 44, D7; Block 15;
// Blocks 1, 33; Blocks 1, 5, 17; Block 28, D4; Block 41, D4; Blocks 3, 8,
// 29; Block 28, D3; Blocks 9, 10; Block 47, D6; Block 33; Block 8; Block
// 12; Block 45, D5; Block 45, D15; Block 47, D11; Block 47, D4; Block 13;
// Block 47, D7; Blocks 13, 30; Block 5; Block 10; Blocks 8, 10, 17; Block
// 11; Block 41, D6; Block 46, D10; Block 3; Block 45.

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
    /// client never asks for there.
    // Block 19, D8.
    pub const DELTA_ERROR: u32 = 5;
    /// `reconnect()` was called.
    // Block 25, D5.
    pub const RECONNECT_CALLED: u32 = 6;
    /// A message arrived while the inbox of a browser transport was full:
    /// the application did not read its streams, and the transport does not
    /// stop reading; events not taken are recovered by the next connection
    /// on recoverable channels.
    // Block 44, D7.
    pub const READ_AHEAD_OVERFLOW: u32 = 7;
}

/// Codes of [`crate::ClientEvent::Disconnected`].
pub mod disconnected {
    /// `disconnect()` was called.
    pub const DISCONNECT_CALLED: u32 = 0;
    /// The token callback denied access, or an expiring token could not be
    /// refreshed for lack of a callback.
    pub const UNAUTHORIZED: u32 = 1;
    /// The task running the client panicked (centrifuge-js `badProtocol`,
    /// with which it ends a client whose message handling threw).
    // Block 28, D4.
    pub const BAD_PROTOCOL: u32 = 2;
    /// The server refused a refresh command as too large (an emulation
    /// request answered with HTTP 413); retrying it would fail the same way
    /// (centrifuge-js `messageSizeLimit`).
    pub const MESSAGE_SIZE_LIMIT: u32 = 3;
    /// The tokio runtime the client ran on shut down.
    // Block 28, D3.
    pub const RUNTIME_SHUT_DOWN: u32 = 4;
}

/// Codes of [`crate::SubscriptionEvent::Subscribing`].
pub mod subscribing {
    /// `subscribe()` was called.
    pub const SUBSCRIBE_CALLED: u32 = 0;
    /// The connection closed; the subscription resubscribes on the next.
    pub const TRANSPORT_CLOSED: u32 = 1;
    /// A delta publication could not be applied: the subscription
    /// subscribes again, recovering from its last publication taken.
    // Block 19, D8.
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
    /// its driver panicked (centrifuge-js
    /// `unsubscribedCodes.clientClosed`).
    // Block 33.
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
    /// would end the same way. Only map subscriptions, which need the
    /// `unstable` feature, use it.
    // Block 45, D5.
    pub const MAP_STATE_TOO_LARGE: u32 = 5;
    /// The server answered a map subscription as a stream subscription, as
    /// a Centrifugo before v6.8.0 does. Only map subscriptions, which need
    /// the `unstable` feature, use it.
    // Block 45, D15.
    pub const MAP_NOT_SUPPORTED: u32 = 6;
    /// A shared poll subscription's delta bases would pass its
    /// `max_delta_bases_bytes`: every subscribe would end the same way.
    /// Only shared poll subscriptions, which need the `unstable` feature,
    /// use it.
    // Block 47, D11.
    pub const SHARED_POLL_DELTA_BASES_TOO_LARGE: u32 = 7;
    /// The server answered a shared poll subscription as a stream
    /// subscription, as a Centrifugo before v6.8.0 does. Only shared poll
    /// subscriptions, which need the `unstable` feature, use it.
    // Block 47, D4.
    pub const SHARED_POLL_NOT_SUPPORTED: u32 = 8;
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
    /// its driver panicked; retrying cannot succeed (centrifuge-js
    /// `clientClosed`).
    // Block 33.
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
    /// A shared poll subscription's `get_signature` callback failed
    /// (centrifuge-js's value). Only shared poll subscriptions, which need
    /// the `unstable` feature, use it.
    // Block 47, D6.
    pub const SHARED_POLL_GET_SIGNATURE: u32 = 14;
    /// A delta publication could not be applied.
    // Block 19, D8.
    pub const DELTA: u32 = 15;
    /// A handler of the client waited on that client.
    // Block 41, D6.
    pub const WOULD_DEADLOCK: u32 = 16;
    /// A publish waiting in a debounce window was dropped, never sent, as
    /// its subscription left `subscribed` while the client stayed up; safe
    /// to publish again once subscribed.
    // Block 46, D10.
    pub const PUBLISH_DROPPED: u32 = 17;
}
