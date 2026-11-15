# Resilience guide

What the client does by itself when the network, the server or the
application fails, what the application must do, what the client
guarantees, and how to tell one failure from another. The
[protocol flows](protocol.md) draw each sequence. Codes are written as
there: a client-side code with its kind (`connecting` 2 "no ping"), a
server code after the kind of message that carries it (disconnect 3008).
The [codes table](../README.md#codes) lists every code. Blocks are those of
the repository's [CLAUDE.md](../CLAUDE.md).

Sources: Block 34 D1; Block 59 D3; Block 59 D4

## Strategies

Each strategy says what it is for, what the client does by itself, what the
application must do, and one way it fails.

Sources: Block 59 D3

### Backoff and jitter

- **What it is for:** reconnecting soon after a short outage without
  hammering a server that is down or overloaded.
- **What the client does:** attempt n waits
  min(max, min + rand(0, min(max, min × 2ⁿ))). For connections, min and max
  are `min_reconnect_delay` (default 500 ms) and
  `max_reconnect_delay` (default 20 s). For subscriptions they are
  `min_resubscribe_delay` (default 500 ms) and
  `max_resubscribe_delay` (default 20 s), set on each subscription. The first attempt after `connect()` is immediate, and so is
  each transport of the first fallback round. The connection's counter
  restarts on `connect()` and `reconnect()`, and once a connection proves
  usable (see
  [Disconnect and reconnect](protocol.md#disconnect-and-reconnect)). A
  subscription's counter restarts when it is subscribed.
- **What the application must do:** choose bounds that fit its fleet and
  its server. It leaves the retries to the client, and calls `connect()`
  again only after `Disconnected`.
- **Failure example:** a server that closes every connection with
  disconnect 3008 "slow" right after the connect reply, because the
  recovery it would send does not fit its client queue. If the connect reply
  restarted the counter, the client would reconnect at the minimum delay
  forever. Instead, a connection closed with disconnect 3008 before a server
  ping is read keeps the backoff growing, up to `max_reconnect_delay`.

Sources: Block 2 "Delay upper bound per attempt", "Delays are jittered"; Block 8 "A failed fallback sequence is retried after the reconnect backoff"; Block 10 "A successful subscribe resets the resubscribe backoff"; Block 15 "Backoff after 3008", "Recovery larger than the server's queue"; Block 25 D5

### Fleet jitter

- **What it is for:** a fleet that loses its connections at the same
  instant, in a server restart or a network outage, must not reconnect at
  the same instant. Tokens issued at the same instant must not be refreshed
  at the same instant either.
- **What the client does:** the first reconnect after a connected client
  loses its connection waits a random extra delay of up to
  `reconnect_jitter_window` (default 0), on top of the backoff. So does the
  first attempt after `reconnect()` on a connected client, and the
  reconnect a long system sleep causes. `connect()` never waits for it.
  Each token, the connection's and every subscription's, is refreshed at a
  random point between 80% and 90% of its ttl.
- **What the application must do:** set `reconnect_jitter_window` to the
  spread its server can absorb. Block 14's simulation of 100 000 clients
  with a window of 100 s has no second holding more than 2% of the first
  reconnect attempts.
- **Failure example:** with `reconnect_jitter_window` (default 0), a server restart makes
  every client's first reconnect fall between 500 ms and 1 s after the
  close, the bounds of attempt 0.

Sources: Block 2 "Delay upper bound per attempt"; Block 14 "Reconnect jitter window", "Token refresh jitter", "Reconnects of a fleet are spread over the jitter window", "Token refreshes of a fleet are spread", "The jitter window does not delay the first connect"; Block 25 D2; Block 25 D5

### Recovery and getState

- **What it is for:** no gap in a channel's stream across a reconnect, an
  `unsubscribe()` and `subscribe()`, or a `disconnect()` and `connect()`.
- **What the client does:** it keeps each subscription's stream position,
  the epoch and offset of the last publication the application took. On
  resubscribe it asks to recover from that position, delivers the missed
  publications in order, and reports `was_recovering` and `recovered` on
  `Subscribed`. Server-side subscriptions are recovered through the connect
  command. With a `get_state` callback, the subscribe command asks the
  server to refuse a recovery it cannot make: subscribe error 112 then makes
  the client drop the position, wait the resubscribe backoff, call
  `get_state` and subscribe from the position it returns, with no
  `Publication` until then. Disconnect 3014 makes every position
  unmatchable, so the next recovery fails and the application is told.
  Unsubscribe 2502 drops the subscription's position.
- **What the application must do:** use channels with history and
  recovery: a namespace with `history_size` and `history_ttl`, and
  `force_recovery`, or `allow_recovery` with the subscription's
  `recoverable` option. Take each stream before calling `subscribe()`.
  Without `get_state`, reload its state when `Subscribed` reports
  `recovered` false. With `get_state`, read the stream position before the
  state, so that publications made meanwhile are recovered.
- **Failure example:** a device offline for longer than the channel's
  `history_ttl`. Its resubscribe reports `was_recovering` true and
  `recovered` false, and the publications made meanwhile are gone. An
  application that ignores the flag shows stale data.

Sources: Block 11 "Missed publications are recovered", "Publications between unsubscribe and subscribe are recovered", "Publications made while explicitly disconnected are recovered", "Recovery fails when history is gone", "getState reloads state when recovery fails", "Connection state invalidation clears the token and invalidates every position", "State invalidation fetches a new subscription token"; Block 12 "Missed publications of a server-side subscription are recovered"; Block 15 "Delivered means taken"

### Publish idempotency

- **What it is for:** deciding whether to send a call again after it
  failed.
- **What the client does:** it never retries a call. A call that timed out
  while it waited for the connection was never sent. A call that was sent
  and then failed, with `error` 1 "timeout", `error` 11 "connection closed"
  or `error` 3 "disconnected", may or may not have reached the server.
- **What the application must do (guidance, not a rule of the client):** the
  client protocol carries no idempotency key (`client.proto`,
  `PublishRequest`), so a retry after those errors can publish twice.
  Retry only what is safe to repeat, or put an id of the application's own
  in the data and drop duplicates where it is consumed.
- **Failure example:** a publish over a link that drops just after the
  command was written. The server publishes it, its reply is lost, and the
  call fails with `error` 11 "connection closed". A blind retry publishes it
  twice.

Sources: Block 13 "A call that timed out while queued is never sent", "A call without a reply times out", "Losing the transport fails calls already sent at once", "Disconnect fails a call already sent at once"; Block 34 D1

### Token refresh timing

- **What it is for:** keeping a connection and its private channels
  authorized for as long as the application allows, and no longer.
- **What the client does:** it refreshes each token at a random point
  between 80% and 90% of its ttl. A failed refresh is retried after 5 to
  10 s for the connection (`error` 6 when the callback failed), and after
  10 to 20 s for a subscription (`error` 9). A token callback that fails
  before a connect gives `error` 5, and the attempt is retried after the
  reconnect backoff. One that fails before a subscribe gives `error` 8, and
  the subscribe is retried after the resubscribe backoff. A callback that
  denies access, by returning `TokenError::Unauthorized` or an empty token,
  ends the client with `disconnected` 1 "unauthorized", or the subscription
  with `unsubscribed` 1 "unauthorized". After connect error 109 or
  subscribe error 109, the client drops the token and fetches a new one.
  Without a token callback it reports `error` 12, then `disconnected` 1
  "unauthorized" or `unsubscribed` 1 "unauthorized". A system sleep moves
  every refresh earlier by the time slept, so no token that expired during
  the sleep is sent.
- **What the application must do:** give an expiring token a `get_token`
  callback that returns a fresh token. Return `TokenError::Unauthorized`
  only to stop for good, and `TokenError::Failed` for a backend that may
  answer later. Keep the ttl long enough that its last 10% leaves time for
  the callback and its retries.
- **Failure example:** a token backend that is down. The client reports an
  `Error` event every 5 to 10 s while the current token is still valid. If
  the token expires first, Centrifugo closes the connection with
  disconnect 3005 "connection expired", and the client reconnects, fetching
  a new token first.

Sources: Block 1 "Terminal condition while connected"; Block 5 "Token refreshed before expiry keeps the connection"; Block 10 "A failing subscription token callback is retried", "A failing first subscription token fetch is retried before subscribing", "Subscribe error 109 fetches a new token and resubscribes", "Subscription token denied"; Block 14 "Token refresh jitter"; Block 25 D3; `src/sans_io.rs::failed_refresh_fetch_is_reported_and_retried_in_5_to_10_s`; `src/sans_io.rs::failed_connect_fetch_is_reported_and_retried_with_backoff`

### Backpressure

- **What it is for:** an application that reads slowly holds the server
  back instead of growing the client's memory.
- **What the client does:** an event counts as delivered once the
  application takes it from its stream, or once its handler returns.
  Messages read before that wait in a read-ahead of
  `read_ahead_messages` (default 256) and `read_ahead_bytes` (default 4 MiB).
  Once the read-ahead
  is full, a native client stops reading the connection, pauses its no-ping
  deadline, and lets the server's slow-client handling act: disconnect 3008
  "slow" when the server's client queue overflows, or disconnect 3009
  "write error" when a write times out. The read-ahead belongs to the
  connection, so one stream not read holds every subscription of that
  client. A browser client cannot stop reading: it keeps an inbox with the
  same limits and reconnects with `connecting` 7 "read-ahead overflow". One
  message is never larger than `max_message_bytes` (default 64 MiB).
- **What the application must do:** read every stream it took, or drop it:
  a dropped stream takes its events at once. Move slow work off the loop
  that reads the stream. On recoverable channels a reconnect recovers what
  was missed, within the server's recovery limit. On other channels a slow
  consumer can lose publications. On the server, keep
  `client.queue_max_size` above `client.recovery_max_publication_limit`
  times the largest publication.
- **Failure example:** a loop that writes each publication to a slow
  database before taking the next. The read-ahead fills, the server drops
  the client with disconnect 3008, and the reconnect recovers the backlog,
  only for the loop to fall behind again, the backoff growing each time.

Sources: Block 15 "Bounded read-ahead", "A full read-ahead does not make the client report missing pings", "Head-of-line", "No stream, no hold-up", "Channels without recovery", "Recovery larger than the server's queue"; Block 29 D2; Block 41 D3; Block 44 D7

### Sleep and resume

- **What it is for:** laptops and phones that sleep and change networks
  resume their streams promptly and recover what they missed.
- **What the client does:** it notices a sleep at its first timer, I/O or
  call after the wake, at the latest at the no-ping deadline, by comparing
  the wall clock with the monotonic clock (a gap of at least 2 s). A sleep
  at least as long as the no-ping window closes the transport and
  reconnects with `connecting` 2 "no ping", after the backoff and the
  jitter window. A shorter one keeps the connection and restarts the
  no-ping deadline. Token refreshes move earlier by the time slept. Reply
  deadlines, call timeouts and backoff delays are left alone. On Windows
  the monotonic clock counts a suspend, so no sleep is noticed there. In the
  browser, the page's `offline` event closes the connection, and its
  `online` event, like a `pageshow` from the back/forward cache, reconnects
  a connecting client at once, or after the jitter window when it has just
  lost a connection.
- **What the application must do:** on its platform's wake or
  network-change notification, call `reconnect()`. A connected client then
  reconnects with `connecting` 6 "reconnect called", with no backoff but
  with the jitter window, starting from the first transport and reading the
  proxy settings again. `reconnect()` does nothing to a disconnected
  client. Call `disconnect()` and then `connect()` instead when calls
  waiting for the connection should fail.
- **Failure example:** a phone that slept for ten minutes. The server
  dropped its connection long ago. Without `reconnect()`, the client
  notices only at its first timer or I/O after the wake, which can be the
  no-ping deadline, up to 35 s later with the server's 25 s pings and the
  default `max_server_ping_delay`.

Sources: Block 25 D1; Block 25 D2; Block 25 D3; Block 25 D4; Block 25 D5; Block 25 D6; Block 25 D7; Block 25 "A sleep as long as the no-ping window reconnects after the backoff and jitter", "A shorter sleep keeps the connection and restarts the no-ping window"; Block 44 D10; Block 44 D11; [Sleep and network changes](../README.md#sleep-and-network-changes)

## Delivery guarantees

- **A call:** `Ok` is the server's reply to it; `Err` is either the
  server's error, with its code, message and `temporary` flag, or a
  client-side error. The client never retries a call.
- **Order:** messages are handled in the order the server sent them, across
  connections, each only after the events of earlier ones are taken. A
  call's reply resolves the call when it is read, so call results are not
  ordered with events.
- **Delivered means taken:** an event counts as delivered when the
  application takes it from its stream, or when its handler returns. Only
  then does a publication's stream position move. Events of an object whose
  stream was never taken, or was dropped, count as delivered at once.
- **Exactly once:** on channels with history and recovery, while the
  backlog stays within the server's `client.recovery_max_publication_limit`
  (Centrifugo default 300), for an application that takes its streams, or
  sets its handlers, before calling `connect()` and `subscribe()`. Past the
  limit, `Subscribed` reports `recovered` false.
- **Pending calls across a reconnect:** a call sent on the lost transport
  fails at once with `error` 11 "connection closed". A call not sent yet
  waits for the next connection, within `timeout` (default 5 s), and fails
  with `error` 1 "timeout" without being sent if none comes in time.
  `disconnect()` or a terminal condition fails every pending call with
  `error` 3 "disconnected".
- **Subscriptions across a reconnect:** a client-side subscription goes to
  `subscribing` 1 "transport closed" and subscribes again on the next
  connection, also after `disconnect()` and `connect()`. Server-side
  subscriptions are tracked again from the connect reply; one the server no
  longer grants ends with `unsubscribed` 4 "not subscribed by the server
  after reconnect". Publications not taken when a subscription leaves
  `Subscribed` for a client-side reason are withdrawn, and recovery brings
  them back.
- **Channels without recovery:** a reconnect, or a slow consumer, can lose
  publications, with no further signal.

Sources: Block 9 "Resubscribe after reconnect", "Subscriptions survive an explicit disconnect and connect"; Block 12 "A server-side subscription missing after reconnect is unsubscribed"; Block 13 "call replies", "Losing the transport fails calls already sent at once", "A call that timed out while queued is never sent", "Disconnect fails a call already sent at once"; Block 15 "Delivered means taken", "In order, one message at a time", "No stream, no hold-up", "Withdrawal", "Channels without recovery"; Block 34 D1; Block 41 D3

## Transport matrix

| Transport | JSON | Protobuf | Native | Browser | Notes |
|---|---|---|---|---|---|
| websocket | yes | yes | yes | yes | the default; Protobuf uses the `centrifuge-protobuf` subprotocol |
| http_stream | yes | yes | yes | yes, over `fetch` | needs Centrifugo's `http_stream.enabled` (Centrifugo default false); Safari can hold back the end of a large message after a reconnect (issue #268, see the README's Browser section) |
| sse | yes | no: building the client fails | yes | yes, over `EventSource` | needs Centrifugo's `sse.enabled` (Centrifugo default false) |
| webtransport (unstable) | yes | yes | with the `unstable-webtransport` feature | with the `unstable` feature | TLS and UDP; Centrifugo v6.6.1 through v6.10.3 select no ALPN: a native client retries without it, a browser cannot, so its handshake fails and it falls back, until a release with centrifugal/centrifugo#1323 |

The client tries the configured transports in order during the first
handshake. Reconnects keep the transport that worked, and `reconnect()`
starts from the first one again.

Sources: Block 0; Block 7; Block 8 "Protobuf with the sse transport is rejected", "Fallback to http_stream when websocket is unreachable", "A transport that worked is kept on reconnect"; Block 25 D6; Block 27; Block 44 D7; Block 48 D5; Block 48 D17; [Browser](../README.md#browser); [WebTransport (unstable)](../README.md#webtransport-unstable)

## Observability

- **Events to log:** every `Connecting`, `Disconnected`, `Subscribing` and
  `Unsubscribed` with its code and reason; every `Error` event with its
  kind, code and message; `Subscribed`'s `was_recovering` and `recovered`;
  the transport `Connected` names. The code says what happened and what the
  client does next ([codes table](../README.md#codes)).
- **The client's logs:** records through `tracing`, with targets starting
  with `ferrifuge`, and through `log` when no `tracing` subscriber is set.
  WARN for server anomalies; DEBUG for state changes with their codes and
  reasons, transport attempts and fallbacks, scheduled reconnects and
  resubscribes with their delays, callbacks, the read-ahead filling and
  draining, and failed calls; TRACE for every input, action and frame. Turn
  them on with `RUST_LOG=ferrifuge=debug`. No record holds a token, a
  payload or a header value. The dependencies log on their own, and at
  TRACE tungstenite logs the upgrade request and every frame, the connect
  token included, so keep them at INFO or above in production
  (`RUST_LOG=info,ferrifuge=trace`).
- **Statistics:** `statistics()` returns, without a round trip, the state
  and transport, `connected_since`, `connects`, `failed_attempts`, bytes and
  messages in and out, `publications_delivered` and the sizes of both
  subscription registries. For fleet telemetry, a growing `failed_attempts`
  with `connects` flat means no attempt succeeds.

Sources: Block 21 D2; Block 21 D3; Block 21 D5; Block 21 D6; Block 31 D1; [Logging](../README.md#logging)

## Troubleshooting

| Symptom | Likely cause | Check | Fix |
|---|---|---|---|
| `connecting` 2 "no ping" again and again | server pings lost on the way, often a proxy or load balancer that buffers or drops idle connections | the server's ping interval; the DEBUG records around each reconnect | let pings through the proxy, or raise `max_server_ping_delay` |
| disconnect 3008 "slow", with the backoff growing | the application does not read a stream, or a recovery is larger than the server's client queue | whether every stream taken is read; the size of the backlog after an outage | read the streams; keep `client.queue_max_size` above `client.recovery_max_publication_limit` times the largest publication |
| `disconnected` 3 "message size limit exceeded" | a message from the server over `max_message_bytes`, or the server refusing what the client sent (websocket close 1009) | the size of the largest history or recovery reply; the size of the client's commands | raise `max_message_bytes`, or send smaller commands and lower `batch_frame_bytes` |
| `Connected` names http_stream or sse instead of websocket | websocket is blocked on the network | the `Error` events of the websocket attempts | allow websocket upgrades, or keep the fallback |
| an `Error` event whose message ends with "a captive portal may be answering" | the network's captive portal answered the attempt | the device's network sign-in | tell the user to sign in to the network |
| `error` 2 with "handshake timeout" on every attempt | the network is slower than `handshake_timeout` (default 5 s) | the time a transport takes to open | raise `handshake_timeout` |
| `disconnected` 1 "unauthorized" or `unsubscribed` 1 "unauthorized" | a token callback denied access or returned an empty token, or a token expired with no callback | the token callbacks' answers in the DEBUG records | return `TokenError::Failed` for a temporary failure; set a `get_token` callback for expiring tokens |
| `Subscribed` with `recovered` false after an outage | history expired, the stream's epoch changed, or the backlog passed the recovery limit | `history_ttl`, `history_size` and `client.recovery_max_publication_limit` | reload the state, or set a `get_state` callback |
| calls fail with `error` 1 "timeout" while the client connects | the connection takes longer than `timeout` | the time from `connect()` to `Connected` | raise `timeout`, or wait for `Connected` before calling |
| WebTransport always falls back in the browser | Centrifugo v6.6.1 through v6.10.3 select no ALPN | the server's version | keep a fallback transport after WebTransport |
| every client reconnects at once after a server restart | `reconnect_jitter_window` is 0 | the reconnect times across the fleet | set `reconnect_jitter_window` |

Sources: Block 3 "Close code 1009 stops the client"; Block 4 "Missing pings trigger reconnect"; Block 8 "Fallback to http_stream when websocket is unreachable"; Block 10 "Subscription token denied"; Block 11 "Recovery fails when history is gone"; Block 13 "A queued call fails after the timeout when no connection comes"; Block 14 "Reconnect jitter window"; Block 15 "Recovery larger than the server's queue"; Block 25 D8; Block 29 D1; Block 29 D3; Block 48 D17

## Codes

Every code, the reason it carries, its cause and what the client does next
are in the README's [Codes](../README.md#codes) section, the same table as
the `codes` module documentation, kept complete by a test.

Sources: Block 34 D1; Block 34 D2; Block 34 D3; [Codes](../README.md#codes)
