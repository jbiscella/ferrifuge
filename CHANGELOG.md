# Changelog

All notable changes to Ferrifuge are recorded here, in the form of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The project
follows [Semantic Versioning](https://semver.org/): before 1.0, a minor
version may break the public API, a patch version does not.

Versions up to 0.16.0 were never published: each section records what the
repository held while it carried that version, from the blocks of
`CLAUDE.md` that landed then.

## [Unreleased]

### Added

- Unstable (`unstable` feature): map subscriptions recover and resync
  (Block 45c). A subscribe after a taken event recovers from that
  position in the stream phase, and the missed changes come as
  `MapSubscriptionEvent::Update`s with no `Sync`; error 112 loads the
  state again after the resubscribe backoff with
  `UnrecoverableStrategy::FromScratch`, the default, and ends the
  subscription with `unsubscribed` 112 with `UnrecoverableStrategy::Fatal`.
  2502, 3014 and `set_tag_filter` drop a map subscription's position, and
  2502 and 3014 its token.
- Unstable (`unstable` feature): per-key delta and tag filters on map
  subscriptions (Block 45d). `MapSubscriptionBuilder::delta` asks for
  per-key Fossil deltas: the client keeps a base per key, across a
  recovering resubscribe and not across a load from scratch, and delivers
  whole values; `MapSubscription::delta_statistics`,
  `MapSubscription::set_tag_filter`.

## [0.21.0] - 2026-11-09

### Added

- Unstable (`unstable` feature): map subscriptions (Block 45b).
  `Client::map_subscription_builder`, `Client::new_map_subscription`,
  `Client::get_map_subscription`, `Client::map_subscriptions` and
  `Client::remove_map_subscription` make a `MapSubscription` of a
  `MapKind` (`Map`, `Clients`, `Users`), which loads a keyed state that
  Centrifugo owns page by page (`MapSubscriptionBuilder::page_size`),
  catches up on the changes made meanwhile, and yields one
  `MapSubscriptionEvent::Sync` with every `MapEntry`, then a
  `MapSubscriptionEvent::Update` (`MapUpdate`) per change;
  `MapSubscription::publish`, `publish_json` and `remove` set and remove
  keys once subscribed. Pages are handled as they are read, ahead of the
  events not taken; `MapSubscriptionBuilder::max_state_bytes` (64 MiB)
  bounds a load. `UnrecoverableStrategy` is configurable, used from 45c.
  The core has `Machine::add_map_subscription`, `MapSubscriptionOptions`,
  `Action::EmitMapSubscription` and `Anomaly::UnexpectedMapReply`.

- `codes::unsubscribed::MAP_STATE_TOO_LARGE` (5) and
  `codes::unsubscribed::MAP_NOT_SUPPORTED` (6) (Block 45b): a map
  subscription's load passed its bound, and a server answered a map
  subscription as a stream one; only map subscriptions use them.

- The browser adapter (Block 44b): on `wasm32-unknown-unknown` the same
  `Client`, `Subscription`, streams, handlers and calls run in the browser
  over its WebSocket, behind the new `browser` feature, in the default set,
  which builds nothing on other targets. Its inbox holds at most the
  read-ahead's limits while the application does not read; past them the
  client reconnects with the new `connecting` code 7
  `READ_AHEAD_OVERFLOW`, after the new input `Input::InboxOverflow`.

- The browser's network and page events (Block 44c): in the browser the
  client closes its connection on `offline`, and on a `pagehide` that puts
  the page in the back/forward cache, with the reason "page entered
  back/forward cache", and reconnects on `online` and on the `pageshow`
  that restores it; `ClientBuilder::network_event_target` and
  `ClientBuilder::page_event_target` name other targets than `globalThis`.
  The core has the new inputs `Input::Offline` and `Input::PageCached`.

- http_stream in the browser (Block 44d): the browser client falls back
  to http_stream over the page's `fetch`, JSON or Protobuf, its response
  read only while the read-ahead has room, so a slow page holds the server
  back; closing or dropping the transport aborts its requests.

- sse in the browser (Block 44e): the browser client falls back to sse
  over the page's `EventSource`, its commands in emulation requests over
  `fetch`, its messages in the inbox; when the stream ends the client
  closes the `EventSource` and reconnects by its own backoff, not the
  browser's.

- Shims and credentials in the browser (Block 44f):
  `ClientBuilder::websocket`, `fetch`, `readable_stream` and
  `event_source` give the client the application's constructors in place
  of the page's globals. A transport whose constructors are missing is
  skipped in the fallback round, and `build()` fails when no configured
  transport can be used. `ClientBuilder::include_credentials` makes the
  http_stream and sse requests carry cookies to another origin. The core
  has the new input `Input::TransportUnsupported`, which skips a transport
  its owner cannot open.

- The `unstable` feature (Block 45a): experimental APIs following
  centrifuge-js 63ed73e and Centrifugo's experimental features, not a
  default and outside the semver promise.

- Unstable (`unstable` feature): `Client::map_publish`,
  `Client::map_publish_json` and `Client::map_remove` (Block 45a) set and
  remove keys of a map channel (Centrifugo 6.8.0 and later), waiting for
  the connection as any call; the server's refusal keeps its code. The
  core has `CallRequest::MapPublish` and `CallRequest::MapRemove`.

### Changed

- `machine::Action` is `#[non_exhaustive]` (Block 45b), as `Input` and
  `Anomaly` already are: a variant added under a feature, as
  `Action::EmitMapSubscription` under `unstable`, would otherwise break the
  exhaustive `match` of a crate whose graph another crate enables it in.

- `reconnect()` while connecting, before the first attempt since the
  client lost its connection has started, waits a draw from the reconnect
  jitter window instead of starting that attempt at once (Block 44c), so
  that a fleet whose network comes back does not reconnect at once. With
  the default window 0 nothing changes.

- The `tokio`, `ring`, `aws-lc-rs`, `fips` and `cookies` features build as
  nothing on `wasm32-unknown-unknown` (Block 44b), where the native-only
  options do not exist. With `browser`, the core's random delays are seeded
  from the browser there.

## [0.20.0] - 2026-11-09

### Added

- `MaybeSend` and `MaybeSync` (Block 44a): `Send` and `Sync` on native
  targets, nothing on the browser target the browser adapter adds, so that
  one bound of the client's API serves both.

- An opt-in cookie jar (`cookies` feature): `CookieJar`, `MemoryJar`,
  `ClientBuilder::cookie_jar` (Block 43a). The websocket upgrade sends the
  jar's cookies, merged with the application's `Cookie` header, and stores
  the cookies of its response; so do the http_stream and sse stream
  requests and every emulation request (Block 43b).

- Event handlers (Block 41b): `ClientHandler` and `SubscriptionHandler`,
  set with `ClientBuilder::handler`, `Client::set_handler`,
  `SubscriptionBuilder::handler` and `Subscription::set_handler`, and run
  under `EventDelivery::Task`; an event counts as delivered when its handler
  returns; a panicking handler disconnects the client with code 2; a call of
  its own client from a handler fails with error 16 `WOULD_DEADLOCK`; the
  inputs `EventHanded` and `HandlerPanicked`. `EventDelivery::Inline`
  (Block 41c) runs the handlers on the client's driver, and
  `EventDelivery::Thread` (Block 41d) on a thread of the client's own,
  where a wait on its own client fails only when the handler's event holds
  back the message it needs.

- `Client: Clone` and weak handles (Block 41a): clones share one client;
  `Client::downgrade` gives a `WeakClient` whose `upgrade` gives the client
  while a clone lives.

- An in-memory server double (Block 37c), behind `test-util` with `tokio`:
  `test_util::Server` and `test_util::Command`. The real client runs
  against it through its connector, without a socket; the test sets its
  answers, publishes, disconnects and reads what the client sent.

- `Subscription::publications()` (Block 39): the subscription's publications
  only, as a view of its one stream, which takes over from `events()` and
  back; the events it skips count as delivered as it passes them.

- A wasm32-unknown-unknown build of the sans-IO core in CI (Block 40).

- An injectable connector (Block 37b): `ClientBuilder::connector`, the
  `Connector` trait (implemented for closures and for an `Arc` of one),
  `ConnectedStream`, `Connecting` and `BoxError`, and `ferrifuge::http`, the
  `http` crate its endpoint `Uri` comes from. The client runs TLS and the
  websocket upgrade over the connector's stream and reads no proxy setting.

- Typed JSON helpers (Block 38): `Publication::json`, `Client::publish_json`,
  `Subscription::publish_json` and `Client::rpc_json`, with `JsonCallError`
  telling an encoding failure (nothing sent), a call failure and a result
  that does not decode (the RPC ran) apart.

- Test constructors (Block 37a), behind the new `test-util` feature:
  `ClientEvent::connected`, `Publication::new`, `ClientInfo::new`,
  `Statistics::new`, and setters on those and on `Subscribed`,
  `HistoryResult`, `PresenceResult`, `PresenceStatsResult` and
  `DeltaStatistics`, so that an application's tests build what the client
  yields.

- Automatic command batching (Block 36a): `Machine::with_automatic_batching`
  and `Input::Flush`, with which the core buffers the commands it would send
  until its owner flushes them, in frames of at most `batch_frame_bytes`.

- `Anomaly::ReplyBeforeConnect` (Block 36b): a reply handled before the
  connect reply of its connection, reported before the core reconnects.

- `Publication::channel` and `Subscribed::has_recovered_publications`
  (Block 35c): the channel of a publication that differs from its
  subscription's, as for a wildcard subscription, which then moves no stream
  position; and whether publications, recovered or initial, follow a
  `subscribed` event, client-side and server-side.

- Channel compaction (Block 35b): `SubscriptionBuilder::channel_compaction`
  and `SubscriptionOptions::channel_compaction` ask a Centrifugo PRO server
  for a numeric channel id, and publication, join and leave pushes carrying
  one reach their subscription; a push with an unknown id is dropped.

- `TagFilter` (Block 35a): server-side filtering of a subscription's
  publications by their tags (Centrifugo `allow_tags_filter`, from v6.4.0),
  built from comparisons and `and`, `or`, `!`;
  `SubscriptionBuilder::tag_filter`, `Subscription::set_tag_filter`,
  `SubscriptionOptions::tag_filter`, `Machine::check_subscription_tag_filter`,
  `Input::SetSubscriptionTagFilter`, and `SubscriptionError::InvalidTagFilter`
  and `TagFilterWithDelta`.

- A table of codes (Block 34): every client-side code with its reason, cause
  and what the client does next, and the server codes the client reacts to,
  in the `codes` documentation and the README's "Codes" section.

### Changed

- The `Send` and `Sync` bounds of the client's API read `MaybeSend` and
  `MaybeSync` (Block 44a): the builders' token, data and getState callbacks
  and their futures, `ClientHandler`, `SubscriptionHandler` and their closure
  implementations, and the futures and streams the client and its
  subscriptions return. Natively each bound is the same as before; the
  handler traits' supertrait changes, hence 0.20.0. The adapter is split
  into a shared layer, its tokio transports and a compile-time seam, with no
  behaviour change.

- A subscription's data, token or getState callback running when the
  connection is torn down keeps running, and its answer is used: a token
  or a getState position serves the next connection's subscribe, a denial
  unsubscribes with code 1, a failure arms the resubscribe backoff, as
  centrifuge-js does (Block 42, D11). Disconnect code 3014 still abandons
  it, and a token refresh's fetch stays abandoned.

- `unsubscribe()` or a removal while a subscription waits for its data,
  token or getState callback sends an unsubscribe command, as centrifuge-js
  does, and a later subscribe of the channel waits for its reply (Block 42,
  D10).

- Dropping a client closes it only once its last clone is dropped (Block
  41a); with no clone, as before.

- The crate's own test hooks (`Machine::with_seed` and the others) move from
  the `test-util` feature to the private `__test-hooks` (Block 37a); they
  were never public API.

- The tokio client batches automatically (Block 36a): the commands made
  before its driver next runs go out together, in frames of at most
  `batch_frame_bytes`, in the order they were made, with no delay.

- Over websocket the core sends the subscribe commands that need no callback
  in the frame of the connect command, as many as fit `batch_frame_bytes`
  (Block 36b); their reply deadline starts when the connect reply is read.

### Fixed

- A client at rest no longer keeps what a burst grew in its queues (Block
  14): the actions of its driver, its read-ahead and its event streams fall
  back to a few slots once empty. Since 36b sends a connection's subscribes
  in its connect frame, a reconnect's burst could leave about 4 KiB more held
  after one reconnect than after another, which failed "Memory stays flat
  across reconnects".

## [0.19.0] - 2026-11-07

### Added

- `Client::close` and `Client::is_closed` (Block 33): close a client for
  good, also from a shared reference, and await the end, the transport
  closed and every stream ended; `Input::Close`; unsubscribed code 2 and
  error code 4 "client closed" (`codes::unsubscribed::CLIENT_CLOSED`,
  `codes::error::CLIENT_CLOSED`).

- `Client::set_http_headers` (Block 32): replaces the real HTTP headers,
  validated as at build, for every HTTP request started after the call:
  the next attempt's upgrade or stream request, and the emulation requests
  of an open `http_stream` or sse transport. `Subscription::set_data` and
  `Input::SetSubscriptionData`: the data of the subscription's next
  subscribe commands, a data callback still replacing it.

- `Client::statistics` and `Machine::statistics` (Block 31): a
  `Statistics` snapshot of the client's state, transport, connected-since
  time, connects, failed attempts, bytes and messages each way,
  publications delivered and registry sizes, never a round trip.
  `Subscription::delta_statistics` and `Machine::delta_statistics`: a
  `DeltaStatistics` of full and delta payloads and their bytes, with
  `compression_ratio()`.

- `Client::ready` and `Subscription::ready` (Block 30): wait until the
  client is connected or the subscription subscribed, without taking an
  event from their streams; they fail with code 3 "disconnected" or code 7
  "subscription unsubscribed" (`CallError`) when that will not come.

### Changed

- Dropping a client, or its runtime shutting down, closes it as `close()`
  does (Block 33): its subscriptions, server-side ones included, emit
  `unsubscribed` with code 2 "client closed" before `disconnected`, and a
  closed client's calls and `ready()` futures, those of its leftover
  subscriptions included, fail with code 4 "client closed" instead of 3
  "disconnected", as `Input::DriverDropped` now does too.

## [0.18.0] - 2026-11-07

### Added

- `Config::handshake_timeout` (Block 29): the time allowed for opening a
  transport (DNS, TCP, the proxy handshake, TLS, the websocket upgrade, or
  an HTTP stream's response headers), 5 s by default; an open that takes
  longer fails the attempt with "handshake timeout".

- `Config::max_message_bytes` (Block 29): the largest message accepted
  from the server, 64 MiB by default, on every transport and for the
  payloads deltas build; and `Input::MessageTooLarge`, by which an owner of
  the machine reports a larger one.

- A chainable setter per `Config` field, named after it (Block 29):
  `Config::default().timeout(..).token(..)`.

- `ClientBuilder::runtime` (Block 28): the tokio runtime the client runs
  on, for building it where none is current, such as a Tauri setup hook.
  Disconnected codes 2 "exception during message handling" and 4 "runtime
  shut down" (`codes::disconnected::BAD_PROTOCOL`, `RUNTIME_SHUT_DOWN`),
  and `Input::DriverDropped`.

- Protobuf over http_stream (Block 27): a client with the Protobuf
  protocol can fall back to http_stream when websocket is blocked; its
  stream and emulation requests are `application/octet-stream`.

- `Client::reconnect` and `Input::Reconnect` (Block 25): reconnect at once
  on the platform's wake or network-change notification, with connecting
  code 6 "reconnect called" (`codes::connecting::RECONNECT_CALLED`); the
  jitter window still applies to a connected client, the backoff does not;
  a disconnected client is left alone.

### Changed

- `Config` is `#[non_exhaustive]` (Block 29): outside the crate it is
  built from `Config::default()`, with its setters or by assigning its
  fields, and no longer with a struct literal.

- `Config::timeout` bounds the connect reply once the transport is open,
  and no longer the open itself, as does `Timer::ConnectTimeout` (Block
  29); one attempt can take both timeouts, 10 s by default.

- A message from the server over the size limit after the connect reply
  ends the client with `disconnected` code 3 "message size limit exceeded"
  instead of reconnecting with code 1; one before it fails the attempt with
  an error event instead of reconnecting without one (Block 29).

- `SubscriptionError::ClientClosed` reads "the client is closed": the client
  was dropped, or its runtime shut down (Block 28); the client's and the
  subscriptions' streams end then too.

- Building a client refuses the Protobuf protocol only with an sse
  endpoint; with http_stream it is now accepted (Block 27).

- Maintained components parse what the server streams (Block 27):
  sse-core the sse events, tokio-util the http_stream JSON lines, and
  mediatype the content types. sse data that is not UTF-8 now arrives with
  U+FFFD replacement characters, as browsers' EventSource decodes it; a
  content type with a parameter that has no value is no longer an event
  stream; a long sse comment or unknown field no longer ends the transport.

- An attempt answered by a captive portal's page (a redirect, HTTP 511, or
  `text/html` served as a success) fails with an error whose message ends
  with "; a captive portal may be answering" (Block 25); an `http_stream`
  open answered with `text/html` was read as frames and is now a failed
  attempt with an error event.
- `reconnect()` starts the transport round again from the first transport,
  and makes every client's next attempt read the environment's and the
  operating system's proxy settings again, rebuilding the HTTP clients
  built on them.

- The crate's license expression is `0BSD AND MIT`: `proto/client.proto`,
  which the crate ships, is under the MIT license. `LICENSE` lists every
  file not under 0BSD; `REUSE.toml` states the same per file.

### Fixed

- Building a client outside a tokio runtime returns a configuration error
  instead of panicking (Block 28); so does building on a runtime that has
  shut down.

- A client whose runtime shut down, or whose task panicked, fails its calls
  and ends its streams instead of leaving them pending (Block 28).

## [0.17.0] - 2026-11-07

### Added

- `Input::Slept` and sleep detection in the tokio adapter (Block 25): a
  wake from a system sleep restarts the no-ping window, or reconnects with
  code 2 after a sleep the server gave up on, and moves every token refresh
  earlier by the sleep.

### Changed

- `Action::ShortenTimer` and `Action::ShortenSubscriptionTimer`: an owner
  of the machine must make the named timer fire earlier, at once when that
  time has passed.
- Token refresh timers keep running across a reconnect, until the next
  connect or subscribe reply re-arms or cancels them; one that fires
  meanwhile makes the next attempt fetch a new token first.
- A refresh that came due and got no new token before a reconnect, or
  before a resubscribe advised by the server (codes from 2500), makes the
  next connect or subscribe fetch a new token first, instead of sending
  the token whose refresh came due; a token already fetched is used.

### Fixed

- A token that expired while the system slept is no longer sent.

## [0.16.0] - 2026-11-07

### Added

- `ferrifuge::android` (Android only, exempt from semver): the setup call
  of the operating system's certificate verifier (Block 24).
- Android and iOS builds in CI.

### Changed

- On Android, the ring and aws-lc-rs builds verify servers with the
  operating system's verifier, after one setup call; extra root
  certificates are refused there (give a `tls_config` instead).

### Fixed

- Android found no system root certificate, so `wss://` and `https://`
  failed there without extra roots or `SSL_CERT_FILE`.

## [0.15.0] - 2026-11-07

### Added

- Proxies (Block 23): `Proxy` (`system`, the default, `from_env`, `none`,
  `all`, `custom`, with `no_proxy` lists), `ProxyTarget` (HTTP, HTTPS and
  SOCKS5 proxies with credentials), `ClientBuilder::proxy` and
  `Client::set_proxy`.

### Changed

- Every transport follows the environment's proxy and, on macOS and
  Windows, the system's proxy settings; `Proxy::none()` turns that off.
- The errors of the HTTP transports name their cause; a proxy refusing the
  credentials reports "proxy authentication required".
- `http_headers` refuses `Proxy-Authorization`.

### Fixed

- The websocket transport ignored a proxy set in the environment, which the
  HTTP transports followed, so the client fell back to http_stream by
  accident.

## [0.14.0] - 2026-11-07

### Added

- TLS verification (Block 22): `ClientBuilder::root_certificates` trusts
  extra certificate authorities in addition to the platform's;
  `ClientBuilder::tls_config` takes a whole `rustls::ClientConfig`, used as
  given but for its ALPN list (`http/1.1`); `ferrifuge::rustls` re-exports
  rustls, which is now part of the public API.

### Changed

- On macOS, iOS and Windows, the ring and aws-lc-rs builds verify servers
  with the operating system's verifier (`rustls-platform-verifier`), so
  `SSL_CERT_FILE` and `SSL_CERT_DIR` are no longer read there. Linux and
  the fips build keep webpki over the system's root certificates.

## [0.13.0] - 2026-11-07

### Added

- Logging (Block 21): the tokio adapter logs through `tracing`, its records
  reaching `log` when no `tracing` subscriber is set; a `client` span per
  client and a `subscription` span per subscription; WARN for server
  anomalies, DEBUG for state changes, transport attempts, retries,
  callbacks and failed calls, TRACE for inputs, actions and frames. No
  record holds a token, a payload or a header value.
- `Action::Anomaly` and `machine::Anomaly`: the core reports a frame that
  is not client protocol data and a reply without its command's result.

### Changed

- `Action::Anomaly` is a new variant of the exhaustive `Action` (breaking).
- The `Debug` output of `ClientEvent`, `SubscriptionEvent`,
  `ServerSubscriptionEvent`, `Subscribed`, `Publication` and `ClientInfo`
  shows data, connection and channel info as their length and tags by
  name.

### Fixed

- The error of a failed emulation request no longer carries its URL.
- A no-ping timer that fires at the moment the read-ahead fills no longer
  ends the connection with code 2: the deadline is paused while the
  read-ahead is full (Block 15).

## [0.12.0] - 2026-11-06

### Added

- Batching (Block 18): `Client::start_batching()` returns a `Batch` guard;
  the calls and subscription commands made while a batch is open go out
  together when it stops or is dropped, in frames capped by
  `Config::batch_frame_bytes`; the subscribe commands of a connect reply go
  out as one batch; a request of several commands refused with HTTP 413 is
  resent command by command.
- Fossil delta compression (Block 19): `SubscriptionBuilder::delta`,
  `DeltaFormat`, `Subscribed::delta`; the client rebuilds every
  publication, and a delta it cannot apply makes the subscription
  subscribe again (subscribing code 2, error code 15) or, on a server-side
  subscription, the client reconnect (connecting code 5).
- Release setup (Block 20): this changelog, `SECURITY.md`, examples, the
  crates.io release workflow, the docs.rs configuration.

### Changed

- `Config::batch_frame_bytes` (breaking: `Config` is exhaustive).

## [0.11.0] - 2026-11-06

### Added

- Connect data (Block 17): `Config::data`, `ClientBuilder::get_data`, and
  the server's data on `ClientEvent::Connected`.
- `ClientBuilder::http_headers` (real HTTP headers on the websocket upgrade
  and every HTTP request) and `ClientBuilder::emulated_headers`
  (`ConnectRequest.headers`).
- `Client::set_token`, `Client::set_data`, `Client::set_emulated_headers`.

### Changed

- `ErrorKind`, `machine::Input` and `ClientEvent::Connected` are
  `#[non_exhaustive]` (breaking).

## [0.10.0] - 2026-11-06

### Added

- `Config::write_queue_bytes` bounds the calls waiting to be written
  (Block 13).
- Join and leave events (`join_leave` subscription option).
- The seeded chaos runners of the validation harness (Block 16).

### Changed

- A connection the server closes with 3008 before any ping is read no
  longer resets the reconnect backoff (Block 15).

## [0.9.0] - 2026-11-06

### Added

- Publish, history, presence and presence stats, client-level and
  subscription-level (Block 13).
- `send` and `ClientEvent::Message` for asynchronous server messages.

## [0.8.0] - 2026-11-06

### Added

- Calls and RPC (Block 13): `Client::rpc`, calls waiting for the
  connection within `Config::timeout`.

## [0.7.0] - 2026-11-05

### Added

- A bounded read-ahead: `Config::read_ahead_messages` and
  `Config::read_ahead_bytes` (Block 15).

### Changed

- Messages are handled one at a time, after the events of earlier ones are
  taken; a full read-ahead stops reading.

## [0.6.0] - 2026-11-05

### Changed

- A publication counts as delivered when its stream yields it; a second
  `events()` call takes over the stream (Block 15).

## [0.5.0] - 2026-11-04

### Added

- Server-side subscriptions, their events, publications and recovery
  (Block 12).

## [0.4.0] - 2026-11-04

### Changed

- `Client::new` returns an error for an empty endpoint, or an `http(s)://`
  one given for websocket, instead of panicking.

## [0.3.0] - 2026-11-04

### Added

- getState and the reject-unrecovered flag (Block 11), state invalidation
  (codes 2502 and 3014).

## [0.2.0] - 2026-11-04

### Added

- Subscription tokens, their refresh, server unsubscribe codes and the
  resubscribe backoff (Block 10); recovery of missed publications
  (Block 11).

## [0.1.0] - 2026-10-20

### Added

- The client: connection states, reconnect backoff, disconnect codes,
  ping/pong (Blocks 1–4); a sans-IO core and a tokio adapter.
- The TLS crypto provider chosen at build time: ring, aws-lc-rs or FIPS
  (Block 6).
- The Protobuf protocol (Block 7); the http_stream and sse transports and
  their fallback (Block 8).
- Client-side subscriptions and their data (Block 9).
- O(1) dispatch, token refresh jitter and a reconnect jitter window
  (Block 14).
