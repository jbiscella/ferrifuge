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

- `Client::ready` and `Subscription::ready` (Block 30): wait until the
  client is connected or the subscription subscribed, without taking an
  event from their streams; they fail with code 3 "disconnected" or code 7
  "subscription unsubscribed" (`CallError`) when that will not come.

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
