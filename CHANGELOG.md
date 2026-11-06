# Changelog

All notable changes to Ferrifuge are recorded here, in the form of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The project
follows [Semantic Versioning](https://semver.org/): before 1.0, a minor
version may break the public API, a patch version does not.

Versions up to 0.12.0 were never published: each section records what the
repository held while it carried that version, from the blocks of
`CLAUDE.md` that landed then.

## [Unreleased]

### Fixed

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
