<p align="center">
  <img src="ferrifuge-logo.png" alt="ferrifuge" width="480">
</p>

<p align="center">
  A <a href="https://centrifugal.dev">Centrifugo</a> client SDK for Rust.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-0BSD-6B7280.svg" alt="License: 0BSD"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-6B7280.svg" alt="MSRV 1.88">
</p>

## Overview

ferrifuge connects Rust applications to a Centrifugo server over its client
protocol. It is written from scratch against the
[Client SDK spec](https://centrifugal.dev/docs/transports/client_api), with
[centrifuge-js](https://github.com/centrifugal/centrifuge-js) as the
implementation reference.

The core is a sans-IO state machine with no async runtime of its own. The
default `tokio` feature adds a ready-to-use `Client` that runs the machine on
tokio. Any other runtime can drive the core through `ferrifuge::machine`.

## Status

Early development; the API will change. Work is organised in blocks
specified in [CLAUDE.md](CLAUDE.md).

Available now:

- connect and disconnect, with the client state and its events;
- reconnect attempts with exponential backoff and full jitter;
- server disconnects: the client reconnects or stops according to the
  server's disconnect code, as the spec defines;
- connection tokens (JWT), refreshed through a callback before they
  expire;
- server pings: answered with pongs, and a connection whose pings stop is
  re-established;
- an `error` event for every failed connection attempt;
- websocket transport (`ws://` and `wss://`) with the JSON or the
  Protobuf protocol;
- http_stream and sse transports (`http://` and `https://`, JSON), tried
  in order when websocket cannot connect;
- the TLS crypto provider chosen at build time: ring, aws-lc-rs, or
  aws-lc-rs in FIPS mode;
- client-side subscriptions: states and events, publications delivered
  with their exact bytes, resubscribe after reconnects, subscription data;
- subscription tokens, refreshed through a callback before they expire;
  server unsubscribe codes; a configurable resubscribe backoff;
- recovery of the publications a subscription missed while disconnected
  or unsubscribed, and the `since`, `positioned` and `recoverable` options.

Not yet available:

| Planned | Where |
|---|---|
| `getState`, state invalidation (codes 2502, 3014) | Block 11 |
| Server-side subscriptions | later blocks |
| RPC, publish, history, presence | later blocks |

## Usage

ferrifuge is not on crates.io yet. Depend on the repository:

```toml
[dependencies]
ferrifuge = { git = "https://github.com/jbiscella/ferrifuge" }
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use ferrifuge::{Client, ClientEvent, Config};
use futures_util::StreamExt;

#[tokio::main]
async fn main() {
    let client = Client::new("ws://localhost:8000/connection/websocket", Config::default());
    let mut events = client.events();
    client.connect();

    while let Some(event) = events.next().await {
        match event {
            ClientEvent::Connecting { code, reason } => println!("connecting: {code} {reason}"),
            ClientEvent::Connected { client_id, .. } => println!("connected as {client_id}"),
            ClientEvent::Disconnected { code, reason } => {
                println!("disconnected: {code} {reason}");
                break;
            }
            ClientEvent::Error(error) => eprintln!("error: {}", error.message),
        }
    }
}
```

To try it against a local server that accepts connections without a token
(see [Connection tokens](#connection-tokens) otherwise):

```sh
docker run --rm -p 8000:8000 \
  -e CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN=true \
  centrifugo/centrifugo:v6.10.1 centrifugo
```

### Client

`Client::new` must be called inside a tokio runtime. `connect()` and
`disconnect()` change the state before they return. `events()` returns a
`Stream` of the events emitted from then on; each call returns an independent
stream. A stream borrows nothing from the client, so it can be kept beside
it, e.g. in the same struct. Dropping the client disconnects it and ends its
streams. A builder that cannot work returns a `ClientError`, which
implements `std::error::Error`.

### Subscriptions

```rust
use ferrifuge::SubscriptionEvent;
use futures_util::StreamExt;

let subscription = client.new_subscription("news")?;
let mut events = subscription.events();
subscription.subscribe();

while let Some(event) = events.next().await {
    if let SubscriptionEvent::Publication(publication) = event {
        // JSON text with the JSON protocol, any bytes with Protobuf.
        println!("{}", String::from_utf8_lossy(&publication.data));
    }
}
```

`subscribe()` works before `connect()`: the subscription subscribes once
the client is connected, and again after every reconnect. The registry
refuses a second subscription to a channel; `get_subscription`,
`subscriptions` and `remove_subscription` manage it.
`subscription_builder(channel)` sets data sent with every subscribe, or a
`get_data` callback giving it each time, and the bounds of the resubscribe
backoff (`min_resubscribe_delay`, 500 ms, and `max_resubscribe_delay`,
20 s). When the server unsubscribes the client, a code from 2500 makes the
subscription subscribe again, a lower one ends it.

For channels that need a subscription token, give an initial token, a
callback, or both:

```rust
use ferrifuge::TokenError;

let subscription = client
    .subscription_builder("private:news")
    .get_token(|channel| async move {
        // Ask your backend for a token for `channel`.
        fetch_subscription_token(&channel)
            .await
            .map_err(|e| TokenError::Failed(e.to_string()))
    })
    .build()?;
```

The callback is called before subscribing without a token, after the
server rejects one as expired, and before it expires: the token is then
refreshed with `sub_refresh`. Return `TokenError::Unauthorized` (or an
empty token) to unsubscribe for good, with code 1; other errors are
reported as `error` events and retried. With a `get_data` callback too, the
token comes first.

On a channel that keeps a history and allows recovery, a subscription
recovers the publications it missed while disconnected or unsubscribed:
they are delivered after its `subscribed` event, which says whether the
recovery succeeded. Ask for recovery on channels that allow it without
forcing it, and resume from a position saved earlier:

```rust
use ferrifuge::{StreamPosition, SubscriptionEvent};
use futures_util::StreamExt;

let subscription = client
    .subscription_builder("news")
    .recoverable(true)
    .since(StreamPosition { offset: saved.offset, epoch: saved.epoch })
    .build()?;
let mut events = subscription.events();
subscription.subscribe();

while let Some(event) = events.next().await {
    match event {
        SubscriptionEvent::Subscribed(subscribed) if subscribed.was_recovering && !subscribed.recovered => {
            // Publications were lost: reload the application's state.
        }
        SubscriptionEvent::Publication(publication) => {
            // Save `publication.offset` (and the epoch of the stream
            // position in `subscribed`) to resume from later.
        }
        _ => {}
    }
}
```

### Transport fallback

To fall back to http_stream, then sse, where websocket is blocked, give the
endpoints in the order to try them. They are tried on the first connection
only, until one works; reconnects keep that one.

```rust
use ferrifuge::Client;
use ferrifuge::machine::TransportKind;

let client = Client::builder_with_transports([
    (TransportKind::WebSocket, "wss://example.com/connection/websocket".to_owned()),
    (TransportKind::HttpStream, "https://example.com/connection/http_stream".to_owned()),
    (TransportKind::Sse, "https://example.com/connection/sse".to_owned()),
])
.build()?;
```

The server must enable them (`http_stream.enabled` and `sse.enabled` in
Centrifugo). Their commands go to the server's emulation endpoint,
`/emulation` on the same host by default; set another with
`ClientBuilder::emulation_endpoint`, e.g. behind a path prefix. They carry JSON only: building a client with the Protobuf
protocol and one of them fails with a configuration error.

### Connection tokens

Set the initial token in `Config::token`, and give the builder a callback
that returns a new one:

```rust
use ferrifuge::{Client, Config, TokenError};

let client = Client::builder("ws://localhost:8000/connection/websocket")
    .config(Config {
        token: initial_token,
        ..Config::default()
    })
    .get_token(|| async {
        // Ask your backend for a new token.
        fetch_token().await.map_err(|e| TokenError::Failed(e.to_string()))
    })
    .build()?;
```

When the server says the token expires, the client calls the callback before
then and sends the new token. It also calls it to connect when it has no
token, or after the server rejected one as expired. Return
`TokenError::Unauthorized` (or an empty token) to disconnect the client for
good, with code 1. Any other error is reported as an `error` event and
retried.

### States and events

The client is `Disconnected`, `Connecting` or `Connected`. Each change emits
`Connecting`, `Connected` or `Disconnected`. `Connecting` and `Disconnected`
carry a numeric `code` and a `reason`. Client-side codes are in
`ferrifuge::codes`, and their values follow centrifuge-js. `Error` events
report problems that do not change the state, such as a failed attempt
while the client keeps reconnecting.

### Configuration

| `Config` field | Default | Meaning |
|---|---|---|
| `min_reconnect_delay` | 500 ms | Lower bound of the reconnect delay |
| `max_reconnect_delay` | 20 s | Upper bound of the reconnect delay |
| `timeout` | 5 s | Time allowed from opening the transport to the connect reply |
| `name` | `ferrifuge` | Client name reported to the server |
| `version` | crate version | Client version reported to the server |
| `token` | empty | Connection token sent with the first connect |
| `max_server_ping_delay` | 10 s | Wait past the server's ping interval before reconnecting; 0 disables |
| `protocol` | `Protocol::Json` | Encoding of the client protocol: `Json` or `Protobuf` (binary frames, any payload bytes) |
| `reconnect_jitter_window` | 0 | Upper bound of a random delay added to the first reconnect after losing a connection, so that many clients disconnected at once do not reconnect at once |

### Cargo features

| Feature | Default | Adds |
|---|---|---|
| `tokio` | yes | `Client`, the websocket, http_stream and sse transports, TLS through rustls with the platform's root certificates |
| `ring` | yes | ring as the TLS crypto provider; builds with a C compiler |
| `aws-lc-rs` | no | aws-lc-rs as the TLS crypto provider; builds with a C compiler |
| `fips` | no | aws-lc-rs in FIPS mode; also needs CMake and Go to build |

`tokio` needs one of the provider features. Cargo features are additive, so
a build without ring turns default features off:

```toml
ferrifuge = { version = "0.1", default-features = false, features = ["tokio", "fips"] }
```

When several providers are enabled, the strongest one is used: `fips`, then
`aws-lc-rs`, then `ring`.

`fips` builds the AWS-LC-FIPS 4.x module. That module has been submitted to
NIST for FIPS 140-3 validation but is not certified yet. The certified 3.x
module needs aws-lc-rs 1.17, which only rustls versions with known
vulnerabilities (RUSTSEC-2026-0285, RUSTSEC-2026-0346) accept. Check the
module's status on the NIST CMVP list before relying on it for compliance.

Without default features, ferrifuge has no runtime or network dependency.
`ferrifuge::machine::Machine` then takes inputs (API calls, transport
activity, timers) and returns the actions to perform.

## Development

You need Rust 1.88 or newer, and Docker for the tests that run a real
Centrifugo.

```sh
git submodule update --init      # Claude Code skills used in this repo
cargo test --all-targets         # unit tests, BDD scenarios, fixture smoke tests
cargo test --test bdd -- --tags @A   # one test layer of the scenarios
cargo deny check                 # advisories, licenses, sources
```

The behaviour is specified as Gherkin scenarios in [CLAUDE.md](CLAUDE.md).
Each file in `tests/features/` is a verbatim copy of one of them, and a test
fails if the two drift apart. `tests/bdd.rs` lists the scenarios that are
implemented so far.

### Performance tests

Scenarios tagged `@P` (Block 14) run outside cucumber, each claimed by a
benchmark, measurement or simulation that names it, and a test fails if one
has none:

```sh
cargo test --test perf_sim                                 # simulated clock, runs with the tests
FERRIFUGE_PERF=1 cargo test --release --test perf_alloc     # allocations per publication
FERRIFUGE_PERF=1 cargo test --release --test perf_footprint # memory footprint
cargo bench --bench dispatch -- --save-summary=json         # instruction counts
cargo test --test perf_gates -- --ignored                   # gates on those counts
cargo bench --bench wallclock                               # decode and dispatch time
FERRIFUGE_PERF=1 cargo test --release --test perf_wallclock # latency, then 2 min idle CPU
```

Wall-clock figures depend on the machine: they are reported, never gating.
`FERRIFUGE_PERF_IDLE_SECS` shortens the idle period of `perf_wallclock`,
which reads CPU time from Linux's `/proc`.

The allocation and footprint measurements count with
[dhat](https://crates.io/crates/dhat) and are slow, so they run only with
`FERRIFUGE_PERF=1`. The benchmark needs Valgrind and `gungraun-runner` at the
version of `gungraun` in `Cargo.lock`
(`cargo install gungraun-runner --version <version>`). Under
`cargo test --all-targets` both are skipped.

### Interop, fuzzing and semver

```sh
npm ci --prefix tests/interop --ignore-scripts          # centrifuge-js, pinned
FERRIFUGE_INTEROP=1 cargo test --test interop             # centrifuge-js publishes, ferrifuge receives
cargo bolero test --toolchain nightly json::tests::fuzz_decode -T 30s   # one fuzz target
cargo semver-checks --default-features --baseline-rev main               # public API changes
```

The interop test needs Docker and Node. The fuzz targets (`fuzz_*` tests
next to the JSON, Protobuf, http_stream and sse decoders) run on random
inputs under `cargo test`, and coverage-guided with
[cargo-bolero](https://crates.io/crates/cargo-bolero) and a nightly
toolchain. A breaking change to the public API needs a version bump.

### Continuous integration

CI runs rustfmt, clippy (pedantic, warnings denied) with and without default
features, rustdoc, the tests with a 70% line-coverage floor, an MSRV build
and cargo-deny. It also builds and tests with the aws-lc-rs and fips
providers, and runs the performance tests above, writing their measurements
to the job summary; the wall-clock ones run in a job that never fails the
build. It runs the unit tests and the scenarios that need no server on
Windows and macOS, the interop test, each fuzz target for 30 s (10 min
weekly) and the semver check. To save Actions minutes, the perf,
wall-clock, Windows/macOS and fuzz jobs run on merges to `main` and on
demand (`workflow_dispatch`), not on every pull request push. cargo-deny
also runs daily.

## License

[0BSD](LICENSE). [`proto/client.proto`](proto/client.proto) is copied from
[centrifugal/protocol](https://github.com/centrifugal/protocol) and keeps its
MIT license ([`proto/LICENSE`](proto/LICENSE)).

Builds with the `aws-lc-rs` or `fips` feature include AWS-LC, which is
partly under the OpenSSL license. That license requires its attribution
notice in advertising and in redistributions of the built binaries.
