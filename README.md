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
- connect data, fixed or from a `get_data` callback called before every
  connection attempt, and the server's data on the `connected` event;
- HTTP headers on the websocket upgrade and on every `http_stream` and sse
  request, and emulated headers sent in the connect command, which the
  server forwards to its proxies;
- `set_token`, `set_data` and `set_emulated_headers`, which apply from the
  next connection attempt;
- batches (`start_batching`): calls and subscriptions sent together in
  frames of at most `Config::batch_frame_bytes`;
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
  or unsubscribed, and the `since`, `positioned` and `recoverable` options;
- a `get_state` callback reloading the application's state when there is
  no position to recover from, or a recovery fails;
- the server's "state invalidated" codes: tokens and stream positions are
  dropped before subscribing or connecting again;
- Fossil delta compression: publications sent as deltas against the
  previous one and rebuilt by the client, a broken delta making the
  subscription subscribe again rather than reach the application;
- server-side subscriptions, granted at connect or added and removed by the
  server later, with their events and publications, and recovery of the
  publications they missed across reconnects and `disconnect()`/`connect()`;
- RPC, publish, history, presence and presence stats calls, client-level and
  subscription-level, which wait for the connection, and for the
  subscription, when made before them;
- `send` to the server, and the server's asynchronous messages as
  `Message` events;
- a bound on the bytes of calls waiting to be written
  (`Config::write_queue_bytes`);
- join and leave events of the clients of a channel;
- a seeded chaos harness checking the client's invariants (Block 16).

Planned next, in this order: connect data and headers, `setToken` and
`setData`; batching several commands in one frame.

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
async fn main() -> Result<(), ferrifuge::ClientError> {
    let client = Client::new("ws://localhost:8000/connection/websocket", Config::default())?;
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
            // Server-side subscription events, and events added later.
            _ => {}
        }
    }
    Ok(())
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
`Stream` of the events emitted from then on. An event counts as delivered
only when the stream yields it: a publication's stream position, which
recovery resumes from after a reconnect, moves only then. Until a stream is
taken, and once it is dropped, events count as delivered at once, so take
the stream before calling `connect()` or `subscribe()` to get every event,
and exactly-once delivery on channels with history and recovery, as the
examples here do. The client and each subscription have one active stream:
a second `events()` call ends the first, and the events it had not yielded
go to the new one. Messages from the server are handled in order, each
after the events of the earlier ones are taken: an application that stops
reading a stream makes the client stop reading the connection once
`read_ahead_messages` or `read_ahead_bytes` is reached, while pings are
still answered and commands still sent. The server's handling of slow
clients then decides; on channels with history and recovery, recovery
delivers what was missed, exactly once, within the server's recovery limit.
On channels without recovery a slow consumer can lose publications, as with
any disconnect. A recovery reply is queued whole on the server, so one larger
than Centrifugo's `client.queue_max_size` (1 MiB by default) is answered with
3008 "slow" however fast the client reads, and the client keeps reconnecting,
its backoff growing up to `max_reconnect_delay`, until the backlog passes
`client.recovery_max_publication_limit` or the history expires: keep `client.queue_max_size` above
`client.recovery_max_publication_limit` times the largest publication,
summed over the server-side subscriptions one connect recovers. A stream holding 256 events not taken gets no further
`error` events, which change no state, so retries against a server that is
down do not grow it; events caused by the application's own calls, such as
cycling `connect()` and `disconnect()`, still go to it.
A stream borrows nothing from the client, so it can be kept beside it, e.g.
in the same struct. Dropping the client disconnects it
and ends its streams. `Client::new` and a builder that cannot work return a
`ClientError`, which implements `std::error::Error`: for instance an empty
endpoint, or an `http://` or `https://` one for websocket, which needs
`ws://` or `wss://` (`http(s)://` is right for http_stream and sse).

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
forcing it, and resume from a position saved earlier (`saved`, a
`StreamPosition`):

```rust
use ferrifuge::SubscriptionEvent;
use futures_util::StreamExt;

let subscription = client
    .subscription_builder("news")
    .recoverable(true)
    .since(saved.clone())
    .build()?;
let mut events = subscription.events();
subscription.subscribe();

while let Some(event) = events.next().await {
    match event {
        SubscriptionEvent::Subscribed(subscribed) => {
            if subscribed.was_recovering && !subscribed.recovered {
                // Publications were lost: reload the application's state.
            }
            if let Some(position) = subscribed.stream_position {
                saved = position;
            }
        }
        SubscriptionEvent::Publication(publication) => {
            // Keep the position to resume from: a publication moves the
            // offset, and the epoch when it starts a new stream.
            if let Some(offset) = publication.offset {
                saved.offset = offset;
            }
            if let Some(epoch) = publication.epoch {
                saved.epoch = epoch;
            }
        }
        _ => {}
    }
}
```

When the application keeps its own state, a `get_state` callback loads it
and returns the stream position it was read at. It is called before the
first subscribe without `since`, and again when a recovery fails: the
subscription then stays subscribing, with no publication, until the state is
reloaded, and continues from the new position. A failed load is reported as
an `error` event and retried after the resubscribe backoff.

```rust
use ferrifuge::StateError;

let subscription = client
    .subscription_builder("news")
    .get_state(|channel| async move {
        // Read the stream position first, then the state: publications made
        // meanwhile are then recovered rather than missed.
        let position = stream_position(&channel).await;
        load_state(&channel).await.map_err(|e| StateError(e.to_string()))?;
        Ok(position)
    })
    .build()?;
```

When the server invalidates a subscription's state (unsubscribe code 2502),
the subscription drops its token and its stream position before subscribing
again: the token callback gives a new token, and `get_state` reloads the
state. When it invalidates the connection's state (disconnect code 3014), the
client drops its connection token and the token of every subscription, fixed
ones too, before reconnecting. A token callback then gives a new token; without
one, the connect or subscribe carries no token. A subscription that has a
stream position keeps it with the epoch `_`, so its next recovery fails and you
are told: with `get_state` your state is reloaded; without it, the `subscribed`
event reports `was_recovering` true and `recovered` false.

### Join and leave

A subscription built with `.join_leave(true)` asks the server to tell it
when clients subscribe to the channel or leave it, as
`SubscriptionEvent::Join(info)` and `SubscriptionEvent::Leave(info)`, where
`info` is the client's `ClientInfo` (client id, user, connection and channel
info). The channel must have join and leave events, and the client the right
to see its presence (Centrifugo's `join_leave` channel option; with
`force_push_join_leave` the server sends them without being asked).
Server-side subscriptions report them as `ServerSubscriptionEvent::Join` and
`Leave`. They go through the stream in order with publications; those not
yet taken when the subscription stops are withdrawn, as publications are.

```rust
let subscription = client
    .subscription_builder("chat")
    .join_leave(true)
    .build()?;
```

### Delta compression

A subscription built with `.delta(DeltaFormat::Fossil)` asks the server to
send each publication as a Fossil delta against the previous one, which
saves bandwidth on channels whose payloads change a little at a time. The
client rebuilds every publication: the application always receives whole
payloads, byte for byte as published, and `Subscribed::delta` says whether
the server agreed. On Centrifugo the channel needs `allowed_delta_types:
["fossil"]` and a positioned subscription (`force_positioning`, or
recovery); `delta_publish` sends every publication as a delta.

A delta that cannot be applied (a wrong checksum, a copy past the previous
data, a delta with nothing to apply to) never reaches the application: the
subscription emits an `error` event (`ErrorKind::Delta`, code 15) and
subscribes again after its resubscribe backoff (`subscribing` code 2),
recovering from its last publication taken on channels with recovery. A
delta publication on a server-side subscription, which the client never asks
for, makes the client reconnect (`connecting` code 5). A rebuilt payload, and
the publications of one message together, are capped by the inbound message
limit (64 MiB); a recovery larger than that fails on every resubscribe, as an
oversized recovery reply does without delta.

```rust
use ferrifuge::DeltaFormat;

let subscription = client
    .subscription_builder("dashboard")
    .delta(DeltaFormat::Fossil)
    .build()?;
```

### Server-side subscriptions

Subscriptions the server makes for the connection (at connect, or later
through its API) need no code of yours: the client tracks them and reports
them as `ClientEvent::ServerSubscription { channel, event }`, where `event`
is `Subscribing`, `Subscribed`, `Unsubscribed { code, reason }` or
`Publication`. A publication goes to a client-side subscription to the
channel when that one is subscribed, else to the server-side one.

```rust
use ferrifuge::{ClientEvent, ServerSubscriptionEvent};

while let Some(event) = events.next().await {
    if let ClientEvent::ServerSubscription { channel, event } = event {
        match event {
            ServerSubscriptionEvent::Publication(publication) => {
                println!("{channel}: {} bytes", publication.data.len());
            }
            other => println!("{channel}: {}", other.name()),
        }
    }
}
```

`client.server_subscriptions()` lists their channels: a snapshot, which the
events may already have overtaken. A subscription the server no longer grants
after a reconnect ends with code 4 (`codes::unsubscribed::NOT_GRANTED_ON_RECONNECT`).

A subscription the server makes recoverable keeps its stream position, which
every publication delivered moves; each connect asks to recover from it, so the
publications missed while disconnected arrive after its `subscribed` event,
which reports `was_recovering` and `recovered`. On "state invalidated" (3014)
the position becomes unmatchable, so the recovery fails and `subscribed`
reports `recovered` false, as for client-side subscriptions.

### Calls: RPC, publish, history, presence

The client's calls:

| Call | Returns |
|---|---|
| `client.rpc(method, data)` | the result data of the server's RPC handler (with Centrifugo, its RPC proxy) |
| `client.publish(channel, data)` | `()` once the server accepted the publication |
| `client.history(channel, options)` | a `HistoryResult`: the publications `HistoryOptions` select (`limit`, `since`, `reverse`), and the stream's current `epoch` and `offset` |
| `client.presence(channel)` | a `PresenceResult`: the subscribed clients by client id, with their user, connection info and channel info |
| `client.presence_stats(channel)` | a `PresenceStatsResult`: `num_clients` and `num_users` |
| `client.send(data)` | `()` once the message is handed to the transport, which writes it even if the client disconnects right after: the server sends no reply. It waits for the connection as the others do, but has no reply to wait for |

Data is raw JSON with the JSON protocol, any bytes with Protobuf. Each call
is made when the method returns, and its future borrows nothing from the
client, so it can be spawned.

```rust
use ferrifuge::HistoryOptions;

let result = client.rpc("echo", br#"{"n":1}"#.to_vec()).await?;
client.publish("news", br#"{"text":"hi"}"#.to_vec()).await?;
let history = client.history("news", HistoryOptions::default().limit(10)).await?;
```

`HistoryOptions::default()` returns no publications, only the stream's
position; `limit(-1)` asks for all of them, and the server may cap the number
(Centrifugo `client.history_max_publication_limit`, 300 by default). Without `since`, a forward history
starts at the oldest publication kept, so the newest ones come with
`reverse(true)`, or `since` the position before them.

A subscription has the same calls on its channel: `subscription.publish(data)`,
`history(options)`, `presence()` and `presence_stats()`. While the
subscription is subscribing, such a call waits for it, for at most
`Config::timeout`; it fails with code 7 `SUBSCRIPTION_UNSUBSCRIBED` when the
subscription is, or becomes, unsubscribed before the call is sent.

A call made before the client is connected, `connect()` included, waits for
the connection, then for its reply, each for at most `Config::timeout`. It
fails with a `CallError` carrying the server's code, message and `temporary`
flag, or with a client-side code:

| Code | When |
|---|---|
| 1 `TIMEOUT` | the subscription not subscribed, no connection, no room in the write queue, or no reply, within `Config::timeout` |
| 3 `DISCONNECTED` | the client entered `disconnected`: `disconnect()`, a terminal disconnect code or a denied token |
| 7 `SUBSCRIPTION_UNSUBSCRIBED` | a subscription's call, before it was sent: the subscription is or became unsubscribed |
| 10 `TRANSPORT_WRITE` | the server refused the HTTP request carrying the call as too large (413) |
| 11 `CONNECTION_CLOSED` | the transport the call was sent on closed before the reply |

Calls go out in the order they were made, while their bytes fit
`Config::write_queue_bytes` with those handed to the transport and not yet
written; the others wait for room, so an application that calls faster than
the connection writes is held back instead of growing the client's memory.

A reply settles its call as soon as it is read, even while the application
has not taken earlier events, so call results are not ordered with events.
Dropping the future abandons the call: one still waiting for the connection
is never sent.

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
while the client keeps reconnecting. `Message` events carry the data of the
server's asynchronous messages, exactly as received.

### Configuration

| `Config` field | Default | Meaning |
|---|---|---|
| `min_reconnect_delay` | 500 ms | Lower bound of the reconnect delay |
| `max_reconnect_delay` | 20 s | Upper bound of the reconnect delay |
| `timeout` | 5 s | Time allowed from opening the transport to the connect reply, for a websocket frame to be written (a server that stops reading then counts as a lost connection), for closing a websocket, and for a call to get the connection, then its reply |
| `name` | `ferrifuge` | Client name reported to the server |
| `version` | crate version | Client version reported to the server |
| `token` | empty | Connection token sent with the first connect |
| `max_server_ping_delay` | 10 s | Wait past the server's ping interval before reconnecting; 0 disables |
| `protocol` | `Protocol::Json` | Encoding of the client protocol: `Json` or `Protobuf` (binary frames, any payload bytes) |
| `reconnect_jitter_window` | 0 | Upper bound of a random delay added to the first reconnect after losing a connection, so that many clients disconnected at once do not reconnect at once |
| `read_ahead_messages` | 256 | Most messages read and not yet handled while the application has not taken earlier events; then the client stops reading the connection, holding the server back instead of buffering |
| `read_ahead_bytes` | 4 MiB | Most bytes of those messages; a single larger message is still read when none waits |
| `write_queue_bytes` | 4 MiB | Most bytes of the application's calls handed to the transport and not yet written; a call that does not fit waits for room, for at most `timeout`, and a single larger call goes out when nothing waits. The client's own commands are not counted |

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

### Seeded chaos

`tests/chaos.rs` (Block 16) drives the sans-IO core through random
operations against a simulated server, on a simulated clock, and checks
the client's invariants after every step: legal state changes, at most one
transport, liveness once faults stop, cleanup after `disconnect()`, calls
that settle, exactly-once delivery on recoverable channels, a bounded
read-ahead, and batches that hold their calls and respect the frame cap. A seed fixes every choice, so a failure prints the command that
reruns it:

```sh
cargo test --test chaos                                     # seeds 0-299, runs with the tests
cargo test --release --test chaos seeded_chaos_long_run -- --ignored   # 10 000 seeds from a random start
FERRIFUGE_CHAOS_SEED=<seed> FERRIFUGE_CHAOS_OPS=<ops> cargo test --release --test chaos seeded_chaos_long_run -- --ignored
FERRIFUGE_CHAOS_SEED=<seed> cargo test --test chaos trace -- --ignored --nocapture   # a seed's whole trace
```

`tests/chaos_adapter.rs` runs the same kind of operations on the tokio
client against the fake server, in real time, checking what the adapter
adds: the driver's tasks, hand-off delivery, a second `events()` call and
cancelled reads. Its seeds fix the operations, not their timing, so a
failure is replayed rather than always reproduced:

```sh
cargo test --test chaos_adapter                             # seeds 0-19, runs with the tests
cargo test --test chaos_adapter seeded_chaos_adapter_long_run -- --ignored   # 500 seeds
```

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
build. It runs the unit tests, the scenarios that need no server and the
chaos seeds on Windows and macOS, the interop test, each fuzz target for
30 s (10 min nightly) and the semver check. The perf, wall-clock, fuzz and long chaos jobs
run on merges to `main`, nightly and on demand (`workflow_dispatch`), not
on pull requests. cargo-deny also runs daily.

## License

[0BSD](LICENSE). [`proto/client.proto`](proto/client.proto) is copied from
[centrifugal/protocol](https://github.com/centrifugal/protocol) and keeps its
MIT license ([`proto/LICENSE`](proto/LICENSE)).

Builds with the `aws-lc-rs` or `fips` feature include AWS-LC, which is
partly under the OpenSSL license. That license requires its attribution
notice in advertising and in redistributions of the built binaries.
