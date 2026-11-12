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

Before 1.0, a minor version may break the API; [CHANGELOG.md](CHANGELOG.md)
says what each version changes. Work is organised in blocks specified in
[CLAUDE.md](CLAUDE.md). Vulnerabilities are reported privately, as
[SECURITY.md](SECURITY.md) explains.

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
  next connection attempt; `set_http_headers`, which applies to every HTTP
  request started after it, the emulation requests of an open `http_stream`
  or sse transport included; and `Subscription::set_data`, which applies to
  the subscription's next subscribe commands;
- batches (`start_batching`): calls and subscriptions sent together in
  frames of at most `Config::batch_frame_bytes`;
- server pings: answered with pongs, and a connection whose pings stop is
  re-established;
- an `error` event for every failed connection attempt;
- websocket transport (`ws://` and `wss://`) with the JSON or the
  Protobuf protocol;
- http_stream (JSON or Protobuf) and sse (JSON) transports (`http://` and
  `https://`), tried in order when websocket cannot connect;
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
- the server's "state invalidated" codes: stream positions, and the tokens
  a token callback can fetch again, are dropped before subscribing or
  connecting again;
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

Add it to `Cargo.toml` (until the first release reaches crates.io, use
`ferrifuge = { git = "https://github.com/jbiscella/ferrifuge" }`):

```toml
[dependencies]
ferrifuge = "0.21"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`examples/` holds runnable programs: `quickstart`, `recovery`, `calls` and
`delta`. They expect a server started with `examples/centrifugo.json`, which
accepts connections without a token and keeps a history of channels without
a namespace; it enables the server API without a key, so it is for local
development only:

```sh
docker run --rm -p 8000:8000 -v "$PWD/examples/centrifugo.json:/centrifugo/config.json" \
  centrifugo/centrifugo:v6.10.1 centrifugo --config /centrifugo/config.json
cargo run --example quickstart
curl -d '{"channel":"news","data":{"text":"hello"}}' localhost:8000/api/publish
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

The client runs as a task on the tokio runtime current when it is built,
or on the one given with `ClientBuilder::runtime`, e.g. in a Tauri setup
hook, which runs outside a runtime:

```rust,ignore
let client = Client::builder("wss://example.com/connection/websocket")
    .runtime(tauri::async_runtime::handle().inner().clone())
    .build()?;
```

Without either, building returns a configuration error. When that runtime
shuts down, the client emits `disconnected` with code 4 "runtime shut
down": its calls fail and its streams end. `connect()` and
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
in the same struct. `client.close().await` closes the client for good and
waits until the transport is closed and every stream has ended: its
subscriptions emit `Unsubscribed` with code 2 "client closed", then the
client emits `Disconnected`, and later calls, those of leftover
`Subscription` handles included, fail with code 4 "client closed".
`is_closed()` tells a closed client apart, as it reports `Disconnected`.
Dropping the client closes it the same way, without waiting. `Client::new` and a builder that cannot work return a
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

An application that only consumes publications can take
`subscription.publications()` instead of `events()`: a stream of the
subscription's publications, and nothing else. It is the subscription's one
stream, as `events()` is, and the two take over from each other. The events
it skips count as delivered as it passes them, so an unread publications
stream holds the connection as an unread `events()` does. It hides what only
those events tell: the `Subscribed` above, which reports lost publications
(without `get_state`), an `Unsubscribed` after which it goes silent without
ending, and errors.

```rust
let mut publications = subscription.publications();
subscription.subscribe();
while let Some(publication) = publications.next().await {
    println!("{} bytes", publication.data.len());
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
client drops its connection token and the token of every subscription that has
a token callback, before reconnecting, and the callback gives a new token. A
fixed token without a callback is kept and sent again, as after any other
reconnect; if the server finds it expired, the client or the subscription ends
with code 1 "unauthorized", after an `error` event with code 12. A subscription
that has a stream position keeps it with the epoch `_`, so its next recovery
fails and you are told: with `get_state` your state is reloaded; without it,
the `subscribed` event reports `was_recovering` true and `recovered` false.

### Map subscriptions (unstable)

Centrifugo 6.8.0 and later can hold a channel's state as a map of keys,
in namespaces declaring `subscription_type: map` (or `map_clients`,
`map_users`). With the `unstable` feature, which may change in any minor
release, the client sets and removes keys of such a channel:

```rust
client.map_publish("lobby:1", "seat-3", br#"{"name":"ada"}"#.to_vec()).await?;
client.map_remove("lobby:1", "seat-3").await?;
```

Each waits for the connection, as any call. The namespace must allow a
client's writes (`map.allow_publish_for_client`, `map.allow_remove_for_client`,
or their `_for_anonymous` and `_for_subscriber` forms), or the call fails
with code 103; the server may replace the key (`map.client_key`), and
refuses an empty one, or a publication without data, with 107.

A map subscription loads such a channel's whole state, then follows its
changes per key:

```rust
use futures_util::StreamExt;
use ferrifuge::{MapKind, MapSubscriptionEvent};

let scores = client
    .map_subscription_builder("scores:game-1", MapKind::Map)
    .page_size(500)
    .build()?;
let mut events = scores.events();
scores.subscribe();
while let Some(event) = events.next().await {
    match event {
        MapSubscriptionEvent::Sync(entries) => { /* the whole state: replace your copy */ }
        MapSubscriptionEvent::Update(update) if update.removed => { /* forget update.key */ }
        MapSubscriptionEvent::Update(update) => { /* set update.key to update.data */ }
        _ => {}
    }
}
```

Its stream yields `subscribing`, then `subscribed` once the state is
loaded, then one `Sync` with every entry, keys in the order the server
gave them and the changes made while it loaded already applied, then an
`Update` per change, the publisher's own included. `scores.publish(key,
data)` and `scores.remove(key)` write keys once it is subscribed.
`MapKind::Clients` and `MapKind::Users` follow a channel's presence, keyed
by client id with its info and by user id, on the channel the stream
namespace's `map_clients_presence_channel_prefix` and
`map_users_presence_channel_prefix` name. A map subscription shares the
registry with stream subscriptions, one per channel, but
`get_subscription` and `subscriptions()` list stream subscriptions only,
`get_map_subscription` and `map_subscriptions()` map ones.

The server sends the state in pages of `page_size` entries, clamped to
the namespace's `map.min_page_size` and `map.max_page_size` (100 and 1000
by default; 0 leaves the size to the server), and times the whole load:
past `map.subscribe_catch_up_timeout` (5 s by default) it closes the
connection with 3008 "slow". The client therefore handles a page as soon
as it reads it, even while the application has not taken earlier events;
but a full read-ahead still stops the reading (Block 15), so an
application that leaves its streams unread can make loads time out. The
pages of one load may add up to `max_state_bytes`, 64 MiB by default,
and so may the state or catch-up rebuilt from them; past it the
subscription ends with `unsubscribed` 5 "map state too large". A server older than 6.8.0 answers a map subscription as a stream
one, and the subscription ends with `unsubscribed` 6. The application
holds the `Sync` it gets, and its own copy of the map.

After a lost connection, an `unsubscribe()` or a server's 2500, a map
subscription whose namespace keeps a stream (`map.mode` `recoverable` or
`persistent`) catches up from where the application's last taken event
left it: `subscribed` reports `was_recovering` and `recovered`, and the
missed changes follow as `Update`s, with no `Sync`, since the
application's copy is still right. Each update taken moves that position,
so changes not taken before a disconnect come back. When the server cannot
recover the position (code 112: the stream was trimmed past it, the map
was cleared with `map_clear`, or its epoch changed), the default
`UnrecoverableStrategy::FromScratch` loads the state again after the
resubscribe backoff, with no `error` event, and a new `Sync` replaces the
application's copy; `UnrecoverableStrategy::Fatal` ends the subscription
with `unsubscribed` 112 instead. An ephemeral namespace keeps no stream, so
every resubscribe loads the state again. A `Sync` not taken before the
subscription stops is withdrawn, and the next subscribe loads the state
again. The server's "state invalidated" (unsubscribe 2502, disconnect
3014) and `set_tag_filter` drop the position, so the next subscribe loads
the state; 2502 also drops the subscription token, and 3014 drops it when a
token callback can replace it, keeping a fixed token without one. A load that fails with a temporary error (113
"concurrent pagination", say) is unsubscribed on the server first, then
starts again after the backoff.

`.tag_filter(filter)` limits the state and the changes to the keys a filter
matches, on namespaces with `allow_tags_filter`; offsets then skip what the
filter left out, which is no gap. `set_tag_filter` applies from the next
subscribe, which loads the state through the new filter.

`.delta(DeltaFormat::Fossil)` asks for per-key deltas, on namespaces with
`allowed_delta_types` (`delta_publish` sends every change as a delta): each
key's chain is separate, and the application gets whole values, byte for
byte, in `Sync` and `Update`. `Subscribed::delta` says whether the server
agreed, and `delta_statistics()` what it saved. The client holds a base per
key, one copy of every key's value, each within `max_message_bytes`, as long
as delta is on: account for it next to the `Sync`. A recovering subscribe
keeps the bases, since the next delta of a key the catch-up did not touch
applies to the value held before the reconnect; a load from scratch drops
them. A removal deletes its key's base, and the key's next value comes
whole. A delta that cannot be applied emits an `error` event
(`ErrorKind::Delta`, code 15) and `subscribing` code 2, then the subscription
recovers after the resubscribe backoff, the other keys' bases kept. Delta
and a tag filter cannot go together: `build()` and `set_tag_filter` refuse
it.

`MapSubscriptionBuilder::handler` or `set_handler` takes the events in place
of the stream, a closure or a `MapSubscriptionHandler` with `on_sync` and
`on_update`, run by the client's consumer as any handler (see Event
handlers). `MapEntry::json` and `MapUpdate::json` decode a value,
`publish_json` encodes one. With `test-util`, `MapEntry::new`,
`MapUpdate::new` and `MapUpdate::removal` build events for an application's
tests. Map subscriptions work in the browser too.

### Shared poll subscriptions (unstable)

Centrifugo 6.8.0 and later can poll an application's backend once per key
for every subscriber of a channel and send each new version of a key's
value, in namespaces declaring `subscription_type: shared_poll` (options
under `shared_poll`: `mode`, `versionless` by default or `versioned`,
`refresh_interval`, `max_keys_per_connection`, 5000 by default; the
refresh endpoint is `channel.proxy.shared_poll_refresh`). With the
`unstable` feature, which may change in any minor release, a shared poll
subscription names the keys it follows, each under a signature the
backend issued:

```rust
use futures_util::StreamExt;
use ferrifuge::{SharedPollSubscriptionEvent, SharedPollTrackItem};

let votes = client.new_shared_poll_subscription("votes:feed-1")?;
let mut events = votes.events();
votes.subscribe();
// A signature from your backend over exactly these keys, in this order.
votes.track_with_signature([SharedPollTrackItem::new("post-1", 0)], signature);
while let Some(event) = events.next().await {
    match event {
        SharedPollSubscriptionEvent::Update(update) if update.removed => { /* forget update.key */ }
        SharedPollSubscriptionEvent::Update(update) => { /* set update.key to update.data */ }
        _ => {}
    }
}
```

A signature is `iat:exp:hmac`: the hex HMAC-SHA256, with the server's
`shared_poll.hmac_secret_key`, of `iat`, `exp` (0 for none), the user id
(empty for an anonymous client), the channel and the hex SHA-256 of the
keys joined by NUL bytes, the five joined by NUL bytes. It covers the keys
in their order: the server refuses it for the same keys in another order
(103), one expired more than 5 s ago (109), and keys past
`max_keys_per_connection` (106). The client checks no signature;
it carries it. `SharedPollSubscriptionBuilder::get_signature` gives the
client a callback signing keys itself: `track(keys)` then asks it for the
keys, and the keys tracked without a signature are signed after each
subscribe. A callback that fails reports an `Error` event of kind `Track`
with code 14, and the keys stay tracked.

The stream yields `subscribing`, `subscribed`, then an `Update` per new
version of a tracked key's value, each version once: `update.version` is
the value's version, `None` in versionless mode. A key the backend removes,
or one the server refuses to track, comes as an `Update` with `removed`
and no data, and is no longer tracked; a refused track also reports an
`Error` event of kind `Track` with the server's code. An update not taken
when the subscription leaves `subscribed` for a client-side reason is
withdrawn, and the next subscribe asks for the value again; a removal is
never withdrawn. `untrack(keys)` stops a key at once; an empty set sends
nothing.

The client keeps the keys, the versions taken and the signatures across
reconnects: each subscribe tracks every key again, with the signatures it
holds and the versions taken, so a mass reconnect asks the backend for
nothing while its signatures stay valid, and a device gets a value again
only when it missed it. `unsubscribe()` forgets them. A signature with an
expiry is the application's to renew: call `track_with_signature` again
before it expires, as the server drops its keys some time after, with no
event, while `tracked_keys()` still lists them. Keys and signatures grow
only by the application's calls and its backend's answers, not by the
server, and are not bounded by the client: `max_keys_per_connection` is
the usual limit.

Track and untrack requests are `sub_refresh` commands. An untrack of many
keys is split so that no request passes `max_track_frame_bytes` (60 000
by default) by its worst-case size; keep it below the server's
`websocket.message_size_limit` and the emulation endpoint's
`max_request_body_size`, 65 536 bytes each by default, as a larger
websocket message closes the connection with 1009, which ends the client.
`.delta(DeltaFormat::Fossil)` asks for deltas on namespaces with
`allowed_delta_types`; on Centrifugo OSS a delta exists only when the
backend's refresh returns `prev_data`. A server older than 6.8.0 answers
a shared poll subscription as a stream one, and the subscription ends with
`unsubscribed` 8; a namespace of another type refuses it with 108. A
shared poll subscription shares the registry with the other kinds, one per
channel: `get_shared_poll_subscription` and `shared_poll_subscriptions()`
list shared poll ones only.

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

### Tag filters

A subscription can ask the server to send only the publications whose tags
match a filter, on channels with `allow_tags_filter` (Centrifugo from
v6.4.0). It saves bandwidth; it is not access control.

```rust
use ferrifuge::TagFilter;

let subscription = client
    .subscription_builder("prices")
    .tag_filter(TagFilter::is_in("ticker", ["BTC", "ETH"]).and(!TagFilter::eq("venue", "test")))
    .build()?;
subscription.subscribe();
// From the next subscribe command on.
subscription.set_tag_filter(Some(TagFilter::eq("ticker", "SOL")))?;
```

A filter the server would refuse (a comparison with an empty key, a set
comparison with no value) and a filter with delta compression are refused
when built or set, with `SubscriptionError::InvalidTagFilter` and
`TagFilterWithDelta`.

### Channel compaction

On a Centrifugo PRO server with channel compaction, a subscription built
with `.channel_compaction(true)` asks, with every subscribe command, to be
named by a numeric id instead of its channel in publication, join and leave
pushes, which saves bandwidth when messages are short. A server without it
ignores the request. The ids belong to the connection: the client forgets
them when it ends. CI has no Centrifugo PRO server, so routing by id is
tested against the fake server only.

```rust
let subscription = client
    .subscription_builder("prices")
    .channel_compaction(true)
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
limit (`Config::max_message_bytes`, 64 MiB by default); a recovery larger than that fails on every resubscribe, as an
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

With serde types, `client.publish_json(channel, &value)`,
`subscription.publish_json(&value)` and `client.rpc_json(method, &value)`
encode the value as JSON, and `rpc_json` decodes the result into the type
asked for; `publication.json::<T>()` decodes a publication, live, recovered
or from history. They work under both protocols: with Protobuf the data are
the JSON bytes. A typed call fails with a `JsonCallError`: `Encode` when the
value did not encode, and nothing was sent; `Call` with the `CallError` of
the untyped call; `Decode`, holding the result data, when the server
answered with data the type does not accept, so the RPC ran. serde_json's
error messages may quote parts of the data: mind it when logging them. For an
RPC that returns no data, use `rpc`: empty data is not JSON.

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Note {
    text: String,
}

client.publish_json("news", &Note { text: "hi".into() }).await?;
let sum: serde_json::Value = client.rpc_json("sum", &serde_json::json!({"a": 1, "b": 2})).await?;
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
flag, or with a client-side code, among them 1 "timeout", 3 "disconnected",
4 "client closed", 7 "subscription unsubscribed", 10 "transport write error"
and 11 "connection closed": see [Codes](#codes) for each one's cause and
whether a retry can succeed.

Calls go out in the order they were made, while their bytes fit
`Config::write_queue_bytes` with those handed to the transport and not yet
written; the others wait for room, so an application that calls faster than
the connection writes is held back instead of growing the client's memory.

A reply settles its call as soon as it is read, even while the application
has not taken earlier events, so call results are not ordered with events.
Dropping the future abandons the call: one still waiting for the connection
is never sent.

#### Publish debouncing (Centrifugo PRO)

On a namespace where Centrifugo PRO advises a debounce interval
(`client_publish_debounce_interval`, sent as `publish_debounce` in the
subscribe reply), `Subscription::publish` and `publish_json` send the first
value at once and the latest one per interval, per channel: a publish that
comes while a window is open is merged, and the value is sent when the
interval ends, the timer not reset by later publishes. Each publish reports
the server's answer for the value that replaced it: it resolves with the
result of the send that carried later data, and never succeeds before the
server has answered that send (`client.proto` advises the interval, the
result is the SDK's; centrifuge-js and Centrifugo's documentation resolve at
once with an empty result).

Data waiting in a window is never sent once its subscription leaves
`subscribed`: the publishes waiting fail with code 7 if the subscription
became `unsubscribed`, 3 if the client became `disconnected`, 4 if it was
closed, and otherwise (a lost transport, a server resubscribe, a delta
failure) with 17 `PUBLISH_DROPPED`, safe to publish again once subscribed. A
trailing send already handed to the connection, its publishes following it,
keeps Block 13's rules for that connection. For data that must arrive, use
`Client::publish`, which is never debounced (the documentation's own caveat).
On a map subscription (unstable, the `unstable` feature),
`MapSubscription::publish` and `publish_json` are debounced per key, each
key with its own window, so a chatty key never delays a quiet one;
`MapSubscription::remove` is sent at once and opens no window: a window of
the key closes, its pending value is never sent, and the publishes waiting
in it settle with the remove's result, so a remove is not undone by a
trailing publish. `Client::map_publish` and `Client::map_remove` are never
debounced. Debouncing is untested against a real Centrifugo PRO server: only
the interval the protocol carries has been tested, as the open-source server,
which never sends it, publishes every value. In a hidden browser tab, timer
throttling (see [Browser](#browser)) makes windows and trailing sends lag by
as much, the latest value still kept.

### Batching

The client batches by itself: the commands made before its driver next runs,
from any task, go out together, in the order they were made, in frames, or
emulation requests over http_stream and sse, of at most
`Config::batch_frame_bytes`. There is no delay: an idle client sends a call
at once. A `batch_frame_bytes` of 1 sends every command alone.

An application can also hold its calls and subscriptions for longer, until
it stops a batch:

```rust
let batch = client.start_batching();
let rpc = client.rpc("echo", br#"{"n":1}"#.to_vec());
let publish = client.publish("news", br#"{"n":2}"#.to_vec());
batch.stop(); // or drop it
let (rpc, publish) = (rpc.await?, publish.await?);
```

A machine of the sans-IO core batches only when built with
`Machine::with_automatic_batching`; its owner then feeds `Input::Flush`
once it has fed the inputs it has, before performing the actions.

Over websocket the subscribe commands of the subscriptions waiting to
subscribe go in the frame of the connect command, as many as fit
`batch_frame_bytes`, so that the first publications arrive a round trip
sooner; a subscription that needs its token, data or getState callback
first subscribes after the connect reply, as do the rest. Over http_stream
and sse they follow the connect reply. An attempt whose connect fails
carries those subscribe commands for nothing: across a fleet reconnecting
to a failing server, that is up to `batch_frame_bytes` per attempt. A
smaller `batch_frame_bytes` lowers it; a `batch_frame_bytes` of 1 turns it
off, and automatic batching with it.

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
`ClientBuilder::emulation_endpoint`, e.g. behind a path prefix. http_stream
carries JSON or Protobuf; sse carries JSON only, as in Centrifugo, so
building a client with the Protobuf protocol and an sse endpoint fails with
a configuration error.

### Connection tokens

Set the initial token in `Config::token`, and give the builder a callback
that returns a new one:

```rust
use ferrifuge::{Client, Config, TokenError};

let client = Client::builder("ws://localhost:8000/connection/websocket")
    .config(Config::default().token(initial_token))
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
`ferrifuge::codes`, and their values follow centrifuge-js; [Codes](#codes)
says what each means and what the client does next. `Error` events
report problems that do not change the state, such as a failed attempt
while the client keeps reconnecting. `Message` events carry the data of the
server's asynchronous messages, exactly as received.

To wait until the client is connected, or a subscription subscribed,
without reading their streams, await `ready()`. A disconnected client fails
it at once with code 3 "disconnected". While connecting, it waits across
failed attempts and fails with code 3 if the client gives up. A
subscription's fails with code 7 "subscription unsubscribed" once the
subscription is unsubscribed, and with code 3 if its client becomes
disconnected while it waits. No timeout applies; wrap it in one:

```rust
use std::time::Duration;

client.connect();
tokio::time::timeout(Duration::from_secs(10), client.ready()).await??;
```

`ready()` follows `state()`, not the events taken, so it can resolve before
the `Connected` or `Subscribed` event is read. The state moves only as the
client handles messages, each after the events of earlier ones are taken: a
task that holds a stream with events not taken, and awaits `ready()` without
reading it, waits until the stream is read or dropped.

`client.statistics()` returns a snapshot of counters since the client was
built, for status displays, support tickets and telemetry: the state, the
transport and when the client connected, connects and failed attempts,
bytes and messages each way (as read and as written, without transport
framing), publications delivered (as taken from the streams) and the
registry sizes. `subscription.delta_statistics()` reports what delta
compression did: full and delta payloads, their bytes as received and as
rebuilt, and `compression_ratio()`.

### Handlers

Instead of reading a stream, an application can set a handler on the client
or on a subscription: `ClientBuilder::handler`, `Client::set_handler`,
`SubscriptionBuilder::handler` or `Subscription::set_handler`. A handler is a
closure `FnMut(ClientEvent)` or `FnMut(SubscriptionEvent)`, or a type
implementing `ClientHandler` or `SubscriptionHandler`, whose `on_event` calls
by default one method per kind of event (`on_connected`, `on_publication`,
and so on), each doing nothing unless overridden.

```rust
use ferrifuge::{Publication, SubscriptionHandler};

struct Prices;

impl SubscriptionHandler for Prices {
    fn on_publication(&mut self, publication: Publication) {
        println!("{} bytes", publication.data.len());
    }
}

let subscription = client.subscription_builder("prices").handler(Prices).build()?;
subscription.subscribe();
```

- A handler takes over the object's stream, as a second `events()` call
  does, and a later `events()` call takes over from it.
- One consumer runs every handler of a client and of its subscriptions, one
  call at a time, in the order the events were emitted.
- An event counts as delivered when its handler method returns, as a
  stream's when it yields it (see States and events). Recovery is therefore
  exactly-once in handlers too.
- A handler that panics disconnects the client with code 2 "exception in
  event handler: …" without reconnecting. Its event is not delivered, so the
  next `connect()` recovers it.
- Handlers run on a task of the client's runtime (`EventDelivery::Task`, the
  default). On a multi-thread runtime, a handler that blocks lets the
  client's timers and pongs go on. On a current-thread runtime nothing else
  runs until it returns.
- `EventDelivery::Inline` runs them on the client's driver instead, adding
  no task per client. It gives up "timers keep running" (see States and
  events): while a handler runs, no timer fires, nothing is read and no ping
  is answered, so a handler that runs longer than the ping interval plus
  `max_server_ping_delay` may make the client reconnect.
- `EventDelivery::Thread` runs them on a thread of the client's own, named
  `ferrifuge-events` and started at the first handler: handlers that block
  take this mode, since the runtime and the driver go on meanwhile. If the
  thread cannot start, the client disconnects with code 2 as for a panic,
  and the next `connect()` tries again.
- Under `Task` and `Inline`, a handler that calls its own client and waits
  fails at once with error 16 "would deadlock": a call, `ready()` or
  `close()` (`close()` then returns once closing has started). Under
  `Thread` a wait fails with it only when nothing but a message held back
  by the handler's own event could end it. That is a call or `ready()`
  waiting for the connection or a subscription while that event holds the
  messages after it, or a call whose reply cannot be read because a full
  read-ahead stopped the reading. Other waits complete, and `close()`
  returns once the driver stops. Calls of another client never fail.

The bounds of the handler traits, of the token, data and getState
callbacks and of the futures and streams the client returns read
`MaybeSend` and `MaybeSync`. On native targets they are `Send` and `Sync`,
so native code sees no difference; on the browser target they ask nothing,
since JavaScript handles are neither.

### Codes

How to read a code: a code means something only with the event or error
that carries it: 1 is "transport closed" in `Connecting` and "timeout" in
an error. The same fields also carry server codes, in their own ranges
(the last table). The client's logs show the state, code and reason
(Block 21). Values follow centrifuge-js 63ed73e `src/codes.ts`; Ferrifuge
adds `connecting` 5, 6 and 7, `subscribing` 2, `unsubscribed` 3 to 6 and 8
(8: Block 47), and errors 15, 16 and 17; error 14 is centrifuge-js's
shared-poll code (Block 47). Blocks are those of the repository's
`CLAUDE.md`.

#### `connecting`

`ClientEvent::Connecting`: the client lost or has not yet made its connection and is trying again.

| Code | Constant | Reason or message | Cause | Then |
|---|---|---|---|---|
| 0 | `codes::connecting::CONNECT_CALLED` | `"connect called"` | `connect()` was called (Block 1) | at once |
| 1 | `codes::connecting::TRANSPORT_CLOSED` | `"transport closed"` | the transport failed or closed, an undecodable frame included (Blocks 1, 8) | the next transport of a first fallback sequence at once, else after the reconnect backoff (and the jitter window when it was connected) |
| 2 | `codes::connecting::NO_PING` | `"no ping"` | no data from the server within its ping interval plus `max_server_ping_delay`, or a system sleep longer than that (Blocks 4, 25) | after the reconnect backoff |
| 3 | `codes::connecting::SUBSCRIBE_TIMEOUT` | `"subscribe timeout"` | a subscribe got no reply within `Config::timeout`, so the server's state is unknown (Block 9) | after the reconnect backoff |
| 4 | `codes::connecting::UNSUBSCRIBE_ERROR` | `"unsubscribe error"` | an unsubscribe failed or got no reply in time (Block 9) | after the reconnect backoff |
| 5 | `codes::connecting::DELTA_ERROR` | `"delta error"` | a server-side subscription received a delta publication (Block 19, D8) | after the reconnect backoff, which this connection did not reset |
| 6 | `codes::connecting::RECONNECT_CALLED` | `"reconnect called"` | `reconnect()` was called (Block 25, D5) | at once, after the jitter window when it was connected |
| 7 | `codes::connecting::READ_AHEAD_OVERFLOW` | `"read-ahead overflow"` | a message arrived while the inbox of a browser transport was full, the application not reading its streams (Block 44, D7) | after the reconnect backoff and the jitter window; before a server ping on that connection, the backoff keeps growing (Block 15) |

#### `disconnected`

`ClientEvent::Disconnected`: the client stopped trying.

| Code | Constant | Reason or message | Cause | Then |
|---|---|---|---|---|
| 0 | `codes::disconnected::DISCONNECT_CALLED` | `"disconnect called"` | `disconnect()` or `close()` was called, or the client was dropped (Blocks 1, 33) | never, until `connect()`; never after `close()` |
| 1 | `codes::disconnected::UNAUTHORIZED` | `"unauthorized"` | a token or connect data callback denied access, returned an empty token, or a token expired without a callback (Blocks 1, 5, 17) | never, until `connect()` |
| 2 | `codes::disconnected::BAD_PROTOCOL` | `"exception during message handling"`, or `"exception in event handler: "` and the panic's message | the task running the client panicked (Block 28, D4), or an event handler did (Block 41, D4) | never: the client is closed; after a handler, until `connect()` |
| 3 | `codes::disconnected::MESSAGE_SIZE_LIMIT` | `"message size limit exceeded"` | the server closed with 1009, refused a refresh with HTTP 413, or sent a message over `max_message_bytes` once connected (Blocks 3, 8, 29) | never, until `connect()` |
| 4 | `codes::disconnected::RUNTIME_SHUT_DOWN` | `"runtime shut down"` | the tokio runtime the client ran on shut down (Block 28, D3) | never: the client is closed |

#### `subscribing`

`SubscriptionEvent::Subscribing`: the subscription is not subscribed now and will be again.

| Code | Constant | Reason or message | Cause | Then |
|---|---|---|---|---|
| 0 | `codes::subscribing::SUBSCRIBE_CALLED` | `"subscribe called"` | `subscribe()` was called (Block 9) | at once when connected, else when the client connects |
| 1 | `codes::subscribing::TRANSPORT_CLOSED` | `"transport closed"` | the client lost its connection, or `disconnect()` was called (Block 9) | when the client connects |
| 2 | `codes::subscribing::DELTA_ERROR` | `"delta error"` | a delta publication could not be applied (Block 19, D8) | after the resubscribe backoff, recovering from the last publication taken |

#### `unsubscribed`

`SubscriptionEvent::Unsubscribed`, and a server-side subscription's `Unsubscribed`: the subscription stopped.

| Code | Constant | Reason or message | Cause | Then |
|---|---|---|---|---|
| 0 | `codes::unsubscribed::UNSUBSCRIBE_CALLED` | `"unsubscribe called"` | `unsubscribe()` was called, or the subscription was removed (Block 9) | never, until `subscribe()` |
| 1 | `codes::unsubscribed::UNAUTHORIZED` | `"unauthorized"` | a subscription token or data callback denied access, or returned an empty token, or error 109 came without a token callback (Blocks 9, 10) | never, until `subscribe()` |
| 2 | `codes::unsubscribed::CLIENT_CLOSED` | `"client closed"` | the client was closed: `close()`, dropped, its runtime shut down or its driver panicked (Block 33) | never: the client is closed |
| 3 | `codes::unsubscribed::MESSAGE_SIZE_LIMIT` | `"message size limit exceeded"` | the server refused a subscribe or `sub_refresh` with HTTP 413 (Block 8) | never, until `subscribe()` |
| 4 | `codes::unsubscribed::NOT_GRANTED_ON_RECONNECT` | `"not subscribed by the server after reconnect"` | a server-side subscription absent from a reconnect's connect reply (Block 12) | never: it left the registry |
| 5 | `codes::unsubscribed::MAP_STATE_TOO_LARGE` | `"map state too large"` | a map subscription's load passed its `max_state_bytes` (Block 45, D5; `unstable`) | never, until `subscribe()` |
| 6 | `codes::unsubscribed::MAP_NOT_SUPPORTED` | `"map subscriptions not supported"` | the server answered a map subscription as a stream one, as a Centrifugo before v6.8.0 does (Block 45, D15; `unstable`) | never, until `subscribe()` |
| 8 | `codes::unsubscribed::SHARED_POLL_NOT_SUPPORTED` | `"shared poll subscriptions not supported"` | the server answered a shared poll subscription as a stream one, as a Centrifugo before v6.8.0 does (Block 47, D4; `unstable`) | never, until `subscribe()` |

#### `error`

`ClientError` (in `Error` events and build errors) and `CallError` (failed calls).

| Code | Constant | Reason or message | Cause | Then |
|---|---|---|---|---|
| 1 | `codes::error::TIMEOUT` | `"timeout"` | a command got no reply within `Config::timeout`, or a call waited longer for the connection or its subscription (Block 13) | a call fails and may succeed when retried; a refresh is retried |
| 2 | `codes::error::TRANSPORT_CLOSED` | the transport's own message | a transport failed to open or closed while connecting (Blocks 1, 8) | an `Error` event; the client keeps trying |
| 3 | `codes::error::DISCONNECTED` | `"disconnected"` | the client entered `disconnected` while a call was pending, or a `ready()` waited (Blocks 13, 30) | the call fails; a retry can succeed after `connect()` |
| 4 | `codes::error::CLIENT_CLOSED` | `"client closed"` | the client is closed: `close()`, dropped, its runtime shut down or its driver panicked (Block 33) | the call fails; a retry never succeeds |
| 5 | `codes::error::CONNECT_TOKEN` | the token callback's message | the token callback failed while getting a token to connect with (Block 1) | an `Error` event; the attempt is retried after the reconnect backoff |
| 6 | `codes::error::REFRESH_TOKEN` | the token callback's message | the token callback failed while getting a token to refresh with (Block 5) | an `Error` event; the refresh is retried after 5 to 10 s |
| 7 | `codes::error::SUBSCRIPTION_UNSUBSCRIBED` | `"subscription unsubscribed"` | a subscription-level call or `ready()` met an unsubscribed subscription (Blocks 13, 30) | the call fails; a retry can succeed after `subscribe()` |
| 8 | `codes::error::SUBSCRIPTION_SUBSCRIBE_TOKEN` | the token callback's message | a subscription token callback failed while getting a token to subscribe with (Block 10) | an `Error` event; the subscribe is retried after the resubscribe backoff |
| 9 | `codes::error::SUBSCRIPTION_REFRESH_TOKEN` | the token callback's message | a subscription token callback failed while getting a token to refresh with (Block 10) | an `Error` event; the refresh is retried after 10 to 20 s |
| 10 | `codes::error::TRANSPORT_WRITE` | `"transport write error"` | an emulation request carrying the call was answered with HTTP 413 (Block 8) | the call fails; a retry with the same data never succeeds |
| 11 | `codes::error::CONNECTION_CLOSED` | `"connection closed"` | the transport a call was sent on closed before its reply (Block 13) | the call fails; a retry can succeed once reconnected |
| 12 | `codes::error::BAD_CONFIGURATION` | what is misconfigured, or the data callback's message | a builder that cannot work, a token expiring without a callback, error 109 without a token callback, or a failing connect or subscription data callback (Blocks 8, 10, 17) | building fails; for a data callback, an `Error` event and a retry after the backoff |
| 13 | `codes::error::SUBSCRIPTION_GET_STATE` | the getState callback's message | the subscription getState callback failed (Block 11) | an `Error` event; getState is retried after the resubscribe backoff |
| 8 | `codes::error::SUBSCRIPTION_SUBSCRIBE_TOKEN` | the token callback's message | a subscription token callback failed while getting a token to subscribe with (Block 10) | an `Error` event; the subscribe is retried after the resubscribe backoff |
| 15 | `codes::error::DELTA` | what failed in the delta | a delta publication could not be applied (Block 19, D8) | an `Error` event; the subscription subscribes again (`subscribing` 2) or, server-side, the client reconnects (`connecting` 5) |
| 16 | `codes::error::WOULD_DEADLOCK` | `"would deadlock: called from an event handler of this client"` | a handler of the client waited on a call, `ready()` or `close()` of that client (Block 41, D6) | the call fails at once and is abandoned |
| 17 | `codes::error::PUBLISH_DROPPED` | `"publish dropped"` | a publish waiting in a debounce window was dropped, never sent, as its subscription left `subscribed` while the client stayed up (Block 46, D10) | the call fails; publish again when subscribed |

#### Server codes

| Codes | Carried by | Then |
|---|---|---|
| disconnect 3500–3999, 4500–4999 | `Disconnected` | stays disconnected until `connect()` (Block 3) |
| disconnect 3000–3499, 4000–4499, from 5000 | `Connecting` | reconnects after the reconnect backoff (Block 3) |
| disconnect 3008 | `Connecting` | as above; before the first server ping on that connection, the backoff keeps growing (Block 15); during a map subscription's load it means the namespace's `map.subscribe_catch_up_timeout` passed, and the next connection loads again (Block 45) |
| disconnect 3014 | `Connecting` | drops the tokens a token callback can fetch again, keeps a fixed token without one, makes every stream position unmatchable (drops a map subscription's position and delta bases, Block 45), then reconnects (Block 11) |
| websocket close 1009 | `Disconnected` 3 | stays disconnected until `connect()` (Block 3); other close codes below 3000 give `Connecting` 1 |
| unsubscribe below 2500 | `Unsubscribed` | stays unsubscribed (Block 10) |
| unsubscribe from 2500 | `Subscribing` | subscribes again at once (Block 10); 2502 drops the token and the position first (Block 11); on a map subscription 2500 recovers, and after `map_clear` that recovery fails with 112 (Block 45); any code ends a server-side subscription (Block 12) |
| connect error, temporary or below 100, or 109 | `Error`, then `Connecting` | reconnects after the backoff; for 109 the next attempt gets a new token, and without a token callback the client reports error 12, then `Disconnected` 1 (Blocks 1, 5, 17) |
| connect error, any other | `Disconnected` | with the error's code, until `connect()` (Block 1) |
| refresh error, temporary or below 100 | `Error` | retried after 5 to 10 s (Block 5) |
| refresh error, any other | `Disconnected` | with the error's code (Block 5) |
| subscribe error 109 | `Error` or `Unsubscribed` 1 | a new token, then resubscribes; without a token callback, error 12, then `Unsubscribed` 1 (Block 10) |
| subscribe error 112 with getState | none | the state is reloaded and the subscription resubscribes after the backoff (Block 11); on a map subscription, with `FromScratch` no `Error` and the state is loaded again after the backoff, with `Fatal` `Unsubscribed` 112 (Block 45) |
| subscribe error 113 | `Error` | temporary: the map subscription's load starts again after the backoff (Block 45) |
| subscribe error, temporary or below 100 | `Error` | resubscribes after the resubscribe backoff (Block 9) |
| subscribe error, any other | `Unsubscribed` | with the error's code (Block 9) |
| `sub_refresh` error, temporary or below 100 | `Error` | retried after 10 to 20 s (Block 10) |
| `sub_refresh` error, any other | `Unsubscribed` | with the error's code (Block 10) |
| 14 | `codes::error::SHARED_POLL_GET_SIGNATURE` | the `get_signature` callback's message | a shared poll subscription's `get_signature` callback failed (Block 47, D6; `unstable`) | an `Error` event of kind `Track`; the keys stay tracked and are asked for again on the next subscribe |
| shared poll `sub_refresh` error, `type` 1 (track) | `Error` of kind `Track` | except for 109, a temporary error or one below 100, the keys of the request are untracked, with an untrack command and a removal `Update` each, as for 103 (signature refused) and 106 (key limit) (Block 47, D7) |
| shared poll `sub_refresh` error, `type` 2 (untrack) | `Error` of kind `Untrack` | nothing more (Block 47, D7) |
| shared poll subscribe error (`type` 4) | as subscribe errors | as for a stream subscription (Blocks 9, 10); 108 on a namespace that is not shared poll (Block 47, D4) |
| call error | `CallError` | the server's code, message and `temporary`; the client never retries a call (Block 13) |

### Configuration

| `Config` field | Default | Meaning |
|---|---|---|
| `min_reconnect_delay` | 500 ms | Lower bound of the reconnect delay |
| `max_reconnect_delay` | 20 s | Upper bound of the reconnect delay |
| `timeout` | 5 s | Time allowed for the connect reply once the transport is open, for a websocket frame to be written (a server that stops reading then counts as a lost connection), for closing a websocket, and for a call to get the connection, then its reply |
| `handshake_timeout` | 5 s | Time allowed for opening a transport: DNS, TCP, the proxy handshake, TLS and the websocket upgrade, or an HTTP stream's response headers. One attempt can take both timeouts (10 s by default, 30 s for a first fallback over three transports); set both to 2.5 s to keep 5 s per attempt. A call made while connecting still waits for the connection for at most `timeout`, so on a slow open it can fail first |
| `name` | `ferrifuge` | Client name reported to the server |
| `version` | crate version | Client version reported to the server |
| `token` | empty | Connection token sent with the first connect |
| `max_server_ping_delay` | 10 s | Wait past the server's ping interval before reconnecting; 0 disables |
| `protocol` | `Protocol::Json` | Encoding of the client protocol: `Json` or `Protobuf` (binary frames, any payload bytes) |
| `reconnect_jitter_window` | 0 | Upper bound of a random delay added to the first reconnect after losing a connection, so that many clients disconnected at once do not reconnect at once |
| `read_ahead_messages` | 256 | Most messages read and not yet handled while the application has not taken earlier events; then the client stops reading the connection, holding the server back instead of buffering (in the browser an inbox of the same size waits instead, and past it the client reconnects with code 7) |
| `read_ahead_bytes` | 4 MiB | Most bytes of those messages; a single larger message is still read when none waits |
| `write_queue_bytes` | 4 MiB | Most bytes of the application's calls handed to the transport and not yet written; a call that does not fit waits for room, for at most `timeout`, and a single larger call goes out when nothing waits. The client's own commands are not counted |
| `batch_frame_bytes` | 16 KiB | Most bytes of a frame of several commands; keep it below the server's message and request size limits |
| `max_message_bytes` | 64 MiB | Largest message accepted from the server, on every transport: the client's memory bound per message, as the read-ahead takes one larger message alone. Lower it on devices with little memory, above the largest history or recovery reply expected. A larger message before the connect reply fails the attempt; after it the client ends with `disconnected` code 3 "message size limit exceeded" and does not reconnect, since the server would send the same data again |

`Config` is `#[non_exhaustive]`: start from `Config::default()`, then use
the setter named after each field, or assign the fields.

```rust
use std::time::Duration;
use ferrifuge::Config;

let config = Config::default()
    .handshake_timeout(Duration::from_secs(10))
    .max_message_bytes(4 << 20);
```

### Cargo features

| Feature | Default | Adds |
|---|---|---|
| `tokio` | yes | `Client`, the websocket, http_stream and sse transports, TLS through rustls verified as the platform does (see TLS verification) |
| `ring` | yes | ring as the TLS crypto provider; builds with a C compiler |
| `aws-lc-rs` | no | aws-lc-rs as the TLS crypto provider; builds with a C compiler |
| `fips` | no | aws-lc-rs in FIPS mode; also needs CMake and Go to build |
| `test-util` | no | constructors of the events and results the client yields, and with `tokio` an in-memory server double, for an application's tests (see Testing your application) |
| `cookies` | no | a cookie jar for the client: `CookieJar`, `MemoryJar` and `ClientBuilder::cookie_jar`; implies `tokio` (see Cookies) |
| `browser` | yes | on `wasm32-unknown-unknown`, the same `Client` over the browser's WebSocket, fetch (http_stream) and EventSource (sse); nothing on other targets (see Browser) |
| `unstable` | no | experimental APIs following centrifuge-js 63ed73e and Centrifugo's experimental features, outside the semver promise: they may change in any minor release (see Map subscriptions and Shared poll subscriptions) |

`tokio` needs one of the provider features. Cargo features are additive, so
a build without ring turns default features off:

```toml
ferrifuge = { version = "0.21", default-features = false, features = ["tokio", "fips"] }
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

The core alone builds for `wasm32-unknown-unknown`, and CI checks it on every
pull request. There `tokio`, `ring`, `aws-lc-rs`, `fips` and `cookies` build
as nothing, so the default features give the browser adapter (see Browser).
Without `browser`, as for a non-JavaScript wasm host, the machine's random
delays (backoff, refresh jitter) start from a constant, because fastrand has
no entropy source there without its `js` feature: every machine of a page,
and the n-th machine of every page, draws the same delays. `browser` seeds
them from the browser's `crypto.getRandomValues`.

### Cookies

With the `cookies` feature, a client keeps the cookies a load balancer or
gateway sets (session affinity, an auth session) and sends them back, as a
browser does for centrifuge-js:

```rust
use std::sync::Arc;
use ferrifuge::{Client, MemoryJar};

let jar = Arc::new(MemoryJar::new());
let client = Client::builder("wss://example.com/connection/websocket")
    .cookie_jar(jar.clone())
    .build()?;
```

- The websocket upgrade sends the jar's cookies, and the cookies of its
  response, a refused upgrade's included, go into the jar. A proxy's 407
  response sets none, and no proxy is sent them.
- A `Cookie` header given in `http_headers` and the jar's cookies go in one
  header, the application's pairs first; a jar cookie of the same name is
  left out.
- The jar sees `ws://` and `wss://` URLs as `http://` and `https://`, so
  `HttpOnly` and `Secure` cookies work, and never sees the URL's query.
- `MemoryJar` keeps cookies for the life of the process, takes at most 3000
  cookies of at most 4096 bytes each, and does no public-suffix check. An
  application that needs one, or cookies that outlive the process,
  implements `CookieJar` over its own store; its method set is reqwest's
  `CookieStore`'s, so one jar can serve both clients.

### Testing your application

With the `test-util` feature, an application's tests build the events and
results the client yields, to test the code that handles them:

```toml
[dev-dependencies]
ferrifuge = { version = "0.21", features = ["test-util"] }
```

```rust
use ferrifuge::{ClientEvent, ClientInfo, HistoryResult, Publication};

let connected = ClientEvent::connected("c1", "websocket", None);
let publication = Publication::new(br#"{"n":1}"#.to_vec())
    .offset(4)
    .info(ClientInfo::new("c1", "u1"));
let history = HistoryResult::default()
    .publications([publication.clone()])
    .epoch("e")
    .offset(4);
```

Each constructor takes what has no sensible default, and each setter one
field; a field left out holds what the client yields when the server leaves
it out. `SubscriptionEvent`, `ClientError` and `CallError` are built
directly, without the feature.

`test_util::Server` is an in-memory Centrifugo double the real client runs
against, through its connector, with no socket: the test sets its answers,
publishes, disconnects with a code and reads the commands it received.

```rust,ignore
use ferrifuge::test_util::Server;
use ferrifuge::{CallError, Client};

let server = Server::new();
server.on_rpc(|method, data| match method {
    "echo" => Ok(data.to_vec()),
    _ => Err(CallError::new(104, "method not found")),
});
let client = Client::builder(server.endpoint())
    .connector(server.connector())
    .build()?;
let news = client.new_subscription("news")?;
client.connect();
news.subscribe();
news.ready().await?;
server.publish("news", br#"{"n":1}"#.to_vec()); // the subscription receives it at offset 1
server.disconnect(3001, "shutdown"); // the client reconnects and recovers what it missed
```

Every channel has one stream with offsets from 1 and a history, so a
reconnect recovers what the client missed; the double answers JSON and
Protobuf clients, grants server-side subscriptions, refuses connects and
subscribes as asked, and answers history, presence and RPC. Tokens, server
pings, join and leave, delta compression and the HTTP transports are left
to Centrifugo itself.

### TLS verification

How a `wss://` or `https://` server is verified depends on the build and
the platform:

| Build | macOS, iOS, Windows, Android | Linux and other Unix systems |
|---|---|---|
| `ring`, `aws-lc-rs` | the operating system's verifier (`rustls-platform-verifier`): its store, its distrust settings, revocation and intermediate fetching; on Android after one setup call (see Mobile) | webpki over the system's root certificates (`rustls-native-certs`, which reads `SSL_CERT_FILE` and `SSL_CERT_DIR`) |
| `fips` | webpki over the system's root certificates, so that chains are checked inside the FIPS module (unsupported on Android and iOS) | the same |

`SSL_CERT_FILE` and `SSL_CERT_DIR` apply only where webpki verifies: on
Linux, and in the `fips` build. On Android, `root_certificates` is refused:
the platform verifier there takes no extra roots, so give a `tls_config`
trusting them instead.

A private certificate authority, such as a corporate TLS inspection proxy's,
is trusted in addition to the platform's with `root_certificates`:

```rust,ignore
use ferrifuge::rustls::pki_types::CertificateDer;
use ferrifuge::rustls::pki_types::pem::PemObject;

let roots = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
let client = Client::builder(endpoint).root_certificates(roots).build()?;
```

`tls_config` takes a whole `rustls::ClientConfig` instead, for client
certificates, pinning or a verifier of your own: its roots, verifier and
provider are used as given, and Ferrifuge only sets its ALPN list to
`http/1.1`. It cannot be combined with `root_certificates`; in the `fips`
build, its `fips()` must be true, and its verifier is yours to choose. Each
client given extra roots holds its own TLS settings, so many clients should
share one `tls_config` instead. `ferrifuge::rustls` re-exports the rustls
version Ferrifuge uses: moving to another rustls 0.x line is a breaking
release of Ferrifuge. No setting accepts invalid certificates.

### Proxies

Every transport takes the same proxy decision for each URL it opens. By
default (`Proxy::system()`) it follows the environment, then the operating
system: `ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` (upper case
before lower case, ignored when `REQUEST_METHOD` is set), then macOS's HTTP
and HTTPS proxies and Windows' Internet settings. `ws://` endpoints follow
the HTTP proxy, `wss://` ones the HTTPS proxy. The settings are read once per
process.

```rust,ignore
use ferrifuge::{Client, Proxy, ProxyTarget};

let proxy = ProxyTarget::new("http://proxy.corp.example:3128")?.basic_auth("user", "password");
let client = Client::builder(endpoint)
    .proxy(Proxy::all(proxy).no_proxy("localhost, .internal.example"))
    .build()?;
```

`Proxy::from_env()` reads the environment only, `Proxy::none()` connects
directly, `Proxy::all` uses one proxy, and `Proxy::custom` lets a function
choose per endpoint (given as `scheme://host:port`). A `ProxyTarget` is
`http://`, `https://` (TLS to the proxy, verified with the client's TLS
settings), `socks5://` (the client resolves names) or `socks5h://` (the
proxy does), with Basic or SOCKS5 username and password credentials.
`Client::set_proxy` applies from the next connection attempt.

Websocket and `https://` requests go through a CONNECT tunnel, or SOCKS5,
with TLS to Centrifugo inside it. Plain `http://` requests of http_stream
and sse go to an HTTP proxy in absolute form, as curl sends them: such a
proxy sees them in full, including the sse connect command and its token in
the URL, so prefer `https://` endpoints behind a proxy. A proxy refusing the
credentials makes the attempt fail with "proxy authentication required". No
record, error or `Debug` output shows the credentials, and `http_headers`
refuses `Proxy-Authorization`.

For networking the client does not have, a VPN tunnel, a custom proxy or
an in-process transport, give it a connector: a function from the
websocket endpoint (an `http::Uri`, its port always written) to a
connected stream. The client then runs TLS, for `wss://`, and the
websocket upgrade over that stream, as over one it dials, and reads no
proxy setting, the environment's included: a connector replaces the proxy
setting, so `ClientBuilder::proxy` with it fails `build()`, as do
http_stream and sse endpoints, which it cannot carry. Its errors and panics
fail the attempt as a refused connection does, and the handshake timeout
bounds it.

```rust,ignore
let client = Client::builder("wss://centrifugo.example/connection/websocket")
    .connector(|endpoint: ferrifuge::http::Uri| async move {
        my_tunnel.open(endpoint.host().unwrap_or_default(), endpoint.port_u16().unwrap_or(443)).await
    })
    .build()?;
```

Not supported: PAC files and WPAD, NTLM, Negotiate and Kerberos proxy
authentication (a local helper such as Px or Cntlm can bridge them), SOCKS4,
and the settings hyper-util does not read (macOS's exceptions list and SOCKS
proxy, Linux desktop settings, iOS and Android system settings, where the
environment applies).

### Mobile

Ferrifuge builds for Android (`aarch64-linux-android`, API level 22 and up)
and iOS (`aarch64-apple-ios`, iOS 13 and up); CI builds both, without
running on a device. For iOS, set `IPHONEOS_DEPLOYMENT_TARGET` to the app's
deployment target, so that rustc and the C builds of ring or aws-lc agree.

**Backgrounding.** Mobile systems suspend an app's sockets in the
background. Disconnect when the app goes to the background and connect when
it returns, or keep the client running only where the system allows it.
Recovery and in-order delivery make the gap lossless on channels with
history and recovery, within the server's history size and TTL: the next
subscribe asks for what was missed. For what a client does after the
device slept, see Sleep and network changes.

**Android setup.** The operating system's certificate verifier needs:

- the Kotlin component of `rustls-platform-verifier` in the app's Gradle
  build, at the version of `rustls-platform-verifier-android` in your
  `Cargo.lock`, kept by the ProGuard rule of the verifier's README;
- one call to `ferrifuge::android::init_with_env` (or `init_with_refs`,
  `init_with_runtime`) per process, before building a client that uses TLS.
  Until then such a client fails `build()`, or its handshakes fail, naming
  the call. `ferrifuge::android` exposes `jni` 0.22 types and is exempt from
  semver.

Revocation is checked from API level 24. Extra roots go through a
`tls_config`.

**iOS** needs no setup: its verifier is Security.framework, and
`root_certificates` works there. On both, only the environment's proxy
applies (see Proxies), and the `fips` build is unsupported.

### Browser

A web front-end in Rust (Leptos, Yew, Dioxus) runs the same `Client`,
built for `wasm32-unknown-unknown` with the default features: the `browser`
feature gives it the browser's WebSocket, `fetch` and `EventSource`, the page's event loop
for its tasks, `setTimeout` for its timers, `performance.now()` and
`Date.now()` for its clocks. With `--no-default-features`, add `features = ["browser"]`.

```rust,ignore
let client = ferrifuge::Client::new("wss://example.com/connection/websocket", Config::default())?;
client.connect();
```

**What is not there.** The options the browser decides itself do not
exist on that target, so code using them does not build for it: real HTTP
headers (`http_headers`, `set_http_headers`; the emulated headers stay),
TLS settings (`root_certificates`, `tls_config`, the `rustls` re-export),
proxies (`proxy`, `set_proxy`, `Proxy`), the connector and the `http`
re-export, `runtime`, the cookie jar (the browser keeps cookies),
`EventDelivery::Thread` (no threads), `test_util::Server` and
`ferrifuge::android`. There the client is neither `Send` nor `Sync`, and
its callbacks need not be either: a token callback may hold an `Rc` and
await a `fetch`.

**The inbox.** A browser client never stops reading: a WebSocket or an
EventSource delivers every message as an event, and Safari stops
delivering a `fetch` body left unread, so http_stream reads its body
continuously too. While the client's read-ahead is full, because the
application does not take its events, arriving messages wait in an inbox
with the read-ahead's own limits (`read_ahead_messages`,
`read_ahead_bytes`), so a client holds at most twice its read-ahead. A
message that would overflow it closes the connection, and the client
reconnects with `connecting` code 7 "read-ahead overflow": the events it
had not taken are withdrawn, and recovery brings them back on recoverable
channels within the server's recovery limit; elsewhere they can be lost.
Before a server ping is read on the connection the backoff keeps growing,
as after a 3008 close. The server is not held back, as natively.

Over websocket a frame counts as written when the browser takes it, so
there is no write timeout; a stalled connection still ends at the no-ping
deadline. A message over `max_message_bytes` is refused once the browser
has received it whole. The browser hides the upgrade's response, so a
failed attempt says "websocket closed before open (code N)" and names no
captive portal.

**http_stream.** A client given an http_stream endpoint
(`Client::builder_with_transports`) falls back to it as natively, over the
page's `fetch`: the stream request posts the connect command, commands go
in requests to the emulation endpoint, JSON or Protobuf, with
`credentials: "same-origin"` unless credentials are included (below), so a
server on another origin must allow CORS (Centrifugo does, per
`allowed_origins`). Its response is read
continuously into the inbox described above. Closing or dropping the
transport aborts the stream and every emulation request in flight. A
redirect is not followed: the browser hides it, and the failed attempt
says "HTTP redirect; a captive portal may be answering"; a 511 or an HTML
page answered with a success says so too.

**sse.** An sse endpoint is opened with the page's `EventSource`, the
connect command in the URL's `cf_connect` as natively, without
credentials unless they are included (below); commands go in emulation requests over `fetch`, as from
http_stream, and Protobuf is still refused with sse at build. Its messages
wait in the inbox, as over websocket. When the stream ends or fails, the
client closes the `EventSource` at once, so the browser's own retry, which
would resend a connect command whose token may have expired, never runs:
the client emits `connecting` with code 1 "transport closed" and
reconnects by its backoff. `EventSource` shows neither status nor
headers, so a failed attempt names no captive portal.

**Credentials and CORS.** `include_credentials(true)` makes the
http_stream and sse requests carry the page's cookies and HTTP
authentication to a server on another origin, and store its `Set-Cookie`:
fetch's `credentials: "include"` and `EventSource`'s `withCredentials:
true`, for sticky sessions or cookie-based admission behind a load
balancer on another origin. The server must answer credentialed CORS
requests from the page's origin, with that origin rather than `*`
(Centrifugo does, per `allowed_origins`). By default, as in centrifuge-js,
only the page's own origin gets them. The websocket upgrade is unaffected:
the browser sends its cookies by their `SameSite` rules.

**Shims.** `websocket`, `fetch`, `readable_stream` and `event_source` give
the client a constructor or function of the application's in place of the
page's global of that name, as centrifuge-js's options of those names do:
for an environment that lacks one, or to wrap one. Each attempt uses the
shim given, else the global: websocket needs `WebSocket`, http_stream
`fetch`, `ReadableStream` and `AbortController`, sse `EventSource` and
`fetch`. A transport whose constructors are missing is skipped in the
fallback round, without an error event, and `build()` fails with a
configuration error naming what each transport needs when none of the
configured transports can be used; a global removed under a running client
makes the attempt that finds none fail with "no supported transport
found". The WebSocket and `EventSource` shims are called as constructors
and their objects used by method and property name; the `fetch` shim is
called as `fetch(url, init)` with a plain `init`. With the
`ReadableStream` shim the http_stream response is read through a stream
built with it, one chunk per `pull` with `highWaterMark` 0.

```rust,ignore
// http_stream through the application's fetch, cookies included.
let client = ferrifuge::Client::builder_with_transports([(
    ferrifuge::machine::TransportKind::HttpStream,
    "https://realtime.example.com/connection/http_stream".to_owned(),
)])
.fetch(my_fetch)
.include_credentials(true)
.build()?;
```

**Network and page events.** While it is not disconnected, the client
listens to the page's `offline` and `online` events and to `pagehide` and
`pageshow`, as centrifuge-js does, on `globalThis` (the window, or a
worker's global). Going offline closes the connection: a connected client
emits `connecting` with code 1 "transport closed", a connecting one closes
its attempt without an event, and the next attempt waits the reconnect
delay. Coming online while connecting reconnects at once, from the first
transport, with the backoff restarted; a client that lost its connection
waits a draw from `reconnect_jitter_window` first, so that a building whose
network comes back does not reconnect all its devices at once. A
`pagehide` whose `persisted` is true, the page entering the back/forward
cache, closes the connection the same way, with the reason "page entered
back/forward cache", so the server does not keep a frozen page's
connection; the `pageshow` that restores the page reconnects as `online`
does. A `pagehide` or `pageshow` whose `persisted` is false changes
nothing. Without these events the reconnect delay still runs as usual.
`network_event_target` and `page_event_target` name other targets;
`web_sys::EventTarget::new()`, which never fires, turns either off. The
listeners do not keep the client alive, and are removed when it
disconnects, is closed or is dropped.

```rust,ignore
// Only page events, through a target the app forwards them to.
let page = web_sys::EventTarget::new()?;
let client = ferrifuge::Client::builder("wss://example.com/connection/websocket")
    .network_event_target(web_sys::EventTarget::new()?)
    .page_event_target(page.clone())
    .build()?;
```

**Handlers and panics.** `EventDelivery::Task` runs handlers as a task of
the page, `Inline` on the client's driver; both block the page while a
handler runs, and a call of the client from its own handler fails at once
with `WOULD_DEADLOCK`. wasm32-unknown-unknown builds with `panic =
"abort"`: a panicking handler or callback traps the module, whose later
calls are undefined. Set a panic hook (`std::panic::set_hook`) that logs
to the console to see why.

**Clocks and hidden tabs.** The sleep detection compares `Date.now()` with
`performance.now()`. Whether a browser's monotonic clock counts a system
sleep is not specified: where it does, no gap is seen and timers fire at
the wake as if they had counted it. Browsers throttle the timers of hidden
tabs, Chrome's intensive throttling included, which an open WebSocket does
not prevent: reconnect delays, token refreshes and the no-ping deadline may
fire late, while pongs, answered from message events, are not delayed.

**Logging** goes through `tracing` as natively; a browser subscriber such
as [tracing-web](https://crates.io/crates/tracing-web) shows the records
in the console.

**Size.** A release module that builds a client and connects it is about
1.1 MiB after `wasm-bindgen`, 313 KiB gzipped, before any `wasm-opt`
(2026-11-09); CI reports it on every change.

**Tested browsers:** headless Chrome and Firefox on every pull request,
Safari on `main` and nightly.

### Sleep and network changes

The client notices a system sleep at the first input after the wake: a
timer, data from the server, a callback's answer or a call. It compares how
far the wall clock and the monotonic clock moved since the previous input,
and takes a difference of 2 s or more for a sleep. Nothing wakes an idle
client to look sooner: it notices at its next timer, at the latest the
no-ping deadline (the server's ping interval plus `max_server_ping_delay`).
Then:

- after a sleep at least as long as that no-ping window, the server has
  given up on the connection: the client closes it and reconnects with
  code 2 "no ping", after the reconnect backoff and the jitter window, so
  devices waking together do not reconnect together;
- after a shorter sleep the connection stays and the no-ping window starts
  again from the wake; without server pings the connection always stays;
- every token refresh, the connection's and each subscription's, moves
  earlier by the sleep, so a token that expired meanwhile is refreshed at
  the wake, or fetched before the next connect or subscribe, never sent
  expired;
- reply deadlines, call timeouts and the reconnect and resubscribe delays
  are left alone: their clock stopped too.

A wall clock set forward by 2 s or more counts as a sleep; one set back is
ignored. On Windows the monotonic clock (`QueryPerformanceCounter`) counts a
suspend, so no sleep is seen there: the timers come due at the wake as if
the device had stayed awake, with the same effect.

To reconnect at once instead, call `reconnect()` when the platform reports a
wake or a network change: macOS `NSWorkspace.didWakeNotification`, Windows
`WM_POWERBROADCAST`, systemd-logind `PrepareForSleep`, Android
`ConnectivityManager.NetworkCallback`, iOS `NWPathMonitor`. A connected
client closes its transport, emits `connecting` with code 6 "reconnect
called" and connects again after the jitter window only, without the
backoff; a connecting one starts a new attempt at once. Either way the
transports are tried again from the first, as a new network may allow
websocket again, and every client's next attempt reads the environment's
and the operating system's proxy settings again. Calls waiting for the
connection keep waiting, and on channels with history and recovery the
next connection recovers what the old one had not delivered. A disconnected
client stays disconnected: `connect()` starts it. To fail waiting calls
instead, call `disconnect()`, then `connect()`.

A network behind a captive portal answers the client's attempts with its
sign-in page. Such an attempt fails like any other, retried after the
backoff, and its `error` event's message ends with "; a captive portal may
be answering": a redirect, HTTP 511, or a web page served as a success, in
place of the websocket upgrade, the HTTP stream or the event stream. An app
can tell its user to sign in to the network.

### Logging

The adapters log through [`tracing`](https://docs.rs/tracing); the
sans-IO core logs nothing and reports the protocol breaches only it sees as
`Action::Anomaly`. Every record's target starts with `ferrifuge`:

| Level | What |
|---|---|
| WARN | Server anomalies: a frame that is not client protocol data, a reply without its command's result, a delta that cannot be applied, a message over the size limit, a websocket closed with 1009 |
| DEBUG | Each state change of the client and of its subscriptions, with its code and reason; each transport attempt, its opening, failure and close; the next transport of a fallback; each reconnect and resubscribe scheduled, with its delay; each callback started and how it ended; the read-ahead filling and draining; each call that fails; a system sleep detected, with its gap |
| TRACE | Every input given to the core and every action it returns; each frame sent or received, by length |

Each client logs in a `client` span whose `client` field is a number unique
in the process, and each subscription in a `subscription` span with its
`channel`. Values are fields (`state`, `code`, `reason`, `transport`,
`endpoint`, `channel`, `bytes`, `delay`, `kind`, `gap`). No record holds a
token, any payload, connection or channel info, or a header value: data
shows as its length and endpoints without userinfo, query or fragment.

With `tracing-subscriber`:

```rust,ignore
tracing_subscriber::fmt()
    .with_env_filter("info,ferrifuge=debug")
    .init();
```

Without a `tracing` subscriber, the records go to the `log` crate, e.g.
`env_logger` with `RUST_LOG=info,ferrifuge=debug`. Through `log` they carry
no span fields; WARN records carry their own `client` field, and span
entries and exits show at TRACE under the target `tracing::span::active`.

Keep Ferrifuge's dependencies at INFO or above in production: they log on
their own, through `log`. At TRACE, tungstenite 0.30 logs the whole
websocket upgrade request, its query and header values included (the
`Cookie` header of a client with a cookie jar among them), and every frame
sent and received, the connect frame with its token. reqwest's own `Jar`,
if an application uses one, logs cookie values at DEBUG; `MemoryJar` logs
none.
rustls-platform-verifier logs each failed verification at ERROR, with no
secret. A `tracing` application sees those dependency records only through
`tracing-log`.

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
read-ahead, batches that hold their calls and respect the frame cap, and
publications that carry exactly the payload published, also when they come
as Fossil deltas, some of them broken by the simulated server. A seed fixes
every choice, so a failure prints the command that reruns it:

```sh
cargo test --test chaos                                     # seeds 0-299, runs with the tests
cargo test --release --test chaos seeded_chaos_long_run -- --ignored   # 10 000 seeds from a random start
FERRIFUGE_CHAOS_SEED=<seed> FERRIFUGE_CHAOS_OPS=<ops> cargo test --release --test chaos seeded_chaos_long_run -- --ignored
FERRIFUGE_CHAOS_SEED=<seed> cargo test --test chaos trace -- --ignored --nocapture   # a seed's whole trace
FERRIFUGE_CHAOS_SUBSCRIPTIONS=3 cargo test --release --test chaos seeded_chaos_long_run -- --ignored   # the long run with delta subscriptions
```

`tests/chaos_adapter.rs` runs the same kind of operations on the tokio
client against the fake server, in real time, checking what the adapter
adds: the driver's tasks, hand-off delivery, a second `events()` call and
cancelled reads. Its seeds fix the operations, not their timing, so a
failure is replayed rather than always reproduced:

```sh
cargo test --test chaos_adapter                             # seeds 0-19, runs with the tests
cargo test --test chaos_adapter seeded_chaos_adapter_long_run -- --ignored   # 500 seeds
FERRIFUGE_CHAOS_DELTA=1 cargo test --test chaos_adapter seeded_chaos_adapter_long_run -- --ignored   # with delta subscriptions
```

### Interop, fuzzing and semver

```sh
npm ci --prefix tests/interop --ignore-scripts          # centrifuge-js, pinned
FERRIFUGE_INTEROP=1 cargo test --test interop             # centrifuge-js publishes, ferrifuge receives
cargo bolero test --toolchain nightly json::tests::fuzz_decode -T 30s   # one fuzz target
cargo semver-checks --default-features --baseline-rev main               # public API changes
```

The interop test needs Docker and Node. The fuzz targets (`fuzz_*` tests
next to the JSON and Protobuf decoders and the http_stream and sse
splitters, Protobuf's included) run on random inputs under `cargo test`,
and coverage-guided with
[cargo-bolero](https://crates.io/crates/cargo-bolero) and a nightly
toolchain. A breaking change to the public API needs a version bump.

### Continuous integration

CI runs rustfmt, clippy (pedantic, warnings denied) with and without default
features, rustdoc, the tests with a 70% line-coverage floor, an MSRV build
and cargo-deny. It also builds and tests with the aws-lc-rs and fips
providers, and runs the performance tests above, writing their measurements
to the job summary; the wall-clock ones run in a job that never fails the
build. It runs the unit tests, the scenarios that need no server and the
chaos seeds on Windows and macOS, builds the library and its unit tests for
Android and iOS, the core alone and the browser adapter for
`wasm32-unknown-unknown` (their clippy also refusing the std clock and
thread calls that panic there), with a check that a module linking them
imports nothing but wasm-bindgen's glue, and runs the `@W` tests in headless
Chrome and Firefox (`browser` job; Safari on `main` and nightly), the
interop test, each fuzz target for
30 s (10 min nightly) and the semver check, natively and for the browser
target. The perf, wall-clock, fuzz and long chaos jobs
run on merges to `main`, nightly and on demand (`workflow_dispatch`), not
on pull requests. cargo-deny also runs daily.

## License

[0BSD](LICENSE), except for the files listed under "Exceptions" in
[LICENSE](LICENSE), which keep their own licenses; [REUSE.toml](REUSE.toml)
states the same per file. The crate ships
[`proto/client.proto`](proto/client.proto), copied from
[centrifugal/protocol](https://github.com/centrifugal/protocol) under the MIT
license ([`proto/LICENSE`](proto/LICENSE)), so its license expression is
`0BSD AND MIT`.

Binaries that link ferrifuge must carry the notices of what they link: the
MIT notice of `proto/client.proto` and those of the dependencies (MIT, ISC,
Apache-2.0, BSD, Unicode-3.0), which `cargo about` can generate for the
features built.
Builds with the `fips` feature also include AWS-LC-FIPS, partly under the
OpenSSL and SSLeay licenses: their notices go with every redistribution, and
advertising and documentation must state "This product includes software
developed by the OpenSSL Project for use in the OpenSSL Toolkit.
(http://www.openssl.org/)" and "This product includes cryptographic software
written by Eric Young (eay@cryptsoft.com)".
