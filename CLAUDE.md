# CLAUDE.md — Rust Centrifugo client SDK

Status: DRAFT — blocks 0–13 of N. Further blocks to be appended.

Markers used below:
- `VERIFY:` a detail not confirmed from documentation; resolve by reading the cited source before implementing. If the source contradicts the scenario, stop and report — do not silently adapt.
- `@A @B @D @E`: test layer that must implement the scenario (see Block 0).

---

## Block 0 — Scope, references, test layers

**Goal:** fix what is built, against which references, and where each scenario is tested.

**Dependencies:** none.

### Decisions

| Decision | Choice | Reason |
|---|---|---|
| Build vs reuse existing crates (tokio-centrifuge, centrifuge-client) | Build from scratch | Owner's choice |
| Behavioural oracle | Centrifugo Client SDK spec | All official SDKs follow it |
| Implementation reference | centrifuge-js | Widest feature set |
| Wire contract | `client.proto` of centrifugal/protocol | Source of truth for frames |
| Serialization | JSON and Protobuf | Both supported by centrifuge-js |
| Transports | websocket, http_stream, sse | Fallback in this order |
| `closed` client state | Not implemented | Spec marks it optional |
| Async runtime | Runtime-agnostic core; tokio adapter behind the `tokio` cargo feature (default) | Owner's choice |
| Scenario execution | cucumber-rs; `.feature` files under `tests/features/` are verbatim copies of the Gherkin in this file | Scenario text is the test; drift is checked mechanically |
| Real server for @A/@B | testcontainers-rs, image `centrifugo/centrifugo:v6.10.1` | `cargo test` needs only Docker |
| Protobuf codegen | prost, `client.proto` vendored at a pinned upstream commit, protoc from `protoc-bin-vendored` | No system protoc needed |
| Crate layout | Single crate `ferrifuge`, edition 2024, license 0BSD | Split only when a second crate is justified |
| Core design | Sans-IO: the core is a state machine turning inputs (API calls, frames, transport open/close, timer fired) into actions (open/close transport, send, set timer, emit event); the `tokio` adapter performs the actions | Runtime-agnostic without async traits; @E tests drive the clock directly |
| Events API | `client.events()` returns a `futures::Stream` of a `ClientEvent` enum; `client.state()` returns the current state | Idiomatic async Rust, runtime-neutral |
| Code hygiene | Lint levels in `Cargo.toml` `[lints]`: no `unsafe`, clippy pedantic, every public item documented; CI denies warnings | Quality gates hold from the first line of SDK code |
| Dependency policy | `deny.toml` (cargo-deny): RustSec advisories, permissive licenses only, crates.io only | New dependencies must pass it; an exception needs a reason in `deny.toml` |
| Coverage | cargo-llvm-cov, line floor 70% on the library | Mirrors the owner's JVM projects |
| TLS crypto provider | Chosen at build time by cargo features: `ring` (a default feature), `aws-lc-rs`, or `fips` (aws-lc-rs in FIPS mode). A non-ring build disables default features, so ring is not linked; when several are enabled the strongest wins (`fips` > `aws-lc-rs` > `ring`) | FedRAMP requires FIPS 140-validated cryptography (aws-lc-rs has a FIPS mode, ring has none); ring builds with a C compiler alone, the FIPS build also needs CMake and Go |
| HTTP client (http_stream, sse) | reqwest without default features, `rustls-no-provider` and `stream`, given the client's own TLS configuration | Less code than hyper; no SDK precedent (only centrifuge-js has these transports); keeps the crypto provider ours |
| Publication payload type | Raw bytes (`Vec<u8>`): raw JSON text with the JSON protocol, any bytes with Protobuf | Every official SDK uses bytes (Go, Java, Swift, Dart) and so does tokio-centrifuge |
| Protocol selection | A `Config` option, JSON by default, fixed when the client is created | Every SDK fixes it at creation (Go constructors, centrifuge-js builds); tokio-centrifuge uses a config enum |
| Request/response API | `async fn` returning `Result` | Every SDK returns a result or an error (Go, Dart, Swift, tokio-centrifuge) |

### References

- Spec: https://centrifugal.dev/docs/transports/client_api
- Protocol framing: https://centrifugal.dev/docs/transports/client_protocol
- Proto schema: https://github.com/centrifugal/protocol/blob/master/definitions/client.proto
- Implementation reference: https://github.com/centrifugal/centrifuge-js
- Fake-server prior art: `fakeserver_test.go` in https://github.com/centrifugal/centrifuge-go
- Server API (used by tests): https://centrifugal.dev/docs/server/server_api
- Proxy protocol (used by tests): https://centrifugal.dev/docs/server/proxy

Precedence on conflict: spec > `client.proto` > centrifuge-js behaviour. A centrifuge-js behaviour that contradicts the spec is not ported; report it.

### Out of scope (v1)

- sockjs, webtransport transports
- map subscriptions, shared poll subscriptions (experimental)
- client publish debouncing (Centrifugo PRO)
- browser-only options: online/offline network events, fetch/ReadableStream/EventSource shims
- `closed` state

### Test layers

| Tag | Layer | Use for |
|---|---|---|
| @A | Real Centrifugo server, config variants, server HTTP API driven by the test | Behaviour reproducible with a real server |
| @B | @A plus an HTTP proxy backend owned by the test (connect/subscribe/publish/rpc/refresh/sub_refresh proxy) | Server responses the test must control: custom errors with `temporary` flag, custom disconnects, delayed replies, inspecting connect request fields |
| @D | Scripted fake server speaking the client protocol | Server behaviour Centrifugo cannot be made to produce: unsubscribe push codes, missing pings, malformed/unexpected frames, frame-level assertions |
| @E | Unit tests with a controlled clock, no network | Timing: backoff and jitter bounds, refresh retry delays |

A scenario tagged with several layers must pass in each of them.

**Exceptions introduced:** none.

---

## Block 1 — Client connection states

**Goal:** client state machine `disconnected → connecting → connected` and its events.

**Dependencies:** Block 0.

**Out of scope:** `closed` state; reconnect timing (Block 2); disconnect code classification (Block 3).

**Owner decision (resolves the out-of-scope note above):** the scenarios below cannot pass without a reconnect delay and without classifying disconnect codes. Block 1 therefore implements the spec's classification rule (Block 3) and the verified backoff formula (Block 2), the latter with unit tests; Blocks 2 and 3 add their scenarios against that code.

**Transport and codec in Block 1:** websocket with the JSON codec. http_stream, sse and Protobuf come in later blocks.

**Sub-increments** (one branch and PR each):

| Id | Value clause | Scenarios |
|---|---|---|
| 1a | As an app developer, I want to connect and disconnect a client and observe its state, so that I can drive and react to the connection lifecycle | New client starts disconnected; Successful connect; Explicit disconnect stops reconnection; Connect after explicit disconnect; Connect failures not reflected in state are reported as errors |
| 1b | As an app developer, I want the client to recover from server-initiated disconnects, so that reconnect advice needs no code of mine | Temporary connection loss then successful reconnect; Connection loss then terminal error on reconnect |
| 1c | As an app developer, I want an expiring connection token refreshed, and the client to stop when my callback denies it, so that revoked users are disconnected | Terminal condition while connected |

```gherkin
Feature: Client connection states

  Scenario: New client starts disconnected
    Given a new client
    Then the client state is "disconnected"

  @A
  Scenario: Successful connect
    Given a new client
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"
    And the client state is "connected"

  @A
  Scenario: Temporary connection loss then successful reconnect
    Given a connected client
    When the server disconnects the client with a reconnect-advised code
    Then the client emits "connecting" with that code and reason
    And then the client emits "connected"

  @B
  Scenario: Connection loss then terminal error on reconnect
    Given a connected client
    And the connect proxy will answer the next connect with a terminal disconnect code
    When the server disconnects the client with a reconnect-advised code
    Then the client emits "connecting"
    And then the client emits "disconnected" with the terminal code and reason
    And no further connect attempt is made

  @A
  Scenario: Terminal condition while connected
    Given a connected client using a connection token with expiration
    When the token callback signals "unauthorized" during refresh
    Then the client emits "disconnected"
    And no further connect attempt is made

  @A
  Scenario: Explicit disconnect stops reconnection
    Given a connected client
    When disconnect is called
    Then the client emits "disconnected"
    And no further connect attempt is made

  @A
  Scenario: Connect after explicit disconnect
    Given a client disconnected by an explicit disconnect call
    When connect is called
    Then the client reaches state "connected"

  Scenario: Connect failures not reflected in state are reported as errors
    Given a client configured with an unreachable endpoint
    When connect is called
    Then the client state is "connecting"
    And the client emits an "error" event for each failed transport attempt
```

**Exceptions introduced:** none.

---

## Block 2 — Reconnect backoff

**Goal:** reconnect delays follow exponential backoff with full jitter, bounded by configurable min and max.

**Dependencies:** Block 1.

**Out of scope:** resubscribe backoff (later block, same rules with its own options).

Defaults (from centrifuge-js README): `minReconnectDelay` 500 ms, `maxReconnectDelay` 20000 ms.

`VERIFY:` exact delay formula, whether the attempt counter resets after a successful connect, and whether the first attempt is immediate. Source: centrifuge-js reconnect/backoff implementation. Encode the verified formula as additional @E scenarios before implementing.

Verified (centrifuge-js `163dcba`, `src/utils.ts` `backoff`, `src/centrifuge.ts` `_getReconnectDelay`, `_startConnecting`, `_connectResponse`):
- delay for attempt `n` (0-based, `n` capped at 31) = `min(max, min + rand(0, min(max, min * 2^n)))`, so a delay is never below `min`;
- the attempt counter increments each time a delay is computed and resets to 0 when a connect reply arrives over websocket;
- the first attempt after `connect()` is immediate; delays apply to reconnects.

```gherkin
Feature: Reconnect backoff

  @E
  Scenario: Delays never exceed the maximum
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When 50 consecutive reconnect attempts fail
    Then every computed delay is at most 20000 ms

  @E
  Scenario: Delay upper bound grows exponentially until capped
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When consecutive reconnect attempts fail
    Then the upper bound of the delay for attempt n+1 is greater than for attempt n
    And the upper bound stops growing once it reaches 20000 ms

  @E
  Scenario Outline: Delay upper bound per attempt
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When the delay for attempt <attempt> is computed 1000 times
    Then every computed delay is at most <bound> ms

    Examples:
      | attempt | bound |
      | 0       | 1000  |
      | 1       | 1500  |
      | 2       | 2500  |
      | 3       | 4500  |
      | 4       | 8500  |
      | 5       | 16500 |
      | 6       | 20000 |

  @E
  Scenario: Delays are jittered
    Given a client with fixed backoff options
    When the delay for the same attempt number is computed 1000 times
    Then the computed delays are not all equal

  @E
  Scenario: Backoff options are configurable
    Given a client with minReconnectDelay 100 ms and maxReconnectDelay 1000 ms
    When 50 consecutive reconnect attempts fail
    Then every computed delay is at most 1000 ms
```

**Exceptions introduced:** none.

---

## Block 3 — Disconnect codes

**Goal:** classify server disconnect codes into reconnect vs terminal.

**Dependencies:** Blocks 1, 2.

**Out of scope:** client-side disconnect code values (codes < 3000).

Rule (spec): codes 3000–3499 and 4000–4499 → reconnect (`connecting`); every other server code → `disconnected`.

**Owner decision (1b review):** the spec's "other codes" covers custom codes only. Codes ≥ 5000 are reserved by Centrifuge and reconnect, as `client.proto` (`Disconnect`) and centrifuge-js specify. Effective rule: 3500–3999 and 4500–4999 → `disconnected`; every other server code (3000–3499, 4000–4499, ≥ 5000) → reconnect.

`VERIFY:` the server HTTP API `disconnect` method accepts custom `code` and `reason`; whether it accepts codes outside 4000–4999 is not confirmed. Codes 3000–3999 are therefore tested on @D. Client-side code values (< 3000): take from centrifuge-js source.

Verified (Centrifugo v6.10.1, raw websocket client, while implementing 1b):
- the server API `disconnect` accepts a custom code and reason and also works for anonymous connections (`"user": ""`, optionally narrowed by `"client"`);
- over websocket the disconnect arrives as a close frame carrying the code and the reason as plain text (not JSON, not a Disconnect push); observed for 3001, 3999, 4000 and 4999, so the @D-only rationale above no longer holds for codes 3000–3999 (tags left unchanged: owner decision);
- codes 2999 and 5000 are accepted by the API but are not valid websocket close codes: the client library reports `1002 Protocol violation` instead;
- a connect proxy answering `{"disconnect": {"code", "reason"}}` makes the server open the websocket and then close it with that code and reason, before any connect reply.

```gherkin
Feature: Disconnect codes

  @A
  Scenario Outline: Application disconnect codes sent via server API
    Given a connected client
    When the server API disconnects the client with code <code>
    Then the client goes to state "<state>"
    And the event carries code <code> and the reason sent

    Examples:
      | code | state        |
      | 4000 | connecting   |
      | 4499 | connecting   |
      | 4500 | disconnected |
      | 4999 | disconnected |

  @D
  Scenario Outline: Protocol-range disconnect codes
    Given a client connected to the fake server
    When the fake server disconnects the client with code <code>
    Then the client goes to state "<state>"

    Examples:
      | code | state        |
      | 3000 | connecting   |
      | 3499 | connecting   |
      | 3500 | disconnected |
      | 3999 | disconnected |

  @B
  Scenario Outline: Disconnect returned by the connect proxy
    Given the connect proxy answers with disconnect code <code>
    When connect is called
    Then the client goes to state "<state>"

    Examples:
      | code | state        |
      | 4000 | connecting   |
      | 4500 | disconnected |
```

**Exceptions introduced:** none.

---

## Block 4 — Ping/pong

**Goal:** answer server pings; detect a broken connection when pings stop.

**Dependencies:** Blocks 1, 3.

**Out of scope:** client-initiated pings.

Rule (spec): the client answers server PINGs with PONGs; if no PING arrives within ping interval + `maxServerPingDelay`, the connection is considered broken and re-established. Default `maxServerPingDelay` (centrifuge-js): 10000 ms.

`VERIFY:` where the ping interval and the "pong expected" flag are communicated (connect reply fields in `client.proto`).

Verified (`client.proto` at the pinned commit; centrifuge-js `163dcba` agrees): `ConnectResult.ping` is the interval in seconds (0: no pings) and `ConnectResult.pong` whether the client must answer; a ping is an empty `Reply` (`{}` in JSON) and a pong an empty `Command` (`{}`). centrifuge-js re-arms the no-ping deadline on any data from the server, and `maxServerPingDelay` 0 disables the check. Centrifugo v6.10.1 defaults: ping 25 s, pong expected.

```gherkin
Feature: Ping/pong

  @D
  Scenario: Client answers ping with pong
    Given a client connected to the fake server that announces pong is expected
    When the fake server sends a ping
    Then the client sends a pong

  @D
  Scenario: Client does not answer when pong is not expected
    Given a client connected to the fake server that announces pong is not expected
    When the fake server sends a ping
    Then the client sends no pong

  @D
  Scenario: Missing pings trigger reconnect
    Given a client with maxServerPingDelay 1000 ms
    And connected to the fake server announcing a ping interval of 2 s
    When the fake server stops sending pings
    Then within 3 s plus tolerance the client emits "connecting"
    And the client opens a new connection

  @D
  Scenario: Regular pings keep the connection alive
    Given a client with maxServerPingDelay 1000 ms
    And connected to the fake server pinging every 2 s
    When 10 s elapse
    Then the client stays "connected"
```

**Exceptions introduced:** none.

---

## Block 5 — SOLID refactorings

**Goal:** apply the findings of the SOLID review of `src/` (solidifier skill, `conservative` rigor) so that later blocks (subscriptions, Protobuf, http_stream/sse, more commands) extend the code instead of editing it. Observable behaviour does not change.

**Dependencies:** Blocks 1–4.

**Out of scope:** the features the seams prepare for (Protobuf encoding, http_stream and sse, subscriptions): later blocks. Review finding #1 (one place per timer kind) is already done (PR #16).

**Rules for every sub-increment:**
- every scenario of Blocks 1–4 keeps passing unchanged;
- no observable behaviour change; public API changes only where the sub-increment says so;
- no abstraction without a second, decided use (a Block 0 decision or a later block); for a closed set, an enum rather than a trait.

**Sub-increments** (one branch and PR each, in this order):

| Id | Value clause | Review finding | Structural acceptance |
|---|---|---|---|
| 5a | As a maintainer, I want each JSON command to set only its own request field, so that adding a command touches one place | #5, `json.rs` command encoding | `JsonCommand` derives `Default`; each command sets its id and its own field only; codec unit tests cover every command |
| 5b | As a maintainer, I want connection token handling in its own component, so that subscription tokens can reuse it instead of duplicating it | #2, `machine.rs` token fields and methods | A crate-private token component owns the current and pending token, the token fetch, the refresh command and its timers' decisions; `Machine` delegates to it and keeps no token field of its own; scenario "Token refreshed before expiry keeps the connection" |
| 5c | As a maintainer, I want the codec chosen in one place, so that Protobuf (Block 0 decision) is added as a second variant | #3, direct `json::` calls in `machine.rs` | A crate-private `Codec` enum (only `Json` until the Protobuf block) encodes commands and decodes frames; `machine.rs` calls no codec module directly; the frame type (text for JSON, binary for Protobuf) travels with each frame to the owner (`Action::Send` carries it, public API change), so the driver no longer assumes UTF-8 text; scenario "The client speaks JSON by default" |
| 5d | As a maintainer, I want the transport kind to be data, so that http_stream and sse (Block 0 decision) are added as variants | #4, transport name constant in `machine.rs`, `WebSocket` hard-wired in the tokio driver | A `TransportKind` enum (only `WebSocket` until the transports block), `#[non_exhaustive]` so that adding http_stream and sse breaks no owner's `match`, names the transport in events and selects the driver's transport; `Action::OpenTransport` carries the kind (public API change); scenario "The connected event names the transport" |

The scenarios below guard behaviour the refactorings move but no earlier scenario pins down. They pass before the refactoring; they must still pass after it.

```gherkin
Feature: SOLID refactorings

  @A
  Scenario: Token refreshed before expiry keeps the connection
    Given a connected client using a connection token with expiration
    When 6 s elapse
    Then the client stays "connected"

  @D
  Scenario: The client speaks JSON by default
    Given a client connected to the fake server
    Then the fake server received the connect command as JSON

  @A
  Scenario: The connected event names the transport
    Given a new client
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "websocket"
```

For "Token refreshed before expiry keeps the connection" the @A server closes expired connections after a short delay, so that a client which failed to refresh would be disconnected within the scenario. "the client stays" fails on any event during the elapsed interval and on any new transport connection (relay count), so a disconnect followed by a quick reconnect inside the interval does not pass. `VERIFY:` the Centrifugo v6 option for that delay (`client.expired_close_delay`, default 25 s) and its environment variable.

Verified (Centrifugo v6.10.1): `CENTRIFUGO_CLIENT_EXPIRED_CLOSE_DELAY=1s` makes the server close a connection whose 3 s token was never refreshed at 4 s, with `3005 connection expired`.

**Exceptions introduced:** 5a adds no scenario: the JSON codec is crate-private and its command encoding is not observable through the public API beyond what Blocks 1–4 already exercise; codec unit tests cover it.

---

## Block 6 — TLS crypto provider

**Goal:** the cryptography used for `wss://` (and later HTTPS) is chosen at build time, so that a FedRAMP deployment can build with FIPS-validated cryptography and others avoid the FIPS build's extra toolchain (ring needs only a C compiler; aws-lc's FIPS mode also needs CMake and Go).

**Dependencies:** Block 1 (websocket transport).

**Out of scope:** client certificates; custom root certificates; HTTPS, which uses the same configuration once Block 8 adds it.

**Decisions:** cargo features `ring` (in `default`), `aws-lc-rs`, and `fips` (implies `aws-lc-rs`). Cargo unifies features, so enabling `aws-lc-rs` or `fips` does not remove a default `ring`: a build without ring sets `default-features = false` (and re-enables `tokio`). Features stay additive: when several providers are enabled the strongest wins at run time (`fips` > `aws-lc-rs` > `ring`), never a compile error; the `tokio` adapter (the only TLS user) fails to compile without a provider feature, with a message naming the three; the runtime-agnostic core (`--no-default-features`) needs none and keeps compiling (Block 0).

`VERIFY:` the build requirements of aws-lc-rs and of its FIPS mode (C toolchain, CMake, Go) on the CI runners; and that a FIPS build reports FIPS mode at run time (aws-lc-rs API).

Value clause: As an app developer deploying under FedRAMP, I want to build the client with FIPS-validated cryptography, so that its TLS connections meet the FIPS 140 requirement.

**Structural acceptance:** the TLS configuration is built from the selected provider in one place; the default build links only ring, and `--no-default-features --features tokio,aws-lc-rs` (or `tokio,fips`) links only aws-lc (`cargo tree`); CI builds and tests those three feature sets, installing CMake and Go explicitly for the aws-lc ones, and runs the encrypted scenarios below (and Block 8's HTTPS one) in each; a unit test per build reports the provider in use, and for `fips` the TLS client configuration the transports actually use reports `fips() == true` (rustls checks TLS-level requirements there, beyond the provider's primitives).

The test server's certificate is issued by a test CA that the client trusts through `SSL_CERT_FILE`, which `rustls-native-certs` reads (verified in its source); no custom-root API is added.

```gherkin
Feature: TLS crypto provider

  @A
  Scenario: Secure websocket connection with the build's provider
    Given a server serving TLS with a certificate from the test CA
    And a client configured with the server's wss endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"
```

**Exceptions introduced:** the provider choice itself is a build configuration, verified by the CI feature matrix (each build runs the scenario above) and its unit tests rather than by a scenario per provider.

---

## Block 7 — Protobuf encoding

**Goal:** the client can use the Protobuf encoding of the client protocol instead of JSON.

**Dependencies:** Block 5 (5c codec seam, frame type).

**Out of scope:** Protobuf over http_stream and sse (Block 8 covers them with JSON); payload serialization (publication data stays bytes).

**Decisions:** `Config::protocol`, an enum `Protocol { Json, Protobuf }`, default `Json` (Block 0). Over websocket the client requests the `centrifuge-protobuf` subprotocol (verified in centrifuge-js and centrifuge-go); several messages in one frame are varint length-delimited (`client.proto`).

`VERIFY:` what Centrifugo v6 does when Protobuf is requested and its configuration does not allow it; encode the observed behaviour as a scenario before implementing.

Value clause: As an app developer, I want to use the Protobuf encoding, so that I can send and receive binary payloads (centrifuge-js README: "if you want to send binary payloads").

```gherkin
Feature: Protobuf encoding

  @A
  Scenario: Connect with Protobuf
    Given a new client configured with the Protobuf protocol
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @D
  Scenario: Frames are binary Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server
    Then the fake server received the connect command as binary Protobuf

  @D
  Scenario: Ping answered in Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server that announces pong is expected
    When the fake server sends a Protobuf ping
    Then the client sends a Protobuf pong

  @D
  Scenario: Every message of a multi-message Protobuf frame is handled
    Given a client configured with the Protobuf protocol connected to the fake server that announces pong is expected
    When the fake server sends two Protobuf pings in one frame
    Then the client sends two Protobuf pongs

  @D
  Scenario: Malformed Protobuf frame forces a reconnect
    Given a client configured with the Protobuf protocol connected to the fake server
    When the fake server sends a binary frame that is not valid Protobuf
    Then the client emits "connecting" with code 1
```

**Exceptions introduced:** none.

---

## Block 8 — HTTP-streaming transports and fallback

**Goal:** when websocket cannot connect, the client falls back to http_stream, then to sse (Block 0 order).

**Dependencies:** Blocks 5 (5d transport kind), 6.

**Out of scope:** sockjs, webtransport, unidirectional http_stream and sse (Block 0); Protobuf over these transports (rejected, see decisions); browser-only shims.

**Decisions** (documented in the centrifuge-js README unless noted):
- the client takes a list of (transport, endpoint); a single endpoint string means websocket only;
- transports are tried in order during the initial handshake only, until one succeeds; reconnects then use only that transport;
- reqwest as the HTTP client (Block 0); cargo-deny checks only the targets the crate supports (Linux, macOS, Windows), so data crates used only on wasm32 are not in scope;
- a client configured with the Protobuf protocol and an http_stream or sse endpoint is rejected when it is built, with a configuration error naming the combination, rather than failing at run time.

`VERIFY:` the bidirectional emulation protocol (stream request, `/emulation` command requests and their session and node fields) in centrifuge-js `transport_http_stream.ts` and `transport_sse.ts`; there is no other implementation to cross-check against. Centrifugo v6 defaults verified: `http_stream.enabled` and `sse.enabled` are false, the emulation handler prefix is `/emulation`.

Value clause: As an app developer, I want the client to fall back from websocket to http_stream and then sse, so that it connects in networks where websocket is blocked.

```gherkin
Feature: Transport fallback

  @A
  Scenario: Fallback to http_stream when websocket is unreachable
    Given a server with http_stream and sse enabled
    And a client configured with an unreachable websocket endpoint and the server's http_stream and sse endpoints
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "http_stream"

  @A
  Scenario: Fallback over HTTPS uses the build's TLS provider
    Given a server serving TLS with a certificate from the test CA, with http_stream enabled
    And a client configured with an unreachable websocket endpoint and the server's https http_stream endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "http_stream"

  @A
  Scenario: Fallback to sse when websocket and http_stream are unreachable
    Given a server with http_stream and sse enabled
    And a client configured with unreachable websocket and http_stream endpoints and the server's sse endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "sse"

  @A
  Scenario: A transport that worked is kept on reconnect
    Given a client connected over transport "http_stream"
    When the server disconnects the client with a reconnect-advised code
    Then the client emits "connecting" with that code and reason
    And then the client emits "connected" over transport "http_stream"

  @A
  Scenario: Commands work over an emulated transport
    Given a client connected over transport "sse" using a connection token with expiration
    When 6 s elapse
    Then the client stays "connected"

  Scenario: Every transport unreachable
    Given a client configured with unreachable websocket, http_stream and sse endpoints
    When connect is called
    Then the client state is "connecting"
    And the client emits an "error" event for each failed transport attempt

  Scenario: Protobuf with an HTTP transport is rejected
    Given client options with the Protobuf protocol and an http_stream endpoint
    When the client is built
    Then building fails with a configuration error naming Protobuf and http_stream
```

**Exceptions introduced:** none.

---

## Block 9 — Client-side subscriptions

**Goal:** subscriptions with the states `unsubscribed`, `subscribing` and `subscribed`, a client registry, publications, and automatic resubscribe after a reconnect (spec, "Subscription states", "Subscription management", "Listen to channel publications").

**Dependencies:** Blocks 1–5.

**Out of scope:** subscription tokens, server unsubscribe codes, resubscribe backoff (Block 10); recovery and `getState` (Block 11); server-side subscriptions (Block 12); publish, history, presence (Block 13); join and leave pushes; map and shared-poll subscriptions (Block 0).

**Decisions:** a `Subscription` handle with its own `state()` and `events()` stream, like `Client` (spec: subscriptions have their own states and events); creating a second subscription to a channel already in the registry fails (spec: "`newSubscription` can throw"; centrifuge-js: "already exists"); the registry offers lookup by channel, the list of subscriptions, and removal, which unsubscribes first (spec, "Subscription management"); the subscription `data` option is attached to every subscribe and resubscribe request (spec, "Subscription common options"); `subscribe()` before `connect()` is allowed (spec); publication data is bytes (Block 0), so with Protobuf any bytes round-trip, including non-UTF-8 (Block 7's value, testable once publications exist).

`VERIFY:` the Centrifugo v6 channel options letting an anonymous client subscribe to channels without a namespace (`channel.without_namespace` permissions), and the server API `publish` request shape, including `b64data` for binary data.

Value clause: As an app developer, I want to subscribe to a channel and receive its publications, so that they keep arriving across reconnects without code of mine.

```gherkin
Feature: Client-side subscriptions

  Scenario: New subscription starts unsubscribed
    Given a new client
    When a subscription to channel "news" is created
    Then the subscription state is "unsubscribed"

  @A
  Scenario: Subscribe while connected
    Given a connected client
    When a subscription to channel "news" is created and subscribed
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed"

  @A
  Scenario: Subscribe before connect
    Given a new client
    When a subscription to channel "news" is created and subscribed
    Then the subscription state is "subscribing"
    And the subscription emits "subscribing"
    When connect is called
    Then the subscription reaches state "subscribed"

  @A
  Scenario: Publication delivered
    Given a client subscribed to channel "news"
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

  @A
  Scenario: Resubscribe after reconnect
    Given a client subscribed to channel "news"
    When the server disconnects the client with a reconnect-advised code
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed"

  @A
  Scenario: Unsubscribe stops publications
    Given a client subscribed to channel "news"
    When unsubscribe is called on the subscription
    Then the subscription emits "unsubscribed" with code 0
    And a publication to channel "news" is not received

  Scenario: Duplicate subscription is refused
    Given a new client with a subscription to channel "news"
    When a second subscription to channel "news" is created
    Then creating it fails

  @B
  Scenario: Permanent subscribe error
    Given the subscribe proxy answers channel "news" with error code 403
    When a subscription to channel "news" is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 403

  @B
  Scenario: Subscription data is sent on subscribe and on resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with subscription data {"room":1}
    When the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"room":1} on both subscribe requests

  Scenario: Subscriptions are found by channel and listed
    Given a new client with subscriptions to channels "news" and "sport"
    Then looking up channel "news" returns its subscription
    And looking up channel "weather" returns none
    And the client lists the subscriptions to "news" and "sport"

  @A
  Scenario: Removing a subscription unsubscribes it first
    Given a client subscribed to channel "news"
    When the subscription is removed from the client
    Then the subscription emits "unsubscribed" with code 0
    And looking up channel "news" returns none

  @A
  Scenario: Binary publication delivered over Protobuf
    Given a client configured with the Protobuf protocol subscribed to channel "news"
    When the server API publishes the bytes ff 00 fe to channel "news"
    Then the subscription receives a publication with the bytes ff 00 fe
```

**Exceptions introduced:** none.

---

## Block 10 — Subscription tokens, unsubscribe codes, resubscribe backoff

**Goal:** private channels stay authorized through subscription tokens, server unsubscribe codes are honoured, and resubscribes follow their own backoff (spec, "Subscription token", "Unsubscribe codes"; Block 2: "resubscribe backoff, later block, same rules with its own options").

**Dependencies:** Blocks 5 (5b token component), 9.

**Out of scope:** recovery (Block 11); connection tokens (Block 1).

**Decisions:** each subscription owns a token component (5b); the subscription token callback receives the channel (centrifuge-js `SubscriptionTokenContext`); expiring tokens are refreshed with `sub_refresh`; server unsubscribe codes from 2500 resubscribe, below 2500 unsubscribe (spec); resubscribe backoff defaults 500 ms and 20000 ms (centrifuge-js), same formula as Block 2, and the attempt counter resets when the subscription reaches `subscribed` (centrifuge-js); a failing subscription token callback or a temporary `sub_refresh` error emits an `error` event and retries the refresh after 5 to 10 s, as for connection tokens (centrifuge-js `subscription.ts`).

`VERIFY:` whether the server API `unsubscribe` accepts a code (if not, the code table runs on @D only, as decided); the client-side unsubscribed code for a denied token (centrifuge-js `unsubscribedCodes`).

Value clause: As an app developer, I want subscription tokens refreshed and server unsubscribe codes honoured, so that private channels stay authorized only while my application allows it.

```gherkin
Feature: Subscription tokens, unsubscribe codes and resubscribe backoff

  @D
  Scenario Outline: Server unsubscribe codes
    Given a client subscribed to channel "news" on the fake server
    When the fake server unsubscribes the subscription with code <code>
    Then the subscription goes to state "<state>"

    Examples:
      | code | state        |
      | 2000 | unsubscribed |
      | 2499 | unsubscribed |
      | 2500 | subscribing  |
      | 2999 | subscribing  |

  @A
  Scenario: Subscription token refreshed before expiry keeps the subscription
    Given a client subscribed to a private channel using a subscription token with expiration
    When 6 s elapse
    Then the subscription stays "subscribed"

  @E
  Scenario: A failing subscription token callback is retried
    Given a subscribed subscription whose token expires and whose token callback fails once
    When the subscription refresh is due
    Then the subscription emits an "error" event
    And the refresh is retried after 5 to 10 s

  @E
  Scenario: A temporary sub_refresh error is retried
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with a temporary error
    Then the subscription emits an "error" event
    And the refresh is retried after 5 to 10 s

  @A
  Scenario: Each subscription token callback receives its own channel
    Given a connected client with subscriptions to private channels "a" and "b" using one token callback
    When both subscriptions are subscribed
    Then the token callback was called once for channel "a" and once for channel "b"

  @A
  Scenario: Subscription token denied
    Given a client subscribed to a private channel using a subscription token with expiration
    When the subscription token callback signals "unauthorized" during refresh
    Then the subscription emits "unsubscribed"

  @E
  Scenario: Resubscribe delays are bounded
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When 50 consecutive subscribe attempts fail with a temporary error
    Then every computed delay is at most 20000 ms

  @E
  Scenario: Resubscribe delay upper bound grows exponentially until capped
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When consecutive subscribe attempts fail with a temporary error
    Then the upper bound of the delay for attempt n+1 is greater than for attempt n
    And the upper bound stops growing once it reaches 20000 ms

  @E
  Scenario: Resubscribe delays are jittered
    Given a subscription with fixed resubscribe backoff options
    When the resubscribe delay for the same attempt number is computed 1000 times
    Then the computed delays are not all equal

  @E
  Scenario: A successful subscribe resets the resubscribe backoff
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When 10 consecutive subscribe attempts fail with a temporary error
    And the next subscribe attempt succeeds
    And a later subscribe attempt fails with a temporary error
    Then the delay after that failure is at most 1000 ms

  @E
  Scenario: Resubscribe backoff options are configurable
    Given a subscription with minResubscribeDelay 100 ms and maxResubscribeDelay 1000 ms
    When 50 consecutive subscribe attempts fail with a temporary error
    Then every computed delay is at most 1000 ms
```

The resubscribe steps apply the same checks as Block 2's: each delay is at least the minimum and within its attempt's bound, the bound must reach the maximum exactly, and jitter must cover the whole range.

**Exceptions introduced:** none.

---

## Block 11 — Recovery and getState

**Goal:** missed publications are recovered on resubscribe, and with `getState` a failed recovery reloads the application's state (spec, "Subscription recovery state", "Subscription getState").

**Dependencies:** Blocks 9, 10.

**Out of scope:** delta compression; map and shared-poll subscriptions (Block 0); server-side subscriptions (Block 12).

**Decisions:** the `subscribed` event carries `was_recovering` and `recovered` (spec); `unsubscribe()` keeps the stream position (spec); the `since` option starts recovery on the first subscribe (spec); `getState` is called before the initial subscribe, whose stream position it provides, and again when recovery fails (spec); a failing `getState` emits an `error` event and is retried with the resubscribe backoff before subscribing (Centrifugo blog "App-owned state with stream subscriptions", 2026-07-27); unsubscribe code 2502 "state invalidated" drops the subscription token and the stream position, then resubscribes: with a token callback a new token is fetched first, without one the resubscribe carries no token (`client.proto`, `Unsubscribe`: "client must drop its subscription token and any state accumulated"; it outranks centrifuge-js, which keeps the token when there is no callback).

`VERIFY:` the Centrifugo v6 channel options for history and recovery, and how a test makes history expire; `getState` in centrifuge-js `163dcba`.

Value clause: As an app developer, I want missed publications recovered after a reconnect, or my state reloaded when recovery fails, so that my application sees no gap in a channel's stream.

```gherkin
Feature: Subscription recovery

  @A
  Scenario: A first subscribe does not recover
    Given a connected client
    When a subscription to a channel with history and recovery is subscribed
    Then the subscription emits "subscribed" with was_recovering false and recovered false

  @A
  Scenario: Missed publications are recovered
    Given a client subscribed to a channel with history and recovery
    And the client is disconnected with a reconnect-advised code
    When 2 publications are made to the channel while the client reconnects
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly those 2 publications in order

  @A
  Scenario: Publications between unsubscribe and subscribe are recovered
    Given a client subscribed to a channel with history and recovery
    When unsubscribe is called on the subscription
    And 2 publications are made to the channel
    And subscribe is called on the subscription again
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly those 2 publications in order

  @A
  Scenario: The since option recovers on the first subscribe
    Given a channel with history and recovery holding 3 publications
    And a subscription to the channel with the since option at the position of the first one
    When the subscription is subscribed on a connected client
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly the 2 later publications in order

  @A
  Scenario: Recovery fails when history is gone
    Given a client subscribed to a channel with history and recovery
    And the channel history expires while the client is disconnected
    When the client resubscribes
    Then the subscription emits "subscribed" with was_recovering true and recovered false

  @A
  Scenario: getState provides the position of the initial subscribe
    Given a subscription with a getState callback to a channel with history and recovery
    When the subscription is subscribed on a connected client
    Then the getState callback is called once before the subscribe command
    And the subscribe command carries the stream position getState returned

  @A
  Scenario: getState reloads state when recovery fails
    Given a client subscribed with a getState callback to a channel with history and recovery
    And the channel history expires while the client is disconnected
    When the client resubscribes
    Then the getState callback is called a second time
    And the next subscribe command carries the stream position it returned
    And the subscription emits "subscribed"

  @E
  Scenario: A failing getState is retried before subscribing
    Given a subscription with a getState callback that fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And getState is called again after the resubscribe backoff delay
    And the subscribe command carries the stream position the second call returned

  @D
  Scenario: State invalidation fetches a new subscription token
    Given a client subscribed to channel "news" on the fake server using a subscription token callback
    When the fake server unsubscribes the subscription with code 2502
    Then the subscription token callback is called again
    And the subscription resubscribes with the new token

  @D
  Scenario: State invalidation drops a fixed subscription token
    Given a client subscribed to channel "news" on the fake server using a fixed subscription token
    When the fake server unsubscribes the subscription with code 2502
    Then the subscription resubscribes without a token and without a stream position
```

**Exceptions introduced:** none.

---

## Block 12 — Server-side subscriptions

**Goal:** the client tracks subscriptions made by the server at connect and reports their events (spec, "Server-side subscriptions").

**Dependencies:** Block 9.

**Out of scope:** client control over them (the spec gives none); top-level publish, history and presence (Block 13).

**Decisions:** server-side subscriptions live in a registry separate from client-side ones and emit their own events (spec); those granted at connect arrive in `ConnectResult.subs`, those added later in a `Subscribe` push, and both are tracked (`client.proto`); on reconnect the connect command carries each recoverable server-side subscription's position in `ConnectRequest.subs`, so missed publications are recovered (`client.proto`).

`VERIFY:` how a test makes Centrifugo v6 subscribe a connection on the server side (connect proxy result `channels`, or token `channels` claim), and the server API `subscribe` method for a live connection.

Value clause: As an app developer, I want publications of channels the server subscribed me to, so that server-managed subscriptions need no client code to keep.

```gherkin
Feature: Server-side subscriptions

  @B
  Scenario: Publications of a server-side subscription are delivered
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    When the server API publishes {"level":1} to channel "alerts"
    Then the client receives a server-side publication on channel "alerts" with data {"level":1}

  @B
  Scenario: Server-side subscriptions survive a reconnect
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    When the server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "subscribing" event for channel "alerts"
    And then the client emits a server-side "subscribed" event for channel "alerts"

  @B
  Scenario: Missed publications of a server-side subscription are recovered
    Given the connect proxy subscribes the connection to channel "alerts" with history and recovery
    And a connected client
    And the client is disconnected with a reconnect-advised code
    When 2 publications are made to channel "alerts" while the client reconnects
    Then the client receives exactly those 2 server-side publications in order

  @A
  Scenario: A subscription added by the server after connect is tracked
    Given a connected client
    When the server API subscribes the client to channel "alerts"
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And a publication to channel "alerts" is received as a server-side publication

  @A
  Scenario: A server-side subscription removed by the server is unsubscribed
    Given a connected client
    And the server API subscribes the client to channel "alerts"
    When the server API unsubscribes the client from channel "alerts"
    Then the client emits a server-side "unsubscribed" event for channel "alerts"

  @B
  Scenario: A server-side subscription missing after reconnect is unsubscribed
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    And the connect proxy will subscribe the next connection to no channel
    When the server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "unsubscribed" event for channel "alerts"
```

**Exceptions introduced:** none.

---

## Block 13 — Client commands

**Goal:** RPC, publish, send, history, presence and presence stats over the client's connection (spec, "Client methods", "Subscription methods", "RPC", "Channel history API", "Presence and presence stats API").

**Dependencies:** Blocks 9, 12.

**Out of scope:** publish debouncing (Block 0); batching several calls into one frame.

**Decisions:** `async fn` returning `Result` (Block 0); a call made while not connected waits for the connection, bounded by `Config::timeout`, and fails at once on `disconnect()` (centrifuge-js `_methodCall`); a command without a reply fails with the timeout error (code 1).

`VERIFY:` the Centrifugo v6 RPC proxy request and response shape, for the @B fixtures. `send` is tested on @D only: Centrifugo does not handle it (centrifuge-js README: "only valid for Centrifuge server library for Go and does not work for Centrifugo server"; no send proxy in v6, verified).

Value clause: As an app developer, I want to call RPC, publish, send, history and presence over the client's connection, so that request/response work needs no separate HTTP client.

```gherkin
Feature: Client commands

  @B
  Scenario: RPC returns the proxy's result
    Given a connected client and an RPC proxy answering method "echo" with its input
    When the client calls RPC "echo" with {"n":1}
    Then the call returns {"n":1}

  @A
  Scenario: Publish reaches subscribers
    Given a client subscribed to channel "news"
    When the client publishes {"text":"hi"} to channel "news"
    Then the subscription receives a publication with data {"text":"hi"}

  @A
  Scenario: History returns the last publications
    Given a channel with history holding 3 publications
    When a connected client requests the last 2 publications of the channel
    Then it receives those 2 publications with their offsets

  @B
  Scenario: Temporary RPC error is reported as such
    Given a connected client and an RPC proxy answering method "busy" with a temporary error
    When the client calls RPC "busy"
    Then the call fails with a temporary error

  @A
  Scenario: Subscription-level commands use the subscription's channel
    Given two clients subscribed to a channel with history and presence
    When the first client's subscription publishes {"text":"hi"}
    Then the second client's subscription receives a publication with data {"text":"hi"}
    And the first client's subscription history ends with that publication
    And the first client's subscription presence lists both clients
    And the first client's subscription presence stats count 2 clients

  @D
  Scenario: Concurrent calls get their own replies
    Given a client connected to the fake server that answers commands in reverse order
    When the client calls RPC "first" and RPC "second" concurrently
    Then the "first" call returns the reply to "first"
    And the "second" call returns the reply to "second"

  @D
  Scenario: A call without a reply times out
    Given a client connected to the fake server that never replies to commands
    When the client calls RPC "slow"
    Then the call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A call made while connecting waits for the connection
    Given an RPC proxy answering method "echo" with its input
    And a new client
    When connect is called
    And the client calls RPC "echo" before it is connected
    Then the call completes once the client is connected

  Scenario: A queued call fails after the timeout when no connection comes
    Given a client configured with an unreachable endpoint
    When connect is called
    And the client calls RPC "echo"
    Then the call fails with a timeout error within the configured timeout plus tolerance

  Scenario: Disconnect fails a queued call at once
    Given a client configured with an unreachable endpoint
    And connect is called
    And the client calls RPC "echo"
    When disconnect is called
    Then the call fails with a "disconnected" error

  @D
  Scenario: Send delivers a message without a reply
    Given a client connected to the fake server
    When the client sends {"note":"x"}
    Then the fake server received a send command with data {"note":"x"} and no command id

  @A
  Scenario: Presence lists the subscribed clients
    Given two clients subscribed to a channel with presence
    When one of them requests the channel's presence
    Then it receives both clients

  @A
  Scenario: Presence stats count clients and users
    Given two clients of different users subscribed to a channel with presence
    When one of them requests the channel's presence stats
    Then it receives 2 clients and 2 users
```

**Exceptions introduced:** none.
