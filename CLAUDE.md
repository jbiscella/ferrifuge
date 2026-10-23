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
| Dependency policy | `deny.toml` (cargo-deny): RustSec advisories, permissive licenses only, crates.io only; the OpenSSL license is allowed globally (owner decision: needed by aws-lc, no on/off variant to maintain) | New dependencies must pass it; an exception needs a reason in `deny.toml` |
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
| @P | Benchmark, allocation-counting and simulation harness, outside cucumber | Footprint, per-message cost and fleet behaviour. Deterministic metrics (instruction counts, allocation counts, live bytes, simulated time) gate CI; wall-clock metrics are measured and reported, never gating |

A scenario tagged with several layers must pass in each of them.

Cucumber skips `@P` scenarios. A test fails if a `@P` scenario name in `CLAUDE.md` has no matching benchmark or simulation, or if a benchmark claims a scenario name that does not exist.

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

**Decisions:** cargo features `ring` (in `default`), `aws-lc-rs`, and `fips` (implies `aws-lc-rs`). aws-lc-rs is not pinned (owner decision, Block 6 implementation): from 1.18 its `fips` feature builds AWS-LC-FIPS 4.x, submitted to NIST but not yet certified, while 1.17.x ships the FIPS 140-3 validated 3.x module (CMVP certificates #5314 static, #5298 dynamic), but pinning `<1.18` forces rustls `<= 0.23.43` and rustls-webpki `< 0.103.16` on every build (rustls 0.23.44+ and rustls-webpki 0.103.16 require aws-lc-rs 1.18), and those carry RUSTSEC-2026-0285 and RUSTSEC-2026-0346, fixed in rustls 0.23.45 and 0.23.46 only; a FIPS build therefore uses the 4.x module, and the README states that its certification is pending; revisit when 4.x is certified (the module check below then needs no change) or if rustls supports aws-lc-rs 1.17 again. The supported operating environments are those of the module's security policy. Cargo unifies features, so enabling `aws-lc-rs` or `fips` does not remove a default `ring`: a build without ring sets `default-features = false` (and re-enables `tokio`). Features stay additive: when several providers are enabled the strongest wins at run time (`fips` > `aws-lc-rs` > `ring`), never a compile error; the `tokio` adapter (the only TLS user) fails to compile without a provider feature, with a message naming the three; the runtime-agnostic core (`--no-default-features`) needs none and keeps compiling (Block 0).

`VERIFY:` the build requirements of aws-lc-rs and of its FIPS mode (C toolchain, CMake, Go) on the CI runners; and that a FIPS build reports FIPS mode at run time (aws-lc-rs API).

Value clause: As an app developer deploying under FedRAMP, I want to build the client with FIPS-validated cryptography, so that its TLS connections meet the FIPS 140 requirement.

**Structural acceptance:** the TLS configuration is built from the selected provider in one place; the default build links only ring, and `--no-default-features --features tokio,aws-lc-rs` (or `tokio,fips`) links only aws-lc (`cargo tree`); mixed builds keep the precedence: `--features aws-lc-rs` (ring still on by default) uses aws-lc-rs, `--features fips` uses FIPS mode, both verified by the per-build unit test; CI builds and tests those three feature sets, installing CMake and Go explicitly for the aws-lc ones, and runs the encrypted scenarios below (and Block 8's HTTPS one) in each; a unit test per build reports the provider in use, and for `fips` the TLS client configuration the transports actually use reports `fips() == true` (rustls checks TLS-level requirements there, beyond the provider's primitives) and the linked AWS-LC module is the 4.x FIPS generation (its reported version), so a dependency update cannot silently switch to another module generation; `deny.toml` allows the OpenSSL license (aws-lc crates: `ISC AND (Apache-2.0 OR ISC) AND OpenSSL`), and the README's license section notes that builds with `aws-lc-rs` or `fips` include OpenSSL-licensed code and its attribution requirement.

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

Verified (Centrifugo v6.10.1, configuration types and a raw websocket client, while implementing Block 7): no option disables the Protobuf encoding, so the case does not arise and needs no scenario; with the default configuration the server accepts the `centrifuge-protobuf` subprotocol, echoes it in the handshake, and answers a length-delimited Protobuf connect command in a binary frame.

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
- a client configured with the Protobuf protocol and an http_stream or sse endpoint is rejected when it is built, with a configuration error naming the combination, rather than failing at run time;
- the http_stream and sse decoders buffer across body chunks: network chunk boundaries are unrelated to message boundaries, so a message split over several chunks and several messages in one chunk both decode exactly; unit tests cover both cases for each transport (the real server's chunking in @A cannot be controlled);
- server pings are the same empty `Reply` as over websocket, and are answered with a pong, sent through the emulation endpoint, when `ConnectResult.pong` is set (`client.proto`, `ConnectResult.pong`; centrifuge-js `_handleServerPing`; verified: Centrifugo v6.10.1 sets `pong` on these transports and disconnects an unanswered client with 3012 "no pong"). The unanswered empty `Push` ping of `client.proto` belongs to the unidirectional transports, out of scope (Block 0);
- an attempt that opens but gets no connect reply within `Config::timeout` fails like a refused one, and the next transport is tried;
- within the first sequence the next transport is tried at once; once every transport has failed, the next sequence starts after the reconnect backoff delay (Block 2), as centrifuge-js counts only the first round as handshake;
- a failed emulation request (network error, rejection, or a 404 or 5xx response) closes the transport and the client reconnects, since the command it carried can no longer be answered (centrifuge-js `transport_sse.ts`).

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
  Scenario Outline: Fallback over HTTPS uses the build's TLS provider
    Given a server serving TLS with a certificate from the test CA, with http_stream and sse enabled
    And a client configured with an unreachable websocket endpoint and only the server's https <transport> endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "<transport>"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

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

  @B
  Scenario: A failed emulation request makes the client reconnect
    Given a client connected over transport "http_stream" through a proxy that rejects the next emulation request
    When the client sends a command
    Then the client emits "connecting" with code 1
    And then the client emits "connected" over transport "http_stream"

  @A
  Scenario: An HTTP stream that never answers the handshake falls back
    Given a server with http_stream and sse enabled, whose http_stream endpoint is behind a relay that accepts but never answers
    And a client configured with an unreachable websocket endpoint, the relayed http_stream endpoint and the server's sse endpoint
    When connect is called
    Then within the configured timeout plus tolerance the client emits "connected" over transport "sse"

  Scenario: Every transport unreachable
    Given a client configured with unreachable websocket, http_stream and sse endpoints
    When connect is called
    Then the client state is "connecting"
    And the client emits an "error" event for each failed transport attempt

  @E
  Scenario: A failed fallback sequence is retried after the reconnect backoff
    Given a client configured with websocket, http_stream and sse endpoints
    When connect is called and every transport attempt fails
    Then the transports of the first sequence are tried without delay
    And the next sequence starts only after the reconnect backoff delay

  @A
  Scenario: The fallback sequence is retried until a transport succeeds
    Given a server with http_stream and sse enabled, whose http_stream endpoint is behind a relay that is down
    And a client configured with an unreachable websocket endpoint, the relayed http_stream endpoint and an unreachable sse endpoint
    When connect is called
    And the relay comes up after every transport has failed once
    Then the client emits "connected" over transport "http_stream"

  Scenario Outline: Protobuf with an HTTP transport is rejected
    Given client options with the Protobuf protocol and an <transport> endpoint
    When the client is built
    Then building fails with a configuration error naming Protobuf and <transport>

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @A
  Scenario Outline: Server pings over an HTTP transport are answered
    Given a server with http_stream and sse enabled, a ping interval of 2 s and a pong timeout of 1 s
    And a client connected over transport "<transport>"
    When 6 s elapse
    Then the client stays "connected"

    Examples:
      | transport   |
      | http_stream |
      | sse         |
```

**Exceptions introduced:** none.

---

## Block 9 — Client-side subscriptions

**Goal:** subscriptions with the states `unsubscribed`, `subscribing` and `subscribed`, a client registry, publications, and automatic resubscribe after a reconnect (spec, "Subscription states", "Subscription management", "Listen to channel publications").

**Dependencies:** Blocks 1–5, 8 (publications over the emulated transports arrive as top-level pushes rather than websocket's reply envelope, so delivery is checked over every transport here).

**Out of scope:** subscription tokens, server unsubscribe codes, resubscribe backoff (Block 10); recovery and `getState` (Block 11); server-side subscriptions (Block 12); publish, history, presence (Block 13); join and leave pushes; map and shared-poll subscriptions (Block 0); delta compression (`delta` in `SubscribeRequest`, `SubscribeResult` and `Publication`): not in v1, the client never requests it, so the server never sends patches; tag filters (`SubscribeRequest.tf`): not among the spec's subscription options, not in v1, the client never sends one.

**Decisions:** a `Subscription` handle with its own `state()` and `events()` stream, like `Client` (spec: subscriptions have their own states and events); creating a second subscription to a channel already in the registry fails (spec: "`newSubscription` can throw"; centrifuge-js: "already exists"); the registry offers lookup by channel, the list of subscriptions, and removal, which unsubscribes first (spec, "Subscription management"); the subscription `data` option is attached to every subscribe and resubscribe request (spec, "Subscription common options"), and a `get_data` callback, given the channel, is called before every subscribe and resubscribe instead when set (centrifuge-js `getData`); a `get_data` callback that signals "unauthorized" unsubscribes the subscription with code 1 "unauthorized" and schedules no retry, while any other failure emits an `error` event and retries after the resubscribe backoff (centrifuge-js `_handleGetDataError`); a permanent subscribe error unsubscribes with the error's code and its message as the reason (centrifuge-js `_subscribeError`); `subscribe()` before `connect()` is allowed (spec); a subscribe without a reply within `Config::timeout` makes the client reconnect with code 3 "subscribe timeout", since the server-side subscription state is unknown (centrifuge-js `connectingCodes.subscribeTimeout`); subscribed handles go to `subscribing` on any disconnect, explicit or not, and resubscribe on the next connection (spec, "Subscription states"); an `unsubscribe()` made while the subscribe is in flight takes effect after it is settled, so no server subscription outlives the handle, and a new subscription to a channel waits until the unsubscribe of a removed subscription to that channel is settled (centrifuge-js `_removedSubscriptionUnsubscribed`), as does a `subscribe()` of the same handle until its own unsubscribe is settled (centrifuge-js `_setSubscribing`, `_unsubPromise`); the messages of a frame that follow one closing or replacing the transport (a disconnect push, a malformed message) are discarded, so no stale publication is delivered and no position moves (centrifuge-js `_dispatchSynchronized`); both matter on the emulated transports, whose commands travel as separate requests; publication data is bytes (Block 0), so with Protobuf any bytes round-trip, including non-UTF-8 (Block 7's value, testable once publications exist); the `subscribed` event carries the data the server attached to the subscribe result (`client.proto`, `SubscribeResult.data`), and publications included in a non-recovering subscribe result are delivered after it (`SubscribeResult.publications`); publication data is exposed as the exact bytes received, never re-serialized; the publication event carries `offset`, `tags` and `info` when the server sends them (spec, "Listen to channel publications"; `client.proto`, `Publication`).

`VERIFY:` the Centrifugo v6 channel options letting an anonymous client subscribe to channels without a namespace (`channel.without_namespace` permissions), and the server API `publish` request shape, including `b64data` for binary data.

Verified (Centrifugo v6.10.1, raw websocket client and server API, while implementing Block 9): anonymous subscribe to a channel without namespace needs both `channel.without_namespace.allow_subscribe_for_client` and `allow_subscribe_for_anonymous` (the latter alone answers error 103 "permission denied"); `history_size` and `history_ttl` give publications an offset; the server API `publish` takes `channel`, `data` (JSON) or `b64data` (binary) and `tags`, and the client receives `{"push":{"channel":...,"pub":{"data":...,"offset":...,"tags":...,"epoch":...}}}` with `data` byte for byte as published; binary `b64data` reaches a JSON client as raw non-UTF-8 bytes in the frame, so binary payloads are for Protobuf clients only.

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
  Scenario Outline: Publication delivered over each transport
    Given a server with http_stream and sse enabled
    And a client connected over transport "<transport>" and subscribed to channel "news"
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @A
  Scenario: Publication metadata is delivered
    Given a client subscribed to channel "news" with history
    When the server API publishes {"text":"hello"} to channel "news" with tags {"kind":"greeting"}
    Then the subscription receives a publication with tags {"kind":"greeting"}
    And the publication carries the channel's next stream offset

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
    Given the subscribe proxy answers channel "news" with error code 403 and message "news is closed"
    When a subscription to channel "news" is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 403 and reason "news is closed"

  @B
  Scenario: Unsubscribe during an in-flight subscribe over an emulated transport
    Given a server with http_stream enabled and a subscribe proxy that answers after 1 s
    And a client connected over transport "http_stream"
    When a subscription to channel "news" is subscribed
    And unsubscribe is called on the subscription before the subscribe is answered
    Then the subscription emits "unsubscribed" with code 0
    And a publication to channel "news" after the subscribe was answered is not received

  @A
  Scenario: Subscriptions survive an explicit disconnect and connect
    Given a client subscribed to channel "news"
    When disconnect is called
    Then the subscription emits "subscribing"
    When connect is called
    Then the subscription emits "subscribed"
    And a publication to channel "news" is received

  @D
  Scenario: Initial publications in the subscribe reply are delivered
    Given a client connected to the fake server
    When a subscription to channel "news" is subscribed and the fake server answers with 1 initial publication
    Then the subscription emits "subscribed"
    And then the subscription receives that publication

  @D
  Scenario: JSON publication bytes are exposed exactly as received
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes a publication whose data is the non-canonical JSON {"b": 1.0,  "a":2}
    Then the subscription receives a publication whose data is exactly those bytes

  @D
  Scenario: A subscribe without a reply makes the client reconnect
    Given a client connected to the fake server that never answers the first subscribe command
    When a subscription to channel "news" is subscribed
    Then within the configured timeout plus tolerance the client emits "connecting" with code 3
    And the client opens a new connection
    And the subscription reaches state "subscribed" on it

  @B
  Scenario: Data returned with the subscribe reply is delivered
    Given the subscribe proxy answers channel "news" with data {"welcome":true}
    When a subscription to channel "news" is subscribed on a connected client
    Then the subscription emits "subscribed" with data {"welcome":true}

  @B
  Scenario: Subscription data is sent on subscribe and on resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with subscription data {"room":1}
    When the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"room":1} on both subscribe requests

  @B
  Scenario: Subscription data from a callback is renewed on resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with a data callback returning {"n":1} then {"n":2}
    When the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"n":1} then {"n":2}

  @B
  Scenario: Each subscription data callback receives its own channel
    Given a subscribe proxy recording requests
    And a connected client with subscriptions to channels "a" and "b" using one data callback
    When both subscriptions are subscribed
    Then the data callback was called once for channel "a" and once for channel "b"

  @E
  Scenario: A failing subscription data callback is retried
    Given a subscription whose data callback fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And the data callback is called again after the resubscribe backoff delay
    And the subscribe command carries the data the second call returned

  @E
  Scenario: A data callback denying access unsubscribes
    Given a subscription whose data callback signals "unauthorized"
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1
    And no subscribe command is sent
    And no resubscribe is scheduled

  Scenario: Subscriptions are found by channel and listed
    Given a new client with subscriptions to channels "news" and "sport"
    Then looking up channel "news" returns its subscription
    And looking up channel "weather" returns none
    And the client lists the subscriptions to "news" and "sport"

  @B
  Scenario: A subscription recreated after removal stays subscribed on an emulated transport
    Given a client connected over transport "http_stream" through a proxy that delays the next unsubscribe emulation request by 1 s
    And a client subscribed to channel "news"
    When the subscription is removed from the client
    And a new subscription to channel "news" is created and subscribed at once
    Then the new subscription reaches state "subscribed" after the delayed unsubscribe is settled
    And a publication to channel "news" is received by the new subscription

  @B
  Scenario: A subscription resubscribed at once stays subscribed on an emulated transport
    Given a client connected over transport "http_stream" through a proxy that delays the next unsubscribe emulation request by 1 s
    And a client subscribed to channel "news"
    When unsubscribe is called on the subscription
    And subscribe is called on the subscription again at once
    Then the subscription reaches state "subscribed" after the delayed unsubscribe is settled
    And a publication to channel "news" made after both requests settled is received

  @D
  Scenario: Messages after a disconnect in the same frame are discarded
    Given a client subscribed to channel "news" on the fake server
    When the fake server sends one frame holding a reconnect-advised disconnect push followed by a publication on channel "news"
    Then the client emits "connecting" with that code
    And the subscription receives no publication from that frame

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

**Decisions:** each subscription owns a token component (5b); the subscription token callback receives the channel (centrifuge-js `SubscriptionTokenContext`); expiring tokens are refreshed with `sub_refresh`; server unsubscribe codes from 2500 resubscribe, below 2500 unsubscribe (spec); resubscribe backoff defaults 500 ms and 20000 ms (centrifuge-js), same formula as Block 2, and the attempt counter resets when the subscription reaches `subscribed` (centrifuge-js); a subscribe answered with error 109 (token expired) drops the token and resubscribes, so the callback supplies a new one (centrifuge-js `_subscribeError`); a failing subscription token callback or a temporary `sub_refresh` error emits an `error` event and retries the refresh after 5 to 10 s, as for connection tokens (centrifuge-js `subscription.ts`); a `sub_refresh` without a reply within `Config::timeout` fails with the timeout error (code 1), which, as any error code below 100, is retried like a temporary one (centrifuge-js `_refreshError`); a token callback that signals "unauthorized", on the first fetch as on a refresh, unsubscribes with code 1 "unauthorized", sends no subscribe command and schedules no retry (centrifuge-js `_failUnauthorized`); `unsubscribe()` and removal cancel a scheduled resubscribe and a scheduled refresh retry, so no subscribe command, token callback or `sub_refresh` follows them (centrifuge-js `_setUnsubscribed`); a subscribe answered with error 109 while a token callback is set emits an `error` event (type subscribe) when the client is connected, then resubscribes after the resubscribe backoff, as any temporary subscribe error (centrifuge-js `_subscribeError`); a subscribe answered with error 109 without a token callback emits an `error` event (type configuration, code 12 "subscription token expired but no getToken function set") and unsubscribes with code 1 "unauthorized", with no unsubscribe command and no retry (centrifuge-js `_subscribeError`); a token callback returning an empty token, on the first fetch as on a refresh, is handled as "unauthorized" (centrifuge-js `_handleTokenResponse`, `_refresh`; as Block 5 for connection tokens); when both a token callback and a `get_data` callback are set and the attempt has no token, the token is fetched first and `get_data` is called only once a token is obtained, so a denied or failed token fetch calls no `get_data` for that attempt; an attempt that has a token calls `get_data` without calling the token callback (centrifuge-js `_canSubscribeWithoutGettingToken`, `_handleTokenResponse`, `_handleTokenError`).

Verified (2026-10-22): the server API `unsubscribe` accepts no code (centrifugo `api.proto` `UnsubscribeRequest`, `api.go` `Executor.Unsubscribe`), so the server unsubscribe code table runs on @D only; a denied subscription token unsubscribes with code 1 "unauthorized", on first fetch and on refresh (centrifuge-js `codes.ts` `unsubscribedCodes.unauthorized`, `subscription.ts` `_failUnauthorized`).

Value clause: As an app developer, I want subscription tokens refreshed and server unsubscribe codes honoured, so that private channels stay authorized only while my application allows it.

```gherkin
Feature: Subscription tokens, unsubscribe codes and resubscribe backoff

  @D
  Scenario Outline: Server unsubscribe codes
    Given a client subscribed to channel "news" on the fake server
    When the fake server unsubscribes the subscription with code <code>
    Then the subscription emits "<state>" with code <code> and the reason sent
    And the subscription state is "<state>"

    Examples:
      | code | state        |
      | 2000 | unsubscribed |
      | 2499 | unsubscribed |
      | 2500 | subscribing  |
      | 2999 | subscribing  |

  @A
  Scenario: Subscription token refreshed before expiry keeps the subscription
    Given a client subscribed to a private channel using a subscription token with expiration
    When 10 s elapse
    Then the subscription stays "subscribed"
    And the subscription token was refreshed at least twice

  @E
  Scenario: A failing first subscription token fetch is retried before subscribing
    Given a subscription to a private channel without an initial token whose token callback fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And the token callback is called again after the resubscribe backoff delay
    And the subscribe command carries the token the second call returned

  @E
  Scenario: Subscribe error 109 fetches a new token and resubscribes
    Given a subscription to a private channel using a subscription token callback
    When the server answers its subscribe command with error 109
    Then the subscription token callback is called again
    And the subscription resubscribes with the new token

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

  @E
  Scenario: A sub_refresh without a reply is retried
    Given a subscribed subscription whose token expires
    When the server never answers its sub_refresh
    Then after the configured timeout the subscription emits an "error" event with the timeout code
    And the subscription token callback is called again after 5 to 10 s
    And a new sub_refresh carrying the new token is sent

  @E
  Scenario: A subscription token denied before the first subscribe unsubscribes
    Given a subscription to a private channel without an initial token whose token callback signals "unauthorized"
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1
    And no subscribe command is sent
    And no resubscribe is scheduled

  @E
  Scenario Outline: Ending a subscription during the resubscribe backoff cancels the retry
    Given a subscription whose subscribe attempt failed with a temporary error
    When <action> before the resubscribe delay elapses
    Then the subscription emits "unsubscribed" with code 0
    And no subscribe command is sent after the resubscribe delay

    Examples:
      | action                                        |
      | unsubscribe is called on the subscription     |
      | the subscription is removed from the client   |

  @E
  Scenario Outline: Ending a subscription during the refresh retry delay cancels the refresh
    Given a subscribed subscription whose token refresh failed with a temporary error
    When <action> before the refresh retry delay elapses
    Then the subscription emits "unsubscribed" with code 0
    And neither the token callback nor a sub_refresh runs after the refresh retry delay

    Examples:
      | action                                        |
      | unsubscribe is called on the subscription     |
      | the subscription is removed from the client   |

  @E
  Scenario: A refresh reply without expiry stops refreshing
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with expires false
    Then no further refresh is scheduled

  @D
  Scenario: After a resubscribe-advised unsubscribe the subscription is subscribed again
    Given a client subscribed to channel "news" on the fake server
    When the fake server unsubscribes the subscription with code 2500
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed" after the fake server answers the new subscribe command

  @E
  Scenario: A permanent sub_refresh error unsubscribes
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with a permanent error
    Then the subscription emits "unsubscribed" with that error's code and message
    And no further refresh is scheduled

  @E
  Scenario: An expiring fixed subscription token without a callback unsubscribes
    Given a subscribed subscription with a fixed token that expires and no token callback
    When the subscription refresh is due
    Then the subscription emits an "error" event for the missing token callback
    And then the subscription emits "unsubscribed"

  @E
  Scenario: A subscribe rejected for an expired token without a callback unsubscribes
    Given a subscription with a fixed token and no token callback
    When the subscription is subscribed on a connected client
    And the server answers its subscribe command with error 109
    Then the subscription emits a configuration "error" event with code 12 and message "subscription token expired but no getToken function set"
    And then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And no resubscribe is scheduled
    And no unsubscribe command is sent

  @E
  Scenario: A subscribe rejected for an expired token fetches a new one after the backoff
    Given a subscription to a private channel whose token callback is set
    When the subscription is subscribed on a connected client
    And the server answers its subscribe command with error 109
    Then the subscription emits an "error" event of kind "subscribe"
    And no unsubscribe command is sent
    And the subscription token callback is called again after the resubscribe delay
    And the new subscribe command carries the new token

  @E
  Scenario: An empty subscription token before the first subscribe unsubscribes
    Given a subscription to a private channel without an initial token whose token callback returns an empty token
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And no subscribe command is sent
    And no resubscribe is scheduled

  @E
  Scenario: An empty subscription token on refresh unsubscribes
    Given a subscribed subscription whose token expires
    When the subscription token callback returns an empty token during refresh
    Then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And an unsubscribe command is sent
    And no further refresh is scheduled
    And no sub_refresh command is sent

  @E
  Scenario Outline: The data callback runs only after a subscription token is obtained
    Given a subscription to a private channel without an initial token, with a data callback whose token callback <outcome>
    When the subscription is subscribed on a connected client
    Then the data callback is not called for that attempt

    Examples:
      | outcome                      |
      | signals "unauthorized"       |
      | fails with a temporary error |
      | returns an empty token       |

  @E
  Scenario: The subscription token is fetched before the data callback runs
    Given a subscription to a private channel without an initial token, with a token callback and a data callback
    When the subscription is subscribed on a connected client
    Then the token callback is called before the data callback
    And the subscribe command carries both the token and the data

  @E
  Scenario: A subscription that has a token calls the data callback without the token callback
    Given a subscription to a private channel with an initial token, a token callback and a data callback
    When the subscription is subscribed on a connected client
    Then the token callback is not called
    And the subscribe command carries the initial token and the data

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

**Decisions:** the `subscribed` event carries `was_recovering` and `recovered` (spec); `unsubscribe()` keeps the stream position (spec); the `since` option starts recovery on the first subscribe (spec); the `positioned` and `recoverable` options ask the server for those capabilities on channels that do not force them, by setting the corresponding `SubscribeRequest` fields (spec, "Subscription common options"); the server decides, so the `subscribed` event reports the granted `positioned` and `recoverable` and the stream position (`epoch`, `offset`) from `SubscribeResult` (centrifuge-js); the stored position follows every publication delivered, live or recovered, including a changed `epoch` (`client.proto`, `Publication.epoch`); `getState` is called before the initial subscribe, whose stream position it provides, and again when recovery fails (spec); when `since` is also set, `since` provides the first position and getState is not called until a recovery fails (centrifuge-js calls getState only without a position); with getState the subscribe command sets the "reject unrecovered" flag, so a failed recovery is answered with error 112 instead of a `recovered = false` result, and the client drops the position and resubscribes after the resubscribe backoff, calling getState first: the subscription stays `subscribing`, with no `subscribed` event, no publication and no released subscription call, until that subscribe succeeds (`client.proto`, `SubscribeRequest.flag`; centrifuge-js `subscriptionFlags.rejectUnrecovered`); a failing `getState` emits an `error` event and is retried with the resubscribe backoff before subscribing (Centrifugo blog "App-owned state with stream subscriptions", 2026-07-27); disconnect code 3014 "state invalidated" (centrifuge-js; within the spec's reconnect range) clears the connection token, so the next connect fetches a new one when a callback exists, and the stream positions of every client-side and server-side subscription, then reconnects; unsubscribe code 2502 "state invalidated" drops the subscription token and the stream position, then resubscribes: with a token callback a new token is fetched first, without one the resubscribe carries no token (`client.proto`, `Unsubscribe`: "client must drop its subscription token and any state accumulated"; it outranks centrifuge-js, which keeps the token when there is no callback).

`VERIFY:` the Centrifugo v6 channel options for history and recovery (including allowing client-requested positioning and recovery), and how a test makes history expire; `getState` in centrifuge-js `163dcba`; that Centrifugo v6.10.1 honours the "reject unrecovered" subscribe flag with error 112 (if not, "getState reloads state when recovery fails" moves to @D).

Value clause: As an app developer, I want missed publications recovered after a reconnect, or my state reloaded when recovery fails, so that my application sees no gap in a channel's stream.

```gherkin
Feature: Subscription recovery

  @A
  Scenario: A first subscribe does not recover
    Given a connected client
    When a subscription to a channel with history and recovery is subscribed
    Then the subscription emits "subscribed" with was_recovering false and recovered false

  @D
  Scenario: Positioned and recoverable options are requested
    Given a client connected to the fake server
    When a subscription to channel "news" with the positioned and recoverable options is subscribed
    Then the fake server received a subscribe command for "news" requesting positioned and recoverable

  @D
  Scenario Outline: The subscribed event reports the granted capabilities
    Given a client connected to the fake server
    When a subscription to channel "news" with the positioned and recoverable options is subscribed and the fake server grants <granted>
    Then the subscription emits "subscribed" reporting positioned <positioned>, recoverable <recoverable> and the fake server's stream position

    Examples:
      | granted          | positioned | recoverable |
      | both             | true       | true        |
      | positioned only  | true       | false       |
      | recoverable only | false      | true        |
      | neither          | false      | false       |

  @D
  Scenario: The first subscribe reply's position is used for recovery
    Given a client connected to the fake server
    And a subscription to recoverable channel "news" whose subscribe reply carries a stream position
    When the fake server disconnects the client with a reconnect-advised code before any publication
    Then the next subscribe command for "news" asks to recover from that stream position

  @D
  Scenario: A changed epoch on a live publication is used for recovery
    Given a client subscribed to recoverable channel "news" on the fake server
    When the fake server pushes a publication with a new epoch
    And the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "news" carries the new epoch and that publication's offset

  @D
  Scenario: Recovered publications advance the stored position
    Given a client subscribed to recoverable channel "news" on the fake server
    And the client recovered 2 publications after a reconnect
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next subscribe command for "news" carries the offset of the second recovered publication

  @A
  Scenario: Missed publications are recovered
    Given a client subscribed to a channel with history and recovery
    And the subscription has received 1 live publication
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
  Scenario: Publications made while explicitly disconnected are recovered
    Given a client subscribed to a channel with history and recovery
    And the subscription has received 1 live publication
    When disconnect is called
    And 2 publications are made to the channel
    And connect is called
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

  @D
  Scenario: A failed recovery's reply replaces the stored position
    Given a client subscribed to recoverable channel "news" on the fake server
    And a resubscribe after a reconnect that the fake server answered with recovered false and a new stream position
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next subscribe command for "news" asks to recover from the new stream position

  @B
  Scenario: getState provides the position of the initial subscribe
    Given a subscribe proxy recording requests
    And a subscription with a getState callback to a channel with history and recovery
    When the subscription is subscribed on a connected client
    Then the getState callback is called once before the subscribe command
    And the subscribe command carries the stream position getState returned

  @B
  Scenario: getState reloads state when recovery fails
    Given a subscribe proxy recording requests
    And a client subscribed with a getState callback to a channel with history and recovery
    And the channel history expires while the client is disconnected
    When the client resubscribes
    Then the getState callback is called a second time
    And until then the subscription stays "subscribing" and receives no publication
    And the next subscribe command carries the stream position it returned
    And then the subscription emits "subscribed"

  @E
  Scenario: The since option takes precedence over getState on the first subscribe
    Given a subscription with the since option and a getState callback
    When the subscription is subscribed on a connected client
    Then the getState callback is not called
    And the subscribe command asks to recover from the since position

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
    And the subscription resubscribes with the new token and without a stream position

  @D
  Scenario: Connection state invalidation clears the token and every position
    Given a client using a connection token callback, connected to the fake server
    And a client-side subscription to channel "news" with a stream position
    And a server-side subscription to channel "alerts" with a stream position from the connect reply
    When the fake server disconnects the client with code 3014
    Then the connection token callback is called before the next connect
    And the next connect command carries no position for "alerts"
    And the next subscribe command for "news" carries no stream position

  @D
  Scenario: Connection state invalidation drops a fixed connection token
    Given a client using a fixed connection token and no token callback, connected to the fake server
    When the fake server disconnects the client with code 3014
    Then the next connect command carries no token

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

**Dependencies:** Blocks 8 (over the emulated transports the handshake arrives as a `Connect` push, whose `subs` must be read too), 9.

**Out of scope:** client control over them (the spec gives none); top-level publish, history and presence (Block 13).

**Decisions:** server-side subscriptions live in a registry separate from client-side ones and emit their own events (spec); their `subscribed` events carry the data the server attached, at connect (`SubscribeResult.data` in `ConnectResult.subs`) and in a `Subscribe` push (`Subscribe.data`), and the same context as client-side ones: granted `positioned` and `recoverable`, `was_recovering`, `recovered` and the stream position (centrifuge-js `ServerSubscribedContext`); a channel can have both a client-side and a server-side subscription, kept apart, with a pushed publication routed to the server-side one unless the client-side one is subscribed (centrifuge-js `_getSubForPush`); the registry and its positions survive an explicit `disconnect()`, so a later `connect()` restores and recovers them, as for client-side subscriptions; those granted at connect arrive in `ConnectResult.subs`, those added later in a `Subscribe` push, and both are tracked (`client.proto`); on reconnect the connect command carries each recoverable server-side subscription's position in `ConnectRequest.subs`, so missed publications are recovered (`client.proto`); that position comes from the `ConnectResult.subs` entry or the `Subscribe` push that added the subscription, and follows every server-side publication delivered, live or recovered (centrifuge-js `_handleSubscribe`, `_handlePublication`); publications in a `ConnectResult.subs` entry are delivered after its `subscribed` event whether or not it recovers, as for client-side subscribe replies (Block 9; `client.proto` puts no condition on `SubscribeResult.publications`, while centrifuge-js delivers them only when recovered: not ported); a subscription absent from a reconnect's `ConnectResult.subs`, or removed by an `Unsubscribe` push, emits `unsubscribed` once and is removed from the registry with its position (centrifuge-js `_processServerSubs`, `_handleUnsubscribe`); the server-side `unsubscribed` event carries the code and reason of the `Unsubscribe` push (`client.proto`; centrifuge-js passes only the channel: extended, as the proto carries them).

`VERIFY:` how a test makes Centrifugo v6 subscribe a connection on the server side (connect proxy result `channels`, or token `channels` claim), and the server API `subscribe` method for a live connection.

Value clause: As an app developer, I want publications of channels the server subscribed me to, so that server-managed subscriptions need no client code to keep.

```gherkin
Feature: Server-side subscriptions

  @B
  Scenario: Publications of a server-side subscription are delivered
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    Then the client emits a server-side "subscribed" event for channel "alerts"
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
    And the client has received 1 live server-side publication on channel "alerts"
    And the client is disconnected with a reconnect-advised code
    When 2 publications are made to channel "alerts" while the client reconnects
    Then the client emits a server-side "subscribed" event for channel "alerts" reporting recoverable true, was_recovering true, recovered true and the new stream position
    And the client receives exactly those 2 server-side publications in order

  @D
  Scenario: Client-side and server-side subscriptions to one channel stay separate
    Given a client with an unsubscribed client-side subscription to channel "alerts"
    And the fake server grants a server-side subscription to channel "alerts" at connect
    And the client is connected to the fake server
    When the fake server pushes a publication on channel "alerts"
    Then the client receives it as a server-side publication
    And the client-side subscription receives nothing and stays "unsubscribed"
    When the client-side subscription is subscribed
    Then it reaches state "subscribed"
    And the next publication on channel "alerts" goes to the client-side subscription
    And the server-side registry still lists channel "alerts"

  @B
  Scenario: Server-side subscriptions survive an explicit disconnect and connect
    Given the connect proxy subscribes the connection to channel "alerts" with history and recovery
    And a connected client
    And the client has received 1 live server-side publication on channel "alerts"
    When disconnect is called
    Then the client emits a server-side "subscribing" event for channel "alerts"
    When 2 publications are made to channel "alerts"
    And connect is called
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And the client receives exactly those 2 server-side publications in order

  @B
  Scenario: A subscription added by a push is recovered after reconnect
    Given a connected client
    And the server API subscribes the client to recoverable channel "alerts"
    And the client has received 1 live server-side publication on channel "alerts"
    And the connect proxy will subscribe the next connection to channel "alerts"
    When the client is disconnected with a reconnect-advised code
    And 2 publications are made to channel "alerts" while the client reconnects
    Then the client receives exactly those 2 server-side publications in order

  @A
  Scenario: A subscription added by the server after connect is tracked
    Given a connected client
    When the server API subscribes the client to channel "alerts"
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And a publication to channel "alerts" is received as a server-side publication

  @B
  Scenario Outline: Server-side subscriptions granted at connect over an emulated transport
    Given a server with http_stream and sse enabled
    And the connect proxy subscribes the connection to channel "alerts"
    When a client connects over transport "<transport>"
    Then the client emits a server-side "subscribed" event for channel "alerts"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @D
  Scenario: Server-side subscribed events carry the server's data
    Given the fake server grants a server-side subscription to channel "alerts" with data {"tier":"gold"} at connect
    And a client connected to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts" with data {"tier":"gold"}
    When the fake server pushes a server-side subscription to channel "news" with data {"tier":"silver"}
    Then the client emits a server-side "subscribed" event for channel "news" with data {"tier":"silver"}

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

  @D
  Scenario: A server-side subscription missing after reconnect is forgotten
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server
    And the fake server will grant no server-side subscription on later connects
    When the fake server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "unsubscribed" event for channel "alerts"
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next connect command carries no entry for "alerts"
    And no second server-side "unsubscribed" event for channel "alerts" is emitted

  @D
  Scenario: A pushed server-side subscription's position is used for recovery
    Given a client connected to the fake server
    When the fake server pushes a recoverable server-side subscription to channel "alerts" with a stream position
    And the fake server disconnects the client with a reconnect-advised code before any publication
    Then the next connect command asks to recover "alerts" from that stream position

  @D
  Scenario: Recovered server-side publications advance the stored position
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server
    And the client recovered 2 server-side publications on channel "alerts" after a reconnect
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next connect command asks to recover "alerts" from the offset of the second recovered publication

  @D
  Scenario: Initial publications of a server-side subscription granted at connect are delivered
    Given the fake server grants a server-side subscription to channel "alerts" with 1 initial publication and recovered false at connect
    When a client connects to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And then the client receives that publication as a server-side publication

  @D
  Scenario: A server-side unsubscribe carries its code and reason and forgets the subscription
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at the first connect only
    And a client connected to the fake server
    When the fake server pushes an unsubscribe for channel "alerts" with code 2000 and reason "alerts closed"
    Then the client emits a server-side "unsubscribed" event for channel "alerts" with code 2000 and reason "alerts closed"
    When the fake server disconnects the client with a reconnect-advised code
    Then the next connect command carries no entry for "alerts"
    And no further server-side event for channel "alerts" is emitted
```

**Exceptions introduced:** none.

---

## Block 13 — Client commands

**Goal:** RPC, publish, send, history, presence and presence stats over the client's connection (spec, "Client methods", "Subscription methods", "RPC", "Channel history API", "Presence and presence stats API").

**Dependencies:** Blocks 9, 12.

**Out of scope:** publish debouncing (Block 0); batching several calls into one frame.

**Decisions:** `async fn` returning `Result` (Block 0); a call made while not connected waits for the connection, bounded by `Config::timeout`, also on a client whose `connect()` was never called, and entering `disconnected`, by `disconnect()` or a terminal condition (terminal disconnect code, denied token), fails at once every pending call, queued or already sent (centrifuge-js `_methodCall`, `_disconnect`, which rejects them whenever it does not reconnect); losing the transport fails calls already sent at once with "connection closed" (centrifuge-js error code 11), since their replies can no longer arrive; a subscription-level call made while the subscription is `subscribing` waits until it is `subscribed`, bounded by `Config::timeout`, made while it is `unsubscribed` fails at once with "subscription unsubscribed" (centrifuge-js error code 7) and sends nothing, and a waiting call fails the same way when the subscription becomes `unsubscribed` (centrifuge-js `_methodCall`, `_setUnsubscribed`); a command without a reply fails with the timeout error (code 1); a call that times out while waiting for the connection is dropped and never sent, since its command is built only once the connection is there (centrifuge-js `_methodCall`); a command error keeps the protocol error's code, message and `temporary` flag (`client.proto`, `Error`); RPC data is bytes both ways, like publication data (Block 0; `client.proto`, `RPCRequest.data`, `RPCResult.data`); history takes `limit`, `since` and `reverse` and returns the publications with the stream's current `epoch` and `offset`, and `limit: 0` returns only that position (spec, "Channel history API"); an asynchronous server `message` push is delivered to the application as a client event carrying its bytes (`client.proto`, `Push.message`; spec, client protocol "Asynchronous pushes").

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

  @A
  Scenario: Binary publish over Protobuf keeps its bytes
    Given two clients configured with the Protobuf protocol subscribed to channel "news"
    When the first client publishes the bytes ff 00 fe to channel "news"
    Then the second client's subscription receives a publication with the bytes ff 00 fe

  @D
  Scenario: A subscription command fails after the timeout when the subscribe never completes
    Given a client connected to the fake server that never answers subscribe commands
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the history call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A call made before connect waits for the connection
    Given an RPC proxy answering method "echo" with its input
    And a new client
    When the client calls RPC "echo" before connect is called
    And connect is called
    Then the call completes once the client is connected

  @A
  Scenario: History since a known position
    Given a channel with history holding 3 publications
    When a connected client requests the channel's history since the position of the first one
    Then it receives the 2 later publications and the stream's current position

  @A
  Scenario: History in reverse order
    Given a channel with history holding 3 publications
    When a connected client requests the last 2 publications of the channel in reverse order
    Then it receives the third publication then the second

  @A
  Scenario: History with limit 0 returns only the stream position
    Given a channel with history holding 3 publications
    When a connected client requests the channel's history with limit 0
    Then it receives no publications and the stream's current epoch and offset

  @A
  Scenario: A client-published publication carries its publisher
    Given two clients of different users with connection and channel info subscribed to channel "news"
    When the first client publishes {"text":"hi"} to channel "news"
    Then the second client's subscription receives a publication whose info names the first client, its user, its connection info and its channel info

  @D
  Scenario: Asynchronous server messages are delivered with their exact bytes
    Given a client connected to the fake server
    When the fake server sends a message push whose data is the non-canonical JSON {"b": 1.0,  "a":2}
    Then the client emits a "message" event whose data is exactly those bytes

  @D
  Scenario: Asynchronous server messages over Protobuf keep binary data
    Given a client configured with the Protobuf protocol connected to the fake server
    When the fake server sends a Protobuf message push with the bytes ff 00 fe
    Then the client emits a "message" event with the bytes ff 00 fe

  @B
  Scenario: Temporary RPC error is reported as such
    Given a connected client and an RPC proxy answering method "busy" with a temporary error with code 1000 and message "try later"
    When the client calls RPC "busy"
    Then the call fails with a temporary error with code 1000 and message "try later"

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

  @D
  Scenario: A call that timed out while queued is never sent
    Given a client configured with the fake server's endpoint behind a relay that is down
    When connect is called
    And the client calls RPC "echo"
    And the call fails with a timeout error
    And the relay comes up
    Then the client emits "connected"
    And the fake server receives no RPC command

  @D
  Scenario: Binary RPC over Protobuf keeps its bytes
    Given a client configured with the Protobuf protocol connected to the fake server that answers RPC "bin" with the bytes fe 00 ff
    When the client calls RPC "bin" with the bytes ff 00 fe
    Then the fake server received a Protobuf RPC command with the bytes ff 00 fe
    And the call returns the bytes fe 00 ff

  Scenario: A call on a client never connected fails after the timeout
    Given a new client
    When the client calls RPC "echo"
    And connect is never called
    Then the call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A terminal connect failure fails a queued call at once
    Given an RPC proxy answering method "echo" with its input
    And the connect proxy answers after 1 s with disconnect code 4500
    And a new client
    When connect is called
    And the client calls RPC "echo" before it is connected
    Then the client emits "disconnected" with code 4500
    And the call fails with a "disconnected" error before the configured timeout

  @D
  Scenario: A subscription command on an unsubscribed subscription fails at once
    Given a client connected to the fake server with an unsubscribed subscription to channel "news"
    When its history is requested
    Then the call fails at once with a "subscription unsubscribed" error
    And the fake server receives no history command

  @D
  Scenario: A subscription command waiting for a failed subscribe fails at once
    Given a client connected to the fake server that answers subscribe commands after 1 s with a permanent error
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the subscription emits "unsubscribed"
    And the call fails with a "subscription unsubscribed" error before the configured timeout
    And the fake server receives no history command

  Scenario: Disconnect fails a queued call at once
    Given a client configured with an unreachable endpoint
    And connect is called
    And the client calls RPC "echo"
    When disconnect is called
    Then the call fails with a "disconnected" error

  @D
  Scenario: Disconnect fails a call already sent at once
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When disconnect is called
    Then the call fails with a "disconnected" error before the configured timeout

  @D
  Scenario: A subscription command waits until the subscription is subscribed
    Given a client connected to the fake server that answers subscribe commands after 1 s
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the fake server receives the history command only after it answered the subscribe command
    And the history call completes

  @D
  Scenario: Send delivers a message without a reply
    Given a client connected to the fake server
    When the client sends {"note":"x"}
    Then the fake server received a send command with data {"note":"x"} and no command id

  @A
  Scenario: Presence lists the subscribed clients
    Given two clients of different users with connection and channel info subscribed to a channel with presence
    When one of them requests the channel's presence
    Then it receives both clients with their user, connection info and channel info

  @D
  Scenario Outline: Commands round-trip over Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server
    When the client calls <command>
    Then the fake server received the <command> command in Protobuf
    And the call returns the fake server's Protobuf result for <command>

    Examples:
      | command        |
      | rpc            |
      | history        |
      | presence       |
      | presence_stats |

  @D
  Scenario: Send over Protobuf is received without a reply
    Given a client configured with the Protobuf protocol connected to the fake server
    When the client sends the bytes ff 00 fe
    Then the fake server received a Protobuf send command with the bytes ff 00 fe and no command id

  @B
  Scenario: A call whose emulation request fails fails at once
    Given an RPC proxy answering method "echo" with its input
    And a client connected over transport "http_stream" through a proxy that rejects the next emulation request
    When the client calls RPC "echo"
    Then the call fails with a "connection closed" error before the configured timeout
    And the client reconnects over transport "http_stream"

  @D
  Scenario: Losing the transport fails calls already sent at once
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When the fake server disconnects the client with a reconnect-advised code
    Then the call fails with a "connection closed" error before the configured timeout

  @A
  Scenario: Presence stats count clients and users
    Given two clients of different users subscribed to a channel with presence
    When one of them requests the channel's presence stats
    Then it receives 2 clients and 2 users
```

**Exceptions introduced:** none.

---

## Block 14 — Footprint, performance and fleet behaviour

**Goal:**
- a client costs almost nothing at rest;
- each message costs a constant amount, whatever the number of subscriptions;
- memory stays flat over time;
- a fleet of clients spreads its reconnects and token refreshes instead of synchronising them.

**Dependencies:** Blocks 2, 4, 5, 9. Subscription-token refresh jitter applies once Block 10 is implemented.

**Out of scope:**
- HTTP proxy (CONNECT) support;
- sleep/resume behaviour;
- server load testing (the server side has its own tooling);
- the event-buffer policy for slow consumers, pending the owner's decision on review item D1;
- per-platform behaviour beyond running the existing suites on Windows and macOS.

**Decisions:**
- **O(1) dispatch.** A publication is dispatched to its subscription by a lookup keyed by channel. Its cost does not depend on the number of subscriptions. This replaces the linear scan in `src/machine/subscriptions.rs` (`publication`), and must keep Block 9's rule that only a `Subscribed` handle receives publications.
- **Token refresh jitter.** A token refresh is scheduled at a uniformly random point between 80% and 90% of the ttl.
  - This replaces the deterministic margin of Block 5.
  - It diverges from centrifuge-js on purpose: refreshes of tokens issued at the same instant must not happen at the same instant across a fleet.
  - It applies to connection tokens now, and to subscription tokens in Block 10.
- **Reconnect jitter window.** A new client option, named idiomatically, with default 0. When a connected client loses its connection and reconnect is advised, a uniformly random delay in [0, window] is added to the Block 2 delay of the first reconnect attempt; the following attempts follow the Block 2 backoff unchanged.
  - With 0 the behaviour is unchanged, and the Block 2 scenarios keep passing.
  - The option does not apply to the first `connect()`, nor to an explicit `disconnect()` followed by `connect()`.
- **Measurement conventions.**
  - Footprint is measured over a plain websocket to the fake server, with live bytes counted by a counting global allocator in the harness. No runtime dependency is added for this.
  - Instruction counts are measured with gungraun (successor of iai-callgrind; Linux with Valgrind).
  - Wall-clock is measured with criterion, locally and in a non-gating CI job.
  - Verify crate versions on crates.io before adding them.
- **Wall-clock reference targets**, reported only, never gating:
  - decode + dispatch ≤ 10 µs per 1 KiB publication;
  - p99 added latency ≤ 1 ms at 1000 msg/s;
  - idle CPU < 0.1% of a core;
  - allocations per 1 KiB publication: reported (sans-IO core measured at 9 on 2026-10-23).

Value clause: As an app developer shipping Ferrifuge to large fleets of end-user devices, I want each client to cost almost nothing at rest and a constant amount per message, and a fleet of clients to spread its reconnects and token refreshes, so that devices stay unburdened and the Centrifugo cluster never receives synchronised reconnect or refresh waves.

```gherkin
Feature: Footprint, performance and fleet behaviour

  @P
  Scenario: Idle footprint with ten subscriptions
    Given a client connected to the fake server with 10 subscribed channels
    When the client is idle
    Then the live heap attributable to the client is at most 1 MiB

  @P
  Scenario: Marginal cost of a subscription
    Given a client connected to the fake server
    When 1000 more channels are subscribed
    Then the live heap grows by at most 2 KiB per subscription

  @P
  Scenario Outline: Allocations per publication do not depend on the number of subscriptions
    Given a client connected to the fake server with <count> subscribed channels
    When 10000 publications of 1 KiB are delivered to one of them
    Then the allocations per publication are reported
    And they differ by less than 1 from the count with 1 subscribed channel

    Examples:
      | count |
      | 1     |
      | 100   |
      | 10000 |

  @P
  Scenario: Dispatch cost does not depend on the number of subscriptions
    Given dispatch benchmarks with 1 and with 10000 subscribed channels
    When one publication is dispatched in each
    Then the instruction count with 10000 channels is at most 1.1 times the count with 1

  @P
  Scenario: Memory stays flat across reconnects
    Given a client connected to the fake server with 10 subscribed channels
    When the fake server forces 10000 reconnect cycles
    Then the live heap after the last cycle is within 1% of the live heap after the first

  @P
  Scenario: An idle client wakes only for pings and refreshes
    Given a client with 10 subscribed channels and an expiring connection token, driven by a controlled clock
    And the server pings every 25 s
    When 10 minutes of simulated time elapse without publications
    Then the only timers set are for ping detection and token refresh
    And no timer fires more often than once per ping interval

  @P
  Scenario: Reconnects of a fleet are spread over the jitter window
    Given 100000 simulated clients with a reconnect jitter window of 100 s, driven by a controlled clock
    When all of them lose their connection at the same instant with a reconnect-advised code
    Then no second of the window holds more than 2% of the first reconnect attempts

  @P
  Scenario: Token refreshes of a fleet are spread
    Given 100000 simulated clients whose connection tokens have the same ttl and were issued at the same instant
    When their refreshes come due
    Then the refreshes span at least 9.9% of the ttl
    And no interval of 1% of the ttl holds more than twice the average share

  @E
  Scenario: Without a jitter window the first reconnect follows the backoff
    Given a client with a reconnect jitter window of 0
    When the connection is lost with a reconnect-advised code
    Then the first reconnect delay is within the Block 2 bound for attempt 0

  @E
  Scenario: The jitter window does not delay the first connect
    Given a client with a reconnect jitter window of 100 s
    When connect is called
    Then the first connect attempt is made without the jitter delay

  @E
  Scenario: The jitter window does not delay a connect after an explicit disconnect
    Given a connected client with a reconnect jitter window of 100 s
    When disconnect is called
    And connect is called
    Then the connect attempt is made without the jitter delay

  @P
  Scenario: Wall-clock cost per publication is reported
    Given a client connected to the fake server with 10 subscribed channels
    When publications of 1 KiB are delivered at 1000 per second
    Then decode and dispatch time per publication and p99 delivery latency are reported

  @P
  Scenario: Idle CPU is reported
    Given a client connected to the fake server with 10 subscribed channels and server pings every 25 s
    When 10 minutes elapse without publications
    Then the CPU time used by the client is reported
```
