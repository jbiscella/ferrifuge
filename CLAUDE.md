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
| Project purpose (owner decision, 2026-11-04) | Production use on large fleets of end-user devices; not a personal or learning project. Production readiness is an explicit goal. The official centrifugal/centrifuge-rust (created 2026-10-26; "Experimental" per its GitHub repository description, not its README) is a benchmark to compare against, not the production reference; Ferrifuge stays built from scratch | Reconsidered once centrifuge-rust appeared; the build-from-scratch decision above stands |
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
| Events API | `client.events()` returns a `futures::Stream` of a `ClientEvent` enum, one active stream per client, delivered by hand-off with a bounded read-ahead (Block 15); `client.state()` returns the current state | Idiomatic async Rust, runtime-neutral |
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
- channel compaction (Centrifugo PRO; owner decision, 2026-11-04): the client never offers it
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

**Close code 1009 (review item C4):** a websocket closed with code 1009 ("message too big") ends the client: `disconnected` with client-side code 3 "message size limit exceeded", without reconnecting, since a reconnect would send the same oversized data again (owner decision, 2026-11-04, on the code; centrifuge-js 63ed73e `centrifuge.ts:1310-1313`, `disconnectedCodes.messageSizeLimit`).

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

  @D
  Scenario: Close code 1009 stops the client
    Given a client connected to the fake server
    When the fake server disconnects the client with code 1009
    Then the client emits "disconnected" with code 3 and reason "message size limit exceeded"
    And the client makes no new connection attempt

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

Rule (spec): the client answers server PINGs with PONGs; if no PING arrives within ping interval + `maxServerPingDelay`, the connection is considered broken and re-established. Default `maxServerPingDelay` (centrifuge-js): 10000 ms. While the application holds events, pings are still answered when read, and the no-ping deadline pauses only while a full read-ahead stops reading (Block 15).

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

**Failed TLS setup (review item C7):** the TLS settings, which today include the platform's root certificates, are built at the first secure connection attempt and shared by every client in the process, as are the HTTP clients built from them; a build that fails is not kept, so the next secure attempt (each transport tried) builds again, and a transient failure (a CA bundle being replaced, no file descriptor left) heals without a restart.

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
- transports are tried in order during the initial handshake only, until the server answers on one: a connect reply, an error reply to the connect, a Disconnect push, or, over websocket, a close code from 3000 (review items C2 and C3; centrifuge-js 63ed73e `_transportWasOpen`); reconnects then use only that transport, the first of them after the reconnect backoff delay (Block 2);
- reqwest as the HTTP client (Block 0); cargo-deny checks only the targets the crate supports (Linux, macOS, Windows), so data crates used only on wasm32 are not in scope;
- a client configured with the Protobuf protocol and an http_stream or sse endpoint is rejected when it is built, with a configuration error naming the combination, rather than failing at run time;
- the http_stream and sse decoders buffer across body chunks: network chunk boundaries are unrelated to message boundaries, so a message split over several chunks and several messages in one chunk both decode exactly; unit tests cover both cases for each transport (the real server's chunking in @A cannot be controlled);
- server pings are the same empty `Reply` as over websocket, and are answered with a pong, sent through the emulation endpoint, when `ConnectResult.pong` is set (`client.proto`, `ConnectResult.pong`; centrifuge-js `_handleServerPing`; verified: Centrifugo v6.10.1 sets `pong` on these transports and disconnects an unanswered client with 3012 "no pong"). The unanswered empty `Push` ping of `client.proto` belongs to the unidirectional transports, out of scope (Block 0);
- an attempt that opens but gets no connect reply within `Config::timeout` fails like a refused one, and the next transport is tried (owner decision, 2026-11-04, kept although centrifuge-js 63ed73e keeps an opened websocket: Ferrifuge targets corporate networks, where a middlebox may accept the upgrade and then forward nothing, and never falling back would leave the client stuck; the fallback applies only before the first success, so a transport that worked once is still kept);
- within the first sequence the next transport is tried at once; once every transport has failed, the next sequence starts after the reconnect backoff delay (Block 2), as centrifuge-js counts only the first round as handshake;
- a failed emulation request (network error, rejection, or a 404 or 5xx response) closes the transport and the client reconnects, since the command it carried can no longer be answered (centrifuge-js `transport_sse.ts`);
- an emulation request answered with HTTP 413 keeps the transport and ends only the commands it carried, at once; none is retried automatically with the same payload (owner decision, 2026-11-04; diverges from centrifuge-js 63ed73e, which leaves the command to its reply timeout, `transport_http_stream.ts:199-205`): a call the application awaits (publish, RPC, history, presence, presence stats, send) fails with error code 10 "transport write error" (centrifuge-js `errorCodes.transportWriteError`); a subscribe or a sub_refresh moves the subscription to `unsubscribed` with code 3 "message size limit exceeded", sending an unsubscribe command when the server holds the subscription (after a sub_refresh); a connection refresh disconnects the client with code 3 "message size limit exceeded", the code and reason review item C4 uses for websocket close code 1009 (centrifuge-js `disconnectedCodes.messageSizeLimit`); an unsubscribe fails as any unsubscribe error, so the client reconnects with code 4 (Block 9);
- a websocket write never blocks the client: frames are queued and written while the client goes on handling timers, incoming frames and calls; a frame not written within `Config::timeout` of being queued closes the transport, and the client reconnects with code 1 "transport closed", as for any lost transport; closing a websocket, its close handshake included, is bounded by `Config::timeout` too, after which the connection is aborted (owner decision, 2026-11-05, review item C1; centrifuge-js 63ed73e has no counterpart, since a browser's `WebSocket.send` never blocks);
- no construction path accepts an empty endpoint, or an `http://` or `https://` endpoint configured for the websocket transport: building the client fails with a configuration error naming the transport, and `Client::new` returns that error instead of panicking; `http(s)://` endpoints stay valid for http_stream and sse (owner decision, 2026-11-04).

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
    Given a client configured with a websocket endpoint behind a relay that is down and the server's http_stream and sse endpoints, connected over transport "http_stream"
    When the server disconnects the client with a reconnect-advised code
    Then the client emits "connecting" with that code and reason
    And then the client emits "connected" over transport "http_stream"
    And the client made no connection attempt to the websocket endpoint after the disconnect

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

  @B
  Scenario: A websocket that never answers the handshake falls back
    Given the connect proxy will leave the next connect unanswered
    And a client configured with the server's websocket and http_stream endpoints
    When connect is called
    Then within the configured timeout plus tolerance the client emits "connected" over transport "http_stream"

  @E
  Scenario: An opened websocket without a connect reply falls back
    Given a client configured with websocket and http_stream endpoints
    When the websocket opens and no connect reply arrives within the configured timeout
    Then the next attempt opens the http_stream transport

  @E
  Scenario: An emulated transport the server answered is kept
    Given a client configured with http_stream and sse endpoints
    When the http_stream transport opens and the server answers its connect with a temporary error
    Then the next attempt opens the http_stream transport again, after the reconnect backoff delay

  @D
  Scenario: A websocket write the server never reads closes the transport
    Given a client connected to the fake server
    And the fake server stops reading from the connection
    When the client sends more data than the connection can buffer
    Then within the configured timeout plus tolerance the client emits "connecting" with code 1
    And the client opens a new connection

  @D
  Scenario: A stalled websocket write does not delay disconnect
    Given a client connected to the fake server
    And the fake server stops reading from the connection
    And the client has sent more data than the connection can buffer
    When disconnect is called
    Then the client emits "disconnected" before the configured timeout
    And the fake server sees the connection closed within the configured timeout plus tolerance

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

**Out of scope:** subscription tokens, server unsubscribe codes, resubscribe backoff (Block 10); recovery and `getState` (Block 11); server-side subscriptions (Block 12); publish, history, presence (Block 13); join and leave pushes (Block 13); map and shared-poll subscriptions (Block 0); delta compression (`delta` in `SubscribeRequest`, `SubscribeResult` and `Publication`): not in v1, the client never requests it, so the server never sends patches; tag filters (`SubscribeRequest.tf`): not among the spec's subscription options, not in v1, the client never sends one.

**Decisions:** a `Subscription` handle with its own `state()` and `events()` stream, like `Client` (one active stream per object, hand-off delivery: Block 15) (spec: subscriptions have their own states and events); creating a second subscription to a channel already in the registry fails (spec: "`newSubscription` can throw"; centrifuge-js: "already exists"); the registry offers lookup by channel, the list of subscriptions, and removal, which unsubscribes first (spec, "Subscription management"); the subscription `data` option is attached to every subscribe and resubscribe request (spec, "Subscription common options"), and a `get_data` callback, given the channel, is called before every subscribe and resubscribe instead when set (centrifuge-js `getData`); a `get_data` callback that signals "unauthorized" unsubscribes the subscription with code 1 "unauthorized" and schedules no retry, while any other failure emits an `error` event and retries after the resubscribe backoff (centrifuge-js `_handleGetDataError`); a permanent subscribe error unsubscribes with the error's code and its message as the reason (centrifuge-js `_subscribeError`); `subscribe()` before `connect()` is allowed (spec); a subscribe without a reply within `Config::timeout` makes the client reconnect with code 3 "subscribe timeout", since the server-side subscription state is unknown (centrifuge-js `connectingCodes.subscribeTimeout`), while a reply already read but waiting behind events the application has not taken does not time out (Block 15); subscribed handles go to `subscribing` on any disconnect, explicit or not, and resubscribe on the next connection (spec, "Subscription states"); an `unsubscribe()` made while the subscribe is in flight takes effect after it is settled, so no server subscription outlives the handle, and a new subscription to a channel waits until the unsubscribe of a removed subscription to that channel is settled (centrifuge-js `_removedSubscriptionUnsubscribed`), as does a `subscribe()` of the same handle until its own unsubscribe is settled (centrifuge-js `_setSubscribing`, `_unsubPromise`); the messages of a frame that follow one closing or replacing the transport (a disconnect push, a malformed message) are discarded, so no stale publication is delivered and no position moves (centrifuge-js `_dispatchSynchronized`); both matter on the emulated transports, whose commands travel as separate requests; publication data is bytes (Block 0), so with Protobuf any bytes round-trip, including non-UTF-8 (Block 7's value, testable once publications exist); the `subscribed` event carries the data the server attached to the subscribe result (`client.proto`, `SubscribeResult.data`), and publications included in a non-recovering subscribe result are delivered after it (`SubscribeResult.publications`); publication data is exposed as the exact bytes received, never re-serialized; the publication event carries `offset`, `tags` and `info` when the server sends them (spec, "Listen to channel publications"; `client.proto`, `Publication`).

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

**Decisions:** each subscription owns a token component (5b); the subscription token callback receives the channel (centrifuge-js `SubscriptionTokenContext`); expiring tokens are refreshed with `sub_refresh`; server unsubscribe codes from 2500 resubscribe, below 2500 unsubscribe (spec); resubscribe backoff defaults 500 ms and 20000 ms (centrifuge-js), same formula as Block 2, and the attempt counter resets when the subscription reaches `subscribed` (centrifuge-js); a subscribe answered with error 109 (token expired) drops the token and resubscribes, so the callback supplies a new one (centrifuge-js `_subscribeError`); a failing subscription token callback or a temporary `sub_refresh` error emits an `error` event and retries the refresh after 10 to 20 s (centrifuge-js `subscription.ts` `_getRefreshRetryDelay`, `backoff(0, 10000, 20000)`; connection tokens retry after 5 to 10 s, `centrifuge.ts`; the spec asks only for "some jittered time"); a `sub_refresh` without a reply within `Config::timeout` fails with the timeout error (code 1), which, as any error code below 100, is retried like a temporary one (centrifuge-js `_refreshError`); a token callback that signals "unauthorized", on the first fetch as on a refresh, unsubscribes with code 1 "unauthorized", sends no subscribe command and schedules no retry (centrifuge-js `_failUnauthorized`); `unsubscribe()` and removal cancel a scheduled resubscribe and a scheduled refresh retry, so no subscribe command, token callback or `sub_refresh` follows them (centrifuge-js `_setUnsubscribed`); a subscribe answered with error 109 while a token callback is set emits an `error` event (type subscribe) when the client is connected, then resubscribes after the resubscribe backoff, as any temporary subscribe error (centrifuge-js `_subscribeError`); a subscribe answered with error 109 without a token callback emits an `error` event (type configuration, code 12 "subscription token expired but no getToken function set") and unsubscribes with code 1 "unauthorized", with no unsubscribe command and no retry (centrifuge-js `_subscribeError`); a token callback returning an empty token, on the first fetch as on a refresh, is handled as "unauthorized" (centrifuge-js `_handleTokenResponse`, `_refresh`; as Block 5 for connection tokens); when both a token callback and a `get_data` callback are set and the attempt has no token, the token is fetched first and `get_data` is called only once a token is obtained, so a denied or failed token fetch calls no `get_data` for that attempt; an attempt that has a token calls `get_data` without calling the token callback (centrifuge-js `_canSubscribeWithoutGettingToken`, `_handleTokenResponse`, `_handleTokenError`); a resubscribe waiting for its backoff delay keeps waiting across a reconnect: the new connection subscribes it only once the delay has elapsed (owner decision, 2026-11-04; centrifuge-js 63ed73e `centrifuge.ts` `_sendSubscribeCommands`).

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
    And the refresh is retried after 10 to 20 s

  @E
  Scenario: A temporary sub_refresh error is retried
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with a temporary error
    Then the subscription emits an "error" event
    And the refresh is retried after 10 to 20 s

  @E
  Scenario: A sub_refresh without a reply is retried
    Given a subscribed subscription whose token expires
    When the server never answers its sub_refresh
    Then after the configured timeout the subscription emits an "error" event with the timeout code
    And the subscription token callback is called again after 10 to 20 s
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

**Decisions:** the `subscribed` event carries `was_recovering` and `recovered` (spec); `unsubscribe()` keeps the stream position (spec); the `since` option starts recovery on the first subscribe (spec); the `positioned` and `recoverable` options ask the server for those capabilities on channels that do not force them, by setting the corresponding `SubscribeRequest` fields (spec, "Subscription common options"); the server decides, so the `subscribed` event reports the granted `positioned` and `recoverable` and the stream position (`epoch`, `offset`) from `SubscribeResult` (centrifuge-js); the stored position follows every publication delivered (taken from the application's stream, Block 15), live or recovered, including a changed `epoch` (`client.proto`, `Publication.epoch`); `getState` is called before the initial subscribe, whose stream position it provides, and again when recovery fails (spec); when `since` is also set, `since` provides the first position and getState is not called until a recovery fails (centrifuge-js calls getState only without a position); with getState the subscribe command sets the "reject unrecovered" flag, so a failed recovery is answered with error 112 instead of a `recovered = false` result, and the client drops the position and resubscribes after the resubscribe backoff, calling getState first: the subscription stays `subscribing`, with no `subscribed` event, no publication and no released subscription call, until that subscribe succeeds (`client.proto`, `SubscribeRequest.flag`; centrifuge-js `subscriptionFlags.rejectUnrecovered`); a failing `getState` emits an `error` event and is retried with the resubscribe backoff before subscribing (Centrifugo blog "App-owned state with stream subscriptions", 2026-07-27); disconnect code 3014 "state invalidated" (centrifuge-js; within the spec's reconnect range) reconnects; on 3014 a server-side subscription with a stream position keeps a sentinel position (offset 0, epoch "_"), so its recovery at the next connect fails and its subscribed event reports was_recovering true and recovered false (owner decision, 2026-11-04; centrifuge-js 63ed73e centrifuge.ts _disconnect); on 3014 a client-side subscription with a stream position keeps a sentinel position (offset 0, epoch "_") instead of dropping it, so the next recovery fails and the app is told: with getState, error 112 leads to a reload; without getState, `subscribed` reports `was_recovering` true and `recovered` false (owner decision, 2026-11-04; centrifuge-js 63ed73e `_invalidateState`; `client.proto` does not define 3014); on 3014 the client clears the connection token and the token of every client-side subscription, unconditionally; before the next connect or resubscribe a new token is fetched only where a token callback exists, otherwise the command carries no token (owner decision, 2026-11-04; centrifuge-js 63ed73e clears them only when getToken is set, `centrifuge.ts` `_disconnect`, `subscription.ts` `_invalidateState`; `client.proto` does not define 3014); unsubscribe code 2502 "state invalidated" drops the subscription token and the stream position, then resubscribes: with a token callback a new token is fetched first, without one the resubscribe carries no token (`client.proto`, `Unsubscribe`: "client must drop its subscription token and any state accumulated"; it outranks centrifuge-js, which keeps the token when there is no callback).

`VERIFY:` the Centrifugo v6 channel options for history and recovery (including allowing client-requested positioning and recovery), and how a test makes history expire; `getState` in centrifuge-js `163dcba`; that Centrifugo v6.10.1 honours the "reject unrecovered" subscribe flag with error 112 (if not, "getState reloads state when recovery fails" moves to @D).

Verified (Centrifugo v6.10.1, raw websocket client and server API, 2026-11-04): a namespace with `allow_positioning` and `allow_recovery` grants the client's `positioned` and `recoverable` hints (asking for `recoverable` also grants `positioned`), and `force_recovery` makes a channel recoverable without them; recovering from a known offset returns the missed publications with `recovered` true and the `offset` recovered from; a wrong epoch, or a history emptied by a short `history_ttl` or by the server API `history_remove` (both keep the epoch and offset), returns `was_recovering` true without `recovered`; with the "reject unrecovered" flag (`1 << 1`) those subscribes are answered with error 112 "unrecoverable position", so "getState reloads state when recovery fails" stays @B; the subscribe proxy request carries no stream position (centrifugo `proxy.proto`, `SubscribeRequest`); centrifuge-js 63ed73e implements getState (`subscription.ts` `_getState`, flag `rejectUnrecovered`, error 112). On unsubscribe code 2502 and disconnect code 3014 centrifuge-js keeps a sentinel position (epoch `_`) so that the next recovery fails; on 2502 this block drops the position instead, as `client.proto` requires, and on 3014 it keeps the sentinel for client-side and server-side subscriptions (owner decisions, 2026-11-04).

Value clause: As an app developer, I want missed publications recovered after a reconnect, or my state reloaded when recovery fails, so that my application sees no gap in a channel's stream.

**Sub-increments** (owner decision, 2026-11-04; one branch and PR each, in this order):

| Id | Value clause | Scenarios |
|---|---|---|
| 11a | As an app developer, I want missed publications recovered after a reconnect, an unsubscribe or a disconnect, so that my app sees no gap in a channel's stream | "A first subscribe does not recover" to "Recovery fails when history is gone", and "A failed recovery's reply replaces the stored position" |
| 11b | As an app developer, I want my state reloaded through getState when recovery fails, so that a lost stream never leaves my app with stale data | "getState provides the position of the initial subscribe", "getState reloads state when recovery fails", "The since option takes precedence over getState on the first subscribe", "A failing getState is retried before subscribing" |
| 11c | As an app developer, I want a server's "state invalidated" to drop tokens and positions, so that my app resubscribes from fresh state | "State invalidation fetches a new subscription token", "Connection state invalidation clears the token and invalidates every position", "Connection state invalidation drops a fixed connection token", "State invalidation drops a fixed subscription token" |

**Owner decision (2026-11-04):** "Connection state invalidation clears the token and invalidates every position" needs server-side subscriptions (Block 12). 11c implements disconnect code 3014 for the connection token and client-side positions, and covers that half with unit tests on the sans-IO core: the connection token callback is called before the next connect, and the next subscribe of a client-side subscription asks to recover from the sentinel position (offset 0, epoch "_"). The scenario passes once Block 12 adds server-side positions, and Block 12's PR adds it to the implemented scenarios.

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
  Scenario: Connection state invalidation clears the token and invalidates every position
    Given a client using a connection token callback, connected to the fake server
    And a client-side subscription to channel "news" with a stream position
    And a server-side subscription to channel "alerts" with a stream position from the connect reply
    When the fake server disconnects the client with code 3014
    Then the connection token callback is called before the next connect
    And the next connect command asks to recover "alerts" from offset 0 and epoch "_"
    And the next subscribe command for "news" asks to recover from offset 0 and epoch "_"

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

**Dependencies:** Blocks 8 (the `subs` of the connect reply must be read over every transport; verified: with bidirectional emulation the connect reply is an ordinary reply, not a `Connect` push), 9.

**Out of scope:** client control over them (the spec gives none); top-level publish, history and presence (Block 13).

**Decisions:** server-side subscriptions live in a registry separate from client-side ones and emit their own events (spec); their `subscribed` events carry the data the server attached, at connect (`SubscribeResult.data` in `ConnectResult.subs`) and in a `Subscribe` push (`Subscribe.data`), and the same context as client-side ones: granted `positioned` and `recoverable`, `was_recovering`, `recovered` and the stream position (centrifuge-js `ServerSubscribedContext`); a channel can have both a client-side and a server-side subscription, kept apart, with a pushed publication routed to the server-side one unless the client-side one is subscribed (centrifuge-js `_getSubForPush`); the registry and its positions survive an explicit `disconnect()`, so a later `connect()` restores and recovers them, as for client-side subscriptions; those granted at connect arrive in `ConnectResult.subs`, those added later in a `Subscribe` push, and both are tracked (`client.proto`); on reconnect the connect command carries each recoverable server-side subscription's position in `ConnectRequest.subs`, so missed publications are recovered (`client.proto`); that position comes from the `ConnectResult.subs` entry or the `Subscribe` push that added the subscription, and follows every server-side publication delivered (taken from the application's stream, Block 15), live or recovered (centrifuge-js `_handleSubscribe`, `_handlePublication`); publications in a `ConnectResult.subs` entry are delivered after its `subscribed` event whether or not it recovers, as for client-side subscribe replies (Block 9; `client.proto` puts no condition on `SubscribeResult.publications`, while centrifuge-js delivers them only when recovered: not ported); a subscription absent from a reconnect's `ConnectResult.subs`, or removed by an `Unsubscribe` push, emits `unsubscribed` once and is removed from the registry with its position (centrifuge-js `_processServerSubs`, `_handleUnsubscribe`); the server-side `unsubscribed` event carries the code and reason of the `Unsubscribe` push (`client.proto`; centrifuge-js passes only the channel: extended, as the proto carries them); a server-side subscription absent from a reconnect's `ConnectResult.subs` emits `unsubscribed` with client-side code 4 "not subscribed by the server after reconnect" (`client.proto` sends no code for it; centrifuge-js passes only the channel).

`VERIFY:` how a test makes Centrifugo v6 subscribe a connection on the server side (connect proxy result `channels`, or token `channels` claim), and the server API `subscribe` method for a live connection.

Verified (Centrifugo v6.10.1, raw JSON client, server API and a connect proxy, 2026-11-04): a connect proxy result's `channels` list, or its `subs` map (which also carries per-channel `data` and option overrides such as `force_recovery`), subscribes the connection on the server side, and so does a connection token's `channels` or `subs` claim (the connect proxy is then not called); the connect reply lists them in `ConnectResult.subs`, by channel, with `recoverable`, `positioned`, `epoch`, `offset` and `data` as granted; the server API `subscribe` sends a `Subscribe` push with the same fields and optional `data`, and `unsubscribe` an `Unsubscribe` push with code 2000 "server unsubscribe" (the API takes no code); a reconnect whose `ConnectRequest.subs` asks to recover a channel the proxy grants again returns `recovered`, `was_recovering` and the missed publications, its `offset` being the position recovered from, also for a subscription first made by the server API; a channel sent in `ConnectRequest.subs` but not granted is left out of the reply, with no error; over http_stream and sse the connect reply is an ordinary reply carrying `subs`, not a `Connect` push.

Value clause: As an app developer, I want publications of channels the server subscribed me to, so that server-managed subscriptions need no client code to keep.

**Sub-increments** (owner decision, 2026-11-04; one branch and PR each, in this order):

| Id | Value clause | Scenarios |
|---|---|---|
| 12a | As an app developer, I want the server-side subscriptions granted at connect tracked, with their events and publications, so that they need no code of mine | "Publications of a server-side subscription are delivered", "Server-side subscriptions survive a reconnect", "Client-side and server-side subscriptions to one channel stay separate", "Server-side subscriptions granted at connect over an emulated transport", "Initial publications of a server-side subscription granted at connect are delivered", "A server-side subscription missing after reconnect is unsubscribed" |
| 12b | As an app developer, I want subscriptions the server adds or removes during a connection tracked, so that my app sees the server's changes as they happen | "A subscription added by the server after connect is tracked", "A server-side subscription removed by the server is unsubscribed", "Server-side subscribed events carry the server's data" |
| 12c | As an app developer, I want server-side subscriptions recovered across reconnects and state invalidation, so that my app sees no gap in their streams | "Missed publications of a server-side subscription are recovered", "Server-side subscriptions survive an explicit disconnect and connect", "A subscription added by a push is recovered after reconnect", "A server-side subscription missing after reconnect is forgotten", "A pushed server-side subscription's position is used for recovery", "Recovered server-side publications advance the stored position", "A server-side unsubscribe carries its code and reason and forgets the subscription", and Block 11's "Connection state invalidation clears the token and invalidates every position" |

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

**Dependencies:** Blocks 8, 9, 12, 15.

**Out of scope:** publish debouncing (Block 0); batching several calls into one frame, planned as its own block after this one (owner decision, 2026-11-04: the scope is centrifuge-js parity, which includes its batching API).

**Decided while planning this block** (2026-11-06, through the reviewer the owner delegated such points to; the owner's decisions of 2026-11-04 and 2026-11-05 put them here):
- **join and leave:** a `join_leave` subscription option, sent as `SubscribeRequest.join_leave`, asks the server for join and leave pushes, delivered, each carrying the `ClientInfo`, as `SubscriptionEvent::Join` and `SubscriptionEvent::Leave`, and as `ServerSubscriptionEvent::Join` and `ServerSubscriptionEvent::Leave` for server-side subscriptions; a push for a subscription that is not subscribed is dropped; under Block 15 these events wait and are withdrawn like publications (spec, "ask server to push Join/Leave"; `client.proto`, `Push.join`, `Push.leave`; centrifuge-js 63ed73e `subscription.ts:1252-1262`, `centrifuge.ts:2556-2593`);
- **write queue:** `Config::write_queue_bytes`, default 4 MiB, at least 1 or `Client::new` returns an error, bounds the bytes of the application's calls (`send` included) waiting to be written, on every transport; a single larger command fits alone when none waits; a call that does not fit waits for room, bounded by `Config::timeout`, then fails with the timeout error (code 1) and is never sent, as a call waiting for the connection does; the client's own commands (connect, subscribe, unsubscribe, refresh, sub_refresh, pong) always go through, bounded by its state;
- **call replies:** a call's reply resolves the call when it is read, ahead of messages held by Block 15, so call results are not ordered with events (centrifuge-rust `docs/spec.md` §9.3); a call's deadline does not pause while reading is stopped, so a call never hangs; reading the close of a transport fails the calls already sent on it at once with "connection closed" (code 11);
- **sub-increments:** 13a, calls and RPC (the waiting, timeout and failure scenarios); 13b, publish, history, presence and presence stats, and subscription-level calls; 13c, `send`, the asynchronous `message` push and the Protobuf and binary round-trips; 13d, the write-queue bound; 13e, join and leave.

**Decisions:** `async fn` returning `Result` (Block 0); a call made while not connected waits for the connection, bounded by `Config::timeout`, also on a client whose `connect()` was never called, and entering `disconnected`, by `disconnect()` or a terminal condition (terminal disconnect code, denied token), fails at once every pending call, queued or already sent (centrifuge-js `_methodCall`, `_disconnect`, which rejects queued calls whenever it does not reconnect; sent calls: see Exceptions introduced); losing the transport fails calls already sent at once with "connection closed" (centrifuge-js error code 11), since their replies can no longer arrive; a subscription-level call made while the subscription is `subscribing` waits until it is `subscribed`, bounded by `Config::timeout`, made while it is `unsubscribed` fails at once with "subscription unsubscribed" (centrifuge-js error code 7) and sends nothing, and a waiting call fails the same way when the subscription becomes `unsubscribed` (centrifuge-js `_methodCall`, `_setUnsubscribed`); a command without a reply fails with the timeout error (code 1); a call that times out while waiting for the connection is dropped and never sent, since its command is built only once the connection is there (centrifuge-js `_methodCall`); a command error keeps the protocol error's code, message and `temporary` flag (`client.proto`, `Error`); RPC data is bytes both ways, like publication data (Block 0; `client.proto`, `RPCRequest.data`, `RPCResult.data`); history takes `limit`, `since` and `reverse` and returns the publications with the stream's current `epoch` and `offset`, and `limit: 0` returns only that position (spec, "Channel history API"); an asynchronous server `message` push is delivered to the application as a client event carrying its bytes (`client.proto`, `Push.message`; spec, client protocol "Asynchronous pushes").

`VERIFY:` the Centrifugo v6 RPC proxy request and response shape, for the @B fixtures. `send` is tested on @D only: Centrifugo does not handle it (centrifuge-js README: "only valid for Centrifuge server library for Go and does not work for Centrifugo server"; no send proxy in v6, verified).

Verified (Centrifugo v6.10.1, the @B RPC proxy, 2026-11-06): with `rpc.proxy.endpoint` and `rpc.without_namespace.proxy_enabled` (environment `CENTRIFUGO_RPC_PROXY_ENDPOINT`, `CENTRIFUGO_RPC_WITHOUT_NAMESPACE_PROXY_ENABLED`), Centrifugo posts a JSON client's RPC to the proxy with its `method` and its `data` as JSON (`b64data` in binary proxy mode, per the proxy documentation; not tested); the proxy answers `{"result": {"data": ...}}`, which the client returns as the call's result, equal as JSON, or `{"error": {"code", "message", "temporary"}}`, which reaches the client with its code, message and `temporary` flag, although the proxy documentation mentions `temporary` only for subscribe errors; the proxy documentation requires error codes in 400–1999.

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

**Exceptions introduced:** `disconnect()` fails the calls already sent with "disconnected" (code 3), as the scenario "Disconnect fails a call already sent at once" states, where centrifuge-js 63ed73e fails them with "connection closed" (code 11, `centrifuge.ts:1987`). Entering `disconnected` also fails a subscription-level call still waiting for its subscription to be subscribed with "disconnected" (code 3), as the rule for pending calls above requires, where centrifuge-js 63ed73e rejects only the client's own waiting calls (`centrifuge.ts:1947`, `:1989`, `:2011`): its subscriptions stay in, or move to, `subscribing` (`:963-966`), so such a call ends with the timeout error (code 1) unless its subscription is subscribed or unsubscribed first.

---

## Planned after Block 13 (owner decisions, 2026-11-04)

Each becomes a block with its value clause and scenarios when its turn comes; the owner sets the order.

- **centrifuge-js parity, client options and methods:** connect data with a `get_data` callback, and the server's data on the `connected` event (spec, "Client common options"); headers in the websocket upgrade request (spec); `setToken` and `setData` (centrifuge-js).
- **Batching API:** several commands in one frame (centrifuge-js batching API). It must keep Block 8's HTTP 413 rule, which relies on one command per emulation request: a request of several commands refused with 413 resends each command in its own request first, and only a command whose own request is refused gets the terminal outcome.
- **Validation harness:** seeded chaos against the fake server, checking invariants like I1–I8 of centrifugal/centrifuge-rust `docs/spec.md` §11.3, including "exactly once" across reconnects; read its §11.3 and §12 first. Replaying the traces of its TLA+ model is optional, later, keeping its MIT notice.

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
- the event-buffer policy for slow consumers (Block 15);
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
    When 2 minutes elapse without publications
    Then the CPU time used by the client is reported
```

## Block 15 — Event delivery and backpressure

**Goal:** a publication counts as delivered only when the application takes it from its stream, and an application that reads slowly holds the server back instead of growing the client's memory (review item C6/D1; owner decision, 2026-11-05, option 5, modelled on centrifuge-rust `docs/spec.md` §9.2 and §9.4).

**Dependencies:** Blocks 4, 8, 9, 11, 12, 14.

**Out of scope:**
- a callback-style handler API: ferrifuge has streams only;
- a read-ahead scenario over the HTTP streaming transports: the fake server speaks websocket only, so unit tests cover them;
- the size bound of the write queue (Block 13);
- whether call replies resolve when read, ahead of held events (Block 13; centrifuge-rust §9.3).

**Decisions** (owner decision, 2026-11-05: option 5, with hand-off delivery, a bounded read-ahead, one active stream per object, and timers and pings running while reading is paused; the remaining points decided through the review the owner delegated them to):
- **Delivered means taken.** A publication counts as delivered only when the `next()` of its stream returns it; only then does its stream position move, for client-side and server-side subscriptions alike (Blocks 11, 12). Exactly-once delivery follows on channels with history and recovery while the backlog stays within the server's recovery limit (Centrifugo `client.recovery_max_publication_limit`, default 300; past it recovery fails and `subscribed` reports `recovered` false).
- **In order, one message at a time.** Messages from the server are handled in order, and a message is handled only after every event of earlier messages, of any connection, has been taken or withdrawn. When a message is read, before it is handled, the client answers it if it is a ping and the connect reply asked for pongs, re-arms the no-ping deadline, and matches a reply to its command.
- **Bounded read-ahead.** Messages read but not yet handled wait in a read-ahead of at most a number of messages (default 256) and a number of bytes (default 4 MiB), two `Config` fields; each must be at least 1, or `Client::new` returns an error. A single message larger than the byte limit is admitted alone when the read-ahead is empty. When the read-ahead is full, the client stops reading the connection (the websocket, or the response stream of an HTTP streaming transport), so the server's slow-client handling acts: 3008 "slow" when its client queue (`client.queue_max_size`, 1 MiB by default) overflows, dropping what it had queued, or 3009 "write error" when a write times out (Centrifugo v6.10.1).
- **Timers keep running** while the application holds events, with two pauses. The no-ping deadline pauses while the client has stopped reading because the read-ahead is full, and restarts in full when reading resumes; without this pause an application stuck with a full read-ahead would make the client reconnect with code 2 about every 35 s, recovering a full read-ahead each time. A reply deadline (connect, refresh, subscribe, unsubscribe, sub_refresh) pauses while its reply has been read but waits to be handled.
- **One active stream per object.** The client and each subscription have one active stream. A second `events()` call ends the earlier stream, whose next item is its end, and every event not yet taken moves to the new stream, in order. `next()` is cancel-safe: a cancelled read loses no event.
- **No stream, no hold-up.** An object whose stream was never requested, or was dropped, takes its events at once: they count as delivered and positions move, events not yet taken included (centrifuge-rust §9.4: "a dropped stream acks like an unregistered handler"); so an application that needs every event, and exactly-once delivery on recoverable channels, takes the stream before calling connect() or subscribe(): events emitted before the first events() call count as delivered (documented, with the README examples in that order).
- **Lifecycle events** go through the same stream as publications, in order, and count as delivered only when taken; none is dropped or merged, and each state change is one event. `state()` returns the current state, which may be ahead of the stream. An event caused by an application call or a timer joins its stream at once.
- **Withdrawal.** A publication not yet taken when its subscription, client-side or server-side, leaves `subscribed` for a client-side reason (`disconnect()`, `unsubscribe()`, a client-side timeout) is withdrawn and never yielded; its position did not move, so recovery brings it back.
- **Errors past 256.** When a stream holds 256 events not taken, further `error` events for it are dropped, on the client's stream and on subscription streams alike: they change no state, and they are the only events a timer can add without bound (one per failed connection or subscribe attempt). Events caused by application calls are not bounded by the client: an application that cycles `connect()` and `disconnect()` without reading its stream grows it (documented).
- **Channels without recovery.** There a slow consumer can lose publications, as with any disconnect; this is documented, with no further signal.
- **Head-of-line.** The read-ahead belongs to the connection, so one stream not read pauses every subscription of that client (centrifuge-rust §9.2).
- **Versions 0.6.0 and 0.7.0:** 0.6.0 (15a) for the delivery acknowledgement in the public `machine` API and a second `events()` call ending the earlier stream; 0.7.0 (15b) for the two read-ahead fields of `Config`, since the semver check compares each pull request with its base and 0.6.0 is unreleased (as 12a went from 0.4.0 to 0.5.0).

Value clause: As an app developer on a device with limited memory, I want the client to hold the server back instead of buffering without limit when my code reads its events slowly, and to deliver each publication exactly once across reconnects, so that a slow or stalled consumer neither exhausts memory nor loses data on recoverable channels.

```gherkin
Feature: Event delivery and backpressure

  @D
  Scenario: Publications not taken before disconnect are recovered on the next connect
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the subscription's stream yields, in order, "subscribing", "subscribed", and the publications at offsets 2 and 3
    And no publication is yielded twice

  @D
  Scenario: Server-side publications not taken before disconnect are recovered on the next connect
    Given the fake server grants a recoverable server-side subscription to channel "alerts" at offset 0 at connect
    And the fake server has sent publications at offsets 1, 2 and 3 to "alerts"
    And the application has read the client's stream up to the server-side publication at offset 1, and no further
    And the client holds the server-side publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    Then that connect command asks to recover "alerts" from offset 1
    And the client's stream yields no server-side publication before "connected"

  @D
  Scenario: Publications not taken before unsubscribe are withdrawn
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When unsubscribe is called on the subscription
    Then the subscription's stream yields "unsubscribed" and no publication

  @D
  Scenario: The server closing the connection is handled only after the publications before it are taken
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1, 2 and 3 to "news", then closed the connection with a reconnect-advised code
    When 2 s elapse
    Then the client has opened no new connection
    And when the application reads the subscription's stream, it yields the publications at offsets 1, 2 and 3, then "subscribing"
    And the next subscribe command for "news" asks to recover from offset 3

  @D
  Scenario: A state change not taken holds the messages of the next connection
    Given a client whose stream was taken before connect is called and is not read, connected to the fake server
    When disconnect is called
    And connect is called
    Then within 1 s the fake server has received a second connect command
    And 1 s later the client's state is "connecting"
    And when the application reads the client's stream, it yields "connecting", "connected", "disconnected", "connecting", "connected"

  @D
  Scenario: A stream that is not read stops the client reading after 256 items
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    When the fake server starts sending 10000 publications of 100 bytes to "news"
    Then the client stops reading with 256 messages in its read-ahead
    And 1 s later its read-ahead still holds 256 messages
    And when the application reads the subscription's stream, it yields the 10000 publications in order, each once

  @D
  Scenario: The read-ahead also stops at 4 MiB
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    When the fake server starts sending 40 publications of 512 KiB to "news"
    Then the client stops reading with more than 3 MiB and at most 4 MiB in its read-ahead
    And 1 s later its read-ahead still holds at most 4 MiB
    And when the application reads the subscription's stream, it yields the 40 publications in order, each once

  @D
  Scenario: An item larger than the read-ahead budget is still delivered
    Given a client configured with a read-ahead of 1 KiB, subscribed to channel "news" on the fake server
    When the fake server sends a publication of 64 KiB to "news"
    Then the subscription's stream yields it

  @E
  Scenario Outline: A read-ahead limit of 0 is rejected
    Given a Config whose read-ahead limit of <limit> is 0
    When a client is created
    Then Client::new returns an error

    Examples:
      | limit    |
      | messages |
      | bytes    |

  @D
  Scenario: A second events() call takes over the subscription's stream
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When the application calls the subscription's events() again
    Then the first stream's next item is its end
    And the new stream yields the publications at offsets 1 and 2

  @D
  Scenario: A second events() call takes over the client's stream
    Given a client whose stream was taken before connect is called and is not read, connected to the fake server
    When the application calls the client's events() again
    Then the first stream's next item is its end
    And the new stream yields "connecting", then "connected"

  @D
  Scenario: A cancelled read loses no publication
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the application has started reading the subscription's stream
    And the client holds a publication to "news" not yet taken
    When the application cancels that read before polling it again
    Then the subscription's stream yields that publication

  @D
  Scenario: Publications of a subscription without a stream do not hold the connection
    Given a client subscribed to channels "news" and "chat" on the fake server
    And the application dropped the stream of "news" and reads the stream of "chat"
    When the fake server sends 1000 publications to "news", then one to "chat"
    Then the stream of "chat" yields that publication

  @D
  Scenario: Pings are answered while a stream is not read
    Given a client connected to the fake server, which pings every 1 s and expects pongs
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent 10 publications to "news"
    When 5 s elapse with the fake server pinging every 1 s
    Then the client has answered every ping
    And the client has not emitted "connecting" with code 2

  @D
  Scenario: A full read-ahead does not make the client report missing pings
    Given a client with maxServerPingDelay 1000 ms, connected to the fake server pinging every 1 s
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications of 100 bytes to "news" until the client stopped reading
    When 5 s elapse
    Then the client has not emitted "connecting" with code 2

  @D
  Scenario: A subscribe reply waiting behind a publication not taken does not time out
    Given a client with a timeout of 1 s, subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent a publication to "news"
    When the application subscribes to channel "chat"
    And 3 s elapse
    Then the client has not emitted "connecting" with code 3
    And when the application reads the stream of "news", the stream of "chat" yields "subscribed"

  @D
  Scenario: The connection token is refreshed while a stream is not read
    Given a client connected to the fake server with a connection token that expires in 2 s
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications of 100 bytes to "news" until the client stopped reading
    When 3 s elapse
    Then the fake server has received a refresh command

  @E
  Scenario: A client stream not read keeps every state change but drops errors past 256 untaken events
    Given a client whose stream was taken before connect is called and is not read, and whose server is unreachable
    When 1000 connection attempts fail
    And the server becomes reachable and the client connects
    Then reading the stream yields "connecting", 255 error events, then "connected"

  @E
  Scenario: A subscription stream not read drops errors past 256 untaken events
    Given a connected client with a subscription whose stream was taken before subscribe is called and is not read, and whose get_data callback fails
    When 1000 subscribe attempts fail
    And the get_data callback succeeds and the subscription subscribes
    Then reading the subscription's stream yields "subscribing", 255 error events, then "subscribed"

  @A
  Scenario: A slow consumer on a channel with history and recovery loses no publication
    Given a server whose channel "news" keeps a history of 8000 publications and recovers up to 8000, with a client queue of 64 MiB
    And a client subscribed to "news", whose subscription's stream has been read up to "subscribed" and is not read after
    When the server API publishes 8000 publications of 4 KiB to "news"
    And 2 s elapse
    And the application then reads the subscription's stream until it has 8000 publications
    Then they are the offsets 1 to 8000, in order, each exactly once
    And between them the stream yielded "subscribing", then "subscribed" with recovered true
```
