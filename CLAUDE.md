# CLAUDE.md — Rust Centrifugo client SDK

Status: DRAFT — blocks 0–20 of N. Further blocks to be appended.

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
| Components (owner decision, 2026-11-06) | For every build, not only FedRAMP. When a maintained component exists it is used instead of writing one: anything that parses or encodes network input, protocol handshakes and framing, codecs, delta formats and cryptography. When only unmaintained components exist, the component is written from scratch instead of depending on one. Maintained means published on crates.io, a release or a commit in the last 12 months, a public repository, and passing cargo-deny and RustSec like every dependency. A component written in-house gets a fuzz target run in CI like the decoder fuzz targets (Block 14), explicit bounds on input and output sizes so that hostile input cannot exhaust memory, and no unsafe code (Block 0 lints). Cryptography is never written in-house: it stays in rustls and aws-lc, and a cryptographic need with no maintained component goes to the owner. The Centrifugo client logic itself stays built from scratch (first row) | An unmaintained component is a FedRAMP risk, as no one ships its security fixes |

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
- the attempt counter increments each time a delay is computed and resets to 0 when a connect reply arrives over websocket; Ferrifuge undoes that reset for a connection the server closes with 3008 "slow" before a server ping is read on it (Block 15, "Backoff after 3008"; diverges from centrifuge-js on purpose);
- `connect()` resets the attempt counter to 0 (centrifuge-js 63ed73e, `centrifuge.ts:479`);
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

**Failed TLS setup (review item C7):** the TLS settings, which today include the platform's root certificates, are built at the first secure connection attempt and shared by every client in the process, as are the HTTP clients built from them; a build that fails is not kept, so the next secure attempt (each transport tried) builds again, and a transient failure (a CA bundle being replaced, no file descriptor left) heals without a restart. A read of the platform's root certificates that reports errors (an unreadable or malformed file, a store or trust setting the system fails to open) still serves the attempt when it found a certificate, but neither it nor the HTTP clients built from it are kept, so a later secure attempt reads again; a read without errors is kept. A file truncated without an error is not detected.

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

**Out of scope:** publish debouncing (Block 0); batching several calls into one frame, Block 18 (owner decision, 2026-11-04: the scope is centrifuge-js parity, which includes its batching API).

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

  @E
  Scenario: A call that does not fit the write queue waits until earlier calls are written
    Given a connected client with a write queue of 1000 bytes
    And a publish of 800 bytes sent and not yet written
    When the client publishes 400 bytes
    Then no command is sent for it
    When the transport reports the first publish written
    Then the second publish is sent

  @E
  Scenario: A call waiting for room fails after the timeout and is never sent
    Given a connected client with a write queue of 1000 bytes
    And a publish of 800 bytes sent and not yet written
    When the client publishes 400 bytes
    And the configured timeout elapses
    Then the call fails with a timeout error
    When the transport reports the first publish written
    Then no command is sent for it

  @E
  Scenario: A call larger than the write queue is sent once nothing waits to be written
    Given a connected client with a write queue of 1000 bytes
    And a publish of 100 bytes sent and not yet written
    When the client publishes 4000 bytes
    Then no command is sent for it
    When the transport reports the first publish written
    Then the 4000-byte publish is sent

  @E
  Scenario: The client's own commands are not held by a full write queue
    Given a connected client with a write queue of 1000 bytes, whose server expects pongs
    And a publish of 800 bytes sent and not yet written
    And a publish of 400 bytes waiting for room
    When the server sends a ping
    Then the client sends a pong at once

  @E
  Scenario: Calls are admitted in the order they were made
    Given a connected client with a write queue of 1000 bytes
    And a publish of 800 bytes sent and not yet written
    When the client publishes 400 bytes and then 100 bytes
    Then no command is sent for either
    When the transport reports the first publish written
    Then the 400-byte publish is sent before the 100-byte one

  @E
  Scenario: A call behind one that stopped waiting is sent when it fits
    Given a connected client with a write queue of 1000 bytes
    And a publish of 800 bytes sent and not yet written
    And a publish of 400 bytes waiting for room
    When the client publishes 100 bytes
    And the 400-byte publish is abandoned
    Then the 100-byte publish is sent

  @E
  Scenario: A call waiting for room keeps its deadline when the transport is lost
    Given a connected client with a write queue of 1000 bytes
    And a publish of 800 bytes sent and not yet written
    And a publish of 400 bytes waiting for room
    When the transport is lost
    Then the first publish fails with a "connection closed" error
    When the configured timeout elapses since the 400-byte publish was made, the client still connecting
    Then the 400-byte publish fails with a timeout error
    When the client is connected again
    Then no command is sent for it

  @E
  Scenario: A write queue limit of 0 is rejected
    Given a Config whose write queue limit is 0
    When a client is created
    Then Client::new returns an error

  @D
  Scenario: Sends beyond the write queue go out as earlier ones are written
    Given a client with a write queue of 1 byte connected to the fake server
    When the client sends {"n":1}, {"n":2} and {"n":3} without awaiting them
    Then every send completes before the configured timeout
    And the fake server received the 3 send commands in that order

  @A
  Scenario: Join and leave of other clients are delivered with the join_leave option
    Given a client subscribed with the join_leave option to a channel with join and leave events
    When a second client subscribes to the channel
    Then the subscription emits a "join" event naming the second client
    When the second client unsubscribes from the channel
    Then the subscription emits a "leave" event naming the second client

  @A
  Scenario: Without the join_leave option no join or leave event is delivered
    Given a client subscribed without the join_leave option to a channel with join and leave events
    And a second client subscribed to the channel with the join_leave option
    When a third client subscribes to the channel and then unsubscribes
    And the second client's subscription has emitted a "leave" event naming the third client
    Then the first client's subscription has emitted no "join" or "leave" event

  @D
  Scenario: The join_leave option is sent on subscribe and on resubscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the join_leave option is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each requesting join_leave

  @D
  Scenario: Join and leave pushes of a server-side subscription are delivered
    Given the fake server grants a server-side subscription to channel "alerts" at connect
    And a client connected to the fake server
    When the fake server pushes a join and then a leave of different clients for channel "alerts"
    Then the client emits a server-side "join" event for channel "alerts" with the pushed client info
    And then the client emits a server-side "leave" event for channel "alerts" with the pushed client info

  @D
  Scenario: A join push for a subscription that is not subscribed is dropped
    Given a client connected to the fake server with an unsubscribed subscription to channel "news"
    When the fake server pushes a join for channel "news"
    And the subscription is subscribed
    And the fake server pushes a leave for channel "news"
    Then the subscription's stream yields "subscribing", "subscribed" and "leave", and no "join" event

  @D
  Scenario: Join and leave events not taken before unsubscribe are withdrawn
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has pushed a join and a leave for channel "news"
    And the client holds them not yet taken
    When unsubscribe is called on the subscription
    Then the subscription's stream yields "unsubscribed" and no "join" or "leave" event
```

**Exceptions introduced:** `disconnect()` fails the calls already sent with "disconnected" (code 3), as the scenario "Disconnect fails a call already sent at once" states, where centrifuge-js 63ed73e fails them with "connection closed" (code 11, `centrifuge.ts:1987`). Entering `disconnected` also fails a subscription-level call still waiting for its subscription to be subscribed with "disconnected" (code 3), as the rule for pending calls above requires, where centrifuge-js 63ed73e rejects only the client's own waiting calls (`centrifuge.ts:1947`, `:1989`, `:2011`): its subscriptions stay in, or move to, `subscribing` (`:963-966`), so such a call ends with the timeout error (code 1) unless its subscription is subscribed or unsubscribed first.

---

## Planned after Block 13 (owner decisions, 2026-11-04)

The order is decided (reviewer, 2026-11-06, delegated by the owner): Block 15d (backoff after 3008), then the validation harness, then centrifuge-js parity, then the batching API. Each becomes a block with its value clause and scenarios when its turn comes.

- **centrifuge-js parity, client options and methods:** connect data with a `get_data` callback, and the server's data on the `connected` event (spec, "Client common options"); headers in the websocket upgrade request (spec); `setToken` and `setData` (centrifuge-js). Specified as Block 17.
- **Batching API:** several commands in one frame (centrifuge-js batching API). It must keep Block 8's HTTP 413 rule, which relies on one command per emulation request: a request of several commands refused with 413 resends each command in its own request first, and only a command whose own request is refused gets the terminal outcome. Specified as Block 18.
- **Validation harness:** seeded chaos against the fake server, checking invariants like I1–I8 of centrifugal/centrifuge-rust `docs/spec.md` §11.3, including "exactly once" across reconnects; read its §11.3 and §12 first. Replaying the traces of its TLA+ model is optional, later, keeping its MIT notice. Specified as Block 16.

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
- **Recovery larger than the server's queue.** A recovery reply (a subscribe reply, or a connect reply recovering server-side subscriptions) is queued whole, so one larger than `client.queue_max_size` is answered with 3008 "slow" however fast the client reads, and the queued reply is dropped (Centrifugo v6.10.1: centrifuge `writer.go` `enqueue`, `client.go` `writeEncodedCommandReply`; a 3008 close does not flush the queue). Each reconnect asks for the same backlog, so the client keeps reconnecting, its backoff growing up to `max_reconnect_delay` ("Backoff after 3008"; a connect reply that is itself dropped resets nothing), until the backlog passes `client.recovery_max_publication_limit` or the history expires. The client cannot ask for a smaller batch (`client.proto`, `SubscribeRequest`); the README tells operators to keep `client.queue_max_size` above `client.recovery_max_publication_limit` × the largest publication, summed over the server-side subscriptions one connect recovers.
- **Backoff after 3008.** A connection the server closes with 3008 "slow", by close code or by Disconnect push, before the client has read a server ping on it, does not reset the reconnect backoff: the next reconnect delay is computed from the attempt counter as it stood when that connection's connect reply arrived, as if neither the connect reply nor a reply to a command had reset it (Block 2). A server ping read on the connection lets the reset stand, as does a close with any other code. With a server that sends no pings (`ConnectResult.ping` 0), every 3008 close keeps the backoff growing, up to `max_reconnect_delay`. The reconnect jitter window (Block 14) still applies to the first reconnect after each connection; stream positions and recovery are unchanged. (Reviewer decision, 2026-11-06, delegated by the owner. Diverges from centrifuge-js 63ed73e, which resets the counter on every connect reply over websocket, `centrifuge.ts:2308-2320`, in the spirit of its own deferred reset for emulated transports, which keeps a client failing right after each connect from reconnecting "at the minimum delay forever". The spec does not say when the counter resets.)
- **Timers keep running** while the application holds events, with two pauses. The no-ping deadline pauses while the client has stopped reading because the read-ahead is full, and restarts in full when reading resumes; without this pause an application stuck with a full read-ahead would make the client reconnect with code 2 about every 35 s, recovering a full read-ahead each time. A reply deadline (connect, refresh, subscribe, unsubscribe, sub_refresh) pauses while its reply has been read but waits to be handled.
- **One active stream per object.** The client and each subscription have one active stream. A second `events()` call ends the earlier stream, whose next item is its end, and every event not yet taken moves to the new stream, in order. `next()` is cancel-safe: a cancelled read loses no event.
- **No stream, no hold-up.** An object whose stream was never requested, or was dropped, takes its events at once: they count as delivered and positions move, events not yet taken included (centrifuge-rust §9.4: "a dropped stream acks like an unregistered handler"); so an application that needs every event, and exactly-once delivery on recoverable channels, takes the stream before calling connect() or subscribe(): events emitted before the first events() call count as delivered (documented, with the README examples in that order).
- **Lifecycle events** go through the same stream as publications, in order, and count as delivered only when taken; none is dropped or merged, and each state change is one event. `state()` returns the current state, which may be ahead of the stream. An event caused by an application call or a timer joins its stream at once.
- **Withdrawal.** A publication not yet taken when its subscription, client-side or server-side, leaves `subscribed` for a client-side reason (`disconnect()`, `unsubscribe()`, a client-side timeout) is withdrawn and never yielded; its position did not move, so recovery brings it back.
- **Errors past 256.** When a stream holds 256 events not taken, further `error` events for it are dropped, on the client's stream and on subscription streams alike: they change no state, and they are the only events a timer can add without bound (one per failed connection or subscribe attempt). Events caused by application calls are not bounded by the client: an application that cycles `connect()` and `disconnect()` without reading its stream grows it (documented).
- **Channels without recovery.** There a slow consumer can lose publications, as with any disconnect; this is documented, with no further signal.
- **Head-of-line.** The read-ahead belongs to the connection, so one stream not read pauses every subscription of that client (centrifuge-rust §9.2).
- **Versions 0.6.0 and 0.7.0:** 0.6.0 (15a) for the delivery acknowledgement in the public `machine` API and a second `events()` call ending the earlier stream; 0.7.0 (15b) for the two read-ahead fields of `Config`, since the semver check compares each pull request with its base and 0.6.0 is unreleased (as 12a went from 0.4.0 to 0.5.0).

**Sub-increments** (one branch and PR each, in this order):

| Id | Scope | Scenarios |
|---|---|---|
| 15a | delivered means taken, withdrawal, one active stream | "Publications not taken before disconnect are recovered on the next connect", "Server-side publications not taken before disconnect are recovered on the next connect", "Publications not taken before unsubscribe are withdrawn", "A second events() call takes over the subscription's stream", "A second events() call takes over the client's stream", "A cancelled read loses no publication", "Publications of a subscription without a stream do not hold the connection" |
| 15b | in-order handling, bounded read-ahead, timers while held | "The server closing the connection is handled only after the publications before it are taken", "A state change not taken holds the messages of the next connection", "A stream that is not read stops the client reading after 256 items", "The read-ahead also stops at 4 MiB", "An item larger than the read-ahead budget is still delivered", "A read-ahead limit of 0 is rejected", "Pings are answered while a stream is not read", "A full read-ahead does not make the client report missing pings", "A subscribe reply waiting behind a publication not taken does not time out", "The connection token is refreshed while a stream is not read", "A slow consumer on a channel with history and recovery loses no publication" |
| 15c | errors past 256 | "A client stream not read keeps every state change but drops errors past 256 untaken events", "A subscription stream not read drops errors past 256 untaken events" |
| 15d | backoff after 3008 | "Reconnect delays keep growing while the server closes each connection with 3008", "A Disconnect push with 3008 keeps the backoff growing over an emulated transport", "A server ping read before 3008 lets the reset stand", "A close with another code lets the reset stand" |

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

  @E
  Scenario Outline: Reconnect delays keep growing while the server closes each connection with 3008
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes <closes> connections in a row with code 3008 "slow", each after its connect reply and the subscribe reply for "chat", before any server ping
    And the reconnect delay after the last of them is computed 1000 times
    Then every computed delay is at most <bound> ms
    And the largest computed delay is more than <above> ms

    Examples:
      | closes | above | bound |
      | 2      | 1000  | 1500  |
      | 3      | 1500  | 2500  |
      | 4      | 2500  | 4500  |
      | 5      | 4500  | 8500  |
      | 6      | 8500  | 16500 |
      | 7      | 16500 | 20000 |

  @E
  Scenario: A Disconnect push with 3008 keeps the backoff growing over an emulated transport
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over http_stream
    When the server ends 4 connections in a row with a Disconnect push of code 3008 "slow", each after its connect reply and the subscribe reply for "chat", before any server ping
    And the reconnect delay after the last of them is computed 1000 times
    Then every computed delay is at most 4500 ms
    And the largest computed delay is more than 2500 ms

  @E
  Scenario: A server ping read before 3008 lets the reset stand
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes 5 connections in a row with code 3008 "slow", each after its connect reply, before any server ping
    And the next connection reads a server ping after its connect reply, then the server closes it with code 3008 "slow"
    And the reconnect delay after that close is computed 1000 times
    Then every computed delay is at most 1000 ms

  @E
  Scenario: A close with another code lets the reset stand
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes 5 connections in a row with code 3008 "slow", each after its connect reply, before any server ping
    And the server closes the next connection with code 3001 "shutdown" after its connect reply, before any server ping
    And the reconnect delay after that close is computed 1000 times
    Then every computed delay is at most 1000 ms
```

---

## Block 16 — Validation harness

**Goal:** seeded random-operations runners drive the client through connects, disconnects, subscriptions, calls, slow reading and server faults, and check its invariants after every step and at quiescent points, so that a regression in exactly-once delivery, liveness or cleanup fails CI with the seed that found it (owner decision, 2026-11-04, "Planned after Block 13"; modelled on centrifugal/centrifuge-rust `docs/spec.md` §11.3).

**Dependencies:** Blocks 1–15, 15d included.

**Out of scope:**
- replaying the traces of centrifuge-rust's TLA+ model (optional, later, keeping its MIT notice);
- a chaos TCP proxy against a real Centrifugo (centrifuge-rust §11.4);
- the emulated transports and Protobuf in the runners: the core runner uses websocket and JSON, and the fake server speaks websocket only (Block 15);
- delta compression, map and shared-poll subscriptions (Block 0);
- performance (Block 14);
- changes to the client's behaviour: a bug the runners find is fixed in its own pull request.

**Decisions** (reviewer, 2026-11-06, delegated by the owner):
- **Two runners.**
  - The **core runner** drives the sans-IO `Machine`, seeded with `Machine::with_seed`, against a server simulated in the test, on a simulated clock: no network, no real time. A seed fixes every choice, so it gives the same trace on every run and platform. It reads frames only while `Machine::wants_frames()` is true, as the adapter does.
  - The **adapter runner** drives the tokio `Client` against the fake server (`tests/support/fake_server.rs`) in real time. A seed fixes the operations, not their timing: a failure is replayed, not always reproduced. It covers what the core cannot: the driver's tasks, hand-off delivery, a second `events()` taking over, cancel-safe `next()`. The fake server gains the controls it needs (connect errors, history that expires or passes the recovery limit).
- **Operations** (chosen by the seed): `connect`, `disconnect`; add, `subscribe`, `unsubscribe` and remove a subscription; calls (rpc, publish, history, presence); the application taking the next event, holding its events, opening and dropping a stream, and in the adapter a second `events()` and a cancelled read; advancing the simulated clock.
- **Server behaviour** (chosen by the seed): numbered publications on recoverable channels with history, live and recovered; answers, no answer, late answers; temporary and permanent errors, 109 and 112 included; closes with reconnect-advised and terminal codes, 3008 and 3014 included; disconnect and unsubscribe pushes; server-side subscriptions granted at connect, added and removed; history that expires or passes the recovery limit, so recovery fails; server pings, and gaps in them. 3008 is a seeded close, never the result of a modelled client queue, so the loop of "Recovery larger than the server's queue" (Block 15) does not defeat I4.
- **Chaos configuration:** reconnect and resubscribe delays from 50 ms to 500 ms, `Config::timeout` 1 s, a read-ahead of 8 messages and 4 KiB so that it fills; other options default.
- **Invariants** (centrifuge-rust §11.3, adapted: ferrifuge has streams, no handlers). "Healed": the last fault is over, the server answers every command and pings, the application takes every event, and, when the client is `disconnected` (after `disconnect()` or a terminal condition), the application calls `connect()`.
  - I1 state chain: each state event of the client or a subscription differs from the previous one of that object and never goes from `disconnected` to `connected` or from `unsubscribed` to `subscribed`; once every event is taken, `state()` equals the last state event. Core: both; adapter: the first.
  - I2 no panic: core, in the step; adapter, on any thread, the driver's tasks included, recorded by a panic hook.
  - I3 at most one open transport per client: core, after every step; adapter, at quiescence (`FakeServer::connections`).
  - I4 liveness: within 3 s of healing, the `connect()` call included, the client is `connected` and every subscription not `unsubscribed` is `subscribed`. Core; the adapter reports I4 when quiescence is not reached within 10 s.
  - I5 cleanup: after `disconnect()` and until the next `connect()`, no transport is open or opened, no client timer runs, no subscription timer is set, and no token, data or state fetch starts; only the timers of calls made since run (Block 13), and a resubscribe backoff armed before `disconnect()`, which may elapse but starts nothing; a later `connect()` subscribes it only once its delay has elapsed (Block 10; centrifuge-js 63ed73e `centrifuge.ts` `_sendSubscribeCommands`, `subscription.ts` `_scheduleResubscribe`, `_subscribe`). Core; adapter: no new connection and no callback call at quiescence.
  - I6 every call settles: each of its waits (for its subscription or the connection, for room, for its reply) ends within `Config::timeout` (Block 13), so it settles within 3 × `Config::timeout`: exactly in the core, plus 1 s in the adapter.
  - I7 no duplicate `connected` or `subscribed` without a state change between: part of I1, since each state change is one event (Block 15).
  - I8 exactly once: per recoverable subscription, client-side and server-side, the offsets delivered continue without gap or duplicate across reconnects, resubscribes and `unsubscribe()`/`subscribe()`; they restart only after `subscribed` with `recovered` false or a getState reload. Delivered means taken, or emitted while no stream is open (Block 15). A removed and added subscription is a new one. Core; adapter: on subscriptions whose stream is never dropped.
  - I9 read-ahead (Block 15): after every step, `Machine::wants_frames()` is false whenever `Machine::read_ahead()` has reached the message or byte limit, and `Action::Withdraw` names only events emitted and not yet taken. Core.
- **Checking the checker.** Each invariant but I2 and I7 has a planted fault, in the simulated server, the application model or the tap between the machine and the checker, never in the client, that the pull-request seeds must report. It proves that the checker fires and that the seeds reach the state it watches.
- **Run counters.** Each run counts every operation and server behaviour, and the outcomes its scenario lists; a count of 0 fails the run.
- **Reproduction.** A failure prints the seed, the invariant and the operations up to it; `FERRIFUGE_CHAOS_SEED=<seed>` reruns that seed alone. A seed that found a bug joins the pull-request seeds with its fix. The core trace depends only on the seed: no wall clock, no unseeded randomness, nothing ordered by hash-map iteration.
- **CI.** Core scenarios: seeds 0–299 × 60 operations in the `test` job and the portability job (Windows, macOS) of every pull request; on main and nightly, 10 000 seeds × 200 operations in release, from a start seed drawn per run and printed. Adapter: seeds 0–19 × 30 operations in the `test` job of every pull request; on main and nightly, 500 seeds on Linux and seeds 0–19 on Windows and macOS.
- **Public API:** none added. A hook the runners need is gated by `test-util` and `#[doc(hidden)]`, as `Client::read_ahead` is, outside the semver check (`--default-features`).
- **Test layer:** `@P` (Block 0), each scenario claimed by its runner with `// @P scenario:`.

**Sub-increments** (one branch and PR each, in this order):

| Id | Scope | Scenarios |
|---|---|---|
| 16a | core runner without subscriptions: connection, calls, the client's stream, server faults, clock; I1–I7; reproduction; CI | "Seeded chaos on the core keeps the connection invariants", "A seed of the core runner reproduces its run", "The core runner reports a planted connection fault" |
| 16b | client-side and server-side subscriptions, recovery, streams and read-ahead in the core runner; I8, I9 | "Seeded chaos with subscriptions on the core keeps every invariant", "The core runner reports a planted delivery fault" |
| 16c | adapter runner, tokio client and fake server, with the fake server's new controls | "Seeded chaos on the tokio client keeps the invariants", "The adapter runner reports a planted fault" |

Value clause: As a maintainer shipping ferrifuge to large fleets of end-user devices, I want seeded random-operations runners to check the client's invariants under chaos, so that a regression in exactly-once delivery, liveness or cleanup fails CI, with a seed that reproduces it when the sans-IO core is at fault.

```gherkin
Feature: Validation harness

  @P
  Scenario: Seeded chaos on the core keeps the connection invariants
    Given the core runner with the chaos configuration and no subscriptions
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I7
    And the run counted every operation and server behaviour, reconnects and call timeouts, none of them 0

  @P
  Scenario: A seed of the core runner reproduces its run
    Given seed 7 of the core runner
    When it runs twice in one process
    Then both traces are equal, each holding 60 operations and every input and action of the machine
    And seed 8 gives a different list of operations

  @P
  Scenario Outline: The core runner reports a planted connection fault
    Given the core runner without subscriptions, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it
    And rerunning that seed alone with FERRIFUGE_CHAOS_SEED reports the same violation at the same operation

    Examples:
      | fault                                                                     | invariant |
      | a state event of the client dropped before the checker                    | I1        |
      | a transport the client closed kept open by the simulated network          | I3        |
      | a simulated server that ignores connect commands from healing on          | I4        |
      | a timer the client cancelled after disconnect kept by the simulated clock | I5        |
      | a call outcome dropped before the checker                                 | I6        |

  @P
  Scenario: Seeded chaos with subscriptions on the core keeps every invariant
    Given the core runner with the chaos configuration and client-side and server-side subscriptions to recoverable channels
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I9
    And the run counted every operation and server behaviour, resubscribes, recoveries with recovered true and with recovered false, full read-aheads and withdrawals, none of them 0

  @P
  Scenario Outline: The core runner reports a planted delivery fault
    Given the core runner with subscriptions, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                           | invariant |
      | a subscribe reply that recovers with an offset skipped                          | I8        |
      | a connect reply that recovers a server-side subscription with an offset skipped | I8        |
      | a publication duplicated before the checker                                     | I8        |
      | a state event of a subscription dropped before the checker                      | I1        |
      | wants_frames() reported true to the checker while the read-ahead is full        | I9        |

  @P
  Scenario: Seeded chaos on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a tokio client and the fake server
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted reconnects, recoveries with recovered true, second events() calls and cancelled reads, none of them 0

  @P
  Scenario: The adapter runner reports a planted fault
    Given the adapter runner with a fake server whose recovery skips an offset
    When seeds 0 to 19 run
    Then the runner reports a violation of I8, naming the seed and the operations up to it
```

**Exceptions introduced:** none.

---

## Block 17 — centrifuge-js parity: connect data, headers, set_token, set_data

**Goal:** an application sends its own data and headers with each connection attempt, fixed or from a callback, replaces the token, the data and the emulated headers used by later attempts, and reads the data the server returns at connect (owner decision, 2026-11-04, "Planned after Block 13"; order decided 2026-11-06).

**Dependencies:** Blocks 1, 5 (token component), 8 (transports), 9 (data callbacks, `DataError`), 10 (token before data), 15 (event delivery).

**Out of scope:**
- replacing the HTTP headers of a built client (no `set_http_headers`): neither the spec nor centrifuge-js has it;
- changing the current connection: every option and setter here applies from the next connection attempt and sends no command;
- a timeout on the `get_data` callback (none in centrifuge-js, none in Block 9);
- the other `ConnectRequest` and `ConnectResult` fields of the vendored `client.proto` (`flag`, `profile`, `dict`, `time`);
- a cookie jar: a native client has none; an application sets `Cookie` as an HTTP header.

**Decisions** (reviewer, 2026-11-06, delegated by the owner):
- **Connect data.** `Config::data: Option<Vec<u8>>`, raw JSON with JSON, any bytes with Protobuf, encoded as subscription data is (Block 9), is sent as `ConnectRequest.data` with every connect command (spec, "Client common options": "option to set connect data"; `client.proto`, `ConnectRequest.data`). `ClientBuilder::get_data` sets an async callback returning `Result<Vec<u8>, DataError>` (Block 9's type); when set, it is called before every connection attempt, each transport of a fallback sequence included, and its result is sent instead of `data` (centrifuge-js 63ed73e `_startReconnecting`). It runs after the reconnect delay, never before it, so Block 14's jitter spreads a fleet's callback calls as it spreads its connects.
- **Order with the token.** As Block 10 for subscriptions: when the attempt needs a token, the token callback runs first and `get_data` only once a token is obtained, so a denied, failed or empty token calls no `get_data` for that attempt; an attempt that needs no token calls `get_data` without the token callback (centrifuge-js `_startReconnecting`).
- **get_data failures.** `DataError::Unauthorized` disconnects the client with code 1 "unauthorized", with no error event and no reconnect (centrifuge-js `_handleGetDataError`, `_failUnauthorized`). `DataError::Failed` emits an `error` event of the new kind `ErrorKind::ConnectData`, code 12 "bad configuration", carrying the callback's message, and retries after the reconnect backoff (Block 2) while the client stays `connecting` (centrifuge-js `_handleGetDataError`: type `connectData`, `errorCodes.badConfiguration`). A `disconnect()` while the callback runs abandons it: its answer is ignored and opens no transport (centrifuge-js `_attemptAborted`; Block 16 I5). `DataError`'s documentation and `Display` stop naming subscriptions only.
- **Server data at connect.** `ClientEvent::Connected` gains `data: Option<Vec<u8>>`: `ConnectResult.data` exactly as received, never re-serialized, as `SubscribeResult.data` on `subscribed` (Block 9) (`client.proto`, `ConnectResult.data`; centrifuge-js `_connectResponse`, `ctx.data`; the spec does not mention it).
- **Two kinds of headers**, named after the Centrifugo proxy options that forward them, which v6.9.0 split: v6.10.1 forwards an emulated value only when its name is in `emulated_headers`, never through `http_headers` (`internal/proxy/http.go`). No option is called just `headers`, a word meaning real headers in the spec and emulated ones in centrifuge-js (centrifuge-rust `docs/spec.md` §5.1, decision 12). Both are in scope as centrifuge-js parity (its `headers` option and `setHeaders` method), as the owner read "centrifuge-js parity" for the batching API (Block 13).
  - `ClientBuilder::http_headers`: real HTTP headers on the websocket upgrade request (spec, "Client common options") and on every HTTP request of `http_stream` and sse, the stream request and the emulation requests, so that a gateway checking a header still admits a client that fell back (Block 8). Names are case-insensitive, one value each, a later value replacing an earlier one. A name or value that is not valid HTTP, or a header the transport sets (`Host`, `Connection`, `Upgrade`, `Content-Length`, `Transfer-Encoding`, `Content-Type`, `Accept`, any `Sec-WebSocket-*`), fails `build()` with a configuration error naming the header, never its value.
  - `ClientBuilder::emulated_headers`: a name-to-value map sent as `ConnectRequest.headers` with every connect command, on every transport (`client.proto`; centrifuge-js `headers`; spec feature matrix, "headers emulation"). Names and values must be valid HTTP, since the server forwards them as HTTP headers; otherwise `build()` fails as above. They are client-controlled input to the server's proxies, as the Centrifugo proxy documentation warns.
- **Setters.** `Client::set_token`, `Client::set_data` and `Client::set_emulated_headers` replace the value used from the next connection attempt and send nothing (centrifuge-js `setToken`, `setData`, `setHeaders`).
  - `set_token`: without a token callback it also clears a pending "token expired" (error 109 to a connect), so the next attempt connects with the new token instead of failing with the configuration error and code 1; with a callback, a pending "token expired" still makes the next attempt call it, since the token set may be the expired one; a 109 answering a connect that carried an older token than the one set leaves nothing pending (centrifuge-js `setToken`, `_connectError`; Block 5's token component already compares them). A connected client's refresh still needs the token callback (Block 1). An empty token clears it: the next attempt carries none, or calls the callback when there is one.
  - `set_data`: with `get_data` set, the callback's result still replaces it on every attempt (centrifuge-js `setData`).
- **Secrets in `Debug`.** The `Debug` output of `Config`, `ClientBuilder` and the `machine` inputs and actions shows connect data as its length (as `SubscriptionOptions` does), headers by name without their values, and never a token.
- **Sans-IO API.** `Machine::with_data_source` (as `with_token_source`), `Action::FetchConnectData`, `Action::CancelConnectData`, `Input::ConnectDataFetched`, `Machine::with_emulated_headers`, and an input per setter. HTTP headers stay in the adapter.
- **Public API and version.** 17a adds a field to `Config` and to `ClientEvent::Connected` and variants to `ErrorKind`, `Input` and `Action`: breaking, version 0.11.0. The same release marks `ErrorKind`, `Input` and the `Connected` variant `#[non_exhaustive]`, so that later fields and variants are additions; `Action` stays exhaustive, so that an owner's `match` fails to compile when a new action needs performing. 17b and 17c only add items.
- **Fixtures.**
  - The @B connect proxy records each request's HTTP headers and can answer with result `data`.
  - The @B server forwards `x-device-real` through `http_headers` and `x-device-emulated` through `emulated_headers` (`CENTRIFUGO_CLIENT_PROXY_CONNECT_HTTP_HEADERS`, `CENTRIFUGO_CLIENT_PROXY_CONNECT_EMULATED_HEADERS`, space-separated lists), so each header scenario fails when its header travels the other way.
  - The HTTP proxy (Block 8) records the headers of each request it relays.
  - The fake server answers connects with given raw data, and the next connect with a given error.

Verified (Centrifugo v6.10.1, the 17b @B scenarios, 2026-11-06): the two environment variables above take space-separated header names, and the connect proxy receives the listed `http_headers` of an `http_stream` or sse stream request as it does those of a websocket upgrade.

**Sub-increments** (one branch and PR each, in this order):

| Id | Scope | Scenarios |
|---|---|---|
| 17a | connect data, `get_data`, server data on `connected`; version 0.11.0 | "Connect data is sent on connect and on reconnect", "Connect data from a callback is renewed on every attempt", "A failing connect data callback is retried after the reconnect backoff", "A connect data callback denying access disconnects the client", "A disconnect while the data callback runs opens no transport", "The connect data callback runs only after a connection token is obtained", "The connection token is fetched before the connect data", "A client that has a token calls the data callback without the token callback", "Data returned with the connect reply is delivered", "The connected event carries the server's data exactly" |
| 17b | `http_headers`, `emulated_headers` | "HTTP headers reach the connect proxy over each transport", "HTTP headers ride on every request of an emulated transport", "Emulated headers reach the connect proxy", "A header the transport sets is rejected", "Client options do not show secrets" |
| 17c | `set_token`, `set_data`, `set_emulated_headers` | "set_token replaces an expired token when there is no token callback", "set_token does not skip the token callback after an expired token", "set_data applies from the next connection attempt", "set_emulated_headers applies from the next connection attempt" |

Value clause: As an app developer shipping ferrifuge to fleets of devices, I want to send data and headers with each connection attempt, replace them and the token between attempts, and read the data the server returns at connect, so that my connect proxy and gateways can identify and authorize each device on every connect and reconnect, over every transport.

```gherkin
Feature: Connect data, headers, set_token and set_data

  @B
  Scenario: Connect data is sent on connect and on reconnect
    Given a connect proxy recording requests
    And a client with connect data {"device":"d1"}
    When connect is called
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy recorded data {"device":"d1"} on both connect requests

  @B
  Scenario: Connect data from a callback is renewed on every attempt
    Given a connect proxy recording requests
    And a client with connect data {"device":"d1"} and a data callback returning {"n":1} then {"n":2}
    When connect is called
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy recorded data {"n":1} then {"n":2}

  @E
  Scenario: A failing connect data callback is retried after the reconnect backoff
    Given a client whose data callback fails once
    When connect is called
    Then the client emits an "error" event of kind connect data with code 12
    And the client state is "connecting"
    And the data callback is called again after the reconnect backoff delay
    And the connect command carries the data the second call returned

  @E
  Scenario: A connect data callback denying access disconnects the client
    Given a client whose data callback signals "unauthorized"
    When connect is called
    Then the client emits "disconnected" with code 1 and reason "unauthorized"
    And no transport is opened
    And no reconnect is scheduled

  @E
  Scenario: A disconnect while the data callback runs opens no transport
    Given a client whose data callback has not answered since connect was called
    When disconnect is called
    And the data callback answers
    Then the client state is "disconnected"
    And no transport is opened

  @E
  Scenario Outline: The connect data callback runs only after a connection token is obtained
    Given a client without an initial token, with a data callback whose token callback <outcome>
    When connect is called
    Then the data callback is not called for that attempt

    Examples:
      | outcome                      |
      | signals "unauthorized"       |
      | fails with a temporary error |
      | returns an empty token       |

  @E
  Scenario: The connection token is fetched before the connect data
    Given a client without an initial token, with a token callback and a data callback
    When connect is called
    Then the token callback is called before the data callback
    And the connect command carries both the token and the data

  @E
  Scenario: A client that has a token calls the data callback without the token callback
    Given a client with an initial token, a token callback and a data callback
    When connect is called
    Then the token callback is not called
    And the connect command carries the initial token and the data

  @B
  Scenario: Data returned with the connect reply is delivered
    Given the connect proxy answers with data {"welcome":true}
    When connect is called
    Then the client emits "connected" with data {"welcome":true}

  @D
  Scenario: The connected event carries the server's data exactly
    Given a fake server answering connects with the non-canonical JSON data {"b": 1.0,  "a":2}
    When a client connects to it
    Then the client emits "connected" with data exactly those bytes

  @B
  Scenario Outline: HTTP headers reach the connect proxy over each transport
    Given a connect proxy recording requests
    And a server with http_stream and sse enabled
    And a client with HTTP header "X-Device-Real: r1" configured with only the server's <transport> endpoint
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the connect proxy received header "X-Device-Real: r1"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @B
  Scenario Outline: HTTP headers ride on every request of an emulated transport
    Given a client with HTTP header "X-Device-Real: r1" connected over transport "<transport>" through the HTTP proxy
    When the client sends a command
    Then the HTTP proxy relayed the stream request and at least one emulation request
    And every request it relayed carried header "X-Device-Real: r1"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @B
  Scenario: Emulated headers reach the connect proxy
    Given a connect proxy recording requests
    And a client with emulated header "X-Device-Emulated: e1"
    When connect is called
    Then the connect proxy received header "X-Device-Emulated: e1"

  Scenario: A header the transport sets is rejected
    Given client options with HTTP header "Sec-WebSocket-Key: k1"
    When the client is built
    Then building fails with a configuration error naming "Sec-WebSocket-Key" and not "k1"

  Scenario: Client options do not show secrets
    Given client options with token "tok-secret", connect data {"pin":"1234"}, HTTP header "Authorization: h-secret" and emulated header "Authorization: e-secret"
    When their debug output is formatted
    Then it names the header "Authorization" and shows the data as 14 bytes
    And it contains none of "tok-secret", "1234", "h-secret" and "e-secret"

  @D
  Scenario: set_token replaces an expired token when there is no token callback
    Given a client with token "old", no token callback and a minimum reconnect delay of 1 s, connected to the fake server
    And the fake server will answer the next connect with error 109 "token expired"
    When the fake server disconnects the client with a reconnect-advised code
    And set_token is called with "new" when the client emits the "error" event with code 109
    Then the next connect command carries token "new"
    And the client emits "connected"
    And the client emits no "error" event of kind configuration

  @E
  Scenario: set_token does not skip the token callback after an expired token
    Given a client using a token callback whose connect was answered with error 109
    When set_token is called with "new" before the next attempt
    Then the token callback is called before the next connect command

  @D
  Scenario: set_data applies from the next connection attempt
    Given a client with connect data {"v":1}, connected to the fake server
    When set_data is called with {"v":2}
    And the fake server disconnects the client with a reconnect-advised code
    Then the fake server received no command between set_data and the disconnect
    And the next connect command carries data {"v":2}

  @B
  Scenario: set_emulated_headers applies from the next connection attempt
    Given a connect proxy recording requests
    And a connected client with emulated header "X-Device-Emulated: e1"
    When set_emulated_headers is called with "X-Device-Emulated: e2"
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy received header "X-Device-Emulated: e1" then "X-Device-Emulated: e2"
```

**Exceptions introduced:** none.

---

## Block 18 — Batching API

**Goal:** an application groups its calls and subscriptions into one frame (websocket) or one emulation request (http_stream, sse), and the client sends the subscribe commands of a connect reply the same way, so that a burst of commands costs one write or one HTTP request instead of one each, and never ends a connection that sending them one by one would have kept (owner decision, 2026-11-04, "Planned after Block 13"; order decided 2026-11-06).

**Dependencies:** Blocks 3 (close code 1009), 7 (Protobuf frames), 8 (emulation requests, the HTTP 413 rule), 9 and 10 (subscribe and unsubscribe commands, their reply deadlines), 13 (calls, their phases, the write queue), 15 (in-order handling), 16 (runners and invariants), 17 (`Input` is `#[non_exhaustive]`).

**Out of scope:**
- automatic batching of every command a step or a burst produces (centrifugal/centrifuge-rust `docs/spec.md` §9.10, decision 11): only the application's batches and the subscribe commands of a connect reply are batched;
- subscribe commands in the frame of the connect command on websocket open (centrifuge-js 63ed73e `centrifuge.ts:1243-1258`): ferrifuge subscribes once connected (Block 9), and subscribing earlier is optimistic subscription (centrifuge-rust §9.11, phase 3);
- batching the calls a new connection releases (Block 13) when no batch is open: each still goes in its own frame;
- a batch delay timer (centrifuge-rust §9.10);
- publish debouncing (Block 0);
- emulated transports and Protobuf in the validation runners (Block 16).

**Decisions** (reviewer, 2026-11-06, delegated by the owner; the official spec does not mention batching; the client protocol lets one frame carry several commands, newline-joined in JSON and varint-delimited in Protobuf, and several replies, https://centrifugal.dev/docs/transports/client_protocol, "Batching: many messages, one frame"):
- **D1 API.** `Client::start_batching()` returns a `#[must_use]` `Batch`, which borrows nothing from the client and is `Send`; `Batch::stop()`, or dropping the `Batch`, stops it (centrifuge-js `startBatching`, `stopBatching`). A guard, so that a forgotten stop cannot outlive its scope; there is no client-level `stop_batching`. Several batches may be open at once, from several tasks: commands are held while any batch is open, and stopping one sends everything held so far, the others staying open. Stopping sends at once, without centrifuge-js's two promise turns (`centrifuge.ts:675-685`), which exist only for its asynchronous subscribe path. Dropping the `Client` while a batch is open disconnects it as Block 13 says. Sans-IO: `Input::StartBatching`, `Input::StopBatching`; no new `Action`.
- **D2 What is held.** While a batch is open the client holds the application's calls other than `send` (rpc, publish, history, presence, presence stats, client-level and subscription-level), each from when it would be sent, a call released by a new connection included, except a call already waiting for room when the batch opened; and the subscribe and unsubscribe commands that the application's `subscribe()`, `unsubscribe()` and subscription removal cause, built while a batch is open, after a token or data callback answers included. Not held: `send` (centrifuge-js writes it at once, `centrifuge.ts:523`), pong (`:2746`), connect, refresh, sub_refresh, and the subscribe commands a connect reply or a resubscribe backoff starts. A `send` or a pong made during a batch can therefore reach the server before the batch's commands (documented). Diverges from centrifuge-js 63ed73e, which also holds refresh and sub_refresh (both go through `_call`, `centrifuge.ts:1871-1884`): they are timer-driven, and holding them could let a token expire.
- **D3 Deadlines.** A held subscribe or unsubscribe is handled as if sent when held: its reply deadline runs from then (centrifuge-js `_call` registers the reply timer before `_addCommand`, `centrifuge.ts:1871-1884`, `:2789`), so a batch never stopped ends in "subscribe timeout" (connecting code 3) or an unsubscribe error (connecting code 4), as when the reply never comes; losing the transport or entering `disconnected` drops it, as a sent command is lost (centrifuge-js `_disconnect` empties `_commands`, `:1986`). A held call has no command yet (Block 13: its command is built when sent); its hold counts as waiting for room: if it is not sent within `Config::timeout` of being held, it fails with the timeout error (code 1) and is never sent, and waiting for room after the batch stops does not restart that deadline, so Block 16 I6 keeps its bound of 3 × `Config::timeout`. Diverges from centrifuge-js 63ed73e, whose flush still sends a command whose call timed out. A held call keeps waiting across a lost transport, as a call waiting for room does, and fails with "disconnected" (code 3) when the client enters `disconnected` (Block 13). A batch that stays open across a reconnect holds the commands of the new connection.
- **D4 Write queue (Block 13).** When a batch stops, its held commands go out in the order they were held. The client's own commands (subscribe, unsubscribe) always go through. Held calls are admitted one by one, in order, behind the calls already waiting for room: a call joins the batch while it fits `Config::write_queue_bytes` with what is unwritten and the calls before it in the batch, the first held call fitting alone when nothing is unwritten; the first call that does not fit and every later one wait for room with their deadline, each sent in its own frame as room frees. A held call is not in the queue of calls waiting for room until its batch stops, so a `send` made during a batch is not delayed by it. A frame counts the encoded size of the calls it carries until `Input::FrameWritten`.
- **D5 Frame cap.** `Config::batch_frame_bytes`, default 16 KiB, at least 1 or `Client::new` returns an error, bounds every frame of several commands, measured as sent (over an emulated transport, the request body); a batch that would exceed it is split, in order, and a single larger command goes alone. Centrifugo v6.10.1 refuses a websocket message over `websocket.message_size_limit` (65 536 bytes by default) with close code 1009, which ends the client (Block 3), and an emulation request over `max_request_body_size` (65 536 bytes by default) with HTTP 413 (`internal/configtypes/types.go`): without a cap, commands that each fit could end the client together, on every reconnect when they are the subscribe commands of a connect reply. Diverges from centrifuge-js 63ed73e, which has no cap; centrifuge-rust §9.10 caps frames at 15 KB, as centrifuge-csharp. The README tells operators to keep `batch_frame_bytes` below both server limits.
- **D6 Emulated transports and HTTP 413 (Block 8).** A frame of several commands is one emulation request (centrifuge-js `transport_http_stream.ts`, `transport_sse.ts` `send`: one request per `_transportSendCommands`). The owner's rule (2026-11-04) holds: a request of several commands refused with 413 resends each command in its own request first, and only a command whose own request is refused gets the terminal outcome Block 8 lists. The resent requests go at once, in the order of the refused request, without write-queue admission (their calls count as unwritten again), and every deadline keeps running; a command no longer awaited (its call settled or abandoned, its subscription no longer waiting for that reply) is not resent.
- **D7 Encodings.** As the client protocol and centrifuge-js (`json.ts` `encodeCommands`, `protobuf.codec.ts` `encodeCommands`): JSON commands joined with `\n`, without a trailing newline; Protobuf commands varint-delimited, over websocket only (Block 8). Replies to a batch may come in one frame or several; they are handled as any (Block 15), each call settling when its reply is read (Block 13).
- **D8 Connect reply.** The subscribe commands a connect reply starts go out as one batch, split by the cap (centrifuge-js `_connectResponse`, `centrifuge.ts:2350-2352`), and are never held by an application's batch.
- **D9 Validation harness (Block 16).** Both runners gain the operations `start_batching` and stop, and the adapter runner also drops a `Batch`; "healed" also requires every batch to be stopped. New invariant I10, core, after every step: no frame of several commands exceeds `batch_frame_bytes`, and no call a batch holds (D2: not `send`, nor a call already waiting for room when the batch opened) is sent before a batch is stopped. The chaos configuration sets `batch_frame_bytes` to 1 KiB so that batches split. Each new invariant check has a planted fault, as Block 16 requires.
- **Public API and version.** 18a adds a field to `Config`, which is exhaustive: breaking, version 0.12.0. `Client::start_batching`, `Batch` and the two `Input` variants are additions; no `Action`, `CallTimer` or `SubscriptionTimer` variant is added. 18b, 18c and 18d add no public item.
- **Fixtures.**
  - The fake server records the frame each command arrived in, text or binary.
  - The HTTP proxy (Block 8) can answer HTTP 413, without relaying, to every emulation request whose body exceeds a given size, and records the requests it refused.

Verified (Centrifugo v6.10.1, the 18b @B scenario's setup, 2026-11-06): one emulation request carrying two publish commands, over http_stream and sse, runs both, and a request body over `max_request_body_size` (a 70 000-byte publish) is answered with HTTP 413, which fails that call with code 10 while the next command, resent alone, succeeds.

**Sub-increments** (one branch and PR each, in this order):

| Id | Scope | Scenarios |
|---|---|---|
| 18a | `start_batching`, `Batch`, what is held, deadlines, write-queue admission, `Config::batch_frame_bytes`, disconnect and lost transport, JSON and Protobuf over websocket; version 0.12.0 | "Calls made while batching go out in one frame when the batch ends", "Subscriptions subscribed while batching send their subscribe commands in one frame", "A batch over Protobuf is one binary frame", "A batch is split at the frame cap", "Held calls are admitted to the write queue one by one", "A call held past the timeout fails and is never sent", "A subscribe held past the timeout reconnects the client", "The client's own commands and send are not held", "Disconnect fails the calls a batch holds", "A held call waits across a lost transport", "Stopping one of two batches sends what is held" |
| 18b | emulated transports and the 413 resend | "A batch refused with HTTP 413 is resent command by command", "Over an emulated transport only the command refused alone fails" |
| 18c | the subscribe commands of a connect reply in one batch | "A reconnect sends its subscribe commands in one frame", "The subscribe commands of a connect reply are split at the frame cap", "An open batch does not hold the subscribe commands of a connect reply" |
| 18d | batching in both runners, I10, planted faults | "Seeded chaos with batching on the core keeps every invariant", "The core runner reports a planted batching fault", "Seeded chaos with batching on the tokio client keeps the invariants" |

Value clause: As an app developer shipping ferrifuge to fleets of devices, I want to group calls and subscriptions into one frame or emulation request, and the client to resubscribe in as few frames as the frame cap allows, so that a burst of commands costs one write or one HTTP request per `batch_frame_bytes` instead of one per command, and a batch never ends the connection or fails a command that would have succeeded alone.

```gherkin
Feature: Batching

  @D
  Scenario Outline: Calls made while batching go out in one frame when the batch ends
    Given a client connected to the fake server
    When the application starts a batch, calls RPC "a", publishes {"n":1} to channel "news" and requests the history of channel "news"
    And 200 ms elapse
    Then the fake server has received no command since the batch started
    When the batch is <ended>
    Then the fake server receives one frame carrying the rpc, publish and history commands, in that order
    And each call returns the fake server's reply

    Examples:
      | ended   |
      | stopped |
      | dropped |

  @D
  Scenario: Subscriptions subscribed while batching send their subscribe commands in one frame
    Given a client connected to the fake server, with three subscriptions not subscribed
    When the application starts a batch and calls subscribe on the three subscriptions
    And 200 ms elapse
    Then the fake server has received no command since the batch started
    When the batch is stopped
    Then the fake server receives one frame carrying the three subscribe commands, in that order
    And the three subscriptions reach state "subscribed"

  @D
  Scenario: A batch over Protobuf is one binary frame
    Given a client configured with the Protobuf protocol, connected to the fake server
    When the application starts a batch, calls RPC "a" and RPC "b", and stops the batch
    Then the fake server receives one binary frame carrying the two rpc commands, varint-delimited, in that order
    And both calls return the fake server's reply

  @E
  Scenario Outline: A batch is split at the frame cap
    Given a connected client with a batch frame cap of 1000 bytes
    When the application starts a batch, makes calls whose commands encode to <sizes> bytes, and stops the batch
    Then the client sends frames carrying <frames> calls, in the order the calls were made
    And no frame carrying several commands exceeds 1000 bytes

    Examples:
      | sizes                   | frames  |
      | 300, 300, 300, 300, 300 | 3, 2    |
      | 300, 1500, 300          | 1, 1, 1 |

  @E
  Scenario: Held calls are admitted to the write queue one by one
    Given a connected client with a write queue of 1000 bytes and nothing unwritten
    When the application starts a batch, makes five calls whose commands encode to 300 bytes each, and stops the batch
    Then the client sends one frame carrying the first three calls
    And the client sends nothing more until that frame is reported written
    And then the fourth and fifth calls are sent, each in its own frame

  @E
  Scenario: A call held past the timeout fails and is never sent
    Given a connected client with a timeout of 5 s
    When the application starts a batch and calls RPC "a"
    And 5 s elapse
    Then the call fails with error code 1
    When the batch is stopped
    Then the client has sent no frame since the batch started

  @E
  Scenario: A subscribe held past the timeout reconnects the client
    Given a connected client with a timeout of 5 s and a subscription not subscribed
    When the application starts a batch and calls subscribe on the subscription
    And 5 s elapse
    Then the client has sent no frame since the batch started
    And the client emits "connecting" with code 3 and reason "subscribe timeout"

  @E
  Scenario: The client's own commands and send are not held
    Given a connected client with a token callback, whose connect reply asked for pongs and whose connection token expires
    When the application starts a batch
    And the server sends a ping
    And the application calls send with {"n":1}
    And the connection token's refresh time comes and the token callback answers
    Then the client sends a pong, the send command and a refresh command, each in its own frame, before the batch is stopped
    And the send call returns

  @E
  Scenario: Disconnect fails the calls a batch holds
    Given a connected client
    When the application starts a batch and calls RPC "a"
    And disconnect is called
    Then the call fails with error code 3
    When connect is called and the client reaches state "connected"
    And the batch is stopped
    Then the client has sent no rpc command

  @E
  Scenario: A held call waits across a lost transport
    Given a connected client
    When the application starts a batch and calls RPC "a"
    And the transport closes
    Then the call has not failed
    When the client reaches state "connected" on a new transport
    And the batch is stopped
    Then the rpc command is sent on the new transport
    And no rpc command was sent on the closed transport

  @E
  Scenario: Stopping one of two batches sends what is held
    Given a connected client
    When the application starts batches A and B and calls RPC "a"
    And batch A is stopped
    Then the client sends one frame carrying RPC "a"
    When the application calls RPC "b"
    Then the client sends no frame
    When batch B is stopped
    Then the client sends one frame carrying RPC "b"

  @E
  Scenario: A batch refused with HTTP 413 is resent command by command
    Given a client connected over an emulated transport, with a subscription not subscribed
    And the application started a batch, called RPC "a" and subscribe on the subscription, and stopped the batch
    When the server refuses the request carrying both commands with HTTP 413
    Then the call has not failed and the subscription is "subscribing"
    And the client sends two requests, carrying RPC "a" and then the subscribe command
    When the server refuses the request carrying RPC "a" with HTTP 413 and answers the subscribe command
    Then the call fails with error code 10
    And the subscription reaches state "subscribed"
    And the transport stays open

  @B
  Scenario Outline: Over an emulated transport only the command refused alone fails
    Given a client subscribed to channel "news" over transport "<transport>" through an HTTP proxy answering HTTP 413 to every emulation request over 2000 bytes
    When the application starts a batch, publishes a 2100-byte JSON string and then {"n":1} to channel "news", and stops the batch
    Then the HTTP proxy refused one request carrying both publish commands
    And the first publish fails with error code 10
    And the second publish succeeds
    And the subscription receives the publication {"n":1} and no other

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @D
  Scenario: A reconnect sends its subscribe commands in one frame
    Given a client with three subscriptions subscribed, connected to the fake server
    When the fake server disconnects the client with a reconnect-advised code
    Then after the next connect reply the fake server receives one frame carrying the three subscribe commands
    And the three subscriptions reach state "subscribed"

  @E
  Scenario: The subscribe commands of a connect reply are split at the frame cap
    Given a client with a batch frame cap of 1000 bytes and five subscriptions whose subscribe commands encode to 300 bytes each
    When connect is called and the server answers the connect command
    Then the client sends two frames, carrying three and then two subscribe commands

  @E
  Scenario: An open batch does not hold the subscribe commands of a connect reply
    Given a client with a subscription in state "subscribing"
    And the application has started a batch
    When connect is called and the server answers the connect command
    Then the client sends the subscribe command while the batch is open

  @P
  Scenario: Seeded chaos with batching on the core keeps every invariant
    Given the core runner with the chaos configuration, a batch frame cap of 1 KiB, client-side and server-side subscriptions to recoverable channels and batching operations
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I10
    And the run counted batches stopped, frames of several commands, split batches and held calls that timed out, none of them 0

  @P
  Scenario Outline: The core runner reports a planted batching fault
    Given the core runner with batching operations, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of I10, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                  |
      | two frames of several commands merged by the tap past the frame cap    |
      | a batch stop reported to the checker one step after the client handled it |

  @P
  Scenario: Seeded chaos with batching on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a batch frame cap of 1 KiB, a tokio client, the fake server and batching operations
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted batches stopped and batches dropped, none of them 0
```

**Exceptions introduced:** three divergences from centrifuge-js 63ed73e, each recorded in the decisions above:
- refresh and sub_refresh are not held (D2);
- a held call that times out is never sent (D3);
- frames of several commands are capped by `Config::batch_frame_bytes` (D5).

---

## Block 19 — Fossil delta compression

**Goal:** a subscription can ask the server to send each publication as a Fossil delta against the previous one, and the client rebuilds every publication exactly, so that channels whose payloads change a little at a time cost a fraction of their bandwidth; a broken delta never reaches the application and never stops the client (owner decisions, 2026-11-06: item 5 of the parity analysis, Q5).

**Dependencies:** Blocks 2 (reconnect backoff), 5 (5c codec seam), 9 (subscriptions, publication bytes), 10 (resubscribe backoff), 11 (recovery, state invalidation), 12 (server-side subscriptions), 14 (jitter window, fuzz targets), 15 (delivery, withdrawal, read-ahead, backoff after 3008), 16 (validation runners), and Block 0's components rule.

**Out of scope:**
- encoding deltas: the client never sends one (owner decision);
- delta for server-side subscriptions: the client never asks for it in `ConnectRequest.subs` (centrifuge-js 63ed73e has no server-side delta either), so a delta publication there is a protocol fault (D8);
- a tag filter together with delta: tag filters are not implemented yet (parity item 6); when they are, a subscription with both is rejected when built (`client.proto`, `SubscribeRequest.tf`: "disables delta compression"; centrifuge-js `subscription.ts:368-370`);
- delta statistics (centrifuge-js `deltaStats()`): with `statistics()`, parity item 8;
- a configurable inbound message limit (parity item 12);
- map and shared-poll subscriptions and their per-key delta chains (Block 0).

**Decisions** (reviewer, 2026-11-06, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Component.** The decoder is written from scratch (owner decision, item 5). No crate meets Block 0's definition of a maintained component: fossil-delta 0.2.0 has had no release or commit since 2023-01-14; heroforge-core has no public repository; libfossil-rs is AGPL-3.0; delta-compression, xpatch and gdelta implement other formats; centrifuge-client-core 0.1.0 is excluded, as the sans-IO state machine behind centrifuge-client, which Block 0's first row does not reuse, and whose source the clean-room rule closes. The decoder is implemented from the Fossil documentation ("Fossil Delta Format", fossil-scm.org, read 2026-11-06) and cross-checked against centrifuge-js 63ed73e `src/fossil.ts`; no Rust crate's code is read or copied. As Block 0 requires of in-house components, it has a cargo-bolero target, run in ci.yml's `fuzz` loop and the fuzz.yml matrix (Block 14e), whose oracle round-trips the test-only writer's deltas (D9) and checks that any input either fails or builds a target within the limit; explicit bounds (D6); and no unsafe code. Only apply is implemented.
- **D2 Format.** A delta is a header (the target size, then `\n`), segments (a copy `count@offset,` of bytes of the base, or an insert `count:` followed by that many bytes of the delta), and a trailer (a checksum, then `;`), the trailer ending the delta. Integers are base-64 digits of the alphabet `0-9A-Z_a-z~`, most significant first: the document says both "MSB first" and least significant first, and its example (`1Xb` = 6246) settles MSB first, as centrifuge-js reads them. The checksum is the sum, modulo 2^32, of the target read as 32-bit big-endian words, the last zero-padded: centrifuge-js (`fossil.ts`, `checksum`), centrifugal/fdelta 9e22462 (`checksum.go`, the encoder Centrifugo uses) and the five vectors Fossil itself produced (fdelta `testdata/fossil-vectors/`) agree, against the document's "modulo 2^32-1", which four of the five vectors fail. A copy of count 0 copies nothing, as centrifuge-js and fdelta do; the document says it copies to the end of the base (recorded divergence; no vector has one).
- **D3 Failures.** A delta fails when: the header is not a number followed by `\n`; a segment is neither a copy nor an insert; a copy is not followed by `,`; a copy reads past the end of the base; an insert runs past the end of the delta; the segments produce more or fewer bytes than the header declares; the checksum differs; the trailer is missing or bytes follow it; a number has no digit, or a value above 2^32 − 1 (the document: "the format currently handles only 32 bit integers"); a byte outside the alphabet appears where a digit is expected, high-bit bytes included. centrifuge-js and fdelta accept some of these (empty numbers read as 0, values wrap, centrifuge-js masks high-bit bytes and ignores bytes after the trailer); neither encoder writes them, so being stricter rejects no delta Centrifugo sends. Offsets, counts and totals are computed without overflow.
- **D4 Negotiation.** `SubscriptionOptions::delta: Option<DeltaFormat>`, set with `SubscriptionBuilder::delta`; `DeltaFormat` is `#[non_exhaustive]` with one variant, `Fossil`, the only value `client.proto` supports. It is sent as `SubscribeRequest.delta = "fossil"` with every subscribe and resubscribe (centrifuge-js `subscription.ts:990`). Each subscribe reply's `SubscribeResult.delta` says whether delta is on for that subscription until its next subscribe (centrifuge-js `:567-571`); the `subscribed` event reports it in a new `Subscribed::delta` field. The wire model gains the `Publication.delta` flag, decoded by both codecs.
- **D5 Base and order.** A delta applies to the data of the previous publication of the subscription (`client.proto`, `Publication.delta`: "against the data of the previous publication in this subscription"), live or carried by a subscribe reply, in the order Block 15 handles them. The base is cleared at each subscribe and on state invalidation (unsubscribe code 2502, disconnect code 3014, Block 11): a delta with no base fails. centrifuge-js keeps its base across resubscribes and clears it only on state invalidation (`subscription.ts:495-518`) and on error 112 with getState (`:1025-1033`); `client.proto`'s "in this subscription" outranks it, and Centrifugo v6.10.1 sends the first publication after each subscribe whole, recovered or live (centrifuge 7ac037196c95, pinned by v6.10.1: `client.go:5436-5446`, `makeRecoveredPubsDeltaFossil`, and the live write path from `:5761`). A delta publication on a subscription whose last subscribe reply did not turn delta on fails as well, with or without the delta option (centrifuge-js delivers the delta bytes as the payload, `subscription.ts:1119`). Delta checks apply after routing: a publication no subscribed subscription takes (Block 9, Block 12) is dropped as today, unchecked.
- **D6 Bounds.** Every D3 check but the checksum is made before the target is allocated, and the checksum after it is built: a 20-byte delta cannot make the client allocate 64 MiB. A target larger than the inbound message limit fails; the limit becomes one core constant, 64 MiB, used by both adapters (today `src/adapter/transport.rs:303` and `src/adapter/http.rs:27`) and by the decoder. So does a message whose decoded publications together exceed that limit, since the same publications sent whole would not fit one message either; such a recovery fails on every resubscribe until history no longer holds it, as a recovery reply over 64 MiB does without delta (documented). The Block 15 read-ahead keeps counting the bytes read; decoded data is held only for the message being handled (Block 15, "one message at a time"), and each delta subscription holds one base of at most the limit.
- **D7 Encodings.** With Protobuf, `Publication.data` is the delta or the whole payload. With JSON, on a subscription with delta on, `data` is a JSON string whatever the publication (`client.proto`, `Publication.data`): its value, unescaped as JSON, gives UTF-8 bytes that are the delta, or the whole payload, and the payload delivered is that value, so the application gets the JSON text it would get without delta, byte for byte (centrifuge-js `json.ts`, `applyDeltaIfNeeded`). A `data` that is not a JSON string there fails. The decoded payload is not validated as JSON: the checksum binds it to the server's payload. The unwrapping goes through `Codec` (Block 5c), after routing, since only the subscription knows whether delta is on.
- **D8 Failure outcomes** (owner decision Q5).
  - Client-side subscription: an `error` event of the new kind `ErrorKind::Delta`, code 15 "delta error", then the subscription resubscribes: it moves to `subscribing` with the new code 2 "delta error", sends an unsubscribe command, and subscribes again after the resubscribe backoff (Block 10), once that unsubscribe is settled (Block 9). Its position stays at the last publication taken, so on a recoverable channel the publication that failed, and those after it, are recovered, the first of them whole (D5); publications not yet taken are withdrawn (Block 15 gains this reason), recovered ones of the same reply included. On a channel without recovery they are lost, as on any resubscribe.
  - Server-side subscription: a delta publication there is a protocol fault (out of scope above); the client emits an `error` event of kind `Delta` and reconnects with the new connecting code 5 "delta error", after the reconnect backoff (Block 2) and the Block 14 jitter window, as for a lost transport; the next connect recovers its server-side subscriptions from their last positions taken (Block 12).
  - Backoff: a subscribe ended by a delta failure does not reset the resubscribe attempt counter, and a connection ended by code 5 does not reset the reconnect attempt counter, as for 3008 (Block 15, "Backoff after 3008"): a server that breaks every delta sees each client's retries spread to `max_resubscribe_delay` and `max_reconnect_delay`, not a fleet resubscribing every second.
  - The new codes are not in centrifuge-js 63ed73e, whose code tables stop at error 14, connecting 4 and subscribing 1 (`codes.ts`), and which stops the client on a delta failure instead (`_dispatchFailed`, `centrifuge.ts:1820-1823`: disconnected code 2, no reconnect); Q5 decides otherwise.
- **D9 Validation harness (Block 16).** Both runners gain subscriptions with delta and a fault that corrupts one delta; invariant I8 (exactly once on recoverable channels) must hold across the resubscribes it causes. New invariant I11, payload integrity: every publication delivered carries exactly the payload published, checked in both runners, with a planted fault (the tap flips one byte of a delivered payload). The fake server and the simulated server build their deltas with a test-only writer of the format (literal inserts and copies of explicit ranges, with their checksum); the decoder's unit tests use the five Fossil-produced vectors of centrifugal/fdelta (`testdata/fossil-vectors/`, BSD-2-Clause, its notice kept beside them).
- **D10 Public API and version.** All additions: `SubscriptionOptions` and `Subscribed` are `#[non_exhaustive]`, `ErrorKind` too (Block 17), the codes are new constants, `DeltaFormat` and `SubscriptionBuilder::delta` are new, the inbound limit stays a constant. Version unchanged.

`VERIFY:` the environment variables of the Centrifugo v6.10.1 namespace options `allowed_delta_types` and `delta_publish` for the @A server (and the server API `publish` field `delta`, `api.proto:325`); whether a live delta needs channel history; that every v6.10.1 path writing a JSON publication to a delta subscriber wraps it as a string (the @A JSON row, live and recovered).

Verified (Centrifugo v6.10.1, raw websocket client and server API, 2026-11-06, while implementing 19a): `CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOWED_DELTA_TYPES=fossil` (namespace option `allowed_delta_types`) makes the subscribe reply turn delta on; live publications go as deltas only to a positioned subscription (`force_positioning`, or recovery, which implies it), and whole with history alone; with `CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_DELTA_PUBLISH=true` (`delta_publish`) every publication after the first goes as a delta, whatever the server API `publish` field `delta`; with JSON every live publication of a delta subscription, whole or delta, arrives as a JSON string. The @A `delta` namespace therefore has history, `force_recovery`, `allowed_delta_types` and `delta_publish`; recovered publications are checked by 19c's scenario.

Value clause: As an app developer shipping to devices on metered networks, I want a channel's publications sent as deltas against the previous one and rebuilt exactly by the client, so that large, slowly changing payloads cost a fraction of their bandwidth without my application ever seeing a corrupted payload.

**Sub-increments** (one branch and PR each, in this order):

| Id | Scope | Scenarios |
|---|---|---|
| 19a | decoder, negotiation, both encodings | "Delta compression is negotiated with the server", "Publications sent as deltas arrive whole", "A delta publication is applied to the previous publication", "The delta option is sent on subscribe and on resubscribe", "With JSON, publications of a delta subscription arrive as their JSON text" |
| 19b | failures and their backoff | "A broken delta makes the subscription resubscribe", "A delta with no base makes the subscription resubscribe", "A delta on a subscription without delta on makes it resubscribe", "Each subscribe starts a new delta chain", "A broken delta among recovered publications makes the subscription resubscribe from the same position", "Resubscribe delays keep growing while every subscribe is followed by a broken delta", "A delta on a server-side subscription makes the client reconnect", "A delta declaring more than the inbound message limit fails before allocating" |
| 19c | recovery and the runners | "Publications missed by a delta subscription are recovered whole", "Seeded chaos with delta subscriptions keeps every invariant", "Seeded chaos with delta subscriptions on the tokio client keeps the invariants", "The core runner reports a planted payload fault" |

```gherkin
Feature: Fossil delta compression

  @A
  Scenario: Delta compression is negotiated with the server
    Given a connected client
    When a subscription with the fossil delta option to a channel allowing fossil delta is subscribed
    Then the subscription emits "subscribed" reporting delta true

  @A
  Scenario Outline: Publications sent as deltas arrive whole
    Given a client configured with the <protocol> protocol, connected through a relay counting bytes, subscribed with the fossil delta option to a channel with history allowing fossil delta and delta publishing
    And the subscription emitted "subscribed" reporting delta true
    When the server API publishes, as deltas, 3 payloads of 4 KiB that differ in a few bytes to the channel
    Then the subscription receives exactly those 3 payloads in order
    And the client read fewer than 2 KiB for the last 2 publications

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: A delta publication is applied to the previous publication
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a whole publication and then a delta publication against it
    Then the subscription receives the whole payload and then the payload the delta builds

  @D
  Scenario: The delta option is sent on subscribe and on resubscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the fossil delta option is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each asking for fossil delta

  @D
  Scenario: With JSON, publications of a delta subscription arrive as their JSON text
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a whole publication whose data is the JSON string of the non-canonical JSON {"b": 1.0,  "a":"é<"}, its é and < sent as \u escapes
    And then a delta publication against it, as a JSON string
    Then the subscription receives exactly the bytes {"b": 1.0,  "a":"é<"} and then the bytes the delta builds

  @D
  Scenario Outline: A broken delta makes the subscription resubscribe
    Given a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, which turned delta on, whose subscription's stream has taken the whole publication at offset 1
    When the fake server pushes, at offset 2, a delta <fault>
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the fake server receives an unsubscribe command for "news"
    And the next subscribe command for "news" asks to recover from offset 1
    And the subscription receives no publication at offset 2 before that subscribe is answered
    And when the fake server answers it with the publication at offset 2 recovered whole, the subscription receives it exactly once

    Examples:
      | fault                                         |
      | whose checksum is wrong                       |
      | that copies past the end of the previous data |
      | that builds fewer bytes than it declares      |
      | that has no trailer                           |

  @D
  Scenario: A delta with no base makes the subscription resubscribe
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a delta publication before any whole publication
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the subscription receives no publication

  @D
  Scenario Outline: A delta on a subscription without delta on makes it resubscribe
    Given a client subscribed to channel "news" on the fake server <how>
    When the fake server pushes a delta publication
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the subscription receives no publication

    Examples:
      | how                                                         |
      | with the fossil delta option, which did not turn delta on   |
      | without the delta option                                    |

  @D
  Scenario: Each subscribe starts a new delta chain
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on, and has received a whole publication
    When the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again, the fake server turning delta on
    And the fake server pushes a delta publication against the publication received before the disconnect
    Then the subscription emits an "error" event of kind "delta" with code 15

  @D
  Scenario: A broken delta among recovered publications makes the subscription resubscribe from the same position
    Given a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, whose subscription's stream has taken the publication at offset 1
    When the fake server disconnects the client with a reconnect-advised code
    And answers the next subscribe with delta on and the publications at offsets 2 and 3 recovered, the one at 3 a delta whose checksum is wrong
    Then the subscription's stream yields "subscribing", "subscribed", an "error" of kind "delta", and "subscribing" with code 2
    And the next subscribe command for "news" asks to recover from offset 1

  @E
  Scenario: Resubscribe delays keep growing while every subscribe is followed by a broken delta
    Given a subscription with the fossil delta option, min_resubscribe_delay 500 ms and max_resubscribe_delay 20000 ms
    When each of 8 subscribes is answered with delta on, a whole publication and then a broken delta
    Then the upper bound of each resubscribe delay is greater than the previous one until it reaches 20000 ms

  @D
  Scenario: A delta on a server-side subscription makes the client reconnect
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server that has taken the server-side publication at offset 1
    When the fake server pushes a delta publication at offset 2 on channel "alerts"
    Then the client emits an "error" event of kind "delta" with code 15
    And then the client emits "connecting" with code 5
    And no server-side publication at offset 2 is yielded
    And the next connect command asks to recover "alerts" from offset 1

  @P
  Scenario: A delta declaring more than the inbound message limit fails before allocating
    Given a subscription with the fossil delta option, subscribed with delta on, that has received a whole publication
    When a delta publication of 20 bytes declaring a target one byte larger than the inbound message limit arrives
    Then the subscription emits an "error" event of kind "delta" with code 15
    And the live bytes counted while it is handled stay below 1 MiB

  @A
  Scenario: Publications missed by a delta subscription are recovered whole
    Given a client subscribed with the fossil delta option to a channel with history and recovery allowing fossil delta and delta publishing
    And the subscription emitted "subscribed" reporting delta true
    And the subscription has received 1 live publication
    And the client is disconnected with a reconnect-advised code
    When 2 publications that differ in a few bytes from it are made to the channel, as deltas, while the client reconnects
    Then the subscription emits "subscribed" with was_recovering true and recovered true, reporting delta true
    And the subscription receives exactly those 2 publications in order

  @P
  Scenario: Seeded chaos with delta subscriptions keeps every invariant
    Given the core runner with the chaos configuration, client-side subscriptions with delta to recoverable channels and a fault corrupting deltas
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I11
    And the run counted delta publications, corrupted deltas and the resubscribes they caused, none of them 0

  @P
  Scenario: Seeded chaos with delta subscriptions on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a tokio client, the fake server, subscriptions with delta and a fault corrupting deltas
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6, I8 or I11 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted corrupted deltas and the resubscribes they caused, none of them 0

  @P
  Scenario: The core runner reports a planted payload fault
    Given the core runner with delta subscriptions, with the tap flipping one byte of a delivered payload
    When seeds 0 to 299 run
    Then the runner reports a violation of I11, naming the first seed that shows it and the operations up to it
```

**Exceptions introduced:** divergences, each recorded above: from the Fossil document (the checksum modulus, a copy of count 0; D2); from centrifuge-js 63ed73e (a new chain at each subscribe, a delta without delta on fails, stricter parsing, and resubscribing or reconnecting instead of stopping the client on a delta failure; D3, D5, D8); the new codes (D8); and a subscription without the delta option now fails on a delta publication instead of delivering its bytes (D5).

---

## Planned after Block 19 (owner decision, 2026-11-06; decided by the reviewer by state of the art, delegated by the owner)

The owner decided parity with centrifugal/centrifuge-rust and the listed community requests (`ANALYSIS-PARITY.md`, branch `analysis/parity`, items 1–28), under the clean-room rule, and delegated the open choices to the reviewer with the criterion "what would be state of the art" (mature, maintained Rust network clients: reqwest, hyper-util, tonic, aws-sdk-rust, async-nats, rumqttc, tokio-tungstenite, rustls; the Rust API Guidelines). Each item becomes a block with its value clause and scenarios when its turn comes, one spec PR then the implementation; each block records in Block 0 the exclusion it reverses.

Decisions that bind the blocks:
- **Items adopted** as the analysis recommends, except: item 1's CONNECT and SOCKS5 handshakes use hyper-util's maintained `Tunnel` and `SocksV5` connectors, not in-house code (Block 0, "Components"; reqwest 0.13.5 uses them), so a CONNECT failure reports 407 or "unsuccessful", not every status; item 2's raw `rustls::ClientConfig` is public without a semver-exempt feature (async-nats, tokio-tungstenite and rumqttc expose it), the fips build keeping webpki verification; items 11 and 12 land in one breaking release that also marks `Config` `#[non_exhaustive]` (as tungstenite's `WebSocketConfig`). Not adopted: items 17 (callback API, WeakClient, delivery modes, deadlock detection), 20 (TLA+ trace replay; Block 16's "optional, later" replay is dropped) and 24 (cookie jar). Item 25 is a wasm32 build check of the core only. Item 5 is Block 19.
- **Proxies (item 1):** environment variables, then OS settings, by default, with an opt-out (reqwest 0.13.5 `system-proxy` default, aws-sdk-rust since behaviour version 2025-08-07, ureq 3).
- **Logging (item 9):** `tracing` with its `log` feature, in the tokio adapter only, as a regular dependency of the `tokio` feature; DEBUG and TRACE, WARN for server anomalies; spans skip their fields, values go through the redacting `Debug`; no payload switch; a capture test; the docs warn that dependencies log secrets at TRACE through `log`.
- **Release (item 21):** a tag-triggered `release.yml` with crates.io trusted publishing (`rust-lang/crates-io-auth-action`, environment `release`, version and `cargo-semver-checks` checked, actions pinned by SHA), a hand-written Keep a Changelog `CHANGELOG.md`, `SECURITY.md` pointing to GitHub private vulnerability reporting, docs.rs metadata, and an MSRV raised only in a minor version and only when needed. The account, first publish, trusted-publisher registration and repository settings are the owner's.
- **JSON helpers (item 18):** always available, no `serde` feature: serde and serde_json are unconditional dependencies, so a feature would only hide API.
- **Components audit:** the in-house sse decoder is replaced by a maintained parser (sse-parse, finesse or sse-core, chosen in that block's spec by fuzzing and benchmarks), the http_stream splitter by tokio-util's `AnyDelimiterCodec` with a maximum length, the content-type check by mime; the Block 5c drift (`machine.rs` calling `json::` directly) is refactored through `Codec`, with no API change.

Order of the blocks:

| Block | Item(s) |
|---|---|
| 20 | 21: release setup |
| 21 | 9: logging |
| 22 | 2: platform TLS verifier, extra roots, raw rustls configuration |
| 23 | 1: proxies |
| 24 | 3: Android and iOS builds, mobile lifecycle |
| 25 | 4: sleep, network changes, captive portals |
| 26 | Block 5c drift refactor |
| 27 | components: sse and http_stream decoders, content-type check |
| 28 | 15: tokio runtime handle (fixes `Client::new` panicking outside a runtime) |
| 29 | 11 and 12: handshake timeout, maximum incoming message size, `Config` `#[non_exhaustive]` |
| 30 | 10: `ready()` |
| 31 | 8: `statistics()` |
| 32 | 14 and 27: `set_http_headers`, `Subscription::set_data` |
| 33 | 16: `close()` |
| 34 | 23: table of codes |
| 35 | 6, 7 and 28: tag filters, channel compaction, `Publication.channel` |
| 36 | 26: automatic command batching |
| 37 | 19 and 22: test constructors, in-memory server double |
| 38 | 18: typed JSON helpers |
| 39 | 13: publications-only stream |
| 40 | 25: wasm32 build check |

---

## Block 20 — Release setup

**Goal:** Ferrifuge can be depended on as a production component: it is published to crates.io from CI only, with a changelog, a security policy, its documentation on docs.rs, compiled examples, and a stated toolchain floor that CI checks for every build (parity item 21; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art).

**Dependencies:** Blocks 0 (lints, deny.toml, crate layout), 6 (feature sets), 16 (CI layout), 19 (the fossil vectors under `tests/fixtures`).

**Out of scope:**
- the owner's actions outside the repository: the crates.io account and ownership of the name, the first publish (crates.io requires an API token for it), the trusted-publisher entry naming `release.yml` and the environment `release`, the GitHub `release` environment with a required reviewer and deployments limited to `v*` tags, a tag ruleset for `v*`, private vulnerability reporting (to be on before `SECURITY.md` is relied on), immutable releases;
- publishing a version: this block makes it possible, the owner decides when;
- committing the generated Protobuf code instead of generating it at build time (opentelemetry-proto does): a later decision;
- conventional-commit tooling (git-cliff, release-plz): the commit style is the project's `[block][phase]` one;
- workflow linters (zizmor, actionlint) as CI jobs: a later block.

**Decisions** (reviewer, 2026-11-06, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Release workflow.** `.github/workflows/release.yml`, triggered by a pushed tag `v*`, in three jobs, so that every check comes before the publish, which cannot be undone (a version can only be yanked):
  - `verify` (no environment; `contents: read`, `actions: read`): runs `scripts/check-release-tag` (D2), checks that the tagged commit is on `main` and that the `ci` run of the push to `main` with that head commit has concluded with success (a run still in progress fails the release, which is re-run later), and runs `cargo-semver-checks` (a pinned version, default features, as the pull-request job) against the crate's latest version on crates.io, skipped only when the index has none;
  - `publish` (`needs: verify`; environment `release`; `id-token: write`): `cargo publish` with a token from `rust-lang/crates-io-auth-action` (trusted publishing, no stored token);
  - `github-release` (`needs: publish`; `contents: write`): the GitHub release with that version's changelog section as its notes; re-running it never publishes again.
  Every action is pinned by commit SHA; checkouts use `persist-credentials: false`; no build cache is used; the toolchain action names its toolchain.
- **D2 Release check.** `scripts/check-release-tag` (bash and cargo only, so it runs on every CI OS) takes a tag and succeeds only when the tag is exactly `v` followed by the `Cargo.toml` version, without a suffix, and `CHANGELOG.md` has a section for that version; otherwise it fails naming the version the tag must be, or the missing section. The workflow and a test run it.
- **D3 Package contents.** `Cargo.toml` lists what is published (`include`): `src`, `proto`, `build.rs`, `README.md`, `LICENSE`, `CHANGELOG.md`, `SECURITY.md`; Cargo adds `Cargo.toml.orig`, `Cargo.lock` and `.cargo_vcs_info.json`. Tests, benches, fixtures, workflows, `.claude`, `CLAUDE.md`, `deny.toml` and the logo stay out (today 155 files are packaged). The published crate's own unit tests do not build, as `src/fossil.rs` includes fixtures that are not shipped; that is accepted (they run in this repository). A `package` CI job on pull requests runs `cargo publish --dry-run`, which builds the crate as published (verified: `build.rs`, `proto/` and `protoc-bin-vendored` work; the path-only dev-dependency on itself is dropped).
- **D4 Changelog.** `CHANGELOG.md` in Keep a Changelog form, written by hand: `## [Unreleased]` on top, then `## [x.y.z] - YYYY-MM-DD` for each version from 0.1.0, newest first, with Added, Changed, Deprecated, Removed, Fixed and Security subsections, backfilled from the history (the version each block reached, `git log -G'^version'` on `Cargo.toml`, and the block table of this file); no compare links for versions never tagged. A pull request that changes the public API or behaviour adds an `Unreleased` entry; the release moves them under the new version.
- **D5 Security policy.** `SECURITY.md`: reports go through GitHub private vulnerability reporting (the repository's "Report a vulnerability" button), never public issues; the supported versions are the latest minor release; fixes are released as a patch of it and announced as a GitHub security advisory, with a RustSec advisory requested for crates.io users. No response-time commitment.
- **D6 MSRV policy.** `rust-version` in `Cargo.toml` is the floor; it is raised only when a dependency or a language feature needs it, never in a patch release, and the changelog says so. The `msrv` CI job checks it for every feature set CI builds: the default (ring), the core alone, `aws-lc-rs` and `fips`, installing CMake and Go for the last two as the `providers` job does (closing the analysis's finding F4).
- **D7 docs.rs.** `[package.metadata.docs.rs]` builds the default features on `x86_64-unknown-linux-gnu` only (never `fips`, whose build needs Go and CMake); items behind a feature say so (`doc(cfg)` under the `docsrs` cfg, which docs.rs sets). A `docs-rs` CI job on nightly builds with `RUSTDOCFLAGS="--cfg docsrs -D warnings"`; `lint` keeps its stable `cargo doc` step. The crate-level documentation (`src/lib.rs`) gains an overview and a `no_run` quickstart, instead of pointing to this file.
- **D8 Examples.** `examples/` holds runnable programs, each `required-features = ["tokio"]`: `quickstart` (connect, subscribe, print publications), `recovery` (a recoverable subscription, its position saved and resumed with `since`), `calls` (publish, history, presence and presence stats; RPC needs a server-side handler and is left out) and `delta` (a subscription with fossil delta). They take the endpoint from `FERRIFUGE_EXAMPLE_ENDPOINT`, default `ws://localhost:8000/connection/websocket`, and run against one server configuration, `examples/centrifugo.json`, which the README shows and the @A scenario uses. The `portability` jobs build the examples on every OS on pull requests (`lint` already builds them with `--all-targets`).
- **D9 Crate metadata and README.** `documentation` (docs.rs) is added; `homepage` is not (it would repeat `repository`), nor `readme` (found automatically). The README stops saying the crate is not on crates.io and shows a crates.io dependency instead of a git one, with the version the manifest has; a test checks that every `ferrifuge = "x.y"` in the README matches the manifest's major and minor version and that the MSRV it states matches `rust-version`.

Value clause: As a team adopting Ferrifuge in production, I want it published from CI with a changelog, a security policy, its documentation, compiled examples and a checked toolchain floor, so that we can audit what we depend on, upgrade knowingly and report vulnerabilities privately.

**Structural acceptance:** `release.yml` as D1 (three jobs, SHA-pinned actions, no stored token, no cache); `cargo publish --dry-run` in a pull-request `package` job; the `msrv` job over the four feature sets; the `docs-rs` job as D7; the examples built on every OS on pull requests.

```gherkin
Feature: Release setup

  Scenario: The changelog is in step with the crate's version
    Given the crate's manifest
    Then CHANGELOG.md begins with an Unreleased section, then version sections from newest to oldest, each headed by its version and a date
    And the manifest's version is the newest of those versions, or above it

  Scenario Outline: The release check refuses a tag that does not name the crate's version
    Given the crate's manifest
    When the release check runs for tag "<tag>"
    Then it <outcome>

    Examples:
      | tag                                 | outcome                                   |
      | v followed by its version           | succeeds                                  |
      | v0.0.0                              | fails, naming the version the tag must be |
      | its version without the v           | fails, naming the version the tag must be |
      | v followed by its version and -rc.1 | fails, naming the version the tag must be |

  Scenario: The release check refuses a version without a changelog section
    Given a copy of the crate whose CHANGELOG.md has no section for its version
    When the release check runs for tag "v" followed by its version
    Then it fails, naming the missing section

  Scenario: The published package holds only what the crate needs
    Given the crate's manifest
    When the files of its package are listed
    Then each is under src or proto, or is build.rs, Cargo.toml, Cargo.toml.orig, Cargo.lock, .cargo_vcs_info.json, README.md, LICENSE, CHANGELOG.md or SECURITY.md
    And build.rs, proto/client.proto, proto/LICENSE, LICENSE, README.md, CHANGELOG.md and SECURITY.md are among them

  @A
  Scenario: The quickstart example receives a publication
    Given a server
    When the quickstart example runs against the server's websocket endpoint
    And it prints that it is subscribed to its channel
    And the server API publishes {"text":"hello"} to that channel
    Then the example prints {"text":"hello"}
```

The release-check scenarios pass the outline's tag descriptions to the step, which builds the tag from the manifest's version. The quickstart scenario runs the example binary Cargo builds next to the test binary (not a nested `cargo run`), and waits for its "subscribed" line before publishing.

**Exceptions introduced:** the workflow's publishing, the GitHub release and the CI jobs are verified by running them, not by scenarios: they need the owner's crates.io and repository setup (out of scope above). The first release is the first full run of `release.yml`.
