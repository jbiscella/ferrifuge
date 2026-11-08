# CLAUDE.md — Rust Centrifugo client SDK

Status: DRAFT — blocks 0–39 of N. Further blocks to be appended.

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
| `closed` client state | Not implemented; a closed client reports `disconnected`, and `Client::is_closed()` tells it apart (Block 33) | Spec marks it optional |
| Async runtime | Runtime-agnostic core; tokio adapter behind the `tokio` cargo feature (default) | Owner's choice |
| Scenario execution | cucumber-rs; `.feature` files under `tests/features/` are verbatim copies of the Gherkin in this file | Scenario text is the test; drift is checked mechanically |
| Real server for @A/@B | testcontainers-rs, image `centrifugo/centrifugo:v6.10.1` | `cargo test` needs only Docker |
| Protobuf codegen | prost, `client.proto` vendored at a pinned upstream commit, protoc from `protoc-bin-vendored` | No system protoc needed |
| Crate layout | Single crate `ferrifuge`, edition 2024, license 0BSD; the crate's license expression is `0BSD AND MIT`, as it ships the MIT `proto/client.proto`. Every file not under 0BSD is listed under "Exceptions" in `LICENSE` and in `REUSE.toml` (owner request, 2026-11-07) | Split only when a second crate is justified |
| Core design | Sans-IO: the core is a state machine turning inputs (API calls, frames, transport open/close, timer fired) into actions (open/close transport, send, set timer, emit event); the `tokio` adapter performs the actions | Runtime-agnostic without async traits; @E tests drive the clock directly |
| Events API | `client.events()` returns a `futures::Stream` of a `ClientEvent` enum, one active stream per client, delivered by hand-off with a bounded read-ahead (Block 15); `client.state()` returns the current state | Idiomatic async Rust, runtime-neutral |
| Code hygiene | Lint levels in `Cargo.toml` `[lints]`: no `unsafe`, clippy pedantic, every public item documented; CI denies warnings | Quality gates hold from the first line of SDK code |
| Dependency policy | `deny.toml` (cargo-deny): RustSec advisories, permissive licenses only, crates.io only; the OpenSSL license is allowed globally (owner decision: needed by aws-lc, no on/off variant to maintain) | New dependencies must pass it; an exception needs a reason in `deny.toml` |
| Coverage | cargo-llvm-cov, line floor 70% on the library | Mirrors the owner's JVM projects |
| TLS crypto provider | Chosen at build time by cargo features: `ring` (a default feature), `aws-lc-rs`, or `fips` (aws-lc-rs in FIPS mode). A non-ring build disables default features, so ring is not linked; when several are enabled the strongest wins (`fips` > `aws-lc-rs` > `ring`) | FedRAMP requires FIPS 140-validated cryptography (aws-lc-rs has a FIPS mode, ring has none); ring builds with a C compiler alone, the FIPS build also needs CMake and Go |
| HTTP client (http_stream, sse) | reqwest without default features, `rustls-no-provider`, `stream` and `socks` (`socks` from Block 23), given the client's own TLS configuration | Less code than hyper; no SDK precedent (only centrifuge-js has these transports); keeps the crypto provider ours |
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

- sockjs, webtransport transports (webtransport planned: Block 48)
- map subscriptions, shared poll subscriptions (experimental; planned: Blocks 45 and 47)
- client publish debouncing (Centrifugo PRO; planned: Block 46)
- channel compaction (Centrifugo PRO; owner decision, 2026-11-04): the client never offers it (reversed by the owner decision of 2026-11-06: Block 35)
- browser-only options: online/offline network events, fetch/ReadableStream/EventSource shims (planned: Block 44)
- `closed` state

### Reversed exclusions

| Excluded in | Exclusion | Reversed by | Block |
|---|---|---|---|
| Block 15, out of scope | a callback-style handler API | owner, 2026-11-06, late (item 17) | 41 |
| Block 16, out of scope; "Planned after Block 13" | trace replay "optional, later" | same (item 20) | 42 |
| "Planned after Block 19" | items 17, 20, 24 not adopted | same | 41–43 |
| Block 17, out of scope | a cookie jar | same (item 24) | 43 |
| Block 6, out of scope | custom root certificates | owner, 2026-11-06 (item 2) | 22 |
| Block 14, out of scope | HTTP proxy (CONNECT) support | owner, 2026-11-06 (item 1) | 23 |
| Block 14, out of scope | sleep/resume behaviour | owner, 2026-11-06 (item 4) | 25 |
| Block 17, out of scope | replacing the HTTP headers of a built client (no `set_http_headers`) | owner, 2026-11-06 (item 14) | 32 |
| Block 9, out of scope | delta compression, "the client never requests it" | owner, 2026-11-06 (item 5) | 19 |
| Block 0, out of scope | channel compaction, "the client never offers it" | owner, 2026-11-06 (item 7) | 35 |
| Block 9, out of scope | tag filters (`SubscribeRequest.tf`), "the client never sends one" | owner, 2026-11-06 (item 6) | 35 |
| Block 18, out of scope | automatic batching of every command a step or a burst produces | owner, 2026-11-06 (item 26) | 36 |
| Block 18, out of scope | batching the calls a new connection releases when no batch is open | owner, 2026-11-06 (item 26) | 36 |
| Block 18, out of scope | subscribe commands in the frame of the connect command on websocket open | owner, 2026-11-06, late (item 31) | 36 |

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

**Out of scope:** client certificates; custom root certificates (reversed by the owner decision of 2026-11-06: Block 22); HTTPS, which uses the same configuration once Block 8 adds it.

**Decisions:** cargo features `ring` (in `default`), `aws-lc-rs`, and `fips` (implies `aws-lc-rs`). aws-lc-rs is not pinned (owner decision, Block 6 implementation): from 1.18 its `fips` feature builds AWS-LC-FIPS 4.x, submitted to NIST but not yet certified, while 1.17.x ships the FIPS 140-3 validated 3.x module (CMVP certificates #5314 static, #5298 dynamic), but pinning `<1.18` forces rustls `<= 0.23.43` and rustls-webpki `< 0.103.16` on every build (rustls 0.23.44+ and rustls-webpki 0.103.16 require aws-lc-rs 1.18), and those carry RUSTSEC-2026-0285 and RUSTSEC-2026-0346, fixed in rustls 0.23.45 and 0.23.46 only; a FIPS build therefore uses the 4.x module, and the README states that its certification is pending; revisit when 4.x is certified (the module check below then needs no change) or if rustls supports aws-lc-rs 1.17 again. The supported operating environments are those of the module's security policy. Cargo unifies features, so enabling `aws-lc-rs` or `fips` does not remove a default `ring`: a build without ring sets `default-features = false` (and re-enables `tokio`). Features stay additive: when several providers are enabled the strongest wins at run time (`fips` > `aws-lc-rs` > `ring`), never a compile error; the `tokio` adapter (the only TLS user) fails to compile without a provider feature, with a message naming the three; the runtime-agnostic core (`--no-default-features`) needs none and keeps compiling (Block 0).

**Failed TLS setup (review item C7):** the TLS settings, which today include the platform's root certificates, are built at the first secure connection attempt and shared by every client in the process, as are the HTTP clients built from them; a build that fails is not kept, so the next secure attempt (each transport tried) builds again, and a transient failure (a CA bundle being replaced, no file descriptor left) heals without a restart. A read of the platform's root certificates that reports errors (an unreadable or malformed file, a store or trust setting the system fails to open) still serves the attempt when it found a certificate, but neither it nor the HTTP clients built from it are kept, so a later secure attempt reads again; a read without errors is kept. A file truncated without an error is not detected.

`VERIFY:` the build requirements of aws-lc-rs and of its FIPS mode (C toolchain, CMake, Go) on the CI runners; and that a FIPS build reports FIPS mode at run time (aws-lc-rs API).

Value clause: As an app developer deploying under FedRAMP, I want to build the client with FIPS-validated cryptography, so that its TLS connections meet the FIPS 140 requirement.

**Structural acceptance:** the TLS configuration is built from the selected provider in one place; the default build links only ring, and `--no-default-features --features tokio,aws-lc-rs` (or `tokio,fips`) links only aws-lc (`cargo tree`); mixed builds keep the precedence: `--features aws-lc-rs` (ring still on by default) uses aws-lc-rs, `--features fips` uses FIPS mode, both verified by the per-build unit test; CI builds and tests those three feature sets, installing CMake and Go explicitly for the aws-lc ones, and runs the encrypted scenarios below (and Block 8's HTTPS one) in each; a unit test per build reports the provider in use, and for `fips` the TLS client configuration the transports actually use reports `fips() == true` (rustls checks TLS-level requirements there, beyond the provider's primitives) and the linked AWS-LC module is the 4.x FIPS generation (its reported version), so a dependency update cannot silently switch to another module generation; `deny.toml` allows the OpenSSL license (aws-lc crates: `ISC AND (Apache-2.0 OR ISC) AND OpenSSL`), and the README's license section notes that builds with `aws-lc-rs` or `fips` include OpenSSL-licensed code and its attribution requirement (today only the `fips` build does: aws-lc-fips-sys 0.14.3 is `ISC AND (Apache-2.0 OR ISC) AND OpenSSL`, while aws-lc-sys 0.47.0, the locked version, no longer names OpenSSL; the README says so, 2026-11-07).

The test server's certificate is issued by a test CA that the client trusts through `SSL_CERT_FILE`, which `rustls-native-certs` reads (verified in its source); no custom-root API is added (reversed by the owner decision of 2026-11-06: Block 22).

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

**Out of scope:** sockjs, webtransport, unidirectional http_stream and sse (Block 0); Protobuf over sse (rejected, see decisions); browser-only shims.

**Decisions** (documented in the centrifuge-js README unless noted):
- the client takes a list of (transport, endpoint); a single endpoint string means websocket only;
- transports are tried in order during the initial handshake only, until the server answers on one: a connect reply, an error reply to the connect, a Disconnect push, or, over websocket, a close code from 3000 (review items C2 and C3; centrifuge-js 63ed73e `_transportWasOpen`); reconnects then use only that transport, the first of them after the reconnect backoff delay (Block 2);
- reqwest as the HTTP client (Block 0); cargo-deny checks only the targets the crate supports (Linux, macOS, Windows), so data crates used only on wasm32 are not in scope;
- a client configured with the Protobuf protocol and an sse endpoint is rejected when it is built, with a configuration error naming the combination (Centrifugo's sse carries JSON only; Protobuf over http_stream: Block 27), rather than failing at run time;
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
    And a client with a handshake timeout of 2 s, configured with an unreachable websocket endpoint, the relayed http_stream endpoint and the server's sse endpoint
    When connect is called
    Then within the configured handshake timeout plus tolerance the client emits "connected" over transport "sse"

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

  Scenario: Protobuf with the sse transport is rejected
    Given client options with the Protobuf protocol and an sse endpoint
    When the client is built
    Then building fails with a configuration error naming Protobuf and sse

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

**Out of scope:** subscription tokens, server unsubscribe codes, resubscribe backoff (Block 10); recovery and `getState` (Block 11); server-side subscriptions (Block 12); publish, history, presence (Block 13); join and leave pushes (Block 13); map and shared-poll subscriptions (Block 0); delta compression (`delta` in `SubscribeRequest`, `SubscribeResult` and `Publication`): not in v1, the client never requests it, so the server never sends patches (reversed by the owner decision of 2026-11-06: Block 19); tag filters (`SubscribeRequest.tf`): not among the spec's subscription options, not in v1, the client never sends one (reversed by the owner decision of 2026-11-06: Block 35).

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

**Out of scope:** publish debouncing (Block 0); batching several calls into one frame, Block 18 (owner decision, 2026-11-04: the scope is centrifuge-js parity, which includes its batching API); automatic batching, Block 36.

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
- **Validation harness:** seeded chaos against the fake server, checking invariants like I1–I8 of centrifugal/centrifuge-rust `docs/spec.md` §11.3, including "exactly once" across reconnects; read its §11.3 and §12 first. Replaying the traces of its TLA+ model is optional, later, keeping its MIT notice (reversed by the owner decision of 2026-11-06, late: Block 42). Specified as Block 16.

## Block 14 — Footprint, performance and fleet behaviour

**Goal:**
- a client costs almost nothing at rest;
- each message costs a constant amount, whatever the number of subscriptions;
- memory stays flat over time;
- a fleet of clients spreads its reconnects and token refreshes instead of synchronising them.

**Dependencies:** Blocks 2, 4, 5, 9. Subscription-token refresh jitter applies once Block 10 is implemented.

**Out of scope:**
- HTTP proxy (CONNECT) support (reversed by the owner decision of 2026-11-06: Block 23);
- sleep/resume behaviour (reversed by the owner decision of 2026-11-06: Block 25);
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
- a callback-style handler API: ferrifuge has streams only (reversed by the owner decision of 2026-11-06, late: Block 41);
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
- **One active stream per object.** The client and each subscription have one active stream. A second `events()` call (or `publications()`, Block 39) ends the earlier stream, whose next item is its end, and every event not yet taken moves to the new stream, in order. `next()` is cancel-safe: a cancelled read loses no event.
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
- replaying the traces of centrifuge-rust's TLA+ model (optional, later, keeping its MIT notice) (reversed by the owner decision of 2026-11-06, late: Block 42);
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
- **Public API:** none added. A hook the runners need is gated by the private feature `__test-hooks` and `#[doc(hidden)]`, as `Client::read_ahead` is, outside the semver check (Block 37 D1; it was `test-util` until Block 37).
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
- replacing the HTTP headers of a built client (no `set_http_headers`): neither the spec nor centrifuge-js has it (reversed by the owner decision of 2026-11-06: Block 32);
- changing the current connection: every option and setter here applies from the next connection attempt and sends no command;
- a timeout on the `get_data` callback (none in centrifuge-js, none in Block 9);
- the other `ConnectRequest` and `ConnectResult` fields of the vendored `client.proto` (`flag`, `profile`, `dict`, `time`);
- a cookie jar: a native client has none; an application sets `Cookie` as an HTTP header (reversed by the owner decision of 2026-11-06, late: Block 43).

**Decisions** (reviewer, 2026-11-06, delegated by the owner):
- **Connect data.** `Config::data: Option<Vec<u8>>`, raw JSON with JSON, any bytes with Protobuf, encoded as subscription data is (Block 9), is sent as `ConnectRequest.data` with every connect command (spec, "Client common options": "option to set connect data"; `client.proto`, `ConnectRequest.data`). `ClientBuilder::get_data` sets an async callback returning `Result<Vec<u8>, DataError>` (Block 9's type); when set, it is called before every connection attempt, each transport of a fallback sequence included, and its result is sent instead of `data` (centrifuge-js 63ed73e `_startReconnecting`). It runs after the reconnect delay, never before it, so Block 14's jitter spreads a fleet's callback calls as it spreads its connects.
- **Order with the token.** As Block 10 for subscriptions: when the attempt needs a token, the token callback runs first and `get_data` only once a token is obtained, so a denied, failed or empty token calls no `get_data` for that attempt; an attempt that needs no token calls `get_data` without the token callback (centrifuge-js `_startReconnecting`).
- **get_data failures.** `DataError::Unauthorized` disconnects the client with code 1 "unauthorized", with no error event and no reconnect (centrifuge-js `_handleGetDataError`, `_failUnauthorized`). `DataError::Failed` emits an `error` event of the new kind `ErrorKind::ConnectData`, code 12 "bad configuration", carrying the callback's message, and retries after the reconnect backoff (Block 2) while the client stays `connecting` (centrifuge-js `_handleGetDataError`: type `connectData`, `errorCodes.badConfiguration`). A `disconnect()` while the callback runs abandons it: its answer is ignored and opens no transport (centrifuge-js `_attemptAborted`; Block 16 I5). `DataError`'s documentation and `Display` stop naming subscriptions only.
- **Server data at connect.** `ClientEvent::Connected` gains `data: Option<Vec<u8>>`: `ConnectResult.data` exactly as received, never re-serialized, as `SubscribeResult.data` on `subscribed` (Block 9) (`client.proto`, `ConnectResult.data`; centrifuge-js `_connectResponse`, `ctx.data`; the spec does not mention it).
- **Two kinds of headers**, named after the Centrifugo proxy options that forward them, which v6.9.0 split: v6.10.1 forwards an emulated value only when its name is in `emulated_headers`, never through `http_headers` (`internal/proxy/http.go`). No option is called just `headers`, a word meaning real headers in the spec and emulated ones in centrifuge-js (centrifuge-rust `docs/spec.md` §5.1, decision 12). Both are in scope as centrifuge-js parity (its `headers` option and `setHeaders` method), as the owner read "centrifuge-js parity" for the batching API (Block 13).
  - `ClientBuilder::http_headers`: real HTTP headers on the websocket upgrade request (spec, "Client common options") and on every HTTP request of `http_stream` and sse, the stream request and the emulation requests, so that a gateway checking a header still admits a client that fell back (Block 8). Names are case-insensitive, one value each, a later value replacing an earlier one. A name or value that is not valid HTTP, or a header the transport sets (`Host`, `Connection`, `Upgrade`, `Content-Length`, `Transfer-Encoding`, `Content-Type`, `Accept`, any `Sec-WebSocket-*`, and `Proxy-Authorization` from Block 23), fails `build()` with a configuration error naming the header, never its value.
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
    Given a client with token "old", no token callback and a minimum reconnect delay of 2 s, connected to the fake server
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
- automatic batching of every command a step or a burst produces (centrifugal/centrifuge-rust `docs/spec.md` §9.10, decision 11): only the application's batches and the subscribe commands of a connect reply are batched (reversed by the owner decision of 2026-11-06: Block 36);
- subscribe commands in the frame of the connect command on websocket open (centrifuge-js 63ed73e `centrifuge.ts:1243-1258`): ferrifuge subscribes once connected (Block 9), and subscribing earlier is optimistic subscription (centrifuge-rust §9.11, phase 3) (reversed by the owner decision of 2026-11-06, late: Block 36);
- batching the calls a new connection releases (Block 13) when no batch is open: each still goes in its own frame (reversed by the owner decision of 2026-11-06: Block 36);
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
- **D8 Connect reply.** The subscribe commands a connect reply starts go out as one batch, split by the cap (centrifuge-js `_connectResponse`, `centrifuge.ts:2350-2352`), and are never held by an application's batch. Over websocket, the subscribes that fit go in the connect command's frame instead (Block 36, D5); D8 applies to the rest.
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
| 18c | the subscribe commands of a connect reply in one batch | "A reconnect over an emulated transport sends its subscribe commands in one request", "The subscribe commands of a connect reply are split at the frame cap", "An open batch does not hold the subscribe commands of a connect reply" |
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

  @E
  Scenario: A reconnect over an emulated transport sends its subscribe commands in one request
    Given a client core over http_stream with three subscriptions subscribed
    When the server closes the transport with a reconnect-advised code
    Then after the next connect reply the core sends one emulation request carrying the three subscribe commands
    And the three subscriptions reach state "subscribed"

  @E
  Scenario: The subscribe commands of a connect reply are split at the frame cap
    Given a client over http_stream with a batch frame cap of 1000 bytes and five subscriptions whose subscribe commands encode to 300 bytes each
    When connect is called and the server answers the connect command
    Then the client sends two frames, carrying three and then two subscribe commands

  @E
  Scenario: An open batch does not hold the subscribe commands of a connect reply
    Given a client over http_stream with a subscription in state "subscribing"
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
- a tag filter together with delta: a subscription with both is rejected when built (Block 35, D1; `client.proto`, `SubscribeRequest.tf`: "disables delta compression"; centrifuge-js `subscription.ts:368-370`);
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

The owner decided parity with centrifugal/centrifuge-rust and the listed community requests (`ANALYSIS-PARITY.md`, branch `analysis/parity`, items 1–28), then centrifuge-js 63ed73e parity where it fits a native client (items 29–39, owner decision 2026-11-06, late), under the clean-room rule, and delegated the open choices to the reviewer with the criterion "what would be state of the art" (mature, maintained Rust network clients: reqwest, hyper-util, tonic, aws-sdk-rust, async-nats, rumqttc, tokio-tungstenite, rustls; the Rust API Guidelines). Each item becomes a block with its value clause and scenarios when its turn comes, one spec PR then the implementation; each block records in Block 0 the exclusion it reverses.

Decisions that bind the blocks:
- **Items adopted** as the analysis recommends, except: item 1's CONNECT and SOCKS5 handshakes use hyper-util's maintained `Tunnel` and `SocksV5` connectors, not in-house code (Block 0, "Components"; reqwest 0.13.5 uses them), so a CONNECT failure reports 407 or "unsuccessful", not every status; item 2's raw `rustls::ClientConfig` is public without a semver-exempt feature (async-nats, tokio-tungstenite and rumqttc expose it), the fips build keeping webpki verification; items 11 and 12 land in one breaking release that also marks `Config` `#[non_exhaustive]` (as tungstenite's `WebSocketConfig`). Items 17, 20 and 24 are adopted (owner decision, 2026-11-06, late; reversals recorded in Block 0): handlers keep Block 15's guarantees, and a delivery mode that gives one up says which in its documentation; item 20 restores Block 16's trace replay, reading the trace data files for its block only, keeping their MIT notice, never centrifuge-rust's `crates/` source. Item 25 is Block 40's wasm32 build check, then a browser adapter (design B) taking item 39's browser-only options. centrifuge-js 63ed73e parity (items 29–39) is adopted except item 34 (SockJS: Centrifugo v6 removed it; no native client needs it). Experimental APIs (items 29, 30, 33) are behind a non-default `unstable` feature, documented unstable and pinned to 63ed73e. Item 5 is Block 19.
- **Proxies (item 1):** environment variables, then OS settings, by default, with an opt-out (reqwest 0.13.5 `system-proxy` default, aws-sdk-rust since behaviour version 2025-08-07, ureq 3).
- **Logging (item 9):** `tracing` with its `log` feature, in the tokio adapter only, as a regular dependency of the `tokio` feature; DEBUG and TRACE, WARN for server anomalies; spans skip their fields, values go through the redacting `Debug`; no payload switch; a capture test; the docs warn that dependencies log secrets at TRACE through `log`.
- **Release (item 21):** a tag-triggered `release.yml` with crates.io trusted publishing (`rust-lang/crates-io-auth-action`, environment `release`, version and `cargo-semver-checks` checked, actions pinned by SHA), a hand-written Keep a Changelog `CHANGELOG.md`, `SECURITY.md` pointing to GitHub private vulnerability reporting, docs.rs metadata, and an MSRV raised only in a minor version and only when needed. The account, first publish, trusted-publisher registration and repository settings are the owner's.
- **JSON helpers (item 18):** always available, no `serde` feature: serde and serde_json are unconditional dependencies, so a feature would only hide API.
- **Components audit:** the in-house sse decoder is replaced by a maintained parser (sse-parse, finesse or sse-core, chosen in that block's spec by fuzzing and benchmarks), the http_stream splitter by tokio-util's `AnyDelimiterCodec` with a maximum length, the content-type check by a maintained media-type parser (mediatype, Block 27 D4); the Block 5c drift (`machine.rs` calling `json::` directly) is refactored through `Codec`, with no API change.
- **Handlers (item 17):** a handler trait (`ClientHandler`, `SubscriptionHandler`) with a default no-op method per event kind, and a blanket implementation for `FnMut(Event) + Send` closures; one handler per object, taking over its stream (Block 15); methods are synchronous. Delivery modes: `Task` (default), `Thread` (one OS thread per client, started lazily), `Inline` (gives up "timers keep running while events are held", and says so). A panicking handler is caught, its event is not taken, and the client disconnects with code 2 "exception in event handler" without reconnecting, as centrifuge-js 63ed73e does (README.md:1063-1072, centrifuge.ts:1813). `Client: Clone` around an `Arc`, the last drop shutting it down, with `downgrade` and `upgrade` as tokio's `WeakSender`; `close()` ends every clone. Deadlock detection: a thread-local marker around each handler call; under `Inline` a call from a handler fails at once with `WouldDeadlock`; under `Task` and `Thread` a marked call fails with it only when reading pauses on a full read-ahead while that handler still runs.
- **Cookies (item 24):** a `CookieJar` trait in the shape of reqwest's `CookieStore`, and `MemoryJar` on cookie_store 0.22.1 (`public_suffix` on, `log_secure_cookie_values` off), opt-in through `cookie_jar` behind a non-default `cookies` feature. The adapter sets `Cookie` and reads `Set-Cookie` itself on every transport, merging the jar's cookies with a `Cookie` from `http_headers` into one header (RFC 6265 §5.4), without reqwest's `cookies` feature, which skips the jar when a `Cookie` header is set (reqwest 0.13.5 `src/cookie.rs:242`).
- **Optimistic subscriptions (item 31):** on by default, with no option, over websocket and WebTransport, as centrifuge-js does; only subscribes that fit `batch_frame_bytes` together with the connect go in its frame, and subscribes still waiting for their callbacks wait for the connect reply. Verified: the server handles a frame's commands in order (centrifuge `handler_websocket.go:416-425`), and after a connect error it drops the rest of the frame and answers the next frame with 3501 (`client.go:1965-1968`, `:2025-2037`).
- **Publish debouncing (item 32):** in the core, also for stream publishes (key ""); the first publish is sent at once; a superseded publish resolves with the result of the send that carried later data, never with a success before the server answers.
- **WebTransport (item 33):** the `wtransport` crate, whose `fips` build maps to quinn's `aws-lc-rs-fips` and rustls `fips`. `VERIFY:` that it accepts the client's own `rustls::ClientConfig`.
- **Connector (item 35):** a `Connector` trait taken as an `Arc` and returning a boxed future (the shape of reqwest's `dns::Resolve`), with a blanket implementation for closures, given the endpoint as an `http::Uri` and yielding `AsyncRead + AsyncWrite + Send + Unpin + 'static`; Ferrifuge runs TLS and the websocket handshake over it. Websocket only: a connector with an HTTP transport, or with a proxy setting given with `ClientBuilder::proxy`, fails `build()`, as reqwest's connector layer cannot carry an outside connection (reqwest 0.13.5 `client.rs:2475-2481`, `connect.rs:1346-1352`); with a connector no proxy setting is read, the environment's and the system's included (Block 37 D4).
- **Unstable APIs (items 29, 30, 33):** behind a non-default `unstable` feature, since the release check runs cargo-semver-checks on default features (Block 20, D1; precedent: `tokio_unstable`, `reqwest_unstable`).
- **Copied trace data (item 20):** the trace files copied from centrifuge-rust sit in `tests/fixtures/tla-traces/` with its MIT text and copyright line (`LICENSE`) and a README naming the source commit, as the fossil vectors do; `LICENSE`'s "Exceptions" and `REUSE.toml` gain an entry for them (owner request, 2026-11-07).

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
| 27 | components: sse and http_stream decoders, content-type check; and 36: Protobuf over http_stream (stream and emulation requests included) |
| 28 | 15: tokio runtime handle (fixes `Client::new` panicking outside a runtime) |
| 29 | 11 and 12: handshake timeout, maximum incoming message size, `Config` `#[non_exhaustive]` |
| 30 | 10: `ready()` |
| 31 | 8 and 38: `statistics()`, delta statistics |
| 32 | 14 and 27: `set_http_headers`, `Subscription::set_data` |
| 33 | 16: `close()` |
| 34 | 23: table of codes |
| 35 | 6, 7, 28 and 37: tag filters, channel compaction, `Publication.channel`, `has_recovered_publications` |
| 36 | 26 and 31: automatic command batching, optimistic subscriptions |
| 37 | 19, 22 and 35: test constructors, in-memory server double, injectable connector |
| 38 | 18: typed JSON helpers |
| 39 | 13: publications-only stream |
| 40 | 25: wasm32 build check |
| 41 | 17: handler API, `WeakClient`, delivery modes, deadlock detection |
| 42 | 20: TLA+ trace replay |
| 43 | 24: cookie jar |
| 44 | 25 and 39: browser adapter, browser-only options |
| 45 | 29: map subscriptions (unstable) |
| 46 | 32: publish debouncing |
| 47 | 30: shared poll subscriptions (unstable) |
| 48 | 33: WebTransport (unstable) |

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
  - `verify` (no environment; `contents: read`, `actions: read`): runs `scripts/check-release-tag` (D2), checks that the tagged commit is on `main` and that the `ci` run of the push to `main` with that head commit has concluded with success (a run still in progress fails the release, which is re-run later), and runs `cargo-semver-checks` (a pinned version, default features and `test-util`, as the pull-request job; Block 37 D7) against the crate's latest version on crates.io, skipped only when the index has none;
  - `publish` (`needs: verify`; environment `release`; `id-token: write`): `cargo publish` with a token from `rust-lang/crates-io-auth-action` (trusted publishing, no stored token);
  - `github-release` (`needs: publish`; `contents: write`): the GitHub release with that version's changelog section as its notes; re-running it never publishes again.
  Every action is pinned by commit SHA; checkouts use `persist-credentials: false`; no build cache is used; the toolchain action names its toolchain.
- **D2 Release check.** `scripts/check-release-tag` (bash and cargo only, so it runs on every CI OS) takes a tag and succeeds only when the tag is exactly `v` followed by the `Cargo.toml` version, without a suffix, and `CHANGELOG.md` has a section for that version; otherwise it fails naming the version the tag must be, or the missing section. The workflow and a test run it.
- **D3 Package contents.** `Cargo.toml` lists what is published (`include`): `src`, `proto`, `build.rs`, `README.md`, `LICENSE`, `CHANGELOG.md`, `SECURITY.md`; Cargo adds `Cargo.toml.orig`, `Cargo.lock` and `.cargo_vcs_info.json`. Tests, benches, fixtures, workflows, `.claude`, `CLAUDE.md`, `deny.toml` and the logo stay out (today 155 files are packaged). The published crate's own unit tests do not build, as `src/fossil.rs` includes fixtures that are not shipped; that is accepted (they run in this repository). A `package` CI job on pull requests runs `cargo publish --dry-run`, which builds the crate as published (verified: `build.rs`, `proto/` and `protoc-bin-vendored` work; the path-only dev-dependency on itself is dropped).
- **D4 Changelog.** `CHANGELOG.md` in Keep a Changelog form, written by hand: `## [Unreleased]` on top, then `## [x.y.z] - YYYY-MM-DD` for each version from 0.1.0, newest first, with Added, Changed, Deprecated, Removed, Fixed and Security subsections, backfilled from the history (the version each block reached, `git log -G'^version'` on `Cargo.toml`, and the block table of this file); no compare links for versions never tagged. A pull request that changes the public API or behaviour adds an `Unreleased` entry; the release moves them under the new version.
- **D5 Security policy.** `SECURITY.md`: reports go through GitHub private vulnerability reporting (the repository's "Report a vulnerability" button), never public issues; the supported versions are the latest minor release; fixes are released as a patch of it and announced as a GitHub security advisory, with a RustSec advisory requested for crates.io users. No response-time commitment.
- **D6 MSRV policy.** `rust-version` in `Cargo.toml` is the floor; it is raised only when a dependency or a language feature needs it, never in a patch release, and the changelog says so. The `msrv` CI job checks it for every feature set CI builds: the default (ring), the core alone, `aws-lc-rs` and `fips`, installing CMake and Go for the last two as the `providers` job does (closing the analysis's finding F4).
- **D7 docs.rs.** `[package.metadata.docs.rs]` builds the default features and `test-util` (Block 37 D7) on `x86_64-unknown-linux-gnu` only (never `fips`, whose build needs Go and CMake); items behind a feature say so (`doc(cfg)` under the `docsrs` cfg, which docs.rs sets). A `docs-rs` CI job on nightly builds with `RUSTDOCFLAGS="--cfg docsrs -D warnings"`; `lint` keeps its stable `cargo doc` step. The crate-level documentation (`src/lib.rs`) gains an overview and a `no_run` quickstart, instead of pointing to this file.
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

The release-check scenarios pass the outline's tag descriptions to the step, which builds the tag from the manifest's version. The quickstart scenario runs the example's binary, which its step builds with `cargo build --example quickstart` and the features of the test build, since `cargo test --all-targets` builds examples only as test harnesses (not a nested `cargo run`), and waits for its "subscribed" line before publishing.

**Exceptions introduced:** the workflow's publishing, the GitHub release and the CI jobs are verified by running them, not by scenarios: they need the owner's crates.io and repository setup (out of scope above). The first release is the first full run of `release.yml`.

---

## Block 21 — Logging

**Goal:** an operator of a fleet can see why a client reconnects, falls back, resubscribes or stops, by turning on a log filter, without a rebuild and without any secret reaching a log (parity item 9; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art).

**Dependencies:** Blocks 3 (close code 1009), 5c and 5d (the codec and transport seams), 8 (transports and fallback), 14 (the @P gates), 15 (the read-ahead), 17 (the redacting `Debug` implementations, `Action` exhaustive), 19 (delta errors), 20 (changelog).

**Out of scope:**
- logging in the sans-IO core: it stays free of any logging dependency and reports what only it sees as an action (D4);
- metrics and counters (item 8, `statistics()`, Block 31);
- a switch to log payloads: no payload reaches a Ferrifuge record;
- the records of Ferrifuge's dependencies: tungstenite, reqwest and rustls log through `log`, hyper-util through `tracing`, at their own levels; the documentation warns about them (D6);
- INFO and ERROR records: Ferrifuge's events are WARN, DEBUG and TRACE only (D3).

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Dependency.** `tracing` 0.1.44 without default features, with `std` and `log`, a regular dependency of the `tokio` feature, with no feature of its own: with no subscriber a callsite costs a few relaxed atomic loads, and the default build already links `tracing` through hyper-util. With `log`, Ferrifuge's records go to the `log` logger as long as no `tracing` dispatcher, global or scoped, has ever been set in the process (tracing-core 0.1.36 `dispatcher::has_been_set`); once one is, they go to `tracing` only, never to both. No `#[instrument]` (no `attributes` feature): spans are created by hand.
- **D2 Spans.** A DEBUG span `client` per client, with one field `client`: a number unique within the process, given when the client is built. A DEBUG span `subscription` per client-side subscription, child of its client's, with one field `channel`. Server-side subscriptions log in the client's span with a `channel` field on the record. Spans carry no other field. Every record is made in its span: the adapter enters the client's span where it feeds the machine (application calls and stream reads run on the application's tasks), instruments the driver and every task it spawns, and gives subscription records their span as explicit parent. WARN records also carry `client`, and `channel` when they have one, so they keep their client under a filter that disables DEBUG spans and through `log`, which drops span fields. A `__test-hooks` hook (Block 37 D1) gives the tests a client's number.
- **D3 Levels.** Every event's target starts with `ferrifuge`. Records give their values as fields: `state`, `code`, `reason`, `transport`, `endpoint`, `channel`, `bytes`, `delay`, `kind`; `gap` from Block 25 (D10).
  - WARN, server anomalies: a frame that is not client protocol data and a reply without the result of the command it answers (D4); a delta that cannot be applied (Block 19); a message larger than the size limit; a websocket closed with 1009. A reply whose id matches no command is no anomaly (the reply to a call abandoned or timed out): it shows only at TRACE, as an input.
  - DEBUG: every state change of the client and of each subscription, client-side and server-side, with its code and reason; each transport attempt (kind and endpoint, D5), its opening, its failure (the error's message) and its close (code and reason); the next transport of a fallback; each reconnect and resubscribe scheduled, with its delay; each callback (token, data, getState) started and how it ended (got a value, failed, denied, panicked), never the value; the read-ahead filling (reading stops) and draining (reading resumes); each call that fails, with its kind and code.
  - TRACE: every input the adapter gives the machine and every action the machine returns, through their `Debug` (D5); each frame sent or received, with its transport kind, frame type and length.
- **D4 Anomalies only the core sees.** A frame that is not client protocol data, and a reply without its command's result, make the core reconnect with code 1 "transport closed", as a lost transport does (`machine/inbound.rs` `frame`, `machine/calls.rs` `call_reply`, `machine.rs` `command_reply`). The core reports each, before the actions of that reconnect, with a new action `Action::Anomaly(Anomaly)`: `Anomaly` is a `#[non_exhaustive]` enum naming the transport and, for a frame, its length, for a reply, the kind of command it answers, never payload bytes. The adapter logs it at WARN; another owner of the machine may ignore it. `Action` stays exhaustive (Block 17), so the variant is breaking (D9). The core logs nothing itself. The adapter sees the other anomalies of D3 itself: the delta error event, its size limit, the 1009 close. Block 36 adds `Anomaly::ReplyBeforeConnect`.
- **D5 Redaction.** Every value logged goes through a redacting `Debug` or `Display`: tokens as `<redacted>`; connect, subscription, publication, RPC, publish, send, message and server data, and connection and channel info, as a length; headers and tags by name only. This block makes the `Debug` of `ClientEvent`, `SubscriptionEvent`, `ServerSubscriptionEvent`, `Subscribed`, `Publication` and `ClientInfo` redacting: `Action::Emit` and `Action::EmitSubscription` carry them, and their derived `Debug` shows data bytes. Endpoints, of every transport and the emulation endpoint, are logged as scheme, host, port and path: userinfo, query and fragment are left out, as each may hold a credential; an endpoint that does not parse is logged as `<invalid>`; `ClientBuilder`'s `Debug` shows them the same way. The sse request URL, whose `cf_connect` carries the connect command and its token, is never logged. No error message carries a URL: the emulation request's error drops it (`reqwest::Error::without_url`), as the stream request's already does.
- **D6 Documentation.** A "Logging" section in the crate documentation and the README: the targets, levels and fields (D2, D3); how to enable them with `tracing-subscriber`'s `EnvFilter` or with `env_logger` (`RUST_LOG=ferrifuge=debug`); that through `log` records carry no span fields, the WARN records keeping their own `client`, and span entries and exits appear at TRACE under the target `tracing::span::active`; and a warning: Ferrifuge's dependencies log on their own, and at TRACE tungstenite 0.30.0 logs the whole upgrade request with its query and header values (`handshake/client.rs:197`), every message received (`protocol/mod.rs:475`) and every frame sent (`:768`), the connect frame and its token included, so production filters keep dependencies at INFO or above (`RUST_LOG=info,ferrifuge=trace`). A `tracing` application sees the dependencies' `log` records only through `tracing-log`.
- **D7 Capture.** The BDD run installs once a process-wide subscriber (`tracing-subscriber` 0.3.23 as a dev-dependency, registry only) with a test layer recording each event with its level, target, fields and the fields of the spans in its scope. A scenario reads the records in its clients' spans, by their `client` field. The secret checks read every record whose target starts with `ferrifuge`, in a span or not, and look for each secret as text and as the `Debug` list of its bytes; the secrets differ per scenario, so concurrent scenarios cannot match each other's. The `log` path is tested in its own binary, `tests/log_facade.rs`, which sets a capturing `log` logger and never a `tracing` dispatcher (one set anywhere in a process stops `log` records for good): a client connects to the fake server with a token, its DEBUG state changes reach the logger under targets starting with `ferrifuge`, and none of Ferrifuge's records holds the token.
- **D8 Footprint.** With its level off, logging adds no allocation per message, call or state change: `tracing` formats fields only for an enabled record; a span is kept per client and per subscription. The Block 14 @P gates pass unchanged.
- **D9 Version and changelog.** 0.13.0 (D4, D5); `Unreleased` entries: "Added" logging; "Changed" `Action::Anomaly` and the `Debug` output of events showing data as lengths; "Fixed" the emulation request's error no longer carrying its URL.

Value clause: As an operator of a fleet of devices running Ferrifuge, I want each client to log why it reconnects, falls back, resubscribes or stops, at levels I turn on without a rebuild, with no token, payload or header value in any record, so that I can diagnose connection problems in production without exposing user secrets.

**Structural acceptance:** `tracing` is a normal dependency of the `tokio` feature only (`cargo tree --no-default-features -e normal -i tracing` finds no package); no logging call in `src/machine*`, `src/codec.rs`, `src/json.rs`, `src/protobuf.rs`, `src/wire.rs` or `src/fossil.rs`; every event's target starts with `ferrifuge`; a unit test checks that the `Debug` of every event type holding data or info shows none of it; `tests/log_facade.rs` passes; the Block 14 @P gates pass unchanged.

```gherkin
Feature: Logging

  @D
  Scenario: State changes are logged with their codes and reasons
    Given a client connected to the fake server, its log records captured
    When the fake server disconnects the client with code 3001 and reason "going away"
    Then a DEBUG record in the client's span says it is connecting with code 3001 and reason "going away"
    And then a DEBUG record in the client's span says it is connected over transport "websocket"

  @D
  Scenario: Subscription records are in the subscription's span
    Given a client connected to the fake server, its log records captured
    When a subscription to channel "news" is created and subscribed
    Then a DEBUG record in the span of the subscription to "news" says it is subscribed

  @D
  Scenario: A frame that is not client protocol data is logged as a warning
    Given a client connected to the fake server, its log records captured
    When the fake server sends the text frame "not-json-7f3a"
    Then a WARN record in the client's span says a frame from the server could not be decoded, with a length of 13 bytes and the client's number
    And no record contains "not-json-7f3a"

  Scenario: Endpoints are logged without credentials, query or fragment
    Given a client configured with the unreachable websocket endpoint "ws://user:pw-5c1e@127.0.0.1:9/connection/websocket?token=tk-8d2b#f" and the unreachable http_stream endpoint "http://user:pw-5c1e@127.0.0.1:9/connection/http_stream?token=tk-8d2b", its log records captured
    When connect is called
    Then a DEBUG record in the client's span names a websocket attempt to "ws://127.0.0.1:9/connection/websocket"
    And then a DEBUG record in the client's span names an http_stream attempt to "http://127.0.0.1:9/connection/http_stream"
    And no record contains "pw-5c1e" or "tk-8d2b"

  @D
  Scenario: No secret reaches a record
    Given a client with a connection token, connect data, an HTTP header and an emulated header, each holding a distinct secret, its log records captured
    And the fake server answers connects with data holding a distinct secret and answers RPC with the data it received
    And the client is connected to the fake server
    When a subscription to channel "news" with a subscription token and subscription data, each holding a distinct secret, is subscribed
    And the fake server pushes a publication on "news" whose data holds a distinct secret
    And the client publishes data holding a distinct secret to "news" and calls RPC "echo" with data holding a distinct secret
    Then the client's span holds TRACE records of inputs, actions, and frames sent and received
    And no record contains any of those secrets, as text or as the Debug list of its bytes

  @D
  Scenario: The records of two clients are told apart
    Given two clients, each connected to its own fake server, their log records captured
    When the first client's fake server disconnects it with code 3001 and reason "going away"
    Then the record saying a client is connecting with code 3001 is in the first client's span only

  @E
  Scenario: The core reports a frame that is not client protocol data
    Given a machine connected over a websocket transport
    When it is given a frame of 13 bytes that is not client protocol data
    Then its actions report an anomaly naming that transport and a length of 13 bytes
    And then they close the transport and emit "connecting" with code 1 and reason "transport closed"

  @E
  Scenario: The core reports a reply without its command's result
    Given a machine connected over a websocket transport, with an RPC call sent
    When it is given the reply to that call carrying a history result
    Then its actions report an anomaly naming that transport and the command kind "rpc"
    And then the call fails with a "connection closed" error and the machine emits "connecting" with code 1
```

The secrets are distinct random strings per scenario, so a match cannot be a coincidence of the scenario's text or of another scenario's. "Says" in a Then step means the record's fields (D3), not the wording of its message. The fake server gains an RPC mode answering with the data it received. The @E scenarios are unit tests of the machine.

**Exceptions introduced:** `tests/log_facade.rs` runs outside cucumber (D7): once a process sets a `tracing` dispatcher, `tracing` sends nothing more to `log`, so the facade check needs its own binary.

---

## Block 22 — Platform TLS verifier, extra roots, raw rustls configuration

**Goal:** a client trusts what the operating system trusts, and the private certificate authorities the application gives it, and an application can give its own TLS configuration, so that it connects through corporate TLS inspection and private PKI without turning verification off (parity item 2; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art).

**Dependencies:** Blocks 6 (crypto provider, the one-place TLS build, review item C7), 8 (HTTPS for http_stream and sse), 17 (`ClientBuilder`), 20 (changelog), 21 (redacting `Debug`, the dependencies' records).

**Out of scope:**
- Android: the platform verifier there needs the application to initialise it through JNI (rustls-platform-verifier 0.7.1 `src/android.rs:90` panics otherwise) and takes no extra roots; Block 24 (item 3) decides Android's verifier. Until then Android keeps today's path, extra roots included (decided by Block 24: the platform verifier after one setup call, extra roots refused there);
- a switch accepting invalid certificates: not offered (the analysis, item 2); the raw configuration covers development needs;
- client certificates (mutual TLS): the raw configuration carries them; no dedicated API;
- certificate pinning, revocation settings: the raw configuration, or the operating system's own;
- proxies (Block 23).

This block reverses Block 6's exclusion of custom root certificates; Block 0's "Reversed exclusions" gains the row | Block 6, out of scope | custom root certificates | owner, 2026-11-06 (item 2) | 22 |.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Verifier per build and platform.** Chosen in one function, Block 6's one-place TLS build.
  - ring and aws-lc-rs builds on macOS, iOS and Windows: `rustls-platform-verifier` 0.7.1, given the build's crypto provider (it forces none). The operating system makes the trust decision: its store, its distrust settings, name constraints, revocation and intermediate fetching as it does them. `SSL_CERT_FILE` and `SSL_CERT_DIR` are no longer read there (rustls-native-certs 0.8.5 `lib.rs:118-124` reads them on every platform today).
  - Linux and the other Unix systems but Apple and Android: webpki over `rustls-native-certs`, built by Ferrifuge as today, with the extra roots added. The platform verifier there is the same verification (0.7.1 `src/verification/others.rs:85-111`), but it only logs a root read's errors (`:98-100`), so its caller cannot tell an errored read from a complete one and C7 could not hold.
  - The fips build, on every platform: webpki over the platform's root certificates and the extra roots, as today. The platform verifier checks chains inside the operating system, outside the AWS-LC FIPS module, while `ClientConfig::fips()` would still report true (rustls 0.23.46 `client_conn.rs:396-409`) (plan "Planned after Block 19").
  - On the webpki path, a read finding no platform certificate builds with the extra roots alone (as `others.rs:102-107`); with none, it fails as today.
- **D2 Extra root certificates.** `ClientBuilder::root_certificates(roots)`, `roots` an `IntoIterator<Item = rustls::pki_types::CertificateDer<'static>>`, adding to earlier calls as `http_headers` does: the shape of reqwest 0.13.5 `tls_certs_merge`, tonic's `trust_anchors` and the platform verifier's `new_with_extra_roots`; no Ferrifuge certificate type, since rustls is public (D3). The documentation shows PEM parsing with `rustls::pki_types::pem::PemObject::pem_slice_iter`. They are trusted in addition to the platform's, on every platform and build. `build()` checks each with webpki and fails with a configuration error naming the first that does not parse by its index. An empty list is no extra roots.
- **D3 Raw configuration.** `ClientBuilder::tls_config(Arc<rustls::ClientConfig>)` (tokio-tungstenite's `Connector::Rustls`, rumqttc): used for `wss://` and every `https://` request of http_stream and sse, with no root, provider or verifier of Ferrifuge's. Ferrifuge only sets the ALPN list of its copy to `http/1.1`, as its transports speak HTTP/1.1 only and a configuration offering `h2` would fail them (hyper-rustls rewrites ALPN the same way). `ferrifuge::rustls` re-exports rustls (as tokio-rustls does). Public, not behind a semver-exempt feature (plan). `build()` fails with a configuration error when both `tls_config` and extra roots are given, naming both, and, in the fips build, when the configuration's `fips()` is false. The verifier of a raw configuration is the application's: the README says so for the fips build.
- **D4 Sharing and C7.** A client without extra roots or a raw configuration keeps the process-wide TLS settings and HTTP clients of Blocks 6 and 8. A client with extra roots builds its own at its first secure attempt, with C7's rules where roots are read (a failed build is not kept; a read with errors serves the attempt and is read again next time); on the webpki path it holds its own root store, so the README tells an application with many clients to give them one raw configuration. A raw configuration is shared as given; its HTTP clients are built once per client, a failed build not kept. The Block 14 @P gates pass unchanged.
- **D5 Logging.** `ClientBuilder`'s `Debug` shows the number of extra roots and whether a raw configuration is set, never their content (Block 21, D5). The README's list of dependencies logging on their own (Block 21, D6) adds rustls-platform-verifier: each failed verification at ERROR through `log`, with no secret.
- **D6 Version and changelog.** 0.14.0: verification on macOS, iOS and Windows changes, and rustls 0.23 enters the public API. `Unreleased` entries: "Added" `root_certificates`, `tls_config`, `ferrifuge::rustls`; "Changed" the operating system's verifier on macOS, iOS and Windows, where `SSL_CERT_FILE` and `SSL_CERT_DIR` are no longer read. The README states that moving to another rustls 0.x line is a breaking release of Ferrifuge, and where `SSL_CERT_FILE` still applies.

Value clause: As an app developer whose clients run behind corporate TLS inspection or on a private PKI, I want the client to trust what the operating system trusts and the certificate authorities I give it, and to supply my own TLS configuration when I need more, so that it connects without my turning certificate verification off.

**Structural acceptance:** the verifier is chosen in one function; `rustls-platform-verifier` is a direct dependency at reqwest's version, named only there (`cargo tree -d` shows one copy); a unit test per build reports the verifier kind: platform on macOS and Windows, webpki on Linux and in the fips build, with `fips() == true` there; a unit test runs the chosen verifier on a chain from the second test CA: accepted with that CA as an extra root, refused as an unknown issuer without; a unit test checks that the copies of a raw configuration the transports use offer ALPN `http/1.1` only; in the fips build, a unit test checks that a raw configuration with `require_ems` false fails `build()`; the `test` job runs these on Linux, `providers` in the aws-lc-rs and fips builds, `portability` on macOS and Windows.

`VERIFY:` that the second test CA's chain (serverAuth EKU, a SAN, validity under 398 days) passes Apple's and Windows' checks on the runners; that Centrifugo offers `h2` over TLS, so the raw configuration's ALPN in the scenario matters.

Verified (Centrifugo v6.10.1, 2026-11-07, by removing the ALPN rewrite): a raw configuration offering `h2` and `http/1.1` makes the server choose `h2`, and the websocket upgrade then fails ("A raw rustls configuration is used as given", websocket); the http_stream example still connects. On the macOS and Windows runners (2026-11-07, the `portability` jobs of the implementation PR), the platform verifier, chosen there, accepted a chain from a short-lived rcgen CA (serverAuth EKU, a SAN, 31 days) given as an extra root and refused it as an unknown issuer without (`adapter::tls` unit tests).

```gherkin
Feature: TLS verification

  @A
  Scenario: A private certificate authority given as an extra root is trusted
    Given a server serving TLS with a certificate from a private CA the platform does not trust
    And a client configured with the server's wss endpoint and that CA as an extra root certificate
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @A
  Scenario: A server whose CA is not trusted is refused
    Given a server serving TLS with a certificate from a private CA the platform does not trust
    And a client configured with the server's wss endpoint and no extra root certificate
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted
    And the client state is "connecting"

  @A
  Scenario Outline: Extra roots also apply to HTTPS transports
    Given a server serving TLS with a certificate from a private CA the platform does not trust, with http_stream and sse enabled
    And a client configured with an unreachable websocket endpoint, only the server's https <transport> endpoint, and that CA as an extra root certificate
    When connect is called
    Then the client emits "connected" over transport "<transport>"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @A
  Scenario Outline: A raw rustls configuration is used as given
    Given a server serving TLS with a certificate from a private CA the platform does not trust, with http_stream and sse enabled
    And a client configured with only the server's <transport> endpoint and a raw rustls configuration trusting only that CA, offering ALPN "h2" and "http/1.1"
    When connect is called
    Then the client emits "connected" over transport "<transport>"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |

  @A
  Scenario: A raw rustls configuration replaces the platform's trust
    Given a server serving TLS with a certificate from the test CA
    And a client configured with the server's wss endpoint and a raw rustls configuration trusting only a private CA the platform does not trust
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted
    And the client state is "connecting"

  Scenario: Extra roots and a raw configuration together are rejected
    Given client options with an extra root certificate and a raw rustls configuration
    When the client is built
    Then building fails with a configuration error naming both

  Scenario: An extra root that does not parse is rejected
    Given client options with a valid extra root certificate followed by one whose bytes are not a certificate
    When the client is built
    Then building fails with a configuration error naming extra root certificate 1
```

The private CA is a second test CA, never in `SSL_CERT_FILE`, so the platform does not trust it on the Linux runners; the second scenario guards the first three by showing that, the fifth shows that a raw configuration does not fall back to Ferrifuge's settings. "Not trusted" in a Then step means the error names rustls's invalid peer certificate. The scenarios' raw configurations are built with the build's provider, so they pass the fips check.

**Exceptions introduced:** the trust decisions of macOS and Windows (distrust, revocation, intermediate fetching) are the operating system's and are not scenarios: the @A scenarios run on Linux only (Docker), where the verifier is webpki; on macOS and Windows the unit tests of the structural acceptance check that the platform verifier is chosen, trusts an extra root and refuses an unknown issuer. The fips rejection of a non-FIPS raw configuration is a unit test of the fips build.

---

## Block 23 — Proxies

**Goal:** a client reaches Centrifugo through the HTTP, HTTPS or SOCKS5 proxy its network requires, found where the shell and the operating system configure it or given by the application, with one decision for every transport, so that a fleet on corporate networks connects without per-device code (parity item 1; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art).

**Dependencies:** Blocks 8 (transports, fallback, the emulation endpoint), 17 (`ClientBuilder`, setters applying from the next attempt, the headers the transport sets), 20 (changelog), 21 (logging, redaction of endpoints), 22 (per-client TLS settings and HTTPS clients).

**Out of scope:**
- PAC files and WPAD; NTLM, Negotiate and Kerberos proxy authentication: documented, with the local-helper workaround (Px, Cntlm), as the official SDK does (centrifuge-rust `docs/spec.md` §5.2, 88d6edf);
- SOCKS4 and SOCKS4a (the official SDK has neither);
- the settings hyper-util 0.1.21 does not read: macOS's exceptions list and SOCKS proxy, Linux desktop settings, iOS and Android system settings (there the environment applies);
- reading the settings again during the process (network changes, Block 25);
- the handshake timeout (Block 29), which will count the proxy handshake.

This block reverses Block 14's exclusion of HTTP proxy (CONNECT) support; Block 0's "Reversed exclusions" gains the row | Block 14, out of scope | HTTP proxy (CONNECT) support | owner, 2026-11-06 (item 1) | 23 |. Block 0's "HTTP client" row gains reqwest's `socks` feature. It fixes the analysis's finding F2: websocket dials directly while the HTTP client of http_stream and sse applies the environment's proxy, so behind such a proxy websocket fails and the client falls back by accident.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Setting.** `ferrifuge::Proxy`, opaque and `Clone`, set with `ClientBuilder::proxy`:
  - `Proxy::system()`, the `Default`: the environment, then the operating system's settings, as hyper-util 0.1.21 `Matcher::from_system` reads them: `ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY`, `NO_PROXY`, upper case before lower case, none of them when `REQUEST_METHOD` is set (a CGI process); then macOS `SCDynamicStore` HTTP and HTTPS proxies, and Windows `Internet Settings` `ProxyEnable`, `ProxyServer`, `ProxyOverride` (plan: reqwest 0.13.5 `system-proxy` default, aws-sdk-rust since behaviour version 2025-08-07, ureq 3);
  - `Proxy::from_env()`: the environment only (`Matcher::from_env`);
  - `Proxy::none()`: direct;
  - `Proxy::all(target)`: one proxy for every endpoint;
  - `Proxy::custom(f)`, `f: Fn(&str) -> Option<ProxyTarget> + Send + Sync + 'static`: given each endpoint as `scheme://host:port` (`ws`, `wss`, `http` or `https`, the port always written), it returns the endpoint's proxy, or `None` for direct; it is called once per endpoint per connection attempt (the official SDK's `ProxyConfig::custom`, reqwest's `Proxy::custom`);
  - `no_proxy(list)`, for `all` and `custom`: a host in the list, in `NO_PROXY` syntax (a domain and its subdomains, an IP address, a CIDR range, `*`), is reached directly, without calling `f`; `system` and `from_env` ignore it and take `NO_PROXY` and the system's exceptions.
  - `ProxyTarget::new(url)` takes `http://`, `https://` (TLS to the proxy), `socks5://` (DNS by the client) or `socks5h://` (DNS by the proxy), with optional `user:password@`, percent-encoded; `basic_auth(user, password)` sets them unencoded: Basic for an HTTP proxy, username and password for SOCKS5. A URL that does not parse, or of another scheme, fails at once with a configuration error naming the scheme, never the credentials; `build()` and `set_proxy` then cannot fail on a proxy.
  - `Client::set_proxy(proxy)` replaces the setting from the next connection attempt, as Block 17's setters; the current connection is kept. The setting stays in the adapter; the sans-IO core does not change.
- **D2 One decision.** One function decides, for each URL a transport opens (websocket upgrade, http_stream or sse stream request, emulation request), direct or which proxy, from the setting and the URL; `ws` and `wss` are asked as `http` and `https`, the only schemes hyper-util's matcher knows. `system` and `from_env` are read once per process, at the first attempt that needs them, and shared by every client, as reqwest reads them when a client is built. Both transports use that decision, so a proxy from the environment or the system now applies to websocket too (F2). A proxy found in the environment or the system with a scheme `ProxyTarget` refuses (`socks4`, `socks4a`) fails the attempt on every transport, with an error naming the scheme, and never connects directly.
- **D3 Websocket executor.** Ferrifuge opens the stream and tokio-tungstenite runs over it (`client_async_tls_with_config`):
  - direct TCP; or hyper-util's `Tunnel` (HTTP CONNECT, with `Proxy-Authorization: Basic` from the credentials), over TCP or, for an `https://` proxy, over TLS to the proxy through hyper-rustls's `HttpsConnector`, with the client's TLS settings (Block 22) and no ALPN, as reqwest does; or hyper-util's `SocksV5`, with username and password when given;
  - then TLS to Centrifugo for `wss://`, verifying Centrifugo's certificate, then the websocket handshake.
  - Plain `ws://` also goes through CONNECT (RFC 6455 §4.1; the official SDK). The destination given to `Tunnel` and `SocksV5` always carries its port, 80 for `ws` and 443 for `wss`, since both default to 443. For `socks5://` Ferrifuge resolves the host itself, without blocking, and gives `SocksV5` the address, as reqwest does: `SocksV5::local_dns` resolves with a blocking call. `TCP_NODELAY` stays set.
  - No handshake is written in Ferrifuge (Block 0, "Components"; plan).
- **D4 HTTP executor.** The HTTP clients of http_stream and sse are built with `no_proxy()`, so reqwest reads nothing itself, then `Proxy::custom` calling D2's function; reqwest's `socks` feature carries SOCKS5. reqwest 0.13.5 runs CONNECT and SOCKS5 with the same hyper-util `Tunnel` and `SocksV5` (`connect.rs:811`, `:856`, `:1957`), and TLS to an `https://` proxy with the client's TLS settings. It tunnels `https://` requests with CONNECT. It sends plain `http://` requests to an HTTP proxy in absolute form with `Proxy-Authorization`, as curl does, and unlike the official SDK, so a proxy allowing CONNECT only to TLS ports (Squid's default) still carries them. The README says that such a proxy sees these requests in full, including the sse connect command in the URL, and recommends `https://`.
- **D5 Failures.** A failed proxy handshake is a failed transport attempt, as for an unreachable endpoint (Block 8: error event, fallback, backoff). The error says "proxy authentication required" for hyper-util's `TunnelError::ProxyAuthRequired` (a CONNECT answered 407) and for a 407 answering a forwarded request. Another failed CONNECT reports hyper-util's message ("tunnel error: unsuccessful"), not the status, which `TunnelError` does not carry (plan). The error messages of the HTTP transports include their sources' messages, since reqwest's own stops at "error sending request". No error message carries the proxy's userinfo or credentials.
- **D6 Sharing.** A client with the default setting and the platform's trust keeps the process-wide HTTP clients. Any other client builds its own (Block 22, D4) at the first attempt needing one, and again at the first attempt after `set_proxy`. The Block 14 @P gates pass unchanged.
- **D7 Logging and redaction.** The DEBUG record of each transport attempt (Block 21) gains a field `proxy`: the proxy as scheme, host and port, `direct`, or `connector` for a client given one (Block 37 D4). The `Debug` of `Proxy`, `ProxyTarget` and `ClientBuilder` shows the kind, the proxy as scheme, host and port, and whether credentials are set, with `custom` as `Custom(..)`; it never shows userinfo or credentials. `http_headers` and its checks refuse `Proxy-Authorization`, now one of the headers the transport sets (Block 17): proxy credentials go through `ProxyTarget` only, so they never travel through a tunnel to Centrifugo.
- **D8 Version and changelog.** 0.15.0: websocket now follows the environment, every transport follows the macOS and Windows settings, error messages change, and `http_headers` refuses a header it used to take. `Unreleased` entries:
  - "Added": `Proxy`, `ProxyTarget`, `ClientBuilder::proxy`, `Client::set_proxy`, HTTP, HTTPS and SOCKS5 proxies;
  - "Changed": the environment's and the system's proxies on every transport, HTTP transport errors naming their cause, `Proxy-Authorization` refused in `http_headers`;
  - "Fixed": websocket ignoring the environment's proxy (F2).

Value clause: As an app developer whose clients run on corporate networks, I want each client to reach Centrifugo through the proxy the network requires, found where the system and the shell configure it or given by my code, so that it connects over every transport without per-device code of mine.

**Structural acceptance:**
- the proxy decision is one function used by both executors; no proxy handshake is written in `src/`;
- hyper-util is a direct dependency at reqwest's version, with `client-legacy`, `client-proxy`, `client-proxy-system` and `tokio`; hyper-rustls is one at reqwest's version, without a provider of its own; reqwest gains `socks`; `cargo tree -d` shows one copy of each;
- a unit test sends credentials holding reserved characters, from a proxy URL in the environment's syntax, through the decision, and gets the same credentials for both executors;
- `tests/proxy_env.rs` runs in its own binary, since environment variables are process-wide:
  - with `ALL_PROXY` set, a websocket client connects to the fake server through the proxy, and an http_stream attempt to `http://centrifugo.invalid` reaches the proxy in absolute form;
  - a host in `NO_PROXY` is reached directly;
  - `Proxy::none()` ignores both;
  - with `ALL_PROXY` set to a `socks4://` URL, websocket and http_stream attempts fail, naming the scheme;
- a unit test reads the operating system's settings, and the `portability` jobs run it on macOS and Windows (none set on the runners: direct).

```gherkin
Feature: Proxies

  @D
  Scenario Outline: A websocket goes through a proxy with credentials
    Given a <proxy> proxy requiring credentials in front of the fake server
    And a client configured with a websocket endpoint only that proxy can reach, and that proxy with its credentials, its log records captured
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the proxy tunnelled the client's connection to that endpoint's host and port, with the credentials
    And a DEBUG record in the client's span names the attempt's proxy by scheme, host and port
    And no record contains the proxy's password, as text or in its Basic encoding

    Examples:
      | proxy   |
      | http    |
      | https   |
      | socks5  |
      | socks5h |

  @A
  Scenario Outline: Secure endpoints go through a proxy, with TLS to Centrifugo
    Given a server serving TLS with a certificate from the test CA, with http_stream and sse enabled
    And a <proxy> proxy requiring credentials in front of it
    And a client configured with only a <transport> endpoint only that proxy can reach, and that proxy with its credentials
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the proxy tunnelled every connection of the client to that endpoint's host and port, with the credentials

    Examples:
      | transport   | proxy   |
      | websocket   | http    |
      | http_stream | http    |
      | sse         | http    |
      | http_stream | https   |
      | http_stream | socks5h |

  @A
  Scenario Outline: Plain HTTP transports go through an HTTP proxy in absolute form
    Given a server with http_stream and sse enabled
    And an HTTP proxy requiring credentials in front of it
    And a client configured with only an http <transport> endpoint only that proxy can reach, and that proxy with its credentials
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the proxy forwarded every request of the client to the server, with the credentials

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  Scenario Outline: A proxy refusing the credentials fails the attempt
    Given an HTTP proxy answering every request with 407
    And a client configured with only the <transport> endpoint "<endpoint>" and that proxy with the password "pw-3e9a", its log records captured
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying proxy authentication is required
    And the client state is "connecting"
    And no record or error contains "pw-3e9a", as text or in its Basic encoding

    Examples:
      | transport   | endpoint                                             |
      | websocket   | ws://centrifugo.invalid:8000/connection/websocket    |
      | http_stream | http://centrifugo.invalid:8000/connection/http_stream |
      | http_stream | https://centrifugo.invalid:8443/connection/http_stream |

  @D
  Scenario: An endpoint in the no-proxy list is reached directly
    Given an HTTP proxy in front of the fake server
    And a client configured with the fake server's websocket endpoint and that proxy, the fake server's host in its no-proxy list
    When connect is called
    Then the client emits "connected"
    And the proxy carried nothing

  @D
  Scenario: A proxy function decides per endpoint
    Given an HTTP proxy in front of the fake server
    And a client configured with a websocket endpoint only that proxy can reach, and a proxy function choosing that proxy for it
    When connect is called
    Then the client emits "connected"
    And the proxy function was asked once, for that endpoint's scheme, host and port
    And the proxy tunnelled the client's connection to that endpoint's host and port

  @D
  Scenario: A proxy set at run time applies from the next attempt
    Given an HTTP proxy in front of the fake server
    And a client connected directly to the fake server, with no proxy
    When set_proxy is called with that proxy
    And the fake server disconnects the client with a reconnect-advised code
    Then the client emits "connected"
    And the proxy tunnelled exactly one connection, the client's new one, to the fake server

  Scenario Outline: A proxy URL of another scheme is refused
    When a proxy target is made from "<url>"
    Then it fails with a configuration error naming the scheme "<scheme>"
    And the error does not contain "pw-1d5b"

    Examples:
      | url                           | scheme |
      | ftp://u:pw-1d5b@proxy:21      | ftp    |
      | socks4://u:pw-1d5b@proxy:1080 | socks4 |
```

"An endpoint only that proxy can reach" has the host `centrifugo.invalid`, which never resolves (RFC 6761). For `socks5`, whose DNS is the client's, it is a port of 127.0.0.1 where nothing listens. The test proxies connect every tunnel or forwarded request to the server, whatever the destination, and record the destination asked for, how it was carried (tunnel or absolute form) and the credentials received. The test server's certificate gains the name `centrifugo.invalid`. The test doubles in `tests/support`:
- an HTTP proxy: CONNECT, absolute-form forwarding, a 407 mode, and a TLS listener with a certificate from the test CA for the `https` rows, the test CA given to the client as an extra root;
- a SOCKS5 server with username and password.

"Saying proxy authentication is required" means the error message contains "proxy authentication required" (D5).

**Exceptions introduced:** `tests/proxy_env.rs` runs outside cucumber, since environment variables are process-wide. The tests read the operating system's proxy settings but do not change them: the `portability` jobs check only that reading them works with none set.

---

## Block 24 — Android and iOS builds, mobile lifecycle

**Goal:** CI proves Ferrifuge builds for Android and iOS, an Android app verifies servers with the operating system's trust after one setup call, and a mobile app knows how to keep its streams across backgrounding, so that fleets of phones run the same client as desktops (parity item 3; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A, emulator CI deferred).

**Dependencies:** Blocks 6 (providers, the one-place TLS build), 8 (failed attempts), 11 (recovery), 15 (delivery), 16 (CI layout), 20 (MSRV, docs.rs, examples, changelog), 22 (the platform verifier, Android left to this block), 23 (proxies: on mobile the environment applies).

**Out of scope:**
- emulator and simulator CI running a client against Centrifugo (the analysis's design C, deferred);
- the fips build on Android and iOS (AWS-LC-FIPS's supported operating environments): not built in CI; the README says it is unsupported there;
- extra roots on Android (D3);
- sleep, network changes and captive portals (Block 25, item 4), which the lifecycle guide only points to;
- Android and iOS proxy settings (Block 23: the environment applies; hyper-util 0.1.21 reads system settings on macOS and Windows only);
- other Android ABIs, the iOS simulator and Mac Catalyst: the same code paths as the two targets built;
- packaging for app stores, Gradle or Xcode project templates.

This block amends Block 22: D1's platform verifier also covers Android, and D2's extra roots apply everywhere but Android (D3). Today Android verifies with webpki over `rustls-native-certs`, which finds no system certificate there (openssl-probe 0.2.1 probes `/etc/ssl/certs` and a Termux path, not `/system/etc/security/cacerts`), so a `wss://` client on Android works only with extra roots or `SSL_CERT_FILE`.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 CI builds.** The `portability` set, on pull requests, gains two jobs. Each runs `cargo clippy --lib --tests -- -D warnings` and `cargo test --lib --no-run` with the default features (ring) and with `--no-default-features --features tokio,aws-lc-rs`. Nothing runs on a device (out of scope above).
  - `android` on `ubuntu-latest`: target `aarch64-linux-android` through cargo-ndk at a pinned version, API level 22 (`--platform 22`), which takes the compiler, archiver and linker from the runner's NDK (`ANDROID_NDK_HOME`, NDK 27.3 on the 2026-11 image), as rustls-platform-verifier's own CI does. API 22 is the floor of the stack: the verifier's Kotlin component has `minSdk 22` (v/0.7.1).
  - `ios` on `macos-latest`: target `aarch64-apple-ios`, with Xcode's SDK.
  - The examples are not built for mobile: they are command-line programs, and Block 20 D8's "every OS" means the host systems.
  - `VERIFY:` that the dev-dependencies build for both targets; if one does not, that step becomes `cargo build --lib`.
- **D2 Android verifier.** In the ring and aws-lc-rs builds on Android, a client trusting the platform alone verifies with `rustls-platform-verifier` 0.7.1, given the build's provider, as on macOS, iOS and Windows (Block 22, D1). The app's Gradle build includes the verifier's Kotlin component, its version identical to `rustls-platform-verifier-android` in `Cargo.lock` and kept by the ProGuard rule of the verifier's README, and the app initialises it once per process (D4).
- **D3 Extra roots on Android.** The platform verifier there takes no extra roots (0.7.1 has no `new_with_extra_roots` on Android). `build()` fails with a configuration error saying so and naming `tls_config`, as reqwest 0.13.5 (`async_impl/client.rs:776-781`) and the official SDK (centrifuge-rust `docs/spec.md` §5.2, 88d6edf) do. Dropping them would break Block 22 D2's "in addition to the platform's"; a webpki fallback would trust the extra roots alone. An app needing them gives its own `tls_config`.
- **D4 Initialisation.** `ferrifuge::android`, compiled on Android only, wraps the verifier's `init_with_env`, `init_with_refs` and `init_with_runtime`, and re-exports `jni` (0.22, a direct Android-only dependency at the verifier's version) and the verifier's `Runtime` trait, so that the setup reaches the copy of the verifier Ferrifuge links. The module is exempt from semver, and its documentation says so, because those types are pre-1.0 (the official SDK). It needs no unsafe code. The verifier exposes no "initialised" query (0.7.1 `src/android.rs`: `GLOBAL` is private), so Ferrifuge records, process-wide, that one of its wrappers succeeded. The verifier keeps the first setup and ignores later ones, so an app that set the verifier up another way still calls a wrapper once.
- **D5 Without the initialisation.** Until a wrapper succeeded:
  - `build()` fails with a configuration error naming `ferrifuge::android::init_with_env` when a connection known at build uses the platform verifier: a `wss://` or `https://` endpoint, an `https://` emulation endpoint with an HTTP transport, or `Proxy::all` with an `https://` target. A raw `tls_config` is the application's and is not checked.
  - Every other TLS use (a proxy from the environment, a proxy function, `set_proxy`) reaches Ferrifuge's verifier, which wraps the platform one and fails the handshake with the same message before delegating: a failed transport attempt (Block 8), so a setup made after `build()` applies from the next attempt. The platform verifier would otherwise panic at its first check (`src/android.rs:87-91`), ending the client's task without an event.
  - Ferrifuge's verifier also turns a panic of the platform verifier into a failed handshake, as Ferrifuge guards its callbacks: a Kotlin component of another version, or one ProGuard stripped, panics there, where panics unwind. A missing Kotlin component is already an error ("failed to call native verifier").
- **D6 Lifecycle guide.** A README "Mobile" section:
  - disconnect when the app goes to the background and connect when it returns, or keep running only where the OS allows it;
  - recovery (Block 11) and delivery (Block 15) make a background gap lossless on recoverable channels within the server's history size and TTL;
  - a suspended device's monotonic clock stops, so the no-ping deadline and token refresh run late after a resume until Block 25; disconnecting in the background avoids it, and a connect answered with error 109 gets a new token from the token callback;
  - the Android setup: the Gradle component, the ProGuard rule, one call to `ferrifuge::android::init_with_env` before any client is built, extra roots through `tls_config`, revocation checks from API 24;
  - iOS needs no setup: its platform verifier is Security.framework, extra roots included;
  - on both, only the environment's proxy applies, and the fips build is unsupported.
  The TLS verification table gains Android, and the "Continuous integration" section names the mobile jobs. docs.rs stays on Linux (Block 20, D7): the module is documented in the README and by `cargo doc --target aarch64-linux-android`.
- **D7 Version and changelog.** 0.16.0: verification on Android changes, and extra roots there now fail `build()`. `Unreleased` entries: "Added" `ferrifuge::android` (Android only, exempt from semver), the Android and iOS CI builds; "Changed" the operating system's verifier on Android, its setup required, extra roots refused there; "Fixed" Android finding no system certificate.

Value clause: As an app developer shipping Ferrifuge in Android and iOS apps, I want CI to prove the client builds for both, the Android build to verify servers with the device's trust after one setup call, and a guide to backgrounding, so that my phones verify servers as the device does, a missing setup is reported instead of stopping the client silently, and recoverable channels lose nothing across a background gap within the server's history.

**Structural acceptance:**
- the `android` and `ios` jobs pass on pull requests;
- the verifier is still chosen in one function; the `VERIFIER` unit test gains the Android row (platform, but webpki in the fips build), compiled by the `android` job;
- `jni` is a direct dependency on Android only, at the verifier's version; `cargo tree -d --target aarch64-linux-android` shows one copy;
- host unit tests, the gate being functions of the options and an "initialised" flag:
  - without the setup, `build()` refuses a `wss://` endpoint, an `https://` stream endpoint, an `https://` emulation endpoint and `Proxy::all` with an `https://` target, naming the call, and accepts a `ws://` endpoint and a raw configuration;
  - on Android, extra roots fail `build()`, naming `tls_config`;
  - Ferrifuge's verifier, around a stub verifier, fails the handshake naming the call without the setup, delegates with it, and turns the stub's panic into an error.

**Exceptions introduced:** no Gherkin scenario: the behaviour exists only on Android devices, and nothing runs on a device or emulator in CI (design C deferred). The host unit tests above cover the gate; the mobile jobs only build.

---

## Block 25 — System sleep, network changes, captive portals

**Goal:** a client waking from a system sleep sends no token that expired while it slept, keeps a connection whose pings the sleep only delayed, and drops one the server gave up on at its first timer or I/O after the wake; an app told by its platform that the device woke or changed network reconnects the client at once with `reconnect()`; a captive portal answering an attempt is named in its error; so that laptops and phones resume their streams promptly and recover what they missed (parity item 4; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A, with the reconnect jitter kept).

**Dependencies:** Blocks 2 (backoff), 4 (no-ping deadline), 5b and 10 (token components, refresh timers), 8 (transports, fallback, failed opens), 11 (recovery), 13 (calls), 14 (the jitter window, the idle-client gate), 15 (withdrawal, the paused no-ping deadline), 16 (chaos runners), 17 (`Input` non-exhaustive, `Action` exhaustive), 21 (logging), 23 (proxy settings), 24 (the mobile guide).

**Out of scope:**
- built-in OS sleep and network watchers (the analysis's design B, not adopted): apps get these notifications from their platform and call `reconnect()` (D9);
- a periodic wake to notice a sleep sooner (D1);
- a captive portal behind a websocket that opened (a lost transport, Block 21 D4) or one that breaks TLS (a certificate error);
- emulation requests answered by a portal: Block 8's handling stays;
- a real system sleep in CI.

This block reverses Block 14's exclusion of sleep/resume behaviour; Block 0's "Reversed exclusions" gains the row | Block 14, out of scope | sleep/resume behaviour | owner, 2026-11-06 (item 4) | 25 |. It amends Block 8 (D6, D8), Block 14 (D5: the jitter window), Block 21 (D10: the field `gap`), Block 23 (D7) and Block 24 (D9).

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Detection, in the adapter.** Before each input it gives the core (a timer, a transport event, a callback answer, an application call), the adapter compares how far the wall clock (`SystemTime`) and the monotonic clock (`Instant`) moved since the previous input. When the wall clock moved further by at least 2 s, it first gives the core `Input::Slept { gap }`, `gap` being the difference.
  - `Instant` is `CLOCK_MONOTONIC` on Linux and Android and `CLOCK_UPTIME_RAW` on macOS and iOS; neither counts a suspend (std `Instant` documentation; clock_gettime(2)), so tokio's timers resume with the time they had left. The official SDK compares the same clocks (centrifuge-rust `docs/spec.md` §9.7, 88d6edf).
  - No periodic wake: a sleep is seen at the first input after the wake, at the latest the no-ping deadline, as in the official SDK (§13). A periodic check would break Block 14's "An idle client wakes only for pings and refreshes". An app wanting an immediate reconnect calls `reconnect()`.
  - No `CLOCK_BOOTTIME`: std does not expose it, `libc`'s call is unsafe (Block 0 lints), and `rustix` would be a new dependency.
  - A wall clock set forward by 2 s or more counts as a sleep, with D2 and D3's safe consequences; one set backward is ignored.
  - The core ignores a gap under 2 s, so another owner may report every gap it measures.
  - The check reads two clocks and allocates nothing; the Block 14 @P gates pass unchanged.
  - When the input that revealed the sleep is the firing of a timer that the core's answer to `Slept` re-arms (`Action::SetTimer` or `Action::SetSubscriptionTimer` for that timer), the adapter drops that firing: it belongs to the deadline the core just replaced, so a sleep first seen at the no-ping deadline does not reconnect when D2 keeps the transport. Otherwise it delivers that input after `Slept`, so a refresh whose firing revealed the sleep still runs although `ShortenTimer` finds that timer no longer running (Codex, #119).
  - The same holds when another task handled the core between the driver taking a firing and giving it: the core and the driver number each setting of a timer alike, the driver performing the actions in order, and a firing whose timer was set again or cancelled since its setting is dropped (Codex, #120).
  - Between `Slept` and that input, the adapter gives the core the firing of every token refresh timer the sleep made due, and stops its own timer for it, so that an attempt or subscribe the input starts fetches a new token first (D3). It keeps each refresh timer's deadline beside the core, under the same lock, since an application call that reveals a sleep cannot reach the driver's timers. Every input still takes effect before its call returns (Codex, #120).
  - While the client is connected, an event taken from a stream (`Input::EventTaken`), a frame read and the transport's close (`Input::Frame`, `Input::TransportClosed`) go before `Slept`: the event is already the application's, so its position moves even when the sleep drops the connection and recovery does not deliver it again; what the server did before the wake stands, so a terminal close or disconnect push still ends the client. The refreshes the sleep made due go before them, also before the held messages that taking an event releases, so that a connect or subscribe they start fetches a new token, and one they tear down makes the next attempt fetch one (D3). While the client is connecting, `Slept` changes only the refresh timers and goes first, so that a connect reply is not judged by a sleep it did not live through (Codex, #120). A `reconnect()` call (`Input::Reconnect`) while connected also goes before `Slept`, so that it keeps its code 6 and the jitter window rather than finding the client already reconnecting for the sleep (Codex, #122).
  - A timer that an input given before `Slept` sets (a refresh, or its retry, set by a reply) lost no time to the sleep: the sleep does not shorten it (Codex, #120).
  - Whatever input reveals a sleep, the adapter wakes its driver for the actions the sleep queued: creating an event stream feeds the core too, and has no notification of its own (Codex, #120).
  - `VERIFY:` whether `Instant` (QueryPerformanceCounter) counts a suspend on Windows. If it does, no gap is seen there and the timers fire at the wake as if they counted it; the README says which.
  - Verified (2026-11-07, while implementing 25a): it does. A Rust maintainer cites Microsoft that `QueryPerformanceCounter` makes the adjustments for sleep (rust-lang/rust#87907), and Rust 1.77's `Instant` on Windows was observed counting a suspend (Rippling, "Suspend-unaware cross-platform monotonic time in Rust", 2024); Microsoft names `QueryUnbiasedInterruptTime` as the count that excludes sleep. The README says so.
- **D2 The connection, in the core.** On `Slept { gap }` while connected:
  - when `gap` is at least the no-ping window (Block 4: the ping interval plus `max_server_ping_delay`, neither 0), the core closes the transport and reconnects with code 2 "no ping", as when the no-ping deadline fires: Block 2's delay and Block 14's jitter window apply, so a fleet waking on a schedule does not reconnect at once (diverges on purpose from the official SDK's immediate reconnect, §9.7);
  - when `gap` is shorter, the no-ping deadline restarts in full from the wake, unless a full read-ahead pauses it (Block 15), so pings queued during the sleep cause no reconnect;
  - without a no-ping window, the transport is kept.
  While connecting, the attempt in flight keeps its connect timeout and the backoff its delay; only D3 applies.
- **D3 Refresh timers.** On `Slept { gap }`, the core shortens by `gap` every token refresh timer running, the connection's and each subscription's, with two new actions, `Action::ShortenTimer(Timer, Duration)` and `Action::ShortenSubscriptionTimer(SubscriptionId, SubscriptionTimer, Duration)`: the owner makes the timer fire that much earlier, at once if that time has passed, and ignores a timer not running. The core holds no clock (Block 0); only the owner knows how long each timer has left.
  - A refresh that came due during the sleep starts at the wake.
  - Every reconnect, whether a sleep caused it (D2) or not, keeps the refresh timers running until the next connect or subscribe reply re-arms or cancels them, so a transport lost just before a sleep still has its refresh timer for D3 to shorten (Codex, #119); `disconnect()`, `unsubscribe()` and a terminal condition still cancel them (amends Blocks 5b and 10, whose teardown cancelled them on every reconnect). One that fires while the client is connecting makes the next attempt fetch a new connection token first; one that fires while its subscription is subscribing makes its next subscribe fetch a new token first, also when the subscribe was waiting for its data callback, which then runs again for the new token, and likewise for the connect data callback, and for a websocket that was opening: once open, it waits for the new token to send its connect command, within the connect timeout, and a failed fetch closes it and reconnects after the backoff (Codex, #120).
  - A token refresh that came due and had not completed, its token fetch still pending or its retry after a failure still waiting, when the connection is torn down for a reconnect, or the subscription for a resubscribe (a server unsubscribe push with a code from 2500), makes the next connect, or the next subscribe of that subscription, fetch a new token first when a token callback exists, as a refresh that fires while connecting or subscribing already does. A refresh whose token was fetched, its refresh or sub_refresh command still pending, leaves that token for the next connect or subscribe. The answer of the abandoned fetch is still not used (owner decision, 2026-11-04): a connection refresh's denial still disconnects; a subscription's abandoned fetch stays cancelled. `disconnect()` and `unsubscribe()` leave the next connect or subscribe unchanged (reviewer, 2026-11-07, delegated by the owner; Codex, #120). So a token that expired during the sleep is not sent, and no error 109 round trip is needed (the official SDK keeps expiry as wall time, §6.6).
- **D4 Timers a sleep leaves alone.** Reply deadlines (connect, refresh, subscribe, unsubscribe, `sub_refresh`), call timers, and the reconnect and resubscribe delays. The monotonic clock stopped during the sleep, so it used none of their time. Shortening the delays would make a fleet that slept together reconnect together.
- **D5 `Client::reconnect()`.** It mirrors `connect()`; in the core it is `Input::Reconnect`. The client goes to, or stays in, `connecting`, never `disconnected`:
  - connected: the transport closes and the client emits "connecting" with code 6 "reconnect called" (`codes::connecting::RECONNECT_CALLED`);
  - connecting: the attempt in flight, if any, closes, and no event is emitted (Block 15: one event per state change);
  - the next attempt has no backoff delay, and the attempt counter restarts, as after `connect()` (Block 2);
  - the jitter window applies when the client was connected (amends Block 14, which applies it to lost connections only), so a building-wide network change does not synchronise a fleet;
  - disconnected: nothing. `connect()` starts a stopped client, and a handler calling `reconnect()` on every client must not revive one stopped on purpose (precedent for the call: async-nats 0.52.0 `Client::force_reconnect`).
  Calls waiting for the connection keep waiting; calls sent on the closed transport fail with code 11 "connection closed" (Block 13). Subscriptions resubscribe as after any reconnect. Publications not taken are withdrawn (Block 15: a client-side reason) and recovered by the next connect (Block 11), so delivery stays exactly once on recoverable channels.
- **D6 Transport round.** `reconnect()` also starts the transport round again from the first transport (amends Block 8, which keeps a chosen transport for the client's life): a new network may allow websocket again. The official SDK plans the same on the app's network signal (§5.8, "Fallback reset"). A sleep does not restart the round.
- **D7 Proxy settings.** `reconnect()` makes the next attempt of every client read the environment's and the operating system's proxy settings again, and rebuild the HTTP clients built from them (amends Block 23 D2, which reads them once per process and left network changes to this block). An attempt keeps the settings and the process-wide HTTP clients it took first for all its requests, its stream and its emulation requests alike, whoever asks for the settings to be read again meanwhile (Codex, #122).
- **D8 Captive portals.** An open answered by a web page is a failed attempt whose error message ends with "; a captive portal may be answering", retried like any failed open (Block 8: error event, fallback, backoff):
  - websocket: the upgrade answered with a 3xx, a 511, or a `text/html` body instead of 101;
  - http_stream: the stream request answered with a 3xx (not followed), a 511, or a 2xx `text/html` body; the 2xx case is new, as today the page is read as frames and the client reconnects without an error event (Block 21 D4);
  - sse: the same, beside the existing `text/event-stream` check.
  Other statuses keep their messages. The media type's essence is compared case-insensitively, as the sse check does.
- **D9 Documentation.** A README section "Sleep and network changes" covers what D1–D4 do and when a sleep is noticed. It names `reconnect()` as the call for the platform's notifications (macOS `NSWorkspace.didWakeNotification`, Windows `WM_POWERBROADCAST`, systemd-logind `PrepareForSleep`, Android `ConnectivityManager.NetworkCallback`, iOS `NWPathMonitor`), and `disconnect()` then `connect()` as the calls when waiting calls should fail. In Block 24's "Mobile" section, the bullet on the stopped monotonic clock is replaced by a pointer to that section.
- **D10 Logging.** The adapter logs a detected sleep at DEBUG, "system sleep detected", with a field `gap` (added to Block 21 D3's fields). `reconnect()`'s state change and attempt are logged like any other. `Input::Slept` and the new actions show at TRACE through their `Debug`.
- **D11 Version and changelog.** 0.17.0: the two actions are breaking for owners of the machine (`Action` is exhaustive, Block 17), as `Action::Anomaly` was (Block 21 D9). `Unreleased` entries:
  - "Added": `Client::reconnect`, `Input::Reconnect`, `Input::Slept`, connecting code 6, sleep detection;
  - "Changed": `Action::ShortenTimer` and `Action::ShortenSubscriptionTimer`; captive-portal error messages; an http_stream open answered with `text/html` is now a failed attempt with an error event; `reconnect()` restarts the transport round and reads the proxy settings again;
  - "Fixed": a token that expired during a sleep is no longer sent.

**Sub-increments** (reviewer, 2026-11-07, delegated by the owner; one branch and PR each, in this order; 0.17.0 lands in 25a, and each PR writes its own changelog entries and README part):

| Id | Value clause | Scope | Scenarios |
|---|---|---|---|
| 25a | As an app developer whose clients run on laptops and phones that sleep, I want the client to handle a wake without sending a token that expired while it slept, keeping a connection the sleep only delayed and dropping one the server gave up on after the backoff and jitter, so that my users' streams resume without an expired-token round trip and without the fleet reconnecting at once | sleep: detection, `Input::Slept`, the shorten actions, refresh timers kept across reconnects, logging, 0.17.0 | "A sleep as long as the no-ping window reconnects after the backoff and jitter", "A shorter sleep keeps the connection and restarts the no-ping window", "Without pings a sleep keeps the connection", "A gap under 2 s changes nothing", "A refresh due during a short sleep starts at the wake", "A refresh not yet due moves earlier by the sleep", "A token due during a long sleep is fetched before the next connect", "A subscription token due during a short sleep is refreshed at the wake", "A sleep leaves reply deadlines alone", "A token due during a sleep in a reconnect delay is fetched before the next connect", "A sleep while connecting keeps the reconnect delay" |
| 25b | As an app developer receiving my platform's wake and network-change notifications, I want `reconnect()` to reconnect the client at once, from the first transport and with the current proxy settings, so that my users' streams resume promptly on the new network and recover what they missed, without reviving a client stopped on purpose or the fleet reconnecting together | `reconnect()`: code 6, jitter, transport round, proxy settings | "reconnect() while connected skips the backoff but keeps the jitter window", "reconnect() while connecting starts an attempt at once and keeps waiting calls", "reconnect() tries the first transport again", "reconnect() does nothing to a disconnected client", "reconnect() reconnects at once without \"disconnected\"", "Publications not taken before reconnect() are recovered once" |
| 25c | As an app developer whose users join networks behind captive portals, I want an attempt answered by a web page to name a captive portal in its error, so that I can tell users to sign in to the network instead of leaving them with silent retries | captive portals: open checks of the three transports, raw HTTP upgrade answers | "A captive portal answering the websocket upgrade is reported" |

Value clause: As an app developer whose clients run on laptops and phones that sleep and change networks, I want the client to handle a wake without sending an expired token, to reconnect at once when I call `reconnect()` on my platform's wake or network notification, and to name a captive portal in its error, so that my users' streams resume promptly and recover what they missed, without the fleet reconnecting at once.

**Structural acceptance:**
- a unit test of the adapter's comparison: a wall clock ahead by 2 s gives a gap; ahead by 1999 ms, or behind, gives none;
- an adapter unit test: a sleep revealed by the no-ping timer's firing, shorter than the no-ping window, drops that firing and keeps the transport;
- an adapter unit test: a no-ping firing the driver took before another task handled a short sleep is dropped, and one of the running setting is delivered;
- an adapter unit test: a sleep first seen at a refresh timer's firing still refreshes the token;
- an adapter unit test: a sleep first seen at the reconnect timer's firing, the transport lost before it, fetches a new token before the attempt opens a transport;
- an adapter unit test: a publication taken from its stream as a sleep longer than the no-ping window is noticed moves its position, so the next subscribe recovers from it;
- unit tests of the open checks: a websocket upgrade answered with 200 `text/html`, 302 or 511 names a captive portal, and one answered with 403 does not; http_stream and sse the same, against `src/adapter/http.rs`'s one-shot server;
- a unit test: after `reconnect()`, the delay after a failed attempt stays within Block 2's bound for attempt 0, drawn 1000 times;
- a unit test: after `reconnect()`, the next attempt reads the system proxy settings again;
- the fake server can answer websocket upgrades with a raw HTTP response, written on the TCP stream itself (tungstenite's server refuses a 2xx error response);
- `tests/chaos.rs` gains the operations `reconnect()` and a sleep of 0 to 120 s, and `tests/chaos_adapter.rs` gains `reconnect()`; I1–I11 hold;
- the Block 14 @P gates pass unchanged.

```gherkin
Feature: Sleep, network changes, captive portals

  @E
  Scenario: A sleep as long as the no-ping window reconnects after the backoff and jitter
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s and a reconnect jitter window of 10 s
    When it is told it slept for 35 s
    Then it closes the transport and emits "connecting" with code 2 and reason "no ping"
    And its next attempt is delayed by at least 500 ms and at most 11000 ms

  @E
  Scenario: A shorter sleep keeps the connection and restarts the no-ping window
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s
    And 10 s have elapsed since its last ping
    When it is told it slept for 34 s
    Then it keeps the transport
    And its no-ping deadline is 35 s from now

  @E
  Scenario: Without pings a sleep keeps the connection
    Given a connected machine whose server sends no pings
    When it is told it slept for 600 s
    Then it keeps the transport

  @E
  Scenario: A gap under 2 s changes nothing
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 1 s
    When it is told it slept for 1999 ms
    Then it returns no action

  @E
  Scenario: A refresh due during a short sleep starts at the wake
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 20 s
    When it is told it slept for 30 s
    Then it keeps the transport
    And it asks for a new connection token at once

  @E
  Scenario: A refresh not yet due moves earlier by the sleep
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 50 s
    When it is told it slept for 30 s
    Then its connection token refresh is due in 20 s

  @E
  Scenario: A token due during a long sleep is fetched before the next connect
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 50 s
    When it is told it slept for 60 s
    And its next attempt starts
    Then it asks for a new connection token before opening a transport

  @E
  Scenario: A subscription token due during a short sleep is refreshed at the wake
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, subscribed to "news" with a token whose refresh is due in 20 s
    When it is told it slept for 30 s
    Then it asks for a new token for "news" at once

  @E
  Scenario: A sleep leaves reply deadlines alone
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s and timeout 5 s, with an RPC call sent 2 s ago and not answered
    When it is told it slept for 30 s
    Then the call's reply deadline is 3 s from now

  @E
  Scenario: A token due during a sleep in a reconnect delay is fetched before the next connect
    Given a connected machine with minReconnectDelay 10000 ms whose connection token refresh is due in 50 s
    And its transport was lost a moment ago
    When it is told it slept for 60 s
    And its next attempt starts
    Then it asks for a new connection token before opening a transport

  @E
  Scenario: A sleep while connecting keeps the reconnect delay
    Given a machine with minReconnectDelay 10000 ms whose first connection attempt failed
    And 6 s have elapsed
    When it is told it slept for 60 s
    Then its reconnect timer is unchanged

  @E
  Scenario: reconnect() while connected skips the backoff but keeps the jitter window
    Given 100 connected machines with different seeds, minReconnectDelay 5000 ms and a reconnect jitter window of 1000 ms
    When reconnect is called on each
    Then each closes its transport and emits "connecting" with code 6 and reason "reconnect called"
    And every next attempt is delayed by at most 1000 ms
    And the delays are not all equal

  @E
  Scenario: reconnect() while connecting starts an attempt at once and keeps waiting calls
    Given a machine with minReconnectDelay 10000 ms whose first connection attempt failed, with an RPC call waiting for the connection
    When reconnect is called
    Then it starts an attempt at once
    And it emits no event
    And the call is still waiting

  @E
  Scenario: reconnect() tries the first transport again
    Given a machine with transports websocket and http_stream, connected over http_stream after websocket failed
    When reconnect is called
    Then its next attempt opens a websocket transport

  @E
  Scenario: reconnect() does nothing to a disconnected client
    Given a machine disconnected by an explicit disconnect call
    When reconnect is called
    Then it returns no action

  @D
  Scenario: reconnect() reconnects at once without "disconnected"
    Given a client with minReconnectDelay 5000 ms connected to the fake server
    When reconnect is called
    Then the client emits "connecting" with code 6 and reason "reconnect called"
    And then within 3 s the client emits "connected"
    And the client never emitted "disconnected"

  @D
  Scenario: Publications not taken before reconnect() are recovered once
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When reconnect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the subscription's stream yields, in order, "subscribing", "subscribed", and the publications at offsets 2 and 3
    And no publication is yielded twice

  @D
  Scenario Outline: A captive portal answering the websocket upgrade is reported
    Given a fake server answering every websocket upgrade with HTTP status <status> and an HTML page
    When connect is called
    Then the client emits an "error" event for the failed transport attempt whose message ends with "a captive portal may be answering"
    And the client state is "connecting"
    And the client makes another connection attempt

    Examples:
      | status |
      | 200    |
      | 302    |
      | 511    |
```

**Exceptions introduced:** detection (D1) is checked by unit tests of the adapter's comparison, not by a real system sleep in CI; the http_stream and sse portal cases are checked by unit tests, since the fake server speaks websocket only (Block 15).

---

## Block 26 — Codec seam for the emulation envelope (Block 5c drift)

**Goal:** the machine reaches every encoding of the client protocol through `Codec`, the emulation envelope and the split of a frame into its commands included, as Block 5c requires ("`machine.rs` calls no codec module directly"), so that Block 27 enables Protobuf over http_stream without editing the machine; observable behaviour does not change.

**Dependencies:** Blocks 5 (5c codec seam), 8 (bidirectional emulation), 13d (write-queue sizes of emulation requests), 18 (batch frames, the 413 resend).

**Out of scope:**
- enabling Protobuf over http_stream (Block 27, item 36): Block 8's build check stays, the adapter keeps sending `application/json`, and the Protobuf envelope is verified against Centrifugo there;
- the in-house decoders and the content-type check (Block 27);
- replacing `Codec::command_ids` or `Codec::split_commands` with a maintained parser: they read frames the client encoded itself, not network input (Block 0, "Components").

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Seam.** `Codec` gains `emulation_request(self, session: &str, node: &str, frame: &[u8]) -> Vec<u8>`, `emulation_frame(self, body: &[u8]) -> Option<Vec<u8>>` (`None` for a body that is not an envelope) and `split_commands(self, frame: &[u8]) -> Vec<&[u8]>` (each command's encoding in a frame `encode` made, each itself a one-command frame). JSON keeps today's envelope and its newline split. Protobuf, in `src/protobuf.rs`, encodes `protocol::EmulationRequest` (as centrifuge-js 63ed73e's `encodeEmulationRequest`, `transport_http_stream.ts` `send`) and splits on the length prefixes. The Protobuf arms cannot be reached through the public API until Block 27 (Block 8's build check), but they are correct and unit-tested; no arm panics.
- **D2 Call sites.** The four direct calls move to the seam, and the `use crate::json;` lines in `src/machine.rs` and `src/machine/calls.rs` go: in `src/machine.rs`, `send_rejected` (the 413 read-back), `resend_alone` (each command's emulation request, and its newline split, which becomes `split_commands`) and `frame_of`; in `src/machine/calls.rs`, the emulation request's size for the write queue (Block 13d). The test uses in `src/machine/batching.rs` and `src/machine/subscriptions.rs` call `Codec::Json.emulation_frame` instead.
- **D3 Check.** A unit test in `src/codec.rs` reads `src/machine.rs` and every `.rs` file under `src/machine/` (from `CARGO_MANIFEST_DIR`). It fails, naming the file and line, when a line holds `json::` or `protobuf::` not preceded by an identifier character, or is a `use` naming `json` or `protobuf` as a whole word. Test code is included.
- **D4 Version.** None: the codec is crate-private and nothing observable changes; no changelog entry.

Value clause: As a maintainer, I want the machine to reach the emulation envelope and the split of a frame into its commands through `Codec`, so that Protobuf over http_stream (Block 27) adds no codec knowledge to the machine.

**Structural acceptance:** no file under `src/machine*` names the `json` or `protobuf` module (D3's test) or splits a frame itself; every scenario and test passes unchanged; the `Codec` unit tests cover, in both encodings, the envelope's round trip (`emulation_frame` reads back what `emulation_request` made, and a body that is not an envelope gives `None`) and the split (each piece of a two-command frame is a one-command frame with its id); the JSON envelope's bytes stay those of `emulation_request_carries_the_frame_as_a_string`.

**Exceptions introduced:** no scenario, as for 5a: the codec is crate-private and its emulation envelope is not observable through the public API beyond what Blocks 8, 13d and 18 already exercise.

---

## Block 27 — Maintained stream decoders, Protobuf over http_stream

**Goal:** the bytes the server streams are split, and their content types read, by maintained components where one exists (Block 0, "Components"), and an app that uses Protobuf can fall back to http_stream, so that binary payloads reach clients behind networks that block websocket (the plan's "Components audit" and parity item 36; "Planned after Block 19", owner decision 2026-11-06).

**Dependencies:** Blocks 0 (Components), 7 (Protobuf, varint-delimited frames), 8 (transports, decoders buffering across chunks, the Protobuf + sse build check), 14 (fuzz targets), 19 (`MAX_MESSAGE_BYTES`), 25 (D8, the captive-portal check), 26 (the Codec seam).

**Out of scope:**
- Protobuf over sse: it stays refused at build (Centrifugo's sse is JSON only: centrifuge `handler_sse.go:208-209`; centrifuge-js 63ed73e `transport_sse.ts:35`);
- a maintained varint stream splitter: none exists (tokio-util's `LengthDelimitedCodec` takes fixed-width lengths), so the Protobuf splitter is written in-house (D3);
- a Protobuf 413 scenario: Block 26's unit tests cover the read-back;
- WHATWG's leniency for content-type parameters without a value;
- unidirectional transports (Block 0).

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft and a research report verified against the crates, centrifuge-js 63ed73e, centrifuge 7ac037196c95 and a live Centrifugo v6.10.1):
- **D1 sse.** The in-house sse decoder is replaced by sse-core 0.2.3 (`default-features = false`, `features = ["std"]`; MIT OR Apache-2.0; release and commit 2026-07-31; no unsafe in its source; no shipped crate beyond bytes, memchr and thiserror, already in the tree), `SseDecoder::with_limit(MAX_MESSAGE_BYTES)`: an event over the limit fails the transport as today (`TOO_LARGE`), the events before it in the same chunk delivered first. Chosen over finesse 2.4.3 (Apache-2.0 only, whose NOTICE adds an attribution duty, and 3 to 5 times slower) and sse-parse 0.1.0 (drops an oversized event and resyncs, which hides it; one release), by bolero fuzzing (chunk invariance, and a differential against the in-house decoder, 2.0 M runs clean) and benchmarks (about 7 times the in-house decoder's throughput). The limit bounds an event's data, name and id each (sse-core `SseDecoder::next`), so the decoder holds at most about three times `MAX_MESSAGE_BYTES`; comment and unknown-field lines are skipped unbuffered, so a long one no longer ends the transport. Data that is not UTF-8 is delivered with U+FFFD replacement characters, as EventSource decodes (WHATWG HTML, "UTF-8 decode"); Centrifugo's sse frames are JSON, hence UTF-8. The first error ends the transport; sse-core's resynchronisation is not used.
- **D2 http_stream, JSON.** The line splitter is tokio-util's `codec::AnyDelimiterCodec::new_with_max_length(b"\n", b"\n", MAX_MESSAGE_BYTES)` (tokio-util 0.7.20, already in the tree; the `codec` feature adds no crate), fed each body chunk; empty and whitespace-only lines are skipped and a trailing `\r` stripped, as today; an over-long line fails the transport (`TOO_LARGE`). The first `MaxChunkLengthExceeded` ends the transport; the codec's discarding is not used.
- **D3 http_stream, Protobuf (item 36).** Building no longer refuses Protobuf with http_stream (amends Block 8). The stream request is a POST whose body is the varint-delimited connect frame; emulation requests carry the Protobuf `EmulationRequest` (Block 26's `Codec::emulation_request`); both send `Content-Type` and `Accept` exactly `application/octet-stream`, with no parameter (centrifuge `handler_http_stream.go:73` and `emulation.go:72` compare the value exactly; Centrifugo v6.10.1 answers 400 to `application/octet-stream; charset=binary`, verified). Replies on the stream are uvarint length-delimited (centrifuge `handler_http_stream.go:163-168`, protocol `encode.go:353-372`; centrifuge-js `protobuf.codec.ts:9-91`). The splitter is in-house, on prost's `decode_length_delimiter`, with a bolero fuzz target in CI like the others (Block 14) and no unsafe. It yields each reply with its prefix, a one-reply frame for Block 7's decode; a zero length is an empty reply, the server's ping, delivered, never skipped. A prefix whose buffered bytes (fewer than 10) all have the continuation bit set waits; any other prefix prost refuses ends the transport (prost reports both as `InvalidVarint`); an announced length over `MAX_MESSAGE_BYTES` ends it at once (`TOO_LARGE`), as centrifuge-js `decodeReply`. Its limit is a parameter, fuzzed small.
- **D4 Content type.** `is_event_stream` and the captive-portal check (amends Block 25 D8) parse the value with mediatype 0.23.0 (MIT, no required dependency, `forbid(unsafe_code)`, released 2026-07-27) and compare type and subtype, case-insensitively, with `text/event-stream` and `text/html`; a value that does not parse is neither. Chosen over mime 0.3.18 (amends the plan's "Components audit" line), which refuses `text/event-stream ; charset=utf-8`, allowed by RFC 9110 §5.6.6, and over the in-house compare, as a maintained parser exists (Block 0). A parameter without a value, which RFC 9110 forbids and EventSource accepts, is now refused.
- **D5 Version.** No bump: nothing breaks (Block 25 D11). `Unreleased` entries: 27a "Changed": non-UTF-8 sse data arrives with U+FFFD; a content type with a valueless parameter is not an event stream; a long sse comment or unknown field no longer ends the transport. 27b "Added": Protobuf over http_stream; "Changed": building refuses Protobuf only with sse.

**Sub-increments** (one branch and PR each, in this order):

| Id | Value clause | Scope | Scenarios |
|---|---|---|---|
| 27a | As a maintainer, I want the sse and http_stream JSON streams and the content-type check handled by maintained components bounded by `MAX_MESSAGE_BYTES`, so that a fix to their parsing reaches the client from upstream | D1, D2, D4, D5 | none (see Exceptions) |
| 27b | As an app developer using Protobuf behind networks that block websocket, I want the client to fall back to http_stream, so that binary payloads still arrive | D3, D5 | the three scenarios below |

Value clause: As an app developer using Protobuf behind networks that block websocket, I want the client to fall back to http_stream, with the server's stream split by maintained, bounded components, so that binary payloads arrive and a parsing fix reaches my client from upstream.

**Structural acceptance:** new shipped crates are sse-core and mediatype only (`cargo tree -e normal`), passing `cargo deny`; the in-house sse decoder and line splitter are removed; the Protobuf splitter has a bolero target in `fuzz.yml` and the `ci.yml` fuzz loop (chunk invariance, a frame over the limit failing, a truncated prefix waiting); the existing sse and http_stream fuzz targets keep their invariants against the new components; unit tests: a Protobuf reply split over chunks (inside its prefix too), several in one chunk, a zero-length reply delivered, an over-limit length failing before its bytes, an over-long varint failing, and the exact `Content-Type` and `Accept` of the stream and emulation requests in both encodings; `a_build_error_is_a_standard_error` uses sse; the README says sse carries JSON only.

```gherkin
Feature: Protobuf over http_stream

  @A
  Scenario: Fallback to http_stream with Protobuf
    Given a server with http_stream and sse enabled
    And a client configured with the Protobuf protocol, an unreachable websocket endpoint and the server's http_stream endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "http_stream"

  @A
  Scenario: Binary publication delivered over Protobuf on http_stream
    Given a server with http_stream and sse enabled
    And a client configured with the Protobuf protocol connected over transport "http_stream" and subscribed to channel "news"
    When the server API publishes the bytes ff 0a 00 fe to channel "news"
    Then the subscription receives a publication with the bytes ff 0a 00 fe

  @A
  Scenario: Server pings over Protobuf on http_stream are answered
    Given a server with http_stream and sse enabled, a ping interval of 2 s and a pong timeout of 1 s
    And a client configured with the Protobuf protocol connected over transport "http_stream"
    When 6 s elapse
    Then the client stays "connected"
```

Block 8's outline "Protobuf with an HTTP transport is rejected" is replaced in place by the scenario "Protobuf with the sse transport is rejected" (amends Block 8).

**Exceptions introduced:** 27a has no scenario, as Block 26: its observable changes concern malformed input only (D5) and are unit-tested in `src/adapter/http.rs`.

---

## Block 28 — Tokio runtime handle

**Goal:** an application can build a client where no tokio runtime is current, such as a Tauri setup hook, a plugin's initialisation or synchronous start-up code, by giving the runtime the client runs on; building without one returns an error instead of panicking; and a client whose runtime shuts down fails its calls and ends its streams instead of leaving them pending (parity item 15 and finding F1; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A).

**Dependencies:** Blocks 0 (tokio adapter), 1 (client states), 3 (client-side disconnect codes), 9 (the subscription registry), 13 (pending calls, code 3), 15 (one event per state change, streams ending), 16 (invariant I2, the chaos adapter runner), 21 (state changes logged), 26 (D3, the source check).

**Out of scope:**
- other executors: the adapter is tokio by design (Block 0); a spawner trait (the analysis's design B) is not adopted;
- a blocking facade owning a runtime (centrifuge-rust `docs/spec.md`, "Blocking facade", phase 2): not in the plan;
- the futures and streams the application awaits: they already need no runtime (tokio `sync` primitives and the outboxes' wakers; the call timers are the driver's), so any executor can await them; this block keeps that; the application's token, data and getState callbacks run on the client's runtime;
- detecting at build a runtime without time or I/O: tokio offers no query (D4 covers the panic it causes);
- `close()` (Block 33), the delivery modes' threads (Block 41), a `closed` state (Block 0).

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Option.** `ClientBuilder::runtime(tokio::runtime::Handle)`: the client's driver runs on that runtime. Without it, `build()` takes `Handle::try_current()`; with neither, `build()` and `Client::new` return an `ErrorKind::Configuration` error saying that no tokio runtime is current and naming `ClientBuilder::runtime`, and start nothing (centrifuge-rust `docs/spec.md` §4.1 line 236 and `docs/api.md` line 71, commit 88d6edf). A given handle wins over the current runtime. The client keeps the handle, and every spawn of the adapter goes through it (`Handle::spawn`), the transports' closing included (`Transport::close`), so that none depends on the context it runs in (analysis item 15, design A); the dependencies' own spawns (hyper-util's `TokioExecutor`, DNS lookups in `spawn_blocking`) happen while the driver polls them, so on its runtime. The runtime needs time and I/O enabled (`enable_all`), as today; the option's documentation says so. The `# Panics` sections of `build()` and `Client::new` go, and the README's "`Client::new` must be called inside a tokio runtime" becomes the option, with a Tauri setup hook (`tauri::async_runtime::handle()`) as its example.
- **D2 Runtime not driven.** A current-thread runtime runs the client only while a thread is inside its `Runtime::block_on` (tokio `runtime` module, "Driving the runtime": `Handle::block_on` does not drive it); meanwhile calls and streams wait and no timeout runs, the timers being the driver's. Documented on the option, as tokio documents `Handle::spawn`.
- **D3 Runtime shut down.** A `Handle` does not keep its runtime alive (tokio `Runtime`, "Sharing"), and a runtime shutting down drops its tasks without running them to the end (`Runtime`, "Shutdown"); today the driver's cleanup then never runs: calls wait forever and streams never end, even after the client is dropped. The driver gets a drop guard, as hyper's dispatch callback fails its request when the runtime drops the dispatch task (hyper 1.12.0 `src/client/dispatch.rs:242-271`, "runtime dropped the dispatch task"). When the driver's task is dropped before the client was closed, the client ends as when it is dropped, with its own code: it emits `disconnected` with code 4 "runtime shut down" (`codes::disconnected::RUNTIME_SHUT_DOWN`, through a new `Input::DriverDropped { panicked: false }`; no event when it was `disconnected` already, as no state changes), the SDK spec letting client implementations use codes below 3000 for client-side reasons ("Disconnect codes"). Every subscription, client-side and server-side, first emits `unsubscribed` with code 2 "client closed", and pending calls fail with "client closed" (code 4), as on `close()` (Block 33, D4); every stream, the subscriptions' included, yields the events already queued and ends; later calls fail at once with code 4, `connect()` does nothing and `new_subscription` returns `SubscriptionError::ClientClosed`. The guard checks and sets the closed flag under the core lock, as `Shared::shut_down` does, so a client dropped while its runtime shuts down ends once, with the code of whichever came first; after a normal stop it runs again with no effect. It spawns nothing and does no I/O: the transport is dropped, and the server sees the connection close without a close frame.
- **D4 Driver panic.** A driver task dropped while panicking (`std::thread::panicking()`: a bug, which invariant I2 forbids, or a runtime without time or I/O) is not a runtime shutdown (hyper 1.12.0 reports "user code panicked" then, same file): the client ends as in D3 with code 2 "exception during message handling" (`codes::disconnected::BAD_PROTOCOL`; centrifuge-js 63ed73e `codes.ts` `badProtocol`, `centrifuge.ts:1816-1822`; Block 41's "exception in event handler" uses the same code), through `Input::DriverDropped { panicked: true }`. When the panic poisoned the core's lock the machine may be inconsistent, and a second panic would abort the process: it is not fed and no event is emitted, while the calls and streams still end.
- **D5 Runtime already shut down.** Spawning on a runtime that has shut down drops the task at once (tokio 1.54.1 `src/runtime/task/list.rs:140-143`), so the guard closes the client before `build()` returns; `build()` then returns an `ErrorKind::Configuration` error saying that the tokio runtime has shut down, instead of a client that can never connect.
- **D6 Version.** No bump: `ClientBuilder::runtime`, the two codes and `Input::DriverDropped` (`Input` is `#[non_exhaustive]`, Block 17) are additive, and a build that panicked now returns an error (Block 27 D5). `Unreleased` entries: "Added" `ClientBuilder::runtime`; disconnected codes 2 "exception during message handling" and 4 "runtime shut down" (`codes::disconnected::BAD_PROTOCOL`, `RUNTIME_SHUT_DOWN`); `Input::DriverDropped`. "Fixed" building outside a runtime returning an error instead of panicking; a client whose runtime shut down, or whose driver panicked, failing its calls and ending its streams instead of leaving them pending. "Changed" `SubscriptionError::ClientClosed` reads "the client is closed" (dropped, or its runtime shut down), and the `events()` documentation says that the streams also end then.

Value clause: As an app developer whose start-up code runs outside a tokio runtime, I want to give the client the runtime it runs on, so that building it never panics and its calls and streams end when that runtime does.

**Structural acceptance:** unit tests in the adapter, on current-thread runtimes that are not driven unless stated: `Client::new` on a plain thread returns D1's error (no panic, no `catch_unwind`, as `client_new_returns_an_endpoint_error_instead_of_panicking`); with a handle given inside another runtime, the driver is a task of the given one (`RuntimeMetrics::num_alive_tasks`, stable since tokio 1.39, goes from 0 to 1 there and is unchanged on the current one); dropping the given runtime while the client is `connecting` emits `disconnected` code 4 and fails a call waiting for the connection with code 4; dropping it while `disconnected` emits nothing and ends the stream; dropping the client and then its runtime ends the streams after the subscription's `unsubscribed` code 2 and `disconnected` code 0, and a subscription's call fails with code 4; after a shutdown a call fails at once, `connect()` emits nothing and `state()` is `disconnected`; a runtime built without `enable_all`, driven, makes `connect()` end the client with code 2; the guard dropped during a panic with the core's lock poisoned emits nothing and still ends the calls and streams; building with the handle of a runtime already shut down returns D5's error. A source check, like Block 26 D3: no `tokio::spawn` or `tokio::task::spawn` in non-test code under `src/adapter/`. The steps build "outside any tokio runtime" on a `std::thread`, never in `spawn_blocking` (whose threads are inside the runtime), and shut a runtime down from an async step with `shutdown_background()`. The chaos adapter runner and the Block 14 @P gates pass.

```gherkin
Feature: Tokio runtime handle

  Scenario: Building outside a runtime without a handle fails
    Given client options without a runtime handle
    When the client is built outside any tokio runtime
    Then building fails with a configuration error naming the runtime option

  @A
  Scenario: A client built outside a runtime with a handle connects
    Given a separate multi-thread tokio runtime
    And a client built outside any tokio runtime with that runtime's handle
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @D
  Scenario: The client ends when its runtime shuts down
    Given a separate multi-thread tokio runtime
    And a client built with that runtime's handle and connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When that runtime shuts down
    Then the call fails with a "client closed" error before the configured timeout
    And the client emits "disconnected" with code 4 and reason "runtime shut down"
    And the client's stream ends
    And creating a subscription fails because the client is closed
```

**Exceptions introduced:** none (the codes 2 and 4 are recorded in `src/codes.rs`; Block 34 tabulates them).

---

## Block 29 — Handshake timeout, maximum incoming message size

**Goal:** an app on a slow link or behind a proxy gets time to open a transport without lengthening every command's timeout, an app on a device with little memory bounds the largest message the server may send it, and `Config` can gain options without breaking the applications that build one (parity items 11 and 12, finding F3; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A for both, in one breaking release that marks `Config` `#[non_exhaustive]`).

**Dependencies:** Blocks 3 (close code 1009, code 3 "message size limit exceeded"), 7 (several Protobuf messages in one websocket frame), 8 (the fallback, `Config::timeout`, the relay scenarios), 13 (calls bounded by `timeout`), 15 (the read-ahead admits one larger message alone), 17 (`Input` is `#[non_exhaustive]`), 19 (D6, the delta bounds), 21 (the WARN record of an oversized message), 23 (the proxy handshake counts in the open), 25 (D1, a replaced timer's firing; D3, a token fetched after the open), 27 (D3, the decoders' bounds).

**Out of scope:**
- a separate timeout per command kind, or for writes and closing: `timeout` keeps bounding them (Block 8);
- a size limit on what the client sends: `write_queue_bytes` and `batch_frame_bytes` (Blocks 13, 18) and the server's own limits (close code 1009, HTTP 413) stay as they are;
- deriving the cap from `read_ahead_bytes` (the analysis's design B for item 12), or one timer for open plus reply (design B for item 11): not adopted;
- an `error` event beside `disconnected` code 3 (centrifuge-rust `docs/spec.md` §5.3 emits one): the outcome stays Block 3's for close code 1009;
- a separate `ConfigBuilder`;
- `#[non_exhaustive]` on other public structs.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Handshake timeout.** `Config::handshake_timeout`, default 5 s, bounds opening a transport: from `Action::OpenTransport` to `Input::TransportOpened`, which covers DNS, TCP, the proxy handshake (Block 23), an application's connector (Block 37 D4), TLS and the websocket upgrade, or, for http_stream and sse, the stream request until its response headers. The core arms `Timer::ConnectTimeout` with `handshake_timeout` at `OpenTransport` and sets it again with `timeout` at `TransportOpened`, for every transport kind, emulated ones included; so the connect reply, and a token fetched after the open (Block 25 D3), keep `timeout`. Setting a timer again replaces the running one (`Timer`: at most one of each kind runs at a time), and the tokio adapter drops a firing of the replaced one (Block 25 D1). One timer kind, so no new variant of the exhaustive `Timer` (analysis item 11, design A; centrifuge-rust `docs/spec.md` §4.2, §5.1, §6.2, commit 88d6edf). An open that times out fails the attempt as a refused one (Block 8: error event, the next transport, the backoff after a full sequence); its error event's message is "handshake timeout", while a missing connect reply keeps "connect timeout". The worst case of one attempt goes from 5 s to 10 s, and of a first sequence over three transports from 15 s to 30 s. The documentation says so, says that setting both to 2.5 s keeps today's bound, and says that a call made while connecting still waits for the connection for at most `timeout` (Block 13), so on a slow open it can fail before the attempt does. `Config::timeout`'s and `Timer::ConnectTimeout`'s documentation no longer name the open as `timeout`'s.
- **D2 Maximum incoming message size.** `Config::max_message_bytes`, default 64 MiB (today's fixed `MAX_MESSAGE_BYTES`, which it replaces), is at least 1, or `build()` and `Client::new` return a configuration error naming it, as the read-ahead limits do (Block 15). It bounds what a transport delivers as one message: a websocket message whole, even when it holds several JSON or Protobuf replies (Block 7), and each of its frames (tungstenite's `max_message_size` and `max_frame_size`); an http_stream JSON line; an http_stream Protobuf reply, by its length prefix (Block 27 D3); an sse event's data. It is also the core's bound of Block 19 D6 (a delta target, and the decoded publications of one subscribe reply), which keeps its outcome. The read-ahead keeps admitting one message larger than `read_ahead_bytes` alone (Block 15), so this field is the per-message memory bound; the documentation says so for devices with little memory. `Config`'s `Debug` shows both new fields.
- **D3 An oversized message.** The adapter refuses a message once it is known to exceed `max_message_bytes`, holding at most the limit and one read (tungstenite checks a frame from its header and a message as its frames arrive; the line and sse decoders once the part buffered exceeds the limit; the Protobuf splitter from the length prefix). It ends the transport and reports it with a new `Input::MessageTooLarge(TransportId)` instead of `Input::TransportClosed` (additive: `Input` is `#[non_exhaustive]`, Block 17).
  - Before the connect reply on that transport, the attempt fails as a refused one (Block 8), with an error event whose message is "message larger than the size limit": before the server has answered, the bytes may not be Centrifugo's (centrifuge-rust `docs/spec.md` §5.3, "an HTML page read as a varint").
  - Once the client is connected on it, the client ends: `disconnected` with code 3 "message size limit exceeded" and no reconnect, as for close code 1009 (Block 3, review item C4; centrifuge-rust §5.3: "never a silent reconnect loop"), since a reconnect would ask for the same data again. The field's documentation says that the application decides when to connect again, and that the limit must exceed the largest reply it expects (history, recovery).
  - The rule is the adapter's: the core does not compare an `Input::Frame`'s length with the field. The frame is in memory by then, and an http_stream Protobuf frame carries its length prefix, so the core would refuse a reply at the limit that the splitter accepted. Mature clients bound size where the bytes are read: tungstenite `max_message_size`, tonic `max_decoding_message_size`, tokio-util `AnyDelimiterCodec` `max_length`. An owner's own adapter applies the rule as the field and `Input::MessageTooLarge` document.
  - The WARN record of an oversized message (Block 21) stays.
- **D4 `Config` `#[non_exhaustive]`.** `Config` is marked `#[non_exhaustive]`, so later options are additions (plan; tungstenite's `WebSocketConfig`). Outside the crate a struct literal, `..Config::default()` included, no longer compiles; an application starts from `Config::default()`. It sets fields either by assigning the public fields, which stay public, or through chainable setters, one per field and named after it, as tungstenite's `WebSocketConfig` offers since 0.25.0: `Config::default().timeout(Duration::from_secs(3)).token(token)`. Each setter takes `self`, returns it and is `#[must_use]`. It takes the field's type, except that `name`, `version` and `token` take `impl Into<String>`, and `data` takes `impl Into<Vec<u8>>` and sets `Some`, as `Client::set_token` and `Client::set_data` do. Values are still checked by `build()`. The README's examples, the doc tests, the integration tests and the chaos and perf runners change accordingly.
- **D5 Version.** 0.18.0: a new field of an exhaustive `Config` and `#[non_exhaustive]` break the public API, `timeout` no longer bounds the open, and an oversized message after the connect reply ends the client instead of reconnecting. `Unreleased` entries:
  - "Added": `Config::handshake_timeout`, `Config::max_message_bytes`, the `Config` setters, `Input::MessageTooLarge`.
  - "Changed": `Config` is `#[non_exhaustive]`; `timeout` bounds the connect reply and no longer the open, as does `Timer::ConnectTimeout` once the transport is open; a message over the limit after the connect reply ends the client with code 3 instead of reconnecting with code 1, and one before it fails the attempt with an error event instead of reconnecting without one.

This block amends Block 19 D6 and D10 (the limit is no longer a constant) and one Block 8 scenario.

**Changed scenario (Block 8).** In "An HTTP stream that never answers the handshake falls back", the client has a handshake timeout of 2 s and the client must emit "connected" over sse within the configured handshake timeout plus tolerance (the text is in Block 8). Its relay never answers the stream request, so the open never completes; its handshake timeout is shorter than the default `timeout`, so a client that kept bounding the open with `timeout` fails it. The scenario leaves `IMPLEMENTED` in `tests/bdd.rs` until this block's implementation adds its steps. "A websocket that never answers the handshake falls back" and "An opened websocket without a connect reply falls back" exercise the connect reply and keep `timeout`.

Value clause: As an app developer shipping to devices on slow links or with little memory, I want to bound the opening of a transport apart from its replies and to cap the size of a message from the server, so that a slow open does not slow down every command's timeout and a single oversized message cannot exhaust the device's memory.

**Structural acceptance:**
- @E unit tests on the core:
  - `Timer::ConnectTimeout` is set with `handshake_timeout` at `OpenTransport` and with `timeout` at `TransportOpened`, for websocket and for http_stream;
  - the error messages are "handshake timeout" and "connect timeout";
  - `Input::MessageTooLarge` before the connect reply emits the error event and opens the next transport; after it, the client disconnects with code 3 and sets no reconnect timer;
  - a frame longer than `max_message_bytes` given as `Input::Frame` is handled as any frame;
  - the delta bound follows the field.
- A unit test: each setter sets its field, as assigning it does.
- Adapter unit tests: the websocket reader, the http_stream JSON and Protobuf splitters and the sse decoder each refuse a message one byte over a small limit and accept one at the limit; a refused message is reported as `Input::MessageTooLarge`.
- New test support:
  - a websocket endpoint behind the relay in `Mode::Silent`;
  - the fake server pads a push, or the first connect reply, to an exact websocket message size in both encodings, the Protobuf length prefixes included.
- The chaos runners and the Block 14 @P gates pass.

```gherkin
Feature: Handshake timeout and maximum incoming message size

  Scenario: An open that never completes is bounded by the handshake timeout
    Given a client with a handshake timeout of 1 s and a timeout of 10 s, configured with a websocket endpoint behind a relay that accepts but never answers
    When connect is called
    Then within 1 s plus tolerance the client emits an "error" event for the failed transport attempt whose message ends with "handshake timeout"

  @E
  Scenario: The connect reply keeps the timeout after the open
    Given a client with a handshake timeout of 1 s and a timeout of 3 s, configured with websocket and http_stream endpoints
    When the websocket opens after 900 ms and no connect reply arrives
    Then 2.9 s after it opened the websocket attempt has not failed
    And 3 s after it opened the next attempt opens the http_stream transport

  @D
  Scenario Outline: A message over the limit ends a connected client
    Given a client with a maximum message size of 1024 bytes and the <protocol> protocol, connected to the fake server
    When the fake server sends a publication push in a websocket message of 1025 bytes
    Then the client emits "disconnected" with code 3 and reason "message size limit exceeded"
    And the client makes no new connection attempt

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario Outline: A message at the limit is delivered
    Given a client with a maximum message size of 1024 bytes and the <protocol> protocol, subscribed to channel "news" on the fake server
    When the fake server sends a publication on channel "news" in a websocket message of exactly 1024 bytes
    Then the subscription receives that publication

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: A message over the limit before the connect reply fails the attempt
    Given a client with a maximum message size of 1024 bytes, configured with the fake server's endpoint
    And the fake server answers the first connect command with a websocket message of 1025 bytes
    When connect is called
    Then the client emits an "error" event for the failed transport attempt whose message ends with "message larger than the size limit"
    And then the client emits "connected"

  Scenario: A maximum message size of 0 is rejected
    Given client options with a maximum message size of 0
    When the client is built
    Then building fails with a configuration error naming the maximum message size
```

**Exceptions introduced:** none.

---

## Block 30 — Waiting until ready

**Goal:** an app can wait until the client is connected, or a subscription subscribed, without taking events from their one active stream (Block 15) to watch for it, and learns at once when that will not happen (parity item 10; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A).

**Dependencies:** Blocks 1 (client states), 9 (subscription states, subscribing before `connect()`), 10 (unsubscribe codes, subscription tokens), 13 (calls waiting for the connection, codes 3 and 7, `CallError`, its Exceptions introduced), 15 (in-order handling, one active stream, `state()` ahead of the stream), 28 (a closed client, the drop guard, futures awaitable on any executor).

**Out of scope:**
- a timeout parameter (the analysis's design B): the caller wraps the future in `tokio::time::timeout` or its executor's equivalent, as the methods' documentation shows;
- a stream of state changes (design C);
- `ready()` for server-side subscriptions, which have no handle (Block 12);
- whether `ready()` resolves only after an event handler returned (centrifuge-js 63ed73e `subscription.ts:597-602`; centrifuge-rust `docs/spec.md:1087`, commit 88d6edf): Block 41's handlers;
- `close()` and its "client closed" error (Block 33, which revisits the code a closed client's waiters get); a `closed` state (Block 0).

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Client.** `Client::ready(&self) -> impl Future<Output = Result<(), CallError>> + Send + 'static`. The wait starts when the method returns: the closed flag and the state are judged, and the waiter registered, in one section under the core lock, as `Shared::call` makes a call. The future borrows nothing from the client, so it can be spawned, as `Client::rpc`'s can.
  - A closed client (closed by `close()`, its runtime shut down or its driver panicked, Blocks 28 D3, D4 and 33) fails at once with code 4 "client closed" (`codes::error::CLIENT_CLOSED`), whatever `state()` reports.
  - Connected: it resolves at once.
  - Disconnected: it fails at once with code 3 "disconnected" (`codes::error::DISCONNECTED`), also on a client whose `connect()` was never called. A call made then waits instead (Block 13). Sources: centrifuge-js 63ed73e `centrifuge.ts:440-463`, whose message here is "client disconnected" (Ferrifuge keeps its calls' "disconnected": one message per code); centrifuge-rust `docs/spec.md:353`, "`ready()` is different: it fails at once in `disconnected`".
  - Connecting: it waits, across failed attempts and reconnects, until the client is connected, and then resolves. If instead the client enters `disconnected` for any reason, it fails with code 3, as the calls waiting for the connection do (Block 13; centrifuge-js `_rejectPromises`, `centrifuge.ts:1947`, `:1989`, `:2011`). The reasons are `disconnect()`, a terminal disconnect code or connect error, or a denied token. If the client is closed instead (`close()`, the `Client` dropped, its runtime shut down or its driver panicked), it fails with code 4 "client closed" (Block 33).
  - `Config::timeout` does not bound it.
- **D2 Subscription.** `Subscription::ready(&self)`, with the same signature, starts its wait the same way.
  - When its client is closed, it fails at once with code 4 "client closed", whatever its state, as its calls do then (Blocks 28 D3, 33).
  - Subscribed: it resolves at once.
  - Unsubscribed: it fails at once with code 7 "subscription unsubscribed" (`codes::error::SUBSCRIPTION_UNSUBSCRIBED`, the message of its calls; centrifuge-js `subscription.ts:209-231`, whose message is "unsubscribed"). This includes a removed subscription, whose `state()` reports `unsubscribed`.
  - Subscribing: it waits, also while the client is disconnected or connecting (a subscription subscribed before `connect()`, Block 9) and across the client's reconnects, which keep it subscribing. It resolves once the subscription is subscribed. It fails with code 7 when the subscription becomes `unsubscribed` for any reason: `unsubscribe()`, removal, a permanent subscribe error, a denied token or data callback, a server unsubscribe code below 2500, or an oversized message (Blocks 9, 10, 29; centrifuge-js `subscription.ts:1100`). It fails with code 3 when its client enters `disconnected`, and with code 4 when it is closed (Block 33), as a subscription-level call waiting for its subscription does (Block 13, Exceptions introduced; see below).
- **D3 Error type.** `CallError`, the error of the calls `ready()` gates, so `client.ready().await?` and the calls after it share one error type. Precedents: hyper's `SendRequest::ready()` returns `hyper::Error`, and tower's `Service::poll_ready` returns the service's error. Codes 3 and 7 are already `CallError` codes, it is `#[non_exhaustive]`, and it carries centrifuge-js's `{code, message}` (`centrifuge.ts:443`, `subscription.ts:211`). A `ReadyError` enum (the analysis's sketch) would duplicate those codes. Each method has an `# Errors` section.
- **D4 State, not events.**
  - `ready()` follows the states `state()` reports, not the events taken. Waiters are settled under the core lock when the adapter sees the state after an input it gives the machine (`Shared::handle_fired`), not when the future is polled. So a waiter released by `connected` stays resolved even if the client is connecting again before the future is polled. A state that lasts only within one input, such as a connect reply followed by a disconnect push in the same frame, is not seen, as `state()` never reports it.
  - `ready()` can resolve before the `connected` or `subscribed` event is taken, since `state()` can be ahead of the stream (Block 15). It takes no event and opens no stream: a second `events()` call would end the application's stream.
  - The state moves only as messages are handled, and a message is handled only after every event of earlier messages has been taken, on any stream of the client (Block 15, "In order", "Head-of-line"). So a task that holds a stream with such events and awaits `ready()` without reading the stream waits until the stream is read or dropped (centrifuge-rust `docs/spec.md:1094` says the same).
  - The documentation of both methods states all of this.
- **D5 Futures.**
  - Any number of `ready()` futures may wait at once; each is settled once.
  - They are cancel-safe: dropping one removes its waiter.
  - They wait on `tokio::sync` primitives only, so any executor can await them (Block 28). On a runtime that is not driven (Block 28 D2) they wait as calls do.
  - The drop guard of Block 28 D3 and D4 fails every waiter with code 4, also when the poisoned core lock leaves the machine unfed (Block 33).
  - Cost: the client's state is compared after each input, and subscription states are read only for subscriptions that have a waiter, so without waiters the cost per message is unchanged (Block 14 @P gates).
  - There is no new `Input` or `Action`. An owner of the machine does the same with `Machine::state` and `Machine::subscription_state` after each input.
- **D6 Version.** No bump: the two methods are additive. `Unreleased` entry: "Added" `Client::ready` and `Subscription::ready`.

Value clause: As an app developer, I want to wait until the client is connected or a subscription subscribed without reading their event streams, so that start-up code can await readiness and learn at once when it will not come.

**Structural acceptance:**
- Adapter unit tests:
  - a dropped `ready()` future leaves no waiter;
  - several futures waiting at once all settle;
  - a waiter released by `connected` stays resolved when the client reconnects before the future is polled;
  - a subscription's waiter keeps waiting across a client reconnect and fails with code 7 when the subscription is removed;
  - after the `Client` is dropped, a subscription's `ready()` fails at once with code 4, and one already waiting fails with code 4;
  - a runtime shut down while a client `ready()` and a subscription `ready()` wait fails both with code 4, also with the core lock poisoned (Block 28 D4);
  - a `ready()` future awaited with `futures-executor` 0.3's `block_on` (a new dev-dependency, from the same project as `futures-core` and `futures-util`) on a plain thread outside any runtime fails with code 3 when another thread calls `disconnect()`.
- The steps "ready is awaited on ..." start the future on a task and record its outcome and time, as the call steps do (`track`, `outcome`).
- The Block 14 @P gates and the chaos adapter runner pass.

```gherkin
Feature: Waiting until ready

  @D
  Scenario: Client ready resolves on the state, before the "connected" event is taken
    Given a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    And ready is awaited on the client
    Then ready on the client resolves
    And when the application reads the client's stream, it yields "connecting", "connected"

  Scenario: Client ready on a disconnected client fails at once
    Given a new client
    When ready is awaited on the client
    Then ready on the client fails at once with a "disconnected" error

  Scenario: Disconnect fails a waiting client ready
    Given a client configured with an unreachable endpoint
    When connect is called
    And ready is awaited on the client
    And disconnect is called
    Then ready on the client fails with a "disconnected" error

  @D
  Scenario: Client ready keeps waiting across failed attempts
    Given the fake server answers the first 2 connect commands with a temporary error
    And a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    And ready is awaited on the client
    Then 1 s later the client's state is "connecting"
    And ready on the client is still waiting
    And when the application reads the client's stream, it yields "connecting", "error", "error", "connected"
    And ready on the client resolves

  @B
  Scenario: A terminal connect failure fails a waiting client ready
    Given the connect proxy answers after 1 s with disconnect code 4500
    And a new client
    When connect is called
    And ready is awaited on the client
    Then the client emits "disconnected" with code 4500
    And ready on the client fails with a "disconnected" error before the configured timeout

  @D
  Scenario: Subscription ready resolves on the state, before the "subscribed" event is taken
    Given a client connected to the fake server
    And a subscription to channel "news" whose stream was taken and is not read
    When subscribe is called on the subscription
    And ready is awaited on the subscription
    Then ready on the subscription resolves
    And when the application reads the subscription's stream, it yields "subscribing", "subscribed"

  Scenario: Subscription ready on an unsubscribed subscription fails at once
    Given a new client
    When a subscription to channel "news" is created
    And ready is awaited on the subscription
    Then ready on the subscription fails at once with a "subscription unsubscribed" error

  @D
  Scenario: A permanent subscribe error fails a waiting subscription ready
    Given a client connected to the fake server that answers subscribe commands after 1 s with a permanent error
    When a subscription to channel "news" is subscribed
    And ready is awaited on the subscription
    Then the subscription emits "unsubscribed"
    And ready on the subscription fails with a "subscription unsubscribed" error before the configured timeout

  @A
  Scenario: Subscription ready made before connect waits for the connection
    Given a new client
    When a subscription to channel "news" is created and subscribed
    And ready is awaited on the subscription
    And connect is called
    Then ready on the subscription resolves

  Scenario: Disconnect fails a waiting subscription ready
    Given a client configured with an unreachable endpoint
    When connect is called
    And a subscription to channel "news" is created and subscribed
    And ready is awaited on the subscription
    And disconnect is called
    Then ready on the subscription fails with a "disconnected" error
    And the subscription state is "subscribing"
```

**Exceptions introduced:** a subscription's `ready()` that is waiting fails with "disconnected" (code 3) when its client enters `disconnected`, as Block 13's subscription-level calls waiting for their subscription do. centrifuge-js 63ed73e rejects a subscription's `ready()` promises only on `unsubscribed` (`subscription.ts:1100`): its subscriptions move to, or stay in, `subscribing` (`centrifuge.ts:963-966`), so such a wait lasts until the next connection or the caller's timeout. centrifuge-rust `docs/spec.md:439` keeps waiting too.

---

## Block 31 — Statistics

**Goal:** support staff and fleet telemetry read a client's connection and traffic counters, and an operator reads what delta compression saves on a subscription, without a round trip and without reading the event streams (parity items 8 and 38; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A for both).

**Dependencies:** Blocks 1 (states), 5d (transport kind), 8 (attempts, fallback, emulated transports, HTTP 413), 9 (subscription registry), 12 (server-side subscriptions), 13 (calls), 14 (@P gates), 15 (delivered means taken, withdrawal, in-order handling), 18 (several commands per frame), 19 (delta), 28 (a closed client, the poisoned core lock), 29 (`max_message_bytes`), 30 (the adapter watching the state after each input).

**Out of scope:**
- a metrics facade (the analysis's design B for item 8): an application can feed one from the snapshot;
- per-channel traffic counters beyond delta statistics;
- delta statistics of server-side subscriptions, which have no handle and no delta (Blocks 12, 19);
- resetting counters.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Snapshot.** `Client::statistics(&self) -> Statistics`, sync, `#[must_use]`, never a round trip (centrifuge-rust `docs/spec.md` §4.3, commit 88d6edf). `Statistics` is a `#[non_exhaustive]` struct with public fields, deriving `Debug`, `Clone`, `PartialEq` and `Eq` (not `Copy`, so that it can gain any field). It holds counters since the client was built. Fields:
  - `state: State`, as `state()` reports it.
  - `transport: Option<TransportKind>`: the transport while connected, `None` otherwise.
  - `connected_since: Option<SystemTime>`: when the adapter saw the client become connected, `None` while it is not connected (D2).
  - `connects: u64`: the times the client became `connected`, reconnects included. This counts one too when a disconnect push follows the connect reply in the same frame.
  - `failed_attempts: u64`: attempts that ended without the client becoming connected.
    - An attempt is a transport the client opens to connect (`Action::OpenTransport`), each fallback transport being one.
    - It fails when its opening fails, when it closes or exceeds the size limit before the connect reply, on a handshake or connect timeout, on a connect error reply (temporary or terminal), on a disconnect push or close code before connecting, or on a token denied during it.
    - An attempt ended by `disconnect()`, `reconnect()` or the client's closing (Blocks 28, 33) is not counted.
    - A failure before any transport is opened (a connect data or token callback failing) is not an attempt.
    - Not every failure emits a transport `error` event (a connect error reply's is `ErrorKind::Connect`), so the counter follows attempts, not events. The name says what it counts, unlike centrifuge-rust's `reconnect_attempts`, which also counts the first connect's failures; async-nats 0.52.0 `Statistics` names its counter `connects` too.
  - `bytes_in: u64`: bytes of the frames the core reads on the current transport (`Input::Frame` data), an undecodable frame included.
  - `bytes_out: u64`: bytes of the frames the core hands over once they are written. These are `Action::Send` frames reported with `Input::FrameWritten` on the current transport, and an emulated transport's connect command (`OpenTransport.initial_frame`), counted when the transport opens.
    - On emulated transports a frame is the emulation request body.
    - Transport framing is not counted: websocket and HTTP headers, http_stream delimiters and length prefixes, sse fields.
    - A frame the transport ends before it is reported written is not counted.
    - A request refused with HTTP 413 is written, so it counts once; each command then sent again alone counts again (Block 8).
  - `messages_in: u64`: replies and pushes decoded from those frames when read, pings included, and those later dropped included (messages after a disconnect push, messages of a transport that ended while they were held, Block 15).
  - `messages_out: u64`: commands in the frames counted in `bytes_out`, pongs included, each command of a batch frame counted (Block 18).
  - `publications_delivered: u64`: publications of client-side and server-side subscriptions once they count as taken under Block 15. That means taken from a stream, emitted while no stream is open, or left in a stream the application dropped. A withdrawn publication never counts.
  - `subscriptions: usize`: the client-side registry's size. `server_subscriptions: usize`: the server-side registry's size (`Machine::server_subscriptions`, Block 12).
- **D2 Where the counters live.**
  - Every counter but `connected_since` lives in the core: `Machine::statistics(&self) -> Statistics`, deterministic and @E-testable. It reports `connected_since: None`, as the core has no clock; its owner sets that public field.
  - The tokio adapter's `Client::statistics` reads the machine's snapshot under the core lock, also when the lock is poisoned (Block 28 D4). It sets `connected_since` from `SystemTime::now()` when it sees the state become `connected` after an input, where Block 30 D4 settles waiters, and clears it when it sees any other state.
  - A closed client keeps its last counters (Blocks 28, 33).
  - Cost: plain `u64` additions on inputs and actions the core already handles. The only addition is that the core keeps each sent frame's size and command count until it is reported written, and drops the entries of a transport that ends. The Block 14 @P gates hold, "Memory stays flat across reconnects" among them.
- **D3 Delta statistics.**
  - `Subscription::delta_statistics(&self) -> DeltaStatistics`, a `#[non_exhaustive]` struct (`Debug`, `Clone`, `PartialEq`, `Eq`), and `Machine::delta_statistics(&self, SubscriptionId) -> Option<DeltaStatistics>`, `None` for an id not in the registry (centrifuge-js 63ed73e `subscription.ts:388-400`, `types.ts:818-831`).
  - Fields:
    - `publications: u64`;
    - `full_payloads: u64`;
    - `delta_payloads: u64`;
    - `bytes_received: u64`: each payload as carried, a delta or a full payload, after the JSON protocol's string unwrapping, as centrifuge-js `src/json.ts:19-39` measures it; Protobuf payloads as they are;
    - `bytes_decoded: u64`: each payload after the delta is applied.
  - A publication counts when its payload is rebuilt while delta is negotiated (Block 19), recovered ones included, as centrifuge-js counts in `_handlePublication` (`subscription.ts:1110-1140`; its other sites, `:1225-1250` and `:2578-2587`, are map subscriptions', Block 45). This happens before delivery, since the counters measure the wire, so a publication withdrawn later still counts. A delta that fails to apply does not.
  - Counted since the subscription was created, across resubscribes and delta errors.
  - `compression_ratio(&self) -> f64`, `#[must_use]`: `1 - bytes_received / bytes_decoded`, 0 when nothing was decoded, negative when deltas carried more than they built (documented).
  - A subscription without negotiated delta reports zeros.
  - A removed subscription reports zeros: its handle has no registry entry, as its `state()` reports `unsubscribed` (Block 30). centrifuge-js keeps the counters on the object.
- **D4 Version.** No bump: methods and types are additive. `Unreleased` entries under "Added": `Client::statistics`, `Machine::statistics`, `Statistics`; `Subscription::delta_statistics`, `Machine::delta_statistics`, `DeltaStatistics`.

Value clause: As an operator of a fleet, I want each client's connection and traffic counters, and what delta compression saves per subscription, so that support tickets and telemetry show how a client behaves without extra logging.

**Structural acceptance:**
- @E unit tests on the core, one counter per assertion after scripted inputs:
  - a failed attempt of each ending in D1;
  - an attempt ended by `disconnect()` or `reconnect()`, not counted;
  - a reconnect;
  - a ping and its pong;
  - a frame of an abandoned transport, not counted;
  - an undecodable frame, counting bytes and no message;
  - messages after a disconnect push in one frame, counted in;
  - a frame whose transport ends before it is reported written, counted nowhere and its entry dropped;
  - a batch frame of 3 commands, counting 3 out and its bytes once;
  - an emulated transport's connect command, counted when it opens;
  - an HTTP 413 refusal and the commands sent again alone;
  - publications taken, withdrawn (not counted), emitted with no stream, left in a dropped stream, and server-side.
- Delta unit tests:
  - recovered publications counted;
  - a failed delta not counted;
  - JSON and Protobuf sizes;
  - counters kept across a resubscribe and a delta error;
  - a removed subscription reporting zeros;
  - a negative ratio.
- Adapter unit tests:
  - `connected_since` is `None` before connect, between the times before and after `connected`, `None` after `disconnect()`, and a new time after a reconnect;
  - a client whose runtime shut down, with the core lock poisoned too, keeps its counters.
- The fake server counts the bytes and messages it sends (at its writer) and receives (at its reader).
- The Block 14 @P gates and the chaos adapter runner pass. Each method has a doc example; `compression_ratio` carries an `#[expect(clippy::cast_precision_loss)]` with its reason.

```gherkin
Feature: Statistics

  Scenario: A new client's statistics are zero
    Given a new client
    Then its statistics report state "disconnected", no transport, no connected-since time and every counter 0

  @E
  Scenario: The core counts attempts, connects and publications taken
    Given a client core configured with websocket and http_stream endpoints
    When connect is called and the websocket attempt fails
    And the http_stream attempt connects
    And the server pushes 2 publications on a subscribed channel, which the application takes
    Then the core's statistics report 1 connect, 1 failed attempt and 2 publications delivered
    And they report transport "http_stream" and no connected-since time

  @E
  Scenario: The core counts messages as read and as written
    Given a client core connected over websocket, whose server expects pongs
    When the core reads a frame holding a ping and a publication push
    And it sends the pong, which the transport has not yet reported written
    Then the core's statistics report 2 more messages in, the frame's bytes in, and nothing more out
    And when the transport reports the pong written, they report 1 more message out and the pong's bytes out

  @D
  Scenario: Statistics count a session's traffic as the fake server sees it
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes 3 publications on channel "news"
    And the subscription receives them
    And the client publishes {"text":"hi"} to channel "news"
    Then the client's statistics report 1 connect, 0 failed attempts, 3 publications delivered, 1 subscription and transport "websocket"
    And they report as many bytes and messages in as the fake server sent, and as many bytes and commands out as it received

  @D
  Scenario: Statistics count failed attempts
    Given the fake server answers the first 2 connect commands with a temporary error
    And a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    Then when the application reads the client's stream, it yields "connecting", "error", "error", "connected"
    And the client's statistics report 1 connect and 2 failed attempts

  @D
  Scenario: Statistics count across reconnects
    Given a client connected to the fake server
    When the fake server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the client's statistics report 2 connects and 0 failed attempts
    And they report a connected-since time after the fake server's disconnect

  @D
  Scenario: Only publications taken count as delivered
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When unsubscribe is called on the subscription
    Then the subscription's stream yields "unsubscribed" and no publication
    And the client's statistics report 1 publication delivered

  @D
  Scenario: Delta statistics count full and delta payloads
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes 3 publications of 1 KiB that differ in a few bytes to "news"
    And the subscription receives them
    Then its delta statistics report 3 publications, 1 full payload and 2 delta payloads
    And they report as bytes received the full payload's size plus the 2 deltas' sizes, and as bytes decoded the 3 payloads' sizes
    And they report a compression ratio above 0.5

  @D
  Scenario Outline: Delta statistics stay zero unless delta is on
    Given a client subscribed to channel "news" on the fake server <option>
    When the fake server pushes 3 publications of 1 KiB that differ in a few bytes to "news"
    And the subscription receives them
    Then its delta statistics report every counter 0 and a compression ratio of 0

    Examples:
      | option                                                    |
      | with the fossil delta option, which did not turn delta on |
      | without the delta option                                  |
```

**Exceptions introduced:** none.

---

## Block 32 — Runtime HTTP headers and subscription data

**Goal:** an app behind a gateway that checks an expiring real header on each request replaces it without rebuilding the client and keeps being admitted, and an app changes what a subscription's subscribe requests carry without recreating the subscription (parity items 14 and 27; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A for both).

**Dependencies:** Blocks 8 (attempts, emulated transports, emulation requests), 9 (subscription `data` and `get_data`, resubscribes), 14 (@P gates), 17 (`http_headers`, its validation, `set_data`, `set_emulated_headers`, "from the next attempt"), 20 (changelog), 21 (headers never logged), 23 (`Proxy-Authorization` refused, `set_proxy` from the next attempt), 28 (a closed client ignores setters), 31 (latest format).

**Out of scope:**
- an async header callback before each attempt or request (the analysis's design B for item 14);
- adding to the headers instead of replacing them;
- reopening the current websocket or stream request, or resending a subscribe, to apply a new value: an app calls `reconnect()`, or `unsubscribe()` then `subscribe()`;
- what an emulation request answered 401 or 403 does (Block 8 behaviour, unchanged);
- clearing a subscription's data back to none (`set_data` sets bytes, as `Client::set_data` does).

This block reverses Block 17's exclusion of replacing the HTTP headers of a built client: Block 0's "Reversed exclusions" gains its row, and Block 17's out-of-scope line says so.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 `Client::set_http_headers`.** `pub fn set_http_headers<I, K, V>(&self, headers: I) -> Result<(), ClientError>`, taking what `ClientBuilder::http_headers` takes, replaces the whole set of real HTTP headers (Block 17): a header missing from the new set is no longer sent, and a name given twice keeps its later value. The same rules as `build()` validate it (Block 17; Block 23 D7). A name or value that is not valid HTTP, or a header the transport sets, `Proxy-Authorization` included, is an `ErrorKind::Configuration` error naming the header, never its value. The set is then left as it was, with none of the call's headers applied. It sends nothing and opens nothing. Adapter only, as Block 17 keeps real headers out of the core: the set moves from the driver into the state the client handle and the driver share. A closed client (Block 28) still reports an invalid set and ignores a valid one, as it ignores the other setters.
- **D2 What the new set applies to.** Each HTTP request the client starts after the call returns carries the new set. That means the websocket upgrade and the stream request of every later attempt, and each emulation request of the open http_stream or sse transport. The open connection is kept: its upgrade or stream request is not sent again, and a request already started keeps the headers it started with. Reason:
  - HTTP clients set headers per request (reqwest `RequestBuilder::headers`, hyper `Request`), and per-request credentials take the current value on each request over an existing connection (tonic interceptors, aws-sdk-rust's per-request signing); connection settings apply from the next connection (`set_proxy`, Block 23).
  - A gateway checks each emulation request, which is why Block 17 sends them the headers. Applied only from the next attempt, an expired header would keep reaching it until the client reconnects.
  - The official SDK applies the set from the next connect attempt (centrifuge-rust `docs/spec.md` §4.3, §5.1, commit 88d6edf). Over websocket the result is the same, and on the emulated transports this rule goes further.
- **D3 `Subscription::set_data`.** `pub fn set_data(&self, data: impl Into<Vec<u8>>)` replaces the subscription's `data` (Block 9) in every subscribe command built after the call: the next `subscribe()` and every resubscribe.
  - The data is raw JSON with JSON and any bytes with Protobuf, unchecked, as `SubscriptionBuilder::data` is.
  - It sends nothing now, and a subscribe command already sent keeps its data.
  - A `get_data` callback still replaces it on every attempt (centrifuge-js 63ed73e `subscription.ts:382-386`, `_getDataAndSubscribe`). Unlike `SubscriptionBuilder::data` and `SubscriptionOptions::data`, where the last option set decides, `set_data` never turns the callback off.
  - Sans-IO: a new `Input::SetSubscriptionData(SubscriptionId, Vec<u8>)`, additive (`Input` is `#[non_exhaustive]`), as `Input::SetData` is for the client (Block 17). An id not in the registry (a removed subscription) is ignored, and so is the input on a closed client (Block 28).
  - It returns nothing, as `Client::set_data` does. Ferrifuge never validates data (Block 9), while the official SDK's `set_data` returns a `Result` because it checks JSON (centrifuge-rust `docs/api.md`).
- **D4 Logging and Debug.** Neither setter logs, and no value reaches a record (Block 21, D5). The `Debug` of the shared header set shows names only, as `ClientBuilder`'s does, since `Batch` derives `Debug` over the shared state. `Input::SetSubscriptionData`'s `Debug` shows the id and the data as its length, as `Input::SetData`'s does.
- **D5 Version.** No bump: additive. `Unreleased` entries under "Added": `Client::set_http_headers`, also applied to the emulation requests of the open transport; `Subscription::set_data`; `Input::SetSubscriptionData`.

Value clause: As an app developer behind a gateway that checks an expiring header on each request, or whose subscriptions carry app data that changes, I want to replace the headers and a subscription's data on a running client, so that the gateway keeps admitting the client and later subscribes carry the new data, without rebuilding the client or recreating the subscription.

**Structural acceptance:**
- Adapter unit tests:
  - a refused header (a transport header, `Proxy-Authorization`, an invalid name, an invalid value), alone or with valid ones in one call, leaves the set as it was, and the error names the header, not the value;
  - a header missing from the new set is not sent, and a name given twice keeps the later value;
  - the next attempt's upgrade request carries the new set;
  - the `Debug` of `Client` and of `Batch`, for a client whose set holds "Authorization: h-secret", contains no "h-secret".
- Core unit tests:
  - `Input::SetSubscriptionData` while subscribed adds no action;
  - the subscribe after `unsubscribe()` then `subscribe()`, and the resubscribe after a reconnect, both carry the new data;
  - with a data callback, the fetch still happens and its result is sent;
  - an id not in the registry is ignored;
  - its `Debug` shows the id and the length.
- Documentation:
  - `ClientBuilder::http_headers` names `Proxy-Authorization` among the refused headers;
  - `set_http_headers` says what D2 applies to;
  - the README's setter list names both setters.
- The Block 14 @P gates pass: reading the set costs one lock per request.
- New steps:
  - "a client with HTTP header ..., configured with only the server's ... endpoint, connected" waits for `connected` and records the client id.
  - "set_http_headers is called with ..." keeps its result for the next Then step; the success scenarios fail at their Then step when it fails.
  - "... right after the HTTP proxy relayed an emulation request" calls the setter just after a pong is relayed, so no request with the old set is in flight (the server pings every 2 s).

```gherkin
Feature: Runtime HTTP headers and subscription data

  @B
  Scenario Outline: set_http_headers applies from the next connection attempt
    Given a connect proxy recording requests
    And a server with http_stream and sse enabled
    And a client with HTTP header "X-Device-Real: r1", configured with only the server's <transport> endpoint, connected
    When set_http_headers is called with "X-Device-Real: r2"
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy received header "X-Device-Real: r1" then "X-Device-Real: r2"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @B
  Scenario Outline: set_http_headers applies to the emulation requests of the open transport
    Given a client with HTTP header "X-Device-Real: r1" connected over transport "<transport>" through the HTTP proxy
    When set_http_headers is called with "X-Device-Real: r2" right after the HTTP proxy relayed an emulation request
    And the HTTP proxy relays another emulation request
    Then the HTTP proxy relayed one stream request, carrying header "X-Device-Real: r1"
    And every emulation request it relayed after the call carried header "X-Device-Real: r2"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  Scenario Outline: set_http_headers refuses a header the transport sets
    Given a new client with HTTP header "X-Device-Real: r1"
    When set_http_headers is called with "<header>"
    Then it fails with a configuration error naming "<name>" and not "<value>"

    Examples:
      | header                        | name                | value    |
      | Sec-WebSocket-Key: k1         | Sec-WebSocket-Key   | k1       |
      | Proxy-Authorization: p-secret | Proxy-Authorization | p-secret |

  @B
  Scenario: Subscription set_data applies from the next resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with subscription data {"room":1}
    When set_data is called on the subscription with {"room":2}
    And the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"room":1} then {"room":2}

  @E
  Scenario: A subscription data callback takes precedence over set_data
    Given a subscription whose data callback returns {"n":1}
    When set_data is called on the subscription with {"n":2}
    And the subscription is subscribed on a connected client
    Then the subscribe command carries data {"n":1}
```

**Exceptions introduced:** none.

---

## Block 33 — Closing a client

**Goal:** an app stops a client for good and awaits the end (the transport closed, the driver stopped, every stream ended after the events already queued), also from a shared reference such as Tauri's managed state, and subscription handles that outlive their client learn that it is closed instead of staying `subscribing` forever (parity item 16; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A's behaviour with design B's `&self` receiver, without its `Closed` state).

**Dependencies:** Blocks 0 (no `closed` state), 1 (states, `disconnect()`), 8 (closing a websocket bounded by `Config::timeout`, emulated transports), 9 (subscription states, the registry, `SubscriptionError::ClientClosed`), 12 (server-side subscriptions), 13 (calls, codes 3, 7 and 11), 15 (streams, events taken, the read-ahead), 28 (the closed flag, `Shared::shut_down`, the drop guard, `Input::DriverDropped`, disconnected codes 2 and 4), 30 (`ready()` on a closed client), 31 (counters kept, attempts ended by closing not counted).

**Out of scope:**
- a `closed` client state (Block 0: the spec marks it optional; `State` is exhaustive, so a variant would break every `match` on it, the analysis's design B): a closed client reports `disconnected`, and `Client::is_closed()` tells it apart (D1);
- `Client: Clone`, `downgrade` and `WeakClient` (Block 41, where `close(&self)` ends every clone);
- closing from an event handler, and its deadlock detection (Block 41);
- a timeout parameter on `close()`: closing is bounded by `Config::timeout` (D1), and a caller wanting a shorter bound wraps the future, as for `ready()` (Block 30);
- a status code in the websocket close frame (Block 8, unchanged).

This block reverses no exclusion.

**Decisions** (reviewer, 2026-11-07, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Method.** `pub fn close(&self) -> impl Future<Output = ()> + Send + use<>`, as centrifuge-rust (`docs/spec.md` §4.3, `docs/api.md`, commit 88d6edf) and as `ready()` (Block 30 D1). It does not consume the client: an app holding it behind a shared reference (Tauri managed state, an `Arc<Client>`, Block 41's clones) must be able to close it, and Block 41's `Clone` would undo what a consuming `close` expresses (async-nats `Client::drain(&self)`, quinn `Connection::close(&self)`, rumqttc `AsyncClient::disconnect(&self)`).
  - Closing starts when the method returns, not when the future is first polled: the closed flag is checked, the machine fed `Input::Close` (D2) and the flag set in one section under the core lock, as `Shared::shut_down` does (Block 28), so an API call either runs before and is closed with the rest, or after and sees the client closed. Dropping the future, or never awaiting it, still closes the client (centrifuge-rust §4.3, "Cancel safety"). The future is `#[must_use]`, as every future is; an app closes without waiting by dropping the client or spawning the future.
  - The future resolves once the driver's task has ended, however it ends. The driver awaits the closing of its transport instead of spawning it (`Transport::close`: a websocket's close handshake bounded by `Config::timeout`, then aborted, Block 8; an emulated transport's stream request dropped and its emulation requests ended, each within `Config::timeout`), then stops; its drop guard (Block 28 D3) ends every stream and releases the future through a `tokio::sync` primitive, so any executor can await it. A runtime shutting down during the close releases it too.
  - It does not wait for events to be taken: every stream yields the events queued before the close, then ends (Block 28 D3), so a task awaiting `close()` while holding an unread stream does not wait for it (unlike `ready()`, Block 30 D4).
  - A second `close()`, or one on a client already closed (its runtime shut down or its driver panicked), changes nothing and resolves once the driver's task has ended, at once when it has. On a runtime that is not driven (Block 28 D2) it waits until the runtime runs.
  - Awaiting it inside the client's own token, data or getState callback does not deadlock: the driver drops or aborts a running callback when it stops.
  - `pub fn is_closed(&self) -> bool`, `#[must_use]`: whether the client is closed, by `close()`, its runtime shut down or its driver panicked; one atomic load, as tokio's `mpsc::Sender::is_closed`. A closed client reports `disconnected` from `state()`; `connect()`, `reconnect()`, `disconnect()` and the setters do nothing; `new_subscription` returns `SubscriptionError::ClientClosed`; `events()` gives a stream that ends at once, as after Block 28's shutdown.
- **D2 What closing does.** A new `Input::Close` (additive: `Input` is `#[non_exhaustive]`), in one step:
  - every call not settled, client-level or subscription-level, whether queued, waiting for the connection or its subscription, or sent, fails with a new error code 4 "client closed" (`codes::error::CLIENT_CLOSED`; centrifuge-js 63ed73e `codes.ts` `errorCodes.clientClosed`, defined there and unused, centrifuge-js having no `close()`), and not with 3, 7 or 11;
  - then every client-side subscription not `unsubscribed` emits `Unsubscribed` with a new client-side code 2 "client closed" (`codes::unsubscribed::CLIENT_CLOSED`; `codes.ts` `unsubscribedCodes.clientClosed`; centrifuge-rust §6.5) and stays in the registry, as after `unsubscribe()`; every server-side subscription emits its `unsubscribed` with the same code and reason and leaves the registry, as on an unsubscribe push (centrifuge-rust §8); then the client emits `Disconnected` with code 0 "disconnect called", unless it was `disconnected` already (centrifuge-rust §6.1: every `unsubscribed(2)` before `disconnected`);
  - no unsubscribe command is sent: the connection closes (`Action::CloseTransport`), and timers and running callbacks are cancelled as `disconnect()` cancels them; there is no new `Action`;
  - the attempt it ends is not a failed one (Block 31 D1).

  An owner of the machine feeds it nothing after `Input::Close`, as after `Input::DriverDropped` (Block 28); the machine does not check.
- **D3 Waiters and leftover handles.** In the same locked section the adapter fails every waiting `ready()` future, of the client and of its subscriptions, with code 4, before Block 30 D4 judges the new states (which would give 3 or 7). After closing, a leftover `Subscription` reports `unsubscribed`; `subscribe()`, `unsubscribe()` and `set_data()` on it do nothing (the closed flag, Block 28); its calls and `ready()` fail at once with code 4; its streams end after the events queued.
- **D4 Drop and the driver's end.** Dropping the client closes it as `close()` does, without waiting: `Shared::shut_down` feeds `Input::Close` instead of `Input::Disconnect`. A client whose runtime shut down or whose driver panicked is closed too, and gets the same codes: `Input::DriverDropped` does what `Input::Close` does, its `Disconnected` keeping code 4 "runtime shut down" or 2 "exception during message handling" (Block 28 D3, D4). Its subscriptions therefore emit `unsubscribed` code 2 first, and its calls, pending or later, and its `ready()` futures fail with code 4 instead of 3. When the poisoned core lock leaves the machine unfed (Block 28 D4), no event is emitted, and the guard still fails calls and waiters with code 4. Code 3 "disconnected" stays for a client that entered `disconnected` and can connect again. Reason: an error code tells the caller whether retrying can succeed, as tokio's channels return one closed error whatever closed them; the cause is reported once on the client's stream, as the `Disconnected` code; and `SubscriptionError::ClientClosed` already calls every one of these cases closed (Block 28 D6).
- **D5 Statistics.** The closed client's handle stays (D1), so `Client::statistics()` stays readable: its last counters (Block 31 D2), `state` `disconnected`, `transport` and `connected_since` `None`, `subscriptions` unchanged (D2 keeps client-side subscriptions registered), `server_subscriptions` 0. A leftover subscription's `delta_statistics()` stays readable. Block 31 is otherwise unchanged.
- **D6 Version.** 0.19.0, set in this block's PR. Dropping a client changes what its leftover subscription handles report (`unsubscribed` code 2 instead of `subscribing`), and a closed client's calls and `ready()` fail with code 4 instead of 3, also after a runtime shutdown, which 0.18.0 released; an app matching codes sees both. Behaviour changes an app can see took a minor version before (Block 23 D8, Block 29 D5); blocks that only add API did not (Block 27 D5). `Unreleased` entries: "Added" `Client::close`, `Client::is_closed`, `Input::Close`, unsubscribed code 2 and error code 4 "client closed" (`codes::unsubscribed::CLIENT_CLOSED`, `codes::error::CLIENT_CLOSED`); "Changed" dropping a client, or its runtime shutting down, unsubscribes its subscriptions with code 2 before `disconnected`, and a closed client's calls and `ready()` futures, those of its leftover subscriptions included, fail with code 4 "client closed" instead of 3 "disconnected", as `Input::DriverDropped` now does too.

This block amends Block 0's `closed` state row, Block 28 D3, its structural acceptance and its scenario "The client ends when its runtime shuts down" (the call fails with "client closed"; the scenario leaves `IMPLEMENTED` in `tests/bdd.rs` until this block's implementation), Block 30 D1, D2, D5 and its structural acceptance (code 4 for a closed client), and Block 31's references to closing.

Value clause: As an app developer, I want to close a client and await its end, also from a shared reference, and leftover subscription handles to learn that the client is closed, so that shutdown is orderly and no handle waits forever.

**Structural acceptance:**
- Core unit tests:
  - for `Input::Close`: client-side `unsubscribed` code 2, server-side `unsubscribed` code 2, then `disconnected` code 0, in the order of the emit actions; no unsubscribe command; client-level and subscription-level calls, queued, waiting and sent, failing with code 4; a disconnected client emitting no `Disconnected` while its subscribing subscriptions emit `unsubscribed` code 2; client-side subscriptions kept in the registry and server-side ones removed; the attempt ended not counted as failed;
  - `Input::DriverDropped` doing the same with its codes 4 and 2.
- Adapter unit tests:
  - on a separate runtime given with `ClientBuilder::runtime`, when `close()` resolves no task of the client is left there (`RuntimeMetrics::num_alive_tasks`, polled for at most 100 ms, as the driver's task is released just after its guard runs), with a server answering the close handshake late, so that a closing left detached would still be alive;
  - dropping the future at once still closes (state, streams, calls); a second `close()` resolves;
  - `close()` resolves while the client's stream holds events not taken, which the stream then yields before ending;
  - a call racing `close()` from another thread fails with code 4 whichever runs first;
  - waiting client and subscription `ready()` futures fail with code 4;
  - a token callback awaiting `close()` on its own client ends;
  - a runtime shut down during `close()` releases it;
  - a dropped client's subscription reports `unsubscribed` at once;
  - `is_closed()` after `close()` and after a runtime shutdown.
- Steps:
  - "close is awaited" awaits the future, bounded by `Config::timeout` plus tolerance, and records when it resolved;
  - "close resolves at once" uses the `AT_ONCE` bound of the ready steps;
  - "the client is dropped" takes the client out of the world;
  - "the fake server received no unsubscribe command" is checked after the server saw the connection end;
  - the call steps' error regex gains "client closed".
- The Block 14 @P gates and the chaos adapter runner pass.

```gherkin
Feature: Closing a client

  @D
  Scenario: close waits until the connection is closed and the stream ends
    Given a client connected to the fake server
    When close is awaited
    Then close resolves
    And the fake server sees the connection closed within the configured timeout plus tolerance
    And the client emits "disconnected" with code 0 and reason "disconnect called"
    And the client's stream ends
    And the client is closed

  @D
  Scenario: close unsubscribes every subscription with code 2 and sends no unsubscribe command
    Given a client subscribed to channel "news" on the fake server
    When close is awaited
    Then the subscription emits "unsubscribed" with code 2 and reason "client closed"
    And the subscription's stream ends
    And the fake server sees the connection closed within the configured timeout plus tolerance
    And the fake server received no unsubscribe command

  @D
  Scenario: close ends a server-side subscription before the client's "disconnected"
    Given the fake server grants a server-side subscription to channel "news" at connect
    And a client connected to the fake server
    And the client emits a server-side "subscribed" event for channel "news"
    When close is awaited
    Then the client emits a server-side "unsubscribed" event for channel "news" with code 2
    And then the client emits "disconnected" with code 0
    And the client's stream ends

  @D
  Scenario: A call pending at close fails with "client closed"
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When close is awaited
    Then the call fails with a "client closed" error before the configured timeout

  @D
  Scenario: Dropping the client unsubscribes its subscriptions with code 2
    Given a client subscribed to channel "news" on the fake server
    When the client is dropped
    Then the subscription state is "unsubscribed"
    And the subscription emits "unsubscribed" with code 2 and reason "client closed"
    And the subscription's stream ends
    And the subscription's history call fails at once with a "client closed" error

  Scenario: close on a disconnected client resolves at once
    Given a new client
    When close is awaited
    Then close resolves at once
    And the client's stream ends with no "disconnected" event

  Scenario: A closed client stays closed
    Given a new client
    When close is awaited
    And connect is called
    Then the client state is "disconnected"
    And the client is closed
    And an RPC call fails at once with a "client closed" error
    And creating a subscription fails because the client is closed
```

**Exceptions introduced:** none. centrifuge-js 63ed73e has no `close()`, and the absent `closed` state is Block 0's decision.

---

## Block 34 — Table of codes

**Goal:** an operator who reads a code in a log, an event or an error finds in one table what it means and what the client does next, in the crate documentation and in the README, kept complete by a test (parity item 23, centrifuge-js #264; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A).

**Dependencies:** Blocks 1–5, 8–13, 15, 17–19, 25, 28, 29 and 33 (where each code and each reaction is decided), 20 (README, changelog, the docs-rs job), 21 (logs carry state, code and reason).

**Out of scope:**
- changing any code, reason or behaviour: this block documents them; a row found wrong against the code is fixed in the row, a behaviour found wrong goes to its block;
- public constants for server codes and an `is_reconnect_code` function (centrifuge-rust `docs/spec.md` §10): new API, not item 23;
- the server's error codes one by one (Centrifugo's documentation lists them);
- a separate `docs/codes.md` (the analysis's design B).

This block reverses no exclusion.

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Tables.** The `codes` module documentation (centrifuge-rust `docs/spec.md` §6.5, §10, commit 88d6edf) opens by saying how to read a code:
  - a code means something only with the event or error that carries it: 1 is "transport closed" in `connecting` and "timeout" in an error;
  - the same field also carries server codes, in their own ranges (the last table);
  - the client's logs show the state, code and reason (Block 21);
  - values follow centrifuge-js 63ed73e `src/codes.ts`; Ferrifuge adds `connecting` 5 and 6, `subscribing` 2, `unsubscribed` 3 and 4, and error 15; error 14 is centrifuge-js's shared-poll code, unused until Block 47.

  Then comes one table per kind, in module order: `connecting`, `disconnected`, `subscribing`, `unsubscribed` (also carried by server-side subscriptions), `error` (`ClientError`, `CallError`). Each table has a heading naming the event or error that carries its codes, and the columns Code | Constant | Reason or message | Cause | Then:
  - Reason or message: the text carried, as a string literal when it is fixed (`` `"transport closed"` ``), otherwise what it is made of;
  - Cause: from the constant's documentation, with the block that decides it (blocks of the repository's `CLAUDE.md`);
  - Then, for an event kind: whether and when the client tries again by itself. The options are: at once (`connecting` 0; 6, after the jitter window when it was connected); after the reconnect backoff; when the client connects (`subscribing` 1); after the resubscribe backoff (`subscribing` 2); never, until `connect()` or `subscribe()`;
  - Then, for `error`: what the client does (it keeps retrying, or the call fails), and whether a retry can succeed. It never can for 4 "client closed", only after `connect()` for 3, and never with the same data for 10.

  Each row stays on one line, with identifiers in backticks.

  A last table, "Server codes" (Codes | Carried by | Then), has these rows:
  - disconnect 3500–3999 and 4500–4999: `disconnected`, stays disconnected until `connect()` (Block 3);
  - every other disconnect code (3000–3499, 4000–4499, from 5000): `connecting`, reconnects after the backoff (Block 3);
  - 3008: as above; before the first server ping on that connection, the backoff keeps growing (Block 15);
  - 3014: tokens dropped and stream positions made unmatchable, then reconnects (Block 11);
  - websocket close 1009: `disconnected` code 3 (Block 3); other close codes below 3000: `connecting` code 1;
  - unsubscribe below 2500: `unsubscribed`, stays unsubscribed;
  - unsubscribe from 2500: `subscribing`, subscribes again at once (Block 10); 2502 drops the token and the position first (Block 11); any code ends a server-side subscription (Block 12);
  - connect error that is temporary, below 100, or 109: an `error` event, then `connecting`, reconnecting after the backoff; for 109 the next attempt gets a new token, and without a token callback the client reports error 12 and then `disconnected` code 1 (Blocks 1, 5, 17);
  - any other connect error: `disconnected` with its code;
  - refresh error that is temporary or below 100: an `error` event, retried after 5 to 10 s; any other: `disconnected` with its code (Block 5);
  - subscribe errors:
    - 109: a new token, then resubscribes; without a token callback, error 12 and then `unsubscribed` code 1 (Block 10);
    - 112 with getState: the state is reloaded and the subscription resubscribes after the backoff, with no `error` event (Block 11);
    - temporary or below 100: an `error` event, then resubscribes after the backoff;
    - any other: `unsubscribed` with its code (Block 9);
  - sub_refresh error that is temporary or below 100: an `error` event, retried after 10 to 20 s; any other: `unsubscribed` with its code (Block 10);
  - call error: a `CallError` with the server's code, message and `temporary`; the client never retries a call (Block 13).
- **D2 README.** A "Codes" section after "States and events" mirrors every table row for row. The client-side code table in "Calls" is replaced by a pointer to it, as it lacks code 4. The "States and events" sentence on `ferrifuge::codes` points to it too.
- **D3 Test.** Steps read `src/codes.rs` and `README.md` from the repository, as Block 20's do, with no new dependency:
  - **Constants.** Inside `pub mod <kind> {`, each line `pub const <NAME>: u32 = <value>;` is one constant. Any other line holding `pub const` or `pub static`, or a constant outside a kind, fails the step.
  - **Rows.** A row is a line starting with `|`, in the module documentation (`//!`) or in the README's "Codes" section, other than a delimiter row.
  - **Code rows.** A code row's Constant cell is exactly `` [`kind::NAME`] `` in the documentation (an intra-doc link) or `` `codes::kind::NAME` `` in the README. Its Code cell is the value.
  - **Checks:**
    - each constant has exactly one code row, with its value;
    - every code row names a constant;
    - the code rows of one table name constants of one kind;
    - the README's rows equal the documentation's, in order and cell for cell, once each Constant cell is normalised to `kind::NAME`;
    - every string literal a reason cell quotes occurs as that literal in a `.rs` file under `src/` other than `codes.rs`.
- **D4 Version.** No bump: documentation and a test. `Unreleased` entry under "Added": a table of codes, with the client's reaction to each and the server codes it reacts to, in the `codes` documentation and the README.

Value clause: As an operator reading a code in a production log, I want one table saying what each code means and what the client does next, so that I can tell a transient reconnect from a terminal stop without reading the source.

**Structural acceptance:**
- `cargo doc` in `lint` and in the docs-rs job, with warnings denied, resolves every Constant link of the tables;
- the steps parse as D3;
- each scenario is seen failing once on a mutation (a constant without a row, a wrong value, a README cell changed, a misspelt reason), recorded in the PR.

```gherkin
Feature: Table of codes

  Scenario: Every code constant has one row with its value
    Given the crate's codes module
    Then the codes documentation has exactly one row for each code constant, giving its value

  Scenario: The table names no code the crate lacks
    Given the crate's codes module
    Then every constant a row of the codes documentation or the README names is a code constant of the crate
    And the rows of each table name constants of one kind

  Scenario: The README's table is the documentation's
    Given the crate's codes module and its README
    Then the README's rows are the rows of the codes documentation, in the same order and cell for cell

  Scenario: Every quoted reason is one the client sends
    Given the crate's codes module
    Then every reason a row quotes occurs as a string literal in the crate's source outside the codes module
```

**Exceptions introduced:** none.

---

## Block 35 — Tag filters, channel compaction, publication channel, recovered publications

**Goal:** a subscriber to a broad channel receives only the publications whose tags it asks for, filtered by the server; an app with many subscriptions and short messages lets a Centrifugo PRO server send numeric channel ids instead of names; a publication of a wildcard subscription says its channel; and the `subscribed` event says whether recovered publications follow (parity items 6, 7, 28 and 37; "Planned after Block 19", owner decision 2026-11-06, decided by the reviewer by state of the art: design A for each).

**Dependencies:** Blocks 0 (the compaction exclusion), 7 (Protobuf), 9 (subscription options, its tag-filter exclusion, `Publication`), 11 (`Subscribed`, recovery, positions), 12 (server-side subscriptions), 13 (join and leave pushes), 14 (O(1) dispatch by channel), 19 (delta, its tag-filter line), 21 (redacting `Debug`), 32 (`set_data` pattern), 34 (latest, format).

**Out of scope:**
- `Publication.time` (`client.proto` field 9: "not sent to clients by the current server");
- map and shared-poll subscriptions (Blocks 45, 47), whose tag filters and keys come with them;
- a string filter language (the analysis's design C for item 6);
- channel compaction for server-side subscriptions: `ConnectRequest.subs` uses "only recover, epoch, offset and delta" (`client.proto`), so a client cannot ask for it; ids in `ConnectResult.subs` are ignored (centrifuge-js 63ed73e routes server-side pushes by channel only, `centrifuge.ts:2245-2262`, `:2690-2696`);
- requesting compaction on every subscribe (centrifuge-js 63ed73e `subscription.ts:977`, the analysis's design B);
- tag filters set by the server (Centrifugo PRO "server tags filter"): nothing for the client;
- checking a filter beyond D1 (operators, numeric values): the server's, answered as Block 9's permanent subscribe error.

This block reverses two exclusions: Block 0's "channel compaction (Centrifugo PRO; owner decision, 2026-11-04): the client never offers it" and Block 9's "tag filters (`SubscribeRequest.tf`): not among the spec's subscription options, not in v1, the client never sends one" (owner decision 2026-11-06, items 6 and 7); Block 0's "Reversed exclusions" gains both rows, and both lines say so. Block 19's out-of-scope line on a tag filter with delta points to D1. Block 9's delta clause, reversed by Block 19 and not marked then, is marked now, with its row.

**Sub-increments** (one branch and PR each, in this order):

| Id | Value clause | Scenarios |
|---|---|---|
| 35a | As an app developer subscribing to a broad channel, I want the server to send me only the publications whose tags match my filter, so that my clients spend no bandwidth on the rest | "A tag filter delivers only matching publications", "The tag filter is sent on subscribe and on resubscribe", "A tag filter set on a subscription is sent from the next subscribe", "A tag filter with delta is refused", "A tag filter the server would reject is refused" |
| 35b | As an app developer with many subscriptions on Centrifugo PRO, I want pushes to carry numeric channel ids, so that short messages are not dominated by channel names | "Channel compaction is requested on every subscribe", "Pushes with a numeric channel id reach their subscription", "A push with an unknown channel id is dropped", "Numeric channel ids are forgotten when the connection ends", "Channel compaction on a server without it changes nothing" |
| 35c | As an app developer, I want a publication's own channel and whether a subscribe brought recovered publications, so that wildcard subscriptions and recovery metrics need no guessing | "A publication of a wildcard subscription carries its channel", "A publication of another channel does not move the stream position", "The subscribed event says whether recovered publications follow", "A server-side subscribed event says whether publications follow" |

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Tag filter (35a).**
  - **Type.** `TagFilter` is an opaque struct with a private representation of `client.proto` `FilterNode`: Ferrifuge's own type, not prost's. It derives `Clone`, `PartialEq`, `Eq` and `Hash`.
  - **Leaves.** Associated functions taking their operands as `impl Into<String>`, `vals` as an `IntoIterator`:
    - `eq` and `neq`;
    - `is_in` and `not_in` (`in` is a keyword; the names follow sea-query);
    - `exists` and `not_exists`;
    - `starts_with`, `ends_with` and `contains` (the names follow std's `str`);
    - `lt`, `lte`, `gt` and `gte`, whose value is decimal text, as `client.proto` compares.

    Each has a `#[doc(alias)]` naming its wire operator (`in`, `nin`, `ex`, `nex`, `sw`, `ew`, `ct`).
  - **Combinators.** `a.and(b)` and `a.or(b)` merge into one node when an operand already has that operation. `!a` comes from `std::ops::Not` (Rust API Guidelines C-OVERLOAD). A list is combined with `Iterator::reduce(TagFilter::and)`, which gives `None` when the list is empty. An empty `and` or `or`, a `not` with other than one node, and a leaf without its comparison therefore cannot be built (Q1, C-VALIDATE: static checks first).
  - **Checked when attached.** `build()` and `set_tag_filter` check what no signature can: a comparison other than `exists` and `not_exists` with an empty key, and an `is_in` or `not_in` with no value (`client.proto`: "Required"). Centrifugo rejects either with ErrorBadRequest (centrifugal.dev, "Channel publication filtering"), which would unsubscribe the subscription for good. Either fails with `SubscriptionError::InvalidTagFilter { reason }`, the reason naming the key, and nothing is sent.
  - **API.** `SubscriptionBuilder::tag_filter(TagFilter)` and `SubscriptionOptions::tag_filter: Option<TagFilter>`. `Subscription::set_tag_filter(Option<TagFilter>) -> Result<(), SubscriptionError>` applies from the next subscribe command, as `set_data` does (Block 32; centrifuge-js 63ed73e `subscription.ts:367-380` `setTagsFilter`), sending nothing now. `None` removes the filter.
  - **Wire.** Sent as `SubscribeRequest.tf` on every subscribe and resubscribe, in JSON and Protobuf.
  - **Delta.** A filter together with the delta option fails `build()`, and fails `set_tag_filter(Some(_))` on a delta subscription, with `SubscriptionError::TagFilterWithDelta`, changing nothing. centrifuge-js throws "cannot use delta and tagsFilter together" (`:368-370`, `:1415-1417`); `client.proto`: a filter "disables delta compression".
  - **Debug.** Shows keys and operators, never values (Block 21, D5: tags by name only).
  - **Verified** (centrifugal.dev, read 2026-11-08): the channel option `allow_tags_filter` (default false, "Available since Centrifugo v6.4.0"), so v6.10.1 honours `tf` and the delivery scenario is @A. Recovery returns only matching publications.
  - `VERIFY:` that `CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_TAGS_FILTER=true` allows it on channels without a namespace (the documentation speaks of namespaces), by a raw client run as Block 19 did. If it does not, the @A server gains a namespace with `allow_tags_filter`; the scenario stays @A.
  - Verified (Centrifugo v6.10.1, the 35a @A scenario, 2026-11-08): with `CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_TAGS_FILTER=true` a channel without a namespace honours `tf`, over JSON and Protobuf: a filter `ticker in BTC, ETH` delivers the BTC and ETH publications and not the XRP one.
- **D2 Channel compaction (35b).**
  - **Option.** `SubscriptionBuilder::channel_compaction(bool)` and `SubscriptionOptions::channel_compaction`, default off (analysis design A, opt-in like `join_leave`). It sets `SubscribeRequest.flag` bit 0 beside bit 1 (`REJECT_UNRECOVERED`) on every subscribe. A server without compaction ignores the bit; the @A scenario checks v6.10.1.
  - **Map.** The core keeps a map from id to subscription for the connection (Block 14's O(1) dispatch).
    - A subscribe reply carrying `SubscribeResult.id` > 0 maps that id to its subscription, replacing any earlier holder of the id (centrifuge-js 63ed73e `subscription.ts:548-550`).
    - A subscription's entry is removed when it leaves `subscribed`: on a resubscribe, an unsubscribe, a removal or state invalidation (`:623-626`, `:1084-1087`).
    - The whole map is cleared when the connection ends (centrifuge-rust `docs/spec.md` §7.7, §7.8, commit 88d6edf).
  - **Routing.**
    - A pub, join or leave push with `Push.id` > 0 and an empty channel goes to the subscription its id maps to, under the same rules as routing by channel: only while subscribed, and delta checks after routing (Block 19, D5).
    - A push with an unknown id is dropped and never given to server-side subscriptions (Q2, out of scope).
    - A push with a channel is routed by channel, as before. In JSON the id is a number (centrifuge-js 63ed73e `fakeServer.ts:17`).
  - **Tests.** @D for the routing: there is no Centrifugo PRO server in CI, and the documentation of the option says so.
- **D3 Publication channel (35c).**
  - **Field.** `Publication::channel: Option<String>` is set when the server sends a non-empty `Publication.channel` (`client.proto` field 10: "only set when it differs from the channel of the subscription, i.e. for wildcard subscriptions") and is `None` otherwise. It applies to client-side and server-side subscriptions alike, and `Debug` shows it.
  - **Position.** A publication with its own channel moves neither the offset nor the epoch of its subscription, since it belongs to another stream (centrifuge-rust `docs/spec.md` §7.4, commit 88d6edf).
  - **Verified:** the open-source Centrifugo v6.10.1 has no wildcard subscriptions. Its documentation reserves `*` "for the future Centrifugo needs" (centrifugal.dev, "Channels"), and a maintainer said only concrete channels can be subscribed to (centrifugal/centrifugo#799). The scenarios are therefore @D.
- **D4 Recovered publications (35c).**
  - **Field.** `Subscribed::has_recovered_publications: bool` is true when the subscribe reply, or the `ConnectResult.subs` entry, carries publications, recovered or initial, which follow the event unless withdrawn (Block 15). This is centrifuge-js 63ed73e `_getSubscribeContext` (`centrifuge.ts:2484-2524`, `types.ts:326` server-side, `:347` client-side).
  - **Subscribe push.** A `Subscribe` push carries no publications (`client.proto`, `Subscribe`), so the field is false there.
  - **Documentation.** The field is documented for metrics and logging.
- **D5 Version and documentation.**
  - **Version.** No bump: the change is additive. `Publication`, `Subscribed`, `SubscriptionOptions` and `SubscriptionError` are `#[non_exhaustive]`, and the types, methods and `Input::SetSubscriptionTagFilter` are new.
  - **Changelog.** `Unreleased` "Added" entries per sub-increment.
  - **README.** The "Subscriptions" section gains the tag filter and the compaction option, the latter marked Centrifugo PRO and untested in CI.

Value clause: As an app developer on a busy Centrifugo deployment, I want server-side tag filtering, numeric channel ids, a publication's own channel and the recovered-publications flag, so that clients receive and decode only what they need.

**Structural acceptance:**
- codec unit tests encode a nested filter and the flag (bit 0 alone, bit 0 with bit 1, neither without the option) in JSON and Protobuf, and decode `SubscribeResult.id`, `Push.id` and `Publication.channel`;
- unit tests: `and` and `or` chains merge; the D1 checks; `Debug` of a filter shows no value;
- core unit tests route a pub, join and leave by id; drop an unknown id without a server-side publication; drop an id after its subscription unsubscribes or resubscribes; clear the map on disconnect;
- each scenario is seen failing once on a mutation (filter not sent, flag not set, id map not cleared, channel not decoded, flag always true), recorded in the PR.

```gherkin
Feature: Tag filters, channel compaction, publication channel, recovered publications

  @A
  Scenario Outline: A tag filter delivers only matching publications
    Given a client configured with the <protocol> protocol, subscribed to a channel allowing tag filters with the filter ticker in BTC, ETH
    When the server API publishes to the channel with tags {"ticker":"BTC"}, then {"ticker":"XRP"}, then {"ticker":"ETH"}
    Then the subscription receives exactly the publications tagged BTC and ETH, in order

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: The tag filter is sent on subscribe and on resubscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the filter ticker eq BTC is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each carrying the filter ticker eq BTC

  @D
  Scenario: A tag filter set on a subscription is sent from the next subscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the filter ticker eq BTC is subscribed
    And the tag filter of the subscription is set to ticker eq ETH
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received exactly 2 subscribe commands for "news", the first carrying the filter ticker eq BTC and the second ticker eq ETH

  Scenario: A tag filter with delta is refused
    Given subscription options with the fossil delta option and a tag filter
    When the subscription is built
    Then building fails because a tag filter cannot be combined with delta
    And setting a tag filter on a subscription with the fossil delta option fails the same way

  Scenario Outline: A tag filter the server would reject is refused
    Given subscription options with the tag filter <filter>
    When the subscription is built
    Then building fails because the tag filter is invalid
    And setting that tag filter on a built subscription fails the same way

    Examples:
      | filter                                      |
      | a set comparison on "ticker" with no value  |
      | an equality with an empty key               |

  @D
  Scenario: Channel compaction is requested on every subscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with channel compaction is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each requesting channel compaction

  @D
  Scenario Outline: Pushes with a numeric channel id reach their subscription
    Given a client subscribed to channel "news" on the fake server with channel compaction and join and leave pushes, the fake server giving it id 7
    When the fake server pushes a <push> with id 7 and no channel
    Then the subscription receives that <push>

    Examples:
      | push        |
      | publication |
      | join        |
      | leave       |

  @D
  Scenario: A push with an unknown channel id is dropped
    Given a client subscribed to channel "news" on the fake server with channel compaction, the fake server giving it id 7
    When the fake server pushes a publication with id 8 and no channel, then one with id 7 and no channel
    Then the subscription receives exactly one publication, the one pushed with id 7
    And the client receives no server-side publication

  @D
  Scenario: Numeric channel ids are forgotten when the connection ends
    Given a client subscribed to channel "news" on the fake server with channel compaction, the fake server giving it id 7
    When the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again, the fake server giving it id 9
    And the fake server pushes a publication with id 7 and no channel, then one with id 9 and no channel
    Then the subscription receives exactly one publication, the one pushed with id 9

  @A
  Scenario: Channel compaction on a server without it changes nothing
    Given a client subscribed to channel "news" with channel compaction
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

  @D
  Scenario: A publication of a wildcard subscription carries its channel
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes a publication for channel "news" whose own channel is "news.sport", then one with no own channel
    Then the subscription receives a publication whose channel is "news.sport", then one with no channel

  @D
  Scenario: A publication of another channel does not move the stream position
    Given a client subscribed to recoverable channel "news" on the fake server
    When the fake server pushes a publication for channel "news" at offset 1, then one whose own channel is "news.sport" at offset 5
    And the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "news" asks to recover from offset 1 in the same epoch

  @A
  Scenario Outline: The subscribed event says whether recovered publications follow
    Given a client subscribed to a channel with history and recovery
    When <count> publications are made to the channel while the client reconnects
    Then the subscription emits "subscribed" again with has_recovered_publications <flag>

    Examples:
      | count | flag  |
      | 2     | true  |
      | 0     | false |

  @D
  Scenario: A server-side subscribed event says whether publications follow
    Given the fake server grants a server-side subscription to channel "alerts" with 1 initial publication and recovered false at connect
    When a client connects to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts" with has_recovered_publications true
```

**Exceptions introduced:** none.

---

## Block 36 — Automatic command batching, optimistic subscriptions

**Goal:** the commands a tokio client produces before its driver next runs go out in as few frames or emulation requests as `batch_frame_bytes` allows, without the application opening a batch and without a delay; and a client connecting over websocket sends its subscribe commands in the frame of its connect command instead of a round trip later (parity items 26 and 31; "Planned after Block 19", owner decisions 2026-11-06 and 2026-11-06, late, decided by the reviewer by state of the art: design A for each).

**Dependencies:** Blocks 8 (emulated transports, the HTTP 413 rule, the fallback of a websocket that never answers), 9 (subscribing, unsubscribing while a subscribe is in flight), 10 (subscription tokens, resubscribe backoff), 11 (getState), 13 (calls, their phases, the write queue, the order of calls), 15 (in-order handling, a reply read stops its deadline), 16 (runners, invariants, planted faults), 18 (batches, frame building, `batch_frame_bytes`, D6 resend, D8 connect-reply batch, I10), 21 (`Action::Anomaly`), 25 (D3: a connect command waiting for a token), 27 (Protobuf over http_stream), 29 (connect timeout once open), 31 (`bytes_out`, `messages_out`).

**Out of scope:**
- a batch delay timer: an idle client sends each command when its driver next runs (centrifuge-rust `docs/spec.md` §9.10, commit 88d6edf, "Why no delay timer");
- an option to turn either behaviour off: `batch_frame_bytes` of 1 makes every command go alone, which turns off both (D2, D5);
- optimistic subscriptions over http_stream and sse: an emulated transport's commands need the session of the connect reply (centrifuge-js 63ed73e `centrifuge.ts:1242-1244` returns before the batch on emulation); WebTransport gets D5 in Block 48;
- starting a subscription's token, data or getState callback before the connect reply: such a subscription subscribes after it, as today (D5);
- sending a subscribe command made by `subscribe()` after the connect command left, before the connect reply;
- server-side subscriptions (Block 12): the connect command recovers them itself (`ConnectRequest.subs`).

This block reverses three exclusions of Block 18: "automatic batching of every command a step or a burst produces" and "batching the calls a new connection releases (Block 13) when no batch is open" (owner decision 2026-11-06, item 26), and "subscribe commands in the frame of the connect command on websocket open" (owner decision 2026-11-06, late, item 31). Block 0's "Reversed exclusions" gains the three rows, and Block 18's lines say so. Block 18's three 18c scenarios run over http_stream, where D8 alone applies (D10).

**Sub-increments** (one branch and PR each, in this order):

| Id | Value clause | Scenarios |
|---|---|---|
| 36a | As an app developer, I want a burst of calls and subscriptions to go out in as few frames or emulation requests as the frame cap allows without opening a batch, so that chatty code costs the network and the server no more writes than batched code | "A burst of calls goes out in one frame", "A burst of subscribes goes out in one frame", "A burst of publishes reaches Centrifugo in one frame", "Calls a new connection releases go out together", "The core batches automatically only when its owner enables it", "Automatic batching keeps the order of commands, send and pong included", "Automatic batching splits at the frame cap", "Automatic batching admits calls to the write queue as they are made", "An explicit batch holds its commands across flushes", "Commands refused with HTTP 413 are resent alone", "A call abandoned before the flush is never sent" |
| 36b | As an app developer on a mobile link, I want the subscribe commands of a client connecting over websocket sent with its connect command, so that the first publication arrives a round trip sooner | "Subscribes go with the connect command over websocket", "Subscribes go with the connect command over websocket only", "Only subscribes that fit the frame cap go with the connect command", "A subscription that needs a callback subscribes after the connect reply", "An optimistic subscribe's deadline starts at the connect reply", "A failed connect loses the optimistic subscribes as a teardown does", "Unsubscribing before the connect reply undoes an optimistic subscribe", "A subscribe reply before the connect reply breaks the protocol", "Optimistic subscriptions on Centrifugo" |
| 36c | As a maintainer, I want automatic batching and optimistic subscriptions in both validation runners, so that their interleavings are checked as the rest of the client is | "Seeded chaos with automatic batching and optimistic subscriptions on the core keeps every invariant", "The core runner reports a planted automatic-batching fault", "Seeded chaos on the tokio client batches automatically and keeps the invariants" |

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):
- **D1 Output buffer (36a).** With automatic batching on, every command the core would hand to the transport goes into an output buffer of the core instead of an `Action::Send`, in the order the core produces it.
  - **What.** The application's calls, `send` included; subscribe and unsubscribe commands; pong, refresh and sub_refresh; the frames of a stopped batch (Block 18 D4) and of a connect reply (D8). An emulated transport's connect command still goes in `OpenTransport.initial_frame`.
  - **Handled as sent.** A buffered command is otherwise handled as sent at that moment: its reply deadline runs, a call is in its sent phase, and a call's bytes count against `Config::write_queue_bytes` from then. A call is admitted to the write queue when it is made, as without automatic batching (Block 13), so a call that does not fit waits for room as before.
  - **Flush.** `Input::Flush` emits the buffer as frames, in order, built as Block 18 builds a batch: the D5 cap, the D7 encodings (Protobuf over http_stream too, Block 27), one emulation request per frame (D6). The calls of a frame count as unwritten until `Input::FrameWritten` reports that frame, their bytes added together. A flush with an empty buffer, or without automatic batching, produces no action.
  - **Cost.** Each command is encoded once, when buffered. A frame's size is the sum of its commands' encoded sizes plus the separators (JSON: one `\n` between commands; Protobuf: each command's varint prefix), never the frame encoded again per added command; Block 18's batches are measured the same way. The Block 14 @P gates pass unchanged.
  - **Calls no longer awaited.** A buffered call that settles (its timeout) or is abandoned before the flush is not sent (Block 13: a call no longer awaited is never sent; Block 18 D3, D6). The client's own commands are always sent: their replies are awaited.
  - **Teardown.** A transport that ends drops its buffered commands as it loses sent ones: calls fail with "connection closed" (code 11), subscribe and unsubscribe commands are lost (Block 13; Block 18 D3).
- **D2 Who flushes (Q1).** Automatic batching is opt-in on the sans-IO core: `Machine::with_automatic_batching(self) -> Self`, `#[must_use]`, as `with_data_source`, and `Input::Flush`, which the owner feeds once it has fed the inputs it has and before it performs the actions.
  - **Without it** the core sends each command at once, as before, and ignores `Input::Flush`: an existing owner, which never feeds a flush, keeps working unchanged. With it on by default, such an owner would compile and then hold every command until its deadline.
  - **The tokio adapter** always turns it on. There is no `Config` field and no user option: framing is not API, and `batch_frame_bytes` of 1 sends every command alone.
  - **No `Action`** asks the owner to flush: `Action` is exhaustive (Block 17), and the owner, not the core, knows when it has fed every input it has.
  - **Sources.** Sans-IO crates leave the time of writing to their owner, who pulls what the state machine queued once it has handled its inputs: quinn-proto `Connection::poll_transmit`, rustls `Connection::wants_write` and `write_tls`, h2's buffered `FramedWrite` drained by `poll_flush`, tungstenite's `WebSocket::write` and `flush` (`send` being both). Their async layers coalesce whatever is queued when they write, with no switch (quinn's connection driver, h2's connection task, async-nats's writer). centrifuge-rust `docs/spec.md` §9.10: everything queued when the writer is ready goes as one frame, split at a cap, with no option. In each the pull is in the owner's contract from the first release, so for an owner written against "a command is an `Action::Send` at once" it is opt-in: a change of contract that still compiles would fail silently, at run time.
- **D3 Tokio driver.** At the top of each iteration of `Driver::run`, under the core lock and before performing the actions queued, the driver feeds `Input::Flush`.
  - API calls still feed the core on the application's task and notify `actions_queued` (Block 13; `Shared::call`). A notification made while the driver performs actions is kept by the `Notify`, so the driver runs another iteration and flushes again.
  - So one frame carries the commands of the input the driver handled (a frame's replies, a timer) and those of every API call made since its last flush, from any task. An idle client's call goes out on the driver's next iteration, with no timer.
  - `ready()` (Block 30), statistics (Block 31), `close()` (Block 33) and sleep detection (Block 25) need no change: the flush is an input like any other.
- **D4 Order, explicit batches, HTTP 413 (Q2).**
  - **Order.** Frames leave in buffer order, so commands reach the transport in the order the core produced them, `send`, pongs, refreshes and the subscribes a resubscribe backoff starts included (Block 13: calls are sent in the order they were made; centrifuge-rust §9.10: "Order is the queue order", "Pongs are queued like any command"). Block 18 D2's reasons for not holding these (a batch may stay open, a token may expire) concern explicit batches, not a buffer emptied on the same driver iteration. This supersedes the analysis's note on design A that `send` and pongs "would still go at once": that would let a `send` overtake an RPC made before it.
  - **Explicit batches.** An explicit batch (Block 18) keeps holding what its D2 holds until it stops, across flushes. When it stops, its commands go into the buffer as Block 18 D4 admits them. A `send` or a pong made during an explicit batch goes at the next flush, so Block 18 D2's documented overtaking stays for explicit batches only.
  - **HTTP 413.** The commands a refused request carried are resent (Block 18 D6) through the buffer, each in a frame of its own, never joined to another command, in the order of the refused request.
- **D5 Optimistic subscriptions (36b; Q3: "Planned after Block 19" settles it: on, with no option).** When the connect command is sent over websocket, the subscribe commands that a connect reply would send at once go in the connect command's frame. That is when the transport opens, or once a connection token is fetched (Block 25 D3).
  - **Which subscriptions.** Those in `subscribing` with no resubscribe backoff pending (as `subscriptions_connected` chooses) that need no callback first: no getState load (Block 11), no token fetch (Block 10), no data callback (Block 9). They go in registry order (creation order), the connect command first, while the frame stays within `batch_frame_bytes`. A connect command larger than the cap goes alone.
  - **The rest.** The others subscribe after the connect reply, as today: their callbacks start then, and their subscribe commands go as Block 18 D8 says. That includes a subscription whose `subscribe()` is called after the connect command left.
  - **Never held** by an application batch, as D8.
  - **In flight.** The connect reply does not subscribe again a subscription whose optimistic subscribe is in flight; its command is in flight as any subscribe (`subscribe_command`). No field is added per subscription.
  - **Emulated transports** send no optimistic subscribe.
  - **Sources.** centrifuge-js 63ed73e `centrifuge.ts:1249-1258` sends the connect command and `_sendSubscribeCommands` in one batch when a websocket opens. Centrifugo v6.10.1 (centrifuge 7ac0371, its `go.mod`) reads a frame's commands in order (`handler_websocket.go:416-425`) and handles the connect synchronously (`client.go:2130-2131`). The cap is Block 18 D5's.
- **D6 Deadline and reply order.**
  - **Deadline.** An optimistic subscribe's reply deadline (`Config::timeout`) starts when the connect reply is read, not when it is sent. Until then the connect timeout bounds the attempt (Block 29), and a subscribe reply read before that reply was handled stops its deadline (Block 15). So a slow connect reply, such as one through a connect proxy, cannot end with "subscribe timeout" a connection that did answer, and a websocket that never answers still falls back with the connect timeout (Block 8). This diverges from centrifuge-js 63ed73e, whose `_call` arms the reply timer when the command is made (`centrifuge.ts:1871-1884`, `_registerCall` `:2789`).
  - **Reply order.** The replies are handled after the connect reply, in order (Block 15), so no subscription becomes `subscribed` while the client is `connecting`.
  - **A breach.** A subscribe reply handled before the connect reply breaks the protocol: the core reports `Action::Anomaly(Anomaly::ReplyBeforeConnect { transport, command: "subscribe" })` and reconnects with code 1 "transport closed", as Block 21 D4 does for its anomalies. `Anomaly` is `#[non_exhaustive]`.
- **D7 A connect that fails, an unsubscribe in flight.**
  - **Failed connect.** A connect error reply, a close or a disconnect push before the connect reply tears the connection down. The optimistic subscribes are lost as sent commands are; their subscriptions stay `subscribing`, emit nothing and go with the next connect command. No unsubscribe is sent: Centrifugo v6.10.1 handles nothing after a failed connect in its frame and closes the connection with 3501 on the next command (`client.go:2025-2036`, `:1965-1968`).
  - **Unsubscribe in flight.** `unsubscribe()` or a removal while an optimistic subscribe is in flight follows Block 9: once the subscribe's reply comes, after the connect reply, an unsubscribe command undoes a subscribe that succeeded.
  - **Cost at fleet scale.** A failed attempt now carries up to `batch_frame_bytes` of subscribe commands, which the server drops without handling. The README says so, and says how to lower it.
- **D8 Statistics (Block 31).** No change of definition. `bytes_out` counts each written frame, separators included. `messages_out` counts each command of a frame. Block 31 has no frame counter.
- **D9 Validation harness (36c).**
  - **Core runner.** Even seeds run machines with automatic batching. The simulated owner feeds `Input::Flush` after a seeded number of inputs, and always before the simulated clock moves, as the driver flushes before it waits.
  - **Simulated server.** It handles a frame's commands in order. After a connect error it drops the rest of the frame and closes on the next frame with 3501, as Centrifugo does.
  - **I10.** It gains two clauses, core, after every step:
    - with automatic batching, an `Action::Send` appears only among the actions of `Input::Flush`;
    - the first command sent on every transport is its connect command, and over an emulated transport nothing is sent before the connect reply.
  - **Planted faults.** In the tap, never in the client (Block 16): see the 36c scenario.
  - **Adapter runner.** It runs the tokio client, which batches automatically from 36a on. It counts frames of several commands no batch held, and connect frames carrying subscribe commands.
- **D10 Fixtures and existing scenarios.**
  - **Fake server.** It sends a frame's replies in the order of its commands, a subscribe answered from history included (today that reply can leave before the connect reply of its frame). It can delay its connect reply, answering the commands after it in its frame once it has. It can, as Centrifugo, drop the rest of a frame after answering its connect with an error.
  - **@A server.** A Centrifugo v6.10.1 container of its own, with Prometheus metrics on. `VERIFY:` the environment variable `CENTRIFUGO_PROMETHEUS_ENABLED=true` and the series `centrifugo_transport_frame_size_count{transport="websocket"}` (centrifuge 7ac0371 `metrics.go:80-83`, `:780-790`: its `_count` is the number of frames read from clients; observed in `handler_websocket.go:422`, `:437`, `:444`).
    Verified (Centrifugo v6.10.1, a raw websocket client, 2026-11-08): with `CENTRIFUGO_PROMETHEUS_ENABLED=true` the server's `/metrics` serves `centrifugo_transport_frame_size_count{transport="websocket"}`, which counts the frames read from clients: a frame of three commands counts once.
  - **Block 18.** Its scenarios "A reconnect sends its subscribe commands in one frame", "The subscribe commands of a connect reply are split at the frame cap" and "An open batch does not hold the subscribe commands of a connect reply" run over http_stream, where D8 alone applies, with the first renamed "A reconnect over an emulated transport sends its subscribe commands in one request".
  - **Unit tests.** Core unit tests that subscribe before connecting over websocket expect the subscribe in the connect frame.
- **D11 Public API, version and documentation.**
  - **API.** `Input::Flush` (`Input` is `#[non_exhaustive]`), `Machine::with_automatic_batching` and `Anomaly::ReplyBeforeConnect` are additions. No `Action` and no `Config` field is added.
  - **Version.** No bump: the API change is additive (Block 35 D5).
  - **Changelog.** `Unreleased` "Added": the three items. "Changed": the tokio client batches automatically; over websocket the core sends optimistic subscribes.
  - **README.** The "Batching" section gains both behaviours, the fleet cost of D7 and `batch_frame_bytes` of 1 as the way to send every command alone. The sans-IO documentation tells an owner of `with_automatic_batching` to feed `Input::Flush`.
  - **Footprint.** No field per subscription; the buffer is per client.

Value clause: As an app developer shipping ferrifuge to fleets of devices, I want bursts of commands coalesced and my subscribe commands sent with the connect command, so that a burst costs one write or one HTTP request per `batch_frame_bytes` and a connection's first publication arrives a round trip sooner, with no command reordered and no connection ended that sending them one by one would have kept.

**Structural acceptance:**
- core unit tests: buffer order across calls, `send`, pong, refresh and resubscribes; a flush with an empty buffer, and one without automatic batching, produces no action; the computed size of a frame equals its encoded length, JSON and Protobuf (a property test); `Input::FrameWritten` frees the bytes of every call of a frame; teardown empties the buffer and fails its calls with code 11; a resend after 413 goes alone; a buffered call settled or abandoned is skipped; the optimistic choice skips a backoff pending, each kind of callback, what does not fit, and everything when the connect alone exceeds the cap; the deadline starts when the connect reply is read and stops when the subscribe reply is read; the breach anomaly; `Debug` of `Input::Flush`;
- an adapter unit test on a current-thread runtime: calls made without yielding share a frame, and the driver flushes on the iteration a call's notification starts;
- the Block 14 @P gates pass unchanged, the heap per subscription included;
- each scenario is seen failing once on a mutation, recorded in the PR: the flush ignored (sent at once); the cap ignored at the flush; `send` or pong sent outside the buffer; resends joined; an abandoned call sent; the driver not flushing on a call's notification; optimistic subscribes off; optimistic subscribes over an emulated transport; callbacks started before the connect reply; the deadline started when sent; a subscribe reply before the connect reply accepted; `subscribe_command` kept across a teardown; an unsubscribe in flight not undone.

```gherkin
Feature: Automatic command batching, optimistic subscriptions

  @D
  Scenario: A burst of calls goes out in one frame
    Given a client on a current-thread runtime with a timeout of 10 s, connected to the fake server, which sends no pings
    When the application makes RPC "a", publishes {"n":1} to channel "news" and requests the history of channel "news", without yielding between the calls
    Then the fake server receives one frame carrying the rpc, publish and history commands, in that order
    And each call returns the fake server's reply
    When the application then calls RPC "d" and awaits it
    Then the call returns the fake server's reply within 1 s, its command alone in its frame

  @D
  Scenario: A burst of subscribes goes out in one frame
    Given a client on a current-thread runtime, connected to the fake server, with three subscriptions not subscribed
    When the application calls subscribe on the three subscriptions, without yielding between the calls
    Then the fake server receives one frame carrying the three subscribe commands, in the order of the calls
    And the three subscriptions reach state "subscribed"

  @A
  Scenario: A burst of publishes reaches Centrifugo in one frame
    Given a Centrifugo server of its own with Prometheus metrics, and a client on a current-thread runtime subscribed to channel "news" on it
    When the application publishes {"n":1}, {"n":2} and {"n":3} to channel "news", without yielding between the calls
    Then each publish succeeds
    And the subscription receives the publications {"n":1}, {"n":2} and {"n":3}, in that order
    And the server's count of websocket frames received grew by exactly 1 during the publishes

  @D
  Scenario: Calls a new connection releases go out together
    Given a client configured with the fake server's endpoint, not connected
    When the application calls RPC "a", RPC "b" and RPC "c"
    And connect is called
    Then after the connect reply the fake server receives one frame carrying the three rpc commands, in the order of the calls
    And each call returns the fake server's reply

  @E
  Scenario Outline: The core batches automatically only when its owner enables it
    Given a client core <batching>, connected
    When the application calls RPC "a", then RPC "b"
    Then <before>
    When the owner feeds a flush
    Then <after>

    Examples:
      | batching                    | before                                                                                         | after                                              |
      | with automatic batching     | the core has sent no frame                                                                     | the core sends one frame carrying RPC "a" and RPC "b" |
      | without automatic batching  | the core sent a frame carrying RPC "a" among the actions of the first call, and one carrying RPC "b" among those of the second | the flush produces no action                       |

  @E
  Scenario: Automatic batching keeps the order of commands, send and pong included
    Given a client core with automatic batching, connected over websocket, whose connect reply asked for pongs
    When the core reads a server ping
    And the application calls RPC "a", then send with {"n":1}, then publishes {"n":2} to channel "news"
    Then the core has sent no frame
    When the owner feeds a flush
    Then the core sends one frame carrying the pong, the rpc, the send and the publish commands, in that order

  @E
  Scenario Outline: Automatic batching splits at the frame cap
    Given a client core with automatic batching, connected, with a batch frame cap of 1000 bytes
    When the application makes calls whose commands encode to <sizes> bytes and the owner feeds a flush
    Then the core sends frames carrying <frames> calls, in the order the calls were made
    And no frame carrying several commands exceeds 1000 bytes

    Examples:
      | sizes                   | frames  |
      | 300, 300, 300, 300, 300 | 3, 2    |
      | 300, 1500, 300          | 1, 1, 1 |

  @E
  Scenario: Automatic batching admits calls to the write queue as they are made
    Given a client core with automatic batching, connected, with a write queue of 1000 bytes and nothing unwritten
    When the application makes five calls whose commands encode to 300 bytes each and the owner feeds a flush
    Then the core sends one frame carrying the first three calls
    When the owner feeds a flush
    Then the core sends nothing
    When the transport reports that frame written and the owner feeds a flush
    Then the core sends one frame carrying the fourth and fifth calls

  @E
  Scenario: An explicit batch holds its commands across flushes
    Given a client core with automatic batching, connected
    When the application starts a batch and calls RPC "a"
    And the owner feeds a flush
    Then the core has sent no frame
    When the batch is stopped
    Then the core has sent no frame
    When the owner feeds a flush
    Then the core sends one frame carrying RPC "a"

  @E
  Scenario: Commands refused with HTTP 413 are resent alone
    Given a client core with automatic batching, connected over an emulated transport
    When the application calls RPC "a" and RPC "b" and the owner feeds a flush
    Then the core sends one emulation request carrying both rpc commands
    When the server refuses that request with HTTP 413 and the owner feeds a flush
    Then the core sends two emulation requests, carrying RPC "a" and then RPC "b"
    And neither call has failed

  @E
  Scenario: A call abandoned before the flush is never sent
    Given a client core with automatic batching, connected
    When the application calls RPC "a" and RPC "b", then abandons RPC "a"
    And the owner feeds a flush
    Then the core sends one frame carrying RPC "b" and no rpc command for "a"

  @D
  Scenario Outline: Subscribes go with the connect command over websocket
    Given <given>
    When <trigger>
    Then the next frame the fake server receives carries the connect command, then the subscribe commands for "a", "b" and "c", in that order
    And the three subscriptions reach state "subscribed"
    And the fake server received exactly one subscribe command per subscription on that connection

    Examples:
      | given                                                                                                   | trigger                                                              |
      | a client configured with the fake server's endpoint, with subscriptions to "a", "b" and "c" subscribed before connect | connect is called                                                    |
      | a client connected to the fake server, with subscriptions to "a", "b" and "c" subscribed               | the fake server disconnects the client with a reconnect-advised code |

  @E
  Scenario Outline: Subscribes go with the connect command over websocket only
    Given a client core over <transport> with a subscription subscribed before connect
    When connect is called and the transport opens
    Then <first>
    When the server answers the connect command
    Then <after>

    Examples:
      | transport   | first                                                                                 | after                                                              |
      | websocket   | the core sends one frame carrying the connect command, then the subscribe command     | the core sends no subscribe command                                |
      | http_stream | the request opening the stream carries only the connect command, and the core sends nothing | the core sends an emulation request carrying the subscribe command |

  @E
  Scenario Outline: Only subscribes that fit the frame cap go with the connect command
    Given a client core over websocket with a batch frame cap of 1000 bytes, whose connect command encodes to <connect> bytes, and five subscriptions subscribed before connect whose subscribe commands encode to 300 bytes each
    When connect is called and the transport opens
    Then the core sends one frame carrying the connect command and the subscribe commands of the first <with> subscriptions
    When the server answers the connect command
    Then the core sends frames carrying <after> subscribe commands, for the other subscriptions in the order they were created

    Examples:
      | connect | with | after |
      | 90      | 3    | 2     |
      | 1200    | 0    | 3, 2  |

  @D
  Scenario Outline: A subscription that needs a callback subscribes after the connect reply
    Given a client configured with the fake server's endpoint, with subscriptions to "news" and "private" subscribed before connect, "private" with <callback>
    When connect is called
    Then the first frame the fake server receives carries the connect command and the subscribe command for "news", and no subscribe command for "private"
    And the <callback> of "private" is first called after the fake server sent its connect reply
    And the subscribe command for "private" reaches the fake server in a later frame
    And both subscriptions reach state "subscribed"

    Examples:
      | callback                       |
      | a token callback and no token  |
      | a data callback                |
      | a getState callback            |

  @E
  Scenario: An optimistic subscribe's deadline starts at the connect reply
    Given a client core over websocket with a timeout of 5 s and a subscription subscribed before connect
    When connect is called and the transport opens
    Then the core sends one frame carrying the connect command and the subscribe command
    When 4 s elapse and the core reads the connect reply
    And 4 s elapse
    Then the client is "connected" and the subscription is "subscribing"
    When 1 s elapses
    Then the client emits "connecting" with code 3 and reason "subscribe timeout"

  @D
  Scenario: A failed connect loses the optimistic subscribes as a teardown does
    Given the fake server answers the first connect command with a temporary error and drops the rest of its frame, as Centrifugo does
    And a client configured with the fake server's endpoint, with a subscription to "news" subscribed before connect
    When connect is called
    Then the client emits "connecting", then an "error" event, then "connected"
    And the fake server received exactly 2 subscribe commands for "news", each in the frame of a connect command, and no unsubscribe command
    And the subscription emits "subscribed" exactly once and never "unsubscribed"

  @D
  Scenario: Unsubscribing before the connect reply undoes an optimistic subscribe
    Given the fake server delays its connect reply by 300 ms
    And a client configured with the fake server's endpoint, with a subscription to "news" subscribed before connect
    When connect is called
    And the fake server has received the subscribe command for "news" in the frame of the connect command
    And unsubscribe is called on the subscription before the connect reply
    Then after answering the connect and the subscribe commands, the fake server receives an unsubscribe command for "news"
    And the subscription is "unsubscribed" and never emitted "subscribed"

  @E
  Scenario: A subscribe reply before the connect reply breaks the protocol
    Given a client core configured with a websocket endpoint only, with a subscription subscribed before connect
    When connect is called and the transport opens
    And the core reads one frame holding the reply to the subscribe command, then the connect reply
    Then the core reports a reply-before-connect anomaly naming the subscribe command
    And the core closes the transport and, after the reconnect backoff delay, opens a new one
    And the client stays "connecting" and emits no further event
    And the subscription emits no "subscribed" for that transport

  @A
  Scenario: Optimistic subscriptions on Centrifugo
    Given a Centrifugo server of its own with Prometheus metrics
    And a client with subscriptions to channels "news" and "chat" subscribed before connect
    When connect is called
    Then both subscriptions reach state "subscribed"
    And the server counted exactly 1 websocket frame received
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription to "news" receives a publication with data {"text":"hello"}

  @P
  Scenario: Seeded chaos with automatic batching and optimistic subscriptions on the core keeps every invariant
    Given the core runner with the chaos configuration, a batch frame cap of 1 KiB, client-side and server-side subscriptions to recoverable channels, batching operations, and automatic batching on even seeds
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I10
    And the run counted flushes sending several commands, connect frames carrying subscribe commands and optimistic subscribes lost to a failed connect, none of them 0

  @P
  Scenario Outline: The core runner reports a planted automatic-batching fault
    Given the core runner with automatic batching, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of I10, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                                    |
      | a frame sent by a flush reported to the checker as sent by the input before it           |
      | the first subscribe command of a connect frame reported to the checker before the connect command |

  @P
  Scenario: Seeded chaos on the tokio client batches automatically and keeps the invariants
    Given the adapter runner with the chaos configuration, a batch frame cap of 1 KiB, a tokio client, the fake server and batching operations
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the fake server received frames of several commands that no batch held, and connect frames carrying subscribe commands, none of them 0
```

**Exceptions introduced:** five divergences from centrifuge-js 63ed73e, each recorded in the decisions above:
- automatic batching, which centrifuge-js lacks (D1, D4; the source is centrifuge-rust `docs/spec.md` §9.10);
- the connect command's frame is capped by `batch_frame_bytes` (D5, as Block 18 D5);
- a subscription that needs a token, data or getState callback starts it after the connect reply, where centrifuge-js starts it when the websocket opens and sends the subscribe once it answers (`subscription.ts:650-692`) (D5);
- a `subscribe()` made after the connect command left waits for the connect reply, where centrifuge-js sends it once the transport is open (`subscription.ts:653`, `:735-738`) (D5);
- an optimistic subscribe's deadline starts when the connect reply is read (D6).

---

## Block 37 — Test constructors, an in-memory server double, an injectable connector

**Goal:** an application tests its own code that uses ferrifuge without a Centrifugo server. It builds the events and results ferrifuge yields (item 19). It can give the client a connected byte stream of its own for the websocket (item 35). It runs its real `Client` against an in-memory server double that its test drives (item 22). Source: parity items 19, 22 and 35, "Planned after Block 19", decided by the reviewer by state of the art: design A for each.

**Dependencies:**
- Blocks 0 (components rule, features), 6 and 22 (TLS settings), 7 (Protobuf), 8 (a failed attempt, the fallback);
- Blocks 9–13 (subscriptions, recovery, server-side subscriptions, calls) and 15 (delivery);
- Block 16 (the crate's test hooks, today behind `test-util`);
- Blocks 20 (release, docs.rs, semver job), 21 (logging, redaction), 23 (proxy settings, `Proxy::custom`'s endpoint form), 28 (runtime handle);
- Blocks 29 (handshake timeout), 31 (statistics), 35 (`Publication.channel`, `has_recovered_publications`) and 36 (subscribe commands in the connect frame).

**Out of scope:**
- A client trait with a mock implementation (analysis item 22, design B): each new method breaks implementers, the centrifuge-go maintainer's objection (centrifuge-go #86).
- A connector for http_stream and sse. reqwest's connector layer can only wrap reqwest's own connector, whose `Conn` cannot be built outside reqwest (reqwest 0.13.5 `client.rs:2475-2481`, `connect.rs:1346-1352`).
- In the double:
  - http_stream and sse;
  - token checks, connection expiry and refresh;
  - server pings;
  - join and leave pushes;
  - delta compression;
  - unsubscribe pushes;
  - history expiry and recovery limits;
  - delayed answers and malformed frames.

  These stay with the crate's fake server or the @A and @B layers.
- The server API, proxies and other server features in the double, beyond what the client protocol carries.
- Constructors for the sans-IO types (`Input`, `Action`): a sans-IO owner drives a `Machine` directly.
- Moving the crate's fake server (`tests/support/fake_server.rs`) onto the double (D5).

**Sub-increments** (one branch and PR each, in this order):

| Id | Value clause | Scenarios |
|---|---|---|
| 37a | As an app developer, I want to build the events and results ferrifuge yields, so that I can unit-test the code that handles them | "Test constructors build what the client core yields" |
| 37b | As an app developer behind a VPN tunnel, an in-process transport or a custom proxy, I want to give the client the connected stream its websocket runs over, so that it connects where its own networking cannot | "A connector gives the websocket its stream", "A wss endpoint over a connector is verified as without one", "A connector that fails fails the attempt", "A connector that never finishes fails the attempt with the handshake timeout", "A connector goes with websocket endpoints and no proxy setting only" |
| 37c | As an app developer, I want my real client to run against an in-memory server my test drives, so that I test subscriptions, publications, recovery, calls and disconnects without a Centrifugo server | "An application tests its subscription against the server double", "The server double recovers publications missed during a reconnect", "The server double answers calls", "The server double refuses as the test asks", "The server double disconnects the client with a code", "The server double grants server-side subscriptions and sends messages", "The server double and Centrifugo answer alike" |

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):

- **D1 Features (37a; Q1).**
  - **Private hooks.** The crate's own hooks move to a private feature, `__test-hooks`. The hooks are:
    - `Machine::with_seed`, which gains `#[doc(hidden)]` like the others;
    - `Client::log_number`, `Client::read_ahead` and `Client::held_events`;
    - `Subscription::held_events`;
    - `stalled_websocket_writes`;
    - `write_fossil_delta`.

    The files are `src/lib.rs`, `src/machine.rs`, `src/fossil.rs` and `src/adapter/{client,mod,outbox,subscription,transport}.rs`. Three other places change: the self dev-dependency (`features = ["__test-hooks", "test-util"]`), the `required-features` of the `perf_sim` and `chaos` tests, and the comment of the semver job.

    The Cargo.toml comment says the feature is never for applications. A leading underscore marks it private: cargo-semver-checks 0.51.0 leaves `_` features out of its default feature set and out of `feature_missing` (`src/lints/feature_missing.ron`). reqwest 0.13.5 (`__tls`, `__rustls`) and tokio-tungstenite 0.30.0 (`__rustls-tls`) do the same, and Cargo has no private features (rust-lang/cargo#10882). This amends Block 16 and Block 21 D2.
  - **Public feature.** `test-util = ["tokio?/io-util"]` is public and not a default.
    - The constructors (D2) exist in every build with it, the core alone included.
    - The server double (D5) exists when `tokio` is also enabled (`cfg(all(feature = "test-util", feature = "tokio"))`).
    - One feature for both, as tokio's single `test-util` and the aws-sdk crates' `test-util`.
    - The double adds no crate. tokio-tungstenite's `handshake` is already enabled. `tokio/io-util` is what `tokio::io::duplex` needs (tokio 1.54 `src/io/util/mem.rs:108`).
  - **Documentation.** Each item shows its feature (`doc(cfg)` under `docsrs`, Block 20 D7).
- **D2 Constructors (37a; Q2).** The test is what an application cannot build today, or can build only field by field.
  - **Already buildable, unchanged:**
    - every variant of `SubscriptionEvent` and `ServerSubscriptionEvent`;
    - the variants of `ClientEvent` other than `Connected`;
    - `ClientError`, `StreamPosition`, `CallReply`, `SubscriptionError`, `TokenError`, `DataError`, `StateError`;
    - `CallError`, through its public `new` and `temporary`, which stay ungated.
  - **New, behind `test-util`:**
    - `ClientEvent::connected(client_id: impl Into<String>, transport: impl Into<String>, data: Option<Vec<u8>>)`. The `#[non_exhaustive]` variant cannot be built outside the crate. A field added to the variant later takes the value the client gives when the server leaves it out.
    - `Publication::new(data: impl Into<Vec<u8>>)`, with `offset(u64)`, `tags(impl IntoIterator<Item = (K, V)>)` (`K`, `V`: `Into<String>`, replacing), `info(ClientInfo)`, `epoch(impl Into<String>)` and `channel(impl Into<String>)`.
    - `ClientInfo::new(client, user)` (`impl Into<String>`), with `conn_info` and `chan_info` (`impl Into<Vec<u8>>`).
    - `Subscribed`, from `Default`, with:
      - `data(impl Into<Vec<u8>>)`;
      - `positioned`, `recoverable`, `was_recovering`, `recovered`, `delta` and `has_recovered_publications` (`bool`);
      - `stream_position(StreamPosition)`.
    - `HistoryResult`, from `Default`, with `publications(impl IntoIterator<Item = Publication>)`, `epoch(impl Into<String>)` and `offset(u64)`.
    - `PresenceResult`, from `Default`, with `clients(impl IntoIterator<Item = ClientInfo>)`, keyed by each `ClientInfo::client` as the wire map is.
    - `PresenceStatsResult`, from `Default`, with `num_clients` and `num_users`.
    - `Statistics::new(state: State)`, with every counter at 0 and no transport. It has a setter per field; `transport(TransportKind)` and `connected_since(SystemTime)` set `Some`.
    - `DeltaStatistics`, from `Default`, with a setter per field.
  - **Setter rules.** Each setter takes `self`, returns it and is `#[must_use]`. An `Option` field's setter takes the inner type and sets `Some`. A field a constructor does not name holds what the client yields when the server leaves it out. In particular, `Publication`'s crate-private delta flag is `false`, as on every publication the client delivers.
  - **Sources.**
    - The crate's own pattern: `Config`'s setters (Block 29 D4), `HistoryOptions`, `CallError`.
    - Rust API Guidelines C-CTOR (constructors are static inherent methods) and C-BUILDER.
    - centrifuge-rust `docs/spec.md` §3.3 (commit 88d6edf): `test-util`, "public constructors for events and results (`#[non_exhaustive]` blocks struct literals)". Its `docs/api.md` documents no shape.
    - aws-sdk-rust builds its outputs with builders for its mocks (aws-smithy-mocks 0.3.0).
  - **Doc-tests.** Each constructor has a doc-test (C-EXAMPLE).
- **D3 Connector API (37b; Q3, the argument).** In the tokio adapter, not behind a feature:
  - **The trait.** `pub trait Connector: Send + Sync { fn connect(&self, endpoint: http::Uri) -> Connecting; }`, with `pub type Connecting = Pin<Box<dyn Future<Output = Result<ConnectedStream, BoxError>> + Send>>` and `BoxError = Box<dyn std::error::Error + Send + Sync>`.
  - **The stream.** `ConnectedStream` is opaque: `ConnectedStream::new<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(stream)`, with tokio's traits. Those bounds are exactly what `client_async_tls_with_config` needs (tokio-tungstenite 0.30.0 `src/tls.rs:189`). Opaque, so connection details can be added later (Rust API Guidelines C-STRUCT-PRIVATE).
  - **Closures.** A blanket implementation covers `F: Fn(http::Uri) -> Fut + Send + Sync + 'static`, where `Fut: Future<Output = Result<S, E>> + Send + 'static` and `E: Into<BoxError>`. This is the shape of reqwest 0.13.5 `dns::Resolve` (`src/dns/resolve.rs:18-33`) and of wiremock 0.6.5 `Respond`, which has a closure blanket implementation.
  - **The endpoint.** `http::Uri`, as hyper-util 0.1.21 `Connect` (a `Service<Uri>`, `client/legacy/connect/mod.rs:338-344`) and tonic 0.14.9 `Endpoint::connect_with_connector` take it. It is the configured websocket endpoint with its port always written, 80 for `ws` and 443 for `wss`, as Block 23 D1 gives `Proxy::custom`; path and query are kept. `ferrifuge::http` re-exports the `http` crate (1.x), as `ferrifuge::rustls` re-exports rustls.
  - **Setting it.** `ClientBuilder::connector(connector: impl Connector + 'static)`. An `Arc` of a connector is itself a connector, so clients can share one, as reqwest's `dns_resolver` takes a resolver or an `Arc` of one. If coherence refuses `impl Connector for Arc<C>` next to the closure implementation, it takes reqwest's `IntoResolve` shape.
  - **`Debug`.** `ClientBuilder`'s `Debug` shows `connector: true` or `false`.
- **D4 Connector behaviour (37b; Q3, proxy and timeout).**
  - **Each attempt.** Each connection attempt calls the connector once. Over the stream it returns, Ferrifuge then makes the same `client_async_tls_with_config` call as over a stream it dialled (`src/adapter/transport.rs:119`):
    - for `wss://`, TLS with the client's settings (Blocks 6, 22), verifying the endpoint's host;
    - the upgrade, with `http_headers` and the subprotocol;
    - `max_message_bytes`.

    There is one websocket path. `TCP_NODELAY` and socket options are the connector's.
  - **Failure.** An error from the connector, or a panic, fails the attempt as a refused connection does (Block 8): an `error` event of kind transport, then the reconnect backoff; `failed_attempts` counts it (Block 31).
    - The message is `connector: <its error>`, or "connector panicked".
    - The connector is guarded as the token and data callbacks are, so a panic never ends the driver (invariant I2).
  - **Timeout.** `Config::handshake_timeout` bounds the connector's future with the rest of the open (Block 29 D1). At the timeout the future is dropped and the error says "handshake timeout"; the documentation tells connectors to be cancel-safe. tonic: "The `connect_timeout` will still be applied".
  - **Proxies.** A client with a connector reads no proxy setting: not the environment's, not the system's (Block 23 D1, D2). The connector is the route, as a proxy is itself a connector wrapping another (hyper-util `Tunnel` and `SocksV5`; Block 23 D3).
    - `build()` fails with a configuration error (code 12) naming the connector and the other option in two cases: `ClientBuilder::proxy` was called with any setting, or an endpoint is not websocket (http_stream, sse).
    - `Client::set_proxy` on such a client changes nothing, and its documentation says so.
  - **Logging.** The DEBUG record of each attempt (Block 23 D7) gives `proxy` as `connector`. The connector sees the endpoint unredacted: it is the application's own.
- **D5 Server double (37c; Q4).** `ferrifuge::test_util::Server` is `Clone` and shares one state.
  - **Connecting a client.**
    - `Server::new()` needs no runtime.
    - `server.endpoint()` gives `ws://ferrifuge-double.invalid/connection/websocket`. It never resolves (RFC 6761), so a client that bypasses the connector fails.
    - `server.connector()` gives a `Connector` that opens a `tokio::io::duplex` pair and answers the upgrade over it with tokio-tungstenite's `accept_hdr_async`, taking the `centrifuge-protobuf` subprotocol when the client asks for it. It serves the connection on a task of the runtime that polls the connector, the client's own, until the connection or the last `Server` handle ends.
    - The real client, driver and core run unchanged; no socket is opened.
    - Every connection gets a distinct client id. Several clients may share one double.
  - **Answers, set synchronously before or during the test** (wiremock 0.6.5's registered mocks and synchronous `Respond`; aws-smithy-mocks rules). The connect reply announces no pings.
    - `connect_data(data)` sets the data of later connect replies.
    - `refuse_next_connect(CallError)` answers the next connect command with that error, its `temporary` flag included.
    - `grant_server_subscription(channel)` adds the channel to the `subs` of later connect replies.
    - `refuse_subscriptions(channel, CallError)` answers subscribes to the channel with that error.
    - `on_rpc(F)`, with `F: Fn(&str, &[u8]) -> Result<Vec<u8>, CallError> + Send + Sync + 'static`, answers RPCs given the method and the data. Without a handler an RPC fails with 108 "not available", as Centrifugo does without an RPC proxy. Verified (Centrifugo v6.10.1, a raw websocket client, 2026-11-08): an RPC without an RPC proxy is answered with error 108 "not available".
  - **Channels.** Every channel has one stream and one epoch, with offsets from 1 and a history of every publication. The history is unbounded and never expires.
    - **Subscribe.** A subscribe is answered with the `positioned` and `recoverable` it asked for, and the stream's position.
    - **Recovery.** A subscribe asking to recover from an offset and epoch of that stream gets the publications after that offset with `recovered` true. One with another epoch gets `recovered` false and no publications.
    - **Publish.** A publish from a client is accepted and goes into the stream, to every connection subscribed, the publisher included.
    - **Calls.** `history` follows `limit`, `since` and `reverse`, as Centrifugo does (Block 13). `presence` lists the connections subscribed, by client id, with an empty user. `presence_stats` counts them and their distinct users. `send` is recorded only.
  - **Actions on open connections.**
    - `publish(channel, data) -> u64` publishes into the stream and returns the offset.
    - `message(data)` sends an async message push.
    - `disconnect(code, reason)` sends a close frame. From that moment the connection receives nothing more, and later publications go to the history only.
  - **Inspection.**
    - `received() -> Vec<Command>` gives the commands of every connection, in the order read, decoded the same way from JSON and Protobuf.
    - `connections() -> usize` gives the connections open.
    - `Command` is `#[non_exhaustive]`, with `#[non_exhaustive]` variants with public fields: `Connect { token, data }`, `Subscribe { channel, recover: Option<StreamPosition> }`, `Unsubscribe { channel }`, `Publish { channel, data }`, `Rpc { method, data }`, `Send { data }`, `History { channel }`, `Presence { channel }`, `PresenceStats { channel }`.
    - Its `Debug` shows data as its length and the token only as set or not (Block 21).
    - Wiremock's `MockServer::received_requests` records requests the same way.
  - **Components.** Framing is tungstenite's. Encoding is the crate's prost types and serde_json. No parser is written (Block 0, "Components").
  - **Placement.** The double lives in `src/test_util/`. Block 28's source check, which bans `tokio::spawn` from `src/adapter/`, does not apply there.
  - **The crate's fake server stays separate.** Its controls (portal pages, stopped reading, sized and corrupted frames, reversed RPC answers) are faults an application API should not carry. centrifuge-rust keeps its fake server out of the public API too (`docs/spec.md` §11.2, `docs/api.md`, 88d6edf).
- **D6 Tests (Q6).**
  - **37a.** An @E outline checks the constructors against what the core yields from the wire. A unit test checks each setter against assigning its field. Each constructor has a doc-test.
  - **37b.** @D scenarios run over the fake server and a TLS listener; build refusals are @E.
  - **37c.** @D scenarios run the real client against the double, in both encodings. One @A scenario keeps the double faithful to Centrifugo.
- **D7 Version, CI, documentation (Q5).**
  - **Version.** No bump: every addition is additive (Block 35 D5).
    - The name `test-util` is kept and `__test-hooks` is new, so `feature_missing` and `feature_no_longer_enables_feature` (Deny) do not fire. The old `test-util = []` enabled nothing; `feature_newly_enables_feature` is Allow.
    - The hooks were never public API (Block 16), and the crate is not on crates.io (2026-11-08).
  - **CI.** The `semver` job checks the new public surface.
    - In 37a it runs `--default-features --current-features test-util`, since the base's `test-util` holds the visible `Machine::with_seed`.
    - From 37b, `ci.yml` and `release.yml` run `--default-features --features test-util` (cargo-semver-checks 0.51.0 README, "What features does cargo-semver-checks enable").
    - `[package.metadata.docs.rs]` and the `docs-rs` job build `test-util` (amends Block 20 D1, D7).
  - **Changelog.** `Unreleased`:
    - "Added": the constructors (37a); `Connector`, `ConnectedStream`, `ClientBuilder::connector`, `ferrifuge::http` (37b); `test_util::Server` and `test_util::Command` (37c).
    - "Changed": the crate's own test hooks move from `test-util` to the private `__test-hooks`.
  - **README.** The features table gains `test-util`. A "Testing your application" section shows a constructor, a connector and a test against the double, as a doc-tested example on a current-thread runtime. The "Proxies" section says that a connector replaces the proxy setting.

Value clause: As an app developer shipping ferrifuge to fleets of devices, I want to build its events and results, to give its websocket a stream of my own, and to run my real client against an in-memory server my test drives, so that I test my code's handling of publications, recovery, calls and disconnects in CI without a Centrifugo server, and connect through networking ferrifuge does not have.

**Structural acceptance:**
- Features:
  - `Cargo.toml` has `test-util = ["tokio?/io-util"]` and `__test-hooks = []`;
  - `cargo tree` shows no new crate;
  - `cargo check --no-default-features --features test-util` builds the constructors without tokio;
  - the constructors sit in a module compiled only under `test-util`, and no hook is reachable through `test-util`. Both are source checks, as Block 28's.
- Unit tests:
  - each setter of every D2 type sets its field as assigning it does, `Statistics` and `DeltaStatistics` included, and leaves the other fields at their defaults;
  - `Command`'s `Debug` shows no token and no data;
  - `ClientBuilder`'s `Debug` shows `connector` as a flag;
  - the endpoint given to a connector carries its port for `ws` and `wss`, with and without one configured.
- `tests/proxy_env.rs`: with `ALL_PROXY` set to a recording proxy, a client with a connector connects, and the proxy sees no connection.
- Doc-tests cover every constructor, `Connector` (a closure dialling TCP) and `test_util::Server` (the README's test). All run in the `test` job.
- The semver, docs.rs and `docs-rs` job changes are as in D7.
- Each scenario is seen failing once on a mutation, recorded in the PR:
  - **37a:** a setter that sets another field, or none; `ClientEvent::connected` dropping its data; `Publication::new` with the delta flag set.
  - **37b:**
    - the connector bypassed (a direct dial), or its endpoint passed without its port;
    - the subprotocol not sent over the connector's stream;
    - TLS skipped over it, or certificate verification turned off;
    - a connector error that panics the driver or emits no `error` event;
    - its future not bounded by the handshake timeout, or not dropped at it;
    - the build check removed;
    - a proxy setting applied with a connector.
  - **37c:**
    - the double answering JSON to a Protobuf client;
    - offsets restarting per connection;
    - `recovered` always true, or always false;
    - a publication delivered to a connection after its `disconnect`;
    - a refusal ignored;
    - the close code replaced with 1000;
    - server-side subscriptions not granted;
    - messages dropped;
    - `received()` missing a subscribe;
    - `history` ignoring its limit.

```gherkin
Feature: Test constructors, an in-memory server double, an injectable connector

  @E
  Scenario Outline: Test constructors build what the client core yields
    Given a client core with the JSON protocol over websocket, with a subscription to "news" subscribed before connect
    When connect is called and the transport opens
    And <reading>
    Then the core yields <yielded>, equal to <built>
    And the built value holds every value the row names

    Examples:
      | reading                                                                                                                                                                   | yielded                                    | built                                                                                                                                            |
      | the core reads the connect reply with client id "c1" and data {"a":1}                                                                                                     | the client's "connected" event             | the connected event built with client id "c1", transport "websocket" and data {"a":1}                                                            |
      | the core reads the connect reply, then the subscribe reply for "news", recoverable and positioned, at offset 3 with epoch "e"                                           | the "subscribed" event of "news"           | the subscribed details built as recoverable and positioned, with stream position offset 3, epoch "e"                                             |
      | the core reads the connect reply and the subscribe reply, then a publication of "news" with data {"n":1}, offset 4, tag "k" "v" and info of client "c1", user "u1"      | the publication event of "news"            | the publication built with data {"n":1}, offset 4, tag "k" "v" and the client info built with client "c1", user "u1"                             |
      | the core reads the connect reply, the application requests the history of "news" with limit -1, and the core reads publications {"n":1} at 1 and {"n":2} at 2, epoch "e", offset 2 | the history result                         | the history result built with the publications built with data {"n":1} offset 1 and data {"n":2} offset 2, epoch "e" and offset 2                |
      | the core reads the connect reply, the application requests the presence of "news", and the core reads one client "c1" of user "u1"                                      | the presence result                        | the presence result built with the client info built with client "c1", user "u1"                                                                 |
      | the core reads the connect reply, the application requests the presence stats of "news", and the core reads 2 clients and 1 user                                       | the presence stats result                  | the presence stats result built with 2 clients and 1 user                                                                                        |

  @D
  Scenario Outline: A connector gives the websocket its stream
    Given the fake server
    And a client configured with the <protocol> protocol and the endpoint "ws://centrifugo.invalid/connection/websocket", with a connector that opens a TCP connection to the fake server and records the endpoints it is given
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the connector was called once, with "ws://centrifugo.invalid:80/connection/websocket"
    And the fake server received a connect command in the <protocol> encoding
    When the fake server disconnects the client with a reconnect-advised code
    Then the client emits "connected" again
    And the connector was called twice

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario Outline: A wss endpoint over a connector is verified as without one
    Given the fake server behind a TLS listener whose certificate for "centrifugo.invalid" is issued by <issuer>
    And a client configured with the endpoint "wss://centrifugo.invalid/connection/websocket" and the test CA as an extra root certificate, with a connector that opens a TCP connection to that listener
    When connect is called
    Then <outcome>

    Examples:
      | issuer         | outcome                                                                                                                         |
      | the test CA    | the client emits "connected" over transport "websocket"                                                                         |
      | the private CA | the client emits an "error" event of kind transport naming an invalid peer certificate, and the fake server receives no command |

  @D
  Scenario Outline: A connector that fails fails the attempt
    Given the fake server
    And a client configured with the endpoint "ws://centrifugo.invalid/connection/websocket", with a connector that <fails> on its first call and opens a TCP connection to the fake server on later calls
    When connect is called
    Then the client emits "connecting", then an "error" event of kind transport whose message contains "<message>", then "connected"
    And the connector was called twice
    And the client's statistics count 1 failed attempt

    Examples:
      | fails                          | message            |
      | returns the error "tunnel down" | tunnel down        |
      | panics                         | connector panicked |

  @D
  Scenario: A connector that never finishes fails the attempt with the handshake timeout
    Given the fake server
    And a client configured with the endpoint "ws://centrifugo.invalid/connection/websocket" and a handshake timeout of 500 ms, with a connector whose first call never finishes and whose later calls open a TCP connection to the fake server
    When connect is called
    Then within the configured handshake timeout plus tolerance the client emits an "error" event of kind transport with message "handshake timeout"
    And the future of the connector's first call has been dropped
    And the client then emits "connected" over transport "websocket"

  @E
  Scenario Outline: A connector goes with websocket endpoints and no proxy setting only
    Given a client builder with a connector and <options>
    When the client is built
    Then <outcome>

    Examples:
      | options                                                                  | outcome                                                                                     |
      | the endpoint "ws://centrifugo.invalid/connection/websocket"             | the client is built                                                                         |
      | a websocket endpoint, then an http_stream endpoint as a fallback        | building fails with a configuration error naming the connector and transport "http_stream" |
      | an sse endpoint                                                          | building fails with a configuration error naming the connector and transport "sse"         |
      | a websocket endpoint and the proxy setting none                         | building fails with a configuration error naming the connector and the proxy setting        |
      | a websocket endpoint and the system proxy setting                       | building fails with a configuration error naming the connector and the proxy setting        |

  @D
  Scenario Outline: An application tests its subscription against the server double
    Given a server double
    And a client built with the double's endpoint and connector and the <protocol> protocol
    When connect is called and the application subscribes to channel "news"
    Then the client emits "connected" over transport "websocket", with a client id the double gave
    And the subscription reaches state "subscribed"
    When the double publishes {"n":1} to channel "news"
    Then the subscription receives a publication with data {"n":1} and offset 1
    And the double received a connect command, then a subscribe command for "news", and no other command

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: The server double recovers publications missed during a reconnect
    Given a server double
    And a client built with the double's endpoint and connector and a minimum reconnect delay of 200 ms, connected, with a recoverable subscription to "news" subscribed
    And the double published {"n":1} to channel "news", which the subscription received
    When the double disconnects the client with code 3001 "shutdown"
    And the double publishes {"n":2} and then {"n":3} to channel "news" while the connector has been called once
    Then the subscription emits "subscribing", then "subscribed" with recovered true
    And the subscription receives the publications {"n":2} at offset 2 and {"n":3} at offset 3, in that order, each exactly once
    And the double received a second subscribe command for "news", asking to recover from offset 1

  @D
  Scenario Outline: The server double answers calls
    Given a server double whose RPC handler answers method "echo" with the data it is given, and any other method with the error 104 "method not found"
    And a client built with the double's endpoint and connector, connected, with a subscription to "news" subscribed
    And the double published {"n":1} to channel "news"
    When the application <call>
    Then <outcome>

    Examples:
      | call                                                 | outcome                                                                                 |
      | calls RPC "echo" with {"a":1}                        | the call returns {"a":1}                                                                |
      | calls RPC "other" with {"a":1}                       | the call fails with code 104 and message "method not found"                             |
      | publishes {"n":2} to channel "news"                  | the publish succeeds and the subscription receives a publication {"n":2} at offset 2    |
      | requests the history of "news" with limit -1         | the result holds the publication {"n":1} at offset 1, and its offset is 1               |
      | requests the history of "news" with limit 0          | the result holds no publication, and its offset is 1                                    |
      | requests the presence of "news"                      | the result holds one client, with the client id of the client's "connected" event       |
      | requests the presence stats of "news"                | the result counts 1 client and 1 user                                                   |

  @D
  Scenario Outline: The server double refuses as the test asks
    Given a server double that <refusal>
    And a client built with the double's endpoint and connector
    When connect is called and the application subscribes to channel "secret"
    Then <outcome>

    Examples:
      | refusal                                                                  | outcome                                                                                                                                  |
      | refuses the next connect with the temporary error 100 "internal server error" | the client emits "connecting", an "error" event of kind connect with code 100, then "connected", and the double received two connect commands |
      | refuses subscriptions to "secret" with the error 103 "permission denied"  | the client is "connected" and the subscription emits "unsubscribed" with code 103                                                        |

  @D
  Scenario Outline: The server double disconnects the client with a code
    Given a server double
    And a client built with the double's endpoint and connector and reconnect delays from 50 ms to 200 ms, connected
    When the double disconnects the client with code <code> "<reason>"
    Then the client emits "<state>" with code <code> and reason "<reason>"
    And after 1 s the connector has been called <calls> times and the double has <open> connection open

    Examples:
      | code | reason      | state        | calls | open |
      | 3001 | shutdown    | connecting   | 2     | 1    |
      | 3501 | bad request | disconnected | 1     | 0    |

  @D
  Scenario: The server double grants server-side subscriptions and sends messages
    Given a server double that grants the server-side subscription "personal:1" at connect
    And a client built with the double's endpoint and connector
    When connect is called
    Then the client emits a server-side "subscribed" event for channel "personal:1"
    When the double publishes {"n":1} to channel "personal:1"
    Then the client emits a server-side publication of "personal:1" with data {"n":1} and offset 1
    When the double sends the message {"m":1}
    Then the client emits a "message" event with data {"m":1}

  @A
  Scenario: The server double and Centrifugo answer alike
    Given a Centrifugo server whose channel "news" keeps history, allows recovery and accepts publishes from clients, and a server double
    And for each of them, a client with a minimum reconnect delay of 1 s and a connection token for user "u1", connected to it, with a recoverable subscription to "news" subscribed
    When each client publishes {"n":1} and then {"n":2} to channel "news"
    And each client requests the history of "news" with limit -1
    And each server disconnects its client with code 3001 "shutdown", Centrifugo through its server API
    And each server publishes {"n":3} to channel "news" before its client reconnects
    Then both clients emit the same client and subscription events, with the same codes, in the same order, "subscribed" with recovered true included
    And both subscriptions receive the publications {"n":1}, {"n":2} and {"n":3} at offsets 1, 2 and 3
    And both history results hold {"n":1} at offset 1 and {"n":2} at offset 2, with offset 2
```

The steps of "The server double and Centrifugo answer alike" compare everything except epochs and client ids, which differ by design. "Before its client reconnects" is checked: the double's connector has been called once, and Centrifugo has had no new connection.

**Exceptions introduced:** one divergence from centrifuge-js 63ed73e. Its `websocket`, `fetch`, `readableStream` and `eventsource` options replace the constructors of every transport (`centrifuge.ts:1070-1100`); ferrifuge's connector carries the websocket only (D3, D4; out of scope).

---

## Block 38 — Typed JSON helpers

**Goal:** an application whose messages are serde types publishes, calls RPC and reads publications as those types, without encoding and decoding `data` by hand. A failed typed call says whether it was never sent, refused by the server, or answered with data the type does not accept. Source: parity item 18, design A ("Planned after Block 19": the helpers are always available, with no `serde` feature).

Value clause: As an app developer whose messages are serde types, I want to publish, call RPC and read publications as those types, so that I do not encode and decode `data` by hand on every call and publication, and a failed call tells me whether it was never sent, refused, or answered with data my type does not accept.

**Dependencies:**
- Blocks 0 (raw bytes payloads, `async fn` returning `Result`) and 7 (Protobuf);
- Blocks 9 (publications), 12 (server-side publications) and 13 (publish, RPC, history, their waiting, timeout and failure rules);
- Blocks 20 (changelog), 21 (redacting `Debug`), 34 (table of codes) and 35 (no version bump for an additive change, D5).

**Out of scope:**
- `send_json`.
- Typed decoding of `ClientEvent::Message`, of `Connected` and `Subscribed` data, of connect data, `get_data` and `set_data`. `Message(Vec<u8>)` is raw bytes with no type of ferrifuge's to carry a method; its documentation points to `serde_json::from_slice`.
- `history_json`: history publications decode through `Publication::json`.
- A `serde` feature (already decided).
- Formats other than JSON (MessagePack, typed prost messages).
- Refusing the helpers under the Protobuf protocol.
- Checking that raw JSON given to `publish` or `rpc` is valid.
- Treating empty RPC data as `null`.
- Re-exporting `serde_json`: Cargo unifies its 1.x versions.

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):

- **D1 Surface.** Four helpers, the four that centrifuge-rust documents (`docs/api.md` at 88d6edf, lines 38-40, 115 and 161; clean room: its docs only):
  - `Publication::json<'de, T: serde::Deserialize<'de>>(&'de self) -> Result<T, serde_json::Error>`, in the core, in every build. Borrowing lets `T` hold a `&str` or a `&serde_json::value::RawValue`. History publications and server-side publications decode through it.
  - `Client::publish_json<T: serde::Serialize + ?Sized>(&self, channel: impl Into<String>, data: &T)` and `Subscription::publish_json<T: Serialize + ?Sized>(&self, data: &T)`, returning `impl Future<Output = Result<(), JsonCallError>> + Send + 'static`.
  - `Client::rpc_json<Req: Serialize + ?Sized, Resp: serde::de::DeserializeOwned>(&self, method: impl Into<String>, data: &Req)`, returning `impl Future<Output = Result<Resp, JsonCallError>> + Send + 'static`.
- **D2 Errors.**
  - `Publication::json` returns `serde_json::Error`: one cause, and serde_json is 1.x (Rust API Guidelines C-STABLE), as centrifuge-rust's signature (`docs/api.md:161`).
  - The call helpers return `JsonCallError`, `#[non_exhaustive]`, in `src/call.rs`:
    - `Encode(serde_json::Error)`: the value did not encode; nothing was sent;
    - `Call(CallError)`: the call failed as the untyped one does (Block 13), its code, message and `temporary` flag kept;
    - `Decode { error: serde_json::Error, data: Vec<u8> }`, itself `#[non_exhaustive]`: the server answered, and its result data did not decode as `Resp`. The RPC already ran on the server, so this must not look like a failed call that is safe to retry.
  - Sources: aws-smithy-runtime-api 1.20.0 `SdkError` (`ConstructionFailure`, "the request failed during construction. It was not dispatched over the network"; `ResponseError` with `raw()`); reqwest 0.13.5 `Error::is_builder` and `is_decode` (`error.rs:97`, `:196`). A `CallError` with a client-side code cannot hold a `serde_json::Error`, as `CallError` derives `Clone` and `Eq` (`src/call.rs:239`).
  - `JsonCallError` implements `Display` and `std::error::Error` (with `source`) and `From<CallError>`. Its `Debug` shows `Decode`'s data as its length (Block 21 D3). serde_json's messages may quote fragments of the data: ferrifuge never logs them, and the documentation warns applications.
- **D3 Encoding before the call.** The helper encodes its value with `serde_json::to_vec` before it registers the call, then makes the untyped call (`publish`, `rpc`), which registers when the method returns (Block 13). On an encode failure the future resolves at once with `Encode` and nothing is sent, as reqwest's `RequestBuilder::json` keeps its error for `send` (`request.rs:466-483`). The value is borrowed only while the method runs, so the futures stay `Send + 'static` whatever `T`, `Req` and `Resp` are: a borrowed, non-`Send` `Req` and a `Resp` neither `Send` nor `'static` compile on rustc 1.88 and 1.99 (edition 2024; checked by the reviewer). A compile test pins that.
- **D4 Protobuf.** The helpers work under the Protobuf protocol too: `data` is then the JSON bytes, which a JSON client receives unchanged. The documentation says so; centrifuge-rust: the helpers are "the same in both protocols" (`docs/spec.md:480`). `VERIFY:` that Centrifugo v6.10.1 forwards a Protobuf client's JSON bytes to a JSON client byte for byte.
- **D5 An empty RPC result.** No special case, as reqwest's `Response::json` has none: an RPC answered with no data gives a `Decode` error unless `Resp` accepts empty input. The documentation says to use `rpc` for an RPC that returns no data.
- **D6 Documentation and release.**
  - Each helper has a doc-test (C-EXAMPLE; Block 37 D2): the `Client` and `Subscription` ones `no_run`; the `Publication` one written as a function taking `&Publication`, so it builds without `test-util`.
  - Each has an `# Errors` section (clippy pedantic `missing_errors_doc`).
  - The README's calls section shows `publish_json` and `rpc_json`.
  - Additive: a changelog entry under `Unreleased`, "Added" (Block 20 D4), and no version bump (Block 35 D5).

The steps use a test type `Note { text: String, n: u32 }`, with `serde`'s derive and its default field order, and a type whose `Serialize` fails. "Data exactly" compares bytes; "equal as JSON" compares parsed values. "The fake server has received one command since" counts every command the fake server read after the failed helper was called.

```gherkin
Feature: Typed JSON helpers

  @A
  Scenario Outline: A typed publication reaches subscribers as the JSON its type encodes to
    Given two clients subscribed to channel "news"
    When the first client publishes the note {text: "hi", n: 1} as JSON through its <publisher>
    Then the second client's subscription receives a publication with data exactly {"text":"hi","n":1}

    Examples:
      | publisher    |
      | client       |
      | subscription |

  @A
  Scenario: A publication from the server API decodes as the application's type
    Given a client subscribed to channel "news"
    When the server API publishes {"n":1,"extra":true,"text":"hi"} to channel "news"
    Then the subscription receives a publication that decodes as the note {text: "hi", n: 1}

  @A
  Scenario: A typed publication from a Protobuf client reaches a JSON client
    Given a client configured with the Protobuf protocol and a client configured with the JSON protocol, both subscribed to channel "news"
    When the Protobuf client publishes the note {text: "hi", n: 1} as JSON
    Then the JSON client's subscription receives a publication with data exactly {"text":"hi","n":1}

  @B
  Scenario: A typed RPC sends JSON and decodes the proxy's result
    Given a connected client and an RPC proxy answering method "sum" with {"sum":3}
    When the client calls RPC "sum" as JSON with the terms {a: 1, b: 2}, expecting a sum
    Then the RPC proxy received data equal as JSON to {"a":1,"b":2}
    And the call returns the sum {sum: 3}

  @D
  Scenario Outline: A typed RPC tells its failures apart
    Given a client connected to the fake server that answers RPC "sum" <answer>
    When the client calls RPC "sum" as JSON with the terms {a: 1, b: 2}, expecting a sum
    Then the call fails with <error>

    Examples:
      | answer                                                   | error                                                    |
      | with {"sum":"three"}                                     | a decoding error holding the result data {"sum":"three"} |
      | with no data                                             | a decoding error holding empty result data               |
      | with a temporary error with code 1000 and message "busy" | a call error with code 1000, message "busy", temporary   |

  @D
  Scenario Outline: A value that cannot be encoded fails the call at once and sends nothing
    Given a client connected to the fake server with a subscription to channel "news"
    When the client calls <helper> with a value whose serialization fails
    Then the call fails at once with an encoding error
    When the client publishes {"n":2} to channel "news"
    Then the fake server has received one command since, a publish with data {"n":2}

    Examples:
      | helper                           |
      | publish_json to channel "news"   |
      | publish_json on its subscription |
      | rpc_json with method "sum"       |

  @D
  Scenario: A typed call is made when the method returns
    Given a client connected to the fake server
    When the client publishes the note {text: "hi", n: 1} as JSON to channel "news" and holds the future without polling it
    Then the fake server receives a publish command with data exactly {"text":"hi","n":1}

  Scenario Outline: A publication's data decodes as the application's type, or says why not
    Given a publication whose data is <data>
    When it is decoded as a note
    Then <outcome>

    Examples:
      | data                    | outcome                                       |
      | {"text":"hi","n":1}     | it gives the note {text: "hi", n: 1}          |
      | {"text":"hi","n":"one"} | it fails with a serde_json data error         |
      | not json                | it fails with a serde_json syntax error       |
      | the bytes ff 00 fe      | it fails with a serde_json syntax error       |
      | nothing                 | it fails with a serde_json end-of-input error |
```

**Exceptions introduced:** none.

---

## Block 39 — A publications-only stream

**Goal:** an application that only consumes a channel's publications takes a stream of just those, without matching lifecycle events by hand, while delivery, take-over and recovery keep Block 15's guarantees. Source: parity item 13, design A ("Planned after Block 19").

Value clause: As an app developer who only consumes a channel's publications, I want a stream of just its publications, so that I do not match lifecycle events by hand, while delivery, take-over and recovery keep Block 15's guarantees.

**Dependencies:** Blocks 9 (subscriptions), 11 (recovery, the gap signal), 13 (join and leave), 15 (delivery), 33 (`close()`) and 35 (`Publication.channel`, no version bump for an additive change, D5).

**Out of scope:**
- A client-level publications stream for server-side subscriptions: item 13 is per subscription, and such a view would also hide `connected` and `disconnected`.
- Independent streams of each kind, active together (design B; centrifuge-rust `docs/spec.md` §9.4, "Each kind has one active stream per object"): ferrifuge keeps one active stream per object (D1).
- Signalling a gap in the item type, such as a `Lagged` item in the style of tokio-stream's `BroadcastStream`: design A chose a documentation warning (D4).
- `FusedStream`, for either stream, so that `publications()` stays consistent with `events()`.
- Typed publication streams: Block 38's `Publication::json` decodes each item.
- Any change to the sans-IO `machine` API: no new `Input`.
- Handlers: Block 41's handler takes over this stream like any other.

**Decisions** (reviewer, 2026-11-08, delegated by the owner, after a three-amigos round table on the draft):

- **D1 One active stream.** `Subscription::publications(&self) -> impl Stream<Item = Publication> + Send + Unpin + use<>` is the subscription's one active stream (Block 15, "One active stream per object"). `events()` and `publications()` take over from each other: either call ends the earlier stream, whose next item is its end (`None`, and `None` on every later poll), and every event it had not taken moves to the new stream, in order. This diverges on purpose from centrifuge-rust, whose kinds each have their own active stream (§9.4): one outbox per object keeps "in order, one message at a time" unchanged.
- **D2 The events it skips.** The other events (subscribing, subscribed, unsubscribed, error, join, leave) are taken when the stream passes them while being polled, not when they are queued. "In order, one message at a time" and withdrawal hold unchanged, and a later `events()` that takes over still gets a skipped `subscribed` in place. Only publications move positions, so exactly-once on recoverable channels is unchanged. An unread `publications()` stream holds the connection as an unread `events()` does (Block 15, head-of-line), and the documentation says so.
- **D3 Its end.** As `events()`: it ends when the subscription is removed, when the client is closed or dropped, or when it is superseded. It does not end on `unsubscribed`: it continues across unsubscribe and subscribe.
- **D4 Item and documentation.** The item is `Publication`, which already carries `channel` (Block 35c). The rustdoc and the README's "Subscriptions" section say what the view hides:
  - `subscribed` reporting `was_recovering` true and `recovered` false, the one signal of a gap on a recoverable channel without `get_state` (with `get_state`, a failed recovery reloads state instead, Block 11);
  - `unsubscribed`: after a permanent unsubscribe the stream goes silent but does not end;
  - `error` events.

  They also warn that an `events()` call elsewhere ends this stream, and that dropping it counts its held events taken (Block 15, "No stream, no hold-up").
- **D5 Implementation.** `events()` with a filter that keeps the publications (`futures_util::StreamExt::filter_map`), which is `Send`, `Unpin` and cancel-safe; no change to the outbox, the event stream or the core.
- **D6 Release.** Additive: a changelog entry under `Unreleased`, "Added" (Block 20 D4), no version bump (Block 35 D5), and a `no_run` rustdoc example (C-EXAMPLE).

**Structural acceptance:** the rustdoc of `Subscription::publications` names the gap signal it hides (D4).

```gherkin
Feature: Publications-only stream

  @D
  Scenario: The publications stream yields only the subscription's publications
    Given a client connected to the fake server, with a subscription to channel "news" with the join_leave option whose publications stream was taken before subscribe is called
    And the subscription is subscribed
    When the fake server sends a publication at offset 1, a join, a leave, then a publication at offset 2 to "news"
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    And the fake server sends a publication at offset 3 to "news"
    Then the publications stream yields exactly the publications at offsets 1, 2 and 3, in order

  @D
  Scenario: Events the publications stream skips hold the connection until it is read
    Given a client connected to the fake server, subscribed to channel "chat", whose subscription's stream is read
    And a subscription to channel "news" whose publications stream was taken before subscribe is called and is not read
    When the application subscribes to "news"
    And the fake server has answered the subscribe command for "news", then sent a publication to "chat"
    Then 1 s later the stream of "chat" has yielded no publication
    And when the application reads the publications stream of "news" for 100 ms, it yields nothing
    And then the stream of "chat" yields that publication

  @D
  Scenario: Publications the publications stream has not yielded are recovered on the next connect
    Given a client subscribed to recoverable channel "news" on the fake server, whose publications stream was taken before subscribe is called
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the publications stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the publications stream yields the publications at offsets 2 and 3, in order
    And no publication is yielded twice

  @D
  Scenario: publications() takes over the subscription's events stream
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When the application calls the subscription's publications()
    Then the first stream's next item is its end
    And the publications stream yields the publications at offsets 1 and 2

  @D
  Scenario: events() takes over the subscription's publications stream
    Given a client connected to the fake server, with a subscription to channel "news" whose publications stream was taken before subscribe is called and is not read
    And the subscription is subscribed
    And the fake server has sent publications at offsets 1 and 2 to "news"
    When the application calls the subscription's events()
    Then the publications stream's next item is its end
    And the new stream yields "subscribing", "subscribed", and the publications at offsets 1 and 2, in order

  @D
  Scenario: The publications stream outlives unsubscribe, which withdraws what it had not yielded
    Given a client subscribed to channel "news" on the fake server, whose publications stream was taken once it was subscribed and is not read
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When unsubscribe is called on the subscription
    And subscribe is called on the subscription
    And the fake server answers the subscribe command, then sends a publication at offset 3 to "news"
    Then the publications stream yields the publication at offset 3, and no other

  @D
  Scenario Outline: The publications stream ends with its subscription or its client
    Given a client connected to the fake server, with a subscription to channel "news" whose publications stream was taken
    When <cause>
    Then the publications stream's next item is its end

    Examples:
      | cause                       |
      | the subscription is removed |
      | the client is closed        |
      | the client is dropped       |
```

**Exceptions introduced:** none.
