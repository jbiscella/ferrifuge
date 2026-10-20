# CLAUDE.md — Rust Centrifugo client SDK

Status: DRAFT — blocks 0–4 of N. Further blocks to be appended.

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
    Until the upper bound reaches 20000 ms

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

`VERIFY:` the server HTTP API `disconnect` method accepts custom `code` and `reason`; whether it accepts codes outside 4000–4999 is not confirmed. Codes 3000–3999 are therefore tested on @D. Client-side code values (< 3000): take from centrifuge-js source.

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
