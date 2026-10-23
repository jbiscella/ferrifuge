# Ferrifuge handoff (2026-10-23)

Written for the next session, on any machine, to resume development without losing context. This branch is not meant to be merged.

## How to resume
1. Read `CLAUDE.md` (spec, owner decisions) and this file.
2. Check merged PR #38 for late Codex findings (it was merged while Codex was still reviewing). Route any finding to the Block 10b PR, and put wording changes to the owner.
3. Continue Block 10a from draft PR #39 (branch `b10a/unsubscribe-codes`). Its description lists the red and green steps left.
4. Then 10b, then Blocks 11, 12 and 13.

## Status
- `main`: Blocks 1–9 and 14 (footprint, performance, fleet behaviour, CI on Windows/macOS, fuzzing, interop, semver). Also: event streams that borrow nothing, `ClientError` implementing `std::error::Error`, `Machine::with_seed` behind the `test-util` feature, and the Block 10/14 spec amendments (#28, #38).
- **Block 10**, split by the owner:
  - **10a:** wire prerequisites, server unsubscribe codes, resubscribe backoff options. The prerequisites are done on draft PR #39.
  - **10b:** subscription tokens, sub_refresh, retries, the denial, empty-token and error-109 paths, token before `get_data`, cancellation.
- **Left:** Block 10 (28 scenarios: 7 in 10a, 21 in 10b), 11 (20), 12 (16), 13 (34). After Block 13 adds publishing, add the Ferrifuge → centrifuge-js direction to `tests/interop.rs`.

## Working rules (owner decisions)
- **Workflow:** follow the increments in CLAUDE.md with the `incremental-implementation-workflow` skill (`.claude/skills`). Commits go prereq → red → green → refactor, tagged `[<id>][phase]`. One PR per block or sub-increment.
- **Autonomy:** proceed with development without asking. Ask only for spec wording, public API or scope decisions.
- **Merging:**
  - Rebase merge only (`gh pr merge N --rebase --delete-branch`): `main` requires linear history. Update branches with `git rebase origin/main`, never by merging main.
  - Implementation PRs merge when CI is green and Codex reports no findings on the head commit.
  - Spec PRs (CLAUDE.md text): fix the current Codex round, then merge on green CI. Later spec findings go to the PR of the block that implements them. Wording changes to approved text need the owner's approval.
  - Before merging, list every unanswered Codex review thread on the PR, all commits included. About 5 minutes after merging, check again for late findings. Never drop a finding.
- **Codex** reviews each push automatically. Do not post "@codex review". If a review hangs (over 80 minutes "Running"), report it and ask.
- **Spec conflicts:** on a mismatch between CLAUDE.md and reality, check the reference implementation (centrifuge-js) and the specs (client SDK spec, `client.proto`) first. If they agree, correct CLAUDE.md and cite them; ask only when they disagree. Precedence: spec > `client.proto` > centrifuge-js.
- **Shell:** use `set -eo pipefail` in chained commands, so a failure is never hidden.

## Block 10 constraints (owner)
- **Version 0.2.0:** bump when cargo semver-checks requires it, which will be at 10b's new `Action`, `Input` and `SubscriptionTimer` variants. `SubscriptionOptions` is already `#[non_exhaustive]`.
- **`Action` stays exhaustive** on purpose: a new action must break adapters at compile time.
- **Subscription tokens** never appear in `Debug` output or error messages. Add a unit test, as for connection tokens (the wire uses `Secret`).
- **Wire:** `Method::SubRefresh` is `#[expect(dead_code)]` outside tests. Remove the attribute once 10b sends it.

## Tools (development machine)
- Rust stable (MSRV 1.88), plus nightly for coverage-guided fuzzing.
- Docker, for the @A/@B scenarios, the perf and interop tests, and `centrifugo/centrifugo:v6.10.1`.
- Node 22 and `npm ci --prefix tests/interop --ignore-scripts`, for the interop test.
- Valgrind and `cargo install gungraun-runner --version 0.20.0 --locked`, for the instruction-count benchmark.
- `cargo install cargo-semver-checks --version 0.51.0 --locked`; run it with `--default-features`.
- `cargo install cargo-bolero --version 0.13.5 --locked`, for fuzzing.
- `cargo-deny`.
- The README's Development section lists the commands.

## Pending owner decisions: external review, Parts C and D
Briefs C1–C7 (runtime defects) and the Part D triage table follow. No option has been chosen for any of them yet.

---


Written for the reviewer of the Block 10 review prompt (Parts A–D).

## Status of Parts A and B
- **Part A:** PR #28 (Block 10 decisions A1, plus scenarios A2 and A3, verbatim). Awaiting CI and Codex.
- **Part B:** PR #29 (B1–B4). Each item's broken-implementation evidence is in the PR description. Awaiting CI and Codex.
- **PR #27** (Block 10 `Verified` line) is still open. The owner interrupted its merge.

## Where my reading differs from the prompt (Parts C and D)
- **C1:**
  - `disconnect()` is not lost entirely. The state change and the Disconnected event still happen at once (client.rs:91-101, 319-323). What never runs is closing the socket and the driver exiting after drop.
  - A stall needs the kernel send buffer to fill. The client sends only connect, subscribe, unsubscribe, refresh and pong, so in practice it takes large subscribe data or many subscriptions.
  - HTTP transports are not affected.
- **C2:** the "close code ≥ 3000" path does not apply to Ferrifuge as it stands:
  - HTTP transports carry no close code;
  - a websocket is kept as soon as it opens (machine.rs:687-689).

  It would apply if C3 is fixed by changing the code. The connect-reply and Disconnect-push claims are confirmed:
  - centrifuge.ts:1585-1590 (comment on 1586, `_transportWasOpen = true` on 1589);
  - centrifuge.ts:2640-2645.
- **C3:** the spec decision (commit cc118b2) predates the code (40f2341). No "Exceptions introduced" note records the departure. centrifuge.ts:1244-1248 shows that keeping an opened websocket is deliberate upstream ("as in 5.7.4").
- **C4:** the reconnects follow the backoff, so this is not a tight loop. That Centrifugo actually closes with 1009 for oversized client frames is not verified. centrifuge.ts:1310-1313 is confirmed.
- **C5:** the same Given step is used by two other scenarios (block8 feature lines 34 and 93; block9 line 78), so the fix needs a new Given, not an edit.
- **D:** none refuted.
  - Confirmed: D1, D4, D6.
  - Partially confirmed: D2, D3, D5.
  - Only D1 and D2 are runtime defects; they have briefs C6 and C7 below. D3–D6 are test or CI integrity issues, with no spec decision needed.

---


## C1. A stalled websocket write freezes the driver

Verdict: confirmed, with two corrections.
- client.rs:438-441 performs queued actions before the `select!` (451-486). `Action::Send` (538-551) awaits `Transport::send` (639-648), which awaits `self.stream.send(message)` (transport.rs:70). No timeout wraps any of these.
- client.rs:661 runs `tokio::spawn(websocket.close())`. `close` (transport.rs:95-97) awaits `stream.close(None)` with no timeout.
- Correction 1: `disconnect()` still moves the state and emits the event at once (client.rs:91-101, 319-323). The steps that never run are the socket close and the driver's exit after drop (442-449).
- Correction 2: the stall needs the kernel send buffer to be full. The client only sends connect, subscribe, unsubscribe, refresh and pong (it has no publish API), so the realistic trigger is large subscribe data or many subscriptions. HTTP transports are not affected: `send` does not block, and each request is limited by `Config::timeout` (http.rs:119-130).

### 1. Context
The tokio adapter's driver performs actions one at a time. A websocket write that never completes stops it from polling timers, inbound frames and API actions.

### 2. Source artifacts
- src/adapter/client.rs:541-543 `&& open.send(frame, frame_type).await.is_err()`
- src/adapter/transport.rs:70 `self.stream.send(message).await.map_err(|e| e.to_string())`
- src/adapter/client.rs:661 `tokio::spawn(websocket.close());`
- CLAUDE.md:301 (Block 4): "if no PING arrives within ping interval + `maxServerPingDelay`, the connection is considered broken and re-established."
- The spec and centrifuge-js define no write timeout. Browser `WebSocket.send` never blocks.

### 3. Ambiguity statement
The ambiguity is how long a websocket write may block, and what the client does when that limit runs out.

### 4. Why it blocks progress
A fix needs a timeout value, a resulting state and code, and a Gherkin scenario. None of these is in the spec.

### 5. Options considered
1. Wrap the websocket `send` and `close` in `Config::timeout`. On expiry, treat the write as a closed transport (connecting, code 1), as the HTTP emulation requests already do. Supported: detection and teardown. Still broken: timers and `disconnect()` can wait up to the timeout. Cost: low.
2. Write from a separate writer task through a queue, so the driver never awaits a write. A write timeout lives in that writer. Supported: everything, with no delay. Broken: none, but the queue needs a bound. Cost: high.
3. Poll the pending write inside the `select!`, and queue later sends behind it. Same coverage as option 2. Cost: medium, and send ordering needs care.
4. Keep the current behaviour and document it. Cost: none, but the Block 4 rule is not met.

### 6. Recommendation and why
Option 1. It is the smallest change that restores the Block 4 rule, and it reuses an existing option and the HTTP precedent.

### 7. Question to the reviewer
Choose 1, 2, 3 or 4. Criterion: is a bounded delay of up to `Config::timeout` on timers and `disconnect()` acceptable (choose 1), or must the driver never wait on a write (choose 2 or 3)? If you choose 1–3, also say whether the event is connecting code 1 or an error event with centrifuge-js `transportWriteError` (10, codes.ts:11).

### 8. Out of scope for this question
- HTTP transports.
- An unbounded `events()` queue (D1).
- TCP keepalive tuning.

Reproducing test:
- Unit test in src/adapter/client.rs, `stalled_websocket_write_does_not_block_disconnect`. It uses a local `tokio_tungstenite::accept_async` server that answers connect and then stops reading, and a subscription with 16 MiB of data.
- @D scenario "A websocket write the server never reads is detected". It needs a new FakeServer control that stops reading.

## C2. An emulated transport answered by the server is abandoned

Verdict: confirmed, on both the Ferrifuge and the centrifuge-js side.
- Ferrifuge: `disconnect` calls `attempt_failed()` for any reconnecting teardown (machine.rs:876-880). For emulated transports, `transport_opened` returns before `choose()` (683-686), so only `connected()` chooses one (822).
  - A temporary or 109 connect error goes through 806-815 and 856-863, then `attempt_failed`. Delay is zero while `in_first_round` (911-918).
  - A Disconnect push takes the same path (773-774).
- centrifuge-js keeps the transport whenever the server answered:
  - Connect error in a reply: centrifuge.ts:1585-1590 (`replied` means `rejectCtx.next !== undefined`, 1407).
  - Disconnect push: 2640-2645.
  - Close code ≥ 3000: 1321-1324.
  - The following reconnect then uses the backoff delay (1611-1614).
- Correction: the close-code path cannot apply to Ferrifuge's emulated transports. They never carry a close code (http.rs:155-176), and a websocket is chosen on open (machine.rs:689).

### 1. Context
In a fallback list, Ferrifuge moves to the next transport even after the server answered on the current one. centrifuge-js keeps the current one.

### 2. Source artifacts
- machine.rs:876-880 `if reconnect { self.transports.attempt_failed(); }`
- centrifuge.ts:1585-1590 `if (replied) { ... this._transportWasOpen = true; }`
- centrifuge.ts:2645 `this._transportWasOpen = true;`
- CLAUDE.md:494: "transports are tried in order during the initial handshake only, until one succeeds; reconnects then use only that transport;"

### 3. Ambiguity statement
The ambiguity is whether "succeeds" (CLAUDE.md:494) means a connect result, or any server answer (an error reply or a Disconnect push).

### 4. Why it blocks progress
The fix's direction depends on the meaning of "succeeds". With kinds `[HttpStream, Sse]`, today a temporary error moves the client to sse permanently, with zero delay.

### 5. Options considered
1. A server answer chooses the transport: call `choose()` on a connect error reply and on a Disconnect push. This matches centrifuge-js. It applies the backoff delay instead of zero. Cost: low.
2. Keep the current code, and amend CLAUDE.md:494 to say "a connect result". Supported: transport-specific server rejections. Broken: a transient server error moves the client to another transport for good. Cost: documentation only.
3. Retry the same transport once after a server answer, but do not choose it. Cost: medium, and it adds state.

### 6. Recommendation and why
Option 1. A server answer proves the transport works. Block 0 makes centrifuge-js the reference where the spec is silent.

### 7. Question to the reviewer
Choose 1, 2 or 3. Criterion: does an error reply or a Disconnect push prove the transport (choose 1), or only a connect result (choose 2)?

### 8. Out of scope for this question
- The websocket open rule (C3).
- Non-temporary errors, which already end in disconnected.

Reproducing test:
- @E scenario "A transport the server answered is kept". It uses a Machine with `[HttpStream, Sse]`, a temporary connect error, and expects the next `OpenTransport` to be HttpStream after the backoff delay.
- Optional @B variant: `ProxyBackend::disconnect_next(4000, ..)`. Centrifugo's http_stream behaviour here is not verified.

## C3. A websocket that opens without a connect reply never falls back

Verdict: confirmed.
- machine.rs:687-689 calls `choose()` on websocket open.
- `ConnectTimeout` (583-587) leads to `transport_failed` (712-720), then `disconnect`. `attempt_failed` returns early because the transport is chosen (transports.rs:47-50), so the client retries the websocket forever.
- The decision is still at CLAUDE.md:499.
- The only scenario is "An HTTP stream that never answers the handshake falls back" (block8 feature:53).
- New finding: the spec decision (commit cc118b2) is an ancestor of the code commit (40f2341). The code departed from a decision that already existed, and no "Exceptions introduced" note records it.
- centrifuge-js does keep an opened websocket: centrifuge.ts:1246-1248, and `failTransport` advances only when `!_transportWasOpen` (1195-1197).

### 1. Context
A proxy that completes the websocket upgrade but drops frames stops the client from ever connecting through http_stream or sse.

### 2. Source artifacts
- machine.rs:687-689 `// An open websocket keeps its transport, even if the connect reply is slow (centrifuge-js onOpen). self.transports.choose();`
- CLAUDE.md:499: "an attempt that opens but gets no connect reply within `Config::timeout` fails like a refused one, and the next transport is tried;"
- CLAUDE.md:54: "Precedence on conflict: spec > `client.proto` > centrifuge-js behaviour."
- centrifuge.ts:1246-1247: "A socket that opened keeps its transport, as in 5.7.4: a slow first connect reply must not move the client to a fallback transport for good."

### 3. Ambiguity statement
The ambiguity is whether an opened websocket that gets no connect reply moves the client to the next transport (CLAUDE.md:499) or keeps the websocket (centrifuge-js).

### 4. Why it blocks progress
The code and an explicit owner decision contradict each other. Either one must change.

### 5. Options considered
1. Change the code: do not choose a websocket on open, only on a server answer (see C2). Supported: the Block 8 value clause (networks where websocket is blocked). Broken: a first connect reply slower than 5 s moves the client to the fallback for good. Cost: low. Close codes ≥ 3000 before the reply then also need the C2 rule.
2. Amend CLAUDE.md:499 to cover emulated transports only, and record the exception. The opposite trade-off. Cost: documentation only.
3. Retry the websocket once, then fall back. Cost: medium.

### 6. Recommendation and why
Option 1. The decision predates the code, Block 0 ranks the project's decision above centrifuge-js, and the value clause names exactly this network.

### 7. Question to the reviewer
Choose 1, 2 or 3. Criterion: which matters more, connecting behind a websocket-breaking proxy (choose 1), or keeping a websocket whose server is slow to reply (choose 2)?

### 8. Out of scope for this question
- Timeout values.
- The emulated-transport rules (C2).

Reproducing test:
- @B scenario "A websocket that never answers the handshake falls back". It uses `ProxyBackend::hold_next()` with endpoints [server websocket, server http_stream] and expects "connected" over http_stream within the timeout plus tolerance.
- @E Machine variant of the same test.

## C4. Close code 1009 reconnects forever

Verdict: confirmed.
- machine.rs:723-732 sends every code below 3000 (or no code) to `reconnect_after_transport_closed` (734-740), which gives connecting code 1.
- codes.rs has no message-size-limit code.
- centrifuge-js maps 1009 to disconnected `messageSizeLimit` (3) with `needReconnect = false` (centrifuge.ts:1309-1313; codes.ts:30).
- Correction: the reconnect uses the backoff delay, not a tight loop.
- Not verified: that Centrifugo closes with 1009 for oversized subscribe data.

### 1. Context
A server that closes with 1009 ("message too big") makes Ferrifuge reconnect and resubscribe with the same data, indefinitely.

### 2. Source artifacts
- machine.rs:726-730 `Some(code) if code >= 3000 => ...` and `_ => self.reconnect_after_transport_closed(),`
- centrifuge.ts:1310-1313 `if (code === 1009) { code = disconnectedCodes.messageSizeLimit; reason = 'message size limit exceeded'; needReconnect = false; }`
- CLAUDE.md:233 "Out of scope: client-side disconnect code values (codes < 3000)."
- CLAUDE.md:239 "Client-side code values (< 3000): take from centrifuge-js source."
- The spec allows these codes ("Client implementation can use codes < 3000 for client-side specific disconnect reasons") but does not mention 1009.

### 3. Ambiguity statement
The ambiguity is whether 1009 is terminal (disconnected, code 3) or an ordinary transport closure.

### 4. Why it blocks progress
Block 3 puts codes below 3000 out of scope, but also says to take their values from centrifuge-js.

### 5. Options considered
1. Port centrifuge-js: add `disconnected::MESSAGE_SIZE_LIMIT = 3` and make 1009 terminal. Broken: a one-off 1009 needs an app-level `connect()`. Cost: low.
2. Keep reconnecting, and document it. Cost: none, but the loop never ends.
3. Reconnect, but emit an error event first. Cost: low, but the code is Ferrifuge's own.

### 6. Recommendation and why
Option 1. CLAUDE.md:239 already points to centrifuge-js for these values.

### 7. Question to the reviewer
Choose 1, 2 or 3. Criterion: should a message-size violation stop the client (choose 1) or not (choose 2 or 3)?

### 8. Out of scope for this question
- Other codes below 3000.
- Size limits the client enforces itself.

Reproducing test:
- @D scenario "Close code 1009 stops the client", using `FakeServer::close(1009, ..)`.
- Unit test `close_code_1009_is_a_terminal_message_size_limit` next to machine.rs:1457.

## C5. "A transport that worked is kept on reconnect" cannot fail

Verdict: confirmed.
- tests/steps/transports.rs:189-195 configures only one endpoint, so `attempt_failed` wraps back to the same transport.
- New findings:
  - The same step is used by block8 feature:34 and :93 and by block9 feature:78, so a new Given is needed rather than an edit to this one.
  - `next_event_after_transport_errors` (tests/steps/mod.rs:275-286) skips transport errors. An unreachable earlier transport alone therefore proves nothing.

### 1. Context
The scenario passes even if the keep-the-transport logic is removed.

### 2. Source artifacts
- transports.rs:193 `configure(world, vec![(kind, url)]);`
- CLAUDE.md:540-544 (scenario):

```gherkin
Given a client connected over transport "http_stream"
When the server disconnects the client with a reconnect-advised code
Then the client emits "connecting" with that code and reason
And then the client emits "connected" over transport "http_stream"
```

### 3. Ambiguity statement
The ambiguity is which endpoint list and which assertion the scenario must use to prove the transport is kept.

### 4. Why it blocks progress
Changing the Gherkin requires a spec edit in CLAUDE.md Block 8.

### 5. Options considered
1. Use "an unreachable websocket endpoint behind a counting relay, the server's http_stream and sse endpoints", and assert no relay connection after the disconnect. This catches both an advance to sse and a wrap back to the websocket. Cost: medium.
2. Use a later reachable sse only. This catches an advance, but not a wrap. Cost: low.
3. Use an earlier unreachable transport only. This misses the advance, because errors are skipped. Cost: low.

### 6. Recommendation and why
Option 1. It is the only option that fails for both broken behaviours.

### 7. Question to the reviewer
Choose 1, 2 or 3. Criterion: must the scenario fail when the client advances and also when it wraps (choose 1)?

### 8. Out of scope for this question
- Fixing the skipped transport errors in general (D5).

Reproducing test:
- @A, the amended scenario. CountingRelay in `Mode::Down` in front of the websocket.
- Check it against a broken implementation in which `choose()` does nothing.

## C6 (from D1). A kept but unpolled `events()` stream grows without bound

Verdict: confirmed (Part D triage).

### 1. Context
`Client::events()` and `Subscription::events()` hand out the receivers of unbounded channels. Each event is cloned into every live receiver.

### 2. Source artifacts
- src/adapter/client.rs:307-311 `let (sender, receiver) = events::unbounded();`
- src/adapter/subscription.rs:58-74, the same for subscriptions.
- src/adapter/client.rs:127-135 `retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok())`. Only a dropped receiver is pruned.
- CLAUDE.md:34 says only that `events()` returns a `futures::Stream`. No buffering policy is stated.

### 3. Ambiguity statement
The ambiguity is what an event stream does when its consumer stops polling it while still holding it.

### 4. Why it blocks progress
Each fix changes the public API's behaviour, which the spec does not define:
- drop events silently;
- report lag;
- apply backpressure to the driver.

### 5. Options considered
1. Keep it unbounded and document that a stream must be polled or dropped.
   - Cost: documentation only. A forgotten stream keeps every publication's bytes for the client's lifetime.
2. Bound it per stream (for example 1024 events). On overflow, drop that stream's oldest events and deliver a "lagged(n)" item first, as `tokio::sync::broadcast` does.
   - Cost: medium. Adds a stream item variant.
3. Bound it per stream and end an overflowing stream.
   - Cost: low. A slow consumer loses its stream and must call `events()` again.
4. Backpressure: the driver waits for slow streams.
   - Rejected: one idle stream would stall pings and every other consumer.

### 6. Recommendation and why
Option 2. Memory stays bounded, the driver never waits, and the consumer learns that it missed events.

### 7. Question to the reviewer
Choose 1, 2, 3 or 4. Criterion: may a slow consumer lose events (2, 3), or must it get all of them at the cost of unbounded memory (1)? If you choose 2 or 3, also give the bound.

### 8. Out of scope for this question
- Streams that were dropped: they are pruned already.
- The publication history API (Block 13).

Reproducing test: unit test `an_unpolled_event_stream_stays_bounded` in src/adapter/client.rs. Keep a receiver and emit 10 000 events through the fan-out. Assert that the buffered count stays at most the chosen bound (with option 1, document the behaviour instead).

## C7 (from D2). A failed TLS setup is cached until restart

Verdict: partially confirmed (Part D triage). The mechanism is real, and the impact is narrow.

### 1. Context
The TLS client configuration is built once per process and cached, including when building it fails.

### 2. Source artifacts
- src/adapter/transport.rs:137-155:
  - `static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>>`;
  - `if added == 0 { return Err("no platform root certificates found".to_owned()); }`.
- src/adapter/http.rs:206-228 caches the `reqwest::Client` result the same way.
- tests/bdd.rs:243-262 re-execs the runner with `SSL_CERT_FILE` because the roots are read once per process.

### 3. Ambiguity statement
The ambiguity is whether a failed TLS setup should be retried on the next connect attempt, or stay failed for the life of the process.

### 4. Why it blocks progress
The spec (Block 6) says nothing on when root certificates are loaded. The two choices give different results after a transient failure, for example:
- file descriptors exhausted at the first secure connect;
- the CA bundle replaced during a package update.

In both cases every later `wss://` or `https://` connect fails until restart, while the client keeps retrying with the same cached error.

### 5. Options considered
1. Cache only `Ok`. Retry the build on the next attempt after an `Err`.
   - Cost: low. A deterministic failure (no CA bundle) re-reads the roots on every reconnect, at most once per backoff delay.
2. Keep caching the `Err`, and document "restart after fixing the trust store".
   - Cost: documentation only.
3. Let the application pass its own roots or a `ClientConfig`.
   - Cost: medium. It is an API addition and does not by itself remove the cache.

### 6. Recommendation and why
Option 1. It is cheap, and the deterministic cases are no worse off.

### 7. Question to the reviewer
Choose 1, 2 or 3. Criterion: must the client recover from a transient trust-store failure without a restart (choose 1)?

### 8. Out of scope for this question
- Which roots are loaded (platform or webpki).
- Crypto provider selection (Block 6).

Reproducing test: unit test `a_failed_tls_setup_is_retried`, with the build function injected so its first call fails. Not reproducible through `SSL_CERT_FILE` alone, because the roots are read once per process.

---

## Part D triage (origin/main @ 82b6392)

| Item | Verdict | Evidence (file:line, short) | Impact / note |
|---|---|---|---|
| D1 unbounded `events()` | **Confirmed** | `src/adapter/client.rs:12` `futures_channel::mpsc as events`; `client.rs:48,51` `UnboundedSender`; `client.rs:307-310` `events::unbounded()`; `src/adapter/subscription.rs:58-72` same. Dropped receivers: pruned lazily on the *next* emit via `retain(... unbounded_send(..).is_ok())` (`client.rs:127-135`); subscription senders also dropped on removal (`subscription.rs:227`) and on client close (`client.rs:446-447`). Kept-but-unpolled receiver: send always succeeds, nothing caps or signals lag. Spec silent: `CLAUDE.md:34` only says "returns a `futures::Stream`"; `README.md:105-107` likewise. | A held, unpolled `events()` stream (client or subscription) buffers every event, including publication bytes, for the client's lifetime. Dropped streams are fine (one stale sender until the next event). |
| D2 TLS `Result` cached in `OnceLock` | **Partially** | `src/adapter/transport.rs:137-155` `OnceLock<Result<Arc<ClientConfig>, String>>`, Err cached; `src/adapter/http.rs:206-228` caches `reqwest::Client` Result again (SECURE/PLAIN). Failure sources: `add_parsable_certificates(load_native_certs().certs)` with 0 added -> Err (`transport.rs:143-146`); `with_safe_default_protocol_versions` (`:148-149`, deterministic per build); `reqwest` build (`http.rs:225-226`, deterministic). `rustls-native-certs` 0.8.5 reads `SSL_CERT_FILE`/`SSL_CERT_DIR` from env at call time, else openssl-probe paths; I/O errors are collected, not fatal. Only `wss://`/`https://` are affected (`transport.rs:37-45`, `http.rs:211-216`). `tests/bdd.rs:243-262` re-execs the runner with `SSL_CERT_FILE` because of this once-per-process read. | The mechanism is real, but almost every failure is deterministic for the process (no CA bundle, bad `SSL_CERT_FILE`, provider), so caching the Err is harmless there. Transient cases do exist but are narrow: fd exhaustion (EMFILE) or a CA bundle briefly missing during a package update, at the moment of the first secure connect. Either one poisons all later secure connects until restart, and the client keeps retrying with the same error. A partial root set from a transient I/O error would also be cached as `Ok`. |
| D3 timing flakiness | **Partially** | No `max_concurrent_scenarios` set in `tests/bdd.rs:158-212`, and CI passes no `--concurrency` (`ci.yml:98,128`), so cucumber's default applies: 64 (`cucumber-0.23.0/src/runner/basic.rs:434`). **Upper-bound windows (can false-fail when slow):** `ping.rs:18` TOLERANCE 1 s, used at `ping.rs:98` (3 s + 1 s); `block4_ping_pong.feature:25-28`: maxServerPingDelay 1000 ms with pings every 2 s means 1 s slack per ping over 10 s; `transports.rs:26,28,241`: 2 s + 1 s; `transports.rs:92-101`: Centrifugo pong timeout 1 s over HTTP emulation; `transports.rs:254` and `subscriptions.rs:718`: 5 s + 1 s; `subscriptions.rs:344`: sleep 1 s + 1 s, then expect no publication; `bdd.rs:125`: `EXPIRED_CLOSE_DELAY` 1 s with `TOKEN_LIFETIME` 3 s (`token.rs:16`). **Negative windows (weak, never flaky):** `ping.rs:20,92` 500 ms no-pong; `subscriptions.rs:27,323,754` 500 ms no-publication; `protobuf.rs:116` checks "== count" right after reaching it, so a late third pong is missed (vacuous, not flaky); `subscriptions.rs:615` lower bound (deterministic). **CI:** the last 30 `main` runs are all green (1 `ci` run cancelled as superseded). Across ~107 `ci` runs on all branches there were 3 failures: 39498776793, a real race fixed by `8dde91d`; 39489244723, dependabot's MSRV job; 39836367637, the watchdog caught 12 scenarios hung 184 s. That hang was caused by testcontainers' blocking drop stalling the shared runner future, so the timers of every concurrent scenario stopped firing; it was fixed by `59c1338`. | No short-window timing flake observed. The real risk is the shared runner: one blocking step or a CPU-starved runner (up to 64 scenarios, each @A one with its own Centrifugo container) delays every scenario's timers, and only the upper-bound windows above can then fail. The most exposed is the block4 "stays connected" scenario with 1 s slack per ping. The negative windows are weak, not flaky. |
| D4 HTTP "stays connected" bypasses relay | **Confirmed** (low impact) | `transports.rs:54-56`: HTTP clients connect to the server directly, by design. "Server pings over an HTTP transport" (`block8...feature:91-100`): `transports.rs:100` sets `world.relay = None`, so `ping.rs:69-76` `map_or(0, ..)` always yields 0. "Commands work over an emulated transport" (`feature:40-43`): the before-hook relay exists (`bdd.rs:190-191`), but the client uses `server.sse_url()` (`transports.rs:200`), so the relay sees no connections and the delta is 0. The WebSocket variants do go through the relay (`steps/mod.rs:173-178`). | The `elapsed_new_connections == 0` assertion (`ping.rs:130-133`) is vacuous for HTTP transports. The scenarios still catch a reconnect through `elapsed_events.is_empty()` and the state check (`ping.rs:125-134`), because any transport loss emits `connecting`. Only a silent reconnect would slip through. |
| D5 fallback drops transport errors | **Partially** | `steps/mod.rs:275-286` `next_event_after_transport_errors` discards `ErrorKind::Transport`; used by `connection.rs:235-245` (only asserts the final `transport`) and `transports.rs:65-72`; `transports.rs:253-265` does the same. "Fallback to sse" (`feature:25-30`) passes even if ws and http_stream were never attempted. "Fallback to http_stream" (`feature:4-9`) proves http_stream came before sse (both reachable) but not that websocket was tried. "Every transport unreachable" (`connection.rs:185-233`) checks one error per transport but not their order. | Order *is* asserted, but at the sans-IO core: @E "A failed fallback sequence is retried" (`feature:65-70`, `transports.rs:344-354` asserts opened kinds == configured order). The adapter-level scenarios do not prove attempts or their order, although the `error.transport` field makes that cheap to check. |
| D6 CI gaps | **Confirmed** (latent / low impact) | `ci.yml:41-42` `--no-default-features` is linted by clippy only, never tested. Provider jobs run `cargo test --all-targets` (`ci.yml:97-98`), which excludes doctests (the comment at `ci.yml:130` says so); only the `test` job runs `cargo test --doc` (`:131-132`). MSRV job: `cargo check --all-targets` with default features only (`ci.yml:172-173`). | `src/` has 0 code fences and `lib.rs` includes no README, so no doctests exist today; the provider gap is latent. The README's Rust examples (`README.md:76,116`) are compiled nowhere. The core has no feature-dependent code (`cfg` only at `lib.rs:6,21`), so an untested `--no-default-features` build is at most a compile risk, and clippy already covers that. MSRV is check-only: no test, and `--no-default-features` is not checked. |

## Classification

- **D1: runtime defect, needs a spec decision brief.** The spec (`CLAUDE.md:34`) only says "a Stream" and never sets a buffering policy. Options: keep it unbounded and document that a stream must be polled or dropped; bounded with a lag event (tokio `broadcast`-style); or bounded with drop-oldest. That is an API and behaviour decision for the spec, not a test fix.
- **D2: runtime defect, narrow; a short spec decision brief is optional.** Caching an Err is harmless for deterministic causes, which are most of them. The open question for the spec is whether a failed root load should be retried on the next connect (cache only `Ok`). Low priority, but cheap to decide.
- **D3: test/CI integrity issue (flake risk), not a runtime defect.** No timing flake has been observed. The demonstrated risk is shared-runner stalls under 64-way concurrency, already fixed once (`59c1338`). Mitigations to consider: cap concurrency, widen the block4 1 s ping slack, and strengthen the "no more" check in `protobuf.rs:116`.
- **D4: test integrity issue.** The new-connection assertion is vacuous for HTTP transports. The scenario still detects reconnects through events, so its value is low unless a silent reconnect path matters.
- **D5: test integrity issue.** The adapter-level fallback scenarios do not prove that earlier transports were tried, or in what order. Order is covered only at the sans-IO core (@E). Asserting the discarded `error.transport` sequence would close the gap.
- **D6: CI integrity issue, latent.** There are no doctests yet and the core has no feature-dependent code. Worth adding `cargo test --no-default-features`, `--doc` in the provider jobs, and MSRV `test` (or MSRV `--no-default-features` check) before doctests or `cfg` branches appear. The README examples are never compiled.
