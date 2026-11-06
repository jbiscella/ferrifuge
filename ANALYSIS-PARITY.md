# Parity analysis: official centrifuge-rust features and community requests

Status: Phase 1 draft for the owner's decisions, 2026-11-06. Nothing here is decided. Each item ends with a recommendation for the owner to approve, change or reject.

Owner decision (2026-11-06): Ferrifuge gets every feature centrifugal/centrifuge-rust has and Ferrifuge lacks, plus the community requests listed below, each after a pattern-elegance analysis. This reverses earlier exclusions, to be recorded in Block 0 when the specs land (see "Earlier exclusions this reverses").

## Clean-room record

- Read from centrifugal/centrifuge-rust: only `docs/spec.md` and `docs/api.md`, at commit `88d6edf178f6db370d8e6756d389c89bf100436b` (2026-11-06). Citations below give the section ("rust spec §5.2", "rust api.md").
- Not opened: anything under `crates/`, `xtask/`, `docs/decisions-pending.md`, the examples, the workflows. The TLA+ trace files were listed by name and size through the GitHub tree API, never read (item 20).
- Behaviour sources keep the Block 0 precedence: the Centrifugo client SDK spec, then `client.proto` (vendored at `4455d8e`), then centrifuge-js `63ed73e`. Where centrifuge-rust's documented behaviour is the only source, the item says so.
- No design below copies centrifuge-rust internal names, module layout or code. Some public method names documented in its `api.md` recur (`ready`, `close`, `statistics`, `set_http_headers`, `publications`, `runtime`, `test-util`): they are the owner's own item names, protocol terms or common Rust idioms, and public API names are what parity means.

## How each item is laid out

1. Problem and audience.
2. How the official SDK exposes it, from its docs only.
3. At least two candidate designs for Ferrifuge.
4. Elegance table, with one column per design and these rows:
   - **Fit**: fit with the sans-IO core (`Machine::handle(Input) -> Vec<Action>`) and the stream-based API of Block 15;
   - **Idiom**: idiomatic Rust;
   - **Consistency**: with Ferrifuge's existing API;
   - **Tests**: testability on the existing layers (@A real Centrifugo, @B proxy backend, @D fake server, @E controlled clock, @P harness);
   - **Semver**: impact on the public API;
   - **Cost**: S (one PR), M (one block of 2–4 PRs), L (several blocks or platform work).
5. Recommendation.

The audience of every item is an application developer shipping Ferrifuge to a fleet of end-user devices or services (Block 0's project purpose); an item names a narrower audience when it has one.

Block 16's chaos runners grow with the items that change connection or delivery behaviour: 4 (`reconnect()`, sleep), 5 (delta), 7 (compaction), 16 (`close()`), 26 (automatic batching). Each such item's block adds its operations and planted faults there.

Ratings in the tables: **good**, **fair**, **poor**, each with its reason.

Semver baseline: Ferrifuge is 0.12.0. `Config` has public fields and is not `#[non_exhaustive]`, and `State` is an exhaustive enum. A new `Config` field or `State` variant is therefore a breaking change: a 0.x minor bump. New builder methods, new functions and new variants of the `#[non_exhaustive]` enums (`ClientEvent`, `SubscriptionEvent`, `ErrorKind`, `Input`) are additions.

## Summary

Order: production priority. Items 1–4, 9 and 21 come first, as the owner asked.

| # | Item | Recommendation | Semver | Cost |
|---|---|---|---|---|
| 1 | HTTP CONNECT and SOCKS5 proxies, env, OS settings, per-endpoint, `set_proxy` | Adopt: one proxy decision for every transport, the CONNECT and SOCKS5 handshakes in the adapter; default follows env then OS settings (question Q1) | additive | L |
| 2 | Platform TLS verifier, extra roots, raw rustls escape hatch | Adopt the platform verifier as the default, extra roots, a raw `rustls::ClientConfig` behind a semver-exempt feature; the fips build keeps webpki verification (decided through the reviewer, see the end) | additive | M |
| 3 | Android setup, Android/iOS CI builds, mobile lifecycle docs | Adopt: build-only CI jobs, an Android init function (with item 2), a lifecycle guide | additive | M |
| 4 | Sleep detection, network changes, captive portals | Adopt sleep detection in the core and a `reconnect()` call for network changes; captive portals as a distinct transport error; no OS network watcher | additive | M |
| 9 | Logging | Adopt `tracing` in the tokio adapter only, events at the input/action boundary, Ferrifuge's own records kept free of secrets by construction and by a test; dependencies log secrets at TRACE, documented (question Q3) | additive | M |
| 21 | SECURITY.md, examples in CI, CHANGELOG, crates.io workflow, docs.rs, MSRV policy | Adopt all; publishing needs owner actions outside the repo (question Q4) | none | M |
| 15 | Injected tokio runtime handle | Adopt, and stop `Client::new` panicking outside a runtime | additive | S |
| 12 | Maximum incoming message size | Adopt as a `Config` field, an oversized message ending the client like close code 1009 | breaking (Config) | S |
| 11 | Separate handshake timeout | Adopt as a `Config` field bounding the open; the connect reply keeps `timeout` | breaking (Config) | S |
| 23 | Table of error and disconnect codes | Adopt: one table in the `codes` rustdoc, mirrored in the README, checked by a test | none | S |
| 10 | `ready()` on client and subscription | Adopt, without a timeout parameter | additive | S |
| 8 | `statistics()` | Adopt: counters kept by the core, bytes by the adapter | additive | M |
| 14 | `set_http_headers` at runtime | Adopt, adapter only | additive | S |
| 27 | `Subscription::set_data` (found by the review) | Adopt, as Block 17's setters | additive | S |
| 16 | `close()` and a `Closed` state | Adopt `close(self)`, consuming the client; no `Closed` state; leftover subscriptions report `unsubscribed` with code 2 on close and on drop | additive | S |
| 5 | Fossil delta compression | Adopt, own implementation in the core from the published format; a delta error resubscribes a client-side subscription and reconnects for a server-side one, instead of ending the client (question Q5) | additive | M |
| 6 | Server-side tag filters | Adopt: an owned filter type mirroring `client.proto` `FilterNode` | additive | S |
| 28 | `Publication.channel` (found by the review) | Adopt `channel`; `time` when a server sends it | additive | S |
| 7 | Channel compaction (PRO) | Adopt as an opt-in subscription option, tested on @D only | additive | S |
| 26 | Automatic command batching (found by the review) | Adopt as an implicit Block 18 batch around each wake of the driver | additive | M |
| 19 | Test constructors for events and results | Adopt behind `test-util`, renaming the current internal hooks feature | additive | S |
| 22 | Mocking support for applications | Adopt an in-memory server double for the real `Client`, behind `test-util`; no client trait | additive | M |
| 18 | Typed JSON helpers | Adopt; no new feature, since serde is already a dependency (question Q6) | additive | S |
| 13 | Publications-only stream | Adopt as a thin view of the subscription's stream, same delivery rules | additive | S |
| 24 | Cookies on the HTTP transports | Adopted (owner, 2026-11-06 late): a `CookieJar` trait with a default in-memory jar on `cookie_store`, on every transport | additive | M |
| 17 | Callback API, WeakClient, delivery modes, deadlock detection | Adopted (owner, 2026-11-06 late): handlers as one more consumer of Block 15's outbox, delivery modes Task, Thread and Inline, `Client: Clone` with `WeakClient`, deadlock detection in Thread and Inline | additive | L |
| 20 | Replay of their TLA+ traces | Adopted (owner, 2026-11-06 late): a replay harness with a mapping and an explicit divergence list | none | L |
| 25 | WebAssembly / browser adapter | Adopted as design B (owner, 2026-11-06 late): a browser adapter on the sans-IO core, taking the browser-only options (item 39) | additive | L |

## Earlier exclusions this reverses

To record in Block 0 when the specs land. Each line becomes "reversed by the owner decision of 2026-11-06" once its item is approved.

| Where | Excluded | Item |
|---|---|---|
| Block 0, decisions and out of scope | `closed` client state | 16 (recommended without a state; see there) |
| Block 0, out of scope | channel compaction, "the client never offers it" | 7 |
| Block 6, out of scope and closing note | custom root certificates; "no custom-root API is added" | 2 |
| Block 9, out of scope | delta compression; tag filters | 5, 6 |
| Block 14, out of scope | HTTP proxy (CONNECT) support; sleep/resume behaviour | 1, 4 |
| Block 15, out of scope | a callback-style handler API | 17 (recommended not adopted) |
| Block 17, out of scope | replacing HTTP headers of a built client; a cookie jar | 14, 24 |
| Block 1, out of scope | `closed` state | 16 |
| Blocks 11 and 16, out of scope | delta compression | 5 |
| Block 16 and "Planned after Block 13" | trace replay "optional, later" | 20 (recommended dropped) |
| Blocks 0 and 8, out of scope | browser-only options (fetch, EventSource shims, network events) | 24, 25 |
| Block 18 | batching only on request, plus the connect reply's subscribes | 26 |

## Findings in the current code

Found while preparing this analysis. Each is production-relevant and belongs to the item named.

| # | Finding | Evidence | Item |
|---|---|---|---|
| F1 | `Client::new` calls `tokio::spawn`, which panics when no tokio runtime is current (for example in a Tauri setup hook) | `src/adapter/client.rs:616` | 15 |
| F2 | The websocket transport dials directly, while the reqwest client of http_stream and sse applies `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY` by default: behind a proxy set in the environment, websocket fails and the client falls back to http_stream through the proxy, by accident rather than by design | `src/adapter/transport.rs:93`, `src/adapter/http.rs:281-288`; reqwest 0.13.5 `async_impl/client.rs:310` (`auto_sys_proxy: true`), `:417-418`, `proxy.rs:515-517`. OS settings are not read in Ferrifuge's feature set (reqwest's `system-proxy` feature is off) | 1 |
| F3 | The 64 MiB incoming limit is fixed, and what an oversized message does (reconnect loop or terminal) is not specified | `src/adapter/transport.rs:296-303`, `src/adapter/http.rs:27` | 12 |
| F4 | The MSRV job (`cargo check` on 1.88, default and no default features) never checks the `aws-lc-rs` and `fips` builds | `.github/workflows/ci.yml:435-449` | 21 |
| F5 | The `test-util` feature name is taken by internal test hooks "never enabled by applications", the name item 19 would use | `Cargo.toml` `[features]` | 19 |

---

## 1. HTTP(S) CONNECT and SOCKS5 proxies

**Problem.** Corporate networks send egress through an HTTP proxy (sometimes SOCKS5), often with credentials. A GUI app on macOS gets no shell environment variables, and Windows keeps proxy settings in the system, so a client that reads only `HTTPS_PROXY` fails there. Who: Ferrifuge's fleets on corporate networks (Block 8 already names them).

**Official SDK** (rust spec §5.2, §3.2; api.md):
- one proxy setting for both transports: none, from the environment, system (environment then macOS/Windows settings, the default), an explicit HTTP CONNECT or SOCKS5 proxy with optional credentials and local or remote DNS, or an application function deciding per endpoint;
- the tunnel is set up before TLS to Centrifugo, so certificate checks target Centrifugo; plain `ws://`/`http://` also go through CONNECT;
- a non-2xx CONNECT answer is a transport error carrying the status (407 for credentials), retried with backoff;
- `NO_PROXY` follows curl rules; Basic and SOCKS5 username/password only; no PAC, no NTLM/Kerberos;
- `set_proxy` applies from the next connection attempt; credentials are never logged.

**Designs.**
- **A. One decision, two executors.** A Ferrifuge proxy setting resolves, per endpoint, to direct, HTTP CONNECT or SOCKS5. The websocket path performs the handshake itself (TCP, CONNECT or SOCKS5, then rustls, then tungstenite over that stream); the reqwest client of http_stream and sse gets the same decision through reqwest's own proxy support. The decision comes from a maintained matcher (env and OS settings) or from the application.
- **B. One connector for everything.** Ferrifuge dials every connection itself, the proxy handshake included, and hands the stream to tungstenite and to the HTTP client. This needs an HTTP client that accepts a custom connector, which reqwest does not offer (it takes proxies through `Proxy`, including `Proxy::custom` and SOCKS5 behind its `socks` feature): it means moving http_stream and sse to hyper.
- **C. Application-provided stream.** An application callback returns a connected byte stream for an endpoint; Ferrifuge only speaks TLS and the protocol over it. Maximum flexibility, but every application re-implements proxies.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter only; the core sees a failed open as today | good: adapter only | good: adapter only |
| Idiom | good: an enum setting plus a function variant | good | fair: a boxed stream factory in the public API |
| Consistency | good: reqwest stays (Block 0) | poor: replaces reqwest, a Block 0 decision | fair |
| Tests | good: in-process CONNECT and SOCKS5 proxies on @D, Centrifugo behind them on @A; env in a separate test binary; OS settings on the macOS and Windows runners | good, same | fair: only the callback is ours |
| Semver | additive: a builder method and a runtime setter | additive | additive |
| Cost | L | L, plus the HTTP client migration | M, but the cost moves to every application |

Verified facts for A (2026-11-06):
- hyper-util 0.1.21's `Matcher` works without hyper's connector: `intercept(&Uri)` returns the proxy URI and its credentials, SOCKS schemes included. `from_env` reads `ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`, upper and lower case. `from_system` adds macOS `SCDynamicStore` HTTP/HTTPS settings, without macOS's exceptions list, and Windows `Internet Settings` `ProxyEnable`/`ProxyServer`/`ProxyOverride`. There is no PAC or WPAD. `NO_PROXY` takes IPs, CIDR, `*` and domain suffixes. The matcher needs hyper-util's `client-proxy` feature, which brings in hyper's client and `tracing`; OS settings need `client-proxy-system`.
- reqwest 0.13.5 already uses that matcher, offers `Proxy::custom`, and SOCKS5 behind its `socks` feature.
- tokio-tungstenite 0.30 runs over any caller-provided stream (`client_async_tls_with_config`) and has no proxy support of its own.

Two behaviours cannot be the same in A without care, and need a scenario each: the proxy decision for an endpoint, and the error reported for a 407 on each transport.

**Recommendation.** A. Default: environment, then OS settings (question Q1), since that is what browsers do and what a centrifuge-js application gets for free. `set_proxy` applies from the next attempt, like Block 17's setters. Out of scope: PAC/WPAD and NTLM/Kerberos, documented with the local-helper workaround (Px, Cntlm), as the official SDK does. F2 is fixed by A whatever the default.

## 2. Platform TLS verifier, extra roots, raw rustls configuration

**Problem.** Corporate TLS inspection installs its CA in the OS trust store. Ferrifuge today reads the OS store's certificates with `rustls-native-certs` and verifies with webpki, so it trusts the same roots but not the OS's trust decisions (distrust settings, name constraints, revocation, intermediate fetching on Windows). It has no way to add a private CA except `SSL_CERT_FILE`, and no use of the Android trust manager (whether `rustls-native-certs` finds a store on Android and iOS is not verified). Who: corporate fleets, mobile apps, private PKI deployments.

**Official SDK** (rust spec §3.2, §5.2, decision 26; api.md): verification by `rustls-platform-verifier` (OS store and its decisions; Linux falls back to the system bundle and `SSL_CERT_FILE`), extra PEM roots, a development-only "accept invalid certificates" switch that warns on every connect, and a raw `rustls::ClientConfig` behind a feature exempt from semver. On Android the verifier takes no extra roots, so extra roots there fail the build of the client.

**Designs.**
- **A. Platform verifier by default**, a TLS settings type with extra PEM roots, and a raw `rustls::ClientConfig` behind a semver-exempt feature.
- **B. Keep `rustls-native-certs` by default**, offer the platform verifier as an opt-in setting, plus extra roots and the raw escape hatch.
- **C. Raw `rustls::ClientConfig` only**: applications build what they need.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter only; Block 6's one-place TLS build stays | good | good |
| Idiom | good | good | fair: pushes rustls knowledge onto every app |
| Consistency | fair: changes the default trust behaviour of Block 6 | good: default unchanged | good |
| Tests | good: extra roots on @A with the test CA, replacing the `SSL_CERT_FILE` trick; platform behaviour on the three OS runners | good | fair |
| Semver | additive (the raw type is behind an exempt feature) | additive | exempt feature |
| Cost | M | M | S |

**FIPS (Block 6).** Block 6 requires `ClientConfig::fips() == true` in the fips build. Verified (rustls 0.23.46 `src/client/client_conn.rs:396-409`; rustls-platform-verifier 0.7.1): `ClientConfig::fips()` checks only the provider, `require_ems` and ECH, never the certificate verifier, so it stays true with the platform verifier. But the verifier checks the certificate chain's signatures inside the OS (Security framework on Apple, CryptoAPI on Windows, the Android trust manager through JNI); only the handshake signature uses the provider. On those platforms the chain checks run outside the AWS-LC FIPS module while `fips()` still reports true. On Linux the verifier uses webpki with the provider's algorithms, so the chain stays inside the module. Decided through the reviewer (see the end).

Verified facts: rustls-platform-verifier 0.7.1 (MSRV 1.85, below Ferrifuge's 1.88) takes the crypto provider from its caller and forces none; extra roots are supported on Apple, Windows and Linux, not on Android.

**Recommendation.** A for the ring and aws-lc-rs builds. For the fips build, keep webpki verification with the provider's algorithms (native roots plus extra roots), since Block 6's FIPS goal is cryptography inside the validated module, and `fips()` alone does not show where chains are checked. The "accept invalid certificates" switch is not recommended: a fleet SDK should not ship it; the raw `ClientConfig` covers development needs.

## 3. Android setup, Android and iOS CI builds, mobile lifecycle

**Problem.** Fleets of end-user devices include phones. Today nothing proves Ferrifuge builds for Android or iOS, and nothing tells an app how to behave across backgrounding, which mobile OSes enforce by suspending sockets.

**Official SDK** (rust spec §5.2 "Mobile", §11.6): CI builds the client for `aarch64-linux-android` and `aarch64-apple-ios`, build only. Android needs the verifier's Kotlin component in the app's Gradle build and a one-time JNI init, which the SDK wraps (`init_with_env` and variants) so that the init reaches the copy of the verifier the client links; until it ran, a client whose endpoints need TLS fails to build, and one that only discovers TLS at attempt time fails the attempt. iOS needs no setup. The wrapper module is exempt from semver (it exposes pre-1.0 `jni` types).

**Designs.**
- **A. Build-only CI jobs plus an Android init wrapper plus a lifecycle guide.** The wrapper exists only if item 2 adopts the platform verifier.
- **B. Build-only CI jobs plus docs**, the app calling the verifier's own init. Risk the official docs name: two copies of the verifier in one app, one never initialised.
- **C. Emulator CI** (Android emulator, iOS simulator) running a smoke test against Centrifugo.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter only | good | good |
| Idiom | fair: a semver-exempt module with JNI types | good | good |
| Consistency | good | good | good |
| Tests | fair: build-only; init failure paths unit-tested | fair: build-only | good: runs on a device image |
| Semver | additive, exempt module | none | none |
| Cost | M | S | L, slow and flaky CI |

Mobile lifecycle guide, independent of the design: disconnect on background and connect on foreground, or keep running only where the OS allows it; recovery (Block 11) and exactly-once delivery (Block 15) make a background gap lossless on recoverable channels within the server's recovery limit; token refresh after a long suspension (Block 5's timers are monotonic); item 4's `reconnect()` on network changes.

Verified facts: the ubuntu-24.04 runner image ships Android NDK 27.3 (default), 29 and 30, with `ANDROID_NDK_HOME` set; the macos-15 arm64 image ships Xcode 16.0–26.3 with iOS SDKs; aws-lc-sys builds without CMake for both targets outside FIPS, and ring supports both. That the two targets actually cross-compile with Ferrifuge's dependency tree is only known once a CI job runs.

**Recommendation.** A, C deferred. CI targets: `aarch64-linux-android` (NDK on the Ubuntu runner) and `aarch64-apple-ios` (macOS runner), in the portability set that runs on PRs.

## 4. System sleep, network changes, captive portals

**Problem.** A laptop that sleeps for an hour wakes with a dead socket. Tokio's clock does not advance during sleep on Linux and macOS, and Android and iOS (the platforms where it matters most) behave the same, so the no-ping deadline (35 s with Centrifugo defaults) still has its full time left, and the app waits up to that long for a reconnect. A phone moving from Wi-Fi to cellular keeps a socket bound to the old network until a timer notices. A captive portal answers with an HTML page, which today shows up as a generic transport error.

**Official SDK** (rust spec §9.7, §13, §5.3, decisions 19, 24, 34):
- the driver compares wall-clock and monotonic time; a jump gives late timers a 1 s grace, re-checks token expiry, restarts the no-ping window from the wake, and reconnects at once when the sleep was at least the no-ping window;
- no network-change API, on purpose: the app calls `disconnect()` then `connect()`;
- a 200 HTML answer to the HTTP stream request is a transport failure (captive portal), retried; undecodable data before the first message is a transport failure, never terminal.

**Designs.**
- **A. Sleep detection split across the sans-IO line, plus a `reconnect()` call.** The adapter notices that wall-clock time advanced well past monotonic time and feeds the core an input carrying the gap; the core decides: when the gap is at least the no-ping window it closes the transport and handles it as a lost transport (code 1): the Block 2 first-attempt delay and the Block 14 jitter window apply, so it does not reconnect "at once" as the official SDK does, since phones waking on a schedule would otherwise reconnect together; otherwise it restarts the no-ping window. In both cases it moves the token refresh timers by the gap, so a token that expired during the sleep is refreshed (or fetched before the next connect) instead of being sent expired. `Client::reconnect()` closes the current transport and connects again without the backoff delay, through `connecting`, never `disconnected`, so calls waiting for the connection keep waiting; the Block 14 jitter window still applies, so a building-wide network change does not synchronise a fleet. Captive portals: an HTML or redirect answer during the open is reported as a transport error saying so, and is retried like any failed open.
- **B. Built-in OS network watchers** driving reconnects automatically. A maintained crate exists: `netwatcher` 0.8.0 (2026-08, MIT), event-based on Windows, macOS, Linux, iOS and Android (extra setup there); `if-watch` 3.2.2 polls every 10 s on Android.
- **C. Documentation only**: `disconnect(); connect()` and a lower `max_server_ping_delay`.

| | A | B | C |
|---|---|---|---|
| Fit | good: detection is IO, the decision is in the core and @E-testable | fair: platform IO grows inside the adapter | good |
| Idiom | good | fair: platform-specific code paths, threads on some OSes | good |
| Consistency | good: same codes as a lost transport; `reconnect()` mirrors `connect()` | fair | fair: `disconnect()` fails waiting calls, which an app may not want |
| Tests | good: @E with a clock whose wall and monotonic parts diverge; @D for `reconnect()`; the HTML answer on @D | poor: needs real network changes per OS | good |
| Semver | additive (an `Input` variant, a method) | additive | none |
| Cost | M | M–L | S |

**Recommendation.** A. B not adopted: apps already receive network-change notifications from their platform and can call `reconnect()`. The jitter on `reconnect()` and after a sleep is a fleet decision the owner may want to see (it diverges from "at once"): the recommendation is to keep it.

## 9. Logging

**Problem.** Operators of a fleet need to see why a client reconnects, falls back or stops, without a rebuild. Ferrifuge logs nothing today; the only signals are events.

**Official SDK** (rust spec §3.2, §9.9): `tracing`, spans per client and per subscription, frames at TRACE with payloads redacted unless a flag is set; `log` rejected for lack of spans.

**Designs.**
- **A. `tracing` in the tokio adapter only.** The core stays free of logging (it remains a pure state machine). The adapter logs at the input/action boundary: state changes, transport opens and failures with their codes, fallback steps, retries, at INFO/WARN/DEBUG; frames at TRACE as kind and length only. Every logged value goes through the `Debug` implementations Block 17 already made redacting (tokens never, data as a length, headers by name), so Ferrifuge's own records keep secrets out by construction. Its dependencies do not: tungstenite 0.30.0 logs, through `log` at TRACE, the whole upgrade request with header values (`handshake/client.rs:197`), every message received (`protocol/mod.rs:475`) and every frame sent (`:768`), the connect frame with its token included. The docs must warn that enabling TRACE for dependencies logs tokens, data and headers, and the capture test must capture `log` records at TRACE too, asserting that Ferrifuge's own records are clean.
- **B. `log`** at the same places. Lighter, universally consumed, no spans.
- **C. No logging**; richer events instead.

| | A | B | C |
|---|---|---|---|
| Fit | good: nothing in the core | good: same | good |
| Idiom | good: tokio, hyper and the async ecosystem use it; spans per client | good: the classic facade | fair |
| Consistency | good: reuses the redacting `Debug` | good | good |
| Tests | good: a capturing subscriber in the BDD run asserts that no token, data or header value from the scenarios appears in any record | good: same with a capturing logger | — |
| Semver | additive | additive | none |
| Cost | M | M | none |

**Recommendation** (question Q3). A, as a regular dependency of the `tokio` feature, not a separate feature: a callsite with no subscriber costs a relaxed atomic load. Enabling `tracing`'s `log` feature makes it emit `log` records when no tracing subscriber is set (tracing 0.1.44), so applications using `log` see Ferrifuge's records too. Note that most of Ferrifuge's dependencies (rustls, reqwest, tungstenite, rustls-platform-verifier) log through `log`, so a `tracing` application adds `tracing-log` to see theirs, whichever facade Ferrifuge picks. No payload-logging switch: payloads never reach a log, which keeps the "never logged" rule absolute.

## 21. SECURITY.md, examples, CHANGELOG, crates.io, docs.rs, MSRV

**Problem.** A production dependency needs a disclosure channel, examples that compile, a change record, a published crate with docs, and a stated toolchain floor.

**Official SDK** (rust spec §3.1, §11.6, §14; the repository has `SECURITY.md` and `CHANGELOG.md`): MSRV declared and checked by CI, raised only in minor versions and noted in the changelog; examples per feature; tag-triggered release with crates.io trusted publishing (OIDC, no stored token) from a protected environment, version and semver checked; rustdoc built with `--cfg docsrs`.

**Designs.**
- **A. All in the repository, publication by tag.** `SECURITY.md` pointing to GitHub private vulnerability reporting; `examples/` compiled in CI (`cargo build --examples` on every OS) and one run against the @A Centrifugo; `CHANGELOG.md` in Keep a Changelog form, backfilled from 0.1.0 from the git history and the block table; a `release.yml` on a pushed tag using crates.io trusted publishing and `cargo-semver-checks`; `[package.metadata.docs.rs]` with the features docs.rs can build (not `fips`, which needs Go and CMake) and `--cfg docsrs`; an MSRV job building with the declared `rust-version`.
- **B. Same, but publishing by hand**, the workflow only checking.

| | A | B |
|---|---|---|
| Fit | n/a | n/a |
| Idiom | good: trusted publishing is the crates.io recommendation | fair |
| Consistency | good: CI layout already splits PR and main jobs | good |
| Tests | good: adding the aws-lc-rs and fips builds to the MSRV job closes F4 | good |
| Semver | none | none |
| Cost | M | S |

Name check (2026-11-06): `ferrifuge` is free on crates.io (the API answers 404; no `ferrifuge-*` crate exists). Owner actions needed, outside the repository: a crates.io account owning the name, the first publish (trusted publishing needs the crate to exist), the trusted-publisher entry, a protected `release` environment, and turning on private vulnerability reporting in the repository settings (question Q4).

**Recommendation.** A. MSRV policy: declared in `Cargo.toml`, checked by CI, raised only in a minor version, never above what the dependencies require, noted in the changelog.

---

## 15. Injecting a tokio runtime handle

**Problem.** `Client::new` spawns its driver with `tokio::spawn`, which panics outside a runtime context (F1). Tauri setup hooks, plugin systems and synchronous initialisation code run there.

**Official SDK** (rust spec §4.1; api.md): a builder option taking a `tokio::runtime::Handle`, defaulting to the current one; with neither, `build()` returns a configuration error.

**Designs.**
- **A. `ClientBuilder::runtime(Handle)`**, defaulting to `Handle::try_current()`, a configuration error otherwise; every spawn of the adapter uses the stored handle.
- **B. A spawner trait** the application implements, so any executor works.
- **C. Docs only**: enter the runtime with `handle.enter()` before building.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter only | fair: generalises an adapter that is tokio by design (Block 0) | good |
| Idiom | good: `tokio::runtime::Handle` is the standard way | fair | fair: leaves the panic |
| Consistency | good: a builder option like `get_token` | poor: a trait for one implementation | good |
| Tests | good: build outside a runtime returns the error; @A with a handle from another thread | fair | — |
| Semver | additive | additive | none |
| Cost | S | M | S |

**Recommendation.** A, fixing F1 in the same PR.

## 12. Maximum incoming message size

**Problem.** Devices with little memory want a smaller cap than the fixed 64 MiB, since Block 15 admits one oversized message alone into an empty read-ahead: the per-message cap is the real memory bound. Large history and recovery replies need room in the other direction.

**Official SDK** (rust spec §4.2, §5.3): an option, 64 MiB by default, applied to the websocket message and frame limits and to the stream decoders; an oversized message before the first decoded one is a transport failure (an HTML page read as a length), after it the client stops with code 3 "message size limit", like close code 1009.

**Designs.**
- **A. `Config::max_message_bytes`**, 64 MiB default, at least 1, applied to every transport; over it after the connect reply the client ends with code 3 "message size limit exceeded" (Block 3's 1009 rule), before it the attempt fails as a transport error.
- **B. One memory budget**: derive the cap from `read_ahead_bytes`.

| | A | B |
|---|---|---|
| Fit | good: the adapter enforces, the core gets the same input as for 1009 | fair: couples two limits Block 15 keeps apart |
| Idiom | good | fair |
| Consistency | good: same outcome as 1009 | poor: changes Block 15's "a larger message is admitted alone" |
| Tests | good: @D sends one byte over the cap on each transport, before and after connect | good |
| Semver | breaking (Config field) | breaking (meaning of a field) |
| Cost | S | S |

**Recommendation.** A. It also settles F3.

## 11. Separate handshake timeout

**Problem.** Today one `Config::timeout` (5 s) bounds the open (DNS, TCP, proxy, TLS, upgrade) and the connect reply together, through `Timer::ConnectTimeout` armed at `OpenTransport` (`src/machine.rs:1353`). On slow links, or through a proxy, the open alone can take longer than a sensible command timeout.

**Official SDK** (rust spec §4.2, §5.1): `handshake_timeout`, 5 s, bounding DNS, TCP, TLS and upgrade, separate from the 5 s command timeout.

**Designs.**
- **A. `Config::handshake_timeout`** bounds the open, from `OpenTransport` to `TransportOpened`; the connect reply then has `timeout`. The core arms `Timer::ConnectTimeout` with `handshake_timeout` at `OpenTransport` and re-arms it with `timeout` at `TransportOpened`: one timer kind, so no new variant of the exhaustive `Timer` and `Action` enums; @E-testable.
- **B. Rename the meaning**: the single timer takes `handshake_timeout` for open plus reply.
- **C. Not adopted**: `timeout` already bounds the whole attempt.

| | A | B | C |
|---|---|---|---|
| Fit | good: `TransportOpened` already exists as an input | good | good |
| Idiom | good | fair: a command timeout that is no longer one | good |
| Consistency | fair: Block 8's fallback scenarios name "the configured timeout" and must say which | fair | good |
| Tests | good: @E for both timers; the Block 8 relay scenarios split into open and reply | good | — |
| Semver | breaking (Config field) | breaking | none |
| Cost | S | S | none |

**Recommendation.** A, default 5 s. The worst-case attempt goes from 5 s to 10 s, and a first fallback sequence over three transports from 15 s to 30 s; a fleet operator who wants today's bound sets both to 2.5 s. Block 8's scenarios are reworded to name the timer each one exercises.

## 23. Table of error and disconnect codes

**Problem.** centrifuge-js #264: users see `timeout` or `transport closed` in production logs and find no description. Ferrifuge's codes are rustdoc on constants in `src/codes.rs`, with no single table saying what the client does on each.

**Official SDK** (rust spec §6.5, §10): a table of client-side codes by kind, and the server codes it reacts to.

**Designs.**
- **A. One table in the `codes` module rustdoc**: every client-side code, the server ranges (errors 100–1999, unsubscribe 2000–2999, disconnect 3000–4999, 5000+) with the client's reaction (reconnect, resubscribe, retry, terminal) and the block that decides it; mirrored in the README; a test checks that every constant appears in both.
- **B. A separate `docs/codes.md`.**

| | A | B |
|---|---|---|
| Fit | n/a | n/a |
| Idiom | good: docs live with the items | fair |
| Consistency | good: `spec_sync` already checks text against code | fair |
| Tests | good: the cross-check test | fair |
| Semver | none | none |
| Cost | S | S |

**Recommendation.** A.

## 10. `ready()` on client and subscription

**Problem.** An app wants to know when the client is connected, or a subscription subscribed, without consuming the one active event stream (Block 15) just to watch for it.

**Official SDK** (rust spec §4.3, §4.5): `ready(timeout: Option<Duration>)`, resolving when connected, failing at once when disconnected or closed; the subscription's resolves when subscribed and fails at once when unsubscribed. It resolves after the matching handler returned.

**Designs.**
- **A. `async fn ready(&self) -> Result<(), ReadyError>`**, no timeout parameter (the caller wraps it in `tokio::time::timeout`, the idiom); resolves on the state, fails at once in `disconnected` (client) or `unsubscribed` (subscription) and when entering them.
- **B. With `Option<Duration>`** as centrifuge-js.
- **C. A state watch** (`state_changes()` stream of states), `ready()` written by the app.

| | A | B | C |
|---|---|---|---|
| Fit | good: a waiter on the state the adapter already tracks; independent of event delivery | good | good |
| Idiom | good: timeouts compose | fair: a timeout parameter is unusual in async Rust | good |
| Consistency | good: calls already wait for the connection the same way (Block 13) | good | fair: a second stream next to `events()` |
| Tests | good: @A connect then ready; @D terminal code fails a waiting ready | good | good |
| Semver | additive | additive | additive |
| Cost | S | S | M |

Note: with Block 15, `state()` can be ahead of the stream, so `ready()` can resolve before the `Connected` event is taken. Documented.

**Recommendation.** A.

## 8. `statistics()`

**Problem.** Support and fleet telemetry want counters: connects, reconnect attempts, bytes and messages each way, publications delivered, current transport.

**Official SDK** (rust spec §4.3; api.md): a snapshot struct of counters since the client was built, read without a round trip.

**Designs.**
- **A. A `#[non_exhaustive]` snapshot struct.** Protocol counters (connects, attempts, messages, publications taken, fallbacks) live in the core and change with its inputs, so they are deterministic and @E-testable; byte counters and the current transport live in the adapter. Block 15's `held_events()` and `read_ahead()` stay as they are.
- **B. A metrics facade** (the `metrics` crate): counters and gauges emitted to the app's recorder.
- **C. Events only**: apps count from the stream.

| | A | B | C |
|---|---|---|---|
| Fit | good: core counters are machine state | fair: emitting from the core is IO-like | poor: bytes are invisible |
| Idiom | good | good | fair |
| Consistency | good: `state()`, `held_events()` are snapshots too | fair: a new dependency style | good |
| Tests | good: @E counter values after scripted inputs; @D for bytes | fair | — |
| Semver | additive | additive | none |
| Cost | M | M | none |

**Recommendation.** A. B can be layered on top by an app.

## 14. `set_http_headers` at runtime

**Problem.** A gateway in front of Centrifugo checks a real header (for example a bearer token in `Authorization`) that expires; the app must replace it before the next attempt without rebuilding the client. Block 17 excluded it.

**Official SDK** (rust spec §4.3, §5.1; api.md): replaces the real headers from the next attempt, validated as at build time.

**Designs.**
- **A. `Client::set_http_headers(...) -> Result<(), ClientError>`**, adapter only (Block 17 keeps HTTP headers out of the core), validated as `ClientBuilder::http_headers`, applied from the next attempt.
- **B. An async header callback** called before each attempt, like `get_data`. Needs the core to fetch before `OpenTransport`.

| | A | B |
|---|---|---|
| Fit | good: adapter only | fair: a new fetch in the core for an adapter concern |
| Idiom | good | good |
| Consistency | good: mirrors `set_emulated_headers` | good: mirrors `get_data` |
| Tests | good: @B, the proxy records the new header on the reconnect | good |
| Semver | additive | additive |
| Cost | S | M |

**Recommendation.** A. B only if a user needs headers computed at attempt time.

## 16. `close()` and a `Closed` state

**Problem.** An app wants to stop a client for good and know when it has finished: the close handshake done, streams ended. Ferrifuge's `Client` is not `Clone`; dropping it disconnects (`src/adapter/client.rs:912-916`) and the driver then ends every stream (`src/adapter/client.rs:1042-1053`; test `streams_end_and_callbacks_go_when_the_client_is_dropped`). `Subscription` is `Clone` and outlives the client: its state stays `subscribing` and its calls fail with "disconnected" (`src/adapter/client.rs:226`).

**Official SDK** (rust spec §4.3, §6.1, §9.8): `close()` starts at the call and its future waits for completion; a terminal `closed` state; every subscription goes `unsubscribed` with code 2 "client closed"; later calls fail with a "client closed" error. Their `Client` is `Clone`, so a terminal state is how other clones learn of it.

**Designs.**
- **A. `async fn close(self)`, consuming the client.** Ownership already makes "closed" unrepresentable for the client: no handle is left to ask. It disconnects, ends the client's stream and every subscription's stream after their last events (each subscription first emits `Unsubscribed` with client-side code 2 "client closed"), and resolves once the transport is closed (bounded by `Config::timeout`, as Block 8's close). Dropping the client does the same without waiting. `Subscription` handles left over then report `unsubscribed`; their calls fail with a new "client closed" error instead of today's "disconnected", a behaviour change.
- **B. `close(&self)` plus `State::Closed`**, as the official SDK.
- **C. Not adopted**: drop is close, as today.

| | A | B | C |
|---|---|---|---|
| Fit | good: one `Input::Disconnect` plus outbox closes, as drop already does | fair: a new terminal state checked by every input | good |
| Idiom | good: type-state through ownership | fair: runtime state for what ownership expresses | good |
| Consistency | good: `Client` is single-owner | poor: breaks every `match` on `State` | good |
| Tests | good: @D close waits for the server to see the close; streams end | good | good |
| Semver | additive (a method, an `Unsubscribed` code) | breaking (`State` variant) | none |
| Cost | S | M | S |

Answer to the owner's question: a `Closed` state adds nothing over dropping the client, because a `Client` cannot be shared without `Arc`, and `Arc<Client>` already gives every holder `Weak` and a well-defined last drop. What `close()` adds is an awaitable end (the close frame sent, the driver stopped). The real gain is for leftover `Subscription` handles, which today stay `subscribing` forever: a code-2 `Unsubscribed`, on close and on drop alike. Both come with A.

**Recommendation.** A.

## 5. Fossil delta compression

**Problem.** Channels whose publications are large and change a little each time (state snapshots, dashboards) cost bandwidth, which fleets on mobile data pay for. Centrifugo can send a Fossil delta against the previous publication instead (`client.proto`: `SubscribeRequest.delta`, `SubscribeResult.delta`, `Publication.delta`).

**Official SDK** (rust spec §7.5; api.md): requested per subscription, applied only when the reply says it was negotiated; the base is the previous full payload, reset at each subscribe (the server sends the first publication after a subscribe in full); JSON carries the delta inside a JSON string; any delta failure (no negotiation, no base, bad patch) ends the client with "delta error", no reconnect; own port of centrifugal/fdelta with its vectors and a fuzz target.

centrifuge-js 63ed73e has it too, so this is also centrifuge-js parity: a `delta: 'fossil'` subscription option, rejected together with a tag filter (`types.ts:639-644`, `subscription.ts:368-370`), decoding in the codec (`applyDeltaIfNeeded`, `subscription.ts`), and `deltaStats()` (`types.ts:818-830`). It has no handling for a failed decode.

**Designs.**
- **A. Own decoder in the core, from the published format.** The Fossil delta format is documented by the Fossil project; the decoder is a pure function, a natural fit for the sans-IO core and for bolero, which Ferrifuge already uses for fuzzing. On a delta failure the subscription resubscribes from its last delivered position (an unsubscribe-and-resubscribe like code 2500), so the server sends a full payload again and the failed publication never moved the position: on recoverable channels within the server's recovery limit nothing is lost or delivered twice; elsewhere the gap is reported as any failed recovery is (Block 11). Server-side subscriptions also receive deltas (requested through `ConnectRequest.subs`, `client.proto:409`) and cannot be resubscribed (Block 12): there a delta failure is handled as a lost transport, so the next connect recovers them from their last delivered position. Publications of that subscription not yet taken are withdrawn, which adds a reason to Block 15's withdrawal rule. With JSON, a delta-negotiated subscription receives every payload inside a JSON string (`client.proto:254-256`): the client unwraps it, so Block 9's "exact bytes" rule applies to the decoded payload.
- **B. Depend on the existing `fossil-delta` crate** (0.2.0, MIT, last release and last push 2023-01-14: unmaintained), same failure rule.
- **C. Same as A, but a delta failure ends the client**, as the official SDK.

| | A | B | C |
|---|---|---|---|
| Fit | good: pure function in the core; the failure is a subscription input | good | good |
| Idiom | good | fair: an unmaintained dependency in the data path | good |
| Consistency | good: same recovery path as Block 11's resubscribes | good | fair: a client-wide stop for one channel's fault |
| Tests | good: @A with a Centrifugo namespace allowing fossil; @D with deltas made by an independent encoder in the test (a dev-dependency), fuzzing the decoder | good | good |
| Semver | additive (a subscription option, an error kind, delta statistics) | additive | additive |
| Cost | M | S–M | M |

Sources for A: the Fossil delta format is documented by the Fossil project; the reference `delta.c` is BSD 2-Clause (D. Richard Hipp). centrifugal/fdelta (Go, BSD 2-Clause) ships 5 origin/target/delta vectors produced by Fossil itself (`testdata/fossil-vectors/`), usable as test data with their notice. Neither is centrifuge-rust, so the clean-room rule does not apply to them.

**Decided by the owner (2026-11-06):** A, from scratch: no other crate meets the components rule's definition of maintained (Block 0, PR #97). The failure rules are Q5's answer. Format facts checked through the reviewer: the trailer is a checksum, and centrifuge-js checks it; the sum is modulo 2^32 (centrifuge-js, fdelta and Fossil's own vectors), not the document's 2^32−1; integers are MSB first; a zero-length copy copies nothing. Ferrifuge is stricter where the document allows: integers over 32 bits, numbers with no digits, and anything after the trailer are failures; the whole delta is validated before the declared size is allocated. Block 15's read-ahead byte bound counts the decoded size.

**Recommendation** (question Q5, answered). A. The failure rules (resubscribe a client-side subscription, reconnect for a server-side one) are new behaviour: centrifuge-js has none, and the only source for "end the client" is centrifuge-rust's documentation. Delta with a tag filter: `client.proto` says a tag filter disables delta compression, so the builder rejects both together.

## 6. Server-side tag filters

**Problem.** A subscriber to a broad channel wants only publications with certain tags, filtered by the server to save bandwidth (`client.proto`: `SubscribeRequest.tf`, `FilterNode`; "not an access control mechanism").

**Official SDK** (rust spec §7.9; api.md): a filter built with combinator functions, set on the builder or while unsubscribed, rejected together with delta. centrifuge-js 63ed73e has the same option and a `setTagsFilter` taking effect from the next subscribe (`subscription.ts:367-380`), so this is centrifuge-js parity too.

**Designs.**
- **A. An owned `#[non_exhaustive]` filter enum** mirroring `FilterNode`'s operations (and, or, not, and the leaf comparisons), with constructor functions; encoded by the codec; a subscription option.
- **B. Expose the generated prost `FilterNode`.**
- **C. A string expression parsed by Ferrifuge.**

| | A | B | C |
|---|---|---|---|
| Fit | good: data in the subscribe command | good | fair: a parser in the core for no protocol reason |
| Idiom | good | poor: prost types in the public API | fair |
| Consistency | good: like `HistoryOptions` | poor | fair |
| Tests | good: @A with tags (Block 9 already publishes tags) if Centrifugo v6.10.1 allows filters (to verify); @D for encoding | good | good |
| Semver | additive | ties the API to prost | additive |
| Cost | S | S | M |

**Recommendation.** A. Whether v6.10.1 honours `tf` decides @A or @D for the delivery scenario.

## 7. Channel compaction (Centrifugo PRO)

**Problem.** With many subscriptions and short messages, the channel name is a large share of each push. Centrifugo PRO can give a subscription a numeric id that pushes carry instead (`client.proto`: `SubscribeRequest.flag` bit 0 requests it, `SubscribeResult.id`, `Push.id`).

**Official SDK** (rust spec §7.8, decision 37): the core maps id to subscription, reset when the session ends and re-registered on each subscribe; a push with an unknown id is dropped; PRO tests run locally only.

**Designs.**
- **A. An opt-in subscription option** setting bit 0; the core keeps id → subscription per connection; pushes routed by id when the channel is empty.
- **B. Always request it**, letting the server decide.

| | A | B |
|---|---|---|
| Fit | good: a lookup next to Block 14's channel map | good |
| Idiom | good | good |
| Consistency | good: opt-in like `join_leave` | fair: changes every subscribe command |
| Tests | fair: @D only (no PRO server in CI); a local PRO run documented | fair: same, and every @D frame assertion changes |
| Semver | additive | none |
| Cost | S | S |

**Recommendation.** A. The protocol makes it opt-in, so the Block 0 note "the client never offers it" was safe; A keeps the default the same.

## 19. Test constructors for events and results

**Problem.** Ferrifuge's events and results are `#[non_exhaustive]`, so an app cannot build a `Publication` or a `HistoryResult` to unit-test its own code.

**Official SDK** (rust spec §3.3, decision 16; api.md): public constructors behind a `test-util` feature.

**Designs.**
- **A. Constructors behind `test-util`**, after renaming today's internal hooks feature (F5) to a private name (`__test-hooks`), since it is documented as not for applications. The rename edits Block 16's recorded decision (its test hooks are "gated by `test-util`"), which the item's spec PR states.
- **B. Constructors always available** (`Publication::new(data)` and setters).

| | A | B |
|---|---|---|
| Fit | n/a: data types | n/a |
| Idiom | good: the tokio convention | good |
| Consistency | good, once F5 is resolved | fair: production code could fabricate events |
| Tests | good: doc-tests per constructor | good |
| Semver | additive (feature-gated surface) | additive, permanent surface |
| Cost | S | S |

**Recommendation.** A.

## 22. Mocking support for applications

**Problem.** centrifuge-go #86 and centrifuge-swift #112: apps want to test code that uses the client without a server. The Go maintainer declined interfaces because adding methods would break implementers; users wrote their own wrappers.

**Official SDK.** Nothing beyond `test-util` constructors in its docs.

**Designs.**
- **A. An in-memory server double for the real client.** Behind `test-util`, a test server the `Client` connects to without sockets, where the test grants subscriptions, pushes publications, answers calls and disconnects with codes. The app tests its real code against the real client, so behaviour (states, recovery, Block 15 delivery) is the real one. Ferrifuge's @D fake server is prior art in the repository, but this is new public API, designed for apps.
- **B. A client trait** apps code against, with a mock implementation. Async methods and `impl Stream` returns make it awkward, and every new method breaks implementers (the Go maintainer's objection).
- **C. Constructors only** (item 19), plus a guide to wrapping the client behind the app's own trait.

| | A | B | C |
|---|---|---|---|
| Fit | good: the sans-IO core does not care where frames come from; an in-memory transport is one more adapter path | fair | good |
| Idiom | good: like `tokio::io::duplex` or an in-process server | fair | good |
| Consistency | good: same `Client` type | poor: a parallel API to keep in sync | good |
| Tests | good: it is test infrastructure, exercised by its own doc-tests | fair | good |
| Semver | additive (feature-gated) | each new method is breaking for implementers | additive |
| Cost | M | M | S |

**Recommendation.** A with item 19.

## 18. Typed JSON helpers

**Problem.** With the JSON protocol, apps decode `data` with serde on every publication and encode on every publish or RPC.

**Official SDK** (rust spec §3.2, §4.6; api.md): `Publication::json`, `rpc_json`, `publish_json` behind a default `serde` feature.

**Designs.**
- **A. Helper methods, always available.** Ferrifuge already depends on `serde` and `serde_json` for its JSON codec (`Cargo.toml`), so a feature would gate only API, not dependencies.
- **B. Behind a `serde` feature**, as the owner wrote.
- **C. An extension trait** (`JsonExt`) in a module.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter and data types | good | good |
| Idiom | good | fair: a feature that removes no dependency | good |
| Consistency | good | good | fair: one-off pattern |
| Tests | good: @A round-trip with a typed struct | good | good |
| Semver | additive | additive | additive |
| Cost | S | S | S |

**Recommendation** (question Q6). A, unless the owner wants the feature for a future build without serde; that build does not exist today, since the JSON codec needs it.

## 13. Publications-only stream per subscription

**Problem.** Many apps only want publications and use `state()` for the rest.

**Official SDK** (rust spec §9.4; api.md): a separate publications stream with its own "one active stream per kind".

**Designs.**
- **A. A view of the subscription's stream** (`publications()`): it is the subscription's active stream, taking over like a second `events()` call, and yields publications only; the other events are taken as they pass, so Block 15's rules hold unchanged (delivered when yielded, withdrawn when not taken).
- **B. A second, independent stream kind** with its own outbox.
- **C. Not adopted**: `events().filter_map(...)` is three lines.

| | A | B | C |
|---|---|---|---|
| Fit | good: no new delivery path | poor: two outboxes break "one message at a time, in order" and can hold each other | good |
| Idiom | good | fair | good |
| Consistency | good: same take-over rule | poor | good |
| Tests | good: @D, lifecycle events skipped, positions move only for yielded publications | fair | — |
| Semver | additive | additive | none |
| Cost | S | M | none |

A view without lifecycle events also hides a `Subscribed` event reporting `was_recovering` true and `recovered` false, the one signal of a gap on a recoverable channel (Block 11). Its docs must say so: an app that needs to know about gaps reads `events()`.

**Recommendation.** A, with that warning.

## 24. Cookie-based authentication on the HTTP transports

**Owner decision (2026-11-06, late): adopted.** A cookie jar on the HTTP transports, alongside the `Cookie` header in `http_headers`.

**Problem.** centrifuge-js #308 and #310, centrifugo #1131: deployments authenticate with session cookies, which a gateway or Centrifugo's connect proxy reads; some gateways also set cookies (sticky sessions, bot protection) that they expect back. A native client has no browser jar, so it neither stores a `Set-Cookie` nor sends it back.

**Official SDK** (rust spec §5.1): no jar; "cookies are the app's": a session cookie goes in the real headers.

**Designs.**
- **A. A Ferrifuge `CookieJar` trait with a default in-memory jar.** `ClientBuilder::cookie_jar(Arc<dyn CookieJar>)`: the jar is asked for the cookies of each request's URL (the websocket upgrade, the http_stream and sse requests, every emulation request) and given the `Set-Cookie` headers of each response, the upgrade's included. The default `MemoryJar` parses and matches cookies with the maintained `cookie_store` crate (0.22.1, 2026-02, MIT OR Apache-2.0; Block 0 "Components": `Set-Cookie` is network input). The reqwest transports use it through reqwest's `cookie_provider`, adapted inside the adapter; the websocket path adds the `Cookie` header itself. A `Cookie` set in `http_headers` is sent as given, the jar's cookies appended.
- **B. Expose reqwest's cookie store** (`reqwest::cookie::Jar`) for the HTTP transports, applied by hand to the websocket upgrade.
- **C. No jar**: `Cookie` in `http_headers` and `set_http_headers` (item 14) only.

| | A | B | C |
|---|---|---|---|
| Fit | good: adapter only | good: adapter only | good |
| Idiom | good: a trait object with a default, as reqwest's `CookieStore` | fair: reqwest types in the public API | good |
| Consistency | good: every transport the same | fair: the websocket path differs | good |
| Tests | good: @D, the fake server sets a cookie on the upgrade and expects it on the next connection; @B on http_stream | good | good |
| Semver | additive | ties the public API to reqwest 0.x | none |
| Cost | M | M | none |

**Recommendation.** A. `withCredentials` stays a browser flag: item 25's browser adapter.

## 17. Callback API, WeakClient, event-delivery modes, deadlock detection

**Owner decision (2026-11-06, late): adopted.** Each keeps Block 15's guarantees (delivered means taken, exactly once on recoverable channels); where a mode cannot keep one, its documentation states which one it gives up.

**Problem.** Callback-style code (handlers registered once, called per event) is what applications ported from centrifuge-js, centrifuge-go or centrifuge-swift are written in; handlers that capture the client need a handle that does not keep it alive; blocking handlers need their own thread; a handler that blocks on a call needing the loop it is blocking would wait for its timeout instead of failing at once.

**Official SDK** (rust spec §4.4, §9.4, §9.5, §9.8): `on_*` handlers per event kind, on the builder or the client; a dedicated event thread per client by default, or inline delivery; `WeakClient` from `downgrade()`; a `WouldDeadlock` error from a thread-local or task-local marker set while a handler runs; a handler panic stops the client.

**Designs.**
- **A. Handlers as one more consumer of the same outbox.** `ClientBuilder::on_*` and `SubscriptionBuilder::on_*` closures per event kind (`FnMut(Event) + Send + 'static`), one per kind, registering again replacing it. An object with a handler has no stream: registering a handler takes over, as a second `events()` call does (Block 15), so the two models never split one object's events. An event counts as delivered when its handler returns, which keeps "delivered means taken" (a handler that returned has taken it) and exactly-once on recoverable channels. Delivery modes: `EventDelivery::Task` (default: the handlers of a client run one at a time on a tokio task of their own), `EventDelivery::Thread` (on an OS thread, for handlers that block), `EventDelivery::Inline` (on the driver, the cheapest; a handler must neither block nor wait). A handler that panics takes nothing (its event's position does not move) and stops the client with an error event, so recovery brings the event back on the next `connect()`. `WeakClient`: `Client` becomes `Clone` (clones share one connection, the last dropped one shutting it down, as dropping does today) and `Client::downgrade()` returns a `WeakClient` whose `upgrade()` gives a `Client` while one is alive. Deadlock detection: while a handler runs in `Thread` or `Inline` mode, a thread-local marker names its client; a call or `ready()` of that client that would have to wait for the loop the handler holds fails at its first poll with a new `CallError` kind, instead of reaching its timeout. In `Task` mode the handler is synchronous and cannot await, so the marker is set on that task too.
- **B. Handlers as a separate delivery path** in the adapter: outboxes call closures directly, beside the streams.
- **C. A handler trait** (`ClientHandler` with a default no-op method per event), registered once, as rdkafka's contexts or russh's handlers.

| | A | B | C |
|---|---|---|---|
| Fit | good: the core and Block 15's outbox unchanged; a handler is a consumer | poor: two delivery paths to keep in step | good: as A, one trait object instead of closures |
| Idiom | good: closures per kind, as centrifuge-js and the official SDK; `Clone` handles with `Weak` downgrades, as `Arc`/`Weak` | fair | good: common in Rust network crates, heavier for one handler |
| Consistency | good: one active consumer per object, as Block 15 | poor | good |
| Tests | good: the Block 15 delivery scenarios run in each mode; a deadlock scenario per mode | poor: every scenario twice | good |
| Semver | additive (`Client: Clone` is an addition; `CallError` and the events are `#[non_exhaustive]`) | additive | additive |
| Cost | L | L | L |

**Recommendation.** A. Whether to add C's trait as a second registration form is left to its block's spec.

## 20. Replay of their TLA+ model traces

**Owner decision (2026-11-06, late): adopted**, against the sans-IO core, keeping the MIT notice of the trace data; Block 16's "optional, later" replay is restored as a planned block. Reading the trace data files is allowed for this block only; the clean-room rule still forbids centrifuge-rust's `crates/` source.

**Problem.** Model traces are an oracle independent of Ferrifuge's own tests: each step of a formal model of the SDK, with its expected states and events, replayed into the core.

**Official SDK** (rust spec §0, §11.1, §11.3): the model is internal to Centrifugal Labs; traces generated by TLC under a narrowing spec specific to their implementation, committed as JSON lines (21 files, about 2.9 MB, under `crates/centrifuge-client-core/tests/tla/traces/`, plus `bugs.jsonl` counterexamples), replayed into their core.

**Designs.**
- **A. Replay with a mapping and an explicit divergence list.** A replay harness (an `@P` simulation, as Block 16's runners) maps each trace step to Ferrifuge `Input`s and compares states and events. Where Ferrifuge decided differently (known today: the backoff reset rule, `disconnect()` failing sent calls with code 3, 2502 dropping the position, delivery by take rather than by handler return, deferred unsubscribes), the step is not skipped silently: the harness names the decision that diverges, counts it, and asserts the rest of the trace; a divergence not on the list fails. The trace files are copied with their MIT notice; their format is learned from the data and its README only.
- **B. Replay only the traces that never meet a divergence**, dropping the others.
- **C. Translate the traces once into Ferrifuge's own scenario format** (Gherkin or Block 16 runner seeds).

| | A | B | C |
|---|---|---|---|
| Fit | good: the core is pure, inputs map to steps | good | fair: loses the traces' step-level oracle |
| Idiom | n/a | n/a | n/a |
| Consistency | good: divergences recorded where the spec records them | fair: hides how much is not checked | fair |
| Tests | good: every step checked or named | poor: coverage shrinks with each divergence | fair |
| Semver | none | none | none |
| Cost | L | M | L |

**Recommendation.** A.

## 25. WebAssembly and browser target

**Problem.** centrifuge-go #67: a client usable from WebAssembly. For Rust, that means front-ends written in Rust (Leptos, Yew, Dioxus), which today must call centrifuge-js through JavaScript bindings.

**Official SDK** (rust spec §1 non-goals, §9.11): not built; prepared for (the core crate builds for `wasm32-unknown-unknown` in CI; `MaybeSend` bounds; browser-meaningless options become configuration errors there).

**Designs.**
- **A. A wasm32 build check of the core now**, the browser adapter on demand. The check guards the sans-IO promise of Block 0 for free; an adapter would use the browser's WebSocket, fetch streams and EventSource, timers from the browser, the same `Stream` API, `withCredentials` as an option (item 24), and no TLS, proxy or real headers.
- **B. The browser adapter now**, behind a feature or as a second crate.

| | A | B |
|---|---|---|
| Fit | good: tests the architecture's main claim | good: the second adapter Block 0 anticipates |
| Idiom | good | fair: `Send` bounds of the current API need relaxing on wasm |
| Consistency | good | fair: options with no browser meaning |
| Tests | good: one CI build job | fair: headless browser tests (wasm-bindgen-test) against Centrifugo |
| Semver | none | additive, but the `Send` bounds may need a breaking change |
| Cost | S | L |

**Owner decision (2026-11-06, late):** design B, the browser adapter, which takes the browser-only options of centrifuge-js (item 39). A's build check comes first, in its block. Before B, verify which core dependencies build for `wasm32-unknown-unknown` (not checked: fastrand's seed source there). Tokio's timers do not work on that target (tokio 1.54.1 docs), so the browser adapter would use browser timers (`gloo-timers` 0.4.0, `futures-timer` 3.0.4) and the browser WebSocket (`web-sys`, or `gloo-net` 0.7.0).

---

## 26. Automatic command batching

Found by the review: part of the official SDK, not in the owner's list.

**Problem.** A burst of commands (subscribing 100 channels in a loop, concurrent publishes from many tasks) costs one write, and over http_stream or sse one HTTP request, per command unless the app opens a Block 18 batch around it. Emulation requests are the expensive case. Block 18 excludes automatic batching.

**Official SDK** (rust spec §9.10, decision 11): one writer per transport takes everything queued when it is ready and writes it as one frame or one emulation request, split at a 15 KB cap; the driver takes every input already waiting before it flushes; no delay timer, so an idle client sends each command at once.

**Designs.**
- **A. An implicit Block 18 batch around each wake of the driver.** Commands the core produces between two wakes are held as in an open batch and sent when the driver performs its actions, through Block 18's frame building: the same cap, the same write-queue admission, the same HTTP 413 resend rule. No delay is added, since the driver wakes as soon as an action is queued. Sans-IO: one input marking the flush.
- **B. Coalescing in the adapter's writer**: frames queued while a write is in progress are merged into one message or request.
- **C. Not adopted**: Block 18's explicit batches suffice.

| | A | B | C |
|---|---|---|---|
| Fit | good: reuses Block 18's core logic, @E-testable | poor: the adapter would merge frames whose write and 413 outcomes the core tracks per frame | good |
| Idiom | good | fair | good |
| Consistency | good: one batching mechanism | poor: a second one with its own 413 handling | good |
| Tests | good: @E burst scenarios; @D frame counts; Block 16's I10 extended | fair | — |
| Semver | additive (an `Input` variant) | none | none |
| Cost | M | M | none |

What A changes: Block 18 D2 says a `send` and a pong are never held; under A they would still go at once, so only what Block 18 holds is batched automatically. centrifuge-js 63ed73e has no automatic batching, so A is a divergence from it, with centrifuge-rust's documentation as the only source.

**Recommendation.** A.

## 27. `Subscription::set_data`

Found by the review: in the official SDK (api.md) and in centrifuge-js 63ed73e (`subscription.ts:382-386`, `setData`), not in Ferrifuge, which has `Client::set_data` (Block 17) and subscription `data` and `get_data` options (Block 9) only.

**Problem.** An app changes what its subscribe requests carry (a room, a filter of its own) without recreating the subscription.

**Designs.**
- **A. `Subscription::set_data`**, applied from the next subscribe attempt and to resubscribes, a `get_data` callback still taking precedence (centrifuge-js `setData` docs); an `Input` variant, as for `Client::set_data`.
- **B. Not adopted**: `get_data` covers dynamic data.

| | A | B |
|---|---|---|
| Fit | good: mirrors Block 17's setters | good |
| Idiom | good | good |
| Consistency | good | fair: a client setter with no subscription counterpart |
| Tests | good: @B, the subscribe proxy records the new data on resubscribe | — |
| Semver | additive | none |
| Cost | S | none |

**Recommendation.** A.

## 28. `Publication.channel` and `Publication.time`

Found by the review: `client.proto` fields 9 and 10 of `Publication`, exposed by the official SDK (api.md), not by Ferrifuge.

**Problem.** `channel` is set when a publication's channel differs from its subscription's (wildcard subscriptions, `client.proto`); `time` is the publication time, which "the current server" does not send to clients (`client.proto`).

**Designs.**
- **A. Add `channel: Option<String>`**, set only when the server sends it; leave `time` out until a server sends it.
- **B. Add both.**

| | A | B |
|---|---|---|
| Fit | good: decoded data | good |
| Idiom | good | fair: a field that is always empty today |
| Consistency | good: `Publication` is `#[non_exhaustive]` | good |
| Tests | good: @D pushes a publication with a channel | fair: `time` only on @D |
| Semver | additive | additive |
| Cost | S | S |

**Recommendation.** A. Whether Centrifugo v6.10.1 has wildcard subscriptions decides @A or @D.

---

## centrifuge-js parity (owner decision, 2026-11-06, late)

Ferrifuge gets every centrifuge-js 63ed73e (5.8.0) feature it still lacks, but only if it makes sense for a native Rust client. Each item first answers three questions: does the problem exist outside a browser or JavaScript runtime; does Centrifugo v6.10.1 still support the server side; would a native Rust application use it rather than an existing Rust alternative. An item that fails is not adopted and never implemented. Experimental features follow the pinned commit: their public API is marked unstable in its documentation, and later centrifuge-js changes are not chased without a decision. Block 0's components rule applies. Inventory from centrifuge-js 63ed73e's README and `src/`, checked against Centrifugo v6.10.1 and centrifugal/centrifuge 7ac0371 (the version v6.10.1 pins).

## 29. Map subscriptions (map, map_clients, map_users; experimental)

**Fit.** Outside a browser: yes, a protocol feature. Server: Centrifugo v6.10.1 supports it (`subscription_type`: stream, map, map_clients, map_users, shared_poll, `internal/configtypes/namespace.go:204-213`; needs ≥ 6.8.0, centrifuge-js README.md:891). Rust alternative: none. **Makes sense.**

**Problem.** A channel whose state is a keyed map (presence lists, live dashboards, game lobbies): the client loads the state page by page, then receives updates per key, and resynchronises after a gap instead of recovering a stream.

**centrifuge-js** (`centrifuge.ts:267`, `:294`, `:322`; `subscription.ts:2771`, `:2896-2913`; `types.ts:676-716`): `newMapSubscription`, `newMapClientsSubscription`, `newMapUsersSubscription`; `sync` and `update` events; `publish(key, data)` and `remove(key)`; `pageSize`; `unrecoverableStrategy`. README.md:880-893: "may change in a backwards-incompatible way".

**Designs.**
- **A. A handle type of its own**, `MapSubscription`, from `Client::new_map_subscription(channel, MapKind::{Map, Clients, Users})`, with its own `MapSubscriptionEvent` stream (`Sync` pages, `Update` per key) and calls `publish(key, data)`, `remove(key)`; the core gains a subscription kind with its state-then-stream phases (`client.proto`: `type`, `phase`, `cursor`, `limit`, `state`).
- **B. One `Subscription` with a `type` option**, the map events added to `SubscriptionEvent`.

| | A | B |
|---|---|---|
| Fit | good: a subscription kind in the core, Block 15 delivery reused | fair: map phases mixed into stream subscriptions |
| Idiom | good: types say what a handle can do | poor: `publish(key)` valid on some subscriptions only |
| Consistency | good: as `Subscription` | fair |
| Tests | good: @A on Centrifugo v6.10.1 (`subscription_type: map`); @D for pages and resync | good |
| Semver | additive, marked unstable | additive, and every `SubscriptionEvent` match grows |
| Cost | L | L |

**Recommendation.** A, its API documented as unstable.

## 30. Shared poll subscriptions (experimental)

**Fit.** Outside a browser: yes. Server: Centrifugo v6.10.1 supports it (`namespace.go:213`, `:244`; HMAC keys `types.go:75-81`; `shared_poll_refresh` proxy `types.go:521`). Rust alternative: none. **Makes sense.**

**Problem.** Many clients tracking the same items (prices, statuses) without each polling the backend: the server polls once per key and fans the versions out; the client tracks and untracks keys, each signed by the backend.

**centrifuge-js** (`centrifuge.ts:350`; `subscription.ts:2915-3084`; `types.ts:791-816`): `newSharedPollSubscription`; `track(keys)`, `untrack(keys)`, `trackedKeys()`; a `getSignature` callback; versions deduplicated per key. README.md:895-908 marks it experimental.

**Designs.**
- **A. A handle type of its own**, `SharedPollSubscription`, with `track`, `untrack`, `tracked_keys` and an async `get_signature` callback built like `get_token` (Block 10); the client checks no signature (the server does), so no cryptography is involved.
- **B. Options and methods on `Subscription`.**

| | A | B |
|---|---|---|
| Fit | good: a kind in the core, per-key versions there | fair |
| Idiom | good | poor: track on a stream subscription is meaningless |
| Consistency | good: the callback as `get_token` | fair |
| Tests | @B: the shared-poll refresh proxy answered by the test backend; @D | same |
| Semver | additive, marked unstable | additive |
| Cost | L | L |

**Recommendation.** A, its API documented as unstable.

## 31. Optimistic subscriptions

**Fit.** Outside a browser: yes (saves a round trip per connection). Server: the client protocol carries several commands per frame; the server's handling of subscribes before the connect reply needs verifying on v6.10.1. Rust alternative: none. **Makes sense.**

**Problem.** A client that subscribes at startup waits a full round trip for the connect reply before subscribing; on mobile links that doubles the time to the first publication.

**centrifuge-js** (`centrifuge.ts:1240-1258`): when a websocket opens (never over emulation, which needs the connect reply's session), the connect command and the subscribe commands of the subscriptions subscribing go out in one batch, before the connect reply.

**Designs.**
- **A. As centrifuge-js, always over websocket**: the subscribes join the connect command's frame; their replies are handled after the connect reply; a failed connect fails them as a teardown does. Reverses Block 18's out-of-scope line.
- **B. Opt-in**, a `Config` option.

| | A | B |
|---|---|---|
| Fit | good: a core change in the connect step | good |
| Idiom | good | fair: an option for a behaviour with no downside found |
| Consistency | good: centrifuge-js behaviour | fair |
| Tests | good: @D, one frame with the connect and the subscribes; @A on v6.10.1 | good |
| Semver | none | additive |
| Cost | M | M |

**Recommendation.** A, after verifying the server's ordering on v6.10.1 (`VERIFY` in its block).

## 32. Client publish debouncing

**Fit.** Outside a browser: yes (fewer publishes from chatty producers). Server: `SubscribeResult.publish_debounce` is set by the centrifuge library from `ClientPublishDebounceInterval` (`client.go:5101-5102`, `client_map.go:1062`), which Centrifugo v6.10.1 OSS never sets: a Centrifugo PRO feature, as channel compaction. Rust alternative: none. **Makes sense** (PRO users), tested on @D as compaction (item 7).

**centrifuge-js** (`subscription.ts:255-312`, `:573-575`, `:2626-2628`): with a debounce interval from the subscribe reply, publishes of a key within the interval are merged, the latest data sent when it ends.

**Designs.**
- **A. In the core**: a debounce timer per subscription and key, the latest data kept, @E-testable.
- **B. In the adapter**, around the call futures.

| | A | B |
|---|---|---|
| Fit | good: a timer and state in the core | fair: the adapter has no per-subscription state |
| Idiom | good | fair |
| Consistency | good: as Block 13's calls | fair |
| Tests | good: @E timing, @D reply carrying `publish_debounce` | fair |
| Semver | additive (a call outcome) | additive |
| Cost | M | M |

**Recommendation.** A, with item 29 (keys are map keys).

## 33. WebTransport transport (experimental)

**Fit.** Outside a browser: yes (QUIC: connection migration across networks, no TCP head-of-line blocking; relevant to mobile fleets). Server: Centrifugo v6.10.1 supports it only with experimental HTTP/3 (`webtransport.enabled`, `types.go:122-126`; `mux.go:407-411`; webtransport-go v0.13.0). Rust alternative: none for Centrifugo. **Makes sense.**

**centrifuge-js** (`transport_webtransport.ts:4-148`; `centrifuge.ts:1076-1081`): one bidirectional stream; JSON or Protobuf; README.md:174 marks it experimental.

**Designs.**
- **A. A fourth `TransportKind`** over `wtransport` 0.7.2 (2026-08-11, MIT OR Apache-2.0, MSRV 1.88, rustls 0.23 with ring, aws-lc-rs or fips features, on quinn 0.11), behind a `webtransport` cargo feature, in the fallback list.
- **B. The same over `web-transport-quinn`** 0.13.6 (2026-11-03, MIT OR Apache-2.0; no fips feature).

| | A | B |
|---|---|---|
| Fit | good: a transport of the adapter, the core unchanged | good |
| Idiom | good | good |
| Consistency | good: keeps the fips build's provider (Block 6) | poor: no FIPS build |
| Tests | @A with Centrifugo's experimental HTTP/3 and the test CA; interop with webtransport-go 0.13 to verify | same |
| Semver | additive, feature-gated, marked unstable | same |
| Cost | L | L |

**Recommendation.** A (the components rule: maintained crates; cryptography stays in rustls).

## 34. SockJS transport

**Fit.** Outside a browser: no (SockJS emulates WebSocket in old browsers). Server: removed in Centrifugo v6 (centrifuge-js README.md:145-147; no SockJS in v6.10.1's sources or `go.mod`). Rust alternative: n/a. **Not adopted**: Centrifugo v6.10.1 cannot serve it, and no native client needs it.

## 35. An injectable connector (centrifuge-js's custom WebSocket constructor)

**Fit.** Outside a browser: yes, in its Rust form: applications bring their own networking (a VPN tunnel, a custom proxy, an in-process transport for tests). Server: n/a. Rust alternative: none for Ferrifuge. **Makes sense.**

**centrifuge-js** (`centrifuge.ts:1070-1100`): `websocket`, `fetch`, `readableStream`, `eventsource` constructor options.

**Designs.**
- **A. `ClientBuilder::connector`**: an async function from the endpoint URL to a connected byte stream (`AsyncRead + AsyncWrite + Send + Unpin`); Ferrifuge runs TLS (when the scheme asks) and the websocket handshake over it. It is also what item 22's in-memory server double plugs into.
- **B. A public `Transport` trait** applications implement end to end.

| | A | B |
|---|---|---|
| Fit | good: adapter only | fair: exposes the adapter's internals |
| Idiom | good: as tokio-tungstenite's `client_async` | fair |
| Consistency | good: with item 1's proxy decision (a connector is the general case) | fair |
| Tests | good: @D through a test connector | good |
| Semver | additive | additive, but every transport change breaks implementers |
| Cost | M | L |

**Recommendation.** A.

## 36. Protobuf over http_stream

**Fit.** Outside a browser: yes. Server: Centrifugo v6.10.1 serves it (`Content-Type: application/octet-stream`, centrifuge `handler_http_stream.go:73-74`; sse has no Protobuf). Rust alternative: n/a. **Makes sense.** Reverses Block 8's decision rejecting Protobuf with an HTTP transport at build time, for http_stream only.

**centrifuge-js** (`transport_http_stream.ts:107-119`, `:176-183`).

**Designs.**
- **A. Length-delimited Protobuf in the http_stream decoder**, split with prost's `decode_length_delimiter` (a maintained component) over a bounded buffer, fuzzed with the other decoders; sse stays JSON.
- **B. Keep rejecting it.**

| | A | B |
|---|---|---|
| Fit | good: a decoder mode, the core unchanged | good |
| Idiom | good | good |
| Consistency | good: the codec seam (Block 5c) | fair: a combination centrifuge-js allows |
| Tests | good: @A over http_stream with Protobuf; fuzz | — |
| Semver | none (a build error becomes a working build) | none |
| Cost | M | none |

**Recommendation.** A, with the components block (27) that replaces the http_stream decoder.

## 37. `hasRecoveredPublications` on the subscribed event

**Fit.** Yes, client-only (metrics). **Makes sense.** centrifuge-js `types.ts:324-326`, `centrifuge.ts:2491`, `:2520`.

**Designs.** A: `Subscribed::has_recovered_publications: bool`. B: leave it to the application to count. **Recommendation.** A (one field, `Subscribed` is `#[non_exhaustive]`).

## 38. Delta statistics (`deltaStats()`)

**Fit.** Yes (operators measure what delta saves). **Makes sense.** centrifuge-js `subscription.ts:388-400`, `types.ts:818-830`.

**Designs.** A: `Subscription::delta_statistics()` (publications, full payloads, deltas, bytes received and decoded), next to item 8's `statistics()`. B: folded into the client's `statistics()` per channel. **Recommendation.** A, with item 8.

## 39. Browser-only options

**Fit.** Online and offline events, page hide and show (back/forward cache), fetch, ReadableStream and EventSource shims, `withCredentials` (`centrifuge.ts:796-918`, `types.ts:177`, `:184`): browser APIs. **Not native items**: they belong to the browser adapter, item 25 design B (owner decision), whose block takes them.

## Components audit (owner decision, 2026-11-06)

The components rule (Block 0, PR #97) applied to the existing in-house parsing and encoding of network input in `src/`. Existing code is unchanged; proposals become blocks in the owner's order. Checked through the reviewer.

| # | Component | Where | Maintained replacement | Proposal | Fuzz target | Size bound |
|---|---|---|---|---|---|---|
| 1 | SSE (text/event-stream) parser | `src/adapter/http.rs` `Decoder`, sse mode | sse-parse 0.1.0 (2026-08-24, MIT, forbid(unsafe), fuzzed upstream; `max_buffer` counts line + data + event, overflow drops the event and counts it; decodes invalid UTF-8 to U+FFFD where Ferrifuge passes bytes on); finesse 2.4.3 (2026-11-03, Apache-2.0, MSRV 1.88, no dependencies, separate line and event limits, fails with `Overflow`; possible quadratic drain on many tiny lines, not measured); sse-core 0.2.3 (2026-07-31, per-field limits, `Buf` API) | replacement block, choosing among the three in its spec; Ferrifuge keeps failing on overflow and keeps its fuzz target | yes | yes, 64 MiB (line + data) |
| 2 | http_stream newline-delimited splitter | same `Decoder`, other mode | tokio-util 0.7.20 `LinesCodec::new_with_max_length` (already in the build through reqwest; a direct dependency with feature `codec`, no new crate) | in the same block as 1; Ferrifuge keeps the length error fatal and keeps skipping blank lines | yes | yes, 64 MiB |
| 3 | `Content-Type` check of sse responses | `src/adapter/http.rs:249` `is_event_stream` | mime 0.3.18 (hyperium, 2026-11-05; a new direct dependency; the release before it is from 2023-03-20) | in the same block as 1 | no | header size bounded by hyper |
| 4 | Protobuf length-delimited framing | `src/protobuf.rs:19-60` | already prost's | none | yes | frame bounded by the transport |
| 5 | JSON reply decoding | `src/json.rs:60-82` and `reply()` | already serde_json; the field dispatch is client logic | none | yes | frame bounded by the transport |
| 6 | JSON command encoding, emulation request bodies | `src/json.rs:31-112` | already serde_json; the client's own output | none | — | write queue and frame cap |
| 7 | Websocket, TLS, HTTP, URLs, headers | tungstenite, rustls, reqwest/hyper, url, http | components | none | — | 64 MiB websocket message and frame |

The client also re-parses frames it encoded itself (`Codec::command_ids`, `json::emulation_frame`, the line split in `Machine::resend_alone`): not network input, outside the rule.

Separate from the rule, a solidifier review found drift from Block 5c ("`machine.rs` calls no codec module directly"): `src/machine.rs:1502`, `:1577`, `:1976` and `src/machine/calls.rs:321` call `json::` directly. Proposal: a refactor block routing them through `Codec` or an emulation helper, with no API change.

## Questions for the owner

Each question has a recommendation. Reply per question; "OK" approves the recommendation.

| Q | Question | Recommendation |
|---|---|---|
| Q0 | Approve, change or reject each item's recommendation in the summary table (items 26–28 included), and the order of the blocks | as in the summary table, in that order |
| Q1 | Default proxy behaviour (item 1): follow the environment, then OS settings, as browsers do; or direct unless configured | environment, then OS settings |
| Q3 | Logging (item 9): `tracing` or `log` | `tracing` with its `log` feature, adapter only, no payload switch |
| Q4 | Publishing (item 21): the owner creates the crates.io ownership (the name is free), does the first publish, adds the trusted publisher and a `release` environment, and turns on private vulnerability reporting | yes |
| Q6 | JSON helpers (item 18): always available; or behind a `serde` feature | always available |

## Decided through the reviewer

Answered without the owner under the delegation of 2026-11-06; listed for audit.

| Question | Answer | Why | Sources |
|---|---|---|---|
| TLS under FIPS (item 2): keep webpki verification in the fips build, or use the platform verifier there too | keep webpki in the fips build | The owner asked for a question only if `ClientConfig::fips()` stopped being true; it stays true either way, and keeping webpki leaves Block 6 unchanged | rustls 0.23.46 `client_conn.rs:396-409`; rustls-platform-verifier 0.7.1 |
| TLA+ traces (item 20): an exception to the clean-room rule to read the trace data under `crates/` | superseded: the owner adopted item 20 (2026-11-06, late) and allowed reading the trace data files for its block only | — | owner decision |
