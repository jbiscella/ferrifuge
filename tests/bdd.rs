//! Runs the Gherkin scenarios in `tests/features/`.
//!
//! Only scenarios listed in [`IMPLEMENTED`] run: feature files are verbatim
//! copies of CLAUDE.md and also hold scenarios of later sub-increments.
//! Select a test layer with cucumber's tag filter, e.g.
//! `cargo test --test bdd -- --tags @A`, or scenarios by name with `--name`:
//! both narrow that list, never widen it. Scenarios with undefined steps
//! fail the run rather than being skipped.

mod steps;
mod support;

use std::collections::HashSet;
use std::process::{self, Command};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use cucumber::gherkin::{Feature, GherkinEnv, Rule, Scenario};
use cucumber::runner::ScenarioType;
use cucumber::tag::Ext as _;
use cucumber::{World, cli};

use steps::ClientWorld;
use support::centrifugo::{self, Centrifugo};
use support::fake_server::{Announce, FakeServer};
use support::proxy::ProxyBackend;
use support::relay::CountingRelay;
use support::tls;
use support::token::{HMAC_SECRET, SHARED_POLL_SECRET};

const FEATURES: &str = "tests/features";

/// Scenarios whose behaviour is implemented, by name.
const IMPLEMENTED: &[&str] = &[
    // Block 46, sub-increment 46a.
    "Publishes within the interval are merged and the latest is sent when it ends",
    "A window with nothing to send closes",
    "A window stays open while it keeps sending",
    "A stale debounce timer is ignored",
    "An interval longer than the call timeout is clamped",
    "A publish made while subscribing is debounced once subscribed",
    "Without an interval every publish is sent",
    "A later subscribe reply without an interval turns debouncing off",
    "A superseded publish waits for the send that replaced it",
    "A superseded publish fails with the error of the send that replaced it",
    "A publish the server refuses at once keeps its own result",
    "Client-level publishes are not debounced",
    "Leaving subscribed drops what a window holds",
    "A window dropped by a reconnect is not resent",
    "A trailing send lost with its transport fails the publishes that followed it",
    "A trailing send waits for room in the write queue",
    "A trailing send is held by an open batch",
    "Trailing sends of a burst share a frame with automatic batching",
    "Dropping a publish withdraws its data only when no other publish waits for it",
    "A Thread handler's publish waiting in a window is not a would-be deadlock",
    "Publishing without Centrifugo PRO sends every publish",
    // Block 1, sub-increment 1a.
    "New client starts disconnected",
    "Successful connect",
    "Explicit disconnect stops reconnection",
    "Connect after explicit disconnect",
    "Connect failures not reflected in state are reported as errors",
    // Block 1, sub-increment 1b.
    "Temporary connection loss then successful reconnect",
    "Connection loss then terminal error on reconnect",
    // Block 1, sub-increment 1c.
    "Terminal condition while connected",
    // Block 2.
    "Delays never exceed the maximum",
    "Delay upper bound grows exponentially until capped",
    "Delay upper bound per attempt",
    "Delays are jittered",
    "Backoff options are configurable",
    // Block 3.
    "Application disconnect codes sent via server API",
    "Protocol-range disconnect codes",
    "Disconnect returned by the connect proxy",
    // Block 4.
    "Client answers ping with pong",
    "Client does not answer when pong is not expected",
    "Missing pings trigger reconnect",
    "Regular pings keep the connection alive",
    // Block 5, sub-increment 5b.
    "Token refreshed before expiry keeps the connection",
    // Block 5, sub-increment 5c.
    "The client speaks JSON by default",
    // Block 5, sub-increment 5d.
    "The connected event names the transport",
    // Block 6.
    "Secure websocket connection with the build's provider",
    // Block 7.
    "Connect with Protobuf",
    "Frames are binary Protobuf",
    "Ping answered in Protobuf",
    "Every message of a multi-message Protobuf frame is handled",
    "Malformed Protobuf frame forces a reconnect",
    // Block 8.
    "Fallback to http_stream when websocket is unreachable",
    "Fallback over HTTPS uses the build's TLS provider",
    "Fallback to sse when websocket and http_stream are unreachable",
    "A transport that worked is kept on reconnect",
    "Commands work over an emulated transport",
    "A failed emulation request makes the client reconnect",
    "An HTTP stream that never answers the handshake falls back",
    // Brief C3: an opened websocket is chosen only when the server answers.
    "A websocket that never answers the handshake falls back",
    "An opened websocket without a connect reply falls back",
    "A websocket write the server never reads closes the transport",
    "A stalled websocket write does not delay disconnect",
    "Every transport unreachable",
    "A failed fallback sequence is retried after the reconnect backoff",
    "The fallback sequence is retried until a transport succeeds",
    "Protobuf with the sse transport is rejected",
    "Server pings over an HTTP transport are answered",
    // Block 9.
    "New subscription starts unsubscribed",
    "Subscribe while connected",
    "Subscribe before connect",
    "Publication delivered",
    "Publication delivered over each transport",
    "Publication metadata is delivered",
    "Resubscribe after reconnect",
    "Unsubscribe stops publications",
    "Duplicate subscription is refused",
    "Permanent subscribe error",
    "Unsubscribe during an in-flight subscribe over an emulated transport",
    "Subscriptions survive an explicit disconnect and connect",
    "Initial publications in the subscribe reply are delivered",
    "JSON publication bytes are exposed exactly as received",
    "A subscribe without a reply makes the client reconnect",
    "Data returned with the subscribe reply is delivered",
    "Subscription data is sent on subscribe and on resubscribe",
    "Subscription data from a callback is renewed on resubscribe",
    "Each subscription data callback receives its own channel",
    "A failing subscription data callback is retried",
    "A data callback denying access unsubscribes",
    "Subscriptions are found by channel and listed",
    "A subscription recreated after removal stays subscribed on an emulated transport",
    "A subscription resubscribed at once stays subscribed on an emulated transport",
    "Messages after a disconnect in the same frame are discarded",
    "Removing a subscription unsubscribes it first",
    "Binary publication delivered over Protobuf",
    "Without a jitter window the first reconnect follows the backoff",
    "The jitter window does not delay the first connect",
    "The jitter window does not delay a connect after an explicit disconnect",
    "Server unsubscribe codes",
    "After a resubscribe-advised unsubscribe the subscription is subscribed again",
    "Resubscribe delays are bounded",
    "Resubscribe delay upper bound grows exponentially until capped",
    "Resubscribe delays are jittered",
    "A successful subscribe resets the resubscribe backoff",
    "Resubscribe backoff options are configurable",
    "Subscription token refreshed before expiry keeps the subscription",
    "A failing first subscription token fetch is retried before subscribing",
    "Subscribe error 109 fetches a new token and resubscribes",
    "A failing subscription token callback is retried",
    "A temporary sub_refresh error is retried",
    "A sub_refresh without a reply is retried",
    "A subscription token denied before the first subscribe unsubscribes",
    "Ending a subscription during the resubscribe backoff cancels the retry",
    "Ending a subscription during the refresh retry delay cancels the refresh",
    "A refresh reply without expiry stops refreshing",
    "A permanent sub_refresh error unsubscribes",
    "An expiring fixed subscription token without a callback unsubscribes",
    "A subscribe rejected for an expired token without a callback unsubscribes",
    "A subscribe rejected for an expired token fetches a new one after the backoff",
    "An empty subscription token before the first subscribe unsubscribes",
    "An empty subscription token on refresh unsubscribes",
    "The data callback runs only after a subscription token is obtained",
    "The subscription token is fetched before the data callback runs",
    "A subscription that has a token calls the data callback without the token callback",
    "Each subscription token callback receives its own channel",
    "Subscription token denied",
    // Block 11a: stream positions and recovery.
    "A first subscribe does not recover",
    "Positioned and recoverable options are requested",
    "The subscribed event reports the granted capabilities",
    "The first subscribe reply's position is used for recovery",
    "A changed epoch on a live publication is used for recovery",
    "Recovered publications advance the stored position",
    "Missed publications are recovered",
    "Publications between unsubscribe and subscribe are recovered",
    "Publications made while explicitly disconnected are recovered",
    "The since option recovers on the first subscribe",
    "Recovery fails when history is gone",
    "A failed recovery's reply replaces the stored position",
    // Block 11b: getState.
    "getState provides the position of the initial subscribe",
    "getState reloads state when recovery fails",
    "The since option takes precedence over getState on the first subscribe",
    "A failing getState is retried before subscribing",
    // Block 11c: state invalidation. "Connection state invalidation clears
    // the token and invalidates every position" is with Block 12c, which
    // brings its server-side positions (owner decision).
    "State invalidation fetches a new subscription token",
    "Connection state invalidation keeps a fixed connection token",
    "Connection state invalidation keeps a fixed subscription token",
    "State invalidation drops a fixed subscription token",
    // Block 12a: server-side subscriptions granted at connect.
    "Publications of a server-side subscription are delivered",
    "Server-side subscriptions survive a reconnect",
    "Client-side and server-side subscriptions to one channel stay separate",
    "Server-side subscriptions granted at connect over an emulated transport",
    "Initial publications of a server-side subscription granted at connect are delivered",
    "A server-side subscription missing after reconnect is unsubscribed",
    // Block 12b: subscriptions the server adds or removes later.
    "A subscription added by the server after connect is tracked",
    "A server-side subscription removed by the server is unsubscribed",
    "Server-side subscribed events carry the server's data",
    // Block 12c: positions and recovery, and Block 11's 3014 scenario.
    "Missed publications of a server-side subscription are recovered",
    "Server-side subscriptions survive an explicit disconnect and connect",
    "A subscription added by a push is recovered after reconnect",
    "A server-side subscription missing after reconnect is forgotten",
    "A pushed server-side subscription's position is used for recovery",
    "Recovered server-side publications advance the stored position",
    "A server-side unsubscribe carries its code and reason and forgets the subscription",
    "Connection state invalidation clears the token and invalidates every position",
    // Review item C4.
    "Close code 1009 stops the client",
    // Review item C2.
    "An emulated transport the server answered is kept",
    // Block 15, sub-increment 15a.
    "Publications not taken before disconnect are recovered on the next connect",
    "Server-side publications not taken before disconnect are recovered on the next connect",
    "Publications not taken before unsubscribe are withdrawn",
    "A second events() call takes over the subscription's stream",
    "A second events() call takes over the client's stream",
    "A cancelled read loses no publication",
    "Publications of a subscription without a stream do not hold the connection",
    // Block 15, sub-increment 15b.
    "The server closing the connection is handled only after the publications before it are taken",
    "A state change not taken holds the messages of the next connection",
    "A stream that is not read stops the client reading after 256 items",
    "The read-ahead also stops at 4 MiB",
    "An item larger than the read-ahead budget is still delivered",
    "A read-ahead limit of 0 is rejected",
    "Pings are answered while a stream is not read",
    "A full read-ahead does not make the client report missing pings",
    "A subscribe reply waiting behind a publication not taken does not time out",
    "The connection token is refreshed while a stream is not read",
    "A slow consumer on a channel with history and recovery loses no publication",
    // Block 15, sub-increment 15c.
    "A client stream not read keeps every state change but drops errors past 256 untaken events",
    "A subscription stream not read drops errors past 256 untaken events",
    // Block 15, sub-increment 15d.
    "Reconnect delays keep growing while the server closes each connection with 3008",
    "A Disconnect push with 3008 keeps the backoff growing over an emulated transport",
    "A server ping read before 3008 lets the reset stand",
    "A close with another code lets the reset stand",
    // Block 17, sub-increment 17a.
    "Connect data is sent on connect and on reconnect",
    "Connect data from a callback is renewed on every attempt",
    "A failing connect data callback is retried after the reconnect backoff",
    "A connect data callback denying access disconnects the client",
    "A disconnect while the data callback runs opens no transport",
    "The connect data callback runs only after a connection token is obtained",
    "The connection token is fetched before the connect data",
    "A client that has a token calls the data callback without the token callback",
    "Data returned with the connect reply is delivered",
    "The connected event carries the server's data exactly",
    // Block 17, sub-increment 17b.
    "HTTP headers reach the connect proxy over each transport",
    "HTTP headers ride on every request of an emulated transport",
    "Emulated headers reach the connect proxy",
    "A header the transport sets is rejected",
    "Client options do not show secrets",
    // Block 17, sub-increment 17c.
    "set_token replaces an expired token when there is no token callback",
    "set_token does not skip the token callback after an expired token",
    "set_data applies from the next connection attempt",
    "set_emulated_headers applies from the next connection attempt",
    // Block 18, sub-increment 18a.
    "Calls made while batching go out in one frame when the batch ends",
    "Subscriptions subscribed while batching send their subscribe commands in one frame",
    "A batch over Protobuf is one binary frame",
    "A batch is split at the frame cap",
    "Held calls are admitted to the write queue one by one",
    "A call held past the timeout fails and is never sent",
    "A subscribe held past the timeout reconnects the client",
    "The client's own commands and send are not held",
    "Disconnect fails the calls a batch holds",
    "A held call waits across a lost transport",
    "Stopping one of two batches sends what is held",
    // Block 18, sub-increment 18b.
    "A batch refused with HTTP 413 is resent command by command",
    "Over an emulated transport only the command refused alone fails",
    // Block 18, sub-increment 18c.
    // Over http_stream since Block 36 (D10).
    "A reconnect over an emulated transport sends its subscribe commands in one request",
    "The subscribe commands of a connect reply are split at the frame cap",
    "An open batch does not hold the subscribe commands of a connect reply",
    // Block 13, sub-increment 13a: calls and RPC.
    "RPC returns the proxy's result",
    "Temporary RPC error is reported as such",
    "Concurrent calls get their own replies",
    "A call without a reply times out",
    "A call made before connect waits for the connection",
    "A call made while connecting waits for the connection",
    "A queued call fails after the timeout when no connection comes",
    "A call that timed out while queued is never sent",
    "A call on a client never connected fails after the timeout",
    "A terminal connect failure fails a queued call at once",
    "Disconnect fails a queued call at once",
    "Disconnect fails a call already sent at once",
    "A call whose emulation request fails fails at once",
    "Losing the transport fails calls already sent at once",
    // Block 13, sub-increment 13b: publish, history, presence and
    // subscription-level calls.
    "Publish reaches subscribers",
    "History returns the last publications",
    "A subscription command fails after the timeout when the subscribe never completes",
    "History since a known position",
    "History in reverse order",
    "History with limit 0 returns only the stream position",
    "A client-published publication carries its publisher",
    "Subscription-level commands use the subscription's channel",
    "A subscription command on an unsubscribed subscription fails at once",
    "A subscription command waiting for a failed subscribe fails at once",
    "A subscription command waits until the subscription is subscribed",
    "Presence lists the subscribed clients",
    "Presence stats count clients and users",
    // Block 13, sub-increment 13c: send, the message push and the Protobuf
    // round-trips.
    "Binary publish over Protobuf keeps its bytes",
    "Asynchronous server messages are delivered with their exact bytes",
    "Asynchronous server messages over Protobuf keep binary data",
    "Binary RPC over Protobuf keeps its bytes",
    "Send delivers a message without a reply",
    "Commands round-trip over Protobuf",
    "Send over Protobuf is received without a reply",
    // Block 13, sub-increment 13d: the write-queue bound.
    "A call that does not fit the write queue waits until earlier calls are written",
    "A call waiting for room fails after the timeout and is never sent",
    "A call larger than the write queue is sent once nothing waits to be written",
    "The client's own commands are not held by a full write queue",
    "Calls are admitted in the order they were made",
    "A call behind one that stopped waiting is sent when it fits",
    "A call waiting for room keeps its deadline when the transport is lost",
    "A write queue limit of 0 is rejected",
    "Sends beyond the write queue go out as earlier ones are written",
    // Block 13, sub-increment 13e: join and leave.
    "Join and leave of other clients are delivered with the join_leave option",
    "Without the join_leave option no join or leave event is delivered",
    "The join_leave option is sent on subscribe and on resubscribe",
    "Join and leave pushes of a server-side subscription are delivered",
    "A join push for a subscription that is not subscribed is dropped",
    "Join and leave events not taken before unsubscribe are withdrawn",
    // Block 19, sub-increment 19a.
    "Delta compression is negotiated with the server",
    "Publications sent as deltas arrive whole",
    "A delta publication is applied to the previous publication",
    "The delta option is sent on subscribe and on resubscribe",
    "With JSON, publications of a delta subscription arrive as their JSON text",
    // Block 19, sub-increment 19b.
    "A broken delta makes the subscription resubscribe",
    "A delta with no base makes the subscription resubscribe",
    "A delta on a subscription without delta on makes it resubscribe",
    "Each subscribe starts a new delta chain",
    "A broken delta among recovered publications makes the subscription resubscribe from the same position",
    "Resubscribe delays keep growing while every subscribe is followed by a broken delta",
    "A delta on a server-side subscription makes the client reconnect",
    // Block 19, sub-increment 19c.
    "Publications missed by a delta subscription are recovered whole",
    // Block 20: release setup.
    "The changelog is in step with the crate's version",
    "The release check refuses a tag that does not name the crate's version",
    "The release check refuses a version without a changelog section",
    "The published package holds only what the crate needs",
    "The quickstart example receives a publication",
    // Block 21: logging.
    "The core reports a frame that is not client protocol data",
    "The core reports a reply without its command's result",
    "State changes are logged with their codes and reasons",
    "Subscription records are in the subscription's span",
    "A frame that is not client protocol data is logged as a warning",
    "Endpoints are logged without credentials, query or fragment",
    "No secret reaches a record",
    "The records of two clients are told apart",
    // Block 22: TLS verification.
    "A private certificate authority given as an extra root is trusted",
    "A server whose CA is not trusted is refused",
    "Extra roots also apply to HTTPS transports",
    "A raw rustls configuration is used as given",
    "A raw rustls configuration replaces the platform's trust",
    "Extra roots and a raw configuration together are rejected",
    "An extra root that does not parse is rejected",
    // Block 23: proxies.
    "A websocket goes through a proxy with credentials",
    "Secure endpoints go through a proxy, with TLS to Centrifugo",
    "Plain HTTP transports go through an HTTP proxy in absolute form",
    "A proxy refusing the credentials fails the attempt",
    "An endpoint in the no-proxy list is reached directly",
    "A proxy function decides per endpoint",
    "A proxy set at run time applies from the next attempt",
    "A proxy URL of another scheme is refused",
    // Block 25, sub-increment 25a: system sleep.
    "A sleep as long as the no-ping window reconnects after the backoff and jitter",
    "A shorter sleep keeps the connection and restarts the no-ping window",
    "Without pings a sleep keeps the connection",
    "A gap under 2 s changes nothing",
    "A refresh due during a short sleep starts at the wake",
    "A refresh not yet due moves earlier by the sleep",
    "A token due during a long sleep is fetched before the next connect",
    "A subscription token due during a short sleep is refreshed at the wake",
    "A sleep leaves reply deadlines alone",
    "A token due during a sleep in a reconnect delay is fetched before the next connect",
    "A sleep while connecting keeps the reconnect delay",
    // Block 25, sub-increment 25b: reconnect().
    "reconnect() while connected skips the backoff but keeps the jitter window",
    "reconnect() while connecting starts an attempt at once and keeps waiting calls",
    "reconnect() tries the first transport again",
    "reconnect() does nothing to a disconnected client",
    "reconnect() reconnects at once without \"disconnected\"",
    "Publications not taken before reconnect() are recovered once",
    // Block 25, sub-increment 25c: captive portals.
    "A captive portal answering the websocket upgrade is reported",
    // Block 27, sub-increment 27b: Protobuf over http_stream.
    "Fallback to http_stream with Protobuf",
    "Binary publication delivered over Protobuf on http_stream",
    "Server pings over Protobuf on http_stream are answered",
    // Block 28: the tokio runtime handle.
    "Building outside a runtime without a handle fails",
    "A client built outside a runtime with a handle connects",
    "The client ends when its runtime shuts down",
    // Block 29: the handshake timeout and the maximum message size.
    "An open that never completes is bounded by the handshake timeout",
    "The connect reply keeps the timeout after the open",
    "A message over the limit ends a connected client",
    "A message at the limit is delivered",
    "A message over the limit before the connect reply fails the attempt",
    "A maximum message size of 0 is rejected",
    // Block 30: waiting until ready.
    "Client ready resolves on the state, before the \"connected\" event is taken",
    "Client ready on a disconnected client fails at once",
    "Disconnect fails a waiting client ready",
    "Client ready keeps waiting across failed attempts",
    "A terminal connect failure fails a waiting client ready",
    "Subscription ready resolves on the state, before the \"subscribed\" event is taken",
    "Subscription ready on an unsubscribed subscription fails at once",
    "A permanent subscribe error fails a waiting subscription ready",
    "Subscription ready made before connect waits for the connection",
    "Disconnect fails a waiting subscription ready",
    // Block 31: statistics and delta statistics.
    "A new client's statistics are zero",
    "The core counts attempts, connects and publications taken",
    "The core counts messages as read and as written",
    "Statistics count a session's traffic as the fake server sees it",
    "Statistics count failed attempts",
    "Statistics count across reconnects",
    "Only publications taken count as delivered",
    "Delta statistics count full and delta payloads",
    "Delta statistics stay zero unless delta is on",
    // Block 32: runtime HTTP headers and subscription data.
    "set_http_headers applies from the next connection attempt",
    "set_http_headers applies to the emulation requests of the open transport",
    "set_http_headers refuses a header the transport sets",
    "Subscription set_data applies from the next resubscribe",
    "A subscription data callback takes precedence over set_data",
    // Block 33: closing a client.
    "close waits until the connection is closed and the stream ends",
    "close unsubscribes every subscription with code 2 and sends no unsubscribe command",
    "close ends a server-side subscription before the client's \"disconnected\"",
    "A call pending at close fails with \"client closed\"",
    "Dropping the client unsubscribes its subscriptions with code 2",
    "close on a disconnected client resolves at once",
    "A closed client stays closed",
    // Block 34: the table of codes.
    "Every code constant has one row with its value",
    "The table names no code the crate lacks",
    "The README's table is the documentation's",
    "Every quoted reason is one the client sends",
    // Block 35a: tag filters.
    "A tag filter delivers only matching publications",
    "The tag filter is sent on subscribe and on resubscribe",
    "A tag filter set on a subscription is sent from the next subscribe",
    "A tag filter with delta is refused",
    "A tag filter the server would reject is refused",
    // Block 35b: channel compaction.
    "Channel compaction is requested on every subscribe",
    "Pushes with a numeric channel id reach their subscription",
    "A push with an unknown channel id is dropped",
    "Numeric channel ids are forgotten when the connection ends",
    "Channel compaction on a server without it changes nothing",
    // Block 36a: automatic command batching.
    "The core batches automatically only when its owner enables it",
    "Automatic batching keeps the order of commands, send and pong included",
    "Automatic batching splits at the frame cap",
    "Automatic batching admits calls to the write queue as they are made",
    "An explicit batch holds its commands across flushes",
    "Commands refused with HTTP 413 are resent alone",
    "A call abandoned before the flush is never sent",
    "A burst of calls goes out in one frame",
    "A burst of subscribes goes out in one frame",
    "Calls a new connection releases go out together",
    "A burst of publishes reaches Centrifugo in one frame",
    // Block 35c: publication channel, recovered publications.
    "A publication of a wildcard subscription carries its channel",
    "A publication of another channel does not move the stream position",
    "The subscribed event says whether recovered publications follow",
    "A server-side subscribed event says whether publications follow",
    // Block 36b: optimistic subscriptions.
    "Subscribes go with the connect command over websocket",
    "Subscribes go with the connect command over websocket only",
    "Only subscribes that fit the frame cap go with the connect command",
    "A subscription that needs a callback subscribes after the connect reply",
    "An optimistic subscribe's deadline starts at the connect reply",
    "A failed connect loses the optimistic subscribes as a teardown does",
    "Unsubscribing before the connect reply undoes an optimistic subscribe",
    "A subscribe reply before the connect reply breaks the protocol",
    "Optimistic subscriptions on Centrifugo",
    // Block 37a: test constructors.
    "Test constructors build what the client core yields",
    // Block 37b: an injectable connector.
    "A connector gives the websocket its stream",
    "A wss endpoint over a connector is verified as without one",
    "A connector that fails fails the attempt",
    "A connector that never finishes fails the attempt with the handshake timeout",
    "A connector goes with websocket endpoints and no proxy setting only",
    // Block 37c: an in-memory server double.
    "An application tests its subscription against the server double",
    "The server double recovers publications missed during a reconnect",
    "The server double answers calls",
    "The server double refuses as the test asks",
    "The server double disconnects the client with a code",
    "The server double grants server-side subscriptions and sends messages",
    "The server double and Centrifugo answer alike",
    // Block 38: typed JSON helpers.
    "A typed publication reaches subscribers as the JSON its type encodes to",
    "A publication from the server API decodes as the application's type",
    "A typed publication from a Protobuf client reaches a JSON client",
    "A typed RPC sends JSON and decodes the proxy's result",
    "A typed RPC tells its failures apart",
    "A value that cannot be encoded fails the call at once and sends nothing",
    "A typed call is made when the method returns",
    "A publication's data decodes as the application's type, or says why not",
    // Block 41a: clones and weak handles.
    "Dropping one clone of a client leaves it running",
    "A weak handle upgrades only while a clone of the client is alive",
    "close on one clone closes every clone",
    // Block 41b: handlers under Task delivery; 41c adds the @inline
    // examples, 41d the @thread ones.
    "Handlers receive the client's and its subscriptions' events one at a time, in order",
    "A publication a handler had not returned from before disconnect is taken once",
    "A panicking handler disconnects the client with code 2, and its publication is recovered",
    "events() takes over a subscription's handler",
    "A handler takes over the subscription's stream",
    "Pings are answered while a handler runs, except under Inline",
    "A call a Task or Inline handler blocks on fails at once",
    "close awaited inside a handler returns",
    // Block 41d: the Thread handler scenarios.
    "A Thread handler may block on a call that completes",
    "A Thread handler's call stuck behind a full read-ahead fails with \"would deadlock\"",
    "A Thread handler blocking on a subscription's ready() its own event holds back fails at once",
    "A Thread handler may block on a ready() its event does not hold back",
    "A Thread handler may block on another client's call",
    // Block 39: a publications-only stream.
    "The publications stream yields only the subscription's publications",
    "Events the publications stream skips hold the connection until it is read",
    "Publications the publications stream has not yielded are recovered on the next connect",
    "publications() takes over the subscription's events stream",
    "events() takes over the subscription's publications stream",
    "The publications stream outlives unsubscribe, which withdraws what it had not yielded",
    "The publications stream ends with its subscription or its client",
    // Block 44c: network and page events, on the core.
    "Going offline while connected closes the transport and reconnects after the backoff",
    "Going offline while connecting closes the attempt in flight without an event",
    "Going offline does nothing to a disconnected client",
    "A page entering the back/forward cache reconnects with its own reason",
    "Coming online after going offline keeps the jitter window",
];

/// Server configuration of @A scenarios.
pub(crate) const SERVER_A: &[(&str, &str)] = &[
    (
        "CENTRIFUGO_CLIENT_ALLOW_ANONYMOUS_CONNECT_WITHOUT_TOKEN",
        "true",
    ),
    ("CENTRIFUGO_CLIENT_TOKEN_HMAC_SECRET_KEY", HMAC_SECRET),
    // An expired, unrefreshed connection is closed within a scenario
    // (default 25 s).
    ("CENTRIFUGO_CLIENT_EXPIRED_CLOSE_DELAY", "1s"),
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
    // Anonymous clients subscribe to channels without namespace, whose
    // publications carry offsets (both options needed; verified, Block 9).
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_SUBSCRIBE_FOR_ANONYMOUS",
        "true",
    ),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "10"),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s"),
    // Clients publish to, and read the history and presence of, channels
    // without namespace (Block 13b; option names from Centrifugo v6.10.1's
    // configuration defaults).
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_PRESENCE", "true"),
    // Join and leave pushes, for clients that ask (Block 13e): not forced,
    // and only to clients allowed to see presence.
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_JOIN_LEAVE", "true"),
    // Tag filters, for subscriptions that send one (Block 35a; channel
    // option `allow_tags_filter`, Centrifugo from v6.4.0).
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_TAGS_FILTER",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_HISTORY_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_HISTORY_FOR_ANONYMOUS",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PRESENCE_FOR_CLIENT",
        "true",
    ),
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PRESENCE_FOR_ANONYMOUS",
        "true",
    ),
    // Channels of the `private` namespace need a subscription token
    // (Block 10); an expired, unrefreshed one is ended after 1 s past the
    // server's periodic check (default 25 s). Those of the `rec` namespace
    // keep a history and are recoverable (Block 11). Those of the `delta`
    // namespace are recoverable too, and send every publication as a fossil
    // delta to subscriptions asking for one, which needs a positioned
    // subscription (Block 19). Channels of the `map` namespace are map
    // channels anyone may subscribe to, set keys of and remove keys from;
    // those of `map_private` need a token and refuse a client's writes.
    // `map_rec` keeps a stream of 5 publications, so a client further
    // behind cannot recover; `map_eph` keeps no stream at all (Block 45c).
    // `map_delta` sends per-key fossil deltas to map subscriptions asking
    // for them, and `map_tag` filters by tags (Block 45d).
    // Subscribing to a channel of `game` adds its client to the
    // `clients:` map and its user to the `users:` map of that channel
    // (Block 45, D16). Channels of `poll_v` are shared poll channels in
    // versioned mode written by the server API's `shared_poll_publish`;
    // `poll_l` tracks at most 2 keys per connection, `poll_private` needs a
    // subscription token (Block 47, D18).
    (
        "CENTRIFUGO_CHANNEL_NAMESPACES",
        r#"[{"name":"private"},{"name":"rec","history_size":10,"history_ttl":"300s","force_recovery":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"delta","history_size":10,"history_ttl":"300s","force_recovery":true,"allowed_delta_types":["fossil"],"delta_publish":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"map","subscription_type":"map","map":{"mode":"persistent","stream_size":100,"stream_ttl":"300s","min_page_size":1,"default_page_size":2,"allow_publish_for_client":true,"allow_publish_for_anonymous":true,"allow_remove_for_client":true,"allow_remove_for_anonymous":true},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"map_private","subscription_type":"map","map":{"mode":"persistent"}},{"name":"map_rec","subscription_type":"map","map":{"mode":"recoverable","key_ttl":"300s","stream_size":5,"min_page_size":1},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"map_eph","subscription_type":"map","map":{"mode":"ephemeral","key_ttl":"300s","min_page_size":1,"max_page_size":5},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"map_delta","subscription_type":"map","map":{"mode":"recoverable","key_ttl":"300s"},"allowed_delta_types":["fossil"],"delta_publish":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"map_tag","subscription_type":"map","map":{"mode":"recoverable","key_ttl":"300s"},"allow_tags_filter":true,"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"game","map_clients_presence_channel_prefix":"clients:","map_users_presence_channel_prefix":"users:","allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"clients","subscription_type":"map_clients","map":{"mode":"recoverable","key_ttl":"60s"},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"users","subscription_type":"map_users","map":{"mode":"recoverable","key_ttl":"2s"},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"poll_v","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s","publish_enabled":true},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"poll_l","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s","publish_enabled":true,"max_keys_per_connection":2},"allow_subscribe_for_client":true,"allow_subscribe_for_anonymous":true},{"name":"poll_private","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s","publish_enabled":true}}]"#,
    ),
    ("CENTRIFUGO_CLIENT_EXPIRED_SUB_CLOSE_DELAY", "1s"),
    // Shared poll namespaces need a signature key and a refresh endpoint:
    // nothing listens on this one, as `poll_v` is written by the server API
    // (Block 47, D18).
    ("CENTRIFUGO_SHARED_POLL_HMAC_SECRET_KEY", SHARED_POLL_SECRET),
    (
        "CENTRIFUGO_CHANNEL_PROXY_SHARED_POLL_REFRESH_ENDPOINT",
        "http://127.0.0.1:9/",
    ),
];

/// Server configuration added for @B scenarios, besides the proxy's.
pub(crate) const SERVER_B: &[(&str, &str)] = &[
    ("CENTRIFUGO_HTTP_STREAM_ENABLED", "true"),
    ("CENTRIFUGO_SSE_ENABLED", "true"),
    // Clients publish to channels without namespace (Block 18b's 413
    // scenario); proxy connections belong to a user.
    (
        "CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_ALLOW_PUBLISH_FOR_CLIENT",
        "true",
    ),
    // A history for channels without namespace, so that server-side
    // subscriptions granted recoverable can recover (Block 12c). It gives
    // publications offsets; client-side subscriptions stay unrecoverable.
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_SIZE", "10"),
    ("CENTRIFUGO_CHANNEL_WITHOUT_NAMESPACE_HISTORY_TTL", "300s"),
    // Channels of the `rec` namespace keep a history and are recoverable
    // (Block 11); the subscribe proxy sees them too. Channels of `poll` are
    // shared poll channels in versionless mode, refreshed by the proxy
    // backend (Block 47, D18); those of `poll_v` in versioned mode, the
    // backend giving the versions and the epoch (47c's epoch flip); those
    // of `poll_d` versioned with fossil delta, a change going as a delta
    // when the backend gives its previous value as `prev_data` (47d).
    (
        "CENTRIFUGO_CHANNEL_NAMESPACES",
        r#"[{"name":"rec","history_size":10,"history_ttl":"300s","force_recovery":true,"subscribe_proxy_enabled":true},{"name":"poll","subscription_type":"shared_poll","shared_poll":{"refresh_interval":"1s","channel_shutdown_delay":"1s"},"allow_subscribe_for_client":true},{"name":"poll_v","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s"},"allow_subscribe_for_client":true},{"name":"poll_d","subscription_type":"shared_poll","shared_poll":{"mode":"versioned","refresh_interval":"1s"},"allowed_delta_types":["fossil"],"allow_subscribe_for_client":true}]"#,
    ),
    ("CENTRIFUGO_SHARED_POLL_HMAC_SECRET_KEY", SHARED_POLL_SECRET),
];

/// A scenario still running after this long is reported as hung and fails
/// the run: none takes a minute, and a hang would otherwise last until the
/// CI job's timeout without naming the scenario (concurrent output is
/// printed in order, once scenarios finish).
const SCENARIO_LIMIT: Duration = Duration::from_secs(180);

/// Scenarios running now, with when each started, for the watchdog.
static RUNNING: Mutex<Vec<(String, Instant)>> = Mutex::new(Vec::new());

/// Marks the runner process started by [`trust_test_ca`].
const TEST_CA_MARKER: &str = "FERRIFUGE_TEST_CA";

/// Stack of the thread running the scenarios: Windows gives the main
/// thread 1 MiB, too little for the runner's futures in a debug build. In
/// a debug build cucumber's scenario and step frames hold the world by
/// value some 330 times over, about 8 MiB once the world reached 25 KiB
/// (Block 35b), so the stack leaves room for it to grow; it is reserved,
/// not committed, until used.
const RUNNER_STACK: usize = 32 << 20;

/// Block 43a: the cookie jar on the websocket upgrade, run only when the
/// BDD binary is built with `cookies` (Block 43, D8).
#[cfg(feature = "cookies")]
const COOKIES: &[&str] = &[
    "A cookie set on the websocket upgrade is sent on the next connection",
    "A cookie set on a refused upgrade is sent on the next attempt",
    "The app's Cookie header and the jar's cookies are sent in one header",
    "Without a cookie jar no cookie is kept",
    "No cookie value reaches a log record",
    // Block 43b: the emulated transports.
    "A cookie set on the stream response rides on every emulation request",
    "A cookie set on an emulation response is sent on the next emulation request",
];
#[cfg(not(feature = "cookies"))]
const COOKIES: &[&str] = &[];

/// Block 45: map subscriptions, run only when the BDD binary is built with
/// `unstable` (Block 45, D16).
#[cfg(feature = "unstable")]
const MAPS: &[&str] = &[
    // Block 46b: map keys.
    "Each map key has its own window",
    "A remove cancels the key's pending publish and settles the publishes it replaced",
    "Client-level map writes are not debounced",
    "A map subscribe reply's interval is honoured",
    // 45a: map_publish and map_remove on the client.
    "A refused map publish fails with the server's code",
    // 45b: map subscriptions load their state, then follow its changes.
    "A map subscription cannot share a channel with a stream subscription",
    "A new map subscription starts unsubscribed",
    "Removing a map subscription unsubscribes it and frees its channel",
    "A map subscription loads its state page by page",
    "A page size of 0 leaves the page size to the server",
    "Changes made during paging are part of the sync",
    "An epoch change during paging restarts the load",
    "A map subscription syncs then delivers updates",
    "A map subscription syncs over each transport and protocol",
    "map_publish sets a key",
    "map_remove removes a key",
    "publish and remove on a map subscription change the map for every subscriber",
    "A map subscription's publish waits until it is subscribed",
    "Presence of clients is a map keyed by client id",
    "Presence of users is a map keyed by user id whose entries expire",
    "A subscription type the namespace does not declare is refused",
    "A map subscription to a private namespace gets its token and refreshes it",
    "Every request of a load carries the token, and only the first carries the data",
    "A missing page reply makes the client reconnect",
    "A server without map subscriptions ends the map subscription",
    "A reply in a phase the request does not allow forces a reconnect",
    "A load larger than its bound ends the map subscription",
    "Pages are handled while another subscription's stream is not read",
    "An unsubscribe during a load waits for the page in flight",
    "Statistics count map subscriptions and their updates",
    // 45c: recovery and resync.
    "Missed changes are recovered as updates after a reconnect",
    "A recovering subscribe starts in the stream phase from the position",
    "Updates move the position only when taken",
    "A sync not taken before disconnect() is withdrawn and the state is loaded again",
    "A map subscription too far behind resyncs from a fresh state",
    "An unrecoverable position resyncs after the resubscribe backoff",
    "With the fatal strategy an unrecoverable position unsubscribes",
    "An ephemeral map syncs again on every reconnect",
    "A cleared map is loaded again",
    "State invalidation drops the position and the token",
    "A temporary error during a load starts it again after the backoff",
    "A load the server closes as slow starts again on the next connection",
    "Delta compression decodes each key's changes",
    "A recovered map subscription decodes deltas against the values it held",
    "A removal ends a key's delta chain",
    "Page data is decoded once the live reply says whether delta is on",
    "A tag filter limits the state and the updates",
    "Setting a tag filter loads the state again",
    // 45e: handlers, test constructors.
    "A map subscription handler receives the events in order",
    "A test builds map events without a server",
];
#[cfg(not(feature = "unstable"))]
const MAPS: &[&str] = &[];

/// Block 47: shared poll subscriptions, run only when the BDD binary is
/// built with `unstable` (Block 47, D18).
#[cfg(feature = "unstable")]
const SHARED_POLLS: &[&str] = &[
    // 47a: tracking keys under signatures.
    "A shared poll subscription cannot share a channel with another subscription",
    "A new shared poll subscription starts unsubscribed",
    "Removing a shared poll subscription unsubscribes it and frees its channel",
    "A shared poll subscription subscribes with type 4 and nothing else",
    "Tracking with a signature sends one track request",
    "Keys tracked before subscribing are sent once subscribed",
    "A key tracked twice is tracked once at the higher version",
    "An empty key set sends nothing",
    "An untrack of more keys than one frame fits is split",
    "Tracked keys receive their values and each new version",
    "Centrifugo polls the backend and fans the value out",
    "A shared poll subscription works over each transport and protocol",
    "An untracked key delivers nothing more",
    "A version not newer than the one held is dropped",
    "A publication of a key not tracked is dropped",
    "A backend removal ends the key and gives one update",
    "Items of a track reply are delivered before later pushes",
    "A signature the server refuses untracks its keys and reports the error",
    "A limit the server enforces untracks the keys over it",
    "A namespace that is not shared poll refuses a shared poll subscription",
    "A server without shared poll ends the subscription",
    "A private namespace gets its token and refreshes it",
    "A signature batch over the frame budget is refused before it is sent",
    // 47b: signatures asked for and refreshed by the client.
    "track asks get_signature for the keys and tracks what it returns",
    "A key the callback does not return is revoked",
    "A key untracked while get_signature runs is untracked in the same request",
    "Without get_signature a key set reports error 14",
    "A failing get_signature reports error 14 and retries",
    "An unauthorized get_signature unsubscribes with code 1",
    "A signature with an expiry is refreshed when it expires",
    "A track refused with 109 refreshes the signature",
    "A track refused with 109 again waits before the next refresh",
    "A signature accepted within the grace period is refreshed soon",
    "A key set larger than one frame is signed and tracked in chunks",
    // 47c: keys kept across reconnects, epochs, retries.
    "Tracked keys come back after a reconnect with the versions held",
    "A replay reuses the signatures it holds",
    "A key untracked while offline is untracked in the replay frame",
    "A replay signs only the keys without a signature",
    "A changed epoch sends every key from version 0",
    "A first epoch from the backend sends every key from version 0",
    "Versions given before the first subscribe are kept whatever its epoch",
    "An epoch flip resubscribes and tracks again",
    "A replay with held versions stays within the frame budget",
    "unsubscribe forgets the keys and the signatures",
    "A failing track is retried or untracked by its error",
    "Updates not taken before disconnect come back on the next connection",
    "A removal not taken before disconnect is still delivered",
    // 47d: per-key delta compression, its bound, statistics.
    "Delta compression decodes each tracked key's changes",
    "Delta bases are dropped at each subscribe",
    "A delta in flight when its key was tracked again is dropped",
    "Delta bases past their bound end the subscription",
    "A delta that cannot be applied resubscribes and tracks again",
    "Statistics count shared poll subscriptions and their updates",
    // 47e: handlers, the JSON helper, test constructors.
    "A shared poll subscription handler receives the events in order",
    "A test builds shared poll events without a server",
];
#[cfg(not(feature = "unstable"))]
const SHARED_POLLS: &[&str] = &[];

/// Tags of `Examples` tables whose sub-increment is not implemented yet
/// (Block 41, D8): none since 41d ran `@thread`.
const EXAMPLES_NOT_YET: &[&str] = &[];

/// Scenarios run alone: each spends seconds of CPU on a 32 MiB frame in a
/// debug build, enough to delay the timers of scenarios running alongside
/// (seen: a missed ping deadline in "Regular pings keep the connection
/// alive"). The quickstart one builds the example, under coverage in a
/// target directory of its own, for minutes on every core: alongside it,
/// Centrifugo containers missed their 60 s startup wait.
const SERIAL: &[&str] = &[
    "A websocket write the server never reads closes the transport",
    "A stalled websocket write does not delay disconnect",
    "The quickstart example receives a publication",
];

fn main() {
    // `cargo test <filter>` passes a libtest filter to every test target,
    // e.g. when cargo-bolero runs one fuzz target; cucumber takes options
    // only, so a leading positional argument means another target's tests.
    if env::args().nth(1).is_some_and(|arg| !arg.starts_with('-')) {
        println!("bdd skipped: a test filter is for libtest targets");
        return;
    }
    trust_test_ca();
    assert_implemented_scenarios_exist();
    // Before any client: a client built earlier would have no span.
    support::logs::install();
    start_watchdog();
    let runner = thread::Builder::new()
        .name("scenarios".to_owned())
        .stack_size(RUNNER_STACK)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(run_scenarios());
        })
        .expect("start the scenario runner");
    // The runner exits the process when done; it returns only by panicking.
    if runner.join().is_err() {
        process::exit(101);
    }
}

async fn run_scenarios() {
    // cucumber's `--tags` and `--name` replace the filter given to
    // `filter_run` instead of narrowing it, so they are taken out and
    // applied in it.
    let mut cli = cli::Opts::<_, _, _, cli::Empty>::parsed();
    let name_filter = cli.re_filter.take();
    let tags_filter = cli.tags_filter.take();
    let selected = move |feature: &Feature, rule: Option<&Rule>, scenario: &Scenario| {
        name_filter
            .as_ref()
            .is_none_or(|name| name.is_match(&scenario.name))
            && tags_filter.as_ref().is_none_or(|tags| {
                tags.eval(
                    feature
                        .tags
                        .iter()
                        .chain(rule.iter().flat_map(|rule| &rule.tags))
                        .chain(&scenario.tags),
                )
            })
    };
    ClientWorld::cucumber()
        .with_cli(cli)
        .which_scenario(|_, _, scenario| {
            if SERIAL.contains(&scenario.name.as_str()) {
                ScenarioType::Serial
            } else {
                ScenarioType::Concurrent
            }
        })
        .before(|_, _, scenario, world| {
            Box::pin(async move {
                running().push((scenario_key(scenario), Instant::now()));
                if scenario
                    .steps
                    .iter()
                    .any(|step| steps::STALL_PROOF_STEPS.contains(&step.value.as_str()))
                {
                    world.timeout = Some(steps::STALL_PROOF_TIMEOUT);
                }
                if scenario.tags.iter().any(|tag| tag == "A") {
                    world.server = Some(Centrifugo::start(SERVER_A).await);
                } else if scenario.tags.iter().any(|tag| tag == "B") {
                    let proxy = ProxyBackend::start().await;
                    let env = proxy.centrifugo_env();
                    let env: Vec<_> = env
                        .iter()
                        .map(|(name, value)| (*name, value.as_str()))
                        .chain(SERVER_B.iter().copied())
                        .collect();
                    world.server = Some(Centrifugo::start(&env).await);
                    world.proxy = Some(proxy);
                }
                if scenario.tags.iter().any(|tag| tag == "D") {
                    world.fake = Some(FakeServer::start(Announce::default()).await);
                }
                let target = match (&world.server, &world.fake) {
                    (Some(server), _) => Some(server.address().to_owned()),
                    (None, Some(fake)) => Some(fake.address()),
                    (None, None) => None,
                };
                if let Some(target) = target {
                    world.relay = Some(CountingRelay::start(target).await);
                }
            })
        })
        .after(|_, _, scenario, _, world| {
            Box::pin(async move {
                // Removed here, awaited, rather than by the world's drop.
                if let Some(server) = world.and_then(|world| world.server.take()) {
                    server.stop().await;
                }
                let key = scenario_key(scenario);
                let mut running = running();
                if let Some(index) = running.iter().position(|(name, _)| *name == key) {
                    running.remove(index);
                }
            })
        })
        .fail_on_skipped()
        .filter_run_and_exit(FEATURES, move |feature, rule, scenario| {
            // @P and @W run outside cucumber (Block 0, "Test layers"; Block
            // 44, D18); a scenario also tagged with a cucumber layer runs in
            // that layer.
            let outside = !scenario.tags.is_empty()
                && scenario.tags.iter().all(|tag| tag == "P" || tag == "W");
            (IMPLEMENTED.contains(&scenario.name.as_str())
                || COOKIES.contains(&scenario.name.as_str())
                || MAPS.contains(&scenario.name.as_str())
                || SHARED_POLLS.contains(&scenario.name.as_str()))
                && !outside
                && !scenario
                    .tags
                    .iter()
                    .any(|tag| EXAMPLES_NOT_YET.contains(&tag.as_str()))
                && selected(feature, rule, scenario)
        })
        .await;
}

fn running() -> std::sync::MutexGuard<'static, Vec<(String, Instant)>> {
    RUNNING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Rows of an outline share its name and line: they are told apart only
/// by being removed one at a time.
fn scenario_key(scenario: &Scenario) -> String {
    format!("{} (line {})", scenario.name, scenario.position.line)
}

/// Fails the run, naming the scenarios still running, once one of them
/// runs past [`SCENARIO_LIMIT`].
fn start_watchdog() {
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(5));
            let running = running();
            if running
                .iter()
                .any(|(_, started)| started.elapsed() > SCENARIO_LIMIT)
            {
                eprintln!("Hung scenarios, running for more than {SCENARIO_LIMIT:?}:");
                for (name, started) in running.iter() {
                    eprintln!("  {name}: running for {:?}", started.elapsed());
                }
                process::exit(1);
            }
        }
    });
}

/// Makes the client trust the test CA of the encrypted scenarios (Block 6).
///
/// `rustls-native-certs` reads `SSL_CERT_FILE` when the first secure
/// connection is configured. Setting a variable in-process is `unsafe`, so
/// the runner creates the CA and runs itself again with the variable set.
fn trust_test_ca() {
    if env::var_os(TEST_CA_MARKER).is_some() {
        return;
    }
    tls::create();
    let run = process::id().to_string();
    let status = Command::new(env::current_exe().expect("runner path"))
        .args(env::args_os().skip(1))
        .env("SSL_CERT_FILE", tls::ca_path())
        .env(TEST_CA_MARKER, "1")
        .env(centrifugo::RUN_VARIABLE, &run)
        .status()
        .expect("run the scenarios");
    remove_containers(&run);
    process::exit(status.code().unwrap_or(1));
}

/// Removes the Centrifugo containers of run `run` still there: the runner
/// exits the process without dropping every world (a hang, a panic, or a
/// server a step replaced and dropped in the background), and those would
/// otherwise keep running. Best effort: without the Docker CLI it does
/// nothing.
fn remove_containers(run: &str) {
    let label = format!("label={}={run}", centrifugo::RUN_LABEL);
    let Ok(listed) = Command::new("docker")
        .args(["ps", "-aq", "--filter", &label])
        .output()
    else {
        return;
    };
    let ids: Vec<String> = String::from_utf8_lossy(&listed.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    if !ids.is_empty() {
        let _ = Command::new("docker")
            .args(["rm", "-f"])
            .args(&ids)
            .output();
    }
}

/// A misspelt entry in [`IMPLEMENTED`] would silently run nothing.
fn assert_implemented_scenarios_exist() {
    let mut names = HashSet::new();
    for entry in fs::read_dir(FEATURES).expect("features directory") {
        let path = entry.expect("feature file").path();
        if path.extension().is_some_and(|ext| ext == "feature") {
            let feature = Feature::parse_path(&path, GherkinEnv::default())
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            names.extend(feature.scenarios.into_iter().map(|s| s.name));
        }
    }
    for name in IMPLEMENTED
        .iter()
        .chain(COOKIES)
        .chain(MAPS)
        .chain(SHARED_POLLS)
    {
        assert!(
            names.contains(*name),
            "no scenario named {name:?} in {FEATURES}"
        );
    }
}
