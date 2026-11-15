# Protocol flows

How the client talks to a Centrifugo server, drawn as sequence diagrams.
Each one shows the application's calls and events next to the messages on
the wire, the timers that bound each step, and the branches the client
takes when something fails. The [resilience guide](resilience.md) says what
the application should do about each failure, and the
[codes table](../README.md#codes) lists every code.

`Connecting`, `Connected` and `Disconnected` are `ClientEvent`s;
`Subscribing`, `Subscribed`, `Publication` and `Unsubscribed` are
`SubscriptionEvent`s; an `Error` event carries a `ClientError`. A
client-side code is written with its kind (`connecting` 2 "no ping"), a
server code after the kind of message that carries it (disconnect 3008).
Options are fields of `Config`, or of a subscription's options where a
diagram says so. Blocks are those of the repository's
[CLAUDE.md](../CLAUDE.md), which these diagrams follow.

Sources: Block 34 D1; Block 59 D2; Block 59 D4

## Connect and heartbeat

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Server
    App->>Client: connect()
    Client-->>App: Connecting 0 "connect called"
    Client->>Server: open the transport
    Note over Client,Server: the open is bounded by handshake_timeout (default 5 s)
    alt the open fails or takes longer
        Client-->>App: Error event, error 2 with the transport's message or "handshake timeout"
        Note over Client: the next transport of the list at once in the first round, after the reconnect backoff later
    else the transport opens
        Client->>Server: connect command with the token and the connect data
        Note over Client,Server: the connect reply is bounded by timeout (default 5 s)
        Server-->>Client: connect reply with the ping interval and the pong flag
        Client-->>App: Connected with the client id and the transport
    end
    loop every ping interval (Centrifugo default 25 s)
        Server->>Client: ping, an empty reply
        opt the connect reply asked for pongs
            Client->>Server: pong, an empty command
        end
    end
    Note over Client: no-ping deadline: the ping interval plus max_server_ping_delay (default 10 s), re-armed by every message read, none when either is 0
    alt no message is read before the deadline
        Client->>Server: close the transport
        Client-->>App: Connecting 2 "no ping"
        Note over Client: reconnects after the reconnect backoff, plus the jitter window
    end
```

Sources: Block 1 "Successful connect"; Block 2; Block 4 "Client answers ping with pong", "Client does not answer when pong is not expected", "Missing pings trigger reconnect", "Regular pings keep the connection alive"; Block 8 "Every transport unreachable"; Block 14 "Reconnect jitter window"; Block 15 "In order, one message at a time"; Block 29 D1; `src/machine.rs::timer_fired`

## Disconnect and reconnect

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Server
    Note over Client,Server: connected
    alt the transport is lost or closed without a server code
        Client-->>App: Connecting 1 "transport closed"
    else disconnect 3000–3499, 4000–4499 or from 5000
        Server->>Client: close, or disconnect push, with the server's code and reason
        Client-->>App: Connecting with the server's code and reason
    else disconnect 3500–3999 or 4500–4999
        Server->>Client: close, or disconnect push, with the server's code and reason
        Client-->>App: Disconnected with the server's code and reason
        Note over Client: no reconnect until connect()
    else websocket close 1009
        Client-->>App: Disconnected 3 "message size limit exceeded"
        Note over Client: no reconnect until connect()
    end
    Note over Client: delay of attempt n: min(max, min + rand(0, min(max, min x 2^n))), min_reconnect_delay (default 500 ms), max_reconnect_delay (default 20 s)
    Note over Client: the first reconnect after a connection adds a random delay up to reconnect_jitter_window (default 0)
    Client->>Server: open the transport, connect command
    Server-->>Client: connect reply
    Client-->>App: Connected
    alt websocket or WebTransport
        Note over Client: the connect reply restarts the attempt counter
    else http_stream or sse
        Note over Client: the first reply to a command, or server ping, after the connect reply restarts it
    end
    opt disconnect 3008, Connecting 7, or a WebTransport stream ending without a code, before a server ping is read
        Note over Client: no restart stands: the backoff keeps growing
    end
    opt Connecting 5, at any time
        Note over Client: no restart stands: the backoff keeps growing
    end
    Note over App,Client: connect() and reconnect() restart the counter, with no backoff before their first attempt
```

Sources: Block 1 "Temporary connection loss then successful reconnect", "Connection loss then terminal error on reconnect"; Block 2 "Delay upper bound per attempt", "Delays are jittered"; Block 3 "Application disconnect codes sent via server API", "Protocol-range disconnect codes", "Close code 1009 stops the client"; Block 14 "Reconnect jitter window", "Without a jitter window the first reconnect follows the backoff"; Block 15 "Backoff after 3008", "Reconnect delays keep growing while the server closes each connection with 3008", "A server ping read before 3008 lets the reset stand"; Block 19 D8; Block 25 D5; Block 44 D7; Block 48 D9; `src/machine.rs::an_emulated_connect_reply_alone_does_not_reset_the_backoff`

## Subscription and recovery

```mermaid
sequenceDiagram
    participant App
    participant Sub as Subscription
    participant Client
    participant Server
    App->>Sub: subscribe()
    Sub-->>App: Subscribing 0 "subscribe called"
    Client->>Server: subscribe command
    Server-->>Client: subscribe reply with the stream position (epoch, offset)
    Sub-->>App: Subscribed, was_recovering false, recovered false
    Server->>Client: publication with epoch e and offset n
    Sub-->>App: Publication at offset n
    Note over App,Sub: the stored position moves to n once the app takes the publication
    Note over Client,Server: the transport is lost
    Sub-->>App: Subscribing 1 "transport closed"
    Client->>Server: connect, then subscribe asking to recover from epoch e and offset n
    alt the server recovers the stream
        Server-->>Client: subscribe reply, recovered, with the missed publications
        Sub-->>App: Subscribed, was_recovering true, recovered true
        Sub-->>App: each missed Publication, in order
    else it cannot (history gone, another epoch, or past its recovery limit)
        Server-->>Client: subscribe reply, not recovered
        Sub-->>App: Subscribed, was_recovering true, recovered false
        Note over App: the app reloads its state itself
    else the subscription has a get_state callback
        Note over Client,Server: the subscribe command set the "reject unrecovered" flag
        Server-->>Client: subscribe error 112
        Note over Sub: no Error event, the position is dropped, the subscription stays Subscribing
        Note over Client: waits the resubscribe backoff, min_resubscribe_delay (default 500 ms), max_resubscribe_delay (default 20 s)
        Client->>App: get_state(GetStateContext { channel })
        App-->>Client: the stream position the state was loaded at
        Client->>Server: subscribe from that position
        Server-->>Client: subscribe reply
        Sub-->>App: Subscribed
    end
```

Sources: Block 9 "Subscribe while connected", "Resubscribe after reconnect"; Block 10 "Resubscribe delays are bounded"; Block 11 "A first subscribe does not recover", "Missed publications are recovered", "Recovery fails when history is gone", "getState provides the position of the initial subscribe", "getState reloads state when recovery fails"; Block 15 "Delivered means taken"

The resubscribe backoff options are a subscription's (`SubscriptionBuilder`).
`get_state` is also called before the first subscribe of a subscription
without `since`.

## Token lifecycle

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Server
    Note over App,Server: connection token
    Client->>App: get_token(ConnectionTokenContext), before a connect without a token
    alt the callback fails
        Client-->>App: Error event, error 5, the attempt retried after the reconnect backoff
    else it denies: Unauthorized, or an empty token
        Client-->>App: Disconnected 1 "unauthorized"
    else it returns a token
        Client->>Server: connect command with the token
        Server-->>Client: connect reply with expires and ttl
        Note over Client: refresh at a random point between 80% and 90% of the ttl
        Client->>App: get_token(ConnectionTokenContext)
        App-->>Client: a new token
        Client->>Server: refresh command
        alt the callback fails (error 6), or refresh error, temporary or below 100
            Note over Client: retried after 5 to 10 s
        else the callback denies
            Client-->>App: Disconnected 1 "unauthorized"
        else refresh error, any other
            Client-->>App: Disconnected with the error's code
        end
    end
    opt connect error 109
        Note over Client: the token is dropped, the next attempt fetches a new one after the backoff
        Client-->>App: Error event, the client stays connecting
        opt there is no get_token callback
            Client-->>App: Error event, error 12, then Disconnected 1 "unauthorized"
        end
    end
    Note over App,Server: subscription token, one per channel
    Client->>App: the subscription's get_token(SubscriptionTokenContext { channel }), before a subscribe without a token
    alt the callback fails
        Client-->>App: Error event, error 8, the subscribe retried after the resubscribe backoff
    else it denies: Unauthorized, or an empty token
        Client-->>App: Unsubscribed 1 "unauthorized"
    else it returns a token
        Client->>Server: subscribe command with the token
        Note over Client: sub_refresh at a random point between 80% and 90% of the ttl
        alt the callback fails (error 9), or sub_refresh error, temporary or below 100
            Note over Client: retried after 10 to 20 s
        else the callback denies
            Client-->>App: Unsubscribed 1 "unauthorized"
        end
    end
    opt subscribe error 109
        Note over Client: the token is dropped, a new one fetched, the subscription resubscribes after the resubscribe backoff
        opt there is no get_token callback
            Client-->>App: Error event, error 12, then Unsubscribed 1 "unauthorized"
        end
    end
    Note over Client: a refresh that fell due during a system sleep runs at the wake, or before the next connect or subscribe
```

Sources: Block 1 "Terminal condition while connected"; Block 5 "Token refreshed before expiry keeps the connection"; Block 10 "Subscription token refreshed before expiry keeps the subscription", "A failing first subscription token fetch is retried before subscribing", "A failing subscription token callback is retried", "A temporary sub_refresh error is retried", "Subscription token denied", "Subscribe error 109 fetches a new token and resubscribes", "A subscribe rejected for an expired token without a callback unsubscribes"; Block 14 "Token refresh jitter"; Block 17; Block 25 D3 "A refresh due during a short sleep starts at the wake", "A token due during a long sleep is fetched before the next connect"; `src/machine.rs::failed_connect_fetch_is_reported_and_retried_with_backoff`; `src/machine.rs::failed_refresh_fetch_is_reported_and_retried_in_5_to_10_s`; `src/machine.rs::refresh_reply_errors_retry_when_temporary_and_disconnect_otherwise`; `src/machine.rs::connect_error`

## Calls and timeouts

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Server
    App->>Client: publish, rpc, history, presence or presence_stats
    opt the client is not connected
        Note over Client: the call waits for the connection, for at most timeout (default 5 s)
        opt no connection comes in time
            Client-->>App: Err, error 1 "timeout", and the command is never sent
        end
    end
    Client->>Server: command
    alt a reply within timeout
        Server-->>Client: reply
        Client-->>App: Ok with the result, or Err with the server's code, message and temporary flag
    else no reply within timeout
        Client-->>App: Err, error 1 "timeout": the outcome is unknown
    else the transport is lost
        Client-->>App: Err, error 11 "connection closed": the outcome is unknown
        Note over Client: a call not sent yet keeps waiting, within its timeout
    else disconnect(), or a terminal condition
        Client-->>App: Err, error 3 "disconnected", for calls sent and calls waiting
    end
    Note over App,Client: the client never retries a call: the application decides
```

Sources: Block 13 "A call made while connecting waits for the connection", "A queued call fails after the timeout when no connection comes", "A call that timed out while queued is never sent", "A call without a reply times out", "Temporary RPC error is reported as such", "Losing the transport fails calls already sent at once", "A call waiting for room keeps its deadline when the transport is lost", "Disconnect fails a call already sent at once", "A terminal connect failure fails a queued call at once"; Block 34 D1

## Backpressure

```mermaid
sequenceDiagram
    participant App
    participant Client
    participant Server
    Note over App: the app stops taking events from one of its streams
    loop while the read-ahead fills, up to read_ahead_messages (default 256) or read_ahead_bytes (default 4 MiB)
        Server->>Client: publications and pings
        Client->>Server: a pong for each ping, when the connect reply asked for pongs
    end
    Note over Client: the read-ahead is full: reading stops, so no ping is read, and the no-ping deadline pauses
    Note over Client: a reply deadline whose reply was read pauses, a call keeps its timeout
    alt natively, the server's client queue overflows (client.queue_max_size, Centrifugo default 1 MiB)
        Server->>Client: disconnect 3008 "slow", dropping what it had queued
        Note over App,Client: the close is handled once the app has taken the events read before it
        Client-->>App: Connecting with disconnect 3008
        Note over Client: reconnects after the backoff, which keeps growing as no server ping was read
        Client->>Server: subscribe asking to recover
        Note over Client,Server: recovered within client.recovery_max_publication_limit (Centrifugo default 300), else recovered false
    else natively, a server write times out
        Server->>Client: disconnect 3009 "write error"
        Client-->>App: Connecting with disconnect 3009
    else in the browser, reading never stops
        Note over Client: messages wait in an inbox with the read-ahead's limits
        Client-->>App: Connecting 7 "read-ahead overflow", once a message would overflow it
        Note over Client: events not taken are withdrawn, and recovery brings them back on recoverable channels
    end
```

Sources: Block 4; Block 13 "call replies"; Block 15 "Bounded read-ahead", "Head-of-line", "Recovery larger than the server's queue", "A stream that is not read stops the client reading after 256 items", "Pings are answered while a stream is not read", "A full read-ahead does not make the client report missing pings", "The server closing the connection is handled only after the publications before it are taken", "A slow consumer on a channel with history and recovery loses no publication"; Block 44 D7
