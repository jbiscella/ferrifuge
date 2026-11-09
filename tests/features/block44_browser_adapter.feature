Feature: Browser adapter

  @W
  Scenario: A browser client connects over websocket and receives a publication
    Given a browser client with a subscription to channel "news", whose streams were taken
    When connect is called
    And subscribe is called on the subscription
    And a publication "hello" is published to "news"
    Then the client's stream yields "connecting", then "connected" over transport "websocket"
    And the subscription's stream yields "subscribing", "subscribed", then the publication "hello"

  @W
  Scenario: A browser client speaks Protobuf over websocket
    Given a browser client with the Protobuf protocol and a subscription to channel "news", whose stream was taken
    When connect is called
    And subscribe is called on the subscription
    And a publication of the bytes 00 ff 7f is published to "news"
    Then the client emits "connected" over transport "websocket"
    And the subscription's stream yields the publication with the bytes 00 ff 7f

  @W
  Scenario: Publications a browser client had not taken before reconnect() are recovered once
    Given a browser client subscribed to recoverable channel "history:news", whose subscription's stream has been read up to "subscribed"
    And publications at offsets 1, 2 and 3 have been published to "history:news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    When reconnect is called
    Then the subscription's stream yields "subscribing", "subscribed" with recovered true, then the publications at offsets 2 and 3
    And no publication is yielded twice

  @W
  Scenario: A browser client whose stream is not read reconnects once its inbox overflows, losing no publication
    Given a browser client with a read-ahead of 16 messages, subscribed to recoverable channel "history:news", whose subscription's stream has been read up to "subscribed" and is not read after, and whose client stream is read
    When 100 publications are published to "history:news"
    Then the client emits "connecting" with code 7 and reason "read-ahead overflow"
    And when the application reads the subscription's stream, it yields "subscribing", "subscribed" with recovered true, then the publications at offsets 1 to 100 in order, each once

  @W
  Scenario Outline: Handlers receive a browser client's events in order
    Given a browser client with <mode> delivery whose client handler and subscription handler record the events, with a subscription to channel "news"
    When subscribe is called on the subscription
    And connect is called
    And a publication is published to "news" once the subscription is subscribed
    Then the handlers have received "subscribing", "connecting", "connected", "subscribed" and the publication, in this order
    And no two handler calls overlapped

    Examples:
      | mode   |
      | Task   |
      | Inline |

  @W
  Scenario: close() ends a browser client
    Given a browser client connected to the server, whose stream was taken
    When close is awaited
    Then it returns within the configured timeout plus tolerance
    And the client's stream yields "disconnected" with code 0, then ends
    And the client is closed

  @W
  Scenario: A browser client's statistics tell when it connected
    Given the page's wall clock read before connect is called
    When a browser client connects to the server
    Then its statistics give a connected_since between that reading and a reading taken once "connected" is yielded

  @W
  Scenario Outline: A browser client falls back to http_stream when websocket is unreachable
    Given a browser client with the <protocol> protocol, configured with an unreachable websocket endpoint and the server's <endpoints>
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "http_stream"

    Examples:
      | protocol | endpoints                     |
      | JSON     | http_stream and sse endpoints |
      | Protobuf | http_stream endpoint          |

  @W
  Scenario: Commands work over http_stream from a browser client
    Given a browser client connected over transport "http_stream", subscribed to channel "news"
    When the client publishes "hello" to "news"
    Then the publish succeeds
    And the subscription's stream yields the publication "hello"

  @W
  Scenario: A stream not read does not stop a browser client reading its http_stream response
    Given a browser client with a read-ahead of 600 messages, connected over transport "http_stream", subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    When 1000 publications of 100 bytes are published to "news"
    Then its read-ahead holds 600 messages
    And 1 s later its read-ahead still holds 600 messages and the client has not emitted "connecting"
    And when the application reads the subscription's stream, it yields the 1000 publications in order, each once

  @W
  Scenario: A browser client whose stream is not read reconnects once its inbox overflows over http_stream, losing no publication
    Given a browser client with a read-ahead of 16 messages, connected over transport "http_stream", subscribed to recoverable channel "history:news", whose subscription's stream has been read up to "subscribed" and is not read after, and whose client stream is read
    When 100 publications are published to "history:news"
    Then the client emits "connecting" with code 7 and reason "read-ahead overflow"
    And when the application reads the subscription's stream, it yields "subscribing", "subscribed" with recovered true, then the publications at offsets 1 to 100 in order, each once

  @W
  Scenario: A browser client falls back to sse when websocket and http_stream are unreachable
    Given a browser client configured with unreachable websocket and http_stream endpoints and the server's sse endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "sse"

  @W
  Scenario: An sse stream that ends makes a browser client reconnect, not the browser's EventSource
    Given a browser client connected over transport "sse" through the relay
    When the relay ends the stream response
    Then the client emits "connecting" with code 1
    And then the client emits "connected" over transport "sse"
    And 10 s after the end the relay has seen exactly 2 stream requests

  @W
  Scenario: Going offline closes a browser client's connection, and coming online reconnects it at once
    Given a browser client with minReconnectDelay 20000 ms, whose network event target is the test's, connected over websocket
    When the test dispatches "offline" on that target
    Then the client emits "connecting" with code 1 and reason "transport closed"
    And 2 s later the client is still "connecting"
    And when the test dispatches "online" on that target, the client emits "connected" within 2 s

  @W
  Scenario: Coming online while connected changes nothing
    Given a browser client whose network event target is the test's, connected over websocket, whose stream has been read up to "connected"
    When the test dispatches "online" on that target
    Then 1 s later the client is "connected"
    And its stream has yielded nothing more

  @W
  Scenario: A page entering the back/forward cache closes the connection, and its restore reconnects at once
    Given a browser client with minReconnectDelay 20000 ms, whose page event target is the test's, connected over websocket
    When the test dispatches "pagehide" with persisted true on that target
    Then the client emits "connecting" with code 1 and reason "page entered back/forward cache"
    And when the test dispatches "pageshow" with persisted true on that target, the client emits "connected" within 2 s

  @W
  Scenario: A page hidden without entering the back/forward cache keeps the connection
    Given a browser client whose page event target is the test's, connected over websocket, whose stream has been read up to "connected"
    When the test dispatches "pagehide" with persisted false on that target
    Then 1 s later the client is "connected"
    And its stream has yielded nothing more

  @W
  Scenario: A disconnected browser client stops listening to network and page events
    Given a browser client whose network and page event targets are recording targets of the test, connected over websocket
    When disconnect is called
    Then neither target holds a listener of the client
    And when the test dispatches "online" on the network target, the client stays "disconnected"

  @E
  Scenario: Going offline while connected closes the transport and reconnects after the backoff
    Given a client with minReconnectDelay 500 ms, connected over websocket
    When the device goes offline
    Then the client closes the transport
    And the client emits "connecting" with code 1 and reason "transport closed"
    And the next attempt starts after the reconnect backoff delay and the jitter window

  @E
  Scenario: Going offline while connecting closes the attempt in flight without an event
    Given a client configured with websocket and http_stream endpoints, whose websocket transport is opening on its first attempt
    When the device goes offline
    Then the client closes the opening transport
    And the client emits no event
    And the next attempt opens the websocket transport again, after the reconnect backoff delay

  @E
  Scenario: Going offline does nothing to a disconnected client
    Given a disconnected client
    When the device goes offline
    Then the machine returns no action

  @E
  Scenario: A page entering the back/forward cache reconnects with its own reason
    Given a client connected over websocket
    When the page enters the back/forward cache
    Then the client closes the transport
    And the client emits "connecting" with code 1 and reason "page entered back/forward cache"

  @E
  Scenario: Coming online after going offline keeps the jitter window
    Given 100 connected machines with different seeds, minReconnectDelay 20000 ms and a reconnect jitter window of 1000 ms
    When the device goes offline on each
    And the device comes online on each
    Then every next attempt is delayed by at most 1000 ms
    And the delays are not all equal

  @W
  Scenario Outline: A constructor given to a browser client replaces the global
    Given a browser client whose <shim> shim is a counting wrapper of the global, configured with only the server's <transport> endpoint
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the <shim> shim has been called at least once

    Examples:
      | shim            | transport   |
      | websocket       | websocket   |
      | fetch           | http_stream |
      | readable_stream | http_stream |
      | event_source    | sse         |

  @W
  Scenario: A browser client skips a transport the page has no constructor for
    Given the page has no global EventSource
    And a browser client configured with an unreachable websocket endpoint and the server's sse and http_stream endpoints, in that order
    When connect is called
    Then the client emits "connected" over transport "http_stream"
    And the client made no sse attempt

  @W
  Scenario: Building a browser client with no usable transport fails
    Given the page has no global EventSource
    When a browser client configured with only the server's sse endpoint is built
    Then building fails with a configuration error naming sse and the event_source shim

  @W
  Scenario Outline: With credentials included, a browser client's HTTP requests carry the page's cookies to the server's origin
    Given the page has set the cookie "affinity=a1" for its host
    And a browser client with credentials <credentials>, connected over transport "<transport>" through the relay, on the page's host at another port
    When the client sends a command
    Then the relay has seen the stream request and at least one emulation request
    And every one of them carried <cookie>

    Examples:
      | transport   | credentials  | cookie                          |
      | http_stream | included     | one Cookie header "affinity=a1" |
      | http_stream | not included | no Cookie header                |
      | sse         | included     | one Cookie header "affinity=a1" |
      | sse         | not included | no Cookie header                |
