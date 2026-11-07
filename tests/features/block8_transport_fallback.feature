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
