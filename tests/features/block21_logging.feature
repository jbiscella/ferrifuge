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
