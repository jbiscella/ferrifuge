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
    Given a server with http_stream and sse enabled, a ping interval of 3 s and a pong timeout of 2 s
    And a client configured with the Protobuf protocol connected over transport "http_stream"
    When 6 s elapse
    Then the client stays "connected"
