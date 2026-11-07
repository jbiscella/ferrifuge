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
