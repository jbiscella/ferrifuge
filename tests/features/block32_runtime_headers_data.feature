Feature: Runtime HTTP headers and subscription data

  @B
  Scenario Outline: set_http_headers applies from the next connection attempt
    Given a connect proxy recording requests
    And a server with http_stream and sse enabled
    And a client with HTTP header "X-Device-Real: r1", configured with only the server's <transport> endpoint, connected
    When set_http_headers is called with "X-Device-Real: r2"
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy received header "X-Device-Real: r1" then "X-Device-Real: r2"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @B
  Scenario Outline: set_http_headers applies to the emulation requests of the open transport
    Given a client with HTTP header "X-Device-Real: r1" connected over transport "<transport>" through the HTTP proxy
    When set_http_headers is called with "X-Device-Real: r2" right after the HTTP proxy relayed an emulation request
    And the HTTP proxy relays another emulation request
    Then the HTTP proxy relayed one stream request, carrying header "X-Device-Real: r1"
    And every emulation request it relayed after the call carried header "X-Device-Real: r2"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  Scenario Outline: set_http_headers refuses a header the transport sets
    Given a new client with HTTP header "X-Device-Real: r1"
    When set_http_headers is called with "<header>"
    Then it fails with a configuration error naming "<name>" and not "<value>"

    Examples:
      | header                        | name                | value    |
      | Sec-WebSocket-Key: k1         | Sec-WebSocket-Key   | k1       |
      | Proxy-Authorization: p-secret | Proxy-Authorization | p-secret |

  @B
  Scenario: Subscription set_data applies from the next resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with subscription data {"room":1}
    When set_data is called on the subscription with {"room":2}
    And the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"room":1} then {"room":2}

  @E
  Scenario: A subscription data callback takes precedence over set_data
    Given a subscription whose data callback returns {"n":1}
    When set_data is called on the subscription with {"n":2}
    And the subscription is subscribed on a connected client
    Then the subscribe command carries data {"n":1}
