Feature: TLS verification

  @A
  Scenario: A private certificate authority given as an extra root is trusted
    Given a server serving TLS with a certificate from a private CA the platform does not trust
    And a client configured with the server's wss endpoint and that CA as an extra root certificate
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @A
  Scenario: A server whose CA is not trusted is refused
    Given a server serving TLS with a certificate from a private CA the platform does not trust
    And a client configured with the server's wss endpoint and no extra root certificate
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted
    And the client state is "connecting"

  @A
  Scenario Outline: Extra roots also apply to HTTPS transports
    Given a server serving TLS with a certificate from a private CA the platform does not trust, with http_stream and sse enabled
    And a client configured with an unreachable websocket endpoint, only the server's https <transport> endpoint, and that CA as an extra root certificate
    When connect is called
    Then the client emits "connected" over transport "<transport>"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @A
  Scenario Outline: A raw rustls configuration is used as given
    Given a server serving TLS with a certificate from a private CA the platform does not trust, with http_stream and sse enabled
    And a client configured with only the server's <transport> endpoint and a raw rustls configuration trusting only that CA, offering ALPN "h2" and "http/1.1"
    When connect is called
    Then the client emits "connected" over transport "<transport>"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |

  @A
  Scenario: A raw rustls configuration replaces the platform's trust
    Given a server serving TLS with a certificate from the test CA
    And a client configured with the server's wss endpoint and a raw rustls configuration trusting only a private CA the platform does not trust
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted
    And the client state is "connecting"

  Scenario: Extra roots and a raw configuration together are rejected
    Given client options with an extra root certificate and a raw rustls configuration
    When the client is built
    Then building fails with a configuration error naming both

  Scenario: An extra root that does not parse is rejected
    Given client options with a valid extra root certificate followed by one whose bytes are not a certificate
    When the client is built
    Then building fails with a configuration error naming extra root certificate 1
