Feature: Refusing endpoints that are not TLS

  @D
  Scenario Outline: With tls_only an endpoint that is not TLS fails the build
    Given client options with tls_only and the <transport> endpoint "<endpoint>"
    When the client is built
    Then building fails with a configuration error of transport <transport> with message "<message>"

    Examples:
      | transport   | endpoint                                                         | message                                                                 |
      | websocket   | ws://example.invalid/connection/websocket?token=secret-token     | tls_only refuses the websocket endpoint: its scheme ws is not wss       |
      | websocket   | WS://example.invalid/connection/websocket?token=secret-token     | tls_only refuses the websocket endpoint: its scheme ws is not wss       |
      | websocket   | secret-token://example.invalid/connection/websocket              | tls_only refuses the websocket endpoint: its scheme is not wss          |
      | http_stream | http://example.invalid/connection/http_stream?token=secret-token | tls_only refuses the http_stream endpoint: its scheme http is not https |
      | http_stream | wss://example.invalid/connection/http_stream?token=secret-token  | tls_only refuses the http_stream endpoint: its scheme wss is not https  |
      | sse         | http://example.invalid/connection/sse?token=secret-token         | tls_only refuses the sse endpoint: its scheme http is not https         |

  @D
  Scenario Outline: With tls_only an emulation endpoint that is not TLS fails the build
    Given client options with tls_only, the <transport> endpoint "<endpoint>" and the emulation endpoint "http://example.invalid/emulation?token=secret-token"
    When the client is built
    Then building fails with a configuration error of no transport with message "tls_only refuses the emulation endpoint: its scheme http is not https"

    Examples:
      | transport   | endpoint                                       |
      | http_stream | https://example.invalid/connection/http_stream |
      | websocket   | wss://example.invalid/connection/websocket     |

  @D
  Scenario: With tls_only a connector does not exempt a plain endpoint
    Given a server double
    And client options with tls_only and the double's endpoint and connector
    When the client is built
    Then building fails with a configuration error of transport websocket with message "tls_only refuses the websocket endpoint: its scheme ws is not wss"

  @D
  Scenario Outline: With tls_only TLS endpoints build, whatever the scheme's case
    Given client options with tls_only and <endpoints>
    When the client is built
    Then the client is built

    Examples:
      | endpoints                                                                                                                                |
      | the websocket endpoint "wss://example.invalid/connection/websocket"                                                                      |
      | the websocket endpoint "WSS://example.invalid/connection/websocket"                                                                      |
      | the http_stream endpoint "HTTPS://example.invalid/connection/http_stream" and the emulation endpoint "https://example.invalid/emulation" |
      | the sse endpoint "https://example.invalid/connection/sse" and the emulation endpoint "HTTPS://example.invalid/emulation"                 |

  @D
  Scenario: Without tls_only endpoints that are not TLS build as before
    Given client options with the websocket endpoint "ws://example.invalid/connection/websocket", the http_stream endpoint "http://example.invalid/connection/http_stream" and the emulation endpoint "http://example.invalid/emulation"
    When the client is built
    Then the client is built

  @W
  Scenario: A browser client with tls_only refuses an endpoint that is not TLS
    When a browser client with tls_only and the websocket endpoint "ws://example.invalid/connection/websocket?token=secret-token" is built
    Then building fails with a configuration error of transport websocket with message "tls_only refuses the websocket endpoint: its scheme ws is not wss"
