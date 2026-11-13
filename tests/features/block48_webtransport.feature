Feature: WebTransport (unstable)

  @A
  Scenario: A client connects over WebTransport and receives a publication
    Given a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled
    And a client configured with only the server's WebTransport endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "webtransport"
    When the client subscribes to channel "news" and the server API publishes {"n":1} to channel "news"
    Then the subscription receives a publication with data {"n":1}

  @A
  Scenario: Binary publication delivered over Protobuf on WebTransport
    Given a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled
    And a client configured with the Protobuf protocol connected over transport "webtransport" and subscribed to channel "news"
    When the server API publishes the bytes ff 0a 00 fe to channel "news"
    Then the subscription receives a publication with the bytes ff 0a 00 fe

  @A
  Scenario: Server pings over WebTransport are answered
    Given a server with HTTP/3 and WebTransport enabled, a ping interval of 3 s and a pong timeout of 2 s
    And a client connected over transport "webtransport"
    When 6 s elapse
    Then the client stays "connected"

  @A
  Scenario: A server disconnect over WebTransport reaches the client with its code
    Given a client connected over transport "webtransport"
    When the server API disconnects the client with code 4501 and reason "b48 test"
    Then the client emits "disconnected" with code 4501 and reason "b48 test"
    And the client makes no new connection attempt

  @A
  Scenario: WebTransport falls back to websocket when UDP is unreachable
    Given a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled
    And a client with a handshake timeout of 2 s, configured with a WebTransport endpoint on a UDP port where nothing listens and the server's wss endpoint
    When connect is called
    Then the client emits an "error" event for the failed transport attempt whose message ends with "handshake timeout"
    And within the configured handshake timeout plus tolerance the client emits "connected" over transport "websocket"

  @A
  Scenario: A server without WebTransport refuses the session and the client falls back
    Given a server serving TLS with a certificate from the test CA, with HTTP/3 enabled and WebTransport disabled
    And a client configured with the server's WebTransport and wss endpoints
    When connect is called
    Then the client emits an "error" event for the failed transport attempt over transport "webtransport"
    And then the client emits "connected" over transport "websocket"

  @A
  Scenario: A connect refused over WebTransport stops the client, whichever transport carries the refusal
    Given a server serving TLS with a certificate from the test CA, with HTTP/3 and WebTransport enabled, that requires a connection token
    And a client with an invalid connection token, configured with the server's WebTransport and wss endpoints
    When connect is called
    Then the client emits "disconnected" with code 3500 and reason "invalid token", either after one failed attempt and no "error" event, or after two failed attempts, the first reported by an "error" event for the failed transport attempt over transport "webtransport"
    And the client makes no new connection attempt

  @A
  Scenario Outline: TLS settings apply to WebTransport
    Given a server serving TLS with a certificate from a private CA the platform does not trust, with HTTP/3 and WebTransport enabled
    And a client configured with only the server's WebTransport endpoint and <trust>
    When connect is called
    Then <outcome>

    Examples:
      | trust                                                                               | outcome                                                                                                             |
      | that CA as an extra root certificate                                                | the client emits "connected" over transport "webtransport"                                                          |
      | no extra root certificate                                                           | the client emits an "error" event for the failed transport attempt, saying the server's certificate is not trusted |
      | a raw rustls configuration trusting only that CA, offering ALPN "h2" and "http/1.1" | the client emits "connected" over transport "webtransport"                                                          |

  @B
  Scenario: Real HTTP headers go with the WebTransport session request
    Given a server with HTTP/3 and WebTransport enabled and a connect proxy that receives header "X-Probe"
    And a client configured with only the server's WebTransport endpoint and the HTTP header "X-Probe" set to "b48"
    When connect is called
    Then the client emits "connected" over transport "webtransport"
    And the connect proxy received the header "X-Probe" with value "b48"

  @E
  Scenario: Subscribes go with the connect command over WebTransport
    Given a client configured with a WebTransport endpoint and two subscriptions in state "subscribing" that need no callback
    When the WebTransport transport opens
    Then the first frame sent carries the connect command followed by the two subscribe commands, in registry order
    And no subscription reaches "subscribed" before the connect reply is handled

  Scenario Outline: A WebTransport endpoint that is not https is rejected
    Given client options with transport "webtransport" and endpoint "<endpoint>"
    When the client is built
    Then building fails with a configuration error naming webtransport and https

    Examples:
      | endpoint                                      |
      | wss://localhost:8443/connection/webtransport  |
      | http://localhost:8000/connection/webtransport |
      |                                               |

  Scenario Outline: A raw TLS configuration unfit for QUIC is rejected for WebTransport
    Given client options with a WebTransport endpoint and a raw rustls configuration <configuration>
    When the client is built
    Then building fails with a configuration error naming TLS 1.3 and webtransport

    Examples:
      | configuration                                   |
      | offering TLS 1.2 only                           |
      | offering TLS 1.3 without TLS13_AES_128_GCM_SHA256 |

  Scenario: A connector with a WebTransport endpoint is rejected
    Given client options with a connector and a WebTransport endpoint
    When the client is built
    Then building fails with a configuration error naming the connector and webtransport

  Scenario: A WebTransport attempt with a proxy fails without sending a datagram
    Given a UDP socket on 127.0.0.1 that counts the datagrams it receives
    And a client configured with only a WebTransport endpoint at that socket's port and the proxy "http://127.0.0.1:9" for every endpoint
    When connect is called
    Then the client emits an "error" event for the failed transport attempt whose message contains "webtransport cannot go through a proxy"
    And the client state is "connecting"
    And the socket received no datagram

  Scenario: A WebTransport open that never completes is bounded by the handshake timeout
    Given a UDP socket on 127.0.0.1 that receives and never answers
    And a client with a handshake timeout of 1 s and a timeout of 10 s, configured with only a WebTransport endpoint at that socket's port
    When connect is called
    Then within 1 s plus tolerance the client emits an "error" event for the failed transport attempt whose message ends with "handshake timeout"
    And the socket received at least one datagram

  @D
  Scenario: A server that selects no application protocol is reached on a second handshake
    Given a fake WebTransport server whose TLS configuration offers no application protocol
    And a client configured with only the fake server's WebTransport endpoint
    When connect is called
    Then the client emits "connected" over transport "webtransport"
    And the fake server counted 2 QUIC handshakes
    When the fake server ends the session after a disconnect push with a reconnect-advised code
    Then the client emits "connected" over transport "webtransport"
    And the fake server counted 3 QUIC handshakes

  @D
  Scenario: A server that requires h3 is reached on the first handshake
    Given a fake WebTransport server whose TLS configuration offers only "h3"
    And a client configured with only the fake server's WebTransport endpoint
    When connect is called
    Then the client emits "connected" over transport "webtransport"
    And the fake server counted 1 QUIC handshake

  @D
  Scenario: An endpoint remembered without ALPN that starts requiring h3 is reached again
    Given a fake WebTransport server whose TLS configuration offers no application protocol
    And a client connected to the fake WebTransport server after 2 QUIC handshakes
    When the fake server's TLS configuration changes to offer only "h3"
    And the fake server ends the session after a disconnect push with a reconnect-advised code
    Then the client emits "connected" over transport "webtransport"
    And the fake server counted 4 QUIC handshakes
    When the fake server ends the session after a disconnect push with a reconnect-advised code
    Then the client emits "connected" over transport "webtransport"
    And the fake server counted 5 QUIC handshakes

  @D
  Scenario: A disconnect push right before the end of the WebTransport stream stops the client
    Given a client connected to the fake WebTransport server
    When the fake server writes a disconnect push with code 3500 and reason "bye" and ends the stream in the same write
    Then the client emits "disconnected" with code 3500 and reason "bye"
    And the client makes no new connection attempt

  @D
  Scenario: A WebTransport stream that ends without a disconnect push makes the client reconnect
    Given a client connected to the fake WebTransport server
    When the fake server ends the stream without a disconnect push
    Then the client emits "connecting" with code 1 and reason "transport closed"
    And then the client emits "connected" over transport "webtransport"

  @E
  Scenario: A WebTransport connection ended without a disconnect push before a server ping keeps the backoff growing
    Given a client with a minimum reconnect delay of 1 s and a maximum of 20 s, configured with a WebTransport endpoint
    When three times in a row the transport opens, the connect reply arrives and the transport ends with no code before a server ping is read
    Then the third reconnect delay lies within the backoff bounds of attempt 3
    When the next connection reads a server ping before its transport ends with no code
    Then the reconnect delay after it lies within the backoff bounds of attempt 1

  @D
  Scenario: A chosen WebTransport that ends before answering the connect gives way to the next transport
    Given a client configured with the fake WebTransport server's endpoint and the fake websocket server's endpoint, connected over transport "webtransport"
    And the fake WebTransport server ends every new stream before answering the connect
    When the fake WebTransport server ends the session after a disconnect push with a reconnect-advised code
    Then the client emits an "error" event for the failed transport attempt over transport "webtransport"
    And then the client emits "connected" over transport "websocket"

  @D
  Scenario: A WebTransport write the server never reads closes the transport
    Given a client with a timeout of 2 s connected to the fake WebTransport server
    And the fake server stops reading the stream
    When the client sends more data than the stream's flow-control window
    Then within the configured timeout plus tolerance the client emits "connecting" with code 1
    And the client opens a new connection

  @D
  Scenario Outline: A message over the limit ends a client connected over WebTransport
    Given a client with a maximum message size of 1024 bytes and the <protocol> protocol, connected to the fake WebTransport server
    When the fake server writes a publication push of 1025 bytes, split over three writes
    Then the client emits "disconnected" with code 3 and reason "message size limit exceeded"
    And the client makes no new connection attempt

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: Closing a WebTransport client ends the session within the timeout
    Given a client with a timeout of 2 s connected to the fake WebTransport server
    When disconnect is called
    Then the client emits "disconnected" before the configured timeout
    And the fake server sees the stream finished and the QUIC connection closed within the configured timeout plus tolerance

  @W
  Scenario Outline: A browser client connects over WebTransport and receives a publication
    Given a browser client with the <protocol> protocol, configured with only the WebTransport relay's endpoint
    When connect is called and the client subscribes to channel "news"
    And the server API publishes to channel "news"
    Then the client emits "connected" over transport "webtransport"
    And the subscription receives that publication

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @W
  Scenario: A browser without WebTransport skips the transport
    Given the test has removed WebTransport from globalThis
    And a browser client configured with the WebTransport relay's endpoint and the server's websocket endpoint
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the client emits no "error" event for transport "webtransport"
    And the WebTransport relay accepted no session

  @W
  Scenario: A browser client falls back from WebTransport to websocket when the session is refused
    Given the WebTransport relay refuses every session
    And a browser client configured with the WebTransport relay's endpoint and the server's websocket endpoint
    When connect is called
    Then the client emits an "error" event for the failed transport attempt over transport "webtransport"
    And then the client emits "connected" over transport "websocket"
