Feature: Test constructors, an in-memory server double, an injectable connector

  @E
  Scenario Outline: Test constructors build what the client core yields
    Given a client core with the JSON protocol over websocket, with a subscription to "news" subscribed before connect
    When connect is called and the transport opens
    And <reading>
    Then the core yields <yielded>, equal to <built>
    And the built value holds every value the row names

    Examples:
      | reading                                                                                                                                                                   | yielded                                    | built                                                                                                                                            |
      | the core reads the connect reply with client id "c1" and data {"a":1}                                                                                                     | the client's "connected" event             | the connected event built with client id "c1", transport "websocket" and data {"a":1}                                                            |
      | the core reads the connect reply, then the subscribe reply for "news", recoverable and positioned, at offset 3 with epoch "e"                                           | the "subscribed" event of "news"           | the subscribed details built as recoverable and positioned, with stream position offset 3, epoch "e"                                             |
      | the core reads the connect reply and the subscribe reply, then a publication of "news" with data {"n":1}, offset 4, tag "k" "v" and info of client "c1", user "u1"      | the publication event of "news"            | the publication built with data {"n":1}, offset 4, tag "k" "v" and the client info built with client "c1", user "u1"                             |
      | the core reads the connect reply, the application requests the history of "news" with limit -1, and the core reads publications {"n":1} at 1 and {"n":2} at 2, epoch "e", offset 2 | the history result                         | the history result built with the publications built with data {"n":1} offset 1 and data {"n":2} offset 2, epoch "e" and offset 2                |
      | the core reads the connect reply, the application requests the presence of "news", and the core reads one client "c1" of user "u1"                                      | the presence result                        | the presence result built with the client info built with client "c1", user "u1"                                                                 |
      | the core reads the connect reply, the application requests the presence stats of "news", and the core reads 2 clients and 1 user                                       | the presence stats result                  | the presence stats result built with 2 clients and 1 user                                                                                        |

  @D
  Scenario Outline: A connector gives the websocket its stream
    Given the fake server
    And a client configured with the <protocol> protocol and the endpoint "ws://centrifugo.invalid/connection/websocket", with a connector that opens a TCP connection to the fake server and records the endpoints it is given
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the connector was called once, with "ws://centrifugo.invalid:80/connection/websocket"
    And the fake server received a connect command in the <protocol> encoding
    When the fake server disconnects the client with a reconnect-advised code
    Then the client emits "connected" again
    And the connector was called twice

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario Outline: A wss endpoint over a connector is verified as without one
    Given the fake server behind a TLS listener whose certificate for "centrifugo.invalid" is issued by <issuer>
    And a client configured with the endpoint "wss://centrifugo.invalid/connection/websocket" and the test CA as an extra root certificate, with a connector that opens a TCP connection to that listener
    When connect is called
    Then <outcome>

    Examples:
      | issuer         | outcome                                                                                                                         |
      | the test CA    | the client emits "connected" over transport "websocket"                                                                         |
      | the private CA | the client emits an "error" event of kind transport naming an invalid peer certificate, and the fake server receives no command |

  @D
  Scenario Outline: A connector that fails fails the attempt
    Given the fake server
    And a client configured with the endpoint "ws://centrifugo.invalid/connection/websocket", with a connector that <fails> on its first call and opens a TCP connection to the fake server on later calls
    When connect is called
    Then the client emits "connecting", then an "error" event of kind transport whose message contains "<message>", then "connected"
    And the connector was called twice
    And the client's statistics count 1 failed attempt

    Examples:
      | fails                          | message            |
      | returns the error "tunnel down" | tunnel down        |
      | panics                         | connector panicked |

  @D
  Scenario: A connector that never finishes fails the attempt with the handshake timeout
    Given the fake server
    And a client configured with the endpoint "ws://centrifugo.invalid/connection/websocket" and a handshake timeout of 500 ms, with a connector whose first call never finishes and whose later calls open a TCP connection to the fake server
    When connect is called
    Then within the configured handshake timeout plus tolerance the client emits an "error" event of kind transport with message "handshake timeout"
    And the future of the connector's first call has been dropped
    And the client then emits "connected" over transport "websocket"

  @E
  Scenario Outline: A connector goes with websocket endpoints and no proxy setting only
    Given a client builder with a connector and <options>
    When the client is built
    Then <outcome>

    Examples:
      | options                                                                  | outcome                                                                                     |
      | the endpoint "ws://centrifugo.invalid/connection/websocket"             | the client is built                                                                         |
      | a websocket endpoint, then an http_stream endpoint as a fallback        | building fails with a configuration error naming the connector and transport "http_stream" |
      | an sse endpoint                                                          | building fails with a configuration error naming the connector and transport "sse"         |
      | a websocket endpoint and the proxy setting none                         | building fails with a configuration error naming the connector and the proxy setting        |
      | a websocket endpoint and the system proxy setting                       | building fails with a configuration error naming the connector and the proxy setting        |

  @D
  Scenario Outline: An application tests its subscription against the server double
    Given a server double
    And a client built with the double's endpoint and connector and the <protocol> protocol
    When connect is called and the application subscribes to channel "news"
    Then the client emits "connected" over transport "websocket", with a client id the double gave
    And the subscription reaches state "subscribed"
    When the double publishes {"n":1} to channel "news"
    Then the subscription receives a publication with data {"n":1} and offset 1
    And the double received a connect command, then a subscribe command for "news", and no other command

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: The server double recovers publications missed during a reconnect
    Given a server double
    And a client built with the double's endpoint and connector and a minimum reconnect delay of 200 ms, connected, with a recoverable subscription to "news" subscribed
    And the double published {"n":1} to channel "news", which the subscription received
    When the double disconnects the client with code 3001 "shutdown"
    And the double publishes {"n":2} and then {"n":3} to channel "news" while the connector has been called once
    Then the subscription emits "subscribing", then "subscribed" with recovered true
    And the subscription receives the publications {"n":2} at offset 2 and {"n":3} at offset 3, in that order, each exactly once
    And the double received a second subscribe command for "news", asking to recover from offset 1

  @D
  Scenario Outline: The server double answers calls
    Given a server double whose RPC handler answers method "echo" with the data it is given, and any other method with the error 104 "method not found"
    And a client built with the double's endpoint and connector, connected, with a subscription to "news" subscribed
    And the double published {"n":1} to channel "news"
    When the application <call>
    Then <outcome>

    Examples:
      | call                                                 | outcome                                                                                 |
      | calls RPC "echo" with {"a":1}                        | the call returns {"a":1}                                                                |
      | calls RPC "other" with {"a":1}                       | the call fails with code 104 and message "method not found"                             |
      | publishes {"n":2} to channel "news"                  | the publish succeeds and the subscription receives a publication {"n":2} at offset 2    |
      | requests the history of "news" with limit -1         | the result holds the publication {"n":1} at offset 1, and its offset is 1               |
      | requests the history of "news" with limit 0          | the result holds no publication, and its offset is 1                                    |
      | requests the presence of "news"                      | the result holds one client, with the client id of the client's "connected" event       |
      | requests the presence stats of "news"                | the result counts 1 client and 1 user                                                   |

  @D
  Scenario Outline: The server double refuses as the test asks
    Given a server double that <refusal>
    And a client built with the double's endpoint and connector
    When connect is called and the application subscribes to channel "secret"
    Then <outcome>

    Examples:
      | refusal                                                                  | outcome                                                                                                                                  |
      | refuses the next connect with the temporary error 100 "internal server error" | the client emits "connecting", an "error" event of kind connect with code 100, then "connected", and the double received two connect commands |
      | refuses subscriptions to "secret" with the error 103 "permission denied"  | the client is "connected" and the subscription emits "unsubscribed" with code 103                                                        |

  @D
  Scenario Outline: The server double disconnects the client with a code
    Given a server double
    And a client built with the double's endpoint and connector and reconnect delays from 50 ms to 200 ms, connected
    When the double disconnects the client with code <code> "<reason>"
    Then the client emits "<state>" with code <code> and reason "<reason>"
    And after 1 s the connector has been called <calls> times and the double has <open> connection open

    Examples:
      | code | reason      | state        | calls | open |
      | 3001 | shutdown    | connecting   | 2     | 1    |
      | 3501 | bad request | disconnected | 1     | 0    |

  @D
  Scenario: The server double grants server-side subscriptions and sends messages
    Given a server double that grants the server-side subscription "personal:1" at connect
    And a client built with the double's endpoint and connector
    When connect is called
    Then the client emits a server-side "subscribed" event for channel "personal:1"
    When the double publishes {"n":1} to channel "personal:1"
    Then the client emits a server-side publication of "personal:1" with data {"n":1} and offset 1
    When the double sends the message {"m":1}
    Then the client emits a "message" event with data {"m":1}

  @A
  Scenario: The server double and Centrifugo answer alike
    Given a Centrifugo server whose channel "news" keeps history, allows recovery and accepts publishes from clients, and a server double
    And for each of them, a client with a minimum reconnect delay of 1 s and a connection token for user "u1", connected to it, with a recoverable subscription to "news" subscribed
    When each client publishes {"n":1} and then {"n":2} to channel "news"
    And each client requests the history of "news" with limit -1
    And each server disconnects its client with code 3001 "shutdown", Centrifugo through its server API
    And each server publishes {"n":3} to channel "news" before its client reconnects
    Then both clients emit the same client and subscription events, with the same codes, in the same order, "subscribed" with recovered true included
    And both subscriptions receive the publications {"n":1}, {"n":2} and {"n":3} at offsets 1, 2 and 3
    And both history results hold {"n":1} at offset 1 and {"n":2} at offset 2, with offset 2
