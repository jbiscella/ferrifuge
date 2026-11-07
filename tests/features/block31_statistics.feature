Feature: Statistics

  Scenario: A new client's statistics are zero
    Given a new client
    Then its statistics report state "disconnected", no transport, no connected-since time and every counter 0

  @E
  Scenario: The core counts attempts, connects and publications taken
    Given a client core configured with websocket and http_stream endpoints
    When connect is called and the websocket attempt fails
    And the http_stream attempt connects
    And the server pushes 2 publications on a subscribed channel, which the application takes
    Then the core's statistics report 1 connect, 1 failed attempt and 2 publications delivered
    And they report transport "http_stream" and no connected-since time

  @E
  Scenario: The core counts messages as read and as written
    Given a client core connected over websocket, whose server expects pongs
    When the core reads a frame holding a ping and a publication push
    And it sends the pong, which the transport has not yet reported written
    Then the core's statistics report 2 more messages in, the frame's bytes in, and nothing more out
    And when the transport reports the pong written, they report 1 more message out and the pong's bytes out

  @D
  Scenario: Statistics count a session's traffic as the fake server sees it
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes 3 publications on channel "news"
    And the subscription receives them
    And the client publishes {"text":"hi"} to channel "news"
    Then the client's statistics report 1 connect, 0 failed attempts, 3 publications delivered, 1 subscription and transport "websocket"
    And they report as many bytes and messages in as the fake server sent, and as many bytes and commands out as it received

  @D
  Scenario: Statistics count failed attempts
    Given the fake server answers the first 2 connect commands with a temporary error
    And a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    Then when the application reads the client's stream, it yields "connecting", "error", "error", "connected"
    And the client's statistics report 1 connect and 2 failed attempts

  @D
  Scenario: Statistics count across reconnects
    Given a client connected to the fake server
    When the fake server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the client's statistics report 2 connects and 0 failed attempts
    And they report a connected-since time after the fake server's disconnect

  @D
  Scenario: Only publications taken count as delivered
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When unsubscribe is called on the subscription
    Then the subscription's stream yields "unsubscribed" and no publication
    And the client's statistics report 1 publication delivered

  @D
  Scenario: Delta statistics count full and delta payloads
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes 3 publications of 1 KiB that differ in a few bytes to "news"
    And the subscription receives them
    Then its delta statistics report 3 publications, 1 full payload and 2 delta payloads
    And they report as bytes received the full payload's size plus the 2 deltas' sizes, and as bytes decoded the 3 payloads' sizes
    And they report a compression ratio above 0.5

  @D
  Scenario Outline: Delta statistics stay zero unless delta is on
    Given a client subscribed to channel "news" on the fake server <option>
    When the fake server pushes 3 publications of 1 KiB that differ in a few bytes to "news"
    And the subscription receives them
    Then its delta statistics report every counter 0 and a compression ratio of 0

    Examples:
      | option                                                    |
      | with the fossil delta option, which did not turn delta on |
      | without the delta option                                  |
