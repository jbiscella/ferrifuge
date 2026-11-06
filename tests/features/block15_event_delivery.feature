Feature: Event delivery and backpressure

  @D
  Scenario: Publications not taken before disconnect are recovered on the next connect
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the subscription's stream yields, in order, "subscribing", "subscribed", and the publications at offsets 2 and 3
    And no publication is yielded twice

  @D
  Scenario: Server-side publications not taken before disconnect are recovered on the next connect
    Given the fake server grants a recoverable server-side subscription to channel "alerts" at offset 0 at connect
    And the fake server has sent publications at offsets 1, 2 and 3 to "alerts"
    And the application has read the client's stream up to the server-side publication at offset 1, and no further
    And the client holds the server-side publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    Then that connect command asks to recover "alerts" from offset 1
    And the client's stream yields no server-side publication before "connected"

  @D
  Scenario: Publications not taken before unsubscribe are withdrawn
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When unsubscribe is called on the subscription
    Then the subscription's stream yields "unsubscribed" and no publication

  @D
  Scenario: The server closing the connection is handled only after the publications before it are taken
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1, 2 and 3 to "news", then closed the connection with a reconnect-advised code
    When 2 s elapse
    Then the client has opened no new connection
    And when the application reads the subscription's stream, it yields the publications at offsets 1, 2 and 3, then "subscribing"
    And the next subscribe command for "news" asks to recover from offset 3

  @D
  Scenario: A state change not taken holds the messages of the next connection
    Given a client whose stream was taken before connect is called and is not read, connected to the fake server
    When disconnect is called
    And connect is called
    Then within 1 s the fake server has received a second connect command
    And 1 s later the client's state is "connecting"
    And when the application reads the client's stream, it yields "connecting", "connected", "disconnected", "connecting", "connected"

  @D
  Scenario: A stream that is not read stops the client reading after 256 items
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    When the fake server starts sending 10000 publications of 100 bytes to "news"
    Then the client stops reading with 256 messages in its read-ahead
    And 1 s later its read-ahead still holds 256 messages
    And when the application reads the subscription's stream, it yields the 10000 publications in order, each once

  @D
  Scenario: The read-ahead also stops at 4 MiB
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    When the fake server starts sending 40 publications of 512 KiB to "news"
    Then the client stops reading with more than 3 MiB and at most 4 MiB in its read-ahead
    And 1 s later its read-ahead still holds at most 4 MiB
    And when the application reads the subscription's stream, it yields the 40 publications in order, each once

  @D
  Scenario: An item larger than the read-ahead budget is still delivered
    Given a client configured with a read-ahead of 1 KiB, subscribed to channel "news" on the fake server
    When the fake server sends a publication of 64 KiB to "news"
    Then the subscription's stream yields it

  @E
  Scenario Outline: A read-ahead limit of 0 is rejected
    Given a Config whose read-ahead limit of <limit> is 0
    When a client is created
    Then Client::new returns an error

    Examples:
      | limit    |
      | messages |
      | bytes    |

  @D
  Scenario: A second events() call takes over the subscription's stream
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When the application calls the subscription's events() again
    Then the first stream's next item is its end
    And the new stream yields the publications at offsets 1 and 2

  @D
  Scenario: A second events() call takes over the client's stream
    Given a client whose stream was taken before connect is called and is not read, connected to the fake server
    When the application calls the client's events() again
    Then the first stream's next item is its end
    And the new stream yields "connecting", then "connected"

  @D
  Scenario: A cancelled read loses no publication
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the application has started reading the subscription's stream
    And the client holds a publication to "news" not yet taken
    When the application cancels that read before polling it again
    Then the subscription's stream yields that publication

  @D
  Scenario: Publications of a subscription without a stream do not hold the connection
    Given a client subscribed to channels "news" and "chat" on the fake server
    And the application dropped the stream of "news" and reads the stream of "chat"
    When the fake server sends 1000 publications to "news", then one to "chat"
    Then the stream of "chat" yields that publication

  @D
  Scenario: Pings are answered while a stream is not read
    Given a client connected to the fake server, which pings every 1 s and expects pongs
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent 10 publications to "news"
    When 5 s elapse with the fake server pinging every 1 s
    Then the client has answered every ping
    And the client has not emitted "connecting" with code 2

  @D
  Scenario: A full read-ahead does not make the client report missing pings
    Given a client with maxServerPingDelay 1000 ms, connected to the fake server pinging every 1 s
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications of 100 bytes to "news" until the client stopped reading
    When 5 s elapse
    Then the client has not emitted "connecting" with code 2

  @D
  Scenario: A subscribe reply waiting behind a publication not taken does not time out
    Given a client with a timeout of 1 s, subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent a publication to "news"
    When the application subscribes to channel "chat"
    And 3 s elapse
    Then the client has not emitted "connecting" with code 3
    And when the application reads the stream of "news", the stream of "chat" yields "subscribed"

  @D
  Scenario: The connection token is refreshed while a stream is not read
    Given a client connected to the fake server with a connection token that expires in 2 s
    And the client is subscribed to channel "news", whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications of 100 bytes to "news" until the client stopped reading
    When 3 s elapse
    Then the fake server has received a refresh command

  @E
  Scenario: A client stream not read keeps every state change but drops errors past 256 untaken events
    Given a client whose stream was taken before connect is called and is not read, and whose server is unreachable
    When 1000 connection attempts fail
    And the server becomes reachable and the client connects
    Then reading the stream yields "connecting", 255 error events, then "connected"

  @E
  Scenario: A subscription stream not read drops errors past 256 untaken events
    Given a connected client with a subscription whose stream was taken before subscribe is called and is not read, and whose get_data callback fails
    When 1000 subscribe attempts fail
    And the get_data callback succeeds and the subscription subscribes
    Then reading the subscription's stream yields "subscribing", 255 error events, then "subscribed"

  @A
  Scenario: A slow consumer on a channel with history and recovery loses no publication
    Given a server whose channel "news" keeps a history of 8000 publications and recovers up to 8000, with a client queue of 64 MiB
    And a client subscribed to "news", whose subscription's stream has been read up to "subscribed" and is not read after
    When the server API publishes 8000 publications of 4 KiB to "news"
    And 2 s elapse
    And the application then reads the subscription's stream until it has 8000 publications
    Then they are the offsets 1 to 8000, in order, each exactly once
    And between them the stream yielded "subscribing", then "subscribed" with recovered true

  @E
  Scenario Outline: Reconnect delays keep growing while the server closes each connection with 3008
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes <closes> connections in a row with code 3008 "slow", each after its connect reply and the subscribe reply for "chat", before any server ping
    And the reconnect delay after the last of them is computed 1000 times
    Then every computed delay is at most <bound> ms
    And the largest computed delay is more than <above> ms

    Examples:
      | closes | above | bound |
      | 2      | 1000  | 1500  |
      | 3      | 1500  | 2500  |
      | 4      | 2500  | 4500  |
      | 5      | 4500  | 8500  |
      | 6      | 8500  | 16500 |
      | 7      | 16500 | 20000 |

  @E
  Scenario: A Disconnect push with 3008 keeps the backoff growing over an emulated transport
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over http_stream
    When the server ends 4 connections in a row with a Disconnect push of code 3008 "slow", each after its connect reply and the subscribe reply for "chat", before any server ping
    And the reconnect delay after the last of them is computed 1000 times
    Then every computed delay is at most 4500 ms
    And the largest computed delay is more than 2500 ms

  @E
  Scenario: A server ping read before 3008 lets the reset stand
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes 5 connections in a row with code 3008 "slow", each after its connect reply, before any server ping
    And the next connection reads a server ping after its connect reply, then the server closes it with code 3008 "slow"
    And the reconnect delay after that close is computed 1000 times
    Then every computed delay is at most 1000 ms

  @E
  Scenario: A close with another code lets the reset stand
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms, subscribed to channels "news" and "chat" over websocket
    When the server closes 5 connections in a row with code 3008 "slow", each after its connect reply, before any server ping
    And the server closes the next connection with code 3001 "shutdown" after its connect reply, before any server ping
    And the reconnect delay after that close is computed 1000 times
    Then every computed delay is at most 1000 ms
