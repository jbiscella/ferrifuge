Feature: Server-side subscriptions

  @B
  Scenario: Publications of a server-side subscription are delivered
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    Then the client emits a server-side "subscribed" event for channel "alerts"
    When the server API publishes {"level":1} to channel "alerts"
    Then the client receives a server-side publication on channel "alerts" with data {"level":1}

  @B
  Scenario: Server-side subscriptions survive a reconnect
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    When the server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "subscribing" event for channel "alerts"
    And then the client emits a server-side "subscribed" event for channel "alerts"

  @B
  Scenario: Missed publications of a server-side subscription are recovered
    Given the connect proxy subscribes the connection to channel "alerts" with history and recovery
    And a connected client
    And the client has received 1 live server-side publication on channel "alerts"
    And the client is disconnected with a reconnect-advised code
    When 2 publications are made to channel "alerts" while the client reconnects
    Then the client emits a server-side "subscribed" event for channel "alerts" reporting recoverable true, was_recovering true, recovered true and the new stream position
    And the client receives exactly those 2 server-side publications in order

  @D
  Scenario: Client-side and server-side subscriptions to one channel stay separate
    Given a client with an unsubscribed client-side subscription to channel "alerts"
    And the fake server grants a server-side subscription to channel "alerts" at connect
    And the client is connected to the fake server
    When the fake server pushes a publication on channel "alerts"
    Then the client receives it as a server-side publication
    And the client-side subscription receives nothing and stays "unsubscribed"
    When the client-side subscription is subscribed
    Then it reaches state "subscribed"
    And the next publication on channel "alerts" goes to the client-side subscription
    And the server-side registry still lists channel "alerts"

  @B
  Scenario: Server-side subscriptions survive an explicit disconnect and connect
    Given the connect proxy subscribes the connection to channel "alerts" with history and recovery
    And a connected client
    And the client has received 1 live server-side publication on channel "alerts"
    When disconnect is called
    Then the client emits a server-side "subscribing" event for channel "alerts"
    When 2 publications are made to channel "alerts"
    And connect is called
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And the client receives exactly those 2 server-side publications in order

  @B
  Scenario: A subscription added by a push is recovered after reconnect
    Given a connected client
    And the server API subscribes the client to recoverable channel "alerts"
    And the client has received 1 live server-side publication on channel "alerts"
    And the connect proxy will subscribe the next connection to channel "alerts"
    When the client is disconnected with a reconnect-advised code
    And 2 publications are made to channel "alerts" while the client reconnects
    Then the client receives exactly those 2 server-side publications in order

  @A
  Scenario: A subscription added by the server after connect is tracked
    Given a connected client
    When the server API subscribes the client to channel "alerts"
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And a publication to channel "alerts" is received as a server-side publication

  @B
  Scenario Outline: Server-side subscriptions granted at connect over an emulated transport
    Given a server with http_stream and sse enabled
    And the connect proxy subscribes the connection to channel "alerts"
    When a client connects over transport "<transport>"
    Then the client emits a server-side "subscribed" event for channel "alerts"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @D
  Scenario: Server-side subscribed events carry the server's data
    Given the fake server grants a server-side subscription to channel "alerts" with data {"tier":"gold"} at connect
    And a client connected to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts" with data {"tier":"gold"}
    When the fake server pushes a server-side subscription to channel "news" with data {"tier":"silver"}
    Then the client emits a server-side "subscribed" event for channel "news" with data {"tier":"silver"}

  @A
  Scenario: A server-side subscription removed by the server is unsubscribed
    Given a connected client
    And the server API subscribes the client to channel "alerts"
    When the server API unsubscribes the client from channel "alerts"
    Then the client emits a server-side "unsubscribed" event for channel "alerts"

  @B
  Scenario: A server-side subscription missing after reconnect is unsubscribed
    Given the connect proxy subscribes the connection to channel "alerts"
    And a connected client
    And the connect proxy will subscribe the next connection to no channel
    When the server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "unsubscribed" event for channel "alerts"

  @D
  Scenario: A server-side subscription missing after reconnect is forgotten
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server
    And the fake server will grant no server-side subscription on later connects
    When the fake server disconnects the client with a reconnect-advised code
    Then the client emits a server-side "unsubscribed" event for channel "alerts"
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next connect command carries no entry for "alerts"
    And no second server-side "unsubscribed" event for channel "alerts" is emitted

  @D
  Scenario: A pushed server-side subscription's position is used for recovery
    Given a client connected to the fake server
    When the fake server pushes a recoverable server-side subscription to channel "alerts" with a stream position
    And the fake server disconnects the client with a reconnect-advised code before any publication
    Then the next connect command asks to recover "alerts" from that stream position

  @D
  Scenario: Recovered server-side publications advance the stored position
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server
    And the client recovered 2 server-side publications on channel "alerts" after a reconnect
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next connect command asks to recover "alerts" from the offset of the second recovered publication

  @D
  Scenario: Initial publications of a server-side subscription granted at connect are delivered
    Given the fake server grants a server-side subscription to channel "alerts" with 1 initial publication and recovered false at connect
    When a client connects to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts"
    And then the client receives that publication as a server-side publication

  @D
  Scenario: A server-side unsubscribe carries its code and reason and forgets the subscription
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at the first connect only
    And a client connected to the fake server
    When the fake server pushes an unsubscribe for channel "alerts" with code 2000 and reason "alerts closed"
    Then the client emits a server-side "unsubscribed" event for channel "alerts" with code 2000 and reason "alerts closed"
    When the fake server disconnects the client with a reconnect-advised code
    Then the next connect command carries no entry for "alerts"
    And no further server-side event for channel "alerts" is emitted
