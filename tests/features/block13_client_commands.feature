Feature: Client commands

  @B
  Scenario: RPC returns the proxy's result
    Given a connected client and an RPC proxy answering method "echo" with its input
    When the client calls RPC "echo" with {"n":1}
    Then the call returns {"n":1}

  @A
  Scenario: Publish reaches subscribers
    Given a client subscribed to channel "news"
    When the client publishes {"text":"hi"} to channel "news"
    Then the subscription receives a publication with data {"text":"hi"}

  @A
  Scenario: History returns the last publications
    Given a channel with history holding 3 publications
    When a connected client requests the last 2 publications of the channel
    Then it receives those 2 publications with their offsets

  @A
  Scenario: Binary publish over Protobuf keeps its bytes
    Given two clients configured with the Protobuf protocol subscribed to channel "news"
    When the first client publishes the bytes ff 00 fe to channel "news"
    Then the second client's subscription receives a publication with the bytes ff 00 fe

  @D
  Scenario: A subscription command fails after the timeout when the subscribe never completes
    Given a client connected to the fake server that never answers subscribe commands
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the history call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A call made before connect waits for the connection
    Given an RPC proxy answering method "echo" with its input
    And a new client
    When the client calls RPC "echo" before connect is called
    And connect is called
    Then the call completes once the client is connected

  @A
  Scenario: History since a known position
    Given a channel with history holding 3 publications
    When a connected client requests the channel's history since the position of the first one
    Then it receives the 2 later publications and the stream's current position

  @A
  Scenario: History in reverse order
    Given a channel with history holding 3 publications
    When a connected client requests the last 2 publications of the channel in reverse order
    Then it receives the third publication then the second

  @A
  Scenario: History with limit 0 returns only the stream position
    Given a channel with history holding 3 publications
    When a connected client requests the channel's history with limit 0
    Then it receives no publications and the stream's current epoch and offset

  @A
  Scenario: A client-published publication carries its publisher
    Given two clients of different users with connection and channel info subscribed to channel "news"
    When the first client publishes {"text":"hi"} to channel "news"
    Then the second client's subscription receives a publication whose info names the first client, its user, its connection info and its channel info

  @D
  Scenario: Asynchronous server messages are delivered with their exact bytes
    Given a client connected to the fake server
    When the fake server sends a message push whose data is the non-canonical JSON {"b": 1.0,  "a":2}
    Then the client emits a "message" event whose data is exactly those bytes

  @D
  Scenario: Asynchronous server messages over Protobuf keep binary data
    Given a client configured with the Protobuf protocol connected to the fake server
    When the fake server sends a Protobuf message push with the bytes ff 00 fe
    Then the client emits a "message" event with the bytes ff 00 fe

  @B
  Scenario: Temporary RPC error is reported as such
    Given a connected client and an RPC proxy answering method "busy" with a temporary error with code 1000 and message "try later"
    When the client calls RPC "busy"
    Then the call fails with a temporary error with code 1000 and message "try later"

  @A
  Scenario: Subscription-level commands use the subscription's channel
    Given two clients subscribed to a channel with history and presence
    When the first client's subscription publishes {"text":"hi"}
    Then the second client's subscription receives a publication with data {"text":"hi"}
    And the first client's subscription history ends with that publication
    And the first client's subscription presence lists both clients
    And the first client's subscription presence stats count 2 clients

  @D
  Scenario: Concurrent calls get their own replies
    Given a client connected to the fake server that answers commands in reverse order
    When the client calls RPC "first" and RPC "second" concurrently
    Then the "first" call returns the reply to "first"
    And the "second" call returns the reply to "second"

  @D
  Scenario: A call without a reply times out
    Given a client connected to the fake server that never replies to commands
    When the client calls RPC "slow"
    Then the call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A call made while connecting waits for the connection
    Given an RPC proxy answering method "echo" with its input
    And a new client
    When connect is called
    And the client calls RPC "echo" before it is connected
    Then the call completes once the client is connected

  Scenario: A queued call fails after the timeout when no connection comes
    Given a client configured with an unreachable endpoint
    When connect is called
    And the client calls RPC "echo"
    Then the call fails with a timeout error within the configured timeout plus tolerance

  @D
  Scenario: A call that timed out while queued is never sent
    Given a client configured with the fake server's endpoint behind a relay that is down
    When connect is called
    And the client calls RPC "echo"
    And the call fails with a timeout error
    And the relay comes up
    Then the client emits "connected"
    And the fake server receives no RPC command

  @D
  Scenario: Binary RPC over Protobuf keeps its bytes
    Given a client configured with the Protobuf protocol connected to the fake server that answers RPC "bin" with the bytes fe 00 ff
    When the client calls RPC "bin" with the bytes ff 00 fe
    Then the fake server received a Protobuf RPC command with the bytes ff 00 fe
    And the call returns the bytes fe 00 ff

  Scenario: A call on a client never connected fails after the timeout
    Given a new client
    When the client calls RPC "echo"
    And connect is never called
    Then the call fails with a timeout error within the configured timeout plus tolerance

  @B
  Scenario: A terminal connect failure fails a queued call at once
    Given an RPC proxy answering method "echo" with its input
    And the connect proxy answers after 1 s with disconnect code 4500
    And a new client
    When connect is called
    And the client calls RPC "echo" before it is connected
    Then the client emits "disconnected" with code 4500
    And the call fails with a "disconnected" error before the configured timeout

  @D
  Scenario: A subscription command on an unsubscribed subscription fails at once
    Given a client connected to the fake server with an unsubscribed subscription to channel "news"
    When its history is requested
    Then the call fails at once with a "subscription unsubscribed" error
    And the fake server receives no history command

  @D
  Scenario: A subscription command waiting for a failed subscribe fails at once
    Given a client connected to the fake server that answers subscribe commands after 1 s with a permanent error
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the subscription emits "unsubscribed"
    And the call fails with a "subscription unsubscribed" error before the configured timeout
    And the fake server receives no history command

  Scenario: Disconnect fails a queued call at once
    Given a client configured with an unreachable endpoint
    And connect is called
    And the client calls RPC "echo"
    When disconnect is called
    Then the call fails with a "disconnected" error

  @D
  Scenario: Disconnect fails a call already sent at once
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When disconnect is called
    Then the call fails with a "disconnected" error before the configured timeout

  @D
  Scenario: A subscription command waits until the subscription is subscribed
    Given a client connected to the fake server that answers subscribe commands after 1 s
    When a subscription to channel "news" is subscribed
    And its history is requested at once
    Then the fake server receives the history command only after it answered the subscribe command
    And the history call completes

  @D
  Scenario: Send delivers a message without a reply
    Given a client connected to the fake server
    When the client sends {"note":"x"}
    Then the fake server received a send command with data {"note":"x"} and no command id

  @A
  Scenario: Presence lists the subscribed clients
    Given two clients of different users with connection and channel info subscribed to a channel with presence
    When one of them requests the channel's presence
    Then it receives both clients with their user, connection info and channel info

  @D
  Scenario Outline: Commands round-trip over Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server
    When the client calls <command>
    Then the fake server received the <command> command in Protobuf
    And the call returns the fake server's Protobuf result for <command>

    Examples:
      | command        |
      | rpc            |
      | history        |
      | presence       |
      | presence_stats |

  @D
  Scenario: Send over Protobuf is received without a reply
    Given a client configured with the Protobuf protocol connected to the fake server
    When the client sends the bytes ff 00 fe
    Then the fake server received a Protobuf send command with the bytes ff 00 fe and no command id

  @B
  Scenario: A call whose emulation request fails fails at once
    Given an RPC proxy answering method "echo" with its input
    And a client connected over transport "http_stream" through a proxy that rejects the next emulation request
    When the client calls RPC "echo"
    Then the call fails with a "connection closed" error before the configured timeout
    And the client reconnects over transport "http_stream"

  @D
  Scenario: Losing the transport fails calls already sent at once
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When the fake server disconnects the client with a reconnect-advised code
    Then the call fails with a "connection closed" error before the configured timeout

  @A
  Scenario: Presence stats count clients and users
    Given two clients of different users subscribed to a channel with presence
    When one of them requests the channel's presence stats
    Then it receives 2 clients and 2 users
