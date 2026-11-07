Feature: Closing a client

  @D
  Scenario: close waits until the connection is closed and the stream ends
    Given a client connected to the fake server
    When close is awaited
    Then close resolves
    And the fake server sees the connection closed within the configured timeout plus tolerance
    And the client emits "disconnected" with code 0 and reason "disconnect called"
    And the client's stream ends
    And the client is closed

  @D
  Scenario: close unsubscribes every subscription with code 2 and sends no unsubscribe command
    Given a client subscribed to channel "news" on the fake server
    When close is awaited
    Then the subscription emits "unsubscribed" with code 2 and reason "client closed"
    And the subscription's stream ends
    And the fake server sees the connection closed within the configured timeout plus tolerance
    And the fake server received no unsubscribe command

  @D
  Scenario: close ends a server-side subscription before the client's "disconnected"
    Given the fake server grants a server-side subscription to channel "news" at connect
    And a client connected to the fake server
    And the client emits a server-side "subscribed" event for channel "news"
    When close is awaited
    Then the client emits a server-side "unsubscribed" event for channel "news" with code 2
    And then the client emits "disconnected" with code 0
    And the client's stream ends

  @D
  Scenario: A call pending at close fails with "client closed"
    Given a client connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When close is awaited
    Then the call fails with a "client closed" error before the configured timeout

  @D
  Scenario: Dropping the client unsubscribes its subscriptions with code 2
    Given a client subscribed to channel "news" on the fake server
    When the client is dropped
    Then the subscription state is "unsubscribed"
    And the subscription emits "unsubscribed" with code 2 and reason "client closed"
    And the subscription's stream ends
    And the subscription's history call fails at once with a "client closed" error

  Scenario: close on a disconnected client resolves at once
    Given a new client
    When close is awaited
    Then close resolves at once
    And the client's stream ends with no "disconnected" event

  Scenario: A closed client stays closed
    Given a new client
    When close is awaited
    And connect is called
    Then the client state is "disconnected"
    And the client is closed
    And an RPC call fails at once with a "client closed" error
    And creating a subscription fails because the client is closed
