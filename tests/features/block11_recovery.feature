Feature: Subscription recovery

  @A
  Scenario: A first subscribe does not recover
    Given a connected client
    When a subscription to a channel with history and recovery is subscribed
    Then the subscription emits "subscribed" with was_recovering false and recovered false

  @D
  Scenario: Positioned and recoverable options are requested
    Given a client connected to the fake server
    When a subscription to channel "news" with the positioned and recoverable options is subscribed
    Then the fake server received a subscribe command for "news" requesting positioned and recoverable

  @D
  Scenario Outline: The subscribed event reports the granted capabilities
    Given a client connected to the fake server
    When a subscription to channel "news" with the positioned and recoverable options is subscribed and the fake server grants <granted>
    Then the subscription emits "subscribed" reporting positioned <positioned>, recoverable <recoverable> and the fake server's stream position

    Examples:
      | granted          | positioned | recoverable |
      | both             | true       | true        |
      | positioned only  | true       | false       |
      | recoverable only | false      | true        |
      | neither          | false      | false       |

  @D
  Scenario: The first subscribe reply's position is used for recovery
    Given a client connected to the fake server
    And a subscription to recoverable channel "news" whose subscribe reply carries a stream position
    When the fake server disconnects the client with a reconnect-advised code before any publication
    Then the next subscribe command for "news" asks to recover from that stream position

  @D
  Scenario: A changed epoch on a live publication is used for recovery
    Given a client subscribed to recoverable channel "news" on the fake server
    When the fake server pushes a publication with a new epoch
    And the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "news" carries the new epoch and that publication's offset

  @D
  Scenario: Recovered publications advance the stored position
    Given a client subscribed to recoverable channel "news" on the fake server
    And the client recovered 2 publications after a reconnect
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next subscribe command for "news" carries the offset of the second recovered publication

  @A
  Scenario: Missed publications are recovered
    Given a client subscribed to a channel with history and recovery
    And the subscription has received 1 live publication
    And the client is disconnected with a reconnect-advised code
    When 2 publications are made to the channel while the client reconnects
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly those 2 publications in order

  @A
  Scenario: Publications between unsubscribe and subscribe are recovered
    Given a client subscribed to a channel with history and recovery
    When unsubscribe is called on the subscription
    And 2 publications are made to the channel
    And subscribe is called on the subscription again
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly those 2 publications in order

  @A
  Scenario: Publications made while explicitly disconnected are recovered
    Given a client subscribed to a channel with history and recovery
    And the subscription has received 1 live publication
    When disconnect is called
    And 2 publications are made to the channel
    And connect is called
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly those 2 publications in order

  @A
  Scenario: The since option recovers on the first subscribe
    Given a channel with history and recovery holding 3 publications
    And a subscription to the channel with the since option at the position of the first one
    When the subscription is subscribed on a connected client
    Then the subscription emits "subscribed" with was_recovering true and recovered true
    And the subscription receives exactly the 2 later publications in order

  @A
  Scenario: Recovery fails when history is gone
    Given a client subscribed to a channel with history and recovery
    And the channel history expires while the client is disconnected
    When the client resubscribes
    Then the subscription emits "subscribed" with was_recovering true and recovered false

  @D
  Scenario: A failed recovery's reply replaces the stored position
    Given a client subscribed to recoverable channel "news" on the fake server
    And a resubscribe after a reconnect that the fake server answered with recovered false and a new stream position
    When the fake server disconnects the client with a reconnect-advised code again
    Then the next subscribe command for "news" asks to recover from the new stream position

  @B
  Scenario: getState provides the position of the initial subscribe
    Given a subscribe proxy recording requests
    And a subscription with a getState callback to a channel with history and recovery
    When the subscription is subscribed on a connected client
    Then the getState callback is called once before the subscribe command
    And the subscribe command carries the stream position getState returned

  @B
  Scenario: getState reloads state when recovery fails
    Given a subscribe proxy recording requests
    And a client subscribed with a getState callback to a channel with history and recovery
    And the channel history expires while the client is disconnected
    When the client resubscribes
    Then the getState callback is called a second time
    And until then the subscription stays "subscribing" and receives no publication
    And the next subscribe command carries the stream position it returned
    And then the subscription emits "subscribed"

  @E
  Scenario: The since option takes precedence over getState on the first subscribe
    Given a subscription with the since option and a getState callback
    When the subscription is subscribed on a connected client
    Then the getState callback is not called
    And the subscribe command asks to recover from the since position

  @E
  Scenario: A failing getState is retried before subscribing
    Given a subscription with a getState callback that fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And getState is called again after the resubscribe backoff delay
    And the subscribe command carries the stream position the second call returned

  @D
  Scenario: State invalidation fetches a new subscription token
    Given a client subscribed to channel "news" on the fake server using a subscription token callback
    When the fake server unsubscribes the subscription with code 2502
    Then the subscription token callback is called again
    And the subscription resubscribes with the new token and without a stream position

  @D
  Scenario: Connection state invalidation clears the token and invalidates every position
    Given a client using a connection token callback, connected to the fake server
    And a client-side subscription to channel "news" with a stream position
    And a server-side subscription to channel "alerts" with a stream position from the connect reply
    When the fake server disconnects the client with code 3014
    Then the connection token callback is called before the next connect
    And the next connect command carries no position for "alerts"
    And the next subscribe command for "news" asks to recover from offset 0 and epoch "_"

  @D
  Scenario: Connection state invalidation drops a fixed connection token
    Given a client using a fixed connection token and no token callback, connected to the fake server
    When the fake server disconnects the client with code 3014
    Then the next connect command carries no token

  @D
  Scenario: State invalidation drops a fixed subscription token
    Given a client subscribed to channel "news" on the fake server using a fixed subscription token
    When the fake server unsubscribes the subscription with code 2502
    Then the subscription resubscribes without a token and without a stream position
