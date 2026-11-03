Feature: Subscription tokens, unsubscribe codes and resubscribe backoff

  @D
  Scenario Outline: Server unsubscribe codes
    Given a client subscribed to channel "news" on the fake server
    When the fake server unsubscribes the subscription with code <code>
    Then the subscription emits "<state>" with code <code> and the reason sent
    And the subscription state is "<state>"

    Examples:
      | code | state        |
      | 2000 | unsubscribed |
      | 2499 | unsubscribed |
      | 2500 | subscribing  |
      | 2999 | subscribing  |

  @A
  Scenario: Subscription token refreshed before expiry keeps the subscription
    Given a client subscribed to a private channel using a subscription token with expiration
    When 10 s elapse
    Then the subscription stays "subscribed"
    And the subscription token was refreshed at least twice

  @E
  Scenario: A failing first subscription token fetch is retried before subscribing
    Given a subscription to a private channel without an initial token whose token callback fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And the token callback is called again after the resubscribe backoff delay
    And the subscribe command carries the token the second call returned

  @E
  Scenario: Subscribe error 109 fetches a new token and resubscribes
    Given a subscription to a private channel using a subscription token callback
    When the server answers its subscribe command with error 109
    Then the subscription token callback is called again
    And the subscription resubscribes with the new token

  @E
  Scenario: A failing subscription token callback is retried
    Given a subscribed subscription whose token expires and whose token callback fails once
    When the subscription refresh is due
    Then the subscription emits an "error" event
    And the refresh is retried after 5 to 10 s

  @E
  Scenario: A temporary sub_refresh error is retried
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with a temporary error
    Then the subscription emits an "error" event
    And the refresh is retried after 5 to 10 s

  @E
  Scenario: A sub_refresh without a reply is retried
    Given a subscribed subscription whose token expires
    When the server never answers its sub_refresh
    Then after the configured timeout the subscription emits an "error" event with the timeout code
    And the subscription token callback is called again after 5 to 10 s
    And a new sub_refresh carrying the new token is sent

  @E
  Scenario: A subscription token denied before the first subscribe unsubscribes
    Given a subscription to a private channel without an initial token whose token callback signals "unauthorized"
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1
    And no subscribe command is sent
    And no resubscribe is scheduled

  @E
  Scenario Outline: Ending a subscription during the resubscribe backoff cancels the retry
    Given a subscription whose subscribe attempt failed with a temporary error
    When <action> before the resubscribe delay elapses
    Then the subscription emits "unsubscribed" with code 0
    And no subscribe command is sent after the resubscribe delay

    Examples:
      | action                                        |
      | unsubscribe is called on the subscription     |
      | the subscription is removed from the client   |

  @E
  Scenario Outline: Ending a subscription during the refresh retry delay cancels the refresh
    Given a subscribed subscription whose token refresh failed with a temporary error
    When <action> before the refresh retry delay elapses
    Then the subscription emits "unsubscribed" with code 0
    And neither the token callback nor a sub_refresh runs after the refresh retry delay

    Examples:
      | action                                        |
      | unsubscribe is called on the subscription     |
      | the subscription is removed from the client   |

  @E
  Scenario: A refresh reply without expiry stops refreshing
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with expires false
    Then no further refresh is scheduled

  @D
  Scenario: After a resubscribe-advised unsubscribe the subscription is subscribed again
    Given a client subscribed to channel "news" on the fake server
    When the fake server unsubscribes the subscription with code 2500
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed" after the fake server answers the new subscribe command

  @E
  Scenario: A permanent sub_refresh error unsubscribes
    Given a subscribed subscription whose token expires
    When the server answers its sub_refresh with a permanent error
    Then the subscription emits "unsubscribed" with that error's code and message
    And no further refresh is scheduled

  @E
  Scenario: An expiring fixed subscription token without a callback unsubscribes
    Given a subscribed subscription with a fixed token that expires and no token callback
    When the subscription refresh is due
    Then the subscription emits an "error" event for the missing token callback
    And then the subscription emits "unsubscribed"

  @E
  Scenario: A subscribe rejected for an expired token without a callback unsubscribes
    Given a subscription with a fixed token and no token callback
    When the subscription is subscribed on a connected client
    And the server answers its subscribe command with error 109
    Then the subscription emits a configuration "error" event with code 12 and message "subscription token expired but no getToken function set"
    And then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And no resubscribe is scheduled
    And no unsubscribe command is sent

  @E
  Scenario: A subscribe rejected for an expired token fetches a new one after the backoff
    Given a subscription to a private channel whose token callback is set
    When the subscription is subscribed on a connected client
    And the server answers its subscribe command with error 109
    Then the subscription emits an "error" event of kind "subscribe"
    And no unsubscribe command is sent
    And the subscription token callback is called again after the resubscribe delay
    And the new subscribe command carries the new token

  @E
  Scenario: An empty subscription token before the first subscribe unsubscribes
    Given a subscription to a private channel without an initial token whose token callback returns an empty token
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And no subscribe command is sent
    And no resubscribe is scheduled

  @E
  Scenario: An empty subscription token on refresh unsubscribes
    Given a subscribed subscription whose token expires
    When the subscription token callback returns an empty token during refresh
    Then the subscription emits "unsubscribed" with code 1 and reason "unauthorized"
    And an unsubscribe command is sent
    And no further refresh is scheduled
    And no sub_refresh command is sent

  @E
  Scenario Outline: The data callback runs only after a subscription token is obtained
    Given a subscription to a private channel without an initial token, with a data callback whose token callback <outcome>
    When the subscription is subscribed on a connected client
    Then the data callback is not called for that attempt

    Examples:
      | outcome                      |
      | signals "unauthorized"       |
      | fails with a temporary error |
      | returns an empty token       |

  @E
  Scenario: The subscription token is fetched before the data callback runs
    Given a subscription to a private channel without an initial token, with a token callback and a data callback
    When the subscription is subscribed on a connected client
    Then the token callback is called before the data callback
    And the subscribe command carries both the token and the data

  @E
  Scenario: A subscription that has a token calls the data callback without the token callback
    Given a subscription to a private channel with an initial token, a token callback and a data callback
    When the subscription is subscribed on a connected client
    Then the token callback is not called
    And the subscribe command carries the initial token and the data

  @A
  Scenario: Each subscription token callback receives its own channel
    Given a connected client with subscriptions to private channels "a" and "b" using one token callback
    When both subscriptions are subscribed
    Then the token callback was called once for channel "a" and once for channel "b"

  @A
  Scenario: Subscription token denied
    Given a client subscribed to a private channel using a subscription token with expiration
    When the subscription token callback signals "unauthorized" during refresh
    Then the subscription emits "unsubscribed"

  @E
  Scenario: Resubscribe delays are bounded
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When 50 consecutive subscribe attempts fail with a temporary error
    Then every computed delay is at most 20000 ms

  @E
  Scenario: Resubscribe delay upper bound grows exponentially until capped
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When consecutive subscribe attempts fail with a temporary error
    Then the upper bound of the delay for attempt n+1 is greater than for attempt n
    And the upper bound stops growing once it reaches 20000 ms

  @E
  Scenario: Resubscribe delays are jittered
    Given a subscription with fixed resubscribe backoff options
    When the resubscribe delay for the same attempt number is computed 1000 times
    Then the computed delays are not all equal

  @E
  Scenario: A successful subscribe resets the resubscribe backoff
    Given a subscription with minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When 10 consecutive subscribe attempts fail with a temporary error
    And the next subscribe attempt succeeds
    And a later subscribe attempt fails with a temporary error
    Then the delay after that failure is at most 1000 ms

  @E
  Scenario: Resubscribe backoff options are configurable
    Given a subscription with minResubscribeDelay 100 ms and maxResubscribeDelay 1000 ms
    When 50 consecutive subscribe attempts fail with a temporary error
    Then every computed delay is at most 1000 ms
