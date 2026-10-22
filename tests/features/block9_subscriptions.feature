Feature: Client-side subscriptions

  Scenario: New subscription starts unsubscribed
    Given a new client
    When a subscription to channel "news" is created
    Then the subscription state is "unsubscribed"

  @A
  Scenario: Subscribe while connected
    Given a connected client
    When a subscription to channel "news" is created and subscribed
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed"

  @A
  Scenario: Subscribe before connect
    Given a new client
    When a subscription to channel "news" is created and subscribed
    Then the subscription state is "subscribing"
    And the subscription emits "subscribing"
    When connect is called
    Then the subscription reaches state "subscribed"

  @A
  Scenario: Publication delivered
    Given a client subscribed to channel "news"
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

  @A
  Scenario Outline: Publication delivered over each transport
    Given a server with http_stream and sse enabled
    And a client connected over transport "<transport>" and subscribed to channel "news"
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @A
  Scenario: Publication metadata is delivered
    Given a client subscribed to channel "news" with history
    When the server API publishes {"text":"hello"} to channel "news" with tags {"kind":"greeting"}
    Then the subscription receives a publication with tags {"kind":"greeting"}
    And the publication carries the channel's next stream offset

  @A
  Scenario: Resubscribe after reconnect
    Given a client subscribed to channel "news"
    When the server disconnects the client with a reconnect-advised code
    Then the subscription emits "subscribing"
    And then the subscription emits "subscribed"

  @A
  Scenario: Unsubscribe stops publications
    Given a client subscribed to channel "news"
    When unsubscribe is called on the subscription
    Then the subscription emits "unsubscribed" with code 0
    And a publication to channel "news" is not received

  Scenario: Duplicate subscription is refused
    Given a new client with a subscription to channel "news"
    When a second subscription to channel "news" is created
    Then creating it fails

  @B
  Scenario: Permanent subscribe error
    Given the subscribe proxy answers channel "news" with error code 403 and message "news is closed"
    When a subscription to channel "news" is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 403 and reason "news is closed"

  @B
  Scenario: Unsubscribe during an in-flight subscribe over an emulated transport
    Given a server with http_stream enabled and a subscribe proxy that answers after 1 s
    And a client connected over transport "http_stream"
    When a subscription to channel "news" is subscribed
    And unsubscribe is called on the subscription before the subscribe is answered
    Then the subscription emits "unsubscribed" with code 0
    And a publication to channel "news" after the subscribe was answered is not received

  @A
  Scenario: Subscriptions survive an explicit disconnect and connect
    Given a client subscribed to channel "news"
    When disconnect is called
    Then the subscription emits "subscribing"
    When connect is called
    Then the subscription emits "subscribed"
    And a publication to channel "news" is received

  @D
  Scenario: Initial publications in the subscribe reply are delivered
    Given a client connected to the fake server
    When a subscription to channel "news" is subscribed and the fake server answers with 1 initial publication
    Then the subscription emits "subscribed"
    And then the subscription receives that publication

  @D
  Scenario: JSON publication bytes are exposed exactly as received
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes a publication whose data is the non-canonical JSON {"b": 1.0,  "a":2}
    Then the subscription receives a publication whose data is exactly those bytes

  @D
  Scenario: A subscribe without a reply makes the client reconnect
    Given a client connected to the fake server that never answers the first subscribe command
    When a subscription to channel "news" is subscribed
    Then within the configured timeout plus tolerance the client emits "connecting" with code 3
    And the client opens a new connection
    And the subscription reaches state "subscribed" on it

  @B
  Scenario: Data returned with the subscribe reply is delivered
    Given the subscribe proxy answers channel "news" with data {"welcome":true}
    When a subscription to channel "news" is subscribed on a connected client
    Then the subscription emits "subscribed" with data {"welcome":true}

  @B
  Scenario: Subscription data is sent on subscribe and on resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with subscription data {"room":1}
    When the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"room":1} on both subscribe requests

  @B
  Scenario: Subscription data from a callback is renewed on resubscribe
    Given a subscribe proxy recording requests
    And a client subscribed to channel "news" with a data callback returning {"n":1} then {"n":2}
    When the server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the subscribe proxy recorded data {"n":1} then {"n":2}

  @B
  Scenario: Each subscription data callback receives its own channel
    Given a subscribe proxy recording requests
    And a connected client with subscriptions to channels "a" and "b" using one data callback
    When both subscriptions are subscribed
    Then the data callback was called once for channel "a" and once for channel "b"

  @E
  Scenario: A failing subscription data callback is retried
    Given a subscription whose data callback fails once
    When the subscription is subscribed on a connected client
    Then the subscription emits an "error" event
    And the data callback is called again after the resubscribe backoff delay
    And the subscribe command carries the data the second call returned

  @E
  Scenario: A data callback denying access unsubscribes
    Given a subscription whose data callback signals "unauthorized"
    When the subscription is subscribed on a connected client
    Then the subscription emits "unsubscribed" with code 1
    And no subscribe command is sent
    And no resubscribe is scheduled

  Scenario: Subscriptions are found by channel and listed
    Given a new client with subscriptions to channels "news" and "sport"
    Then looking up channel "news" returns its subscription
    And looking up channel "weather" returns none
    And the client lists the subscriptions to "news" and "sport"

  @B
  Scenario: A subscription recreated after removal stays subscribed on an emulated transport
    Given a client connected over transport "http_stream" through a proxy that delays the next unsubscribe emulation request by 1 s
    And a client subscribed to channel "news"
    When the subscription is removed from the client
    And a new subscription to channel "news" is created and subscribed at once
    Then the new subscription reaches state "subscribed" after the delayed unsubscribe is settled
    And a publication to channel "news" is received by the new subscription

  @B
  Scenario: A subscription resubscribed at once stays subscribed on an emulated transport
    Given a client connected over transport "http_stream" through a proxy that delays the next unsubscribe emulation request by 1 s
    And a client subscribed to channel "news"
    When unsubscribe is called on the subscription
    And subscribe is called on the subscription again at once
    Then the subscription reaches state "subscribed" after the delayed unsubscribe is settled
    And a publication to channel "news" made after both requests settled is received

  @D
  Scenario: Messages after a disconnect in the same frame are discarded
    Given a client subscribed to channel "news" on the fake server
    When the fake server sends one frame holding a reconnect-advised disconnect push followed by a publication on channel "news"
    Then the client emits "connecting" with that code
    And the subscription receives no publication from that frame

  @A
  Scenario: Removing a subscription unsubscribes it first
    Given a client subscribed to channel "news"
    When the subscription is removed from the client
    Then the subscription emits "unsubscribed" with code 0
    And looking up channel "news" returns none

  @A
  Scenario: Binary publication delivered over Protobuf
    Given a client configured with the Protobuf protocol subscribed to channel "news"
    When the server API publishes the bytes ff 00 fe to channel "news"
    Then the subscription receives a publication with the bytes ff 00 fe
