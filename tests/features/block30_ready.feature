Feature: Waiting until ready

  @D
  Scenario: Client ready resolves on the state, before the "connected" event is taken
    Given a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    And ready is awaited on the client
    Then ready on the client resolves
    And when the application reads the client's stream, it yields "connecting", "connected"

  Scenario: Client ready on a disconnected client fails at once
    Given a new client
    When ready is awaited on the client
    Then ready on the client fails at once with a "disconnected" error

  Scenario: Disconnect fails a waiting client ready
    Given a client configured with an unreachable endpoint
    When connect is called
    And ready is awaited on the client
    And disconnect is called
    Then ready on the client fails with a "disconnected" error

  @D
  Scenario: Client ready keeps waiting across failed attempts
    Given the fake server answers the first 2 connect commands with a temporary error
    And a client whose stream was taken before connect is called and is not read, configured with the fake server's endpoint
    When connect is called
    And ready is awaited on the client
    Then 1 s later the client's state is "connecting"
    And ready on the client is still waiting
    And when the application reads the client's stream, it yields "connecting", "error", "error", "connected"
    And ready on the client resolves

  @B
  Scenario: A terminal connect failure fails a waiting client ready
    Given the connect proxy answers after 1 s with disconnect code 4500
    And a new client
    When connect is called
    And ready is awaited on the client
    Then the client emits "disconnected" with code 4500
    And ready on the client fails with a "disconnected" error before the configured timeout

  @D
  Scenario: Subscription ready resolves on the state, before the "subscribed" event is taken
    Given a client connected to the fake server
    And a subscription to channel "news" whose stream was taken and is not read
    When subscribe is called on the subscription
    And ready is awaited on the subscription
    Then ready on the subscription resolves
    And when the application reads the subscription's stream, it yields "subscribing", "subscribed"

  Scenario: Subscription ready on an unsubscribed subscription fails at once
    Given a new client
    When a subscription to channel "news" is created
    And ready is awaited on the subscription
    Then ready on the subscription fails at once with a "subscription unsubscribed" error

  @D
  Scenario: A permanent subscribe error fails a waiting subscription ready
    Given a client connected to the fake server that answers subscribe commands after 1 s with a permanent error
    When a subscription to channel "news" is subscribed
    And ready is awaited on the subscription
    Then the subscription emits "unsubscribed"
    And ready on the subscription fails with a "subscription unsubscribed" error before the configured timeout

  @A
  Scenario: Subscription ready made before connect waits for the connection
    Given a new client
    When a subscription to channel "news" is created and subscribed
    And ready is awaited on the subscription
    And connect is called
    Then ready on the subscription resolves

  Scenario: Disconnect fails a waiting subscription ready
    Given a client configured with an unreachable endpoint
    When connect is called
    And a subscription to channel "news" is created and subscribed
    And ready is awaited on the subscription
    And disconnect is called
    Then ready on the subscription fails with a "disconnected" error
    And the subscription state is "subscribing"
