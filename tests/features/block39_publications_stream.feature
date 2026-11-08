Feature: Publications-only stream

  @D
  Scenario: The publications stream yields only the subscription's publications
    Given a client connected to the fake server, with a subscription to channel "news" with the join_leave option whose publications stream was taken before subscribe is called
    And the subscription is subscribed
    When the fake server sends a publication at offset 1, a join, a leave, then a publication at offset 2 to "news"
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    And the fake server sends a publication at offset 3 to "news"
    Then the publications stream yields exactly the publications at offsets 1, 2 and 3, in order

  @D
  Scenario: Events the publications stream skips hold the connection until it is read
    Given a client connected to the fake server, subscribed to channel "chat", whose subscription's stream is read
    And a subscription to channel "news" whose publications stream was taken before subscribe is called and is not read
    When the application subscribes to "news"
    And the fake server has answered the subscribe command for "news", then sent a publication to "chat"
    Then 1 s later the stream of "chat" has yielded no publication
    And when the application reads the publications stream of "news" for 100 ms, it yields nothing
    And then the stream of "chat" yields that publication

  @D
  Scenario: Publications the publications stream has not yielded are recovered on the next connect
    Given a client subscribed to recoverable channel "news" on the fake server, whose publications stream was taken before subscribe is called
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the publications stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When disconnect is called
    And connect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the publications stream yields the publications at offsets 2 and 3, in order
    And no publication is yielded twice

  @D
  Scenario: publications() takes over the subscription's events stream
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When the application calls the subscription's publications()
    Then the first stream's next item is its end
    And the publications stream yields the publications at offsets 1 and 2

  @D
  Scenario: events() takes over the subscription's publications stream
    Given a client connected to the fake server, with a subscription to channel "news" whose publications stream was taken before subscribe is called and is not read
    And the subscription is subscribed
    And the fake server has sent publications at offsets 1 and 2 to "news"
    When the application calls the subscription's events()
    Then the publications stream's next item is its end
    And the new stream yields "subscribing", "subscribed", and the publications at offsets 1 and 2, in order

  @D
  Scenario: The publications stream outlives unsubscribe, which withdraws what it had not yielded
    Given a client subscribed to channel "news" on the fake server, whose publications stream was taken once it was subscribed and is not read
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When unsubscribe is called on the subscription
    And subscribe is called on the subscription
    And the fake server answers the subscribe command, then sends a publication at offset 3 to "news"
    Then the publications stream yields the publication at offset 3, and no other

  @D
  Scenario Outline: The publications stream ends with its subscription or its client
    Given a client connected to the fake server, with a subscription to channel "news" whose publications stream was taken
    When <cause>
    Then the publications stream's next item is its end

    Examples:
      | cause                       |
      | the subscription is removed |
      | the client is closed        |
      | the client is dropped       |
