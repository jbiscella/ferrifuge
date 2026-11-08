Feature: Event handlers

  @D
  Scenario: Dropping one clone of a client leaves it running
    Given a client connected to the fake server
    And a clone of the client
    When the client is dropped
    Then 1 s later the fake server still sees the connection open
    And the clone's state is "connected"
    And when the clone is dropped, the fake server sees the connection closed within the configured timeout plus tolerance

  Scenario: A weak handle upgrades only while a clone of the client is alive
    Given a new client whose stream was taken
    And a weak handle downgraded from it
    And a clone of the client
    When the client is dropped
    Then upgrading the weak handle gives a client that is not closed
    And when the clone is dropped, upgrading the weak handle gives nothing
    And the client's stream ends

  Scenario: close on one clone closes every clone
    Given a new client
    And a clone of the client
    When close is awaited on the clone
    Then the client is closed
    And an RPC call fails at once with a "client closed" error

  @D
  Scenario Outline: Handlers receive the client's and its subscriptions' events one at a time, in order
    Given a client with <mode> delivery whose client handler takes 200 ms on "connected", with a subscription to channel "news" that has a subscription handler
    When subscribe is called on the subscription
    And connect is called
    And the fake server answers the connect and subscribe commands, then sends a publication at offset 1 to "news"
    Then the handlers have received "subscribing", "connecting", "connected", "subscribed" and the publication at offset 1, in this order
    And no two handler calls overlapped

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |

  @D
  Scenario Outline: A publication a handler had not returned from before disconnect is taken once
    Given a client with <mode> delivery, subscribed to recoverable channel "news" on the fake server, whose subscription handler blocks on the publication at offset 2 until released
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the subscription handler has returned from the publication at offset 1 and is running for the one at offset 2
    When disconnect is called
    And connect is called
    And the subscription handler is released
    Then the next subscribe command for "news" asks to recover from offset 2
    And when the fake server answers it with the publication at offset 3 recovered, the subscription handler receives it
    And the subscription handler has received each publication exactly once

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |

  @D
  Scenario Outline: A panicking handler disconnects the client with code 2, and its publication is recovered
    Given a client with <mode> delivery, subscribed to recoverable channel "news" on the fake server, whose subscription handler panics with "boom" on the first publication at offset 2 it receives
    When the fake server sends publications at offsets 1, 2 and 3 to "news"
    Then the client emits "disconnected" with code 2 and reason "exception in event handler: boom"
    And the subscription handler then receives "subscribing"
    And 1 s later the client has opened no new connection
    And when connect is called and the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered, that subscribe command asked to recover from offset 1
    And the subscription handler receives the publications at offsets 2 and 3, in order

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |

  @D
  Scenario Outline: events() takes over a subscription's handler
    Given a client with <mode> delivery, subscribed to channel "news" on the fake server, whose subscription handler blocks on the publication at offset 1 until released
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the subscription handler is running for the publication at offset 1
    When the application calls the subscription's events()
    And the subscription handler is released
    Then the new stream yields the publications at offsets 2 and 3
    And the subscription handler receives nothing more

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |

  @D
  Scenario: A handler takes over the subscription's stream
    Given a client subscribed to channel "news" on the fake server, whose subscription's stream has been read up to "subscribed" and is not read after
    And the fake server has sent publications at offsets 1 and 2 to "news"
    And the client holds them not yet taken
    When the application sets a subscription handler
    Then the first stream's next item is its end
    And the subscription handler receives the publications at offsets 1 and 2

  @D
  Scenario Outline: Pings are answered while a handler runs, except under Inline
    Given a client with <mode> delivery connected to the fake server, which pings every 1 s and expects pongs, subscribed to channel "news" whose subscription handler blocks for 5 s on each publication
    And the fake server has sent a publication to "news"
    And the subscription handler is running for it
    When 4 s elapse with the fake server pinging every 1 s
    Then the client has answered <answered> of those pings

    Examples:
      | mode | answered |
      | Task | every    |

    @inline
    Examples:
      | mode   | answered |
      | Inline | none     |

    @thread
    Examples:
      | mode   | answered |
      | Thread | every    |

  @D
  Scenario Outline: A call a Task or Inline handler blocks on fails at once
    Given a client with <mode> delivery whose client handler blocks on an RPC call of the client when it receives "connected"
    When connect is called
    And the fake server answers the connect command
    Then the handler's call fails at once with a "would deadlock" error

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

  @D
  Scenario: A Thread handler may block on a call that completes
    Given a client with Thread delivery whose client handler blocks on an RPC call of the client when it receives "connected"
    When connect is called
    And the fake server answers the connect command, then the RPC command
    Then the handler's call succeeds

  @D
  Scenario: A Thread handler's call stuck behind a full read-ahead fails with "would deadlock"
    Given a client with Thread delivery, subscribed to channel "news" on the fake server, whose subscription handler blocks on an RPC call of the client when it receives the publication at offset 1
    When the fake server sends 300 publications of 100 bytes to "news", then answers the RPC command
    Then the client stops reading with 256 messages in its read-ahead
    And the handler's call fails with a "would deadlock" error before the configured timeout
    And the subscription handler then receives the other 299 publications in order

  @D
  Scenario: A Thread handler blocking on a subscription's ready() its own event holds back fails at once
    Given a client with Thread delivery and a subscription to channel "news", whose client handler blocks on the subscription's ready() when it receives "connected"
    When subscribe is called on the subscription
    And connect is called
    And the fake server answers the connect and subscribe commands
    Then the handler's ready() fails at once with a "would deadlock" error
    And then the subscription state becomes "subscribed"

  @D
  Scenario: A Thread handler may block on a ready() its event does not hold back
    Given a client with Thread delivery whose client handler blocks on the client's ready() when it receives "connecting"
    When connect is called
    And the fake server answers the connect command
    Then the handler's ready() resolves

  @D
  Scenario: A Thread handler may block on another client's call
    Given two clients with Thread delivery connected to the fake server, the first's client handler blocking on an RPC call of the second when it receives an asynchronous message
    When the fake server sends an asynchronous message to the first client, then answers the second client's RPC command
    Then the handler's call succeeds

  @D
  Scenario Outline: close awaited inside a handler returns
    Given a client with <mode> delivery connected to the fake server, whose client handler blocks on close() when it receives "connected"
    Then the handler's close() returns within the configured timeout plus tolerance
    And the client handler then receives "disconnected" with code 0
    And the client is closed

    Examples:
      | mode |
      | Task |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |
