Feature: Automatic command batching, optimistic subscriptions

  @D
  Scenario: A burst of calls goes out in one frame
    Given a client on a current-thread runtime with a timeout of 10 s, connected to the fake server, which sends no pings
    When the application makes RPC "a", publishes {"n":1} to channel "news" and requests the history of channel "news", without yielding between the calls
    Then the fake server receives one frame carrying the rpc, publish and history commands, in that order
    And each call returns the fake server's reply
    When the application then calls RPC "d" and awaits it
    Then the call returns the fake server's reply within 1 s, its command alone in its frame

  @D
  Scenario: A burst of subscribes goes out in one frame
    Given a client on a current-thread runtime, connected to the fake server, with three subscriptions not subscribed
    When the application calls subscribe on the three subscriptions, without yielding between the calls
    Then the fake server receives one frame carrying the three subscribe commands, in the order of the calls
    And the three subscriptions reach state "subscribed"

  @A
  Scenario: A burst of publishes reaches Centrifugo in one frame
    Given a Centrifugo server of its own with Prometheus metrics, and a client on a current-thread runtime subscribed to channel "news" on it
    When the application publishes {"n":1}, {"n":2} and {"n":3} to channel "news", without yielding between the calls
    Then each publish succeeds
    And the subscription receives the publications {"n":1}, {"n":2} and {"n":3}, in that order
    And the server's count of websocket frames received grew by exactly 1 during the publishes

  @D
  Scenario: Calls a new connection releases go out together
    Given a client configured with the fake server's endpoint, not connected
    When the application calls RPC "a", RPC "b" and RPC "c"
    And connect is called
    Then after the connect reply the fake server receives one frame carrying the three rpc commands, in the order of the calls
    And each call returns the fake server's reply

  @E
  Scenario Outline: The core batches automatically only when its owner enables it
    Given a client core <batching>, connected
    When the application calls RPC "a", then RPC "b"
    Then <before>
    When the owner feeds a flush
    Then <after>

    Examples:
      | batching                    | before                                                                                         | after                                              |
      | with automatic batching     | the core has sent no frame                                                                     | the core sends one frame carrying RPC "a" and RPC "b" |
      | without automatic batching  | the core sent a frame carrying RPC "a" among the actions of the first call, and one carrying RPC "b" among those of the second | the flush produces no action                       |

  @E
  Scenario: Automatic batching keeps the order of commands, send and pong included
    Given a client core with automatic batching, connected over websocket, whose connect reply asked for pongs
    When the core reads a server ping
    And the application calls RPC "a", then send with {"n":1}, then publishes {"n":2} to channel "news"
    Then the core has sent no frame
    When the owner feeds a flush
    Then the core sends one frame carrying the pong, the rpc, the send and the publish commands, in that order

  @E
  Scenario Outline: Automatic batching splits at the frame cap
    Given a client core with automatic batching, connected, with a batch frame cap of 1000 bytes
    When the application makes calls whose commands encode to <sizes> bytes and the owner feeds a flush
    Then the core sends frames carrying <frames> calls, in the order the calls were made
    And no frame carrying several commands exceeds 1000 bytes

    Examples:
      | sizes                   | frames  |
      | 300, 300, 300, 300, 300 | 3, 2    |
      | 300, 1500, 300          | 1, 1, 1 |

  @E
  Scenario: Automatic batching admits calls to the write queue as they are made
    Given a client core with automatic batching, connected, with a write queue of 1000 bytes and nothing unwritten
    When the application makes five calls whose commands encode to 300 bytes each and the owner feeds a flush
    Then the core sends one frame carrying the first three calls
    When the owner feeds a flush
    Then the core sends nothing
    When the transport reports that frame written and the owner feeds a flush
    Then the core sends one frame carrying the fourth and fifth calls

  @E
  Scenario: An explicit batch holds its commands across flushes
    Given a client core with automatic batching, connected
    When the application starts a batch and calls RPC "a"
    And the owner feeds a flush
    Then the core has sent no frame
    When the batch is stopped
    Then the core has sent no frame
    When the owner feeds a flush
    Then the core sends one frame carrying RPC "a"

  @E
  Scenario: Commands refused with HTTP 413 are resent alone
    Given a client core with automatic batching, connected over an emulated transport
    When the application calls RPC "a" and RPC "b" and the owner feeds a flush
    Then the core sends one emulation request carrying both rpc commands
    When the server refuses that request with HTTP 413 and the owner feeds a flush
    Then the core sends two emulation requests, carrying RPC "a" and then RPC "b"
    And neither call has failed

  @E
  Scenario: A call abandoned before the flush is never sent
    Given a client core with automatic batching, connected
    When the application calls RPC "a" and RPC "b", then abandons RPC "a"
    And the owner feeds a flush
    Then the core sends one frame carrying RPC "b" and no rpc command for "a"

  @D
  Scenario Outline: Subscribes go with the connect command over websocket
    Given <given>
    When <trigger>
    Then the next frame the fake server receives carries the connect command, then the subscribe commands for "a", "b" and "c", in that order
    And the three subscriptions reach state "subscribed"
    And the fake server received exactly one subscribe command per subscription on that connection

    Examples:
      | given                                                                                                   | trigger                                                              |
      | a client configured with the fake server's endpoint, with subscriptions to "a", "b" and "c" subscribed before connect | connect is called                                                    |
      | a client connected to the fake server, with subscriptions to "a", "b" and "c" subscribed               | the fake server disconnects the client with a reconnect-advised code |

  @E
  Scenario Outline: Subscribes go with the connect command over websocket only
    Given a client core over <transport> with a subscription subscribed before connect
    When connect is called and the transport opens
    Then <first>
    When the server answers the connect command
    Then <after>

    Examples:
      | transport   | first                                                                                 | after                                                              |
      | websocket   | the core sends one frame carrying the connect command, then the subscribe command     | the core sends no subscribe command                                |
      | http_stream | the request opening the stream carries only the connect command, and the core sends nothing | the core sends an emulation request carrying the subscribe command |

  @E
  Scenario Outline: Only subscribes that fit the frame cap go with the connect command
    Given a client core over websocket with a batch frame cap of 1000 bytes, whose connect command encodes to <connect> bytes, and five subscriptions subscribed before connect whose subscribe commands encode to 300 bytes each
    When connect is called and the transport opens
    Then the core sends one frame carrying the connect command and the subscribe commands of the first <with> subscriptions
    When the server answers the connect command
    Then the core sends frames carrying <after> subscribe commands, for the other subscriptions in the order they were created

    Examples:
      | connect | with | after |
      | 90      | 3    | 2     |
      | 1200    | 0    | 3, 2  |

  @D
  Scenario Outline: A subscription that needs a callback subscribes after the connect reply
    Given a client configured with the fake server's endpoint, with subscriptions to "news" and "private" subscribed before connect, "private" with <callback>
    When connect is called
    Then the first frame the fake server receives carries the connect command and the subscribe command for "news", and no subscribe command for "private"
    And the <callback> of "private" is first called after the fake server sent its connect reply
    And the subscribe command for "private" reaches the fake server in a later frame
    And both subscriptions reach state "subscribed"

    Examples:
      | callback                       |
      | a token callback and no token  |
      | a data callback                |
      | a getState callback            |

  @E
  Scenario: An optimistic subscribe's deadline starts at the connect reply
    Given a client core over websocket with a timeout of 5 s and a subscription subscribed before connect
    When connect is called and the transport opens
    Then the core sends one frame carrying the connect command and the subscribe command
    When 4 s elapse and the core reads the connect reply
    And 4 s elapse
    Then the client is "connected" and the subscription is "subscribing"
    When 1 s elapses
    Then the client emits "connecting" with code 3 and reason "subscribe timeout"

  @D
  Scenario: A failed connect loses the optimistic subscribes as a teardown does
    Given the fake server answers the first connect command with a temporary error and drops the rest of its frame, as Centrifugo does
    And a client configured with the fake server's endpoint, with a subscription to "news" subscribed before connect
    When connect is called
    Then the client emits "connecting", then an "error" event, then "connected"
    And the fake server received exactly 2 subscribe commands for "news", each in the frame of a connect command, and no unsubscribe command
    And the subscription emits "subscribed" exactly once and never "unsubscribed"

  @D
  Scenario: Unsubscribing before the connect reply undoes an optimistic subscribe
    Given the fake server delays its connect reply by 300 ms
    And a client configured with the fake server's endpoint, with a subscription to "news" subscribed before connect
    When connect is called
    And the fake server has received the subscribe command for "news" in the frame of the connect command
    And unsubscribe is called on the subscription before the connect reply
    Then after answering the connect and the subscribe commands, the fake server receives an unsubscribe command for "news"
    And the subscription is "unsubscribed" and never emitted "subscribed"

  @E
  Scenario: A subscribe reply before the connect reply breaks the protocol
    Given a client core over websocket with a subscription subscribed before connect
    When connect is called and the transport opens
    And the core reads one frame holding the reply to the subscribe command, then the connect reply
    Then the core reports a reply-before-connect anomaly naming the subscribe command
    And the client emits "connecting" with code 1 and reason "transport closed"
    And the subscription emits no "subscribed" for that transport

  @A
  Scenario: Optimistic subscriptions on Centrifugo
    Given a Centrifugo server of its own with Prometheus metrics
    And a client with subscriptions to channels "news" and "chat" subscribed before connect
    When connect is called
    Then both subscriptions reach state "subscribed"
    And the server counted exactly 1 websocket frame received
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription to "news" receives a publication with data {"text":"hello"}

  @P
  Scenario: Seeded chaos with automatic batching and optimistic subscriptions on the core keeps every invariant
    Given the core runner with the chaos configuration, a batch frame cap of 1 KiB, client-side and server-side subscriptions to recoverable channels, batching operations, and automatic batching on even seeds
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I10
    And the run counted flushes sending several commands, connect frames carrying subscribe commands and optimistic subscribes lost to a failed connect, none of them 0

  @P
  Scenario Outline: The core runner reports a planted automatic-batching fault
    Given the core runner with automatic batching, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of I10, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                                    |
      | a frame sent by a flush reported to the checker as sent by the input before it           |
      | the first subscribe command of a connect frame reported to the checker before the connect command |

  @P
  Scenario: Seeded chaos on the tokio client batches automatically and keeps the invariants
    Given the adapter runner with the chaos configuration, a batch frame cap of 1 KiB, a tokio client, the fake server and batching operations
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the fake server received frames of several commands that no batch held, and connect frames carrying subscribe commands, none of them 0
