Feature: Bug-pattern catalogue

  Scenario: Every pattern of centrifuge-rust's catalogue has a status
    Given the bug-pattern catalogue in CLAUDE.md and the numbers of centrifuge-rust docs/spec.md section 12 at commit cbeac19
    Then every number from 1 to 84 has exactly one row
    And every row's status is one of covered, gap, not applicable or not reconstructible

  Scenario: Every covered entry and every gap names a scenario or test that exists
    Given the bug-pattern catalogue in CLAUDE.md
    Then every scenario a covered or gap row names is the name of a scenario in CLAUDE.md
    And every function a covered or gap row names exists at that path
    And every not applicable or not reconstructible row gives a reason
    And every gap row names a probe scenario or a finding that is listed

  Scenario: Every fix commit an entry cites exists
    Given the bug-pattern catalogue in CLAUDE.md
    Then every commit an entry cites is an ancestor of HEAD

  Scenario: Every probe records its mutation and where it was seen failing
    Given the probes of the bug-pattern catalogue
    Then each names the mutation it was seen failing on
    And each names the pull request of that record, or is pending

  @P
  Scenario Outline: What arrived during a process suspension beats a deadline that fired meanwhile
    Given a client process connected to the fake server, with <waiting>
    When the client process is suspended, the fake server sends <arrival>, the deadline elapses, and the process resumes
    Then <outcome>
    And this holds for all of 32 repetitions

    Examples:
      | waiting                               | arrival          | outcome                                                     |
      | an rpc in flight and a timeout of 200 ms | the rpc's reply  | the rpc succeeds                                            |
      | a server ping interval of 200 ms      | its next ping    | the client stays connected and no reconnect is counted      |

  @E
  Scenario: A stream decoder releases what a large reply grew
    Given a stream decoder that decoded a reply of 1 MiB
    When it decodes a reply of 64 bytes
    Then the capacity of its buffer is at most 64 KiB

  @D
  Scenario: A websocket that answers with data that is not client protocol advances the fallback at once
    Given a client configured with the fake server's websocket endpoint and an unreachable http_stream endpoint
    And the fake server answering the first connection with one text frame that is not client protocol data, and no connect reply
    When connect is called
    Then the client attempts the http_stream endpoint within 3 s of that frame

  @D
  Scenario: Dropping the last clone of a client inside its handler ends the client
    Given a connected client whose event handler holds its only clone
    When the handler drops that clone on the next publication
    Then the fake server sees the connection closed within 2 s
    And the handler's thread has returned

  @D
  Scenario: Disconnect during a websocket handshake that never finishes ends it at once
    Given a client connecting through the relay, whose upgrade the relay holds
    When disconnect is called
    Then the client is disconnected before disconnect returns
    And the relay's connection closes within 3 s

  @E
  Scenario: A transport the client closed reports no error
    Given a machine that closed its transport on a connect timeout and is opening the next one
    When the closed transport reports a read error as its close
    Then the machine returns no action

  @A
  Scenario Outline: A terminal disconnect push right before the end of an HTTP stream stops the client
    Given a client connected over transport "<transport>"
    When the server API disconnects the client with code 4500
    Then the client goes to state "disconnected" with code 4500 and the reason sent

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @B
  Scenario: An emulation request that never answers ends the transport within the timeout
    Given a client with a timeout of 1 s, connected over transport "http_stream" through a proxy that never answers the next emulation request
    When the client sends a command
    Then within 3 s the client emits "connecting" with code 1
    And it connects again and the next command is answered

  @D
  Scenario: A panic in the client's driver ends the client visibly
    Given a client connected to the fake server, with a panic planted in the driver's next step
    And a call in flight and a waiting ready() on a subscription
    When the driver takes the next input
    Then the client's state is "disconnected" with code 2
    And the call fails, and ready() fails, each within 1 s
    And the client's stream ends

  @E
  Scenario: Command ids skip 0 when they wrap
    Given a machine whose next command id is the largest value a 32-bit id can hold
    When it sends two commands
    Then the ids are the largest value and 1

  @P
  Scenario: Seeded chaos on the core applies no stale answer
    Given the core runner with the chaos configuration and client-side and server-side subscriptions to recoverable channels
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariant I13
    And the run counted late token answers, late data answers, late state answers and frames of an abandoned transport, none of them 0

  @P
  Scenario Outline: The core runner reports a planted continuity fault
    Given the core runner with subscriptions, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                       | invariant |
      | a token answer of a torn-down flow passed on as current by the tap          | I13       |
      | a frame of an abandoned transport passed on as current by the tap           | I13       |
      | a tap rendering a token into a Debug record of an Action                    | I14       |

  @P
  Scenario: Seeded chaos on the core leaks no secret
    Given the core runner with marked tokens and payloads
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariant I14
    And the run counted Debug records rendered, none of them 0

  @P
  Scenario: Seeded chaos with handlers on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a tokio client and the fake server, with an event handler on the client and on every second subscription, a handler that sometimes unsubscribes or calls close from inside
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted handler panics, close calls from a handler and handler take-overs, none of them 0
