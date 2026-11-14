Feature: Real-server chaos

  @P
  Scenario Outline: Seeded real-server chaos keeps the invariants
    Given the real-server runner with a Centrifugo container, engine <engine>, history and recovery, Toxiproxy between client and server, and the chaos configuration
    When the seeds of the budget each run random operations over transport "<transport>" with the <protocol> protocol, with network faults, server pause and restart, client suspension and short-lived tokens
    Then no seed confirms a violation of I1, I2, I3, I5 or I6, and each reaches quiescence within 30 s plus its suspended time
    And for each recoverable channel the offsets delivered are the channel's history offsets from the segment's start, in order, each once, only a restart on the memory engine leaving the tail of its segment undelivered
    And a "subscribed" with was_recovering true and recovered false occurs only after a server restart on the memory engine
    And at each quiescent point the channels API lists at most one client on the canary channel and exactly the channels the client subscribed to
    And the run ended with close and the channels API listing none of the run's channels
    And the run counted every fault kind, none of them 0

    Examples:
      | transport   | protocol | engine |
      | websocket   | JSON     | memory |
      | websocket   | Protobuf | memory |
      | http_stream | JSON     | memory |
      | http_stream | Protobuf | memory |
      | sse         | JSON     | memory |
      | websocket   | JSON     | redis  |

  @P
  Scenario: A server restart with the memory engine reports recovered false
    Given the real-server runner with the memory engine and a client subscribed to recoverable channel "news", which received publications 1 to 3
    When the Centrifugo container restarts
    Then the subscription emits "subscribed" with was_recovering true, recovered false and an epoch different from the one before the restart
    And the next publication is delivered with the offset the server gave it, offsets restarting after that "subscribed"
    And the run counted exactly one restart

  @P
  Scenario: A server restart with the Redis engine loses nothing
    Given the real-server runner with the Redis engine and a client subscribed to recoverable channel "news", which received publications 1 to 3
    When the proxy is turned down, the Centrifugo container restarts, publication 4 is published through the server API, and the proxy is turned up
    Then the subscription emits "subscribed" with recovered true
    And the epoch is unchanged
    And the offsets delivered are 1 to 4, each once

  @P
  Scenario: A failing real-server seed is a finding only when it fails twice
    Given a real-server chaos seed that violated an invariant
    When it is rerun with the same seed
    Then it fails the run only if it violates the same invariant again, naming the seed and the operations up to it
    And otherwise it is counted as unconfirmed and fails nothing

  @P
  Scenario: A seed of the real-server runner fixes its operations
    Given seed 7 of the real-server runner
    When its operation list is generated twice
    Then both lists are equal
    And seed 8 gives a different list

  @P
  Scenario Outline: The real-server runner reports a planted fault
    Given the real-server runner with <fault> planted
    When the seeds run
    Then the runner reports a violation of <invariant>, naming the seed and the operations up to it

    Examples:
      | fault                                                  | invariant |
      | a tap dropping one delivered publication before the checker | I8        |
      | a second connection opened by the runner as the run's user  | I3        |

  Scenario Outline: A scheduled validation run is skipped only when main's head already has a real run
    Given the validation-run script and a workflow history where <history>
    When it decides a scheduled run of the head commit
    Then it <decision>

    Examples:
      | history                                                | decision |
      | no real run has concluded                              | runs     |
      | the last real run concluded on another commit          | runs     |
      | the last real run concluded on the head commit, passed | skips    |
      | the last real run concluded on the head commit, failed | skips    |
      | the head commit has only cancelled runs                | runs     |

  Scenario Outline: A release needs a passing real run of its commit
    Given the validation-run script and the runs of the tagged commit, where <history>
    When it checks the release gate for the commit
    Then it <outcome>

    Examples:
      | history                                         | outcome                                                  |
      | the latest real run passed                      | succeeds                                                 |
      | the latest real run failed, a skipped run after | fails, naming the workflow and the commit                |
      | the latest real run is still in progress        | fails, naming the workflow and the commit                |
      | only a skipped run exists                       | fails, naming the workflow and saying to dispatch it     |
      | no run exists                                   | fails, naming the workflow and saying to dispatch it     |
