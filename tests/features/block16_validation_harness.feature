Feature: Validation harness

  @P
  Scenario: Seeded chaos on the core keeps the connection invariants
    Given the core runner with the chaos configuration and no subscriptions
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I7
    And the run counted every operation and server behaviour, reconnects and call timeouts, none of them 0

  @P
  Scenario: A seed of the core runner reproduces its run
    Given seed 7 of the core runner
    When it runs twice in one process
    Then both traces are equal, each holding 60 operations and every input and action of the machine
    And seed 8 gives a different list of operations

  @P
  Scenario Outline: The core runner reports a planted connection fault
    Given the core runner without subscriptions, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it
    And rerunning that seed alone with FERRIFUGE_CHAOS_SEED reports the same violation at the same operation

    Examples:
      | fault                                                                     | invariant |
      | a state event of the client dropped before the checker                    | I1        |
      | a transport the client closed kept open by the simulated network          | I3        |
      | a simulated server that ignores connect commands from healing on          | I4        |
      | a timer the client cancelled after disconnect kept by the simulated clock | I5        |
      | a call outcome dropped before the checker                                 | I6        |

  @P
  Scenario: Seeded chaos with subscriptions on the core keeps every invariant
    Given the core runner with the chaos configuration and client-side and server-side subscriptions to recoverable channels
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I9
    And the run counted every operation and server behaviour, resubscribes, recoveries with recovered true and with recovered false, full read-aheads and withdrawals, none of them 0

  @P
  Scenario Outline: The core runner reports a planted delivery fault
    Given the core runner with subscriptions, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                           | invariant |
      | a subscribe reply that recovers with an offset skipped                          | I8        |
      | a connect reply that recovers a server-side subscription with an offset skipped | I8        |
      | a publication duplicated before the checker                                     | I8        |
      | a state event of a subscription dropped before the checker                      | I1        |
      | wants_frames() reported true to the checker while the read-ahead is full        | I9        |

  @P
  Scenario: Seeded chaos on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a tokio client and the fake server
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted reconnects, recoveries with recovered true, second events() calls and cancelled reads, none of them 0

  @P
  Scenario: The adapter runner reports a planted fault
    Given the adapter runner with a fake server whose recovery skips an offset
    When seeds 0 to 19 run
    Then the runner reports a violation of I8, naming the seed and the operations up to it
