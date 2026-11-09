Feature: TLA+ trace replay

  @P
  Scenario: Every model trace replays into the core up to its recorded divergence
    Given the 663 traces of the 18 model configurations copied from centrifuge-rust
    When each trace is replayed step by step into the sans-IO core
    Then 577 traces match the model at every step
    And each of the other 86 matches the model up to the first recorded divergence it reaches, and differs there as recorded
    And the recorded divergences DV1 to DV4 match 70, 3, 16 and 5 traces
    And the run counted the steps replayed of every act but Sdk_Publication and Sdk_SSDeliver, and the values compared of every kind, none of them 0

  @P
  Scenario Outline: The replayer reports a planted mismatch
    Given a copy of a model trace held by the replayer, with <fault>
    When it is replayed
    Then the replay fails naming the configuration, the trace's line, the step, its act and <report>

    Examples:
      | fault                                                         | report                                      |
      | the channel of one expected subscribe command changed         | the command's channel with both values      |
      | the expected state of the client changed at one step          | the client's state with both values         |
      | the expected state of a subscription changed at one step      | the subscription's state with both values   |
      | one expected token callback removed                           | the callbacks started with both values      |
      | an expected resubscribe timer marked armed                    | the timers with both values                 |
      | the expected application offset raised by one                 | the application's position with both values |
      | one expected unsubscribe command removed                      | the commands sent with both values          |
      | one step's act renamed to one without a mapping               | the act without a mapping                   |
      | a step of a trace free of recorded divergences marked as DV2  | DV2 not found at that step                  |

  @P
  Scenario: Every counterexample of a bug variant diverges where recorded
    Given the 7 counterexample traces of bugs.jsonl
    When each is replayed step by step into the sans-IO core
    Then each matches the model up to the step recorded for it and differs there as recorded
    And the 3 counterexamples over http_stream differ at the step where their bug departs from the model
