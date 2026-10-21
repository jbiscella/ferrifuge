Feature: Reconnect backoff

  @E
  Scenario: Delays never exceed the maximum
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When 50 consecutive reconnect attempts fail
    Then every computed delay is at most 20000 ms

  @E
  Scenario: Delay upper bound grows exponentially until capped
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When consecutive reconnect attempts fail
    Then the upper bound of the delay for attempt n+1 is greater than for attempt n
    And the upper bound stops growing once it reaches 20000 ms

  @E
  Scenario Outline: Delay upper bound per attempt
    Given a client with minReconnectDelay 500 ms and maxReconnectDelay 20000 ms
    When the delay for attempt <attempt> is computed 1000 times
    Then every computed delay is at most <bound> ms

    Examples:
      | attempt | bound |
      | 0       | 1000  |
      | 1       | 1500  |
      | 2       | 2500  |
      | 3       | 4500  |
      | 4       | 8500  |
      | 5       | 16500 |
      | 6       | 20000 |

  @E
  Scenario: Delays are jittered
    Given a client with fixed backoff options
    When the delay for the same attempt number is computed 1000 times
    Then the computed delays are not all equal

  @E
  Scenario: Backoff options are configurable
    Given a client with minReconnectDelay 100 ms and maxReconnectDelay 1000 ms
    When 50 consecutive reconnect attempts fail
    Then every computed delay is at most 1000 ms
