Feature: Footprint, performance and fleet behaviour

  @P
  Scenario: Idle footprint with ten subscriptions
    Given a client connected to the fake server with 10 subscribed channels
    When the client is idle
    Then the live heap attributable to the client is at most 1 MiB

  @P
  Scenario: Marginal cost of a subscription
    Given a client connected to the fake server
    When 1000 more channels are subscribed
    Then the live heap grows by at most 2 KiB per subscription

  @P
  Scenario Outline: Allocations per publication do not depend on the number of subscriptions
    Given a client connected to the fake server with <count> subscribed channels
    When 10000 publications of 1 KiB are delivered to one of them
    Then the allocations per publication are reported
    And they differ by less than 1 from the count with 1 subscribed channel

    Examples:
      | count |
      | 1     |
      | 100   |
      | 10000 |

  @P
  Scenario: Dispatch cost does not depend on the number of subscriptions
    Given dispatch benchmarks with 1 and with 10000 subscribed channels
    When one publication is dispatched in each
    Then the instruction count with 10000 channels is at most 1.1 times the count with 1

  @P
  Scenario: Memory stays flat across reconnects
    Given a client connected to the fake server with 10 subscribed channels
    When the fake server forces 10000 reconnect cycles
    Then the live heap after the last cycle is within 1% of the live heap after the first

  @P
  Scenario: An idle client wakes only for pings and refreshes
    Given a client with 10 subscribed channels and an expiring connection token, driven by a controlled clock
    And the server pings every 25 s
    When 10 minutes of simulated time elapse without publications
    Then the only timers set are for ping detection and token refresh
    And no timer fires more often than once per ping interval

  @P
  Scenario: Reconnects of a fleet are spread over the jitter window
    Given 100000 simulated clients with a reconnect jitter window of 100 s, driven by a controlled clock
    When all of them lose their connection at the same instant with a reconnect-advised code
    Then no second of the window holds more than 2% of the first reconnect attempts

  @P
  Scenario: Token refreshes of a fleet are spread
    Given 100000 simulated clients whose connection tokens have the same ttl and were issued at the same instant
    When their refreshes come due
    Then the refreshes span at least 9.9% of the ttl
    And no interval of 1% of the ttl holds more than twice the average share

  @E
  Scenario: Without a jitter window the first reconnect follows the backoff
    Given a client with a reconnect jitter window of 0
    When the connection is lost with a reconnect-advised code
    Then the first reconnect delay is within the Block 2 bound for attempt 0

  @E
  Scenario: The jitter window does not delay the first connect
    Given a client with a reconnect jitter window of 100 s
    When connect is called
    Then the first connect attempt is made without the jitter delay

  @E
  Scenario: The jitter window does not delay a connect after an explicit disconnect
    Given a connected client with a reconnect jitter window of 100 s
    When disconnect is called
    And connect is called
    Then the connect attempt is made without the jitter delay

  @P
  Scenario: Wall-clock cost per publication is reported
    Given a client connected to the fake server with 10 subscribed channels
    When publications of 1 KiB are delivered at 1000 per second
    Then decode and dispatch time per publication and p99 delivery latency are reported

  @P
  Scenario: Idle CPU is reported
    Given a client connected to the fake server with 10 subscribed channels and server pings every 25 s
    When 2 minutes elapse without publications
    Then the CPU time used by the client is reported
