Feature: Numbered timer settings

  @E
  Scenario Outline: A firing of a replaced setting changes nothing
    Given a machine whose <timer> was set and then set again
    When the firing of the first setting is fed
    Then the step returns no action
    When the firing of the second setting is fed
    Then the machine acts on it

    Examples:
      | timer                    |
      | no-ping deadline         |
      | connect deadline         |
      | subscribe reply deadline |

  @E
  Scenario: A firing of a cancelled setting changes nothing
    Given a connected machine whose server pings, with a message held unread while the application holds its events
    When the transport's close is read behind it, cancelling the no-ping deadline
    And the firing of the no-ping deadline set before the close is fed
    Then the step returns no action

  @E
  Scenario: A firing of a deadline a sleep's answer replaced changes nothing
    Given a connected machine whose server pings
    When a sleep shorter than the no-ping window is fed, setting the no-ping deadline again
    And the firing of the no-ping deadline set before the sleep is fed
    Then the step returns no action and the client stays connected

  @E
  Scenario: No two timer settings share a number
    Given a machine driven through connects, subscribes, calls, refreshes and reconnects
    Then every timer setting it gave carries a number no other setting carried

  @E
  Scenario Outline: A duration of 0 is rejected
    Given a <builder> whose <duration> is 0
    When it is built
    Then building fails with an error naming <duration>

    Examples:
      | builder              | duration              |
      | client               | timeout               |
      | client               | handshake_timeout     |
      | client               | min_reconnect_delay   |
      | client               | max_reconnect_delay   |
      | subscription builder | min_resubscribe_delay |
      | subscription builder | max_resubscribe_delay |
