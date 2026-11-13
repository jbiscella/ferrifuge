Feature: Client publish debouncing

  @E
  Scenario: Publishes within the interval are merged and the latest is sent when it ends
    Given a core subscribed to channel "cursors:a" whose subscribe reply advised an interval of 100 ms
    When publishes "a", "b" and "c" are started on the subscription, in that order, without replies
    Then the actions contain one send, of publish "a", and one debounce timer of 100 ms
    And when the debounce timer fires, the actions contain one send, of publish "c", and one new debounce timer of 100 ms
    And when the reply to publish "a" is read, the publish "a" settles Ok and no other
    And when the reply to the trailing send is read, the publishes "b" and "c" settle Ok, in that order, and no other

  @E
  Scenario: A window with nothing to send closes
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms, with one publish sent and its reply read
    When the debounce timer fires
    Then the actions contain no send and no debounce timer

  @E
  Scenario: A window stays open while it keeps sending
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms, with publishes "a" and "b" started
    When the debounce timer fires
    And the publish "c" is started
    And the debounce timer fires again
    And the debounce timer fires again
    Then the sends in order were "a", "b" and "c"
    And the last firing left no debounce timer

  @E
  Scenario: A stale debounce timer is ignored
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms, with a publish in a window
    When the subscription is unsubscribed and the window's timer then fires
    Then the firing produces no action

  @E
  Scenario: An interval longer than the call timeout is clamped
    Given a core configured with a call timeout of 5 s, subscribed to channel "cursors:a" with an advised interval of 60000 ms
    When one publish is started on the subscription
    Then the actions contain a debounce timer of 5 s

  @D
  Scenario: A publish made while subscribing is debounced once subscribed
    Given a client connected to the fake server, whose subscribe replies to channel "cursors:a" advise an interval of 300 ms and are held
    When a subscription to channel "cursors:a" publishes {"n":1}, {"n":2} and {"n":3} while subscribing
    And the fake server answers the subscribe command
    Then the fake server receives one publish, with data {"n":1}
    And it receives exactly one more, with data {"n":3}, no sooner than 200 ms and within 2 s of the first

  @D
  Scenario Outline: Without an interval every publish is sent
    Given a client subscribed to channel "cursors:a" on the fake server, whose subscribe reply has <reply>
    When the subscription publishes {"n":1} and {"n":2} and {"n":3} without waiting
    Then the fake server has received 3 publish commands, in order, within 500 ms

    Examples:
      | reply                  |
      | no publish_debounce    |
      | publish_debounce of 0  |

  @D
  Scenario: A later subscribe reply without an interval turns debouncing off
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 2000 ms
    And a resubscribe of the channel whose reply advises none
    When the subscription publishes {"n":1} and {"n":2} without waiting
    Then the fake server has received 2 publish commands within 1 s

  @D
  Scenario: A superseded publish waits for the send that replaced it
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 300 ms, which holds its publish replies
    When the subscription publishes {"n":1}, {"n":2} and {"n":3} and each publish is awaited in its own task
    Then the fake server receives {"n":1} at once and answers it, and only that publish completes
    And no sooner than 200 ms after the first the fake server receives {"n":3}
    And while the fake server holds its reply, the publishes of {"n":2} and {"n":3} have not completed
    And once the fake server answers it, both complete with success

  @D
  Scenario: A superseded publish fails with the error of the send that replaced it
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 300 ms
    And the fake server refuses publish commands of data {"n":3} with error 103 and message "permission denied"
    When the subscription publishes {"n":1}, {"n":2} and {"n":3}, each publish awaited in its own task
    Then the publishes of {"n":2} and {"n":3} fail with code 103 and message "permission denied"
    And the publish of {"n":1} succeeds

  @D
  Scenario: A publish the server refuses at once keeps its own result
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 300 ms
    And the fake server refuses publish commands of data {"n":1} with error 103
    When the subscription publishes {"n":1} and {"n":2}, each publish awaited in its own task
    Then the publish of {"n":1} fails with code 103
    And the publish of {"n":2} succeeds after the interval

  @D
  Scenario: Client-level publishes are not debounced
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 2000 ms
    When the client calls publish on "cursors:a" with {"n":1} and {"n":2} without waiting
    Then the fake server has received 2 publish commands within 1 s

  @D
  Scenario Outline: Leaving subscribed drops what a window holds
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 300 ms
    And the publish of {"n":1} completed, and the publish of {"n":2} waits in the window
    When <cause> before the interval ends
    Then the publish of {"n":2} fails with code <code>
    And the fake server receives no publish command of data {"n":2} within 500 ms

    Examples:
      | cause                                                    | code |
      | the fake server closes the connection with code 3005     | 17   |
      | the fake server sends an unsubscribe push with code 2500 | 17   |
      | unsubscribe is called on the subscription                | 7    |
      | the fake server sends an unsubscribe push with code 2000 | 7    |
      | disconnect is called                                     | 3    |
      | close is called                                          | 4    |

  @D
  Scenario: A window dropped by a reconnect is not resent
    Given a client subscribed to channel "cursors:a" on the fake server with an advised interval of 300 ms
    And the publish of {"n":1} completed, and the publish of {"n":2} waits in the window
    When the fake server closes the connection with code 3005
    And the client is subscribed again on a new connection
    Then the publish of {"n":2} has failed with code 17
    And the fake server receives no publish command of data {"n":2} within 1 s of the new subscribe

  @E
  Scenario: A trailing send lost with its transport fails the publishes that followed it
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms, with publishes "a" and "b" started
    And the debounce timer fired and the trailing send of "b" was handed to the transport
    When the transport is lost
    Then the publish "b" settles with code 11

  @E
  Scenario: A trailing send waits for room in the write queue
    Given a core with a write queue of 1000 bytes, subscribed to channel "cursors:a" with an interval of 100 ms, its queue filled by an unwritten call
    And the publishes "a" and "b" started, "a" waiting for room
    When the debounce timer fires
    Then the trailing send of "b" waits for room behind "a", in order
    And when the queue drains, "a" is sent then "b"

  @E
  Scenario: A trailing send is held by an open batch
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms and an open batch
    And the publishes "a" and "b" started
    When the debounce timer fires
    Then the actions contain a new debounce timer and no send
    When the batch stops
    Then the frames sent carry "a" then "b", in that order

  @E
  Scenario: Trailing sends of a burst share a frame with automatic batching
    Given a core with automatic batching, subscribed to channel "cursors:a" with an interval of 100 ms, and subscribed to a second channel with the same interval, each with a first publish sent and a later publish in its window
    When both debounce timers fire
    Then no frame is sent before the core is flushed
    When the core is flushed
    Then one frame carries both trailing sends

  @E
  Scenario Outline: Dropping a publish withdraws its data only when no other publish waits for it
    Given a core subscribed to channel "cursors:a" with an interval of 100 ms and publish "a" sent
    And the publishes <waiting> started, waiting in the window
    When the publish <dropped> is abandoned
    And the debounce timer fires
    Then the firing sends <sent>

    Examples:
      | waiting | dropped | sent    |
      | b       | b       | nothing |
      | b and c | c       | c       |
      | b and c | b       | c       |

  @D
  Scenario: A Thread handler's publish waiting in a window is not a would-be deadlock
    Given a client with a Thread handler and a subscription to channel "cursors:a" on the fake server with an advised interval of 300 ms
    And the handler, running on a publication event, has published {"n":1} and it completed
    When the handler awaits a publish of {"n":2}, which waits in the window
    Then the fake server receives the publish of {"n":2} no sooner than 200 ms after the first
    And once the fake server answers it, the handler's publish returns success

  @A
  Scenario: Publishing without Centrifugo PRO sends every publish
    Given a subscription to a channel on a real server configured with client_publish_debounce_interval on its namespace
    When the subscription publishes 3 values in a row without waiting
    Then all 3 publications reach a second client's subscription, in order

  @P
  Scenario: A closed window frees its memory
    Given a core with 10000 subscribed subscriptions, each with an open window holding a 1 KiB value
    When every window closes
    Then live bytes return to their value before the windows opened

  @P
  Scenario: Waking with many open windows is reported
    Given a client whose driver holds 10000 open windows
    When its driver wakes once with no timer due
    Then the wall-clock cost of the wake is reported

  @D
  Scenario: Each map key has its own window
    Given a client with a map subscription to "map:m" on the fake server, whose live reply advises an interval of 300 ms
    When the subscription publishes to keys "a" and "b" twice each without waiting
    Then the fake server receives one publish per key at once
    And no sooner than 200 ms and within 2 s of those it receives one more per key, each the second value, and no other

  @D
  Scenario: A remove cancels the key's pending publish and settles the publishes it replaced
    Given a client with a map subscription to "map:m" on the fake server with an advised interval of 300 ms
    And the publish of {"x":1} to key "k" completed, and the publish of {"x":2} to key "k" waits in the window
    When the subscription removes key "k"
    Then the fake server receives the remove of key "k" at once, and no publish of {"x":2}
    And when it answers the remove with success, the publish of {"x":2} and the remove complete with success
    And a publish of {"x":3} to key "k" is sent at once

  @D
  Scenario: Client-level map writes are not debounced
    Given a client with a map subscription to "map:m" on the fake server with an advised interval of 2000 ms
    When the client calls map_publish on "map:m" with key "k" and {"x":1}, then with {"x":2}, without waiting
    Then the fake server has received 2 map publish commands within 1 s

  @D
  Scenario: A map subscribe reply's interval is honoured
    Given a client with a map subscription to "map:m" on the fake server, whose pages carry no interval and whose live reply advises 300 ms
    When the subscription publishes twice to key "k" without waiting
    Then the fake server receives one publish at once
    And it receives the second no sooner than 200 ms and within 2 s of the first

  @P
  Scenario: Seeded chaos with debounced publishes on the core keeps every invariant
    Given the core runner with debounced subscriptions and publish bursts
    When the pull-request seeds run
    Then no seed violates invariants I1 to I12

  @P
  Scenario Outline: The core runner reports a planted debounce fault
    Given the core runner with the planted fault <fault>
    When the pull-request seeds run
    Then the runner reports a violation of <invariant>, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                 | invariant |
      | a debounce timer the simulated clock fires before its delay             | I12       |
      | a publish in a window whose settle action is dropped before the checker | I6        |

  @P
  Scenario: Seeded chaos with debounced publishes on the tokio client keeps the invariants
    Given the adapter runner with debounced subscriptions and publish bursts
    When the pull-request seeds run
    Then no seed violates invariants I1, I2, I3, I5, I6, I8 or I11 as the adapter checks them, and each reaches quiescence within 10 s
