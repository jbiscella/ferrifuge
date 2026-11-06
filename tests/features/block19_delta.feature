Feature: Fossil delta compression

  @A
  Scenario: Delta compression is negotiated with the server
    Given a connected client
    When a subscription with the fossil delta option to a channel allowing fossil delta is subscribed
    Then the subscription emits "subscribed" reporting delta true

  @A
  Scenario Outline: Publications sent as deltas arrive whole
    Given a client configured with the <protocol> protocol, connected through a relay counting bytes, subscribed with the fossil delta option to a channel with history allowing fossil delta and delta publishing
    And the subscription emitted "subscribed" reporting delta true
    When the server API publishes, as deltas, 3 payloads of 4 KiB that differ in a few bytes to the channel
    Then the subscription receives exactly those 3 payloads in order
    And the client read fewer than 2 KiB for the last 2 publications

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: A delta publication is applied to the previous publication
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a whole publication and then a delta publication against it
    Then the subscription receives the whole payload and then the payload the delta builds

  @D
  Scenario: The delta option is sent on subscribe and on resubscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the fossil delta option is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each asking for fossil delta

  @D
  Scenario: With JSON, publications of a delta subscription arrive as their JSON text
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a whole publication whose data is the JSON string of the non-canonical JSON {"b": 1.0,  "a":"é<"}, its é and < sent as \u escapes
    And then a delta publication against it, as a JSON string
    Then the subscription receives exactly the bytes {"b": 1.0,  "a":"é<"} and then the bytes the delta builds

  @D
  Scenario Outline: A broken delta makes the subscription resubscribe
    Given a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, which turned delta on, whose subscription's stream has taken the whole publication at offset 1
    When the fake server pushes, at offset 2, a delta <fault>
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the fake server receives an unsubscribe command for "news"
    And the next subscribe command for "news" asks to recover from offset 1
    And the subscription receives no publication at offset 2 before that subscribe is answered
    And when the fake server answers it with the publication at offset 2 recovered whole, the subscription receives it exactly once

    Examples:
      | fault                                         |
      | whose checksum is wrong                       |
      | that copies past the end of the previous data |
      | that builds fewer bytes than it declares      |
      | that has no trailer                           |

  @D
  Scenario: A delta with no base makes the subscription resubscribe
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on
    When the fake server pushes a delta publication before any whole publication
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the subscription receives no publication

  @D
  Scenario Outline: A delta on a subscription without delta on makes it resubscribe
    Given a client subscribed to channel "news" on the fake server <how>
    When the fake server pushes a delta publication
    Then the subscription emits an "error" event of kind "delta" with code 15
    And then the subscription emits "subscribing" with code 2
    And the subscription receives no publication

    Examples:
      | how                                                         |
      | with the fossil delta option, which did not turn delta on   |
      | without the delta option                                    |

  @D
  Scenario: Each subscribe starts a new delta chain
    Given a client subscribed with the fossil delta option to channel "news" on the fake server, which turned delta on, and has received a whole publication
    When the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again, the fake server turning delta on
    And the fake server pushes a delta publication against the publication received before the disconnect
    Then the subscription emits an "error" event of kind "delta" with code 15

  @D
  Scenario: A broken delta among recovered publications makes the subscription resubscribe from the same position
    Given a client subscribed with the fossil delta option to recoverable channel "news" on the fake server, whose subscription's stream has taken the publication at offset 1
    When the fake server disconnects the client with a reconnect-advised code
    And answers the next subscribe with delta on and the publications at offsets 2 and 3 recovered, the one at 3 a delta whose checksum is wrong
    Then the subscription's stream yields "subscribing", "subscribed", an "error" of kind "delta", and "subscribing" with code 2
    And the next subscribe command for "news" asks to recover from offset 1

  @E
  Scenario: Resubscribe delays keep growing while every subscribe is followed by a broken delta
    Given a subscription with the fossil delta option, min_resubscribe_delay 500 ms and max_resubscribe_delay 20000 ms
    When each of 8 subscribes is answered with delta on, a whole publication and then a broken delta
    Then the upper bound of each resubscribe delay is greater than the previous one until it reaches 20000 ms

  @D
  Scenario: A delta on a server-side subscription makes the client reconnect
    Given the fake server grants a recoverable server-side subscription to channel "alerts" with a stream position at connect
    And a client connected to the fake server that has taken the server-side publication at offset 1
    When the fake server pushes a delta publication at offset 2 on channel "alerts"
    Then the client emits an "error" event of kind "delta" with code 15
    And then the client emits "connecting" with code 5
    And no server-side publication at offset 2 is yielded
    And the next connect command asks to recover "alerts" from offset 1

  @P
  Scenario: A delta declaring more than the inbound message limit fails before allocating
    Given a subscription with the fossil delta option, subscribed with delta on, that has received a whole publication
    When a delta publication of 20 bytes declaring a target one byte larger than the inbound message limit arrives
    Then the subscription emits an "error" event of kind "delta" with code 15
    And the live bytes counted while it is handled stay below 1 MiB

  @A
  Scenario: Publications missed by a delta subscription are recovered whole
    Given a client subscribed with the fossil delta option to a channel with history and recovery allowing fossil delta and delta publishing
    And the subscription emitted "subscribed" reporting delta true
    And the subscription has received 1 live publication
    And the client is disconnected with a reconnect-advised code
    When 2 publications that differ in a few bytes from it are made to the channel, as deltas, while the client reconnects
    Then the subscription emits "subscribed" with was_recovering true and recovered true, reporting delta true
    And the subscription receives exactly those 2 publications in order

  @P
  Scenario: Seeded chaos with delta subscriptions keeps every invariant
    Given the core runner with the chaos configuration, client-side subscriptions with delta to recoverable channels and a fault corrupting deltas
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I11
    And the run counted delta publications, corrupted deltas and the resubscribes they caused, none of them 0

  @P
  Scenario: Seeded chaos with delta subscriptions on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a tokio client, the fake server, subscriptions with delta and a fault corrupting deltas
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6, I8 or I11 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted corrupted deltas and the resubscribes they caused, none of them 0

  @P
  Scenario: The core runner reports a planted payload fault
    Given the core runner with delta subscriptions, with the tap flipping one byte of a delivered payload
    When seeds 0 to 299 run
    Then the runner reports a violation of I11, naming the first seed that shows it and the operations up to it
