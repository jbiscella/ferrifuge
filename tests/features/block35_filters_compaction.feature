Feature: Tag filters, channel compaction, publication channel, recovered publications

  @A
  Scenario Outline: A tag filter delivers only matching publications
    Given a client configured with the <protocol> protocol, subscribed to a channel allowing tag filters with the filter ticker in BTC, ETH
    When the server API publishes to the channel with tags {"ticker":"BTC"}, then {"ticker":"XRP"}, then {"ticker":"ETH"}
    Then the subscription receives exactly the publications tagged BTC and ETH, in order

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario: The tag filter is sent on subscribe and on resubscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the filter ticker eq BTC is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each carrying the filter ticker eq BTC

  @D
  Scenario: A tag filter set on a subscription is sent from the next subscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with the filter ticker eq BTC is subscribed
    And the tag filter of the subscription is set to ticker eq ETH
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received exactly 2 subscribe commands for "news", the first carrying the filter ticker eq BTC and the second ticker eq ETH

  Scenario: A tag filter with delta is refused
    Given subscription options with the fossil delta option and a tag filter
    When the subscription is built
    Then building fails because a tag filter cannot be combined with delta
    And setting a tag filter on a subscription with the fossil delta option fails the same way

  Scenario Outline: A tag filter the server would reject is refused
    Given subscription options with the tag filter <filter>
    When the subscription is built
    Then building fails because the tag filter is invalid
    And setting that tag filter on a built subscription fails the same way

    Examples:
      | filter                                      |
      | a set comparison on "ticker" with no value  |
      | an equality with an empty key               |

  @D
  Scenario: Channel compaction is requested on every subscribe
    Given a client connected to the fake server
    When a subscription to channel "news" with channel compaction is subscribed
    And the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again
    Then the fake server received 2 subscribe commands for "news", each requesting channel compaction

  @D
  Scenario Outline: Pushes with a numeric channel id reach their subscription
    Given a client subscribed to channel "news" on the fake server with channel compaction and join and leave pushes, the fake server giving it id 7
    When the fake server pushes a <push> with id 7 and no channel
    Then the subscription receives that <push>

    Examples:
      | push        |
      | publication |
      | join        |
      | leave       |

  @D
  Scenario: A push with an unknown channel id is dropped
    Given a client subscribed to channel "news" on the fake server with channel compaction, the fake server giving it id 7
    When the fake server pushes a publication with id 8 and no channel, then one with id 7 and no channel
    Then the subscription receives exactly one publication, the one pushed with id 7
    And the client receives no server-side publication

  @D
  Scenario: Numeric channel ids are forgotten when the connection ends
    Given a client subscribed to channel "news" on the fake server with channel compaction, the fake server giving it id 7
    When the fake server disconnects the client with a reconnect-advised code
    And the subscription reaches state "subscribed" again, the fake server giving it id 9
    And the fake server pushes a publication with id 7 and no channel, then one with id 9 and no channel
    Then the subscription receives exactly one publication, the one pushed with id 9

  @A
  Scenario: Channel compaction on a server without it changes nothing
    Given a client subscribed to channel "news" with channel compaction
    When the server API publishes {"text":"hello"} to channel "news"
    Then the subscription receives a publication with data {"text":"hello"}

  @D
  Scenario: A publication of a wildcard subscription carries its channel
    Given a client subscribed to channel "news" on the fake server
    When the fake server pushes a publication for channel "news" whose own channel is "news.sport", then one with no own channel
    Then the subscription receives a publication whose channel is "news.sport", then one with no channel

  @D
  Scenario: A publication of another channel does not move the stream position
    Given a client subscribed to recoverable channel "news" on the fake server
    When the fake server pushes a publication for channel "news" at offset 1, then one whose own channel is "news.sport" at offset 5
    And the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "news" asks to recover from offset 1 in the same epoch

  @A
  Scenario Outline: The subscribed event says whether recovered publications follow
    Given a client subscribed to a channel with history and recovery
    When <count> publications are made to the channel while the client reconnects
    Then the subscription emits "subscribed" again with has_recovered_publications <flag>

    Examples:
      | count | flag  |
      | 2     | true  |
      | 0     | false |

  @D
  Scenario: A server-side subscribed event says whether publications follow
    Given the fake server grants a server-side subscription to channel "alerts" with 1 initial publication and recovered false at connect
    When a client connects to the fake server
    Then the client emits a server-side "subscribed" event for channel "alerts" with has_recovered_publications true
