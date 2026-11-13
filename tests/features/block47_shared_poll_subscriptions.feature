Feature: Shared poll subscriptions (unstable)

  Scenario Outline: A shared poll subscription cannot share a channel with another subscription
    Given a new client with a <existing> to channel "poll_v:a"
    When a <created> to channel "poll_v:a" is created
    Then creating it fails with "a subscription to channel poll_v:a already exists"

    Examples:
      | existing                 | created                  |
      | subscription             | shared poll subscription |
      | shared poll subscription | subscription             |
      | map subscription         | shared poll subscription |

  Scenario: A new shared poll subscription starts unsubscribed
    Given a new client
    When a shared poll subscription to channel "poll_v:a" is created
    Then the shared poll subscription state is "unsubscribed"
    And the client's shared poll subscriptions are "poll_v:a" and its subscriptions are none
    And tracked_keys is empty

  @D
  Scenario: Removing a shared poll subscription unsubscribes it and frees its channel
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking key "a"
    When the shared poll subscription is removed
    Then the fake server receives an unsubscribe command for "items"
    And the client's shared poll subscriptions are none
    And a subscription to channel "items" can be created

  @D
  Scenario: A shared poll subscription subscribes with type 4 and nothing else
    Given a client connected to the fake server
    And a shared poll subscription to channel "items" with data {"a":1} and a token
    When subscribe is called on the shared poll subscription
    Then the subscribe command for "items" has type 4, the data {"a":1} and the token
    And it has no recover, epoch, offset, positioned, recoverable, join_leave, flag or tf field

  @D
  Scenario: Tracking with a signature sends one track request
    Given a client connected to the fake server, with a shared poll subscription to "items" that has subscribed
    When track_with_signature is called with keys "a" at version 3 and "b" at version 0 and signature "sig1"
    Then the fake server receives a sub_refresh for "items" with type 1 and one batch with signature "sig1", item "a" at version 3 and item "b" with no version
    And tracked_keys is "a" and "b"

  @D
  Scenario: Keys tracked before subscribing are sent once subscribed
    Given a client connected to the fake server, with a shared poll subscription to "items" with get_signature that signs what it is given with "sig1"
    When track is called with key "a" before subscribe
    Then get_signature has not been called
    And when subscribe is called and the subscribe is answered, get_signature is called with key "a"
    And the fake server receives a sub_refresh for "items" with type 1, signature "sig1" and item "a"

  @D
  Scenario: A key tracked twice is tracked once at the higher version
    Given a client connected to the fake server, with a shared poll subscription to "items" that has subscribed
    When track_with_signature is called with key "a" at version 5 and signature "sig1"
    And track_with_signature is called with key "a" at version 2 and signature "sig2"
    And the transport closes and the client reconnects and subscribes again
    Then the replay's track request carries key "a" at version 5

  @D
  Scenario: An empty key set sends nothing
    Given a client connected to the fake server, with a shared poll subscription to "items" with a get_signature callback that has subscribed
    When track is called with no keys and untrack is called with no keys
    Then the fake server receives no sub_refresh and get_signature is not called within 1 s

  @A
  Scenario: Tracked keys receive their values and each new version
    Given a connected client with a shared poll subscription to "poll_v:t1" whose stream was taken, subscribed
    When track_with_signature is called with key "a" at version 0 and a valid signature for "a"
    And the server API publishes key "a" of "poll_v:t1" at version 2 with data {"v":2}
    And the server API publishes key "a" of "poll_v:t1" at version 4 with data {"v":4}
    Then the shared poll subscription's stream yields "subscribing", "subscribed", an update of "a" with data {"v":2} and version 2, then an update of "a" with data {"v":4} and version 4

  @B
  Scenario: Centrifugo polls the backend and fans the value out
    Given the backend's refresh answers key "a" of "poll:f1" with data {"v":1}
    And a client and another client each with a shared poll subscription to "poll:f1" subscribed, each tracking "a" with a valid signature
    Then each shared poll subscription's stream yields an update of "a" with data {"v":1} and a version greater than 0
    And when the backend's refresh answers key "a" with data {"v":2}, each stream yields an update of "a" with data {"v":2}
    And the backend's refresh requests for "poll:f1" never name "a" more than once per request

  @A
  Scenario Outline: A shared poll subscription works over each transport and protocol
    Given a server with http_stream and sse enabled, and a client with the <protocol> protocol connected over transport "<transport>" with a shared poll subscription to "poll_v:tr1"
    When subscribe is called on the shared poll subscription and key "a" is tracked with a valid signature
    And the server API publishes key "a" of "poll_v:tr1" at version 3 with data {"v":3}
    Then the shared poll subscription's stream yields an update of "a" with data {"v":3} and version 3

    Examples:
      | transport   | protocol |
      | websocket   | JSON     |
      | websocket   | Protobuf |
      | http_stream | JSON     |
      | http_stream | Protobuf |
      | sse         | JSON     |

  @A
  Scenario: An untracked key delivers nothing more
    Given a connected client with a shared poll subscription to "poll_v:u1" that has subscribed, tracking "a" and "b"
    When untrack is called with key "a"
    And the untrack reply is received
    And the server API publishes key "a" at version 3 and key "b" at version 5
    Then the stream yields an update of "b" and no update of "a" within 2 s
    And tracked_keys is "b"

  @D
  Scenario: A version not newer than the one held is dropped
    Given a client connected to the fake server, with a shared poll subscription to "items" tracking "a" at version 5, whose stream was taken
    When the fake server pushes key "a" at version 5 and then key "a" at version 6
    Then the stream yields exactly one update of "a" with version 6

  @D
  Scenario: A publication of a key not tracked is dropped
    Given a client connected to the fake server, with a shared poll subscription to "items" tracking "a", whose stream was taken
    When the fake server pushes key "z" at version 1 and then key "a" at version 1
    Then the stream yields exactly one update, of "a" at version 1

  @B
  Scenario: A backend removal ends the key and gives one update
    Given a client with a shared poll subscription to "poll:rm1" subscribed, tracking "a" with a valid signature, whose update of "a" was taken
    When the backend's refresh answers key "a" with removed true
    Then the stream yields an update of "a" with removed true, empty data and no version
    And tracked_keys is empty

  @D
  Scenario: Items of a track reply are delivered before later pushes
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken
    When track_with_signature is called with key "a" and "b" and the fake server answers with items "a" at version 2 and "b" at version 3
    And the fake server pushes "a" at version 4
    Then the stream yields updates of "a" version 2, "b" version 3 and "a" version 4, in that order

  @A
  Scenario: A signature the server refuses untracks its keys and reports the error
    Given a connected client with a shared poll subscription to "poll_v:s1" subscribed, whose stream was taken
    When track_with_signature is called with key "a" and signature "1:0:deadbeef"
    Then the stream yields an error with code 103 of kind Track and an update of "a" with removed true
    And tracked_keys is empty

  @A
  Scenario: A limit the server enforces untracks the keys over it
    Given a connected client with a shared poll subscription to "poll_l:l1" subscribed, whose stream was taken
    When track_with_signature is called with keys "a", "b" and "c" and a valid signature
    Then the stream yields an error with code 106 and updates of "a", "b" and "c" with removed true
    And tracked_keys is empty

  @A
  Scenario: A namespace that is not shared poll refuses a shared poll subscription
    Given a connected client with a shared poll subscription to "news:x"
    When subscribe is called on the shared poll subscription
    Then the stream yields "unsubscribed" with code 108

  @D
  Scenario: A server without shared poll ends the subscription
    Given a client connected to the fake server, with a shared poll subscription to "items" whose stream was taken
    When subscribe is called and the fake server answers with a subscribe result of type 0
    Then the fake server receives an unsubscribe command for "items"
    And the stream yields "unsubscribed" with code 8 and reason "shared poll subscriptions not supported"

  @A
  Scenario: A private namespace gets its token and refreshes it
    Given a connected client with a shared poll subscription to "poll_private:p1" with a get_token callback returning tokens with a 2 s lifetime
    When subscribe is called on the shared poll subscription
    Then the subscribe carries the token and the subscription becomes "subscribed"
    And within 4 s get_token has been called at least twice and the subscription stays "subscribed"

  @D
  Scenario: track asks get_signature for the keys and tracks what it returns
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, with get_signature answering keys "a" and "b" with "sig1"
    When track is called with keys "a" and "b"
    Then get_signature is called with keys "a" and "b"
    And the fake server receives a sub_refresh with type 1, signature "sig1" and items "a" and "b"

  @D
  Scenario: A key the callback does not return is revoked
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken, with get_signature answering only key "a" with "sig1"
    When track is called with keys "a" and "b"
    Then the stream yields an update of "b" with removed true
    And the fake server receives a sub_refresh with type 2 and key "b", and one with type 1 and item "a"
    And tracked_keys is "a"

  @D
  Scenario: A key untracked while get_signature runs is untracked in the same request
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, with a get_signature callback that waits for the test and then answers keys "a" and "b" with "sig1"
    When track is called with keys "a" and "b"
    And untrack is called with key "a"
    And the callback answers
    Then the fake server receives a track request with batch items "a" and "b" and an untrack field "a"

  @D
  Scenario: Without get_signature a key set reports error 14
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken, without a get_signature callback
    When track is called with key "a"
    Then the stream yields an error with code 14 of kind Track
    And the fake server receives no sub_refresh

  @D
  Scenario: A failing get_signature reports error 14 and retries
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken, with a get_signature callback that fails once and then answers
    When track is called with key "a"
    Then the stream yields an error with code 14 of kind Track
    And after the 5 s to 30 s backoff the fake server receives a sub_refresh with type 1 and item "a"

  @D
  Scenario: An unauthorized get_signature unsubscribes with code 1
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken, with a get_signature callback answering unauthorized
    When track is called with key "a"
    Then the stream yields "unsubscribed" with code 1 and reason "unauthorized"
    And the fake server receives an unsubscribe command for "items"

  @A
  Scenario: A signature with an expiry is refreshed when it expires
    Given a connected client with a shared poll subscription to "poll_v:e1" subscribed, whose stream was taken, with a get_signature callback signing every key for 3 s
    When track is called with key "a"
    Then get_signature is called for "a" a second time within 4 s
    And once the second signature is accepted, the server API's publish at version 7 yields an update of "a" with version 7

  @A
  Scenario: A track refused with 109 refreshes the signature
    Given a connected client with a shared poll subscription to "poll_v:e2" subscribed, whose stream was taken, with a get_signature callback signing valid signatures
    When track_with_signature is called with key "a" and a signature that expired 10 s ago
    Then the stream yields an error with code 109 of kind Track
    And get_signature is called for "a" and the next track request is accepted

  @D
  Scenario: A track refused with 109 again waits before the next refresh
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, whose stream was taken, with a get_signature callback counting its calls
    And the fake server answers every track request with error 109
    When track is called with key "a"
    Then get_signature is called twice within 1 s
    And get_signature is not called a third time within 4 s

  @D
  Scenario: A signature accepted within the grace period is refreshed soon
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, with a get_signature callback
    When track is called with key "a"
    And the fake server answers with expires true and no ttl
    Then get_signature is called again within 3 s

  @D
  Scenario: An untrack of more keys than one frame fits is split
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed and 1800 keys of 44 bytes each tracked
    When untrack is called with those keys
    Then the fake server receives several sub_refresh frames with type 2, each at most 60000 bytes as sent, together covering all 1800 keys, each once
    And the client stays connected

  @D
  Scenario: A key set larger than one frame is signed and tracked in chunks
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, with a get_signature callback
    When track is called with 1800 keys of 44 bytes each
    Then get_signature is called at least twice and no call covers a key set over the frame budget
    And the fake server receives several sub_refresh frames with type 1, each at most 60000 bytes as sent, together covering all 1800 keys
    And the client stays connected

  @D
  Scenario: A signature batch over the frame budget is refused before it is sent
    Given a client connected to the fake server, with a shared poll subscription to "items" with max_track_frame_bytes 1000, subscribed, whose stream was taken
    When track_with_signature is called with 100 keys of 20 bytes and signature "sig1"
    Then the stream yields an error with code 12 of kind Track and 100 updates with removed true
    And the fake server receives no sub_refresh
    And tracked_keys is empty

  @A
  Scenario: Tracked keys come back after a reconnect with the versions held
    Given a connected client with a shared poll subscription to "poll_v:r1" subscribed, tracking "a" and "b", whose updates of "a" at version 4 and "b" at version 6 were taken
    When the server API disconnects the client with a reconnect-advised code
    And the client reconnects and the shared poll subscription becomes "subscribed"
    Then no update is yielded within 1 s without a new publish
    And the server API's publish of "a" at version 5 yields an update of "a" with version 5

  @D
  Scenario: A replay reuses the signatures it holds
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking "a" with signature "sig1" from get_signature
    When the transport closes and the client reconnects and the subscribe is answered
    Then the replay's track request carries signature "sig1" and item "a"
    And get_signature was called once

  @D
  Scenario: A key untracked while offline is untracked in the replay frame
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking "a" and "b" under signature "sig1"
    When the transport closes and untrack is called with key "a"
    And the client reconnects and the subscribe is answered
    Then the replay's track request carries items "a" and "b" under signature "sig1" and the untrack field "a"

  @D
  Scenario: A replay signs only the keys without a signature
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking "a" under signature "sig1", with a get_signature callback
    When the client is not subscribed and track is called with key "b"
    And the client reconnects and the subscribe is answered
    Then the replay's first track request carries item "a" under "sig1"
    And get_signature is called with key "b" only
    And a second track request carries item "b"

  @D
  Scenario: A changed epoch sends every key from version 0
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed at epoch "e1", tracking "a" at version 9
    When the transport closes and the client reconnects and the subscribe is answered with epoch "e2"
    Then the replay's track request carries item "a" with no version

  @B
  Scenario: An epoch flip resubscribes and tracks again
    Given a client over http_stream with a shared poll subscription to "poll_v:ef1" tracking "a" with a valid signature, whose backend has reported epoch "E1" and whose update of "a" at version 3 was taken
    When the backend's refresh reports epoch "E2" and key "a" at version 1 with data {"v":2}
    Then the stream yields "subscribing" with code 2500, then "subscribed"
    And the next track request carries key "a" with no version
    And the stream yields an update of "a" with data {"v":2}

  @D
  Scenario: A first epoch from the backend sends every key from version 0
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed with no epoch, tracking "a" at version 9
    When the fake server unsubscribes it with code 2500 and answers the next subscribe with epoch "e1"
    Then the replay's track request carries item "a" with no version

  @D
  Scenario: Versions given before the first subscribe are kept whatever its epoch
    Given a client connected to the fake server, with a shared poll subscription to "items"
    And track_with_signature was called with key "a" at version 5 and signature "sig1" before subscribe
    When subscribe is called and the subscribe is answered with epoch "e1"
    Then the replay's track request carries item "a" at version 5

  @D
  Scenario: A replay with held versions stays within the frame budget
    Given a client connected to the fake server, with a shared poll subscription to "items" with get_signature that signs what it is given with "sig1", subscribed, whose stream was taken
    And track is called with 2500 keys of 8 bytes each
    And the fake server has pushed every key at version 9007199254740992 and each update was taken
    When the transport closes and the client reconnects and the subscribe is answered
    Then every sub_refresh frame the fake server receives is at most 60000 bytes as sent
    And the replay's track requests together carry every key at version 9007199254740992
    And the client stays connected

  @D
  Scenario: unsubscribe forgets the keys and the signatures
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking "a" under signature "sig1"
    When unsubscribe is called and the fake server answers it
    And subscribe is called and the subscribe is answered
    Then tracked_keys is empty and the fake server receives no sub_refresh within 1 s

  @D
  Scenario Outline: A failing track is retried or untracked by its error
    Given a client connected to the fake server, with a shared poll subscription to "items" with a get_signature callback, subscribed, tracking "a", whose stream was taken
    When track_with_signature is called with key "b" and the fake server answers it with error <code> and temporary <temporary>
    Then the stream yields an error with code <code>
    And <outcome>

    Examples:
      | code | temporary | outcome                                                                                 |
      | 109  | false     | get_signature is called for the tracked keys                                            |
      | 1    | false     | after the 1 s to 15 s backoff the replay's track request carries key b                  |
      | 106  | false     | the fake server receives an untrack of key b and tracked_keys is a                      |
      | 500  | true      | after the 1 s to 15 s backoff the replay's track request carries key b                  |

  @D
  Scenario: Updates not taken before disconnect come back on the next connection
    Given a client connected to the fake server, with a shared poll subscription to "items" tracking "a" at version 2, whose stream has been read up to "subscribed"
    And the fake server has pushed "a" at version 3, not taken
    When disconnect is called, then connect is called and the subscribe is answered
    Then the replay's track request carries item "a" at version 2
    And the stream yields "subscribing", "subscribed" and no update at version 3 before the fake server sends it again

  @D
  Scenario: A removal not taken before disconnect is still delivered
    Given a client connected to the fake server, with a shared poll subscription to "items" tracking "a" at version 2, whose stream has been read up to "subscribed"
    And the fake server has pushed "a" as removed, not taken
    When disconnect is called
    Then the stream yields an update of "a" with removed true before "subscribing"
    And tracked_keys is empty

  @B
  Scenario: Delta compression decodes each tracked key's changes
    Given a client with a shared poll subscription to "poll_d:d1" with delta, subscribed, tracking "a" with a valid signature, whose stream was taken
    When the backend's refresh answers key "a" at version 1 with data {"v":"long value one"}
    And the backend's refresh answers key "a" at version 2 with data {"v":"long value two"} and that previous data
    Then the stream yields updates of "a" with data {"v":"long value one"} and {"v":"long value two"}
    And the subscription's delta_statistics counts one delta

  @D
  Scenario: Delta bases are dropped at each subscribe
    Given a client connected to the fake server, with a shared poll subscription to "items" with delta, subscribed, tracking "a", which has decoded one delta on key "a"
    When the transport closes and the client reconnects and the subscribe is answered with delta true
    And the fake server pushes a delta for "a" against the former base
    Then the stream yields an error of kind Delta with code 15 and "subscribing" with code 2

  @D
  Scenario: A delta in flight when its key was tracked again is dropped
    Given a client connected to the fake server, with a shared poll subscription to "items" with delta, subscribed, tracking "a" under signature "sig1", which has decoded one delta on key "a", whose stream was taken
    When untrack is called with key "a" and track_with_signature is called with key "a" and signature "sig2"
    And the fake server, before answering the untrack, pushes a delta for "a" against the former base
    Then the stream yields no error and no update of "a" within 1 s
    And the shared poll subscription stays "subscribed"
    When the fake server answers the untrack and the track and pushes "a" whole at version 9
    Then the stream yields an update of "a" with version 9

  @D
  Scenario: Delta bases past their bound end the subscription
    Given a client connected to the fake server, with a shared poll subscription to "items" with delta and max_delta_bases_bytes 1024, subscribed, tracking "a", whose stream was taken
    When the fake server pushes "a" with 2048 bytes of data
    Then the fake server receives an unsubscribe command for "items"
    And the stream yields "unsubscribed" with code 7 and reason "shared poll delta bases too large"

  @D
  Scenario: A delta that cannot be applied resubscribes and tracks again
    Given a client connected to the fake server, with a shared poll subscription to "items" with delta, subscribed, tracking "a" under signature "sig1", whose stream was taken
    When the fake server pushes a delta for key "a" with no base
    Then the stream yields an error of kind Delta with code 15 and "subscribing" with code 2
    And after the resubscribe backoff the fake server receives a subscribe and a track request with signature "sig1" and item "a"

  @D
  Scenario: Statistics count shared poll subscriptions and their updates
    Given a client connected to the fake server, with a shared poll subscription to "items" subscribed, tracking "a"
    When the fake server pushes "a" at version 1 and it is taken
    Then the client's statistics report 1 subscription and 1 publication delivered

  @A
  Scenario Outline: A shared poll subscription handler receives the events in order
    Given a client with <mode> delivery whose shared poll subscription to "poll_v:h1" has a handler recording its events, subscribed, tracking "a"
    When the server API publishes key "a" at version 3
    Then the handler has received "subscribing", "subscribed" and an update of "a" with version 3, in this order

    Examples:
      | mode   |
      | Task   |

    @inline
    Examples:
      | mode   |
      | Inline |

    @thread
    Examples:
      | mode   |
      | Thread |

  Scenario: A test builds shared poll events without a server
    Given the test-util and unstable features
    When a test builds an update of "a" with data {"x":1} at version 2 and a removal of "b"
    Then the events equal those the client would emit, and the update's json() gives {"x":1}

  @W
  Scenario: A browser client tracks a key and receives its updates
    Given a browser client with a shared poll subscription to "poll_v:w1", whose stream was taken
    When connect is called and the subscription becomes "subscribed"
    And key "a" is tracked with a valid signature
    And the server API publishes key "a" of "poll_v:w1" at version 2 with data {"v":2}
    Then the stream yields "subscribing", "subscribed" and an update of "a" with version 2
