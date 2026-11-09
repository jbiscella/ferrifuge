Feature: Map subscriptions (unstable)

  Scenario Outline: A map subscription cannot share a channel with a stream subscription
    Given a new client with a <existing> to channel "map:a"
    When a <created> to channel "map:a" is created
    Then creating it fails with "a subscription to channel map:a already exists"

    Examples:
      | existing                       | created                        |
      | subscription                   | map subscription of kind "map" |
      | map subscription of kind "map" | subscription                   |

  Scenario: A new map subscription starts unsubscribed
    Given a new client
    When a map subscription of kind "map" to channel "map:a" is created
    Then the map subscription state is "unsubscribed"
    And the client's map subscriptions are "map:a" and its subscriptions are none

  @D
  Scenario: Removing a map subscription unsubscribes it and frees its channel
    Given a client connected to the fake server, with a map subscription to "scores" that has synced
    When the map subscription is removed
    Then the fake server receives an unsubscribe command for "scores"
    And the client's map subscriptions are none
    And a subscription to channel "scores" can be created

  @A
  Scenario: map_publish sets a key
    Given a connected client and another client with a map subscription to "map:pub1" that has synced
    When the client calls map_publish on "map:pub1" with key "k1" and data {"x":1}
    Then the call succeeds
    And the other client's map subscription emits an update with key "k1", data {"x":1}, removed false and info naming the publishing client

  @A
  Scenario: map_remove removes a key
    Given the server API has set key "k1" of "map:rm1" to {"x":1}
    And a client with a map subscription to "map:rm1" that has synced
    When the client calls map_remove on "map:rm1" with key "k1"
    Then the call succeeds
    And the map subscription emits an update with key "k1", removed true and empty data

  @A
  Scenario Outline: A refused map publish fails with the server's code
    Given a connected client
    When the client calls map_publish on "<channel>" with key "<key>" and data <data>
    Then the call fails with code <code>

    Examples:
      | channel        | key | data    | code |
      | news           | k1  | {"x":1} | 103  |
      | map_private:p1 | k1  | {"x":1} | 103  |
      | map:perm       |     | {"x":1} | 107  |

  @D
  Scenario: A map subscription loads its state page by page
    Given a client connected to the fake server
    And a map subscription of kind "map" to channel "scores" with page size 2, whose stream was taken
    When subscribe is called on the map subscription
    And the fake server answers the first request with phase 2, entries "a" and "b", cursor "c1", epoch "e1" and offset 5
    And answers the next request with phase 2, entry "c", no cursor, epoch "e1" and offset 5
    And answers the next request live with a publication setting "d" at offset 6, epoch "e1" and offset 6
    Then the fake server received, for "scores", a subscribe with type 1, phase 2 and limit 2 and no cursor, then one with phase 2, limit 2, cursor "c1", epoch "e1" and offset 5, then one with phase 1, limit 2, epoch "e1" and offset 5
    And the map subscription's stream yields "subscribing", "subscribed", then a sync with entries "a", "b", "c" and "d" in that order

  @D
  Scenario: A page size of 0 leaves the page size to the server
    Given a client connected to the fake server
    When a map subscription to channel "scores" with page size 0 is subscribed
    Then the first subscribe command for "scores" has type 1, phase 2 and no limit

  @D
  Scenario: Changes made during paging are part of the sync
    Given a client connected to the fake server, with a map subscription to "scores" whose stream was taken
    When subscribe is called on the map subscription
    And the fake server answers with state pages holding "a" = 1, "b" = 1 and "c" = 1, then a stream page with "a" = 2 at offset 6 and the removal of "b" at offset 7, then live with "d" = 1 at offset 8
    Then the map subscription's stream yields "subscribing", "subscribed" with recovered false, then a sync with exactly "a" = 2, "c" = 1 and "d" = 1, in that order
    And it yields no update

  @D
  Scenario: An epoch change during paging restarts the load
    Given a client connected to the fake server, with a map subscription to "scores" whose stream was taken
    When subscribe is called on the map subscription
    And the fake server answers the first request with phase 2, entry "a", cursor "c1" and epoch "e1"
    And answers the next request with phase 2, entry "b", no cursor and epoch "e2"
    Then the fake server receives an unsubscribe command for "scores"
    And once it is answered, the next subscribe command for "scores" has phase 2, no cursor, no epoch and no offset
    And when the fake server answers it live with entry "z" and epoch "e2", the stream yields "subscribing", "subscribed" and a sync with exactly "z"

  @A
  Scenario: A map subscription syncs then delivers updates
    Given the server API has set keys "k1" to "k5" of "map:s1" to {"v":1} to {"v":5}
    And a client with a map subscription of kind "map" to "map:s1" with page size 2, whose stream was taken
    When the client connects and subscribes the map subscription
    And the server API sets key "k6" of "map:s1" to {"v":6} once the subscription is subscribed
    And the server API removes key "k1" of "map:s1"
    Then the map subscription's stream yields "subscribing", "subscribed" with recoverable true and recovered false, a sync with keys "k1" to "k5" and their data
    And then an update with key "k6", data {"v":6} and offset 6, then an update with key "k1" and removed true

  @A
  Scenario Outline: A map subscription syncs over each transport and protocol
    Given a server with http_stream and sse enabled, and the server API has set keys "a", "b" and "c" of "map:t1"
    And a client with the <protocol> protocol connected over transport "<transport>" with a map subscription to "map:t1" with page size 2
    When subscribe is called on the map subscription
    Then the map subscription emits a sync with keys "a", "b" and "c"
    And when the server API sets key "d" of "map:t1", the map subscription emits an update of "d"

    Examples:
      | transport   | protocol |
      | websocket   | JSON     |
      | websocket   | Protobuf |
      | http_stream | JSON     |
      | http_stream | Protobuf |
      | sse         | JSON     |

  @A
  Scenario: publish and remove on a map subscription change the map for every subscriber
    Given two clients each with a map subscription to "map:pr1" that has synced
    When the first client's map subscription publishes {"x":1} to key "cursor"
    And then removes key "cursor"
    Then both calls succeed
    And each map subscription emits an update with key "cursor" and data {"x":1}, then an update with key "cursor" and removed true

  @D
  Scenario: A map subscription's publish waits until it is subscribed
    Given a client connected to the fake server, with a map subscription to "scores" whose first page is not answered yet
    When the map subscription publishes {"x":1} to key "a"
    Then the fake server has received no publish command 1 s later
    And when the fake server answers the page live, it receives a publish with type 1, key "a" and data {"x":1}

  @A
  Scenario: Presence of clients is a map keyed by client id
    Given a client with a map subscription of kind "clients" to "clients:game:g1" that has synced with no entry
    When a second client of user "alice" subscribes to "game:g1"
    Then the map subscription emits an update whose key is the second client's id and whose info names user "alice"
    And when the second client unsubscribes from "game:g1", the map subscription emits an update with that key and removed true

  @A
  Scenario: Presence of users is a map keyed by user id whose entries expire
    Given a client with a map subscription of kind "users" to "users:game:g2" that has synced with no entry
    When a second client of user "bob" subscribes to "game:g2"
    Then the map subscription emits an update with key "bob", empty data and no info
    And when the second client disconnects, the map subscription emits an update with key "bob" and removed true within 10 s

  @A
  Scenario Outline: A subscription type the namespace does not declare is refused
    Given a connected client
    When a <subscription> to "<channel>" is subscribed
    Then it emits "unsubscribed" with code 103 and reason "permission denied"

    Examples:
      | subscription                       | channel     |
      | map subscription of kind "map"     | news        |
      | subscription                       | map:refused |
      | map subscription of kind "clients" | map:refused |

  @A
  Scenario: A map subscription to a private namespace gets its token and refreshes it
    Given a client whose map subscription to "map_private:a" has a token callback giving tokens that expire after 3 s
    When the client connects and subscribes the map subscription
    Then the map subscription emits a sync
    And the token callback was called once before the first subscribe command
    And 5 s later the map subscription is still "subscribed" and the token callback was called at least twice

  @D
  Scenario: Every request of a load carries the token, and only the first carries the data
    Given a client connected to the fake server, with a map subscription to "scores" with token "t1" and data {"room":1}
    When subscribe is called and the fake server answers two state pages and then live
    Then each of the three subscribe commands carries token "t1"
    And only the first carries data {"room":1}

  @D
  Scenario: A missing page reply makes the client reconnect
    Given a client connected to the fake server, with a map subscription to "scores" whose first page was answered with a cursor
    When the fake server does not answer the second page within the configured timeout
    Then the client emits "connecting" with code 3 and reason "subscribe timeout"

  @D
  Scenario: A server without map subscriptions ends the map subscription
    Given a client connected to the fake server, with a map subscription to "scores"
    When subscribe is called on the map subscription
    And the fake server answers it with a subscribe result without type and without phase
    Then the fake server receives an unsubscribe command for "scores"
    And the map subscription emits "unsubscribed" with code 6 and reason "map subscriptions not supported", and no sync
    And 2 s later the fake server has received no further subscribe command for "scores"

  @D
  Scenario Outline: A reply in a phase the request does not allow forces a reconnect
    Given a client connected to the fake server, with a map subscription to "scores" whose <request> is in flight
    When the fake server answers it with phase <phase>
    Then the client emits "connecting" with code 1

    Examples:
      | request             | phase |
      | first state request | 1     |
      | stream request      | 2     |
      | first state request | 3     |

  @D
  Scenario: A load larger than its bound ends the map subscription
    Given a client connected to the fake server, with a map subscription to "scores" whose max_state_bytes is 1024 and whose stream was taken
    When subscribe is called on the map subscription
    And the fake server answers each request with a state page of 600 bytes and a cursor
    Then after the second page the fake server receives an unsubscribe command for "scores" and no further subscribe command
    And the map subscription's stream yields "subscribing", then "unsubscribed" with code 5 and reason "map state too large", and no sync

  @D
  Scenario: Pages are handled while another subscription's stream is not read
    Given a client connected to the fake server, with a subscription to "news" whose stream has been read up to "subscribed" and is not read after
    And the fake server has pushed 3 publications to "news"
    And a map subscription to "scores" whose stream was taken
    When subscribe is called on the map subscription
    And the fake server answers its first page with a cursor
    Then the client sends the second page request for "scores" within 1 s
    And when the fake server answers it live, the map subscription's stream yields nothing beyond "subscribing" within 1 s
    And once the 3 publications of "news" are taken, the map subscription's stream yields "subscribed" and a sync

  @D
  Scenario: An unsubscribe during a load waits for the page in flight
    Given a client connected to the fake server, with a map subscription to "scores" whose first page is not answered yet
    When unsubscribe is called on the map subscription
    And the fake server answers the page with a cursor
    Then the fake server receives an unsubscribe command for "scores" and no further subscribe command

  @A
  Scenario: Statistics count map subscriptions and their updates
    Given a client with a map subscription to "map:st1" that has synced 3 keys
    When the server API sets 2 keys of "map:st1" and the application takes both updates
    Then the client's statistics count 1 subscription and 2 publications delivered

  @A
  Scenario: Missed changes are recovered as updates after a reconnect
    Given a client with a map subscription to "map:r1" that has synced keys "a" and "b"
    When the server disconnects the client with a reconnect-advised code
    And the server API sets key "c" of "map:r1" and removes key "a" while the client reconnects
    Then the map subscription emits "subscribing", then "subscribed" with was_recovering true and recovered true
    And then an update with key "c", then an update with key "a" and removed true
    And it emits no sync

  @D
  Scenario: A recovering subscribe starts in the stream phase from the position
    Given a client connected to the fake server, with a map subscription to "scores" that has taken a sync at epoch "e1" and offset 7
    When the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "scores" has type 1, phase 1, recover true, epoch "e1" and offset 7
    And no subscribe command with phase 2 is sent for "scores"

  @D
  Scenario: Updates move the position only when taken
    Given a client connected to the fake server, with a map subscription to "scores" synced at epoch "e1" and offset 7, whose stream has been read up to the sync
    And the fake server has pushed updates at offsets 8 and 9
    And the application has taken only the update at offset 8
    When disconnect is called
    And connect is called
    Then the next subscribe command for "scores" asks to recover from epoch "e1" and offset 8
    And no update is yielded twice

  @D
  Scenario: A sync not taken before disconnect() is withdrawn and the state is loaded again
    Given a client connected to the fake server, with a map subscription to "scores" whose stream was taken and is not read
    And the fake server has answered its load live with entries "a" and "b"
    When disconnect is called
    And connect is called
    Then the next subscribe command for "scores" has phase 2 and no epoch
    And when the fake server answers it live with entries "a", "b" and "c" and the application reads the stream, it yields exactly one sync, with "a", "b" and "c"

  @A
  Scenario: A map subscription too far behind resyncs from a fresh state
    Given a client with a map subscription to "map_rec:r2" that has synced key "a" = 0, whose stream is read
    When the server disconnects the client with a reconnect-advised code
    And the server API sets key "a" of "map_rec:r2" to 1 to 10 while the client reconnects
    Then the map subscription emits "subscribed" with recovered false, then a sync with exactly "a" = 10
    And it emits no error event and no update

  @E
  Scenario: An unrecoverable position resyncs after the resubscribe backoff
    Given a client connected, with a map subscription to "scores" whose recovering subscribe is answered with error 112, minResubscribeDelay 500 ms and maxResubscribeDelay 20000 ms
    When the error is handled
    Then the subscription stays "subscribing" and emits nothing
    And the next subscribe command for "scores" has phase 2 and no epoch, sent after a delay between 500 ms and 1000 ms

  @A
  Scenario: With the fatal strategy an unrecoverable position unsubscribes
    Given a client with a map subscription to "map_rec:r3" with the fatal unrecoverable strategy, that has synced key "a"
    When the server disconnects the client with a reconnect-advised code
    And the server API sets key "a" of "map_rec:r3" 10 times while the client reconnects
    Then the map subscription emits "unsubscribed" with code 112 and reason "unrecoverable position"
    And 2 s later the map subscription is still "unsubscribed" and has emitted no further event

  @A
  Scenario: An ephemeral map syncs again on every reconnect
    Given a client with a map subscription to "map_eph:e1" that has synced key "a"
    When the server disconnects the client with a reconnect-advised code
    Then the map subscription emits "subscribed" with recoverable false and was_recovering false, then a sync with key "a"

  @A
  Scenario: A cleared map is loaded again
    Given a client with a map subscription to "map:c1" that has synced keys "a" and "b"
    When the server API clears "map:c1"
    And the server API sets key "z" of "map:c1"
    Then the map subscription emits "subscribing" with code 2500 and reason "insufficient state"
    And then "subscribed" with recovered false and a sync with exactly key "z"

  @D
  Scenario Outline: State invalidation drops the position and the token
    Given a client connected to the fake server, with a map subscription to "scores" with a token callback, synced at epoch "e1" and offset 7
    When the fake server sends <invalidation>
    Then the token callback is called before the next subscribe command for "scores"
    And that command has phase 2 and no epoch

    Examples:
      | invalidation                                    |
      | an unsubscribe push with code 2502 for "scores" |
      | a disconnect with code 3014                     |

  @D
  Scenario: A temporary error during a load starts it again after the backoff
    Given a client connected to the fake server, with a map subscription to "scores" whose first page was answered with a cursor
    When the fake server answers the second page with error 113 "concurrent pagination", temporary
    Then the map subscription emits an error with code 113
    And the fake server receives an unsubscribe command for "scores"
    And once it is answered, the next subscribe command for "scores" has phase 2 and no cursor

  @D
  Scenario: A load the server closes as slow starts again on the next connection
    Given a client connected to the fake server, with a map subscription to "scores" whose first page was answered with a cursor
    When the fake server closes the connection with code 3008 "slow"
    Then the client emits "connecting" with code 3008
    And on the next connection the first subscribe command for "scores" has phase 2 and no cursor

  @A
  Scenario: Delta compression decodes each key's changes
    Given a client with a map subscription to "map_delta:d1" with fossil delta, that has synced keys "a" and "b" of 300 bytes each
    When the server API sets key "a", then key "b", then key "a" again, each changing one byte
    Then the map subscription emits three updates whose data equal, byte for byte, what was set
    And its delta statistics count 3 deltas

  @A
  Scenario: A recovered map subscription decodes deltas against the values it held
    Given a client with a map subscription to "map_delta:d2" with fossil delta, that has synced keys "a" and "b" of 300 bytes each
    When the server disconnects the client with a reconnect-advised code
    And the server API sets key "b" while the client reconnects
    And the server API sets key "a", changing one byte, once the map subscription is subscribed again
    Then the map subscription emits "subscribed" with recovered true and delta true, an update of "b", then an update of "a"
    And each update's data equals, byte for byte, what was set
    And its delta statistics count the update of "a" as a delta

  @D
  Scenario: A removal ends a key's delta chain
    Given a client connected to the fake server, with a map subscription to "scores" with fossil delta, synced with key "a" whose delta is on
    When the fake server pushes the removal of "a", then a delta publication of "a"
    Then the map subscription emits an update of "a" with removed true, then an error of kind "delta" with code 15
    And then "subscribing" with code 2 and reason "delta error"

  @D
  Scenario Outline: Page data is decoded once the live reply says whether delta is on
    Given a client with the JSON protocol connected to the fake server, with a map subscription to "scores" with fossil delta
    When the fake server answers a state page whose entry "a" has the JSON string data "{\"x\":1}", then live with delta <delta>
    Then the map subscription emits a sync whose entry "a" has data <data>

    Examples:
      | delta | data                       |
      | true  | {"x":1}                    |
      | false | the JSON string of {"x":1} |

  @A
  Scenario: A tag filter limits the state and the updates
    Given the server API has set key "x" with tag t = "red" and key "y" with tag t = "blue" in "map_tag:f1"
    And a client with a map subscription to "map_tag:f1" with a tag filter t eq "red"
    When the client subscribes the map subscription
    And the server API sets key "y" with tag t = "blue", then key "x" with tag t = "red"
    Then the map subscription emits a sync with exactly key "x"
    And then exactly one update, of key "x"

  @D
  Scenario: Setting a tag filter loads the state again
    Given a client connected to the fake server, with a map subscription to "scores" synced at epoch "e1" and offset 7
    When the tag filter of the map subscription is set to t eq "red"
    And the fake server disconnects the client with a reconnect-advised code
    Then the next subscribe command for "scores" has phase 2, no epoch and the tag filter t eq "red"

  @A
  Scenario Outline: A map subscription handler receives the events in order
    Given a client with <mode> delivery whose map subscription to "map:h1" has a handler recording its events, and the server API has set key "a" of "map:h1"
    When the client connects and subscribes the map subscription
    And the server API sets key "b" of "map:h1" once the subscription is subscribed
    Then the handler has received "subscribing", "subscribed", a sync with key "a" and an update of "b", in this order

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

  Scenario: A test builds map events without a server
    Given the test-util and unstable features
    When a test builds a sync with entry "a" = {"x":1} and an update removing "a"
    Then the events equal those the client would emit, and the entry's json() gives {"x":1}

  @W
  Scenario: A browser client syncs a map and receives its updates
    Given keys "a" and "b" of "map:w1" have been set
    And a browser client with a map subscription to "map:w1", whose stream was taken
    When connect is called
    And subscribe is called on the map subscription
    And key "c" of "map:w1" is set once the subscription is subscribed
    Then the map subscription's stream yields "subscribing", "subscribed", a sync with keys "a" and "b", then an update of "c"
