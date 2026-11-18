Feature: Custom websocket subprotocols

  @D
  Scenario Outline: Without custom subprotocols the upgrade is unchanged
    Given a fake server that records the websocket subprotocols offered
    And a client with the <protocol> protocol and no websocket subprotocols
    When connect is called
    Then the client emits "connected"
    And the upgrade request carried <header>

    Examples:
      | protocol | header                                                  |
      | JSON     | no Sec-WebSocket-Protocol header                        |
      | Protobuf | one Sec-WebSocket-Protocol header "centrifuge-protobuf" |

  @D
  Scenario Outline: Custom subprotocols are offered after the encoding's own value
    Given a fake server that records the websocket subprotocols offered
    And a client with the <protocol> protocol, given websocket subprotocol "tenant-a" in one call and "region-1" in a later one
    When connect is called
    Then the client emits "connected"
    And the upgrade request carried one Sec-WebSocket-Protocol header "<offer>"
    And the fake server selected "<selected>"

    Examples:
      | protocol | offer                                   | selected            |
      | JSON     | centrifuge-json, tenant-a, region-1     | centrifuge-json     |
      | Protobuf | centrifuge-protobuf, tenant-a, region-1 | centrifuge-protobuf |

  @A
  Scenario Outline: A client offering custom subprotocols connects to Centrifugo
    Given a client configured with the <protocol> protocol and websocket subprotocol "tenant-a", with a subscription to channel "news"
    When connect is called
    And the subscription is subscribed
    Then the client emits "connected" over transport "websocket"
    And a publication to channel "news" is received

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @D
  Scenario Outline: A server that does not select the encoding's subprotocol fails the attempt
    Given a fake server that answers the websocket upgrade with <answer>
    And a client with websocket subprotocol "tenant-a"
    When connect is called
    Then the client emits an "error" event of kind transport with message "websocket subprotocol not accepted"
    And the client state is "connecting"
    And the fake server received no connect command

    Examples:
      | answer                   |
      | the subprotocol tenant-a |
      | no subprotocol           |

  @D
  Scenario Outline: A subprotocol that is not a valid, unique application value fails the build
    Given client options with websocket subprotocols <values>
    When the client is built
    Then building fails with a configuration error naming websocket subprotocol <index> and containing none of the values

    Examples:
      | values                     | index |
      | "tenant a"                 | 0     |
      | "a", "b,c"                 | 1     |
      | "a", ""                    | 1     |
      | "a", "b", "a"              | 2     |
      | "tenänt"                   | 0     |
      | "centrifuge-json"          | 0     |
      | "a", "centrifuge-protobuf" | 1     |

  @D
  Scenario: Subprotocol values appear in no Debug output and no log record
    Given a client with websocket subprotocol "tenant-secret-42", its log records captured
    When connect is called and the client connects to the fake server
    Then the builder's Debug output shows 1 websocket subprotocol and does not contain "tenant-secret-42"
    And the client's Debug output does not contain "tenant-secret-42"
    And the client's span holds TRACE records of inputs, actions, and frames sent and received
    And no record contains "tenant-secret-42"

  @W
  Scenario: A browser client offers its custom subprotocols after the encoding's own value
    Given a browser client with the JSON protocol, websocket subprotocol "tenant-a" and a WebSocket shim that records its arguments and constructs the page's WebSocket
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the shim was called with the subprotocols "centrifuge-json" and "tenant-a", in that order, as an array

  @W
  Scenario: A browser socket reporting another subprotocol fails the attempt
    Given a browser client with websocket subprotocol "tenant-a" and a WebSocket shim whose socket's protocol property reads "tenant-a"
    When connect is called
    Then the client emits an "error" event of kind transport with message "websocket subprotocol not accepted"
    And the client state is "connecting"
