Feature: Typed JSON helpers

  @A
  Scenario Outline: A typed publication reaches subscribers as the JSON its type encodes to
    Given two clients subscribed to channel "news"
    When the first client publishes the note {text: "hi", n: 1} as JSON through its <publisher>
    Then the second client's subscription receives a publication with data exactly {"text":"hi","n":1}

    Examples:
      | publisher    |
      | client       |
      | subscription |

  @A
  Scenario: A publication from the server API decodes as the application's type
    Given a client subscribed to channel "news"
    When the server API publishes {"n":1,"extra":true,"text":"hi"} to channel "news"
    Then the subscription receives a publication that decodes as the note {text: "hi", n: 1}

  @A
  Scenario: A typed publication from a Protobuf client reaches a JSON client
    Given a client configured with the Protobuf protocol and a client configured with the JSON protocol, both subscribed to channel "news"
    When the Protobuf client publishes the note {text: "hi", n: 1} as JSON
    Then the JSON client's subscription receives a publication with data exactly {"text":"hi","n":1}

  @B
  Scenario: A typed RPC sends JSON and decodes the proxy's result
    Given a connected client and an RPC proxy answering method "sum" with {"sum":3}
    When the client calls RPC "sum" as JSON with the terms {a: 1, b: 2}, expecting a sum
    Then the RPC proxy received data equal as JSON to {"a":1,"b":2}
    And the call returns the sum {sum: 3}

  @D
  Scenario Outline: A typed RPC tells its failures apart
    Given a client connected to the fake server that answers RPC "sum" <answer>
    When the client calls RPC "sum" as JSON with the terms {a: 1, b: 2}, expecting a sum
    Then the call fails with <error>

    Examples:
      | answer                                                   | error                                                    |
      | with {"sum":"three"}                                     | a decoding error holding the result data {"sum":"three"} |
      | with no data                                             | a decoding error holding empty result data               |
      | with a temporary error with code 1000 and message "busy" | a call error with code 1000, message "busy", temporary   |

  @D
  Scenario Outline: A value that cannot be encoded fails the call at once and sends nothing
    Given a client connected to the fake server with a subscription to channel "news"
    When the client calls <helper> with a value whose serialization fails
    Then the call fails at once with an encoding error
    When the client publishes {"n":2} to channel "news"
    Then the fake server has received one command since, a publish with data {"n":2}

    Examples:
      | helper                           |
      | publish_json to channel "news"   |
      | publish_json on its subscription |
      | rpc_json with method "sum"       |

  @D
  Scenario: A typed call is made when the method returns
    Given a client connected to the fake server
    When the client publishes the note {text: "hi", n: 1} as JSON to channel "news" and holds the future without polling it
    Then the fake server receives a publish command with data exactly {"text":"hi","n":1}

  Scenario Outline: A publication's data decodes as the application's type, or says why not
    Given a publication whose data is <data>
    When it is decoded as a note
    Then <outcome>

    Examples:
      | data                    | outcome                                       |
      | {"text":"hi","n":1}     | it gives the note {text: "hi", n: 1}          |
      | {"text":"hi","n":"one"} | it fails with a serde_json data error         |
      | not json                | it fails with a serde_json syntax error       |
      | the bytes ff 00 fe      | it fails with a serde_json syntax error       |
      | nothing                 | it fails with a serde_json end-of-input error |
