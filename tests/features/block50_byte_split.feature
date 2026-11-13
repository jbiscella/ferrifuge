Feature: Byte-split matrix

  @E
  Scenario Outline: A stream body split at every byte offset decodes the same
    Given a <format> body holding a connect reply, a publication with multi-byte data and a ping
    When it is delivered in two chunks, split at each byte offset in turn
    Then the decoder yields exactly those three messages, in order, for every offset
    And no offset gives an error

    Examples:
      | format            |
      | http_stream JSON  |
      | sse JSON          |
      | http_stream Protobuf |

  @E
  Scenario Outline: A stream body delivered one byte per chunk decodes the same
    Given a <format> body holding a connect reply, a publication with multi-byte data and a ping
    When it is delivered one byte per chunk
    Then the decoder yields exactly those three messages, in order
    And the decoder emitted no message before the byte that ends it

    Examples:
      | format            |
      | http_stream JSON  |
      | sse JSON          |
      | http_stream Protobuf |

  @D
  Scenario Outline: A websocket relayed one byte per write works
    Given a client configured with the <protocol> protocol, subscribed to channel "news" on the fake server through a relay writing one byte per write in both directions
    When the fake server sends a publication on "news" and a ping in one frame
    Then the subscription receives that publication with its exact data
    And the fake server received a pong after the ping
    And the relay made at least one write per byte relayed

    Examples:
      | protocol |
      | JSON     |
      | Protobuf |

  @P
  Scenario Outline: A large reply delivered one byte per chunk costs what its size predicts
    Given a <format> body holding one reply
    When a reply of 256 KiB and a reply of 1 MiB are each decoded one byte per chunk
    Then the instruction count for 1 MiB is at most 5 times the count for 256 KiB

    Examples:
      | format               |
      | http_stream JSON     |
      | sse JSON             |
      | http_stream Protobuf |
