Feature: Protobuf encoding

  @A
  Scenario: Connect with Protobuf
    Given a new client configured with the Protobuf protocol
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @D
  Scenario: Frames are binary Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server
    Then the fake server received the connect command as binary Protobuf

  @D
  Scenario: Ping answered in Protobuf
    Given a client configured with the Protobuf protocol connected to the fake server that announces pong is expected
    When the fake server sends a Protobuf ping
    Then the client sends a Protobuf pong

  @D
  Scenario: Every message of a multi-message Protobuf frame is handled
    Given a client configured with the Protobuf protocol connected to the fake server that announces pong is expected
    When the fake server sends two Protobuf pings in one frame
    Then the client sends two Protobuf pongs

  @D
  Scenario: Malformed Protobuf frame forces a reconnect
    Given a client configured with the Protobuf protocol connected to the fake server
    When the fake server sends a binary frame that is not valid Protobuf
    Then the client emits "connecting" with code 1
