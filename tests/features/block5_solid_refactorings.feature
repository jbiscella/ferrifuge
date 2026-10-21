Feature: SOLID refactorings

  @A
  Scenario: Token refreshed before expiry keeps the connection
    Given a connected client using a connection token with expiration
    When 6 s elapse
    Then the client stays "connected"

  @D
  Scenario: The client speaks JSON by default
    Given a client connected to the fake server
    Then the fake server received the connect command as JSON

  @A
  Scenario: The connected event names the transport
    Given a new client
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected" over transport "websocket"
