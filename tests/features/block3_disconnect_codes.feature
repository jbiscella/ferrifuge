Feature: Disconnect codes

  @A
  Scenario Outline: Application disconnect codes sent via server API
    Given a connected client
    When the server API disconnects the client with code <code>
    Then the client goes to state "<state>"
    And the event carries code <code> and the reason sent

    Examples:
      | code | state        |
      | 4000 | connecting   |
      | 4499 | connecting   |
      | 4500 | disconnected |
      | 4999 | disconnected |

  @D
  Scenario Outline: Protocol-range disconnect codes
    Given a client connected to the fake server
    When the fake server disconnects the client with code <code>
    Then the client goes to state "<state>"

    Examples:
      | code | state        |
      | 3000 | connecting   |
      | 3499 | connecting   |
      | 3500 | disconnected |
      | 3999 | disconnected |

  @D
  Scenario: Close code 1009 stops the client
    Given a client connected to the fake server
    When the fake server disconnects the client with code 1009
    Then the client emits "disconnected" with code 3 and reason "message size limit exceeded"
    And the client makes no new connection attempt

  @B
  Scenario Outline: Disconnect returned by the connect proxy
    Given the connect proxy answers with disconnect code <code>
    When connect is called
    Then the client goes to state "<state>"

    Examples:
      | code | state        |
      | 4000 | connecting   |
      | 4500 | disconnected |
