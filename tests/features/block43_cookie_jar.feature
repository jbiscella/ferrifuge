Feature: Cookie jar

  @D
  Scenario: A cookie set on the websocket upgrade is sent on the next connection
    Given a client with a memory cookie jar connected to the fake server, whose upgrade response sets the cookie "affinity=node-1; Path=/; HttpOnly"
    When the fake server disconnects the client with a reconnect-advised code
    Then the next upgrade request carries one Cookie header "affinity=node-1"

  @D
  Scenario: A cookie set on a refused upgrade is sent on the next attempt
    Given a client with a memory cookie jar and the fake server answering its next upgrade with HTTP status 503 and the cookie "affinity=node-2; Path=/"
    When connect is called
    Then the client reaches state "connected"
    And the upgrade request after the refused one carries one Cookie header "affinity=node-2"

  @D
  Scenario: The app's Cookie header and the jar's cookies are sent in one header
    Given a client with a memory cookie jar and the HTTP header "Cookie: app=a1; affinity=mine", connected to the fake server, whose upgrade response sets the cookies "affinity=node-1; Path=/" and "lb=l1; Path=/"
    When the fake server disconnects the client with a reconnect-advised code
    Then the next upgrade request carries one Cookie header "app=a1; affinity=mine; lb=l1"

  @D
  Scenario: Without a cookie jar no cookie is kept
    Given a client without a cookie jar connected to the fake server, whose upgrade response sets the cookie "affinity=node-1; Path=/"
    When the fake server disconnects the client with a reconnect-advised code
    Then the next upgrade request carries no Cookie header

  @D
  Scenario: No cookie value reaches a log record
    Given a client with a memory cookie jar whose log records are captured, connected to the fake server, whose upgrade response sets the cookie "secret=c-secret; Path=/"
    When the fake server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then no record contains "c-secret"

  @B
  Scenario Outline: A cookie set on the stream response rides on every emulation request
    Given a client with a memory cookie jar connected over transport "<transport>" through the HTTP proxy, whose stream response sets the cookie "session=s1; Path=/"
    When the client sends a command
    Then the HTTP proxy relayed at least one emulation request
    And every emulation request it relayed carried one Cookie header "session=s1"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @B
  Scenario Outline: A cookie set on an emulation response is sent on the next emulation request
    Given a client with a memory cookie jar connected over transport "<transport>" through the HTTP proxy, whose emulation responses set the cookie "lb=e1; Path=/"
    When the client sends a command and, once it is answered, another
    Then the emulation request of the second command carried one Cookie header "lb=e1"

    Examples:
      | transport   |
      | http_stream |
      | sse         |
