Feature: TLS crypto provider

  @A
  Scenario: Secure websocket connection with the build's provider
    Given a server serving TLS with a certificate from the test CA
    And a client configured with the server's wss endpoint
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"
