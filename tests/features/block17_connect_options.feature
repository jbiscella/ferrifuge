Feature: Connect data, headers, set_token and set_data

  @B
  Scenario: Connect data is sent on connect and on reconnect
    Given a connect proxy recording requests
    And a client with connect data {"device":"d1"}
    When connect is called
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy recorded data {"device":"d1"} on both connect requests

  @B
  Scenario: Connect data from a callback is renewed on every attempt
    Given a connect proxy recording requests
    And a client with connect data {"device":"d1"} and a data callback returning {"n":1} then {"n":2}
    When connect is called
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy recorded data {"n":1} then {"n":2}

  @E
  Scenario: A failing connect data callback is retried after the reconnect backoff
    Given a client whose data callback fails once
    When connect is called
    Then the client emits an "error" event of kind connect data with code 12
    And the client state is "connecting"
    And the data callback is called again after the reconnect backoff delay
    And the connect command carries the data the second call returned

  @E
  Scenario: A connect data callback denying access disconnects the client
    Given a client whose data callback signals "unauthorized"
    When connect is called
    Then the client emits "disconnected" with code 1 and reason "unauthorized"
    And no transport is opened
    And no reconnect is scheduled

  @E
  Scenario: A disconnect while the data callback runs opens no transport
    Given a client whose data callback has not answered since connect was called
    When disconnect is called
    And the data callback answers
    Then the client state is "disconnected"
    And no transport is opened

  @E
  Scenario Outline: The connect data callback runs only after a connection token is obtained
    Given a client without an initial token, with a data callback whose token callback <outcome>
    When connect is called
    Then the data callback is not called for that attempt

    Examples:
      | outcome                      |
      | signals "unauthorized"       |
      | fails with a temporary error |
      | returns an empty token       |

  @E
  Scenario: The connection token is fetched before the connect data
    Given a client without an initial token, with a token callback and a data callback
    When connect is called
    Then the token callback is called before the data callback
    And the connect command carries both the token and the data

  @E
  Scenario: A client that has a token calls the data callback without the token callback
    Given a client with an initial token, a token callback and a data callback
    When connect is called
    Then the token callback is not called
    And the connect command carries the initial token and the data

  @B
  Scenario: Data returned with the connect reply is delivered
    Given the connect proxy answers with data {"welcome":true}
    When connect is called
    Then the client emits "connected" with data {"welcome":true}

  @D
  Scenario: The connected event carries the server's data exactly
    Given a fake server answering connects with the non-canonical JSON data {"b": 1.0,  "a":2}
    When a client connects to it
    Then the client emits "connected" with data exactly those bytes

  @B
  Scenario Outline: HTTP headers reach the connect proxy over each transport
    Given a connect proxy recording requests
    And a server with http_stream and sse enabled
    And a client with HTTP header "X-Device-Real: r1" configured with only the server's <transport> endpoint
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the connect proxy received header "X-Device-Real: r1"

    Examples:
      | transport   |
      | websocket   |
      | http_stream |
      | sse         |

  @B
  Scenario Outline: HTTP headers ride on every request of an emulated transport
    Given a client with HTTP header "X-Device-Real: r1" connected over transport "<transport>" through the HTTP proxy
    When the client sends a command
    Then the HTTP proxy relayed the stream request and at least one emulation request
    And every request it relayed carried header "X-Device-Real: r1"

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @B
  Scenario: Emulated headers reach the connect proxy
    Given a connect proxy recording requests
    And a client with emulated header "X-Device-Emulated: e1"
    When connect is called
    Then the connect proxy received header "X-Device-Emulated: e1"

  Scenario: A header the transport sets is rejected
    Given client options with HTTP header "Sec-WebSocket-Key: k1"
    When the client is built
    Then building fails with a configuration error naming "Sec-WebSocket-Key" and not "k1"

  Scenario: Client options do not show secrets
    Given client options with token "tok-secret", connect data {"pin":"1234"}, HTTP header "Authorization: h-secret" and emulated header "Authorization: e-secret"
    When their debug output is formatted
    Then it names the header "Authorization" and shows the data as 14 bytes
    And it contains none of "tok-secret", "1234", "h-secret" and "e-secret"

  @D
  Scenario: set_token replaces an expired token when there is no token callback
    Given a client with token "old", no token callback and a minimum reconnect delay of 1 s, connected to the fake server
    And the fake server will answer the next connect with error 109 "token expired"
    When the fake server disconnects the client with a reconnect-advised code
    And set_token is called with "new" when the client emits the "error" event with code 109
    Then the next connect command carries token "new"
    And the client emits "connected"
    And the client emits no "error" event of kind configuration

  @E
  Scenario: set_token does not skip the token callback after an expired token
    Given a client using a token callback whose connect was answered with error 109
    When set_token is called with "new" before the next attempt
    Then the token callback is called before the next connect command

  @D
  Scenario: set_data applies from the next connection attempt
    Given a client with connect data {"v":1}, connected to the fake server
    When set_data is called with {"v":2}
    And the fake server disconnects the client with a reconnect-advised code
    Then the fake server received no command between set_data and the disconnect
    And the next connect command carries data {"v":2}

  @B
  Scenario: set_emulated_headers applies from the next connection attempt
    Given a connect proxy recording requests
    And a connected client with emulated header "X-Device-Emulated: e1"
    When set_emulated_headers is called with "X-Device-Emulated: e2"
    And the server disconnects the client with a reconnect-advised code
    And the client reaches state "connected" again
    Then the connect proxy received header "X-Device-Emulated: e1" then "X-Device-Emulated: e2"
