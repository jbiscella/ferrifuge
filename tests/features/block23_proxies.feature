Feature: Proxies

  @D
  Scenario Outline: A websocket goes through a proxy with credentials
    Given a <proxy> proxy requiring credentials in front of the fake server
    And a client configured with a websocket endpoint only that proxy can reach, and that proxy with its credentials, its log records captured
    When connect is called
    Then the client emits "connected" over transport "websocket"
    And the proxy tunnelled the client's connection to that endpoint's host and port, with the credentials
    And a DEBUG record in the client's span names the attempt's proxy by scheme, host and port
    And no record contains the proxy's password, as text or in its Basic encoding

    Examples:
      | proxy   |
      | http    |
      | https   |
      | socks5  |
      | socks5h |

  @A
  Scenario Outline: Secure endpoints go through a proxy, with TLS to Centrifugo
    Given a server serving TLS with a certificate from the test CA, with http_stream and sse enabled
    And a <proxy> proxy requiring credentials in front of it
    And a client configured with only a <transport> endpoint only that proxy can reach, and that proxy with its credentials
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the proxy tunnelled every connection of the client to that endpoint's host and port, with the credentials

    Examples:
      | transport   | proxy   |
      | websocket   | http    |
      | http_stream | http    |
      | sse         | http    |
      | http_stream | https   |
      | http_stream | socks5h |

  @A
  Scenario Outline: Plain HTTP transports go through an HTTP proxy in absolute form
    Given a server with http_stream and sse enabled
    And an HTTP proxy requiring credentials in front of it
    And a client configured with only an http <transport> endpoint only that proxy can reach, and that proxy with its credentials
    When connect is called
    Then the client emits "connected" over transport "<transport>"
    And the proxy forwarded every request of the client to the server, with the credentials

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  Scenario Outline: A proxy refusing the credentials fails the attempt
    Given an HTTP proxy answering every request with 407
    And a client configured with only the <transport> endpoint "<endpoint>" and that proxy with the password "pw-3e9a", its log records captured
    When connect is called
    Then the client emits an "error" event for the failed transport attempt, saying proxy authentication is required
    And the client state is "connecting"
    And no record or error contains "pw-3e9a", as text or in its Basic encoding

    Examples:
      | transport   | endpoint                                             |
      | websocket   | ws://centrifugo.invalid:8000/connection/websocket    |
      | http_stream | http://centrifugo.invalid:8000/connection/http_stream |
      | http_stream | https://centrifugo.invalid:8443/connection/http_stream |

  @D
  Scenario: An endpoint in the no-proxy list is reached directly
    Given an HTTP proxy in front of the fake server
    And a client configured with the fake server's websocket endpoint and that proxy, the fake server's host in its no-proxy list
    When connect is called
    Then the client emits "connected"
    And the proxy carried nothing

  @D
  Scenario: A proxy function decides per endpoint
    Given an HTTP proxy in front of the fake server
    And a client configured with a websocket endpoint only that proxy can reach, and a proxy function choosing that proxy for it
    When connect is called
    Then the client emits "connected"
    And the proxy function was asked once, for that endpoint's scheme, host and port
    And the proxy tunnelled the client's connection to that endpoint's host and port

  @D
  Scenario: A proxy set at run time applies from the next attempt
    Given an HTTP proxy in front of the fake server
    And a client connected directly to the fake server, with no proxy
    When set_proxy is called with that proxy
    And the fake server disconnects the client with a reconnect-advised code
    Then the client emits "connected"
    And the proxy tunnelled exactly one connection, the client's new one, to the fake server

  Scenario Outline: A proxy URL of another scheme is refused
    When a proxy target is made from "<url>"
    Then it fails with a configuration error naming the scheme "<scheme>"
    And the error does not contain "pw-1d5b"

    Examples:
      | url                           | scheme |
      | ftp://u:pw-1d5b@proxy:21      | ftp    |
      | socks4://u:pw-1d5b@proxy:1080 | socks4 |
