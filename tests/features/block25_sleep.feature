Feature: Sleep, network changes, captive portals

  @E
  Scenario: A sleep as long as the no-ping window reconnects after the backoff and jitter
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s and a reconnect jitter window of 10 s
    When it is told it slept for 35 s
    Then it closes the transport and emits "connecting" with code 2 and reason "no ping"
    And its next attempt is delayed by at least 500 ms and at most 11000 ms

  @E
  Scenario: A shorter sleep keeps the connection and restarts the no-ping window
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s
    And 10 s have elapsed since its last ping
    When it is told it slept for 34 s
    Then it keeps the transport
    And its no-ping deadline is 35 s from now

  @E
  Scenario: Without pings a sleep keeps the connection
    Given a connected machine whose server sends no pings
    When it is told it slept for 600 s
    Then it keeps the transport

  @E
  Scenario: A gap under 2 s changes nothing
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 1 s
    When it is told it slept for 1999 ms
    Then it returns no action

  @E
  Scenario: A refresh due during a short sleep starts at the wake
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 20 s
    When it is told it slept for 30 s
    Then it keeps the transport
    And it asks for a new connection token at once

  @E
  Scenario: A refresh not yet due moves earlier by the sleep
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 50 s
    When it is told it slept for 30 s
    Then its connection token refresh is due in 20 s

  @E
  Scenario: A token due during a long sleep is fetched before the next connect
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, and whose connection token refresh is due in 50 s
    When it is told it slept for 60 s
    And its next attempt starts
    Then it asks for a new connection token before opening a transport

  @E
  Scenario: A subscription token due during a short sleep is refreshed at the wake
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s, subscribed to "news" with a token whose refresh is due in 20 s
    When it is told it slept for 30 s
    Then it asks for a new token for "news" at once

  @E
  Scenario: A sleep leaves reply deadlines alone
    Given a connected machine whose server pings every 25 s, with maxServerPingDelay 10 s and timeout 5 s, with an RPC call sent 2 s ago and not answered
    When it is told it slept for 30 s
    Then the call's reply deadline is 3 s from now

  @E
  Scenario: A sleep while connecting keeps the reconnect delay
    Given a machine with minReconnectDelay 10000 ms whose first connection attempt failed
    And 6 s have elapsed
    When it is told it slept for 60 s
    Then its reconnect timer is unchanged

  @E
  Scenario: reconnect() while connected skips the backoff but keeps the jitter window
    Given 100 connected machines with different seeds, minReconnectDelay 5000 ms and a reconnect jitter window of 1000 ms
    When reconnect is called on each
    Then each closes its transport and emits "connecting" with code 6 and reason "reconnect called"
    And every next attempt is delayed by at most 1000 ms
    And the delays are not all equal

  @E
  Scenario: reconnect() while connecting starts an attempt at once and keeps waiting calls
    Given a machine with minReconnectDelay 10000 ms whose first connection attempt failed, with an RPC call waiting for the connection
    When reconnect is called
    Then it starts an attempt at once
    And it emits no event
    And the call is still waiting

  @E
  Scenario: reconnect() tries the first transport again
    Given a machine with transports websocket and http_stream, connected over http_stream after websocket failed
    When reconnect is called
    Then its next attempt opens a websocket transport

  @E
  Scenario: reconnect() does nothing to a disconnected client
    Given a machine disconnected by an explicit disconnect call
    When reconnect is called
    Then it returns no action

  @D
  Scenario: reconnect() reconnects at once without "disconnected"
    Given a client with minReconnectDelay 5000 ms connected to the fake server
    When reconnect is called
    Then the client emits "connecting" with code 6 and reason "reconnect called"
    And then within 3 s the client emits "connected"
    And the client never emitted "disconnected"

  @D
  Scenario: Publications not taken before reconnect() are recovered once
    Given a client subscribed to recoverable channel "news" on the fake server, whose subscription's stream has been read up to "subscribed"
    And the fake server has sent publications at offsets 1, 2 and 3 to "news"
    And the application has taken only the publication at offset 1 from the subscription's stream
    And the client holds the publications at offsets 2 and 3 not yet taken
    When reconnect is called
    And the fake server answers the subscribe command with the publications at offsets 2 and 3 recovered
    Then that subscribe command asked to recover from offset 1
    And the subscription's stream yields, in order, "subscribing", "subscribed", and the publications at offsets 2 and 3
    And no publication is yielded twice

  @D
  Scenario Outline: A captive portal answering the websocket upgrade is reported
    Given a fake server answering every websocket upgrade with HTTP status <status> and an HTML page
    When connect is called
    Then the client emits an "error" event for the failed transport attempt whose message ends with "a captive portal may be answering"
    And the client state is "connecting"
    And the client makes another connection attempt

    Examples:
      | status |
      | 200    |
      | 302    |
      | 511    |
