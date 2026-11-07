Feature: Tokio runtime handle

  Scenario: Building outside a runtime without a handle fails
    Given client options without a runtime handle
    When the client is built outside any tokio runtime
    Then building fails with a configuration error naming the runtime option

  @A
  Scenario: A client built outside a runtime with a handle connects
    Given a separate multi-thread tokio runtime
    And a client built outside any tokio runtime with that runtime's handle
    When connect is called
    Then the client emits "connecting"
    And then the client emits "connected"

  @D
  Scenario: The client ends when its runtime shuts down
    Given a separate multi-thread tokio runtime
    And a client built with that runtime's handle and connected to the fake server that never replies to commands
    And the client calls RPC "slow"
    And the fake server has received the RPC command
    When that runtime shuts down
    Then the call fails with a "client closed" error before the configured timeout
    And the client emits "disconnected" with code 4 and reason "runtime shut down"
    And the client's stream ends
    And creating a subscription fails because the client is closed
