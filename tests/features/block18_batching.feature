Feature: Batching

  @D
  Scenario Outline: Calls made while batching go out in one frame when the batch ends
    Given a client connected to the fake server
    When the application starts a batch, calls RPC "a", publishes {"n":1} to channel "news" and requests the history of channel "news"
    And 200 ms elapse
    Then the fake server has received no command since the batch started
    When the batch is <ended>
    Then the fake server receives one frame carrying the rpc, publish and history commands, in that order
    And each call returns the fake server's reply

    Examples:
      | ended   |
      | stopped |
      | dropped |

  @D
  Scenario: Subscriptions subscribed while batching send their subscribe commands in one frame
    Given a client connected to the fake server, with three subscriptions not subscribed
    When the application starts a batch and calls subscribe on the three subscriptions
    And 200 ms elapse
    Then the fake server has received no command since the batch started
    When the batch is stopped
    Then the fake server receives one frame carrying the three subscribe commands, in that order
    And the three subscriptions reach state "subscribed"

  @D
  Scenario: A batch over Protobuf is one binary frame
    Given a client configured with the Protobuf protocol, connected to the fake server
    When the application starts a batch, calls RPC "a" and RPC "b", and stops the batch
    Then the fake server receives one binary frame carrying the two rpc commands, varint-delimited, in that order
    And both calls return the fake server's reply

  @E
  Scenario Outline: A batch is split at the frame cap
    Given a connected client with a batch frame cap of 1000 bytes
    When the application starts a batch, makes calls whose commands encode to <sizes> bytes, and stops the batch
    Then the client sends frames carrying <frames> calls, in the order the calls were made
    And no frame carrying several commands exceeds 1000 bytes

    Examples:
      | sizes                   | frames  |
      | 300, 300, 300, 300, 300 | 3, 2    |
      | 300, 1500, 300          | 1, 1, 1 |

  @E
  Scenario: Held calls are admitted to the write queue one by one
    Given a connected client with a write queue of 1000 bytes and nothing unwritten
    When the application starts a batch, makes five calls whose commands encode to 300 bytes each, and stops the batch
    Then the client sends one frame carrying the first three calls
    And the client sends nothing more until that frame is reported written
    And then the fourth and fifth calls are sent, each in its own frame

  @E
  Scenario: A call held past the timeout fails and is never sent
    Given a connected client with a timeout of 5 s
    When the application starts a batch and calls RPC "a"
    And 5 s elapse
    Then the call fails with error code 1
    When the batch is stopped
    Then the client has sent no frame since the batch started

  @E
  Scenario: A subscribe held past the timeout reconnects the client
    Given a connected client with a timeout of 5 s and a subscription not subscribed
    When the application starts a batch and calls subscribe on the subscription
    And 5 s elapse
    Then the client has sent no frame since the batch started
    And the client emits "connecting" with code 3 and reason "subscribe timeout"

  @E
  Scenario: The client's own commands and send are not held
    Given a connected client with a token callback, whose connect reply asked for pongs and whose connection token expires
    When the application starts a batch
    And the server sends a ping
    And the application calls send with {"n":1}
    And the connection token's refresh time comes and the token callback answers
    Then the client sends a pong, the send command and a refresh command, each in its own frame, before the batch is stopped
    And the send call returns

  @E
  Scenario: Disconnect fails the calls a batch holds
    Given a connected client
    When the application starts a batch and calls RPC "a"
    And disconnect is called
    Then the call fails with error code 3
    When connect is called and the client reaches state "connected"
    And the batch is stopped
    Then the client has sent no rpc command

  @E
  Scenario: A held call waits across a lost transport
    Given a connected client
    When the application starts a batch and calls RPC "a"
    And the transport closes
    Then the call has not failed
    When the client reaches state "connected" on a new transport
    And the batch is stopped
    Then the rpc command is sent on the new transport
    And no rpc command was sent on the closed transport

  @E
  Scenario: Stopping one of two batches sends what is held
    Given a connected client
    When the application starts batches A and B and calls RPC "a"
    And batch A is stopped
    Then the client sends one frame carrying RPC "a"
    When the application calls RPC "b"
    Then the client sends no frame
    When batch B is stopped
    Then the client sends one frame carrying RPC "b"

  @E
  Scenario: A batch refused with HTTP 413 is resent command by command
    Given a client connected over an emulated transport, with a subscription not subscribed
    And the application started a batch, called RPC "a" and subscribe on the subscription, and stopped the batch
    When the server refuses the request carrying both commands with HTTP 413
    Then the call has not failed and the subscription is "subscribing"
    And the client sends two requests, carrying RPC "a" and then the subscribe command
    When the server refuses the request carrying RPC "a" with HTTP 413 and answers the subscribe command
    Then the call fails with error code 10
    And the subscription reaches state "subscribed"
    And the transport stays open

  @B
  Scenario Outline: Over an emulated transport only the command refused alone fails
    Given a client subscribed to channel "news" over transport "<transport>" through an HTTP proxy answering HTTP 413 to every emulation request over 2000 bytes
    When the application starts a batch, publishes a 2100-byte JSON string and then {"n":1} to channel "news", and stops the batch
    Then the HTTP proxy refused one request carrying both publish commands
    And the first publish fails with error code 10
    And the second publish succeeds
    And the subscription receives the publication {"n":1} and no other

    Examples:
      | transport   |
      | http_stream |
      | sse         |

  @E
  Scenario: A reconnect over an emulated transport sends its subscribe commands in one request
    Given a client core over http_stream with three subscriptions subscribed
    When the server closes the transport with a reconnect-advised code
    Then after the next connect reply the core sends one emulation request carrying the three subscribe commands
    And the three subscriptions reach state "subscribed"

  @E
  Scenario: The subscribe commands of a connect reply are split at the frame cap
    Given a client over http_stream with a batch frame cap of 1000 bytes and five subscriptions whose subscribe commands encode to 300 bytes each
    When connect is called and the server answers the connect command
    Then the client sends two frames, carrying three and then two subscribe commands

  @E
  Scenario: An open batch does not hold the subscribe commands of a connect reply
    Given a client over http_stream with a subscription in state "subscribing"
    And the application has started a batch
    When connect is called and the server answers the connect command
    Then the client sends the subscribe command while the batch is open

  @P
  Scenario: Seeded chaos with batching on the core keeps every invariant
    Given the core runner with the chaos configuration, a batch frame cap of 1 KiB, client-side and server-side subscriptions to recoverable channels and batching operations
    When seeds 0 to 299 each run 60 random operations against the simulated server with faults
    Then no seed violates invariants I1 to I10
    And the run counted batches stopped, frames of several commands, split batches and held calls that timed out, none of them 0

  @P
  Scenario Outline: The core runner reports a planted batching fault
    Given the core runner with batching operations, with <fault> planted
    When seeds 0 to 299 run
    Then the runner reports a violation of I10, naming the first seed that shows it and the operations up to it

    Examples:
      | fault                                                                  |
      | two frames of several commands merged by the tap past the frame cap    |
      | a batch stop reported to the checker one step after the client handled it |

  @P
  Scenario: Seeded chaos with batching on the tokio client keeps the invariants
    Given the adapter runner with the chaos configuration, a batch frame cap of 1 KiB, a tokio client, the fake server and batching operations
    When seeds 0 to 19 each run 30 random operations with faults
    Then no seed violates invariants I1, I2, I3, I5, I6 or I8 as the adapter checks them, and each reaches quiescence within 10 s
    And the run counted batches stopped and batches dropped, none of them 0
