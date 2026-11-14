Feature: Soak run

  @P
  Scenario: Hours of normal use keep resources flat and delivery exact
    Given a client with 10 subscriptions to recoverable channels, an expiring connection token and expiring subscription tokens, connected to a real Centrifugo
    When it runs for the soak duration, with one publication per second on each channel, a server-forced reconnect every 5 minutes and every token refresh due
    Then the median live heap of the last three checkpoints is within 1% of the median of the checkpoints at 32.5, 37.5 and 42.5 minutes, or within 64 KiB
    And at every checkpoint the channels API lists one client on each subscribed channel, its running timers are only the ping deadline, the connection refresh and one refresh per subscription, and no call is pending longer than the configured timeout
    And for each channel the offsets delivered are the channel's history offsets from the first "subscribed", each once, with no "subscribed" reporting was_recovering true and recovered false
    And the run counted at least 3 connection token refreshes, at least 3 subscription token refreshes and at least 40 reconnects
