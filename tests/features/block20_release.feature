Feature: Release setup

  Scenario: The changelog is in step with the crate's version
    Given the crate's manifest
    Then CHANGELOG.md begins with an Unreleased section, then version sections from newest to oldest, each headed by its version and a date
    And the manifest's version is the newest of those versions, or above it

  Scenario Outline: The release check refuses a tag that does not name the crate's version
    Given the crate's manifest
    When the release check runs for tag "<tag>"
    Then it <outcome>

    Examples:
      | tag                                 | outcome                                   |
      | v followed by its version           | succeeds                                  |
      | v0.0.0                              | fails, naming the version the tag must be |
      | its version without the v           | fails, naming the version the tag must be |
      | v followed by its version and -rc.1 | fails, naming the version the tag must be |

  Scenario: The release check refuses a version without a changelog section
    Given a copy of the crate whose CHANGELOG.md has no section for its version
    When the release check runs for tag "v" followed by its version
    Then it fails, naming the missing section

  Scenario: The published package holds only what the crate needs
    Given the crate's manifest
    When the files of its package are listed
    Then each is under src or proto, or is build.rs, Cargo.toml, Cargo.toml.orig, Cargo.lock, .cargo_vcs_info.json, README.md, LICENSE, CHANGELOG.md or SECURITY.md
    And build.rs, proto/client.proto, proto/LICENSE, LICENSE, README.md, CHANGELOG.md and SECURITY.md are among them

  @A
  Scenario: The quickstart example receives a publication
    Given a server
    When the quickstart example runs against the server's websocket endpoint
    And it prints that it is subscribed to its channel
    And the server API publishes {"text":"hello"} to that channel
    Then the example prints {"text":"hello"}
