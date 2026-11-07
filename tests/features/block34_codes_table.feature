Feature: Table of codes

  Scenario: Every code constant has one row with its value
    Given the crate's codes module
    Then the codes documentation has exactly one row for each code constant, giving its value

  Scenario: The table names no code the crate lacks
    Given the crate's codes module
    Then every constant a row of the codes documentation or the README names is a code constant of the crate
    And the rows of each table name constants of one kind

  Scenario: The README's table is the documentation's
    Given the crate's codes module and its README
    Then the README's rows are the rows of the codes documentation, in the same order and cell for cell

  Scenario: Every quoted reason is one the client sends
    Given the crate's codes module
    Then every reason a row quotes occurs as a string literal in the crate's source outside the codes module
