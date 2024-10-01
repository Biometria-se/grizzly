Feature: external only
  Background: Gemensamma egenskaper för alla scenarion
    Given spawn rate is "1.0" users per second

  Scenario: test
    Given "3" user of type "Dummy" load testing "dev://null"
    And repeat for "3" iterations
    And restart scenario on failure

    Then sleep in "3600.0"
    Then raise exception "RuntimeError"
