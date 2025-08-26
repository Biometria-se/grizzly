Feature: Template feature file
  Scenario: Template scenario
    # <!-- hello
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host$"

    # // -->

    {% scenario "first", feature="./first.inc.feature", foo="foo", baz="baz" %}
