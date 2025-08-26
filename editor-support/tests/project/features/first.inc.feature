Feature:
  Scenario: first
    Given value for variable "{$ foo $}_bar" is "none"
    Given value for variable "foo{$ baz $}" is "none"
      """
      hello
      world
      """
    {% scenario "second", feature="./second.inc.feature", bar="foo", condition=True %}
