from __future__ import annotations

from pathlib import Path

from grizzly_ls.server.commands import render_gherkin


def test_render_gherkin() -> None:
    feature_file = Path(__file__).parent.parent.parent.parent.parent / 'tests' / 'project' / 'features' / 'render.feature'

    assert feature_file.exists()

    content = feature_file.read_text()

    assert (
        content
        == """Feature: Template feature file
  Scenario: Template scenario
    # <!-- hello
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host$"

    # // -->

    {% scenario "first", feature="./first.inc.feature", foo="foo", baz="baz" %}
"""
    )

    assert (
        render_gherkin(feature_file.as_posix(), content)
        == """Feature: Template feature file
  Scenario: Template scenario
    # &lt;!-- hello
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host$"

    # // --&gt;

    Given value for variable "foo_bar" is "none"
    Given value for variable "foobaz" is "none"
      \"\"\"
      hello
      world
      \"\"\"
    Then log message "foo=foo"
      | foo | bar |
      | bar | foo |

    # &lt;!-- conditional steps --&gt;
    Then log message "{{ foobar }}"
""".rstrip()
    )
