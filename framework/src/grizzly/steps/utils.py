"""Module contains steps that can be useful during development or troubleshooting of a
feature file, but should not be included in a finished, testable, feature.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('fail')
@then('fail scenario')
def step_utils_fail_scenario(context: Context, *_args: Any, **_kwargs: Any) -> None:  # noqa: ARG001
    """Force a failed scenario. Can be useful when writing a new scenario. The scenario will fail before `locust` has started, so only when
    the scenario is setup.

    Example:
    ```gherkin
    Then fail
    ```

    """
    message = 'manually failed'
    raise AssertionError(message)


@then('add orphan template "{template}"')
def step_utils_add_orphan_template(context: Context, template: str) -> None:
    """Add arbitrary templats to fool grizzly that a variable is being used.

    This step should be avoided at all costs, escpecially if you do not know what you are doing.
    There are cases that grizzly will complain that a variable is declared, but not found in any templates, with this step it is possible to
    "fool" that logic, by adding a template containing that variable.

    When adding this step in a feature-file, you should also write an [bug issue](https://github.com/Biometria-se/grizzly/issues/new?assignees=&labels=bug&projects=&template=bug_report.md&title=)
    detailing why it was necessary to use it, so the root cause can be fixed.

    Example:
    ```gherkin
    Then add orphan template "{{ hello world foobar }}"
    ```

    Args:
        template (str): templating string with jinja2 template containing variable names

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    if not context.step.in_background:
        grizzly.scenario.orphan_templates.append(template)
    else:
        for scenario in grizzly.scenarios:
            scenario.orphan_templates.append(template)
