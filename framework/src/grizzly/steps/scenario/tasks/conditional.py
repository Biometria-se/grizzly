"""Module contains step implementations for the [Conditional][grizzly.tasks.conditional] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import ConditionalTask
from grizzly.types.behave import Context, then, when

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@when('condition "{condition}" with name "{name}" is true, execute these tasks')
def step_task_conditional_if(context: Context, condition: str, name: str) -> None:
    """Create an instance of the [Conditional][grizzly.tasks.conditional] task which executes different sets of task depending on `condition`.

    Also sets the task in a state that any following tasks will be run when `condition` is true.

    See [Conditional][grizzly.tasks.conditional] task documentation for more information.

    Example:
    ```gherkin
    When condition "{{ value | int > 0 }}" with name "value-conditional" is true, execute these tasks
    Then get request with name "get-when-true" from endpoint "/api/true"
    Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"
    But if condition is false, execute these tasks
    Then get request with name "get-when-false" from endpoint "/api/false"
    Then end condition
    ```

    Args:
        condition (str): jinja2 template that renders into a boolean
        name (str): name of the conditional, will be used in request statistics

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is None, f'cannot create a new conditional while "{grizzly.scenario.tasks.tmp.conditional.name}" is still open'

    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name=name,
        condition=condition,
    )
    grizzly.scenario.tasks.tmp.conditional.switch(pointer=True)


@then('if condition is false, execute these tasks')
def step_task_conditional_else(context: Context) -> None:
    """Change state of [Conditional][grizzly.tasks.conditional] task instance created by [Conditional if][grizzly.steps.scenario.tasks.conditional.step_task_conditional_if]
    so that any following tasks will be run when `condition` is false.

    See [Conditional][grizzly.tasks.conditional] task documentation for more information.

    Example:
    ```gherkin
    When condition "{{ value | int > 0 }}" with name "value-conditional" is true, execute these tasks
    Then get request with name "get-when-true" from endpoint "/api/true"
    Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"
    But if condition is false, execute these tasks
    Then get request with name "get-when-false" from endpoint "/api/false"
    Then end condition
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is not None, 'there are no open conditional, you need to create one first'

    grizzly.scenario.tasks.tmp.conditional.switch(pointer=False)


@then('end condition')
def step_task_conditional_end(context: Context) -> None:
    """Close the [Conditional][grizzly.tasks.conditional] task instance created by [Conditional if][grizzly.steps.scenario.tasks.conditional.step_task_conditional_if].

    This means that any following tasks specified will not be part of the conditional.

    See [Conditional][grizzly.tasks.conditional] task documentation for more information.

    Example:
    ```gherkin
    When condition "{{ value | int > 0 }}" with name "value-conditional" is true, execute these tasks
    Then get request with name "get-when-true" from endpoint "/api/true"
    Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"
    But if condition is false, execute these tasks
    Then get request with name "get-when-false" from endpoint "/api/false"
    Then end condition
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is not None, 'there are no open conditional, you need to create one before closing it'

    conditional = grizzly.scenario.tasks.tmp.conditional
    grizzly.scenario.tasks.tmp.conditional = None
    grizzly.scenario.tasks.add(conditional)
