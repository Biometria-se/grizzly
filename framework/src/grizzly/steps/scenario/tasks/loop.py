"""Module contains step implementations for the [Loop][grizzly.tasks.loop] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import LoopTask
from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('loop "{values}" as variable "{variable}" with name "{name}"')
def step_task_loop_start(context: Context, values: str, variable: str, name: str) -> None:
    """Create an instance of the [Loop][grizzly.tasks.loop] tasks which executes all wrapped tasks with a value from the list `values`.

    `values` supports [templating][framework.usage.variables.templating] and **must** be a valid JSON list.

    See [Loop][grizzly.tasks.loop] task documentation for more information.

    Example:
    ```gherkin
    Then loop "{{ loop_values }}" as variable "loop_value" with name "test-loop"
    Then log message "loop_value={{ loop_value }}"
    Then end loop
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.loop is None, f'loop task "{grizzly.scenario.tasks.tmp.loop.name}" is already open, close it first'

    grizzly.scenario.tasks.tmp.loop = LoopTask(
        name=name,
        values=values,
        variable=variable,
    )


@then('end loop')
def step_task_loop_end(context: Context) -> None:
    """Close the [Loop][grizzly.tasks.loop] task created by [Start][grizzly.steps.scenario.tasks.loop.step_task_loop_start].

    This means that any following tasks specified will not be part of the loop.

    See [Loop][grizzly.tasks.loop] task documentation for more information.

    Example:
    ```gherkin
    Then loop "{{ loop_values }}" as variable "loop_value" with name "test-loop"
    Then log message "loop_value={{ loop_value }}"
    Then end loop
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.loop is not None, 'there are no open loop, you need to create one before closing it'

    loop = grizzly.scenario.tasks.tmp.loop
    grizzly.scenario.tasks.tmp.loop = None
    grizzly.scenario.tasks.add(loop)
