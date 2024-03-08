"""@anchor pydoc:grizzly.steps.scenario.tasks.loop Loop
This module contains step implementations for the {@pylink grizzly.tasks.loop} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import LoopTask
from grizzly.types.behave import Context, then


@then('loop "{values}" as variable "{variable}" with name "{name}"')
def step_task_loop_start(context: Context, values: str, variable: str, name: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.loop} tasks which executes all wrapped tasks with a value from the list `values`.

    `values` **must** be a valid JSON list and supports {@link framework.usage.variables.templating}.

    See {@pylink grizzly.tasks.loop} task documentation for more information.

    Example:
    ```gherkin
    Then loop "{{ loop_values }}" as variable "loop_value" with name "test-loop"
    Then log message "loop_value={{ loop_value }}"
    Then end loop
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.tasks.tmp.loop is None, f'loop task "{grizzly.scenario.tasks.tmp.loop.name}" is already open, close it first'

    grizzly.scenario.tasks.tmp.loop = LoopTask(
        name=name,
        values=values,
        variable=variable,
    )


@then('end loop')
def step_task_loop_end(context: Context) -> None:
    """Close the {@pylink grizzly.tasks.loop} task created by {@pylink grizzly.steps.scenario.tasks.loop.step_task_loop_start}.

    This means that any following tasks specified will not be part of the loop.

    See {@pylink grizzly.tasks.loop} task documentation for more information.

    Example:
    ```gherkin
    Then loop "{{ loop_values }}" as variable "loop_value" with name "test-loop"
    Then log message "loop_value={{ loop_value }}"
    Then end loop
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.tasks.tmp.loop is not None, 'there are no open loop, you need to create one before closing it'

    loop = grizzly.scenario.tasks.tmp.loop
    grizzly.scenario.tasks.tmp.loop = None
    grizzly.scenario.tasks.add(loop)
