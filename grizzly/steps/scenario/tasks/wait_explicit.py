"""@anchor pydoc:grizzly.steps.scenario.tasks.wait_explicit Explicit wait
This module contains step implementations for the {@pylink grizzly.tasks.wait_explicit} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import ExplicitWaitTask
from grizzly.testdata.utils import resolve_parameters
from grizzly.types.behave import Context, then
from grizzly.utils import has_parameter, has_template


@then('wait for "{wait_time_expression}" seconds')
def step_task_wait_explicit(context: Context, wait_time_expression: str) -> None:
    """Create an instace of the {@pylink grizzly.tasks.wait_explicit} task.

    The scenario will wait the specified time (seconds) in additional to the wait time specified
    by {@pylink grizzly.tasks.wait_between}.

    See {@pylink grizzly.tasks.wait_explicit} task documentation for more information about the task.

    Example:
    ```gherkin
    And ask for value of variable "wait_time"
    And wait "1.5..2.5" seconds between tasks
    ...
    Then wait for "1.5" seconds
    ...
    Then wait for "{{ wait_time }}" seconds
    ```

    Above combinations of steps will result in a wait time between 3 and 4 seconds for the first {@pylink grizzly.tasks} that is defined after the
    `Then wait for...`-step.

    Args:
        wait_time (float): wait time in seconds

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    if not has_template(wait_time_expression):
        try:
            assert float(wait_time_expression) > 0.0, 'wait time cannot be less than 0.0 seconds'
        except ValueError as e:
            message = f'"{wait_time_expression}" is not a template nor a float'
            raise AssertionError(message) from e

    if has_parameter(wait_time_expression):
        wait_time_expression = resolve_parameters(grizzly, wait_time_expression)

    grizzly.scenario.tasks.add(ExplicitWaitTask(time_expression=wait_time_expression))
