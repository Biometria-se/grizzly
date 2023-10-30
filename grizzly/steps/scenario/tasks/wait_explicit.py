"""
This module contains step implementations for the {@pylink grizzly.tasks.wait_explicit} task.
"""
from typing import cast

from grizzly.steps._helpers import is_template
from grizzly.types.behave import Context, then
from grizzly.context import GrizzlyContext
from grizzly.tasks import ExplicitWaitTask


@then(u'wait for "{wait_time_expression}" seconds')
def step_task_wait_seconds(context: Context, wait_time_expression: str) -> None:
    """
    Creates an instace of the {@pylink grizzly.tasks.wait_explicit} task. The scenario will wait the specified time (seconds) in
    additional to the wait time specified by {@pylink grizzly.tasks.wait_between}.

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

    if not is_template(wait_time_expression):
        try:
            assert float(wait_time_expression) > 0.0, 'wait time cannot be less than 0.0 seconds'
        except ValueError:
            raise AssertionError(f'"{wait_time_expression}" is not a template nor a float')

    grizzly.scenario.tasks.add(ExplicitWaitTask(time_expression=wait_time_expression))
