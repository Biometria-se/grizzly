"""
This module contains step implementations for the {@pylink grizzly.tasks.conditional} task.
"""
from typing import cast

from grizzly.types.behave import Context, then, when
from grizzly.context import GrizzlyContext
from grizzly.tasks import ConditionalTask


@when(u'condition "{condition}" with name "{name}" is true, execute these tasks')
def step_task_conditional_if(context: Context, condition: str, name: str) -> None:
    """
    Creates an instance of the {@pylink grizzly.tasks.conditional} task which executes different sets of task depending on `condition`.
    Also sets the task in a state that any following tasks will be run when `condition` is true.

    See {@pylink grizzly.tasks.conditional} task documentation for more information.

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
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is None, f'cannot create a new conditional while "{grizzly.scenario.tasks.tmp.conditional.name}" is still open'

    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name=name,
        condition=condition,
    )
    grizzly.scenario.tasks.tmp.conditional.switch(True)


@then(u'if condition is false, execute these tasks')
def step_task_conditional_else(context: Context) -> None:
    """
    Changes the state of {@pylink grizzly.tasks.conditional} task instance created by {@pylink grizzly.steps.scenario.tasks.step_task_conditional_if}
    so that any following tasks will be run when `condition` is false.

    See {@pylink grizzly.tasks.conditional} task documentation for more information.

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
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is not None, 'there are no open conditional, you need to create one first'

    grizzly.scenario.tasks.tmp.conditional.switch(False)


@then(u'end condition')
def step_task_conditional_end(context: Context) -> None:
    """
    Closes the {@pylink grizzly.tasks.conditional} task instance created by {@pylink grizzly.steps.scenario.tasks.step_task_conditional_if}.
    This means that any following tasks specified will not be part of the conditional.

    See {@pylink grizzly.tasks.conditional} task documentation for more information.

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
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.tasks.tmp.conditional is not None, 'there are no open conditional, you need to create one before closing it'

    conditional = grizzly.scenario.tasks.tmp.conditional
    grizzly.scenario.tasks.tmp.conditional = None
    grizzly.scenario.tasks.add(conditional)
