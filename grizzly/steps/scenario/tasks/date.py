"""@anchor pydoc:grizzly.steps.scenario.tasks.date Date
This module contains step implementations for the {@pylink grizzly.tasks.date} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import DateTask
from grizzly.types.behave import Context, then


@then('parse date "{value}" and save in variable "{variable}"')
def step_task_date(context: Context, value: str, variable: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.date} task.

    Parses a datetime string and transforms it according to specified arguments.

    See {@pylink grizzly.tasks.date} task documentation for more information about arguments.

    This step is useful when changes has to be made to a datetime representation during an iteration of a scenario.

    Example:
    ```gherkin
    ...
    And value for variable "date1" is "none"
    And value for variable "date2" is "none"
    And value for variable "date3" is "none"
    And value for variable "AtomicDate.test" is "now"

    Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"
    Then parse date "{{ AtomicDate.test }} | offset=-1D" and save in variable "date2"
    Then parse date "{{ datetime.now() }} | offset=1Y" and save in variable "date3"
    ```

    Args:
        value (str): datetime string and arguments
        variable (str): name of, initialized, variable where response will be saved in

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert variable in grizzly.state.variables, f'variable {variable} has not been initialized'

    grizzly.scenario.tasks.add(DateTask(
        value=value,
        variable=variable,
    ))
