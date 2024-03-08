"""@anchor pydoc:grizzly.steps.scenario.tasks.log_message Log message
This module contains step implementations for the {@pylink grizzly.tasks.log_message} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import LogMessageTask
from grizzly.types.behave import Context, then


@then('log message "{message}"')
def step_task_log_message(context: Context, message: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.log_message} task.

    Prints a log message in the console, useful for troubleshooting values of variables or set markers in log files.

    The message supports {@link framework.usage.variables.templating}. See {@pylink grizzly.tasks.log_message} task documentation for more
    information about the task.

    Example:
    ```gherkin
    And log message "context_variable='{{ context_variable }}'
    ```

    Args:
        message (str): message to print

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(LogMessageTask(message=message))
