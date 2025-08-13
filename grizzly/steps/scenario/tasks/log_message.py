"""Module contains step implementations for the [Log message][grizzly.tasks.log_message] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import LogMessageTask
from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('log message "{message}"')
def step_task_log_message_print(context: Context, message: str) -> None:
    """Create an instance of the [Log message][grizzly.tasks.log_message] task.

    Prints a log message in the console, useful for troubleshooting values of variables or set markers in log files.
    The message supports [templating][framework.usage.variables.templating].

    See [Log message][grizzly.tasks.log_message] task documentation for more information.

    Example:
    ```gherkin
    And log message "context_variable='{{ context_variable }}'
    ```

    Args:
        message (str): message to print

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(LogMessageTask(message=message))
