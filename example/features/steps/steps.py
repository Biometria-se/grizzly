# noqa: D100, INP001
from __future__ import annotations

from typing import TYPE_CHECKING, cast

from behave import given, then
from custom import Task

from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.testdata.utils import create_context_variable
from grizzly.utils import merge_dicts

if TYPE_CHECKING:  # pragma: no cover
    from behave.runner import Context


@given('also log successful requests')
def step_log_all_requests(context: Context) -> None:
    """Step to explicit enable request logging.

    This step does the same as:

    ```gherkin
    And set context variable "log_all_requests" to "True"
    ```

    Usage:

    ```gherkin
    And log all requests
    ```
    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    context_variable = create_context_variable(grizzly, 'log_all_requests', 'True')
    grizzly.scenario.context = merge_dicts(grizzly.scenario.context, context_variable)


@then('send message "{data}"')
def step_send_message(context: Context, data: str) -> None:
    """Step to send a message.

    This step adds task steps.custom.Task to the scenario, which sends a message of type
    "example_message" from the server to the client, which will trigger the callback registered
    in `before_feature` in the projects `environment.py`.
    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(Task(data))
