from typing import cast

from behave.runner import Context
from behave import given, then  # pylint: disable=no-name-in-module

from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F401
from grizzly.context import GrizzlyContext
from grizzly.utils import merge_dicts
from grizzly.testdata.utils import create_context_variable

from custom import Task  # pylint: disable=import-error


@given(u'also log successful requests')
def step_log_all_requests(context: Context) -> None:
    '''This step does the same as:

    ``` gherkin
    And set context variable "log_all_requests" to "True"
    ```

    Usage:

    ``` gherkin
    And log all requests
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    context_variable = create_context_variable(grizzly, 'log_all_requests', 'True')
    grizzly.scenario.context = merge_dicts(grizzly.scenario.context, context_variable)


@then(u'send message "{data}"')
def step_send_message(context: Context, data: str) -> None:
    '''This step adds task steps.custom.Task to the scenario, which sends a message of type
    "example_message" from the server to the client, which will trigger the callback registered
    in `before_feature` in the projects `environment.py`.
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(Task(data))
