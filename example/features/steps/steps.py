from behave.runner import Context
from behave import given  # pylint: disable=no-name-in-module

from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.context import LocustContext
from grizzly.utils import merge_dicts


@given(u'also log successful requests')
def step_log_all_requests(context: Context) -> None:
    '''This step does the same as:

    ```gherkin
    And set context variable "log_all_requests" to "True"
    ```

    Usage:

    ```gherkin
    And log all requests
    ```
    '''
    context_locust = cast(LocustContext, context.locust)
    context_variable = create_context_variable(context_locust, 'log_all_requests', 'True')
    context_locust.scenario.context = merge_dicts(context_locust.scenario.context, context_variable)
