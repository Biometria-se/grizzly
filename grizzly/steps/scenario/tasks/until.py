"""Module contains step implementations for the [Until][grizzly.tasks.until] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps._helpers import add_request_task, get_task_client
from grizzly.tasks import UntilRequestTask
from grizzly.types import RequestDirection, RequestMethod
from grizzly.types.behave import Context, register_type, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

register_type(
    Method=RequestMethod.from_string,
)


@then('{method:Method} request with name "{name}" from endpoint "{endpoint}" until "{condition}"')
def step_task_request_with_name_endpoint_until(context: Context, method: RequestMethod, name: str, endpoint: str, condition: str) -> None:
    """Create an instance of the [Until][grizzly.tasks.until] task to be executed by the [load user][grizzly.users].

    See [Until][grizzly.tasks.until] task documentation for more information.

    Example:
    ```gherkin
    Then get request with name "test-get" from endpoint "/api/test | content_type=json" until "$.`this`[?success==true]"
    Then receive request with name "test-receive" from endpoint "queue:receive-queue | content_type=xml" until "/header/success[. == 'True']"
    ```

    Args:
        method (Method): type of "from" request
        name (str): name of the requests in logs, can contain variables
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
        condition (str): JSON or XPath expression for specific value in response payload

    """
    assert method.direction == RequestDirection.FROM, 'this step is only valid for request methods with direction FROM'
    assert context.text is None, 'this step does not have support for step text'

    request_tasks = add_request_task(context, method=method, source=context.text, name=name, endpoint=endpoint, in_scenario=False)

    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.async_group is None, f'until tasks cannot be in an async request group, close group {grizzly.scenario.tasks.tmp.async_group.name} first'

    for request_task, substitues in request_tasks:
        condition_rendered = condition
        for key, value in substitues.items():
            condition_rendered = condition_rendered.replace(f'{{{{ {key} }}}}', value)

        grizzly.scenario.tasks.add(
            UntilRequestTask(
                request=request_task,
                condition=condition_rendered,
            ),
        )


@then('get from "{endpoint}" with name "{name}" until "{condition}"')
def step_task_client_get_endpoint_until(context: Context, endpoint: str, name: str, condition: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`.

    Gets information, repeated from another host or endpoint than the scenario is load testing until the response
    matches `expression`.

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then get from "https://www.example.org/example.json" with name "example-1" until "$.response[status='Success']
    Then get from "http://{{ endpoint }}" with name "example-2" until "//*[@status='Success']"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        condition (str): JSON or XPath expression for specific value in response payload

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    client_request = get_task_client(grizzly, endpoint)(
        RequestDirection.FROM,
        endpoint,
        name,
        text=context.text,
        method=RequestMethod.GET,
    )

    grizzly.scenario.tasks.add(
        UntilRequestTask(
            request=client_request,
            condition=condition,
        ),
    )
