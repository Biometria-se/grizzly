"""Module contains step implementations that creates requests executed by the [load user][grizzly.users]
in the scenario.
"""

from __future__ import annotations

from grizzly.steps._helpers import add_request_task
from grizzly.types import RequestDirection, RequestMethod
from grizzly.types.behave import Context, register_type, then
from grizzly.utils import has_template

register_type(
    Direction=RequestDirection.from_string,
    Method=RequestMethod.from_string,
)


@then('{method:Method} request with name "{name}" {direction:Direction} endpoint "{endpoint}"')
def step_task_request_text_with_name_endpoint(context: Context, method: RequestMethod, name: str, direction: RequestDirection, endpoint: str) -> None:
    """Create an instance of the [Request][grizzly.tasks.request] task, where optional payload is defined directly in the feature file.

    See [Request][grizzly.tasks.request] task documentation for more information about arguments.

    * If `Method` in the expression is `get` or `receive`; the `direction` **must** be `from`.

    * If `Method` in the expression is `post`, `pust`, or `send`; the `direction` **must** be `to`, and payload defined in the feature file.

    Example:
    ```gherkin
    Then post request with name "test-post" to endpoint "/api/test"
        \"\"\"
        {
            "test": "hello world"
        }
        \"\"\"
    Then put request with name "test-put" to endpoint "/api/test"
        \"\"\"
        {
            "test": "hello world"
        }
        \"\"\"
    Then get request with name "test-get" from endpoint "/api/test"

    Then send request with name "test-send" to endpoint "queue:receive-queue"
        \"\"\"
        {
            "value": "do something"
        }
        \"\"\"
    Then receive request with name "test-receive" from endpoint "queue:receive-queue"
    ```

    Args:
        method (Method): type of request, either of type "from" or "to"
        name (str): name of the requests in logs, can contain variables
        direction (RequestDirection): one of `to` or `from` depending on the value of `method`
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters

    """
    assert isinstance(direction, RequestDirection), 'invalid direction specified in expression'

    if method.direction == RequestDirection.FROM:
        assert context.text is None, f'step text is not allowed for {method.name}'
        assert direction == RequestDirection.FROM, f'"to endpoint" is not allowed for {method.name}, use "from endpoint"'
    elif method.direction == RequestDirection.TO:
        assert context.text is not None, f'step text is mandatory for {method.name}'
        assert direction == RequestDirection.TO, f'"from endpoint" is not allowed for {method.name}, use "to endpoint"'

    add_request_task(context, method=method, source=context.text, name=name, endpoint=endpoint)


@then('{method:Method} request "{source}" with name "{name}" to endpoint "{endpoint}"')
def step_task_request_file_with_name_endpoint(context: Context, method: RequestMethod, source: str, name: str, endpoint: str) -> None:
    """Create an instance of the [Request][grizzly.tasks.request] task, where the payload is defined in a template file.

    See [Request][grizzly.tasks.request] task documentation for more information about arguments.

    Example:
    ```gherkin
    Then send request "test/request.j2.json" with name "test-send" to endpoint "queue:receive-queue"
    Then post request "test/request.j2.json" with name "test-post" to endpoint "/api/test"
    Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
    ```

    Args:
        method (Method): type of "to" request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters

    """
    assert method.direction == RequestDirection.TO, f'{method.name} is not allowed'
    assert context.text is None, f'step text is not allowed for {method.name}'
    assert not has_template(source), 'source file cannot be a template'
    add_request_task(context, method=method, source=source, name=name, endpoint=endpoint)


@then('{method:Method} request "{source}" with name "{name}"')
def step_task_request_file_with_name(context: Context, method: RequestMethod, source: str, name: str) -> None:
    """Create an instance of the [Request][grizzly.tasks.request] task, with the same `endpoint` as the previous Request task, where the
    payload is defined in a template file.

    See [Request][grizzly.tasks.request] task documentation for more information about arguments.

    Example:
    ```gherkin
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2"

    # same as
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2" to endpoint "/api/test"
    ```

    Args:
        method (Method): type of "to" request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables

    """
    assert method.direction == RequestDirection.TO, f'{method.name} is not allowed'
    assert context.text is None, f'step text is not allowed for {method.name}'
    assert not has_template(source), 'source file cannot be a template'
    add_request_task(context, method=method, source=source, name=name)


@then('{method:Method} request with name "{name}"')
def step_task_request_text_with_name(context: Context, method: RequestMethod, name: str) -> None:
    """Create an instance of the [Request][grizzly.tasks.request] task, where optional payload is defined directly in the feature file.

    See [Request][grizzly.tasks.request] task documentation for more information about arguments.

    If `method` in the expression is `post`, `put` or `send` the payload in the request **must** be defined directly in the feature file after the step.
    This step is useful if `method` and `endpoint` are the same as previous request, but the payload should be different.

    Example:
    ```gherkin
    # example-1
    Then post request with name "test-post-1" to endpoint "/api/test"
        \"\"\"
        {
            "value": "hello world!"
        }
        \"\"\"
    Then post request with name "test-post-2"
        \"\"\"
        {
            "value": "i have good news!"
        }
        \"\"\"

    # same as example-1
    Then post request with name "test-post-1" to endpoint "/api/test"
        \"\"\"
        {
            "value": "hello world!"
        }
        \"\"\"
    Then post request with name "test-post-2" to endpoint "/api/test"
        \"\"\"
        {
            "value": "i have good news!"
        }
        \"\"\"

    # example-2
    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2"

    # same as example-2
    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2" from endpoint "/api/test"
    ```

    Args:
        method (Method): type of request, either "from" or "to"
        name (str): name of the requests in logs, can contain variables

    """
    if method.direction == RequestDirection.FROM:
        assert context.text is None, f'step text is not allowed for {method.name}'
    elif method.direction == RequestDirection.TO:
        assert context.text is not None, f'step text is mandatory for {method.name}'

    add_request_task(context, method=method, source=context.text, name=name)
