"""Module contains step implementations for [client][grizzly.tasks.clients] tasks."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.steps._helpers import get_task_client
from grizzly.types import RequestDirection, RequestMethod
from grizzly.types.behave import Context, then
from grizzly.utils import has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('{method:Method} from "{endpoint}" with name "{name}" and save response payload in "{payload_variable}" and metadata in "{metadata_variable}"')
def step_task_client_from_endpoint_payload_and_metadata(context: Context, method: RequestMethod, endpoint: str, name: str, payload_variable: str, metadata_variable: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`.

    Get information from another host or endpoint than the scenario is load testing and saves the response in a variable.

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then get from "https://www.example.org/example.json" with name "example-1" and save response payload in "example_openapi" and metadata in "example_metadata"
    Then get from "http://{{ endpoint }}" with name "example-2" and save response payload in "endpoint_result" and metadata in "result_metadata"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        payload_variable (str): name of, initialized, variable where response payload will be saved in
        metadata_variable (str): name of, initialized, variable where response metadata will be saved in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert method.direction == RequestDirection.FROM, 'chosen request method does not match direction "from"'

    grizzly.scenario.tasks.add(
        get_task_client(grizzly, endpoint)(
            RequestDirection.FROM,
            endpoint,
            name,
            payload_variable=payload_variable,
            metadata_variable=metadata_variable,
            text=context.text,
            method=method,
        ),
    )


@then('{method:Method} from "{endpoint}" with name "{name}" and save response payload in "{variable}"')
def step_task_client_from_endpoint_payload(context: Context, method: RequestMethod, endpoint: str, name: str, variable: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`.

    Get information from another host or endpoint than the scenario is load testing and saves the response in a variable.

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then get from "https://www.example.org/example.json" with name "example-1" and save response payload in "example_openapi"
    Then get from "http://{{ endpoint }}" with name "example-2" and save response payload in "endpoint_result"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        variable (str): name of, initialized, variable where response payload will be saved in

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert method.direction == RequestDirection.FROM, 'chosen request method does not match direction "from"'

    grizzly.scenario.tasks.add(
        get_task_client(grizzly, endpoint)(
            RequestDirection.FROM,
            endpoint,
            name,
            payload_variable=variable,
            metadata_variable=None,
            text=context.text,
            method=method,
        ),
    )


@then('{method:Method} "{source}" to "{endpoint}" with name "{name}" as "{destination}"')
def step_task_client_to_endpoint_file_destination(context: Context, method: RequestMethod, source: str, endpoint: str, name: str, destination: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is
    determined based on the URL scheme specified in `endpoint`.

    Put information, source being a file, to another host or endpoint than the scenario is load testing
    and saves the response in a variable

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then put "test-file.json" to "bs://my-storage/my-container?AccountKey=aaaabbb=" with name "upload-file" as "uploaded-test-file.json"
    ```

    Args:
        source (str): relative path to file in `feature/requests`, supports [templating][framework.usage.variables.templating]
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        destination (str): name of source on the destination

    """
    assert context.text is None, 'step text is not allowed for this step expression'
    assert not has_template(source), 'source file cannot be a template'
    assert method.direction == RequestDirection.TO, 'chosen request method does not match direction "to"'

    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenario.tasks.add(
        get_task_client(grizzly, endpoint)(
            RequestDirection.TO,
            endpoint,
            name,
            source=source,
            destination=destination,
            method=method,
        ),
    )


@then('{method:Method} "{source}" to "{endpoint}" with name "{name}"')
def step_task_client_to_endpoint_file(context: Context, method: RequestMethod, source: str, endpoint: str, name: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`.

    Put information, source being a file, to another host or endpoint than the scenario
    is load testing and saves the response in a variable

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then put "test-file.json" to "bs://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "upload-file"
    ```

    Args:
        source (str): relative path to file in `feature/requests`, supports [templating][framework.usage.variables.templating]
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics

    """
    assert context.text is None, 'step text is not allowed for this step expression'
    assert not has_template(source), 'source file cannot be a template'
    assert method.direction == RequestDirection.TO, 'chosen request method does not match direction "to"'

    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenario.tasks.add(
        get_task_client(grizzly, endpoint)(
            RequestDirection.TO,
            endpoint,
            name,
            source=source,
            destination=None,
            method=method,
        ),
    )


@then('{method:Method} to "{endpoint}" with name "{name}"')
def step_task_client_to_endpoint_text(context: Context, method: RequestMethod, endpoint: str, name: str) -> None:
    """Create an instance of a [client][grizzly.tasks.clients] task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`.

    Put information, source step text, to another host or endpoint than the scenario
    is load testing and saves the response in a variable

    See [client][grizzly.tasks.clients] task documentation for more information about client tasks.

    Example:
    ```gherkin
    Then put to "https://api.example.com/v2/echo" with name "put-request"
      \"\"\"
      hello world
      \"\"\"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics

    """
    assert context.text is not None, 'step text is mandatory for this step expression'
    assert len(context.text) > 0, 'step text cannot be an empty string'
    assert method.direction == RequestDirection.TO, 'chosen request method does not match direction "to"'

    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenario.tasks.add(
        get_task_client(grizzly, endpoint)(
            RequestDirection.TO,
            endpoint,
            name,
            source=context.text,
            destination=None,
            method=method,
        ),
    )
