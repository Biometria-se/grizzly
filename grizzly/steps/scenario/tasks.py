'''This module contains step implementations that describes requests sent by `user_class_name` targeting `host`.'''
from typing import cast
from urllib.parse import urlparse

from behave.runner import Context
from behave import register_type, then  # pylint: disable=no-name-in-module

from ..helpers import add_request_task
from ...types import RequestDirection, RequestMethod
from ...context import GrizzlyContext
from ...task import PrintTask, WaitTask, TransformerTask
from ...task.getter import getterof

from grizzly_extras.transformer import TransformerContentType


def parse_method(text: str) -> RequestMethod:
    return RequestMethod.from_string(text.strip())


def parse_direction(text: str) -> RequestDirection:
    return RequestDirection.from_string(text.strip())


register_type(
    Direction=parse_direction,
    Method=parse_method,
    ContentType=TransformerContentType.from_string,
)


@then(u'{method:Method} request with name "{name}" {direction:Direction} endpoint "{endpoint}"')
def step_task_request_text_with_name_to_endpoint(context: Context, method: RequestMethod, name: str, direction: RequestDirection, endpoint: str) -> None:
    '''Creates a named request to an endpoint on `host`, where optional payload is defined directly in the feature file.

    If `method` in the expression is `get` or `receive`; the `direction` **must** be `from`.
    If `method` in the expression is `post`, `pust`, or `send`; the `direction` **must** be `to`, and payload defined in the feature file.

    ```gherkin
    Then post request with name "test-post" to endpoint "/api/test"
        """
        {
            "test": "hello world"
        }
        """
    Then put request with name "test-post" to endpoint "/api/test"
        """
        {
            "test": "hello world"
        }
        """
    Then get request with name "test-get" from endpoint "/api/test"

    Then send request with name "test-send" to endpoint "queue:receive-queue"
        """
        {
            "value": "do something"
        }
        """
    Then receive request with name "test-receive" from endpoint "queue:receive-queue"
    ```

    `endpoint` has support for setting response content type as a parameter:

    ```gherkin
    Then receive request with name "test-receive" from endpoint "queue:receive-queue | content_type=xml"

    # same as
    Then receive request with name "test-receive" from endpoint "queue:receive-queue"
    And set response content type to "xml"
    ```

    `content_type` will be removed from the actual `endpoint` value.

    Args:
        method (RequestMethod): type of request
        name (str): name of the requests in logs, can contain variables
        direction (RequestDirection): one of `to` or `from` depending on the value of `method`
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
    '''

    assert isinstance(direction, RequestDirection), f'invalid direction specified in expression'

    if method.direction == RequestDirection.FROM:
        assert context.text is None, f'Step text is not allowed for {method.name}'
        assert direction == RequestDirection.FROM, f'"to endpoint" is not allowed for {method.name}, use "from endpoint"'
    elif method.direction == RequestDirection.TO:
        assert context.text is not None, f'Step text is mandatory for {method.name}'
        assert direction == RequestDirection.TO, f'"from endpoint" is not allowed for {method.name}, use "to endpoint"'

    add_request_task(context, method=method, source=context.text, name=name, endpoint=endpoint)


@then(u'{method:Method} request "{source}" with name "{name}" to endpoint "{endpoint}"')
def step_task_request_file_with_name_endpoint(context: Context, method: RequestMethod, source: str, name: str, endpoint: str) -> None:
    '''Creates a named request to an endpoint on `host`, where the payload is defined in a template file.

    ```gherkin
    Then send request "test/request.j2.json" with name "test-send" to endpoint "queue:receive-queue"
    Then post request "test/request.j2.json" with name "test-post" to endpoint "/api/test"
    Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
    ```

    `endpoint` has support for setting response content type as a parameter:

    ```gherkin
    Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test | content_type=json"

    # same as
    Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
    And set response content type to "application/json"
    ```

    `content_type` will be removed from the actual `endpoint` value.

    Args:
        method (RequestMethod): type of request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
    '''
    assert method.direction == RequestDirection.TO, f'{method.name} not allowed'
    assert context.text is None, f'Step text is not allowed for {method.name}'
    add_request_task(context, method=method, source=source, name=name, endpoint=endpoint)


@then(u'{method:Method} request "{source}" with name "{name}"')
def step_task_request_file_with_name(context: Context, method: RequestMethod, source: str, name: str) -> None:
    '''Creates a named request to the same endpoint as previous request, where the payload is defined in a template file.

    ```gherkin
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2"

    # same as
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2" to endpoint "/api/test"
    ```

    `endpoint` has support for setting response content type as a parameter:

    ```gherkin
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test | content_type=json"

    # same as
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    And set response content type to "application/json"
    ```

    `content_type` will be removed from the actual `endpoint` value.

    Args:
        method (RequestMethod): type of request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables
    '''
    assert method.direction == RequestDirection.TO, f'{method.name} not allowed'
    assert context.text is None, f'Step text is not allowed for {method.name}'
    add_request_task(context, method=method, source=source, name=name)


@then(u'{method:Method} request with name "{name}"')
def step_task_request_text_with_name(context: Context, method: RequestMethod, name: str) -> None:
    '''Creates a named request to the same endpoint as previous request, where optional payload is defined directly in the feature file.

    If `method` in the expression is `post`, `put` or `send` the payload in the request **must** be defined directly in the feature file after the step.
    This step is useful if `method` and `endpoint` are the same as previous request, but the payload should be different.

    ```gherkin
    Then post request with name "test-post-1" to endpoint "/api/test"
        """
        {
            "value": "hello world!"
        }
        """
    Then post request with name "test-post-2"
        """
        {
            "value": "i have good news!"
        }
        """

    # same as
    Then post request with name "test-post-1" to endpoint "/api/test"
        """
        {
            "value": "hello world!"
        }
        """
    Then post request with name "test-post-2" to endpoint "/api/test"
        """
        {
            "value": "i have good news!"
        }
        """

    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2"

    # same as
    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2" from endpoint "/api/test"
    ```

    Args:
        method (RequestMethod): type of request
        name (str): name of the requests in logs, can contain variables
    '''

    if method.direction == RequestDirection.FROM:
        assert context.text is None, f'Step text is not allowed for {method.name}'
    elif method.direction == RequestDirection.TO:
        assert context.text is not None, f'Step text is mandatory for {method.name}'

    add_request_task(context, method=method, source=context.text, name=name)


@then(u'wait for "{wait_time:f}" seconds')
def step_task_wait_seconds(context: Context, wait_time: float) -> None:
    '''Create an explicit wait (task) in the scenario. The scenario will wait the time specified (seconds) on top
    of what has been defined in `step_setup_wait_time`.

    ```gherkin
    And wait time inbetween requests is random between "1.5" and "2.5" seconds
    ...
    And wait for "1.5" seconds
    ```

    Above combinations of steps will result in a wait time between 3 and 4 seconds for the first request that is defined after the
    `And wait for...`-step.

    Args:
        wait_time (float): wait time in seconds
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert wait_time > 0.0, f'wait time cannot be less than 0.0 seconds'

    grizzly.scenario.add_task(WaitTask(time=wait_time))


@then(u'print message "{message}"')
def step_task_print_message(context: Context, message: str) -> None:
    '''Print a message in the scenario. Useful for visualizing values of variables.
    The message can be a jinja template, and any variables will be rendered at the time the task executes.

    ```gherkin
    And print message "context_variable='{{ context_variable }}'
    ```

    Args:
        message (str): message to print
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.add_task(PrintTask(message=message))

    if '{{' in message and '}}' in message:
        grizzly.scenario.orphan_templates.append(message)


@then(u'parse "{content}" as "{content_type:ContentType}" and save value of "{expression}" in variable "{variable}"')
def step_task_transform(context: Context, content: str, content_type: TransformerContentType, expression: str, variable: str) -> None:
    '''Parse (part of) a JSON object or a XML document and extract a specific value from that and save into a variable.

    This can be especially useful in combination with [`AtomicMessageQueue`](/grizzly/usage/variables/testdata/messagequeue/) variable.

    ```gherkin
    And value for variable "document_id" is "None"
    And value for variable "document_title" is "None"
    And value for variable "document" is "{\"document\": {\"id\": \"DOCUMENT_8843-1\", \"title\": \"TPM Report 2021\"}}"
    ...
    Then parse "{{ document }}" as "json" and save value of "$.document.id" in variable "document_id"
    Then parse "{{ document }}" as "json" and save value of "$.document.title" in variable "document_title"
    ```

    Args:
        contents (str): contents to parse, supports templating or a static string
        content_type (TransformerContentType): MIME type of `contents`
        expression (str): JSON or XPath expression for specific value in `contents`
        variable (str): name of variable to save value to, must have been initialized
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.add_task(TransformerTask(
        content=content,
        content_type=content_type,
        expression=expression,
        variable=variable,
    ))

    if '{{' in content and '}}' in content:
        grizzly.scenario.orphan_templates.append(content)


@then(u'get "{endpoint}" and save response in "{variable}"')
def step_task_get_endpoint(context: Context, endpoint: str, variable: str) -> None:
    '''Get information from another host or endpoint than the scenario is load testing and save the response in a variable.

    Task implementations are found in `grizzly.task.getter` and each implementation is looked up through the scheme in the
    specified endpoint. If the endpoint is a variable, one have to manually specify the endpoint scheme even though the
    resolved variable contains the scheme. In this case the manually specified scheme will be removed to the endpoint actually
    used by the task.

    ```gherkin
    Then get "https://www.example.org/example.json" and save response in "example_openapi"
    Then get "http://{{ endpoint }}" and save response in "endpoint_result"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        variable (str): name of, initialized, variable where response will be saved in
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    scheme = urlparse(endpoint).scheme

    assert scheme is not None and len(scheme) > 0, f'could not find scheme in "{endpoint}"'

    getter = getterof.available.get(scheme, None)

    assert getter is not None, f'no getter task registered for {scheme}'

    if '{{' in endpoint and '}}' in endpoint:
        grizzly.scenario.orphan_templates.append(endpoint)
        index = len(scheme) + 3
        endpoint = endpoint[index:]

    grizzly.scenario.add_task(getter(
        endpoint=endpoint,
        variable=variable,
    ))
