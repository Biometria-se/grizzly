'''This module contains step implementations that creates requests executed by the specified {@pylink grizzly.users} in the scenario.'''
from typing import cast

from behave.runner import Context
from behave import register_type, then, given  # pylint: disable=no-name-in-module

from .._helpers import add_request_task, get_task_client, is_template
from ...types import RequestDirection, RequestMethod
from ...context import GrizzlyContext
from ...tasks import LogMessage, WaitTask, TransformerTask, UntilRequestTask, DateTask, AsyncRequestGroupTask, TimerTask, RequestWaitTask

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


@then(u'{method:Method} request with name "{name}" from endpoint "{endpoint}" until "{condition}"')
def step_task_request_with_name_endpoint_until(context: Context, method: RequestMethod, name: str, endpoint: str, condition: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.until} task, see task documentation for more information.

    Example:

    ``` gherkin
    Then get request with name "test-get" from endpoint "/api/test | content_type=json" until "$.`this`[?success==true]"
    Then receive request with name "test-receive" from endpoint "queue:receive-queue | content_type=xml" until "/header/success[. == 'True']"
    ```

    Args:
        method (RequestMethod): type of request
        name (str): name of the requests in logs, can contain variables
        direction (RequestDirection): one of `to` or `from` depending on the value of `method`
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
    '''

    assert method.direction == RequestDirection.FROM, 'this step is only valid for request methods with direction FROM'
    assert context.text is None, 'this step does not have support for step text'

    request_tasks = add_request_task(context, method=method, source=context.text, name=name, endpoint=endpoint, in_scenario=False)

    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.async_group is None, f'until tasks cannot be in an async request group, close group {grizzly.scenario.async_group.name} first'

    for request_task, substitues in request_tasks:
        condition_rendered = condition
        for key, value in substitues.items():
            condition_rendered = condition_rendered.replace(f'{{{{ {key} }}}}', value)

        grizzly.scenario.tasks.add(UntilRequestTask(
            grizzly,
            request=request_task,
            condition=condition_rendered,
        ))


@then(u'{method:Method} request with name "{name}" {direction:Direction} endpoint "{endpoint}"')
def step_task_request_text_with_name_endpoint(context: Context, method: RequestMethod, name: str, direction: RequestDirection, endpoint: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request} task, where optional payload is defined directly in the feature file.
    See {@pylink grizzly.tasks.request} task documentation for more information about arguments.

    * If `method` in the expression is `get` or `receive`; the `direction` **must** be `from`.

    * If `method` in the expression is `post`, `pust`, or `send`; the `direction` **must** be `to`, and payload defined in the feature file.

    Example:

    ``` gherkin
    Then post request with name "test-post" to endpoint "/api/test"
        """
        {
            "test": "hello world"
        }
        """
    Then put request with name "test-put" to endpoint "/api/test"
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

    Args:
        method (RequestMethod): type of request
        name (str): name of the requests in logs, can contain variables
        direction (RequestDirection): one of `to` or `from` depending on the value of `method`
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
    '''

    assert isinstance(direction, RequestDirection), 'invalid direction specified in expression'

    if method.direction == RequestDirection.FROM:
        assert context.text is None, f'step text is not allowed for {method.name}'
        assert direction == RequestDirection.FROM, f'"to endpoint" is not allowed for {method.name}, use "from endpoint"'
    elif method.direction == RequestDirection.TO:
        assert context.text is not None, f'step text is mandatory for {method.name}'
        assert direction == RequestDirection.TO, f'"from endpoint" is not allowed for {method.name}, use "to endpoint"'

    add_request_task(context, method=method, source=context.text, name=name, endpoint=endpoint)


@then(u'{method:Method} request "{source}" with name "{name}" to endpoint "{endpoint}"')
def step_task_request_file_with_name_endpoint(context: Context, method: RequestMethod, source: str, name: str, endpoint: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request} task, where the payload is defined in a template file.

    See {@pylink grizzly.tasks.request} task documentation for more information about arguments.

    Example:

    ``` gherkin
    Then send request "test/request.j2.json" with name "test-send" to endpoint "queue:receive-queue"
    Then post request "test/request.j2.json" with name "test-post" to endpoint "/api/test"
    Then put request "test/request.j2.json" with name "test-put" to endpoint "/api/test"
    ```

    Args:
        method (RequestMethod): type of request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables
        endpoint (str): URI relative to `host` in the scenario, can contain variables and in certain cases `user_class_name` specific parameters
    '''
    assert method.direction == RequestDirection.TO, f'{method.name} is not allowed'
    assert context.text is None, f'step text is not allowed for {method.name}'
    assert not is_template(source), 'source file cannot be a template'
    add_request_task(context, method=method, source=source, name=name, endpoint=endpoint)


@then(u'{method:Method} request "{source}" with name "{name}"')
def step_task_request_file_with_name(context: Context, method: RequestMethod, source: str, name: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request} task, with the same `endpoint` as the previous Request task, where the
    payload is defined in a template file.

    See {@pylink grizzly.tasks.request} task documentation for more information about arguments.

    Example:

    ``` gherkin
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2"

    # same as
    Then post request "test/request1.j2.json" with name "test-post1" to endpoint "/api/test"
    Then post request "test/request2.j2.json" with name "test-post2" to endpoint "/api/test"
    ```

    Args:
        method (RequestMethod): type of request
        source (str): path to a template file relative to the directory `requests/`, which **must** exist in the directory the feature file is located
        name (str): name of the requests in logs, can contain variables
    '''
    assert method.direction == RequestDirection.TO, f'{method.name} is not allowed'
    assert context.text is None, f'step text is not allowed for {method.name}'
    assert not is_template(source), 'source file cannot be a template'
    add_request_task(context, method=method, source=source, name=name)


@then(u'{method:Method} request with name "{name}"')
def step_task_request_text_with_name(context: Context, method: RequestMethod, name: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request} task, where optional payload is defined directly in the feature file.

    See {@pylink grizzly.tasks.request} task documentation for more information about arguments.

    If `method` in the expression is `post`, `put` or `send` the payload in the request **must** be defined directly in the feature file after the step.
    This step is useful if `method` and `endpoint` are the same as previous request, but the payload should be different.

    Example:

    ``` gherkin
    # example-1
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

    # same as example-1
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

    # example-2
    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2"

    # same as example-2
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
    '''
    Creates an instace of the {@pylink grizzly.tasks.wait} task. The scenario will wait the specified time (seconds) in
    additional to the wait time specified by {@pylink grizzly.tasks.request_wait}.

    See {@pylink grizzly.tasks.wait} task documentation for more information about the task.

    Example:

    ``` gherkin
    And wait "1.5..2.5" seconds between tasks
    ...
    Then wait for "1.5" seconds
    ```

    Above combinations of steps will result in a wait time between 3 and 4 seconds for the first {@pylink grizzly.tasks} that is defined after the
    `Then wait for...`-step.

    Args:
        wait_time (float): wait time in seconds
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert wait_time > 0.0, 'wait time cannot be less than 0.0 seconds'

    grizzly.scenario.tasks.add(WaitTask(time=wait_time))


@then(u'log message "{message}"')
def step_task_log_message(context: Context, message: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.log_message} task. Prints a log message in the console, useful for troubleshooting values
    of variables or set markers in log files.

    The message supports {@link framework.usage.variables.templating}. See {@pylink grizzly.tasks.log_message} task documentation for more
    information about the task.

    Example:

    ``` gherkin
    And log message "context_variable='{{ context_variable }}'
    ```

    Args:
        message (str): message to print
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(LogMessage(message=message))


@then(u'parse "{content}" as "{content_type:ContentType}" and save value of "{expression}" in variable "{variable}"')
def step_task_transform(context: Context, content: str, content_type: TransformerContentType, expression: str, variable: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.transformer} task. Transforms the specified `content` with `content_type` to an
    object that an transformer can extract information from with the specified `expression`.

    See {@pylink grizzly.tasks.transformer} task documentation for more information about the task.

    Example:

    ``` gherkin
    And value for variable "document_id" is "None"
    And value for variable "document_title" is "None"
    And value for variable "document" is "{\"document\": {\"id\": \"DOCUMENT_8843-1\", \"title\": \"TPM Report 2021\"}}"
    ...
    Then parse "{{ document }}" as "json" and save value of "$.document.id" in variable "document_id"
    Then parse "{{ document }}" as "json" and save value of "$.document.title" in variable "document_title"
    ```

    Args:
        contents (str): contents to parse, supports {@link framework.usage.variables.templating} or a static string
        content_type (TransformerContentType): MIME type of `contents`
        expression (str): JSON or XPath expression for specific value in `contents`
        variable (str): name of variable to save value to, must have been initialized
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(TransformerTask(
        grizzly,
        content=content,
        content_type=content_type,
        expression=expression,
        variable=variable,
    ))


@then(u'get "{endpoint}" with name "{name}" and save response in "{variable}"')
def step_task_client_get_endpoint(context: Context, endpoint: str, name: str, variable: str) -> None:
    '''
    Creates an instance of a {@pylink grizzly.tasks.clients} task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`. Gets information from another host or endpoint than the scenario
    is load testing and saves the response in a variable.

    See {@pylink grizzly.tasks.clients} task documentation for more information about client tasks.

    Example:

    ``` gherkin
    Then get "https://www.example.org/example.json" with name "example-1" and save response in "example_openapi"
    Then get "http://{{ endpoint }}" with name "example-2" and save response in "endpoint_result"
    ```

    Args:
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        variable (str): name of, initialized, variable where response will be saved in
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    task_client = get_task_client(endpoint)

    grizzly.scenario.tasks.add(task_client(
        RequestDirection.FROM,
        endpoint,
        name,
        variable=variable,
    ))


@then(u'put "{source}" to "{endpoint}" with name "{name}" as "{destination}"')
def step_task_client_put_endpoint_file_destination(context: Context, source: str, endpoint: str, name: str, destination: str) -> None:
    '''
    Creates an instance of a {@pylink grizzly.tasks.clients} task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`. Puts information, source being a file, to another host or endpoint than the scenario
    is load testing and saves the response in a variable

    See {@pylink grizzly.tasks.clients} task documentation for more information about client tasks.

    Example:

    ``` gherkin
    Then put "test-file.json" to "bs://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "upload-file" as "uploaded-test-file.json"
    ```

    Args:
        source (str): relative path to file in `feature/requests`, supports {@link framework.usage.variables.templating}
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
        destination (str): name of source on the destination
    '''
    assert context.text is None, 'step text is not allowed for this step expression'

    grizzly = cast(GrizzlyContext, context.grizzly)

    task_client = get_task_client(endpoint)

    assert not is_template(source), 'source file cannot be a template'

    grizzly.scenario.tasks.add(task_client(
        RequestDirection.TO,
        endpoint,
        name,
        source=source,
        destination=destination,
    ))


@then(u'put "{source}" to "{endpoint}" with name "{name}"')
def step_task_client_put_endpoint_file(context: Context, source: str, endpoint: str, name: str) -> None:
    '''
    Creates an instance of a {@pylink grizzly.tasks.clients} task, actual implementation of the task is determined
    based on the URL scheme specified in `endpoint`. Puts information, source being a file, to another host or endpoint than the scenario
    is load testing and saves the response in a variable

    See {@pylink grizzly.tasks.clients} task documentation for more information about client tasks.

    Example:

    ``` gherkin
    Then put "test-file.json" to "bs://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "upload-file"
    ```

    Args:
        source (str): relative path to file in `feature/requests`, supports {@link framework.usage.variables.templating}
        endpoint (str): information about where to get information, see the specific getter task implementations for more information
        name (str): name of the request, used in request statistics
    '''
    assert context.text is None, 'step text is not allowed for this step expression'

    grizzly = cast(GrizzlyContext, context.grizzly)

    task_client = get_task_client(endpoint)

    assert not is_template(source), 'source file cannot be a template'

    grizzly.scenario.tasks.add(task_client(
        RequestDirection.TO,
        endpoint,
        name,
        source=source,
        destination=None,
    ))


@then(u'parse date "{value}" and save in variable "{variable}"')
def step_task_date(context: Context, value: str, variable: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.date} task. Parses a datetime string and transforms it according to specified
    arguments.

    See {@pylink grizzly.tasks.date} task documentation for more information about arguments.

    This step is useful when changes has to be made to a datetime representation during an iteration of a scenario.

    Example:

    ``` gherkin
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
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert variable in grizzly.state.variables, f'variable {variable} has not been initialized'

    grizzly.scenario.tasks.add(DateTask(
        value=value,
        variable=variable,
    ))


@given(u'an async request group with name "{name}"')
def step_task_async_group_start(context: Context, name: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.async_group} task. All {@pylink grizzly.tasks.request} tasks created after this step will be added to the
    request group, until the group is closed.

    See {@pylink grizzly.tasks.async_group} task documentation for more information.

    Example:

    ``` gherkin
    Given an async request group with name "async-group-1"
    Then post request with name "test-post-2" to endpoint "/api/test"
        """
        {
            "value": "i have good news!"
        }
        """

    Then get request with name "test-get-1" from endpoint "/api/test"
    And close async request group
    ```

    In this example, the `put` and `get` requests will run asynchronously, and both requests will block following requests until both are finished.

    Args:
        name (str): name of the group, will be used in locust statistics
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.async_group is None, f'async request group "{grizzly.scenario.async_group.name}" has not been closed'

    grizzly.scenario.async_group = AsyncRequestGroupTask(name=name)


@then(u'close async request group')
def step_task_async_group_close(context: Context) -> None:
    '''
    Closes the instance created in {@pylink grizzly.steps.scenario.tasks.step_task_async_group_start}, and adds the {@pylink grizzly.tasks.async_group} task to the list of tasks
    that the scenario is going to execute.

    See {@pylink grizzly.tasks.async_group} task documentation for more information.

    Example:

    ``` gherkin
    Given an async request group with name "async-group-1"
    Then post request with name "test-post-2" to endpoint "/api/test"
        """
        {
            "value": "i have good news!"
        }
        """

    Then get request with name "test-get-1" from endpoint "/api/test"
    And close async request group
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert grizzly.scenario.async_group is not None, 'no async request group is open'
    assert len(grizzly.scenario.async_group.requests) > 0, f'there are no requests in async group "{grizzly.scenario.async_group.name}"'

    grizzly.scenario.tasks.add(grizzly.scenario.async_group)
    grizzly.scenario.async_group = None


@then(u'start timer with name "{name}"')
def step_task_timer_start(context: Context, name: str) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.timer} task. Starts a timer to measure the "request time" for all tasks between
    the start and stop of the timer.

    See {@pylink grizzly.tasks.timer} task documentation for more information.

    Example:

    ``` gherkin
    Then start timer with name "parsing-xml"
    ...
    And stop timer with name "parsing-xml"
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert name not in grizzly.scenario.timers, f'timer with name {name} has already been defined'

    task = TimerTask(name=name)

    grizzly.scenario.timers.update({
        name: task,
    })

    grizzly.scenario.tasks.add(task)


@then(u'stop timer with name "{name}"')
def step_task_timer_stop(context: Context, name: str) -> None:
    '''
    Adds the instance created by {@pylink grizzly.steps.scenario.tasks.step_task_timer_start} to the list of scenario tasks.

    See {@pylink grizzly.tasks.timer} task documentation for more information.

    Example:

    ``` gherkin
    Then start timer with name "parsing-xml"
    ...
    And stop timer with name "parsing-xml"
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    task = grizzly.scenario.timers.get(name, None)

    assert task is not None, f'timer with name {name} has not been defined'

    grizzly.scenario.tasks.add(task)
    grizzly.scenario.timers.update({name: None})


@given(u'wait "{min_time:g}..{max_time:g}" seconds between tasks')
def step_task_request_wait_between(context: Context, min_time: float, max_time: float) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request_wait} task. Sets number of, randomly, seconds the {@pylink grizzly.users}
    will wait between executing each task.

    See {@pylink grizzly.tasks.request_wait} task documentation for more information.

    Example:

    ``` gherkin
    And wait "1.4..1.7" seconds between tasks
    # wait between 1.4 and 1.7 seconds
    Then get request with name "test-get-1" from endpoint "..."
    # wait between 1.4 and 1.7 seconds
    Then get request with name "test-get-2" from endpoint "..."
    # wait between 1.4 and 1.7 seconds
    And wait "0.1" seconds between tasks
    # wait 0.1 seconds
    Then get request with name "test-get-3" from endpoint "..."
    # wait 0.1 seconds
    ...
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    if min_time > max_time:
        min_time, max_time = max_time, min_time

    grizzly.scenario.tasks.add(RequestWaitTask(min_time=min_time, max_time=max_time))


@given(u'wait "{time:g}" seconds between tasks')
def step_task_request_wait_constant(context: Context, time: float) -> None:
    '''
    Creates an instance of the {@pylink grizzly.tasks.request_wait} task. Sets number of, constant, seconds the {@pylink grizzly.users}
    will wait between executing each task.

    See {@pylink grizzly.tasks.request_wait} task documentation for more information.

    Example:

    ``` gherkin
    And wait "1.4" seconds between tasks
    # wait 1.4 seconds
    Then get request with name "test-get-1" from endpoint "..."
    # wait 1.4 seconds
    Then get request with name "test-get-2" from endpoint "..."
    # wait 1.4 seconds
    And wait "0.1" seconds between tasks
    # wait 0.1 seconds
    Then get request with name "test-get-3" from endpoint "..."
    # wait 0.1 seconds
    ...
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(RequestWaitTask(time))
