"""Module contains step implementations that configures the load test scenario with parameters applicable for all scenarios."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, cast, get_type_hints
from urllib.parse import urlparse

import parse
from grizzly_common.text import permutation

from grizzly.testdata.utils import resolve_variable
from grizzly.types import MessageDirection
from grizzly.types.behave import Context, given, register_type
from grizzly.types.locust import Environment, Message

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@parse.with_pattern(r'(client|server)', regex_group_count=1)
@permutation(vector=(True, True))
def parse_message_direction(text: str) -> str:
    return text.strip()


register_type(
    MessageDirection=parse_message_direction,
)


@given('save statistics to "{url}"')
def step_setup_save_statistics(context: Context, url: str) -> None:
    """Set an URL where locust statistics should be sent.

    It has support for InfluxDB and Azure Application Insights endpoints.

    For InfluxDB the following format **must** be used:

    ```plain
    influxdb://[<username>:<password>@]<hostname>[:<port>]/<database>?TargetEnviroment=<target environment>[&Testplan=<test plan>]
    [&TargetEnvironment=<target environment>][&ProfileName=<profile name>][&Description=<description>]
    ```

    For Azure Application Insights the following format **must** be used:

    ```plain
    insights://?InstrumentationKey=<instrumentation key>&IngestionEndpoint=<ingestion endpoint>[&Testplan=<test plan>]
    insights://<ingestion endpoint>/?InstrumentationKey=<instrumentation key>[&Testplan=<test plan>]
    ```

    Example:
    ```gherkin
    And save statistics to "influxdb://grizzly:secret-password@influx.example.com/grizzly-statistics"
    And save statistics to "insights://?IngestionEndpoint=https://insights.example.com&Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "insights://insights.example.com/?Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "influxdb://$conf::statistics.username$:$conf::statistics.password$@influx.example.com/$conf::statistics.database$"
    ```

    Args:
        url (str): URL for statistics endpoint

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    url = cast('str', resolve_variable(grizzly.scenario, url))
    parsed = urlparse(url)

    assert parsed.scheme in ['influxdb', 'influxdb2', 'insights'], f'"{parsed.scheme}" is not a supported scheme'

    grizzly.setup.statistics_url = url


@given('log level is "{log_level}"')
def step_setup_log_level(context: Context, log_level: str) -> None:
    """Configure log level for `grizzly`.

    Default value is `INFO`, by changing to `DEBUG` there is more information what `grizzly` is doing behind the curtains.

    Example:
    ```gherkin
    And log level is "DEBUG"
    ```

    Args:
        log_level (str): one of `INFO`, `DEBUG`, `WARNING`, `ERROR`

    """
    assert log_level in ['INFO', 'DEBUG', 'WARNING', 'ERROR'], f'log level {log_level} is not supported'
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.setup.log_level = log_level


@given('run for maximum "{timespan}"')
def step_setup_run_time(context: Context, timespan: str) -> None:
    """Configure the time period a headless test should run for.

    If available test data is infinite, the test will run forever if this step is not used.

    Example:
    ```gherkin
    And run for maximum "1h"
    ```

    Args:
        timespan (str): description of how long the test should run for, e.g. 10s, 1h, 40m etc.

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.setup.timespan = timespan


@given('register callback "{callback_name}" for message type "{message_type}" from {from_node:MessageDirection} to {to_node:MessageDirection}')
def step_setup_message_type_callback(context: Context, callback_name: str, message_type: str, from_node: str, to_node: str) -> None:
    """Register a custom callback function for a custom message type, that should be sent from client/server to client/server (exclusive).

    ```python title="steps/custom.py"
    def my_custom_callback(env: Environment, msg: Message) -> None:
        print(msg)
    ```

    ```gherkin
    Given register callback "steps.custom.my_custom_callback" for message type "custom_msg" from client to server
    ```

    Args:
        callback_name (str): full namespace and method name to the callback function
        message_type (str): unique name of the message
        from_node (MessageDirection): server or client, exclusive
        to_node (MessageDirection): client or server, exclusive

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert from_node != to_node, f'cannot register message handler that sends from {from_node} and is received at {to_node}'

    message_direction = MessageDirection.from_string(f'{from_node}_{to_node}')

    module_name, callback_name = callback_name.rsplit('.', 1)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        message = f'no module named {e.name}'
        raise AssertionError(message) from e

    callback = getattr(module, callback_name, None)

    assert callback is not None, f'module {module_name} has no method {callback_name}'
    assert callable(callback), f'{module_name}.{callback_name} is not a method'

    method_signature = get_type_hints(callback)

    assert method_signature == {
        'environment': Environment,
        'msg': Message,
        'return': None.__class__,
    }, f'{module_name}.{callback_name} does not have grizzly.types.MessageCallback method signature: {method_signature}'

    grizzly.setup.locust.messages.register(message_direction, message_type, callback)


@given('value for configuration "{name}" is "{value}"')
def step_setup_configuration_value(context: Context, name: str, value: str) -> None:
    """Step to set configuration variables not present in specified environment file.

    The configuration value can then be used in the following steps. If the specified `name` already exists, it will be
    overwritten.

    Example:
    ```gherkin
    Given value for configuration "default.host" is "example.com"
    ...
    Then log message "default.host=$conf::default.host$"
    ```

    Args:
        name (str): dot separated name/path of configuration value
        value (str): configuration value, any `$..$` variables are resolved, but `{{ .. }}` templates are kept

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    resolved_value = resolve_variable(grizzly.scenario, value, try_template=False)

    grizzly.state.configuration.update({name: resolved_value})


@given('wait "{timeout:g}" seconds until spawning is complete')
def step_setup_wait_spawning_complete_timeout(context: Context, timeout: float) -> None:
    """Step to make scenarios wait with execution of tasks until spawning is complete, at most `timeout` seconds.

    This is when there are dependencies between scenarios. This will make all scenarios to wait until all defined
    users are spawned.

    Example:
    ```gherkin
    Given wait "13.37" seconds until spawning is complete
    ```

    Args:
        timeout (float): number of seconds to wait until locust spawning is complete

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.setup.wait_for_spawning_complete = timeout


@given('wait until spawning is complete')
def step_setup_wait_spawning_complete_indefinitely(context: Context) -> None:
    """Step to make scenarios wait with execution until spawning is complete, without timeout.

    This is when there are dependencies between scenarios. This will make all scenarios to wait until all defined
    users are spawned.

    Example:
    ```gherkin
    Given wait until spawning is complete
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.setup.wait_for_spawning_complete = -1
