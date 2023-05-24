'''
@anchor pydoc:grizzly.steps.background.setup Setup
This module contains step implementations that configures the load test scenario with parameters applicable for all scenarios.'''
import parse

from urllib.parse import urlparse
from typing import cast
from importlib import import_module
from inspect import signature

from grizzly_extras.text import permutation

from grizzly.types.locust import Environment, Message
from grizzly.types.behave import Context, given, register_type
from grizzly.types import MessageDirection
from grizzly.context import GrizzlyContext
from grizzly.utils import merge_dicts
from grizzly.testdata.utils import create_context_variable, resolve_variable


@parse.with_pattern(r'(client|server)', regex_group_count=1)
@permutation(vector=(True, True,))
def parse_message_direction(text: str) -> str:
    return text.strip()


register_type(
    MessageDirection=parse_message_direction,
)


@given(u'save statistics to "{url}"')
def step_setup_save_statistics(context: Context, url: str) -> None:
    '''Sets an URL where locust statistics should be sent.

    It has support for InfluxDB and Azure Application Insights endpoints.

    For InfluxDB the following format **must** be used:

    ``` plain
    influxdb://[<username>:<password>@]<hostname>[:<port>]/<database>?TargetEnviroment=<target environment>[&Testplan=<test plan>]
    [&TargetEnvironment=<target environment>][&ProfileName=<profile name>][&Description=<description>]
    ```

    For Azure Application Insights the following format **must** be used:

    ``` plain
    insights://?InstrumentationKey=<instrumentation key>&IngestionEndpoint=<ingestion endpoint>[&Testplan=<test plan>]
    insights://<ingestion endpoint>/?InstrumentationKey=<instrumentation key>[&Testplan=<test plan>]
    ```

    ``` gherkin
    And save statistics to "influxdb://grizzly:secret-password@influx.example.com/grizzly-statistics"
    And save statistics to "insights://?IngestionEndpoint=https://insights.example.com&Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "insights://insights.example.com/?Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "influxdb://$conf::statistics.username$:$conf::statistics.password$@influx.example.com/$conf::statistics.database$"
    ```

    Args:
        url (str): URL for statistics endpoint
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    url = cast(str, resolve_variable(grizzly, url))
    parsed = urlparse(url)

    assert parsed.scheme in ['influxdb', 'insights'], f'"{parsed.scheme}" is not a supported scheme'

    grizzly.setup.statistics_url = url


@given(u'log level is "{log_level}"')
def step_setup_log_level(context: Context, log_level: str) -> None:
    '''Configure log level for `grizzly`.

    Default value is `INFO`, by changing to `DEBUG` there is more information what `grizzly` is doing behind the curtains.

    ``` gherkin
    And log level is "DEBUG"
    ```

    Args:
        log_level (str): allowed values `INFO`, `DEBUG`, `WARNING` och `ERROR`
    '''
    assert log_level in ['INFO', 'DEBUG', 'WARNING', 'ERROR'], f'log level {log_level} is not supported'
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.setup.log_level = log_level


@given(u'run for maximum "{timespan}"')
def step_setup_run_time(context: Context, timespan: str) -> None:
    '''Configures the time period a headless test should run for.
    If available test data is infinite, the test will run forever if this step is not used.

    ``` gherkin
    And run for maximum "1h"
    ```

    Args:
        timespan (str): description of how long the test should run for, e.g. 10s, 1h, 40m etc.
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.setup.timespan = timespan


@given(u'set global context variable "{variable}" to "{value}"')
def step_setup_set_global_context_variable(context: Context, variable: str, value: str) -> None:
    '''Create a global variable in the context. Depending on which type of user a scenario is configured for, different variables
    are available. Check `grizzly.users` documentation for which context variables are available for each user.

    This step can be used if the feature file has multiple scenarios and all of them have the same context variables.

    Variable names can contain (one ore more) dot (`.`) or slash (`/`) to indicate that the variable is in a structure. All names will also be
    converted to lower case.

    E.g. `token.url` and `token/URL` results in:

    ``` json
    {
        'token': {
            'url': '<value>'
        }
    }
    ```

    Space in variable names is also allowed and will then be translated to an underscore (`_`)

    E.g. `Client ID` results in `client_id`.

    ``` gherkin
    And set global context variable "token.url" to "http://example.com/api/auth"
    And set global context variable "token/client_id" to "aaaa-bbbb-cccc-dddd"
    And set global context variable "token/client secret" to "aasdfasdfasdf=="
    And set global context variable "token.resource" to "0000-aaaaaaa-1111-1111-1111"
    And set global context variable "log_all_requests" to "True"
    And set global context variable "validate_certificates" to "False"
    And set global context variable "run_id" to "13"
    ```

    Data type of values will be guessed, if not explicitly specified by the type of variable used (`Atomic*`). E.g. the last two
    examples above will result in:

    ``` json
    {
        'validate_certificates': False,
        'run_id': 13
    }
    ```

    Args:
        variable (str): variable name, as used in templates
        value (str): variable value
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    context_variable = create_context_variable(grizzly, variable, value)

    grizzly.setup.global_context = merge_dicts(grizzly.setup.global_context, context_variable)


@given(u'add callback "{callback_name}" for message type "{message_type}" from {from_node:MessageDirection} to {to_node:MessageDirection}')
def step_setup_message_type_callback(context: Context, callback_name: str, message_type: str, from_node: str, to_node: str) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert from_node != to_node, f'cannot register message handler that sends from {from_node} and is received at {to_node}'

    message_direction = MessageDirection.from_string(f'{from_node}_{to_node}')

    module_name, callback_name = callback_name.rsplit('.', 1)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        raise AssertionError(f'no module named {e.name}')

    callback = getattr(module, callback_name, None)

    assert callback is not None, f'module {module_name} has no method {callback_name}'
    assert callable(callback), f'{module_name}.{callback_name} is not a method'

    method_signature = signature(callback)
    parameters = method_signature.parameters
    parameter_names = list(method_signature.parameters.keys())

    correct_signature = (
        len(parameter_names) >= 2
        and parameter_names[0] == 'environment'
        and issubclass(parameters['environment'].annotation, Environment)
        and parameter_names[1] == 'msg'
        and issubclass(parameters['msg'].annotation, Message)
        and method_signature.return_annotation is None
    )

    assert correct_signature, f'{module_name}.{callback_name} does not have grizzly.types.MessageCallback method signature: {method_signature}'

    grizzly.setup.locust.messages.register(message_direction, message_type, callback)
