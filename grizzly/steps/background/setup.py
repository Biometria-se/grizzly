'''This module contains step implementations that configures the load test scenario with parameters applicable for all scenarios.'''

from urllib.parse import urlparse, parse_qs, urlunparse
from typing import cast, List

from behave import given  # pylint: disable=no-name-in-module
from behave.runner import Context

from ...context import GrizzlyContext
from ...utils import merge_dicts
from ...testdata.utils import create_context_variable, resolve_variable


@given(u'save statistics to "{url}"')
def step_setup_save_statistics(context: Context, url: str) -> None:
    '''Sets an URL where locust statistics should be sent.

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

    ```gherkin
    And save statistics to "influxdb://grizzly:secret-password@influx.example.com/grizzly-statistics"
    And save statistics to "insights://?IngestionEndpoint=https://insights.example.com&Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "insights://insights.example.com/?Testplan=grizzly-statistics&InstrumentationKey=asdfasdfasdf="
    And save statistics to "influxdb://$conf::statistics.username:$conf::statistics.password@influx.example.com/$conf::statistics.database"
    ```

    Args:
        url (str): URL for statistics endpoint
    '''
    parsed = urlparse(url)

    assert parsed.scheme in ['influxdb', 'insights'], f'"{parsed.scheme}" is not a supported scheme'

    grizzly = cast(GrizzlyContext, context.grizzly)

    paths: List[str] = []

    for path in parsed.path.split('/'):
        resolved = cast(str, resolve_variable(grizzly, path))
        paths.append(resolved)

    parsed = parsed._replace(path='/'.join(paths))

    variables = parse_qs(parsed.query)

    parameters: List[str] = []

    for variable in variables:
        resolved = cast(str, resolve_variable(grizzly, variables[variable][0]))
        parameters.append(f'{variable}={resolved}')

    parsed = parsed._replace(query='&'.join(parameters))

    # bug in urlunparse, // is not added if netloc is empty
    if len(parsed.netloc) == 0:
        host = ' '
    elif '@' in parsed.netloc:
        credentials, host = parsed.netloc.split('@')
        host = cast(str, resolve_variable(grizzly, host))
        credentials = credentials.replace('::', '%%')
        username, password = credentials.split(':', 1)
        username = cast(str, resolve_variable(grizzly, username.replace('%%', '::')))
        password = cast(str, resolve_variable(grizzly, password.replace('%%', '::')))
        host = f'{username}:{password}@{host}'
    else:
        host = cast(str, resolve_variable(grizzly, parsed.netloc))

    parsed = parsed._replace(netloc=host)

    url = urlunparse(parsed)

    # normalize bug fix above
    if ':// ' in url:
        url = url.replace(':// ', '://')

    grizzly.setup.statistics_url = url


@given(u'log level is "{log_level}"')
def step_setup_log_level(context: Context, log_level: str) -> None:
    '''Configure log level for `grizzly`.

    Default value is `INFO`, by changing to `DEBUG` there is more information what `grizzly` is doing behind the curtains.

    ```gherkin
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

    ```gherkin
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

    ```json
    {
        'token': {
            'url': '<value>'
        }
    }
    ```

    Space in variable names is also allowed and will then be translated to an underscore (`_`)

    E.g. `Client ID` results in `client_id`.

    ```gherkin
    And set global context variable "token.url" to "http://test.nu/api/auth"
    And set global context variable "token/client_id" to "aaaa-bbbb-cccc-dddd"
    And set global context variable "token/client secret" to "aasdfasdfasdf=="
    And set global context variable "token.resource" to "0000-aaaaaaa-1111-1111-1111"
    And set global context variable "log_all_requests" to "True"
    And set global context variable "validate_certificates" to "False"
    And set global context variable "run_id" to "13"
    ```

    Data type of values will be guessed, if not explicitly specified by the type of variable used (`Atomic*`). E.g. the last two
    examples above will result in:

    ```json
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
