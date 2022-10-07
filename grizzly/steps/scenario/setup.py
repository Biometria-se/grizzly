'''This module contains step implementations that setup the load test scenario with parameters that is going to be used in the scenario they are defined in.'''
from typing import Optional, cast

import parse

from behave.runner import Context
from behave import register_type, given, then  # pylint: disable=no-name-in-module
from locust.exception import StopUser

from grizzly_extras.text import permutation

from ...context import GrizzlyContext
from ...testdata.utils import create_context_variable, resolve_variable
from ...utils import merge_dicts
from ...exceptions import RestartScenario
from .._helpers import is_template
from ...tasks import RequestTask, GrizzlyTask


@parse.with_pattern(r'(iteration[s]?)')
@permutation(vector=(False, True,))
def parse_iteration_gramatical_number(text: str) -> str:
    return text.strip()


register_type(
    IterationGramaticalNumber=parse_iteration_gramatical_number,
)


@given(u'set context variable "{variable}" to "{value}"')
def step_setup_set_context_variable(context: Context, variable: str, value: str) -> None:
    '''Sets a variable in the scenario context.

    Variable names can contain (one or more) dot (`.`) or slash (`/`) to indicate that the variable has a nested structure. E.g. `token.url`
    and `token/url` results in `{'token': {'url': '<value'>}}`

    It is also possible to have spaces in a variable names, they will then be replaced with underscore (`_`), and the name will be
    converted to lowercase.

    E.g. `Client ID` results in `client_id`.

    Example:

    ``` gherkin
    And set context variable "token.url" to "https://example.com/api/auth"
    And set context variable "token/client_id" to "aaaa-bbbb-cccc-dddd"
    And set context variable "token/client secret" to "aasdfasdfasdf=="
    And set context variable "token.resource" to "0000-aaaaaaa-1111-1111-1111"
    And set context variable "log_all_requests" to "True"
    And set context variable "validate_certificates" to "False"
    ```

    Args:
        variable (str): name, can contain `.` and `/`
        value (str): value, data type will be guessed and casted
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    context_variable = create_context_variable(grizzly, variable, value)

    grizzly.scenario.context = merge_dicts(grizzly.scenario.context, context_variable)


@given(u'repeat for "{value}" {iteration_number:IterationGramaticalNumber}')
def step_setup_iterations(context: Context, value: str, iteration_number: str) -> None:
    '''Sets how many iterations of the {@pylink grizzly.tasks} in the scenario should execute.

    Default value is `1`. A value of `0` means to run until all test data is consumed, or that the (optional) specified
    runtime for the scenario is reached.

    Example:

    ``` gherkin
    And repeat for "10" iterations
    And repeat for "1" iteration
    And value for variable "leveranser" is "100"
    And repeat for "{{ leveranser * 0.25 }}" iterations
    ```

    Args:
        value (str): number of iterations of the scenario, can be a templatning string or a environment configuration variable
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    should_resolve = is_template(value) or value[0] == '$'
    iterations = max(int(round(float(resolve_variable(grizzly, value)), 0)), 0)

    if is_template(value):
        grizzly.scenario.orphan_templates.append(value)

    if should_resolve and iterations < 1:
        iterations = 1

    grizzly.scenario.iterations = iterations


@given(u'value for variable "{name}" is "{value}"')
def step_setup_variable_value(context: Context, name: str, value: str) -> None:
    '''Use this step to initialize a variable that should have the same [start] value for every run of
    the scenario.

    Data type for the value of the variable is based on the type of variable. If the variable is an testdata {@pylink grizzly.testdata.variables}
    then the value needs to match the format and type that the variable has implemented. If it is not a testdata variable
    `grizzly` will try to guess the data type. E.g.:

    * `"10"` becomes `int`

    * `"1.0"` becomes `float`

    * `"True"` becomes `bool`

    * everything else becomes `str`

    It is also possible to set the value of a variable based on another variable, which can be usable if you have a variable in
    multiple scenarios which all should have the same initial value.

    Example:

    ``` gherkin title="example.feature"
    Feature:
        Background:
            And ask for value of variable "messageID"
        Scenario:
            And value for variable "HelloWorld" is "default"
            And value for variable "AtomicIntegerIncrementer.mid1" is "{{ messageID }}"
            And value for variable "AtomicIntegerIncrementer.persistent" is "1 | step=10, persist=True"
    ```

    If the file `features/persistent/example.json` (name of feature file and `feature` extension replaced with `json`) exists, and contains an entry for
    the variable, the initial value will be read from the file and override the value specified in the feature file.

    Args:
        name (str): variable name
        value (Any): initial value
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert name not in grizzly.state.variables, f'variable "{name}" has already been set'

    try:
        # data type will be guessed when setting the variable
        if name not in grizzly.state.persistent:
            resolved_value = resolve_variable(grizzly, value, guess_datatype=False)
            if is_template(value):
                grizzly.scenario.orphan_templates.append(value)
        else:
            resolved_value = grizzly.state.persistent[name]

        grizzly.state.variables[name] = resolved_value
    except ValueError as e:
        raise AssertionError(str(e))


@given(u'set alias "{alias}" for variable "{variable}"')
def step_setup_set_variable_alias(context: Context, alias: str, variable: str) -> None:
    '''Creates an alias for a variable that points to another structure in the context.

    This is useful if you have test data that somehow should change the behavior for a
    user, e.g. username and password.

    Example:

    ``` gherkin
    And value for variable "AtomicCsvRow.users" is "users.csv | repeat=True"
    And set alias "auth.user.username" for variable "AtomicCsvRow.users.username"
    And set alias "auth.user.password" for variable "AtomicCsvRow.users.password"
    ```

    Variables in payload templates are not allowed to have an alias.

    Args:
        alias (str): which node in the context that should get the value of `variable`
        variable (str): an already initialized variable that should be renamed
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)

    if variable.count('.') > 1:
        base_variable = '.'.join(variable.split('.')[:2])
    else:
        base_variable = variable

    assert base_variable in grizzly.state.variables, f'variable {base_variable} has not been declared'
    assert variable not in grizzly.state.alias, f'alias for variable {variable} already exists: {grizzly.state.alias[variable]}'

    grizzly.state.alias[variable] = alias


@given(u'log all requests')
def step_setup_log_all_requests(context: Context) -> None:
    '''Sets if all requests should be logged to a file.

    By default only failed requests (and responses) will be logged.

    Example:

    ``` gherkin
    And log all requests
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.context['log_all_requests'] = True


@given(u'stop user on failure')
def step_setup_stop_user_on_failure(context: Context) -> None:
    '''Stop user if a request fails.

    Default behavior is to continue the scenario if a request fails.

    Example:

    ``` gherkin
    And stop user on failure
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.failure_exception = StopUser
    context.config.stop = True


@given(u'restart scenario on failure')
def step_setup_restart_scenario_on_failure(context: Context) -> None:
    '''Restart scenario, from first task, if a request fails.

    Default behavior is to continue the scenario if a request fails.

    Example:

    ``` gherkin
    And restart scenario on failure
    ```
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.failure_exception = RestartScenario
    context.config.stop = False


@then(u'metadata "{key}" is "{value}"')
@given(u'metadata "{key}" is "{value}"')
def step_setup_metadata(context: Context, key: str, value: str) -> None:
    '''Set a metadata (header) value to be used by the user when sending requests.

    Example:

    ``` gherkin
    And metadata "Content-Type" is "application/xml"
    And metadata "Ocp-Apim-Subscription-Key" is "9asdf00asdf00adsf034"
    ```

    Or, for use in one request only, specify metadata after the request:
    ``` gherkin
    Then post request ...
    And metadata "x-header" is "{{ value }}"
    ```
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    casted_value = resolve_variable(grizzly, value)

    previous_task: Optional[GrizzlyTask] = None
    if len(grizzly.scenario.tasks) > 0:
        previous_task = grizzly.scenario.tasks[-1]

    if isinstance(previous_task, RequestTask):
        previous_task.add_metadata(key, value)
    else:
        if grizzly.scenario.context.get('metadata', None) is None:
            grizzly.scenario.context['metadata'] = {}

        grizzly.scenario.context['metadata'].update({key: casted_value})
