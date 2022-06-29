'''This module contains step implementations that setup the load test scenario with parameters that is going to be used in the scenario they are defined in.'''
from typing import cast

import parse

from behave.runner import Context
from behave import register_type, given  # pylint: disable=no-name-in-module
from locust.exception import StopUser

from grizzly_extras.text import permutation

from ...context import GrizzlyContext
from ...testdata.utils import create_context_variable, resolve_variable
from ...utils import merge_dicts
from ...exceptions import RestartScenario


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
        iterations (int): number of iterations of the scenario
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    should_resolve = '{{' in value and '}}' in value or value[0] == '$'
    iterations = int(round(float(resolve_variable(grizzly, value)), 0))

    if should_resolve and iterations < 1:
        iterations = 1

    assert iterations >= 0, f'{value} resolved to {iterations} iterations, which is not valid'

    # only strict grammar if there's a static value
    if not should_resolve:
        if iterations > 1 or iterations == 0:
            assert iteration_number == 'iterations', 'when iterations is 0 or greather than 1, use "iterations"'
        else:
            assert iteration_number == 'iteration', 'when iterations is 1, use "iteration"'

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

    ``` gherkin
    Feature:
        Background:
            And ask for value of variable "messageID"
        Scenario:
            And value for variable "HelloWorld" is "default"
            And value for variable "AtomicIntegerIncrementer.mid1" is "{{ messageID }}"
    ```

    Args:
        name (str): variable name
        value (Any): initial value
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    assert name not in grizzly.state.variables, f'variable "{name}" has already been set'

    try:
        # data type will be guessed when setting the variable
        resolved_value = resolve_variable(grizzly, value, guess_datatype=False)
        grizzly.state.variables[name] = resolved_value
    except ValueError as e:
        assert 0, str(e)


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


@given(u'metadata "{key}" is "{value}"')
def step_setup_metadata(context: Context, key: str, value: str) -> None:
    '''Set a metadata (header) value to be used by the user when sending requests.

    Example:

    ``` gherkin
    And metadata "Content-Type" is "application/xml"
    And metadata "Ocp-Apim-Subscription-Key" is "9asdf00asdf00adsf034"
    ```
    '''

    grizzly = cast(GrizzlyContext, context.grizzly)
    casted_value = resolve_variable(grizzly, value)

    if grizzly.scenario.context.get('metadata', None) is None:
        grizzly.scenario.context['metadata'] = {}

    grizzly.scenario.context['metadata'].update({key: casted_value})
