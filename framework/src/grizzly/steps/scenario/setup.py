"""Module contains step implementations that setup the load test scenario with parameters that is going to be used in the scenario they are defined in."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import parse
from grizzly_common.text import permutation

from grizzly.auth import GrizzlyHttpAuthClient
from grizzly.tasks import GrizzlyTask, RequestTask
from grizzly.testdata.utils import resolve_variable
from grizzly.types import FailureAction
from grizzly.types.behave import Context, given, register_type, then, when
from grizzly.utils import ModuleLoader, has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@parse.with_pattern(r'(iteration[s]?)')
@permutation(vector=(False, True))
def parse_iteration_gramatical_number(text: str) -> str:
    return text.strip()


def parse_failure_type(value: str) -> type[Exception] | str:
    result: type[Exception] | str | None = None

    if '.' in value:
        module_name, value = value.rsplit('.', 1)
        module_names = [module_name]
    else:
        module_names = ['grizzly.exceptions', 'builtins']

    # check if value corresponds to an exception type
    for module_name in module_names:
        with suppress(Exception):
            result = ModuleLoader[Exception].load(module_name, value)
            break

    # an exception message which should be checked against the string representation
    # of the exception that was thrown
    if result is None:
        result = value

    return result


register_type(
    IterationGramaticalNumber=parse_iteration_gramatical_number,
    FailureType=parse_failure_type,
    FailureActionStepExpression=FailureAction.from_string,
)


@given('repeat for "{value}" {iteration_number:IterationGramaticalNumber}')
def step_setup_iterations(context: Context, value: str, *_args: Any, **_kwargs: Any) -> None:
    """Set how many iterations the [tasks][grizzly.tasks] in the scenario should execute.

    Default value is `1`. A value of `0` means to run until all test data is consumed, or that the (optional) specified
    runtime for the scenario is reached.

    Example:
    ```gherkin
    And repeat for "10" iterations
    And repeat for "1" iteration
    And value for variable "leveranser" is "100"
    And repeat for "{{ leveranser * 0.25 }}" iterations
    ```

    Args:
        value (str): number of iterations of the scenario, can be a templatning string or a environment configuration variable

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    should_resolve = has_template(value) or value[0] == '$'
    iterations = max(int(round(float(resolve_variable(grizzly.scenario, value)), 0)), 0)

    if has_template(value):
        grizzly.scenario.orphan_templates.append(value)

    if should_resolve and iterations < 1:
        iterations = 1

    grizzly.scenario.iterations = iterations


@given('set iteration time to "{pace_time}" milliseconds')
def step_setup_iteration_pace(context: Context, pace_time: str) -> None:
    """Set minimum time one iterations of all the [tasks][grizzly.tasks] in the scenario should take.

    E.g. if `pace` is set to `2000` ms and the time since it last ran was `300` ms, this task will
    sleep for `1700` ms. If the time of all tasks is greater than the specified time, there will be
    an error, but the scenario will continue.

    This is useful to be able to control the intensity towards the load testing target.

    Example:
    ```gherkin
    Then set iteration time to "2000" milliseconds
    Then set iteration time to "{{ pace }}" milliseconds
    ```

    Args:
        pace_time (str): number of milliseconds each iteration at max should take, supports templating

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    if not has_template(pace_time):
        try:
            float(pace_time)
        except ValueError as e:
            message = f'"{pace_time}" is neither a template or a number'
            raise AssertionError(message) from e
    else:
        grizzly.scenario.orphan_templates.append(pace_time)

    grizzly.scenario.pace = pace_time


@given('set alias "{alias}" for variable "{variable}"')
def step_setup_set_variable_alias(context: Context, alias: str, variable: str) -> None:
    """Create an alias for a variable that points to another structure in the context.

    This is useful if you have test data that somehow should change the behavior for a
    user, e.g. username and password.

    Example:
    ```gherkin
    And value for variable "AtomicCsvReader.users" is "users.csv | repeat=True"
    And set alias "auth.user.username" for variable "AtomicCsvReader.users.username"
    And set alias "auth.user.password" for variable "AtomicCsvReader.users.password"
    ```

    Variables in payload templates are not allowed to have an alias.

    Args:
        alias (str): which node in the context that should get the value of `variable`
        variable (str): an already initialized variable that should be renamed

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    base_variable = '.'.join(variable.split('.')[:2]) if variable.count('.') > 1 else variable

    assert base_variable in grizzly.scenario.variables, f'variable {base_variable} has not been declared'
    assert variable not in grizzly.scenario.variables.alias, f'alias for variable {variable} already exists: {grizzly.scenario.variables.alias[variable]}'

    grizzly.scenario.variables.alias.update({variable: alias})


@given('log all requests')
def step_setup_log_all_requests(context: Context) -> None:
    """Set if all requests should be logged to a file.

    By default only failed requests (and responses) will be logged.

    Example:
    ```gherkin
    And log all requests
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.context['log_all_requests'] = True


@when('the task fails with "{failure:FailureType}" {failure_action:FailureActionStepExpression}')
def step_setup_the_failed_task_custom(context: Context, failure: type[Exception] | str, failure_action: FailureAction) -> None:
    """Set behavior when specific failure occurs for the latest task.

    It can be either a `str` or an exception type, where the later is more specific.

    Example:
    ```gherkin
    ...
    When the task fails with "504 gateway timeout" retry task
    When the task fails with "RuntimeError" stop user
    When the task fails with "TaskTimeoutError" retry task
    ```

    Args:
        failure (type[Exception] | str): exception type or exception message that should be handled
        failure_action (FailureAction): how the specified failure should be handled

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert len(grizzly.scenario.tasks()) > 0, 'scenario does not have any tasks'
    grizzly.scenario.tasks()[-1].failure_handling.update({failure: failure_action.exception})


@when('the task fails {failure_action:FailureActionStepExpression}')
def step_setup_the_failed_task_default(context: Context, failure_action: FailureAction) -> None:
    """Set default behavior when the latest task fails.

    If no default behavior is set, the scenario will continue as nothing happened.

    Example:
    ```gherkin
    ...
    When the task fails restart scenario
    When the task fails stop user
    ```

    Args:
        failure_action (FailureAction): default failure action when nothing specific is matched

    """
    assert failure_action.default_friendly, f'{failure_action.step_expression} should not be used as the default behavior, only use it for specific failures'

    grizzly = cast('GrizzlyContext', context.grizzly)
    assert len(grizzly.scenario.tasks()) > 0, 'scenario does not have any tasks'
    grizzly.scenario.tasks()[-1].failure_handling.update({None: failure_action.exception})


@when('any task fail with "{failure:FailureType}" {failure_action:FailureActionStepExpression}')
def step_setup_any_failed_task_custom(context: Context, failure: type[Exception] | str, failure_action: FailureAction) -> None:
    """Set behavior when specific failure occurs for any tasks in the scenario.

    It can be either a `str` or an exception type, where the later is more specific.

    Example:
    ```gherkin
    When any task fail with "504 gateway timeout" retry task
    When any task fail with "RuntimeError" stop user
    When any task fail with "TaskTimeoutError" retry task
    ```

    Args:
        failure (type[Exception] | str): exception type or exception message that should be handled
        failure_action (FailureAction): how the specified failure should be handled

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.failure_handling.update({failure: failure_action.exception})


@when('any task fail {failure_action:FailureActionStepExpression}')
def step_setup_any_failed_task_default(context: Context, failure_action: FailureAction) -> None:
    """Set default behavior when latest task fail.

    If no default behavior is set, the scenario will continue as nothing happened.

    Example:
    ```gherkin
    When any task fail restart scenario
    When any task fail stop user
    ```

    Args:
        failure_action (FailureAction): default failure action when nothing specific is matched

    """
    assert failure_action.default_friendly, f'{failure_action.step_expression} should not be used as the default behavior, only use it for specific failures'

    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.failure_handling.update({None: failure_action.exception})


@then('metadata "{key}" is "{value}"')
@given('metadata "{key}" is "{value}"')
def step_setup_metadata(context: Context, key: str, value: str) -> None:
    """Set a metadata (header) value to be used by the user when sending requests.

    When step expression is used before any tasks has been added in the scenario the metadata will
    be used for all requests the specified loadtesting user executes in the scenario.

    If used after a [Request][grizzly.tasks.request] task, the metadata will be added and only used
    for that request.

    If used after a task that implements `grizzly.auth.GrizzlyHttpAuthClient` (e.g. [HTTP client task][grizzly.tasks.clients.http]),
    the metadata will be added and only used when that task executes.

    Example:
    ```gherkin
    And metadata "Content-Type" is "application/xml"
    And metadata "Ocp-Apim-Subscription-Key" is "9asdf00asdf00adsf034"
    ```

    Or, for use in one request only, specify metadata after the request:
    ```gherkin
    Then post request ...
    And metadata "x-header" is "{{ value }}"

    Then get from "https://{{ client_url }}" with name "client-http" and save response payload in "payload"
    And metadata "Ocp-Apim-Subscription-Key" is "deadbeefb00f"
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    casted_value = resolve_variable(grizzly.scenario, value)

    previous_task: GrizzlyTask | None = None
    tasks = grizzly.scenario.tasks()
    if len(tasks) > 0:
        previous_task = tasks[-1]

    if isinstance(previous_task, RequestTask | GrizzlyHttpAuthClient):
        previous_task.add_metadata(key, value)
    else:
        if grizzly.scenario.context.get('metadata', None) is None:
            grizzly.scenario.context['metadata'] = {}

        grizzly.scenario.context['metadata'].update({key: casted_value})
