"""Steps that can be in both `Background` and `Scenario` [Gherkin](https://cucumber.io/docs/gherkin/reference/) sections."""

from __future__ import annotations

from os import environ
from pathlib import Path
from shlex import split as shlex_split
from typing import TYPE_CHECKING, cast

from grizzly.locust import on_worker
from grizzly.tasks import SetVariableTask
from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.utils import create_context_variable, resolve_variable
from grizzly.types import VariableType
from grizzly.types.behave import Context, Feature, given, then
from grizzly.utils import has_template, merge_dicts

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('ask for value of variable "{name}"')
@given('ask for value of variable "{name}"')
def step_setup_ask_variable_value(context: Context, name: str) -> None:
    """Tell `grizzly-cli` that it should ask for an initial value for the variable.

    It will inject the value into the locust runtime environment, and in this step read it and insert it
    into the locust context which `grizzly` will use to setup `locust`.

    If `grizzly-cli` is not used, one has to manually set the environment variable, which requires a prefix of
    `TESTDATA_VARIABLE_` and the suffix should match the variable name in question.

    Use this step for variables that should have different initial values for each run of the feature.

    Example:
    ```gherkin
    And ask for value for variable "AtomicIntegerIncrementer.messageID"
    ```

    Args:
        name (str): variable name used in templates

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    value = environ.get(f'TESTDATA_VARIABLE_{name}', None)
    assert value is not None, f'variable "{name}" does not have a value'

    try:
        if not context.step.in_background:
            assert name not in grizzly.scenario.variables, f'variable "{name}" has already been set'
            resolved_value = resolve_variable(grizzly.scenario, value, guess_datatype=True)
            grizzly.scenario.variables.update({name: resolved_value})
        else:
            for scenario in grizzly.scenarios:
                assert name not in scenario.variables, f'variable "{name}" has already been set in scenario {scenario.name}'
                resolved_value = resolve_variable(scenario, value, guess_datatype=True)
                scenario.variables.update({name: resolved_value})
                scenario.orphan_templates.append(f'{{{{ {name}}}}}')
    except ValueError as e:
        raise AssertionError(e) from e


@given('value for variable "{name}" is "{value}"')
def step_setup_set_variable_value(context: Context, name: str, value: str) -> None:
    """Step to initialize a variable that should have the same [start] value for every run of the scenario.

    If this step is used after a step that adds a task or for a variable that already has been initialized, it is assumed that the value will change during runtime
    so a [Set variable][grizzly.tasks.set_variable] task will be added instead. The [variable][grizzly.testdata.variables] must
    have implemented support for being settable.

    Data type for the value of the variable is based on the type of variable. If the variable is a testdata [variables][grizzly.testdata.variables]
    then the value needs to match the format and type that the variable has implemented. If it is not a testdata variable
    `grizzly` will try to guess the data type. E.g.:

    * `"10"` becomes `int`

    * `"1.0"` becomes `float`

    * `"True"` becomes `bool`

    * everything else becomes `str`

    It is also possible to set the value of a variable based on another variable, which can be usable if you have a variable in
    multiple scenarios which all should have the same initial value.

    Example:
    ```gherkin title="example.feature"
    Feature:
        Background:
            And ask for value of variable "messageID"
            And value for variable "HelloWorld" is "default"
        Scenario:
            And value for variable "AtomicIntegerIncrementer.mid1" is "{{ messageID }}"
            And value for variable "AtomicIntegerIncrementer.persistent" is "1 | step=10, persist=True"
            And value for variable "AtomicCsvWriter.output" is "output.csv | headers='foo,bar'"
            ...
            And value for variable "AtomicCsvWriter.output.foo" is "{{ value }}"
            And value for variable "AtomicCsvWriter.output.bar" is "{{ value }}"
    ```

    If the file `features/persistent/example.json` (name of feature file and `feature` extension replaced with `json`) exists, and contains an entry for
    the variable, the initial value will be read from the file and override the value specified in the feature file.

    Args:
        name (str): variable name
        value (Any): initial value

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(name)

    if module_name is not None and variable_type is not None:
        partial_name = f'{variable_type}.{variable_name}'

        if module_name != 'grizzly.testdata.variables':
            partial_name = f'{module_name}.{partial_name}'
    else:
        partial_name = name

    try:
        # if the scenario doesn't have any tasks, we'll assume that the scenario is trying to initialize a new variable
        # but, we want to allow initializing new variables after tasks in an scenario as well
        if len(grizzly.scenario.tasks) < 1 or partial_name not in grizzly.scenario.variables:
            # make sure it hasn't already been initialized
            assert partial_name not in grizzly.scenario.variables, f'variable {partial_name} has already been initialized'

            # data type will be guessed when setting the variable
            persisted_initial_value = grizzly.scenario.variables.persistent.get(name, None)
            if persisted_initial_value is None:
                resolved_value = resolve_variable(grizzly.scenario, value, guess_datatype=False, try_file=False)
                if isinstance(value, str) and has_template(value):
                    grizzly.scenario.orphan_templates.append(value)
            else:
                resolved_value = persisted_initial_value

            if not context.step.in_background:
                grizzly.scenario.variables.update({name: resolved_value})
            else:
                for scenario in grizzly.scenarios:
                    scenario.variables.update({name: resolved_value})
        else:
            assert not context.step.in_background, 'cannot add runtime variables in `Background`-section'
            assert partial_name in grizzly.scenario.variables, f'variable {partial_name} has not been initialized'
            grizzly.scenario.tasks.add(SetVariableTask(name, value, VariableType.VARIABLES))
    except Exception as e:
        if not isinstance(e, AssertionError):
            raise AssertionError(e) from e  # noqa: TRY004

        raise


def _execute_python_script(context: Context, source: str, args: str | None) -> None:
    if on_worker(context):
        return

    scope_args: list[str] | None = None
    if args is not None:
        scope_args = shlex_split(args)

    scope = {**globals()}
    scope.update({'context': context, 'args': scope_args})

    exec(source, scope, scope)  # noqa: S102


@then('execute python script "{script_path}" with arguments "{arguments}"')
def step_setup_execute_python_script_with_arguments(context: Context, script_path: str, arguments: str) -> None:
    """Execute python script located in specified path, providing the specified arguments.

    The script will not execute on workers, only on master (distributed mode) or local (local mode), and
    it will only execute once before the test starts. Available in the scope is the current `context` object
    and also `args` (list), which is `shlex.split` of specified `arguments`.

    This can be useful for generating test data files.

    Example:
    ```gherkin
    Then execute python script "../bin/generate-testdata.py"
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    script_file = Path(script_path)
    if not script_file.exists():
        feature = cast('Feature', context.feature)
        base_path = Path(feature.filename).parent if feature.filename not in [None, '<string>'] else Path.cwd()
        script_file = (base_path / script_path).resolve()

    assert script_file.exists(), f'script {script_path} does not exist'

    if has_template(arguments):
        grizzly.scenario.orphan_templates.append(arguments)

    arguments = cast('str', resolve_variable(grizzly.scenario, arguments, guess_datatype=False, try_file=False))

    _execute_python_script(context, script_file.read_text(), arguments)


@then('execute python script "{script_path}"')
def step_setup_execute_python_script(context: Context, script_path: str) -> None:
    """Execute python script located in specified path.

    The script will not execute on workers, only on master (distributed mode) or local (local mode), and
    it will only execute once before the test starts. Available in the scope is the current `context` object.

    This can be useful for generating test data files.

    Example:
    ```gherkin
    Then execute python script "../bin/generate-testdata.py"
    ```

    """
    script_file = Path(script_path)
    if not script_file.exists():
        feature = cast('Feature', context.feature)
        base_path = Path(feature.filename).parent if feature.filename not in [None, '<string>'] else Path.cwd()
        script_file = (base_path / script_path).resolve()

    assert script_file.exists(), f'script {script_path} does not exist'

    _execute_python_script(context, script_file.read_text(), None)


@then('execute python script')
def step_setup_execute_python_script_inline(context: Context) -> None:
    """Execute inline python script specified in the step text.

    The script will not execute on workers, only on master (distributed mode) or local (local mode), and
    it will only execute once before the test starts. Available in the scope is the current `context` object.

    This can be useful for generating test data files.

    Example:
    ```gherkin
    Then execute python script
      \"\"\"
      print('foobar script')
      \"\"\"
    ```

    """
    _execute_python_script(context, context.text, None)


@given('set context variable "{variable}" to "{value}"')
def step_setup_set_context_variable(context: Context, variable: str, value: str) -> None:
    """Set a context variable.

    If used in the `Background`-section the context variable will be used in all scenarios and their respective user. If used in a `Scenario` section
    it will only be set for that user.

    If this step is before any step that adds a task in the scenario, it will be added to the context which the user is initialized with at start.
    If it is after any tasks, it will be added as a task which will change the context variable value during runtime.

    Variable names can contain (one or more) dot (`.`) or slash (`/`) to indicate that the variable has a nested structure. E.g. `token.url`
    and `token/url` results in `{'token': {'url': '<value'>}}`

    It is also possible to have spaces in a variable names, they will then be replaced with underscore (`_`), and the name will be
    converted to lowercase.

    E.g. `Client ID` results in `client_id`.

    Example:
    ```gherkin
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

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    if len(grizzly.scenario.tasks) < 1:
        context_variable = create_context_variable(grizzly.scenario, variable, value)

        if not context.step.in_background:
            grizzly.scenario.context = merge_dicts(grizzly.scenario.context, context_variable)
        else:
            grizzly.setup.global_context = merge_dicts(grizzly.setup.global_context, context_variable)
    else:
        assert not context.step.in_background, 'cannot create a context task while in background section'
        grizzly.scenario.tasks.add(SetVariableTask(variable, value, VariableType.CONTEXT))
