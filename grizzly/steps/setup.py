from typing import cast
from os import environ

from grizzly.types.behave import Context, given, then
from grizzly.context import GrizzlyContext
from grizzly.testdata.utils import resolve_variable
from grizzly.testdata import GrizzlyVariables
from grizzly.tasks import SetVariableTask

from ._helpers import is_template


@then(u'ask for value of variable "{name}"')
@given(u'ask for value of variable "{name}"')
def step_setup_variable_value_ask(context: Context, name: str) -> None:
    '''This step is used to indicate for `grizzly-cli` that it should ask for an initial value for the variable.
    It will then inject the value into the locust runtime environment, and in this step read it and insert it
    into the locust context which grizzly will use to setup locust.

    If `grizzly-cli` is not used, one has to manually set the environment variable, which requires a prefix of
    `TESTDATA_VARIABLE_` and the suffix should match the variable name in question.

    Use this step for variables that should have different initial values for each run of the feature.

    ``` gherkin
    And ask for value for variable "AtomicIntegerIncrementer.messageID"
    ```

    Args:
        name (str): variable name used in templates
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    value = environ.get(f'TESTDATA_VARIABLE_{name}', None)
    assert value is not None, f'variable "{name}" does not have a value'
    assert name not in grizzly.state.variables, f'variable "{name}" has already been set'

    try:
        grizzly.state.variables[name] = value
    except ValueError as e:
        assert 0, str(e)


@given(u'value for variable "{name}" is "{value}"')
def step_setup_variable_value(context: Context, name: str, value: str) -> None:
    '''Use this step to initialize a variable that should have the same [start] value for every run of
    the scenario. If this step is used for a variable that has already been initialized, it is assumed that the value will change during runtime
    so instead a {@pylink grizzly.tasks.set_variable} task will be added instead. The {@pylink grizzly.testdata.variables} must have implemented
    support for being settable.

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
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)

    module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(name)

    if module_name is not None and variable_type is not None:
        partial_name = f'{variable_type}.{variable_name}'

        if module_name != 'grizzly.testdata.variables':
            partial_name = f'{module_name}.{partial_name}'
    else:
        partial_name = name

    if partial_name not in grizzly.state.variables:
        try:
            # data type will be guessed when setting the variable
            if name not in grizzly.state.persistent:
                resolved_value = resolve_variable(grizzly, value, guess_datatype=False)
                if is_template(value):
                    grizzly.scenario.orphan_templates.append(value)
            else:
                resolved_value = grizzly.state.persistent[name]

            grizzly.state.variables[name] = resolved_value
        except Exception as e:
            raise AssertionError(str(e))
    else:
        grizzly.scenario.tasks.add(SetVariableTask(name, value))
