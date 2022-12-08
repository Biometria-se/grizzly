from typing import cast
from os import environ

from behave import given, then  # pylint: disable=no-name-in-module
from behave.runner import Context

from ..context import GrizzlyContext
from ..testdata.utils import resolve_variable
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
            And value for variable "HelloWorld" is "default"
        Scenario:
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
