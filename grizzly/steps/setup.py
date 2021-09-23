from typing import cast
from os import environ

from behave import given  # pylint: disable=no-name-in-module
from behave.runner import Context

from ..context import LocustContext


@given(u'ask for value of variable "{name}"')
def step_setup_variable_value_ask(context: Context, name: str) -> None:
    '''This step is used to indicate for `grizzly-cli` that it should ask for an initial value for the variable.
    It will then inject the value into the locust runtime environment, and in this step read it and insert it
    into the locust context which grizzly will use to setup locust.

    If `grizzly-cli` is not used, one has to manually set the environment variable, which requires a prefix of
    `TESTDATA_VARIABLE_` and the suffix should match the variable name in question.

    Use this step for variables that should have different initial values for each run of the feature.

    ```gherkin
    And ask for value for variable "AtomicIntegerIncrementer.messageID"
    ```

    Args:
        name (str): variable name used in templates
    '''
    context_locust = cast(LocustContext, context.locust)

    value = environ.get(f'TESTDATA_VARIABLE_{name}', None)
    assert value is not None, f'variable "{name}" does not have a value'
    assert name not in context_locust.state.variables, f'variable "{name}" has already been set'

    try:
        context_locust.state.variables[name] = value
    except ValueError as e:
        assert 0, str(e)


