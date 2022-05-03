from typing import cast

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ...fixtures import BehaveContextFixture


def test_e2e_step_setup_variable_value_ask(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_variables(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.state.variables['background_variable'] == 'foo-background-value'
        assert grizzly.state.variables['scenario_variable'] == 'bar-scenario-value'

    behave_context_fixture.add_validator(validate_variables)

    feature_file = behave_context_fixture.test_steps(
        background=[
            'And ask for value of variable "background_variable"'
        ],
        scenario=[
            'Then ask for value of variable "scenario_variable"'
        ]
    )

    assert feature_file == 'features/test_e2e_step_setup_variable_value_ask.feature'

    rc, output = behave_context_fixture.execute(feature_file, testdata={
        'background_variable': 'foo-background-value',
        'scenario_variable': 'bar-scenario-value',
    })

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))

        raise
