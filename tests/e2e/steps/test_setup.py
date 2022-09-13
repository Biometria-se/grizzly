from typing import cast

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ...fixtures import End2EndFixture


def test_e2e_step_setup_variable_value_ask(e2e_fixture: End2EndFixture) -> None:
    def validate_variables(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.state.variables['background_variable'] == 'foo-background-value'
        assert grizzly.state.variables['scenario_variable'] == 'bar-scenario-value'

    e2e_fixture.add_validator(validate_variables)

    feature_file = e2e_fixture.test_steps(
        background=[
            'And ask for value of variable "background_variable"',
        ],
        scenario=[
            'Then ask for value of variable "scenario_variable"',
            'Then log message "{{ background_variable }}={{ scenario_variable }}"',
        ]
    )

    assert feature_file == 'features/test_e2e_step_setup_variable_value_ask.feature'

    rc, output = e2e_fixture.execute(feature_file, testdata={
        'background_variable': 'foo-background-value',
        'scenario_variable': 'bar-scenario-value',
    })

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))

        raise
