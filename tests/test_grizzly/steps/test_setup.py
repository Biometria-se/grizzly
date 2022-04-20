from os import environ
from typing import cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from ...fixtures import BehaveFixture


def test_step_setup_variable_value_ask(behave_fixture: BehaveFixture) -> None:
    try:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)

        name = 'AtomicIntegerIncrementer.messageID'
        assert f'TESTDATA_VARIABLE_{name}' not in environ
        assert name not in grizzly.state.variables

        with pytest.raises(AssertionError):
            step_setup_variable_value_ask(behave, name)

        assert name not in grizzly.state.variables

        environ[f'TESTDATA_VARIABLE_{name}'] = '1337'

        step_setup_variable_value_ask(behave, name)

        assert int(grizzly.state.variables.get(name, None)) == 1337

        with pytest.raises(AssertionError):
            step_setup_variable_value_ask(behave, name)

        environ['TESTDATA_VARIABLE_INCORRECT_QUOTED'] = '"incorrectly_quoted\''

        with pytest.raises(AssertionError) as e:
            step_setup_variable_value_ask(behave, 'INCORRECT_QUOTED')
        assert 'incorrectly quoted' in str(e)
    finally:
        for key in environ.keys():
            if key.startswith('TESTDATA_VARIABLE_'):
                del environ[key]
