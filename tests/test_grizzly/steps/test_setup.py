from typing import cast

import pytest

from behave.runner import Context

from grizzly.context import LocustContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ..fixtures import behave_context  # pylint: disable=unused-import


@pytest.mark.usefixtures('behave_context')
def test_step_setup_variable_value_ask(behave_context: Context) -> None:
    try:
        context_locust = cast(LocustContext, behave_context.locust)

        name = 'AtomicIntegerIncrementer.messageID'
        assert f'TESTDATA_VARIABLE_{name}' not in environ
        assert name not in context_locust.state.variables

        with pytest.raises(AssertionError):
            step_setup_variable_value_ask(behave_context, name)

        assert name not in context_locust.state.variables

        environ[f'TESTDATA_VARIABLE_{name}'] = '1337'

        step_setup_variable_value_ask(behave_context, name)

        assert int(context_locust.state.variables.get(name, None)) == 1337

        with pytest.raises(AssertionError):
            step_setup_variable_value_ask(behave_context, name)

        environ[f'TESTDATA_VARIABLE_INCORRECT_QUOTED'] = '"incorrectly_quoted\''

        with pytest.raises(AssertionError) as e:
            step_setup_variable_value_ask(behave_context, 'INCORRECT_QUOTED')
        assert 'incorrectly quoted' in str(e)
    finally:
        for key in environ.keys():
            if key.startswith('TESTDATA_VARIABLE_'):
                del environ[key]
