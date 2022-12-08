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


def test_step_setup_variable_value(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert 'test' not in grizzly.state.variables

    step_setup_variable_value(behave, 'test_string', 'test')
    assert grizzly.state.variables['test_string'] == 'test'

    step_setup_variable_value(behave, 'test_int', '1')
    assert grizzly.state.variables['test_int'] == 1

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1 | step=10')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test'] == '1 | step=10'

    grizzly.state.variables['step'] = 13
    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test2', '1 | step={{ step }}')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test2'] == '1 | step=13'

    grizzly.state.variables['leveranser'] = 100
    step_setup_variable_value(behave, 'AtomicRandomString.regnr', '%sA%s1%d%d | count={{ (leveranser * 0.25 + 1) | int }}, upper=True')
    assert grizzly.state.variables['AtomicRandomString.regnr'] == '%sA%s1%d%d | count=26, upper=True'

    step_setup_variable_value(behave, 'AtomicDate.test', '2021-04-13')
    assert grizzly.state.variables['AtomicDate.test'] == '2021-04-13'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1 | step=10')

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    grizzly.state.variables['value'] = 'hello world!'
    step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    assert grizzly.state.variables['dynamic_variable_value'] == 'hello world!'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave, 'incorrectly_quoted', '"error\'')

    grizzly.state.persistent.update({'AtomicIntegerIncrementer.persistent': '10 | step=10, persist=True'})
    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')
    assert grizzly.state.variables['AtomicIntegerIncrementer.persistent'] == '10 | step=10, persist=True'
