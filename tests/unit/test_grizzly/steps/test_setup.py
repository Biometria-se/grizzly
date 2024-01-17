"""Unit tests of grizzly.steps.setup."""
from __future__ import annotations

from os import environ
from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.tasks import SetVariableTask
from grizzly.types import VariableType

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture, MockerFixture


def test_step_setup_variable_value_ask(behave_fixture: BehaveFixture) -> None:
    try:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)

        name = 'AtomicIntegerIncrementer.messageID'
        assert f'TESTDATA_VARIABLE_{name}' not in environ
        assert name not in grizzly.state.variables

        with pytest.raises(AssertionError, match='variable "AtomicIntegerIncrementer.messageID" does not have a value'):
            step_setup_variable_value_ask(behave, name)

        assert name not in grizzly.state.variables

        environ[f'TESTDATA_VARIABLE_{name}'] = '1337'

        step_setup_variable_value_ask(behave, name)

        assert int(grizzly.state.variables.get(name, None)) == 1337

        with pytest.raises(AssertionError, match='variable "AtomicIntegerIncrementer.messageID" has already been set'):
            step_setup_variable_value_ask(behave, name)

        environ['TESTDATA_VARIABLE_INCORRECT_QUOTED'] = '"incorrectly_quoted\''

        with pytest.raises(AssertionError, match='incorrectly quoted'):
            step_setup_variable_value_ask(behave, 'INCORRECT_QUOTED')
    finally:
        for key in environ:
            if key.startswith('TESTDATA_VARIABLE_'):
                del environ[key]


def test_step_setup_variable_value(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

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

    with pytest.raises(AssertionError, match='value contained variable "value" which has not been declared'):
        step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    grizzly.state.variables['value'] = 'hello world!'
    step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    assert grizzly.state.variables['dynamic_variable_value'] == 'hello world!'

    with pytest.raises(AssertionError, match=r'"error\' is incorrectly quoted'):
        step_setup_variable_value(behave, 'incorrectly_quoted', '"error\'')

    grizzly.state.persistent.update({'AtomicIntegerIncrementer.persistent': '10 | step=10, persist=True'})
    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')
    assert grizzly.state.variables['AtomicIntegerIncrementer.persistent'] == '10 | step=10, persist=True'

    step_setup_variable_value(behave, 'AtomicCsvWriter.output', 'output.csv | headers="foo,bar"')
    assert grizzly.state.variables['AtomicCsvWriter.output'] == 'output.csv | headers="foo,bar"'
    assert len(grizzly.scenario.tasks()) == 0

    grizzly.state.variables.update({'foo_value': 'foobar'})

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_variable_value(behave, 'AtomicCsvWriter.output.foo', '{{ foo_value }}')
    assert len(grizzly.scenario.tasks()) == 2
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, SetVariableTask)
    assert task.variable == 'AtomicCsvWriter.output.foo'
    assert task.value == '{{ foo_value }}'

    grizzly.state.variables.update({'bar_value': 'foobaz'})

    step_setup_variable_value(behave, 'AtomicCsvWriter.output.bar', '{{ bar_value }}')
    assert len(grizzly.scenario.tasks()) == 3
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, SetVariableTask)
    assert task.variable == 'AtomicCsvWriter.output.bar'
    assert task.value == '{{ bar_value }}'

    grizzly.scenario.tasks.clear()

    with pytest.raises(AssertionError, match="No module named 'custom'"):
        step_setup_variable_value(behave, 'custom.variable.AtomicFooBar.value.foo', 'hello')

    assert len(grizzly.scenario.tasks()) == 0

    set_variable_task_mock = mocker.patch('grizzly.tasks.set_variable.SetVariableTask.__init__', return_value=None)
    grizzly.state.variables.update({'custom.variable.AtomicFooBar.value': 'hello'})

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_variable_value(behave, 'custom.variable.AtomicFooBar.value.foo', 'hello')

    set_variable_task_mock.assert_called_once_with('custom.variable.AtomicFooBar.value.foo', 'hello', VariableType.VARIABLES)

    grizzly.scenarios.create(behave_fixture.create_scenario('test zcenario'))

    with pytest.raises(AssertionError, match='variable AtomicIntegerIncrementer.persistent has already been initialized'):
        step_setup_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')

    with pytest.raises(AssertionError, match='variable dynamic_variable_value has already been initialized'):
        step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_variable_value(behave, 'new_variable', 'foobar')
    assert grizzly.state.variables['new_variable'] == 'foobar'

    grizzly.scenario.tasks.clear()

    with pytest.raises(AssertionError, match='variable new_variable has already been initialized'):
        step_setup_variable_value(behave, 'new_variable', 'foobar')
