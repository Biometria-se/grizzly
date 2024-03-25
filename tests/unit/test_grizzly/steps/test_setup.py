"""Unit tests of grizzly.steps.setup."""
from __future__ import annotations

from contextlib import suppress
from os import chdir, environ
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.steps.setup import _execute_python_script
from grizzly.tasks import SetVariableTask
from grizzly.types import VariableType
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture


def test_step_setup_variable_value_ask(behave_fixture: BehaveFixture) -> None:
    try:
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        behave.scenario = grizzly.scenario.behave

        name = 'AtomicIntegerIncrementer.messageID'
        assert f'TESTDATA_VARIABLE_{name}' not in environ
        assert name not in grizzly.state.variables

        assert behave.exceptions == {}

        step_setup_variable_value_ask(behave, name)

        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value')]}

        assert name not in grizzly.state.variables

        environ[f'TESTDATA_VARIABLE_{name}'] = '1337'

        step_setup_variable_value_ask(behave, name)

        assert int(grizzly.state.variables.get(name, None)) == 1337

        step_setup_variable_value_ask(behave, name)

        assert behave.exceptions == {behave.scenario.name: [
            ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value'),
            ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" has already been set'),
        ]}

        environ['TESTDATA_VARIABLE_INCORRECT_QUOTED'] = '"incorrectly_quoted\''

        step_setup_variable_value_ask(behave, 'INCORRECT_QUOTED')

        assert behave.exceptions == {behave.scenario.name: [
            ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value'),
            ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" has already been set'),
            ANY(AssertionError, message='incorrectly quoted'),
        ]}
    finally:
        for key in environ:
            if key.startswith('TESTDATA_VARIABLE_'):
                del environ[key]


def test_step_setup_variable_value(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_setup_variable_value(behave, 'test_string', 'test')
    assert grizzly.state.variables['test_string'] == 'test'

    step_setup_variable_value(behave, 'test_int', '1')
    assert grizzly.state.variables['test_int'] == 1

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1 | step=10')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test'] == '1 | step=10'

    grizzly.state.variables['step'] = 13
    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test2', '1 | step={{ step }}')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test2'] == '1 | step=13'

    grizzly.state.configuration['csv.file.path'] = 'test/input.csv'
    grizzly.state.variables['csv_repeat'] = 'False'
    csv_file_path = behave_fixture.locust._test_context_root / 'requests' / 'test' / 'input.csv'
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)
    csv_file_path.touch()
    step_setup_variable_value(behave, 'AtomicCsvReader.input', '$conf::csv.file.path$ | repeat="{{ csv_repeat }}"')
    assert len(behave.exceptions) == 0
    assert grizzly.state.variables['AtomicCsvReader.input'] == 'test/input.csv | repeat="False"'

    grizzly.state.configuration['env'] = 'test'
    csv_file_path = behave_fixture.locust._test_context_root / 'requests' / 'test' / 'input.test.csv'
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)
    csv_file_path.touch()
    step_setup_variable_value(behave, 'AtomicCsvReader.csv_input', 'test/input.$conf::env$.csv | repeat="{{ csv_repeat }}"')
    assert len(behave.exceptions) == 0
    assert grizzly.state.variables['AtomicCsvReader.csv_input'] == 'test/input.test.csv | repeat="False"'

    grizzly.state.variables['leveranser'] = 100
    step_setup_variable_value(behave, 'AtomicRandomString.regnr', '%sA%s1%d%d | count={{ (leveranser * 0.25 + 1) | int }}, upper=True')
    assert grizzly.state.variables['AtomicRandomString.regnr'] == '%sA%s1%d%d | count=26, upper=True'

    step_setup_variable_value(behave, 'AtomicDate.test', '2021-04-13')
    assert grizzly.state.variables['AtomicDate.test'] == '2021-04-13'

    step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='value contained variable "value" which has not been declared')]}

    grizzly.state.variables['value'] = 'hello world!'
    step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    assert grizzly.state.variables['dynamic_variable_value'] == 'hello world!'

    step_setup_variable_value(behave, 'incorrectly_quoted', '"error\'')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='value contained variable "value" which has not been declared'),
        ANY(AssertionError, message='"error\' is incorrectly quoted'),
    ]}

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

    step_setup_variable_value(behave, 'custom.variable.AtomicFooBar.value.foo', 'hello')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='value contained variable "value" which has not been declared'),
        ANY(AssertionError, message='"error\' is incorrectly quoted'),
        ANY(AssertionError, message="No module named 'custom'"),
    ]}

    behave.exceptions.clear()

    assert len(grizzly.scenario.tasks()) == 0

    set_variable_task_mock = mocker.patch('grizzly.tasks.set_variable.SetVariableTask.__init__', return_value=None)
    grizzly.state.variables.update({'custom.variable.AtomicFooBar.value': 'hello'})

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_variable_value(behave, 'custom.variable.AtomicFooBar.value.foo', 'hello')

    set_variable_task_mock.assert_called_once_with('custom.variable.AtomicFooBar.value.foo', 'hello', VariableType.VARIABLES)

    grizzly.scenarios.create(behave_fixture.create_scenario('test zcenario'))

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
    ]}

    step_setup_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
        ANY(AssertionError, message='variable dynamic_variable_value has already been initialized'),
    ]}

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_variable_value(behave, 'new_variable', 'foobar')
    assert grizzly.state.variables['new_variable'] == 'foobar'

    grizzly.scenario.tasks.clear()

    step_setup_variable_value(behave, 'new_variable', 'foobar')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
        ANY(AssertionError, message='variable dynamic_variable_value has already been initialized'),
        ANY(AssertionError, message='variable new_variable has already been initialized'),
    ]}


def test_step_setup_execute_python_script(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    execute_script_mock = mocker.patch('grizzly.steps.setup._execute_python_script', return_value=None)
    context = grizzly_fixture.behave.context

    original_cwd = Path.cwd()

    try:
        chdir(grizzly_fixture.test_context)
        script_file = grizzly_fixture.test_context / 'bin' / 'generate-testdata.py'
        script_file.parent.mkdir(exist_ok=True, parents=True)
        script_file.write_text("print('foobar')")

        step_setup_execute_python_script(context, script_file.as_posix())

        execute_script_mock.assert_called_once_with(context, "print('foobar')")
        execute_script_mock.reset_mock()

        step_setup_execute_python_script(context, 'bin/generate-testdata.py')

        execute_script_mock.assert_called_once_with(context, "print('foobar')")
        execute_script_mock.reset_mock()

        context.feature.location.filename = f'{grizzly_fixture.test_context}/features/test.feature'

        step_setup_execute_python_script(context, '../bin/generate-testdata.py')

        execute_script_mock.assert_called_once_with(context, "print('foobar')")
        execute_script_mock.reset_mock()
    finally:
        with suppress(Exception):
            chdir(original_cwd)
        script_file.unlink()
        script_file.parent.rmdir()


def test_step_setup_execute_python_script_inline(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    execute_script_mock = mocker.patch('grizzly.steps.setup._execute_python_script', return_value=None)
    context = grizzly_fixture.behave.context
    context.text = "print('foobar')"

    original_cwd = Path.cwd()

    try:
        chdir(grizzly_fixture.test_context)

        step_setup_execute_python_script_inline(context)

        execute_script_mock.assert_called_once_with(context, "print('foobar')")
        execute_script_mock.reset_mock()
    finally:
        with suppress(Exception):
            chdir(original_cwd)


def test__execute_python_script_mock(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    context = behave_fixture.context
    on_worker_mock = mocker.patch('grizzly.steps.setup.on_worker', return_value=True)
    exec_mock = mocker.patch('builtins.exec')

    # do not execute, since we're on a worker
    on_worker_mock.return_value = True

    _execute_python_script(context, "print('foobar')")

    on_worker_mock.assert_called_once_with(context)
    exec_mock.assert_not_called()
    on_worker_mock.reset_mock()

    # execute
    on_worker_mock.return_value = False

    _execute_python_script(context, "print('foobar')")

    on_worker_mock.assert_called_once_with(context)
    exec_mock.assert_called_once_with("print('foobar')", SOME(dict, context=context), SOME(dict, context=context))

def test__execute_python_script(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    context = behave_fixture.context

    mocker.patch('grizzly.steps.setup.on_worker', return_value=False)

    with pytest.raises(KeyError):
        hasattr(context, '__foobar__')

    _execute_python_script(context, "from pathlib import Path\nfrom os import path\nsetattr(context, '__foobar__', 'foobar')")

    assert context.__foobar__ == 'foobar'
    assert hasattr(context, '__foobar__')
    assert globals().get('context', None) is None
