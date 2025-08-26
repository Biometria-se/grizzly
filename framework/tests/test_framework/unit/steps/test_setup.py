"""Unit tests of grizzly.steps.setup."""

from __future__ import annotations

from contextlib import suppress
from hashlib import sha256
from os import chdir, environ
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.auth import AAD
from grizzly.steps import *
from grizzly.steps.setup import _execute_python_script
from grizzly.tasks import SetVariableTask
from grizzly.types import StrDict, VariableType
from grizzly.users import RestApiUser
from grizzly_common.azure.aad import AzureAadCredential

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.behave import Context as BehaveContext

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture


def _assert_variable_value(self: BehaveContext, name: str, value: Any) -> None:
    grizzly = cast('GrizzlyContext', self.grizzly)
    default_value = '__NOT_DEFINED__' if value is not None else None
    for scenario in grizzly.scenarios:
        if self.step.in_background:
            assert scenario.variables.get(name, default_value) == value
        else:  # noqa: PLR5501
            if scenario is grizzly.scenario:
                assert scenario.variables.get(name, default_value) == value
            else:
                assert name not in scenario.variables


@pytest.mark.parametrize('section', ['Scenario', 'Background'])
def test_step_setup_ask_variable_value(behave_fixture: BehaveFixture, section: str) -> None:
    try:
        in_background = section == 'Background'
        behave = behave_fixture.context
        behave.assert_variable_value = _assert_variable_value.__get__(behave, behave.__class__)
        grizzly = cast('GrizzlyContext', behave.grizzly)
        grizzly.scenarios.create(behave_fixture.create_scenario('dummy-1'))
        grizzly.scenarios.create(behave_fixture.create_scenario('dummy-2'))
        grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        behave.scenario = grizzly.scenario.behave
        behave_fixture.create_step('test step', in_background=in_background, context=behave)

        for scenario in grizzly.scenarios:
            scenario.variables.clear()

        name = 'AtomicIntegerIncrementer.messageID'
        assert f'TESTDATA_VARIABLE_{name}' not in environ
        behave.assert_variable_value(name, None)

        assert behave.exceptions == {}

        step_setup_ask_variable_value(behave, name)

        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value')]}

        behave.assert_variable_value(name, None)

        environ[f'TESTDATA_VARIABLE_{name}'] = '1337'

        step_setup_ask_variable_value(behave, name)

        assert int(grizzly.scenario.variables.get(name, '')) == 1337

        step_setup_ask_variable_value(behave, name)

        assert behave.exceptions == {
            behave.scenario.name: [
                ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value'),
                ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" has already been set'),
            ],
        }

        environ['TESTDATA_VARIABLE_INCORRECT_QUOTED'] = '"incorrectly_quoted\''

        step_setup_ask_variable_value(behave, 'INCORRECT_QUOTED')

        assert behave.exceptions == {
            behave.scenario.name: [
                ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" does not have a value'),
                ANY(AssertionError, message='variable "AtomicIntegerIncrementer.messageID" has already been set'),
                ANY(AssertionError, message='incorrectly quoted'),
            ],
        }
    finally:
        for key in environ:
            if key.startswith('TESTDATA_VARIABLE_'):
                del environ[key]


@pytest.mark.parametrize('section', ['Scenario', 'Background'])
def test_step_setup_set_variable_value(behave_fixture: BehaveFixture, mocker: MockerFixture, section: str) -> None:  # noqa: PLR0915
    in_background = section == 'Background'
    behave = behave_fixture.context
    behave.assert_variable_value = _assert_variable_value.__get__(behave, behave.__class__)
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('dummy-1'))
    grizzly.scenarios.create(behave_fixture.create_scenario('dummy-2'))
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave_fixture.create_step('test step', in_background=in_background, context=behave)

    step_setup_set_variable_value(behave, 'test_string', 'test')
    behave.assert_variable_value('test_string', 'test')

    step_setup_set_variable_value(behave, 'test_int', '1')
    behave.assert_variable_value('test_int', 1)

    step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.test', '1 | step=10')
    behave.assert_variable_value('AtomicIntegerIncrementer.test', '1 | step=10')

    grizzly.scenario.variables['step'] = 13
    step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.test2', '1 | step="{{ step }}"')
    behave.assert_variable_value('AtomicIntegerIncrementer.test2', '1 | step="13"')

    grizzly.state.configuration['csv.file.path'] = 'test/input.csv'
    grizzly.scenario.variables['csv_repeat'] = 'False'
    csv_file_path = behave_fixture.locust._test_context_root / 'requests' / 'test' / 'input.csv'
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)
    csv_file_path.touch()
    step_setup_set_variable_value(behave, 'AtomicCsvReader.input', '$conf::csv.file.path$ | repeat="{{ csv_repeat }}"')
    assert len(behave.exceptions) == 0
    behave.assert_variable_value('AtomicCsvReader.input', 'test/input.csv | repeat="False"')

    grizzly.state.configuration['env'] = 'test'
    csv_file_path = behave_fixture.locust._test_context_root / 'requests' / 'test' / 'input.test.csv'
    csv_file_path.parent.mkdir(parents=True, exist_ok=True)
    csv_file_path.touch()
    step_setup_set_variable_value(behave, 'AtomicCsvReader.csv_input', 'test/input.$conf::env$.csv | repeat="{{ csv_repeat }}"')
    assert len(behave.exceptions) == 0
    behave.assert_variable_value('AtomicCsvReader.csv_input', 'test/input.test.csv | repeat="False"')

    grizzly.scenario.variables['leveranser'] = 100
    step_setup_set_variable_value(behave, 'AtomicRandomString.regnr', '%sA%s1%d%d | count={{ (leveranser * 0.25 + 1) | int }}, upper=True')
    behave.assert_variable_value('AtomicRandomString.regnr', '%sA%s1%d%d | count=26, upper=True')

    step_setup_set_variable_value(behave, 'AtomicDate.test', '2021-04-13')
    behave.assert_variable_value('AtomicDate.test', '2021-04-13')

    step_setup_set_variable_value(behave, 'dynamic_variable_value', '{{ value }}')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nvalue')]}

    grizzly.scenario.variables['value'] = 'hello world!'
    step_setup_set_variable_value(behave, 'dynamic_variable_value', '{{ value }}')

    behave.assert_variable_value('dynamic_variable_value', 'hello world!')

    step_setup_set_variable_value(behave, 'incorrectly_quoted', '"error\'')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nvalue'),
            ANY(AssertionError, message='"error\' is incorrectly quoted'),
        ],
    }
    behave.exceptions.clear()

    grizzly.scenario.variables.persistent.update({'AtomicIntegerIncrementer.persistent': '10 | step=10, persist=True'})
    step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')
    behave.assert_variable_value('AtomicIntegerIncrementer.persistent', '10 | step=10, persist=True')

    step_setup_set_variable_value(behave, 'AtomicCsvWriter.output', 'output.csv | headers="foo,bar"')
    behave.assert_variable_value('AtomicCsvWriter.output', 'output.csv | headers="foo,bar"')
    assert len(grizzly.scenario.tasks()) == 0

    grizzly.scenario.variables.update({'foo_value': 'foobar'})

    grizzly.scenario.tasks.add(LogMessageTask('dummy'))

    step_setup_set_variable_value(behave, 'AtomicCsvWriter.output.foo', '{{ foo_value }}')
    if not in_background:
        assert len(grizzly.scenario.tasks()) == 2
        task = grizzly.scenario.tasks()[-1]
        assert task == SOME(SetVariableTask, variable='AtomicCsvWriter.output.foo', value='{{ foo_value }}')
    else:
        assert len(grizzly.scenario.tasks()) == 1
        assert behave.exceptions == {
            grizzly.scenario.behave.name: [
                ANY(AssertionError, message='cannot add runtime variables in `Background`-section'),
            ],
        }

    behave.exceptions.clear()

    if not in_background:
        grizzly.scenario.variables.update({'bar_value': 'foobaz'})

        step_setup_set_variable_value(behave, 'AtomicCsvWriter.output.bar', '{{ bar_value }}')
        assert len(grizzly.scenario.tasks()) == 3
        task = grizzly.scenario.tasks()[-1]
        assert task == SOME(SetVariableTask, variable='AtomicCsvWriter.output.bar', value='{{ bar_value }}')

        grizzly.scenario.tasks.clear()

        step_setup_set_variable_value(behave, 'custom.variable.AtomicFooBar.value.foo', 'hello')

        assert behave.exceptions == {
            behave.scenario.name: [
                ANY(AssertionError, message="No module named 'custom'"),
            ],
        }

        behave.exceptions.clear()

        assert len(grizzly.scenario.tasks()) == 0

        set_variable_task_mock = mocker.patch('grizzly.tasks.set_variable.SetVariableTask.__init__', return_value=None)
        grizzly.scenario.variables.update({'test_framework.helpers.AtomicCustomVariable.value': 'hello'})

        grizzly.scenario.tasks.add(LogMessageTask('dummy'))

        step_setup_set_variable_value(behave, 'test_framework.helpers.AtomicCustomVariable.value.foo', 'hello')

        set_variable_task_mock.assert_called_once_with('test_framework.helpers.AtomicCustomVariable.value.foo', 'hello', VariableType.VARIABLES)

        assert len(grizzly.scenario.tasks) == 2

        prev_scenario = grizzly.scenario.behave
        grizzly.scenarios.create(behave_fixture.create_scenario('test zcenario'))
        grizzly.scenario.variables.update({'value': 'foo'})

        step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')
        assert behave.exceptions == {}
        step_setup_set_variable_value(behave, 'dynamic_variable_value', '{{ value }}')
        assert behave.exceptions == {}

        grizzly.scenarios.select(prev_scenario)
        grizzly.scenario.tasks().clear()
        step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.persistent', '1 | step=10, persist=True')
        assert behave.exceptions == {
            prev_scenario.name: [
                ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
            ],
        }

        step_setup_set_variable_value(behave, 'dynamic_variable_value', '{{ value }}')
        assert behave.exceptions == {
            prev_scenario.name: [
                ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
                ANY(AssertionError, message='variable dynamic_variable_value has already been initialized'),
            ],
        }

        grizzly.scenario.tasks.add(LogMessageTask('dummy'))

        step_setup_set_variable_value(behave, 'new_variable', 'foobar')
        behave.assert_variable_value('new_variable', 'foobar')

        grizzly.scenario.tasks.clear()

        step_setup_set_variable_value(behave, 'new_variable', 'foobar')

        assert behave.exceptions == {
            behave.scenario.name: [
                ANY(AssertionError, message='variable AtomicIntegerIncrementer.persistent has already been initialized'),
                ANY(AssertionError, message='variable dynamic_variable_value has already been initialized'),
                ANY(AssertionError, message='variable new_variable has already been initialized'),
            ],
        }


def test_step_setup_execute_python_script_with_arguments(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    execute_script_mock = mocker.patch('grizzly.steps.setup._execute_python_script', return_value=None)
    context = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    original_cwd = Path.cwd()

    try:
        chdir(grizzly_fixture.test_context)
        script_file = grizzly_fixture.test_context / 'bin' / 'generate-testdata.py'
        script_file.parent.mkdir(exist_ok=True, parents=True)
        script_file.write_text("print('foobar')")

        step_setup_execute_python_script_with_arguments(context, script_file.as_posix(), '--foo=bar --bar foo --baz')

        execute_script_mock.assert_called_once_with(context, "print('foobar')", '--foo=bar --bar foo --baz')
        execute_script_mock.reset_mock()

        step_setup_execute_python_script_with_arguments(context, 'bin/generate-testdata.py', '--foo=bar --bar foo --baz')

        execute_script_mock.assert_called_once_with(context, "print('foobar')", '--foo=bar --bar foo --baz')
        execute_script_mock.reset_mock()

        context.feature.location.filename = f'{grizzly_fixture.test_context}/features/test.feature'

        grizzly.scenario.variables.update({'foo': 'bar'})
        step_setup_execute_python_script_with_arguments(context, '../bin/generate-testdata.py', '--foo={{ foo }} --bar foo --baz')

        execute_script_mock.assert_called_once_with(context, "print('foobar')", '--foo=bar --bar foo --baz')
        execute_script_mock.reset_mock()
    finally:
        with suppress(Exception):
            chdir(original_cwd)
        script_file.unlink()
        script_file.parent.rmdir()


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

        execute_script_mock.assert_called_once_with(context, "print('foobar')", None)
        execute_script_mock.reset_mock()

        step_setup_execute_python_script(context, 'bin/generate-testdata.py')

        execute_script_mock.assert_called_once_with(context, "print('foobar')", None)
        execute_script_mock.reset_mock()

        context.feature.location.filename = f'{grizzly_fixture.test_context}/features/test.feature'

        step_setup_execute_python_script(context, '../bin/generate-testdata.py')

        execute_script_mock.assert_called_once_with(context, "print('foobar')", None)
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

        execute_script_mock.assert_called_once_with(context, "print('foobar')", None)
        execute_script_mock.reset_mock()
    finally:
        with suppress(Exception):
            chdir(original_cwd)


def test__execute_python_script_mock(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    grizzly = grizzly_fixture.grizzly
    context = grizzly_fixture.behave.context
    on_worker_mock = mocker.patch('grizzly.steps.setup.on_worker', return_value=True)
    exec_mock = mocker.patch('builtins.exec')

    # do not execute, since we're on a worker
    on_worker_mock.return_value = True

    _execute_python_script(context, "print('foobar')", None)

    on_worker_mock.assert_called_once_with(context)
    exec_mock.assert_not_called()
    on_worker_mock.reset_mock()

    # execute, no args
    on_worker_mock.return_value = False

    _execute_python_script(context, "print('foobar')", None)

    on_worker_mock.assert_called_once_with(context)
    exec_mock.assert_called_once_with("print('foobar')", SOME(dict, context=context, args=None), SOME(dict, context=context, args=None))
    on_worker_mock.reset_mock()
    exec_mock.reset_mock()

    # execute, args
    grizzly.scenario.variables.update({'foo': 'bar'})
    _execute_python_script(context, "print('foobar')", '--foo=bar --bar foo --baz')

    on_worker_mock.assert_called_once_with(context)
    scope = SOME(dict, context=context, args=['--foo=bar', '--bar', 'foo', '--baz'])
    exec_mock.assert_called_once_with("print('foobar')", scope, scope)


def test__execute_python_script(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    context = behave_fixture.context

    mocker.patch('grizzly.steps.setup.on_worker', return_value=False)

    assert not hasattr(context, 'foobar')

    _execute_python_script(context, "from pathlib import Path\nfrom os import path\nsetattr(context, 'foobar', 'foobar')", None)

    assert hasattr(context, 'foobar')
    assert context.foobar == 'foobar'
    assert globals().get('context', None) is None


def test_step_setup_set_context_variable_runtime(grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
    parent = grizzly_fixture(user_type=RestApiUser)

    assert isinstance(parent.user, RestApiUser)

    grizzly = grizzly_fixture.grizzly
    behave = grizzly_fixture.behave.context
    grizzly_fixture.behave.create_step('test step', in_background=False, context=behave)

    task = grizzly.scenario.tasks.pop()

    assert len(grizzly.scenario.tasks) == 0
    assert parent.user.__cached_auth__ == {}

    step_setup_set_context_variable(behave, 'auth.user.username', 'bob')
    step_setup_set_context_variable(behave, 'auth.user.password', 'foobar')

    assert grizzly.scenario.context == {
        'host': '',
        'auth': {
            'user': {'username': 'bob', 'password': 'foobar'},
        },
    }
    assert parent.user.__cached_auth__ == {}
    assert parent.user.__context_change_history__ == set()

    parent.user._context = merge_dicts(parent.user._context, grizzly.scenario.context)

    assert len(grizzly.scenario.tasks()) == 0

    grizzly.scenario.tasks.add(task)

    step_setup_set_context_variable(behave, 'auth.user.username', 'alice')

    assert len(grizzly.scenario.tasks()) == 2
    assert parent.user.__context_change_history__ == set()

    task_factory = grizzly.scenario.tasks().pop()

    assert isinstance(task_factory, SetVariableTask)
    assert task_factory.variable_type == VariableType.CONTEXT

    AAD.initialize(parent.user, parent.user)

    assert isinstance(parent.user.credential, AzureAadCredential)
    credential_bob = parent.user.credential

    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='bob', password='foobar')
    assert parent.user.credential.username == 'bob'
    assert parent.user.credential.password == 'foobar'  # noqa: S105

    task_factory()(parent)

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='alice', password='foobar')
    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert parent.user.credential is credential_bob

    expected_cache_key = sha256(b'bob:foobar').hexdigest()

    assert parent.user.__context_change_history__ == {'auth.user.username'}
    assert parent.user.__cached_auth__ == {expected_cache_key: SOME(AzureAadCredential, username=credential_bob.username, password=credential_bob.password)}

    step_setup_set_context_variable(behave, 'auth.user.password', 'hello world')

    assert len(grizzly.scenario.tasks()) == 2

    task_factory = grizzly.scenario.tasks().pop()

    assert isinstance(task_factory, SetVariableTask)
    assert task_factory.variable_type == VariableType.CONTEXT

    task_factory()(parent)

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='alice', password='hello world')
    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert getattr(parent.user, 'credential', 'asdf') is None

    assert parent.user.__context_change_history__ == set()
    assert parent.user.__cached_auth__ == {expected_cache_key: SOME(AzureAadCredential, username=credential_bob.username, password=credential_bob.password)}

    step_setup_set_context_variable(behave, 'auth.user.username', 'bob')
    task_factory = grizzly.scenario.tasks().pop()
    task_factory()(parent)

    step_setup_set_context_variable(behave, 'auth.user.password', 'foobar')
    task_factory = grizzly.scenario.tasks().pop()
    task_factory()(parent)

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='bob', password='foobar')
    assert parent.user.credential == SOME(AzureAadCredential, username=credential_bob.username, password=credential_bob.password)
    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert parent.user.__context_change_history__ == set()


@pytest.mark.parametrize('section', ['Scenario', 'Background'])
def test_step_setup_set_context_variable_init(behave_fixture: BehaveFixture, section: str) -> None:
    in_background = section == 'Background'
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave_fixture.create_step('test step', in_background=in_background, context=behave)

    def context() -> StrDict:
        return grizzly.scenario.context if not in_background else grizzly.setup.global_context

    step_setup_set_context_variable(behave, 'token.url', 'test')
    assert context() == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_context_variable(behave, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert context() == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context().clear()

    step_setup_set_context_variable(behave, 'test.decimal.value', '1337')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.float.value', '1.337')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.bool.value', 'true')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
            'bool': {
                'value': True,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.bool.value', 'True')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
            'bool': {
                'value': True,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.bool.value', 'false')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
            'bool': {
                'value': False,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.bool.value', 'FaLsE')
    assert context() == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
            'bool': {
                'value': False,
            },
        },
    }

    context().clear()

    step_setup_set_context_variable(behave, 'text.string.value', 'Hello world!')
    assert context() == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_context_variable(behave, 'text.string.description', 'simple text')
    assert context() == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    context().clear()
    step_setup_set_context_variable(behave, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert context() == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context().clear()
    context().update({'tenant': 'example.com'})
    step_setup_set_context_variable(behave, 'url', 'AZURE')
    assert context() == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    context().update({'host': 'http://example.com'})
    step_setup_set_context_variable(behave, 'url', 'HOST')
    assert context() == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }

    step_setup_set_context_variable(behave, 'www.example.com/auth.user.username', 'bob')
    step_setup_set_context_variable(behave, 'www.example.com/auth.user.password', 'password')
    assert context() == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
        'www.example.com': {
            'auth': {
                'user': {
                    'username': 'bob',
                    'password': 'password',
                },
            },
        },
    }
