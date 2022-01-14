from typing import cast

from os import environ

import pytest

from parse import compile
from behave.runner import Context
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.types import GrizzlyDictValueType, GrizzlyDict

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import


def test_parse_iteration_gramatical_number() -> None:
    p = compile(
        'run for {iteration:d} {iteration_number:IterationGramaticalNumber}',
        extra_types=dict(IterationGramaticalNumber=parse_iteration_gramatical_number),
    )

    assert p.parse('run for 1 iteration')['iteration_number'] == 'iteration'
    assert p.parse('run for 10 iterations')['iteration_number'] == 'iterations'
    assert p.parse('run for 4 laps') is None

    assert parse_iteration_gramatical_number(' asdf ') == 'asdf'


@pytest.mark.usefixtures('behave_context')
def test_step_setup_set_context_variable(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    step_setup_set_context_variable(behave_context, 'token.url', 'test')
    assert grizzly.scenario.context == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_context_variable(behave_context, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.scenario.context == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.scenario.context = {}

    step_setup_set_context_variable(behave_context, 'test.decimal.value', '1337')
    assert grizzly.scenario.context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_context_variable(behave_context, 'test.float.value', '1.337')
    assert grizzly.scenario.context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
        },
    }

    step_setup_set_context_variable(behave_context, 'test.bool.value', 'true')
    assert grizzly.scenario.context == {
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

    step_setup_set_context_variable(behave_context, 'test.bool.value', 'True')
    assert grizzly.scenario.context == {
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

    step_setup_set_context_variable(behave_context, 'test.bool.value', 'false')
    assert grizzly.scenario.context == {
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

    step_setup_set_context_variable(behave_context, 'test.bool.value', 'FaLsE')
    assert grizzly.scenario.context == {
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

    grizzly.scenario.context = {}

    step_setup_set_context_variable(behave_context, 'text.string.value', 'Hello world!')
    assert grizzly.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_context_variable(behave_context, 'text.string.description', 'simple text')
    assert grizzly.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    grizzly.scenario.context = {}
    step_setup_set_context_variable(behave_context, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.scenario.context == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.scenario.context = {'tenant': 'example.com'}
    step_setup_set_context_variable(behave_context, 'url', 'AZURE')
    assert grizzly.scenario.context == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    grizzly.scenario.context['host'] = 'http://example.com'
    step_setup_set_context_variable(behave_context, 'url', 'HOST')
    assert grizzly.scenario.context == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }


@pytest.mark.usefixtures('behave_context')
def test_step_setup_iterations(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.scenario.iterations == 1

    step_setup_iterations(behave_context, '10', 'iterations')
    assert grizzly.scenario.iterations == 10

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '10', 'iteration')

    step_setup_iterations(behave_context, '1', 'iteration')
    assert grizzly.scenario.iterations == 1

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '1', 'iterations')

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')

    grizzly.state.variables['iterations'] = 100
    step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')
    assert grizzly.scenario.iterations == 100

    step_setup_iterations(behave_context, '{{ iterations * 0.25 }}', 'iteration')
    assert grizzly.scenario.iterations == 25

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '-1', 'iteration')

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '0', 'iteration')

    step_setup_iterations(behave_context, '0', 'iterations')
    assert grizzly.scenario.iterations == 0

    step_setup_iterations(behave_context, '{{ iterations / 101 }}', 'iteration')
    assert grizzly.scenario.iterations == 1

    grizzly.state.variables['iterations'] = 0.1
    step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')

    assert grizzly.scenario.iterations == 1

    try:
        environ['ITERATIONS'] = '1337'

        step_setup_iterations(behave_context, '$env::ITERATIONS', 'iteration')
        assert grizzly.scenario.iterations == 1337
    finally:
        try:
            del environ['ITERATIONS']
        except KeyError:
            pass

    grizzly.state.configuration['test.iterations'] = 13
    step_setup_iterations(behave_context, '$conf::test.iterations', 'iterations')
    assert grizzly.scenario.iterations == 13


@pytest.mark.usefixtures('behave_context')
def test_step_setup_wait_time(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.scenario.wait.minimum == 1.0
    assert grizzly.scenario.wait.maximum == 1.0

    step_setup_wait_time(behave_context, 8.3, 10.4)

    assert grizzly.scenario.wait.minimum == 8.3
    assert grizzly.scenario.wait.maximum == 10.4


@pytest.mark.usefixtures('behave_context')
def test_step_setup_variable_value(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert 'test' not in grizzly.state.variables

    step_setup_variable_value(behave_context, 'test_string', 'test')
    assert grizzly.state.variables['test_string'] == 'test'

    step_setup_variable_value(behave_context, 'test_int', '1')
    assert grizzly.state.variables['test_int'] == 1

    step_setup_variable_value(behave_context, 'AtomicIntegerIncrementer.test', '1 | step=10')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test'] == '1 | step=10'

    grizzly.state.variables['step'] = 13
    step_setup_variable_value(behave_context, 'AtomicIntegerIncrementer.test2', '1 | step={{ step }}')
    assert grizzly.state.variables['AtomicIntegerIncrementer.test2'] == '1 | step=13'

    grizzly.state.variables['leveranser'] = 100
    step_setup_variable_value(behave_context, 'AtomicRandomString.regnr', '%sA%s1%d%d | count={{ (leveranser * 0.25 + 1) | int }}, upper=True')
    assert grizzly.state.variables['AtomicRandomString.regnr'] == '%sA%s1%d%d | count=26, upper=True'

    step_setup_variable_value(behave_context, 'AtomicDate.test', '2021-04-13')
    assert grizzly.state.variables['AtomicDate.test'] == '2021-04-13'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'AtomicIntegerIncrementer.test', '1 | step=10')

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'dynamic_variable_value', '{{ value }}')

    grizzly.state.variables['value'] = 'hello world!'
    step_setup_variable_value(behave_context, 'dynamic_variable_value', '{{ value }}')

    assert grizzly.state.variables['dynamic_variable_value'] == 'hello world!'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'incorrectly_quoted', '"error\'')


@pytest.mark.usefixtures('behave_context')
def test_step_setup_set_variable_alias(behave_context: Context, mocker: MockerFixture) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert grizzly.state.alias == {}

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    step_setup_variable_value(behave_context, 'AtomicIntegerIncrementer.test', '1337')
    step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert grizzly.state.alias.get('AtomicIntegerIncrementer.test', None) == 'auth.refresh_time'

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    def setitem(self: GrizzlyDict, key: str, value: GrizzlyDictValueType) -> None:
        super(GrizzlyDict, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.types.GrizzlyDict.__setitem__',
        setitem,
    )

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.user.username', 'AtomicCsvRow.users.username')

    step_setup_variable_value(behave_context, 'AtomicCsvRow.users', 'users.csv')
    step_setup_set_variable_alias(behave_context, 'auth.user.username', 'AtomicCsvRow.users.username')

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.user.username', 'AtomicCsvRow.users.username')


@pytest.mark.usefixtures('behave_context')
def test_step_setup_log_all_requests(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    assert 'log_all_requests' not in grizzly.scenario.context

    step_setup_log_all_requests(behave_context)

    assert grizzly.scenario.context['log_all_requests']
