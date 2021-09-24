from os import environ

import pytest

from parse import compile
from behave.runner import Context
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.testdata.models import TemplateDataType, TemplateData

from ...fixtures import behave_context  # pylint: disable=unused-import


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
    context_locust = cast(LocustContext, behave_context.locust)

    step_setup_set_context_variable(behave_context, 'token.url', 'test')
    assert context_locust.scenario.context == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_context_variable(behave_context, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert context_locust.scenario.context == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context_locust.scenario.context = {}

    step_setup_set_context_variable(behave_context, 'test.decimal.value', '1337')
    assert context_locust.scenario.context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_context_variable(behave_context, 'test.float.value', '1.337')
    assert context_locust.scenario.context == {
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
    assert context_locust.scenario.context == {
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
    assert context_locust.scenario.context == {
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
    assert context_locust.scenario.context == {
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
    assert context_locust.scenario.context == {
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

    context_locust.scenario.context = {}

    step_setup_set_context_variable(behave_context, 'text.string.value', 'Hello world!')
    assert context_locust.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_context_variable(behave_context, 'text.string.description', 'simple text')
    assert context_locust.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    context_locust.scenario.context = {}
    step_setup_set_context_variable(behave_context, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert context_locust.scenario.context == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context_locust.scenario.context = {'tenant': 'example.com'}
    step_setup_set_context_variable(behave_context, 'url', 'AZURE')
    assert context_locust.scenario.context == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    context_locust.scenario.context['host'] = 'http://example.com'
    step_setup_set_context_variable(behave_context, 'url', 'HOST')
    assert context_locust.scenario.context == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }


@pytest.mark.usefixtures('behave_context')
def test_step_setup_iterations(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.scenario.iterations == 1

    step_setup_iterations(behave_context, '10', 'iterations')
    assert context_locust.scenario.iterations == 10

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '10', 'iteration')

    step_setup_iterations(behave_context, '1', 'iteration')
    assert context_locust.scenario.iterations == 1

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '1', 'iterations')

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')

    context_locust.state.variables['iterations'] = 100
    step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')
    assert context_locust.scenario.iterations == 100

    step_setup_iterations(behave_context, '{{ iterations * 0.25 }}', 'iteration')
    assert context_locust.scenario.iterations == 25

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '-1', 'iteration')

    with pytest.raises(AssertionError):
        step_setup_iterations(behave_context, '0', 'iteration')

    step_setup_iterations(behave_context, '0', 'iterations')
    assert context_locust.scenario.iterations == 0

    step_setup_iterations(behave_context, '{{ iterations / 101 }}', 'iteration')
    assert context_locust.scenario.iterations == 1

    context_locust.state.variables['iterations'] = 0.1
    step_setup_iterations(behave_context, '{{ iterations }}', 'iteration')

    assert context_locust.scenario.iterations == 1

    try:
        environ['ITERATIONS'] = '1337'

        step_setup_iterations(behave_context, '$env::ITERATIONS', 'iteration')
        assert context_locust.scenario.iterations == 1337
    finally:
        try:
            del environ['ITERATIONS']
        except KeyError:
            pass

    context_locust.state.configuration['test.iterations'] = 13
    step_setup_iterations(behave_context, '$conf::test.iterations', 'iterations')
    assert context_locust.scenario.iterations == 13


@pytest.mark.usefixtures('behave_context')
def test_step_setup_wait_time(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.scenario.wait.minimum == 1.0
    assert context_locust.scenario.wait.maximum == 1.0

    step_setup_wait_time(behave_context, 8.3, 10.4)

    assert context_locust.scenario.wait.minimum == 8.3
    assert context_locust.scenario.wait.maximum == 10.4


@pytest.mark.usefixtures('behave_context')
def test_step_setup_variable_value(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert 'test' not in context_locust.state.variables

    step_setup_variable_value(behave_context, 'test_string', 'test')
    assert context_locust.state.variables['test_string'] == 'test'

    step_setup_variable_value(behave_context, 'test_int', '1')
    assert context_locust.state.variables['test_int'] == 1

    step_setup_variable_value(behave_context, 'AtomicInteger.test', '1')
    assert context_locust.state.variables['AtomicInteger.test'] == 1

    step_setup_variable_value(behave_context, 'AtomicDate.test', '2021-04-13')
    assert context_locust.state.variables['AtomicDate.test'] == '2021-04-13'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'AtomicInteger.test', '1')

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'dynamic_variable_value', '{{ value }}')

    context_locust.state.variables['value'] = 'hello world!'
    step_setup_variable_value(behave_context, 'dynamic_variable_value', '{{ value }}')

    assert context_locust.state.variables['dynamic_variable_value'] == 'hello world!'

    with pytest.raises(AssertionError):
        step_setup_variable_value(behave_context, 'incorrectly_quoted', '"error\'')


@pytest.mark.usefixtures('behave_context')
def test_step_setup_set_variable_alias(behave_context: Context, mocker: MockerFixture) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert context_locust.state.alias == {}

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicInteger.test')

    step_setup_variable_value(behave_context, 'AtomicInteger.test', '1337')
    step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicInteger.test')

    assert context_locust.state.alias.get('AtomicInteger.test', None) == 'auth.refresh_time'

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave_context, 'auth.refresh_time', 'AtomicInteger.test')

    def setitem(self: TemplateData, key: str, value: TemplateDataType) -> None:
        super(TemplateData, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.testdata.models.TemplateData.__setitem__',
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
    context_locust = cast(LocustContext, behave_context.locust)
    assert 'log_all_requests' not in context_locust.scenario.context

    step_setup_log_all_requests(behave_context)

    assert context_locust.scenario.context['log_all_requests']
