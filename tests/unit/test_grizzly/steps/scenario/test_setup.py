from typing import cast

from os import environ

import pytest

from parse import compile
from pytest_mock import MockerFixture

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403
from grizzly.testdata import GrizzlyVariables, GrizzlyVariableType
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestDirection, RequestMethod

from tests.fixtures import BehaveFixture


def test_parse_iteration_gramatical_number() -> None:
    p = compile(
        'run for {iteration:d} {iteration_number:IterationGramaticalNumber}',
        extra_types=dict(IterationGramaticalNumber=parse_iteration_gramatical_number),
    )

    assert parse_iteration_gramatical_number.__vector__ == (False, True,)

    assert p.parse('run for 1 iteration')['iteration_number'] == 'iteration'
    assert p.parse('run for 10 iterations')['iteration_number'] == 'iterations'
    assert p.parse('run for 4 laps') is None

    assert parse_iteration_gramatical_number(' asdf ') == 'asdf'


def test_step_setup_set_context_variable(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    step_setup_set_context_variable(behave, 'token.url', 'test')
    assert grizzly.scenario.context == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_context_variable(behave, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.scenario.context == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.scenario.context = {}

    step_setup_set_context_variable(behave, 'test.decimal.value', '1337')
    assert grizzly.scenario.context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_context_variable(behave, 'test.float.value', '1.337')
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

    step_setup_set_context_variable(behave, 'test.bool.value', 'true')
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

    step_setup_set_context_variable(behave, 'test.bool.value', 'True')
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

    step_setup_set_context_variable(behave, 'test.bool.value', 'false')
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

    step_setup_set_context_variable(behave, 'test.bool.value', 'FaLsE')
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

    step_setup_set_context_variable(behave, 'text.string.value', 'Hello world!')
    assert grizzly.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_context_variable(behave, 'text.string.description', 'simple text')
    assert grizzly.scenario.context == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    grizzly.scenario.context = {}
    step_setup_set_context_variable(behave, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.scenario.context == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.scenario.context = {'tenant': 'example.com'}
    step_setup_set_context_variable(behave, 'url', 'AZURE')
    assert grizzly.scenario.context == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    grizzly.scenario.context['host'] = 'http://example.com'
    step_setup_set_context_variable(behave, 'url', 'HOST')
    assert grizzly.scenario.context == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }

    step_setup_set_context_variable(behave, 'www.example.com/auth.user.username', 'bob')
    step_setup_set_context_variable(behave, 'www.example.com/auth.user.password', 'password')
    assert grizzly.scenario.context == {
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


def test_step_setup_iterations(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.iterations == 1

    step_setup_iterations(behave, '10', 'iterations')
    assert grizzly.scenario.iterations == 10

    step_setup_iterations(behave, '1', 'iteration')
    assert grizzly.scenario.iterations == 1

    with pytest.raises(AssertionError):
        step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    grizzly.state.variables['iterations'] = 100
    step_setup_iterations(behave, '{{ iterations }}', 'iteration')
    assert grizzly.scenario.iterations == 100

    step_setup_iterations(behave, '{{ iterations * 0.25 }}', 'iteration')
    assert grizzly.scenario.iterations == 25

    step_setup_iterations(behave, '0', 'iterations')
    assert grizzly.scenario.iterations == 0

    step_setup_iterations(behave, '{{ iterations / 101 }}', 'iteration')
    assert grizzly.scenario.iterations == 1

    grizzly.state.variables['iterations'] = 0.1
    step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    assert grizzly.scenario.iterations == 1

    try:
        environ['ITERATIONS'] = '1337'

        step_setup_iterations(behave, '$env::ITERATIONS$', 'iteration')
        assert grizzly.scenario.iterations == 1337
    finally:
        try:
            del environ['ITERATIONS']
        except KeyError:
            pass

    grizzly.state.configuration['test.iterations'] = 13
    step_setup_iterations(behave, '$conf::test.iterations$', 'iterations')
    assert grizzly.scenario.iterations == 13


def test_step_setup_pace(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario, 'pace', '') is None

    step_setup_pace(behave, '2000')

    assert len(grizzly.scenario.orphan_templates) == 0
    assert grizzly.scenario.pace == '2000'

    with pytest.raises(AssertionError) as ae:
        step_setup_pace(behave, 'asdf')
    assert str(ae.value) == '"asdf" is neither a template or a number'

    step_setup_pace(behave, '{{ pace }}')

    assert grizzly.scenario.orphan_templates == ['{{ pace }}']
    assert grizzly.scenario.pace == '{{ pace }}'


def test_step_setup_set_variable_alias(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert grizzly.state.alias == {}

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1337')
    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert grizzly.state.alias.get('AtomicIntegerIncrementer.test', None) == 'auth.refresh_time'

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    def setitem(self: GrizzlyVariables, key: str, value: GrizzlyVariableType) -> None:
        super(GrizzlyVariables, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.testdata.GrizzlyVariables.__setitem__',
        setitem,
    )

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    step_setup_variable_value(behave, 'AtomicCsvReader.users', 'users.csv')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    with pytest.raises(AssertionError):
        step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')


def test_step_setup_log_all_requests(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert 'log_all_requests' not in grizzly.scenario.context

    step_setup_log_all_requests(behave)

    assert grizzly.scenario.context['log_all_requests']


def test_step_setup_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.context.get('metadata', None) is None

    step_setup_metadata(behave, 'Content-Type', 'application/json')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/json',
    }

    step_setup_metadata(behave, 'Content-Type', 'application/xml')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/xml',
    }

    step_setup_metadata(behave, 'Ocp-Apim-Subscription-Key', 'deadbeefb00f')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/xml',
        'Ocp-Apim-Subscription-Key': 'deadbeefb00f',
    }

    grizzly.scenario.context['metadata'] = None
    request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
    grizzly.scenario.tasks.add(request)
    step_setup_metadata(behave, 'new_header', 'new_value')

    assert grizzly.scenario.context.get('metadata', {}) is None
    assert request.metadata == {'new_header': 'new_value'}

    grizzly.state.variables.update({'test_payload': 'none', 'test_metadata': 'none'})
    task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')

    grizzly.scenario.tasks.add(task_factory)
    step_setup_metadata(behave, 'x-test-header', 'foobar')

    assert grizzly.scenario.context.get('metadata', {}) is None
    assert request.metadata == {'new_header': 'new_value'}
    assert task_factory._context['metadata'] == {'x-test-header': 'foobar'}
