"""Unit tests for grizzly.steps.scenario.setup."""
from __future__ import annotations

from contextlib import suppress
from hashlib import sha256
from os import environ
from typing import TYPE_CHECKING, cast

from parse import compile

from grizzly.auth import AAD
from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.tasks.clients import HttpClientTask
from grizzly.testdata import GrizzlyVariables, GrizzlyVariableType
from grizzly.types import RequestDirection, RequestMethod
from grizzly.users import RestApiUser
from grizzly_extras.azure.aad import AzureAadCredential
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture, GrizzlyFixture


def test_parse_iteration_gramatical_number() -> None:
    p = compile(
        'run for {iteration:d} {iteration_number:IterationGramaticalNumber}',
        extra_types={'IterationGramaticalNumber': parse_iteration_gramatical_number},
    )

    assert parse_iteration_gramatical_number.__vector__ == (False, True)

    assert p.parse('run for 1 iteration')['iteration_number'] == 'iteration'
    assert p.parse('run for 10 iterations')['iteration_number'] == 'iterations'
    assert p.parse('run for 4 laps') is None

    assert parse_iteration_gramatical_number(' asdf ') == 'asdf'


def test_step_setup_set_context_variable_runtime(grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
    parent = grizzly_fixture(user_type=RestApiUser)

    assert isinstance(parent.user, RestApiUser)

    grizzly = grizzly_fixture.grizzly
    behave = grizzly_fixture.behave.context

    task = grizzly.scenario.tasks.pop()

    assert len(grizzly.scenario.tasks) == 0
    assert parent.user.__cached_auth__ == {}

    step_setup_set_context_variable(behave, 'auth.user.username', 'bob')
    step_setup_set_context_variable(behave, 'auth.user.password', 'foobar')

    assert grizzly.scenario.context == {
        'host': '',
        'auth': {'user': {'username': 'bob', 'password': 'foobar'},
    }}
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

    AAD.initialize(parent.user)

    assert isinstance(parent.user.credential, AzureAadCredential)
    credential_bob = parent.user.credential

    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='bob', password='foobar')  # noqa: S106
    assert parent.user.credential.username == 'bob'
    assert parent.user.credential.password == 'foobar'  # noqa: S105

    task_factory()(parent)

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='alice', password='foobar')  # noqa: S106
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

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='alice', password='hello world')  # noqa: S106
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

    assert parent.user._context.get('auth', {}).get('user', {}) == SOME(dict, username='bob', password='foobar')  # noqa: S106
    assert parent.user.credential == SOME(AzureAadCredential, username=credential_bob.username, password=credential_bob.password)
    assert parent.user.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': 'RestApiUser_001',
    }
    assert parent.user.__context_change_history__ == set()

def test_step_setup_set_context_variable_init(behave_fixture: BehaveFixture) -> None:
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
    behave.scenario = grizzly.scenario.behave

    assert grizzly.scenario.iterations == 1
    assert behave.exceptions == {}

    step_setup_iterations(behave, '10', 'iterations')
    assert grizzly.scenario.iterations == 10

    step_setup_iterations(behave, '1', 'iteration')
    assert grizzly.scenario.iterations == 1

    step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='value contained variable "iterations" which has not been declared')]}

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
        with suppress(KeyError):
            del environ['ITERATIONS']

    grizzly.state.configuration['test.iterations'] = 13
    step_setup_iterations(behave, '$conf::test.iterations$', 'iterations')
    assert grizzly.scenario.iterations == 13


def test_step_setup_pace(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.scenario, 'pace', '') is None
    assert behave.exceptions == {}

    step_setup_pace(behave, '2000')

    assert len(grizzly.scenario.orphan_templates) == 0
    assert grizzly.scenario.pace == '2000'

    step_setup_pace(behave, 'asdf')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"asdf" is neither a template or a number')]}

    step_setup_pace(behave, '{{ pace }}')

    assert grizzly.scenario.orphan_templates == ['{{ pace }}']
    assert grizzly.scenario.pace == '{{ pace }}'


def test_step_setup_set_variable_alias(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert grizzly.state.alias == {}
    assert behave.exceptions == {}

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared')]}

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1337')
    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert grizzly.state.alias.get('AtomicIntegerIncrementer.test', None) == 'auth.refresh_time'

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
    ]}

    def setitem(self: GrizzlyVariables, key: str, value: GrizzlyVariableType) -> None:
        super(GrizzlyVariables, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.testdata.GrizzlyVariables.__setitem__',
        setitem,
    )

    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
        ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
    ]}

    step_setup_variable_value(behave, 'AtomicCsvReader.users', 'users.csv')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
        ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicCsvReader.users.username already exists: auth.user.username'),
    ]}


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
    HttpClientTask.__scenario__ = grizzly.scenario
    task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')

    grizzly.scenario.tasks.add(task_factory)
    step_setup_metadata(behave, 'x-test-header', 'foobar')

    assert grizzly.scenario.context.get('metadata', {}) is None
    assert request.metadata == {'new_header': 'new_value'}
    assert task_factory._context['metadata'] == {'x-test-header': 'foobar'}
