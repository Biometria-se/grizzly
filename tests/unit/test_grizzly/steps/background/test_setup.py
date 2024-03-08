"""Unit tests for grizzly.steps.background.setup."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.exceptions import RestartScenario
from grizzly.steps import *
from grizzly.types import MessageDirection
from grizzly.types.locust import StopUser
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_parse_message_direction() -> None:
    p = compile(
        'sending from {from:MessageDirection} to {to:MessageDirection}',
        extra_types={
            'MessageDirection': parse_message_direction,
        },
    )

    assert MessageDirection.get_vector() == (True, True)
    assert parse_message_direction.__vector__ == (True, True)

    result = p.parse('sending from server to client')
    message_direction = MessageDirection.from_string(f'{result["from"]}_{result["to"]}')
    assert message_direction == MessageDirection.SERVER_CLIENT

    result = p.parse('sending from client to server')
    message_direction = MessageDirection.from_string(f'{result["from"]}_{result["to"]}')
    assert message_direction == MessageDirection.CLIENT_SERVER


def test_step_setup_save_statistics(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave_fixture.context.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    step_impl = step_setup_save_statistics

    step_impl(behave, 'https://test:8000')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"https" is not a supported scheme')]}
    delattr(behave, 'exceptions')

    step_impl(behave, 'influxdb://test:8000/test_db')
    assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'

    try:
        step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='environment variable "DATABASE" is not set')]}
        delattr(behave, 'exceptions')

        environ['DATABASE'] = 'test_db'
        step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')
        assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'
    finally:
        with suppress(KeyError):
            del environ['TEST_VARIABLE']

    step_impl(behave, 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb=')
    assert grizzly.setup.statistics_url == 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    try:
        step_impl(behave, 'insights://?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='environment variable "TEST_VARIABLE" is not set')]}
        delattr(behave, 'exceptions')

        environ['TEST_VARIABLE'] = 'HelloWorld'

        step_impl(behave, 'insights://username:password@?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='

        step_impl(behave, 'insights://username:password@insights.example.com?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@insights.example.com?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='
    finally:
        with suppress(KeyError):
            del environ['TEST_VARIABLE']

    step_impl(
        behave,
        (
            'insights://$conf::statistics.username$:$conf::statistics.password$@?IngestionEndpoint=$conf::statistics.url$&'
            'Testplan=$conf::statistics.testplan$&InstrumentationKey=$conf::statistics.instrumentationkey$'
        ),
    )
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='configuration variable "statistics.username" is not set')]}
    delattr(behave, 'exceptions')

    grizzly.state.configuration['statistics.url'] = 'insights.example.com'
    grizzly.state.configuration['statistics.testplan'] = 'test'
    grizzly.state.configuration['statistics.instrumentationkey'] = 'aaaabbbb='
    grizzly.state.configuration['statistics.username'] = 'username'
    grizzly.state.configuration['statistics.password'] = 'password'

    step_impl(
        behave,
        (
            'insights://$conf::statistics.username$:$conf::statistics.password$@?'
            'IngestionEndpoint=$conf::statistics.url$&Testplan=$conf::statistics.testplan$&'
            'InstrumentationKey=$conf::statistics.instrumentationkey$'
        ),
    )
    grizzly.setup.statistics_url = 'insights://username:password@?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    parsed = urlparse(grizzly.setup.statistics_url)
    assert parsed.username == 'username'
    assert parsed.password == 'password'  # noqa: S105


def test_step_setup_stop_user_on_failure(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.failure_exception is None

    step_setup_stop_user_on_failure(behave)

    assert grizzly.scenario.failure_exception == StopUser


def test_step_setup_restart_scenario_on_failure(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert not behave.config.stop
    assert grizzly.scenario.failure_exception is None

    step_setup_restart_scenario_on_failure(behave)

    assert not behave.config.stop
    assert grizzly.scenario.failure_exception == RestartScenario


def test_step_setup_log_level(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    step_impl = step_setup_log_level

    assert grizzly.setup.log_level == 'INFO'

    step_impl(behave, 'WARN')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='log level WARN is not supported')]}

    for log_level in ['DEBUG', 'WARNING', 'ERROR', 'INFO']:
        step_impl(behave, log_level)
        assert grizzly.setup.log_level == log_level


def test_step_setup_run_time(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    step_impl = step_setup_run_time

    assert grizzly.setup.timespan is None

    step_impl(behave, '10s')

    assert grizzly.setup.timespan == '10s'


def test_step_setup_set_global_context_variable(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    step_setup_set_global_context_variable(behave, 'token.url', 'test')
    assert grizzly.setup.global_context == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_global_context_variable(behave, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.setup.global_context == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.setup.global_context = {}

    step_setup_set_global_context_variable(behave, 'test.decimal.value', '1337')
    assert grizzly.setup.global_context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_global_context_variable(behave, 'test.float.value', '1.337')
    assert grizzly.setup.global_context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
        },
    }

    step_setup_set_global_context_variable(behave, 'test.bool.value', 'true')
    assert grizzly.setup.global_context == {
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

    step_setup_set_global_context_variable(behave, 'test.bool.value', 'True')
    assert grizzly.setup.global_context == {
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

    step_setup_set_global_context_variable(behave, 'test.bool.value', 'false')
    assert grizzly.setup.global_context == {
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

    step_setup_set_global_context_variable(behave, 'test.bool.value', 'FaLsE')
    assert grizzly.setup.global_context == {
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

    grizzly.setup.global_context = {}

    step_setup_set_global_context_variable(behave, 'text.string.value', 'Hello world!')
    assert grizzly.setup.global_context == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_global_context_variable(behave, 'text.string.description', 'simple text')
    assert grizzly.setup.global_context == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    grizzly.setup.global_context = {}
    step_setup_set_global_context_variable(behave, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert grizzly.setup.global_context == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    grizzly.setup.global_context = {'tenant': 'example.com'}
    step_setup_set_global_context_variable(behave, 'url', 'AZURE')
    assert grizzly.setup.global_context == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    grizzly.setup.global_context['host'] = 'http://example.com'
    step_setup_set_global_context_variable(behave, 'url', 'HOST')
    assert grizzly.setup.global_context == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }


def test_step_setup_message_type_callback(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert grizzly.setup.locust.messages == {}

    step_setup_message_type_callback(behave, 'foobar.method', 'foo_message', 'server', 'server')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='cannot register message handler that sends from server and is received at server')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'foobar.method', 'foo_message', 'client', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='cannot register message handler that sends from client and is received at client')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'foobar.method', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no module named foobar')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'tests.helpers.message_callback_does_not_exist', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='module tests.helpers has no method message_callback_does_not_exist')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'tests.helpers.message_callback_not_a_method', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='tests.helpers.message_callback_not_a_method is not a method')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'tests.helpers.message_callback_incorrect_sig', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='tests.helpers.message_callback_incorrect_sig does not have grizzly.types.MessageCallback method signature: '),
    ]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'tests.helpers.message_callback', 'foo_message', 'server', 'client')

    from tests.helpers import message_callback

    assert grizzly.setup.locust.messages == {
        MessageDirection.SERVER_CLIENT: {
            'foo_message': message_callback,
        },
    }
