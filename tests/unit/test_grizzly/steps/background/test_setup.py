"""Unit tests for grizzly.steps.background.setup."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import pytest
from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.exceptions import RestartScenario
from grizzly.steps import *
from grizzly.types import MessageDirection
from grizzly.types.locust import StopUser

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
    step_impl = step_setup_save_statistics

    with pytest.raises(AssertionError, match='"https" is not a supported scheme'):
        step_impl(behave, 'https://test:8000')

    step_impl(behave, 'influxdb://test:8000/test_db')
    assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'

    try:
        with pytest.raises(AssertionError, match='environment variable "DATABASE" is not set'):
            step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')

        environ['DATABASE'] = 'test_db'
        step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')
        assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'
    finally:
        with suppress(KeyError):
            del environ['TEST_VARIABLE']

    step_impl(behave, 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb=')
    assert grizzly.setup.statistics_url == 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    try:
        with pytest.raises(AssertionError, match='environment variable "TEST_VARIABLE" is not set'):
            step_impl(behave, 'insights://?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        environ['TEST_VARIABLE'] = 'HelloWorld'

        step_impl(behave, 'insights://username:password@?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='

        step_impl(behave, 'insights://username:password@insights.example.com?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@insights.example.com?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='
    finally:
        with suppress(KeyError):
            del environ['TEST_VARIABLE']

    with pytest.raises(AssertionError, match='configuration variable "statistics.username" is not set'):
        step_impl(
            behave,
            (
                'insights://$conf::statistics.username$:$conf::statistics.password$@?IngestionEndpoint=$conf::statistics.url$&'
                'Testplan=$conf::statistics.testplan$&InstrumentationKey=$conf::statistics.instrumentationkey$'
            ),
        )

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

    assert not behave.config.stop
    assert grizzly.scenario.failure_exception is None

    step_setup_stop_user_on_failure(behave)

    assert behave.config.stop
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
    step_impl = step_setup_log_level

    assert grizzly.setup.log_level == 'INFO'

    with pytest.raises(AssertionError, match='log level WARN is not supported'):
        step_impl(behave, 'WARN')

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
    grizzly = behave_fixture.grizzly

    assert grizzly.setup.locust.messages == {}

    with pytest.raises(AssertionError, match='cannot register message handler that sends from server and is received at server'):
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'server', 'server')

    with pytest.raises(AssertionError, match='cannot register message handler that sends from client and is received at client'):
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'client', 'client')

    with pytest.raises(AssertionError, match='no module named foobar'):
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'server', 'client')

    with pytest.raises(AssertionError, match='module tests.helpers has no method message_callback_does_not_exist'):
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_does_not_exist', 'foo_message', 'server', 'client')

    with pytest.raises(AssertionError, match='tests.helpers.message_callback_not_a_method is not a method'):
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_not_a_method', 'foo_message', 'server', 'client')

    with pytest.raises(AssertionError, match='tests.helpers.message_callback_incorrect_sig does not have grizzly.types.MessageCallback method signature: '):
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_incorrect_sig', 'foo_message', 'server', 'client')

    step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback', 'foo_message', 'server', 'client')

    from tests.helpers import message_callback

    assert grizzly.setup.locust.messages == {
        MessageDirection.SERVER_CLIENT: {
            'foo_message': message_callback,
        },
    }
