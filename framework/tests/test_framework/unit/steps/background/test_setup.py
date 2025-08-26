"""Unit tests for grizzly.steps.background.setup."""

from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

from grizzly.steps import *
from grizzly.types import MessageDirection
from parse import compile as parse_compile

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_parse_message_direction() -> None:
    p = parse_compile(
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
    grizzly = cast('GrizzlyContext', behave_fixture.context.grizzly)
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


def test_step_setup_log_level(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
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
    grizzly = cast('GrizzlyContext', behave.grizzly)
    step_impl = step_setup_run_time

    assert grizzly.setup.timespan is None

    step_impl(behave, '10s')

    assert grizzly.setup.timespan == '10s'


def test_step_setup_message_type_callback(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
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

    step_setup_message_type_callback(behave, 'test_framework.helpers.message_callback_does_not_exist', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='module test_framework.helpers has no method message_callback_does_not_exist')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'test_framework.helpers.message_callback_not_a_method', 'foo_message', 'server', 'client')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='test_framework.helpers.message_callback_not_a_method is not a method')]}
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'test_framework.helpers.message_callback_incorrect_sig', 'foo_message', 'server', 'client')
    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='test_framework.helpers.message_callback_incorrect_sig does not have grizzly.types.MessageCallback method signature: '),
        ],
    }
    delattr(behave, 'exceptions')

    step_setup_message_type_callback(behave, 'test_framework.helpers.message_callback', 'foo_message', 'server', 'client')

    from test_framework.helpers import message_callback

    assert grizzly.setup.locust.messages == {
        MessageDirection.SERVER_CLIENT: {
            'foo_message': message_callback,
        },
    }


def test_step_setup_configuration_value(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_setup_configuration_value(behave, 'default.host', 'example.com')

    assert grizzly.state.configuration['default.host'] == 'example.com'

    step_setup_configuration_value(behave, 'default.url', 'https://$conf::default.host$')

    assert grizzly.state.configuration['default.url'] == 'https://example.com'

    environ.update({'TEST_VAR': 'foobar'})

    step_setup_configuration_value(behave, 'env.var', '$env::TEST_VAR$')

    assert grizzly.state.configuration['env.var'] == 'foobar'


def test_step_setup_wait_spawning_complete_timeout(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.setup, 'wait_for_spawning_complete', '') is None

    step_setup_wait_spawning_complete_timeout(behave, 10.0)

    assert grizzly.setup.wait_for_spawning_complete == 10.0


def test_step_setup_wait_spawning_complete_indefinitely(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.setup, 'wait_for_spawning_complete', '') is None

    step_setup_wait_spawning_complete_indefinitely(behave)

    assert grizzly.setup.wait_for_spawning_complete == -1
