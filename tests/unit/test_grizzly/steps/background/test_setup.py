import pytest

from os import environ
from typing import cast

from urllib.parse import urlparse

from parse import compile
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403
from grizzly.context import GrizzlyContext
from grizzly.exceptions import RestartScenario
from grizzly.types import MessageDirection
from grizzly.types.locust import StopUser

from tests.fixtures import BehaveFixture


def test_parse_message_direction() -> None:
    p = compile(
        'sending from {from:MessageDirection} to {to:MessageDirection}',
        extra_types=dict(
            MessageDirection=parse_message_direction,
        )
    )

    assert MessageDirection.get_vector() == (True, True,)
    assert parse_message_direction.__vector__ == (True, True,)

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

    with pytest.raises(AssertionError):
        step_impl(behave, 'https://test:8000')

    step_impl(behave, 'influxdb://test:8000/test_db')
    assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'

    try:
        with pytest.raises(AssertionError):
            step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')

        environ['DATABASE'] = 'test_db'
        step_impl(behave, 'influxdb://test:8000/$env::DATABASE$')
        assert grizzly.setup.statistics_url == 'influxdb://test:8000/test_db'
    finally:
        try:
            del environ['TEST_VARIABLE']
        except KeyError:
            pass

    step_impl(behave, 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb=')
    assert grizzly.setup.statistics_url == 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    try:
        with pytest.raises(AssertionError):
            step_impl(behave, 'insights://?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        environ['TEST_VARIABLE'] = 'HelloWorld'

        step_impl(behave, 'insights://username:password@?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='

        step_impl(behave, 'insights://username:password@insights.example.com?IngestionEndpoint=$env::TEST_VARIABLE$&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert grizzly.setup.statistics_url == 'insights://username:password@insights.example.com?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='
    finally:
        try:
            del environ['TEST_VARIABLE']
        except KeyError:
            pass

    with pytest.raises(AssertionError):
        step_impl(
            behave,
            (
                'insights://$conf::statistics.username:$conf::statistics.password@?IngestionEndpoint=$conf::statistics.url&'
                'Testplan=$conf::statistics.testplan&InstrumentationKey=$conf::statistics.instrumentationkey'
            )
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
    grizzly.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    parsed = urlparse(grizzly.setup.statistics_url)
    assert parsed.username == 'username'
    assert parsed.password == 'password'


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

    with pytest.raises(AssertionError):
        step_impl(behave, 'WARN')

    step_impl(behave, 'DEBUG')
    assert grizzly.setup.log_level == 'DEBUG'

    step_impl(behave, 'WARNING')
    assert grizzly.setup.log_level == 'WARNING'

    step_impl(behave, 'ERROR')
    assert grizzly.setup.log_level == 'ERROR'

    step_impl(behave, 'INFO')
    assert grizzly.setup.log_level == 'INFO'


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

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'server', 'server')
    assert str(ae.value) == 'cannot register message handler that sends from server and is received at server'

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'client', 'client')
    assert str(ae.value) == 'cannot register message handler that sends from client and is received at client'

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'foobar.method', 'foo_message', 'server', 'client')
    assert str(ae.value) == 'no module named foobar'

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_does_not_exist', 'foo_message', 'server', 'client')
    assert str(ae.value) == 'module tests.helpers has no method message_callback_does_not_exist'

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_not_a_method', 'foo_message', 'server', 'client')
    assert str(ae.value) == 'tests.helpers.message_callback_not_a_method is not a method'

    with pytest.raises(AssertionError) as ae:
        step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback_incorrect_sig', 'foo_message', 'server', 'client')
    assert str(ae.value) == (
        'tests.helpers.message_callback_incorrect_sig does not have grizzly.types.MessageCallback method signature: (msg: locust.rpc.protocol.Message, '
        'environment: locust.env.Environment) -> locust.rpc.protocol.Message'
    )

    step_setup_message_type_callback(behave_fixture.context, 'tests.helpers.message_callback', 'foo_message', 'server', 'client')

    from tests.helpers import message_callback

    assert grizzly.setup.locust.messages == {
        MessageDirection.SERVER_CLIENT: {
            'foo_message': message_callback,
        }
    }
