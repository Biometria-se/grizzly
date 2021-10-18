import pytest

from os import environ
from typing import cast

from behave.runner import Context

from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.context import LocustContext

from ...fixtures import behave_context  # pylint: disable=unused-import


@pytest.mark.usefixtures('behave_context')
def test_step_setup_save_statistics(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)
    step_impl = step_setup_save_statistics

    with pytest.raises(AssertionError):
        step_impl(behave_context, 'https://test:8000')

    step_impl(behave_context, 'influxdb://test:8000/test_db')
    assert context_locust.setup.statistics_url == 'influxdb://test:8000/test_db'

    try:
        with pytest.raises(AssertionError):
            step_impl(behave_context, 'influxdb://test:8000/$env::DATABASE')

        environ['DATABASE'] = 'test_db'
        step_impl(behave_context, 'influxdb://test:8000/$env::DATABASE')
        assert context_locust.setup.statistics_url == 'influxdb://test:8000/test_db'
    finally:
        try:
            del environ['TEST_VARIABLE']
        except KeyError:
            pass

    step_impl(behave_context, 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb=')
    assert context_locust.setup.statistics_url == 'insights://?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    try:
        with pytest.raises(AssertionError):
            step_impl(behave_context, 'insights://?IngestionEndpoint=$env::TEST_VARIABLE&Testplan=test&InstrumentationKey=aaaabbbb=')
        environ['TEST_VARIABLE'] = 'HelloWorld'

        step_impl(behave_context, 'insights://username:password@?IngestionEndpoint=$env::TEST_VARIABLE&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert context_locust.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='

        step_impl(behave_context, 'insights://username:password@insights.example.com?IngestionEndpoint=$env::TEST_VARIABLE&Testplan=test&InstrumentationKey=aaaabbbb=')
        assert context_locust.setup.statistics_url == 'insights://username:password@insights.example.com?IngestionEndpoint=HelloWorld&Testplan=test&InstrumentationKey=aaaabbbb='
    finally:
        try:
            del environ['TEST_VARIABLE']
        except KeyError:
            pass

    with pytest.raises(AssertionError):
        step_impl(
            behave_context,
            (
                'insights://$conf::statistics.username:$conf::statistics.password@?IngestionEndpoint=$conf::statistics.url&'
                'Testplan=$conf::statistics.testplan&InstrumentationKey=$conf::statistics.instrumentationkey'
            )
        )

    context_locust.state.configuration['statistics.url'] = 'insights.example.com'
    context_locust.state.configuration['statistics.testplan'] = 'test'
    context_locust.state.configuration['statistics.instrumentationkey'] = 'aaaabbbb='
    context_locust.state.configuration['statistics.username'] = 'username'
    context_locust.state.configuration['statistics.password'] = 'password'

    step_impl(
        behave_context,
        (
            'insights://$conf::statistics.username:$conf::statistics.password@?'
            'IngestionEndpoint=$conf::statistics.url&Testplan=$conf::statistics.testplan&'
            'InstrumentationKey=$conf::statistics.instrumentationkey'
        ),
    )
    context_locust.setup.statistics_url == 'insights://username:password@?IngestionEndpoint=insights.example.com&Testplan=test&InstrumentationKey=aaaabbbb='

    parsed = urlparse(context_locust.setup.statistics_url)
    assert parsed.username == 'username'
    assert parsed.password == 'password'


@pytest.mark.usefixtures('behave_context')
def test_step_setup_enable_stop_on_failure(behave_context: Context) -> None:
    step_impl = step_setup_enable_stop_on_failure

    context_locust = cast(LocustContext, behave_context.locust)

    assert not behave_context.config.stop
    assert not context_locust.scenario.stop_on_failure

    step_impl(behave_context)

    assert behave_context.config.stop
    assert context_locust.scenario.stop_on_failure


@pytest.mark.usefixtures('behave_context')
def test_step_setup_log_level(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)
    step_impl = step_setup_log_level

    assert context_locust.setup.log_level == 'INFO'

    with pytest.raises(AssertionError):
        step_impl(behave_context, 'WARN')

    step_impl(behave_context, 'DEBUG')
    assert context_locust.setup.log_level == 'DEBUG'

    step_impl(behave_context, 'WARNING')
    assert context_locust.setup.log_level == 'WARNING'

    step_impl(behave_context, 'ERROR')
    assert context_locust.setup.log_level == 'ERROR'

    step_impl(behave_context, 'INFO')
    assert context_locust.setup.log_level == 'INFO'


@pytest.mark.usefixtures('behave_context')
def test_step_setup_run_time(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)
    step_impl = step_setup_run_time

    assert context_locust.setup.timespan is None

    step_impl(behave_context, '10s')

    assert context_locust.setup.timespan == '10s'


@pytest.mark.usefixtures('behave_context')
def test_step_setup_set_global_context_variable(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    step_setup_set_global_context_variable(behave_context, 'token.url', 'test')
    assert context_locust.setup.global_context == {
        'token': {
            'url': 'test',
        },
    }

    step_setup_set_global_context_variable(behave_context, 'token.client_id', 'aaaa-bbbb-cccc-dddd')
    assert context_locust.setup.global_context == {
        'token': {
            'url': 'test',
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context_locust.setup.global_context = {}

    step_setup_set_global_context_variable(behave_context, 'test.decimal.value', '1337')
    assert context_locust.setup.global_context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
        },
    }

    step_setup_set_global_context_variable(behave_context, 'test.float.value', '1.337')
    assert context_locust.setup.global_context == {
        'test': {
            'decimal': {
                'value': 1337,
            },
            'float': {
                'value': 1.337,
            },
        },
    }

    step_setup_set_global_context_variable(behave_context, 'test.bool.value', 'true')
    assert context_locust.setup.global_context == {
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

    step_setup_set_global_context_variable(behave_context, 'test.bool.value', 'True')
    assert context_locust.setup.global_context == {
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

    step_setup_set_global_context_variable(behave_context, 'test.bool.value', 'false')
    assert context_locust.setup.global_context == {
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

    step_setup_set_global_context_variable(behave_context, 'test.bool.value', 'FaLsE')
    assert context_locust.setup.global_context == {
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

    context_locust.setup.global_context = {}

    step_setup_set_global_context_variable(behave_context, 'text.string.value', 'Hello world!')
    assert context_locust.setup.global_context == {
        'text': {
            'string': {
                'value': 'Hello world!',
            },
        },
    }

    step_setup_set_global_context_variable(behave_context, 'text.string.description', 'simple text')
    assert context_locust.setup.global_context == {
        'text': {
            'string': {
                'value': 'Hello world!',
                'description': 'simple text',
            },
        },
    }

    context_locust.setup.global_context = {}
    step_setup_set_global_context_variable(behave_context, 'Token/Client ID', 'aaaa-bbbb-cccc-dddd')
    assert context_locust.setup.global_context == {
        'token': {
            'client_id': 'aaaa-bbbb-cccc-dddd',
        },
    }

    context_locust.setup.global_context = {'tenant': 'example.com'}
    step_setup_set_global_context_variable(behave_context, 'url', 'AZURE')
    assert context_locust.setup.global_context == {
        'url': 'AZURE',
        'tenant': 'example.com',
    }

    context_locust.setup.global_context['host'] = 'http://example.com'
    step_setup_set_global_context_variable(behave_context, 'url', 'HOST')
    assert context_locust.setup.global_context == {
        'url': 'HOST',
        'tenant': 'example.com',
        'host': 'http://example.com',
    }
