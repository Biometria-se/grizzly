import logging

from typing import Callable, Dict, Any, Optional

import pytest

from opencensus.ext.azure.log_exporter import AzureLogHandler
from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture

from grizzly.listeners.appinsights import ApplicationInsightsListener
from grizzly.types.locust import CatchResponseError

from tests.fixtures import LocustFixture


@pytest.fixture
def patch_azureloghandler(mocker: MockerFixture) -> Callable[[], None]:
    def wrapper() -> None:
        def AzureLogHandler__init__(self: AzureLogHandler, connection_string: str) -> None:
            pass

        mocker.patch(
            'opencensus.ext.azure.log_exporter.AzureLogHandler.__init__',
            AzureLogHandler__init__,
        )

        def AzureLogHandler_flush(self: AzureLogHandler) -> None:
            pass

        mocker.patch(
            'opencensus.ext.azure.log_exporter.BaseLogHandler.flush',
            AzureLogHandler_flush,
        )

    return wrapper


class TestAppInsightsListener:
    @pytest.mark.usefixtures('patch_azureloghandler')
    def test___init__(self, locust_fixture: LocustFixture, patch_azureloghandler: Callable[[], None]) -> None:
        patch_azureloghandler()

        # fire_deprecated_request_handlers is already an internal event handler for the request event
        assert len(locust_fixture.environment.events.request._handlers) == 1

        with pytest.raises(AssertionError) as ae:
            ApplicationInsightsListener(locust_fixture.environment, '?')
        assert 'IngestionEndpoint was neither set as the hostname or in the query string' in str(ae)

        with pytest.raises(AssertionError) as ae:
            ApplicationInsightsListener(locust_fixture.environment, '?IngestionEndpoint=insights.test.com')
        assert 'InstrumentationKey not found in' in str(ae)

        try:
            listener = ApplicationInsightsListener(locust_fixture.environment, '?IngestionEndpoint=insights.test.com&InstrumentationKey=asdfasdfasdfasdf')

            assert len(locust_fixture.environment.events.request._handlers) == 2
            assert listener.testplan == 'appinsightstestplan'
        finally:
            # remove ApplicationInsightsListener
            locust_fixture.environment.events.request._handlers.pop()

        try:
            listener = ApplicationInsightsListener(locust_fixture.environment, 'https://insights.test.com?InstrumentationKey=asdfasdfasdfasdf&Testplan=test___init__')

            assert len(locust_fixture.environment.events.request._handlers) == 2
            assert listener.testplan == 'test___init__'
        finally:
            # remove ApplicationInsightsListener
            locust_fixture.environment.events.request._handlers.pop()

    @pytest.mark.usefixtures('patch_azureloghandler')
    def test_request(self, locust_fixture: LocustFixture, patch_azureloghandler: Callable[[], None], mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        patch_azureloghandler()

        def generate_logger_info(
            request_type: str, name: str, response_time: float, response_length: int, exception: Optional[Any] = None
        ) -> Callable[[logging.Handler, str, Dict[str, Any]], None]:
            result = 'Success' if exception is None else 'Failure'
            expected_message = f'{result}: {request_type} {name} Response time: {int(round(response_time, 0))} Number of Threads: {""}'

            if exception is not None:
                expected_message = f'{expected_message} Exception: {str(exception)}'

            def logger_info(self: logging.Handler, msg: str, extra: Dict[str, Any]) -> None:
                assert msg == expected_message
                assert extra is not None
                custom_dimensions = extra.get('custom_dimensions', None)
                assert custom_dimensions is not None
                assert custom_dimensions.get('thread_count', None) == ''
                assert custom_dimensions.get('spawn_rate', None) == ''
                assert custom_dimensions.get('target_user_count', None) == ''
                assert custom_dimensions.get('response_length', None) == response_length

            return logger_info

        mocker.patch(
            'logging.Logger.info',
            generate_logger_info('GET', '/api/v1/test', 133.7, 200, None),
        )

        try:
            listener = ApplicationInsightsListener(locust_fixture.environment, 'https://insights.test.com?InstrumentationKey=asdfasdfasdfasdf')
            listener.request('GET', '/api/v1/test', 133.7, 200, None)

            mocker.patch(
                'logging.Logger.info',
                generate_logger_info('POST', '/api/v2/test', 3133.7, 555, CatchResponseError('request failed')),
            )

            listener.request('POST', '/api/v2/test', 3133.7, 555, CatchResponseError('request failed'))

            mocker.patch.object(listener, '_create_custom_dimensions_dict', side_effect=[Exception])

            with caplog.at_level(logging.ERROR):
                listener.request('GET', '/api/v2/test', 123, 100, None)
            assert 'failed to write metric for "GET /api/v2/test' in caplog.text
            caplog.clear()
        finally:
            locust_fixture.environment.events.request._handlers.pop()

    @pytest.mark.usefixtures('patch_azureloghandler')
    def test__create_custom_dimensions_dict(self, locust_fixture: LocustFixture, patch_azureloghandler: Callable[[], None]) -> None:
        patch_azureloghandler()
        try:
            listener = ApplicationInsightsListener(locust_fixture.environment, 'https://insights.test.com?InstrumentationKey=asdfasdfasdfasdf')

            expected_keys = [
                'thread_count', 'target_user_count', 'spawn_rate', 'method', 'result', 'response_time', 'response_length', 'endpoint', 'testplan', 'exception'
            ]

            custom_dimensions = listener._create_custom_dimensions_dict('GET', 'Success', 133, 200, '/api/v1/test')

            for key in custom_dimensions.keys():
                assert key in expected_keys

            for key in expected_keys:
                assert key in custom_dimensions

            assert custom_dimensions.get('method', None) == 'GET'
            assert custom_dimensions.get('result', None) == 'Success'
            assert custom_dimensions.get('response_time', None) == 133
            assert custom_dimensions.get('response_length', None) == 200
            assert custom_dimensions.get('endpoint', None) == '/api/v1/test'
            assert custom_dimensions.get('testplan', None) == 'appinsightstestplan'
        finally:
            locust_fixture.environment.events.request._handlers.pop()

    @pytest.mark.usefixtures('patch_azureloghandler')
    def test__safe_return_runner_values(self, locust_fixture: LocustFixture, patch_azureloghandler: Callable[[], None]) -> None:
        patch_azureloghandler()
        try:
            listener = ApplicationInsightsListener(locust_fixture.environment, 'https://insights.test.com?InstrumentationKey=asdfasdfasdfasdf')

            expected_keys = ['thread_count', 'target_user_count', 'spawn_rate']

            runner_values = listener._safe_return_runner_values()

            for key in runner_values.keys():
                assert key in expected_keys

            for key in expected_keys:
                assert key in runner_values

            assert runner_values.get('thread_count', None) == '0'
            assert runner_values.get('target_user_count', None) == '0'
            assert runner_values.get('spawn_rate', None) == ''

            locust_fixture.environment.runner = None
            locust_fixture.environment.runner = locust_fixture.environment.create_local_runner()

            runner_values = listener._safe_return_runner_values()
            assert runner_values.get('thread_count', None) == '0'
            assert runner_values.get('target_user_count', None) == '0'
            assert runner_values.get('spawn_rate', None) == ''

            listener.environment.runner = locust_fixture.environment.runner = None

            assert listener._safe_return_runner_values() == {
                'thread_count': '',
                'target_user_count': '',
                'spawn_rate': '',
            }
        finally:
            locust_fixture.environment.events.request._handlers.pop()
