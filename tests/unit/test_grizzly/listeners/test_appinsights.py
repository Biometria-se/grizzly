"""Unit tests of grizzly.listeners.appinsights."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

import pytest

from grizzly.listeners.appinsights import ApplicationInsightsListener
from grizzly.types.locust import CatchResponseError

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from tests.fixtures import LocustFixture


@pytest.fixture()
def patch_azureloghandler(mocker: MockerFixture) -> Callable[[], None]:
    def wrapper() -> None:
        mocker.patch(
            'opencensus.ext.azure.log_exporter.AzureLogHandler.__init__',
            return_value=None,
        )

        mocker.patch(
            'opencensus.ext.azure.log_exporter.BaseLogHandler.flush',
            return_value=None,
        )

    return wrapper


class TestAppInsightsListener:
    @pytest.mark.usefixtures('patch_azureloghandler')
    def test___init__(self, locust_fixture: LocustFixture, patch_azureloghandler: Callable[[], None]) -> None:
        patch_azureloghandler()

        # fire_deprecated_request_handlers is already an internal event handler for the request event
        assert len(locust_fixture.environment.events.request._handlers) == 1

        with pytest.raises(AssertionError, match='IngestionEndpoint was neither set as the hostname or in the query string'):
            ApplicationInsightsListener(locust_fixture.environment, '?')

        with pytest.raises(AssertionError, match='InstrumentationKey not found in'):
            ApplicationInsightsListener(locust_fixture.environment, '?IngestionEndpoint=insights.test.com')

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
            request_type: str, name: str, response_time: float, response_length: int, exception: Optional[Any] = None,
        ) -> Callable[[logging.Handler, str, Dict[str, Any]], None]:
            result = 'Success' if exception is None else 'Failure'
            expected_message = f'{result}: {request_type} {name} Response time: {int(round(response_time, 0))} Number of Threads: {""}'

            if exception is not None:
                expected_message = f'{expected_message} Exception: {exception!s}'

            def logger_info(_: logging.Handler, msg: str, extra: Dict[str, Any]) -> None:
                assert msg == expected_message
                assert extra is not None
                custom_dimensions = extra.get('custom_dimensions')
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
                'method', 'result', 'response_time', 'response_length', 'endpoint', 'testplan', 'exception',
            ]

            custom_dimensions = listener._create_custom_dimensions_dict('GET', 'Success', 133, 200, '/api/v1/test')

            assert sorted(expected_keys) == sorted(custom_dimensions.keys())
            assert custom_dimensions == {
                'method': 'GET',
                'result': 'Success',
                'response_time': 133,
                'response_length': 200,
                'endpoint': '/api/v1/test',
                'testplan': 'appinsightstestplan',
                'exception': None,
            }
        finally:
            locust_fixture.environment.events.request._handlers.pop()
