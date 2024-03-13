"""Unit tests of grizzly.listeners.influxdb."""
from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timezone
from json import dumps as jsondumps
from platform import node as get_hostname
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import pytest
from influxdb.exceptions import InfluxDBClientError

from grizzly.listeners.influxdb import InfluxDb, InfluxDbError, InfluxDbListener
from grizzly.types.locust import CatchResponseError
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from influxdb.client import InfluxDBClient
    from pytest_mock import MockerFixture

    from tests.fixtures import GrizzlyFixture, LocustFixture


@pytest.fixture()
def patch_influxdblistener(mocker: MockerFixture) -> Callable[[], None]:
    def wrapper() -> None:
        mocker.patch(
            'gevent.spawn',
            return_value=None,
        )

        def influxdbclient_connect(instance: InfluxDb, *_args: Any, **_kwargs: Any) -> InfluxDb:
            return instance

        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDb.connect',
            influxdbclient_connect,
        )

    return wrapper


class TestInfluxDb:
    def test___init__(self) -> None:
        client = InfluxDb('https://influx.example.com', 1337, 'testdb')
        assert client.host == 'https://influx.example.com'
        assert client.port == 1337
        assert client.database == 'testdb'
        assert client.username is None
        assert client.password is None

        client = InfluxDb('https://influx.example.com', 8888, 'testdb', 'test-user', 'secret!')
        assert client.host == 'https://influx.example.com'
        assert client.port == 8888
        assert client.database == 'testdb'
        assert client.username == 'test-user'
        assert client.password == 'secret!'  # noqa: S105

    def test_connect(self) -> None:
        client = InfluxDb('https://influx.example.com', 1337, 'testdb')

        assert client.connect() is client

    def test___enter__(self) -> None:
        client = InfluxDb('https://influx.example.com', 1337, 'testdb')

        assert client.__enter__() is client

    def test___exit__(self, mocker: MockerFixture) -> None:
        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def noop___exit__(*_args: Any, **_kwargs: Any) -> None:
            pass

        mocker.patch(
            'influxdb.client.InfluxDBClient.__exit__',
            noop___exit__,
        )

        assert influx.__exit__(None, None, None)

        with pytest.raises(InfluxDbError):
            influx.__exit__(RuntimeError, RuntimeError('failure'), None)

        with pytest.raises(InfluxDbError):
            influx.__exit__(InfluxDbError, InfluxDbError(), None)

        exception = InfluxDBClientError('{"error": "failure"}', 400)
        with pytest.raises(InfluxDbError, match='400: failure'):
            influx.__exit__(InfluxDBClientError, exception, None)

    def test_read(self, mocker: MockerFixture) -> None:
        class ResultContainer:
            raw: Dict[str, Any]

        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def test_query(table: str, columns: List[str]) -> None:
            def query(_instance: InfluxDBClient, _query: str) -> ResultContainer:
                results = ResultContainer()
                results.raw = {
                    'series': [{key: 'test' for key in columns}],
                }

                return results

            mocker.patch(
                'influxdb.client.InfluxDBClient.query',
                query,
            )

            def logger_debug(_handler: logging.Handler, msg: str, query: str) -> None:
                assert msg == 'query: %s'
                assert query == f'select {",".join(columns)} from "{table}";'  # noqa: S608

            mocker.patch(
                'logging.Logger.debug',
                logger_debug,
            )

            result = influx.read(table, columns)

            assert result[0] == {key: 'test' for key in columns}

        test_query('testtable', ['col1', 'col2'])
        test_query('monitor', ['cpu_idle', 'cpu_user', 'cpu_system'])

    def test_write(self, mocker: MockerFixture) -> None:
        mocker.patch(
            'influxdb.client.InfluxDBClient.write_points',
            return_value=None,
        )

        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def logger_debug(_logger: logging.Logger, msg: str, count: int, database: str, host: str, port: int) -> None:
            assert count == 0
            assert database == influx.database
            assert host == influx.host
            assert port == influx.port
            assert msg == 'successfully wrote %d points to %s@%s:%d'

        mocker.patch(
            'logging.Logger.debug',
            logger_debug,
        )

        influx.write([])

        def generate_write_error(content: Dict[str, Any], code: Optional[int] = 500) -> None:
            raw_content = jsondumps(content)

            def write_error(_instance: InfluxDBClient, _values: List[Dict[str, Any]]) -> None:
                raise InfluxDBClientError(raw_content, code)

            mocker.patch(
                'influxdb.client.InfluxDBClient.write_points',
                write_error,
            )

        generate_write_error({}, None)
        with pytest.raises(InfluxDbError, match='<unknown>'):
            influx.write([])

        generate_write_error({'error': 'test-failure'}, 400)
        with pytest.raises(InfluxDbError, match='400: test-failure'):
            influx.write([])

        generate_write_error({'message': 'not found'}, 404)
        with pytest.raises(InfluxDbError, match='404: not found'):
            influx.write([])


class TestInfluxDbListener:
    @pytest.mark.usefixtures('patch_influxdblistener')
    def test___init__(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        with pytest.raises(AssertionError, match='hostname not found in'):
            InfluxDbListener(locust_fixture.environment, '')

        with pytest.raises(AssertionError, match='database was not found in'):
            InfluxDbListener(locust_fixture.environment, 'https://influx.test.com')

        with pytest.raises(AssertionError, match='Testplan was not found in'):
            InfluxDbListener(locust_fixture.environment, 'https://influx.test.com/testdb')

        patch_influxdblistener()

        assert len(locust_fixture.environment.events.request._handlers) == 1  # interally added handler for deprecated request events

        listener = InfluxDbListener(locust_fixture.environment, 'https://influx.test.com/testdb?Testplan=unittest-plan')

        assert len(locust_fixture.environment.events.request._handlers) == 2
        assert listener.influx_port == 8086
        assert listener._testplan == 'unittest-plan'
        assert listener._target_environment is None
        assert listener._hostname == socket.gethostname()
        assert listener.environment is locust_fixture.environment
        assert listener._username == os.getenv('USER', 'unknown')
        assert listener._events == []
        assert not listener._finished
        assert listener._profile_name == ''
        assert listener._description == ''

        locust_fixture.environment.events.request._handlers.pop()

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        assert len(locust_fixture.environment.events.request._handlers) == 2
        assert listener.influx_port == 1337
        assert listener._testplan == 'unittest-plan'
        assert listener._target_environment == 'local'
        assert listener._hostname == socket.gethostname()
        assert listener.environment is locust_fixture.environment
        assert listener._username == os.getenv('USER', 'unknown')
        assert listener._events == []
        assert not listener._finished
        assert listener._profile_name == 'unittest-profile'
        assert listener._description == 'unittesting'

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_run_user_count(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture) -> None:
        patch_influxdblistener()

        gsleep_spy = mocker.patch('gevent.sleep', return_value=None)
        mocker.patch(
            'locust.runners.Runner.user_classes_count',
            new_callable=mocker.PropertyMock,
            side_effect=[{}, {'User1': 2, 'User2': 3}, {'User1': 2, 'User2': 3}],
        )
        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDbListener.finished',
            new_callable=mocker.PropertyMock,
            side_effect=[False, True, True, False, False, False, True, True],
        )

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        write_spy = mocker.patch.object(listener.connection, 'write', return_value=None)

        listener.run_user_count()

        write_spy.assert_not_called()
        gsleep_spy.assert_not_called()

        listener.run_user_count()

        gsleep_spy.assert_called_once_with(5.0)
        gsleep_spy.reset_mock()

        assert write_spy.call_count == 2
        for i in range(2):
            args, _ = write_spy.call_args_list[i]
            assert len(args) == 1
            assert len(args[0]) == 2
            for j in range(2):
                assert args[0][j].get('measurement', None) == 'user_count'
                assert args[0][j].get('tags', None) == {
                    'environment': 'local',
                    'testplan': 'unittest-plan',
                    'hostname': get_hostname(),
                    'user_class': f'User{j+1}',
                    'description': 'unittesting',
                    'profile': 'unittest-profile',
                }
                assert args[0][j].get('fields', None) == {
                    'user_count': 2 + j,
                }

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_run_events(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        mocker.patch(
            'gevent.sleep',
            side_effect=RuntimeError('gsleep was called'),
        )
        try:
            listener._events = []
            listener._finished = True
            listener.run_events()
        except RuntimeError:
            pytest.fail('gevent.sleep was unexpectedly called')
        finally:
            listener._finished = False

        def write(_: InfluxDb, events: List[Dict[str, Any]]) -> None:
            assert len(events) == 1
            event = events[-1]
            assert event.get('measurement', None) == 'request'
            assert event.get('tags', None) == {
                'hostname': ANY(str),
                'name': '/api/v1/test',
                'method': 'GET',
                'result': 'Success',
                'testplan': 'unittest-plan',
            }
            assert event.get('time', None) is not None
            assert event.get('fields', None) == {
                'response_time': 133.7,
                'exception': None,
                'request_finished': ANY(str),
                'request_started': ANY(str),
            }

        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDb.write',
            write,
        )

        listener._log_request('GET', '/api/v1/test', 'Success', {'response_time': 133.7}, None)
        assert len(listener._events) == 1

        with pytest.raises(RuntimeError):
            listener.run_events()

        assert len(listener._events) == 0

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test__log_request(self, grizzly_fixture: GrizzlyFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture) -> None:
        patch_influxdblistener()
        grizzly_fixture()

        expected_datetime = datetime(2022, 12, 16, 10, 28, 0, 123456, timezone.utc)

        datetime_mock = mocker.patch(
            'grizzly.listeners.influxdb.datetime',
            side_effect=lambda *args, **kwargs: datetime(*args, **kwargs),  # noqa: DTZ001
        )
        datetime_mock.now.return_value = expected_datetime

        listener = InfluxDbListener(
            grizzly_fixture.behave.locust.environment,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        assert len(listener._events) == 0

        listener._log_request('GET', 'Request: /api/v1/test', 'Success', {'response_time': 133.7}, None)

        assert len(listener._events) == 1

        expected_keys = ['measurement', 'tags', 'time', 'fields']

        event = listener._events[-1]

        assert sorted(expected_keys) == sorted(event.keys())
        assert event.get('measurement', None) == 'request'
        assert event.get('tags', None) == {
            'name': 'Request: /api/v1/test',
            'method': 'GET',
            'result': 'Success',
            'testplan': 'unittest-plan',
            'hostname': get_hostname(),
            'profile': 'unittest-profile',
            'environment': 'local',
            'description': 'unittesting',
        }
        assert event.get('fields', None) == {
            'exception': None,
            'response_time': 133.7,
            'request_started': '2022-12-16T10:27:59.989756+00:00',
            'request_finished': '2022-12-16T10:28:00.123456+00:00',
        }

        try:
            os.environ['TESTDATA_VARIABLE_TEST1'] = 'unittest-1'
            os.environ['TESTDATA_VARIABLE_TEST2'] = 'unittest-2'

            class ClassWithNoRepr:
                def __repr__(self) -> str:
                    raise AttributeError

            import inspect

            exceptions = [
                (CatchResponseError('request failed'), 'request failed'),
                (RuntimeError('request failed'), repr(RuntimeError('request failed'))),
                (
                    ClassWithNoRepr(),
                    (
                        f"<class '{self.__class__.__module__}.{self.__class__.__name__}."
                        f"{inspect.stack()[0][3]}.<locals>.{ClassWithNoRepr.__name__}'>"
                        " (and it has no string representation)"
                    ),
                ),
            ]

            for exception, expected in exceptions:
                listener._log_request('POST', '001 Request: /api/v2/test', 'Failure', {'response_time': 111.1}, exception)
                event = listener._events[-1]

                assert event.get('tags', None) == {
                    'name': '001 Request: /api/v2/test',
                    'method': 'POST',
                    'result': 'Failure',
                    'testplan': 'unittest-plan',
                    'hostname': get_hostname(),
                    'scenario': '001 test scenario',
                    'description': 'unittesting',
                    'environment': 'local',
                    'profile': 'unittest-profile',
                    'TEST1': 'unittest-1',
                    'TEST2': 'unittest-2',
                }
                assert event.get('fields', None) == {
                    'exception': expected,
                    'response_time': 111.1,
                    'request_started': '2022-12-16T10:28:00.012356+00:00',
                    'request_finished': '2022-12-16T10:28:00.123456+00:00',
                }
            assert len(listener._events) == 4
        finally:
            del os.environ['TESTDATA_VARIABLE_TEST1']
            del os.environ['TESTDATA_VARIABLE_TEST2']

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_request(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture) -> None:
        patch_influxdblistener()

        def generate_logger_call(
            request_type: str, name: str, response_time: float, response_length: int, exception: Optional[Any] = None,  # noqa: ARG001
        ) -> Callable[[logging.Handler, str], None]:
            result = 'Success' if exception is None else 'Failure'
            expected_message = f'{result}: {request_type} {name} Response time: {int(round(response_time, 0))}'

            if exception is not None:
                expected_message = f'{expected_message} Exception: {exception!s}'

            def logger_call(_self: logging.Handler, msg: str, *args: Any, **_kwargs: Any) -> None:
                assert msg % args == expected_message

            return logger_call

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        mocker.patch(
            'logging.Logger.debug',
            generate_logger_call('GET', '/api/v1/test', 133.7, 200, None),
        )

        listener.request('GET', '/api/v1/test', 133.7, 200, None)
        assert len(listener._events) == 1

        mocker.patch(
            'logging.Logger.exception',
            generate_logger_call('POST', '/api/v2/test', 555.37, 137, CatchResponseError('request failed')),
        )

        listener.request('POST', '/api/v2/test', 555.37, 137, CatchResponseError('request failed'))
        assert len(listener._events) == 2

    def test_request_exception(
        self, locust_fixture: LocustFixture, mocker: MockerFixture, caplog: LogCaptureFixture,
    ) -> None:
        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        mocker.patch.object(listener, '_create_metrics', side_effect=[Exception])

        with caplog.at_level(logging.ERROR):
            listener.request('GET', '/api/v2/test', 555.37, 137, None)
        assert 'failed to write metric for "GET /api/v2/test' in caplog.text
        caplog.clear()

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test__create_metrics(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        expected_keys = ['response_time', 'response_length']

        metrics = listener._create_metrics(1337, -1)

        assert sorted(expected_keys) == sorted(metrics.keys())

        assert metrics.get('response_time', None) == 1337
        assert metrics.get('response_length', 100) is None

        metrics = listener._create_metrics(555, 1337)

        assert metrics.get('response_time', None) == 555
        assert metrics.get('response_length', None) == 1337
