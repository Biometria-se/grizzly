"""Unit tests of grizzly.listeners.influxdb."""
from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timezone
from json import dumps as jsondumps
from platform import node as get_hostname
from typing import TYPE_CHECKING, Any, Callable, Optional
from unittest.mock import patch

import pytest
from influxdb.exceptions import InfluxDBClientError
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import WriteApi
from influxdb_client.rest import ApiException

from grizzly.listeners.influxdb import InfluxDbError, InfluxDbListener, InfluxDbPoint, InfluxDbV1, InfluxDbV2
from grizzly.types.locust import CatchResponseError
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from influxdb_client import InfluxDBClient  # type: ignore[attr-defined]
    from pytest_mock import MockerFixture

    from tests.fixtures import GrizzlyFixture, LocustFixture


@pytest.fixture()
def patch_influxdblistener(mocker: MockerFixture) -> Callable[[], None]:
    def wrapper() -> None:
        mocker.patch(
            'gevent.spawn',
            return_value=None,
        )

        def influxdbclient_v1_connect(instance: InfluxDbV1, *_args: Any, **_kwargs: Any) -> InfluxDbV1:
            return instance

        def influxdbclient_v2_connect(instance: InfluxDbV2, *_args: Any, **_kwargs: Any) -> InfluxDbV2:
            return instance

        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDbV1.connect',
            influxdbclient_v1_connect,
        )
        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDbV2.connect',
            influxdbclient_v2_connect,
        )

    return wrapper


class TestInfluxDbV1:
    def test___init__(self) -> None:
        client = InfluxDbV1('https://influx.example.com', 1232, 'testdb')
        assert client.host == 'https://influx.example.com'
        assert client.port == 1232
        assert client.database == 'testdb'
        assert client.username is None
        assert client.password is None

        client = InfluxDbV1('https://influx.example.com', 1233, 'testdb', 'test-user', 'secret!')
        assert client.host == 'https://influx.example.com'
        assert client.port == 1233
        assert client.database == 'testdb'
        assert client.username == 'test-user'
        assert client.password == 'secret!'  # noqa: S105

    def test_connect(self) -> None:
        client = InfluxDbV1('https://influx.example.com', 1234, 'testdb')

        assert client.connect() is client

    def test___enter__(self) -> None:
        client = InfluxDbV1('https://influx.example.com', 1235, 'testdb')

        assert client.__enter__() is client

    def test___exit__(self, mocker: MockerFixture) -> None:
        influx = InfluxDbV1('https://influx.example.com', 1236, 'testdb').connect()

        def noop___exit__(*_args: Any, **_kwargs: Any) -> None:
            pass

        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDBClientV1.__exit__',
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
            raw: dict[str, Any]

        influx = InfluxDbV1('https://influx.example.com', 1237, 'testdb').connect()

        def test_query(table: str, columns: list[str]) -> None:
            def query(_instance: InfluxDBClient, _query: str) -> ResultContainer:
                results = ResultContainer()
                results.raw = {
                    'series': [{key: 'test' for key in columns}],
                }

                return results

            mocker.patch(
                'grizzly.listeners.influxdb.InfluxDBClientV1.query',
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
            'grizzly.listeners.influxdb.InfluxDBClientV1.write_points',
            return_value=None,
        )

        influx = InfluxDbV1('https://influx.example.com', 1238, 'testdb').connect()

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

        def generate_write_error(content: dict[str, Any], code: Optional[int] = 500) -> None:
            raw_content = jsondumps(content)

            def write_error(_instance: InfluxDBClient, _values: list[dict[str, Any]]) -> None:
                raise InfluxDBClientError(raw_content, code)

            mocker.patch(
                'grizzly.listeners.influxdb.InfluxDBClientV1.write_points',
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


class TestInfluxDbV2:
    def test___init__(self) -> None:
        client = InfluxDbV2('https://influx.example.com', 1232, 'org', 'testdb')
        assert client.host == 'https://influx.example.com'
        assert client.port == 1232
        assert client.org == 'org'
        assert client.bucket == 'testdb'
        assert client.token is None

        client = InfluxDbV2('https://influx.example.com', 1233, 'org', 'testdb', 'test-token')
        assert client.host == 'https://influx.example.com'
        assert client.port == 1233
        assert client.bucket == 'testdb'
        assert client.token == 'test-token'  # noqa: S105

    def test_connect(self) -> None:
        client = InfluxDbV2('https://influx.example.com', 1234, 'org', 'testdb')

        assert client.connect() is client
        client.__exit__(None, None, None)

    def test___enter__(self) -> None:
        client = InfluxDbV2('https://influx.example.com', 1235, 'org', 'testdb')

        assert client.__enter__() is client
        client.__exit__(None, None, None)

    def test___exit__(self) -> None:
        influx = InfluxDbV2('https://influx.example.com', 1236, 'org', 'testdb').connect()

        with patch('grizzly.listeners.influxdb.InfluxDBClientV2.__exit__', return_value=None):
            assert influx.__exit__(None, None, None)

            with pytest.raises(InfluxDbError):
                influx.__exit__(RuntimeError, RuntimeError('failure'), None)

            with pytest.raises(InfluxDbError):
                influx.__exit__(InfluxDbError, InfluxDbError(), None)

            exception = ApiException(400, 'madness')
            with pytest.raises(InfluxDbError, match='400: madness'):
                influx.__exit__(ApiException, exception, None)

        influx.__exit__(None, None, None)

    def test_read(self, mocker: MockerFixture) -> None:
        class ResultContainer:
            raw: dict[str, Any]

        query_api_mock = mocker.MagicMock(spec=QueryApi)
        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDBClientV2.query_api',
            return_value=query_api_mock,
        )

        query_results_mock = mocker.MagicMock()
        query_api_mock.query.return_value = query_results_mock
        query_results_mock.to_json.return_value = '{"some_key": "some_value"}'

        table = 'testtable'
        columns = ['col1', 'col2']

        flux_query = f"""
        from(bucket: "testdb")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "{table}")
        |> filter(fn: (r) => {" or ".join(f'r._field == "{field}"' for field in columns)})
        """

        mocker.patch(
            'logging.Logger.debug',
            mocker.MagicMock(),
        )

        influx = InfluxDbV2('https://influx.example.com', 1237, 'org', 'testdb').connect()

        result = influx.read(table, columns)
        assert "some_key" in result

        query_api_mock.query.assert_called_once_with(flux_query, org='org')
        influx.__exit__(None, None, None)


    def test_write(self, mocker: MockerFixture) -> None:
        write_api_mock = mocker.MagicMock(spec=WriteApi)
        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDBClientV2.write_api',
            return_value=write_api_mock,
        )

        influx = InfluxDbV2('https://influx.example.com', 1238, 'org', 'testdb').connect()

        def logger_debug(_logger: logging.Logger, msg: str, count: int, database: str, host: str, port: int) -> None:
            assert count == 0
            assert database == influx.bucket
            assert host == influx.host
            assert port == influx.port
            assert msg == 'successfully wrote %d points to bucket %s@%s:%d'

        mocker.patch(
            'logging.Logger.debug',
            logger_debug,
        )

        influx.write([])

        def generate_write_error(raw_content: str|None, code: Optional[int] = 500) -> None:
            write_api_mock.write.side_effect = [ApiException(reason=raw_content, status=code)]

        generate_write_error(None, None)
        with pytest.raises(InfluxDbError, match='<unknown>'):
            influx.write([])

        generate_write_error('test-failure', 400)
        with pytest.raises(InfluxDbError, match='400: test-failure'):
            influx.write([])

        generate_write_error('not found', 404)
        with pytest.raises(InfluxDbError, match='404: not found'):
            influx.write([])

        influx.__exit__(None, None, None)

class TestInfluxDblistener:
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
            'https://influx.test.com:1239/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        assert len(locust_fixture.environment.events.request._handlers) == 2
        assert listener.influx_port == 1239
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
            'https://influx.test.com:1240/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
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
            'https://influx.test.com:1241/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
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

        def write(_: InfluxDbV2, events: list[dict[str, Any]]) -> None:
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
            'grizzly.listeners.influxdb.InfluxDbV1.write',
            write,
        )

        listener._log_request('GET', '/api/v1/test', 'Success', {'response_time': 133.7}, {}, None)
        assert len(listener._events) == 1

        with pytest.raises(RuntimeError):
            listener.run_events()

        assert len(listener._events) == 0

    def test__override_event(self, grizzly_fixture: GrizzlyFixture) -> None:
        event: InfluxDbPoint = {
            'measurement': 'request',
            'tags': {
                'foo': 'bar',
                'hostname': 'localhost',
            },
            'time': '1970-01-01T00:00:00Z',
            'fields': {
                'request_started': '1970-01-01T00:00:00Z',
                'request_finished': '1970-01-01T00:00:00Z',
            },
        }

        listener = InfluxDbListener(
            grizzly_fixture.behave.locust.environment,
            'https://influx.test.com:1242/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        listener._override_event(event, {
            '__tags_foo__': 'foo',
            '__time__': '2024-07-08T10:52:01Z',
            '__fields_request_started__': '2024-07-08T10:52:01Z',
            '__fields_request_finished__': '2024-07-08T10:54:00Z',
        })

        assert event == {
            'measurement': 'request',
            'tags': {
                'foo': 'foo',
                'hostname': 'localhost',
            },
            'time': '2024-07-08T10:52:01Z',
            'fields': {
                'request_started': '2024-07-08T10:52:01Z',
                'request_finished': '2024-07-08T10:54:00Z',
            },
        }

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
            'https://influx.test.com:1243/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        assert len(listener._events) == 0

        listener._log_request('GET', 'Request: /api/v1/test', 'Success', {'response_time': 133.7}, {}, None)

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
            'user': id(listener),
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
                listener._log_request('POST', '001 Request: /api/v2/test', 'Failure', {'response_time': 111.1}, {}, exception)
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
                    'user': id(listener),
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

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1244/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        mocker.patch.object(listener, 'run_events', return_value=None)

        listener.request('GET', '/api/v1/test', 133.7, 200, {}, None)
        assert listener._events == [
            SOME(
                dict,
                measurement='request',
                tags=SOME(
                    dict,
                    name='/api/v1/test',
                    method='GET',
                    result='Success',
                    testplan='unittest-plan',
                    hostname=get_hostname(),
                    environment='local',
                    profile='unittest-profile',
                    description='unittesting',
                ),
                time=ANY(str),
                fields=SOME(
                    dict,
                    response_time=134,
                    response_length=200,
                    exception=None,
                    request_started=ANY(str),
                    request_finished=ANY(str),
                ),
            ),
        ]

        listener.request('POST', '/api/v2/test', 555.37, 137, {}, CatchResponseError('request failed'))
        assert listener._events == [
            SOME(
                dict,
                measurement='request',
                tags=SOME(
                    dict,
                    name='/api/v1/test',
                    method='GET',
                    result='Success',
                    testplan='unittest-plan',
                    hostname=get_hostname(),
                    environment='local',
                    profile='unittest-profile',
                    description='unittesting',
                ),
                time=ANY(str),
                fields=SOME(
                    dict,
                    response_time=134,
                    response_length=200,
                    exception=None,
                    request_started=ANY(str),
                    request_finished=ANY(str),
                ),
            ),
            SOME(
                dict,
                measurement='request',
                tags=SOME(
                    dict,
                    name='/api/v2/test',
                    method='POST',
                    result='Failure',
                    testplan='unittest-plan',
                    hostname=get_hostname(),
                    environment='local',
                    profile='unittest-profile',
                    description='unittesting',
                ),
                time=ANY(str),
                fields=SOME(
                    dict,
                    response_time=555,
                    response_length=137,
                    exception='request failed',
                    request_started=ANY(str),
                    request_finished=ANY(str),
                ),
            ),
        ]

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_request_exception(
        self, locust_fixture: LocustFixture, mocker: MockerFixture, caplog: LogCaptureFixture, patch_influxdblistener: Callable[[], None],
    ) -> None:
        patch_influxdblistener()
        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1245/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        mocker.patch.object(listener, '_create_metrics', side_effect=[Exception])

        with caplog.at_level(logging.ERROR):
            listener.request('GET', '/api/v2/test', 555.37, 137, {}, None)
        assert 'failed to write metric for "GET /api/v2/test' in caplog.text
        caplog.clear()

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test__create_metrics(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.environment,
            'https://influx.example.com:1246/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        expected_keys = ['response_time', 'response_length']

        metrics = listener._create_metrics(1337, -1)

        assert sorted(expected_keys) == sorted(metrics.keys())

        assert metrics.get('response_time', None) == 1337
        assert metrics.get('response_length', 100) is None

        metrics = listener._create_metrics(555, 1337)

        assert metrics.get('response_time', None) == 555
        assert metrics.get('response_length', None) == 1337
