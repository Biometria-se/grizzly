import socket
import os
import logging

from typing import Callable, Any, Dict, Optional, Tuple, List
from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.exception import CatchResponseError
from influxdb.client import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from grizzly.listeners.influxdb import InfluxDbError, InfluxDbListener, InfluxDb

from ...fixtures import LocustFixture


@pytest.fixture
def patch_influxdblistener(mocker: MockerFixture) -> Callable[[], None]:
    def wrapper() -> None:
        def gevent_spawn(func: Callable) -> Any:
            return None

        mocker.patch(
            'gevent.spawn',
            gevent_spawn,
        )

        def influxdbclient_connect(instance: InfluxDb, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> InfluxDb:
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

        client = InfluxDb('https://influx.example.com', 8888, 'testdb', 'test-user', 'password')
        assert client.host == 'https://influx.example.com'
        assert client.port == 8888
        assert client.database == 'testdb'
        assert client.username == 'test-user'
        assert client.password == 'password'

    def test_connect(self) -> None:
        client = InfluxDb('https://influx.example.com', 1337, 'testdb')

        assert client.connect() is client

    def test___enter__(self) -> None:
        client = InfluxDb('https://influx.example.com', 1337, 'testdb')

        assert client.__enter__() is client

    def test___exit__(self, mocker: MockerFixture) -> None:
        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def noop___exit__(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
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

        with pytest.raises(InfluxDbError) as e:
            exception = InfluxDBClientError('{"error": "failure"}', 400)
            influx.__exit__(InfluxDBClientError, exception, None)
        assert '400: failure' in str(e)

    def test_read(self, mocker: MockerFixture) -> None:
        class ResultContainer:
            raw: Dict[str, Any]

        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def test_query(table: str, columns: List[str]) -> None:
            def query(instance: InfluxDBClient, q: str) -> ResultContainer:
                results = ResultContainer()
                results.raw = {
                    'series': [{key: 'test' for key in columns}],
                }

                return results

            mocker.patch(
                'influxdb.client.InfluxDBClient.query',
                query,
            )

            def logger_debug(handler: logging.Handler, msg: str) -> None:
                assert msg == f'query: select {",".join(columns)} from "{table}";'

            mocker.patch(
                'logging.Logger.debug',
                logger_debug,
            )

            result = influx.read(table, columns)

            assert result[0] == {key: 'test' for key in columns}

        test_query('testtable', ['col1', 'col2'])
        test_query('monitor', ['cpu_idle', 'cpu_user', 'cpu_system'])

    def test_write(self, mocker: MockerFixture) -> None:
        def write(instance: InfluxDBClient, values: List[Dict[str, Any]]) -> None:
            pass

        mocker.patch(
            'influxdb.client.InfluxDBClient.write_points',
            write,
        )

        influx = InfluxDb('https://influx.example.com', 1337, 'testdb').connect()

        def logger_debug(logger: logging.Logger, msg: str) -> None:
            assert msg == f'successfully wrote 0 points to {influx.database}@{influx.host}:{influx.port}'

        mocker.patch(
            'logging.Logger.debug',
            logger_debug,
        )

        influx.write([])

        def generate_write_error(content: Dict[str, Any], code: Optional[int] = 500) -> None:
            raw_content = jsondumps(content)

            def write_error(instance: InfluxDBClient, values: List[Dict[str, Any]]) -> None:
                raise InfluxDBClientError(raw_content, code)

            mocker.patch(
                'influxdb.client.InfluxDBClient.write_points',
                write_error,
            )

        generate_write_error({}, None)
        with pytest.raises(InfluxDbError) as e:
            influx.write([])
        assert '<unknown>' in str(e)

        generate_write_error({'error': 'test-failure'}, 400)
        with pytest.raises(InfluxDbError) as e:
            influx.write([])
        assert '400: test-failure' in str(e)

        generate_write_error({'message': 'not found'}, 404)
        with pytest.raises(InfluxDbError) as e:
            influx.write([])
        assert '404: not found' in str(e)


class TestInfluxDbListener:
    @pytest.mark.usefixtures('patch_influxdblistener')
    def test___init__(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        with pytest.raises(AssertionError) as ae:
            InfluxDbListener(locust_fixture.env, '')
        assert 'hostname not found in' in str(ae)

        with pytest.raises(AssertionError) as ae:
            InfluxDbListener(locust_fixture.env, 'https://influx.test.com')
        assert 'database was not found in' in str(ae)

        with pytest.raises(AssertionError) as ae:
            InfluxDbListener(locust_fixture.env, 'https://influx.test.com/testdb')
        assert 'Testplan not found in' in str(ae)

        patch_influxdblistener()

        assert len(locust_fixture.env.events.request._handlers) == 1  # interally added handler for deprecated request events

        listener = InfluxDbListener(locust_fixture.env, 'https://influx.test.com/testdb?Testplan=unittest-plan')

        assert len(locust_fixture.env.events.request._handlers) == 2
        assert listener.influx_port == 8086
        assert listener._testplan == 'unittest-plan'
        assert listener._target_environment is None
        assert listener._hostname == socket.gethostname()
        assert listener.environment is locust_fixture.env
        assert listener._username == os.getenv('USER', 'unknown')
        assert listener._events == []
        assert not listener._finished
        assert listener._profile_name == ''
        assert listener._description == ''

        locust_fixture.env.events.request._handlers.pop()

        listener = InfluxDbListener(
            locust_fixture.env,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        assert len(locust_fixture.env.events.request._handlers) == 2
        assert listener.influx_port == 1337
        assert listener._testplan == 'unittest-plan'
        assert listener._target_environment == 'local'
        assert listener._hostname == socket.gethostname()
        assert listener.environment is locust_fixture.env
        assert listener._username == os.getenv('USER', 'unknown')
        assert listener._events == []
        assert not listener._finished
        assert listener._profile_name == 'unittest-profile'
        assert listener._description == 'unittesting'

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_run(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.env,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        def gsleep(time: float) -> None:
            raise RuntimeError('gsleep was called')

        mocker.patch(
            'gevent.sleep',
            gsleep,
        )
        try:
            listener._events = []
            listener._finished = True
            listener.run()
        except RuntimeError:
            pytest.fail('gevent.sleep was unexpectedly called')
        finally:
            listener._finished = False

        def write(client: InfluxDb, events: List[Dict[str, Any]]) -> None:
            assert len(events) == 1
            event = events[-1]
            assert event.get('measurement', None) == 'request'
            assert event.get('tags', None) == {
                'name': '/api/v1/test',
                'method': 'GET',
                'result': 'Success',
                'testplan': 'unittest-plan',
            }
            assert event.get('time', None) is not None
            assert event.get('fields', None) == {
                'response_time': 133.7,
                'exception': None,
            }

        mocker.patch(
            'grizzly.listeners.influxdb.InfluxDb.write',
            write,
        )

        listener._log_request('GET', '/api/v1/test', 'Success', {'response_time': 133.7}, None)
        assert len(listener._events) == 1

        with pytest.raises(RuntimeError):
            listener.run()

        assert len(listener._events) == 0

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test__log_request(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.env,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        assert len(listener._events) == 0

        listener._log_request('GET', '/api/v1/test', 'Success', {'response_time': 133.7}, None)

        assert len(listener._events) == 1

        expected_keys = ['measurement', 'tags', 'time', 'fields']

        event = listener._events[-1]

        for key in event.keys():
            assert key in expected_keys

        for key in expected_keys:
            assert key in event

        assert event.get('measurement', None) == 'request'
        assert event.get('tags', None) == {
            'name': '/api/v1/test',
            'method': 'GET',
            'result': 'Success',
            'testplan': 'unittest-plan',
        }
        assert event.get('fields', None) == {
            'exception': None,
            'response_time': 133.7,
        }

        try:
            os.environ['TESTDATA_VARIABLE_TEST1'] = 'unittest-1'
            os.environ['TESTDATA_VARIABLE_TEST2'] = 'unittest-2'

            class ClassWithNoRepr:
                def __repr__(self) -> str:
                    raise AttributeError()

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
                listener._log_request('POST', '/api/v2/test', 'Failure', {'response_time': 111.1}, exception)
                event = listener._events[-1]

                assert event.get('tags', None) == {
                    'name': '/api/v2/test',
                    'method': 'POST',
                    'result': 'Failure',
                    'testplan': 'unittest-plan',
                    'TEST1': 'unittest-1',
                    'TEST2': 'unittest-2',
                }
                assert event.get('fields', None) == {
                    'exception': expected,
                    'response_time': 111.1,
                }
            assert len(listener._events) == 4
        finally:
            del os.environ['TESTDATA_VARIABLE_TEST1']
            del os.environ['TESTDATA_VARIABLE_TEST2']

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test_request(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None], mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        patch_influxdblistener()

        def generate_logger_call(
            request_type: str, name: str, response_time: float, response_length: int, exception: Optional[Any] = None
        ) -> Callable[[logging.Handler, str], None]:
            result = 'Success' if exception is None else 'Failure'
            expected_message = f'{result}: {request_type} {name} Response time: {int(round(response_time, 0))} Number of Threads: 0'

            if exception is not None:
                expected_message = f'{expected_message} Exception: {str(exception)}'

            def logger_call(self: logging.Handler, msg: str) -> None:
                assert msg == expected_message

            return logger_call

        listener = InfluxDbListener(
            locust_fixture.env,
            'https://influx.example.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )

        mocker.patch(
            'logging.Logger.debug',
            generate_logger_call('GET', '/api/v1/test', 133.7, 200, None)
        )

        listener.request('GET', '/api/v1/test', 133.7, 200, None)
        assert len(listener._events) == 1

        mocker.patch(
            'logging.Logger.error',
            generate_logger_call('POST', '/api/v2/test', 555.37, 137, CatchResponseError('request failed'))
        )

        listener.request('POST', '/api/v2/test', 555.37, 137, CatchResponseError('request failed'))
        assert len(listener._events) == 2

    def test_request_exception(
        self, locust_fixture: LocustFixture, mocker: MockerFixture, caplog: LogCaptureFixture,
    ) -> None:
        listener = InfluxDbListener(
            locust_fixture.env,
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
            locust_fixture.env,
            'https://influx.example.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        expected_keys = ['thread_count', 'target_user_count', 'spawn_rate', 'response_time', 'response_length']

        metrics = listener._create_metrics(1337, -1)

        for key in metrics.keys():
            assert key in expected_keys

        for key in expected_keys:
            assert key in metrics

        assert metrics.get('response_time', None) == 1337
        assert metrics.get('response_length', 100) is None

        metrics = listener._create_metrics(555, 1337)

        assert metrics.get('response_time', None) == 555
        assert metrics.get('response_length', None) == 1337

    @pytest.mark.usefixtures('patch_influxdblistener')
    def test__safe_return_runner_values(self, locust_fixture: LocustFixture, patch_influxdblistener: Callable[[], None]) -> None:
        patch_influxdblistener()

        listener = InfluxDbListener(
            locust_fixture.env,
            'https://influx.test.com:1337/testdb?Testplan=unittest-plan&TargetEnvironment=local&ProfileName=unittest-profile&Description=unittesting',
        )
        expected_keys = ['thread_count', 'target_user_count', 'spawn_rate']

        runner_values = listener._safe_return_runner_values()

        for key in runner_values.keys():
            assert key in expected_keys

        for key in expected_keys:
            assert key in runner_values

        assert runner_values.get('thread_count', None) == 0
        assert runner_values.get('target_user_count', None) == 0
        assert runner_values.get('spawn_rate', None) == -1

        locust_fixture.env.runner = None
        locust_fixture.env.runner = locust_fixture.env.create_local_runner()

        runner_values = listener._safe_return_runner_values()
        assert runner_values.get('thread_count', None) == 0
        assert runner_values.get('target_user_count', None) == 0
        assert runner_values.get('spawn_rate', None) == -1
