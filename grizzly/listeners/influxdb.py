import logging
import os
import json

from types import TracebackType
from typing import Any, Dict, List, Optional, Type, Literal, TypedDict, Tuple, cast
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs, unquote
from platform import node as get_hostname

import gevent

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from grizzly.context import GrizzlyContext
from grizzly.types.locust import Environment, CatchResponseError


logger = logging.getLogger(__name__)


class InfluxDbError(Exception):
    pass


class InfluxDbPoint(TypedDict):
    measurement: str
    tags: Dict[str, Any]
    time: str
    fields: Dict[str, Any]


class InfluxDb:
    client: InfluxDBClient

    host: str
    port: int
    database: str
    username: Optional[str]
    password: Optional[str]

    def __init__(self, host: str, port: int, database: str, username: Optional[str] = None, password: Optional[str] = None) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def connect(self) -> 'InfluxDb':
        self.client = InfluxDBClient(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            gzip=True,
        )
        return self

    def __enter__(self) -> 'InfluxDb':
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        self.client.__exit__(exc_type, exc, traceback)

        if exc is not None:
            if isinstance(exc, InfluxDBClientError):
                content = json.loads(exc.content)
                message = f'{exc.code}: {content["error"]}'
                raise InfluxDbError(message)
            elif isinstance(exc, InfluxDbError):
                raise exc
            else:
                raise InfluxDbError(exc)

        return True

    def read(self, table: str, columns: List[str]) -> List[Dict[str, Any]]:
        query = f'select {",".join(columns)} from "{table}";'
        logger.debug(f'query: {query}')
        result = self.client.query(query)

        return cast(List[Dict[str, Any]], result.raw['series'])

    def write(self, values: List[InfluxDbPoint]) -> None:
        try:
            self.client.write_points(values)
            logger.debug((
                f'successfully wrote {len(values)} points to '
                f'{self.database}@{self.host}:{self.port}'
            ))
        except InfluxDBClientError as e:
            content = json.loads(e.content)
            code = e.code
            if 'message' in content:
                message = content['message']
            elif 'error' in content:
                message = content['error']
            else:
                message = '<unknown>'

            if code is not None:
                message = f'{code}: {message}'

            raise InfluxDbError(message) from e


class InfluxDbListener:
    run_events_greenlet: gevent.Greenlet
    run_user_count_greenlet: gevent.Greenlet

    def __init__(
        self,
        environment: Environment,
        url: str,
    ) -> None:
        parsed = urlparse(url)
        path = parsed.path[1:] if parsed.path is not None else None

        assert parsed.hostname is not None, f'hostname not found in {url}'
        assert path is not None and len(path) > 0, f'database was not found in {url}'

        self.influx_host = parsed.hostname
        self.influx_port = parsed.port or 8086
        self.influx_database = path
        self.influx_username = parsed.username
        self.influx_password = parsed.password

        params = parse_qs(parsed.query)

        assert 'Testplan' in params, f'Testplan not found in {parsed.query}'
        self._testplan = unquote(params['Testplan'][0])
        # self._env = env.parsed_options.target_env
        self._target_environment = unquote(params['TargetEnvironment'][0]) if 'TargetEnvironment' in params else None
        self.environment = environment
        self._hostname = get_hostname()
        self._username = os.getenv('USER', 'unknown')
        self._events: List[InfluxDbPoint] = []
        self._finished = False
        self._profile_name = params['ProfileName'][0] if 'ProfileName' in params else ''
        self._description = params['Description'][0] if 'Description' in params else ''

        self.run_events_greenlet = gevent.spawn(self.run_events)
        self.run_user_count_greenlet = gevent.spawn(self.run_user_count)
        self.connection = self.create_client().connect()
        self.grizzly = GrizzlyContext()
        self.logger = logging.getLogger(__name__)
        self.environment.events.request.add_listener(self.request)
        self.environment.events.quit.add_listener(self.on_quit)

    def on_quit(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        self._finished = True

    def create_client(self) -> InfluxDb:
        return InfluxDb(
            host=self.influx_host,
            port=self.influx_port,
            database=self.influx_database,
            username=self.influx_username,
            password=self.influx_password,
        )

    @property
    def finished(self) -> bool:
        return self._finished

    def run_user_count(self) -> None:
        runner = self.environment.runner

        assert runner is not None, 'no runner is set'

        while not self.finished:
            points: List[Any] = []
            timestamp = datetime.now(timezone.utc).isoformat()

            for user_class_name, user_count in runner.user_classes_count.items():
                point: InfluxDbPoint = {
                    'measurement': 'user_count',
                    'tags': {
                        'testplan': self._testplan,
                        'hostname': self._hostname,
                        'user_class': user_class_name,
                    },
                    'time': timestamp,
                    'fields': {
                        'user_count': user_count,
                    }
                }
                points.append(point)

            if len(points) > 0:
                self.connection.write(points)

            if not self.finished:
                gevent.sleep(5.0)

    def run_events(self) -> None:
        while not self.finished:
            if self._events:
                # Buffer samples, so that a locust greenlet will write to the new list
                # instead of the one that has been sent into postgres client
                try:
                    events_buffer = self._events
                    self._events = []
                    self.connection.write(events_buffer)
                except Exception as e:
                    self.logger.error(str(e))

            if not self.finished:
                gevent.sleep(0.5)

    def _log_request(
        self,
        request_type: str,
        name: str,
        result: str,
        metrics: Dict[str, Any],
        exception: Optional[Any] = None,
    ) -> None:
        if exception is not None:
            if isinstance(exception, CatchResponseError):
                metrics['exception'] = str(exception)
            else:
                try:
                    metrics['exception'] = repr(exception)
                except AttributeError:
                    metrics['exception'] = (
                        f'{exception.__class__} (and it has no string representation)'
                    )
        else:
            metrics['exception'] = None

        tags = {
            'name': name,
            'method': request_type,
            'result': result,
            'testplan': self._testplan,
            'hostname': self._hostname,
        }

        try:
            scenario_identifier, _ = name.split(' ', 1)
            scenario_index = int(scenario_identifier) - 1
            current_scenario = self.grizzly.scenarios[scenario_index]
            tags.update({'scenario': current_scenario.locust_name})
        except ValueError:
            pass

        for key in os.environ.keys():
            if not key.startswith('TESTDATA_VARIABLE_'):
                continue

            variable = key.replace('TESTDATA_VARIABLE_', '')
            tags[variable] = os.environ[key]

        timestamp_finished = datetime.now(timezone.utc)
        timestamp_started = timestamp = timestamp_finished - timedelta(milliseconds=metrics['response_time'])

        metrics.update({
            'request_started': timestamp_started.isoformat(),
            'request_finished': timestamp_finished.isoformat(),
        })

        event: InfluxDbPoint = {
            'measurement': 'request',
            'tags': tags,
            'time': timestamp.isoformat(),
            'fields': {
                **metrics,
            }
        }

        self._events.append(event)

    def request(
        self,
        request_type: Any,
        name: str,
        response_time: Any,
        response_length: Any,
        exception: Optional[Any] = None,
        **_kwargs: Dict[str, Any],
    ) -> None:
        try:
            result = 'Success' if exception is None else 'Failure'

            if isinstance(response_time, float):
                response_time = int(round(response_time, 0))

            metrics = self._create_metrics(response_time, response_length)

            message_to_log = f'{result}: {request_type} {name} Response time: {response_time}'

            if exception is not None:
                message_to_log = f'{message_to_log} Exception: {str(exception)}'
                logger_method = self.logger.error
            else:
                logger_method = self.logger.debug

            logger_method(message_to_log)
            self._log_request(request_type, name, result, metrics, exception)
        except Exception as e:
            self.logger.error(f'failed to write metric for "{request_type} {name}": {str(e)}')

    def _create_metrics(self, response_time: int, response_length: int) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}

        metrics['response_time'] = response_time
        metrics['response_length'] = response_length if response_length >= 0 else None

        return metrics
