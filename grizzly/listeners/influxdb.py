"""@anchor pydoc:grizzly.listeners.influxdb InfluxDB
Write metrics to InfluxDB.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from platform import node as get_hostname
from typing import TYPE_CHECKING, Any, Literal, Optional, TypedDict, cast
from urllib.parse import parse_qs, unquote, urlparse

import gevent
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from grizzly.context import GrizzlyContext
from grizzly.types.locust import CatchResponseError, Environment

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

    from grizzly.types import Self

logger = logging.getLogger(__name__)


class InfluxDbError(Exception):
    pass


class InfluxDbPoint(TypedDict):
    measurement: str
    tags: dict[str, Any]
    time: str
    fields: dict[str, Any]


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

    def connect(self) -> Self:
        self.client = InfluxDBClient(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            gzip=True,
        )
        return self

    def __enter__(self) -> Self:
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        self.client.__exit__(exc_type, exc, traceback)

        if exc is not None:
            if isinstance(exc, InfluxDBClientError):
                content = json.loads(exc.content)
                message = f'{exc.code}: {content["error"]}'
                raise InfluxDbError(message)
            elif isinstance(exc, InfluxDbError):  # noqa: RET506
                raise exc

            raise InfluxDbError(exc)

        return True

    def read(self, table: str, columns: list[str]) -> list[dict[str, Any]]:
        query = f'select {",".join(columns)} from "{table}";'  # noqa: S608
        logger.debug('query: %s', query)
        result = self.client.query(query)

        return cast(list[dict[str, Any]], result.raw['series'])

    def write(self, values: list[InfluxDbPoint]) -> None:
        try:
            self.client.write_points(values)
            logger.debug('successfully wrote %d points to %s@%s:%d', len(values), self.database, self.host, self.port)
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
        assert path is not None, f'{url} contains no path'
        assert len(path) > 0, f'database was not found in {url}'

        self.influx_host = parsed.hostname
        self.influx_port = parsed.port or 8086
        self.influx_database = path
        self.influx_username = parsed.username
        self.influx_password = parsed.password

        params = parse_qs(parsed.query)

        assert 'Testplan' in params, f'Testplan was not found in {parsed.query}'
        self._testplan = unquote(params['Testplan'][0])
        self._target_environment = unquote(params['TargetEnvironment'][0]) if 'TargetEnvironment' in params else None
        self.environment = environment
        self._hostname = get_hostname()
        self._username = os.getenv('USER', 'unknown')
        self._events: list[InfluxDbPoint] = []
        self._finished = False
        self._profile_name = params['ProfileName'][0] if 'ProfileName' in params else ''
        self._description = params['Description'][0] if 'Description' in params else ''

        self.run_events_greenlet = gevent.spawn(self.run_events)
        self.run_user_count_greenlet = gevent.spawn(self.run_user_count)
        self.connection = self.create_client().connect()
        self.grizzly = GrizzlyContext()
        self.logger = logging.getLogger(__name__)
        self.environment.events.request.add_listener(self.request)
        self.environment.events.heartbeat_sent.add_listener(self.heartbeat_sent)
        self.environment.events.heartbeat_received.add_listener(self.heartbeat_received)
        self.environment.events.usage_monitor.add_listener(self.usage_monitor)
        self.environment.events.quit.add_listener(self.on_quit)

        self.grizzly.events.keystore_request.add_listener(self.on_grizzly_event)
        self.grizzly.events.testdata_request.add_listener(self.on_grizzly_event)
        self.grizzly.events.user_event.add_listener(self.on_grizzly_event)

    def on_quit(self, *_args: Any, **_kwargs: Any) -> None:
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
            points: list[Any] = []
            timestamp = datetime.now(timezone.utc).isoformat()

            for user_class_name, user_count in runner.user_classes_count.items():
                point: InfluxDbPoint = {
                    'measurement': 'user_count',
                    'tags': {
                        'environment': self._target_environment,
                        'testplan': self._testplan,
                        'profile': self._profile_name,
                        'description': self._description,
                        'hostname': self._hostname,
                        'user_class': user_class_name,
                    },
                    'time': timestamp,
                    'fields': {
                        'user_count': user_count,
                    },
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
                except:
                    self.logger.exception('failed to write metrics')

            if not self.finished:
                gevent.sleep(0.5)

    def _override_event(self, event: InfluxDbPoint, context: dict[str, Any]) -> None:
        # override values set in context
        for key, value in context.items():
            if not key.startswith('__') and not key.endswith('__'):
                continue

            override_key = key[2:-2]

            if override_key.startswith('fields_'):
                override_key = override_key[7:]

                if override_key not in event['fields']:
                    continue

                event['fields'].update({override_key: value})
            elif override_key.startswith('tags_'):
                override_key = override_key[5:]

                if override_key not in event['tags']:
                    continue

                event['tags'].update({override_key: value})
            else:
                if override_key not in event:
                    continue

                event.update({override_key: value})  # type: ignore[misc]

    def _create_event(self, timestamp: str, measurement: str, tags: dict[str, str | None], metrics: dict[str, Any]) -> None:
        tags = {
            'testplan': self._testplan,
            'hostname': self._hostname,
            'environment': self._target_environment,
            'profile': self._profile_name,
            'description': self._description,
            **tags,
        }

        event: InfluxDbPoint = {
            'measurement': measurement,
            'tags': tags,
            'time': timestamp,
            'fields': {
                **metrics,
            },
        }

        self._events.append(event)

    def on_grizzly_event(
        self, *,
        timestamp: str,
        metrics: dict[str, Any],
        tags: dict[str, str | None],
        measurement: str,
    ) -> None:
        self._create_event(timestamp, measurement, tags, metrics)

    def _log_request(
        self,
        request_type: str,
        name: str,
        result: str,
        metrics: dict[str, Any],
        context: dict[str, Any],
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
            'environment': self._target_environment,
            'profile': self._profile_name,
            'description': self._description,
        }

        try:
            scenario_identifier, _ = name.split(' ', 1)
            scenario_index = int(scenario_identifier) - 1
            current_scenario = self.grizzly.scenarios[scenario_index]
            tags.update({'scenario': current_scenario.locust_name})
        except ValueError:
            pass

        for key in os.environ:
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
            },
        }

        self._override_event(event, context)

        self._events.append(event)

    def request(
        self,
        request_type: Any,
        name: str,
        response_time: Any,
        response_length: Any,
        context: dict[str, Any],
        exception: Optional[Any] = None,
        **_kwargs: Any,
    ) -> None:
        try:
            result = 'Success' if exception is None else 'Failure'

            if isinstance(response_time, float):
                response_time = int(round(response_time, 0))

            metrics = self._create_metrics(response_time, response_length)

            message_to_log = f'{result}: {request_type} {name} Response time: {response_time}'

            if exception is not None:
                message_to_log = f'{message_to_log} Exception: {exception!r}'
                logger_method = self.logger.error
            else:
                logger_method = self.logger.debug

            logger_method(message_to_log)
            self._log_request(request_type, name, result, metrics, context, exception)
        except Exception:
            self.logger.exception('failed to write metric for "%s %s"', request_type, name)

    def _create_metrics(self, response_time: int, response_length: int) -> dict[str, Any]:
        metrics: dict[str, Any] = {}

        metrics['response_time'] = response_time
        metrics['response_length'] = response_length if response_length >= 0 else None

        return metrics

    def heartbeat_sent(self, client_id: str, timestamp: float) -> None:
        return self._heartbeat(client_id=client_id, direction='sent', timestamp=timestamp)

    def heartbeat_received(self, client_id: str, timestamp: float) -> None:
        return self._heartbeat(client_id=client_id, direction='received', timestamp=timestamp)

    def _heartbeat(self, client_id: str, direction: Literal['sent', 'received'], timestamp: float) -> None:
        _timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

        tags: dict[str, str | None] = {
            'client_id': client_id,
            'direction': direction,
        }

        metrics: dict[str, int] = {'value': 1}

        self._create_event(_timestamp, 'heartbeat', tags, metrics)

    def usage_monitor(self, environment: Environment, cpu_usage: float, memory_usage: float) -> None:  # noqa: ARG002
        timestamp = datetime.now(timezone.utc).isoformat()

        metrics: dict[str, float] = {
            'cpu': cpu_usage,
            'memory': memory_usage,
        }

        self._create_event(timestamp, 'usage_monitor', {}, metrics)
