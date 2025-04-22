"""@anchor pydoc:grizzly.listeners.influxdb InfluxDB
Write metrics to InfluxDB.
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from platform import node as get_hostname
from typing import TYPE_CHECKING, Any, Literal, Optional, Protocol, TypedDict, cast
from urllib.parse import parse_qs, unquote, urlparse

import gevent
from influxdb import InfluxDBClient as InfluxDBClientV1
from influxdb.exceptions import InfluxDBClientError
from influxdb_client import InfluxDBClient as InfluxDBClientV2  # type: ignore[attr-defined]
from influxdb_client.rest import ApiException

from grizzly.types.locust import CatchResponseError, Environment

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

    from influxdb_client.client.query_api import QueryApi
    from influxdb_client.client.write_api import WriteApi

    from grizzly.types import Self

logger = logging.getLogger(__name__)


class InfluxDbError(Exception):
    pass


class InfluxDbPoint(TypedDict):
    measurement: str
    tags: dict[str, Any]
    time: str
    fields: dict[str, Any]


class InfluxDb(Protocol):
    def read(self, table: str, columns: list[str]) -> Any: ...

    def write(self, values: list[InfluxDbPoint]) -> None: ...

    def connect(self) -> Self: ...

    def disconnect(self) -> None: ...


class InfluxDbV1(InfluxDb):
    client: InfluxDBClientV1

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
        self.client = InfluxDBClientV1(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            gzip=True,
        )
        return self

    def disconnect(self) -> None:
        self.__exit__(None, None, None)

    def __enter__(self) -> Self:
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        with suppress(Exception):
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

    def read(self, table: str, columns: list[str]) -> Any:
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


class InfluxDbV2(InfluxDb):
    client: InfluxDBClientV2
    query_api: QueryApi
    write_api: WriteApi

    host: str
    port: int
    bucket: str
    org : str
    token: str | None

    def __init__(self, host: str, port: int, org: str, bucket: str, token: str | None = None) -> None:
        self.host = host
        self.port = port
        self.org = org
        self.bucket = bucket
        self.token = token

    def connect(self) -> Self:
        self.client = InfluxDBClientV2(
            url=f"https://{self.host}:{self.port}",
            token=cast(str, self.token),
            org=self.org,
            enable_gzip=True,
        )
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api()
        return self

    def disconnect(self) -> None:
        self.__exit__(None, None, None)

    def __enter__(self) -> Self:
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        with suppress(Exception):
            self.client.close()
            self.client.__exit__(exc_type, exc, traceback)

        if self.write_api:
            self.write_api.close()

        if exc is not None:
            if isinstance(exc, ApiException):
                message = f'{exc.status}: {exc.reason}'
                raise InfluxDbError(message) from exc

            if isinstance(exc, InfluxDbError):
                raise exc

            raise InfluxDbError(exc)

        return True

    def read(self, measurement: str, fields: list[str]) -> dict[str, Any]:
        flux_query = f"""
        from(bucket: "{self.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "{measurement}")
        |> filter(fn: (r) => {" or ".join(f'r._field == "{field}"' for field in fields)})
        """
        logger.debug('Flux query: %s', flux_query)
        result = self.query_api.query(flux_query, org=self.org)
        # Convert FluxTable results to json, and to dict
        return cast(dict[str, Any], json.loads(result.to_json()))

    def write(self, values: list[InfluxDbPoint]) -> None:
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=values)
            logger.debug('successfully wrote %d points to bucket %s@%s:%d', len(values), self.bucket, self.host, self.port)
        except ApiException as e:
            code = e.status
            message = e.reason if e.reason is not None else "<unknown>"

            if e.status is not None:
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

        if parsed.scheme == 'influxdb2':
            assert parsed.hostname is not None, f'hostname not found in {url}'
            assert path is not None, f'{url} contains no path'
            assert len(path) > 0, f'database was not found in {url}'

            self.influx_host = parsed.hostname
            self.influx_port = parsed.port or 8086
            self.influx_org, self.influx_bucket = path.split(':')
            self.influx_token = parsed.username or ''
            self.influx_version = 2
        else:
            assert parsed.hostname is not None, f'hostname not found in {url}'
            assert path is not None, f'{url} contains no path'
            assert len(path) > 0, f'database was not found in {url}'

            self.influx_host = parsed.hostname
            self.influx_port = parsed.port or 8086
            self.influx_database = path
            self.influx_username = parsed.username
            self.influx_password = parsed.password
            self.influx_version = 1

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

        self.client = self.create_client()
        self.connection = self.client.connect()
        self.logger = logging.getLogger(__name__)
        self.environment.events.request.add_listener(self.request)
        self.environment.events.heartbeat_sent.add_listener(self.heartbeat_sent)
        self.environment.events.heartbeat_received.add_listener(self.heartbeat_received)
        self.environment.events.usage_monitor.add_listener(self.usage_monitor)
        self.environment.events.quit.add_listener(self.on_quit)

        from grizzly.context import grizzly
        self.grizzly = grizzly
        self.grizzly.events.keystore_request.add_listener(self.on_grizzly_event)
        self.grizzly.events.testdata_request.add_listener(self.on_grizzly_event)
        self.grizzly.events.user_event.add_listener(self.on_grizzly_event)
        self.run_events_greenlet = gevent.spawn(self.run_events)
        self.run_user_count_greenlet = gevent.spawn(self.run_user_count)

    def on_quit(self, *_args: Any, **_kwargs: Any) -> None:
        self._finished = True

    def create_client(self) -> InfluxDb:
        if self.influx_version == 1:
            return InfluxDbV1(
                host=self.influx_host,
                port=self.influx_port,
                database=self.influx_database,
                username=self.influx_username,
                password=self.influx_password,
            )
        return InfluxDbV2(
            host=self.influx_host,
            port=self.influx_port,
            bucket=self.influx_bucket,
            token=self.influx_token,
            org=self.influx_org,
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

        with suppress(Exception):
            self.client.disconnect()

    def run_events(self) -> None:
        while not self.finished:
            if self._events:
                # Buffer samples, so that a locust greenlet will write to the new list
                # instead of the one that has been sent into postgres client
                try:
                    events_buffer = [*self._events]
                    self._events = []
                    self.connection.write(events_buffer)
                    self.logger.debug('wrote %d measurements', len(events_buffer))
                except:
                    self.logger.exception('failed to write metrics')

            if not self.finished:
                gevent.sleep(0.5)

        with suppress(Exception):
            self.client.disconnect()

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
            'user': context.get('user', id(self)),
        }

        try:
            scenario_identifier, _ = name.split(' ', 1)
            scenario_index = int(scenario_identifier) - 1
            current_scenario = self.grizzly.scenarios[scenario_index]
            tags.update({'scenario': current_scenario.locust_name})
        except ValueError:
            pass

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

        self.logger.debug('%s %s %s', request_type, name, event['time'])

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
                self.logger.info(message_to_log)

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
