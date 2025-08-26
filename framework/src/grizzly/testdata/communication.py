"""RPC client and server for synchronized testdata."""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from json import dumps as jsondumps
from os import environ
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, TypedDict, cast
from uuid import uuid4

from dateutil.parser import parse as date_parser
from gevent import sleep as gsleep
from gevent.event import AsyncResult
from gevent.lock import Semaphore

from grizzly.events import GrizzlyEventDecoder, GrizzlyEvents, event, events
from grizzly.types.locust import LocalRunner, MasterRunner, MessageHandler, StopUser, WorkerRunner

from . import GrizzlyVariables
from .utils import transform
from .variables import AtomicVariablePersist

if TYPE_CHECKING:  # pragma: no cover
    from locust.event import EventHook
    from locust.rpc.protocol import Message

    from grizzly.context import GrizzlyContext
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import StrDict, TestdataType
    from grizzly.types.locust import Environment


ActionType = Literal['start', 'stop']

logger = logging.getLogger(__name__)


@dataclass
class AsyncTimer:
    name: str
    tid: str
    version: str
    start: datetime | None = field(init=True, default=None)
    stop: datetime | None = field(init=True, default=None)

    def is_complete(self) -> bool:
        return self.start is not None and self.stop is not None

    def complete(self, event: EventHook) -> None:
        error: str | None = None
        duration = 0
        started: str | None = None
        finished: str | None = None

        if not self.is_complete():
            missing_timestamps: list[str] = []

            if self.start is None:
                missing_timestamps.append('start')
            else:
                started = self.start.isoformat()

            if self.stop is None:
                missing_timestamps.append('stop')
            else:
                finished = self.stop.isoformat()

            missing_timestamp = ', '.join(missing_timestamps)

            error = f'cannot complete timer for id "{self.tid}" and version "{self.version}", missing {missing_timestamp} timestamp'
        else:
            duration = int((cast('datetime', self.stop) - cast('datetime', self.start)).total_seconds() * 1000)
            started = cast('datetime', self.start).isoformat()
            finished = cast('datetime', self.stop).isoformat()

        if duration < 0:
            logger.warning('duration for "%s" between stop %s and start %s was weird, %d ms', self.name, self.stop, self.start, duration)

        event.fire(
            request_type=AsyncTimersProducer.__request_method__,
            name=self.name,
            response_time=duration,
            response_length=0,
            exception=error,
            context={
                '__time__': started,
                '__fields_request_started__': started,
                '__fields_request_finished__': finished,
            },
        )


class AsyncTimersConsumer:
    scenario: GrizzlyScenario
    semaphore: Semaphore

    _start: list[dict[str, str]]
    _stop: list[dict[str, str]]

    def __init__(self, scenario: GrizzlyScenario, semaphore: Semaphore) -> None:
        self.semaphore = semaphore
        self.scenario = scenario

        self._start = []
        self._stop = []

        if isinstance(self.scenario.grizzly.state.locust, WorkerRunner):
            self.scenario.grizzly.state.locust.environment.events.report_to_master.add_listener(self.on_report_to_master)

    @property
    def logger(self) -> logging.Logger:
        return self.scenario.user.logger

    @classmethod
    def parse_date(cls, value: str) -> datetime:
        timestamp = date_parser(value)

        # if timezone information wasn't included in the date string, assume localtime
        if timestamp.tzinfo is None:
            timestamp = timestamp.astimezone()

        return timestamp

    def on_report_to_master(self, client_id: str, data: StrDict) -> None:  # noqa: ARG002
        with self.semaphore:
            # append this producers timers that should be started and stopped
            data.update(
                {
                    'async_timers': {
                        'start': [*data.get('async_timers', {}).get('start', []), *self._start],
                        'stop': [*data.get('async_timers', {}).get('stop', []), *self._stop],
                    },
                },
            )

            self.logger.debug('reported start for %d timers and stop for %d timers to master', len(self._start), len(self._stop))

            self._start.clear()
            self._stop.clear()

    def toggle(self, action: ActionType, name: str, tid: str, version: str, timestamp: datetime | str | None = None) -> None:
        if timestamp is None:
            timestamp = datetime.now().astimezone()
        elif isinstance(timestamp, str):
            timestamp = self.parse_date(timestamp)

        data = {
            'name': name,
            'tid': tid,
            'version': version,
            'timestamp': timestamp.isoformat(),
        }

        getattr(self, action)(data)

    def start(self, data: dict[str, str]) -> None:
        if isinstance(self.scenario.grizzly.state.locust, LocalRunner):
            cast('TestdataProducer', self.scenario.grizzly.state.producer).async_timers.toggle('start', data)
        else:
            self._start.append(data)

    def stop(self, data: dict[str, str]) -> None:
        if isinstance(self.scenario.grizzly.state.locust, LocalRunner):
            producer = cast('TestdataProducer', self.scenario.grizzly.state.producer)
            producer.async_timers.toggle('stop', data)
        else:
            self._stop.append(data)


class AsyncTimersProducer:
    __request_method__ = 'DOC'  # @TODO: should not this when merging to main

    grizzly: GrizzlyContext
    semaphore: Semaphore

    timers: dict[str, AsyncTimer]

    def __init__(self, grizzly: GrizzlyContext, semaphore: Semaphore) -> None:
        self.semaphore = semaphore
        self.grizzly = grizzly

        self.timers = {}

        if isinstance(grizzly.state.locust, MasterRunner):
            grizzly.state.locust.environment.events.worker_report.add_listener(self.on_worker_report)

    @property
    def logger(self) -> logging.Logger:
        assert self.grizzly.state.producer is not None
        return self.grizzly.state.producer.logger

    @classmethod
    def extract(cls, data: dict[str, str]) -> tuple[str, str, str, datetime]:
        timestamp = date_parser(data['timestamp'])

        return data['name'], data['tid'], data['version'], timestamp

    def on_worker_report(self, client_id: str, data: StrDict) -> None:
        async_timers = data.get('async_timers', {})

        async_timers_start = async_timers.get('start', [])
        async_timers_stop = async_timers.get('stop', [])

        for async_data in async_timers_start:
            self.toggle('start', async_data)

        for async_data in async_timers_stop:
            self.toggle('stop', async_data)

        self.logger.debug(
            'started %d timers and stopped %d timers from worker %s',
            len(async_timers_start),
            len(async_timers_stop),
            client_id,
        )

    def toggle(self, action: ActionType, data: dict[str, str]) -> None:
        name, tid, version, timestamp = self.extract(data)
        timer_id = f'{name}::{tid}::{version}'

        timer = self.timers.get(timer_id)

        if timer is not None:
            if getattr(timer, action) is not None:
                toggle_action = 'started' if action == 'start' else 'stopped'
                message = f'timer with name "{name}" for id "{tid}" with version "{version}" has already been {toggle_action}'
                self.logger.error(message)
                message = f'timer for id "{tid}" with version "{version}" has already been {toggle_action}'
                self.grizzly.state.locust.stats.log_error(self.__request_method__, name, message)
                return
        else:
            timer = AsyncTimer(name, tid, version)

        setattr(timer, action, timestamp)

        with self.semaphore:
            if timer.is_complete():
                del self.timers[timer_id]
                timer.complete(self.grizzly.state.locust.environment.events.request)
            else:
                self.timers.update({timer_id: timer})


class KeystoreDecoder(GrizzlyEventDecoder):
    def __call__(
        self,
        *args: Any,
        tags: dict[str, str | None] | None,
        return_value: Any,  # noqa: ARG002
        exception: Exception | None,
        **kwargs: Any,
    ) -> tuple[StrDict, dict[str, str | None]]:
        request = cast('StrDict', args[self.arg] if isinstance(self.arg, int) else kwargs.get(self.arg))

        key: str | None = request.get('key')

        extra_tags = {}

        if key is not None and '::' in key:
            """Last suffix (which is prefixed with '::') is considered a unique identifier"""
            key, extra_tag = key.rsplit('::', 1)
            extra_tags.update({'unique_id': extra_tag})

        tags = {
            'key': key,
            'action': request.get('action'),
            'identifier': request.get('identifier'),
            **extra_tags,
            **(tags or {}),
        }

        remove = request.get('remove', None)
        if remove is not None:
            tags.update({'remove': remove})

        metrics: StrDict = {'error': None}

        if exception is not None:
            metrics.update({'error': str(exception)})

        return metrics, tags


class TestdataDecoder(GrizzlyEventDecoder):
    def __call__(
        self,
        *args: Any,
        tags: dict[str, str | None] | None,
        return_value: Any,
        exception: Exception | None,
        **kwargs: Any,
    ) -> tuple[StrDict, dict[str, str | None]]:
        request = cast('StrDict', args[self.arg] if isinstance(self.arg, int) else kwargs.get(self.arg))

        tags = {
            'action': (return_value or {}).get('action'),
            'identifier': request.get('identifier'),
            **(tags or {}),
        }

        metrics: StrDict = {'error': None}

        if exception is not None:
            metrics.update({'error': str(exception)})

        return metrics, tags


class TestdataConsumer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    _responses: ClassVar[dict[int, AsyncResult]] = {}

    scenario: GrizzlyScenario
    runner: LocalRunner | WorkerRunner
    identifier: str
    stopped: bool
    poll_interval: float
    response: StrDict
    events: GrizzlyEvents
    async_timers: AsyncTimersConsumer

    semaphore = Semaphore()

    def __init__(self, runner: LocalRunner | WorkerRunner, scenario: GrizzlyScenario) -> None:
        self.runner = runner
        self.scenario = scenario
        self.identifier = scenario.__class__.__name__

        self.stopped = False

        self.response = {}
        self.logger = logging.getLogger(f'{scenario.__class__.__name__}/testdata')
        self.logger.debug('started consumer')

        self.async_timers = AsyncTimersConsumer(scenario, self.semaphore)

    @classmethod
    def handle_response(cls, environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG003
        uid = msg.data['uid']
        response = msg.data['response']

        cls._responses[uid].set(response)

    @event(events.testdata_request, tags={'type': 'consumer'}, decoder=TestdataDecoder(arg='request'))
    def _testdata_request(self, *, request: StrDict) -> StrDict | None:
        return self._request({'message': 'testdata', **request})

    def testdata(self) -> StrDict | None:
        request = {
            'identifier': self.identifier,
        }

        response = self._testdata_request(request=request)

        if response is None:
            self.logger.error('no testdata received')
            return None

        if response['action'] == 'stop':
            self.logger.debug('received stop command')
            return None

        if response['action'] != 'consume':
            self.logger.error('unknown action "%s" received, stopping user', response['action'])
            raise StopUser

        data = response['data']

        self.logger.debug('testdata received: %r', data)

        variables: StrDict | None = None
        if 'variables' in data:
            variables = transform(self.scenario.user._scenario, data['variables'], objectify=True)
            del data['variables']

        data = transform(self.scenario.user._scenario, data, objectify=False)

        if variables is not None:
            data['variables'] = variables

        return data

    def keystore_get(self, key: str, *, remove: bool) -> Any | None:
        request = {
            'action': 'get',
            'key': key,
            'remove': remove,
        }

        response = self._keystore_request(request=request)

        return (response or {}).get('data', None)

    def keystore_set(self, key: str, value: Any) -> None:
        request = {
            'action': 'set',
            'key': key,
            'data': value,
        }

        self._keystore_request(request=request)

    def keystore_inc(self, key: str, step: int = 1) -> int | None:
        request = {
            'action': 'inc',
            'key': key,
            'data': step,
        }

        response = self._keystore_request(request=request)

        value = (response or {}).get('data', None)

        if value is not None:
            return int(value)

        return value

    def keystore_dec(self, key: str, step: int = 1) -> int | None:
        request = {
            'action': 'dec',
            'key': key,
            'data': step,
        }

        response = self._keystore_request(request=request)

        value = (response or {}).get('data', None)

        if value is not None:
            return int(value)

        return value

    def keystore_push(self, key: str, value: Any) -> None:
        request = {
            'action': 'push',
            'key': key,
            'data': value,
        }

        self._keystore_request(request=request)

    def _keystore_pop_poll(self, request: StrDict) -> str | None:
        response = self._keystore_request(request=request)
        value: str | None = (response or {}).get('data', None)

        return value

    def keystore_pop(self, key: str, *, wait: int = -1, poll_interval: float = 1.0) -> str:
        request = {
            'action': 'pop',
            'key': key,
        }

        value = self._keystore_pop_poll(request)

        start = perf_counter()
        while value is None:
            gsleep(poll_interval)

            with suppress(Exception):
                value = self._keystore_pop_poll(request)

            if value is None and wait > -1 and (int(perf_counter() - start) > wait):
                error_message = 'no value for key "{key}" available within {wait} seconds'
                self.logger.error(error_message.format(key=key, wait=wait))

                if '::' in key:
                    """Last suffix (which is prefixed with '::') is considered a unique identifier"""
                    ambigous_key, _ = key.rsplit('::', 1)
                    ambigous_key = f'{ambigous_key}::{{{{ id }}}}'
                else:
                    ambigous_key = key

                raise RuntimeError(error_message.format(key=ambigous_key, wait=wait))

        return value

    def keystore_del(self, key: str) -> None:
        request = {
            'action': 'del',
            'key': key,
        }

        self._keystore_request(request=request)

    @event(events.keystore_request, tags={'type': 'consumer'}, decoder=KeystoreDecoder(arg='request'))
    def _keystore_request(self, *, request: StrDict) -> StrDict | None:
        request.update({'identifier': self.identifier})

        return self._request({'message': 'keystore', **request})

    def _request(self, request: dict[str, str]) -> StrDict | None:
        with self.semaphore:
            uid = id(self.scenario.user)  # user id (unique instance)
            rid = str(uuid4())  # request id

            if uid in self._responses:
                self.logger.warning('greenlet %d is already waiting for testdata', uid)

            self._responses.update({uid: AsyncResult()})
            self.runner.send_message('produce_testdata', {'uid': uid, 'cid': self.runner.client_id, 'rid': rid, 'request': request})

            # waits for async result
            try:
                return cast('StrDict | None', self._responses[uid].get(timeout=10.0))
            finally:
                # remove request as pending
                del self._responses[uid]


class TestdataProducer:
    # need so pytest doesn't raise PytestCollectionWarning
    __test__: bool = False

    _stopping: bool
    _persist_file: Path

    logger: logging.Logger
    semaphore: ClassVar[Semaphore] = Semaphore()
    semaphores: ClassVar[dict[str, Semaphore]] = {}
    scenarios_iteration: dict[str, int]
    testdata: TestdataType
    has_persisted: bool
    keystore: StrDict
    runner: MasterRunner | LocalRunner
    grizzly: GrizzlyContext
    async_timers: AsyncTimersProducer

    def __init__(self, runner: MasterRunner | LocalRunner, testdata: TestdataType) -> None:
        self.testdata = testdata
        self.runner = runner

        self.logger = logging.getLogger(f'{__name__}/producer')

        self.scenarios_iteration = {}

        self.has_persisted = False

        self.logger.debug('serving:\n%r', self.testdata)

        feature_file = environ.get('GRIZZLY_FEATURE_FILE', None)
        context_root = environ.get('GRIZZLY_CONTEXT_ROOT', None)
        assert feature_file is not None
        assert context_root is not None

        persist_root = Path(context_root) / 'persistent'
        self._persist_file = persist_root / f'{Path(feature_file).stem}.json'

        self.keystore = {}

        from grizzly.context import grizzly  # noqa: PLC0415

        self.grizzly = grizzly

        self.async_timers = AsyncTimersProducer(self.grizzly, self.semaphore)
        self.runner.register_message('produce_testdata', self.handle_request, concurrent=True)
        self.runner.environment.events.test_stop.add_listener(self.on_test_stop)

    def on_test_stop(self, environment: Environment, *_args: Any, **_kwargs: Any) -> None:  # noqa: ARG002
        self.logger.debug('test stopping')
        with self.semaphore:
            self.persist_data()
            for scenario_name in self.scenarios_iteration:
                self.scenarios_iteration[scenario_name] = 0

    def persist_data(self) -> None:
        if self.has_persisted:
            return

        try:
            self.logger.info('persisting test data...')
            variables_state: dict[str, dict[str, str | StrDict]] = {}

            for scenario_name, testdata in self.testdata.items():
                variable_state: dict[str, str | StrDict] = {}
                for key, variable in testdata.items():
                    if '.' not in key or variable == '__on_consumer__':
                        continue

                    with suppress(Exception):
                        _, _, variable_name, _ = GrizzlyVariables.get_variable_spec(key)

                        if not isinstance(variable, AtomicVariablePersist):
                            continue

                        variable_state.update({key: variable.generate_initial_value(variable_name)})

                if len(variable_state) > 0:
                    variables_state.update({scenario_name: variable_state})

            # only write file if we actually have something to write
            if len(variables_state) > 0:
                self._persist_file.parent.mkdir(exist_ok=True, parents=True)
                self._persist_file.write_text(jsondumps(variables_state, indent=2))
                self.logger.info('feature file data persisted in %s', self._persist_file)
                self.has_persisted = True
            else:
                self.logger.info('no data to persist for feature file, skipping')
        except:
            self.logger.exception('failed to persist feature file data')

    def stop(self) -> None:
        self.persist_data()

    def _remove_key(self, key: str, response: StrDict) -> None:
        try:
            del self.keystore[key]
        except:
            message = f'failed to remove key "{key}"'
            self.logger.exception(message)
            response.update({'error': message})

    @event(events.keystore_request, tags={'type': 'producer'}, decoder=KeystoreDecoder(arg='request'))
    def _handle_request_keystore(self, *, request: StrDict) -> StrDict:  # noqa: PLR0915, PLR0912, C901
        response = request
        key: str | None = response.get('key', None)

        if key is None:
            message = 'key is not present in request'
            self.logger.error(message)
            response.update({'data': None, 'error': message})
            return response

        action: str | None = request.get('action')

        if action == 'get':
            response.update({'data': self.keystore.get(key, None)})
            if request.get('remove', False):
                self._remove_key(key, response)

        elif action == 'set':
            set_value: str | None = response.get('data', None)

            self.keystore.update({key: set_value})
            response.update({'data': set_value})
        elif action in ['inc', 'dec']:
            step: int = response.get('data', 1)
            response.update({'data': None})

            operation_value: Any = self.keystore.get(key, 0)

            if isinstance(operation_value, int):
                new_value = operation_value + step if action == 'inc' else operation_value - step
            elif isinstance(operation_value, str) and operation_value.isnumeric():
                new_value = int(operation_value) + step if action == 'inc' else int(operation_value) - step
            else:
                operation = 'incremented' if action == 'inc' else 'decremented'
                message = f'value {operation_value} for key "{key}" cannot be {operation}'
                self.logger.error(message)
                response.update({'error': message})
                return response

            self.keystore.update({key: new_value})
            response.update({'data': new_value})
        elif action == 'push':
            push_value: str | None = response.get('data', None)

            if key not in self.keystore:
                self.keystore.update({key: []})

            self.keystore[key].append(push_value)
            response.update({'data': push_value})
        elif action == 'pop':
            pop_value: str | None
            response.update({'data': None})
            try:
                # since dict throws `KeyError` on pop, and str `AttributeError`
                if key in self.keystore and not isinstance(self.keystore[key], list):
                    raise AttributeError

                pop_value = self.keystore[key].pop(0)

                # remove key if it was the last value
                if len(self.keystore[key]) < 1:
                    response.update({'data': pop_value})
                    self._remove_key(key, response)
            except AttributeError:
                message = f'key "{key}" is not a list, it has not been pushed to'
                self.logger.exception(message)
                pop_value = None
                response.update({'error': message})
            except (KeyError, IndexError):
                pop_value = None

            response.update({'data': pop_value})
        elif action == 'del':
            response.update({'data': None})
            self._remove_key(key, response)
        else:
            message = f'received unknown keystore action "{action}"'
            self.logger.error(message)
            response.update({'data': None, 'error': message})

        return response

    @event(events.testdata_request, tags={'type': 'producer'}, decoder=TestdataDecoder(arg='request'))
    def _handle_request_testdata(self, *, request: StrDict) -> StrDict:  # noqa: PLR0912
        scenario_name = request.get('identifier', '')
        response: StrDict = {
            'action': 'stop',
        }

        try:
            scenario = self.grizzly.scenarios.find_by_class_name(scenario_name)

            if scenario is not None:
                if scenario_name not in self.scenarios_iteration and scenario.iterations > 0:
                    self.scenarios_iteration[scenario_name] = 0

                if (
                    not (scenario_name in self.scenarios_iteration and self.scenarios_iteration[scenario_name] < scenario.iterations)
                    or scenario_name not in self.scenarios_iteration
                ):
                    return response

                testdata = self.testdata.get(scenario_name, {})
                response['action'] = 'consume'
                data: StrDict = {'variables': {}}
                loaded_variable_datatypes: StrDict = {}

                for key, variable in testdata.items():
                    if '.' in key and variable != '__on_consumer__':
                        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(key)
                        _, data_attribute = key.rsplit('.', 1)

                        if variable_name != data_attribute:
                            testdata_type = f'{variable_type}.{variable_name}'
                            if module_name != 'grizzly.testdata.variables':
                                testdata_type = f'{module_name}.{testdata_type}'

                            if testdata_type not in loaded_variable_datatypes:
                                try:
                                    loaded_variable_datatypes[testdata_type] = variable[variable_name]
                                except NotImplementedError:
                                    continue

                            value = loaded_variable_datatypes[testdata_type][data_attribute]
                        else:
                            try:
                                value = variable[variable_name]
                            except NotImplementedError:
                                continue
                    else:
                        value = variable

                    if value is None and scenario_name not in self.scenarios_iteration:
                        response['action'] = 'stop'
                        self.logger.warning('%s does not have a value and iterations is not set for %s, stop test', key, scenario_name)
                        data = {}
                        break

                    data['variables'][key] = value

                    alias = scenario.variables.alias.get(key, None)
                    if alias is not None:
                        data_key = alias
                        data[data_key] = value

                data['__iteration__'] = (self.scenarios_iteration[scenario_name], scenario.iterations)

                response['data'] = data

                if scenario_name in self.scenarios_iteration:
                    self.scenarios_iteration[scenario_name] += 1
                    self.logger.debug('%s: iteration=%d', scenario_name, self.scenarios_iteration[scenario_name])
        except TypeError:
            response = {
                'action': 'stop',
            }
            self.logger.exception('test data error, stop consumer %s', scenario_name)

        return response

    def handle_request(self, environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG002
        cid = msg.data['cid']  # (worker) client id
        uid = msg.data['uid']  # user id (user instance)
        rid = msg.data['rid']  # request id
        request = msg.data['request']

        self.logger.debug('handling message from worker %s, user %s, request %s', cid, uid, rid)

        # only handle one request per worker at a time, but allow parallell requests between different scenarios
        if request['message'] == 'keystore':
            with self.semaphore:
                response = self._handle_request_keystore(request=request)
        elif request['message'] == 'testdata':
            scenario_name = request.get('identifier', None)

            # create semaphore for client if it does not exist already
            with self.semaphore:
                if scenario_name is not None and scenario_name not in self.semaphores:
                    self.semaphores.update({scenario_name: Semaphore()})

            with self.semaphores[scenario_name]:
                response = self._handle_request_testdata(request=request)
        else:
            self.logger.error('received unknown message "%s"', request['message'])
            response = {}

        self.runner.send_message('consume_testdata', {'uid': uid, 'rid': rid, 'response': response}, client_id=cid)


class GrizzlyMessage(TypedDict):
    uid: int
    rid: str


class GrizzlyMessageResponse(GrizzlyMessage):
    response: StrDict


class GrizzlyMessageRequest(GrizzlyMessage):
    cid: str
    request: StrDict


class GrizzlyMessageMapping(TypedDict):
    request: str
    response: str


class GrizzlyContextAware(Protocol):
    grizzly: GrizzlyContext
    logger: logging.Logger


class GrizzlyMessageHandler(metaclass=ABCMeta):
    __message_types__: ClassVar[GrizzlyMessageMapping]

    _responses: ClassVar[dict[int, AsyncResult]] = {}

    semaphore: ClassVar[Semaphore] = Semaphore()
    semaphores: ClassVar[dict[int, Semaphore]] = {}

    @classmethod
    @abstractmethod
    def create_response(cls, environment: Environment, key: int, request: StrDict) -> StrDict: ...

    @classmethod
    def send_request(cls, consumer: GrizzlyContextAware, request: StrDict, *, timeout: float = 10.0) -> StrDict:
        assert not isinstance(consumer.grizzly.state.locust, MasterRunner)

        message_type = cls.__message_types__['request']

        assert message_type is not None

        uid = id(consumer)
        rid = str(uuid4())

        if uid in cls._responses:
            consumer.logger.warning('greenlet %d is already waiting for testdata', uid)

        cls._responses.update({uid: AsyncResult()})

        consumer.grizzly.state.locust.send_message(message_type, {'uid': uid, 'cid': consumer.grizzly.state.locust.client_id, 'rid': rid, 'request': request})

        try:
            response = cast('StrDict', cls._responses[uid].get(timeout=timeout))
            error = response.get('error', None)

            if error is None:
                return response

            raise RuntimeError(error)
        finally:
            with suppress(KeyError):
                del cls._responses[uid]

    @classmethod
    def handle_response(cls, environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG003
        data = cast('GrizzlyMessageResponse', msg.data)
        uid = data['uid']
        response = data['response']

        cls._responses[uid].set(response)

    @classmethod
    def get_key(cls, value: StrDict) -> int:
        key_value: StrDict = {}
        for k, v in value.items():
            k_v = cls.get_key(v) if isinstance(v, dict) else v

            key_value.update({k: k_v})

        return hash(frozenset(key_value.items()))

    @classmethod
    def handle_request(cls, environment: Environment, msg: Message, **_kwargs: Any) -> None:
        message_type = cls.__message_types__['request']
        logger.debug('got %s: %r', msg.data, message_type)
        data = cast('GrizzlyMessageRequest', msg.data)
        cid = data['cid']  # (worker) client id
        uid = data['uid']  # user id (user instance)
        rid = data['rid']  # request id
        request = data['request']
        try:
            key = cls.get_key(request)
        except:
            logger.exception('failed to hash request %r', request)
            raise

        with cls.semaphore:
            if key not in cls.semaphores:
                cls.semaphores.update({key: Semaphore()})

        with cls.semaphores[key]:
            try:
                response = cls.create_response(environment, key, request)
            except Exception as e:
                response = {
                    'error': f'{e.__class__.__name__}: {e!s}',
                }
                logger.exception('failed to handle %s', message_type)

        assert environment.runner is not None

        message_type = cls.__message_types__['response']

        environment.runner.send_message(message_type, {'uid': uid, 'rid': rid, 'response': response}, client_id=cid)


GrizzlyDependencies = set[str | type[GrizzlyMessageHandler] | tuple[str, MessageHandler]]
