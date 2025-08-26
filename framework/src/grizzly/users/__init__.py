"""Package contains implementation for different type of endpoints and protocols.

These implementations are the basis for how to communicate with the system under test.

## Custom

It is possible to implement custom users, the only requirement is that they inherit `grizzly.users.GrizzlyUser`.
To get them to be executed by `grizzly` the full namespace has to be specified as `user_class_name` in the
scenarios [user][grizzly.steps.scenario.user] step.

There are examples of this in the [example][example] documentation.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from contextlib import suppress
from copy import copy, deepcopy
from datetime import datetime, timezone
from errno import ENAMETOOLONG
from itertools import chain
from json import dumps as jsondumps
from json import loads as jsonloads
from logging import Logger
from os import environ
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast, final

from async_messaged import AsyncMessageError
from gevent.event import Event
from locust.event import EventHook
from locust.user.task import LOCUST_STATE_RUNNING
from locust.user.users import User, UserMeta

from grizzly.events import GrizzlyEventHook, RequestLogger, ResponseHandler
from grizzly.exceptions import RestartScenario, StopScenario
from grizzly.testdata import GrizzlyVariables
from grizzly.types import GrizzlyResponse, RequestType, ScenarioState, StrDict
from grizzly.types.locust import Environment, StopUser
from grizzly.utils import has_template, merge_dicts

T = TypeVar('T', bound=UserMeta)

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.tasks import GrizzlyTask, RequestTask
    from grizzly.testdata.communication import GrizzlyDependencies, TestdataConsumer


class AsyncRequests(metaclass=ABCMeta):
    @abstractmethod
    def async_request_impl(self, request: RequestTask) -> GrizzlyResponse:
        message = f'{self.__class__.__name__} has not implemented async_request'
        raise NotImplementedError(message)  # pragma: no cover


class GrizzlyUserMeta(UserMeta):
    pass


class grizzlycontext:
    context: StrDict

    def __init__(self, *, context: StrDict) -> None:
        self.context = context

    def __call__(self, cls: type[GrizzlyUser]) -> type[GrizzlyUser]:
        cls.__context__ = merge_dicts(cls.__context__, self.context)

        return cls


class GrizzlyUserEvents:
    request: GrizzlyEventHook
    state: EventHook

    def __init__(self) -> None:
        self.request = GrizzlyEventHook()
        self.state = EventHook()


@grizzlycontext(
    context={
        'log_all_requests': False,
        'metadata': None,
    },
)
class GrizzlyUser(User, metaclass=GrizzlyUserMeta):
    __dependencies__: ClassVar[GrizzlyDependencies] = set()
    __scenario__: ClassVar[GrizzlyContextScenario]  # reference to grizzly scenario this user is part of
    __context__: ClassVar[StrDict] = {}

    _context: dict
    _context_root: Path
    _scenario: GrizzlyContextScenario  # copy of scenario for this user instance
    _scenario_state: ScenarioState | None

    logger: Logger

    weight: int = 1
    host: str
    abort: Event
    environment: Environment
    grizzly: GrizzlyContext
    sticky_tag: str | None = None
    variables: GrizzlyVariables
    consumer: TestdataConsumer

    events: GrizzlyUserEvents

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')

        self._context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.'))
        self._context = deepcopy(self.__class__.__context__)
        self._scenario_state = None
        self._scenario = copy(self.__scenario__)

        # these are not copied, and we can share reference
        self._scenario._tasks = self.__scenario__._tasks

        self.abort = Event()
        self.events = GrizzlyUserEvents()
        self.events.request.add_listener(ResponseHandler(self))
        self.events.request.add_listener(RequestLogger(self))
        self.events.state.add_listener(self.on_state)

        self.variables = GrizzlyVariables(**dict.fromkeys(self._scenario.variables))

        environment.events.quitting.add_listener(self.on_quitting)

        from grizzly.context import grizzly  # noqa: PLC0415

        self.grizzly = grizzly

    def failure_handler(self, exception: Exception | None, *, task: GrizzlyTask | None = None) -> None:
        # no failure, just return
        if exception is None:
            self.logger.debug('no exception to handle in failure_handler')
            return

        # failure action has already been decided, let it through
        # fugly, but needed to not get cyclic dependencies...
        from grizzly.types import FailureAction  # noqa: PLC0415

        if isinstance(exception, FailureAction.get_failure_exceptions()):
            self.logger.debug('exception is already a failure action, re-raising it')
            raise exception

        # always raise StopUser when these unhandled exceptions has occured
        if isinstance(exception, NotImplementedError | KeyError | IndexError | AttributeError | TypeError | SyntaxError):
            self.logger.debug('exception is a critical error, raising StopUser')
            raise StopUser from exception

        # check for custom actions based on failure
        task_failure_handling = task.failure_handling if task is not None and hasattr(task, 'failure_handling') else {}
        for failure_type, failure_action in chain(task_failure_handling.items(), self._scenario.failure_handling.items()):
            if failure_type is None:
                continue

            # continue test for this specific error, i.e. ignore it
            if failure_action is None:
                self.logger.debug('ignoring exception of type %r', failure_type)
                return

            if (isinstance(failure_type, str) and failure_type in repr(exception)) or exception.__class__ is failure_type:
                self.logger.debug('handling exception of type %r with action %r', failure_type, failure_action)
                raise failure_action from exception

        # no match, raise the default if it has been set
        default_exception = self._scenario.failure_handling.get(None, None)

        if default_exception is not None:
            self.logger.debug('raising default failure action %r for exception %r', default_exception, exception)
            raise default_exception from exception

        self.logger.debug('no failure action matched for exception %r, continue', exception)

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        # if it already has been called with True, do not change it back to False
        if not self.abort.is_set() and cast('bool', kwargs.get('abort', False)):
            self.abort.set()

    def on_start(self) -> None:
        super().on_start()

    def on_stop(self) -> None:
        super().on_stop()

    def on_iteration(self) -> None:
        pass

    def on_state(self, *, state: ScenarioState) -> None:
        pass

    def render(self, template: str, variables: StrDict | None = None) -> str:
        if not has_template(template):
            return template

        if variables is None:
            variables = {}

        return self._scenario.jinja2.from_string(template).render(**self.variables, **variables)

    @property
    def metadata(self) -> StrDict:
        return self._context.get('metadata', None) or {}

    @metadata.setter
    def metadata(self, value: StrDict) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        self._context['metadata'].update(value)

    @property
    def scenario_state(self) -> ScenarioState | None:
        return self._scenario_state

    @scenario_state.setter
    def scenario_state(self, value: ScenarioState) -> None:
        old_state = self._scenario_state
        if old_state != value:
            self._scenario_state = value
            message = f'scenario state={old_state} -> {value}'
            self.logger.debug(message)
            self.events.state.fire(state=value)

    def stop(self, force: bool = False) -> bool:  # noqa: FBT001, FBT002
        """Stop user.
        Make sure to stop gracefully, so that tasks that are currently executing have the chance to finish.
        """
        if not force and not self.abort.is_set():
            self.logger.debug('stop scenarios before stopping user')
            self.scenario_state = ScenarioState.STOPPING
            self._state = LOCUST_STATE_RUNNING
            return False

        return cast('bool', super().stop(force=force))

    @abstractmethod
    def request_impl(self, request: RequestTask) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented request'
        raise NotImplementedError(message)

    @final
    def request(self, request: RequestTask) -> GrizzlyResponse:
        """Perform a request and handle all the common logic that should execute before and after a user request."""
        metadata: StrDict | None = None
        payload: Any = None
        exception: Exception | None = None
        response_length = 0

        start_time = perf_counter()

        timestamp_start = datetime.now(tz=timezone.utc).isoformat()

        try:
            if len(self.metadata or {}) > 0:
                request.metadata = merge_dicts(self.metadata, request.metadata)

            request = self.render_request(request)

            request_impl = self.async_request_impl if isinstance(self, AsyncRequests) and request.async_request else self.request_impl

            metadata, payload = request_impl(request)
        except Exception as e:
            exception = e

            if isinstance(e, StopScenario):
                self.environment.events.quitting.fire(environment=self.environment, abort=True)
                self.logger.warning('scenario aborted')
            else:
                message = f'{request.method.name} request "{request.name}" failed: {str(e) or e.__class__}'
                self.logger.exception(message)
        finally:
            response_time = int((perf_counter() - start_time) * 1000)
            timestamp_finished = datetime.now(tz=timezone.utc).isoformat()
            response_length = len((payload or '').encode())

            if isinstance(exception, StopScenario):
                raise exception

            # execute response listeners, but not on these exceptions
            if not isinstance(exception, RestartScenario | StopUser | AsyncMessageError):
                try:
                    self.events.request.fire(
                        name=request.name,
                        request=request,
                        context=(
                            metadata,
                            payload,
                        ),
                        user=self,
                        exception=exception,
                    )
                except Exception as e:
                    # request exception is the priority one
                    if exception is None:
                        exception = e

            self.environment.events.request.fire(
                request_type=RequestType.from_method(request.method),
                name=request.name,
                response_time=response_time,
                response_length=response_length,
                context={
                    'user': id(self),
                    **self._context,
                    '__time__': timestamp_start,
                    '__fields_request_started__': timestamp_start,
                    '__fields_request_finished__': timestamp_finished,
                },
                exception=exception,
            )

        # ...request handled
        self.failure_handler(exception, task=request)

        return (metadata, payload)

    def render_request(self, request_template: RequestTask) -> RequestTask:
        """Create a copy of the specified request task, where all possible template values is rendered with the values from current context."""
        if request_template.__rendered__:
            return request_template

        request = copy(request_template)

        try:
            name = self.render(request_template.name)
            source: str | None = None
            request.name = f'{self._scenario.identifier} {name}'
            request.endpoint = self.render(request_template.endpoint)

            if request_template.source is not None:
                source = self.render(request_template.source)

                try:
                    file = self._context_root / 'requests' / source

                    if file.is_file():
                        source = file.read_text()

                        # nested template
                        if has_template(source):
                            source = self.render(source)
                except OSError as e:  # source was definitly not a file...
                    if e.errno != ENAMETOOLONG:
                        raise

                request.source = source

            if request_template.arguments is not None:
                request.arguments = jsonloads(self.render(jsondumps(request_template.arguments)))

            if request_template.metadata is not None:
                request.metadata = jsonloads(self.render(jsondumps(request_template.metadata)))

            request.__rendered__ = True
        except Exception as e:
            message = f'failed to render request template:\n! source:\n{request.source}\n! variables:\n{self._scenario.jinja2.globals}'
            self.logger.exception(message)
            raise StopUser from e
        else:
            return request

    def context(self) -> StrDict:
        return self._context

    def add_context(self, context: StrDict) -> None:
        for variable, value in context.get('variables', {}).items():
            self.set_variable(variable, value)

        with suppress(KeyError):
            del context['variables']

        self._context = merge_dicts(self._context, context)

    def set_variable(self, variable: str, value: Any) -> None:
        old_value = self.variables.get(variable, None)
        self.variables.update({variable: value})
        message = f'instance {variable=}, value={old_value} -> {value}'
        self.logger.debug(message)


from .blobstorage import BlobStorageUser
from .dummy import DummyUser
from .iothub import IotHubUser
from .messagequeue import MessageQueueUser
from .restapi import RestApiUser
from .servicebus import ServiceBusUser

__all__ = [
    'BlobStorageUser',
    'DummyUser',
    'IotHubUser',
    'MessageQueueUser',
    'RestApiUser',
    'ServiceBusUser',
]
