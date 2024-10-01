"""@anchor pydoc:grizzly.users Load User
This package contains implementation for different type of endpoints and protocols.

These implementations are the basis for how to communicate with the system under test.

## Custom

It is possible to implement custom users, the only requirement is that they inherit `grizzly.users.GrizzlyUser`. To get them to be executed by `grizzly`
the full namespace has to be specified as `user_class_name` in the scenarios {@pylink grizzly.steps.scenario.user} step.

There are examples of this in the {@link framework.example}.
"""
from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from contextlib import suppress
from copy import copy, deepcopy
from errno import ENAMETOOLONG
from json import dumps as jsondumps
from json import loads as jsonloads
from logging import Logger
from os import environ
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar, cast, final

from gevent.event import Event
from locust.event import EventHook
from locust.user.task import LOCUST_STATE_RUNNING
from locust.user.users import User, UserMeta

from grizzly.context import GrizzlyContext
from grizzly.events import GrizzlyEventHook, RequestLogger, ResponseHandler
from grizzly.exceptions import RestartScenario, StopScenario
from grizzly.testdata import GrizzlyVariables
from grizzly.types import GrizzlyResponse, RequestType, ScenarioState
from grizzly.types.locust import Environment, StopUser
from grizzly.utils import has_template, merge_dicts
from grizzly_extras.async_message import AsyncMessageError

T = TypeVar('T', bound=UserMeta)

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario
    from grizzly.tasks import RequestTask
    from grizzly.testdata.communication import TestdataConsumer

class AsyncRequests(metaclass=ABCMeta):
    @abstractmethod
    def async_request_impl(self, request: RequestTask) -> GrizzlyResponse:
        message = f'{self.__class__.__name__} has not implemented async_request'
        raise NotImplementedError(message)  # pragma: no cover


class GrizzlyUserMeta(UserMeta):
    pass


class grizzlycontext:
    context: dict[str, Any]

    def __init__(self, *, context: dict[str, Any]) -> None:
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


@grizzlycontext(context={
    'log_all_requests': False,
    'metadata': None,
})
class GrizzlyUser(User, metaclass=GrizzlyUserMeta):
    __dependencies__: ClassVar[set[str]] = set()
    __scenario__: GrizzlyContextScenario  # reference to grizzly scenario this user is part of
    __context__: ClassVar[dict[str, Any]] = {}

    _context: dict[str, Any]
    _context_root: Path
    _scenario: GrizzlyContextScenario  # copy of scenario for this user instance
    _scenario_state: Optional[ScenarioState]

    logger: Logger

    weight: int = 1
    host: str
    abort: Event
    environment: Environment
    grizzly = GrizzlyContext()
    sticky_tag: Optional[str] = None
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

        self.variables = GrizzlyVariables(**{key: None for key in self._scenario.variables})

        environment.events.quitting.add_listener(self.on_quitting)

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        # if it already has been called with True, do not change it back to False
        if not self.abort.is_set() and cast(bool, kwargs.get('abort', False)):
            self.abort.set()

    def on_start(self) -> None:
        super().on_start()

    def on_stop(self) -> None:
        super().on_stop()

    def on_state(self, *, state: ScenarioState) -> None:
        pass

    def render(self, template: str, variables: Optional[dict[str, Any]] = None) -> str:
        if not has_template(template):
            return template

        if variables is None:
            variables = {}

        return self._scenario.jinja2.from_string(template).render(**self.variables, **variables)

    @property
    def metadata(self) -> dict[str, Any]:
        return self._context.get('metadata', None) or {}

    @metadata.setter
    def metadata(self, value: dict[str, Any]) -> None:
        if self._context.get('metadata', None) is None:
            self._context['metadata'] = {}

        self._context['metadata'].update(value)

    @property
    def scenario_state(self) -> Optional[ScenarioState]:
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

        return cast(bool, super().stop(force=force))

    @abstractmethod
    def request_impl(self, request: RequestTask) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented request'
        raise NotImplementedError(message)

    @final
    def request(self, request: RequestTask) -> GrizzlyResponse:
        """Perform a request and handle all the common logic that should execute before and after a user request."""
        metadata: Optional[dict[str, Any]] = None
        payload: Optional[Any] = None
        exception: Optional[Exception] = None
        response_length = 0

        start_time = perf_counter()

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
                message = f'request "{request.name}" failed: {str(e) or e.__class__}'
                self.logger.exception(message)
        finally:
            total_time = int((perf_counter() - start_time) * 1000)
            response_length = len((payload or '').encode())

            if isinstance(exception, StopScenario):
                raise exception

            # execute response listeners, but not on these exceptions
            if not isinstance(exception, (RestartScenario, StopUser, AsyncMessageError)):
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
                response_time=total_time,
                response_length=response_length,
                context=self._context,
                exception=exception,
            )

        # ...request handled
        if exception is not None:
            if isinstance(exception, (NotImplementedError, KeyError, IndexError, AttributeError, TypeError, SyntaxError)):
                raise StopUser

            if self._scenario.failure_exception is not None:
                raise self._scenario.failure_exception

        return (metadata, payload)

    def render_request(self, request_template: RequestTask) -> RequestTask:
        """Create a copy of the specified request task, where all possible template values is rendered with the values from current context."""
        if request_template.__rendered__:
            return request_template

        request = copy(request_template)

        try:
            name = self.render(request_template.name)
            source: Optional[str] = None
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

    def context(self) -> dict[str, Any]:
        return self._context

    def add_context(self, context: dict[str, Any]) -> None:
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
    'RestApiUser',
    'MessageQueueUser',
    'ServiceBusUser',
    'BlobStorageUser',
    'IotHubUser',
    'DummyUser',
]
