"""Base class for all Grizzly load user implementations."""
from __future__ import annotations

import logging
from abc import abstractmethod
from copy import copy, deepcopy
from json import dumps as jsondumps
from json import loads as jsonloads
from logging import Logger
from os import environ
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Set, Type, TypeVar, cast, final

from locust.user.task import LOCUST_STATE_RUNNING
from locust.user.users import User, UserMeta

from grizzly.context import GrizzlyContext
from grizzly.events import GrizzlyEventHook, RequestLogger, ResponseHandler
from grizzly.exceptions import RestartScenario
from grizzly.types import GrizzlyResponse, RequestType, ScenarioState
from grizzly.types.locust import Environment, StopUser
from grizzly.users.base import AsyncRequests
from grizzly.utils import merge_dicts

from . import FileRequests

T = TypeVar('T', bound=UserMeta)

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario
    from grizzly.tasks import RequestTask


class GrizzlyUserMeta(UserMeta):
    pass


class grizzlycontext:
    context: Dict[str, Any]

    def __init__(self, *, context: Dict[str, Any]) -> None:
        self.context = context

    def __call__(self, cls: Type[GrizzlyUser]) -> Type[GrizzlyUser]:
        cls.__context__ = merge_dicts(cls.__context__, self.context)

        return cls


@grizzlycontext(context={'log_all_requests': False, 'variables': {}})
class GrizzlyUser(User, metaclass=GrizzlyUserMeta):
    __dependencies__: ClassVar[Set[str]] = set()
    __scenario__: GrizzlyContextScenario  # reference to grizzly scenario this user is part of
    __context__: ClassVar[Dict[str, Any]] = {}

    _context: Dict[str, Any]
    _context_root: Path
    _scenario: GrizzlyContextScenario  # copy of scenario for this user instance
    _scenario_state: Optional[ScenarioState]

    logger: Logger

    weight: int = 1
    host: str
    abort: bool
    environment: Environment
    event_hook: GrizzlyEventHook
    grizzly = GrizzlyContext()

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self._context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.'))
        self._context = deepcopy(self.__class__.__context__)
        self._scenario_state = None
        self._scenario = copy(self.__scenario__)
        # these are not copied, and we can share reference
        self._scenario._tasks = self.__scenario__._tasks

        self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')
        self.abort = False
        self.event_hook = GrizzlyEventHook()
        self.event_hook.add_listener(ResponseHandler(self))
        self.event_hook.add_listener(RequestLogger(self))

        environment.events.quitting.add_listener(self.on_quitting)

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        # if it already has been called with True, do not change it back to False
        if not self.abort:
            self.abort = cast(bool, kwargs.get('abort', False))

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

    def stop(self, force: bool = False) -> bool:  # noqa: FBT001, FBT002
        """Stop user.
        Make sure to stop gracefully, so that tasks that are currently executing have the chance to finish.
        """
        if not force and not self.abort:
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
        metadata: Optional[Dict[str, Any]] = None
        payload: Optional[Any] = None
        exception: Optional[Exception] = None
        response_length = 0

        start_time = perf_counter()

        try:
            request = self.render(request)

            request_impl = self.async_request_impl if isinstance(self, AsyncRequests) and request.async_request else self.request_impl

            metadata, payload = request_impl(request)
        except Exception as e:
            message = f'request failed: {str(e) or e.__class__}'
            self.logger.exception(message)
            exception = e
        finally:
            total_time = int((perf_counter() - start_time) * 1000)
            response_length = len((payload or '').encode())

            # execute response listeners
            if not isinstance(exception, (RestartScenario, StopUser)):
                try:
                    self.event_hook.fire(
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

    def render(self, request_template: RequestTask) -> RequestTask:
        """Create a copy of the specified request task, where all possible template values is rendered with the values from current context."""
        if request_template.__rendered__:
            return request_template

        request = copy(request_template)

        try:
            j2env = self.grizzly.state.jinja2
            source: Optional[str] = None
            name = j2env.from_string(request_template.name).render(**self.context_variables)
            request.name = f'{self._scenario.identifier} {name}'
            request.endpoint = j2env.from_string(request_template.endpoint).render(**self.context_variables)

            if request_template.template is not None:
                source = request_template.template.render(**self.context_variables)

                file = self._context_root / 'requests' / source

                if file.is_file():
                    if not isinstance(self, FileRequests):
                        source = file.read_text()

                        # nested template
                        if '{{' in source and '}}' in source:
                            source = j2env.from_string(source).render(**self.context_variables)
                    else:
                        file_name = file.name
                        if not request.endpoint.endswith(file_name):
                            request.endpoint = f'{request.endpoint}/{file_name}'

                request.source = source

            if request_template.arguments is not None:
                arguments_json = jsondumps(request_template.arguments)
                rendered_json = j2env.from_string(arguments_json).render(**self.context_variables)
                request.arguments = jsonloads(rendered_json)

            if request_template.metadata is not None:
                metadata_json = jsondumps(request_template.metadata)
                rendered_metadata = j2env.from_string(metadata_json).render(**self.context_variables)
                request.metadata = jsonloads(rendered_metadata)

            request.__rendered__ = True
        except Exception as e:
            message = 'failed to render request template'
            self.logger.exception(message)
            raise StopUser from e
        else:
            return request

    def context(self) -> Dict[str, Any]:
        return self._context

    def add_context(self, context: Dict[str, Any]) -> None:
        self._context = merge_dicts(self._context, context)

    def set_context_variable(self, variable: str, value: Any) -> None:
        old_value = self._context['variables'].get(variable, None)
        self._context['variables'][variable] = value
        message = f'context {variable=}, value={old_value} -> {value}'
        self.logger.debug(message)

    @property
    def context_variables(self) -> Dict[str, Any]:
        return cast(Dict[str, Any], self._context.get('variables', {}))
