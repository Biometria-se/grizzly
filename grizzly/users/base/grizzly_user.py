import logging

from os import environ, path
from typing import TYPE_CHECKING, Any, Dict, Tuple, Optional, Set, cast, final
from logging import Logger
from abc import abstractmethod
from json import dumps as jsondumps, loads as jsonloads
from copy import copy
from time import perf_counter

from locust.user.task import LOCUST_STATE_RUNNING

from grizzly.types import GrizzlyResponse, RequestType, ScenarioState
from grizzly.types.locust import Environment, StopUser
from grizzly.tasks import RequestTask
from grizzly.utils import merge_dicts
from grizzly.context import GrizzlyContext
from grizzly.users.base import RequestLogger, AsyncRequests
from grizzly.exceptions import ResponseHandlerError, TransformerLocustError, RestartScenario

from . import FileRequests


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario


class GrizzlyUser(RequestLogger):
    __dependencies__: Set[str] = set()
    __scenario__: 'GrizzlyContextScenario'  # reference to grizzly scenario this user is part of

    _context_root: str
    _context: Dict[str, Any] = {
        'variables': {},
        'log_all_requests': False,
    }
    _scenario: 'GrizzlyContextScenario'  # copy of scenario for this user instance

    _scenario_state: Optional[ScenarioState]

    logger: Logger

    weight: int = 1
    host: str
    abort: bool
    environment: Environment
    grizzly = GrizzlyContext()

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self._context_root = environ.get('GRIZZLY_CONTEXT_ROOT', '.')
        self._context = merge_dicts({}, GrizzlyUser._context)
        self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')
        self._scenario_state = None
        self.abort = False
        self._scenario = copy(self.__scenario__)
        # these are not copied, and we can share reference
        self._scenario._tasks = self.__scenario__._tasks

        environment.events.quitting.add_listener(self.on_quitting)

        assert self.host is not None, f'{self.__class__.__name__} must have host set'

    def on_quitting(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
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
            self.logger.debug(f'scenario state={old_state} -> {value}')

    def stop(self, force: bool = False) -> bool:
        if not force and not self.abort:
            self.logger.debug('stop scenarios before stopping user')
            self.scenario_state = ScenarioState.STOPPING
            self._state = LOCUST_STATE_RUNNING
            return False
        else:
            return cast(bool, super().stop(force=force))

    @abstractmethod
    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented request')  # pragma: no cover

    @final
    def request(self, request: RequestTask) -> GrizzlyResponse:
        metadata: Optional[Dict[str, Any]] = None
        payload: Optional[Any] = None
        exception: Optional[Exception] = None
        response_length = 0

        start_time = perf_counter()

        try:
            request = self.render(request)

            if isinstance(self, AsyncRequests) and request.async_request:
                request_impl = self.async_request_impl  # pylint: disable=no-member
            else:
                request_impl = self.request_impl

            metadata, payload = request_impl(request)
        except Exception as e:
            self.logger.error(f'request failed: {str(e) or e.__class__}', exc_info=self.grizzly.state.verbose)
            exception = e
        finally:
            total_time = int((perf_counter() - start_time) * 1000)
            response_length = len((payload or '').encode())

            # execute response listeners
            if not isinstance(exception, (RestartScenario, StopUser,)):
                try:
                    self.response_event.fire(
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
                    if exception is None:
                        exception = e

            self.environment.events.request.fire(
                request_type=RequestType.from_method(request.method),
                name=request.name,
                response_time=total_time,
                response_length=response_length,
                context=self._context,
                exception=exception
            )

        # ...request handled
        if exception is not None:
            if (
                isinstance(exception, (NotImplementedError, StopUser, KeyError, IndexError, AttributeError,))
                and not isinstance(exception, (ResponseHandlerError, TransformerLocustError,))  # grizzly exceptions that inherits StopUser
            ):
                raise StopUser()
            elif self._scenario.failure_exception is not None:
                raise self._scenario.failure_exception()

        return (metadata, payload,)

    def render(self, request_template: RequestTask) -> RequestTask:
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

                file = path.join(self._context_root, 'requests', source)

                if path.isfile(file):
                    if not isinstance(self, FileRequests):
                        with open(file, 'r') as fd:
                            source = fd.read()

                        # nested template
                        if '{{' in source and '}}' in source:
                            source = j2env.from_string(source).render(**self.context_variables)
                    else:
                        file_name = path.basename(source)
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

            return request
        except:
            self.logger.error('failed to render request template', exc_info=True)
            raise StopUser()

    def context(self) -> Dict[str, Any]:
        return self._context

    def add_context(self, context: Dict[str, Any]) -> None:
        self._context = merge_dicts(self._context, context)

    def set_context_variable(self, variable: str, value: Any) -> None:
        old_value = self._context['variables'].get(variable, None)
        self._context['variables'][variable] = value
        self.logger.debug(f'context {variable=}, value={old_value} -> {value}')

    @property
    def context_variables(self) -> Dict[str, Any]:
        return cast(Dict[str, Any], self._context.get('variables', {}))
