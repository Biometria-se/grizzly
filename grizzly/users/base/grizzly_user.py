import logging

from os import environ, path
from typing import TYPE_CHECKING, Any, Dict, Tuple, Optional, Set, cast
from logging import Logger
from abc import abstractmethod
from json import dumps as jsondumps, loads as jsonloads
from dataclasses import replace as dataclass_copy

from locust.user.users import User
from locust.user.task import LOCUST_STATE_RUNNING

from grizzly.types import GrizzlyResponse, RequestType, ScenarioState
from grizzly.types.locust import Environment, StopUser
from grizzly.tasks import RequestTask
from grizzly.utils import merge_dicts
from grizzly.context import GrizzlyContext

from . import FileRequests


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario
    from grizzly.scenarios import GrizzlyScenario


class GrizzlyUser(User):
    __dependencies__: Set[str] = set()
    __scenario__: 'GrizzlyContextScenario'  # reference to grizzly scenario this user is part of

    _context_root: str
    _context: Dict[str, Any] = {
        'variables': {},
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
        self._scenario = dataclass_copy(self.__scenario__)
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
    def request(self, parent: 'GrizzlyScenario', request: RequestTask) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented request')  # pragma: no cover

    def render(self, request: RequestTask) -> Tuple[str, str, Optional[str], Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        scenario_name = f'{self._scenario.identifier} {request.name}'

        try:
            j2env = self.grizzly.state.jinja2
            payload: Optional[str] = None
            name = j2env.from_string(request.name).render(**self.context_variables)
            scenario_name = f'{self._scenario.identifier} {name}'
            endpoint = j2env.from_string(request.endpoint).render(**self.context_variables)
            arguments: Optional[Dict[str, str]] = None
            metadata: Optional[Dict[str, str]] = None

            if request.template is not None:
                payload = request.template.render(**self.context_variables)

                file = path.join(self._context_root, 'requests', payload)

                if path.isfile(file):
                    if not isinstance(self, FileRequests):
                        with open(file, 'r') as fd:
                            payload = fd.read()

                        # nested template
                        if '{{' in payload and '}}' in payload:
                            payload = j2env.from_string(payload).render(**self.context_variables)
                    else:
                        file_name = path.basename(payload)
                        if not endpoint.endswith(file_name):
                            endpoint = f'{endpoint}/{file_name}'

            if request.arguments is not None:
                arguments_json = jsondumps(request.arguments)
                rendered_json = j2env.from_string(arguments_json).render(**self.context_variables)
                arguments = jsonloads(rendered_json)

            if request.metadata is not None:
                metadata_json = jsondumps(request.metadata)
                rendered_metadata = j2env.from_string(metadata_json).render(**self.context_variables)
                metadata = jsonloads(rendered_metadata)

            return name, endpoint, payload, arguments, metadata
        except Exception as exception:
            self.logger.error(f'{exception=}, {request.name=}, {request.endpoint=}, {self.context_variables=}, {request.arguments=}, {request.metadata=}', exc_info=True)
            self.environment.events.request.fire(
                request_type=RequestType.from_method(request.method),
                name=scenario_name,
                response_time=0,
                response_length=0,
                context=self._context,
                exception=exception,
            )
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
