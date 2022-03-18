from abc import abstractmethod
from os import environ, path
from typing import Any, Dict, Tuple, Optional, Set, cast

from jinja2 import Template
from locust.exception import StopUser
from locust.user.users import User
from locust.env import Environment

from grizzly.context import GrizzlyContextScenario

from ...types import GrizzlyResponse
from ...tasks import RequestTask
from ...utils import merge_dicts
from . import logger, FileRequests


class GrizzlyUser(User):
    _context_root: str
    _context: Dict[str, Any] = {
        'variables': {},
    }
    _scenario: GrizzlyContextScenario

    __dependencies__: Set[str] = set()

    request_name_map: Dict[str, str] = {
        'RECEIVE': 'RECV',
        'HELLO': 'HELO',
    }

    weight: int = 1

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self._context_root = environ.get('GRIZZLY_CONTEXT_ROOT', '.')
        self._context = merge_dicts({}, GrizzlyUser._context)

    @abstractmethod
    def request(self, request: RequestTask) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented request(RequestTask)')

    def get_request_method(self, request: RequestTask) -> str:
        return self.request_name_map.get(request.method.name, request.method.name[:4])

    def render(self, request: RequestTask) -> Tuple[str, str, Optional[str]]:
        scenario_name = f'{request.scenario.identifier} {request.name}'

        try:
            payload: Optional[str] = None
            name = Template(request.name).render(**self.context_variables)
            scenario_name = f'{request.scenario.identifier} {name}'
            endpoint = Template(request.endpoint).render(**self.context_variables)

            if request.template is not None:
                payload = request.template.render(**self.context_variables)

                file = path.join(self._context_root, 'requests', payload)

                if path.isfile(file):
                    if not isinstance(self, FileRequests):
                        with open(file, 'r') as fd:
                            payload = fd.read()

                        # nested template
                        if '{{' in payload and '}}' in payload:
                            payload = Template(payload).render(**self.context_variables)
                    else:
                        file_name = path.basename(payload)
                        if not endpoint.endswith(file_name):
                            endpoint = f'{endpoint}/{file_name}'

            return name, endpoint, payload
        except Exception as exception:
            logger.error(f'{exception=}, {request.name=}, {request.endpoint=}, {self.context_variables=}', exc_info=True)
            self.environment.events.request.fire(
                request_type=request.method.name,
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
        old_value = cast(Dict[str, Any], self._context['variables'])[variable] if variable in cast(Dict[str, Any], self._context['variables']) else None
        self._context['variables'][variable] = value
        logger.debug(f'context: {variable=}, value: {old_value} -> {value}')

    @property
    def context_variables(self) -> Dict[str, Any]:
        return cast(Dict[str, Any], self._context.get('variables', {}))
