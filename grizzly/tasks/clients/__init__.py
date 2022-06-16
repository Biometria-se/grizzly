'''
@anchor pydoc:grizzly.tasks.clients Clients
Client tasks is functionality that is executed by locust and is registred to an URL scheme.
These tasks is used to make a request to another host than the scenario is actually load testing.

## Statistics

Executions of all client tasks will be visible with request type `CLNT`.

## Arguments

* `endpoint` _str_ - describes the request

If `endpoint` is a template variable which includes the scheme, the scheme for the request must be specified so the
correct `grizzly.tasks.client` implementation is used. The additional scheme will be removed when the request is
performed.
'''
from abc import abstractmethod
from typing import Dict, Generator, Type, List, Any, Optional, Callable
from contextlib import contextmanager
from time import perf_counter as time
from urllib.parse import urlparse

from ...context import GrizzlyContext, GrizzlyContextScenario
from ...scenarios import GrizzlyScenario
from ...types import RequestType, RequestDirection
from .. import GrizzlyTask, template


# see https://github.com/python/mypy/issues/5374
@template('endpoint', 'destination', 'source', 'name')  # type: ignore
class ClientTask(GrizzlyTask):
    _schemes: List[str]
    _short_name: str
    _direction_arrow: Dict[RequestDirection, str] = {
        RequestDirection.FROM: '<-',
        RequestDirection.TO: '->',
    }

    grizzly: GrizzlyContext
    direction: RequestDirection
    endpoint: str
    name: Optional[str]
    variable: Optional[str]
    source: Optional[str]
    destination: Optional[str]

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        super().__init__(scenario)

        parsed = urlparse(endpoint)

        if parsed.scheme not in self._schemes:
            raise AttributeError(f'{self.__class__.__name__}: "{parsed.scheme}" is not supported, must be one of {", ".join(self._schemes)}')

        if '{{' in endpoint and '}}' in endpoint:
            index = len(parsed.scheme) + 3
            endpoint = endpoint[index:]

        self.direction = direction
        self.endpoint = endpoint
        self.name = name
        self.variable = variable
        self.source = source
        self.destination = destination

        if self.variable is not None and self.direction != RequestDirection.FROM:
            raise AttributeError(f'{self.__class__.__name__}: variable argument is not applicable for direction {self.direction.name}')

        if self.source is not None and self.direction != RequestDirection.TO:
            raise AttributeError(f'{self.__class__.__name__}: source argument is not applicable for direction {self.direction.name}')

        self.grizzly = GrizzlyContext()

        if self.variable is not None and self.variable not in self.grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: variable {self.variable} has not been initialized')

        if self.source is None and self.direction == RequestDirection.TO:
            raise ValueError(f'{self.__class__.__name__}: source must be set for direction {self.direction.name}')

        self._short_name = self.__class__.__name__.replace('ClientTask', '')

    def __call__(self) -> Callable[[GrizzlyScenario], Any]:
        if self.direction == RequestDirection.FROM:
            return self.get
        else:
            return self.put

    @abstractmethod
    def get(self, parent: GrizzlyScenario) -> Any:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented GET')

    @abstractmethod
    def put(self, parent: GrizzlyScenario) -> Any:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented PUT')

    @contextmanager
    def action(self, parent: GrizzlyScenario, action: Optional[str] = None, supress: bool = False) -> Generator[Dict[str, Any], None, None]:
        exception: Optional[Exception] = None
        response_length = 0
        start_time = time()
        meta: Dict[str, Any] = {}

        try:
            # get metadata back from actual implementation
            yield meta
        except Exception as e:
            exception = e
        finally:
            if self.name is None:
                action = action or meta.get('action', self.variable)
                name = f'{parent.user._scenario.identifier} {self._short_name}{meta.get("direction", self._direction_arrow[self.direction])}{action}'
            else:
                rendered_name = parent.render(self.name)
                name = f'{parent.user._scenario.identifier} {rendered_name}'

            response_time = int((time() - start_time) * 1000)
            response_length = meta.get('response_length', None) or 0

            if not supress or exception is not None:
                parent.user.environment.events.request.fire(
                    request_type=RequestType.CLIENT_TASK(),
                    name=name,
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

        if exception is not None and parent.user._scenario.failure_exception is not None:
            raise parent.user._scenario.failure_exception()


class client:
    available: Dict[str, Type[ClientTask]] = {}
    schemes: List[str]

    def __init__(self, scheme: str, *additional_schemes: str) -> None:
        schemes = [scheme]
        if len(additional_schemes) > 0:
            schemes += list(additional_schemes)
        self.schemes = schemes

    def __call__(self, impl: Type[ClientTask]) -> Type[ClientTask]:
        available = {scheme: impl for scheme in self.schemes}
        impl._schemes = self.schemes
        client.available.update(available)

        return impl


from .http import HttpClientTask
from .blobstorage import BlobStorageClientTask
from .messagequeue import MessageQueueClientTask


__all__ = [
    'HttpClientTask',
    'BlobStorageClientTask',
    'MessageQueueClientTask',
]
