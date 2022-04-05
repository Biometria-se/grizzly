from abc import abstractmethod
from typing import Dict, Generator, Type, List, Any, Optional, Callable
from contextlib import contextmanager
from time import perf_counter as time

from ...context import GrizzlyContext
from ...scenarios import GrizzlyScenario
from ...types import GrizzlyTask, RequestDirection


class ClientTask(GrizzlyTask):
    grizzly: GrizzlyContext
    direction: RequestDirection
    endpoint: str
    variable: Optional[str]
    source: Optional[str]
    destination: Optional[str]

    def __init__(
        self, direction: RequestDirection, endpoint: str, /, variable: Optional[str] = None, source: Optional[str] = None, destination: Optional[str] = None,
    ) -> None:
        self.direction = direction
        self.endpoint = endpoint
        self.variable = variable
        self.source = source
        self.destination = destination

        self.grizzly = GrizzlyContext()

        if self.variable is not None and self.direction != RequestDirection.FROM:
            raise AttributeError(f'{self.__class__.__name__}: variable argument is not applicable for direction {self.direction.name}')

        if self.source is not None and self.direction != RequestDirection.TO:
            raise AttributeError(f'{self.__class__.__name__}: source argument is not applicable for direction {self.direction.name}')

        if self.variable is not None and self.variable not in self.grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: variable {self.variable} has not been initialized')

    def implementation(self) -> Callable[['GrizzlyScenario'], Any]:
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
    def action(self, parent: GrizzlyScenario) -> Generator[Dict[str, Any], None, None]:
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
            response_time = int((time() - start_time) * 1000)
            response_length = meta.get('response_length', None) or 0
            parent.user.environment.events.request.fire(
                request_type='TASK',
                name=f'{parent.user._scenario.identifier} {self.__class__.__name__}->{self.variable}',
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
        client.available.update(available)

        return impl


from .http import HttpClientTask


__all__ = [
    'HttpClientTask',
]
