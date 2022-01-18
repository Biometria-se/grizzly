from typing import Dict, Generator, Type, List, Any, Optional
from contextlib import contextmanager
from time import perf_counter as time

from locust.exception import StopUser

from ...context import GrizzlyContext, GrizzlyScenarioBase


class GetterOfTask(GrizzlyScenarioBase):
    endpoint: str
    variable: str

    def __init__(self, endpoint: str, variable: str) -> None:
        self.variable = variable
        self.endpoint = endpoint

        grizzly = GrizzlyContext()

        if self.variable not in grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: variable {self.variable} has not been initialized')

    @contextmanager
    def get(self, parent: GrizzlyScenarioBase) -> Generator[Dict[str, Any], None, None]:
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

class getterof:
    available: Dict[str, Type[GetterOfTask]] = {}
    schemes: List[str]

    def __init__(self, scheme: str, *additional_schemes: str) -> None:
        schemes = [scheme]
        if len(additional_schemes) > 0:
            schemes += list(additional_schemes)
        self.schemes = schemes

    def __call__(self, impl: Type[GetterOfTask]) -> Type[GetterOfTask]:
        available = {scheme: impl for scheme in self.schemes}
        getterof.available.update(available)

        return impl

from .http import HttpGetTask

__all__ = [
    'HttpGetTask',
]
