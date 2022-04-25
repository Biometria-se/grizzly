'''This task runs all requests in the group in parallell.

Arguments:

* `name` (str): name of the group of requests running in parallell

Instances of this task is created with step expressions:

* [`step_task_parallell_start`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_parallell_start)

* [`step_task_parallell_add_request`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_parallell_add_request)

* [`step_task_parallell_end`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_parallell_end)
'''
from typing import TYPE_CHECKING, Any, Callable, List, Optional
from time import perf_counter as time

import gevent

from . import GrizzlyTask
from .request import RequestTask

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


class ParallellRequestTask(GrizzlyTask):
    requests: List[RequestTask]

    def __init__(self, name: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.name = name
        self.requests = []

    def add(self, request: RequestTask) -> None:
        self.requests.append(request)

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            exception: Optional[Exception] = None
            response_length = 0

            start = time()
            try:
                group = gevent.pool.Group()

                for _, payload in group.imap_unordered(parent.user.request, self.requests):
                    response_length += len(payload)
            except Exception as e:
                exception = e
            finally:
                response_time = int((time() - start) * 1000)

                parent.user.environment.events.request.fire(
                    request_type='PRLL',
                    name=f'{self.scenario.identifier} {self.name} ({len(self.requests)})',
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

        return task
