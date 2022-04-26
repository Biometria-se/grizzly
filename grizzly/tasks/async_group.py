'''This task runs all requests in the group asynchronously.

Arguments:

* `name` (str): name of the group of asynchronously requests

Instances of this task is created with step expressions:

* [`step_task_async_group_start`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_async_group_start)

* [`step_task_async_group_close`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_async_group_close)

Requests are added to the group with the same step expressions as [`RequestTask`](/grizzly/framework/usage/tasks/request/).
'''
import logging
import gevent

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple
from time import perf_counter as time

from . import GrizzlyTask, template
from .request import RequestTask

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


@template('name')
class AsyncRequestGroupTask(GrizzlyTask):
    requests: List[RequestTask]

    def __init__(self, name: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.name = name
        self.requests = []

    def add(self, request: RequestTask) -> None:
        self.requests.append(request)

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        import inspect

        def task(parent: 'GrizzlyScenario') -> Any:
            exception: Optional[Exception] = None
            response_length = 0

            def trace_green(event: str, args: Tuple[Any, Any]) -> None:
                src, target = args
                if event == "switch":
                    parent.user.logger.debug("from %s switch to %s" % (src, target))
                elif event == "throw":
                    parent.user.logger.debug("from %s throw exception to %s" % (src, target))

                if src.gr_frame:
                    tracebacks = inspect.getouterframes(src.gr_frame)
                    buff = []
                    for traceback in tracebacks:
                        srcfile, lineno, func_name, codesample = traceback[1:-1]
                        trace_line = '''File "%s", line %d, in %s\n%s '''
                        buff.append(trace_line %
                                    (srcfile, lineno, func_name, "".join(codesample or [])))

                    parent.user.logger.debug("".join(buff))

            start = time()
            try:
                debug_enabled = parent.user.logger.isEnabledFor(logging.DEBUG)
                greenlets: List[gevent.Greenlet] = []
                for request in self.requests:
                    greenlet = gevent.spawn(parent.user.request, request)
                    if debug_enabled:
                        greenlet.settrace(trace_green)
                    greenlets.append(greenlet)

                gevent.joinall(greenlets)

                for greenlet in greenlets:
                    _, payload = greenlet.value
                    response_length += len(payload) if payload is not None else 0
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
