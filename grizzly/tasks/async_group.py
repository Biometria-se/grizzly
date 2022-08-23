"""
@anchor pydoc:grizzly.tasks.async_group Async Group
This task runs all requests in the group asynchronously.

The name of requests added to the group will be prefixed with async group `<name>:`

Enable `gevent` debugging for this task by running with argument `--verbose` and setting environment variable `GEVENT_MONITOR_THREAD_ENABLE`.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_async_group_start}

* {@pylink grizzly.steps.scenario.tasks.step_task_async_group_close}

Requests are added to the group with the same step implementations as {@pylink grizzly.tasks.request} task.

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `ASYNC`. `name` will be suffixed with ` (<n>)`,
where `<n>` is the number of requests in the group. Each request in the group will have its own entry in the statistics as an ordinary
{@pylink grizzly.tasks.request} task.

## Arguments

* `name` (str): name of the group of asynchronously requests
"""
import logging
import inspect

from os import environ

import gevent

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple
from time import perf_counter as time_perf_counter

from . import GrizzlyTask, GrizzlyTaskWrapper, RequestTask, template
from ..users.base import AsyncRequests
from ..types import RequestType

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


@template('name', 'tasks')
class AsyncRequestGroupTask(GrizzlyTaskWrapper):
    tasks: List[GrizzlyTask]

    def __init__(self, name: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.name = name
        self.tasks = []

    def add(self, task: GrizzlyTask) -> None:
        if not isinstance(task, RequestTask):
            raise ValueError(f'{self.__class__.__name__} only accepts RequestTask tasks, not {task.__class__.__name__}')

        task.name = f'{self.name}:{task.name}'
        self.tasks.append(task)

    def peek(self) -> List[GrizzlyTask]:
        return self.tasks

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            if not isinstance(parent.user, AsyncRequests):
                raise NotImplementedError(f'{parent.user.__class__.__name__} does not inherit AsyncRequests')

            exception: Optional[Exception] = None
            response_length = 0

            def trace_green(event: str, args: Tuple[gevent.Greenlet, gevent.Greenlet]) -> None:  # pragma: no cover
                src, target = args

                if src is gevent.hub.get_hub():
                    return

                if event == 'switch':
                    parent.user.logger.debug(f'from {src} switch to {target}')
                elif event == 'throw':
                    parent.user.logger.debug(f'from {src} throw exception to {target}')

                if src.gr_frame:
                    tracebacks = inspect.getouterframes(src.gr_frame)
                    buff = []
                    for traceback in tracebacks:
                        srcfile, lineno, func_name, codesample = traceback[1:-1]
                        trace_line = f'''File "{srcfile}", line {lineno}, in {func_name}\n{"".join(codesample or [])} '''
                        buff.append(trace_line)

                    parent.user.logger.debug(''.join(buff))

            greenlets: List[gevent.Greenlet] = []
            start = time_perf_counter()

            try:
                debug_enabled = (
                    parent.user.logger.isEnabledFor(logging.DEBUG)
                    and environ.get('GEVENT_MONITOR_THREAD_ENABLE', None) is not None
                )

                for request in self.tasks:
                    greenlet = gevent.spawn(parent.user.async_request, request)
                    if debug_enabled:
                        greenlet.settrace(trace_green)
                    greenlets.append(greenlet)

                gevent.joinall(greenlets)

                for greenlet in greenlets:
                    try:
                        _, payload = greenlet.get()
                        response_length += len(payload) if payload is not None else 0
                    except Exception as e:
                        parent.user.logger.error(str(e), exc_info=True)
                        if exception is None:
                            exception = e
            except Exception as e:
                if exception is None:
                    exception = e
            finally:
                greenlets = []
                response_time = int((time_perf_counter() - start) * 1000)

                parent.user.environment.events.request.fire(
                    request_type=RequestType.ASYNC_GROUP(),
                    name=f'{self.scenario.identifier} {self.name} ({len(self.tasks)})',
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                if exception is not None and self.scenario.failure_exception is not None:
                    raise self.scenario.failure_exception()

        return task
