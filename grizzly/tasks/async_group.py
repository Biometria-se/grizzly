"""Task runs all requests in the group asynchronously.

The name of requests added to the group will be prefixed with async group `<name>:`

Enable `gevent` debugging for this task by running with argument `--verbose` and setting environment variable `GEVENT_MONITOR_THREAD_ENABLE`.

## Step implementations

* [Open][grizzly.steps.scenario.tasks.async_group.step_task_async_group_open]

* [Close][grizzly.steps.scenario.tasks.async_group.step_task_async_group_close]

Requests are added to the group with the same step implementations as [Request][grizzly.tasks.request] task.

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `ASYNC`. `name` will be suffixed with ` (<n>)`,
where `<n>` is the number of requests in the group. Each request in the group will have its own entry in the statistics as an ordinary
[Request][grizzly.tasks.request] task.

"""

from __future__ import annotations

import inspect
import logging
from os import environ
from time import perf_counter as time_perf_counter
from typing import TYPE_CHECKING, Any

import gevent

from grizzly.types import RequestType
from grizzly.users import AsyncRequests

from . import GrizzlyTask, GrizzlyTaskWrapper, RequestTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('name', 'tasks')
class AsyncRequestGroupTask(GrizzlyTaskWrapper):
    tasks: list[GrizzlyTask]

    def __init__(self, name: str) -> None:
        super().__init__(timeout=None)

        self.name = name
        self.tasks = []

    def add(self, task: GrizzlyTask) -> None:
        if not isinstance(task, RequestTask):
            message = f'{self.__class__.__name__} only accepts RequestTask tasks, not {task.__class__.__name__}'
            raise TypeError(message)

        task.name = f'{self.name}:{task.name}'
        task.async_request = True
        self.tasks.append(task)

    def peek(self) -> list[GrizzlyTask]:
        return self.tasks

    def __call__(self) -> grizzlytask:  # noqa: C901
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            if not isinstance(parent.user, AsyncRequests):
                message = f'{parent.user.__class__.__name__} does not inherit AsyncRequests'
                raise NotImplementedError(message)  # pragma: no cover

            exception: Exception | None = None
            response_length = 0

            def trace_green(event: str, args: tuple[gevent.Greenlet, gevent.Greenlet]) -> None:  # pragma: no cover
                src, target = args

                if src is gevent.hub.get_hub():
                    return

                if event == 'switch':
                    parent.user.logger.debug('from %r switch to %r', src, target)
                elif event == 'throw':
                    parent.user.logger.debug('from %r throw exception to %r', src, target)

                if src.gr_frame:
                    tracebacks = inspect.getouterframes(src.gr_frame)
                    buff = []
                    for traceback in tracebacks:
                        srcfile, lineno, func_name, codesample = traceback[1:-1]
                        trace_line = f"""File "{srcfile}", line {lineno}, in {func_name}\n{''.join(codesample or [])} """
                        buff.append(trace_line)

                    parent.user.logger.debug(''.join(buff))

            greenlets: list[gevent.Greenlet] = []
            start = time_perf_counter()

            try:
                debug_enabled = parent.user.logger.isEnabledFor(logging.DEBUG) and environ.get('GEVENT_MONITOR_THREAD_ENABLE', None) is not None

                for request in self.tasks:
                    greenlet = gevent.spawn(parent.user.request, request)
                    if debug_enabled:
                        greenlet.settrace(trace_green)
                    greenlets.append(greenlet)

                gevent.joinall(greenlets)

                for greenlet in greenlets:
                    try:
                        _, payload = greenlet.get()
                        response_length += len(payload.encode()) if payload is not None else 0
                    except Exception as e:  # noqa: PERF203
                        parent.user.logger.exception('async request failed')
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
                    name=f'{parent.user._scenario.identifier} {self.name} ({len(self.tasks)})',
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                parent.user.failure_handler(exception, task=self)

        return task
