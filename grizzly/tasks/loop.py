"""Task executes the wraped tasks for all values in provided list.

All task created between [Start][grizzly.steps.scenario.tasks.loop.step_task_loop_start] and [End][grizzly.steps.scenario.tasks.loop.step_task_loop_end]
will be wrapped in this instance and executed for all values in the provided list (must be in JSON format).

## Step implementations

* [Start][grizzly.steps.scenario.tasks.loop.step_task_loop_start]

* [End][grizzly.steps.scenario.tasks.loop.step_task_loop_end]

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `LOOP` and `name` is suffixed with `(<n>)`, where `n`
is the number of wrapped tasks.

Each wrapped task will have its own entry in the statistics, see respective [tasks][grizzly.tasks] documentation.

"""

from __future__ import annotations

from json import loads as jsonloads
from time import perf_counter
from typing import TYPE_CHECKING, Any

from gevent import sleep as gsleep

from . import GrizzlyTask, GrizzlyTaskWrapper, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('values', 'tasks')
class LoopTask(GrizzlyTaskWrapper):
    tasks: list[GrizzlyTask]

    name: str
    values: str
    variable: str

    def __init__(self, name: str, values: str, variable: str) -> None:
        super().__init__(timeout=None)

        self.name = name
        self.values = values
        self.variable = variable

        self.tasks = []

        assert self.variable in self.grizzly.scenario.variables, f'{self.__class__.__name__}: {self.variable} has not been initialized'

    def add(self, task: GrizzlyTask) -> None:
        task_name = getattr(task, 'name', None)
        if task_name is not None and hasattr(task, 'name'):
            task.name = f'{self.name}:{task_name}'
        self.tasks.append(task)

    def peek(self) -> list[GrizzlyTask]:
        return self.tasks

    def __call__(self) -> grizzlytask:
        tasks = [task() for task in self.tasks]

        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            orig_value = parent.user.variables.get(self.variable, None)
            start = perf_counter()
            exception: Exception | None = None
            task_count = len(self.tasks)
            response_length: int = 0

            try:
                values = jsonloads(parent.user.render(self.values))

                if not isinstance(values, list):
                    message = f'"{self.values}" is not a list'
                    raise TypeError(message)

                response_length = len(values)

                for value in values:
                    parent.user.set_variable(self.variable, value)

                    for task in tasks:
                        task(parent)
                        gsleep(parent.user.wait_time())

                    parent.user.set_variable(self.variable, orig_value)
            except Exception as e:
                exception = e
            finally:
                response_time = int((perf_counter() - start) * 1000)

                # if task in loop throws the "failure_handling" exception, do not fire LOOP request
                if (
                    exception is not None
                    and parent.user._scenario.failure_handling.get(None, None) is not None
                    and exception.__class__ is parent.user._scenario.failure_handling.get(None, None)
                ):
                    raise exception

                parent.user.environment.events.request.fire(
                    request_type='LOOP',
                    name=f'{parent.user._scenario.identifier} {self.name} ({task_count})',
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

                parent.user.failure_handler(exception, task=self)

        @task.on_start
        def on_start(parent: GrizzlyScenario) -> None:
            for task in tasks:
                task.on_start(parent)

        @task.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:
            for task in tasks:
                task.on_stop(parent)

        return task
