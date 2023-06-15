"""
@anchor pydoc:grizzly.tasks.loop Loop
This task executes the wraped tasks for all values in provided list.

All task created between {@pylink grizzly.steps.scenario.tasks.step_task_loop_start} and {@pylink grizzly.steps.scenario.tasks.step_task_loop_end}
will be wrapped in this instance and executed for all values in the provided list (must be in JSON format).

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_loop_start}

* {@pylink grizzly.steps.scenario.tasks.step_task_loop_end}

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `LOOP` and `name` is suffixed with `(<n>)`, where `n`
is the number of wrapped tasks. Each wrapped task will have its own entry in the statistics, see respective {@pylink grizzly.tasks} documentation.

## Arguments

* `name` _str_: name of the for loop, used in `locust` statistics

* `values` _str_: {@link framework.usage.variables.templating} string which must be valid json and render to a list of values

* `variable` _str_: name of variable that a value from `input_list` will be accessible in
"""
from typing import TYPE_CHECKING, Any, List, Optional
from time import perf_counter
from json import loads as jsonloads

from gevent import sleep as gsleep

from . import GrizzlyTask, GrizzlyTaskWrapper, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.context import GrizzlyContext


@template('values', 'tasks')
class LoopTask(GrizzlyTaskWrapper):
    tasks: List[GrizzlyTask]

    name: str
    values: str
    variable: str

    def __init__(self, grizzly: 'GrizzlyContext', name: str, values: str, variable: str) -> None:
        super().__init__()

        self.name = name
        self.values = values
        self.variable = variable

        self.tasks = []

        if self.variable not in grizzly.state.variables:
            raise ValueError(f'{self.__class__.__name__}: {self.variable} has not been initialized')

    def add(self, task: GrizzlyTask) -> None:
        task_name = getattr(task, 'name', None)
        if task_name is not None:
            setattr(task, 'name', f'{self.name}:{task_name}')
        self.tasks.append(task)

    def peek(self) -> List[GrizzlyTask]:
        return self.tasks

    def __call__(self) -> grizzlytask:
        tasks = [task() for task in self.tasks]

        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            orig_value = parent.user._context['variables'].get(self.variable, None)
            start = perf_counter()
            exception: Optional[Exception] = None
            task_count = len(self.tasks)
            response_length: int = 0

            try:
                values = jsonloads(parent.render(self.values))

                if not isinstance(values, list):
                    raise RuntimeError(f'"{self.values}" is not a list')

                response_length = len(values)

                for value in values:
                    parent.user._context['variables'].update({self.variable: value})

                    for task in tasks:
                        task(parent)
                        gsleep(parent.user.wait_time())

                    parent.user._context['variables'].update({self.variable: orig_value})
            except Exception as e:
                exception = e
            finally:
                response_time = int((perf_counter() - start) * 1000)

                if exception is not None and parent.user._scenario.failure_exception is not None and isinstance(exception, parent.user._scenario.failure_exception):
                    raise exception
                else:
                    parent.user.environment.events.request.fire(
                        request_type='LOOP',
                        name=f'{parent.user._scenario.identifier} {self.name} ({task_count})',
                        response_time=response_time,
                        response_length=response_length,
                        context=parent.user._context,
                        exception=exception,
                    )

                    if exception is not None and parent.user._scenario.failure_exception is not None:
                        raise parent.user._scenario.failure_exception()

        @task.on_start
        def on_start(parent: 'GrizzlyScenario') -> None:
            for task in tasks:
                task.on_start(parent)

        @task.on_stop
        def on_stop(parent: 'GrizzlyScenario') -> None:
            for task in tasks:
                task.on_stop(parent)

        return task
