"""
@anchor pydoc:grizzly.taks.loop Loop
This task executes the wraped tasks for all values in provided list.

All task created between {@pylink grizzly.steps.scenario.tasks.step_loop_start} and {@pylink grizzly.steps.scenario.tasks.step_loop_end}
will be wrapped in this instance and executed for all values in the provided list (must be in json format).

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_loop_start}

* {@pylink grizzly.steps.scenario.tasks.step_loop_end}

## Statistics

Executions of this task will be visible in `locust` request statistics with request type `LOOP` for each index in the list. `name` is suffixed with `[<n>}`, where `n`
is the index value of the item in the loop. Each wrapped task will have its own entry in the statistics, see respective {@pylink grizzly.tasks} documentation.

## Arguments

* `name` _str_: name of the for loop, used in `locust` statistics

* `json_input` _str_: {@link framework.usage.variables.templating} string which must be valid json and render to a list/array of values

* `variable` _str_: name of variable that a value from `input_list` will be accessible in
"""
from typing import TYPE_CHECKING, Any, Callable, List, Optional
from time import perf_counter
from json import loads as jsonloads

from gevent import sleep as gsleep

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario, GrizzlyContext

from . import GrizzlyTask, GrizzlyTaskWrapper, template
from ..utils import fastdeepcopy
from ..exceptions import RestartScenario, StopScenario, StopUser


@template('json_input')
class LoopTask(GrizzlyTask, GrizzlyTaskWrapper):
    tasks: List[GrizzlyTask]

    name: str
    json_input: str
    variable: str

    def __init__(self, grizzly: 'GrizzlyContext', name: str, json_input: str, variable: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.name = name
        self.json_input = json_input
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

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        tasks = [task() for task in self.tasks]

        def task(parent: 'GrizzlyScenario') -> Any:
            orig_value = parent.user._context['variables'][self.variable]
            start = perf_counter()
            exception: Optional[Exception] = None
            task_count = len(self.tasks)
            try:
                input_list = jsonloads(parent.render(self.json_input))

                if not isinstance(input_list, list):
                    raise RuntimeError(f'"{self.json_input}" is not a list')

                for index, value in enumerate(input_list):
                    parent.user._context['variables'][self.variable] = value

                    try:
                        for task in tasks:
                            task(parent)
                            gsleep(parent.user.wait_time())
                    except Exception as e:
                        exception = e
                    finally:
                        response_time = int((perf_counter() - start) * 1000)

                        parent.user.environment.events.request.fire(
                            request_type='LOOP',
                            name=f'{self.scenario.identifier} {self.name}[{index}]',
                            response_time=response_time,
                            response_length=task_count,
                            context=fastdeepcopy(parent.user._context),
                            exception=exception,
                        )

                        parent.user._context['variables'][self.variable] = orig_value

                        if exception is not None and self.scenario.failure_exception is not None:
                            raise self.scenario.failure_exception()
            except Exception as e:
                if isinstance(e, (RestartScenario, StopUser, StopScenario,)):
                    raise

                response_time = int((perf_counter() - start) * 1000)

                parent.user.environment.events.request.fire(
                    request_type='LOOP',
                    name=f'{self.scenario.identifier} {self.name}',
                    response_time=response_time,
                    response_length=task_count,
                    context=parent.user._context,
                    exception=e,
                )

                if self.scenario.failure_exception is not None:
                    raise self.scenario.failure_exception()

        return task
