'''
@anchor pydoc:grizzly.tasks.timer Timer
This task "wraps" a group of other tasks, that might not have any requests and hence no statistics, to measure
how long time they took. Request content length for this task in the scenario is number of tasks between starting and
stopping the timer.

Odd executions of this task starts the timer by setting a timestamp for the task. Even executions of this task stops the timer
and logs the "response time" in the `locust` statistics.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_timer_start}

* {@pylink grizzly.steps.scenario.tasks.step_task_timer_stop}

## Statistics

Executions of this task will be visible in `locust` statistics with request type `TIMR`. `name` will be prefixed with ` (<n>)`, where `<n>`
indicates how many tasks was run between the start and stop of this timer. Response time is the total time it took for the `<n>` tasks to
run.

## Arguments

* `name` _str_ - name of the timer
'''
from typing import TYPE_CHECKING, Any, Callable, Optional
from hashlib import sha1
from time import perf_counter

from . import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


class TimerTask(GrizzlyTask):
    name: str
    variable: str

    def __init__(self, name: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        name_hash = sha1(f'timer-{name}'.encode('utf-8')).hexdigest()[:8]

        self.name = name
        self.variable = f'{name_hash}::{name}'

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        name = f'{self.scenario.identifier} {self.name}'

        def task(parent: 'GrizzlyScenario') -> Any:
            variable = parent.user._context['variables'].get(self.variable, None)

            # start timer
            if variable is None:
                parent.user._context['variables'][self.variable] = {
                    'start': perf_counter(),
                    'task-index': (parent._task_index % len(parent.tasks)),
                }
            else:  # stop timer
                response_time = int((perf_counter() - variable['start']) * 1000)
                start_task_index = variable.get('task-index', 0)

                stop_task_index = (parent._task_index % len(parent.tasks))
                response_length = (stop_task_index - start_task_index) + 1

                parent.user.environment.events.request.fire(
                    request_type='TIMR',
                    name=name,
                    response_time=response_time,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=None,
                )

                del parent.user._context['variables'][self.variable]

        return task
