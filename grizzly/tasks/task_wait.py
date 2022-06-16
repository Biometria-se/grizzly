'''
@anchor pydoc:grizzly.tasks.task_wait Task Wait
This task sets the wait time between tasks in a scenario.

The default is to wait `0` seconds between each task.

This is useful in a scenario with many tasks that should have some wait time between them, but there are a group
of tasks (e.g. Transform, Date or Log Messages) that should execute as fast as possible.

If `max_time` is not provided, the wait between tasks is constant `min_time`. If both are provided there will be a
random wait between (and including) `min_time` and `max_time` between tasks.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_wait_constant}

* {@pylink grizzly.steps.scenario.tasks.step_task_wait_between}

## Statistics

This task does not have any request statistics entries.

## Arguments

* `min_time` _float_ - minimum time to wait

* `max_time` _float_ (optional) - maximum time to wait
'''
from typing import TYPE_CHECKING, Optional, Callable, Any

from locust import between, constant

from . import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


class TaskWaitTask(GrizzlyTask):
    min_time: float
    max_time: Optional[float]

    def __init__(self, min_time: float, max_time: Optional[float] = None, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.min_time = min_time
        self.max_time = max_time

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            if self.max_time is None:
                wait_time = constant(self.min_time)
            else:
                wait_time = between(self.min_time, self.max_time)

            bound_wait_time = wait_time.__get__(parent.user, parent.user.__class__)

            setattr(parent.user, 'wait_time', bound_wait_time)

        return task
