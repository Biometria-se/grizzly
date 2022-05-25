'''This task changes the default wait time between tasks that has been set with step:

```gherkin
And wait time inbetween requests is random between "1.0" and "1.0" seconds
```

This is useful in a scenario with many requests that should have some wait time between them, but there are a group
of tasks (e.g. Transform, Date or Log Messages) that should execute as fast as possible.

Instances of this task is created with the step expression:

* [`step_task_request_wait`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_request_wait)
'''
from typing import TYPE_CHECKING, Optional, Callable, Any

from locust import between, constant

from . import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


class RequestWaitTask(GrizzlyTask):
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
