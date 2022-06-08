'''
@anchor pydoc:grizzly.tasks.wait Wait
This task executes a `gevent.sleep` and is used to manually create delays between steps in a scenario.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_wait_seconds}

## Arguments

* `time` _float_ - fractions of seconds to excplicitly sleep in the scenario
'''
from typing import TYPE_CHECKING, Any, Callable, Optional

from gevent import sleep as gsleep

from . import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


class WaitTask(GrizzlyTask):
    time: float

    def __init__(self, time: float, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.time = time

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            parent.logger.debug(f'waiting for {self.time} seconds')
            gsleep(self.time)
            parent.logger.debug(f'done waiting for {self.time} seconds')

        return task
