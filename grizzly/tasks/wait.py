'''This task executes a `gevent.sleep` and is used to manually create delays between steps in a scenario.

Instances of this task is created with the step expression:

* [`step_task_wait_seconds`](/grizzly/usage/steps/scenario/tasks/#step_task_wait_seconds)
'''
from typing import TYPE_CHECKING, Any, Callable
from dataclasses import dataclass

from gevent import sleep as gsleep

from ..types import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario


@dataclass
class WaitTask(GrizzlyTask):
    time: float

    def implementation(self) -> Callable[['GrizzlyScenario'], Any]:
        def _implementation(parent: 'GrizzlyScenario') -> Any:
            parent.logger.debug(f'waiting for {self.time} seconds')
            gsleep(self.time)
            parent.logger.debug(f'done waiting for {self.time} seconds')

        return _implementation
