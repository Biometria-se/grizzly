'''This task calls the `grizzly` logger to print a log message at level `INFO`. It can be used to visualize values for
templating variables.

Instances of this task is created with the step expression:

* [`step_task_print_message`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_print_message)
'''
from typing import TYPE_CHECKING, Any, Callable, Optional

from . import GrizzlyTask, template

if TYPE_CHECKING:  # pragma: no cover
    from ..context import GrizzlyContextScenario
    from ..scenarios import GrizzlyScenario


@template('message')
class PrintTask(GrizzlyTask):
    message: str

    def __init__(self, message: str, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        super().__init__(scenario)

        self.message = message

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        def task(parent: 'GrizzlyScenario') -> Any:
            message = parent.render(self.message)
            parent.logger.info(message)

        return task
