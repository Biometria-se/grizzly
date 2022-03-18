'''This task calls the `grizzly` logger to print a log message at level `INFO`. It can be used to visualize values for
templating variables.

Instances of this task is created with the step expression:

* [`step_task_print_message`](/grizzly/usage/steps/scenario/tasks/#step_task_print_message)
'''
from typing import TYPE_CHECKING, Any, Callable
from dataclasses import dataclass

from jinja2 import Template

from ..types import GrizzlyTask

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario


@dataclass
class PrintTask(GrizzlyTask):
    message: str

    def implementation(self) -> Callable[['GrizzlyScenario'], Any]:
        def _implementation(parent: 'GrizzlyScenario') -> Any:
            message = Template(self.message).render(**parent.user._context['variables'])
            parent.logger.info(message)

        return _implementation
