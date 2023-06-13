'''
@anchor pydoc:grizzly.tasks.log_message Log Message
This task calls the `grizzly` logger to print a log message at level `INFO`. It can be used to visualize values for
{@link framework.usage.variables.templating} variables.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_log_message}

## Statistics

This task does not have any request statistics entries.

## Arguments

* `message` _str_ - message to log at `INFO` level, can be a template
'''
from typing import TYPE_CHECKING, Any

from . import GrizzlyTask, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('message')
class LogMessageTask(GrizzlyTask):
    message: str

    def __init__(self, message: str) -> None:
        super().__init__()

        self.message = message

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            message = parent.render(self.message)
            parent.logger.info(message)

        return task
