"""@anchor pydoc:grizzly.tasks.wait_explicit Explicit Wait
This task executes a `gevent.sleep` and is used to manually create delays between steps in a scenario.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.wait_explicit.step_task_wait_explicit}

## Arguments

* `time_expression` _str_ - float as string or a fractions of seconds to excplicitly sleep in the scenario, supports {@link framework.usage.variables.templating}
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gevent import sleep as gsleep

from grizzly.exceptions import StopUser

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('time_expression')
class ExplicitWaitTask(GrizzlyTask):
    time_expression: str

    def __init__(self, time_expression: str) -> None:
        super().__init__()

        self.time_expression = time_expression

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            try:
                time_rendered = parent.render(self.time_expression)
                if len(time_rendered.strip()) < 1:
                    message = f'"{self.time_expression}" rendered into "{time_rendered}" which is not valid'
                    raise RuntimeError(message)

                time = float(time_rendered.strip())
                parent.logger.debug('waiting for %f seconds', time)
                gsleep(time)
                parent.logger.debug('done waiting for %f seconds', time)
            except Exception as exception:
                parent.user.environment.events.request.fire(
                    request_type='WAIT',
                    name=f'{parent.user._scenario.identifier} ExplicitWaitTask=>{self.time_expression}',
                    response_time=0,
                    response_length=0,
                    context=parent.user._context,
                    exception=exception,
                )

                raise StopUser from exception

        return task
