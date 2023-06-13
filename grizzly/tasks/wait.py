'''
@anchor pydoc:grizzly.tasks.wait Wait
This task executes a `gevent.sleep` and is used to manually create delays between steps in a scenario.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_wait_seconds}

## Arguments

* `time_expression` _str_ - float as string or a {@pydocfractions of seconds to excplicitly sleep in the scenario
'''
from typing import TYPE_CHECKING, Any

from gevent import sleep as gsleep

from grizzly.exceptions import StopUser

from . import GrizzlyTask, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('time_expression')
class WaitTask(GrizzlyTask):
    time_expression: str

    def __init__(self, time_expression: str) -> None:
        super().__init__()

        self.time_expression = time_expression

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            try:
                time_rendered = parent.render(self.time_expression)
                if len(time_rendered.strip()) < 1:
                    raise RuntimeError(f'"{self.time_expression}" rendered into "{time_rendered}" which is not valid')

                time = float(time_rendered.strip())
                parent.logger.debug(f'waiting for {time} seconds')
                gsleep(time)
                parent.logger.debug(f'done waiting for {time} seconds')
            except Exception as exception:
                parent.user.environment.events.request.fire(
                    request_type='WAIT',
                    name=f'{parent.user._scenario.identifier} WaitTask=>{self.time_expression}',
                    response_time=0,
                    response_length=0,
                    context=parent.user._context,
                    exception=exception,
                )

                raise StopUser()

        return task
