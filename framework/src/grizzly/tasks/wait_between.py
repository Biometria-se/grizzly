"""Task sets the wait time between tasks in a scenario.

The default is to wait `0` seconds between each task.

This is useful in a scenario with many tasks that should have some wait time between them, but there are a group
of tasks (e.g. [Transformer][grizzly.tasks.transformer], [Date][grizzly.tasks.date] or [Log Message][grizzly.tasks.log_message])
that should execute as fast as possible.

If `max_time` is not provided, the wait between tasks is constant `min_time`. If both are provided there will be a
random wait between (and including) `min_time` and `max_time` between tasks.

## Step implementations

* [Constant][grizzly.steps.scenario.tasks.wait_between.step_task_wait_between_constant]

* [Random][grizzly.steps.scenario.tasks.wait_between.step_task_wait_between_random]

## Statistics

This task does not have any request statistics entries.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from locust import between, constant

from grizzly.exceptions import StopUser

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('min_time', 'max_time')
class WaitBetweenTask(GrizzlyTask):
    min_time: str
    max_time: str | None

    def __init__(self, min_time: str, max_time: str | None = None) -> None:
        super().__init__(timeout=None)

        self.min_time = min_time
        self.max_time = max_time

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            min_time_rendered = parent.user.render(self.min_time)
            try:
                min_time = float(min_time_rendered.strip())
            except Exception as e:
                message = f'"{self.min_time}" rendered into "{min_time_rendered}" which is not valid'
                parent.logger.exception(message)
                raise StopUser from e

            max_time: float | None = None

            if self.max_time is not None:
                max_time_rendered = parent.user.render(self.max_time)
                try:
                    max_time = float(max_time_rendered.strip())
                except Exception as e:
                    message = f'"{self.max_time}" rendered into "{max_time_rendered}" which is not valid'
                    parent.logger.exception(message)
                    raise StopUser from e

                if min_time > max_time:
                    min_time, max_time = max_time, min_time

            wait_time = constant(min_time) if max_time is None else between(min_time, max_time)

            bound_wait_time = wait_time.__get__(parent.user, parent.user.__class__)

            parent.user.wait_time = bound_wait_time

        return task
