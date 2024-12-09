"""@anchor pydoc:grizzly.tasks.async_timer Asynchronous Timer
This timer can be started in one scenario, on one worker, and be stopped in another scenario on another worker.

It is possible to stop a timer only based on `tid` and `version`, but in that case that combination can only be used for *one* timer.
This is useful if a timers is started in many different scenarios, but are stopped in one scenario which does not have any other information
than the id (which it might have received from a queue or topic), and hence the timer name is not know at the time it is being stopped.

A name must **always** be provided when starting the timer.

`name`, `tid` and `version` supports {@pylink framework.usage.variables.templating}.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.async_timer.step_task_async_timer_start}

* {@pylink grizzly.steps.scenario.tasks.async_timer.step_task_async_timer_stop_name}

* {@pylink grizzly.steps.scenario.tasks.async_timer.step_task_async_timer_stop_tid}

## Statistics

When running distributed, the timer statistics piggyback with normal locust statistics which is sent from workers (default every 3 seconds) via
the event `locust.event.report_to_master`, and is then handled on master with the `locust.event.worker_report` event. If running local, it will
be started and stopped in "real-time".

When the stop-part of this task is executed, a "request" with method `DOC` and name of the timer will be added to the request statistics. If
anything goes wrong when executing the task, the error will also visible in the locust failure summary.

Any timers that has not been stopped when the load test is finished, will be listed in the behave failure summary, and the test will be marked
as failed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from grizzly.tasks import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario


ActionType = Literal['start', 'stop']


@template('name', 'tid', 'version')
class AsyncTimerTask(GrizzlyTask):
    name: str | None
    tid: str
    version: str
    action: ActionType

    def __init__(self, name: str | None, tid: str, version: str, action: ActionType) -> None:
        super().__init__()

        self.name = name
        self.tid = tid
        self.version = version
        self.action = action

        if action == 'start' and name is None:
            message = 'name must be set when starting a timer'
            raise AssertionError(message)

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def implementation(parent: GrizzlyScenario) -> Any:
            timestamp: datetime | str = datetime.now().astimezone()

            # @TODO: this should no look like this when merged in master
            try:
                if self.action == 'stop' and all(var in parent.user.variables for var in ['PutDate', 'PutTime']):
                    timestamp = datetime.strptime(
                        f'{parent.user.variables['PutDate']} {parent.user.variables['PutTime']}',
                        '%Y%m%d %H%M%S%f',
                    ).replace(tzinfo=timezone.utc)

                name = parent.user.render(self.name) if self.name is not None else self.name
                tid = parent.user.render(self.tid)
                version = parent.user.render(self.version)

                parent.consumer.async_timers.toggle(self.action, name, tid, version, timestamp)
            except Exception as e:
                message = f'failed to {self.action} timer "{name}" for id "{tid}" and version "{version}"'
                parent.logger.exception(message)
                parent.user.environment.stats.log_error('DOC', name or f'{tid}::{version}', e)

        return implementation
