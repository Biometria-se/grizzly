"""Timer can be started in one scenario, on one worker, and be stopped in another scenario on another worker.

This is useful if a timers is started in many different scenarios, but are stopped in one scenario which does not have any other information
than the id (which it might have received from a queue or topic), and hence the timer name is not know at the time it is being stopped.

## Step implementations

* [Start][grizzly.steps.scenario.tasks.async_timer.step_task_async_timer_start]

* [Stop][grizzly.steps.scenario.tasks.async_timer.step_task_async_timer_stop]

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

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


ActionType = Literal['start', 'stop']


@template('tname', 'tid', 'version')
class AsyncTimerTask(GrizzlyTask):
    tname: str
    tid: str
    version: str
    action: ActionType

    def __init__(self, name: str, tid: str, version: str, action: ActionType) -> None:
        super().__init__()

        self.tname = name
        self.tid = tid
        self.version = version
        self.action = action

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def implementation(parent: GrizzlyScenario) -> Any:
            timestamp: datetime | str = datetime.now().astimezone()

            name: str | None = None
            tid: str | None = None
            version: str | None = None

            try:
                # short cut for using MQ properties date and time when a message was put on the queue
                # and expressions for extracting these has been made in the scenario
                if self.action == 'stop' and all(str(parent.user.variables.get(var, 'none')).lower() != 'none' for var in ['PutDate', 'PutTime']):
                    timestamp_date = parent.user.variables['PutDate']
                    timestamp_time = parent.user.variables['PutTime']
                    timestamp = datetime.strptime(
                        f'{timestamp_date} {timestamp_time}',
                        '%Y%m%d %H%M%S%f',
                    ).replace(tzinfo=timezone.utc)

                name = parent.user.render(self.tname)
                tid = parent.user.render(self.tid)
                version = parent.user.render(self.version)

                parent.consumer.async_timers.toggle(self.action, name, tid, version, timestamp)
            except Exception as e:
                message = f'failed to {self.action} timer "{name or self.tname}" for id "{tid or self.tid}" and version "{version or self.version}"'
                parent.logger.exception(message)
                parent.user.environment.stats.log_error('DOC', name or f'{tid}::{version}', e)

        return implementation
