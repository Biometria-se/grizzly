"""@anchor pydoc:grizzly.tasks.async_timer Asynchronous Timer
This timer can be started in one scenario, on one worker, and be stopped in another scenario on another worker.
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
            if self.action == 'stop' and all(var in parent.user.variables for var in ['PutDate', 'PutTime']):
                timestamp = datetime.strptime(
                    f'{parent.user.variables['PutDate']} {parent.user.variables['PutTime']}',
                    '%Y%m%d %H%M%S%f',
                ).replace(tzinfo=timezone.utc)

            parent.consumer.async_timers.toggle(self.action, self.name, self.tid, self.version, timestamp)

        return implementation
