"""@anchor pydoc:grizzly.tasks.async_timer Asynchronous Timer
This timer can be started in one scenario, on one worker, and be stopped in another scenario on another worker.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from dateutil.parser import parse as date_parser

from grizzly.tasks import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:
    from gevent.lock import Semaphore

    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.locust import Environment

ActionType = Literal['start', 'stop']


class AsyncTimer:
    name: str
    tid: str
    version: str
    start: datetime
    stop: datetime

    def __init__(self, name: str, tid: str, version: str, start: datetime) -> None:
        self.name = name
        self.tid = tid
        self.version = version
        self.start = start

    def duration_ms(self, stop: datetime) -> int:
        self.stop = stop

        return int((self.stop - self.start).total_seconds() * 1000)


class AsyncTimers:
    _start: list[dict[str, str | None]]
    _stop: list[dict[str, str | None]]

    environment: Environment
    active_timers: dict[str, AsyncTimer]
    semaphore: Semaphore

    def __init__(self, environment: Environment, semaphore: Semaphore) -> None:
        self.semaphore = semaphore
        self.parent = environment

        self._start = []
        self._stop = []

        self.active_timers = {}

    @classmethod
    def parse_date(cls, value: str) -> datetime:
        return date_parser(value).replace(tzinfo=timezone.utc).astimezone()

    @classmethod
    def extract(cls, data: dict[str, Any]) -> tuple[str | None, str, str, datetime]:
        timestamp = date_parser(data['timestamp']).astimezone()

        return data.get('name'), data['tid'], data['version'], timestamp

    def toggle(self, action: ActionType, name: str | None, tid: str, version: str, timestamp: datetime | str) -> None:
        """TestdataConsumer."""
        if isinstance(timestamp, str):
            timestamp = self.parse_date(timestamp)

        data = {
            'name': name,
            'tid': tid,
            'version': version,
            'timestamp': timestamp.isoformat(),
        }

        with self.semaphore:
            if action == 'start':
                self._start.append(data)
            else:
                self._stop.append(data)

    def on_report_to_master(self, client_id: str, data: dict[str, Any]) -> None:  # noqa: ARG002
        """TestdataConsumer."""
        with self.semaphore:
            data.update({'async_timers': {
                'start': [*self._start],
                'stop': [*self._stop],
            }})

            self._start.clear()
            self._stop.clear()

    def start(self, data: dict[str, Any]) -> None:
        name, tid, version, timestamp = self.extract(data)

        assert name is not None

        timer_id = f'{tid}_{version}'

        if timer_id in self.active_timers:
            message = f'{timer_id} has already been started'
            raise KeyError(message)

        timer = AsyncTimer(name, tid, version, timestamp)

        with self.semaphore:
            self.active_timers.update({timer_id: timer})

    def stop(self, data: dict[str, Any]) -> None:
        name, tid, version, timestamp = self.extract(data)

        timer_id = f'{tid}_{version}'

        with self.semaphore:
            timer = self.active_timers.get(timer_id)

            if timer is None:
                message = f'{timer_id} has not been started'
                raise ValueError(message)

            del self.active_timers[timer_id]

        if name is None:
            name = timer.name

        duration = timer.duration_ms(timestamp)

        self.environment.events.request.fire(
            request_type='DOC',  # @TODO: should not this when merging to main
            name=name,
            response_time=duration,
            response_length=0,
            context={
                '__time__': timer.start.isoformat(),
                '__fields_request_started__': timer.start.isoformat(),
                '__fields_request_finished__': timer.stop.isoformat(),
            },
        )


    def on_worker_report(self, client_id: str, data: dict[str, Any]) -> None:  # noqa: ARG002
        """TestdataProducer."""
        async_timers = data.get('async_timers', {})

        for async_data in async_timers.get('start', []):
            self.start(async_data)

        for async_data in async_timers.get('stop', []):
            self.stop(async_data)


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
                timestamp = f'{parent.user.variables['PutDate']} {parent.user.variables['PutTime']}'

            parent.consumer.async_timers.toggle(self.action, self.name, self.tid, self.version, timestamp)

        return implementation
