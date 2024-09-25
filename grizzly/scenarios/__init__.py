"""Core for all grizzly scenarios."""
from __future__ import annotations

import logging
from os import environ
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

from gevent.event import Event
from locust.exception import LocustError
from locust.user.sequential_taskset import SequentialTaskSet

from grizzly.context import GrizzlyContext
from grizzly.exceptions import StopScenario
from grizzly.gevent import GreenletFactory
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.testdata.communication import TestdataConsumer
from grizzly.types import ScenarioState
from grizzly.types.locust import StopUser

if TYPE_CHECKING:  # pragma: no cover
    from gevent import Greenlet
    from locust.user.task import TaskSet

    from grizzly.users import GrizzlyUser


class GrizzlyScenario(SequentialTaskSet):
    consumer: TestdataConsumer
    logger: logging.Logger
    grizzly: GrizzlyContext
    task_greenlet: Optional[Greenlet]
    task_greenlet_factory: GreenletFactory
    abort: Event
    spawning_complete: bool

    _task_index: int

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)
        self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')
        self.grizzly = GrizzlyContext()
        self.user.scenario_state = ScenarioState.STOPPED
        self.task_greenlet = None
        self.task_greenlet_factory = GreenletFactory(logger=self.logger, ignore_exceptions=[StopScenario])
        self.abort = Event()
        self.spawning_complete = False
        self.parent.environment.events.quitting.add_listener(self.on_quitting)
        self._task_index = 0

    @property
    def user(self) -> GrizzlyUser:
        return cast('GrizzlyUser', self._user)

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        cls.tasks.append(task_factory())

    @classmethod
    def _escape_values(cls, values: dict[str, Any]) -> dict[str, Any]:
        _values: dict[str, Any] = {}

        for key, value in values.items():
            _value = value.replace('"', '\\"') if isinstance(value, str) else value
            _values.update({key: _value})

        return _values

    def prefetch(self) -> None:
        """Do not prefetch anything by default."""

    def on_start(self) -> None:
        """When test start the testdata producer should be started, and if the implementing scenario
        has some prefetching todo it must also be one. There might be cases where an on_start method
        needs the first iteration of testdata.
        """
        producer_address = environ.get('TESTDATA_PRODUCER_ADDRESS', None)
        if producer_address is not None:
            self.consumer = TestdataConsumer(
                scenario=self,
                address=producer_address,
            )
            self.user.consumer = self.consumer
            self.user.scenario_state = ScenarioState.RUNNING
        else:
            self.logger.error('no address to testdata producer specified')
            raise StopUser

        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:  # type: ignore[unreachable]
                    task.on_start(self)
                except:
                    self.logger.exception('on_start failed for task %r', task)
                    raise StopUser from None

        # only prefetch iterator testdata if everything was started OK
        self.prefetch()

    def on_stop(self) -> None:
        """When locust test is stopping, all tasks on_stop methods must be called, even though
        one might fail, so just log those as errors.
        """
        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:  # type: ignore[unreachable]
                    task.on_stop(self)
                except Exception:
                    self.logger.exception('on_stop failed')

        try:
            self.consumer.stop()
            self.user.scenario_state = ScenarioState.STOPPED
        except:
            self.logger.exception('on_stop failed')

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        """When locust is quitting, with abort=True (signal received) we should force the
        running task to stop by throwing an exception in the greenlet where it is running.
        """
        if self.task_greenlet is not None and kwargs.get('abort', False) and not self.abort.is_set():
            self.abort.set()
            self.task_greenlet.kill(StopScenario, block=False)
            self.logger.debug('killed task (greenlet)')

    def get_next_task(self) -> Union[TaskSet, Callable]:
        """Use old way of getting task, so we can reset which task to start from."""
        if not self.tasks:
            message = 'No tasks defined. Use the @task decorator or set the "tasks" attribute of the SequentialTaskSet'
            raise LocustError(message)

        task = self.tasks[self._task_index % len(self.tasks)]
        self._task_index += 1

        return task

    def execute_next_task(self, index: int, total: int, description: str) -> None:  # type: ignore[override]
        """Execute task in a greenlet, so that we have the possibility to stop it on demand. Any exceptions
        raised in the greenlet should be caught else where.
        """
        try:
            with self.task_greenlet_factory.spawn_task(super().execute_next_task, index, total, description) as greenlet:
                self.task_greenlet = greenlet
        finally:
            self.task_greenlet = None


from .iterator import IteratorScenario

__all__ = [
    'GrizzlyScenario',
    'IteratorScenario',
]
