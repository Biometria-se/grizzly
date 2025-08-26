"""Core for all grizzly scenarios."""

from __future__ import annotations

from math import floor
from typing import TYPE_CHECKING, Any, ClassVar, cast

from gevent.event import Event
from locust.exception import LocustError
from locust.user.sequential_taskset import SequentialTaskSet

from grizzly.exceptions import StopScenario, TaskTimeoutError
from grizzly.gevent import GreenletFactory
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.testdata.communication import TestdataConsumer
from grizzly.types import ScenarioState, StrDict
from grizzly.types.locust import LocalRunner, StopUser, WorkerRunner

if TYPE_CHECKING:  # pragma: no cover
    import logging
    from collections.abc import Callable

    from gevent import Greenlet
    from locust.user.task import TaskSet

    from grizzly.context import GrizzlyContext
    from grizzly.users import GrizzlyUser


class GrizzlyScenario(SequentialTaskSet):
    _consumer: ClassVar[TestdataConsumer | None] = None
    grizzly: GrizzlyContext
    task_greenlet: Greenlet | None
    task_greenlet_factory: GreenletFactory
    abort: Event

    _task_index: int
    _user: GrizzlyUser

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)
        self.user.scenario_state = ScenarioState.STOPPED
        self.task_greenlet = None
        self.task_greenlet_factory = GreenletFactory(logger=self.logger, ignore_exceptions=[StopScenario])
        self.abort = Event()
        self.parent.environment.events.quitting.add_listener(self.on_quitting)
        self._task_index = 0

        from grizzly.context import grizzly  # noqa: PLC0415

        self.grizzly = grizzly

    @property
    def logger(self) -> logging.Logger:
        return self.user.logger

    @property
    def consumer(self) -> TestdataConsumer:
        if self.__class__._consumer is None:
            message = 'no consumer has been created'
            raise ValueError(message)

        return self.__class__._consumer

    @property
    def user(self) -> GrizzlyUser:
        return self._user

    @property
    def current_iteration(self) -> int:
        return floor(self._task_index / len(self.tasks)) + 1

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        cls.tasks.append(task_factory())

    @classmethod
    def _escape_values(cls, values: StrDict) -> StrDict:
        _values: StrDict = {}

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

        There should be one `TestdataConsumer` per scenario type, which means that all users on the
        same worker will share the same instance.
        """
        if self.__class__._consumer is None:
            self.__class__._consumer = TestdataConsumer(
                scenario=self,
                runner=cast('LocalRunner | WorkerRunner', self.grizzly.state.locust),
            )

        self.user.consumer = self.__class__._consumer
        self.user.scenario_state = ScenarioState.RUNNING

        # only prefetch iterator testdata if everything was started OK
        self.prefetch()

        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:
                    task.on_start(self)
                except:
                    self.logger.exception('on_start failed for task %r', task)
                    raise StopUser from None

    def on_iteration(self) -> None:
        self.user.on_iteration()

        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:
                    task.on_iteration(self)
                except:
                    self.logger.exception('on_iteration failed for task %r', task)
                    raise StopUser from None

    def on_stop(self) -> None:
        """When locust test is stopping, all tasks on_stop methods must be called, even though
        one might fail, so just log those as errors.
        """
        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:
                    task.on_stop(self)
                except Exception:
                    self.logger.exception('task on_stop failed')

        try:
            self.user.scenario_state = ScenarioState.STOPPED
        except:
            self.logger.exception('scenario on_stop failed')

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        """When locust is quitting, with abort=True (signal received) we should force the
        running task to stop by throwing an exception in the greenlet where it is running.
        """
        if self.task_greenlet is not None and kwargs.get('abort', False) and not self.abort.is_set():
            self.abort.set()
            self.task_greenlet.kill(StopScenario, block=False)
            self.logger.debug('scenario killed task (greenlet)')

    def get_next_task(self) -> TaskSet | Callable:
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
            task = self._task_queue.popleft()

            with self.task_greenlet_factory.spawn_task(self, task, index, total, description) as greenlet:
                self.task_greenlet = greenlet
        except TaskTimeoutError as e:
            metadata = getattr(task, '__grizzly_metadata__', {})
            method = metadata.get('method', None) or 'TASK'
            name = metadata.get('name', None) or description

            self.user.environment.stats.log_error(method, name, str(e))
            self.user.failure_handler(e, task=task)
        finally:
            self.task_greenlet = None


from .iterator import IteratorScenario

__all__ = [
    'GrizzlyScenario',
    'IteratorScenario',
]
