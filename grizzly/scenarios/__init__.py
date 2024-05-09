"""Core for all grizzly scenarios."""
from __future__ import annotations

import logging
from os import environ
from typing import TYPE_CHECKING, Any, Dict, Optional, cast

from locust.user.sequential_taskset import SequentialTaskSet

from grizzly.context import GrizzlyContext
from grizzly.exceptions import StopScenario
from grizzly.gevent import GreenletWithExceptionCatching
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.testdata.communication import TestdataConsumer
from grizzly.types import ScenarioState
from grizzly.types.locust import StopUser
from grizzly.utils import has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.users import GrizzlyUser


class GrizzlyScenario(SequentialTaskSet):
    consumer: TestdataConsumer
    logger: logging.Logger
    grizzly: GrizzlyContext
    task_greenlet: Optional[GreenletWithExceptionCatching]
    task_greenlet_factory: GreenletWithExceptionCatching
    abort: bool
    spawning_complete: bool

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)
        self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')
        self.grizzly = GrizzlyContext()
        self.user.scenario_state = ScenarioState.STOPPED
        self.task_greenlet = None
        self.task_greenlet_factory = GreenletWithExceptionCatching()
        self.abort = False
        self.spawning_complete = False
        self.parent.environment.events.quitting.add_listener(self.on_quitting)

    @property
    def user(self) -> GrizzlyUser:
        return cast('GrizzlyUser', self._user)

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        cls.tasks.append(task_factory())

    def render(self, template: str, variables: Optional[Dict[str, Any]] = None) -> str:
        if not has_template(template):
            return template

        if variables is None:
            variables = {}

        return self.grizzly.state.jinja2.from_string(template).render(**self.user._context['variables'], **variables)

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
                identifier=self.__class__.__name__,
            )
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

        self.consumer.stop()
        self.user.scenario_state = ScenarioState.STOPPED

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        """When locust is quitting, with abort=True (signal received) we should force the
        running task to stop by throwing an exception in the greenlet where it is running.
        """
        if self.task_greenlet is not None and kwargs.get('abort', False):
            self.abort = True
            self.task_greenlet.kill(StopScenario, block=False)

    def execute_next_task(self) -> None:
        """Execute task in a greenlet, so that we have the possibility to stop it on demand. Any exceptions
        raised in the greenlet should be caught else where.
        """
        try:
            self.task_greenlet_factory.spawn_blocking(super().execute_next_task)
        finally:
            self.task_greenlet = None


from .iterator import IteratorScenario

__all__ = [
    'GrizzlyScenario',
    'IteratorScenario',
]
