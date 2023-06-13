import logging

from typing import TYPE_CHECKING, Optional, Dict, Any, Tuple, cast
from os import environ

from locust.user.sequential_taskset import SequentialTaskSet

from grizzly.types import ScenarioState
from grizzly.types.locust import StopUser
from grizzly.exceptions import StopScenario
from grizzly.context import GrizzlyContext
from grizzly.testdata.communication import TestdataConsumer
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.gevent import GreenletWithExceptionCatching

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.users.base import GrizzlyUser


class GrizzlyScenario(SequentialTaskSet):
    consumer: TestdataConsumer
    logger: logging.Logger
    grizzly: GrizzlyContext
    task_greenlet: Optional[GreenletWithExceptionCatching]
    task_greenlet_factory: GreenletWithExceptionCatching
    abort: bool
    spawning_complete: bool

    def __init__(self, parent: 'GrizzlyUser') -> None:
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
    def user(self) -> 'GrizzlyUser':
        return cast('GrizzlyUser', self._user)

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        cls.tasks.append(task_factory())

    def render(self, input: str, variables: Optional[Dict[str, Any]] = None) -> str:
        if variables is None:
            variables = {}

        return self.grizzly.state.jinja2.from_string(input).render(**self.user._context['variables'], **variables)

    def prefetch(self) -> None:
        """
        Default implementation is to not prefetch anything.
        """
        pass

    def on_start(self) -> None:
        """
        When test starts the testdata producer should be started, and if the implementing scenario
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
            raise StopUser()

        self.prefetch()

        for task in self.tasks:
            if isinstance(task, grizzlytask):
                task.on_start(self)

    def on_stop(self) -> None:
        """
        When locust test is stopping, all tasks on_stop methods must be called, even though
        one might fail, so just log those as errors
        """
        for task in self.tasks:
            if isinstance(task, grizzlytask):
                try:
                    task.on_stop(self)
                except Exception as e:
                    self.logger.error(f'on_stop: {str(e)}', exc_info=True)

        self.consumer.stop()
        self.user.scenario_state = ScenarioState.STOPPED

    def on_quitting(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        """
        When locust is quitting, with abort=True (signal received) we should force the
        running task to stop by throwing an exception in the greenlet where it is running.
        """
        if self.task_greenlet is not None and kwargs.get('abort', False):
            self.abort = True
            self.task_greenlet.kill(StopScenario, block=False)

    def execute_next_task(self) -> None:
        """
        Execute task in a greenlet, so that we have the possibility to stop it on demand. Any exceptions
        raised in the greenlet should be caught else where.
        """
        try:
            self.task_greenlet = self.task_greenlet_factory.spawn(super().execute_next_task)
            if self.task_greenlet is not None:  # stupid mypy?!
                self.task_greenlet.join()
        finally:
            self.task_greenlet = None


from .iterator import IteratorScenario


__all__ = [
    'GrizzlyScenario',
    'IteratorScenario',
]
