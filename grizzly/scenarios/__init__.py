import logging

from typing import Callable
from os import environ

from locust.exception import StopUser
from locust.user.sequential_taskset import SequentialTaskSet

from ..context import GrizzlyContext
from ..types import GrizzlyTask
from ..testdata.communication import TestdataConsumer
from ..users.base import GrizzlyUser


class GrizzlyScenario(SequentialTaskSet):
    consumer: TestdataConsumer
    logger: logging.Logger = logging.getLogger(__name__)
    grizzly: GrizzlyContext
    wait_time: Callable[[float, float], float]
    user: GrizzlyUser

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)
        self.grizzly = GrizzlyContext()

    @classmethod
    def add_scenario_task(cls, task: GrizzlyTask) -> None:
        cls.tasks.append(task.implementation())

    def on_start(self) -> None:
        producer_address = environ.get('TESTDATA_PRODUCER_ADDRESS', None)
        if producer_address is not None:
            self.consumer = TestdataConsumer(address=producer_address)
        else:
            self.logger.error('no address to testdata producer specified')
            raise StopUser()

    def on_stop(self) -> None:
        self.logger.debug(f'stopping consumer for {self.__class__.__name__}')
        self.consumer.stop()


from .iterator import IteratorScenario


__all__ = [
    'GrizzlyScenario',
    'IteratorScenario',
]
