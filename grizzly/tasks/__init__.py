import logging

from typing import Callable, Type, List
from os import environ

from locust.user.users import User
from locust.exception import StopUser

from ..context import GrizzlyContext, GrizzlyTasksBase, GrizzlyTask
from ..testdata.communication import TestdataConsumer


class GrizzlyTasks(GrizzlyTasksBase):
    consumer: TestdataConsumer
    tasks: List[Callable[[GrizzlyTasksBase], None]] = []
    logger: logging.Logger = logging.getLogger(__name__)
    grizzly: GrizzlyContext
    wait_time: Callable[[float, float], float]

    def __init__(self, parent: Type[User]) -> None:
        super().__init__(parent=parent)

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

from .iterator import IteratorTasks

__all__ = [
    'GrizzlyTasks',
    'IteratorTasks',
]
