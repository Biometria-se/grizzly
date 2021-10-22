import logging

from typing import Union, Callable, Any, Type, List
from os import environ

from locust.user.sequential_taskset import SequentialTaskSet
from locust.user.users import User
from locust.exception import StopUser
from gevent import sleep as gsleep

from ..context import LocustContext
from ..task import RequestTask
from ..testdata.communication import TestdataConsumer


class GrizzlyTasks(SequentialTaskSet):
    consumer: TestdataConsumer
    tasks: List[Callable] = []
    logger: logging.Logger = logging.getLogger(__name__)
    locust_context: LocustContext
    wait_time: Callable

    def __init__(self, parent: Type[User]) -> None:
        super().__init__(parent=parent)

    @classmethod
    def add_scenario_task(cls, task: Union[RequestTask, float]) -> None:
        def request_task(request: RequestTask) -> Callable[[GrizzlyTasks], Any]:
            def _request_task(self: 'IteratorTasks') -> Any:
                return self.user.request(request)

            return _request_task

        def wait_task(wait_time: float) -> Callable[[GrizzlyTasks], Any]:
            def _wait_task(self: 'IteratorTasks') -> Any:
                self.logger.debug(f'waiting for {wait_time} seconds')
                gsleep(wait_time)
                self.logger.debug(f'done waiting for {wait_time} seconds')

            return _wait_task

        if isinstance(task, RequestTask):
            cls.tasks.append(request_task(task))
        elif isinstance(task, float):
            cls.tasks.append(wait_task(task))

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

