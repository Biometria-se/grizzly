from typing import List, Callable, Any, Type, Union
from os import environ
from logging import Logger

from locust import task
from locust.user.users import User
from locust.user.sequential_taskset import SequentialTaskSet
from locust.exception import StopUser
from gevent import sleep as gsleep

from ..context import LocustContext, RequestContext
from ..testdata.communication import TestdataConsumer

from . import logger

class TrafficIteratorTasks(SequentialTaskSet):
    locust_context: LocustContext
    wait_time: Callable
    consumer: TestdataConsumer
    tasks: List[Callable] = []
    logger: Logger

    def __init__(self, parent: Type[User]) -> None:
        super().__init__(parent=parent)
        self.logger = logger

    @classmethod
    def add_scenario_task(cls, task: Union[RequestContext, float]) -> None:
        def request_task(request: RequestContext) -> Callable[[TrafficIteratorTasks], Any]:
            def _request_task(self: 'TrafficIteratorTasks') -> Any:
                return self.user.request(request)

            return _request_task

        def wait_task(wait_time: float) -> Callable[[TrafficIteratorTasks], Any]:
            def _wait_task(self: 'TrafficIteratorTasks') -> Any:
                self.logger.debug(f'waiting for {wait_time} seconds')
                gsleep(wait_time)
                self.logger.debug(f'done waiting for {wait_time} seconds')

            return _wait_task

        if isinstance(task, RequestContext):
            cls.tasks.append(request_task(task))
        elif isinstance(task, float):
            cls.tasks.append(wait_task(task))

    def on_start(self) -> None:
        producer_address = environ.get('TESTDATA_PRODUCER_ADDRESS', None)
        if producer_address is not None:
            self.consumer = TestdataConsumer(address=producer_address)
        else:
            logger.warning('no address to testdata producer specified')
            raise StopUser()

    def on_stop(self) -> None:
        logger.debug(f'stopping consumer for {self.__class__.__name__}')
        self.consumer.stop()

    @task
    def iterator(self) -> None:
        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            logger.debug('no iteration data available, abort')
            raise StopUser()

        self.user.add_context(remote_context)
