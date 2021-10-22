from typing import Type

from locust import task
from locust.user.users import User
from locust.exception import StopUser

from . import GrizzlyTasks

class IteratorTasks(GrizzlyTasks):
    def __init__(self, parent: Type[User]) -> None:
        super().__init__(parent=parent)

    @task
    def iterator(self) -> None:
        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            self.logger.debug('no iteration data available, abort')
            raise StopUser()

        self.user.add_context(remote_context)
