import traceback

from typing import Type

from locust import task
from locust.user.users import User
from locust.user.task import LOCUST_STATE_STOPPING
from locust.exception import StopUser, InterruptTaskSet, RescheduleTaskImmediately, RescheduleTask
from gevent.exceptions import GreenletExit

from . import GrizzlyScenario
from ..exceptions import RestartScenario

class IteratorScenario(GrizzlyScenario):
    def __init__(self, parent: Type[User]) -> None:
        super().__init__(parent=parent)

    def run(self) -> None:
        try:
            self.on_start()
        except InterruptTaskSet as e:
            if e.reschedule:
                raise RescheduleTaskImmediately(e.reschedule).with_traceback(e.__traceback__)
            else:
                raise RescheduleTask(e.reschedule).with_traceback(e.__traceback__)

        while True:
            try:
                if not self._task_queue:
                    self.schedule_task(self.get_next_task())

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        raise StopUser()
                    self.execute_next_task()
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                else:
                    self.wait()
            except InterruptTaskSet as e:
                self.on_stop()
                if e.reschedule:
                    raise RescheduleTaskImmediately(e.reschedule) from e
                else:
                    raise RescheduleTask(e.reschedule) from e
            except RestartScenario:
                # reset locust.user.sequential_task.SequentialTaskSet index pointer to first task
                self._task_index = 0
                self.wait()
            except (StopUser, GreenletExit):
                self.on_stop()
                raise
            except Exception as e:
                self.user.environment.events.user_error.fire(user_instance=self, exception=e, tb=e.__traceback__)
                if self.user.environment.catch_exceptions:
                    self.logger.error("%s\n%s", e, traceback.format_exc())
                    self.wait()
                else:
                    raise

    @task
    def iterator(self) -> None:
        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            self.logger.debug('no iteration data available, abort')
            raise StopUser()

        self.user.add_context(remote_context)
