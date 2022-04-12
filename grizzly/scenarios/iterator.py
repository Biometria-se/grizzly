import traceback

from locust import task
from locust.user.task import LOCUST_STATE_STOPPING
from locust.exception import StopUser, InterruptTaskSet, RescheduleTaskImmediately, RescheduleTask
from gevent.exceptions import GreenletExit
from gevent import sleep as gsleep

from . import GrizzlyScenario
from ..exceptions import RestartScenario
from ..users.base import GrizzlyUser


class IteratorScenario(GrizzlyScenario):
    user: GrizzlyUser

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)

    def run(self) -> None:  # type: ignore
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

                task_count = len(self.tasks)

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        raise StopUser()
                    self.logger.debug(f'executing task {self._task_index} of {task_count}')
                    self.execute_next_task()
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                except RestartScenario:
                    current_task_index = (self._task_index % task_count)
                    self.logger.info(f'restarting scenario at task {current_task_index} of {task_count}')
                    # move locust.user.sequential_task.SequentialTaskSet index pointer the number of tasks left until end, so it will start over
                    tasks_left = task_count - current_task_index
                    self._task_index += tasks_left
                    self.wait()
                else:
                    self.wait()
            except InterruptTaskSet as e:
                self.on_stop()
                if e.reschedule:
                    raise RescheduleTaskImmediately(e.reschedule) from e
                else:
                    raise RescheduleTask(e.reschedule) from e
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

    def stop(self, force: bool = False) -> bool:
        task_count = len(self.tasks)
        counter = 0

        while (self._task_index % task_count) < task_count - 1:
            gsleep(0.1)
            counter += 1

            if counter % 200 == 0:
                self.logger.debug((
                    f'waiting to finish, current task '
                    f'{(self._task_index % task_count)} of {task_count}'
                ))
                counter = 0

        return True

    @task
    def iterator(self) -> None:
        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            self.logger.debug('no iteration data available, abort')
            raise StopUser()

        self.user.add_context(remote_context)
