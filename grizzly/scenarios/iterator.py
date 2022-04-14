import traceback

from typing import TYPE_CHECKING

from locust import task
from locust.user.task import LOCUST_STATE_STOPPING, LOCUST_STATE_RUNNING
from locust.exception import StopUser, InterruptTaskSet, RescheduleTaskImmediately, RescheduleTask
from gevent.exceptions import GreenletExit

from ..types import ScenarioState
from ..exceptions import RestartScenario
from . import GrizzlyScenario

if TYPE_CHECKING:
    from ..users.base import GrizzlyUser


class IteratorScenario(GrizzlyScenario):
    user: 'GrizzlyUser'

    def __init__(self, parent: 'GrizzlyUser') -> None:
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
                current_task_index = (self._task_index % task_count)

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        raise StopUser()
                    if self.user.scenario_state != ScenarioState.STOPPING:
                        self.logger.debug(f'executing task {current_task_index+1} of {task_count}')
                    self.execute_next_task()
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                except RestartScenario:
                    self.logger.info(f'restarting scenario at task {current_task_index+1} of {task_count}')
                    # move locust.user.sequential_task.SequentialTaskSet index pointer the number of tasks left until end, so it will start over
                    tasks_left = task_count - current_task_index
                    self._task_index += tasks_left
                    self.wait()
                else:
                    self.wait()
            except InterruptTaskSet as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    self.on_stop()
                    if e.reschedule:
                        raise RescheduleTaskImmediately(e.reschedule) from e
                    else:
                        raise RescheduleTask(e.reschedule) from e
                else:
                    self.wait()
            except (StopUser, GreenletExit):
                if self.user._scenario_state != ScenarioState.STOPPING:
                    self.on_stop()
                    raise
                else:
                    self.wait()
            except Exception as e:
                self.user.environment.events.user_error.fire(user_instance=self, exception=e, tb=e.__traceback__)
                if self.user.environment.catch_exceptions:
                    self.logger.error("%s\n%s", e, traceback.format_exc())
                    self.wait()
                else:
                    raise

    def wait(self) -> None:
        if self.user._scenario_state == ScenarioState.STOPPING:
            task_count = len(self.tasks)
            current_task_index = self._task_index % task_count

            if current_task_index < task_count - 1:
                self.logger.debug(f'not finished with scenario, currently at task {current_task_index+1} of {task_count}, let me be!')
                self.user._state = LOCUST_STATE_RUNNING
                self._sleep(self.wait_time())
                self.user._state = LOCUST_STATE_RUNNING
                return
            else:
                self.logger.debug("okay, I'm done with my running tasks now")
                self.user._state = LOCUST_STATE_STOPPING
                self.user.scenario_state = ScenarioState.STOPPED

        super().wait()

    @task
    def iterator(self) -> None:
        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            self.logger.debug('no iteration data available, abort')
            raise StopUser()

        self.user.add_context(remote_context)
