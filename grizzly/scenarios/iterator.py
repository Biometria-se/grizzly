import traceback

from typing import TYPE_CHECKING, Optional, Dict
from time import perf_counter
from json import dumps as jsondumps

from locust import task
from locust.user.task import LOCUST_STATE_STOPPING, LOCUST_STATE_RUNNING
from locust.exception import StopUser, InterruptTaskSet, RescheduleTaskImmediately, RescheduleTask
from locust.stats import StatsEntry
from gevent.exceptions import GreenletExit

from grizzly.types import RequestType, ScenarioState

from ..exceptions import RestartScenario, StopScenario
from . import GrizzlyScenario

if TYPE_CHECKING:
    from ..users.base import GrizzlyUser


class IteratorScenario(GrizzlyScenario):
    user: 'GrizzlyUser'

    start: Optional[float]
    task_count: int
    stats: StatsEntry
    behave_steps: Dict[int, str]

    current_task_index: int = 0

    def __init__(self, parent: 'GrizzlyUser') -> None:
        super().__init__(parent=parent)

        self.start = None
        self.task_count = len(self.tasks)
        self.stats = self.user.environment.stats.get(self.user._scenario.locust_name, RequestType.SCENARIO())
        self.behave_steps = self.user._scenario.tasks.behave_steps.copy()

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
                self.current_task_index = (self._task_index % self.task_count)

                if not self._task_queue:
                    self.schedule_task(self.get_next_task())

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        raise StopUser()

                    step = self.behave_steps.get(self.current_task_index + 1, 'unknown')
                    self.logger.debug(f'executing task {self.current_task_index+1} of {self.task_count}: {step}')
                    self.execute_next_task()
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                except RestartScenario:
                    self.logger.info(f'restarting scenario at task {self.current_task_index+1} of {self.task_count}')
                    # move locust.user.sequential_task.SequentialTaskSet index pointer the number of tasks left until end, so it will start over
                    tasks_left = self.task_count - (self._task_index % self.task_count)
                    self._task_index += tasks_left

                    self.stats.log_error(None)
                    self.wait()
                else:
                    self.wait()
            except InterruptTaskSet as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    self.on_stop()
                    self.start = None

                    if e.reschedule:
                        raise RescheduleTaskImmediately(e.reschedule) from e
                    else:
                        raise RescheduleTask(e.reschedule) from e
                else:
                    self.wait()
            except (StopScenario, StopUser, GreenletExit) as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    self.logger.debug(f'{self.user._scenario_state=}, {self.user._state=}, {e=}')
                    has_error = False

                    if isinstance(e, StopScenario):
                        self.start = None

                    # unexpected exit of scenario, log as error
                    if not isinstance(e, StopScenario) and self.user._state != LOCUST_STATE_STOPPING:
                        has_error = True
                    elif not isinstance(e, StopUser):
                        e = StopUser()

                    self.iteration_stop(has_error=has_error)
                    self.on_stop()

                    raise e
                else:
                    self.wait()
            except Exception as e:
                self.iteration_stop(has_error=True)
                self.user.environment.events.user_error.fire(user_instance=self.user, exception=e, tb=e.__traceback__)
                if self.user.environment.catch_exceptions:
                    self.logger.error("%s\n%s", e, traceback.format_exc())
                    self.wait()
                else:
                    raise

    def iteration_stop(self, has_error: bool = False) -> None:
        if self.start is not None:
            response_time = int((perf_counter() - self.start) * 1000)

            response_length = (self.current_task_index % self.task_count) + 1

            self.stats.log(response_time, response_length)
            if has_error:
                self.stats.log_error(None)

    def wait(self) -> None:
        if self.user._scenario_state == ScenarioState.STOPPING:
            if self.current_task_index < self.task_count - 1:
                self.logger.debug(f'not finished with scenario, currently at task {self.current_task_index+1} of {self.task_count}, let me be!')
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
        if self.start is not None:
            response_time = int((perf_counter() - self.start) * 1000)
            self.user.environment.events.request.fire(
                request_type=RequestType.SCENARIO(),
                name=self.user._scenario.locust_name,
                response_time=response_time,
                response_length=self.task_count,
                context=self.user._context,
                exception=None,
            )

        # scenario timer
        self.start = perf_counter()
        # fetching testdata timer
        start = perf_counter()

        remote_context = self.consumer.request(self.__class__.__name__)

        if remote_context is None:
            self.logger.debug('no iteration data available, stop scenario')
            raise StopScenario()

        response_time = int((perf_counter() - start) * 1000)

        self.user.environment.events.request.fire(
            request_type=RequestType.TESTDATA(),
            name=self.user._scenario.locust_name,
            response_time=response_time,
            response_length=len(jsondumps(remote_context)),
            context=self.user._context,
            exception=None,
        )

        self.user.add_context(remote_context)
