"""@anchor pydoc:grizzly.scenarios.iterator Iterator
This module contains the iterator scenario, it is a load testing scenario with fixed load and will iterate as many times
as it is told todo so.

See {@pylink grizzly.steps.scenario.setup.step_setup_iterations}.
"""
from __future__ import annotations

from json import dumps as jsondumps
from time import perf_counter
from typing import TYPE_CHECKING, ClassVar, Dict, Optional

from gevent import sleep as gsleep
from gevent.exceptions import GreenletExit
from locust import task
from locust.exception import InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately
from locust.user.task import LOCUST_STATE_RUNNING, LOCUST_STATE_STOPPING

from grizzly.exceptions import RestartScenario, StopScenario
from grizzly.types import RequestType, ScenarioState
from grizzly.types.locust import StopUser

from . import GrizzlyScenario

if TYPE_CHECKING:  # pragma: no cover
    from locust.stats import StatsEntry

    from grizzly.tasks import GrizzlyTask
    from grizzly.users import GrizzlyUser


class IteratorScenario(GrizzlyScenario):
    user: GrizzlyUser

    start: Optional[float]
    task_count: ClassVar[int] = 0
    stats: StatsEntry
    behave_steps: ClassVar[Dict[int, str]]
    pace_time: ClassVar[Optional[str]] = None  # class variable injected by `grizzly.utils.create_scenario_class_type`

    _prefetch: bool

    current_task_index: int = 0

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)

        self.start = None
        self.__class__.task_count = len(self.tasks)
        self.__class__.behave_steps = self.user._scenario.tasks.behave_steps.copy()
        self.stats = self.user.environment.stats.get(self.user._scenario.locust_name, RequestType.SCENARIO())
        self._prefetch = False

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        """IteratorScenario.pace *must* be the last task for this scenario."""
        cls.tasks.insert(-1, task_factory())

    def run(self) -> None:  # type: ignore[misc]  # noqa: C901, PLR0912, PLR0915
        """Override `locust.user.sequential_taskset.SequentialTaskSet.run` so we can have some control over how a scenario is executed.
        Includes handling of `StopScenario` (we want all tasks to complete before user is allowed to stop) and
        `RestartScenario` (if there's an error in a scenario, we might want to start over from task 0) exceptions.
        """
        try:
            self.on_start()
        except InterruptTaskSet as e:
            if e.reschedule:
                raise RescheduleTaskImmediately(e.reschedule).with_traceback(e.__traceback__) from e

            raise RescheduleTask(e.reschedule).with_traceback(e.__traceback__) from e
        except Exception as e:
            if not isinstance(e, StopScenario):
                self.logger.exception('on_start failed')
                response_time = int((perf_counter() - (self.start or 0)) * 1000)
                self.user.environment.events.request.fire(
                    request_type=RequestType.SCENARIO(),
                    name=self.user._scenario.locust_name,
                    response_time=response_time,
                    response_length=self.task_count,
                    context=self.user._context,
                    exception=StopUser('on_start failed for {self.user.__class__.__name__}'),
                )
            raise StopUser from e

        while True:
            execute_task_logged = False
            try:
                self.current_task_index = (self._task_index % self.task_count)

                if not self._task_queue:
                    self.schedule_task(self.get_next_task())

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        raise StopUser

                    try:
                        step = self.behave_steps.get(self.current_task_index + 1, self._task_queue[0].__name__)
                    except Exception:
                        step = 'unknown'

                    self.logger.debug('executing task %d of %d: %s', self.current_task_index+1, self.task_count, step)
                    try:
                        self.execute_next_task()
                    except Exception as e:
                        if not isinstance(e, StopScenario):
                            self.logger.exception('task %d of %d: %s, failed: %s', self.current_task_index+1, self.task_count, step, e.__class__.__name__)
                            execute_task_logged = True
                        raise
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                except RestartScenario as e:
                    # tasks will wrap the grizzly.exceptions.StopScenario thrown when aborting to what ever
                    # the scenario has specified todo when failing, we must force it to stop scenario
                    if self.abort:
                        raise StopUser from e

                    self.logger.info('restarting scenario at task %d of %d', self.current_task_index+1, self.task_count)
                    # move locust.user.sequential_task.SequentialTaskSet index pointer the number of tasks left until end, so it will start over
                    tasks_left = self.task_count - (self._task_index % self.task_count)
                    self._task_index += tasks_left
                    self.logger.debug('%d tasks in queue', len(self._task_queue))
                    self._task_queue.clear()  # we should remove any scheduled tasks when restarting

                    self.stats.log_error(None)
                    self.wait()
                else:
                    self.wait()
            except InterruptTaskSet as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    try:
                        self.on_stop()
                    except:
                        self.logger.exception('on_stop failed')
                    self.start = None

                    if e.reschedule:
                        raise RescheduleTaskImmediately(e.reschedule) from e
                    raise RescheduleTask(e.reschedule) from e

                self.wait()
            except (StopScenario, StopUser, GreenletExit) as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    scenario_state = self.user._scenario_state.name if self.user._scenario_state is not None else 'UNKNOWN'
                    self.logger.debug('scenario_state=%s, user_state=%s, exception=%r', scenario_state, self.user._state, e)
                    has_error = False

                    if isinstance(e, StopScenario):
                        self.start = None

                    exception = e.__class__

                    # unexpected exit of scenario, log as error
                    if not isinstance(e, StopScenario) and self.user._state != LOCUST_STATE_STOPPING:
                        has_error = True
                    elif not isinstance(e, StopUser):
                        exception = StopUser

                    self.iteration_stop(has_error=has_error)

                    try:
                        self.on_stop()
                    except:
                        self.logger.exception('on_stop failed')

                    # to avoid spawning of a new user, we should wait until spawning is complete
                    # if we abort too soon, locust will see that there are too few users, and spawn
                    # another one
                    if has_error:
                        count = 0
                        while not self.grizzly.state.spawning_complete:
                            if count % 10 == 0:
                                self.logger.debug('spawning not complete, wait')
                                if count > 0:
                                    count = 0
                            gsleep(0.1)
                            count += 1

                    raise exception from e

                self.wait()
            except Exception as e:
                self.iteration_stop(has_error=True)
                self.user.environment.events.user_error.fire(user_instance=self.user, exception=e, tb=e.__traceback__)
                if self.user.environment.catch_exceptions:
                    if not execute_task_logged:
                        self.logger.exception('unhandled exception: %s', e.__class__.__qualname__)
                    self.wait()
                else:
                    raise

    def iteration_stop(self, *, has_error: bool = False) -> None:
        if self.start is not None:
            response_time = int((perf_counter() - self.start) * 1000)
            response_length = (self.current_task_index % self.task_count) + 1

            self.stats.log(response_time, response_length)
            if has_error:
                self.stats.log_error(None)

    def wait(self) -> None:
        if self.user._scenario_state == ScenarioState.STOPPING:
            if self.current_task_index < self.task_count - 1 and not self.abort:
                self.logger.debug('not finished with scenario, currently at task %d of %d, let me be!', self.current_task_index+1, self.task_count)
                self.user._state = LOCUST_STATE_RUNNING
                self._sleep(self.wait_time())
                self.user._state = LOCUST_STATE_RUNNING
                return

            if not self.abort:
                self.logger.debug("okay, I'm done with my running tasks now")
            else:
                self.logger.debug("since you're asking nicely")

            self.user._state = LOCUST_STATE_STOPPING
            self.user.scenario_state = ScenarioState.STOPPED

        super().wait()

    def prefetch(self) -> None:
        self.iterator(prefetch=True)

    @task
    def iterator(self, *, prefetch: Optional[bool] = False) -> None:
        # if data has been prefetched, use it for the first iteration,
        # then ask for new data the second iteration
        if self._prefetch:
            self._prefetch = False
            return

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

        remote_context = self.consumer.testdata(self.user._scenario.class_name)

        if remote_context is None:
            self.logger.debug('no iteration data available, stop scenario')
            raise StopScenario

        response_time = int((perf_counter() - self.start) * 1000)

        self.user.environment.events.request.fire(
            request_type=RequestType.TESTDATA(),
            name=self.user._scenario.locust_name,
            response_time=response_time,
            response_length=len(jsondumps(remote_context)),
            context=self.user._context,
            exception=None,
        )

        self.user.add_context(remote_context)

        # next call to this method should be ignored, first iteration should use the prefetched data
        if prefetch:
            self._prefetch = True

    # <!-- user tasks will be injected between these two static tasks -->

    @task
    def pace(self) -> None:
        """Last task in this scenario, if self.pace_time is set.
        This is ensured by `grizzly.scenarios.GrizzlyScenario.populate`.
        """
        if self.pace_time is None:
            return

        exception: Optional[Exception] = None
        response_length: int = 0

        try:
            start = perf_counter()
            try:
                value = float(self.render(self.pace_time))
            except ValueError as ve:
                message = f'{self.pace_time} does not render to a number'
                raise ValueError(message) from ve

            if self.start is not None:
                pace_correction = (start - self.start)

                if (pace_correction * 1000) < value:
                    self.logger.debug('keeping pace by sleeping %d milliseconds', pace_correction * 1000)
                    gsleep((value / 1000) - pace_correction)
                    response_length = 1
                else:
                    self.logger.error('pace falling behind, currently at %d milliseconds', abs(pace_correction * 1000))
                    message = 'pace falling behind'
                    raise RuntimeError(message)
        except Exception as e:
            exception = e
        finally:
            done = perf_counter()
            response_time = int((done - start) * 1000)

            self.user.environment.events.request.fire(
                request_type=RequestType.PACE(),
                name=self.user._scenario.locust_name,
                response_time=response_time,
                response_length=response_length,
                context=self.user._context,
                exception=exception,
            )

            if exception is not None and isinstance(exception, ValueError):
                raise StopUser from exception
