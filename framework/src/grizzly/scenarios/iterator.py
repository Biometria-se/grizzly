"""Module contains the iterator scenario, it is a load testing scenario with fixed load and will iterate as many times
as it is told todo so.

See [Iterations][grizzly.steps.scenario.setup.step_setup_iterations].
"""

from __future__ import annotations

from contextlib import suppress
from json import dumps as jsondumps
from math import ceil
from random import uniform
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar

from gevent import sleep as gsleep
from gevent.exceptions import GreenletExit
from locust import task
from locust.exception import InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately
from locust.user.task import LOCUST_STATE_RUNNING, LOCUST_STATE_STOPPING

from grizzly.exceptions import RestartIteration, RestartScenario, RetryTask, StopScenario
from grizzly.types import RequestType, ScenarioState
from grizzly.types.locust import StopUser

from . import GrizzlyScenario

if TYPE_CHECKING:  # pragma: no cover
    from locust.stats import StatsEntry

    from grizzly.tasks import GrizzlyTask
    from grizzly.users import GrizzlyUser


NUMBER_TO_WORD: dict[int, str] = {
    1: '1st',
    2: '2nd',
    3: '3rd',
    4: '4th',
}


class IteratorScenario(GrizzlyScenario):
    user: GrizzlyUser

    start: float | None
    task_count: ClassVar[int] = 0
    stats: StatsEntry
    behave_steps: ClassVar[dict[int, str]]
    pace_time: ClassVar[str | None] = None  # class variable injected by `grizzly.utils.create_scenario_class_type`

    _prefetch: bool
    _has_waited: bool

    current_task_index: int = 0

    def __init__(self, parent: GrizzlyUser) -> None:
        super().__init__(parent=parent)

        self.start = None
        self.__class__.task_count = len(self.tasks)
        self.__class__.behave_steps = self.user._scenario.tasks.behave_steps.copy()
        self.stats = self.user.environment.stats.get(self.user._scenario.locust_name, RequestType.SCENARIO())
        self._prefetch = False
        self._on_quitting = False
        self._has_waited = False

    def on_quitting(self, *_args: Any, **kwargs: Any) -> None:
        super().on_quitting(*_args, **kwargs)

        # test has been aborted, log "request" failures for SCENARIO
        if self.abort.is_set() and not self._on_quitting:
            self.iteration_stop(error=StopScenario())
            self._on_quitting = True

    @classmethod
    def populate(cls, task_factory: GrizzlyTask) -> None:
        """IteratorScenario.pace *must* be the last task for this scenario."""
        cls.tasks.insert(-1, task_factory())

    def run(self) -> None:  # type: ignore[misc]  # noqa: C901, PLR0912, PLR0915
        """Override `locust.user.sequential_taskset.SequentialTaskSet.run` so we can have some control over how a
        scenario is executed. Includes handling of `StopScenario` (we want all tasks to complete before user is
        allowed to stop) and `RestartScenario` (if there's an error in a scenario, we might want to start over from
        task 0) exceptions.
        """
        try:
            start = perf_counter()
            self.on_start()
        except InterruptTaskSet as e:
            if e.reschedule:
                raise RescheduleTaskImmediately(e.reschedule).with_traceback(e.__traceback__) from e

            raise RescheduleTask(e.reschedule).with_traceback(e.__traceback__) from e
        except Exception as e:
            if not isinstance(e, StopScenario):
                self.logger.exception('scenario on_start failed')
                response_time = int((perf_counter() - start) * 1000)
                self.user.environment.events.request.fire(
                    request_type=RequestType.SCENARIO(),
                    name=self.user._scenario.locust_name,
                    response_time=response_time,
                    response_length=self.task_count,
                    context=self.user._context,
                    exception=StopUser(f'on_start failed for {self.user._scenario.locust_name}: {e}'),
                )

            with suppress(Exception):
                self.on_stop()

            self.grizzly.state.spawning_complete.wait()

            raise StopUser from e

        iteration_restarted_count = 0

        while True:
            execute_task_logged = False
            try:
                self.current_task_index = self._task_index % self.task_count

                if not self._task_queue:
                    self.schedule_task(self.get_next_task())

                try:
                    if self.user._state == LOCUST_STATE_STOPPING:
                        on_stop_exception: Exception | None = None

                        try:
                            self.on_stop()
                        except Exception as e:
                            on_stop_exception = e
                        raise StopUser from on_stop_exception

                    try:
                        step = self.behave_steps.get(self.current_task_index + 1, self._task_queue[0].__name__)
                    except Exception:
                        step = 'unknown'

                    retries = 0
                    while True:
                        try:
                            self.execute_next_task(self.current_task_index + 1, self.task_count, step)
                            break
                        except RetryTask as e:
                            retries += 1

                            if retries >= 3:
                                message = f'task {self.current_task_index + 1} of {self.task_count} failed after {retries} retries: {step}'
                                self.logger.error(message)  # noqa: TRY400

                                default_exception = self.user._scenario.failure_handling.get(None, None)

                                # default failure handling
                                if default_exception is not None:
                                    raise default_exception from e

                                break

                            sleep_time = retries * uniform(1.0, 5.0)  # noqa: S311
                            message = (
                                f'task {self.current_task_index + 1} of {self.task_count} will execute a {NUMBER_TO_WORD[retries + 1]} time in {sleep_time:.2f} seconds: {step}'
                            )
                            self.logger.warning(message)

                            gsleep(sleep_time)  # random back-off time
                            self.wait()

                            # step back counter, and re-schedule the same task again
                            self._task_index -= 1
                            self.schedule_task(self.get_next_task(), first=True)

                            continue
                        except Exception as e:
                            if not isinstance(e, StopScenario):
                                execute_task_logged = True
                            raise
                except RescheduleTaskImmediately:
                    pass
                except RescheduleTask:
                    self.wait()
                except (RestartIteration, RestartScenario) as e:
                    if isinstance(e, RestartIteration):
                        self._prefetch = True  # same as when prefetching testdata, just use what we have
                        restart_type = 'iteration'

                        iteration_restarted_count += 1
                        _, total_iterations = self.user._context.get('__iteration__', (None, None))

                        if e.max_retries is None and total_iterations is not None:
                            user_fixed_count = self.user._scenario.user.fixed_count
                            allowed_scenario_restarts = max(1, ceil(total_iterations / user_fixed_count)) if user_fixed_count is not None else total_iterations
                            error_message = (
                                f'iteration has been restarted {iteration_restarted_count} times, and scenario should run for {total_iterations} iterations by '
                                f'{user_fixed_count} users, aborting'
                            )
                        else:
                            allowed_scenario_restarts = e.max_retries or 3
                            error_message = (
                                f'iteration has been restarted {iteration_restarted_count} times, which is the maximum allowed of restart '
                                f'({allowed_scenario_restarts}) for this scenario'
                            )

                        # do not allow unlimited number of iteration restarts
                        if iteration_restarted_count >= allowed_scenario_restarts:
                            self.user.logger.error(error_message)  # noqa: TRY400
                            raise StopUser from e
                    else:
                        restart_type = 'scenario'

                    # tasks will wrap the grizzly.exceptions.StopScenario thrown when aborting to what ever
                    # the scenario has specified todo when failing, we must force it to stop scenario
                    if self.abort.is_set():
                        self.on_stop()
                        raise StopUser from e

                    self.logger.info('restarting %s at task %d of %d', restart_type, self.current_task_index + 1, self.task_count)
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
                    self.start = None

                    if e.reschedule:
                        raise RescheduleTaskImmediately(e.reschedule) from e
                    raise RescheduleTask(e.reschedule) from e

                self.wait()
            except (StopScenario, StopUser, GreenletExit) as e:
                if self.user._scenario_state != ScenarioState.STOPPING:
                    scenario_state = self.user._scenario_state.name if self.user._scenario_state is not None else 'UNKNOWN'
                    self.logger.debug('scenario_state=%s, user_state=%s, exception=%r', scenario_state, self.user._state, e)

                    if isinstance(e, StopScenario):
                        self.start = None

                    exception = e.__class__

                    # unexpected exit of scenario, log as error
                    if not isinstance(e, StopUser):
                        exception = StopUser

                    self.iteration_stop(error=e)
                    self.on_stop()

                    # to avoid spawning of a new user, we should wait until spawning is complete
                    # if we abort too soon, locust will see that there are too few users, and spawn
                    # another one
                    self.grizzly.state.spawning_complete.wait()

                    self.logger.debug('stopping scenario with %r', exception)
                    raise exception from e

                self.wait()
            except Exception as e:
                self.iteration_stop(error=e)
                self.user.environment.events.user_error.fire(user_instance=self.user, exception=e, tb=e.__traceback__)
                if self.user.environment.catch_exceptions:
                    if not execute_task_logged:
                        self.logger.exception('unhandled exception: %s', e.__class__.__qualname__)
                    self.wait()
                else:
                    self.on_stop()
                    raise

    def iteration_stop(self, *, error: Exception | None) -> None:
        if self.start is not None:
            response_time = int((perf_counter() - self.start) * 1000)
            response_length = (self.current_task_index % self.task_count) + 1

            if not isinstance(error, StopScenario):
                self.stats.log(response_time, response_length)
                self.start = None

            if error is not None:
                self.stats.log_error(None)

    def wait(self) -> None:
        if self.user._scenario_state == ScenarioState.STOPPING:
            if self.current_task_index < self.task_count - 1 and not self.abort.is_set():
                self.logger.debug('not finished with scenario, currently at task %d of %d, let me be!', self.current_task_index + 1, self.task_count)
                self.user._state = LOCUST_STATE_RUNNING
                self._sleep(self.wait_time())
                self.user._state = LOCUST_STATE_RUNNING
                return

            if not self.abort.is_set():
                self.logger.debug("okay, I'm done with my running tasks now")
            else:
                self.logger.debug("since you're asking nicely")

            self.user._state = LOCUST_STATE_STOPPING
            self.user.scenario_state = ScenarioState.STOPPED

        super().wait()

    def prefetch(self) -> None:
        self.iterator(prefetch=True)

    @task
    def iterator(self, *, prefetch: bool = False) -> None:
        # if data has been prefetched, use it for the first iteration,
        # then ask for new data the second iteration
        if self._prefetch:
            self._prefetch = False
            return

        if not prefetch:
            self.on_iteration()

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

        remote_context = self.consumer.testdata()

        if remote_context is None:
            self.logger.debug('no iteration data available, stop scenario')
            raise StopScenario

        response_time = int((perf_counter() - self.start) * 1000)

        self.user.environment.events.request.fire(
            request_type=RequestType.TESTDATA(),
            name=self.user._scenario.locust_name,
            response_time=response_time,
            response_length=len(jsondumps(remote_context).encode('utf-8')),
            context=self.user._context,
            exception=None,
        )

        self.user.add_context(remote_context)

        # next call to this method should be ignored, first iteration should use the prefetched data
        if prefetch:
            self._prefetch = True

        # wait for locust saying spawning is complete, before starting to execute tasks
        spawn_timeout = self.grizzly.setup.wait_for_spawning_complete
        if not prefetch and spawn_timeout is not None and not self._has_waited:
            spawn_timeout = None if spawn_timeout < 0 else spawn_timeout
            self.user.logger.info('waiting for spawning complete')
            start = perf_counter()

            with suppress(Exception):
                self.grizzly.state.spawning_complete.wait(timeout=spawn_timeout)

            self._has_waited = True
            response_time = int((perf_counter() - start) * 1000)

            self.user.environment.events.request.fire(
                request_type='SPWN',
                name=self.user._scenario.locust_name,
                response_time=response_time,
                response_length=0,
                context=self.user._context,
                exception=None,
            )

    # <!-- user tasks will be injected between these two static tasks -->

    @task
    def pace(self) -> None:
        """Last task in this scenario, if self.pace_time is set.
        This is ensured by `grizzly.scenarios.iterator.IteratorScenario.populate`.
        """
        if self.pace_time is None:
            return

        exception: Exception | None = None
        response_length: int = 0

        try:
            start = perf_counter()
            try:
                value = float(self.user.render(self.pace_time))
            except ValueError as ve:
                message = f'{self.pace_time} does not render to a number'
                raise ValueError(message) from ve

            if self.start is not None:
                pace_correction = start - self.start

                if (pace_correction * 1000) < value:
                    self.logger.debug('scenario keeping pace by sleeping %d milliseconds', pace_correction * 1000)
                    gsleep((value / 1000) - pace_correction)
                    response_length = 1
                else:
                    self.logger.error('scenario pace falling behind, currently at %d milliseconds expecting %d milliseconds', abs(pace_correction * 1000), int(value))
                    message = f'pace falling behind, iteration takes longer than {value} milliseconds'
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
