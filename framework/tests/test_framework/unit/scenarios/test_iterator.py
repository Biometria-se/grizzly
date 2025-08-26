"""Unit tests for grizzly.scenarios.iterator."""

from __future__ import annotations

import logging
from contextlib import suppress
from os import environ
from types import FunctionType
from typing import TYPE_CHECKING, Any, cast

import pytest
from gevent.lock import Semaphore
from grizzly.exceptions import RestartIteration, RestartScenario, RetryTask, StopScenario, TaskTimeoutError
from grizzly.scenarios import IteratorScenario
from grizzly.tasks import ExplicitWaitTask, LogMessageTask, grizzlytask
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform
from grizzly.types import ScenarioState, StrDict
from locust.exception import InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately, StopUser
from locust.user.sequential_taskset import SequentialTaskSet
from locust.user.task import LOCUST_STATE_RUNNING, LOCUST_STATE_STOPPING

from test_framework.helpers import ANY, RequestCalled, TestTask, regex

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.locust import LocalRunner

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


def filter_messages(messages: list[str]) -> list[str]:
    return [
        message
        for message in messages
        if not any(ignore in message for ignore in ['instance variable=', 'CPU usage above', 'checking if heartbeat', 'timeout: ', 'handling exception of type'])
    ]


class TestIterationScenario:
    def test_initialize(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)
        assert isinstance(parent, IteratorScenario)
        assert issubclass(parent.__class__, SequentialTaskSet)
        assert parent.pace_time is None

    def test_populate(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)
        request = grizzly_fixture.request_task.request
        request.endpoint = '/api/v1/test'
        request.source = None

        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        parent.__class__.populate(request)
        assert isinstance(parent, IteratorScenario)
        assert len(parent.tasks) == 3

        task_method = parent.tasks[-2]
        parent.user._scenario.failure_handling.update({None: StopUser})

        assert callable(task_method)
        with pytest.raises(StopUser):
            task_method(parent)

        request_spy.assert_called_once_with(
            request_type='POST',
            name='001 IteratorScenario',
            response_time=ANY(int),
            response_length=0,
            context={
                'log_all_requests': False,
                'host': '',
                'metadata': None,
                'user': id(parent.user),
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(RequestCalled),
        )

        def generate_mocked_wait(sleep_time: float) -> None:
            def mocked_wait(time: float) -> None:
                assert sleep_time == time

            mocker.patch(
                'grizzly.tasks.wait_explicit.gsleep',
                mocked_wait,
            )

        generate_mocked_wait(1.5)
        parent.__class__.populate(ExplicitWaitTask(time_expression='1.5'))
        assert len(parent.tasks) == 4

        task_method = parent.tasks[-2]
        assert callable(task_method)
        task_method(parent)

        parent.__class__.populate(LogMessageTask(message='hello {{ world }}'))
        assert len(parent.tasks) == 5

        logger_spy = mocker.spy(parent.user.logger, 'info')

        task_method = parent.tasks[-2]
        assert callable(task_method)
        task_method(parent)

        assert logger_spy.call_count == 1
        args, _ = logger_spy.call_args_list[0]
        assert args[0] == 'hello {{ world }}'

        parent.user.set_variable('world', 'world!')

        task_method(parent)

        assert logger_spy.call_count == 2
        args, _ = logger_spy.call_args_list[1]
        assert args[0] == 'hello world!'

        first_task = parent.tasks[0]
        assert isinstance(first_task, FunctionType)
        assert first_task.__name__ == 'iterator'

        last_task = parent.tasks[-1]
        assert isinstance(last_task, FunctionType)
        assert last_task.__name__ == 'pace'

    def test_prefetch(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        iterator_mock = mocker.patch.object(parent, 'iterator', return_value=None)

        parent.prefetch()

        iterator_mock.assert_called_once_with(prefetch=True)

    def test_iterator(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        grizzly = grizzly_fixture.grizzly

        assert isinstance(parent, IteratorScenario)
        assert not parent._prefetch

        parent.__class__._consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        on_iteration_mock = mocker.patch.object(parent, 'on_iteration', return_value=None)
        request_fire_mock = mocker.patch.object(parent.user.environment.events.request, 'fire', return_value=None)
        spawning_complete_mock = mocker.patch.object(grizzly.state, 'spawning_complete', spec=Semaphore)

        def mock_request(data: StrDict | None) -> None:
            def testdata_request(self: TestdataConsumer) -> StrDict | None:  # noqa: ARG001
                if data is None or data == {}:
                    return None

                if 'variables' in data:
                    data['variables'] = transform(grizzly.scenario, data['variables'])

                return data

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.testdata',
                testdata_request,
            )

        mock_request(None)
        assert getattr(parent, 'start', '') is None
        assert getattr(grizzly.setup, 'wait_for_spawning_complete', '') is None

        with pytest.raises(StopScenario):
            parent.iterator()

        assert parent.user.variables == {}
        assert getattr(parent, 'start', '') is not None
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        request_fire_mock.assert_not_called()
        spawning_complete_mock.assert_not_called()

        mock_request({})

        with pytest.raises(StopScenario):
            parent.iterator()

        assert parent.user.variables == {}
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        request_fire_mock.assert_called_once_with(
            request_type='SCEN',
            name='001 test scenario',
            response_time=ANY(int),
            response_length=2,
            context=parent.user._context,
            exception=None,
        )
        request_fire_mock.reset_mock()
        spawning_complete_mock.assert_not_called()

        mock_request(
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 1337,
                    'AtomicCsvReader.test': {
                        'header1': 'value1',
                        'header2': 'value2',
                    },
                },
            },
        )

        # called from on_start
        grizzly.setup.wait_for_spawning_complete = 10
        parent.iterator(prefetch=True)

        on_iteration_mock.assert_not_called()
        spawning_complete_mock.assert_not_called()
        request_fire_mock.assert_called_with(
            request_type='TSTD',
            name='001 test scenario',
            response_time=ANY(int),
            response_length=ANY(int),
            context=parent.user._context,
            exception=None,
        )
        assert request_fire_mock.call_count == 2
        request_fire_mock.reset_mock()
        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1337
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value1'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value2'
        assert getattr(parent, '_prefetch', False)

        # without waiting for spawning complete
        grizzly.setup.wait_for_spawning_complete = None
        mock_request(
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 1338,
                    'AtomicCsvReader.test': {
                        'header1': 'value3',
                        'header2': 'value4',
                    },
                },
            },
        )

        # called from Iterator.run, first actual iteration
        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1337
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value1'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value2'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_not_called()  # on_iteration is not needed *before* first iteration
        spawning_complete_mock.reset_mock()

        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1338
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value3'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value4'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        spawning_complete_mock.assert_not_called()

        mock_request(
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 1339,
                    'AtomicCsvReader.test': {
                        'header1': 'value5',
                        'header2': 'value6',
                    },
                },
            },
        )

        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1339
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value5'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value6'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        spawning_complete_mock.assert_not_called()

        # with waiting for spawning complete
        grizzly.setup.wait_for_spawning_complete = 10
        parent._prefetch = True
        mock_request(
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 1338,
                    'AtomicCsvReader.test': {
                        'header1': 'value3',
                        'header2': 'value4',
                    },
                },
            },
        )

        # called from Iterator.run, first actual iteration
        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1339
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value5'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value6'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_not_called()  # on_iteration is not needed *before* first iteration
        spawning_complete_mock.reset_mock()

        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1338
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value3'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value4'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        spawning_complete_mock.wait.assert_called_once_with(timeout=10)
        spawning_complete_mock.reset_mock()

        mock_request(
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 1339,
                    'AtomicCsvReader.test': {
                        'header1': 'value5',
                        'header2': 'value6',
                    },
                },
            },
        )

        parent.iterator()

        assert parent.user.variables['AtomicIntegerIncrementer'].messageID == 1339
        assert parent.user.variables['AtomicCsvReader'].test.header1 == 'value5'
        assert parent.user.variables['AtomicCsvReader'].test.header2 == 'value6'
        assert not getattr(parent, '_prefetch', True)
        on_iteration_mock.assert_called_once_with()
        on_iteration_mock.reset_mock()
        spawning_complete_mock.assert_not_called()

    def test_pace(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', return_value=1000)
        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')
        gsleep_spy = mocker.patch('grizzly.scenarios.iterator.gsleep', return_value=None)

        # return directly, we don't have a pace
        assert getattr(parent, 'pace_time', '') is None

        parent.pace()

        assert perf_counter_spy.call_count == 0
        assert request_spy.call_count == 0
        assert gsleep_spy.call_count == 0

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # invalid float for pace_time
        parent.__class__.pace_time = 'asdf'
        with pytest.raises(StopUser):
            parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 0
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 0
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == 'asdf does not render to a number'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # iteration time < specified pace
        parent.start = 12.26
        parent.__class__.pace_time = '3000'
        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', side_effect=[13.37, 14.48])

        parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 1

        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 1110
        assert kwargs.get('response_length', None) == 1
        assert kwargs.get('exception', RuntimeError) is None

        args, _ = gsleep_spy.call_args_list[-1]
        assert len(args) == 1
        assert args[0] == (3000 / 1000) - (13.37 - 12.26)

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # iteration time > specified pace
        parent.__class__.pace_time = '500'
        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', side_effect=[13.37, 14.48])

        parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 0

        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 1110
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'pace falling behind, iteration takes longer than 500.0 milliseconds'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # templating, no value
        parent.__class__.pace_time = '{{ foobar }}'
        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', side_effect=[13.37, 14.48])

        with pytest.raises(StopUser):
            parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 0
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 1110
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == '{{ foobar }} does not render to a number'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # iteration time < specified pace, variable
        parent.user.set_variable('pace', '3000')
        parent.start = 12.26
        parent.__class__.pace_time = '{{ pace }}'
        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', side_effect=[13.37, 14.48])

        parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 1

        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 1110
        assert kwargs.get('response_length', None) == 1
        assert kwargs.get('exception', RuntimeError) is None

        args, _ = gsleep_spy.call_args_list[-1]
        assert len(args) == 1
        assert args[0] == (3000 / 1000) - (13.37 - 12.26)

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # iteration time > specified pace, templating
        parent.user.set_variable('pace', '500')
        parent.__class__.pace_time = '{{ pace }}'
        perf_counter_spy = mocker.patch('grizzly.scenarios.iterator.perf_counter', side_effect=[13.37, 14.48])

        parent.pace()

        assert perf_counter_spy.call_count == 2
        assert request_spy.call_count == 1
        assert gsleep_spy.call_count == 0

        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == parent.user._scenario.locust_name
        assert kwargs.get('response_time', None) == 1110
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'pace falling behind, iteration takes longer than 500.0 milliseconds'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

    def test_iteration_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)
        assert isinstance(parent, IteratorScenario)

        stats_log_mock = mocker.patch.object(parent.stats, 'log', return_value=None)
        stats_log_error_mock = mocker.patch.object(parent.stats, 'log_error', return_value=None)

        mocker.patch('grizzly.scenarios.iterator.perf_counter', return_value=11.0)

        parent.start = None

        parent.iteration_stop(error=RuntimeError('foobar'))

        stats_log_mock.assert_not_called()
        stats_log_error_mock.assert_not_called()

        parent.start = 1.0

        parent.iteration_stop(error=RuntimeError('foobar'))

        stats_log_mock.assert_called_once_with(10000, (parent._task_index % parent.task_count) + 1)
        stats_log_mock.reset_mock()

        stats_log_error_mock.assert_called_once_with(None)
        stats_log_error_mock.reset_mock()
        assert getattr(parent, 'start', 'foo') is None

        parent.iteration_stop(error=None)

        stats_log_mock.assert_not_called()
        stats_log_error_mock.assert_not_called()

        parent.start = 2.0

        parent.iteration_stop(error=None)

        stats_log_mock.assert_called_once_with(9000, (parent._task_index % parent.task_count) + 1)
        stats_log_mock.reset_mock()

        stats_log_error_mock.assert_not_called()

    def test_wait(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        wait_mocked = mocker.patch('grizzly.scenarios.iterator.GrizzlyScenario.wait', return_value=None)
        sleep_mocked = mocker.patch.object(parent, '_sleep', return_value=None)

        parent.user._scenario_state = ScenarioState.STOPPING
        parent.user._state = LOCUST_STATE_STOPPING
        parent.__class__.task_count = 10
        parent.current_task_index = 3

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        wait_mocked.assert_not_called()
        sleep_mocked.assert_called_once_with(0)
        sleep_mocked.reset_mock()

        assert parent.user._state == LOCUST_STATE_RUNNING
        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == 'not finished with scenario, currently at task 4 of 10, let me be!'
        caplog.clear()

        parent.current_task_index = 9

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        wait_mocked.assert_called_once_with()
        wait_mocked.reset_mock()
        assert parent.user._state == LOCUST_STATE_STOPPING
        assert parent.user.scenario_state == ScenarioState.STOPPED

        assert len(caplog.messages) == 2
        assert caplog.messages[-2] == "okay, I'm done with my running tasks now"
        assert caplog.messages[-1] == 'scenario state=ScenarioState.STOPPING -> ScenarioState.STOPPED'
        caplog.clear()

        parent.user._scenario_state = ScenarioState.STOPPED

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        wait_mocked.assert_called_once_with()
        wait_mocked.reset_mock()
        assert len(caplog.messages) == 0

    def test_run(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        grizzly = grizzly_fixture.grizzly

        assert isinstance(parent, IteratorScenario)
        assert not grizzly.state.spawning_complete.locked()

        # always assume that spawning is complete in unit test
        side_effects: list[InterruptTaskSet | None] = [
            InterruptTaskSet(reschedule=False),
            InterruptTaskSet(reschedule=True),
        ]
        side_effects.extend([None] * 10)

        on_stop_mock = mocker.patch.object(parent, 'on_stop')
        on_start_mock = mocker.patch.object(
            parent,
            'on_start',
            side_effect=side_effects,
        )

        mocker.patch.object(parent, 'tasks', [f'task-{index}' for index in range(10)])
        parent.__class__.task_count = len(parent.tasks)

        schedule_task_mock = mocker.patch.object(parent, 'schedule_task', autospec=True)
        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', autospec=True)
        wait_mock = mocker.patch.object(parent, 'wait', autospec=True)
        user_error_fire_mock = mocker.spy(parent.user.environment.events.user_error, 'fire')

        with pytest.raises(RescheduleTask):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()

        with pytest.raises(RescheduleTaskImmediately):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()

        parent.user._state = LOCUST_STATE_STOPPING

        with pytest.raises(StopUser):
            parent.run()

        parent.user._state = LOCUST_STATE_RUNNING

        on_start_mock.assert_called_once_with()
        get_next_task_mock.assert_called_once_with()
        assert schedule_task_mock.call_count == 1

        on_start_mock.reset_mock()
        get_next_task_mock.reset_mock()

        parent.user.environment.catch_exceptions = False

        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task', side_effect=[RescheduleTaskImmediately])

        with pytest.raises(RuntimeError):
            parent.run()

        on_start_mock.assert_called_once_with()
        wait_mock.assert_not_called()
        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 2
        execute_next_task_mock.assert_called_once_with(1, 10, 'unknown')
        assert user_error_fire_mock.call_count == 1
        _, kwargs = user_error_fire_mock.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        on_start_mock.reset_mock()

        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task', side_effect=[RescheduleTask])

        with pytest.raises(RuntimeError):
            parent.run()

        on_start_mock.assert_called_once_with()
        wait_mock.assert_called_once_with()
        execute_next_task_mock.assert_called_once_with(1, 10, 'unknown')

        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 3
        assert user_error_fire_mock.call_count == 2
        _, kwargs = user_error_fire_mock.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        on_start_mock.reset_mock()

        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task')

        with pytest.raises(RuntimeError):
            parent.run()

        on_start_mock.assert_called_once_with()
        execute_next_task_mock.assert_called_once_with(1, 10, 'unknown')
        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 4
        assert wait_mock.call_count == 2
        assert user_error_fire_mock.call_count == 3
        _, kwargs = user_error_fire_mock.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()
        wait_mock.reset_mock()
        schedule_task_mock.reset_mock()

        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[InterruptTaskSet(reschedule=False), InterruptTaskSet(reschedule=True)])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task')

        with pytest.raises(RescheduleTask):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()
        on_stop_mock.assert_not_called()
        schedule_task_mock.assert_not_called()
        execute_next_task_mock.assert_not_called()
        wait_mock.assert_not_called()
        get_next_task_mock.assert_called_once_with()
        assert user_error_fire_mock.call_count == 3

        with pytest.raises(RescheduleTaskImmediately):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()
        on_stop_mock.assert_not_called()
        schedule_task_mock.assert_not_called()
        execute_next_task_mock.assert_not_called()
        wait_mock.assert_not_called()
        assert get_next_task_mock.call_count == 2
        assert user_error_fire_mock.call_count == 3

        parent._task_index = 10
        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task', side_effect=[RestartScenario])

        with pytest.raises(RuntimeError):
            parent.run()

        assert parent._task_index == 20
        on_start_mock.assert_called_once_with()
        on_stop_mock.assert_called_once_with()
        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 1
        assert execute_next_task_mock.call_count == 1
        wait_mock.assert_called_once_with()
        assert user_error_fire_mock.call_count == 4

        user_error_fire_mock.reset_mock()
        wait_mock.reset_mock()
        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()
        parent.user.environment.catch_exceptions = True

        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[RuntimeError, StopUser])

        with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_stop_mock.assert_called_once_with()
        wait_mock.assert_called_once_with()
        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 1
        assert execute_next_task_mock.call_count == 1
        assert user_error_fire_mock.call_count == 1
        _, kwargs = user_error_fire_mock.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None
        assert 'ERROR' in caplog.text
        assert 'IteratorScenario' not in caplog.text
        assert 'TestUser_001' in caplog.text
        assert 'Traceback (most recent call last):' in caplog.text

        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()
        schedule_task_mock.reset_mock()
        wait_mock.reset_mock()
        user_error_fire_mock.reset_mock()

        print('=' * 200)
        parent._task_index = 0
        parent.user.environment.catch_exceptions = False
        get_next_task_mock = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task_mock = mocker.patch.object(parent, 'execute_next_task', side_effect=[RestartIteration, None])

        assert not getattr(parent, '_prefetch', True)

        with pytest.raises(RuntimeError):
            parent.run()

        assert parent._prefetch

        assert parent._task_index == 10
        assert get_next_task_mock.call_count == 2
        assert schedule_task_mock.call_count == 1
        assert execute_next_task_mock.call_count == 1
        wait_mock.assert_called_once_with()
        on_start_mock.assert_called_once_with()
        on_stop_mock.assert_called_once_with()
        assert user_error_fire_mock.call_count == 1

        user_error_fire_mock.reset_mock()
        wait_mock.reset_mock()
        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()

        # problems in on_start
        on_start_mock.side_effect = [StopScenario]

        with pytest.raises(StopUser):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()
        caplog.clear()

        # problems in on_stop, in handling of StopScenario
        parent.user._scenario_state = ScenarioState.RUNNING
        on_stop_mock.side_effect = None
        on_stop_mock.return_value = None
        get_next_task_mock.side_effect = [StopScenario]

        with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
            parent.run()

        on_start_mock.assert_called_once_with()
        on_start_mock.reset_mock()
        on_stop_mock.assert_called_once_with()

    def test_run_tasks(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        class TestErrorTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask.metadata(timeout=1.0)
                @grizzlytask
                def task(parent: GrizzlyScenario) -> Any:
                    if self.task_call_count == 0:
                        self.task_call_count += 1
                        from time import sleep

                        sleep(2.0)

                    if parent.user.variables.get('foo', None) is None:
                        raise RestartScenario

                    parent.user.stop()

                return task

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        mocker.patch('grizzly.scenarios.iterator.gsleep', return_value=None)
        mocker.patch('grizzly.scenarios.iterator.uniform', return_value=1.0)

        parent.user._scenario.failure_handling.update({TaskTimeoutError: RetryTask})

        # add tasks to IteratorScenario.tasks
        for i in range(1, 6):
            name = f'test-task-{i}'
            parent.user._scenario.tasks.behave_steps.update({i + 1: name})
            parent.__class__.populate(TestTask(name=name))

        parent.__class__.populate(TestErrorTask(name='test-error-task-1'))
        parent.user._scenario.tasks.behave_steps.update({7: 'test-error-task-1'})

        for i in range(6, 11):
            name = f'test-task-{i}'
            parent.user._scenario.tasks.behave_steps.update({i + 2: name})
            parent.__class__.populate(TestTask(name=name))

        scenario = parent.__class__(parent.user)
        scenario.__class__.pace_time = '10000'
        mocker.patch.object(scenario, 'prefetch', return_value=None)

        assert parent.task_count == 13
        assert len(parent.tasks) == 13

        mocker.patch('grizzly.scenarios.TestdataConsumer.__init__', return_value=None)

        scenario.on_start()  # create scenario.consumer, so we can patch request below

        # same as 1 iteration
        mocker.patch.object(
            scenario._consumer,
            'testdata',
            side_effect=[
                {'variables': {'hello': 'world'}},
                {'variables': {'hello': 'world'}},
                {'variables': {'foo': 'bar'}},
                None,
            ],
        )
        mocker.patch.object(scenario, 'on_start', return_value=None)

        with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
            scenario.run()

        expected_messages = [
            'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
            'task 2 of 13 executed: test-task-1',
            'task 3 of 13 executed: test-task-2',
            'task 4 of 13 executed: test-task-3',
            'task 5 of 13 executed: test-task-4',
            'task 6 of 13 executed: test-task-5',
            'task 7 of 13 failed: test-error-task-1',
            'task 7 of 13 will execute a 2nd time in 1.00 seconds: test-error-task-1',
            'task 7 of 13 failed: test-error-task-1',
            'restarting scenario at task 7 of 13',
            '0 tasks in queue',
            'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
            'task 2 of 13 executed: test-task-1',
            'task 3 of 13 executed: test-task-2',
            'task 4 of 13 executed: test-task-3',
            'task 5 of 13 executed: test-task-4',
            'task 6 of 13 executed: test-task-5',
            'task 7 of 13 failed: test-error-task-1',
            'restarting scenario at task 7 of 13',
            '0 tasks in queue',
            'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
            'task 2 of 13 executed: test-task-1',
            'task 3 of 13 executed: test-task-2',
            'task 4 of 13 executed: test-task-3',
            'task 5 of 13 executed: test-task-4',
            'task 6 of 13 executed: test-task-5',
            'stop scenarios before stopping user',
            'scenario state=ScenarioState.RUNNING -> ScenarioState.STOPPING',
            'task 7 of 13 executed: test-error-task-1',
            'not finished with scenario, currently at task 7 of 13, let me be!',
            'task 8 of 13 executed: test-task-6',
            'not finished with scenario, currently at task 8 of 13, let me be!',
            'task 9 of 13 executed: test-task-7',
            'not finished with scenario, currently at task 9 of 13, let me be!',
            'task 10 of 13 executed: test-task-8',
            'not finished with scenario, currently at task 10 of 13, let me be!',
            'task 11 of 13 executed: test-task-9',
            'not finished with scenario, currently at task 11 of 13, let me be!',
            'task 12 of 13 executed: test-task-10',
            'not finished with scenario, currently at task 12 of 13, let me be!',
            r'^scenario keeping pace by sleeping [0-9\.]+ milliseconds$',
            'task 13 of 13 executed: pace',
            "okay, I'm done with my running tasks now",
            'scenario state=ScenarioState.STOPPING -> ScenarioState.STOPPED',
            'scenario_state=STOPPED, user_state=stopping, exception=StopUser()',
            "stopping scenario with <class 'locust.exception.StopUser'>",
        ]

        actual_messages = filter_messages(caplog.messages)

        assert len(actual_messages) == len(expected_messages)

        for actual, _expected in zip(actual_messages, expected_messages, strict=False):
            expected = regex.possible(_expected)
            assert actual == expected

    def test_run_retry_task(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        class TestErrorTask(TestTask):
            def __call__(self) -> grizzlytask:
                self.call_count += 1
                self.task_call_count = 0

                @grizzlytask
                def task(parent: GrizzlyScenario) -> Any:  # noqa: ARG001
                    self.task_call_count += 1

                    # second time, always raise RetryTask
                    if self.task_call_count < 3 or self.task_call_count > 3:
                        raise RetryTask

                return task

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)
        assert not parent.grizzly.state.spawning_complete.locked()

        mocker.patch('grizzly.scenarios.iterator.gsleep', return_value=None)
        mocker.patch('grizzly.scenarios.iterator.uniform', return_value=1.0)

        # add tasks to IteratorScenario.tasks
        try:
            for i in range(1, 6):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 1: name})
                parent.__class__.populate(TestTask(name=name))

            parent.__class__.populate(TestErrorTask(name='test-error-task-1'))
            parent.user._scenario.tasks.behave_steps.update({7: 'test-error-task-1'})

            for i in range(6, 11):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 2: name})
                parent.__class__.populate(TestTask(name=name))

            scenario = parent.__class__(parent.user)
            scenario.__class__.pace_time = '10000'
            mocker.patch.object(scenario, 'prefetch', return_value=None)
            parent.user._scenario.failure_handling.update({None: StopUser})

            assert parent.task_count == 13
            assert len(parent.tasks) == 13

            mocker.patch('grizzly.scenarios.TestdataConsumer.__init__', return_value=None)

            scenario.on_start()  # create scenario.consumer, so we can patch request below

            # same as 1 iteration
            mocker.patch.object(
                scenario.consumer,
                'testdata',
                side_effect=[
                    {'variables': {'hello': 'world'}},
                    {'variables': {'foo': 'bar'}},
                    None,
                ],
            )
            mocker.patch.object(scenario, 'on_start', return_value=None)

            with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
                scenario.run()

            expected_messages = [
                'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
                'task 2 of 13 executed: test-task-1',
                'task 3 of 13 executed: test-task-2',
                'task 4 of 13 executed: test-task-3',
                'task 5 of 13 executed: test-task-4',
                'task 6 of 13 executed: test-task-5',
                'task 7 of 13 failed: test-error-task-1',
                'task 7 of 13 will execute a 2nd time in 1.00 seconds: test-error-task-1',
                'task 7 of 13 failed: test-error-task-1',
                'task 7 of 13 will execute a 3rd time in 2.00 seconds: test-error-task-1',
                'task 7 of 13 executed: test-error-task-1',
                'task 8 of 13 executed: test-task-6',
                'task 9 of 13 executed: test-task-7',
                'task 10 of 13 executed: test-task-8',
                'task 11 of 13 executed: test-task-9',
                'task 12 of 13 executed: test-task-10',
                r'^scenario keeping pace by sleeping [0-9\.]+ milliseconds$',
                'task 13 of 13 executed: pace',
                'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
                'task 2 of 13 executed: test-task-1',
                'task 3 of 13 executed: test-task-2',
                'task 4 of 13 executed: test-task-3',
                'task 5 of 13 executed: test-task-4',
                'task 6 of 13 executed: test-task-5',
                'task 7 of 13 failed: test-error-task-1',
                'task 7 of 13 will execute a 2nd time in 1.00 seconds: test-error-task-1',
                'task 7 of 13 failed: test-error-task-1',
                'task 7 of 13 will execute a 3rd time in 2.00 seconds: test-error-task-1',
                'task 7 of 13 failed: test-error-task-1',
                'task 7 of 13 failed after 3 retries: test-error-task-1',
                'scenario_state=RUNNING, user_state=running, exception=StopUser()',
                'scenario state=ScenarioState.RUNNING -> ScenarioState.STOPPED',
                "stopping scenario with <class 'locust.exception.StopUser'>",
            ]

            actual_messages = filter_messages(caplog.messages)

            assert len(actual_messages) == len(expected_messages)

            for actual, _expected in zip(actual_messages, expected_messages, strict=False):
                expected = regex.possible(_expected)
                assert actual == expected

        finally:
            with suppress(KeyError):
                del environ['TESTDATA_PRODUCER_ADDRESS']

    def test_run_restart_scenario(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        class TestErrorTask(TestTask):
            def __call__(self) -> grizzlytask:
                self.call_count += 1
                self.task_call_count = 0

                @grizzlytask
                def task(parent: GrizzlyScenario) -> Any:  # noqa: ARG001
                    self.task_call_count += 1

                    # second time, always raise RetryTask
                    if self.task_call_count < 2 or self.task_call_count > 2:
                        raise RestartIteration

                return task

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)
        assert not parent.grizzly.state.spawning_complete.locked()

        mocker.patch('grizzly.scenarios.iterator.gsleep', return_value=None)
        mocker.patch('grizzly.scenarios.iterator.uniform', return_value=1.0)

        # add tasks to IteratorScenario.tasks
        try:
            for i in range(1, 6):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 1: name})
                parent.__class__.populate(TestTask(name=name))

            parent.__class__.populate(TestErrorTask(name='test-error-task-1'))
            parent.user._scenario.tasks.behave_steps.update({7: 'test-error-task-1'})

            for i in range(6, 11):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 2: name})
                parent.__class__.populate(TestTask(name=name))

            scenario = parent.__class__(parent.user)
            scenario.__class__.pace_time = '10000'
            mocker.patch.object(scenario, 'prefetch', return_value=None)
            parent.user._scenario.failure_handling.update({None: RestartIteration})

            assert parent.task_count == 13
            assert len(parent.tasks) == 13

            mocker.patch('grizzly.scenarios.TestdataConsumer.__init__', return_value=None)

            scenario.on_start()  # create scenario.consumer, so we can patch request below

            # same as 1 iteration
            testdata_mock = mocker.patch.object(
                scenario.consumer,
                'testdata',
                side_effect=[
                    {'variables': {'hello': 'world'}},
                    None,
                ],
            )
            mocker.patch.object(scenario, 'on_start', return_value=None)

            with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
                scenario.run()

            # first iteration, and third iteration. second iteration fails with "RestartIteration", meaning run again with
            # same testdata
            assert testdata_mock.call_count == 2

            expected_messages = [
                'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
                'task 2 of 13 executed: test-task-1',
                'task 3 of 13 executed: test-task-2',
                'task 4 of 13 executed: test-task-3',
                'task 5 of 13 executed: test-task-4',
                'task 6 of 13 executed: test-task-5',
                'task 7 of 13 failed: test-error-task-1',
                'restarting iteration at task 7 of 13',
                '0 tasks in queue',
                'task 1 of 13 executed: iterator',  # IteratorScenario.iterator()
                'task 2 of 13 executed: test-task-1',
                'task 3 of 13 executed: test-task-2',
                'task 4 of 13 executed: test-task-3',
                'task 5 of 13 executed: test-task-4',
                'task 6 of 13 executed: test-task-5',
                'task 7 of 13 executed: test-error-task-1',
                'task 8 of 13 executed: test-task-6',
                'task 9 of 13 executed: test-task-7',
                'task 10 of 13 executed: test-task-8',
                'task 11 of 13 executed: test-task-9',
                'task 12 of 13 executed: test-task-10',
                r'^scenario keeping pace by sleeping [0-9\.]+ milliseconds$',
                'task 13 of 13 executed: pace',
                'no iteration data available, stop scenario',
                'scenario_state=RUNNING, user_state=running, exception=StopScenario()',
                'scenario state=ScenarioState.RUNNING -> ScenarioState.STOPPED',
                "stopping scenario with <class 'locust.exception.StopUser'>",
            ]

            actual_messages = filter_messages(caplog.messages)

            assert len(actual_messages) == len(expected_messages)

            for actual, _expected in zip(actual_messages, expected_messages, strict=False):
                expected = regex.possible(_expected)
                assert actual == expected

        finally:
            with suppress(KeyError):
                del environ['TESTDATA_PRODUCER_ADDRESS']

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        class TestOnStartTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(_: GrizzlyScenario) -> Any:
                    pass

                @task.on_start
                def on_start(_: GrizzlyScenario) -> None:
                    pass

                return task

        parent.__class__.populate(TestOnStartTask(name='test-on-task-1'))
        parent.__class__.populate(TestOnStartTask(name='test-on-task-2'))

        try:
            scenario = parent.__class__(parent.user)

            testdata_consumer_mock = mocker.patch('grizzly.scenarios.TestdataConsumer.__init__', return_value=None)
            prefetch_mock = mocker.patch.object(scenario, 'prefetch', return_value=None)

            task_1 = scenario.tasks[-3]
            task_2 = scenario.tasks[-2]

            assert isinstance(task_1, grizzlytask)
            assert isinstance(task_2, grizzlytask)
            assert task_1 is not task_2
            assert task_1._on_stop is None
            assert task_2._on_stop is None

            task_1_on_start_spy = mocker.spy(task_1, '_on_start')
            task_2_on_start_spy = mocker.spy(task_2, '_on_start')

            scenario.on_start()

            assert scenario.user._scenario_state == ScenarioState.RUNNING
            testdata_consumer_mock.assert_called_once()
            task_1_on_start_spy.assert_called_once_with(scenario)
            task_2_on_start_spy.assert_called_once_with(scenario)
            prefetch_mock.assert_called_once_with()
        finally:
            with suppress(KeyError):
                del environ['TESTDATA_PRODUCER_ADDRESS']

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        class TestOnStopTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(_: GrizzlyScenario) -> Any:
                    pass

                @task.on_stop
                def on_stop(_: GrizzlyScenario) -> None:
                    pass

                return task

        parent.__class__.populate(TestOnStopTask(name='test-on-task-1'))
        parent.__class__.populate(TestOnStopTask(name='test-on-task-2'))

        scenario = parent.__class__(parent.user)

        task_1 = scenario.tasks[-3]
        task_2 = scenario.tasks[-2]

        assert isinstance(task_1, grizzlytask)
        assert isinstance(task_2, grizzlytask)
        assert task_1 is not task_2

        task_1_on_stop_spy = mocker.spy(task_1, '_on_stop')
        task_2_on_stop_spy = mocker.spy(task_2, '_on_stop')
        scenario.user.scenario_state = ScenarioState.RUNNING

        scenario.on_stop()

        assert scenario.user._scenario_state == ScenarioState.STOPPED
        task_1_on_stop_spy.assert_called_once_with(scenario)
        task_2_on_stop_spy.assert_called_once_with(scenario)
