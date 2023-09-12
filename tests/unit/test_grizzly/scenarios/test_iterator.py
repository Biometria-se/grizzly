import logging

from os import environ
from typing import TYPE_CHECKING, Dict, Any, Optional, List
from types import FunctionType
from importlib import reload
from unittest.mock import ANY

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from locust.user.task import LOCUST_STATE_STOPPING, LOCUST_STATE_RUNNING
from locust.user.sequential_taskset import SequentialTaskSet
from locust.exception import StopUser, InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately

from grizzly.scenarios import iterator
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform
from grizzly.tasks import WaitTask, LogMessageTask, grizzlytask
from grizzly.exceptions import RestartScenario, StopScenario
from grizzly.types import ScenarioState
from grizzly.testdata.utils import templatingfilter

from tests.fixtures import GrizzlyFixture
from tests.helpers import RequestCalled, TestTask, regex


if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario


class TestIterationScenario:
    def test_initialize(self, grizzly_fixture: GrizzlyFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)
        assert isinstance(parent, iterator.IteratorScenario)
        assert issubclass(parent.__class__, SequentialTaskSet)
        assert parent.pace_time is None

    def test_render(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        assert isinstance(parent, iterator.IteratorScenario)

        @templatingfilter
        def sarcasm(value: str) -> str:
            sarcastic_value: List[str] = []
            for index, c in enumerate(value):
                if index % 2 == 0:
                    sarcastic_value.append(c.upper())
                else:
                    sarcastic_value.append(c.lower())

            return ''.join(sarcastic_value)

        parent.user._context['variables'].update({'are': 'foo'})
        assert parent.render('how {{ are }} we {{ doing | sarcasm }} today', variables={'doing': 'bar'}) == 'how foo we BaR today'

    def test_populate(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)
        request = grizzly_fixture.request_task.request
        request.endpoint = '/api/v1/test'
        request.source = None

        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        try:
            iterator.IteratorScenario.populate(request)
            assert isinstance(parent, iterator.IteratorScenario)
            assert len(parent.tasks) == 3

            task_method = parent.tasks[-2]
            parent.user._scenario.failure_exception = StopUser

            assert callable(task_method)
            with pytest.raises(StopUser):
                task_method(parent)

            request_spy.assert_called_once_with(
                request_type='POST',
                name='001 IteratorScenario',
                response_time=ANY,
                response_length=0,
                context={'variables': {}, 'log_all_requests': False},
                exception=ANY,
            )
            args, kwargs = request_spy.call_args_list[-1]
            assert args == ()
            assert isinstance(kwargs['exception'], RequestCalled)

            def generate_mocked_wait(sleep_time: float) -> None:
                def mocked_wait(time: float) -> None:
                    assert sleep_time == time

                mocker.patch(
                    'grizzly.tasks.wait.gsleep',
                    mocked_wait,
                )

            generate_mocked_wait(1.5)
            iterator.IteratorScenario.populate(WaitTask(time_expression='1.5'))
            assert len(parent.tasks) == 4

            task_method = parent.tasks[-2]
            assert callable(task_method)
            task_method(parent)

            iterator.IteratorScenario.populate(LogMessageTask(message='hello {{ world }}'))
            assert len(parent.tasks) == 5

            logger_spy = mocker.spy(parent.logger, 'info')

            task_method = parent.tasks[-2]
            assert callable(task_method)
            task_method(parent)

            assert logger_spy.call_count == 1
            args, _ = logger_spy.call_args_list[0]
            assert args[0] == 'hello '

            parent.user.set_context_variable('world', 'world!')

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
        finally:
            reload(iterator)

    def test_on_event_handlers(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)

        try:
            parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

            def TestdataConsumer__init__(self: 'TestdataConsumer', scenario: 'GrizzlyScenario', address: str, identifier: str) -> None:
                pass

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.__init__',
                TestdataConsumer__init__,
            )

            def TestdataConsumer_on_stop(self: 'TestdataConsumer') -> None:
                raise StopUser()

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.stop',
                TestdataConsumer_on_stop,
            )

            assert parent is not None

            mocker.patch.object(parent, 'prefetch', return_value=None)

            with pytest.raises(StopUser):
                parent.on_start()

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'localhost:5555'

            parent.on_start()

            with pytest.raises(StopUser):
                parent.on_stop()
        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except KeyError:
                pass

    def test_prefetch(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        assert isinstance(parent, iterator.IteratorScenario)

        iterator_mock = mocker.patch.object(parent, 'iterator', return_value=None)

        parent.prefetch()

        iterator_mock.assert_called_once_with(prefetch=True)

    def test_iterator(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        grizzly = grizzly_fixture.grizzly

        assert isinstance(parent, iterator.IteratorScenario)
        assert not getattr(parent, '_prefetch', True)

        parent.consumer = TestdataConsumer(parent, identifier='test')

        def mock_request(data: Optional[Dict[str, Any]]) -> None:
            def testdata_request(self: 'TestdataConsumer', scenario: str) -> Optional[Dict[str, Any]]:
                if data is None or data == {}:
                    return None

                if 'variables' in data:
                    data['variables'] = transform(grizzly, data['variables'])

                return data

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.testdata',
                testdata_request,
            )

        mock_request(None)

        with pytest.raises(StopScenario):
            parent.iterator()

        assert parent.user.context_variables == {}

        mock_request({})

        with pytest.raises(StopScenario):
            parent.iterator()

        assert parent.user.context_variables == {}

        mock_request({
            'variables': {
                'AtomicIntegerIncrementer.messageID': 1337,
                'AtomicCsvReader.test': {
                    'header1': 'value1',
                    'header2': 'value2',
                },
            },
        })

        parent.iterator(prefetch=True)

        assert parent.user.context_variables['AtomicIntegerIncrementer'].messageID == 1337
        assert parent.user.context_variables['AtomicCsvReader'].test.header1 == 'value1'
        assert parent.user.context_variables['AtomicCsvReader'].test.header2 == 'value2'
        assert getattr(parent, '_prefetch', False)

        mock_request({
            'variables': {
                'AtomicIntegerIncrementer.messageID': 1338,
                'AtomicCsvReader.test': {
                    'header1': 'value3',
                    'header2': 'value4',
                },
            },
        })

        parent.iterator()

        assert parent.user.context_variables['AtomicIntegerIncrementer'].messageID == 1337
        assert parent.user.context_variables['AtomicCsvReader'].test.header1 == 'value1'
        assert parent.user.context_variables['AtomicCsvReader'].test.header2 == 'value2'
        assert not getattr(parent, '_prefetch', True)

        parent.iterator()

        assert parent.user.context_variables['AtomicIntegerIncrementer'].messageID == 1338
        assert parent.user.context_variables['AtomicCsvReader'].test.header1 == 'value3'
        assert parent.user.context_variables['AtomicCsvReader'].test.header2 == 'value4'
        assert not getattr(parent, '_prefetch', True)

    def test_pace(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        assert isinstance(parent, iterator.IteratorScenario)

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
        parent.pace_time = 'asdf'
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
        parent.pace_time = '3000'
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
        parent.pace_time = '500'
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
        assert str(exception) == 'pace falling behind'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

        # templating, no value
        parent.pace_time = '{{ foobar }}'
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
        parent.user._context['variables'].update({'pace': '3000'})
        parent.start = 12.26
        parent.pace_time = '{{ pace }}'
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
        parent.user._context['variables'].update({'pace': '500'})
        parent.pace_time = '{{ pace }}'
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
        assert str(exception) == 'pace falling behind'

        perf_counter_spy.reset_mock()
        request_spy.reset_mock()
        gsleep_spy.reset_mock()

    def test_iteration_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)
        assert isinstance(parent, iterator.IteratorScenario)

        stats_log_mock = mocker.patch.object(parent.stats, 'log', return_value=None)
        stats_log_error_mock = mocker.patch.object(parent.stats, 'log_error', return_value=None)

        mocker.patch('grizzly.scenarios.iterator.perf_counter', return_value=11.0)

        parent.start = None

        parent.iteration_stop(has_error=True)

        assert stats_log_mock.call_count == 0

        assert stats_log_error_mock.call_count == 0

        parent.start = 1.0

        parent.iteration_stop(has_error=True)

        assert stats_log_mock.call_count == 1
        args, _ = stats_log_mock.call_args_list[-1]
        assert len(args) == 2
        assert args[0] == 10000
        assert args[1] == (parent._task_index % parent.task_count) + 1

        assert stats_log_error_mock.call_count == 1
        args, _ = stats_log_error_mock.call_args_list[-1]
        assert len(args) == 1
        assert args[0] is None

        parent.iteration_stop(has_error=False)

        assert stats_log_mock.call_count == 2
        args, _ = stats_log_mock.call_args_list[-1]
        assert len(args) == 2
        assert args[0] == 10000
        assert args[1] == (parent._task_index % parent.task_count) + 1

        assert stats_log_error_mock.call_count == 1

    def test_wait(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        assert isinstance(parent, iterator.IteratorScenario)

        wait_mocked = mocker.patch('grizzly.scenarios.iterator.GrizzlyScenario.wait', return_value=None)
        sleep_mocked = mocker.patch.object(parent, '_sleep', return_value=None)

        parent.user._scenario_state = ScenarioState.STOPPING
        parent.user._state = LOCUST_STATE_STOPPING
        parent.task_count = 10
        parent.current_task_index = 3

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        assert wait_mocked.call_count == 0
        assert sleep_mocked.call_count == 1
        assert parent.user._state == LOCUST_STATE_RUNNING
        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == 'not finished with scenario, currently at task 4 of 10, let me be!'
        caplog.clear()

        parent.current_task_index = 9

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        assert wait_mocked.call_count == 1
        assert parent.user._state == LOCUST_STATE_STOPPING
        assert parent.user.scenario_state == ScenarioState.STOPPED

        assert len(caplog.messages) == 2
        assert caplog.messages[-2] == "okay, I'm done with my running tasks now"
        assert caplog.messages[-1] == "scenario state=ScenarioState.STOPPING -> ScenarioState.STOPPED"
        caplog.clear()

        parent.user._scenario_state = ScenarioState.STOPPED

        with caplog.at_level(logging.DEBUG):
            parent.wait()

        assert wait_mocked.call_count == 2
        assert len(caplog.messages) == 0

    def test_run(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        reload(iterator)
        parent = grizzly_fixture(scenario_type=iterator.IteratorScenario)

        assert isinstance(parent, iterator.IteratorScenario)

        # always assume that spawning is complete in unit test
        parent.grizzly.state.spawning_complete = True

        side_effects: List[Optional[InterruptTaskSet]] = [
            InterruptTaskSet(reschedule=False),
            InterruptTaskSet(reschedule=True),
        ]
        side_effects.extend([None] * 10)

        on_stop = mocker.patch.object(parent, 'on_stop', autospec=True)
        on_start = mocker.patch.object(
            parent,
            'on_start',
            side_effect=side_effects,
        )

        mocker.patch.object(parent, 'tasks', [f'task-{index}' for index in range(0, 10)])
        parent.task_count = len(parent.tasks)

        schedule_task = mocker.patch.object(parent, 'schedule_task', autospec=True)
        get_next_task = mocker.patch.object(parent, 'get_next_task', autospec=True)
        wait = mocker.patch.object(parent, 'wait', autospec=True)
        user_error = mocker.spy(parent.user.environment.events.user_error, 'fire')

        with pytest.raises(RescheduleTask):
            parent.run()

        assert on_start.call_count == 1

        with pytest.raises(RescheduleTaskImmediately):
            parent.run()

        assert on_start.call_count == 2

        parent.user._state = LOCUST_STATE_STOPPING

        with pytest.raises(StopUser):
            parent.run()

        parent.user._state = LOCUST_STATE_RUNNING

        assert on_start.call_count == 3
        assert get_next_task.call_count == 1
        assert schedule_task.call_count == 1

        parent.user.environment.catch_exceptions = False

        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(parent, 'execute_next_task', side_effect=[RescheduleTaskImmediately])

        with pytest.raises(RuntimeError):
            parent.run()

        assert on_start.call_count == 4
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 2
        assert execute_next_task.call_count == 1
        assert wait.call_count == 0
        assert user_error.call_count == 1
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(parent, 'execute_next_task', side_effect=[RescheduleTask])

        with pytest.raises(RuntimeError):
            parent.run()

        assert on_start.call_count == 5
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 3
        assert execute_next_task.call_count == 1
        assert wait.call_count == 1
        assert user_error.call_count == 2
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(parent, 'execute_next_task')

        with pytest.raises(RuntimeError):
            parent.run()

        assert on_start.call_count == 6
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 4
        assert execute_next_task.call_count == 1
        assert wait.call_count == 2
        assert user_error.call_count == 3
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        on_stop.reset_mock()
        wait.reset_mock()
        schedule_task.reset_mock()

        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[InterruptTaskSet(reschedule=False), InterruptTaskSet(reschedule=True)])
        execute_next_task = mocker.patch.object(parent, 'execute_next_task')

        with pytest.raises(RescheduleTask):
            parent.run()

        assert on_start.call_count == 7
        assert on_stop.call_count == 1
        assert get_next_task.call_count == 1
        assert schedule_task.call_count == 0
        assert execute_next_task.call_count == 0
        assert wait.call_count == 0
        assert user_error.call_count == 3

        with pytest.raises(RescheduleTaskImmediately):
            parent.run()

        assert on_start.call_count == 8
        assert on_stop.call_count == 2
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 0
        assert execute_next_task.call_count == 0
        assert wait.call_count == 0
        assert user_error.call_count == 3

        parent._task_index = 10
        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(parent, 'execute_next_task', side_effect=[RestartScenario])

        with pytest.raises(RuntimeError):
            parent.run()

        assert parent._task_index == 20
        assert on_start.call_count == 9
        assert on_stop.call_count == 2
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 1
        assert execute_next_task.call_count == 1
        assert wait.call_count == 1
        assert user_error.call_count == 4

        user_error.reset_mock()
        wait.reset_mock()
        parent.user.environment.catch_exceptions = True

        get_next_task = mocker.patch.object(parent, 'get_next_task', side_effect=[RuntimeError, StopUser])

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                parent.run()

        assert wait.call_count == 1
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 1
        assert execute_next_task.call_count == 1
        assert user_error.call_count == 1
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is parent.user
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None
        assert 'ERROR' in caplog.text and 'IteratorScenario' in caplog.text
        assert 'Traceback (most recent call last):' in caplog.text

        # problems in on_start
        on_start.side_effect = [StopScenario]

        with pytest.raises(StopUser):
            parent.run()

        on_stop.reset_mock()

        # problems in on_stop, in handling of InterruptTaskSet
        caplog.clear()
        on_start.side_effect = None
        on_start.return_value = None
        on_stop.side_effect = [RuntimeError]
        get_next_task.side_effect = [InterruptTaskSet(reschedule=False)]

        with caplog.at_level(logging.ERROR):
            with pytest.raises(RescheduleTask):
                parent.run()

        on_stop.assert_called_once_with()
        assert caplog.messages == ['on_stop failed']
        caplog.clear()

        on_stop.reset_mock()

        # problems in on_stop, in handling of StopScenario
        parent.user._scenario_state = ScenarioState.RUNNING
        on_stop.side_effect = [RuntimeError]
        get_next_task.side_effect = [StopScenario]

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                parent.run()

        on_stop.assert_called_once_with()
        assert caplog.messages == ['on_stop failed']
        caplog.clear()

    def test_run_tasks(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, mocker: MockerFixture) -> None:
        reload(iterator)

        class TestErrorTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(parent: 'GrizzlyScenario') -> Any:
                    parent.user.logger.debug(f'{self.name} executed')

                    if parent.user._context.get('variables', {}).get('foo', None) is None:
                        raise RestartScenario()
                    else:
                        parent.user.stop()

                return task

        parent = grizzly_fixture()

        mocker.patch('grizzly.scenarios.iterator.gsleep', return_value=None)

        # add tasks to IteratorScenario.tasks
        try:
            for i in range(1, 6):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 1: name})
                iterator.IteratorScenario.populate(TestTask(name=name))

            iterator.IteratorScenario.populate(TestErrorTask(name='test-error-task-1'))
            parent.user._scenario.tasks.behave_steps.update({7: 'test-error-task-1'})

            for i in range(6, 11):
                name = f'test-task-{i}'
                parent.user._scenario.tasks.behave_steps.update({i + 2: name})
                iterator.IteratorScenario.populate(TestTask(name=name))

            scenario = iterator.IteratorScenario(parent.user)
            scenario.pace_time = '10000'
            mocker.patch.object(scenario, 'prefetch', return_value=None)

            assert scenario.task_count == 13
            assert len(scenario.tasks) == 13

            mocker.patch('grizzly.scenarios.TestdataConsumer.__init__', return_value=None)
            mocker.patch('grizzly.scenarios.TestdataConsumer.stop', return_value=None)

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'tcp://localhost:5555'

            scenario.on_start()  # create scenario.consumer, so we can patch request below

            # same as 1 iteration
            mocker.patch.object(scenario.consumer, 'testdata', side_effect=[
                {'variables': {'hello': 'world'}},
                {'variables': {'foo': 'bar'}},
                None,
            ])
            mocker.patch.object(scenario, 'on_start', return_value=None)

            with caplog.at_level(logging.DEBUG):
                with pytest.raises(StopUser):
                    scenario.run()

            expected_messages = [
                'executing task 1 of 13: iterator',  # IteratorScenario.iterator()
                'executing task 2 of 13: test-task-1',
                'test-task-1 executed',
                'executing task 3 of 13: test-task-2',
                'test-task-2 executed',
                'executing task 4 of 13: test-task-3',
                'test-task-3 executed',
                'executing task 5 of 13: test-task-4',
                'test-task-4 executed',
                'executing task 6 of 13: test-task-5',
                'test-task-5 executed',
                'executing task 7 of 13: test-error-task-1',
                'test-error-task-1 executed',
                'task 7 of 13: test-error-task-1, failed: RestartScenario',
                'restarting scenario at task 7 of 13',
                '0 tasks in queue',
                'executing task 1 of 13: iterator',  # IteratorScenario.iterator()
                'executing task 2 of 13: test-task-1',
                'test-task-1 executed',
                'executing task 3 of 13: test-task-2',
                'test-task-2 executed',
                'executing task 4 of 13: test-task-3',
                'test-task-3 executed',
                'executing task 5 of 13: test-task-4',
                'test-task-4 executed',
                'executing task 6 of 13: test-task-5',
                'test-task-5 executed',
                'executing task 7 of 13: test-error-task-1',
                'test-error-task-1 executed',
                'stop scenarios before stopping user',
                'scenario state=ScenarioState.RUNNING -> ScenarioState.STOPPING',
                'not finished with scenario, currently at task 7 of 13, let me be!',
                'executing task 8 of 13: test-task-6',
                'test-task-6 executed',
                'not finished with scenario, currently at task 8 of 13, let me be!',
                'executing task 9 of 13: test-task-7',
                'test-task-7 executed',
                'not finished with scenario, currently at task 9 of 13, let me be!',
                'executing task 10 of 13: test-task-8',
                'test-task-8 executed',
                'not finished with scenario, currently at task 10 of 13, let me be!',
                'executing task 11 of 13: test-task-9',
                'test-task-9 executed',
                'not finished with scenario, currently at task 11 of 13, let me be!',
                'executing task 12 of 13: test-task-10',
                'test-task-10 executed',
                'not finished with scenario, currently at task 12 of 13, let me be!',
                'executing task 13 of 13: pace',
                r'^keeping pace by sleeping [0-9\.]+ milliseconds$',
                "okay, I'm done with my running tasks now",
                'scenario state=ScenarioState.STOPPING -> ScenarioState.STOPPED',
                "self.user._scenario_state=<ScenarioState.STOPPED: 1>, self.user._state='stopping', e=StopUser()",
            ]

            assert len(caplog.messages) == len(expected_messages)

            for actual, _expected in zip(caplog.messages, expected_messages):
                expected = regex.possible(_expected)
                assert actual == expected

        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except:
                pass

            reload(iterator)

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)

        parent = grizzly_fixture()

        class TestOnStartTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(parent: 'GrizzlyScenario') -> Any:
                    pass

                @task.on_start
                def on_start(parent: 'GrizzlyScenario') -> None:
                    pass

                return task

        iterator.IteratorScenario.populate(TestOnStartTask(name='test-on-task-1'))
        iterator.IteratorScenario.populate(TestOnStartTask(name='test-on-task-2'))

        try:
            scenario = iterator.IteratorScenario(parent.user)

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

            with pytest.raises(StopUser):
                scenario.on_start()

            testdata_consumer_mock.assert_not_called()
            task_1_on_start_spy.assert_not_called()
            task_2_on_start_spy.assert_not_called()
            prefetch_mock.assert_not_called()
            assert getattr(scenario.user, '_scenario_state', None) == ScenarioState.STOPPED

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'tcp://localhost:5555'

            scenario.on_start()

            assert scenario.user._scenario_state == ScenarioState.RUNNING
            testdata_consumer_mock.assert_called_once()
            task_1_on_start_spy.assert_called_once_with(scenario)
            task_2_on_start_spy.assert_called_once_with(scenario)
            prefetch_mock.assert_called_once_with()
        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except:
                pass

            reload(iterator)

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        reload(iterator)

        parent = grizzly_fixture()

        class TestOnStopTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(parent: 'GrizzlyScenario') -> Any:
                    pass

                @task.on_stop
                def on_stop(parent: 'GrizzlyScenario') -> None:
                    pass

                return task

        iterator.IteratorScenario.populate(TestOnStopTask(name='test-on-task-1'))
        iterator.IteratorScenario.populate(TestOnStopTask(name='test-on-task-2'))

        try:
            scenario = iterator.IteratorScenario(parent.user)

            testdata_consumer_mock = mocker.MagicMock()
            scenario.consumer = testdata_consumer_mock

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
            assert testdata_consumer_mock.stop.call_count == 1
            task_1_on_stop_spy.assert_called_once_with(scenario)
            task_2_on_stop_spy.assert_called_once_with(scenario)
        finally:
            reload(iterator)
