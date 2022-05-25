import logging

from os import environ
from typing import TYPE_CHECKING, Dict, Any, Optional, List

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from locust.user.task import LOCUST_STATE_STOPPING, LOCUST_STATE_RUNNING
from locust.user.sequential_taskset import SequentialTaskSet
from locust.exception import StopUser, InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately

from grizzly.scenarios.iterator import IteratorScenario
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform
from grizzly.tasks import WaitTask, LogMessage
from grizzly.exceptions import RestartScenario, StopScenario
from grizzly.types import ScenarioState

from ...fixtures import GrizzlyFixture
from ...helpers import RequestCalled


if TYPE_CHECKING:
    from grizzly.context import GrizzlyContext


class TestIterationScenario:
    def test_initialize(self, grizzly_fixture: GrizzlyFixture) -> None:
        _, _, scenario = grizzly_fixture(scenario_type=IteratorScenario)
        assert isinstance(scenario, IteratorScenario)
        assert issubclass(scenario.__class__, SequentialTaskSet)

    def test_populate(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, user, scenario = grizzly_fixture(scenario_type=IteratorScenario)
        request = grizzly_fixture.request_task.request
        request.endpoint = '/api/v1/test'
        IteratorScenario.populate(request)
        assert isinstance(scenario, IteratorScenario)
        assert len(scenario.tasks) == 2

        task_method = scenario.tasks[-1]

        assert callable(task_method)
        with pytest.raises(RequestCalled) as e:
            task_method(scenario)
        assert e.value.endpoint == '/api/v1/test' and e.value.request is request

        def generate_mocked_wait(sleep_time: float) -> None:
            def mocked_wait(time: float) -> None:
                assert sleep_time == time

            mocker.patch(
                'grizzly.tasks.wait.gsleep',
                mocked_wait,
            )

        generate_mocked_wait(1.5)
        IteratorScenario.populate(WaitTask(time=1.5))
        assert len(scenario.tasks) == 3

        task_method = scenario.tasks[-1]
        assert callable(task_method)
        task_method(scenario)

        IteratorScenario.populate(LogMessage(message='hello {{ world }}'))
        assert len(scenario.tasks) == 4

        logger_spy = mocker.spy(scenario.logger, 'info')

        task_method = scenario.tasks[-1]
        assert callable(task_method)
        task_method(scenario)

        assert logger_spy.call_count == 1
        args, _ = logger_spy.call_args_list[0]
        assert args[0] == 'hello '

        user.set_context_variable('world', 'world!')

        task_method(scenario)

        assert logger_spy.call_count == 2
        args, _ = logger_spy.call_args_list[1]
        assert args[0] == 'hello world!'

    def test_on_event_handlers(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        try:
            _, _, scenario = grizzly_fixture(scenario_type=IteratorScenario)

            def TestdataConsumer__init__(self: 'TestdataConsumer', grizzly: 'GrizzlyContext', address: str, identifier: str) -> None:
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

            assert scenario is not None

            with pytest.raises(StopUser):
                scenario.on_start()

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'localhost:5555'

            scenario.on_start()

            with pytest.raises(StopUser):
                scenario.on_stop()
        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except KeyError:
                pass

    def test_iterator(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, user, scenario = grizzly_fixture(scenario_type=IteratorScenario)

        grizzly = grizzly_fixture.grizzly

        assert scenario is not None
        assert isinstance(scenario, IteratorScenario)

        scenario.consumer = TestdataConsumer(grizzly, identifier='test')

        def mock_request(data: Optional[Dict[str, Any]]) -> None:
            def request(self: 'TestdataConsumer', scenario: str) -> Optional[Dict[str, Any]]:
                if data is None or data == {}:
                    return None

                if 'variables' in data:
                    data['variables'] = transform(grizzly, data['variables'])

                return data

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.request',
                request,
            )

        mock_request(None)

        with pytest.raises(StopScenario):
            scenario.iterator()

        assert user.context_variables == {}

        mock_request({})

        with pytest.raises(StopScenario):
            scenario.iterator()

        assert user.context_variables == {}

        mock_request({
            'variables': {
                'AtomicIntegerIncrementer.messageID': 1337,
                'AtomicCsvRow.test': {
                    'header1': 'value1',
                    'header2': 'value2',
                },
            },
        })

        scenario.iterator()

        assert user.context_variables['AtomicIntegerIncrementer'].messageID == 1337
        assert user.context_variables['AtomicCsvRow'].test.header1 == 'value1'
        assert user.context_variables['AtomicCsvRow'].test.header2 == 'value2'

    def test_iterator_error(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture(scenario_type=IteratorScenario)
        assert isinstance(scenario, IteratorScenario)

        stats_log_mock = mocker.patch.object(scenario.stats, 'log', return_value=None)
        stats_log_error_mock = mocker.patch.object(scenario.stats, 'log_error', return_value=None)

        mocker.patch('grizzly.scenarios.iterator.perf_counter', return_value=11.0)

        scenario.start = None

        scenario.iterator_error()

        assert stats_log_mock.call_count == 1
        args, _ = stats_log_mock.call_args_list[-1]
        assert len(args) == 2
        assert args[0] == 0
        assert args[1] == (scenario._task_index % scenario.task_count) + 1

        assert stats_log_error_mock.call_count == 1
        args, _ = stats_log_error_mock.call_args_list[-1]
        assert len(args) == 1
        assert args[0] is None

        scenario.start = 1.0

        scenario.iterator_error()

        assert stats_log_mock.call_count == 2
        args, _ = stats_log_mock.call_args_list[-1]
        assert len(args) == 2
        assert args[0] == 10000
        assert args[1] == (scenario._task_index % scenario.task_count) + 1

        assert stats_log_error_mock.call_count == 2
        args, _ = stats_log_error_mock.call_args_list[-1]
        assert len(args) == 1
        assert args[0] is None

    def test_wait(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, _, scenario = grizzly_fixture(scenario_type=IteratorScenario)

        assert scenario is not None
        assert isinstance(scenario, IteratorScenario)

        wait_mocked = mocker.patch('grizzly.scenarios.iterator.GrizzlyScenario.wait', return_value=None)
        sleep_mocked = mocker.patch.object(scenario, '_sleep', return_value=None)

        scenario.user._scenario_state = ScenarioState.STOPPING
        scenario.user._state = LOCUST_STATE_STOPPING
        scenario.task_count = 10
        scenario._task_index = 3

        with caplog.at_level(logging.DEBUG):
            scenario.wait()

        assert wait_mocked.call_count == 0
        assert sleep_mocked.call_count == 1
        assert scenario.user._state == LOCUST_STATE_RUNNING
        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == 'not finished with scenario, currently at task 4 of 10, let me be!'
        caplog.clear()

        scenario._task_index = 9

        with caplog.at_level(logging.DEBUG):
            scenario.wait()

        assert wait_mocked.call_count == 1
        assert scenario.user._state == LOCUST_STATE_STOPPING
        assert scenario.user.scenario_state == ScenarioState.STOPPED

        assert len(caplog.messages) == 2
        assert caplog.messages[-2] == "okay, I'm done with my running tasks now"
        assert caplog.messages[-1] == "scenario state=ScenarioState.STOPPING -> ScenarioState.STOPPED"
        caplog.clear()

        scenario.user._scenario_state = ScenarioState.STOPPED

        with caplog.at_level(logging.DEBUG):
            scenario.wait()

        assert wait_mocked.call_count == 2
        assert len(caplog.messages) == 0

    def test_run(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, user, scenario = grizzly_fixture(scenario_type=IteratorScenario)

        assert scenario is not None
        assert isinstance(scenario, IteratorScenario)

        side_effects: List[Optional[InterruptTaskSet]] = [
            InterruptTaskSet(reschedule=False),
            InterruptTaskSet(reschedule=True),
        ]
        side_effects.extend([None] * 10)

        on_stop = mocker.patch.object(scenario, 'on_stop', autospec=True)
        on_start = mocker.patch.object(
            scenario,
            'on_start',
            side_effect=side_effects,
        )

        mocker.patch.object(scenario, 'tasks', [f'task-{index}' for index in range(0, 10)])
        scenario.task_count = len(scenario.tasks)

        schedule_task = mocker.patch.object(scenario, 'schedule_task', autospec=True)
        get_next_task = mocker.patch.object(scenario, 'get_next_task', autospec=True)
        wait = mocker.patch.object(scenario, 'wait', autospec=True)
        user_error = mocker.spy(scenario.user.environment.events.user_error, 'fire')

        with pytest.raises(RescheduleTask):
            scenario.run()

        assert on_start.call_count == 1

        with pytest.raises(RescheduleTaskImmediately):
            scenario.run()

        assert on_start.call_count == 2

        user._state = LOCUST_STATE_STOPPING

        with pytest.raises(StopUser):
            scenario.run()

        user._state = LOCUST_STATE_RUNNING

        assert on_start.call_count == 3
        assert get_next_task.call_count == 1
        assert schedule_task.call_count == 1

        user.environment.catch_exceptions = False

        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(scenario, 'execute_next_task', side_effect=[RescheduleTaskImmediately])

        with pytest.raises(RuntimeError):
            scenario.run()

        assert on_start.call_count == 4
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 2
        assert execute_next_task.call_count == 1
        assert wait.call_count == 0
        assert user_error.call_count == 1
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is scenario
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(scenario, 'execute_next_task', side_effect=[RescheduleTask])

        with pytest.raises(RuntimeError):
            scenario.run()

        assert on_start.call_count == 5
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 3
        assert execute_next_task.call_count == 1
        assert wait.call_count == 1
        assert user_error.call_count == 2
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is scenario
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(scenario, 'execute_next_task')

        with pytest.raises(RuntimeError):
            scenario.run()

        assert on_start.call_count == 6
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 4
        assert execute_next_task.call_count == 1
        assert wait.call_count == 2
        assert user_error.call_count == 3
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is scenario
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None

        on_stop.reset_mock()
        wait.reset_mock()
        schedule_task.reset_mock()

        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[InterruptTaskSet(reschedule=False), InterruptTaskSet(reschedule=True)])
        execute_next_task = mocker.patch.object(scenario, 'execute_next_task')

        with pytest.raises(RescheduleTask):
            scenario.run()

        assert on_start.call_count == 7
        assert on_stop.call_count == 1
        assert get_next_task.call_count == 1
        assert schedule_task.call_count == 0
        assert execute_next_task.call_count == 0
        assert wait.call_count == 0
        assert user_error.call_count == 3

        with pytest.raises(RescheduleTaskImmediately):
            scenario.run()

        assert on_start.call_count == 8
        assert on_stop.call_count == 2
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 0
        assert execute_next_task.call_count == 0
        assert wait.call_count == 0
        assert user_error.call_count == 3

        scenario._task_index = 10
        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[None, RuntimeError])
        execute_next_task = mocker.patch.object(scenario, 'execute_next_task', side_effect=[RestartScenario])

        with pytest.raises(RuntimeError):
            scenario.run()

        assert scenario._task_index == 20
        assert on_start.call_count == 9
        assert on_stop.call_count == 2
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 1
        assert execute_next_task.call_count == 1
        assert wait.call_count == 1
        assert user_error.call_count == 4

        user_error.reset_mock()
        wait.reset_mock()
        scenario.user.environment.catch_exceptions = True

        get_next_task = mocker.patch.object(scenario, 'get_next_task', side_effect=[RuntimeError, StopUser])

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                scenario.run()

        assert wait.call_count == 1
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 1
        assert execute_next_task.call_count == 1
        assert user_error.call_count == 1
        _, kwargs = user_error.call_args_list[-1]
        assert kwargs.get('user_instance', None) is scenario
        assert isinstance(kwargs.get('exception', None), RuntimeError)
        assert kwargs.get('tb', None) is not None
        assert 'ERROR' in caplog.text and 'IteratorScenario' in caplog.text
        assert 'Traceback (most recent call last):' in caplog.text
