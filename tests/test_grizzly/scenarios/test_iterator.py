import logging

from os import environ
from typing import Callable, Dict, Any, Optional, List

import pytest

from _pytest.logging import LogCaptureFixture
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from locust.user.task import TaskSet, LOCUST_STATE_STOPPING, LOCUST_STATE_RUNNING
from locust.exception import StopUser, InterruptTaskSet, RescheduleTask, RescheduleTaskImmediately

from grizzly.scenarios.iterator import IteratorScenario
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform
from grizzly.tasks import WaitTask, PrintTask
from grizzly.exceptions import RestartScenario

from ..fixtures import grizzly_context, request_task  # pylint: disable=unused-import
from ..helpers import RequestCalled


class TestIterationScenario:
    @pytest.mark.usefixtures('grizzly_context')
    def test_initialize(self, grizzly_context: Callable) -> None:
        _, _, task, _ = grizzly_context()
        assert issubclass(task.__class__, TaskSet)

    @pytest.mark.usefixtures('grizzly_context')
    def test_add_scenario_task(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        _, user, task, [_, _, request] = grizzly_context(task_type=IteratorScenario)
        request.endpoint = '/api/v1/test'
        IteratorScenario.add_scenario_task(request)
        assert isinstance(task, IteratorScenario)
        assert len(task.tasks) == 2

        task_method = task.tasks[-1]

        assert callable(task_method)
        with pytest.raises(RequestCalled) as e:
            task_method(task)
        assert e.value.endpoint == '/api/v1/test' and e.value.request is request

        def generate_mocked_wait(sleep_time: float) -> None:
            def mocked_wait(time: float) -> None:
                assert sleep_time == time

            mocker.patch(
                'grizzly.tasks.wait.gsleep',
                mocked_wait,
            )

        generate_mocked_wait(1.5)
        IteratorScenario.add_scenario_task(WaitTask(time=1.5))
        assert len(task.tasks) == 3

        task_method = task.tasks[-1]
        assert callable(task_method)
        task_method(task)

        IteratorScenario.add_scenario_task(PrintTask(message='hello {{ world }}'))
        assert len(task.tasks) == 4

        logger_spy = mocker.spy(task.logger, 'info')

        task_method = task.tasks[-1]
        assert callable(task_method)
        task_method(task)

        assert logger_spy.call_count == 1
        args, _ = logger_spy.call_args_list[0]
        assert args[0] == 'hello '

        user.set_context_variable('world', 'world!')

        task_method(task)

        assert logger_spy.call_count == 2
        args, _ = logger_spy.call_args_list[1]
        assert args[0] == 'hello world!'

    @pytest.mark.usefixtures('grizzly_context')
    def test_on_event_handlers(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        try:
            _, _, task, _ = grizzly_context(task_type=IteratorScenario)

            def TestdataConsumer__init__(self: 'TestdataConsumer', address: str) -> None:
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

            assert task is not None

            with pytest.raises(StopUser):
                task.on_start()

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'localhost:5555'

            task.on_start()

            with pytest.raises(StopUser):
                task.on_stop()
        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except KeyError:
                pass

    @pytest.mark.usefixtures('grizzly_context')
    def test_iterator(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        _, user, task, _ = grizzly_context(task_type=IteratorScenario)

        assert task is not None

        task.consumer = TestdataConsumer()

        def mock_request(data: Optional[Dict[str, Any]]) -> None:
            def request(self: 'TestdataConsumer', scenario: str) -> Optional[Dict[str, Any]]:
                if data is None or data == {}:
                    return None

                if 'variables' in data:
                    data['variables'] = transform(data['variables'])

                return data

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.request',
                request,
            )

        mock_request(None)

        with pytest.raises(StopUser):
            task.iterator()

        assert user.context_variables == {}

        mock_request({})

        with pytest.raises(StopUser):
            task.iterator()

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

        task.iterator()

        assert user.context_variables['AtomicIntegerIncrementer'].messageID == 1337
        assert user.context_variables['AtomicCsvRow'].test.header1 == 'value1'
        assert user.context_variables['AtomicCsvRow'].test.header2 == 'value2'

    @pytest.mark.usefixtures('grizzly_context')
    def test_run(self, grizzly_context: Callable, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        _, user, scenario, _ = grizzly_context(task_type=IteratorScenario)

        assert scenario is not None

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

        assert scenario._task_index == 0
        assert on_start.call_count == 9
        assert on_stop.call_count == 2
        assert get_next_task.call_count == 2
        assert schedule_task.call_count == 1
        assert execute_next_task.call_count == 1
        assert wait.call_count == 1
        assert user_error.call_count == 4

        user_error.reset_mock()
        wait.reset_mock()
        user.environment.catch_exceptions = True

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
        assert 'ERROR' in caplog.text and 'grizzly.scenarios' in caplog.text
        assert 'Traceback (most recent call last):' in caplog.text
