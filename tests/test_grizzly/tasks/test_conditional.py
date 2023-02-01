from typing import Callable, Any, cast

import pytest

from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from grizzly.tasks import ConditionalTask
from grizzly.scenarios import GrizzlyScenario
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario, StopUser

from ...fixtures import GrizzlyFixture
from ...helpers import TestTask


class TestConditionalTask:
    def test___init__(self) -> None:
        task_factory = ConditionalTask(name='test', condition='{{ value | int > 0 }}')

        assert task_factory.tasks == {}
        assert task_factory.name == 'test'
        assert task_factory.condition == '{{ value | int > 0 }}'
        assert task_factory._pointer is None

    def test_switch(self) -> None:
        task_factory = ConditionalTask(name='test', condition='{{ value | int > 0 }}')

        assert task_factory._pointer is None

        task_factory.switch(True)
        assert getattr(task_factory, '_pointer', False)

        task_factory.switch(False)
        assert not getattr(task_factory, '_pointer', True)

        task_factory.switch(None)
        assert task_factory._pointer is None

    def test_add(self) -> None:
        task_factory = ConditionalTask(name='test', condition='{{ value | int > 0 }}')

        # do not add task
        assert task_factory._pointer is None
        assert task_factory.tasks == {}

        test_task = TestTask()
        task_factory.add(test_task)
        assert task_factory.tasks == {}

        # add as True task
        task_factory.switch(True)
        for _ in range(0, 3):
            task_factory.add(test_task)
        assert task_factory.tasks == {True: [test_task] * 3}

        # add as False task
        task_factory.switch(False)
        for _ in range(0, 4):
            task_factory.add(test_task)
        assert task_factory.tasks == {True: [test_task] * 3, False: [test_task] * 4}

        # task has name attribute, prefix it
        task_factory.add(TestTask(name='dummy task'))
        test_task = cast(TestTask, task_factory.tasks.get(False, [])[-1])
        assert test_task.name == 'test:dummy task'
        test_task = cast(TestTask, task_factory.tasks.get(False, [])[-2])
        assert test_task.name is None

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        # pre: get context
        _, _, scenario = grizzly_fixture()

        class TestRestartScenarioTask(TestTask):
            def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
                def task(parent: 'GrizzlyScenario') -> Any:
                    parent.user.environment.events.request.fire(
                        request_type='TSTSK',
                        name=f'TestTask: {self.name}',
                        response_time=13,
                        response_length=37,
                        context={},
                        exception=RuntimeError('error'),
                    )
                    raise RestartScenario()

                return task

        class TestStopUserTask(TestTask):
            def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
                def task(parent: 'GrizzlyScenario') -> Any:
                    parent.user.environment.events.request.fire(
                        request_type='TSTSK',
                        name=f'TestTask: {self.name}',
                        response_time=13,
                        response_length=37,
                        context={},
                        exception=RuntimeError('error'),
                    )
                    raise StopUser()

                return task

        assert scenario is not None

        scenario_context = GrizzlyContextScenario(1)
        scenario_context.name = scenario_context.description = 'test scenario'
        task_factory = ConditionalTask(name='test', condition='{{ value }}', scenario=scenario_context)

        mocker.patch('grizzly.tasks.conditional.gsleep', autospec=True)
        request_spy = mocker.spy(scenario.user.environment.events.request, 'fire')

        # pre: add tasks
        task_factory.switch(True)
        for i in range(0, 3):
            task_factory.add(TestTask(name=f'dummy-true-{i}', scenario=scenario_context))

        task_factory.switch(False)
        for i in range(0, 4):
            task_factory.add(TestTask(name=f'dummy-false-{i}', scenario=scenario_context))

        # pre: get task implementation
        task = task_factory()

        total_task___call___count = 0
        for _task in task_factory.tasks.get(True, []) + task_factory.tasks.get(False, []):
            _task = cast(TestTask, _task)
            total_task___call___count += _task.call_count

        assert total_task___call___count == len(task_factory.tasks.get(True, [])) + len(task_factory.tasks.get(False, []))

        # invalid condition, no scenario.failure_exception
        scenario.user._context.update({'variables': {'value': 'foobar'}})

        task(scenario)

        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: Invalid (0)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is scenario.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"{{ value }}" resolved to "foobar" which is invalid'

        # invalid condition, RestartScenario scenario.failure_exception
        scenario.user._context.update({'variables': {'value': 'foobar'}})
        scenario_context.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            task(scenario)

        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: Invalid (0)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is scenario.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"{{ value }}" resolved to "foobar" which is invalid'

        scenario_context.failure_exception = None  # reset

        # true condition
        task_factory.condition = '{{ value | int > 0 }}'
        scenario.user._context.update({'variables': {'value': 1}})

        task(scenario)

        assert request_spy.call_count == 3 + 3
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: True (3)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 3
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', RuntimeError()) is None

        request_calls = request_spy.call_args_list[2:-1]
        assert len(request_calls) == 3  # DummyTask

        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        # false condition
        scenario.user._context.update({'variables': {'value': 0}})

        task(scenario)

        assert request_spy.call_count == 4 + 3 + 4
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: False (4)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 4
        assert kwargs.get('context', None) is scenario.user._context
        assert kwargs.get('exception', RuntimeError()) is None

        request_calls = request_spy.call_args_list[-5:-1]
        assert len(request_calls) == 4
        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        task_factory.add(TestRestartScenarioTask(name=f'restart-scenario-false-{i}', scenario=scenario_context))
        task_factory.switch(True)
        task_factory.add(TestStopUserTask(name=f'stop-user-true-{i}', scenario=scenario_context))

        scenario._user.environment.stats.clear_all()

        task = task_factory()

        # false condition
        task(scenario)

        assert len(scenario._user.environment.stats.serialize_errors().keys()) == 1

        scenario._user.environment.stats.clear_all()

        # true condition
        task_factory.condition = '{{ value | int > 0 }}'
        scenario.user._context.update({'variables': {'value': 1}})

        task(scenario)

        assert len(scenario._user.environment.stats.serialize_errors().keys()) == 1
