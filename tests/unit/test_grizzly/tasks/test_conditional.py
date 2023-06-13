from typing import Any, cast

import pytest

from pytest_mock import MockerFixture
from grizzly.tasks import ConditionalTask, grizzlytask
from grizzly.scenarios import GrizzlyScenario
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario, StopUser

from tests.fixtures import GrizzlyFixture
from tests.helpers import TestTask


class TestConditionalTask:
    def test___init__(self) -> None:
        task_factory = ConditionalTask(name='test', condition='{{ value | int > 0 }}')

        assert task_factory.tasks == {}
        assert task_factory.name == 'test'
        assert task_factory.condition == '{{ value | int > 0 }}'
        assert task_factory._pointer is None
        assert task_factory.__template_attributes__ == {'condition', 'tasks', 'name'}
        assert task_factory.get_templates() == ['{{ value | int > 0 }}']

        task_factory = ConditionalTask(name='test', condition='{{ value == "True" }}')
        assert task_factory.get_templates() == ['{{ value == "True" }}']

    def test_switch(self) -> None:
        task_factory = ConditionalTask(name='test', condition='{{ value | int > 0 }}')

        assert task_factory._pointer is None

        task_factory.switch(True)
        assert getattr(task_factory, '_pointer', False)

        task_factory.switch(False)
        assert not getattr(task_factory, '_pointer', True)

        task_factory.switch(None)
        assert task_factory._pointer is None

    def test_add_and_peek(self) -> None:
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

        # peek at tasks
        task_factory.switch(True)
        assert len(task_factory.peek()) == 3

        task_factory.switch(False)
        assert len(task_factory.peek()) == 4

        task_factory._pointer = None
        assert len(task_factory.peek()) == 0

        # task has name attribute, prefix it
        task_factory.switch(False)
        task_factory.add(TestTask(name='dummy task'))
        test_task = cast(TestTask, task_factory.tasks.get(False, [])[-1])
        assert test_task.name == 'test:dummy task'
        test_task = cast(TestTask, task_factory.tasks.get(False, [])[-2])
        assert test_task.name is None

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        # pre: get context
        parent = grizzly_fixture()

        class TestRestartScenarioTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
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
            def __call__(self) -> grizzlytask:
                @grizzlytask
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

        assert parent is not None

        scenario_context = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('test scenario'))
        parent.user._scenario = scenario_context
        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        mocker.patch('grizzly.tasks.conditional.gsleep', autospec=True)
        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        # pre: add tasks
        task_factory.switch(True)
        for i in range(0, 3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        task_factory.switch(False)
        for i in range(0, 4):
            task_factory.add(TestTask(name=f'dummy-false-{i}'))

        # pre: get task implementation
        task = task_factory()

        total_task___call___count = 0
        for _task in task_factory.tasks.get(True, []) + task_factory.tasks.get(False, []):
            _task = cast(TestTask, _task)
            total_task___call___count += _task.call_count

        assert total_task___call___count == len(task_factory.tasks.get(True, [])) + len(task_factory.tasks.get(False, []))

        # invalid condition, no scenario.failure_exception
        parent.user._context.update({'variables': {'value': 'foobar'}})

        task(parent)

        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: Invalid (0)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"{{ value }}" resolved to "foobar" which is invalid'

        # invalid condition, RestartScenario scenario.failure_exception
        parent.user._context.update({'variables': {'value': 'foobar'}})
        scenario_context.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            task(parent)

        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: Invalid (0)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"{{ value }}" resolved to "foobar" which is invalid'

        scenario_context.failure_exception = None  # reset

        # true condition
        task_factory.condition = '{{ value | int > 0 }}'
        parent.user._context.update({'variables': {'value': 1}})

        task(parent)

        assert request_spy.call_count == 3 + 3
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: True (3)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 3
        assert kwargs.get('context', None) is parent.user._context
        assert kwargs.get('exception', RuntimeError()) is None

        request_calls = request_spy.call_args_list[2:-1]
        assert len(request_calls) == 3  # DummyTask

        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        # false condition
        parent.user._context.update({'variables': {'value': 0}})

        task(parent)

        assert request_spy.call_count == 4 + 3 + 4
        _, kwargs = request_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'COND'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test: False (4)'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 4
        assert kwargs.get('context', None) is parent.user._context
        assert kwargs.get('exception', RuntimeError()) is None

        request_calls = request_spy.call_args_list[-5:-1]
        assert len(request_calls) == 4
        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        task_factory.add(TestRestartScenarioTask(name=f'restart-scenario-false-{i}'))
        task_factory.switch(True)
        task_factory.add(TestStopUserTask(name=f'stop-user-true-{i}'))

        parent._user.environment.stats.clear_all()

        task = task_factory()

        # false condition
        task(parent)

        assert len(parent._user.environment.stats.serialize_errors().keys()) == 1

        parent._user.environment.stats.clear_all()

        # true condition
        task_factory.condition = '{{ value | int > 0 }}'
        parent.user._context.update({'variables': {'value': 1}})

        task(parent)

        assert len(parent._user.environment.stats.serialize_errors().keys()) == 1

    def test_on_event(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        scenario_context = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('test scenario'))
        parent.user._scenario = scenario_context
        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        task_factory.switch(True)
        for i in range(0, 3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        task_factory.switch(False)
        for i in range(0, 4):
            task_factory.add(TestTask(name=f'dummy-false-{i}'))

        mocker.patch('grizzly.tasks.conditional.gsleep', autospec=True)

        task = task_factory()

        on_start_mock = mocker.patch.object(TestTask, 'on_start', return_value=None)
        on_stop_mock = mocker.patch.object(TestTask, 'on_stop', return_value=None)

        task.on_start(parent)

        on_start_mock.call_count == 7
        on_stop_mock.call_count == 0

        task.on_stop(parent)

        on_start_mock.call_count == 7
        on_stop_mock.call_count == 7

        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()

        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        task_factory.switch(True)
        for i in range(0, 3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        assert task_factory.tasks.get(False, None) is None

        task = task_factory()

        task.on_start(parent)

        on_start_mock.call_count == 3
        on_stop_mock.call_count == 0

        task.on_stop(parent)

        on_start_mock.call_count == 3
        on_stop_mock.call_count == 3
