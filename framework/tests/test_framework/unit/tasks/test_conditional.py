"""Unit tests of grizzly.tasks.conditional."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import pytest
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario, StopUser
from grizzly.tasks import ConditionalTask, grizzlytask

from test_framework.helpers import ANY, TestTask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


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

        task_factory.switch(pointer=True)
        assert getattr(task_factory, '_pointer', False)

        task_factory.switch(pointer=False)
        assert not getattr(task_factory, '_pointer', True)

        task_factory.switch(pointer=None)
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
        task_factory.switch(pointer=True)
        for _ in range(3):
            task_factory.add(test_task)
        assert task_factory.tasks == {True: [test_task] * 3}

        # add as False task
        task_factory.switch(pointer=False)
        for _ in range(4):
            task_factory.add(test_task)
        assert task_factory.tasks == {True: [test_task] * 3, False: [test_task] * 4}

        # peek at tasks
        task_factory.switch(pointer=True)
        assert len(task_factory.peek()) == 3

        task_factory.switch(pointer=False)
        assert len(task_factory.peek()) == 4

        task_factory._pointer = None
        assert len(task_factory.peek()) == 0

        # task has name attribute, prefix it
        task_factory.switch(pointer=False)
        task_factory.add(TestTask(name='dummy task'))
        test_task = cast('TestTask', task_factory.tasks.get(False, [])[-1])
        assert test_task.name == 'test:dummy task'
        test_task = cast('TestTask', task_factory.tasks.get(False, [])[-2])
        assert test_task.name is None

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        # pre: get context
        parent = grizzly_fixture()

        class TestRestartScenarioTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(parent: GrizzlyScenario) -> Any:
                    parent.user.environment.events.request.fire(
                        request_type='TSTSK',
                        name=f'TestTask: {self.name}',
                        response_time=13,
                        response_length=37,
                        context={},
                        exception=RuntimeError('error'),
                    )
                    raise RestartScenario

                return task

        class TestStopUserTask(TestTask):
            def __call__(self) -> grizzlytask:
                @grizzlytask
                def task(parent: GrizzlyScenario) -> Any:
                    parent.user.environment.events.request.fire(
                        request_type='TSTSK',
                        name=f'TestTask: {self.name}',
                        response_time=13,
                        response_length=37,
                        context={},
                        exception=RuntimeError('error'),
                    )
                    raise StopUser

                return task

        assert parent is not None

        scenario_context = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('test scenario'), grizzly=grizzly_fixture.grizzly)
        parent.user._scenario = scenario_context
        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        mocker.patch('grizzly.tasks.conditional.gsleep', autospec=True)
        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        # pre: add tasks
        task_factory.switch(pointer=True)
        for i in range(3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        task_factory.switch(pointer=False)
        for i in range(4):
            task_factory.add(TestTask(name=f'dummy-false-{i}'))

        # pre: get task implementation
        task = task_factory()

        total_task___call___count = 0
        for _task in task_factory.tasks.get(True, []) + task_factory.tasks.get(False, []):
            _task = cast('TestTask', _task)
            total_task___call___count += _task.call_count

        assert total_task___call___count == len(task_factory.tasks.get(True, [])) + len(task_factory.tasks.get(False, []))

        # invalid condition, no scenario.failure_exception
        parent.user.set_variable('value', 'foobar')

        task(parent)

        request_spy.assert_called_once_with(
            request_type='COND',
            name=f'{scenario_context.identifier} test: Invalid (0)',
            response_time=ANY(int),
            response_length=0,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='"{{ value }}" resolved to "foobar" which is invalid'),
        )
        request_spy.reset_mock()

        # invalid condition, RestartScenario scenario.failure_exception
        parent.user.set_variable('value', 'foobar')
        scenario_context.failure_handling.update({None: RestartScenario})

        with pytest.raises(RestartScenario):
            task(parent)

        request_spy.assert_called_once_with(
            request_type='COND',
            name=f'{scenario_context.identifier} test: Invalid (0)',
            response_time=ANY(int),
            response_length=0,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='"{{ value }}" resolved to "foobar" which is invalid'),
        )
        request_spy.reset_mock()

        with suppress(KeyError):
            del scenario_context.failure_handling[None]  # reset

        # true condition
        task_factory.condition = '{{ value | int > 0 }}'
        parent.user.set_variable('value', 1)

        task(parent)

        assert request_spy.call_count == 4
        args, kwargs = request_spy.call_args_list[-1]

        assert args == ()
        assert kwargs == {
            'request_type': 'COND',
            'name': f'{scenario_context.identifier} test: True (3)',
            'response_time': ANY(int),
            'response_length': 3,
            'context': parent.user._context,
            'exception': None,
        }

        request_calls = request_spy.call_args_list[:-1]
        assert len(request_calls) == 3  # DummyTask

        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        request_spy.reset_mock()

        # false condition
        parent.user.set_variable('value', '0')

        task(parent)

        assert request_spy.call_count == 5
        args, kwargs = request_spy.call_args_list[-1]

        assert args == ()
        assert kwargs == {
            'request_type': 'COND',
            'name': f'{scenario_context.identifier} test: False (4)',
            'response_time': ANY(int),
            'response_length': 4,
            'context': parent.user._context,
            'exception': None,
        }

        request_calls = request_spy.call_args_list[:-1]
        assert len(request_calls) == 4
        for _, kwargs in request_calls:
            assert kwargs.get('request_type', None) == 'TSTSK'

        task_factory.add(TestRestartScenarioTask(name=f'restart-scenario-false-{i}'))
        task_factory.switch(pointer=True)
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

        scenario_context = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('test scenario'), grizzly=grizzly_fixture.grizzly)
        parent.user._scenario = scenario_context
        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        task_factory.switch(pointer=True)
        for i in range(3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        task_factory.switch(pointer=False)
        for i in range(4):
            task_factory.add(TestTask(name=f'dummy-false-{i}'))

        mocker.patch('grizzly.tasks.conditional.gsleep', autospec=True)

        task = task_factory()

        on_start_mock = mocker.patch.object(TestTask, 'on_start', return_value=None)
        on_stop_mock = mocker.patch.object(TestTask, 'on_stop', return_value=None)

        task.on_start(parent)

        assert on_start_mock.call_count == 7
        assert on_stop_mock.call_count == 0

        task.on_stop(parent)

        assert on_start_mock.call_count == 7
        assert on_stop_mock.call_count == 7

        on_start_mock.reset_mock()
        on_stop_mock.reset_mock()

        task_factory = ConditionalTask(name='test', condition='{{ value }}')

        task_factory.switch(pointer=True)
        for i in range(3):
            task_factory.add(TestTask(name=f'dummy-true-{i}'))

        assert task_factory.tasks.get(False, None) is None

        task = task_factory()

        task.on_start(parent)

        assert on_start_mock.call_count == 3
        assert on_stop_mock.call_count == 0

        task.on_stop(parent)

        assert on_start_mock.call_count == 3
        assert on_stop_mock.call_count == 3
