from typing import TYPE_CHECKING, Any, cast
from json import JSONDecodeError

import pytest

from pytest_mock import MockerFixture

from grizzly.tasks import LoopTask, GrizzlyTask, grizzlytask
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import RestartScenario, StopUser

from tests.fixtures import GrizzlyFixture
from tests.helpers import TestTask, TestExceptionTask

if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario


class TestErrorTask(TestTask):
    def __call__(self) -> grizzlytask:
        super_task = super().__call__()

        @grizzlytask
        def task(parent: 'GrizzlyScenario') -> Any:
            if self.task_call_count > 0:
                raise ValueError('error')
            else:
                super_task(parent)

        return task


class TestLoopTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        with pytest.raises(ValueError) as ve:
            LoopTask(grizzly=grizzly, name='test', values='["hello", "world"]', variable='asdf')
        assert str(ve.value) == 'LoopTask: asdf has not been initialized'

        grizzly.state.variables['asdf'] = 'none'
        task_factory = LoopTask(grizzly=grizzly, name='test', values='["hello", "world"]', variable='asdf')

        assert task_factory.name == 'test'
        assert task_factory.values == '["hello", "world"]'
        assert task_factory.variable == 'asdf'
        assert task_factory.__template_attributes__ == {'values', 'tasks'}

    def test_add_and_peek(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables['foobar'] = 'none'

        task_factory = LoopTask(grizzly=grizzly, name='test', values='["hello", "world"]', variable='foobar')

        assert len(task_factory.tasks) == 0

        task: GrizzlyTask
        task = grizzly_fixture.request_task.request
        task_factory.add(task)
        assert len(task_factory.tasks) == 1

        task = TestTask(name='test-1')
        task_factory.add(task)
        assert len(task_factory.tasks) == 2

        task = TestTask(name='test-2')
        task_factory.add(task)
        assert len(task_factory.tasks) == 3

        tasks = task_factory.peek()

        assert len(tasks) == 3
        assert isinstance(tasks[0], grizzly_fixture.request_task.request.__class__)
        assert isinstance(tasks[1], TestTask)
        assert isinstance(tasks[2], TestTask)
        assert tasks[2].name == 'test:test-2'

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        mocker.patch('grizzly.tasks.loop.gsleep', autospec=True)
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        request_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        scenario_context = GrizzlyContextScenario(3, behave=grizzly_fixture.behave.create_scenario('test scenario'))
        parent.user._scenario = scenario_context

        grizzly.state.variables['foobar'] = parent.user._context['variables']['foobar'] = 'none'

        task_factory = LoopTask(grizzly, 'test', '["hello", "world"]', 'foobar')

        for i in range(0, 3):
            task_factory.add(TestTask(name=f'{{{{ foobar }}}}-test-{i}'))

        assert sorted(task_factory.get_templates()) == sorted([
            'test:{{ foobar }}-test-0',
            'test:{{ foobar }}-test-1',
            'test:{{ foobar }}-test-2',
        ])

        task = task_factory()
        total_task___call___count = 0

        for _task in task_factory.peek():
            _task = cast(TestTask, _task)
            total_task___call___count += _task.call_count

        assert total_task___call___count == len(task_factory.tasks)

        # normal, static
        task(parent)

        assert request_spy.call_count == 7  # loop task + 3 tasks * 2 values

        for i, (args, kwargs) in enumerate(request_spy.call_args_list[:3]):
            assert args == ()
            assert kwargs.get('request_type', None) == 'TSTSK'
            assert kwargs.get('name', None) == f'TestTask: test:{{{{ foobar }}}}-test-{i}'
            assert kwargs.get('response_time', None) == 13
            assert kwargs.get('response_length', None) == 37
            assert kwargs.get('exception', '') is None
            assert kwargs.get('context', None) == {'variables': {'foobar': 'hello'}, 'log_all_requests': False}

        for i, (args, kwargs) in enumerate(request_spy.call_args_list[3:-1]):
            assert args == ()
            assert kwargs.get('request_type', None) == 'TSTSK'
            assert kwargs.get('name', None) == f'TestTask: test:{{{{ foobar }}}}-test-{i}'
            assert kwargs.get('response_time', None) == 13
            assert kwargs.get('response_length', None) == 37
            assert kwargs.get('exception', '') is None
            assert kwargs.get('context', None) == {'variables': {'foobar': 'world'}, 'log_all_requests': False}

        args, kwargs = request_spy.call_args_list[-1]

        assert args == ()
        assert kwargs.get('request_type', None) == 'LOOP'
        assert kwargs.get('name', None) == '003 test (3)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 2
        assert kwargs.get('exception', '') is None
        assert kwargs.get('context', None) == {'variables': {'foobar': 'none'}, 'log_all_requests': False}

        request_spy.reset_mock()

        # normal, variable input
        grizzly.state.variables['json_input'] = 'none'
        parent.user._context['variables']['json_input'] = '["foo", "bar"]'
        task_factory.values = '{{ json_input }}'
        task(parent)

        assert request_spy.call_count == 7  # loop task + 3 tasks * 2 values

        for i, (args, kwargs) in enumerate(request_spy.call_args_list[:3]):
            assert args == ()
            assert kwargs.get('request_type', None) == 'TSTSK'
            assert kwargs.get('name', None) == f'TestTask: test:{{{{ foobar }}}}-test-{i}'
            assert kwargs.get('response_time', None) == 13
            assert kwargs.get('response_length', None) == 37
            assert kwargs.get('exception', '') is None
            assert kwargs.get('context', None) == {'variables': {'foobar': 'foo', 'json_input': '["foo", "bar"]'}, 'log_all_requests': False}

        for i, (args, kwargs) in enumerate(request_spy.call_args_list[3:-1]):
            assert args == ()
            assert kwargs.get('request_type', None) == 'TSTSK'
            assert kwargs.get('name', None) == f'TestTask: test:{{{{ foobar }}}}-test-{i}'
            assert kwargs.get('response_time', None) == 13
            assert kwargs.get('response_length', None) == 37
            assert kwargs.get('exception', '') is None
            assert kwargs.get('context', None) == {'variables': {'foobar': 'bar', 'json_input': '["foo", "bar"]'}, 'log_all_requests': False}

        args, kwargs = request_spy.call_args_list[-1]

        assert args == ()
        assert kwargs.get('request_type', None) == 'LOOP'
        assert kwargs.get('name', None) == '003 test (3)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 2
        assert kwargs.get('exception', '') is None
        assert kwargs.get('context', None) == {'variables': {'foobar': 'none', 'json_input': '["foo", "bar"]'}, 'log_all_requests': False}

        request_spy.reset_mock()
        del grizzly.state.variables['json_input']
        del parent.user._context['variables']['json_input']

        # not a valid json input
        task_factory.values = '"hello'
        scenario_context.failure_exception = RestartScenario

        task = task_factory()

        with pytest.raises(RestartScenario):
            task(parent)

        assert request_spy.call_count == 1
        args, kwargs = request_spy.call_args_list[-1]
        assert args == ()
        assert kwargs.get('request_type', None) == 'LOOP'
        assert kwargs.get('name', None) == '003 test (3)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {'foobar': 'none'}, 'log_all_requests': False}
        exception = kwargs.get('exception', None)
        assert isinstance(exception, JSONDecodeError)
        assert str(exception).startswith('Unterminated string starting at:')

        request_spy.reset_mock()

        # valid json, but not a list
        task_factory.values = '{"hello": "world"}'

        task = task_factory()

        with pytest.raises(RestartScenario):
            task(parent)

        assert request_spy.call_count == 1
        args, kwargs = request_spy.call_args_list[-1]
        assert args == ()
        assert kwargs.get('request_type', None) == 'LOOP'
        assert kwargs.get('name', None) == '003 test (3)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == {'variables': {'foobar': 'none'}, 'log_all_requests': False}
        exception = kwargs.get('exception', None)
        assert str(exception) == '"{"hello": "world"}" is not a list'

        request_spy.reset_mock()

        # error in wrapped task
        task_factory.tasks = []
        task_factory.values = '["hello", "world"]'

        task_factory.add(TestTask(name='test-1'))
        task_factory.add(TestErrorTask(name='test-error-1'))

        task = task_factory()

        with pytest.raises(RestartScenario):
            task(parent)

        assert request_spy.call_count == 4

        args, kwargs = request_spy.call_args_list[-1]

        assert args == ()
        assert kwargs.get('request_type', None) == 'LOOP'
        assert kwargs.get('name', None) == '003 test (2)'
        assert kwargs.get('response_time', None) >= 0
        assert kwargs.get('response_length', None) == 2
        assert kwargs.get('context', None) == {'variables': {'foobar': 'world'}, 'log_all_requests': False}
        exception = kwargs.get('exception', '')
        assert isinstance(exception, ValueError)
        assert str(exception) == 'error'

        request_spy.reset_mock()

        # wrapped task throws scenario.failure_exception
        for failure_exception in [RestartScenario, StopUser]:
            scenario_context.failure_exception = failure_exception
            task_factory.tasks = []
            task_factory.add(TestExceptionTask(failure_exception))

            task = task_factory()

            with pytest.raises(failure_exception):
                task(parent)

            request_spy.assert_not_called()

    def test_on_event(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        mocker.patch('grizzly.tasks.loop.gsleep', autospec=True)
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        scenario_context = GrizzlyContextScenario(3, behave=grizzly_fixture.behave.create_scenario('test scenario'))
        scenario_context.name = scenario_context.description = 'test scenario'

        grizzly.state.variables['foobar'] = parent.user._context['variables']['foobar'] = 'none'

        task_factory = LoopTask(grizzly, 'test', '[1, 2, 3, 4]', 'foobar')
        task_factory.add(TestTask(name='test-1'))
        task_factory.add(TestTask(name='test-2'))

        on_start_mock = mocker.patch.object(TestTask, 'on_start', return_value=None)
        on_stop_mock = mocker.patch.object(TestTask, 'on_stop', return_value=None)

        task = task_factory()

        task.on_start(parent)

        assert on_start_mock.call_count == 2
        assert on_stop_mock.call_count == 0

        task.on_stop(parent)

        assert on_start_mock.call_count == 2
        assert on_stop_mock.call_count == 2
