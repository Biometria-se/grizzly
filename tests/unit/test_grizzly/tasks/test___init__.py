from os import environ
from typing import Any, Dict, List, Tuple

import pytest

from pytest_mock import MockerFixture

from grizzly.tasks import (
    GrizzlyTask,
    LoopTask,
    ConditionalTask,
    RequestTask,
    AsyncRequestGroupTask,
    template,
    grizzlytask,
)
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.scenarios import GrizzlyScenario, IteratorScenario

from tests.fixtures import GrizzlyFixture


@template('string_template', 'list_template', 'dict_template')
class DummyTask(GrizzlyTask):
    string_template: str
    list_template: List[str]
    dict_template: Dict[str, str]

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.string_template = '{{ string_template }}'
        self.list_template = ['{{ list_template_1 }}', '{{ list_template_2 }}', '{{ list_template_3 }}']
        self.dict_template = {
            'dict_template_1': '{{ dict_template_1 }}',
            'dict_template_2': '{{ dict_template_2 }}',
        }

    def __call__(self) -> grizzlytask:
        raise NotImplementedError(f'{self.__class__.__name__} has not been implemented')


def on_func(parent: GrizzlyScenario) -> Any:
    """
    hello world
    """
    pass


def on_event(parent: GrizzlyScenario) -> None:
    pass


class Testgrizzlytask:
    def test___init__(self) -> None:
        task = grizzlytask(on_func)

        assert task._task is on_func
        assert task.__doc__ is not None
        assert task.__doc__.strip() == 'hello world'

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        task = grizzlytask(on_func)

        task_call_mock = mocker.spy(task, '_task')

        task(parent)

        task_call_mock.assert_called_once_with(parent)

    def test__is_parent(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        task = grizzlytask(on_func)
        assert not task._is_parent(on_func)

        dummy = DummyTask()
        assert not task._is_parent(dummy)

        class DummySomething(GrizzlyScenario):
            pass

        foo = DummySomething(parent.user)
        assert task._is_parent(foo)

        class DummyOther(IteratorScenario):
            pass

        bar = DummyOther(parent.user)
        assert task._is_parent(bar)

    @pytest.mark.parametrize('event', ['on_start', 'on_stop'])
    def test_on_event(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, event: str) -> None:
        parent = grizzly_fixture()

        task = grizzlytask(on_func)
        assert getattr(task, f'_{event}', True) is None

        # used as decorator
        getattr(task, event)(on_event)
        assert getattr(task, f'_{event}') is not None
        assert getattr(task, f'_{event}')._on_func is on_event

        # called
        on_event_mock = mocker.patch.object(getattr(task, f'_{event}'), '_on_func')
        getattr(task, event)(parent)

        on_event_mock.assert_called_once_with(parent)

        # called, but not set
        task = grizzlytask(on_func)

        task.on_start(parent)


class TestGrizzlyTask:
    def test___init__(self) -> None:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        task = DummyTask()

        assert task._context_root == '.'
        assert task.__template_attributes__ == {'string_template', 'list_template', 'dict_template'}

        try:
            environ['GRIZZLY_CONTEXT_ROOT'] = 'foo bar!'
            task = DummyTask()

            assert task._context_root == 'foo bar!'
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except KeyError:
                pass

    def test___call__(self) -> None:
        task = DummyTask()

        with pytest.raises(NotImplementedError) as nie:
            task()
        assert 'DummyTask has not been implemented' == str(nie.value)

    def test_get_templates(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        task = DummyTask()

        assert sorted(task.get_templates()) == sorted([
            '{{ string_template }}',
            '{{ list_template_1 }}',
            '{{ list_template_2 }}',
            '{{ list_template_3 }}',
            '{{ dict_template_1 }}',
            '{{ dict_template_2 }}',
        ])

        mocker.patch('grizzly.tasks.loop.gsleep', autospec=True)
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        scenario_context = GrizzlyContextScenario(3, behave=grizzly_fixture.behave.create_scenario('test scenario'))
        grizzly.scenarios.clear()
        grizzly.scenarios.append(scenario_context)

        grizzly.state.variables['endpoint_suffix'] = parent.user._context['variables']['endpoint_suffix'] = 'none'

        # conditional -> loop -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')

        conditional_factory.switch(True)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        for i in range(0, 3):
            loop_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None))

        conditional_factory.add(loop_factory)

        assert sorted(conditional_factory.get_templates()) == sorted([
            '/api/test/{{ endpoint_suffix }}',
            'conditional-{{ conditional_name }}',
            'loop-{{ loop_name }}:test-0',
            'loop-{{ loop_name }}:test-1',
            'loop-{{ loop_name }}:test-2',
            '{{ value | int > 0 }}',
        ])

        # loop -> conditional -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')

        conditional_factory.switch(True)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        for i in range(0, 3):
            conditional_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None))

        loop_factory.add(conditional_factory)

        assert sorted(loop_factory.get_templates()) == sorted([
            '/api/test/{{ endpoint_suffix }}',
            'conditional-{{ conditional_name }}:test-0',
            'conditional-{{ conditional_name }}:test-1',
            'conditional-{{ conditional_name }}:test-2',
            'loop-{{ loop_name }}:conditional-{{ conditional_name }}',
            '{{ value | int > 0 }}',
        ])

        # conditional -> loop -> async group -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')

        conditional_factory.switch(False)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        async_group_factory = AsyncRequestGroupTask('async-{{ async_name }}')

        for i in range(0, 3):
            async_group_factory.add(RequestTask(
                RequestMethod.GET,
                name=f'request-{i}-{{{{ request_name }}}}',
                endpoint='/api/test/{{ endpoint_suffix }}',
                source=None,
            ))

        loop_factory.add(async_group_factory)

        conditional_factory.add(loop_factory)

        assert sorted(conditional_factory.get_templates()) == sorted([
            '/api/test/{{ endpoint_suffix }}',
            'async-{{ async_name }}:request-0-{{ request_name }}',
            'async-{{ async_name }}:request-1-{{ request_name }}',
            'async-{{ async_name }}:request-2-{{ request_name }}',
            'conditional-{{ conditional_name }}',
            'loop-{{ loop_name }}:async-{{ async_name }}',
            '{{ value | int > 0 }}',
        ])
