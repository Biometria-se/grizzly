"""Unit tests for grizzly.tasks."""

from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, Any

import pytest
from grizzly.context import GrizzlyContextScenario
from grizzly.scenarios import GrizzlyScenario, IteratorScenario
from grizzly.tasks import (
    AsyncRequestGroupTask,
    ConditionalTask,
    GrizzlyTask,
    LoopTask,
    RequestTask,
    grizzlytask,
    template,
)
from grizzly.types import RequestMethod

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture, MockerFixture


@template('string_template', 'list_template', 'dict_template')
class DummyTask(GrizzlyTask):
    string_template: str
    list_template: list[str]
    dict_template: dict[str, str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.string_template = '{{ string_template }}'
        self.list_template = ['{{ list_template_1 }}', '{{ list_template_2 }}', '{{ list_template_3 }}']
        self.dict_template = {
            'dict_template_1': '{{ dict_template_1 }}',
            'dict_template_2': '{{ dict_template_2 }}',
        }

    def __call__(self) -> grizzlytask:
        message = f'{self.__class__.__name__} has not been implemented'
        raise NotImplementedError(message)


def on_func(_: GrizzlyScenario) -> Any:
    """Hello world."""


def on_event(_: GrizzlyScenario) -> None:
    pass


class Testgrizzlytask:
    def test___init__(self) -> None:
        task = grizzlytask(on_func)

        assert task._task is on_func
        assert task.__doc__ is not None
        assert task.__doc__.strip() == 'Hello world.'

        assert not hasattr(task, '__grizzly_metadata__')

        task = grizzlytask.metadata(timeout=1.0)(task)

        assert getattr(task, '__grizzly_metadata__', None) == {'timeout': 1.0, 'method': None, 'name': None}

        task = grizzlytask.metadata(timeout=10.0, method='TEST', name='foobar')(task)

        assert getattr(task, '__grizzly_metadata__', None) == {'timeout': 10.0, 'method': 'TEST', 'name': 'foobar'}

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

        dummy = DummyTask(timeout=None)
        assert not task._is_parent(dummy)

        class DummySomething(GrizzlyScenario):
            pass

        foo = DummySomething(parent.user)
        assert task._is_parent(foo)

        class DummyOther(IteratorScenario):
            pass

        bar = DummyOther(parent.user)
        assert task._is_parent(bar)

    @pytest.mark.parametrize('event', ['on_start', 'on_stop', 'on_iteration'])
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
        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']

        task = DummyTask(timeout=None)

        assert task._context_root == '.'
        assert task.__template_attributes__ == {'string_template', 'list_template', 'dict_template'}

        try:
            environ['GRIZZLY_CONTEXT_ROOT'] = 'foo bar!'
            task = DummyTask(timeout=None)

            assert task._context_root == 'foo bar!'
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

    def test___call__(self) -> None:
        task = DummyTask(timeout=None)

        with pytest.raises(NotImplementedError, match='DummyTask has not been implemented'):
            task()

    def test_get_templates(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        task = DummyTask(timeout=None)

        assert sorted(task.get_templates()) == sorted(
            [
                '{{ string_template }}',
                '{{ list_template_1 }}',
                '{{ list_template_2 }}',
                '{{ list_template_3 }}',
                '{{ dict_template_1 }}',
                '{{ dict_template_2 }}',
            ],
        )

        mocker.patch('grizzly.tasks.loop.gsleep', autospec=True)
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        scenario_context = GrizzlyContextScenario(3, behave=grizzly_fixture.behave.create_scenario('test scenario'), grizzly=grizzly)
        grizzly.scenarios.clear()
        grizzly.scenarios.append(scenario_context)
        grizzly.scenarios.deselect()
        parent.user._scenario = scenario_context

        parent.user._scenario.variables.update({'endpoint_suffix': 'none'})
        parent.user.set_variable('endpoint_suffix', 'none')

        # conditional -> loop -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')
        conditional_factory.switch(pointer=True)

        loop_factory = LoopTask('loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        for i in range(3):
            loop_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None))

        conditional_factory.add(loop_factory)

        assert sorted(conditional_factory.get_templates()) == sorted(
            [
                '/api/test/{{ endpoint_suffix }}',
                'conditional-{{ conditional_name }}',
                'loop-{{ loop_name }}:test-0',
                'loop-{{ loop_name }}:test-1',
                'loop-{{ loop_name }}:test-2',
                '{{ value | int > 0 }}',
            ],
        )

        # loop -> conditional -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')

        conditional_factory.switch(pointer=True)

        loop_factory = LoopTask('loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        for i in range(3):
            conditional_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None))

        loop_factory.add(conditional_factory)

        assert sorted(loop_factory.get_templates()) == sorted(
            [
                '/api/test/{{ endpoint_suffix }}',
                'conditional-{{ conditional_name }}:test-0',
                'conditional-{{ conditional_name }}:test-1',
                'conditional-{{ conditional_name }}:test-2',
                'loop-{{ loop_name }}:conditional-{{ conditional_name }}',
                '{{ value | int > 0 }}',
            ],
        )

        # conditional -> loop -> async group -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}')

        conditional_factory.switch(pointer=False)

        loop_factory = LoopTask('loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix')

        async_group_factory = AsyncRequestGroupTask('async-{{ async_name }}')

        for i in range(3):
            async_group_factory.add(
                RequestTask(
                    RequestMethod.GET,
                    name=f'request-{i}-{{{{ request_name }}}}',
                    endpoint='/api/test/{{ endpoint_suffix }}',
                    source=None,
                ),
            )

        loop_factory.add(async_group_factory)

        conditional_factory.add(loop_factory)

        assert sorted(conditional_factory.get_templates()) == sorted(
            [
                '/api/test/{{ endpoint_suffix }}',
                'async-{{ async_name }}:request-0-{{ request_name }}',
                'async-{{ async_name }}:request-1-{{ request_name }}',
                'async-{{ async_name }}:request-2-{{ request_name }}',
                'conditional-{{ conditional_name }}',
                'loop-{{ loop_name }}:async-{{ async_name }}',
                '{{ value | int > 0 }}',
            ],
        )
