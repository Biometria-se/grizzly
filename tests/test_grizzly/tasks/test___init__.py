from os import environ
from typing import TYPE_CHECKING, Callable, Any, Dict, List, Tuple

import pytest

from pytest_mock import MockerFixture

from grizzly.tasks import GrizzlyTask, LoopTask, ConditionalTask, RequestTask, AsyncRequestGroupTask, template
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario

from ...fixtures import GrizzlyFixture

if TYPE_CHECKING:
    from grizzly.scenarios import GrizzlyScenario


@template('string_template', 'list_template', 'dict_template')
class DummyTask(GrizzlyTask):
    string_template: str
    list_template: List[str]
    dict_template: Dict[str, str]

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
        self.string_template = '{{ string_template }}'
        self.list_template = ['{{ list_template_1 }}', '{{ list_template_2 }}', '{{ list_template_3 }}']
        self.dict_template = {
            'dict_template_1': '{{ dict_template_1 }}',
            'dict_template_2': '{{ dict_template_2 }}',
        }

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not been implemented')


class TestGrizzlyTask:
    def test___init__(self) -> None:

        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        task = DummyTask()

        assert task._context_root == '.'

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
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        grizzly = grizzly_fixture.grizzly

        scenario_context = GrizzlyContextScenario(3)
        scenario_context.name = scenario_context.description = 'test scenario'

        grizzly.state.variables['endpoint_suffix'] = scenario.user._context['variables']['endpoint_suffix'] = 'none'

        # conditional -> loop -> request
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}', scenario=scenario_context)

        conditional_factory.switch(True)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix', scenario_context)

        for i in range(0, 3):
            loop_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None, scenario=scenario_context))

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
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}', scenario=scenario_context)

        conditional_factory.switch(True)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix', scenario_context)

        for i in range(0, 3):
            conditional_factory.add(RequestTask(RequestMethod.GET, name=f'test-{i}', endpoint='/api/test/{{ endpoint_suffix }}', source=None, scenario=scenario_context))

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
        conditional_factory = ConditionalTask('conditional-{{ conditional_name }}', '{{ value | int > 0 }}', scenario=scenario_context)

        conditional_factory.switch(False)

        loop_factory = LoopTask(grizzly, 'loop-{{ loop_name }}', '["hello", "world"]', 'endpoint_suffix', scenario_context)

        async_group_factory = AsyncRequestGroupTask('async-{{ async_name }}', scenario=scenario_context)

        for i in range(0, 3):
            async_group_factory.add(RequestTask(
                RequestMethod.GET,
                name=f'request-{i}-{{{{ request_name }}}}',
                endpoint='/api/test/{{ endpoint_suffix }}',
                source=None,
                scenario=scenario_context,
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
