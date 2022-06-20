from os import environ
from typing import TYPE_CHECKING, Callable, Any, Dict, List, Tuple

import pytest

from grizzly.tasks import GrizzlyTask, template

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
        return super().__call__()


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

    def test_get_templates(self) -> None:
        task = DummyTask()

        assert sorted(task.get_templates()) == sorted([
            '{{ string_template }}',
            '{{ list_template_1 }}',
            '{{ list_template_2 }}',
            '{{ list_template_3 }}',
            '{{ dict_template_1 }}',
            '{{ dict_template_2 }}',
        ])
