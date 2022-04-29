from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, List, Type, Set, Optional
from os import environ
from pathlib import Path

if TYPE_CHECKING:  # pragma: no cover
    from ..scenarios import GrizzlyScenario
    from ..context import GrizzlyContextScenario


class GrizzlyTask(ABC):
    __template_attributes__: List[str]

    _context_root: str

    scenario: 'GrizzlyContextScenario'

    def __init__(self, scenario: Optional['GrizzlyContextScenario'] = None) -> None:
        self._context_root = environ.get('GRIZZLY_CONTEXT_ROOT', '.')
        if scenario is not None:
            self.scenario = scenario

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not been implemented')

    def get_templates(self) -> List[str]:
        def is_template(value: str) -> bool:
            return '{{' in value and '}}' in value

        templates: Set[str] = set()
        attributes = getattr(self, '__template_attributes__', [])
        for attribute in attributes:
            value = getattr(self, attribute, None)
            if value is None:
                continue

            if isinstance(value, str):
                try:
                    possible_file = Path(self._context_root) / 'requests' / value
                    if possible_file.is_file():
                        with open(possible_file, 'r', encoding='utf-8') as fd:
                            value = fd.read()
                except OSError:
                    pass

                if is_template(value):
                    templates.add(value)
            elif isinstance(value, list):
                for list_value in value:
                    if is_template(list_value):
                        templates.add(list_value)
            elif isinstance(value, dict):
                for dict_value in value.values():
                    if is_template(dict_value):
                        templates.add(dict_value)

        return list(templates)


class template:
    attributes: List[str]

    def __init__(self, attribute: str, *additional_attributes: str) -> None:
        self.attributes = [attribute]
        if len(additional_attributes) > 0:
            self.attributes += list(additional_attributes)

    def __call__(self, task: Type[GrizzlyTask]) -> Type[GrizzlyTask]:

        setattr(task, '__template_attributes__', self.attributes)

        return task


from .request import RequestTask, RequestTaskHandlers, RequestTaskResponse
from .wait import WaitTask
from .print import PrintTask
from .transformer import TransformerTask
from .until import UntilRequestTask
from .date import DateTask
from .async_group import AsyncRequestGroupTask


__all__ = [
    'RequestTaskHandlers',
    'RequestTaskResponse',
    'RequestTask',
    'PrintTask',
    'WaitTask',
    'TransformerTask',
    'UntilRequestTask',
    'DateTask',
    'AsyncRequestGroupTask',
]
