'''
@anchor pydoc:grizzly.tasks Tasks
Tasks are functionality that is executed by `locust` at run time as they are specified in the feature file.

The most essential task is {@pylink grizzly.tasks.request}, which all {@pylink grizzly.users} is using to make
requests to the endpoint that is being load tested.

All other tasks are helper tasks for things that needs to happen after or before a {@pylink grizzly.tasks.request}, stuff like extracting information from
a previous response or fetching additional test data from a different endpoint ("{@link pydoc:grizzly.tasks.clients}").

## Custom

It is possible to implement custom tasks, the only requirement is that they inherit `grizzly.tasks.GrizzlyTask`. To get them to be executed by `grizzly`,
a step implementation is also needed.

There are examples of this in the {@link framework.example}.
'''
from abc import ABC, ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, List, Type, Set, Optional, Union, Dict
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

    @abstractmethod
    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        raise NotImplementedError(f'{self.__class__.__name__} has not been implemented')

    def get_templates(self) -> List[str]:
        def is_template(value: str) -> bool:
            return '{{' in value and '}}' in value

        # tasks with no @template decorator
        if not hasattr(self, '__template_attributes__'):
            return []

        templates: Set[str] = set()
        for attribute in self.__template_attributes__:
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
                    if isinstance(list_value, GrizzlyTask):
                        templates.update(list_value.get_templates())
                    elif is_template(list_value):
                        templates.add(list_value)
            elif isinstance(value, dict):
                for dict_value in value.values():
                    if isinstance(dict_value, GrizzlyTask):
                        templates.update(dict_value.get_templates())
                    elif is_template(dict_value):
                        templates.add(dict_value)
            elif isinstance(value, GrizzlyTask):
                templates.update(value.get_templates())

        return list(templates)


class GrizzlyTaskWrapper(GrizzlyTask, metaclass=ABCMeta):
    name: str
    list: Union[List[GrizzlyTask], Dict[str, GrizzlyTask]]

    @abstractmethod
    def add(self, task: GrizzlyTask) -> None:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented add')

    @abstractmethod
    def peek(self) -> List[GrizzlyTask]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented peek')


class template:
    attributes: List[str]

    def __init__(self, attribute: str, *additional_attributes: str) -> None:
        self.attributes = [attribute]
        if len(additional_attributes) > 0:
            self.attributes += list(additional_attributes)

    def __call__(self, cls: Type[GrizzlyTask]) -> Type[GrizzlyTask]:
        cls.__template_attributes__ = self.attributes

        return cls


from .request import RequestTask, RequestTaskHandlers, RequestTaskResponse
from .wait import WaitTask
from .log_message import LogMessageTask
from .transformer import TransformerTask
from .until import UntilRequestTask
from .date import DateTask
from .async_group import AsyncRequestGroupTask
from .timer import TimerTask
from .task_wait import TaskWaitTask
from .conditional import ConditionalTask
from .loop import LoopTask


__all__ = [
    'RequestTaskHandlers',
    'RequestTaskResponse',
    'RequestTask',
    'LogMessageTask',
    'WaitTask',
    'TransformerTask',
    'UntilRequestTask',
    'DateTask',
    'AsyncRequestGroupTask',
    'TimerTask',
    'TaskWaitTask',
    'ConditionalTask',
    'LoopTask',
]
