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

Boilerplate example of a custom task:

```python
from typing import Any, cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.scenarios import GrizzlyScenario
from grizzly.types.behave import Context, then


class TestTask(GrizzlyTask):
    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            print(f'{self.__class__.__name__}::task called')

        @task.on_start
        def on_start() -> None:
            print(f'{self.__class__.__name__}::on_start called')

        @task.on_stop
        def on_stop() -> None:
            print(f'{self.__class__.__name__}::on_stop called')

        return task


@then(u'run `TestTask`')
def step_run_testtask(context: Context) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(TestTask())
```

There are examples of this in the {@link framework.example}.
'''
from __future__ import annotations
from abc import ABC, ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, List, Type, Set, Optional, Union, overload, cast
from os import environ
from pathlib import Path
from inspect import getmro

from grizzly_extras.transformer import TransformerContentType

from grizzly.context import GrizzlyContext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types import GrizzlyResponse
    from grizzly.scenarios import GrizzlyScenario

GrizzlyTaskType = Callable[['GrizzlyScenario'], Any]
GrizzlyTaskOnType = Callable[['GrizzlyScenario'], None]


class grizzlytask:
    __name__ = 'grizzlytask'

    _on_start: Optional['OnGrizzlyTask'] = None
    _on_stop: Optional['OnGrizzlyTask'] = None

    class OnGrizzlyTask:
        _on_func: GrizzlyTaskOnType

        def __init__(self, on_func: GrizzlyTaskOnType) -> None:
            self._on_func = on_func

        def __call__(self, parent: GrizzlyScenario) -> None:
            self._on_func(parent)

    def __init__(self, task: GrizzlyTaskType, doc: Optional[str] = None) -> None:
        self._task = task

        if doc is None and task is not None:
            self.__doc__ = task.__doc__

    def __call__(self, parent: GrizzlyScenario) -> Any:
        return self._task(parent)

    def _is_parent(self, arg: Any) -> bool:
        """
        ugly workaround since it is not possible to properly import GrizzlyScenario
        and use `isinstance` due to cyclic imports...
        """
        mro_list = [m.__name__ for m in getmro(arg.__class__)]

        return 'GrizzlyScenario' in mro_list

    @overload
    def on_start(self, parent: GrizzlyScenario, /) -> None:  # pragma: no coverage
        ...

    @overload
    def on_start(self, on_start: GrizzlyTaskOnType, /) -> None:  # pragma: no coverage
        ...

    def on_start(self, arg: Union[GrizzlyTaskOnType, GrizzlyScenario], /) -> None:
        is_parent = self._is_parent(arg)
        if is_parent and self._on_start is not None:
            self._on_start(cast('GrizzlyScenario', arg))
        elif not is_parent and self._on_start is None:
            self._on_start = self.OnGrizzlyTask(cast(GrizzlyTaskOnType, arg))
        else:  # decorated function does not exist, so don't do anything
            pass

    @overload
    def on_stop(self, parent: GrizzlyScenario, /) -> None:  # pragma: no coverage
        ...

    @overload
    def on_stop(self, on_start: GrizzlyTaskOnType, /) -> None:  # pragma: no coverage
        ...

    def on_stop(self, arg: Union[GrizzlyTaskOnType, GrizzlyScenario], /) -> None:
        is_parent = self._is_parent(arg)
        if is_parent and self._on_stop is not None:
            self._on_stop(cast('GrizzlyScenario', arg))
        elif not is_parent and self._on_stop is None:
            self._on_stop = self.OnGrizzlyTask(cast(GrizzlyTaskOnType, arg))
        else:  # decorated function does not exist, so don't do anything
            pass


class GrizzlyTask(ABC):
    __template_attributes__: Set[str] = set()

    _context_root: str

    step: str
    grizzly: GrizzlyContext

    def __init__(self) -> None:
        self._context_root = environ.get('GRIZZLY_CONTEXT_ROOT', '.')
        self.grizzly = GrizzlyContext()

    @abstractmethod
    def __call__(self) -> grizzlytask:
        raise NotImplementedError(f'{self.__class__.__name__} has not been implemented')  # pragma: no cover

    def get_templates(self) -> List[str]:
        def is_template(value: str) -> bool:
            return '{{' in value and '}}' in value

        def _get_value_templates(value: Any) -> Set[str]:
            templates: Set[str] = set()

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
                    else:
                        templates.update(_get_value_templates(list_value))
            elif isinstance(value, dict):
                for dict_value in value.values():
                    if isinstance(dict_value, GrizzlyTask):
                        templates.update(dict_value.get_templates())
                    else:
                        templates.update(_get_value_templates(dict_value))
            elif isinstance(value, GrizzlyTask):
                templates.update(value.get_templates())

            return templates

        templates: Set[str] = set()
        for attribute in self.__template_attributes__:
            value = getattr(self, attribute, None)
            if value is None:
                continue

            templates.update(_get_value_templates(value))

        return list(templates)


class GrizzlyMetaRequestTask(GrizzlyTask, metaclass=ABCMeta):
    content_type: TransformerContentType
    name: Optional[str]
    endpoint: str

    def execute(self, parent: 'GrizzlyScenario') -> 'GrizzlyResponse':
        raise NotImplementedError(f'{self.__class__.name} has not implemented "execute"')  # pragma: no cover

    def on_start(self, parent: 'GrizzlyScenario') -> None:
        pass

    def on_stop(self, parent: 'GrizzlyScenario') -> None:
        pass


class GrizzlyTaskWrapper(GrizzlyTask, metaclass=ABCMeta):
    name: str

    @abstractmethod
    def add(self, task: GrizzlyTask) -> None:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented add')  # pragma: no cover

    @abstractmethod
    def peek(self) -> List[GrizzlyTask]:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented peek')  # pragma: no cover


class template:
    attributes: List[str]

    def __init__(self, attribute: str, *additional_attributes: str) -> None:
        self.attributes = [attribute]
        if len(additional_attributes) > 0:
            self.attributes += list(additional_attributes)

    def __call__(self, cls: Type[GrizzlyTask]) -> Type[GrizzlyTask]:
        # this class should have it's own instance of this set, not shared with all
        # other tasks that inherits GrizzlyTask
        if len(cls.__template_attributes__) < 1:
            cls.__template_attributes__ = set(self.attributes)
        else:
            # this class already has an instance, but it might inherited by another class, that has some extra
            # attributes that could be a template, but that class should have its own instance
            original_attributes = cls.__template_attributes__.copy()
            cls.__template_attributes__ = original_attributes.union(self.attributes)

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
from .set_variable import SetVariableTask
from .keystore import KeystoreTask


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
    'SetVariableTask',
    'KeystoreTask',
]
