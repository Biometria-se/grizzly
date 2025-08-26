"""Tasks are functionality that is executed by `locust` at run time as they are specified in the feature file.

The most essential task is [Request task][grizzly.tasks.request], which all [load users][grizzly.users] is using to make
requests to the endpoint that is being load tested.

All other tasks are helper tasks for things that needs to happen after or before a [Request task][grizzly.tasks.request], stuff like extracting information from
a previous response or fetching additional test data from a different endpoint ([client tasks][grizzly.tasks.clients]).

## Custom

It is possible to implement custom tasks, the only requirement is that they inherit `grizzly.tasks.GrizzlyTask`. To get them to be executed by `grizzly`,
a step implementation is also needed.

You can also set some metadata (timeout, method, name) on a task with the `@grizzlytask.metadata` decorator. This is not mandatory, but can be useful if
the task should not be able to run forever.

Boilerplate example of a custom task:

```python
from typing import Any, cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import GrizzlyTask, grizzlytask
from grizzly.scenarios import GrizzlyScenario
from grizzly.types.behave import Context, then


class TestTask(GrizzlyTask):
    def __call__(self) -> grizzlytask:
        @grizzlytask.metadata(timeout=20.0, method='TASK', name='TestTask')
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


@then('run `TestTask`')
def step_run_testtask(context: Context) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(TestTask())
```

There are examples of this in the [example][example] documentation.

"""

from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Callable
from inspect import getmro
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast, overload

from grizzly.utils import has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly_common.transformer import TransformerContentType

    from grizzly.context import GrizzlyContext
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.testdata.communication import GrizzlyDependencies
    from grizzly.types import GrizzlyResponse

GrizzlyTaskType = Callable[['GrizzlyScenario'], Any]
GrizzlyTaskOnType = Callable[['GrizzlyScenario'], None]


class grizzlytask:
    __name__ = 'grizzlytask'

    _on_start: OnGrizzlyTask | None = None
    _on_stop: OnGrizzlyTask | None = None
    _on_iteration: OnGrizzlyTask | None = None

    class OnGrizzlyTask:
        _on_func: GrizzlyTaskOnType

        def __init__(self, on_func: GrizzlyTaskOnType) -> None:
            self._on_func = on_func

        def __call__(self, parent: GrizzlyScenario) -> None:
            self._on_func(parent)

    def __init__(self, task: GrizzlyTaskType, doc: str | None = None) -> None:
        self._task = task

        if doc is None and task is not None:
            self.__doc__ = task.__doc__

    def __call__(self, parent: GrizzlyScenario) -> Any:
        return self._task(parent)

    def _is_parent(self, arg: Any) -> bool:
        """Ugly workaround since it is not possible to properly import GrizzlyScenario
        and use `isinstance` due to cyclic imports...
        """
        mro_list = [m.__name__ for m in getmro(arg.__class__)]

        return 'GrizzlyScenario' in mro_list

    @staticmethod
    def metadata(*, timeout: float | None = None, method: str | None = None, name: str | None = None) -> Callable[[grizzlytask], grizzlytask]:
        def wrapper(task: grizzlytask) -> grizzlytask:
            setattr(task, '__grizzly_metadata__', {'timeout': timeout, 'method': method, 'name': name})  # noqa: B010

            return task

        return wrapper

    @overload
    def on_start(self, parent: GrizzlyScenario, /) -> None:  # pragma: no coverage
        ...

    @overload
    def on_start(self, on_start: GrizzlyTaskOnType, /) -> None:  # pragma: no coverage
        ...

    def on_start(self, arg: GrizzlyTaskOnType | GrizzlyScenario, /) -> None:
        is_parent = self._is_parent(arg)
        if is_parent and self._on_start is not None:
            self._on_start(cast('GrizzlyScenario', arg))
        elif not is_parent and self._on_start is None:
            self._on_start = self.OnGrizzlyTask(cast('GrizzlyTaskOnType', arg))
        else:  # decorated function does not exist, so don't do anything
            pass

    @overload
    def on_stop(self, parent: GrizzlyScenario, /) -> None:  # pragma: no coverage
        ...

    @overload
    def on_stop(self, on_start: GrizzlyTaskOnType, /) -> None:  # pragma: no coverage
        ...

    def on_stop(self, arg: GrizzlyTaskOnType | GrizzlyScenario, /) -> None:
        is_parent = self._is_parent(arg)
        if is_parent and self._on_stop is not None:
            self._on_stop(cast('GrizzlyScenario', arg))
        elif not is_parent and self._on_stop is None:
            self._on_stop = self.OnGrizzlyTask(cast('GrizzlyTaskOnType', arg))
        else:  # decorated function does not exist, so don't do anything
            pass

    @overload
    def on_iteration(self, parent: GrizzlyScenario, /) -> None:  # pragma: no coverage
        ...

    @overload
    def on_iteration(self, on_start: GrizzlyTaskOnType, /) -> None:  # pragma: no coverage
        ...

    def on_iteration(self, arg: GrizzlyTaskOnType | GrizzlyScenario, /) -> None:
        is_parent = self._is_parent(arg)
        if is_parent and self._on_iteration is not None:
            self._on_iteration(cast('GrizzlyScenario', arg))
        elif not is_parent and self._on_iteration is None:
            self._on_iteration = self.OnGrizzlyTask(cast('GrizzlyTaskOnType', arg))
        else:  # decorated function does not exist, so don't do anything
            pass


class GrizzlyTask(ABC):
    __template_attributes__: ClassVar[set[str]] = set()
    __dependencies__: ClassVar[GrizzlyDependencies] = set()

    _context_root: str

    timeout: float | None
    grizzly: GrizzlyContext
    failure_handling: dict[type[Exception] | str | None, type[Exception] | None]

    def __init__(self, *, timeout: float | None = None) -> None:
        self._context_root = environ.get('GRIZZLY_CONTEXT_ROOT', '.')
        self.timeout = timeout
        self.failure_handling = {}

        from grizzly.context import grizzly  # noqa: PLC0415

        self.grizzly = grizzly

    @abstractmethod
    def __call__(self) -> grizzlytask:
        message = f'{self.__class__.__name__} has not been implemented'
        raise NotImplementedError(message)  # pragma: no cover

    def _get_template_value_str(self, value: str) -> str:
        try:
            possible_file = Path(self._context_root) / 'requests' / value
            if possible_file.is_file():
                value = possible_file.read_text()
        except OSError:
            pass

        return value

    def _get_value_templates(self, value: Any) -> set[str]:
        templates: set[str] = set()

        if isinstance(value, str):
            value = self._get_template_value_str(value)

            if has_template(value):
                templates.add(value)
        elif isinstance(value, list):
            for list_value in value:
                if isinstance(list_value, GrizzlyTask):
                    templates.update(list_value.get_templates())
                else:
                    templates.update(self._get_value_templates(list_value))
        elif isinstance(value, dict):
            for dict_value in value.values():
                if isinstance(dict_value, GrizzlyTask):
                    templates.update(dict_value.get_templates())
                else:
                    templates.update(self._get_value_templates(dict_value))
        elif isinstance(value, GrizzlyTask):
            templates.update(value.get_templates())

        return templates

    def get_templates(self) -> list[str]:
        templates: set[str] = set()
        for attribute in self.__template_attributes__:
            value = getattr(self, attribute, None)
            if value is None:
                continue

            templates.update(self._get_value_templates(value))

        return list(templates)


class GrizzlyMetaRequestTask(GrizzlyTask, metaclass=ABCMeta):
    content_type: TransformerContentType
    name: str | None
    endpoint: str

    def execute(self, _: GrizzlyScenario) -> GrizzlyResponse:
        message = f'{self.__class__.name} has not implemented "execute"'
        raise NotImplementedError(message)  # pragma: no cover

    def on_start(self, parent: GrizzlyScenario) -> None:
        pass

    def on_stop(self, parent: GrizzlyScenario) -> None:
        pass

    def on_iteration(self, parent: GrizzlyScenario) -> None:
        pass


class GrizzlyTaskWrapper(GrizzlyTask, metaclass=ABCMeta):
    name: str

    @abstractmethod
    def add(self, task: GrizzlyTask) -> None:
        message = f'{self.__class__.__name__} has not implemented add'
        raise NotImplementedError(message)  # pragma: no cover

    @abstractmethod
    def peek(self) -> list[GrizzlyTask]:
        message = f'{self.__class__.__name__} has not implemented peek'
        raise NotImplementedError(message)  # pragma: no cover


class template:
    attributes: list[str]

    def __init__(self, attribute: str, *additional_attributes: str) -> None:
        self.attributes = [attribute]
        if len(additional_attributes) > 0:
            self.attributes += list(additional_attributes)

    def __call__(self, cls: type[GrizzlyTask]) -> type[GrizzlyTask]:
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


from .async_timer import AsyncTimerTask  # noqa: I001
from .conditional import ConditionalTask
from .date import DateTask
from .keystore import KeystoreTask
from .log_message import LogMessageTask
from .loop import LoopTask
from .request import RequestTask, RequestTaskHandlers, RequestTaskResponse
from .set_variable import SetVariableTask
from .transformer import TransformerTask
from .until import UntilRequestTask
from .wait_between import WaitBetweenTask
from .wait_explicit import ExplicitWaitTask
from .write_file import WriteFileTask

from .async_group import AsyncRequestGroupTask

__all__ = [
    'AsyncRequestGroupTask',
    'AsyncTimerTask',
    'ConditionalTask',
    'DateTask',
    'ExplicitWaitTask',
    'KeystoreTask',
    'LogMessageTask',
    'LoopTask',
    'RequestTask',
    'RequestTaskHandlers',
    'RequestTaskResponse',
    'SetVariableTask',
    'TransformerTask',
    'UntilRequestTask',
    'WaitBetweenTask',
    'WriteFileTask',
]
