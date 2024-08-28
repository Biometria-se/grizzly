"""@anchor pydoc:grizzly.testdata.variables Variables
This package contains special variables that can be used in a feature file and is synchronized between locust workers.

## Custom

It is possible to implement custom testdata variables, the only requirement is that they inherit `grizzly.testdata.variables.AtomicVariable`.
When initializing the variable, the full namespace has to be specified as `name` in the scenario {@pylink grizzly.steps.setup.step_setup_variable_value} step.

There are examples of this in the {@link framework.example}.
"""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, Optional, TypeVar

from gevent.lock import DummySemaphore, Semaphore

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.types import bool_type

T = TypeVar('T')


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.locust import MessageHandler


class AbstractAtomicClass:
    pass


class AtomicVariablePersist(metaclass=ABCMeta):
    arguments: ClassVar[dict[str, Any]] = {'persist': bool_type}

    @abstractmethod
    def generate_initial_value(self, variable: str) -> str:
        ...


class AtomicVariableSettable(metaclass=ABCMeta):
    __settable__ = True

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:
        ...


class AtomicVariable(Generic[T], AbstractAtomicClass):
    __base_type__: Optional[Callable] = None
    __dependencies__: ClassVar[set[str]] = set()
    __message_handlers__: ClassVar[dict[str, MessageHandler]] = {}
    __on_consumer__ = False

    _instances: ClassVar[dict[type[AtomicVariable], dict[GrizzlyContextScenario, AtomicVariable]]] = {}

    _initialized: bool
    _scenario: GrizzlyContextScenario
    _values: dict[str, Optional[T]]
    _semaphore: Semaphore = Semaphore()

    arguments: ClassVar[dict[str, Any]] = {}
    grizzly: GrizzlyContext

    def __new__(cls, *, scenario: GrizzlyContextScenario, variable: str, value: Optional[T] = None, outer_lock: bool = False) -> AtomicVariable[T]:  # noqa: ARG003
        if AbstractAtomicClass in cls.__bases__:
            message = f"Can't instantiate abstract class {cls.__name__}"
            raise TypeError(message)

        if cls not in cls._instances:
            cls._instances.update({cls: {}})

        if cls._instances.get(cls, {}).get(scenario, None) is None:
            instance = super().__new__(cls)
            instance._initialized = False
            instance.grizzly = GrizzlyContext()
            instance._scenario = scenario

            cls._instances[cls].update({scenario: instance})

        if cls.__name__ not in globals():
            globals()[cls.__name__] = cls  # load it, globally, needed for custom variables mostly

        return cls._instances[cls][scenario]

    @classmethod
    def get(cls, scenario: GrizzlyContextScenario) -> AtomicVariable[T]:
        if cls._instances.get(cls, {}).get(scenario, None) is None:
            message = f'{cls.__name__} is not instantiated for {scenario.name}'
            raise ValueError(message)

        return cls._instances[cls][scenario]

    @classmethod
    def destroy(cls) -> None:
        instances = cls._instances.get(cls, None)

        if instances is None or len(instances) < 1:
            message = f'{cls.__name__} is not instantiated'
            raise ValueError(message)

        for scenario in instances.copy():
            del instances[scenario]

    @classmethod
    def clear(cls) -> None:
        def _clear(_scenario: GrizzlyContextScenario) -> None:
            instance = cls._instances.get(cls, {}).get(_scenario, None)

            if instance is None:
                message = f'{cls.__name__} is not instantiated for {_scenario.name}'
                raise ValueError(message)

            variables = list(instance._values.keys())
            for variable in variables:
                del instance._values[variable]

        instances = cls._instances.get(cls, None)
        if instances is None or len(instances) < 1:
            message = f'{cls.__name__} is not instantiated'
            raise ValueError(message)

        for scenario in instances:
            _clear(scenario)

    @classmethod
    def obtain(cls, *, scenario: GrizzlyContextScenario, variable: str, value: Optional[T] = None) -> AtomicVariable[T]:
        with cls.semaphore():
            instance: Optional[AtomicVariable[T]] = cls._instances.get(cls, {}).get(scenario, None)
            if instance is not None and variable in instance._values:
                return instance

            return cls(scenario=scenario, variable=variable, value=value, outer_lock=True)

    def __init__(self, *, scenario: GrizzlyContextScenario, variable: str, value: Optional[T] = None, outer_lock: bool = False) -> None:  # noqa: ARG002
        with self.semaphore(outer=outer_lock):
            if self._initialized:
                if variable not in self._values:
                    self._values[variable] = value
                else:
                    message = f'{self.__class__.__name__} object already has attribute "{variable}"'
                    raise AttributeError(message)

                return

            self._values = {variable: value}
            self._initialized = True

    @classmethod
    def semaphore(cls, *, outer: bool = False) -> Semaphore:
        return cls._semaphore if not outer else DummySemaphore()

    def __getitem__(self, variable: str) -> Optional[T]:
        with self.semaphore():
            return self._get_value(variable)

    def __setitem__(self, variable: str, value: Optional[T]) -> None:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented "__setitem__"'
        raise NotImplementedError(message)

    def __delitem__(self, variable: str) -> None:
        with suppress(KeyError):
            del self._values[variable]

    def _get_value(self, variable: str) -> Optional[T]:
        try:
            return self._values[variable]
        except KeyError as e:
            message = f'{self.__class__.__name__} object has no attribute "{variable}"'
            raise AttributeError(message) from e


def destroy_variables() -> None:
    """Destroy all active Atomic variable singleton instances."""
    for name in globals():
        if not ('Atomic' in name and name != 'AtomicVariable'):
            continue

        module = globals()[name]
        if issubclass(module, AtomicVariable):
            with suppress(ValueError):
                module.destroy()

    if 'AtomicVariable' in globals():
        with suppress(ValueError):
            AtomicVariable.destroy()


from .csv_reader import AtomicCsvReader
from .csv_writer import AtomicCsvWriter
from .date import AtomicDate
from .directory_contents import AtomicDirectoryContents
from .integer_incrementer import AtomicIntegerIncrementer
from .json_reader import AtomicJsonReader
from .random_integer import AtomicRandomInteger
from .random_string import AtomicRandomString

__all__ = [
    'AtomicRandomInteger',
    'AtomicIntegerIncrementer',
    'AtomicDate',
    'AtomicDirectoryContents',
    'AtomicCsvReader',
    'AtomicCsvWriter',
    'AtomicJsonReader',
    'AtomicRandomString',
    'destroy_variables',
]
