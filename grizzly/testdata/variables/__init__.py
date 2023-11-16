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
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Generic, Optional, Set, TypeVar

from gevent.lock import DummySemaphore, Semaphore

from grizzly.context import GrizzlyContext
from grizzly.types import bool_type

T = TypeVar('T')


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.locust import MessageHandler


class AbstractAtomicClass:
    pass


class AtomicVariablePersist(metaclass=ABCMeta):
    arguments: ClassVar[Dict[str, Any]] = {'persist': bool_type}

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
    __dependencies__: ClassVar[Set[str]] = set()
    __message_handlers__: ClassVar[Dict[str, MessageHandler]] = {}
    __on_consumer__ = False

    __instance: ClassVar[Optional[AtomicVariable]] = None

    _initialized: bool
    _values: Dict[str, Optional[T]]
    _semaphore: Semaphore = Semaphore()

    arguments: ClassVar[Dict[str, Any]] = {}
    grizzly: GrizzlyContext

    def __new__(cls, *_args: Any, **_kwargs: Any) -> AtomicVariable[T]:
        if AbstractAtomicClass in cls.__bases__:
            message = f"Can't instantiate abstract class {cls.__name__}"
            raise TypeError(message)

        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialized = False
            cls.__instance.grizzly = GrizzlyContext()
            globals()[cls.__name__] = cls  # load it, globally, needed for custom variables mostly

        return cls.__instance

    @classmethod
    def get(cls) -> AtomicVariable[T]:
        if cls.__instance is None:
            message = f'{cls.__name__} is not instantiated'
            raise ValueError(message)

        return cls.__instance

    @classmethod
    def destroy(cls) -> None:
        if cls.__instance is None:
            message = f'{cls.__name__} is not instantiated'
            raise ValueError(message)

        del cls.__instance

    @classmethod
    def clear(cls) -> None:
        if cls.__instance is None:
            message = f'{cls.__name__} is not instantiated'
            raise ValueError(message)

        variables = list(cls.__instance._values.keys())
        for variable in variables:
            del cls.__instance._values[variable]

    @classmethod
    def obtain(cls, variable: str, value: Optional[T] = None) -> AtomicVariable[T]:
        with cls.semaphore():
            if cls.__instance is not None and variable in cls.__instance._values:
                return cls.__instance.get()

            return cls(variable, value, outer_lock=True)

    def __init__(self, variable: str, value: Optional[T] = None, *, outer_lock: bool = False) -> None:
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
from .random_integer import AtomicRandomInteger
from .random_string import AtomicRandomString

__all__ = [
    'AtomicRandomInteger',
    'AtomicIntegerIncrementer',
    'AtomicDate',
    'AtomicDirectoryContents',
    'AtomicCsvReader',
    'AtomicCsvWriter',
    'AtomicRandomString',
    'destroy_variables',
]
