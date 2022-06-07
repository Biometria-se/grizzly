'''
@anchor pydoc:grizzly.testdata.variables Variables
This package contains special variables that can be used in a feature file and is synchronized between locust workers.

## Custom

It is possible to implement custom testdata variables, the only requirement is that they inherit `grizzly.testdata.variables.AtomicVariable`.
When initializing the variable, the full namespace has to be specified as `name` in the scenario {@pylink grizzly.steps.scenario.setup.step_setup_variable_value} step.

There are examples of this in the {@link framework.example}.
'''
from typing import Generic, Optional, Callable, Set, Any, Tuple, Dict, TypeVar

from gevent.lock import Semaphore

from ...context import GrizzlyContext


T = TypeVar('T')


class AbstractAtomicClass:
    pass


class AtomicVariable(Generic[T], AbstractAtomicClass):
    __base_type__: Optional[Callable] = None
    __dependencies__: Set[str] = set()
    __on_consumer__ = False

    __instance: Optional['AtomicVariable'] = None

    _initialized: bool
    _values: Dict[str, Optional[T]]
    _semaphore: Semaphore

    arguments: Dict[str, Any]
    grizzly: GrizzlyContext

    @classmethod
    def __new__(cls, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> 'AtomicVariable[T]':
        if AbstractAtomicClass in cls.__bases__:
            raise TypeError(f"Can't instantiate abstract class {cls.__name__}")

        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._semaphore = Semaphore()
            cls.__instance._initialized = False
            cls.__instance.grizzly = GrizzlyContext()
            globals()[cls.__name__] = cls  # load it, globally, needed for custom variables mostly

        return cls.__instance

    @classmethod
    def get(cls) -> 'AtomicVariable[T]':
        if cls.__instance is None:
            raise ValueError(f"'{cls.__name__}' is not instantiated")

        return cls.__instance

    @classmethod
    def destroy(cls) -> None:
        if cls.__instance is None:
            raise ValueError(f"'{cls.__name__}' is not instantiated")

        del cls.__instance

    @classmethod
    def clear(cls) -> None:
        if cls.__instance is None:
            raise ValueError(f"'{cls.__name__}' is not instantiated")

        variables = list(cls.__instance._values.keys())
        for variable in variables:
            del cls.__instance._values[variable]

    @classmethod
    def obtain(cls, variable: str, value: Optional[T] = None) -> 'AtomicVariable[T]':
        if cls.__instance is not None and variable in cls.__instance._values:
            return cls.__instance.get()

        return cls(variable, value)

    def __init__(self, variable: str, value: Optional[T] = None) -> None:
        with self._semaphore:
            if self._initialized:
                if variable not in self._values:
                    self._values[variable] = value
                else:
                    raise AttributeError(
                        f"'{self.__class__.__name__}' object already has attribute '{variable}'"
                    )

                return

            self._values = {variable: value}
            self._initialized = True

    def __getitem__(self, variable: str) -> Optional[T]:
        with self._semaphore:
            return self._get_value(variable)

    def __setitem__(self, variable: str, value: Optional[T]) -> None:
        with self._semaphore:
            if variable not in self._values:
                raise AttributeError(
                    f"'{self.__class__.__name__}' object has no attribute '{variable}'"
                )

            self._values[variable] = value

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._values[variable]
            except KeyError:
                pass

    def _get_value(self, variable: str) -> Optional[T]:
        try:
            return self._values[variable]
        except KeyError as e:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{variable}'"
            ) from e


def destroy_variables() -> None:
    for name in globals().keys():
        if not ('Atomic' in name and not name == 'AtomicVariable'):
            continue

        module = globals()[name]
        if issubclass(module, AtomicVariable):
            try:
                module.destroy()
            except ValueError:
                pass

    if 'AtomicVariable' in globals().keys():
        try:
            AtomicVariable.destroy()
        except ValueError:
            pass


from .random_integer import AtomicRandomInteger
from .integer_incrementer import AtomicIntegerIncrementer
from .date import AtomicDate
from .directory_contents import AtomicDirectoryContents
from .csv_row import AtomicCsvRow
from .random_string import AtomicRandomString
from .messagequeue import AtomicMessageQueue
from .servicebus import AtomicServiceBus

__all__ = [
    'AtomicRandomInteger',
    'AtomicIntegerIncrementer',
    'AtomicDate',
    'AtomicDirectoryContents',
    'AtomicCsvRow',
    'AtomicRandomString',
    'AtomicMessageQueue',
    'AtomicServiceBus',
    'destroy_variables',
]
