'''This package contains special variables that can be used in a feature file and is synchronized between locust workers.'''
from typing import Any, Dict, Optional, TypeVar, Generic, Tuple, Callable, Type, cast
from importlib import import_module

from gevent.lock import Semaphore


T = TypeVar('T')
U = TypeVar('U')


class AbstractAtomicClass:
    pass


class AtomicVariable(Generic[T], AbstractAtomicClass):
    __base_type__: Optional[Callable] = None
    __instance: Optional['AtomicVariable'] = None

    _initialized: bool
    _values: Dict[str, Optional[T]]
    _semaphore: Semaphore

    @classmethod
    def __new__(cls, *_args: Tuple[Any, ...], **_kwargs: Dict[str, Any]) -> 'AtomicVariable':
        if AbstractAtomicClass in cls.__bases__:
            raise TypeError(f"Can't instantiate abstract class {cls.__name__}")

        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._semaphore = Semaphore()
            cls.__instance._initialized = False

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

    def __init__(self, variable: str, value: Optional[T] = None) -> None:
        with self._semaphore:
            if self._initialized:
                if variable not in self._values:
                    self._values[variable] = value
                else:
                    raise ValueError(
                        f"'{self.__class__.__name__}' object already has attribute '{variable}'"
                    )

                return

            self._semaphore = self._semaphore  # ugly hack to fool mypy?
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


def parse_arguments(variable_type: Type[AtomicVariable], arguments: str) -> Dict[str, Any]:
    if '=' not in arguments or (arguments.count('=') > 1 and ', ' not in arguments):
        raise ValueError(f'{variable_type.__name__}: incorrect format in arguments: "{arguments}"')

    parsed: Dict[str, Any] = {}

    for argument in arguments.split(','):
        argument = argument.strip()

        if len(argument) < 1:
            raise ValueError(f'{variable_type.__name__}: incorrect format for arguments: "{arguments}"')

        if '=' not in argument:
            raise ValueError(f'{variable_type.__name__}: incorrect format for argument: "{argument}"')

        [key, value] = argument.split('=')

        key = key.strip()
        if '"' in key or "'" in key or ' ' in key:
            raise ValueError(f'{variable_type.__name__}: no quotes or spaces allowed in argument names')

        value = value.strip()

        start_quote: Optional[str] = None

        if value[0] in ['"', "'"]:
            if value[-1] != value[0]:
                raise ValueError(f'{variable_type.__name__}: value is incorrectly quoted: "{value}"')
            start_quote = value[0]
            value = value[1:]

        if value[-1] in ['"', "'"]:
            if start_quote is None:
                raise ValueError(f'{variable_type.__name__}: value is incorrectly quoted: "{value}"')
            value = value[:-1]

        if start_quote is None and ' ' in value:
            raise ValueError(f'{variable_type.__name__}: value needs to be quoted: "{value}"')

        parsed[key] = value

    return parsed


def load_variable(name: str) -> Type[AtomicVariable]:
    if name not in globals():
        module = import_module(__name__)
        globals()[name] = getattr(module, name)

    variable = globals()[name]
    return cast(Type[AtomicVariable], variable)


def destroy_variables() -> None:
    for name in globals().keys():
        if not (name.startswith('Atomic') and not name == 'AtomicVariable'):
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


from .integer import AtomicInteger
from .random_integer import AtomicRandomInteger, atomicrandominteger__base_type__
from .integer_incrementer import AtomicIntegerIncrementer, atomicintegerincrementer__base_type__
from .date import AtomicDate, atomicdate__base_type__
from .directory_contents import AtomicDirectoryContents, atomicdirectorycontents__base_type__
from .csv_row import AtomicCsvRow, atomiccsvrow__base_type__
from .random_string import AtomicRandomString, atomicrandomstring__base_type__
