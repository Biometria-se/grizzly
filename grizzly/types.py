from enum import Enum
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List, Type, Generic, Set, cast
from importlib import import_module

from aenum import Enum as AdvancedEnum, NoAlias
from locust.clients import ResponseContextManager
from locust.user.users import User
from gevent.lock import Semaphore

from grizzly_extras.transformer import TransformerContentType

class ResponseTarget(Enum):
    METADATA = 0
    PAYLOAD = 1


class ResponseAction(Enum):
    VALIDATE = 0
    SAVE = 1


class RequestDirection(Enum):
    FROM = 'from'
    TO = 'to'

    @classmethod
    def from_string(cls, value: str) -> 'RequestDirection':
        try:
            return RequestDirection[value.upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e

    @property
    def methods(self) -> List['RequestMethod']:
        methods: List[RequestMethod] = []

        for method in RequestMethod:
            if method.direction == self:
                methods.append(method)

        return methods


# Enum is needed for keeping mypy happy
class RequestMethod(Enum, AdvancedEnum, settings=NoAlias):
    SEND = RequestDirection.TO
    POST = RequestDirection.TO
    PUT = RequestDirection.TO
    RECEIVE = RequestDirection.FROM
    GET = RequestDirection.FROM

    @classmethod
    def from_string(cls, value: str) -> 'RequestMethod':
        try:
            return RequestMethod[value.upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e

    @property
    def direction(self) -> RequestDirection:
        return self.value


HandlerType = Callable[[Tuple[TransformerContentType, Any], User, Optional[ResponseContextManager]], None]

HandlerContextType = Union[ResponseContextManager, Tuple[Optional[Dict[str, Any]], str]]

TestdataType = Dict[str, Dict[str, Any]]

GrizzlyDictValueType = Union[str, float, int, bool]

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])

T = TypeVar('T')

U = TypeVar('U')


def bool_typed(value: str) -> bool:
    if value in ['True', 'False']:
        return value == 'True'

    raise ValueError(f'{value} is not a valid boolean')


def int_rounded_float_typed(value: str) -> int:
    return int(round(float(value)))


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


class GrizzlyDict(dict):
    @classmethod
    def load_variable(cls, name: str) -> Type[AtomicVariable]:
        if name not in globals():
            module = import_module('grizzly.testdata.variables')
            globals()[name] = getattr(module, name)

        variable = globals()[name]
        return cast(Type[AtomicVariable], variable)

    @classmethod
    def guess_datatype(cls, value: Any) -> GrizzlyDictValueType:
        if isinstance(value, (int, bool, float)):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: GrizzlyDictValueType

        if check_value[0] == '-':
            check_value = check_value[1:]

        if check_value.isdecimal():
            if float(value) % 1 == 0:
                if value.startswith('0'):
                    casted_value = str(value)
                else:
                    casted_value = int(float(value))
            else:
                casted_value = float(value)
        elif value.lower() in ['true', 'false']:
            casted_value = value.lower() == 'true'
        else:
            casted_value = str(value)
            if casted_value[0] in ['"', "'"]:
                if casted_value[0] != casted_value[-1] and casted_value.count(casted_value[0]) % 2 != 0:
                    raise ValueError(f'{value} is incorrectly quoted')

                if casted_value[0] == casted_value[-1]:
                    casted_value = casted_value[1:-1]
            elif casted_value[-1] in ['"', "'"] and casted_value[-1] != casted_value[0] and casted_value.count(casted_value[-1]) % 2 != 0:
                raise ValueError(f'{value} is incorrectly quoted')

        return casted_value

    def __setitem__(self, key: str, value: GrizzlyDictValueType) -> None:
        caster: Optional[Callable] = None

        if '.' in key:
            [name, _] = key.split('.', 1)
            try:
                variable = self.load_variable(name)
                caster = variable.__base_type__
            except AttributeError:
                pass

        if isinstance(value, str):
            if caster is None:
                value = self.guess_datatype(value)
            else:
                value = caster(value)
        elif caster is not None:
            value = caster(value)

        super().__setitem__(key, value)
