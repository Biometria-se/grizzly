from enum import Enum
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List, cast
from mypy_extensions import KwArg, Arg

from locust.clients import ResponseContextManager as RequestsResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from locust.rpc.protocol import Message
from grizzly_extras.text import PermutationEnum

from .locust import Environment


class MessageDirection(PermutationEnum):
    __vector__ = (True, True,)

    CLIENT_SERVER = 0
    SERVER_CLIENT = 1

    @classmethod
    def from_string(cls, value: str) -> 'MessageDirection':
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e


class ResponseTarget(PermutationEnum):
    __vector__ = (False, True,)

    METADATA = 0
    PAYLOAD = 1

    @classmethod
    def from_string(cls, value: str) -> 'ResponseTarget':
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e


class ResponseAction(Enum):
    VALIDATE = 0
    SAVE = 1


class ScenarioState(Enum):
    RUNNING = 0
    STOPPED = 1
    STOPPING = 2


class RequestDirection(PermutationEnum):
    __vector__ = (False, True,)

    FROM = 'from'
    TO = 'to'

    @classmethod
    def from_string(cls, value: str) -> 'RequestDirection':
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e

    @property
    def methods(self) -> List['RequestMethod']:
        methods: List[RequestMethod] = []

        for method in RequestMethod:
            if method.direction == self:
                methods.append(method)

        return methods


class RequestDirectionWrapper:
    wrapped: RequestDirection

    def __init__(self, /, wrapped: RequestDirection) -> None:
        self.wrapped = wrapped


class RequestMethod(PermutationEnum):
    __vector__ = (False, True,)

    SEND = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    POST = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    PUT = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    RECEIVE = RequestDirectionWrapper(wrapped=RequestDirection.FROM)
    GET = RequestDirectionWrapper(wrapped=RequestDirection.FROM)

    @classmethod
    def from_string(cls, value: str) -> 'RequestMethod':
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e

    @property
    def direction(self) -> RequestDirection:
        return cast(RequestDirection, self.value.wrapped)


class RequestType(Enum):
    AUTH = ('AUTH', 0,)
    SCENARIO = ('SCEN', 1,)
    TESTDATA = ('TSTD', 2,)
    PACE = ('PACE', 3,)
    UNTIL = ('UNTL', None,)
    VARIABLE = ('VAR', None,)
    ASYNC_GROUP = ('ASYNC', None,)
    CLIENT_TASK = ('CLTSK', None,)
    HELLO = ('HELO', None,)
    RECEIVE = ('RECV', None,)
    CONNECT = ('CONN', None,)
    DISCONNECT = ('DISC', None,)
    SUBSCRIBE = ('SUB', None,)
    UNSUBSCRIBE = ('UNSUB', None,)

    _value: str
    _weight: Optional[int]

    def __new__(cls, value: str, weight: Optional[int] = None) -> 'RequestType':
        obj = object.__new__(cls)
        obj._value = value
        obj._weight = weight

        return obj

    def __call__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.alias

    @property
    def weight(self) -> int:
        return self._weight if self._weight is not None else 10

    @property
    def alias(self) -> str:
        return self._value

    @classmethod
    def get_method_weight(cls, method: str) -> int:
        try:
            request_type = cls.from_alias(method)
            weight = request_type.weight
        except AttributeError:
            weight = 10

        return weight

    @classmethod
    def from_method(cls, request_type: RequestMethod) -> str:
        method_name = cast(Optional[RequestType], getattr(cls, request_type.name, None))
        if method_name is not None:
            return method_name.alias

        return request_type.name

    @classmethod
    def from_alias(cls, alias: str) -> 'RequestType':
        for request_type in cls.__iter__():
            if request_type.alias == alias:
                return request_type

        raise AttributeError(f'no request type with alias {alias}')

    @classmethod
    def from_string(cls, key: str) -> str:
        rt_attribute = cast(Optional[RequestType], getattr(cls, key, None))
        if rt_attribute is not None:
            return rt_attribute.alias

        if key in [e.alias for e in cls.__iter__()]:
            return key

        rm_attribute = cast(Optional[RequestMethod], getattr(RequestMethod, key, None))
        if rm_attribute is not None:
            return rm_attribute.name

        raise AttributeError(f'{key} does not exist')


GrizzlyResponseContextManager = Union[RequestsResponseContextManager, FastResponseContextManager]

GrizzlyResponse = Tuple[Optional[Dict[str, Any]], Optional[str]]

HandlerContextType = Union[GrizzlyResponseContextManager, GrizzlyResponse]

TestdataType = Dict[str, Dict[str, Any]]

GrizzlyVariableType = Union[str, float, int, bool]

MessageCallback = Callable[[Arg(Environment, 'environment'), Arg(Message, 'msg'), KwArg(Dict[str, Any])], None]  # noqa: F821

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])

T = TypeVar('T')

U = TypeVar('U')


def bool_type(value: str) -> bool:
    if value in ['True', 'False']:
        return value == 'True'

    raise ValueError(f'{value} is not a valid boolean')


def list_type(value: str) -> List[str]:
    return [v.strip() for v in value.split(',')]


def int_rounded_float_type(value: str) -> int:
    return int(round(float(value)))


def optional_str_lower_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    return value.lower()
