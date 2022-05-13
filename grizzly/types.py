from enum import Enum
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List, cast
from mypy_extensions import KwArg, Arg

from aenum import Enum as AdvancedEnum, NoAlias

from locust.clients import ResponseContextManager as RequestsResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from locust.env import Environment
from locust.rpc.protocol import Message
from locust.runners import WorkerRunner, MasterRunner, LocalRunner

__all__ = [
    'Message',
    'Environment',
    'WorkerRunner',
    'MasterRunner',
    'LocalRunner',
]


class MessageDirection(Enum):
    CLIENT_SERVER = 0
    SERVER_CLIENT = 1

    @classmethod
    def from_string(cls, value: str) -> 'MessageDirection':
        try:
            return cls[value.upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e


class ResponseTarget(Enum):
    METADATA = 0
    PAYLOAD = 1


class ResponseAction(Enum):
    VALIDATE = 0
    SAVE = 1


class ScenarioState(Enum):
    RUNNING = 0
    STOPPED = 1
    STOPPING = 2


class RequestDirection(Enum):
    FROM = 'from'
    TO = 'to'

    @classmethod
    def from_string(cls, value: str) -> 'RequestDirection':
        try:
            return cls[value.upper()]
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


class RequestType(Enum, AdvancedEnum, settings=NoAlias):
    SCENARIO = 'SCEN'
    TESTDATA = 'TSTD'
    UNTIL = 'UNTL'
    VARIABLE = 'VAR'
    ASYNC_GROUP = 'ASYNC'
    CLIENT_TASK = 'CLTSK'
    HELLO = 'HELO'
    RECEIVE = 'RECV'
    CONNECT = 'CONN'

    def __call__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_method(cls, request_type: RequestMethod) -> str:
        method_name = cast(Optional[RequestType], getattr(cls, request_type.name, None))
        if method_name is not None:
            return method_name.value

        return request_type.name

    @classmethod
    def from_string(cls, key: str) -> str:
        attribute = cast(Optional[RequestType], getattr(cls, key, None))
        if attribute is not None:
            return attribute.value

        if key in [e.value for e in cls]:
            return key

        attribute = cast(Optional[RequestMethod], getattr(RequestMethod, key, None))
        if attribute is not None:
            return attribute.name

        raise AttributeError(f'{key} does not exist')


GrizzlyResponseContextManager = Union[RequestsResponseContextManager, FastResponseContextManager]

GrizzlyResponse = Tuple[Optional[Dict[str, Any]], Optional[Any]]

HandlerContextType = Union[GrizzlyResponseContextManager, GrizzlyResponse]

TestdataType = Dict[str, Dict[str, Any]]

GrizzlyVariableType = Union[str, float, int, bool]

MessageCallback = Callable[[Arg(Environment, 'environment'), Arg(Message, 'msg'), KwArg(Dict[str, Any])], None]  # noqa: F821

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])

T = TypeVar('T')

U = TypeVar('U')


def bool_typed(value: str) -> bool:
    if value in ['True', 'False']:
        return value == 'True'

    raise ValueError(f'{value} is not a valid boolean')


def int_rounded_float_typed(value: str) -> int:
    return int(round(float(value)))
