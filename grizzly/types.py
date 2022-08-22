from enum import Enum, EnumMeta
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List, cast
from mypy_extensions import KwArg, Arg

from aenum import Enum as AdvancedEnum, NoAlias, EnumType as AdvancedEnumType

from locust.clients import ResponseContextManager as RequestsResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from locust.env import Environment
from locust.rpc.protocol import Message
from locust.runners import WorkerRunner, MasterRunner, LocalRunner
from grizzly_extras.text import PermutationEnum

__all__ = [
    'Message',
    'Environment',
    'WorkerRunner',
    'MasterRunner',
    'LocalRunner',
]


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


# Enum is needed for keeping mypy happy
class MixedEnumMeta(AdvancedEnumType, EnumMeta):
    pass


class RequestMethod(Enum, AdvancedEnum, metaclass=MixedEnumMeta, settings=NoAlias):
    SEND = RequestDirection.TO
    POST = RequestDirection.TO
    PUT = RequestDirection.TO
    RECEIVE = RequestDirection.FROM
    GET = RequestDirection.FROM

    @classmethod
    def get_vector(cls) -> Tuple[bool, bool]:
        """
        aenum.Enum has a definition of __getattr__ that makes it "impossible" to implement vector the same
        was as for the enums that only inherits enum.Enum.
        """
        return (False, True,)

    @classmethod
    def from_string(cls, value: str) -> 'RequestMethod':
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            raise ValueError(f'"{value.upper()}" is not a valid value of {cls.__name__}') from e

    @property
    def direction(self) -> RequestDirection:
        return self.value


class RequestType(Enum, AdvancedEnum, metaclass=MixedEnumMeta, init='alias _weight'):
    SCENARIO = ('SCEN', 0,)
    TESTDATA = ('TSTD', 1,)
    UNTIL = ('UNTL', None,)
    VARIABLE = ('VAR', None,)
    ASYNC_GROUP = ('ASYNC', None,)
    CLIENT_TASK = ('CLTSK', None,)
    HELLO = ('HELO', None,)
    RECEIVE = ('RECV', None,)
    CONNECT = ('CONN', None,)

    def __call__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return cast(str, getattr(self, 'alias'))

    @property
    def weight(self) -> int:
        weight = getattr(self, '_weight', None)
        return cast(int, weight) if weight is not None else 10

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
            return cast(str, method_name.alias)

        return request_type.name

    @classmethod
    def from_alias(cls, alias: str) -> 'RequestType':
        for request_type in cls:
            if request_type.alias == alias:
                return request_type

        raise AttributeError(f'no request type with alias {alias}')

    @classmethod
    def from_string(cls, key: str) -> str:
        attribute = cast(Optional[RequestType], getattr(cls, key, None))
        if attribute is not None:
            return cast(str, attribute.alias)

        if key in [e.alias for e in cls]:
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


def bool_type(value: str) -> bool:
    if value in ['True', 'False']:
        return value == 'True'

    raise ValueError(f'{value} is not a valid boolean')


def int_rounded_float_type(value: str) -> int:
    return int(round(float(value)))


def optional_str_lower_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    return value.lower()
