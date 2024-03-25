"""Grizzly types."""
from __future__ import annotations

from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, cast

from locust.rpc.protocol import Message
from typing_extensions import Concatenate, ParamSpec

P = ParamSpec('P')

from grizzly_extras.text import PermutationEnum

from .locust import Environment

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore[no-redef]  # pyright: ignore[reportMissingImports]

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


__all__ = [
    'Self',
    'ZoneInfo',
    'ZoneInfoNotFoundError',
    'pymqi',
]


class VariableType(Enum):
    VARIABLES = auto()
    CONTEXT = auto()


class MessageDirection(PermutationEnum):
    __vector__ = (True, True)

    CLIENT_SERVER = 0
    SERVER_CLIENT = 1

    @classmethod
    def from_string(cls, value: str) -> MessageDirection:
        """Convert string value to enum value."""
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            message = f'"{value.upper()}" is not a valid value of {cls.__name__}'
            raise AssertionError(message) from e


class ResponseTarget(PermutationEnum):
    __vector__ = (False, True)

    METADATA = 0
    PAYLOAD = 1

    @classmethod
    def from_string(cls, value: str) -> ResponseTarget:
        """Convert string value to enum value."""
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            message = f'"{value.upper()}" is not a valid value of {cls.__name__}'
            raise AssertionError(message) from e


class ResponseAction(Enum):
    VALIDATE = 0
    SAVE = 1


class ScenarioState(Enum):
    RUNNING = 0
    STOPPED = 1
    STOPPING = 2


class RequestDirection(PermutationEnum):
    __vector__ = (False, True)

    FROM = 'from'
    TO = 'to'

    @classmethod
    def from_string(cls, value: str) -> RequestDirection:
        """Convert string value to enum value."""
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            message = f'"{value.upper()}" is not a valid value of {cls.__name__}'
            raise AssertionError(message) from e

    @property
    def methods(self) -> List[RequestMethod]:
        """All RequestMethods that has this request direction."""
        return [method for method in RequestMethod if method.direction == self]


class RequestDirectionWrapper:
    wrapped: RequestDirection

    def __init__(self, /, wrapped: RequestDirection) -> None:
        self.wrapped = wrapped


class RequestMethod(PermutationEnum):
    __vector__ = (False, True)

    SEND = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    POST = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    PUT = RequestDirectionWrapper(wrapped=RequestDirection.TO)
    RECEIVE = RequestDirectionWrapper(wrapped=RequestDirection.FROM)
    GET = RequestDirectionWrapper(wrapped=RequestDirection.FROM)

    @classmethod
    def from_string(cls, value: str) -> RequestMethod:
        """Convert string value to enum value."""
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            message = f'"{value.upper()}" is not a valid value of {cls.__name__}'
            raise AssertionError(message) from e

    @property
    def direction(self) -> RequestDirection:
        """Request direction for this request method."""
        return cast(RequestDirection, self.value.wrapped)


class RequestType(Enum):
    AUTH = ('AUTH', 0)
    SCENARIO = ('SCEN', 1)
    TESTDATA = ('TSTD', 2)
    PACE = ('PACE', 3)
    UNTIL = ('UNTL', None)
    VARIABLE = ('VAR', None)
    ASYNC_GROUP = ('ASYNC', None)
    CLIENT_TASK = ('CLTSK', None)
    HELLO = ('HELO', None)
    RECEIVE = ('RECV', None)
    CONNECT = ('CONN', None)
    DISCONNECT = ('DISC', None)
    SUBSCRIBE = ('SUB', None)
    UNSUBSCRIBE = ('UNSUB', None)

    _value: str
    _weight: Optional[int]

    def __new__(cls, value: str, weight: Optional[int] = None) -> RequestType:  # noqa: PYI034
        """Create a multi-value enum value."""
        obj = object.__new__(cls)
        obj._value = value
        obj._weight = weight

        return obj

    def __call__(self) -> str:
        """Format enum name as a string."""
        return str(self)

    def __str__(self) -> str:
        """Format enum as value."""
        return self.alias

    @property
    def weight(self) -> int:
        """Get enum value weight."""
        return self._weight if self._weight is not None else 10

    @property
    def alias(self) -> str:
        """Value is an alias of name."""
        return self._value

    @classmethod
    def get_method_weight(cls, method: str) -> int:
        """Get a methods weight based on string representation."""
        try:
            request_type = cls.from_alias(method)
            weight = request_type.weight
        except AssertionError:
            weight = 10

        return weight

    @classmethod
    def from_method(cls, request_type: RequestMethod) -> str:
        """Convert a request method to a request type."""
        method_name = cast(Optional[RequestType], getattr(cls, request_type.name, None))
        if method_name is not None:
            return method_name.alias

        return request_type.name

    @classmethod
    def from_alias(cls, alias: str) -> RequestType:
        """Convert alias to request type."""
        for request_type in cls.__iter__():
            if request_type.alias == alias:
                return request_type

        message = f'no request type with alias {alias}'
        raise AssertionError(message)

    @classmethod
    def from_string(cls, key: str) -> str:
        """Convert string value (can be either alias or name) to request type."""
        rt_attribute = cast(Optional[RequestType], getattr(cls, key, None))
        if rt_attribute is not None:
            return rt_attribute.alias

        if key in [e.alias for e in cls.__iter__()]:
            return key

        rm_attribute = cast(Optional[RequestMethod], getattr(RequestMethod, key, None))
        if rm_attribute is not None:
            return rm_attribute.name

        message = f'{key} does not exist'
        raise AssertionError(message)


GrizzlyResponse = Tuple[Optional[Dict[str, Any]], Optional[str]]

TestdataType = Dict[str, Dict[str, Any]]

HandlerContextType = Union[Dict[str, Any], Optional[Any]]

GrizzlyVariableType = Union[str, float, int, bool]

MessageCallback = Callable[Concatenate[Environment, Message, P], None]

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])

T = TypeVar('T')

U = TypeVar('U')


def bool_type(value: str) -> bool:
    """Convert a string to a boolean."""
    if value in ['True', 'False']:
        return value == 'True'

    message = f'{value} is not a valid boolean'
    raise ValueError(message)


def list_type(value: str) -> List[str]:
    """Convert a string representation of a list to an actual list."""
    return [v.strip() for v in value.split(',')]


def int_rounded_float_type(value: str) -> int:
    """Convert a string representation of an integer to an actual integer."""
    return int(round(float(value)))


def optional_str_lower_type(value: Optional[str]) -> Optional[str]:
    """Convert an optional string to lower case."""
    if value is None:
        return None

    return value.lower()
