"""Grizzly types."""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from typing import Any, Concatenate, TypeVar, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from grizzly_common.text import PermutationEnum
from locust.rpc.protocol import Message
from typing_extensions import ParamSpec, Self

from grizzly.exceptions import RestartIteration, RestartScenario, RetryTask, StopUser
from grizzly.types.locust import Environment

try:
    import pymqi
except:
    from grizzly_common import dummy_pymqi as pymqi


P = ParamSpec('P')


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

    def get_value(self) -> str:
        return self.name.lower()


class FailureAction(PermutationEnum):
    __vector__ = (False, True)

    STOP_USER = (StopUser, 'stop user', True)
    RESTART_SCENARIO = (RestartScenario, 'restart scenario', True)
    RESTART_ITERATION = (RestartIteration, 'restart iteration', True)
    RETRY_TASK = (RetryTask, 'retry task', False)
    CONTINUE = (None, 'continue', True)

    step_expression: str
    exception: type[Exception] | None
    default_friendly: bool

    def __new__(cls, exception: type[Exception] | None, step_exression: str, default_friendly: bool) -> Self:  # noqa: FBT001
        obj = object.__new__(cls)
        obj._value_ = exception
        obj.step_expression = step_exression
        obj.exception = exception
        obj.default_friendly = default_friendly

        return obj

    @classmethod
    def from_string(cls, value: str) -> FailureAction:
        """Convert string value to enum value."""
        for enum in FailureAction:
            if enum.step_expression == value:
                return enum

        message = f'"{value}" is not a mapped step expression'
        raise AssertionError(message)

    @classmethod
    def get_failure_exceptions(cls) -> tuple[type[Exception], ...]:
        return tuple([action.exception for action in cls if action.exception is not None])

    @classmethod
    def from_step_expression(cls, value: str) -> FailureAction:
        return cls.from_string(value)

    @classmethod
    def from_exception(cls, value: type[Exception]) -> FailureAction:
        for enum in FailureAction:
            if enum.exception is value:
                return enum

        message = f'"{value}" is not a mapped exception'
        raise AssertionError(message)

    def get_value(self) -> str:
        return self.step_expression


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
    def methods(self) -> list[RequestMethod]:
        """All RequestMethods that has this request direction."""
        return [method for method in RequestMethod if method.direction == self]

    def get_value(self) -> str:
        return cast('str', self.value)


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
        return cast('RequestDirection', self.value.wrapped)

    def get_value(self) -> str:
        return self.name.lower()


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
    EMPTY = ('EMPTY', None)

    _value: str
    _weight: int | None

    def __new__(cls, value: str, weight: int | None = None) -> RequestType:  # noqa: PYI034
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
        method_name = cast('RequestType | None', getattr(cls, request_type.name, None))
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
        rt_attribute = cast('RequestType | None', getattr(cls, key, None))
        if rt_attribute is not None:
            return rt_attribute.alias

        if key in [e.alias for e in cls.__iter__()]:
            return key

        rm_attribute = cast('RequestMethod | None', getattr(RequestMethod, key, None))
        if rm_attribute is not None:
            return rm_attribute.name

        message = f'{key} does not exist'
        raise AssertionError(message)


StrDict = dict[str, Any]

GrizzlyResponse = tuple[StrDict | None, str | None]

TestdataType = dict[str, StrDict]

HandlerContextType = StrDict | Any

GrizzlyVariableType = str | float | int | bool

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


def list_type(value: str) -> list[str]:
    """Convert a string representation of a list to an actual list."""
    return [v.strip() for v in value.split(',')]


def int_rounded_float_type(value: str) -> int:
    """Convert a string representation of an integer to an actual integer."""
    return round(float(value))


def optional_str_lower_type(value: str | None) -> str | None:
    """Convert an optional string to lower case."""
    if value is None:
        return None

    return value.lower()
