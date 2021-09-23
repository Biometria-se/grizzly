from enum import Enum, auto
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List

from aenum import Enum as AdvancedEnum, NoAlias
from locust.clients import ResponseContextManager
from locust.user.users import User


class ResponseContentType(Enum):
    GUESS = 0
    JSON = auto()
    XML = auto()
    PLAIN = auto()


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


HandlerType = Callable[[Tuple[ResponseContentType, Any], User, Optional[ResponseContextManager]], None]

HandlerContextType = Union[ResponseContextManager, Tuple[Optional[Dict[str, Any]], str]]

TestdataType = Dict[str, Dict[str, Any]]

TemplateDataType = Union[str, float, int, bool, str]

WrappedFunc = TypeVar('WrappedFunc', bound=Callable[..., Any])


def bool_typed(value: str) -> bool:
    if value in ['True', 'False']:
        return value == 'True'

    raise ValueError(f'{value} is not a valid boolean')


def int_rounded_float_typed(value: str) -> int:
    return int(round(float(value)))
