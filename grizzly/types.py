from enum import Enum, auto
from typing import Callable, Optional, Tuple, Any, Union, Dict, TypeVar, List

from aenum import Enum as AdvancedEnum, NoAlias
from locust.clients import ResponseContextManager
from locust.user.users import User


class ResponseTarget(Enum):
    METADATA = 0
    PAYLOAD = 1


class ResponseAction(Enum):
    VALIDATE = 0
    SAVE = 1


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

class TemplateData(dict):
    @classmethod
    def guess_datatype(cls, value: Any) -> 'TemplateDataType':
        if isinstance(value, (int, bool, float)):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: 'TemplateDataType'

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

    def __setitem__(self, key: str, value: 'TemplateDataType') -> None:
        caster: Optional[Callable] = None

        if '.' in key:
            [name, _] = key.split('.', 1)
            try:
                from .testdata.variables import load_variable
                variable = load_variable(name)
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


def str_response_content_type(value: str) -> ResponseContentType:
    if value.strip() in ['application/json', 'json']:
        return ResponseContentType.JSON
    elif value.strip() in ['application/xml', 'xml']:
        return ResponseContentType.XML
    elif value.strip() in ['text/plain', 'plain']:
        return ResponseContentType.PLAIN
    else:
        raise ValueError(f'"{value}" is an unknown response content type')
