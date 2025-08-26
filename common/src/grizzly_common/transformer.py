"""Module contains the means to transform a raw string to format that is possible to easy search
for attributes and their value in.
"""

from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from functools import wraps
from json import JSONEncoder
from json import dumps as jsondumps
from json import loads as jsonloads
from typing import TYPE_CHECKING, Any, ClassVar, cast

from jsonpath_ng.ext import parse as jsonpath_parse
from lxml import etree as XML  # noqa: N812

from grizzly_common.text import PermutationEnum, caster

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable


class TransformerError(Exception):
    message: str | None = None

    def __init__(self, message: str | None = None) -> None:
        self.message = message


class TransformerContentType(PermutationEnum):
    __vector__ = (False, True)

    UNDEFINED = None
    JSON = 'application/json'
    XML = 'application/xml'
    PLAIN = 'text/plain'
    MULTIPART_FORM_DATA = 'multipart/form-data'
    OCTET_STREAM_UTF8 = 'application/octet-stream; charset=utf-8'

    @classmethod
    def from_string(cls, value: str) -> TransformerContentType:
        value = value.lower()

        for enum in cls:
            if enum.name.lower() == value or enum.value == value:
                return enum

        message = f'"{value}" is an unknown response content type'
        raise ValueError(message)

    def get_value(self) -> str:
        return self.name.lower()


class Transformer(metaclass=ABCMeta):
    __wrapped_transform__: ClassVar[Callable[[str], Any]]

    EMPTY: str = ''

    @classmethod
    @abstractmethod
    def transform(cls, raw: str) -> Any:  # pragma: no cover
        message = f'{cls.__name__} has not implemented transform'
        raise NotImplementedError(message)

    @classmethod
    @abstractmethod
    def validate(cls, expression: str) -> bool:  # pragma: no cover
        message = f'{cls.__name__} has not implemented validate'
        raise NotImplementedError(message)  # pragma: no cover

    @classmethod
    @abstractmethod
    def parser(cls, expression: str) -> Callable[[Any], list[str]]:  # pragma: no cover
        message = f'{cls.__name__} has not implemented parse'
        raise NotImplementedError(message)


class transformer:
    content_type: TransformerContentType
    available: ClassVar[dict[TransformerContentType, type[Transformer]]] = {}

    def __init__(self, content_type: TransformerContentType) -> None:
        if content_type == TransformerContentType.UNDEFINED:
            message = 'it is not allowed to register a transformer of type UNDEFINED'
            raise ValueError(message)

        self.content_type = content_type

    def __call__(self, impl: type[Transformer]) -> type[Transformer]:
        impl_transform = impl.transform
        content_type_name = self.content_type.name

        @wraps(impl.transform)
        def wrapped_transform(raw: str) -> Any:
            try:
                return impl_transform(raw)
            except Exception as e:
                message = f'failed to transform input as {content_type_name}: {e!s}'
                raise TransformerError(message) from e

        impl.__wrapped_transform__ = impl_transform
        setattr(impl, 'transform', wrapped_transform)  # noqa: B010

        if self.content_type not in transformer.available:
            self.available.update({self.content_type: impl})

        return impl


@transformer(TransformerContentType.JSON)
class JsonTransformer(Transformer):
    EMPTY = 'null'

    @classmethod
    def transform(cls, raw: str) -> Any:
        return jsonloads(raw)

    @classmethod
    def validate(cls, expression: str) -> bool:
        valid = expression.startswith('$.') and len(expression) > 2
        if not valid:
            return valid

        try:
            jsonpath_parse(expression)
        except Exception:
            valid = False

        return valid

    @classmethod
    def _op_eq(cls, actual: str, expected: str) -> bool:
        _actual = caster(actual)
        _expected = caster(expected)

        return cast('bool', _actual == _expected)

    @classmethod
    def _op_in(cls, actual: str, expected: list[str]) -> bool:
        return actual in expected

    @classmethod
    def _op_ge(cls, actual: str, expected: str) -> bool:
        _actual = caster(actual)
        _expected = caster(expected)

        return cast('bool', _actual >= _expected)

    @classmethod
    def _op_le(cls, actual: str, expected: str) -> bool:
        _actual = caster(actual)
        _expected = caster(expected)

        return cast('bool', _actual >= _expected)

    @classmethod
    def _get_outer_op(cls, expression: str, op: str) -> tuple[str, str] | None:
        """Find `op` which is not inside of a "group" ([]).
        `rindex` raises `ValueError` if `op` is not found in (sub)string.
        """
        if op not in expression:
            return None

        # check if expression contains any (complete) groups (`[..]`)
        if not ('[' in expression and ']' in expression):
            jsonpath_expression, expected_value = expression.rsplit(op, 1)
            return jsonpath_expression, expected_value

        op_length = len(op)

        try:  # check left side
            op_index = expression.rindex(op)

            start_group = expression[:op_index].rindex('[')
            end_group = expression[:op_index].rindex(']')

            if end_group < start_group:  # check right side
                raise ValueError

            # op was found inline between [ and ]
            if start_group < op_index < end_group:
                return None

            jsonpath_expression = expression[:op_index]
            expected_value = expression[op_index + op_length :]
        except ValueError:  # check right side
            try:
                end_group = expression[op_index:].index(']') + op_index
                start_group = expression[op_index:].index('[') + op_index
                if start_group < op_index < end_group:
                    raise ValueError

            except ValueError:
                return None

            jsonpath_expression, expected_value = expression.rsplit(op, 1)
            jsonpath_expression = expression[:op_index]
            expected_value = expression[op_index + op_length :]

        return jsonpath_expression, expected_value

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], list[str]]:
        try:
            expected: str | list[str] | None = None
            assertion: Callable[[Any, Any], bool] | None = None

            operators: list[tuple[str, Callable[[Any, Any], bool]]] = [
                ('==', cls._op_eq),
                ('|=', cls._op_in),
                ('>=', cls._op_ge),
                ('<=', cls._op_le),
            ]

            for op, func in operators:
                if (outer_op := cls._get_outer_op(expression, op)) is not None:
                    expression, expected_value = outer_op
                    expected_value = expected_value.strip('"\'')

                    if op == '|=':
                        # make string that is json compatible
                        if expected_value[0] in ['"', "'"] and expected_value[0] == expected_value[-1]:
                            expected_value = expected_value[1:-1]

                        expected_value = expected_value.replace("'", '"')
                        expected = [str(ev) for ev in cast('list[str]', jsonloads(expected_value))]
                    else:
                        expected = expected_value

                    assertion = func
                    break

            if not cls.validate(expression):
                message = 'not a valid expression'
                raise RuntimeError(message)

            jsonpath = jsonpath_parse(expression)

            def _parser(input_payload: list | dict) -> list[str]:
                # we need to fool jsonpath-ng to allow "validation" queries on objects on multiple properties
                # this shouldn't be done if the input is a nested object, and the query looks for any properties
                # recursively under the root (`@.`)
                if isinstance(input_payload, dict) and '`this`' in expression and '@.' not in expression:
                    input_payload = [input_payload]

                values: list[str] = []
                for m in jsonpath.find(input_payload):
                    if m is None or m.value is None:
                        continue

                    value = jsondumps(m.value) if isinstance(m.value, dict | list) else str(m.value)

                    if expected is None or (assertion is not None and assertion(value, expected)):
                        values.append(value)

                return values
        except Exception as e:
            message = f'{cls.__name__}: unable to parse with "{expression}": {e!s}'
            raise ValueError(message) from e
        else:
            return _parser


@transformer(TransformerContentType.XML)
class XmlTransformer(Transformer):
    EMPTY = '<?xml version="1.0" encoding="UTF-8"?><thisIsAnEmptyDocument/>'

    _parser = XML.XMLParser(remove_blank_text=True)

    @classmethod
    def transform(cls, raw: str) -> Any:
        document = XML.XML(raw.encode(), parser=cls._parser)

        # remove namespaces, which makes it easier to use XPath...
        for element in document.getiterator():
            if isinstance(element.tag, str):
                element.tag = XML.QName(element).localname

        XML.cleanup_namespaces(document)

        return document

    @classmethod
    def validate(cls, expression: str) -> bool:
        valid = False
        try:
            XML.XPath(expression)
            valid = True
        except Exception:
            valid = False

        return valid

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], list[str]]:
        try:
            try:
                xmlpath = XML.XPath(expression, smart_strings=False)

                def get_values(input_payload: Any) -> list[str]:
                    values: list[str] = []
                    for match in xmlpath(input_payload):
                        if match is not None:
                            value: str
                            value = XML.tostring(match, with_tail=False).decode() if isinstance(match, XML._Element) else str(match).strip()

                            if len(value) > 0:
                                values.append(value)

                    return values
            except XML.XPathSyntaxError as e:
                message = str(e).lower()
                raise RuntimeError(message) from e
            else:
                return get_values
        except Exception as e:
            message = f'{cls.__name__}: unable to parse "{expression}": {e!s}'
            raise ValueError(message) from e


@transformer(TransformerContentType.PLAIN)
class PlainTransformer(Transformer):
    @classmethod
    def transform(cls, raw: str) -> Any:
        return raw

    @classmethod
    def validate(cls, _expression: str) -> bool:
        # everything is a valid expression when it comes to plain transformer...
        return True

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], list[str]]:
        get_values_impl: Callable[[Any], list[str]]

        try:
            strict_expression = expression
            if len(strict_expression) > 1:
                if strict_expression[0] != '^':
                    strict_expression = f'^{strict_expression}'
                if strict_expression[-1] != '$':
                    strict_expression = f'{strict_expression}$'

            pattern = re.compile(strict_expression)

            if pattern.groups < 0 or pattern.groups > 1:
                message = f'{cls.__name__}: only expressions that has zero or one match group is allowed'
                raise ValueError(message)

            def get_values(input_payload: Any) -> list[str]:
                return re.findall(pattern, input_payload)

            get_values_impl = get_values
        except re.error:

            def get_values(input_payload: Any) -> list[str]:
                matches: list[str] = []
                if str(input_payload) == expression:
                    matches.append(str(input_payload))

                return matches

            get_values_impl = get_values

        return get_values_impl


class JsonBytesEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode('utf-8')
            except Exception:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)
