"""@anchor pydoc:grizzly_extras.transformer Transformer
This modules contains the means to transform a raw string to format that is possible to easy search
for attributes and their value in.
"""
from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from functools import wraps
from json import JSONEncoder
from json import dumps as jsondumps
from json import loads as jsonloads
from typing import Any, Callable, ClassVar, Dict, List, Optional, Type

from jsonpath_ng.ext import parse as jsonpath_parse
from lxml import etree as XML  # noqa: N812

from .text import PermutationEnum


class TransformerError(Exception):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message


class TransformerContentType(PermutationEnum):
    __vector__ = (False, True)

    UNDEFINED = None
    JSON = 'application/json'
    XML = 'application/xml'
    PLAIN = 'text/plain'
    MULTIPART_FORM_DATA = 'multipart/form-data'

    @classmethod
    def from_string(cls, value: str) -> TransformerContentType:
        value = value.lower()

        for enum in cls:
            if enum.name.lower() == value or enum.value == value:
                return enum

        message = f'"{value}" is an unknown response content type'
        raise ValueError(message)


class Transformer(metaclass=ABCMeta):
    __wrapped_transform__: ClassVar[Callable[[str], Any]]

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
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:  # pragma: no cover
        message = f'{cls.__name__} has not implemented parse'
        raise NotImplementedError(message)


class transformer:
    content_type: TransformerContentType
    available: ClassVar[Dict[TransformerContentType, Type[Transformer]]] = {}

    def __init__(self, content_type: TransformerContentType) -> None:
        if content_type == TransformerContentType.UNDEFINED:
            message = 'it is not allowed to register a transformer of type UNDEFINED'
            raise ValueError(message)

        self.content_type = content_type

    def __call__(self, impl: Type[Transformer]) -> Type[Transformer]:
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
            transformer.available.update({self.content_type: impl})

        return impl


@transformer(TransformerContentType.JSON)
class JsonTransformer(Transformer):
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
        except:
            valid = False

        return valid

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        try:
            expected: Optional[str] = None

            # we only have one instance of equals
            if '==' in expression and expression.index('==') == expression.rindex('==') and not ('?' in expression and '@' in expression):
                expression, expected = expression.split('==', 1)
                expected = expected.strip('"\'')

            if not cls.validate(expression):
                message = 'not a valid expression'
                raise RuntimeError(message)

            jsonpath = jsonpath_parse(expression)

            def _parser(input_payload: Any) -> List[str]:
                values: List[str] = []
                for m in jsonpath.find(input_payload):
                    if m is None or m.value is None:
                        continue

                    value = jsondumps(m.value) if isinstance(m.value, (dict, list)) else str(m.value)

                    if expected is None or expected == value:
                        values.append(value)

                return values
        except Exception as e:
            message = f'{cls.__name__}: unable to parse with "{expression}": {e!s}'
            raise ValueError(message) from e
        else:
            return _parser


@transformer(TransformerContentType.XML)
class XmlTransformer(Transformer):
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
        except:
            valid = False

        return valid

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        try:
            try:
                xmlpath = XML.XPath(expression, smart_strings=False)

                def get_values(input_payload: Any) -> List[str]:
                    values: List[str] = []
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
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        get_values_impl: Callable[[Any], List[str]]

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

            def get_values(input_payload: Any) -> List[str]:
                return re.findall(pattern, input_payload)

            get_values_impl = get_values
        except re.error:
            def get_values(input_payload: Any) -> List[str]:
                matches: List[str] = []
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
            except:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)
