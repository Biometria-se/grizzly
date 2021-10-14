'''This modules contains the means to transform a raw string to format that is possible to easy search
for attributes and their value in.
'''
import re

from abc import ABCMeta
from typing import Tuple, Any, Dict, Type, List, Callable
from functools import wraps
from json import loads as jsonloads, JSONEncoder

from jsonpath_ng.ext import parse as jsonpath_parse
from lxml import etree as XML

from .exceptions import TransformerError
from .types import ResponseContentType


class Transformer(ABCMeta):
    @classmethod
    def transform(cls, content_type: ResponseContentType, raw: str) -> Tuple[ResponseContentType, Any]:
        raise NotImplementedError(f'{cls.__name__} has not implemented transform')

    @classmethod
    def validate(cls, expression: str) -> bool:
        raise NotImplementedError(f'{cls.__name__} has not implemented validate')

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        raise NotImplementedError(f'{cls.__name__} has not implemented parse')

class transformer:
    content_type: ResponseContentType
    available: Dict[ResponseContentType, Type[Transformer]] = {}

    def __init__(self, content_type: ResponseContentType) -> None:
        if content_type == ResponseContentType.GUESS:
            raise ValueError(f'it is not allowed to register a transformer of type GUESS')

        self.content_type = content_type

    def __call__(self, impl: Type[Transformer]) -> Type[Transformer]:
        impl_transform = impl.transform
        @wraps(impl.transform)
        def wrapped_transform(content_type: ResponseContentType, raw: str) -> Tuple[ResponseContentType, Any]:
            try:
                if content_type in [ResponseContentType.GUESS, self.content_type]:
                    transformed_content_type, transformed = impl_transform(content_type, raw)

                    # transformation was successfully guessed (no exception)
                    if transformed_content_type == ResponseContentType.GUESS:
                        transformed_content_type = self.content_type

                    return (transformed_content_type, transformed, )
            except Exception as e:
                if content_type == self.content_type:
                    raise TransformerError(f'failed to transform input as {self.content_type.name}: {str(e)}') from e

            # fall through, try to transform as next content type
            return (ResponseContentType.GUESS, raw, )

        setattr(impl, '__wrapped_transform__', impl_transform)
        setattr(impl, 'transform', wrapped_transform)

        if self.content_type not in transformer.available:
            transformer.available.update({self.content_type: impl})

        return impl


@transformer(ResponseContentType.JSON)
class JsonTransformer(Transformer):
    @classmethod
    def transform(cls, content_type: ResponseContentType, raw: str) -> Tuple[ResponseContentType, Any]:
        return (content_type, jsonloads(raw), )

    @classmethod
    def validate(cls, expression: str) -> bool:
        return expression.startswith('$.') and len(expression) > 2

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        try:
            if not cls.validate(expression):
                raise RuntimeError(f'{cls.__name__}: not a valid expression')

            jsonpath = jsonpath_parse(expression)

            def get_values(input_payload: Any) -> List[str]:
                return [str(m.value) for m in jsonpath.find(input_payload) if m.value is not None]

            return get_values
        except Exception as e:
            raise ValueError(f'{cls.__name__}: unable to parse "{expression}": {str(e)}') from e


@transformer(ResponseContentType.XML)
class XmlTransformer(Transformer):
    @classmethod
    def transform(cls, content_type: ResponseContentType, raw: str) -> Tuple[ResponseContentType, Any]:
        document = XML.XML(raw.encode('utf-8'))

        # remove namespaces, which makes it easier to use XPath...
        for element in document.getiterator():
            if isinstance(element.tag, str):
                element.tag = XML.QName(element).localname

        XML.cleanup_namespaces(document)

        return (
            content_type,
            document,
        )

    @classmethod
    def validate(cls, expression: str) -> bool:
        valid = False
        try:
            XML.XPath(expression)
            valid = True
        except:
            valid = False
        finally:
            return valid

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        try:
            try:
                xmlpath = XML.XPath(expression, smart_strings=False)

                def get_values(input_payload: Any) -> List[str]:
                    return [str(value).strip() for value in xmlpath(input_payload) if value is not None and len(value) > 1]

                return get_values
            except XML.XPathSyntaxError as e:
                raise RuntimeError(f'{cls.__name__}: not a valid expression: {str(e)}') from e
        except Exception as e:
            raise ValueError(f'{cls.__name__}: unable to parse "{expression}": {str(e)}') from e


@transformer(ResponseContentType.PLAIN)
class PlainTransformer(Transformer):
    @classmethod
    def transform(cls, content_type: ResponseContentType, raw: str) -> Tuple[ResponseContentType, Any]:
        raise NotImplementedError(f'{cls.__name__} has not implemented transform')

    @classmethod
    def validate(cls, expression: str) -> bool:
        # everything is a valid expression when it comes to plain transformer...
        return True

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        try:
            strict_expression = expression
            if len(strict_expression) > 1:
                if not strict_expression[0] == '^':
                    strict_expression = f'^{strict_expression}'
                if not strict_expression[-1] == '$':
                    strict_expression = f'{strict_expression}$'

            pattern = re.compile(strict_expression)

            if pattern.groups < 0 or pattern.groups > 1:
                raise ValueError(f'{cls.__name__}: only expressions that has zero or one match group is allowed')

            def get_values(input_payload: Any) -> List[str]:
                return re.findall(pattern, input_payload)

            return get_values
        except re.error:
            def get_values(input_payload: Any) -> List[str]:
                matches: List[str] = []
                if str(input_payload) == expression:
                    matches.append(str(input_payload))

                return matches

            return get_values


class JsonBytesEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode('utf-8')
            except:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)
