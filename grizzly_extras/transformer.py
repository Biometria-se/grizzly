'''This modules contains the means to transform a raw string to format that is possible to easy search
for attributes and their value in.
'''
import re

from abc import ABCMeta
from typing import Optional, Tuple, Any, Dict, Type, List, Callable
from functools import wraps
from json import loads as jsonloads, dumps as jsondumps, JSONEncoder
from enum import Enum, auto

from jsonpath_ng.ext import parse as jsonpath_parse
from lxml import etree as XML

class TransformerError(Exception):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message

class TransformerContentType(Enum):
    GUESS = 0
    JSON = auto()
    XML = auto()
    PLAIN = auto()

    @classmethod
    def from_string(cls, value: str) -> 'TransformerContentType':
        if value.strip() in ['application/json', 'json']:
            return TransformerContentType.JSON
        elif value.strip() in ['application/xml', 'xml']:
            return TransformerContentType.XML
        elif value.strip() in ['text/plain', 'plain']:
            return TransformerContentType.PLAIN
        else:
            raise ValueError(f'"{value}" is an unknown response content type')

class Transformer(ABCMeta):
    @classmethod
    def transform(cls, content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
        raise NotImplementedError(f'{cls.__name__} has not implemented transform')

    @classmethod
    def validate(cls, expression: str) -> bool:
        raise NotImplementedError(f'{cls.__name__} has not implemented validate')

    @classmethod
    def parser(cls, expression: str) -> Callable[[Any], List[str]]:
        raise NotImplementedError(f'{cls.__name__} has not implemented parse')

class transformer:
    content_type: TransformerContentType
    available: Dict[TransformerContentType, Type[Transformer]] = {}

    def __init__(self, content_type: TransformerContentType) -> None:
        if content_type == TransformerContentType.GUESS:
            raise ValueError(f'it is not allowed to register a transformer of type GUESS')

        self.content_type = content_type

    def __call__(self, impl: Type[Transformer]) -> Type[Transformer]:
        impl_transform = impl.transform
        @wraps(impl.transform)
        def wrapped_transform(content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
            try:
                if content_type in [TransformerContentType.GUESS, self.content_type]:
                    transformed_content_type, transformed = impl_transform(content_type, raw)

                    # transformation was successfully guessed (no exception)
                    if transformed_content_type == TransformerContentType.GUESS:
                        transformed_content_type = self.content_type

                    return (transformed_content_type, transformed, )
            except Exception as e:
                if content_type == self.content_type:
                    raise TransformerError(f'failed to transform input as {self.content_type.name}: {str(e)}') from e

            # fall through, try to transform as next content type
            return (TransformerContentType.GUESS, raw, )

        setattr(impl, '__wrapped_transform__', impl_transform)
        setattr(impl, 'transform', wrapped_transform)

        if self.content_type not in transformer.available:
            transformer.available.update({self.content_type: impl})

        return impl


@transformer(TransformerContentType.JSON)
class JsonTransformer(Transformer):
    @classmethod
    def transform(cls, content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
        return (content_type, jsonloads(raw), )

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
            if not cls.validate(expression):
                raise RuntimeError(f'{cls.__name__}: not a valid expression')

            jsonpath = jsonpath_parse(expression)

            def _parser(input_payload: Any) -> List[str]:
                values: List[str] = []
                for m in jsonpath.find(input_payload):
                    if m is None or m.value is None:
                        continue

                    if isinstance(m.value, (dict, list, )):
                        value = jsondumps(m.value)
                    else:
                        value = str(m.value)

                    values.append(value)

                return values

            return _parser
        except Exception as e:
            raise ValueError(f'{cls.__name__}: unable to parse "{expression}": {str(e)}') from e


@transformer(TransformerContentType.XML)
class XmlTransformer(Transformer):
    _parser = XML.XMLParser(remove_blank_text=True)

    @classmethod
    def transform(cls, content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
        document = XML.XML(raw.encode('utf-8'), parser=cls._parser)

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
                    values: List[str] = []
                    for match in xmlpath(input_payload):
                        if match is not None:
                            value: str
                            if isinstance(match, XML._Element):
                                value = XML.tostring(match, with_tail=False).decode('utf-8')
                            else:
                                value = str(match).strip()

                            if len(value) > 0:
                                values.append(value)

                    return values

                return get_values
            except XML.XPathSyntaxError as e:
                raise RuntimeError(f'{cls.__name__}: not a valid expression: {str(e)}') from e
        except Exception as e:
            raise ValueError(f'{cls.__name__}: unable to parse "{expression}": {str(e)}') from e


@transformer(TransformerContentType.PLAIN)
class PlainTransformer(Transformer):
    @classmethod
    def transform(cls, content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
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
