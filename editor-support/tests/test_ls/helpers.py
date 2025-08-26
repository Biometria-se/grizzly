from __future__ import annotations

import os
import stat
import sys
from abc import ABCMeta
from enum import Enum
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, Any

import parse
from grizzly.types import MessageDirection
from grizzly_common.text import PermutationEnum, permutation

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from lsprotocol.types import CompletionItem, CompletionItemKind


def normalize_completion_item(
    steps: list[CompletionItem],
    kind: CompletionItemKind,
    attr: str = 'label',
) -> list[str]:
    labels: list[str] = []
    for step in steps:
        assert step.kind == kind
        value = getattr(step, attr)
        labels.append(value)

    return labels


def normalize_completion_text_edit(
    steps: list[CompletionItem],
    kind: CompletionItemKind,
) -> list[str]:
    labels: list[str] = []
    for step in steps:
        assert step.kind == kind
        assert step.text_edit is not None
        labels.append(step.text_edit.new_text)

    return labels


class DummyEnum(PermutationEnum):
    HELLO = 0
    WORLD = 1
    FOO = 2
    BAR = 3

    @classmethod
    def from_string(cls, value: str) -> DummyEnum:
        for enum_value in cls:
            if enum_value.name.lower() == value.lower():
                return enum_value

        message = f'{value} is not a valid value'
        raise ValueError(message)


class DummyEnumNoFromString(PermutationEnum):
    ERROR = 0

    @classmethod
    def magic(cls, value: str) -> str:
        return value


class DummyEnumNoFromStringType(Enum):
    ERROR = 1

    @classmethod
    def from_string(cls, value: str):  # type: ignore[no-untyped-def]  # noqa: ANN206
        return value


@parse.with_pattern(r'(hello|world|foo|bar)')
@permutation(
    vector=(
        False,
        True,
    )
)
def parse_with_pattern_and_vector(text: str) -> str:
    return text


@parse.with_pattern(r'(alice|bob)')
def parse_with_pattern(text: str) -> str:
    return text


@parse.with_pattern('')
def parse_with_pattern_error(text: str) -> str:
    return text


def parse_enum_indirect(text: str) -> MessageDirection:
    return MessageDirection.from_string(text)


def ANY(*cls: type, message: str | None = None) -> object:  # noqa: N802
    """Compare equal to everything, as long as it is of the same type."""

    class WrappedAny(metaclass=ABCMeta):  # noqa: B024
        def __eq__(self, other: object) -> bool:
            if len(cls) < 1:
                return True

            return isinstance(other, cls) and (message is None or (message is not None and message in str(other)))

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __repr__(self) -> str:
            c = cls[0] if len(cls) == 1 else cls
            representation: list[str] = [f'<ANY({c})', '>']

            if message is not None:
                representation.insert(-1, f", message='{message}'")

            return ''.join(representation)

        def __hash__(self) -> int:
            return hash(self)

    for c in cls:
        WrappedAny.register(c)

    return WrappedAny()


def SOME(cls: type, *value: Any, **values: Any) -> object:  # noqa: N802
    if len(value) > 0 and len(values) > 0:
        message = 'cannot use both positional and named arguments'
        raise RuntimeError(message)

    if len(values) < 1 and len(value) < 1:
        raise AttributeError(name='values', obj=str(type))

    if len(value) > 1:
        message = 'can only use 1 positional argument'
        raise RuntimeError(message)

    if len(value) > 0 and isinstance(value[0], dict):
        values = {**value[0]}

    class WrappedSome(metaclass=ABCMeta):  # noqa: B024
        def __eq__(self, other: object) -> bool:
            if issubclass(cls, dict):

                def get_value(other: Any, attr: str) -> Any:
                    return other.get(attr)
            else:

                def get_value(other: Any, attr: str) -> Any:
                    return getattr(other, attr)

            return isinstance(other, cls) and all(get_value(other, attr) == value for attr, value in values.items())

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __repr__(self) -> str:
            info = ', '.join([f'{key}={value!r}' for key, value in values.items()])
            return f'<SOME({cls.__name__}, {info})>'

        def __hash__(self) -> int:
            return hash(self)

    WrappedSome.register(cls)

    return WrappedSome()


def onerror(
    func: Callable,
    path: str,
    exc_info: BaseException  # noqa: ARG001
    | tuple[
        type[BaseException],
        BaseException,
        TracebackType | None,
    ],
) -> None:
    """Error handler for shutil.rmtree.

    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.
    If the error is for another reason it re-raises the error.
    Usage : ``shutil.rmtree(path, onerror=onerror)``
    """
    _path = Path(path)
    # Is the error an access error?
    if not os.access(_path, os.W_OK):
        _path.chmod(stat.S_IWUSR)
        func(path)
    else:
        raise  # noqa: PLE0704


def rm_rf(path: str | Path) -> None:
    """Remove the path contents recursively, even if some elements
    are read-only.
    """
    p = path.as_posix() if isinstance(path, Path) else path

    if sys.version_info >= (3, 12):
        rmtree(p, onexc=onerror)
    else:
        rmtree(p, onerror=onerror)
