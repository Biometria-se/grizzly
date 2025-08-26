"""Useful helper stuff for tests."""

from __future__ import annotations

import os
import re
import signal
import stat
import subprocess
import sys
from abc import ABCMeta
from contextlib import suppress
from pathlib import Path
from re import Pattern
from shutil import rmtree
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Generator
    from types import TracebackType


def ANYUUID(version: int = 4) -> object:  # noqa: N802
    class WrappedAnyUuid:
        def __eq__(self, other: object) -> bool:
            if not isinstance(other, str):
                return False

            uuid_obj = UUID(other, version=version)

            return uuid_obj.hex == other.replace('-', '')

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __hash__(self) -> int:
            return hash(self)

    return WrappedAnyUuid()


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
    class WrappedSome:
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
            info = ', '.join([f'{key}={value}' for key, value in values.items()])
            return f'<SOME({cls}, {info})>'

        def __hash__(self) -> int:
            return hash(self)

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

    return WrappedSome()


def run_command(command: list[str], env: dict[str, str] | None = None, cwd: Path | None = None) -> tuple[int, list[str]]:
    output: list[str] = []
    if env is None:
        env = os.environ.copy()

    if cwd is None:
        cwd = Path.cwd()

    process = subprocess.Popen(
        command,
        env=env,
        cwd=cwd,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )

    try:
        while process.poll() is None:
            stdout = process.stdout
            if stdout is None:
                break

            buffer = stdout.readline()
            if not buffer:
                break

            output.append(buffer.decode('utf-8'))

        process.terminate()
    except KeyboardInterrupt:
        pass
    finally:
        with suppress(Exception):
            process.kill()

        process.wait()

        with suppress(Exception):
            if sys.platform != 'win32':
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                os.kill(process.pid, signal.CTRL_BREAK_EVENT)

    return process.returncode, output


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


# prefix components:
space = '    '
branch = '│   '
# pointers:
tee = '├── '
last = '└── '


def tree(dir_path: Path, prefix: str = '', ignore: list[str] | None = None) -> Generator[str, None, None]:
    """Recursive generator.

    Given a directory Path object will yield a visual tree structure line by line
    with each line prefixed by the same characters credit: https://stackoverflow.com/a/59109706
    """
    contents = sorted(dir_path.iterdir())
    # contents each get pointers that are ├── with a final └── :
    pointers = [tee] * (len(contents) - 1) + [last]
    for pointer, sub_path in zip(pointers, contents, strict=False):
        if ignore is None or sub_path.name not in ignore:
            yield prefix + pointer + sub_path.name
            if sub_path.is_dir():  # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from tree(sub_path, prefix=prefix + extension, ignore=ignore)


class regex:
    _regex: Pattern[str]

    @staticmethod
    def valid(value: str) -> bool:
        return len(value) > 1 and value[0] == '^' and value[-1] == '$'

    @staticmethod
    def possible(value: str) -> regex | str:
        if regex.valid(value):
            return regex(value)

        return value

    def __init__(self, pattern: str, flags: int = 0) -> None:
        self._regex = re.compile(pattern, flags)

    def __eq__(self, actual: object) -> bool:
        return isinstance(actual, str) and bool(self._regex.match(actual))

    def __repr__(self) -> str:
        return self._regex.pattern

    def __hash__(self) -> int:
        return hash(self)
