"""Useful helper stuff for tests."""
from __future__ import annotations

import inspect
import os
import re
import signal
import stat
import subprocess
import sys
from abc import ABCMeta
from contextlib import suppress
from copy import deepcopy
from pathlib import Path
from re import Pattern
from shutil import rmtree
from types import MethodType, TracebackType
from typing import TYPE_CHECKING, Any, Callable, Optional, Union
from unittest.mock import MagicMock

from geventhttpclient.response import HTTPSocketPoolResponse
from locust import task
from locust.contrib.fasthttp import FastResponse
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager

from grizzly.scenarios import GrizzlyScenario
from grizzly.tasks import GrizzlyTask, RequestTask, grizzlytask, template
from grizzly.testdata.variables import AtomicVariable
from grizzly.types import GrizzlyResponse, RequestMethod
from grizzly.types.locust import Environment, Message
from grizzly.users import GrizzlyUser

if TYPE_CHECKING:
    from collections.abc import Generator


class AtomicCustomVariable(AtomicVariable[str]):
    pass


message_callback_not_a_method = True


def ANY(*cls: type, message: Optional[str] = None) -> object:  # noqa: N802
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
            info = ', '.join([f"{key}={value}" for key, value in values.items()])
            return f'<SOME({cls}, {info})>'

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


def message_callback(environment: Environment, msg: Message) -> None:  # noqa: ARG001
    pass


def message_callback_incorrect_sig(msg: Message, environment: Environment) -> Message:  # noqa: ARG001
    return Message('test', None, None)


class RequestCalled(Exception):  # noqa: N818
    endpoint: str
    request: RequestTask

    def __init__(self, request: RequestTask) -> None:
        super().__init__()

        self.endpoint = request.endpoint
        self.request = request


class TestUser(GrizzlyUser):
    __test__ = False

    _config_property: Optional[str] = None

    @property
    def config_property(self) -> Optional[str]:
        return self._config_property

    @config_property.setter
    def config_property(self, value: Optional[str]) -> None:
        self._config_property = value

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        raise RequestCalled(request)


class TestScenario(GrizzlyScenario):
    __test__ = False

    @task
    def task(self) -> None:
        self.user.request(
            RequestTask(RequestMethod.POST, name='test', endpoint='payload.j2.json'),
        )


@template('name')
class TestTask(GrizzlyTask):
    __test__ = False

    name: Optional[str]
    call_count: int
    task_call_count: int

    def __init__(self, name: Optional[str] = None) -> None:
        super().__init__()
        self.name = name
        self.call_count = 0
        self.task_call_count = 0

    def on_start(self, _: GrizzlyScenario) -> None:
        return

    def on_stop(self, _: GrizzlyScenario) -> None:
        return

    def __call__(self) -> grizzlytask:
        self.call_count += 1

        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            parent.user.environment.events.request.fire(
                request_type='TSTSK',
                name=f'TestTask: {self.name}',
                response_time=13,
                response_length=37,
                context=deepcopy(parent.user._context),
                exception=None,
            )
            self.task_call_count += 1

        @task.on_start
        def on_start(parent: GrizzlyScenario) -> None:
            self.on_start(parent)

        @task.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:
            self.on_stop(parent)

        return task


class TestExceptionTask(GrizzlyTask):
    __test__ = False

    error_type: type[Exception]

    def __init__(self, error_type: type[Exception]) -> None:
        self.error_type = error_type

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(_: GrizzlyScenario) -> Any:
            raise self.error_type

        return task


def check_arguments(kwargs: dict[str, Any]) -> tuple[bool, list[str]]:
    expected = ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']
    actual = list(kwargs.keys())
    expected.sort()
    actual.sort()

    diff = list(set(expected) - set(actual))

    return actual == expected, diff


def get_property_decorated_attributes(target: Any) -> set[str]:
    return {
        name
            for name, _ in inspect.getmembers(
                target,
                lambda p: isinstance(
                    p,
                    (property, MethodType),
                ) and not isinstance(
                    p,
                    (classmethod, MethodType),  # @classmethod anotated methods becomes @property
                )) if not name.startswith('_')
    }


def run_command(command: list[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> tuple[int, list[str]]:
    output: list[str] = []
    if env is None:
        env = os.environ.copy()

    if cwd is None:
        cwd = str(Path.cwd())

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
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)

    return process.returncode, output


def onerror(func: Callable, path: str, exc_info: Union[  # noqa: ARG001
        BaseException,
        tuple[type[BaseException], BaseException, Optional[TracebackType]],
]) -> None:
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
        raise  # pylint: disable=E0704


def rm_rf(path: Union[str, Path]) -> None:
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


def tree(dir_path: Path, prefix: str = '', ignore: Optional[list[str]] = None) -> Generator[str, None, None]:
    """Recursive generator.

    Given a directory Path object will yield a visual tree structure line by line
    with each line prefixed by the same characters credit: https://stackoverflow.com/a/59109706
    """
    contents = sorted(dir_path.iterdir())
    # contents each get pointers that are ├── with a final └── :
    pointers = [tee] * (len(contents) - 1) + [last]
    for pointer, sub_path in zip(pointers, contents):
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
    def possible(value: str) -> Union[regex, str]:
        if regex.valid(value):
            return regex(value)

        return value

    def __init__(self, pattern: str, flags: int = 0) -> None:
        self._regex = re.compile(pattern, flags)

    def __eq__(self, actual: object) -> bool:
        return isinstance(actual, str) and bool(self._regex.match(actual))

    def __repr__(self) -> str:
        return self._regex.pattern


def create_mocked_fast_response_context_manager(*, content: str, headers: Optional[dict[str, str]] = None, status_code: int = 200) -> FastResponseContextManager:
    ghc_response = MagicMock(spec=HTTPSocketPoolResponse)
    ghc_response.get_code.return_value = status_code
    ghc_response._headers_index = headers or {}
    response = FastResponse(ghc_response)
    response._cached_content = content.encode()

    context_manager = FastResponseContextManager(response, None, {})
    context_manager._entered = True

    return context_manager


JSON_EXAMPLE = {
    'glossary': {
        'title': 'example glossary',
        'GlossDiv': {
            'title': 'S',
            'GlossList': {
                'GlossEntry': {
                    'ID': 'SGML',
                    'SortAs': 'SGML',
                    'GlossTerm': 'Standard Generalized Markup Language',
                    'Acronym': 'SGML',
                    'Abbrev': 'ISO 8879:1986',
                    'GlossDef': {
                        'para': 'A meta-markup language, used to create markup languages such as DocBook.',
                        'GlossSeeAlso': ['GML', 'XML'],
                    },
                    'GlossSee': 'markup',
                    'Additional': [
                        {
                            'addtitle': 'test1',
                            'addvalue': 'hello world',
                        },
                        {
                            'addtitle': 'test2',
                            'addvalue': 'good stuff',
                        },
                    ],
                },
            },
        },
    },
}
