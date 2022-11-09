import inspect
import subprocess
import os
import stat

from typing import Any, Dict, Optional, Tuple, List, Set, Callable, Generator
from types import MethodType, TracebackType
from pathlib import Path

from locust import task
from locust.event import EventHook

from grizzly.context import GrizzlyContextScenario
from grizzly.users.base import GrizzlyUser
from grizzly.types import GrizzlyResponse, RequestMethod, Message, Environment
from grizzly.testdata.variables import AtomicVariable
from grizzly.tasks import RequestTask, GrizzlyTask, template
from grizzly.scenarios import GrizzlyScenario
from grizzly.utils import fastdeepcopy


class AtomicCustomVariable(AtomicVariable[str]):
    pass


message_callback_not_a_method = True


def message_callback(environment: Environment, msg: Message) -> None:
    pass


def message_callback_incorrect_sig(msg: Message, environment: Environment) -> Message:
    return Message('test', None, None)


class RequestCalled(Exception):
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

    def request(self, request: RequestTask) -> GrizzlyResponse:
        raise RequestCalled(request)


class TestScenario(GrizzlyScenario):
    __test__ = False

    @task
    def task(self) -> None:
        self.user.request(
            RequestTask(RequestMethod.POST, name='test', endpoint='payload.j2.json')
        )


@template('name')
class TestTask(GrizzlyTask):
    __test__ = False

    name: Optional[str]
    call_count: int
    task_call_count: int

    def __init__(self, name: Optional[str] = None, scenario: Optional[GrizzlyContextScenario] = None) -> None:
        super().__init__(scenario)
        self.name = name
        self.call_count = 0
        self.task_call_count = 0

    def __call__(self) -> Callable[['GrizzlyScenario'], Any]:
        self.call_count += 1

        def task(parent: 'GrizzlyScenario') -> Any:
            parent.user.environment.events.request.fire(
                request_type='TSTSK',
                name=f'TestTask: {self.name}',
                response_time=13,
                response_length=37,
                context=fastdeepcopy(parent.user._context),
                exception=None,
            )
            self.task_call_count += 1
            parent.user.logger.debug(f'{self.name} executed')

        return task


class ResultSuccess(Exception):
    pass


class ResultFailure(Exception):
    pass


def check_arguments(kwargs: Dict[str, Any]) -> Tuple[bool, List[str]]:
    expected = ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']
    actual = list(kwargs.keys())
    expected.sort()
    actual.sort()

    diff = list(set(expected) - set(actual))

    return actual == expected, diff


class RequestEvent(EventHook):
    def __init__(self, custom: bool = True):
        self.custom = custom

    def fire(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if self.custom:
            valid, diff = check_arguments(kwargs)
            if not valid:
                raise AttributeError(f'missing required arguments: {diff}')

        if 'exception' in kwargs and kwargs['exception'] is not None:
            raise ResultFailure(kwargs['exception'])
        else:
            raise ResultSuccess()


class RequestSilentFailureEvent(RequestEvent):
    def fire(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if self.custom:
            valid, diff = check_arguments(kwargs)
            if not valid:
                raise AttributeError(f'missing required arguments: {diff}')

        if 'exception' not in kwargs or kwargs['exception'] is None:
            raise ResultSuccess()


def get_property_decorated_attributes(target: Any) -> Set[str]:
    return set(
        [
            name
            for name, _ in inspect.getmembers(
                target,
                lambda p: isinstance(
                    p,
                    (property, MethodType)
                ) and not isinstance(
                    p,
                    (classmethod, MethodType)  # @classmethod anotated methods becomes @property
                )) if not name.startswith('_')
        ]
    )


def run_command(command: List[str], env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None) -> Tuple[int, List[str]]:
    output: List[str] = []
    if env is None:
        env = os.environ.copy()

    if cwd is None:
        cwd = os.getcwd()

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
        try:
            process.kill()
        except Exception:
            pass

    process.wait()

    return process.returncode, output


def onerror(func: Callable, path: str, exc_info: TracebackType) -> None:
    '''
    Error handler for ``shutil.rmtree``.
    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.
    If the error is for another reason it re-raises the error.
    Usage : ``shutil.rmtree(path, onerror=onerror)``
    '''
    # Is the error an access error?
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise  # pylint: disable=misplaced-bare-raise


# prefix components:
space = '    '
branch = '│   '
# pointers:
tee = '├── '
last = '└── '


def tree(dir_path: Path, prefix: str = '', ignore: Optional[List[str]] = None) -> Generator[str, None, None]:
    '''A recursive generator, given a directory Path object
    will yield a visual tree structure line by line
    with each line prefixed by the same characters
    credit: https://stackoverflow.com/a/59109706
    '''
    contents = sorted(list(dir_path.iterdir()))
    # contents each get pointers that are ├── with a final └── :
    pointers = [tee] * (len(contents) - 1) + [last]
    for pointer, sub_path in zip(pointers, contents):
        if ignore is None or sub_path.name not in ignore:
            yield prefix + pointer + sub_path.name
            if sub_path.is_dir():  # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from tree(sub_path, prefix=prefix + extension, ignore=ignore)
