import os
import shutil
import socket

from typing import Generator, Tuple, Callable, Type, Optional, Any, Literal, List, Union, Dict
from mypy_extensions import VarArg, KwArg

import pytest

from _pytest.tmpdir import TempPathFactory
from jinja2.environment import Template
from behave.runner import Context, Runner
from behave.model import Scenario, Step, Background
from behave.configuration import Configuration
from behave.step_registry import registry as step_registry
from locust.env import Environment
from locust.user.users import User
from locust.user.task import TaskSet

from pytest_mock.plugin import MockerFixture
from paramiko.transport import Transport
from paramiko.channel import Channel
from paramiko.sftp import BaseSFTP
from paramiko.sftp_client import SFTPClient

from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.task import RequestTask
from grizzly.users.base import GrizzlyUser
from grizzly.scenarios import GrizzlyScenario

from .helpers import TestUser, TestTaskSet
# pylint: disable=redefined-outer-name


REQUEST_TASK_TEMPLATE_CONTENTS = """{
    "result": {
        "id": "ID-{{ AtomicIntegerIncrementer.messageID }}",
        "date": "{{ AtomicDate.now }}",
        "variable": "{{ messageID }}",
        "item": {
            "description": "this is just a description"
        }
    }
}"""


@pytest.fixture
def noop_zmq(mocker: MockerFixture) -> Callable[[str], None]:
    def mocked_noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        pass

    def patch(prefix: str) -> None:
        targets = [
            'zmq.sugar.context.Context.term',
            'zmq.sugar.context.Context.__del__',
            'zmq.sugar.socket.Socket.bind',
            'zmq.sugar.socket.Socket.connect',
            'zmq.sugar.socket.Socket.send_json',
            'zmq.sugar.socket.Socket.recv_json',
            'zmq.sugar.socket.Socket.disconnect',
            'gsleep',
        ]

        for target in targets:
            mocker.patch(
                f'{prefix}.{target}',
                mocked_noop,
            )

    return patch


@pytest.fixture
def request_task(tmp_path_factory: TempPathFactory) -> Generator[Tuple[str, str, RequestTask], None, None]:
    test_context = tmp_path_factory.mktemp('example_payload') / 'requests'
    test_context.mkdir()
    request_file = test_context / 'payload.j2.json'
    request_file.touch()
    request_file.write_text(REQUEST_TASK_TEMPLATE_CONTENTS)
    request_path = os.path.dirname(str(request_file))

    request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
    request.template = Template(REQUEST_TASK_TEMPLATE_CONTENTS)
    request.source = REQUEST_TASK_TEMPLATE_CONTENTS
    request.scenario = GrizzlyContextScenario()
    request.scenario.name = 'test-scenario'
    request.scenario.user.class_name = 'TestUser'
    request.scenario.context['host'] = 'http://example.com'
    request.scenario.behave = None

    try:
        yield (request_path, str(request_file).replace(f'{request_path}/', ''), request)
    finally:
        shutil.rmtree(os.path.dirname(request_path))


@pytest.fixture
def request_task_syntax_error(tmp_path_factory: TempPathFactory) -> Generator[Tuple[str, str], None, None]:
    test_context = tmp_path_factory.mktemp('example_payload') / 'requests'
    test_context.mkdir()
    payload_file = test_context / 'payload-syntax-error.j2.json'
    payload_file.touch()

    # remove all j2 end tags, to create syntax error
    try:
        contents = REQUEST_TASK_TEMPLATE_CONTENTS.replace('}}', '')
        payload_file.write_text(contents)
        path = os.path.dirname(str(payload_file))

        yield (path, str(payload_file).replace(f'{path}/', ''))
    finally:
        shutil.rmtree(os.path.dirname(path))


@pytest.fixture(scope='function')
def locust_environment(tmp_path_factory: TempPathFactory) -> Generator[Environment, None, None]:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = os.path.dirname(test_context)

    try:
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        yield Environment()
    finally:
        try:
            del os.environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        shutil.rmtree(test_context_root)


@pytest.fixture(scope='module')
def locust_user(locust_environment: Environment) -> User:
    return TestUser(locust_environment)

GrizzlyContextFixture = Callable[
    [
        str,
        Optional[Type[GrizzlyUser]],
        Optional[Type[GrizzlyScenario]],
        Optional[bool]
    ],
    Tuple[
        Environment,
        User,
        Optional[TaskSet],
        Tuple[str, str, RequestTask]
    ]
]

@pytest.fixture(scope='function')
def grizzly_context(request_task: Tuple[str, str, RequestTask]) -> Generator[GrizzlyContextFixture, None, None]:
    def wrapper(
        host: str = '',
        user_type: Optional[Type[GrizzlyUser]] = None,
        task_type: Optional[Type[GrizzlyScenario]] = None,
        no_tasks: Optional[bool] = False,
    ) -> Tuple[Environment, GrizzlyUser, Optional[GrizzlyScenario], Tuple[str, str, RequestTask]]:
        if user_type is None:
            user_type = TestUser

        if task_type is None:
            task_type = TestTaskSet

        environment = Environment(
            host=host,
            user_classes=[user_type],
        )

        os.environ['GRIZZLY_CONTEXT_ROOT'] = os.path.abspath(os.path.join(request_task[0], '..'))
        request_task[-1].name = task_type.__name__

        user_type.host = host
        user_type._scenario = request_task[-1].scenario
        user = user_type(environment)

        if not no_tasks:
            user_type.tasks = [task_type]
            task = task_type(parent=user)
        else:
            user_type.tasks = []
            task = None

        return environment, user, task, request_task

    yield wrapper

    try:
        del os.environ['GRIZZLY_CONTEXT_ROOT']
    except KeyError:
        pass


@pytest.fixture
def paramiko_mocker(mocker: MockerFixture) -> Generator[Callable[[], None], None, None]:
    def patch() -> None:
        # unable to import socket.AddressFamily and socket.SocketKind ?!
        def _socket_getaddrinfo(
            hostname: Union[bytearray, bytes, str, None], port: Union[str, int, None], addrfamily: int, kind: int
        ) -> List[Tuple[int, int, Optional[int], Optional[str], Optional[Tuple[str, int]]]]:
            return [(socket.AF_INET, socket.SOCK_STREAM, None, None, None, )]

        def _socket_connect(self: socket.socket, address: Any) -> None:
            pass

        def _start_client(self: Transport, event: Optional[Any] = None, timeout: Optional[Any] = None) -> None:
            self.active = True

        def _auth_password(self: Transport, username: str, password: Optional[str], event: Optional[Any] = None, fallback: Optional[bool] = True) -> List[str]:
            return []

        def _is_authenticated(self: Transport) -> Literal[True]:
            return True

        def __send_version(self: BaseSFTP) -> str:
            return '2.0-grizzly'

        def _open_session(self: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None, timeout: Optional[int] = None) -> Channel:
            return Channel(1)

        def _from_transport(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None) -> SFTPClient:
            channel = _open_session(transport)
            setattr(channel, 'transport', transport)

            return SFTPClient(channel)

        def _sftpclient_close(self: SFTPClient) -> None:
            pass

        def _transport_close(self: Transport) -> None:
            pass

        def _get(self: SFTPClient, remotepath: str, localpath: str, callback: Optional[Callable[[VarArg(Any), KwArg(Any)], None]] = None) -> None:
            if callback is not None:
                callback(100, 1000)

        def _put(self: SFTPClient, localpath: str, remotepath: str, callback: Optional[Callable[[VarArg(Any), KwArg(Any)], None]] = None, confirm: Optional[bool] = True) -> None:
            if callback is not None:
                callback(100, 1000)

        mocker.patch(
            'paramiko.transport.socket.getaddrinfo',
            _socket_getaddrinfo,
        )
        mocker.patch(
            'paramiko.transport.socket.socket.connect',
            _socket_connect,
        )

        mocker.patch(
            'paramiko.transport.Transport.is_authenticated',
            _is_authenticated,
        )

        mocker.patch(
            'paramiko.transport.Transport.start_client',
            _start_client,
        )

        mocker.patch(
            'paramiko.transport.Transport.auth_password',
            _auth_password,
        )

        mocker.patch(
            'paramiko.transport.Transport.close',
            _transport_close,
        )

        mocker.patch(
            'paramiko.sftp.BaseSFTP._send_version',
            __send_version,
        )

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.from_transport',
            _from_transport,
        )

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.close',
            _sftpclient_close,
        )

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.put',
            _put,
        )

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.get',
            _get,
        )

    yield patch


@pytest.fixture(scope='module')
def behave_runner() -> Runner:
    return Runner(config=None)


@pytest.fixture
def behave_context(locust_environment: Environment) -> Generator[Context, None, None]:
    runner = Runner(
        config=Configuration(
            command_args=[],
            load_config=False,
        )
    )
    context = Context(runner)
    setattr(context, '_runner', runner)  # to weakref
    context.config.base_dir = '.'
    context.scenario = Scenario(filename=None, line=None, keyword='', name='')
    context.step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    context.scenario.steps = [context.step]
    context.scenario.background = Background(filename=None, line=None, keyword='', steps=[context.step], name='')
    context._runner.step_registry = step_registry
    grizzly = GrizzlyContext()
    grizzly.state.environment = locust_environment
    setattr(context, 'grizzly', grizzly)

    yield context

    try:
        GrizzlyContext.destroy()
    except ValueError:
        pass


@pytest.fixture
def behave_scenario() -> Scenario:
    return Scenario(filename=None, line=None, keyword='', name='')
