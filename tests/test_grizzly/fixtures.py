import pkgutil
import inspect
import socket

from typing import TYPE_CHECKING, Optional, Union, Callable, Any, Literal, List, Tuple, Type, Dict
from types import TracebackType
from mypy_extensions import VarArg, KwArg
from os import environ, path
from shutil import rmtree

from _pytest.tmpdir import TempPathFactory
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from paramiko.transport import Transport
from paramiko.channel import Channel
from paramiko.sftp import BaseSFTP
from paramiko.sftp_client import SFTPClient
from behave.runner import Context as BehaveContext, Runner
from behave.model import Scenario, Step, Background
from behave.configuration import Configuration
from behave.step_registry import registry as step_registry
from jinja2 import Template

from grizzly.types import RequestMethod
from grizzly.tasks.request import RequestTask

import grizzly.testdata.variables as variables

from grizzly.context import GrizzlyContext, GrizzlyContextScenario

from .helpers import TestUser, TestScenario

if TYPE_CHECKING:
    from grizzly.users.base import GrizzlyUser
    from grizzly.scenarios import GrizzlyScenario

__all__ = [
    'AtomicVariableCleanupFixture',
    'LocustFixture',
    'ParamikoFixture',
    'BehaveFixture',
    'RequestTaskFixture',
    'GrizzlyFixture',
    'NoopZmqFixture',
]


class AtomicVariableCleanupFixture:
    def __call__(self) -> None:
        try:
            GrizzlyContext.destroy()
        except:
            pass

        # automagically find all Atomic variables and try to destroy them, instead of explicitlly define them one by one
        for _, package_name, _ in pkgutil.iter_modules([path.dirname(variables.__file__)]):
            module = getattr(variables, package_name)
            for member_name, member in inspect.getmembers(module):
                if inspect.isclass(member) and member_name.startswith('Atomic') and member_name != 'AtomicVariable':
                    destroy = getattr(member, 'destroy', None)
                    if destroy is None:
                        continue

                    try:
                        destroy()
                    except:
                        pass

        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass


class LocustFixture:
    _test_context_root: str
    _tmp_path_factory: TempPathFactory

    env: Environment

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory

    def __enter__(self) -> 'LocustFixture':
        test_context = self._tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        self._test_context_root = path.dirname(test_context)

        environ['GRIZZLY_CONTEXT_ROOT'] = self._test_context_root
        self.env = Environment()

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        rmtree(self._test_context_root)

        return True


class ParamikoFixture:
    mocker: MockerFixture

    def __init__(self, mocker: MockerFixture) -> None:
        self.mocker = mocker

    def __call__(self) -> None:
        # unable to import socket.AddressFamily and socket.SocketKind ?!
        def _socket_getaddrinfo(
            hostname: Union[bytearray, bytes, str, None], port: Union[str, int, None], addrfamily: int, kind: int
        ) -> List[Tuple[int, int, Optional[int], Optional[str], Optional[Tuple[str, int]]]]:
            return [(socket.AF_INET, socket.SOCK_STREAM, None, None, None, )]

        def _socket_connect(self: socket.socket, address: Any) -> None:
            pass

        def _start_client(transport: Transport, event: Optional[Any] = None, timeout: Optional[Any] = None) -> None:
            transport.active = True

        def _auth_password(transport: Transport, username: str, password: Optional[str], event: Optional[Any] = None, fallback: Optional[bool] = True) -> List[str]:
            return []

        def _is_authenticated(transport: Transport) -> Literal[True]:
            return True

        def __send_version(base_sftp: BaseSFTP) -> str:
            return '2.0-grizzly'

        def _open_session(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None, timeout: Optional[int] = None) -> Channel:
            return Channel(1)

        def _from_transport(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None) -> SFTPClient:
            channel = _open_session(transport)
            setattr(channel, 'transport', transport)

            return SFTPClient(channel)

        def _sftpclient_close(sftp_client: SFTPClient) -> None:
            pass

        def _transport_close(transport: Transport) -> None:
            pass

        def _get(sftp_client: SFTPClient, remotepath: str, localpath: str, callback: Optional[Callable[[VarArg(Any), KwArg(Any)], None]] = None) -> None:
            if callback is not None:
                callback(100, 1000)

        def _put(
            sftp_client: SFTPClient, localpath: str, remotepath: str, callback: Optional[Callable[[VarArg(Any), KwArg(Any)], None]] = None, confirm: Optional[bool] = True,
        ) -> None:
            if callback is not None:
                callback(100, 1000)

        self.mocker.patch(
            'paramiko.transport.socket.getaddrinfo',
            _socket_getaddrinfo,
        )

        self.mocker.patch(
            'paramiko.transport.socket.socket.connect',
            _socket_connect,
        )

        self.mocker.patch(
            'paramiko.transport.Transport.is_authenticated',
            _is_authenticated,
        )

        self.mocker.patch(
            'paramiko.transport.Transport.start_client',
            _start_client,
        )

        self.mocker.patch(
            'paramiko.transport.Transport.auth_password',
            _auth_password,
        )

        self.mocker.patch(
            'paramiko.transport.Transport.close',
            _transport_close,
        )

        self.mocker.patch(
            'paramiko.sftp.BaseSFTP._send_version',
            __send_version,
        )

        self.mocker.patch(
            'paramiko.sftp_client.SFTPClient.from_transport',
            _from_transport,
        )

        self.mocker.patch(
            'paramiko.sftp_client.SFTPClient.close',
            _sftpclient_close,
        )

        self.mocker.patch(
            'paramiko.sftp_client.SFTPClient.put',
            _put,
        )

        self.mocker.patch(
            'paramiko.sftp_client.SFTPClient.get',
            _get,
        )

        print('patched paramiko')


class BehaveFixture:
    _locust_fixture: LocustFixture
    context: BehaveContext

    def __init__(self, locust_fixture: LocustFixture) -> None:
        self._locust_fixture = locust_fixture

    def __enter__(self) -> 'BehaveFixture':
        runner = Runner(
            config=Configuration(
                command_args=[],
                load_config=False,
            )
        )
        context = BehaveContext(runner)
        setattr(context, '_runner', runner)  # to weakref
        context.config.base_dir = '.'
        context.scenario = Scenario(filename=None, line=None, keyword='', name='')
        context.step = Step(filename=None, line=None, keyword='', step_type='step', name='')
        context.scenario.steps = [context.step]
        context.scenario.background = Background(filename=None, line=None, keyword='', steps=[context.step], name='')
        context._runner.step_registry = step_registry
        grizzly = GrizzlyContext()
        grizzly.state.environment = self._locust_fixture.env
        setattr(context, 'grizzly', grizzly)

        self.context = context

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        try:
            GrizzlyContext.destroy()
        except ValueError:
            pass

        return True


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


class RequestTaskFixture:
    _tmp_path_factory: TempPathFactory
    context_root: str
    relative_path: str
    request: RequestTask

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory

    def __enter__(self) -> 'RequestTaskFixture':
        test_context = self._tmp_path_factory.mktemp('example_payload') / 'requests'
        test_context.mkdir()
        request_file = test_context / 'payload.j2.json'
        request_file.touch()
        request_file.write_text(REQUEST_TASK_TEMPLATE_CONTENTS)
        request_path = path.dirname(str(request_file))

        request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
        request.template = Template(REQUEST_TASK_TEMPLATE_CONTENTS)
        request.source = REQUEST_TASK_TEMPLATE_CONTENTS
        request.scenario = GrizzlyContextScenario()
        request.scenario.name = 'test-scenario'
        request.scenario.user.class_name = 'TestUser'
        request.scenario.context['host'] = 'http://example.com'
        request.scenario.behave = None

        request.scenario.add_task(request)

        self.context_root = request_path
        self.request = request
        self.relative_path = str(request_file).replace(f'{request_path}/', '')

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        rmtree(path.dirname(self.context_root))

        return True


class RequestTaskFailureFixture(RequestTaskFixture):
    def __enter__(self) -> 'RequestTaskFailureFixture':
        test_context = self._tmp_path_factory.mktemp('example_payload') / 'requests'
        test_context.mkdir()
        payload_file = test_context / 'payload-syntax-error.j2.json'
        payload_file.touch()

        # remove all j2 end tags, to create syntax error
        contents = REQUEST_TASK_TEMPLATE_CONTENTS.replace('}}', '')
        payload_file.write_text(contents)
        self.context_root = path.dirname(str(payload_file))
        self.relative_path = str(payload_file).replace(f'{path}/', '')

        return self


class GrizzlyFixture:
    request_task: RequestTaskFixture
    grizzly: GrizzlyContext
    behave: BehaveContext

    def __init__(self, request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> None:
        self.request_task = request_task
        self.behave = behave_fixture.context

    def __enter__(self) -> 'GrizzlyFixture':
        environ['GRIZZLY_CONTEXT_ROOT'] = path.abspath(path.join(self.request_task.context_root, '..'))
        self.grizzly = GrizzlyContext()
        self.grizzly._scenarios = [self.request_task.request.scenario]

        return self

    def __call__(
        self,
        host: str = '',
        user_type: Optional[Type['GrizzlyUser']] = None,
        scenario_type: Optional[Type['GrizzlyScenario']] = None,
        no_tasks: Optional[bool] = False,
    ) -> Tuple[Environment, 'GrizzlyUser', Optional['GrizzlyScenario']]:
        if user_type is None:
            user_type = TestUser

        if scenario_type is None:
            scenario_type = TestScenario

        environment = Environment(
            host=host,
            user_classes=[user_type],
        )

        self.request_task.request.name = scenario_type.__name__

        user_type.host = host
        user_type._scenario = self.request_task.request.scenario
        user = user_type(environment)

        if not no_tasks:
            user_type.tasks = [scenario_type]
            scenario = scenario_type(parent=user)
        else:
            user_type.tasks = []
            scenario = None

        return environment, user, scenario

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass

        try:
            GrizzlyContext.destroy()
        except:
            pass

        return True


class NoopZmqFixture:
    _mocker: MockerFixture

    def __init__(self, mocker: MockerFixture) -> None:
        self._mocker = mocker

    def __call__(self, prefix: str) -> None:
        def mocked_noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            pass

        targets = [
            'zmq.Context.term',
            'zmq.Context.__del__',
            'zmq.Socket.bind',
            'zmq.Socket.connect',
            'zmq.Socket.send_json',
            'zmq.Socket.recv_json',
            'zmq.Socket.disconnect',
            'gsleep',
        ]

        for target in targets:
            self._mocker.patch(
                f'{prefix}.{target}',
                mocked_noop,
            )
