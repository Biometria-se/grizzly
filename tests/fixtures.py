import inspect
import socket
import re

from typing import TYPE_CHECKING, Optional, Union, Callable, Any, Literal, List, Tuple, Type, Dict, cast
from types import TracebackType
from unittest.mock import MagicMock
from urllib.parse import urlparse
from mypy_extensions import VarArg, KwArg
from os import environ, getcwd, path
from shutil import rmtree, copytree
from json import dumps as jsondumps
from pathlib import Path
from textwrap import dedent, indent
from hashlib import sha1
from getpass import getuser

from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import FastResponse, FastRequest
from locust.env import Environment
from locust.runners import Runner
from geventhttpclient.header import Headers
from geventhttpclient.response import HTTPSocketPoolResponse
from _pytest.tmpdir import TempPathFactory
from pytest_mock.plugin import MockerFixture
from paramiko.transport import Transport
from paramiko.channel import Channel
from paramiko.sftp import BaseSFTP
from paramiko.sftp_client import SFTPClient
from behave.runner import Context as BehaveContext, Runner as BehaveRunner
from behave.model import Scenario, Step, Background, Feature
from behave.configuration import Configuration
from behave.step_registry import registry as step_registry
from requests.models import CaseInsensitiveDict, Response, PreparedRequest

from grizzly.types import GrizzlyResponseContextManager, RequestMethod
from grizzly.tasks import RequestTask
from grizzly.testdata.variables import destroy_variables

from grizzly.context import GrizzlyContext, GrizzlyContextScenario

from .helpers import TestUser, TestScenario, RequestSilentFailureEvent
from .helpers import onerror, run_command
from .webserver import Webserver


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

        destroy_variables()

        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass


class LocustFixture:
    _test_context_root: str
    _tmp_path_factory: TempPathFactory

    env: Environment
    runner: Runner

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory

    def __enter__(self) -> 'LocustFixture':
        test_context = self._tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        self._test_context_root = path.dirname(test_context)

        environ['GRIZZLY_CONTEXT_ROOT'] = self._test_context_root
        self.env = Environment()
        self.runner = self.env.create_local_runner()

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

    @property
    def grizzly(self) -> GrizzlyContext:
        return cast(GrizzlyContext, self.context.grizzly)

    def create_scenario(self, name: str) -> Scenario:
        return Scenario(filename=None, line=None, keyword='', name=name)

    def __enter__(self) -> 'BehaveFixture':
        runner = BehaveRunner(
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
        grizzly.state.locust = self._locust_fixture.runner
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
        request.source = REQUEST_TASK_TEMPLATE_CONTENTS
        request.scenario = GrizzlyContextScenario(1)
        request.scenario.name = 'test-scenario'
        request.scenario.user.class_name = 'TestUser'
        request.scenario.context['host'] = 'http://example.com'
        request.scenario.behave = None

        request.scenario.tasks.add(request)

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


class GrizzlyFixture:
    request_task: RequestTaskFixture
    grizzly: GrizzlyContext
    behave: BehaveContext
    locust_env: Environment

    def __init__(self, request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> None:
        self.request_task = request_task
        self.behave = behave_fixture.context

    def __enter__(self) -> 'GrizzlyFixture':
        environ['GRIZZLY_CONTEXT_ROOT'] = path.abspath(path.join(self.request_task.context_root, '..'))
        self.grizzly = GrizzlyContext()
        self.grizzly.scenarios.append(self.request_task.request.scenario)

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

        self.locust_env = Environment(
            host=host,
            user_classes=[user_type],
        )

        self.request_task.request.scenario.description = self.request_task.request.scenario.name
        self.request_task.request.name = scenario_type.__name__

        user_type.host = host
        user_type._scenario = self.request_task.request.scenario
        user = user_type(self.locust_env)

        if not no_tasks:
            user_type.tasks = [scenario_type]
            scenario = scenario_type(parent=user)
        else:
            user_type.tasks = []
            scenario = None

        self.grizzly.state.locust = self.locust_env.create_local_runner()

        return self.locust_env, user, scenario

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


class ResponseContextManagerFixture:
    # borrowed from geventhttpclient.client._build_request
    def _build_request(self, method: str, request_url: str, body: Optional[str] = '', headers: Optional[Dict[str, Any]] = None) -> str:
        parsed = urlparse(request_url)

        request = method + ' ' + parsed.path + ' HTTP/1.1\r\n'

        for field, value in (headers or {}).items():
            request += field + ': ' + str(value) + '\r\n'
        request += '\r\n'

        return request

    def __call__(
        self,
        cls_rcm: Type[GrizzlyResponseContextManager],
        status_code: int,
        environment: Optional[Environment] = None,
        response_body: Optional[Any] = None,
        response_headers: Optional[Dict[str, Any]] = None,
        request_method: Optional[str] = None,
        request_body: Optional[Any] = None,
        request_headers: Optional[Dict[str, Any]] = None,
        url: Optional[str] = None,
        **kwargs: Dict[str, Any],
    ) -> GrizzlyResponseContextManager:
        name = kwargs['name']
        event: Any
        if environment is not None:
            event = RequestSilentFailureEvent(False)
        else:
            event = None

        if cls_rcm is ResponseContextManager:
            response = Response()
            if response_headers is not None:
                response.headers = CaseInsensitiveDict(**response_headers)

            if response_body is not None:
                if response.headers.get('Content-Type', None) in [None, 'application/json']:
                    response._content = jsondumps(response_body).encode('utf-8')
                else:
                    response._content = response_body
            response.status_code = status_code

            response.request = PreparedRequest()
            if request_headers is not None:
                response.request.headers = CaseInsensitiveDict(**request_headers)

            if request_body is not None:
                response.request.body = request_body.encode('utf-8')

            response.request.method = (request_method or 'GET').lower()

            if url is not None:
                response.url = response.request.url = url
        else:
            _build_request = self._build_request
            request_url = url

            class FakeGhcResponse(HTTPSocketPoolResponse):
                _headers_index: Optional[Headers]
                _sent_request: str
                _sock: Any

                def __init__(self) -> None:
                    self._headers_index = None

                    body: Optional[Any] = None
                    if request_headers is not None and CaseInsensitiveDict(**request_headers).get('Content-Type', None) in [None, 'application/json']:
                        body = jsondumps(request_body or '')
                    else:
                        body = request_body

                    self._sent_request = _build_request(
                        request_method or '',
                        request_url or '',
                        body=body or '',
                        headers=request_headers,
                    )
                    self._sock = None

                def get_code(self) -> int:
                    return status_code

            request = FastRequest(url, method=request_method, headers=Headers(), payload=request_body)
            for key, value in (request_headers or {}).items():
                request.headers.add(key, value)

            response = FastResponse(FakeGhcResponse(), request)
            response.headers = Headers()
            for key, value in (response_headers or {}).items():
                response.headers.add(key, value)

            if response_body is not None:
                response._cached_content = jsondumps(response_body).encode('utf-8')
            else:
                response._cached_content = None

            if request_body is not None:
                setattr(response, 'request_body', request_body)

            if environment is not None:
                environment.events.request = event
                event = environment

        response_context_manager = cls_rcm(response, event, {})
        response_context_manager._entered = True
        response_context_manager.request_meta = {
            'method': None,
            'name': name,
            'response_time': 1.0,
            'content_size': 1337,
            'exception': None,
        }

        return response_context_manager


class NoopZmqFixture:
    _mocker: MockerFixture

    _mocks: Dict[str, MagicMock]

    def __init__(self, mocker: MockerFixture) -> None:
        self._mocker = mocker
        self._mocks = {}

    def __call__(self, prefix: str) -> None:
        targets = [
            'zmq.Context.term',
            'zmq.Context.__del__',
            'zmq.Socket.bind',
            'zmq.Socket.connect',
            'zmq.Socket.send_json',
            'zmq.Socket.send',
            'zmq.Socket.recv_json',
            'zmq.Socket.recv_multipart',
            'zmq.Socket.send_multipart',
            'zmq.Socket.disconnect',
            'zmq.Socket.send_string',
            'zmq.Poller.poll',
            'zmq.Poller.register',
            'gsleep',
        ]

        for target in targets:
            try:
                self._mocks.update({target: self._mocker.patch(
                    f'{prefix}.{target}',
                    autospec=True,
                )})
            except AttributeError as e:
                if 'gsleep' in str(e):
                    continue

                raise

    def get_mock(self, attr: str) -> MagicMock:
        mock = self._mocks.get(attr, None)

        if mock is not None:
            return mock

        for full_attr, mock in self._mocks.items():
            _, last_part = full_attr.rsplit('.', 1)

            if last_part == attr:
                return mock

        raise AttributeError(f'no mocks for {attr}')


BehaveKeyword = Literal['Then', 'Given', 'And', 'When']


class End2EndValidator:
    name: str
    implementation: Any
    table: Optional[List[Dict[str, str]]]

    def __init__(
        self,
        name: str,
        implementation: Callable[[BehaveContext], None],
        table: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        self.name = name
        self.implementation = implementation
        self.table = table

    @property
    def expression(self) -> str:
        lines: List[str] = [f'Then run validator {self.name}_{self.implementation.__name__}']
        if self.table is not None and len(self.table) > 0:
            lines.append(f'  | {" | ".join([key for key in self.table[0].keys()])} |')

            for row in self.table:
                lines.append(f'  | {" | ".join([value for value in row.values()])} |')

        return '\n'.join(lines)

    @property
    def impl(self) -> str:
        source_lines = inspect.getsource(self.implementation).split('\n')
        source_lines[0] = dedent(source_lines[0].replace('def ', f'def {self.name}_'))
        source = '\n'.join(source_lines)

        return f'''@then(u'run validator {self.name}_{self.implementation.__name__}')
def {self.name}_{self.implementation.__name__}_wrapper(context: Context) -> None:
    {dedent(source)}
    if on_local(context) or on_worker(context):
        {self.name}_{self.implementation.__name__}(context)
'''


class End2EndFixture:
    _tmp_path_factory: TempPathFactory
    _env: Dict[str, str]
    _validators: Dict[Optional[str], List[End2EndValidator]]
    _distributed: bool

    _after_features: Dict[str, Callable[[BehaveContext, Feature], None]]
    _before_features: Dict[str, Callable[[BehaveContext, Feature], None]]

    _root: Optional[Path]

    _has_pymqi: Optional[bool]

    cwd: Path
    test_tmp_dir: Path
    _tmp_path_factory_basetemp: Optional[Path]
    webserver: Webserver

    def __init__(self, tmp_path_factory: TempPathFactory, webserver: Webserver, distributed: bool) -> None:
        self.test_tmp_dir = (Path(__file__) / '..' / '..' / '.pytest_tmp').resolve()
        self._tmp_path_factory_basetemp = tmp_path_factory._basetemp
        self.webserver = webserver
        tmp_path_factory._basetemp = self.test_tmp_dir

        self._tmp_path_factory = tmp_path_factory
        self.cwd = Path(getcwd())
        self._env = {}
        self._validators = {}
        self._root = None
        self._after_features = {}
        self._before_features = {}
        self._distributed = distributed
        self._has_pymqi = None

    @property
    def mode_root(self) -> Path:
        if self._root is None:
            raise AttributeError('root is not set')

        if self._distributed:
            return self.cwd
        else:
            return self._root

    @property
    def root(self) -> Path:
        if self._root is None:
            raise AttributeError('root is not set')

        return self._root

    @property
    def mode(self) -> str:
        return 'dist' if self._distributed else 'local'

    @property
    def host(self) -> str:
        if self._distributed:
            hostname = 'master'
        else:
            hostname = 'localhost'

        return f'{hostname}:{self.webserver.port}'

    def has_pymqi(self) -> bool:
        if self._has_pymqi is None:
            requirements_file = self.root / 'requirements.txt'

            self._has_pymqi = '[mq]' in requirements_file.read_text()

        return self._has_pymqi

    def __enter__(self) -> 'End2EndFixture':
        project_name = 'test-project'
        test_context = self._tmp_path_factory.mktemp('test_context')

        virtual_env_path = test_context / 'grizzly-venv'

        # create virtualenv
        rc, output = run_command(
            ['python3', '-m', 'venv', virtual_env_path.name],
            cwd=str(test_context),
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))

            raise

        path = environ.get('PATH', '')

        self._env.update({
            'PATH': f'{str(virtual_env_path)}/bin:{path}',
            'VIRTUAL_ENV': str(virtual_env_path),
            'PYTHONPATH': environ.get('PYTHONPATH', '.'),
        })

        for env_key in ['SSH_AUTH_SOCK', 'GRIZZLY_MOUNT_CONTEXT']:
            env_value = environ.get(env_key, None)
            if env_value is not None:
                self._env.update({env_key: env_value})

        # install grizzly-cli
        rc, output = run_command(
            ['python3', '-m', 'pip', 'install', 'grizzly-loadtester-cli'],
            cwd=str(test_context),
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise

        # create grizzly project
        rc, output = run_command(
            ['grizzly-cli', 'init', '--yes', project_name],
            cwd=str(test_context),
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise

        self._root = test_context / project_name

        assert self._root.is_dir()

        (self._root / 'features' / f'{project_name}.feature').unlink()

        step_start_webserver = '''

@then(u'start webserver on master port "{{port:d}}"')
def step_start_webserver(context: Context, port: int) -> None:
    from grizzly.locust import on_master
    if not on_master(context):
        return

    from importlib.machinery import SourceFileLoader

    w = SourceFileLoader(
        'steps.webserver',
        '{}/features/steps/webserver.py',
    ).load_module('steps.webserver')

    webserver = w.Webserver(port)
    webserver.start()
'''

        # create base test-project ... steps.py
        with open(self._root / 'features' / 'steps' / 'steps.py', 'w') as fd:
            fd.write('from importlib import import_module\n')
            fd.write('from typing import cast, Callable, Any\n\n')
            fd.write('from behave import then\n')
            fd.write('from behave.runner import Context\n')
            fd.write('from grizzly.locust import on_master, on_worker, on_local\n')
            fd.write('from grizzly.context import GrizzlyContext, GrizzlyContextScenario\n')
            fd.write('from grizzly.tasks import GrizzlyTask\n')
            fd.write('from grizzly.scenarios import GrizzlyScenario\n')
            fd.write('from grizzly.steps import *\n')

        if self._distributed:
            # copy examples
            source = (Path(__file__) / '..' / '..' / 'example').resolve()
            destination = test_context / 'test-example'
            copytree(source, destination)

            # rewrite test requirements.txt to point to local code
            for root in [self._root, destination]:
                with open(f'{root}/requirements.txt', 'r+') as fd:
                    fd.truncate(0)
                    fd.flush()
                    fd.write('grizzly-loadtester\n')

                with open(root / 'features' / 'steps' / 'steps.py', 'a') as fd:
                    fd.write(
                        step_start_webserver.format(
                            str(root).replace(f'{Path.cwd()}', '/srv/grizzly'),
                        )
                    )

                # create steps/webserver.py
                webserver_source = self.test_tmp_dir.parent / 'tests' / 'webserver.py'
                webserver_destination = root / 'features' / 'steps' / 'webserver.py'

                webserver_destination.write_text(webserver_source.read_text())

            command = ['grizzly-cli', 'dist', '--project-name', self._root.name, 'build', '--no-cache', '--local-install']
            rc, output = run_command(
                command,
                cwd=str(self.mode_root),
                env=self._env,
            )
            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                raise
        else:
            # install dependencies, in local venv
            grizzly_package = '.'
            if self.has_pymqi():
                grizzly_package = f'{grizzly_package}[mq]'

            rc, output = run_command(
                ['python3', '-m', 'pip', 'install', grizzly_package],
                cwd=str(self.test_tmp_dir.parent),
                env=self._env,
            )

            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                raise

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        # reset fixture basetemp
        self._tmp_path_factory._basetemp = self._tmp_path_factory_basetemp

        if exc is None:
            if self._distributed:
                rc, output = run_command(
                    ['grizzly-cli', 'dist', '--project-name', self.root.name, 'clean'],
                    cwd=str(self.mode_root),
                    env=self._env,
                )

                if rc != 0:
                    print(''.join(output))

            if environ.get('KEEP_FILES', None) is None:
                try:
                    rmtree(self.root.parent, onerror=onerror)
                except AttributeError:
                    pass
            else:
                print(self._root)

        return True

    def add_validator(
        self,
        implementation: Callable[[BehaveContext], None],
        /,
        scenario: Optional[str] = None,
        table: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        callee = inspect.stack()[1].function

        if self._validators.get(scenario, None) is None:
            self._validators[scenario] = []

        self._validators[scenario].append(End2EndValidator(callee, implementation, table))

    def add_after_feature(self, implementation: Callable[[BehaveContext, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._after_features[callee] = implementation

    def add_before_feature(self, implementation: Callable[[BehaveContext, Feature], None]) -> None:
        callee = inspect.stack()[1].function
        self._before_features[callee] = implementation

    def test_steps(self, /, scenario: Optional[List[str]] = None, background: Optional[List[str]] = None, identifier: Optional[str] = None) -> str:
        callee = inspect.stack()[1].function
        contents: List[str] = ['Feature:']
        add_user_count_step = True
        add_user_type_step = True
        add_spawn_rate_step = True

        if background is None:
            background = []

        if scenario is None:
            scenario = []

        # check required steps
        for step in background + scenario:
            if re.match(r'Given "[^"]*" user[s]?', step) is not None:
                add_user_count_step = False

            if re.match(r'Given a user of type "[^"]*"', step) is not None:
                add_user_type_step = False

            if re.match(r'(And|Given) spawn rate is "[^"]*" user[s]? per second', step) is not None:
                add_spawn_rate_step = False

        if add_user_count_step:
            background.insert(0, 'Given "1" user')
            if add_spawn_rate_step:
                background.insert(1, 'And spawn rate is "1" user per second')

        if add_user_type_step:
            scenario.insert(0, f'Given a user of type "RestApi" load testing "http://{self.host}"')

        if add_spawn_rate_step and not add_user_count_step:
            background.append('And spawn rate is "1" user per second')

        if self._distributed and not any([step.strip().startswith('Then start webserver on master port') for step in background]):
            background.append(f'Then start webserver on master port "{self.webserver.port}"')

        contents.append('  Background: common configuration')
        for step in background:
            contents.append(f'    {step}')

        contents.append('')

        contents.append(f'  Scenario: {callee}')
        for step in scenario or []:
            contents.append(f'    {step}')
        contents.append('    Then log message "dummy"\n')

        return self.create_feature(
            '\n'.join(contents),
            name=callee,
            identifier=identifier,
        )

    def create_feature(self, contents: str, name: Optional[str] = None, identifier: Optional[str] = None) -> str:
        if name is None:
            name = inspect.stack()[1].function

        if identifier is not None:
            identifier = sha1(identifier.encode()).hexdigest()[:8]
            name = f'{name}_{identifier}'

        feature_lines = contents.strip().split('\n')
        feature_lines[0] = f'Feature: {name}'
        steps_file = self.root / 'features' / 'steps' / 'steps.py'
        environment_file = self.root / 'features' / 'environment.py'

        scenario: Optional[str] = None
        indentation = '    '
        modified_feature_lines: List[str] = []
        offset = 0  # number of added steps

        for nr, line in enumerate(feature_lines):
            modified_feature_lines.append(line)

            last_line = nr == len(feature_lines) - 1
            scenario_definition = line.strip().startswith('Scenario:')

            if scenario_definition or last_line:
                if scenario is not None:
                    validators = self._validators.get(scenario, self._validators.get(None, None))
                    if validators is not None:
                        for validator in validators:
                            nr += offset
                            validator_expression = indent(f'{validator.expression}', prefix=indentation * 2)
                            index = nr
                            while modified_feature_lines[index].strip() == '' or 'Scenario:' in modified_feature_lines[index]:
                                index -= 1

                            index += 1
                            modified_feature_lines.insert(index, validator_expression)

                            offset += 1

                if scenario_definition:
                    scenario = line.replace('Scenario:', '').strip()
                    indentation, _ = line.split('Scenario:', 1)

        modified_feature_lines.append('')

        contents = '\n'.join(modified_feature_lines)

        # write feature file
        with open(self.root / 'features' / f'{name}.feature', 'w+') as fd:
            fd.write(contents)

        feature_file_name = fd.name.replace(f'{self.root}/', '')

        # cache current step implementations
        with open(steps_file, 'r') as fd:
            steps_impl = fd.read()

        # add step implementations
        with open(steps_file, 'a') as fd:
            added_validators: List[str] = []
            for validators in self._validators.values():
                for validator in validators:
                    # write expression and step implementation to steps/steps.py
                    if validator.impl not in steps_impl and validator.impl not in added_validators:
                        fd.write(f'\n\n{validator.impl}')
                        added_validators.append(validator.impl)

            added_validators = []

        # add after_feature hook, always write all of 'em
        with open(environment_file, 'w') as fd:
            fd.write('from typing import Any, Tuple, Dict, cast\n\n')
            fd.write('from behave.runner import Context\n')
            fd.write('from behave.model import Feature\n')
            fd.write('from grizzly.context import GrizzlyContext\n')
            fd.write((
                'from grizzly.environment import before_feature as grizzly_before_feature, '
                'after_feature as grizzly_after_feature, before_scenario, after_scenario, before_step\n\n'
            ))

            fd.write('def before_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:\n')
            if len(self._before_features) > 0:
                for feature_name in self._before_features.keys():
                    fd.write(f'    if feature.name == "{feature_name}":\n')
                    fd.write(f'        {feature_name}_before_feature(context, feature)\n\n')
            fd.write('    grizzly_before_feature(context, feature)\n\n')

            for key, before_feature_impl in self._before_features.items():
                source_lines = dedent(inspect.getsource(before_feature_impl)).split('\n')
                source_lines[0] = re.sub(r'^def .*?\(', f'def {key}_before_feature(', source_lines[0])
                source = '\n'.join(source_lines)

                fd.write(source + '\n\n')

            fd.write('def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:\n')
            fd.write('    grizzly_after_feature(context, feature)\n\n')
            if len(self._after_features) > 0:
                for feature_name in self._after_features.keys():
                    fd.write(f'    if feature.name == "{feature_name}":\n')
                    fd.write(f'        {feature_name}_after_feature(context, feature)\n\n')

            for key, after_feature_impl in self._after_features.items():
                source_lines = dedent(inspect.getsource(after_feature_impl)).split('\n')
                source_lines[0] = re.sub(r'^def .*?\(', f'def {key}_after_feature(', source_lines[0])
                source = '\n'.join(source_lines)

                fd.write(source + '\n\n')

        # step validators are are now "burned"...
        self._validators.clear()

        return feature_file_name

    def execute(self, feature_file: str, env_conf_file: Optional[str] = None, testdata: Optional[Dict[str, str]] = None) -> Tuple[int, List[str]]:
        if self._distributed:
            root = (Path(__file__) / '..' / '..').resolve()
            feature_file_root = str(self.root).replace(f'{root}/', '')
            feature_file = f'{feature_file_root}/{feature_file}'

        command = [
            'grizzly-cli',
            self.mode,
            'run',
            '--yes',
            '--verbose',
            feature_file,
        ]

        if self._distributed:
            command = command[:2] + ['--project-name', self.root.name] + command[2:]

        if env_conf_file is not None:
            command += ['-e', env_conf_file]

        if testdata is not None:
            for key, value in testdata.items():
                command += ['-T', f'{key}={value}']

        rc, output = run_command(
            command,
            cwd=str(self.mode_root),
            env=self._env,
        )

        if rc != 0:
            print(''.join(output))

            for container in ['master', 'worker'] if self._distributed else []:
                command = ['docker', 'container', 'logs', f'{self.root.name}-{getuser()}_{container}_1']
                _, output = run_command(
                    command,
                    cwd=str(self.mode_root),
                    env=self._env,
                )

                print(''.join(output))

        return rc, output
