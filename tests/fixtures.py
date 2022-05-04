import pkgutil
import inspect
import socket
import csv
import re

from typing import TYPE_CHECKING, Optional, Union, Callable, Any, Literal, List, Tuple, Type, Dict, cast
from types import TracebackType
from unittest.mock import MagicMock
from urllib.parse import urlparse
from mypy_extensions import VarArg, KwArg
from os import chdir, environ, getcwd, path
from shutil import rmtree
from json import dumps as jsondumps
from pathlib import Path
from textwrap import dedent, indent

import gevent

from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import FastResponse
from geventhttpclient.header import Headers
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
from requests.models import CaseInsensitiveDict, Response, PreparedRequest
from flask import Flask, request, jsonify, Response as FlaskResponse

from grizzly.types import GrizzlyResponseContextManager, RequestMethod
from grizzly.tasks.request import RequestTask

import grizzly.testdata.variables as variables

from grizzly.context import GrizzlyContext, GrizzlyContextScenario

from .helpers import TestUser, TestScenario, RequestSilentFailureEvent
from .helpers import onerror, run_command

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

    @property
    def grizzly(self) -> GrizzlyContext:
        return cast(GrizzlyContext, self.context.grizzly)

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
        request.source = REQUEST_TASK_TEMPLATE_CONTENTS
        request.scenario = GrizzlyContextScenario(1)
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

        if cls_rcm == ResponseContextManager:
            response = Response()
            if response_headers is not None:
                response.headers = CaseInsensitiveDict(**response_headers)

            if response_body is not None:
                response._content = jsondumps(response_body).encode('utf-8')
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

            class FakeGhcResponse:
                _headers_index: Headers
                _sent_request: str

                def __init__(self) -> None:
                    self._headers_index = Headers()
                    for key, value in (response_headers or {}).items():
                        self._headers_index.add(key, value)

                    self._sent_request = _build_request(
                        request_method or '',
                        request_url or '',
                        body=jsondumps(request_body or ''),
                        headers=request_headers,
                    )

                def get_code(self) -> int:
                    return status_code

            response = FastResponse(FakeGhcResponse())
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


app = Flask(__name__)
root_dir = path.realpath(path.join(path.dirname(__file__), '..'))


@app.route('/api/v1/resources/dogs')
def get_dog_fact() -> FlaskResponse:
    _ = int(request.args.get('number', ''))

    return jsonify(['woof woof wooof'])


@app.route('/facts')
def get_cat_fact() -> FlaskResponse:
    _ = int(request.args.get('limit', ''))

    return jsonify(['meow meow meow'])


@app.route('/books/<book>.json')
def get_book(book: str) -> FlaskResponse:
    with open(f'{root_dir}/example/features/requests/books/books.csv', 'r') as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            if row['book'] == book:
                return jsonify({
                    'number_of_pages': row['pages'],
                    'isbn_10': [row['isbn_10']] * 2,
                    'authors': [
                        {'key': '/author/' + row['author'].replace(' ', '_').strip() + '|' + row['isbn_10'].strip()},
                    ]
                })


@app.route('/author/<author_key>.json')
def get_author(author_key: str) -> FlaskResponse:
    name, _ = author_key.rsplit('|', 1)

    return jsonify({
        'name': name.replace('_', ' ')
    })


class Webserver:
    _web_server: gevent.pywsgi.WSGIServer
    port: int

    def __init__(self) -> None:
        self._web_server = gevent.pywsgi.WSGIServer(
            ('127.0.0.1', 0),
            app,
            log=None,
        )

    def __enter__(self) -> 'Webserver':
        gevent.spawn(lambda: self._web_server.serve_forever())
        gevent.sleep(0.01)
        self.port = self._web_server.server_port

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        self._web_server.stop_accepting()
        self._web_server.stop()

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


class BehaveValidator:
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
        source_lines[0] = source_lines[0].replace('def ', f'def {self.name}_')
        source = '\n'.join(source_lines)

        return f'''@then(u'run validator {self.name}_{self.implementation.__name__}')
{dedent(source)}
'''


class BehaveContextFixture:
    _tmp_path_factory: TempPathFactory
    _cwd: str
    _env: Dict[str, str]
    _validators: List[BehaveValidator]

    root: Optional[Path]

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory
        self._cwd = getcwd()
        self._env = {}
        self._validators = []
        self.root = None

    def __enter__(self) -> 'BehaveContextFixture':
        test_context = self._tmp_path_factory.mktemp('test_context')

        # create grizzly project
        rc, output = run_command(
            ['grizzly-cli', 'init', '--yes', 'test-project'],
            cwd=str(test_context),
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise

        self.root = test_context / 'test-project'

        assert self.root.is_dir()

        # create virtualenv
        rc, output = run_command(
            ['python3', '-m', 'venv', '.virtual-env'],
            cwd=str(self.root),
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))

            raise

        path = environ.get('PATH', '')

        virtual_env_path = self.root / '.virtual-env'

        self._env.update({
            'PATH': f'{str(virtual_env_path)}/bin:{path}',
            'VIRTUAL_ENV': str(virtual_env_path),
        })

        # install dependencies
        rc, output = run_command(
            ['python3', '-m', 'pip', 'install', '-r', 'requirements.txt'],
            cwd=str(self.root),
            env=self._env,
        )

        chdir(self.root)

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        chdir(self._cwd)

        if self.root is not None:
            rmtree(self.root.parent.parent, onerror=onerror)

        return True

    def add_validator(
        self,
        implementation: Callable[[BehaveContext], None],
        /,
        table: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        callee = inspect.stack()[1].function
        self._validators.append(BehaveValidator(callee, implementation, table))

    def test_steps(self, /, scenario: Optional[List[str]] = None, background: Optional[List[str]] = None) -> str:
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
            if re.match(r'Given "[0-9]" user[s]?', step) is not None:
                add_user_count_step = False

            if re.match(r'Given a user of type "[^"]*"', step) is not None:
                add_user_type_step = False

            if re.match(r'And spawn rate is "[0-9]+" user[s]? per second', step) is not None:
                add_spawn_rate_step = False

        if add_user_count_step:
            background.insert(0, 'Given "1" user')

        if add_user_type_step:
            scenario.insert(0, 'Given a user of type "RestApi" load testing "http://localhost:1"')

        if add_spawn_rate_step:
            background.append('And spawn rate is "1" user per second')

        contents.append('  Background: common configuration')
        for step in background:
            contents.append(f'    {step}')

        contents.append('')

        if scenario is not None:
            contents.append(f'  Scenario: {callee}')
            for step in scenario or []:
                contents.append(f'    {step}')
            contents.append('    Then print message "dummy"')

        contents.append('')

        return self.create_feature('\n'.join(contents), name=callee)

    def create_feature(self, contents: str, name: Optional[str] = None) -> str:
        if self.root is None:
            raise FileNotFoundError('no test project created')

        if name is None:
            name = inspect.stack()[1].function

        with open(self.root / 'features' / f'{name}.feature', 'w+') as ffd:
            feature_lines = contents.split('\n')
            feature_lines[0] = f'Feature: test {name}'
            contents = '\n'.join(feature_lines)

            ffd.write(contents)

            with open(self.root / 'features' / 'steps' / 'steps.py', 'w') as sfd:
                sfd.write('import json\n\n')
                sfd.write('from typing import cast\n\n')
                sfd.write('from behave import then\n')
                sfd.write('from behave.runner import Context\n')
                sfd.write('from grizzly.context import GrizzlyContext\n')
                sfd.write('from grizzly.steps import *\n')
                for validator in self._validators:
                    # write step to feature file
                    ffd.write(indent(f'{validator.expression}\n', prefix='    '))

                    # write expression and step implementation to steps/steps.py
                    sfd.write(f'\n\n{validator.impl}')

            # they are now "burned"...
            self._validators.clear()

            return ffd.name.replace(f'{self.root}/', '')

    def execute(self, feature_file: str, env_conf_file: Optional[str] = None, testdata: Optional[Dict[str, str]] = None) -> Tuple[int, List[str]]:
        command = [
            'grizzly-cli',
            'local',
            'run',
            '--yes',
            feature_file,
        ]

        if env_conf_file is not None:
            command += ['-e', env_conf_file]

        if testdata is not None:
            for key, value in testdata.items():
                command += ['-T', f'{key}={value}']

        rc, output = run_command(
            command,
            cwd=str(self.root),
            env=self._env,
        )

        if rc != 0:
            print(''.join(output))

        return rc, output
