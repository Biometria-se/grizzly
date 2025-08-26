"""Fixtures used in tests."""

from __future__ import annotations

import inspect
import re
import sys
from contextlib import suppress
from getpass import getuser
from hashlib import sha1
from json import dumps as jsondumps
from os import chdir, environ
from os.path import pathsep, sep
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper, gettempdir
from textwrap import dedent, indent
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlparse

import yaml
from behave.configuration import Configuration
from behave.model import Background
from behave.runner import Runner as BehaveRunner
from behave.step_registry import registry as step_registry
from geventhttpclient.header import Headers
from geventhttpclient.response import HTTPSocketPoolResponse
from grizzly import context as grizzly_context
from grizzly.context import GrizzlyContext
from grizzly.tasks import RequestTask
from grizzly.testdata.variables import destroy_variables
from grizzly.types import RequestMethod, StrDict
from grizzly.types.behave import Context as BehaveContext
from grizzly.types.behave import Feature, Scenario, Step
from grizzly.types.locust import Environment, LocalRunner
from grizzly.utils import create_scenario_class_type, create_user_class_type
from locust.contrib.fasthttp import FastRequest, FastResponse
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from pytest_mock.plugin import MockerFixture
from requests.models import CaseInsensitiveDict

from test_framework.helpers import TestScenario, TestUser, rm_rf, run_command

try:
    import pymqi
except ModuleNotFoundError:
    from grizzly_common import dummy_pymqi as pymqi

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from types import TracebackType
    from unittest.mock import MagicMock

    from _pytest.tmpdir import TempPathFactory
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import Self
    from grizzly.users import GrizzlyUser

    from test_framework.webserver import Webserver


__all__ = [
    'AtomicVariableCleanupFixture',
    'BehaveFixture',
    'GrizzlyFixture',
    'LocustFixture',
    'MockerFixture',
    'NoopZmqFixture',
    'RequestTaskFixture',
]


class AtomicVariableCleanupFixture:
    def __call__(self) -> None:
        destroy_variables()


class LocustFixture:
    _test_context_root: Path
    _tmp_path_factory: TempPathFactory

    environment: Environment
    runner: LocalRunner

    def __init__(self, tmp_path_factory: TempPathFactory) -> None:
        self._tmp_path_factory = tmp_path_factory

    def __enter__(self) -> Self:
        test_context = self._tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        self._test_context_root = test_context.parent

        environ['GRIZZLY_CONTEXT_ROOT'] = str(self._test_context_root)
        self.environment = Environment()
        self.runner = self.environment.create_local_runner()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']

        rm_rf(self._test_context_root)

        with suppress(Exception):
            self.runner.quit()

        return True


class EnvFixture:
    env: dict[str, str]

    def __init__(self) -> None:
        self.env = {}

    def __call__(self, key: str, value: str) -> None:
        self.env.update({key: value})
        environ.update(self.env)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        for key in self.env:
            with suppress(KeyError):
                del environ[key]

        return True


class CwdFixture:
    cwd: Path
    old_cwd: Path

    def __call__(self, cwd: Path) -> Self:
        self.cwd = cwd

        return self

    def __enter__(self) -> Self:
        self.old_cwd = Path.cwd()

        chdir(self.cwd)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        chdir(self.old_cwd)

        return True


class BehaveFixture:
    locust: LocustFixture
    context: BehaveContext

    def __init__(self, locust_fixture: LocustFixture) -> None:
        self.locust = locust_fixture

    @property
    def grizzly(self) -> GrizzlyContext:
        return cast('GrizzlyContext', self.context.grizzly)

    def create_scenario(self, name: str) -> Scenario:
        return Scenario(filename=None, line=None, keyword='', name=name)

    def create_step(self, name: str, *, in_background: bool = False, context: BehaveContext | None = None) -> Step:
        step = Step(filename=None, line=None, keyword='given', step_type='given', name=name, text=None, table=None)
        step.in_background = in_background

        if context is not None:
            context.step = step

        return step

    def __enter__(self) -> Self:
        runner = BehaveRunner(
            config=Configuration(
                command_args=[],
                load_config=False,
            ),
        )
        context = BehaveContext(runner)
        context._runner = runner
        context.config.base_dir = self.locust._test_context_root
        context.feature = Feature(filename=None, line=None, keyword='Feature', name='BehaveFixtureFeature')
        context.scenario = Scenario(filename=None, line=None, keyword='Scenario', name='BehaveFixtureScenario')
        context.step = Step(filename=None, line=None, keyword='Step', step_type='step', name='')
        context.scenario.steps = [context.step]
        context.scenario.background = Background(filename=None, line=None, keyword='', steps=[context.step], name='')
        context._runner.step_registry = step_registry
        context.grizzly = grizzly_context.grizzly
        context.grizzly.state.locust = self.locust.runner
        context.exceptions = {}

        self.context = context

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        if hasattr(grizzly_context.grizzly.state, 'locust') and grizzly_context.grizzly.state.locust is not None:
            del grizzly_context.grizzly.state.locust

        grizzly_context.grizzly = GrizzlyContext()

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
    test_context: Path
    behave_fixture: BehaveFixture

    def __init__(self, tmp_path_factory: TempPathFactory, behave_fixture: BehaveFixture) -> None:
        self._tmp_path_factory = tmp_path_factory
        self.behave_fixture = behave_fixture

    def __enter__(self) -> Self:
        self.test_context = self._tmp_path_factory.mktemp('example_payload') / 'requests'
        self.test_context.mkdir()
        request_file = self.test_context / 'payload.j2.json'
        request_file.touch()
        request_file.write_text(REQUEST_TASK_TEMPLATE_CONTENTS)
        request_path = str(request_file.parent)

        request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
        request.source = REQUEST_TASK_TEMPLATE_CONTENTS

        self.context_root = request_path
        self.request = request
        self.relative_path = str(request_file).replace(f'{request_path}/', '')

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        rm_rf(Path(self.context_root).parent)

        return True


class GrizzlyFixture:
    request_task: RequestTaskFixture
    behave: BehaveFixture

    def __init__(self, request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> None:
        self.request_task = request_task
        self.behave = behave_fixture

    @property
    def test_context(self) -> Path:
        return self.request_task.test_context.parent

    @property
    def grizzly(self) -> GrizzlyContext:
        return cast('GrizzlyContext', self.behave.context.grizzly)

    def __enter__(self) -> Self:
        environ['GRIZZLY_CONTEXT_ROOT'] = Path(self.request_task.context_root).parent.as_posix()
        self.grizzly.scenarios.clear()
        self.grizzly.scenarios.create(self.behave.create_scenario('test scenario'))
        self.grizzly.scenario.user.class_name = 'TestUser'
        self.grizzly.scenario.context['host'] = 'http://example.com'
        self.grizzly.scenario.tasks.add(self.request_task.request)
        self.grizzly.state.verbose = True

        return self

    def __call__(
        self,
        host: str = '',
        user_type: type[GrizzlyUser] | None = None,
        scenario_type: type[GrizzlyScenario] | None = None,
        *,
        no_tasks: bool | None = False,
    ) -> GrizzlyScenario:
        if user_type is None:
            user_type = TestUser

        user_class_name = user_type.__name__ if user_type.__module__.startswith('grizzly.users') else f'{user_type.__module__}.{user_type.__name__}'

        if scenario_type is None:
            scenario_type = TestScenario

        scenario_class_name = scenario_type.__name__ if scenario_type.__module__ == 'grizzly.scenarios' else f'{scenario_type.__module__}.{scenario_type.__name__}'

        self.grizzly.scenario.user.class_name = user_class_name
        self.grizzly.scenario.context['host'] = host
        self.request_task.request.name = scenario_type.__name__

        scenario_type = create_scenario_class_type(scenario_class_name, self.grizzly.scenario)
        user_type = create_user_class_type(self.grizzly.scenario)

        self.behave.locust.environment = Environment(
            host=host,
            user_classes=[user_type],
        )

        user_type.host = host
        user = user_type(self.behave.locust.environment)

        if not no_tasks:
            user_type.tasks = [scenario_type]
        else:
            user_type.tasks = []

        scenario = scenario_type(parent=user)

        self.grizzly.state.locust = self.behave.locust.environment.create_local_runner()

        return scenario

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']

        return True


class ResponseContextManagerFixture:
    # borrowed from geventhttpclient.client._build_request
    def _build_request(self, method: str, request_url: str, body: str | None = '', headers: StrDict | None = None) -> str:  # noqa: ARG002
        parsed = urlparse(request_url)

        request = method + ' ' + parsed.path + ' HTTP/1.1\r\n'

        for field, value in (headers or {}).items():
            request += field + ': ' + str(value) + '\r\n'
        request += '\r\n'

        return request

    def __call__(
        self,
        status_code: int,
        response_body: Any | None = None,
        response_headers: StrDict | None = None,
        request_method: str | None = None,
        request_body: Any | None = None,
        request_headers: StrDict | None = None,
        url: str | None = None,
        **kwargs: StrDict,
    ) -> FastResponseContextManager:
        name = kwargs['name']

        _build_request = self._build_request
        request_url = url

        class FakeGhcResponse(HTTPSocketPoolResponse):
            _headers_index: Headers | None
            _sent_request: str
            _sock: Any

            def __init__(self) -> None:
                self._headers_index = None

                body: Any | None = None
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
            response.request_body = request_body

        response_context_manager = FastResponseContextManager(response, None, {})
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

    _mocks: dict[str, MagicMock]

    def __init__(self, mocker: MockerFixture) -> None:
        self._mocker = mocker
        self._mocks = {}

    def __call__(self, prefix: str) -> None:
        targets = [
            'zmq.Context.term',
            'zmq.Context.__del__',
            'zmq.Socket.bind',
            'zmq.Socket.connect',
            'zmq.Socket.close',
            'zmq.Socket.send_json',
            'zmq.Socket.send',
            'zmq.Socket.recv_json',
            'zmq.Socket.recv_multipart',
            'zmq.Socket.send_multipart',
            'zmq.Socket.disconnect',
            'zmq.Socket.setsockopt',
            'zmq.Socket.send_string',
            'zmq.Poller.poll',
            'zmq.Poller.register',
            'gsleep',
        ]

        for target in targets:
            try:
                self._mocks.update(
                    {
                        target: self._mocker.patch(
                            f'{prefix}.{target}',
                        ),
                    },
                )
            except AttributeError as e:  # noqa: PERF203
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

        message = f'no mocks for {attr}'
        raise AttributeError(message)


BehaveKeyword = Literal['Then', 'Given', 'And', 'When']


class End2EndValidator:
    name: str
    implementation: Any
    table: list[dict[str, str]] | None

    def __init__(
        self,
        name: str,
        implementation: Callable[[BehaveContext], None],
        table: list[dict[str, str]] | None = None,
    ) -> None:
        self.name = name
        self.implementation = implementation
        self.table = table

    @property
    def expression(self) -> str:
        lines: list[str] = [f'Then run validator {self.name}_{self.implementation.__name__}']
        if self.table is not None and len(self.table) > 0:
            lines.append(f'  | {" | ".join(list(self.table[0].keys()))} |')
            lines.extend([f'  | {" | ".join(list(row.values()))} |' for row in self.table])

        return '\n'.join(lines)

    @property
    def impl(self) -> str:
        source_lines = inspect.getsource(self.implementation).split('\n')
        source_lines[0] = dedent(source_lines[0].replace('def ', f'def {self.name}_'))
        source = '\n'.join(source_lines)

        return f"""@then(u'run validator {self.name}_{self.implementation.__name__}')
def {self.name}_{self.implementation.__name__}_wrapper(context: Context) -> None:
    {dedent(source)}
    if on_local(context) or on_worker(context):
        {self.name}_{self.implementation.__name__}(context)
"""


class End2EndFixture:
    step_start_webserver = """

@then(u'start webserver on master port "{{port:d}}"')
def step_start_webserver(context: Context, port: int) -> None:
    from grizzly.locust import on_master
    if not on_master(context):
        return

    import logging
    from importlib.machinery import SourceFileLoader

    logger = logging.getLogger('webserver')

    w = SourceFileLoader(
        'steps.webserver',
        '{}/features/steps/webserver.py',
    ).load_module('steps.webserver')

    webserver = w.Webserver(port)
    logger.info('starting webserver')
    webserver.start(logger)
    logger.info('webserver started')
"""

    _tmp_path_factory: TempPathFactory
    _env: dict[str, str]
    _validators: dict[str | None, list[End2EndValidator]]
    _distributed: bool

    _after_features: dict[str, Callable[[BehaveContext, Feature], None]]
    _before_features: dict[str, Callable[[BehaveContext, Feature], None]]

    _root: Path | None

    _has_pymqi: bool | None

    cwd: Path
    test_tmp_dir: Path
    _tmp_path_factory_basetemp: Path | None
    webserver: Webserver

    def __init__(self, tmp_path_factory: TempPathFactory, webserver: Webserver, *, distributed: bool) -> None:
        self.test_tmp_dir = (Path(__file__) / '..' / '..' / '..' / '.pytest_tmp').resolve()
        self.test_tmp_dir.mkdir(exist_ok=True)
        self._tmp_path_factory_basetemp = tmp_path_factory._basetemp
        tmp_path_factory._basetemp = self.test_tmp_dir

        self._tmp_path_factory = tmp_path_factory
        self.webserver = webserver
        self.cwd = (Path(__file__).parent / '..' / '..').resolve()
        self._env = environ.copy() if sys.platform == 'win32' else {}
        self._validators = {}
        self._root = None
        self._after_features = {}
        self._before_features = {}
        self._distributed = distributed
        self._has_pymqi = None
        self.profile = None

        temp_dir = environ.get('GRIZZLY_TMP_DIR', gettempdir())

        self.log_file = Path(temp_dir) / 'grizzly.log'
        self.log_file.unlink(missing_ok=True)

    @property
    def root(self) -> Path:
        if self._root is None:
            message = 'root is not set'
            raise AttributeError(message)

        return self._root

    @property
    def mode(self) -> str:
        return 'dist' if self._distributed else 'local'

    @property
    def host(self) -> str:
        hostname = 'master' if self._distributed else '127.0.0.1'

        return f'{hostname}:{self.webserver.port}'

    def has_pymqi(self) -> bool:
        if self._has_pymqi is None:
            requirements_file = self.root / 'requirements.txt'

            self._has_pymqi = '[mq]' in requirements_file.read_text()

        return self._has_pymqi

    def __enter__(self) -> Self:  # noqa: PLR0915
        project_name = 'test-project'
        virtual_env = environ.get('VIRTUAL_ENV')

        test_context = self._tmp_path_factory.mktemp('test_context')

        if virtual_env is None or '/hatch/env/virtual' not in virtual_env:
            virtual_env_path = test_context / 'grizzly-venv'

            # create virtualenv
            rc, output = run_command(
                ['python', '-m', 'venv', virtual_env_path.name],
                cwd=test_context,
            )

            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                with self.log_file.open('a+') as fd:
                    fd.write(''.join(output))

                raise
        else:
            virtual_env_path = Path(virtual_env)

        path = environ.get('PATH', '')
        virtual_env_bin_dir = 'Scripts' if sys.platform == 'win32' else 'bin'

        self._env.update(
            {
                'PATH': f'{virtual_env_path!s}{sep}{virtual_env_bin_dir}{pathsep}{path}',
                'VIRTUAL_ENV': f'{virtual_env_path!s}',
                'PYTHONPATH': environ.get('PYTHONPATH', '.'),
                'HOME': environ.get('HOME', '/'),
            },
        )

        for env_key in ['SSH_AUTH_SOCK', 'GRIZZLY_MOUNT_CONTEXT']:
            env_value = environ.get(env_key, None)
            if env_value is not None:
                self._env.update({env_key: env_value})

        # create grizzly project
        cmd = ['grizzly-cli', 'init', '--yes', project_name]
        if pymqi.__name__ != 'grizzly_common.dummy_pymqi':
            cmd.append('--with-mq')

        rc, output = run_command(
            cmd,
            cwd=test_context,
            env=self._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            with self.log_file.open('a+') as fd:
                fd.write(''.join(output))
            raise

        self._root = test_context / project_name

        assert self._root.is_dir()

        (self._root / 'features' / f'{project_name}.feature').unlink()

        # create base test-project ... steps.py
        with (self.root / 'features' / 'steps' / 'steps.py').open('w') as fd:
            fd.write('from importlib import import_module\n')
            fd.write('from typing import cast, Callable, Any\n\n')
            fd.write('from grizzly.types.behave import Context, then\n')
            fd.write('from grizzly.locust import on_master, on_worker, on_local\n')
            fd.write('from grizzly.context import GrizzlyContext, GrizzlyContextScenario\n')
            fd.write('from grizzly.tasks import GrizzlyTask\n')
            fd.write('from grizzly.scenarios import GrizzlyScenario\n')
            fd.write('from grizzly.steps import *\n')

        if self._distributed:
            # rewrite test requirements.txt to point to local code
            with (self.root / 'requirements.txt').open('r+') as fd:
                fd.truncate(0)
                fd.flush()
                fd.write('grizzly-loadtester\n')

            with (self.root / 'features' / 'steps' / 'steps.py').open('a') as fd:
                fd.write(
                    self.step_start_webserver.format(
                        str(self.root).replace(str(self.root), '/srv/grizzly'),
                    ),
                )

            # create steps/webserver.py
            webserver_source = self.test_tmp_dir.parent / 'tests' / 'test_framework' / 'webserver.py'
            webserver_destination = self.root / 'features' / 'steps' / 'webserver.py'
            webserver_destination.write_text(webserver_source.read_text())

            command = [
                'uv',
                'run',
                '--active',
                'grizzly-cli',
                'dist',
                '--project-name',
                self.root.name,
                'build',
                '--no-cache',
                '--local-install',
                self.root.as_posix(),
                '--verbose',
                '--no-progress',
            ]

            rc, output = run_command(
                command,
                cwd=self.cwd.parent,
                env=self._env,
            )
            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                with self.log_file.open('a+') as fd:
                    fd.write(''.join(output))
                raise
        else:
            # install dependencies, in local venv
            command = ['uv', 'sync', '--active', '--locked', '--package', 'grizzly-loadtester']
            if self.has_pymqi():
                command.extend(['--extra', 'mq'])
                self._env.update({'LD_LIBRARY_PATH': environ.get('LD_LIBRARY_PATH', '')})

            rc, output = run_command(
                command,
                cwd=self.cwd,
                env=self._env,
            )

            try:
                assert rc == 0
            except AssertionError:
                print(''.join(output))
                with self.log_file.open('a+') as fd:
                    fd.write(''.join(output))
                raise

        return self

    @property
    def keep_files(self) -> bool:
        return environ.get('KEEP_FILES', None) is not None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[True]:
        # reset fixture basetemp
        self._tmp_path_factory._basetemp = self._tmp_path_factory_basetemp

        if exc is None:
            if self._distributed and not self.keep_files:
                rc, output = run_command(
                    ['grizzly-cli', 'dist', '--project-name', self.root.name, 'clean'],
                    cwd=self.root,
                    env=self._env,
                )

                if rc != 0:
                    print(''.join(output))

            if not self.keep_files:
                with suppress(AttributeError):
                    rm_rf(self.root.parent)
            else:
                print(self._root)

        return True

    def add_validator(
        self,
        implementation: Callable[[BehaveContext], None],
        /,
        scenario: str | None = None,
        table: list[dict[str, str]] | None = None,
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

    def test_steps(self, /, scenario: list[str] | None = None, background: list[str] | None = None, identifier: str | None = None, *, add_dummy_step: bool = True) -> str:
        callee = inspect.stack()[1].function
        contents: list[str] = ['Feature:']
        add_user_count_step = True
        add_user_type_step = True
        add_spawn_rate_step = True
        add_iterations_step = True

        if background is None:
            background = []

        if scenario is None:
            scenario = []

        # check required steps
        for step in background + scenario:
            if re.match(r'Given "[^"]*" user[s]?', step) is not None:
                add_user_count_step = False

            if re.match(r'Given.*user[s]? of type "[^"]*"', step) is not None:
                add_user_type_step = False

            if re.match(r'(And|Given) spawn rate is "[^"]*" user[s]? per second', step) is not None:
                add_spawn_rate_step = False

            if re.match(r'And repeat for "[^"]*" iteration[s]?', step) is not None:
                add_iterations_step = False

        if add_user_count_step:
            background.insert(0, 'Given "1" user')
            if add_spawn_rate_step:
                background.insert(1, 'And spawn rate is "1" user per second')

        if add_user_type_step:
            scenario.insert(0, f'Given a user of type "RestApi" load testing "http://{self.host}"')

        if add_spawn_rate_step and not add_user_count_step:
            background.append('Given spawn rate is "1" user per second')

        if add_iterations_step:
            scenario.insert(1, 'And repeat for "1" iteration')

        if self._distributed and not any(step.strip().startswith('Then start webserver on master port') for step in background):
            background.append(f'Then start webserver on master port "{self.webserver.port}"')

        contents.append('  Background: common configuration')
        contents.extend([f'    {step}' for step in background])
        contents.append('')

        contents.append(f'  Scenario: {callee}')
        contents.extend([f'    {step}' for step in scenario or []])

        if add_dummy_step:
            contents.append('    Then log message "dummy"\n')

        return self.create_feature(
            '\n'.join(contents),
            name=callee,
            identifier=identifier,
        )

    def create_feature(self, contents: str, name: str | None = None, identifier: str | None = None) -> str:  # noqa: C901, PLR0912, PLR0915
        if name is None:
            name = inspect.stack()[1].function

        if identifier is not None:
            identifier = sha1(identifier.encode()).hexdigest()[:8]  # noqa: S324
            name = f'{name}_{identifier}'

        feature_lines = contents.strip().split('\n')
        feature_lines[0] = f'Feature: {name}'
        steps_file = self.root / 'features' / 'steps' / 'steps.py'
        environment_file = self.root / 'features' / 'environment.py'

        scenario: str | None = None
        indentation = '    '
        modified_feature_lines: list[str] = []
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
                            nr += offset  # noqa: PLW2901
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
        with (self.root / 'features' / f'{name}.feature').open('w+') as fd:
            fd.write(contents)
            feature_file_name = str(Path(fd.name).relative_to(self.root))

        # cache current step implementations
        steps_impl = steps_file.read_text()

        # add step implementations
        with steps_file.open('a') as fd:
            added_validators: list[str] = []
            for validators in self._validators.values():
                for validator in validators:
                    # write expression and step implementation to steps/steps.py
                    if validator.impl not in steps_impl and validator.impl not in added_validators:
                        fd.write(f'\n\n{validator.impl}')
                        added_validators.append(validator.impl)

            added_validators = []

        # add after_feature hook, always write all of 'em
        with environment_file.open('w') as fd:
            fd.write('from typing import Any, cast\n\n')
            fd.write('from grizzly.types.behave import Context, Feature\n')
            fd.write('from grizzly.context import GrizzlyContext\n')
            fd.write('from grizzly.behave import before_feature as grizzly_before_feature\n')
            fd.write('from grizzly.behave import after_feature as grizzly_after_feature\n')
            fd.write('from grizzly.behave import before_scenario, after_scenario, before_step\n\n')

            fd.write('def before_feature(context: Context, feature: Feature, *args: Any, **kwargs: Any) -> None:\n')
            if len(self._before_features) > 0:
                for feature_name in self._before_features:
                    fd.write(f'    if feature.name == "{feature_name}":\n')
                    fd.write(f'        {feature_name}_before_feature(context, feature)\n\n')
            fd.write('    grizzly_before_feature(context, feature)\n\n')

            for key, before_feature_impl in self._before_features.items():
                source_lines = dedent(inspect.getsource(before_feature_impl)).split('\n')
                source_lines[0] = re.sub(r'^def .*?\(', f'def {key}_before_feature(', source_lines[0])
                source = '\n'.join(source_lines)

                # "render" references to myself
                source = source.replace('{e2e_fixture.host}', self.host).replace('{e2e_fixture.webserver.auth_provider_uri}', self.webserver.auth_provider_uri)

                fd.write(source + '\n\n')

            fd.write('def after_feature(context: Context, feature: Feature, *args: Any, **kwargs: Any) -> None:\n')
            fd.write('    grizzly_after_feature(context, feature)\n\n')
            if len(self._after_features) > 0:
                for feature_name in self._after_features:
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

    def execute(
        self,
        feature_file: str,
        env_conf: StrDict | None = None,
        testdata: dict[str, str] | None = None,
        project_name: str | None = None,
        *,
        dry_run: bool = False,
    ) -> tuple[int, list[str]]:
        env_conf_fd: _TemporaryFileWrapper[bytes] | None = None
        if env_conf is not None:
            prefix = Path(feature_file).stem
            env_conf_fd = NamedTemporaryFile(delete=False, prefix=prefix, suffix='.yaml', dir=(self.root / 'environments'))  # noqa: SIM115

        if project_name is None:
            project_name = self.root.name

        try:
            command = [
                'grizzly-cli',
                self.mode,
                'run',
                '--yes',
                '--verbose',
                '-l',
                f'{self.log_file!s}',
                feature_file,
            ]

            if dry_run:
                command.append('--dry-run')

            if environ.get('PROFILE', 'false').lower() == 'true':
                command.append('--profile')

            if self._distributed:
                command = [*command[:2], '--project-name', project_name, *command[2:]]

            if env_conf_fd is not None:
                with env_conf_fd as env_conf_file:
                    env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
                    env_conf_file.flush()
                    env_conf_path = str(env_conf_file.name).replace(f'{self.root.as_posix()}/', '')
                    command += ['-e', env_conf_path]

            if testdata is not None:
                for key, value in testdata.items():
                    command += ['-T', f'{key}={value}']

            rc, output = run_command(
                command,
                cwd=self.root,
                env=self._env,
            )

            if rc != 0 and self._distributed:
                validate_command = [*command[:2], '--validate-config', *command[2:]]
                _, output = run_command(
                    validate_command,
                    cwd=self.root,
                    env=self._env,
                )
                output = []

                for container in ['master', 'worker']:
                    command = ['docker', 'container', 'logs', f'{project_name}-{getuser()}-{container}-1']
                    _, o = run_command(
                        command,
                        cwd=self.root,
                        env=self._env,
                    )

                    output += o
        finally:
            if env_conf_fd is not None and not self.keep_files:
                Path(env_conf_fd.name).unlink()

        if sys.platform == 'win32':
            output = [o.replace('\r', '') for o in output]

        return rc, output
